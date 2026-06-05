// rayx Phase 1 runtime: runtime-local cooperative cancel token (Slice 2a).
//
// A faithful, runtime-local MIRROR of the proven harness rayhpx::CancelToken state
// machine (hpx_impl/service_lane.hpp), typed on rayx_runtime::RuntimeResult instead
// of rayhpx::Result -- the runtime cannot literally reuse the harness token because
// it fulfills a DIFFERENT promise type. It is otherwise the same single-arbiter
// design:
//
//   * a std::mutex (NOT hpx::mutex, NO atomics / lock-free) guards all phase
//     transitions; every critical section is a nanosecond phase flip and the
//     promise is ALWAYS set OUTSIDE the lock, so an HPX worker that briefly locks
//     it is never meaningfully pinned;
//   * it holds its OWN copy of the request's hpx::promise plus the lane's actor_id
//     and NO pointer back to the lane, so cancel() is safe even after the owning
//     lane/runtime was destroyed (a stale cancel just sees a terminal phase);
//   * because of that self-containment, RuntimeFuture.cancel() runs DIRECTLY on the
//     external Python thread with NO hpx::run_as_hpx_thread hop (exactly as the
//     harness CancelToken does behind HpxLane).
//
// State machine (all transitions under m_, single arbiter):
//   Queued --cancel()-------------> Cancelled       (cancel() fulfills now; no value)
//   Queued --begin_service()------> Running
//   Running(cancellable) --cancel()--> StopRequested (cancel() True; LANE fulfills at a checkpoint)
//   StopRequested --stop_at_boundary--> Cancelled    (op returns cancelled; lane fulfills)
//   Running --mark_finished()-----> Completed        (op finished completed OR failed;
//                                                     terminal, so a late cancel loses)
//
// cancellable_ is armed by the CHECKPOINT COUNT (begin_service(checkpoint_count) ->
// cancellable_ = count > 1), NOT a bool. A checkpoint_count == 1 operation (every
// instantaneous op, and a short busy_sum) is QUEUED-cancelable only: once running,
// cancel() returns false. A checkpointed op clears cancellable_ at its final
// boundary (stop_at_boundary(next_is_final=true)) BEFORE its last segment, so a
// late cancel deterministically loses instead of returning true while the op
// completes. This is the race-free arming the harness uses (chunks > 1).

#ifndef RAYX_RUNTIME_CANCEL_HPP
#define RAYX_RUNTIME_CANCEL_HPP

#include "runtime_ops.hpp"  // rayx_runtime::RuntimeResult

#include <hpx/hpx.hpp>  // hpx::promise

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace rayx_runtime {

class RuntimeCancelToken {
public:
    // Wire the token to the request it can cancel: a copy of the promise the lane
    // will otherwise fulfill, plus the serving lane's actor_id (stamped onto a
    // queued-cancel result). Called by the lane at submit, before the item is
    // queued.
    void arm(std::shared_ptr<hpx::promise<RuntimeResult>> prom,
             std::string actor_id) {
        prom_ = std::move(prom);
        actor_id_ = std::move(actor_id);
    }

    // Client side (external Python thread; NO HPX hop). Returns true iff THIS call
    // settles a cancellation:
    //   * Queued -> Cancelled: skip all service; fulfilled HERE, now (no value).
    //     The promise is set OUTSIDE the lock so set_value never runs under m_.
    //   * Running & cancellable_ -> StopRequested: the lane stops the op at its next
    //     checkpoint and fulfills a cancelled result there (NOT fulfilled here).
    //     "True" means guaranteed-to-stop, not ready-now.
    // Returns false otherwise (already completed/cancelled/stop-requested, or
    // running a checkpoint_count == 1 op -- no boundary to stop at).
    bool cancel() {
        bool fulfill_now = false;
        {
            std::lock_guard<std::mutex> lk(m_);
            if (phase_ == Phase::Queued) {
                phase_ = Phase::Cancelled;
                fulfill_now = true;
            } else if (phase_ == Phase::Running && cancellable_) {
                phase_ = Phase::StopRequested;  // lane fulfills at the checkpoint
                return true;
            } else {
                return false;
            }
        }
        if (fulfill_now) {
            RuntimeResult r;
            r.actor_id = actor_id_;
            const std::int64_t t = now_ns_();
            r.start_ns = t;
            r.end_ns = t;  // no service time spent
            r.status = "cancelled";
            r.has_value = false;
            prom_->set_value(std::move(r));  // outside the lock
        }
        return true;
    }

    // Lane side: try to begin service. Returns true if the lane may run the op
    // (Queued -> Running); false if it was already Cancelled (queued cancel won --
    // cancel() fulfilled it, the lane must skip). checkpoint_count arms running-
    // cancellability: cancellable_ = (checkpoint_count > 1).
    bool begin_service(int checkpoint_count) {
        std::lock_guard<std::mutex> lk(m_);
        if (phase_ == Phase::Cancelled) return false;
        phase_ = Phase::Running;
        cancellable_ = (checkpoint_count > 1);
        return true;
    }

    // Op side (via the lane-bound StopCheckpoint predicate): the checkpoint arbiter,
    // called BEFORE committing to the next chunk (for chunks after the first).
    // Returns true if the op must STOP NOW (a stop was requested) -- the op returns
    // a cancelled outcome. Otherwise, if the chunk about to start is the FINAL one,
    // clears cancellable_ in the SAME critical section (so a concurrent cancel
    // deterministically loses), and returns false.
    bool stop_at_boundary(bool next_is_final) {
        std::lock_guard<std::mutex> lk(m_);
        if (phase_ == Phase::StopRequested) {
            phase_ = Phase::Cancelled;
            return true;
        }
        if (next_is_final) cancellable_ = false;
        return false;
    }

    // Lane side: mark a fulfilled-without-cancellation op TERMINAL (so a later
    // cancel() sees a terminal phase and returns false). Called by the worker after
    // it set_value()s a non-cancelled outcome -- both "completed" AND "failed" (a
    // failed result is just as terminal as a completed one; a late cancel must not
    // claim to cancel an already-failed future). cancellable_ is already false by
    // this point (count==1, or cleared at the final boundary). Phase::Completed is
    // the single non-cancelled terminal state (failed/completed are indistinguishable
    // to the token -- it only gates cancellability, not the row's status).
    void mark_finished() {
        std::lock_guard<std::mutex> lk(m_);
        if (phase_ == Phase::Running) phase_ = Phase::Completed;
    }

    // Outcome-settled view (non-consuming, any thread): true once cancellation is
    // guaranteed (queued cancel, or a requested running stop), before or after the
    // row is fulfilled/retired.
    bool is_cancelled() {
        std::lock_guard<std::mutex> lk(m_);
        return phase_ == Phase::Cancelled || phase_ == Phase::StopRequested;
    }

private:
    // Runtime monotonic clock -- the SAME steady_clock domain as rayhpx::now_ns()
    // (so a queued-cancel result's start_ns/end_ns are comparable to op-produced
    // rows), replicated inline to keep this header decoupled from the harness.
    static std::int64_t now_ns_() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    enum class Phase { Queued, Running, StopRequested, Cancelled, Completed };
    std::mutex m_;
    Phase phase_ = Phase::Queued;
    bool cancellable_ = false;  // Running with a remaining checkpoint boundary
    std::shared_ptr<hpx::promise<RuntimeResult>> prom_;
    std::string actor_id_;
};

}  // namespace rayx_runtime

#endif  // RAYX_RUNTIME_CANCEL_HPP
