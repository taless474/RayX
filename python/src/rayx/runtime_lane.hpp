// rayx Phase 1 runtime: HPX-native FIFO RuntimeLane (Slice 1).
//
// Part of the EXPERIMENTAL `rayx.runtime` prototype, fully SEPARATE from the
// shipped harness lane core (hpx_impl/service_lane.hpp / hpx_lane.hpp): it does
// NOT touch rayhpx::Request / rayhpx::Result / ServiceLane / HpxLane or the native
// baseline, and it adds NO field to the frozen v1 benchmark row.
//
// A `RuntimeLane` is a single serialized, HPX-native service lane that mirrors the
// proven rayhpx::HpxLane mechanism (see hpx_impl/hpx_lane.hpp), but its service
// slot runs an INJECTED operation-task closure instead of synthetic sleep/spin:
//
//   * the consumer is ONE long-lived hpx::thread, so cooperative suspension (the
//     worker's hpx::async(...).get() and the idle cv_ wait) actually yields the HPX
//     worker instead of blocking an OS thread;
//   * the FIFO queue is guarded by hpx::mutex + hpx::condition_variable_any, so the
//     idle "wait for work" SUSPENDS the HPX thread (frees the worker) rather than
//     pinning it -- a std::condition_variable here would starve an HPX worker (the
//     experiment-16 risk);
//   * the worker pops ONE item at a time and dispatches its task via
//     hpx::async(exec_, task).get(); because the worker is an HPX thread that .get()
//     is a COOPERATIVE HPX SUSPENSION, and FIFO holds because the single worker
//     retires one operation before popping the next.
//
// WHY a worker+queue, not a continuation chain: RayX lanes are SERVING-CONTROL
// objects (owned queue, service slot, drain, and -- in Slice 2 -- bounded admission
// and queued cancellation). The worker's own deque gives a coherent lane_stats
// (queue_depth / active) snapshot under one mutex and a real queue to admit against
// / skip from later; a continuation chain has no owned queue and would blur
// serving-control into task-graph semantics (see the design note, §11a).
//
// THREADING CONTRACT (important): the queue is guarded by hpx::mutex +
// hpx::condition_variable_any, which MUST be locked only from an HPX thread. The
// rayx RuntimeEngine runs on the external Python thread, so it hops every
// submit/stats/stop call through hpx::run_as_hpx_thread (see _rayx.cpp). The
// DESTRUCTOR is intentionally inert: stop/join happens via stop_and_join() under an
// HPX-thread hop BEFORE the lane is destroyed.
//
// Slice 2a adds cooperative cancellation: each queue Item carries a
// RuntimeCancelToken (runtime_cancel.hpp) and a checkpoint_count; the worker calls
// begin_service (queued-cancel skip) and binds a StopCheckpoint predicate (token
// poll + cooperative yield) for the op, and cancel_pending() lets shutdown cancel
// outstanding queued/in-flight work before draining. Bounded admission (try_submit)
// is still NOT here -- that is Slice 2b. runtime_ops.hpp stays HPX-free.

#ifndef RAYX_RUNTIME_LANE_HPP
#define RAYX_RUNTIME_LANE_HPP

#include "runtime_ops.hpp"    // rayx_runtime::RuntimeResult, make_runtime_actor_id, StopCheckpoint
#include "runtime_cancel.hpp"  // rayx_runtime::RuntimeCancelToken (Slice 2a)

#include <hpx/hpx.hpp>  // hpx::thread, hpx::mutex, hpx::condition_variable_any, hpx::async, hpx::promise, this_thread::yield

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace rayx_runtime {

// One serialized HPX-native lane. The op body is supplied as a ready-to-run
// closure (OpTask) so the lane stays decoupled from the registry / timing code in
// _rayx.cpp: the lane's only job is FIFO serialization + HPX-native dispatch +
// observability.
class RuntimeLane {
public:
    // The injected service-slot work: runs the operation body (passing it the
    // lane-bound StopCheckpoint so a checkpointed op can be cooperatively cancelled)
    // and returns a fully built RuntimeResult (actor_id / start_ns / end_ns /
    // status / error / value). Built by _rayx.cpp (which owns the registry lookup,
    // timing, and the value/failure/cancel mapping) and stamped with THIS lane's
    // actor_id at submit time.
    using OpTask = std::function<RuntimeResult(const StopCheckpoint& stop)>;

    // actor_id is generated BEFORE the worker is spawned; the hpx::thread is the
    // LAST thing the ctor touches, so nothing can throw after a joinable worker
    // exists (a joinable hpx::thread destroyed during ctor unwind would terminate).
    RuntimeLane() {
        actor_id_ = make_runtime_actor_id();
        worker_ = hpx::thread([this]() { run(); });
    }

    // Inert by contract: the real stop/join runs in stop_and_join() under an
    // HPX-thread hop BEFORE destruction (RuntimeEngine drives this). We never lock
    // hpx::mutex or join the worker here -- this dtor may run on a non-HPX thread,
    // where that is invalid. If stop_and_join() ran (the contract), worker_ is
    // already joined and ~hpx::thread is a no-op; a still-joinable worker here is a
    // teardown-contract violation.
    ~RuntimeLane() = default;

    RuntimeLane(const RuntimeLane&) = delete;
    RuntimeLane& operator=(const RuntimeLane&) = delete;

    const std::string& actor_id() const { return actor_id_; }

    // Observability snapshot (debugging only), mirroring HpxLane::stats(): a
    // point-in-time view of one lane -- queued-but-not-started depth and whether a
    // request is currently in the service slot. Briefly takes the lane hpx::mutex
    // for the queue size; `active` is an atomic flag. SNAPSHOT only, NON-consuming
    // (touches no future), NOT scheduler state / placement / JSONL schema. MUST be
    // called from an HPX thread (the RuntimeEngine hops here).
    struct LaneStat {
        std::string actor_id;
        int queue_depth = 0;  // queued, not yet popped/started
        bool active = false;  // an item has been popped and is in the service slot
    };
    LaneStat stats() {
        LaneStat s;
        s.actor_id = actor_id_;
        std::lock_guard<hpx::mutex> lk(mu_);
        s.queue_depth = static_cast<int>(queue_.size());
        s.active = active_.load(std::memory_order_relaxed);
        return s;
    }

    // Enqueue one op task; returns a future the lane resolves when the task runs,
    // and (via out_tok) the cancel token wired to that future so the caller can
    // cancel it. `checkpoint_count` arms running-cancellability (begin_service).
    // MUST be called from an HPX thread (hpx::mutex); the RuntimeEngine hops here
    // via run_as_hpx_thread. Slice 2a: every single-submit carries a token; no
    // admission cap yet (that is Slice 2b).
    hpx::future<RuntimeResult> submit(
            OpTask task, int checkpoint_count,
            std::shared_ptr<RuntimeCancelToken>* out_tok) {
        auto prom = std::make_shared<hpx::promise<RuntimeResult>>();
        hpx::future<RuntimeResult> fut = prom->get_future();
        auto tok = std::make_shared<RuntimeCancelToken>();
        tok->arm(prom, actor_id_);  // token fulfills this promise on a queued cancel
        {
            std::lock_guard<hpx::mutex> lk(mu_);
            queue_.push_back(
                Item{std::move(task), std::move(prom), tok, checkpoint_count});
        }
        cv_.notify_one();
        if (out_tok) *out_tok = std::move(tok);
        return fut;
    }

    // Bounded-admission enqueue (Slice 2b, capped path only): like submit(), but
    // ADMIT only if this lane currently holds fewer than `max_queue_depth`
    // queued-but-not-started requests; otherwise REJECT and return std::nullopt.
    // The depth check and the queue push happen under ONE hpx::mutex acquisition,
    // so the depth admitted against is exactly the depth observed -- no read-then-act
    // (TOCTOU) window (stats(), which releases the lock, must NOT be the gate).
    // max_queue_depth counts ONLY queued items (queue_.size()): the in-service item
    // has already been popped and is not in queue_; a cancelled-but-not-yet-popped
    // item IS still in queue_, so it still counts until the worker pops/skips it. On
    // REJECT nothing is created -- no promise, no future, no token, no queue entry,
    // no notify -- so a rejected submit is side-effect-free on the lane. Mirrors
    // rayhpx::HpxLane::try_submit. MUST be called from an HPX thread (hpx::mutex).
    std::optional<hpx::future<RuntimeResult>> try_submit(
            OpTask task, int max_queue_depth, int checkpoint_count,
            std::shared_ptr<RuntimeCancelToken>* out_tok) {
        std::unique_lock<hpx::mutex> lk(mu_);
        if (static_cast<int>(queue_.size()) >= max_queue_depth) {
            return std::nullopt;  // lane full: reject before creating anything
        }
        auto prom = std::make_shared<hpx::promise<RuntimeResult>>();
        hpx::future<RuntimeResult> fut = prom->get_future();
        auto tok = std::make_shared<RuntimeCancelToken>();
        tok->arm(prom, actor_id_);
        queue_.push_back(
            Item{std::move(task), std::move(prom), tok, checkpoint_count});
        lk.unlock();
        cv_.notify_one();
        if (out_tok) *out_tok = std::move(tok);
        return fut;
    }

    // Cancel outstanding work so the lane can tear down promptly (see shutdown).
    // Collects the queued tokens AND the in-flight token UNDER mu_, then calls
    // cancel() OUTSIDE mu_ -- so the lane mutex is never held while the token mutex
    // is taken (no lane<->token lock nesting / deadlock). A queued cancel fulfills
    // its promise now (the worker then skips it on pop); an in-flight running
    // busy_sum is asked to stop at its next checkpoint. MUST be called from an HPX
    // thread (locks hpx::mutex). Non-cancellable / already-terminal tokens no-op.
    void cancel_pending() {
        std::vector<std::shared_ptr<RuntimeCancelToken>> toks;
        {
            std::lock_guard<hpx::mutex> lk(mu_);
            for (auto& item : queue_)
                if (item.tok) toks.push_back(item.tok);
            if (active_tok_) toks.push_back(active_tok_);
        }
        for (auto& t : toks) t->cancel();  // outside mu_
    }

    // Stop the lane and join its worker. MUST be called from an HPX thread (locks
    // hpx::mutex, joins an hpx::thread) and while HPX is still up (the worker drains
    // queued items, each dispatched via hpx::async). The worker fulfills every
    // queued promise before exiting (see run()), so no pending future breaks.
    // Idempotent: a second call sees a non-joinable worker and does nothing.
    void stop_and_join() {
        {
            std::lock_guard<hpx::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
    }

private:
    struct Item {
        OpTask task;
        std::shared_ptr<hpx::promise<RuntimeResult>> prom;
        std::shared_ptr<RuntimeCancelToken> tok;  // per-future cancel token
        int checkpoint_count = 1;                 // arms running-cancellability
    };

    // Long-lived consumer (one HPX thread). The idle wait on
    // hpx::condition_variable_any suspends this HPX thread cooperatively, so the
    // worker is freed while the lane is empty. On stop it DRAINS the remaining
    // queue (servicing/skipping and fulfilling each item) before returning, so no
    // pending RuntimeFuture is left with a broken promise.
    void run() {
        for (;;) {
            Item item;
            {
                std::unique_lock<hpx::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;  // drain-then-exit
                item = std::move(queue_.front());
                queue_.pop_front();
                // Observability: popped, now in the service slot. Set under the
                // same lock that guards queue_ so a stats() snapshot sees the
                // reduced depth and active=true coherently. active_tok_ is the
                // in-flight cancel token (cancel_pending may target it); both are
                // cleared after fulfill (active_tok_ under mu_).
                active_.store(true, std::memory_order_relaxed);
                active_tok_ = item.tok;
            }
            // Cancellation point: begin_service flips Queued -> Running and arms
            // running-cancellability only when checkpoint_count > 1. If a QUEUED
            // cancel already won it returns false: cancel() already fulfilled the
            // promise, so the lane SKIPS service entirely (no second set_value).
            if (item.tok && !item.tok->begin_service(item.checkpoint_count)) {
                clear_active();
                continue;  // queued-cancel skip
            }
            // The lane binds the StopCheckpoint: it polls the token AND performs
            // the cooperative hpx::this_thread::yield() (so a CPU-bound busy_sum
            // does not starve sibling lanes), keeping runtime_ops.hpp HPX-free.
            std::shared_ptr<RuntimeCancelToken> tok = item.tok;
            StopCheckpoint stop = [tok](bool next_is_final) -> bool {
                bool s = tok ? tok->stop_at_boundary(next_is_final) : false;
                hpx::this_thread::yield();
                return s;
            };
            // HPX-native dispatch: launch the op body on the executor and
            // cooperatively .get() it. Because this worker is an hpx::thread the
            // .get() SUSPENDS the worker (frees the OS worker) until the op
            // completes -- one operation retires before the next is popped (FIFO).
            RuntimeResult r;
            try {
                r = hpx::async(exec_, [task = item.task, stop]() {
                        return task(stop);
                    }).get();
            } catch (...) {
                // Defensive: the op task maps its own throws to a failed result, so
                // .get() should not throw. This covers only async/executor failure
                // (rare) -- still fulfill the promise so the future never breaks.
                r.actor_id = actor_id_;
                r.status = "failed";
                r.error = "runtime dispatch failed";
                r.has_value = false;
            }
            // Any non-cancelled outcome (completed OR failed) marks the token
            // TERMINAL, so a later cancel() returns false instead of claiming to
            // cancel an already-fulfilled future. A cancelled outcome leaves the
            // token as the op/cancel state machine already set it (Cancelled).
            if (item.tok && r.status != "cancelled") item.tok->mark_finished();
            item.prom->set_value(std::move(r));  // fulfilled exactly once
            clear_active();
        }
    }

    // Clear the in-flight markers after a fulfilled (or skipped) item. active_tok_
    // is cleared UNDER mu_ (cancel_pending reads it under mu_); active_ is an atomic
    // cleared without the lock for the stats() snapshot.
    void clear_active() {
        {
            std::lock_guard<hpx::mutex> lk(mu_);
            active_tok_.reset();
        }
        active_.store(false, std::memory_order_relaxed);
    }

    std::string actor_id_;
    hpx::thread worker_;
    hpx::mutex mu_;
    hpx::condition_variable_any cv_;
    std::deque<Item> queue_;
    bool stop_ = false;  // under mu_
    // Observability only (stats()): true while an item is in the service slot
    // (popped, not yet fulfilled). Atomic so stats() can read it without holding
    // mu_ across the whole dispatch; set under mu_ on pop, cleared after fulfill.
    // Mirrors HpxLane::active_.
    std::atomic<bool> active_{false};
    // The in-flight item's cancel token (null when idle). Written under mu_ on pop
    // and cleared under mu_ after fulfill, so cancel_pending() can read it under mu_
    // and ask a running op to stop at its next checkpoint (shutdown cancellation).
    std::shared_ptr<RuntimeCancelToken> active_tok_;
    // The executor op bodies are dispatched on. Default pool in Slice 1; the P4
    // placement seam (named control/work pools) is a later, illustrative axis.
    hpx::execution::parallel_executor exec_;
};

}  // namespace rayx_runtime

#endif  // RAYX_RUNTIME_LANE_HPP
