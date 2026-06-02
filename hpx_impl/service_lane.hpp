// Shared HPX synthetic service-lane core.
//
// Extracted from hpx_synthetic_baseline.cpp so the native executable and the
// rayx Python extension use the SAME lane mechanism (one serialized lane per
// std::thread, blocking sleep, hpx::promise/future result channel). Keeping
// this identical across both is what makes the boundary comparison honest:
// only the driver/boundary differs, not the execution lane.
//
// Driver/JSONL concerns (schema version, backend/boundary labels, retire
// modes, CLI, serialization) intentionally stay in the consumers, not here.
//
// Timing: all timestamps come from one monotonic clock (steady_clock).

#ifndef RAYHPX_SERVICE_LANE_HPP
#define RAYHPX_SERVICE_LANE_HPP

#include <hpx/hpx.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

namespace rayhpx {

inline constexpr char WORK_MODE_SLEEP[] = "sleep";
inline constexpr char WORK_MODE_SPIN[] = "spin";

using clock_type = std::chrono::steady_clock;

inline std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               clock_type::now().time_since_epoch())
        .count();
}

struct Request {
    std::string request_id;
    double service_ms_requested = 0.0;
    std::string work_mode = WORK_MODE_SLEEP;
    std::int64_t submit_ns = 0;
    // Chunked synthetic service (v1 streaming). `chunks` splits the TOTAL active
    // service (service_ms_requested) into that many equal active steps of
    // service_ms_requested/chunks each; `chunk_delay_ms` is a PARKED gap (blocking
    // sleep, both work modes) inserted between consecutive chunks (chunks-1 gaps).
    // Defaults (chunks=1, chunk_delay_ms=0) reproduce the single-step service
    // byte-for-byte, preserving native-baseline parity.
    int chunks = 1;
    double chunk_delay_ms = 0.0;
};

struct Result {
    std::string request_id;
    std::string actor_id;
    std::int64_t submit_ns = 0;
    std::int64_t start_ns = 0;
    std::int64_t end_ns = 0;
    std::int64_t recv_ns = 0;  // when the client observes completion
    // Diagnostic-only (populated when the owning lane is constructed with
    // diag=true; left at the defaults otherwise). enqueue_ns is the moment the
    // request was pushed onto the lane queue, splitting the lumped queue_wait
    // into client-push vs lane-pickup. queue_depth_at_enqueue is the queue
    // length INCLUDING this request at that moment (1 == no backlog ahead).
    std::int64_t enqueue_ns = 0;
    int queue_depth_at_enqueue = 0;
    std::string status = "completed";
    std::string error;  // empty == null
    // Chunked service: how many active chunks actually ran. For a normally
    // completed request this equals the requested `chunks`; for a queued-cancel
    // it is 0 (nothing ran); for a running-cancel (early stop at a chunk
    // boundary) it is in [1, chunks-1]. Lane/C++-determined (the client cannot
    // know where an early stop landed), so unlike chunks/chunk_delay_ms this is
    // carried on the Result, not echoed from the Python side.
    int chunks_completed = 0;
};

// ---- Per-request cancellation token -------------------------------------
//
// Honest cancellation: QUEUED (skip before the lane starts) plus RUNNING at a
// CHUNK BOUNDARY (early stop between chunks). Running-cancel never interrupts an
// active chunk or an in-progress parked gap -- it only requests a stop the lane
// honors at the next boundary, if one remains. The token is fully self-contained
// (its own mutex, phase, and a copy of the request's promise; NO pointer back
// into the lane), so cancellation is safe even for a future whose owning
// lane/engine was already shut down and destroyed (a stale cancel just observes
// a terminal/non-cancellable phase and returns false), and the lane's hot path
// carries no cancellation state for token-less requests (the native baseline).
//
// State machine (all transitions under m_, single-arbiter):
//   Queued --cancel()------------> Cancelled        (cancel() fulfills now; chunks_completed=0)
//   Queued --begin_service()-----> Running
//   Running(cancellable) --cancel()--> StopRequested (cancel() True; LANE fulfills at boundary)
//   StopRequested --lane boundary--> Cancelled       (lane fulfills; 1<=chunks_completed<chunks)
//   Running --lane finishes all---> Completed         (lane fulfills; chunks_completed==chunks)
// cancellable_ is true only while Running with a not-yet-started chunk after the
// current one; the lane clears it (under m_) before committing to the FINAL
// active chunk, so a cancel that arrives too late returns false. There is no
// cancellation check inside sleep_for/spin_for: running work is never interrupted.
class CancelToken {
public:
    // Client side. Returns true iff THIS call settles a cancellation:
    //   * Queued -> Cancelled: skip all service; fulfilled here, now (no service
    //     time spent). The promise is set OUTSIDE the lock so set_value never
    //     runs under m_.
    //   * Running & cancellable -> StopRequested: the lane will stop at its next
    //     chunk boundary and fulfill a cancelled Result there (NOT fulfilled
    //     here). "True" means guaranteed-to-stop, not ready-now.
    // Returns false otherwise (already completed/cancelled/stop-requested, or
    // running its final chunk / a started chunks=1 request -- no boundary left).
    bool cancel() {
        bool fulfill_now = false;
        {
            std::lock_guard<std::mutex> lk(m_);
            if (phase_ == Phase::Queued) {
                phase_ = Phase::Cancelled;
                fulfill_now = true;
            } else if (phase_ == Phase::Running && cancellable_) {
                phase_ = Phase::StopRequested;  // lane fulfills at the boundary
                return true;
            } else {
                return false;
            }
        }
        if (fulfill_now) {
            Result r;
            r.actor_id = actor_id_;
            r.submit_ns = submit_ns_;
            const std::int64_t t = now_ns();
            r.start_ns = t;
            r.end_ns = t;  // no service time spent
            r.status = "cancelled";
            r.chunks_completed = 0;
            prom_->set_value(std::move(r));
        }
        return true;
    }

    // Lane side: try to begin service. Returns true if the lane may service the
    // request (Queued -> Running); false if it was already Cancelled (queued
    // cancel won -- cancel() fulfilled it, the lane must skip). `chunks` sets the
    // initial cancellability: a request is running-cancellable only if it has
    // more than one chunk (i.e. at least one boundary).
    bool begin_service(int chunks) {
        std::lock_guard<std::mutex> lk(m_);
        if (phase_ == Phase::Cancelled) return false;
        phase_ = Phase::Running;
        cancellable_ = (chunks > 1);
        return true;
    }

    // Lane side: the chunk-boundary arbiter, called under m_ BEFORE committing to
    // the next active chunk (for chunks after the first). Returns true if the
    // lane must STOP NOW (a stop was requested) -- the caller fulfills a
    // cancelled Result with the chunks done so far. Otherwise, if the chunk it is
    // about to start is the FINAL one, clears cancellable_ in the SAME critical
    // section (so a concurrent cancel deterministically loses), and returns false.
    bool stop_at_boundary(bool next_is_final) {
        std::lock_guard<std::mutex> lk(m_);
        if (phase_ == Phase::StopRequested) {
            phase_ = Phase::Cancelled;
            return true;
        }
        if (next_is_final) cancellable_ = false;
        return false;
    }

    // Lane side: mark a normally-completed request (so a late cancel sees a
    // terminal phase). cancellable_ is already false by this point.
    void mark_completed() {
        std::lock_guard<std::mutex> lk(m_);
        if (phase_ == Phase::Running) phase_ = Phase::Completed;
    }

    // Outcome-settled view: true once cancellation is guaranteed (queued cancel,
    // or a requested running stop), even before the lane has fulfilled the row.
    bool is_cancelled() {
        std::lock_guard<std::mutex> lk(m_);
        return phase_ == Phase::Cancelled || phase_ == Phase::StopRequested;
    }

private:
    friend class ServiceLane;
    enum class Phase { Queued, Running, StopRequested, Cancelled, Completed };
    std::mutex m_;
    Phase phase_ = Phase::Queued;
    bool cancellable_ = false;  // Running with a remaining chunk boundary
    std::shared_ptr<hpx::promise<Result>> prom_;
    std::string actor_id_;
    std::int64_t submit_ns_ = 0;
};

// ---- Single serialized service lane (the "actor") -----------------------
//
// One dedicated consumer thread owns the synthetic backend and processes
// exactly one request at a time, in submission order. For service_ms > 0 it
// uses a BLOCKING std::this_thread::sleep_for so the lane stays occupied and
// queueing builds up the same way Ray's single actor does. A cooperative
// hpx::this_thread::sleep_for would yield the worker and break single-lane
// serialization, so it is deliberately not used.
class ServiceLane {
public:
    // diag=false (default) keeps the lane on its original hot path: no extra
    // timestamp read in submit(). diag=true captures enqueue_ns/queue depth.
    explicit ServiceLane(bool diag = false) : diag_(diag) {
        actor_id_ = make_actor_id();
        worker_ = std::thread([this] { run(); });
    }

    ~ServiceLane() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
    }

    const std::string& actor_id() const { return actor_id_; }

    // Submit a request; returns a future resolved by the lane when serviced.
    //
    // out_tok is opt-in cancellation support: when non-null, a CancelToken is
    // allocated, attached to the queued item, and returned through it so the
    // caller can later cancel-if-still-queued. When null (the native baseline
    // path) NO token is allocated and the lane's service path is byte-identical
    // to before -- the cancel machinery costs nothing for requests that opt out.
    hpx::future<Result> submit(Request req,
                               std::shared_ptr<CancelToken>* out_tok = nullptr) {
        auto prom = std::make_shared<hpx::promise<Result>>();
        hpx::future<Result> fut = prom->get_future();
        std::shared_ptr<CancelToken> tok;
        if (out_tok) {
            tok = std::make_shared<CancelToken>();
            tok->prom_ = prom;            // token fulfills this promise on cancel
            tok->actor_id_ = actor_id_;
            tok->submit_ns_ = req.submit_ns;
        }
        {
            std::lock_guard<std::mutex> lk(mu_);
            queue_.push_back(Item{std::move(req), std::move(prom), tok});
            // Diagnostic capture, under the lock the lane already holds, right
            // after the push and before notify. Guarded so the non-diag hot
            // path takes no extra clock read.
            if (diag_) {
                Item& it = queue_.back();
                it.enqueue_ns = now_ns();
                it.queue_depth = static_cast<int>(queue_.size());
            }
        }
        cv_.notify_one();
        if (out_tok) *out_tok = std::move(tok);
        return fut;
    }

    // Bulk enqueue: push a whole group of requests under ONE lock acquisition and
    // ONE notify, instead of one lock+notify per request (see submit()). Returns
    // one future per request, in the SAME order as `reqs`. This is an ADDITIVE
    // fast path for the rayx batch submit (one Python->C++ crossing); the native
    // baseline does not use it and submit() above is unchanged, preserving
    // single-submit / native parity byte-for-byte.
    //
    // No cancellation: batch-submitted requests are non-cancelable (no
    // CancelToken), matching the existing batch contract -- so the per-item `tok`
    // is null, exactly as submit() leaves it when out_tok == nullptr. Promises and
    // futures are created OUTSIDE the lock to keep the critical section to just the
    // queue push_backs; the single consumer thread drains the whole group after one
    // notify (its wait predicate re-checks !queue_.empty() each loop, so one notify
    // is sufficient regardless of group size). Diagnostic capture (when diag_) is
    // per item, identical in meaning to submit()'s.
    std::vector<hpx::future<Result>> submit_bulk(std::vector<Request> reqs) {
        const std::size_t n = reqs.size();
        std::vector<hpx::future<Result>> futs;
        futs.reserve(n);
        std::vector<Item> items;
        items.reserve(n);
        for (auto& req : reqs) {
            auto prom = std::make_shared<hpx::promise<Result>>();
            futs.push_back(prom->get_future());
            items.push_back(Item{std::move(req), std::move(prom), nullptr});
        }
        {
            std::lock_guard<std::mutex> lk(mu_);
            for (auto& it : items) {
                queue_.push_back(std::move(it));
                if (diag_) {
                    Item& back = queue_.back();
                    back.enqueue_ns = now_ns();
                    back.queue_depth = static_cast<int>(queue_.size());
                }
            }
        }
        cv_.notify_one();
        return futs;
    }

private:
    struct Item {
        Request req;
        std::shared_ptr<hpx::promise<Result>> prom;
        std::shared_ptr<CancelToken> tok;  // null on the non-cancelable path
        std::int64_t enqueue_ns = 0;       // diag-only
        int queue_depth = 0;               // diag-only (includes this item)
    };

    static std::string make_actor_id() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(0, 15);
        const char* hex = "0123456789abcdef";
        std::string id = "act-hpx-";
        for (int i = 0; i < 8; ++i) id += hex[dist(gen)];
        return id;
    }

    void run() {
        for (;;) {
            Item item;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                item = std::move(queue_.front());
                queue_.pop_front();
            }
            // Cancellation point: decide start-vs-cancel OUTSIDE the lane mutex,
            // under the token's own mutex. begin_service(chunks) flips Queued ->
            // Running and arms running-cancellability only when chunks > 1 (i.e.
            // a chunk boundary exists). If a QUEUED cancel already won (or wins
            // this race), it returns false: cancel() already fulfilled the promise
            // with a cancelled Result, so the lane skips service entirely -- no
            // service time spent, FIFO order for the surviving work unchanged.
            // Items without a token (native path) always service.
            if (item.tok && !item.tok->begin_service(item.req.chunks)) {
                continue;  // queued-cancel won before start; promise fulfilled
            }
            // service() carries the token so it can honor a RUNNING stop at a
            // chunk boundary (and record chunks_completed). tok is null on the
            // native path -- service() then runs with no cancellation checks.
            Result res = service(item.req, item.tok.get());
            // Carry the diag-only enqueue info (captured at submit) into the
            // Result. No-ops when diag is off: both fields stay at 0.
            res.enqueue_ns = item.enqueue_ns;
            res.queue_depth_at_enqueue = item.queue_depth;
            item.prom->set_value(std::move(res));
        }
    }

    // Service one request on the lane (no cooperative yield in either mode, so
    // the lane stays occupied and queueing builds up like Ray's single actor).
    // work_mode "sleep" parks the lane (blocking sleep_for); "spin" keeps it
    // busy on-core until the target wall-clock duration elapses.
    //
    // Chunked service (v1 streaming): the TOTAL active service
    // (service_ms_requested) is split into `chunks` equal active steps of
    // service_ms_requested/chunks, each consumed via the request's work_mode
    // (sleep parks, spin stays on-core -- exactly as for a single step). Between
    // consecutive chunks the lane parks for chunk_delay_ms with a BLOCKING sleep
    // in BOTH modes: the inter-chunk gap models client-visible cadence/spacing
    // (waiting), not active work. There is no cooperative yield in either phase,
    // so the lane stays occupied for the request's WHOLE chunked lifecycle and
    // FIFO serialization is preserved. start_ns/end_ns bracket the entire
    // lifecycle, so service_ms_observed = active service + inter-chunk parked
    // gaps (lane-occupancy time), NOT active-only when chunk_delay_ms>0.
    // chunks=1, chunk_delay_ms=0 reproduces the single-step path byte-for-byte
    // (native-baseline parity).
    //
    // Running-cancellation (only when a token is present) is honored ONLY at a
    // chunk boundary -- before committing to the next active chunk, never inside
    // an active chunk's sleep_for/spin_for and never interrupting an in-progress
    // parked gap. tok==nullptr (native path / batch / token-less) means no checks
    // at all and a guaranteed full run. On a normal finish the request is marked
    // completed and chunks_completed == chunks; on an honored boundary stop the
    // lane returns a cancelled Result with 1 <= chunks_completed < chunks (run()
    // fulfills the promise either way -- cancel() does NOT fulfill for a running
    // stop, so the promise is still set exactly once).
    Result service(const Request& req, CancelToken* tok) {
        Result r;
        r.request_id = req.request_id;
        r.actor_id = actor_id_;
        r.submit_ns = req.submit_ns;
        r.start_ns = now_ns();
        try {
            const bool is_sleep = (req.work_mode == WORK_MODE_SLEEP);
            const bool is_spin = (req.work_mode == WORK_MODE_SPIN);
            if (!is_sleep && !is_spin) {
                throw std::runtime_error("unsupported work_mode: " +
                                         req.work_mode);
            }
            const int n = req.chunks < 1 ? 1 : req.chunks;  // defensive backstop
            const double per_chunk_ms = req.service_ms_requested / n;
            for (int c = 0; c < n; ++c) {
                // Chunk boundary (before every chunk after the first). Honor a
                // requested running-stop here: c active chunks already ran, so
                // 1 <= c <= n-1 (a strictly-partial run). Otherwise, when the
                // chunk we are about to start is the FINAL one, stop_at_boundary
                // clears cancellability in the same critical section so a late
                // cancel deterministically loses. There is no check inside the
                // active work below or the parked gap -- neither is interrupted.
                if (c > 0 && tok &&
                    tok->stop_at_boundary(/*next_is_final=*/c == n - 1)) {
                    r.status = "cancelled";
                    r.chunks_completed = c;
                    r.end_ns = now_ns();
                    return r;  // run() fulfills the promise with this Result
                }
                // Active service for this chunk (service_ms==0 -> no-op, as before).
                if (per_chunk_ms > 0.0) {
                    if (is_sleep) {
                        std::this_thread::sleep_for(
                            std::chrono::duration<double, std::milli>(per_chunk_ms));
                    } else {
                        spin_for(per_chunk_ms);
                    }
                }
                // Parked inter-chunk gap (both modes), only BETWEEN chunks.
                if (c + 1 < n && req.chunk_delay_ms > 0.0) {
                    std::this_thread::sleep_for(
                        std::chrono::duration<double, std::milli>(
                            req.chunk_delay_ms));
                }
            }
            // All chunks ran to completion: mark the token terminal (so a late
            // cancel observes a non-cancellable phase) and record the full count.
            if (tok) tok->mark_completed();
            r.chunks_completed = n;
        } catch (const std::exception& exc) {
            r.status = "failed";
            r.error = exc.what();
        }
        r.end_ns = now_ns();
        return r;
    }

    // Busy-spin (no yield) until `service_ms` of wall-clock time elapses on the
    // SAME monotonic clock the metrics use, so service_ms_observed tracks the
    // request without the ~25% sleep/wakeup overshoot that sleep_for carries.
    // CPU-bound: the lane stays pinned on-core. The per-call volatile sink (read
    // and written every iteration) plus the clock read in the loop condition
    // prevent the compiler from eliding the loop; the sink is a local, so there
    // is no cross-lane sharing.
    static void spin_for(double service_ms) {
        const auto deadline =
            clock_type::now() +
            std::chrono::duration_cast<clock_type::duration>(
                std::chrono::duration<double, std::milli>(service_ms));
        volatile std::uint64_t sink = 0;
        while (clock_type::now() < deadline) {
            sink = sink + 1;
        }
        (void)sink;
    }

    std::string actor_id_;
    std::thread worker_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<Item> queue_;
    bool stop_ = false;
    bool diag_ = false;
};

}  // namespace rayhpx

#endif  // RAYHPX_SERVICE_LANE_HPP
