// Single serialized HPX-thread service lane ("HPX cooperative lane").
//
// An ALTERNATIVE, OPT-IN lane mechanism for the NATIVE baseline only. It keeps
// the same actor-like single-consumer FIFO contract as rayhpx::ServiceLane
// (one dedicated consumer, one request at a time, in submission order), but is
// built on HPX-native primitives so a parked sleep YIELDS the HPX worker instead
// of blocking an OS thread:
//
//   * the consumer is a long-lived hpx::thread (an HPX thread) -- required so the
//     cooperative hpx::this_thread::sleep_for actually yields cooperatively;
//   * the FIFO queue uses hpx::mutex + hpx::condition_variable_any, so the idle
//     "wait for work" suspends the HPX thread (frees the worker) instead of
//     blocking it -- a std::condition_variable here would pin/starve an HPX
//     worker (see the risk note in experiments/16);
//   * sleep-mode active service AND the parked inter-chunk gap use
//     hpx::this_thread::sleep_for (cooperative). Spin is byte-identical to
//     ServiceLane (busy on-core, no yield -- cooperative timing does not apply to
//     a busy-wait), so it stays a CPU-bound axis and experiment 16 does not use it.
//
// This is NOT a replacement for ServiceLane (which remains the stable
// Ray-actor-like anchor) and NOT a general HPX-scheduler result: it is ONE
// serialized lane whose timer/suspension primitives are HPX-native, used to probe
// what changes under cooperative parking while FIFO order is preserved. It reuses
// rayhpx::Request and rayhpx::Result UNCHANGED (defined in service_lane.hpp).
//
// Two roles, ONE lane mechanism:
//   * Native baseline (unchanged): the bare single-arg submit(Request) and
//     submit_bulk push token-less work; the native driver never cancels, so the
//     hot path carries no cancellation state. These paths are byte-identical to
//     before.
//   * rayx backend (opt-in, behind Engine(lane_impl="hpx")): the SAME contract
//     surface ServiceLane exposes to the rayx Engine -- a cancel-token-aware
//     submit(Request, out_tok), a bounded-admission try_submit(...), queued and
//     chunk-boundary running cancellation, and a non-consuming stats() snapshot.
//     This reuses the ONE lane-agnostic rayhpx::CancelToken from service_lane.hpp
//     (HpxLane is a friend, see service_lane.hpp) instead of duplicating it, so
//     cancellation semantics are identical to ServiceLane's.
//
// THREADING CONTRACT (important): the queue is guarded by hpx::mutex +
// hpx::condition_variable_any, which MUST be locked only from an HPX thread
// (hpx::mutex suspends the calling HPX thread on contention; locking it from a
// non-HPX OS thread is invalid). The native driver already runs on HPX threads
// (wrap_main). The rayx Engine runs on the external Python thread, so it hops
// every submit/try_submit/submit_bulk/stats call through hpx::run_as_hpx_thread
// (see python/src/rayx/_rayx.cpp RayxLaneAdapter<HpxLane>). Cancellation is the
// exception: CancelToken uses its own std::mutex + an hpx::promise, so cancel()
// is safe directly from the external thread, with NO hop -- exactly as for
// ServiceLane under rayx.
//
// The chunked service body below is the same shape as ServiceLane::service
// (active chunks + parked inter-chunk gaps + chunk-boundary cancel checks), with
// the two parked sleeps swapped to the cooperative HPX timer.
//
// service_lane.hpp is included only to REUSE its shared types (Request, Result,
// now_ns, WORK_MODE_*, clock_type, CancelToken); it is not modified.

#ifndef RAYHPX_HPX_LANE_HPP
#define RAYHPX_HPX_LANE_HPP

#include "service_lane.hpp"  // Request, Result, now_ns, WORK_MODE_*, clock_type

#include <hpx/hpx.hpp>  // hpx::thread, hpx::mutex, hpx::condition_variable_any, hpx::this_thread::sleep_for

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace rayhpx {

class HpxLane {
public:
    // diag=false (default) keeps the lane on its hot path: no extra timestamp read
    // in submit(). diag=true captures enqueue_ns/queue depth, exactly like
    // ServiceLane, so the native binary's --diag works for either lane impl.
    explicit HpxLane(bool diag = false) : diag_(diag) {
        actor_id_ = make_actor_id();
        worker_ = hpx::thread([this] { run(); });
    }

    ~HpxLane() {
        {
            std::lock_guard<hpx::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
    }

    const std::string& actor_id() const { return actor_id_; }

    // Observability snapshot (debugging only), mirroring ServiceLane::LaneStat /
    // stats(): a point-in-time view of one lane (queued-but-not-started depth and
    // whether a request is currently being serviced). Briefly takes the lane
    // hpx::mutex for the queue size; `active` is an atomic flag. SNAPSHOT only,
    // NON-consuming (touches no future), NOT scheduler state / placement control /
    // JSONL schema. MUST be called from an HPX thread (the rayx Engine hops here).
    struct LaneStat {
        std::string actor_id;
        int queue_depth = 0;  // queued, not yet popped/started
        bool active = false;  // a request has been popped and is being serviced
    };
    LaneStat stats() {
        LaneStat s;
        s.actor_id = actor_id_;
        std::lock_guard<hpx::mutex> lk(mu_);
        s.queue_depth = static_cast<int>(queue_.size());
        s.active = active_.load(std::memory_order_relaxed);
        return s;
    }

    // Submit a request; returns a future resolved by the lane when serviced.
    //
    // out_tok mirrors ServiceLane::submit: when non-null (the rayx single-request
    // path) a CancelToken is allocated, attached to the queued item, and returned
    // through it so the caller can cancel-if-still-queued or stop-at-boundary.
    // When null (the native driver's lane.submit(std::move(req)) and the
    // non-cancelable batch path) NO token is allocated and the service path is
    // byte-identical to before. MUST be called from an HPX thread (hpx::mutex);
    // the rayx Engine hops here via run_as_hpx_thread (see file header).
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
            std::lock_guard<hpx::mutex> lk(mu_);
            queue_.push_back(Item{std::move(req), std::move(prom), tok});
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

    // Bounded-admission enqueue (rayx Engine capped path only): like submit(),
    // but ADMIT only if this lane currently holds fewer than max_queue_depth
    // queued-but-not-started requests; otherwise REJECT and return std::nullopt.
    // The depth check and the queue push happen under ONE hpx::mutex acquisition
    // (no TOCTOU window); on REJECT nothing is created (no promise/future/token/
    // entry/notify). Mirrors ServiceLane::try_submit exactly, on the HPX-native
    // queue. MUST be called from an HPX thread (the rayx Engine hops here).
    std::optional<hpx::future<Result>> try_submit(
            Request req, int max_queue_depth,
            std::shared_ptr<CancelToken>* out_tok = nullptr) {
        std::unique_lock<hpx::mutex> lk(mu_);
        if (static_cast<int>(queue_.size()) >= max_queue_depth) {
            return std::nullopt;  // lane full: reject before creating anything
        }
        auto prom = std::make_shared<hpx::promise<Result>>();
        hpx::future<Result> fut = prom->get_future();
        std::shared_ptr<CancelToken> tok;
        if (out_tok) {
            tok = std::make_shared<CancelToken>();
            tok->prom_ = prom;
            tok->actor_id_ = actor_id_;
            tok->submit_ns_ = req.submit_ns;
        }
        queue_.push_back(Item{std::move(req), std::move(prom), tok});
        if (diag_) {
            Item& it = queue_.back();
            it.enqueue_ns = now_ns();
            it.queue_depth = static_cast<int>(queue_.size());
        }
        lk.unlock();
        cv_.notify_one();
        if (out_tok) *out_tok = std::move(tok);
        return fut;
    }

    // Bulk enqueue: push a whole group under ONE lock + ONE notify, one future per
    // request in input order. Mirrors ServiceLane::submit_bulk (single consumer
    // drains the group after one notify; its wait predicate re-checks the queue).
    // Batch-submitted requests are non-cancelable (no CancelToken), so the
    // per-item tok is null, exactly as submit() leaves it when out_tok == nullptr.
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
            std::lock_guard<hpx::mutex> lk(mu_);
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

    // Distinct prefix from ServiceLane's "act-hpx-" so a row's actor_id makes the
    // lane mechanism self-evident: HPX cooperative lane == "act-hpxl-".
    static std::string make_actor_id() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(0, 15);
        const char* hex = "0123456789abcdef";
        std::string id = "act-hpxl-";
        for (int i = 0; i < 8; ++i) id += hex[dist(gen)];
        return id;
    }

    // Long-lived consumer (one HPX thread). The idle wait on
    // hpx::condition_variable_any suspends this HPX thread cooperatively, so the
    // worker is freed while the lane is empty -- it does NOT block an OS thread.
    void run() {
        for (;;) {
            Item item;
            {
                std::unique_lock<hpx::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                item = std::move(queue_.front());
                queue_.pop_front();
                // Observability (stats()): popped, now in the service lifecycle.
                // Set under the same lock that guards queue_ so a snapshot sees
                // the reduced depth and active=true coherently; cleared below.
                active_.store(true, std::memory_order_relaxed);
            }
            // Cancellation point (rayx token path only): begin_service flips
            // Queued -> Running and arms running-cancellability only when
            // chunks > 1. If a QUEUED cancel already won it returns false:
            // cancel() already fulfilled the promise, so the lane skips service
            // entirely. Token-less items (native path) always service.
            if (item.tok && !item.tok->begin_service(item.req.chunks)) {
                active_.store(false, std::memory_order_relaxed);  // queued-cancel skip
                continue;
            }
            // service() carries the token so it can honor a RUNNING stop at a
            // chunk boundary (and record chunks_completed). tok is null on the
            // native path -- service() then runs with no cancellation checks.
            Result res = service(item.req, item.tok.get());
            // Carry diag-only enqueue info (captured at submit) into the Result.
            // No-ops when diag is off: both fields stay 0.
            res.enqueue_ns = item.enqueue_ns;
            res.queue_depth_at_enqueue = item.queue_depth;
            item.prom->set_value(std::move(res));
            active_.store(false, std::memory_order_relaxed);  // lifecycle done
        }
    }

    // Service one request. Same chunked shape as ServiceLane::service (TOTAL active
    // service_ms split into `chunks` equal active steps, with chunks-1 parked
    // inter-chunk gaps + chunk-boundary cancel checks), EXCEPT the two parked
    // sleeps use the cooperative HPX timer (hpx::this_thread::sleep_for) instead of
    // std::this_thread::sleep_for. Spin is identical (busy on-core via spin_for, no
    // yield). Running-cancellation (only when a token is present) is honored ONLY
    // at a chunk boundary -- before committing to the next active chunk, never
    // inside an active chunk or an in-progress parked gap. tok==nullptr (native
    // path / batch) means no checks and a guaranteed full run. chunks=1,
    // chunk_delay_ms=0 reproduces the single-step path.
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
                // cancel deterministically loses. No check inside the active work
                // below or the parked gap -- neither is interrupted.
                if (c > 0 && tok &&
                    tok->stop_at_boundary(/*next_is_final=*/c == n - 1)) {
                    r.status = "cancelled";
                    r.chunks_completed = c;
                    r.end_ns = now_ns();
                    return r;  // run() fulfills the promise with this Result
                }
                // Active service for this chunk (service_ms==0 -> no-op).
                if (per_chunk_ms > 0.0) {
                    if (is_sleep) {
                        cooperative_sleep_ms(per_chunk_ms);
                    } else {
                        spin_for(per_chunk_ms);
                    }
                }
                // Parked inter-chunk gap (both modes), only BETWEEN chunks.
                if (c + 1 < n && req.chunk_delay_ms > 0.0) {
                    cooperative_sleep_ms(req.chunk_delay_ms);
                }
            }
            // All chunks ran: mark the token terminal (so a late cancel sees a
            // non-cancellable phase) and record the full count.
            if (tok) tok->mark_completed();
            r.chunks_completed = n;
        } catch (const std::exception& exc) {
            r.status = "failed";
            r.error = exc.what();
        }
        r.end_ns = now_ns();
        return r;
    }

    // Cooperative parked wait: yields the HPX worker for ~ms milliseconds. This is
    // the one deliberate primitive change from ServiceLane's blocking sleep_for.
    static void cooperative_sleep_ms(double ms) {
        hpx::this_thread::sleep_for(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::duration<double, std::milli>(ms)));
    }

    // Busy-spin (no yield) until `service_ms` of wall-clock elapses on the SAME
    // monotonic clock the metrics use -- byte-identical to ServiceLane::spin_for.
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
    hpx::thread worker_;
    hpx::mutex mu_;
    hpx::condition_variable_any cv_;
    std::deque<Item> queue_;
    bool stop_ = false;
    bool diag_ = false;
    // Observability only (stats()): true while the lane is inside a request's
    // service lifecycle (popped, not yet fulfilled). Atomic so stats() can read
    // it without holding the lane mutex over the whole service call; set under
    // the lane mutex on pop, cleared after fulfillment / queued-cancel skip. Off
    // the inner sleep/spin loop -- it is per-request, not per-chunk. Mirrors
    // ServiceLane::active_.
    std::atomic<bool> active_{false};
};

}  // namespace rayhpx

#endif  // RAYHPX_HPX_LANE_HPP
