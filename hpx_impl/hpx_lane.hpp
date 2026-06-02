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
// Cancellation: this native-only prototype does NOT create CancelTokens. rayx is
// the only token creator in the project and is intentionally not wired to
// HpxLane, and the native driver never cancels -- so cancellation is "not
// applicable" here. The chunked service body below is otherwise the same shape as
// ServiceLane::service (active chunks + parked inter-chunk gaps), minus the
// token-boundary checks, with the two sleeps swapped to the cooperative timer.
//
// service_lane.hpp is included only to REUSE its shared types (Request, Result,
// now_ns, WORK_MODE_*, clock_type); it is not modified.

#ifndef RAYHPX_HPX_LANE_HPP
#define RAYHPX_HPX_LANE_HPP

#include "service_lane.hpp"  // Request, Result, now_ns, WORK_MODE_*, clock_type

#include <hpx/hpx.hpp>  // hpx::thread, hpx::mutex, hpx::condition_variable_any, hpx::this_thread::sleep_for

#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
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

    // Submit a request; returns a future resolved by the lane when serviced.
    // No cancellation token (see file header); single-arg signature is what the
    // native driver uses via lane.submit(std::move(req)).
    hpx::future<Result> submit(Request req) {
        auto prom = std::make_shared<hpx::promise<Result>>();
        hpx::future<Result> fut = prom->get_future();
        {
            std::lock_guard<hpx::mutex> lk(mu_);
            queue_.push_back(Item{std::move(req), std::move(prom)});
            if (diag_) {
                Item& it = queue_.back();
                it.enqueue_ns = now_ns();
                it.queue_depth = static_cast<int>(queue_.size());
            }
        }
        cv_.notify_one();
        return fut;
    }

    // Bulk enqueue: push a whole group under ONE lock + ONE notify, one future per
    // request in input order. Mirrors ServiceLane::submit_bulk (single consumer
    // drains the group after one notify; its wait predicate re-checks the queue).
    std::vector<hpx::future<Result>> submit_bulk(std::vector<Request> reqs) {
        const std::size_t n = reqs.size();
        std::vector<hpx::future<Result>> futs;
        futs.reserve(n);
        std::vector<Item> items;
        items.reserve(n);
        for (auto& req : reqs) {
            auto prom = std::make_shared<hpx::promise<Result>>();
            futs.push_back(prom->get_future());
            items.push_back(Item{std::move(req), std::move(prom)});
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
        std::int64_t enqueue_ns = 0;  // diag-only
        int queue_depth = 0;          // diag-only (includes this item)
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
            }
            Result res = service(item.req);
            // Carry diag-only enqueue info (captured at submit) into the Result.
            // No-ops when diag is off: both fields stay 0.
            res.enqueue_ns = item.enqueue_ns;
            res.queue_depth_at_enqueue = item.queue_depth;
            item.prom->set_value(std::move(res));
        }
    }

    // Service one request. Same chunked shape as ServiceLane::service (TOTAL active
    // service_ms split into `chunks` equal active steps, with chunks-1 parked
    // inter-chunk gaps), EXCEPT the two parked sleeps use the cooperative HPX timer
    // (hpx::this_thread::sleep_for) instead of std::this_thread::sleep_for. Spin is
    // identical (busy on-core via spin_for, no yield). No cancellation checks (no
    // token in this native-only prototype). chunks=1, chunk_delay_ms=0 reproduces
    // the single-step path.
    Result service(const Request& req) {
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
};

}  // namespace rayhpx

#endif  // RAYHPX_HPX_LANE_HPP
