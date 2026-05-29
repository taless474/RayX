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
    hpx::future<Result> submit(Request req) {
        auto prom = std::make_shared<hpx::promise<Result>>();
        hpx::future<Result> fut = prom->get_future();
        {
            std::lock_guard<std::mutex> lk(mu_);
            queue_.push_back(Item{std::move(req), std::move(prom)});
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
        return fut;
    }

private:
    struct Item {
        Request req;
        std::shared_ptr<hpx::promise<Result>> prom;
        std::int64_t enqueue_ns = 0;  // diag-only
        int queue_depth = 0;          // diag-only (includes this item)
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
            Result res = service(item.req);
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
    Result service(const Request& req) {
        Result r;
        r.request_id = req.request_id;
        r.actor_id = actor_id_;
        r.submit_ns = req.submit_ns;
        r.start_ns = now_ns();
        try {
            // service_ms == 0 is the degenerate no-op (null dispatch) in both
            // work modes.
            if (req.work_mode == WORK_MODE_SLEEP) {
                if (req.service_ms_requested > 0.0) {
                    auto dur = std::chrono::duration<double, std::milli>(
                        req.service_ms_requested);
                    std::this_thread::sleep_for(dur);
                }
            } else if (req.work_mode == WORK_MODE_SPIN) {
                if (req.service_ms_requested > 0.0) {
                    spin_for(req.service_ms_requested);
                }
            } else {
                throw std::runtime_error("unsupported work_mode: " +
                                         req.work_mode);
            }
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
