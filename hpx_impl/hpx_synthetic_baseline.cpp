// HPX intra-locality synthetic serving baseline.
//
// Mirrors the Ray actor baseline (ray_impl/ray_actor_baseline.py +
// bench/run_ray_baseline.py) but in a single HPX process. A single serialized
// "service lane" (one long-lived HPX thread that owns the synthetic backend)
// processes requests one at a time, matching Ray's single-actor serialization.
//
// Boundary measured: hpx-intra-locality. Everything is in-process: in-process
// futures, no serialization, no IPC, no process boundary. This measures HPX's
// lower-bound native C++ serving-control cost. It is NOT an identical-boundary
// comparison with Ray's ray-actor-process boundary.
//
// Timing: all timestamps come from one monotonic clock (steady_clock), so
// submit/start/end are directly comparable and queue_wait_ms is exact here
// (unlike Ray's cross-process approximation).

#include <hpx/hpx.hpp>
#include <hpx/hpx_main.hpp>

#include "service_lane.hpp"
#include "hpx_lane.hpp"  // opt-in --lane-impl hpx: HPX cooperative lane (native-only)

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Reuse the shared service-lane core (now_ns, Request, Result, ServiceLane,
// WORK_MODE_SLEEP) so this executable and the rayx Python extension run the
// identical lane mechanism.
using rayhpx::HpxLane;
using rayhpx::now_ns;
using rayhpx::Request;
using rayhpx::Result;
using rayhpx::ServiceLane;
using rayhpx::WORK_MODE_SLEEP;
using rayhpx::WORK_MODE_SPIN;

namespace {

constexpr char SCHEMA_VERSION[] = "1";
constexpr char BACKEND[] = "synthetic";
constexpr char BOUNDARY[] = "hpx-intra-locality";
// Opt-in HPX cooperative lane (--lane-impl hpx) records a DISTINCT boundary, so
// its rows never silently fold into the hpx-intra-locality corpus. Same JSONL
// schema (boundary is a free-string field; no version change); see
// experiments/16_hpx_lane_mechanism_probe/.
constexpr char BOUNDARY_HPXLANE[] = "hpx-intra-locality-hpxlane";

// Lane-impl selector values for --lane-impl.
constexpr char LANE_IMPL_STD[] = "std";    // default: rayhpx::ServiceLane (anchor)
constexpr char LANE_IMPL_HPX[] = "hpx";    // opt-in: rayhpx::HpxLane (cooperative)

// ---- Lane abstraction (non-intrusive) -----------------------------------
//
// Tiny runtime-polymorphic wrapper so the dispatch loops below call lane.submit
// /submit_bulk/actor_id regardless of the concrete lane impl, WITHOUT touching
// service_lane.hpp or duplicating the four retire loops. ServiceLane (the
// anchor) is wrapped unchanged -- its submit's optional CancelToken out-param
// defaults to nullptr here, exactly as the native driver always called it.
// HpxLane has no token param. The virtual is on submit only (not the lane's
// inner loop), so the per-request overhead is a negligible indirect call.
struct LaneIface {
    virtual ~LaneIface() = default;
    virtual hpx::future<Result> submit(Request req) = 0;
    virtual std::vector<hpx::future<Result>> submit_bulk(
        std::vector<Request> reqs) = 0;
    virtual const std::string& actor_id() const = 0;
};

template <class L>
struct LaneAdapter final : LaneIface {
    L lane_;
    explicit LaneAdapter(bool diag) : lane_(diag) {}
    hpx::future<Result> submit(Request req) override {
        return lane_.submit(std::move(req));
    }
    std::vector<hpx::future<Result>> submit_bulk(
        std::vector<Request> reqs) override {
        return lane_.submit_bulk(std::move(reqs));
    }
    const std::string& actor_id() const override { return lane_.actor_id(); }
};

// Client retire modes (mirror bench/run_ray_baseline.py).
constexpr char RETIRE_ONE_BY_ONE[] = "one_by_one";
constexpr char RETIRE_BATCH_WAIT[] = "batch_wait";
constexpr char RETIRE_SUBMIT_ALL[] = "submit_all_get_all";

// ---- CLI ----------------------------------------------------------------

// Service-time patterns (mirror bench/run_hpx_python_baseline.py).
constexpr char PATTERN_FIXED[] = "fixed";
constexpr char PATTERN_BIMODAL[] = "bimodal";

struct Options {
    double service_ms = 0.0;
    int concurrency = 8;
    int requests = 500;
    int warmup_requests = 0;
    int num_lanes = 1;
    int wait_batch = 8;
    int client_threads = 1;
    // Chunked synthetic service: service_ms is split into `chunks` equal active
    // steps with `chunk_delay_ms` parked gaps between them (chunks-1 gaps).
    // Defaults (1 / 0) reproduce the unchunked single-step path byte-for-byte.
    int chunks = 1;
    double chunk_delay_ms = 0.0;
    std::string work_mode = WORK_MODE_SLEEP;
    // Lane mechanism: "std" (default; rayhpx::ServiceLane, the stable anchor) or
    // "hpx" (opt-in; rayhpx::HpxLane cooperative lane). Default keeps every
    // existing invocation byte-identical. `boundary` is derived from this in
    // main() (std -> hpx-intra-locality, hpx -> hpx-intra-locality-hpxlane).
    std::string lane_impl = LANE_IMPL_STD;
    std::string boundary;  // set in main() from lane_impl
    std::string retire_mode = RETIRE_ONE_BY_ONE;
    std::string service_pattern = PATTERN_FIXED;
    double service_low = 1.0;
    double service_high = 20.0;
    double service_p_high = 0.1;
    long long seed = 0;
    std::string out;       // required
    std::string workload;  // auto-derived if empty
    bool diag = false;        // opt-in coordination diagnostic
    std::string diag_out;     // diag JSON path; defaults to <out>.diag.json
    int repeat = 0;           // metadata only: recorded in the diag summary
};

// Per-request requested service time in ms (a pure function of idx). IDENTICAL
// to bench/run_hpx_python_baseline.py::_service_for and run_ray_baseline.py, so
// the same (seed, request_index) yields the same service_ms_requested sequence
// across Ray, native HPX, and rayx. fixed -> service_ms; bimodal -> splitmix64
// draw in [0,1): if < p_high use high, else low. uint64_t wraparound == the
// Python "& (2^64-1)"; (double)z / 2^64 equals Python's z / 2**64 exactly
// (dividing by a power of two is exact), so the bucket comparison matches.
double service_for(const Options& opt, int idx) {
    if (opt.service_pattern == PATTERN_FIXED) return opt.service_ms;
    std::uint64_t z = static_cast<std::uint64_t>(opt.seed) +
                      static_cast<std::uint64_t>(idx) * 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z = z ^ (z >> 31);
    const double draw = static_cast<double>(z) / 18446744073709551616.0;  // 2^64
    return draw < opt.service_p_high ? opt.service_high : opt.service_low;
}

// Recover the request index from a "req-%06d" id (for recording the per-request
// requested service time without changing the shared Result struct).
int idx_from_request_id(const std::string& request_id) {
    auto dash = request_id.find_last_of('-');
    if (dash == std::string::npos) return -1;
    try {
        return std::stoi(request_id.substr(dash + 1));
    } catch (...) {
        return -1;
    }
}

std::string fmt_ms(double service_ms) {
    // Render whole numbers without a trailing ".0" for clean workload names.
    if (service_ms == static_cast<double>(static_cast<long long>(service_ms))) {
        return std::to_string(static_cast<long long>(service_ms));
    }
    std::ostringstream os;
    os << service_ms;
    return os.str();
}

std::string default_workload(const std::string& work_mode, double service_ms,
                             int concurrency) {
    std::string suffix = "c" + std::to_string(concurrency);
    if (service_ms == 0.0) {
        // service_ms == 0 is the no-op / null-dispatch case in both work modes.
        return "noop_" + suffix;
    }
    const std::string prefix =
        (work_mode == WORK_MODE_SPIN) ? "spin_" : "sleep_";
    return prefix + fmt_ms(service_ms) + "ms_" + suffix;
}

std::string gen_run_id() {
    std::time_t t = std::time(nullptr);
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &t);
#else
    gmtime_r(&t, &tm_utc);
#endif
    char stamp[32];
    std::strftime(stamp, sizeof(stamp), "%Y%m%dT%H%M%SZ", &tm_utc);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 15);
    const char* hex = "0123456789abcdef";
    std::string suffix;
    for (int i = 0; i < 6; ++i) suffix += hex[dist(gen)];

    return std::string("hpx-") + stamp + "-" + suffix;
}

bool parse_args(int argc, char** argv, Options& opt, std::string& err) {
    auto need_value = [&](int& i) -> const char* {
        if (i + 1 >= argc) return nullptr;
        return argv[++i];
    };
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        // HPX flags (--hpx:...) are consumed by the runtime before main; if any
        // leak through, ignore them defensively.
        if (a.rfind("--hpx:", 0) == 0) continue;

        if (a == "--service-ms") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --service-ms"; return false; }
            opt.service_ms = std::stod(v);
        } else if (a == "--concurrency") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --concurrency"; return false; }
            opt.concurrency = std::stoi(v);
        } else if (a == "--requests") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --requests"; return false; }
            opt.requests = std::stoi(v);
        } else if (a == "--warmup-requests") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --warmup-requests"; return false; }
            opt.warmup_requests = std::stoi(v);
        } else if (a == "--num-lanes") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --num-lanes"; return false; }
            opt.num_lanes = std::stoi(v);
        } else if (a == "--retire-mode") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --retire-mode"; return false; }
            opt.retire_mode = v;
        } else if (a == "--wait-batch") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --wait-batch"; return false; }
            opt.wait_batch = std::stoi(v);
        } else if (a == "--client-threads") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --client-threads"; return false; }
            opt.client_threads = std::stoi(v);
        } else if (a == "--work-mode") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --work-mode"; return false; }
            opt.work_mode = v;
        } else if (a == "--lane-impl") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --lane-impl"; return false; }
            opt.lane_impl = v;
        } else if (a == "--chunks") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --chunks"; return false; }
            opt.chunks = std::stoi(v);
        } else if (a == "--chunk-delay-ms") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --chunk-delay-ms"; return false; }
            opt.chunk_delay_ms = std::stod(v);
        } else if (a == "--service-pattern") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --service-pattern"; return false; }
            opt.service_pattern = v;
        } else if (a == "--service-low") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --service-low"; return false; }
            opt.service_low = std::stod(v);
        } else if (a == "--service-high") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --service-high"; return false; }
            opt.service_high = std::stod(v);
        } else if (a == "--service-p-high") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --service-p-high"; return false; }
            opt.service_p_high = std::stod(v);
        } else if (a == "--seed") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --seed"; return false; }
            opt.seed = std::stoll(v);
        } else if (a == "--workload") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --workload"; return false; }
            opt.workload = v;
        } else if (a == "--diag") {
            opt.diag = true;  // boolean flag, no value
        } else if (a == "--diag-out") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --diag-out"; return false; }
            opt.diag_out = v;
        } else if (a == "--repeat") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --repeat"; return false; }
            opt.repeat = std::stoi(v);
        } else if (a == "--out") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --out"; return false; }
            opt.out = v;
        } else {
            err = "unknown argument: " + a;
            return false;
        }
    }
    if (opt.out.empty()) { err = "--out is required"; return false; }
    if (opt.concurrency < 1) { err = "--concurrency must be >= 1"; return false; }
    if (opt.num_lanes < 1) { err = "--num-lanes must be >= 1"; return false; }
    if (opt.wait_batch < 1) { err = "--wait-batch must be >= 1"; return false; }
    if (opt.client_threads < 1) {
        err = "--client-threads must be >= 1";
        return false;
    }
    if (opt.work_mode != WORK_MODE_SLEEP && opt.work_mode != WORK_MODE_SPIN) {
        err = "unsupported --work-mode: " + opt.work_mode + " (sleep | spin)";
        return false;
    }
    if (opt.lane_impl != LANE_IMPL_STD && opt.lane_impl != LANE_IMPL_HPX) {
        err = "unsupported --lane-impl: " + opt.lane_impl + " (std | hpx)";
        return false;
    }
    if (opt.chunks < 1) { err = "--chunks must be >= 1"; return false; }
    if (opt.chunk_delay_ms < 0.0) {
        err = "--chunk-delay-ms must be >= 0";
        return false;
    }
    if (opt.retire_mode != RETIRE_ONE_BY_ONE &&
        opt.retire_mode != RETIRE_BATCH_WAIT &&
        opt.retire_mode != RETIRE_SUBMIT_ALL) {
        err = "unsupported --retire-mode: " + opt.retire_mode +
              " (one_by_one | batch_wait | submit_all_get_all)";
        return false;
    }
    if (opt.client_threads > 1 && opt.retire_mode != RETIRE_ONE_BY_ONE) {
        err = "--client-threads > 1 is only supported with --retire-mode "
              "one_by_one";
        return false;
    }
    if (opt.service_pattern != PATTERN_FIXED &&
        opt.service_pattern != PATTERN_BIMODAL) {
        err = "unsupported --service-pattern: " + opt.service_pattern +
              " (fixed | bimodal)";
        return false;
    }
    if (opt.service_pattern == PATTERN_BIMODAL) {
        if (opt.service_low < 0.0 || opt.service_high < 0.0) {
            err = "--service-low and --service-high must be >= 0";
            return false;
        }
        if (opt.service_p_high < 0.0 || opt.service_p_high > 1.0) {
            err = "--service-p-high must be in [0, 1]";
            return false;
        }
    }
    return true;
}

// ---- JSONL writer -------------------------------------------------------

// Minimal escaping for the string fields we actually emit (ids, error text).
std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out += c; break;
        }
    }
    return out;
}

std::string fmt_double(double v) {
    std::ostringstream os;
    os << std::setprecision(15) << v;
    return os.str();
}

std::string record_to_json(const Options& opt, const std::string& run_id,
                           const std::string& workload, const Result& r,
                           double service_ms_requested) {
    const double total_ms = (r.recv_ns - r.submit_ns) / 1e6;
    const double queue_wait_ms = (r.start_ns - r.submit_ns) / 1e6;
    const double service_ms_observed = (r.end_ns - r.start_ns) / 1e6;

    std::ostringstream o;
    o << '{'
      << "\"schema_version\": \"" << SCHEMA_VERSION << "\", "
      << "\"run_id\": \"" << json_escape(run_id) << "\", "
      << "\"backend\": \"" << BACKEND << "\", "
      << "\"boundary\": \"" << opt.boundary << "\", "
      << "\"workload\": \"" << json_escape(workload) << "\", "
      << "\"request_id\": \"" << json_escape(r.request_id) << "\", "
      << "\"actor_id\": \"" << json_escape(r.actor_id) << "\", "
      << "\"worker_id\": null, "
      << "\"status\": \"" << json_escape(r.status) << "\", "
      << "\"submit_ns\": " << r.submit_ns << ", "
      << "\"start_ns\": " << r.start_ns << ", "
      << "\"end_ns\": " << r.end_ns << ", "
      << "\"total_ms\": " << fmt_double(total_ms) << ", "
      << "\"queue_wait_ms\": " << fmt_double(queue_wait_ms) << ", "
      << "\"service_ms_observed\": " << fmt_double(service_ms_observed) << ", "
      << "\"service_ms_requested\": " << fmt_double(service_ms_requested) << ", "
      << "\"chunks\": " << opt.chunks << ", "
      << "\"chunk_delay_ms\": " << fmt_double(opt.chunk_delay_ms) << ", "
      << "\"work_mode\": \"" << json_escape(opt.work_mode) << "\", "
      << "\"retire_mode\": \"" << json_escape(opt.retire_mode) << "\", "
      << "\"error\": ";
    if (r.error.empty()) {
        o << "null";
    } else {
        o << '"' << json_escape(r.error) << '"';
    }
    o << '}';
    return o.str();
}

// ---- Driver: client retire modes (mirror bench/run_ray_baseline.py) ------

// Build and submit request `idx` to the next lane in round-robin order.
// `rr` is advanced so submission order maps to lanes[k % N]; with one lane this
// is lanes[0], reproducing the original single-lane behavior.
hpx::future<Result> submit_idx(
    std::vector<std::unique_ptr<LaneIface>>& lanes, const Options& opt,
    int idx, int& rr) {
    Request req;
    char buf[32];
    std::snprintf(buf, sizeof(buf), "req-%06d", idx);
    req.request_id = buf;
    req.service_ms_requested = service_for(opt, idx);
    req.work_mode = opt.work_mode;
    req.chunks = opt.chunks;
    req.chunk_delay_ms = opt.chunk_delay_ms;
    req.submit_ns = now_ns();
    LaneIface& lane = *lanes[rr % lanes.size()];
    ++rr;
    return lane.submit(std::move(req));
}

// one_by_one: hold --concurrency in flight, retire the oldest, submit one.
// Behavior-preserving with the pre-refactor driver.
std::vector<Result> dispatch_one_by_one(
    std::vector<std::unique_ptr<LaneIface>>& lanes, const Options& opt,
    int count, bool record) {
    std::vector<Result> results;
    if (record) results.reserve(count);

    std::deque<hpx::future<Result>> inflight;
    int next_i = 0;
    int rr = 0;

    while (next_i < count &&
           static_cast<int>(inflight.size()) < opt.concurrency) {
        inflight.push_back(submit_idx(lanes, opt, next_i++, rr));
    }
    while (!inflight.empty()) {
        Result r = inflight.front().get();
        inflight.pop_front();
        r.recv_ns = now_ns();
        if (record) results.push_back(std::move(r));
        if (next_i < count) inflight.push_back(submit_idx(lanes, opt, next_i++, rr));
    }
    return results;
}

// batch_wait: hold --concurrency in flight; block until at least one future is
// ready, then retire up to --wait-batch ready futures with a single batch
// recv_ns (matching Ray's ray.wait(num_returns=k) + one perf_counter after
// ray.get(done)), then refill. No busy-spin: hpx::wait_any blocks.
std::vector<Result> dispatch_batch_wait(
    std::vector<std::unique_ptr<LaneIface>>& lanes, const Options& opt,
    int count, bool record) {
    std::vector<Result> results;
    if (record) results.reserve(count);

    std::vector<hpx::future<Result>> inflight;
    int next_i = 0;
    int rr = 0;
    const int k = opt.wait_batch;

    while (next_i < count &&
           static_cast<int>(inflight.size()) < opt.concurrency) {
        inflight.push_back(submit_idx(lanes, opt, next_i++, rr));
    }
    while (!inflight.empty()) {
        // Block until at least one in-flight future is ready.
        hpx::wait_any(inflight);
        const std::int64_t recv_ns = now_ns();

        // Sweep: retire up to k ready futures; keep the rest for next round.
        std::vector<hpx::future<Result>> remaining;
        remaining.reserve(inflight.size());
        int retired = 0;
        for (auto& f : inflight) {
            if (retired < k && f.is_ready()) {
                Result r = f.get();
                r.recv_ns = recv_ns;
                if (record) results.push_back(std::move(r));
                ++retired;
            } else {
                remaining.push_back(std::move(f));
            }
        }
        inflight.swap(remaining);

        // Refill: submit one per retired request to keep the window full.
        for (int j = 0; j < retired && next_i < count; ++j) {
            inflight.push_back(submit_idx(lanes, opt, next_i++, rr));
        }
    }
    return results;
}

// submit_all_get_all: submit every request (no in-flight window), then wait for
// all with a single batch recv_ns. Latency percentiles here are queue/bulk
// shaped; throughput is the meaningful metric.
std::vector<Result> dispatch_submit_all(
    std::vector<std::unique_ptr<LaneIface>>& lanes, const Options& opt,
    int count, bool record) {
    std::vector<Result> results;
    if (record) results.reserve(count);

    std::vector<hpx::future<Result>> inflight;
    inflight.reserve(count);
    int rr = 0;
    for (int i = 0; i < count; ++i) {
        inflight.push_back(submit_idx(lanes, opt, i, rr));
    }
    hpx::wait_all(inflight);
    const std::int64_t recv_ns = now_ns();
    if (record) {
        for (auto& f : inflight) {
            Result r = f.get();
            r.recv_ns = recv_ns;
            results.push_back(std::move(r));
        }
    }
    return results;
}

// ---- Multi-client one_by_one (native C++ client threads, no GIL) ---------

// Atomic-round-robin variant of submit_idx for the multi-client path. Lane
// routing uses a single shared std::atomic counter -- the lock-free analog of
// the rayx Engine's one GIL-serialized round-robin counter -- so lanes stay
// balanced while multiple client threads submit concurrently. ServiceLane::
// submit() is already mutex-protected, so concurrent submits to the same lane
// are safe with no change to service_lane.hpp.
hpx::future<Result> submit_idx_atomic(
    std::vector<std::unique_ptr<LaneIface>>& lanes, const Options& opt,
    int idx, std::atomic<unsigned>& rr) {
    Request req;
    char buf[32];
    std::snprintf(buf, sizeof(buf), "req-%06d", idx);
    req.request_id = buf;
    req.service_ms_requested = service_for(opt, idx);
    req.work_mode = opt.work_mode;
    req.chunks = opt.chunks;
    req.chunk_delay_ms = opt.chunk_delay_ms;
    req.submit_ns = now_ns();
    const unsigned slot = rr.fetch_add(1, std::memory_order_relaxed);
    LaneIface& lane = *lanes[slot % lanes.size()];
    return lane.submit(std::move(req));
}

// One client thread's windowed one_by_one loop over a contiguous block of
// GLOBAL request indices [begin, end), holding `window` requests in flight.
// req_id and service_ms_requested derive from the GLOBAL index, so they stay
// correct regardless of how the index space was partitioned across threads.
// Mirrors _run_one_by_one in bench/run_hpx_python_baseline.py.
std::vector<Result> client_loop_one_by_one(
    std::vector<std::unique_ptr<LaneIface>>& lanes, const Options& opt,
    int begin, int end, int window, std::atomic<unsigned>& rr, bool record) {
    std::vector<Result> results;
    std::deque<hpx::future<Result>> inflight;
    int next_i = begin;

    while (next_i < end && static_cast<int>(inflight.size()) < window) {
        inflight.push_back(submit_idx_atomic(lanes, opt, next_i++, rr));
    }
    while (!inflight.empty()) {
        Result r = inflight.front().get();
        inflight.pop_front();
        r.recv_ns = now_ns();
        if (record) results.push_back(std::move(r));
        if (next_i < end) {
            inflight.push_back(submit_idx_atomic(lanes, opt, next_i++, rr));
        }
    }
    return results;
}

// Multi-client one_by_one: spawn opt.client_threads native std::threads sharing
// the same lanes. Total in-flight is held at --concurrency by splitting the
// window across threads (base + 1 for the first `rem` threads). Global indices
// [0,count) partition into contiguous disjoint blocks, so request IDs stay
// unique and service_for(idx) stays a pure function of the global index
// (identical service sequence to single-client; only submission interleaving and
// lane routing differ). Per-thread result vectors are merged after join; main()
// sorts by request_id before writing. Mirrors _run_one_by_one_threaded in
// bench/run_hpx_python_baseline.py.
std::vector<Result> dispatch_one_by_one_threaded(
    std::vector<std::unique_ptr<LaneIface>>& lanes, const Options& opt,
    int count, bool record) {
    const int t = std::min(opt.client_threads, std::max(1, count));
    const int base = opt.concurrency / t;
    const int rem = opt.concurrency % t;

    std::atomic<unsigned> rr{0};
    std::vector<std::vector<Result>> partials(t);
    std::vector<std::thread> threads;
    threads.reserve(t);
    for (int k = 0; k < t; ++k) {
        const int window = std::max(1, base + (k < rem ? 1 : 0));
        const int begin =
            static_cast<int>((static_cast<long long>(count) * k) / t);
        const int end =
            static_cast<int>((static_cast<long long>(count) * (k + 1)) / t);
        threads.emplace_back([&lanes, &opt, &rr, &partials, k, begin, end,
                              window, record] {
            partials[k] = client_loop_one_by_one(lanes, opt, begin, end, window,
                                                 rr, record);
        });
    }
    for (auto& th : threads) th.join();

    std::vector<Result> results;
    if (record) {
        std::size_t total = 0;
        for (const auto& p : partials) total += p.size();
        results.reserve(total);
        for (auto& p : partials) {
            for (auto& r : p) results.push_back(std::move(r));
        }
    }
    return results;
}

std::vector<Result> dispatch(
    std::vector<std::unique_ptr<LaneIface>>& lanes, const Options& opt,
    int count, bool record) {
    if (count <= 0) return {};
    // Multi-client is one_by_one only (enforced in parse_args).
    if (opt.client_threads > 1) {
        return dispatch_one_by_one_threaded(lanes, opt, count, record);
    }
    if (opt.retire_mode == RETIRE_BATCH_WAIT) {
        return dispatch_batch_wait(lanes, opt, count, record);
    }
    if (opt.retire_mode == RETIRE_SUBMIT_ALL) {
        return dispatch_submit_all(lanes, opt, count, record);
    }
    return dispatch_one_by_one(lanes, opt, count, record);
}

// ---- Diagnostic summary (opt-in via --diag) -----------------------------
//
// Computed in-process from the in-memory Result vector and written to a
// SEPARATE compact JSON file (<out>.diag.json or --diag-out). The per-request
// JSONL, its schema, and the analyzer are untouched. Only emitted when --diag
// is set (and only then are enqueue_ns/queue_depth populated).

// Nearest-rank-with-interpolation percentile (q in [0,100]); sorts in place.
// Matches bench/analyze_jsonl.py::_percentile so diag percentiles read the same.
double diag_percentile(std::vector<double>& v, double q) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    if (v.size() == 1) return v[0];
    const double rank = (q / 100.0) * (v.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(rank);
    const std::size_t hi = std::min(lo + 1, v.size() - 1);
    const double frac = rank - static_cast<double>(lo);
    return v[lo] + (v[hi] - v[lo]) * frac;
}

// {p50,p90,p99,mean} for one phase vector (ms). Takes by value: the percentile
// calls sort the copy, leaving the caller's data alone.
std::string diag_phase_json(std::vector<double> vals) {
    double sum = 0.0;
    for (double x : vals) sum += x;
    const double mean = vals.empty() ? 0.0 : sum / static_cast<double>(vals.size());
    const double p50 = diag_percentile(vals, 50);
    const double p90 = diag_percentile(vals, 90);
    const double p99 = diag_percentile(vals, 99);
    std::ostringstream o;
    o << "{\"p50\": " << fmt_double(p50) << ", \"p90\": " << fmt_double(p90)
      << ", \"p99\": " << fmt_double(p99) << ", \"mean\": " << fmt_double(mean)
      << "}";
    return o.str();
}

struct LaneAgg {
    std::string id;
    int processed = 0;  // all records the lane handled (success or failure)
    double busy_ms = 0.0;
};

void write_diag(const Options& opt, const std::string& run_id,
                const std::string& workload, const std::vector<Result>& records,
                double wall_s, int completed) {
    std::vector<double> push, pickup, service, completion, total, depth;
    const std::size_t n = records.size();
    push.reserve(n); pickup.reserve(n); service.reserve(n);
    completion.reserve(n); total.reserve(n); depth.reserve(n);

    // First-seen lane order preserved; linear scan is fine for <=16 lanes.
    std::vector<LaneAgg> lanes;
    double depth_max = 0.0;

    for (const Result& r : records) {
        push.push_back((r.enqueue_ns - r.submit_ns) / 1e6);
        pickup.push_back((r.start_ns - r.enqueue_ns) / 1e6);
        service.push_back((r.end_ns - r.start_ns) / 1e6);
        completion.push_back((r.recv_ns - r.end_ns) / 1e6);
        total.push_back((r.recv_ns - r.submit_ns) / 1e6);
        depth.push_back(static_cast<double>(r.queue_depth_at_enqueue));
        depth_max = std::max(depth_max,
                             static_cast<double>(r.queue_depth_at_enqueue));

        LaneAgg* found = nullptr;
        for (auto& la : lanes) {
            if (la.id == r.actor_id) { found = &la; break; }
        }
        if (!found) { lanes.push_back(LaneAgg{r.actor_id, 0, 0.0}); found = &lanes.back(); }
        found->processed += 1;
        found->busy_ms += (r.end_ns - r.start_ns) / 1e6;
    }

    const double throughput = wall_s > 0.0 ? completed / wall_s : 0.0;

    std::ostringstream o;
    o << "{\n";
    o << "  \"schema\": \"diag-1\",\n";
    o << "  \"run_id\": \"" << json_escape(run_id) << "\",\n";
    o << "  \"workload\": \"" << json_escape(workload) << "\",\n";
    o << "  \"boundary\": \"" << opt.boundary << "\",\n";
    o << "  \"config\": {"
      << "\"lanes\": " << opt.num_lanes
      << ", \"client_threads\": " << opt.client_threads
      << ", \"concurrency\": " << opt.concurrency
      << ", \"service_ms\": " << fmt_double(opt.service_ms)
      << ", \"work_mode\": \"" << json_escape(opt.work_mode) << "\""
      << ", \"service_pattern\": \"" << json_escape(opt.service_pattern) << "\""
      << ", \"retire_mode\": \"" << json_escape(opt.retire_mode) << "\""
      << ", \"repeat\": " << opt.repeat
      << ", \"num_requests\": " << n
      << ", \"completed\": " << completed << "},\n";
    o << "  \"throughput_req_s\": " << fmt_double(throughput) << ",\n";
    o << "  \"wall_s\": " << fmt_double(wall_s) << ",\n";
    o << "  \"phases_ms\": {\n";
    o << "    \"push\": " << diag_phase_json(push) << ",\n";
    o << "    \"pickup\": " << diag_phase_json(pickup) << ",\n";
    o << "    \"service\": " << diag_phase_json(service) << ",\n";
    o << "    \"completion\": " << diag_phase_json(completion) << ",\n";
    o << "    \"total\": " << diag_phase_json(total) << "\n";
    o << "  },\n";
    o << "  \"queue_depth_at_enqueue\": {"
      << "\"p50\": " << fmt_double(diag_percentile(depth, 50))
      << ", \"p90\": " << fmt_double(diag_percentile(depth, 90))
      << ", \"p99\": " << fmt_double(diag_percentile(depth, 99))
      << ", \"max\": " << fmt_double(depth_max) << "},\n";
    o << "  \"lanes\": [\n";
    for (std::size_t i = 0; i < lanes.size(); ++i) {
        const LaneAgg& la = lanes[i];
        const double util = wall_s > 0.0 ? la.busy_ms / (wall_s * 1000.0) : 0.0;
        o << "    {\"actor_id\": \"" << json_escape(la.id)
          << "\", \"processed\": " << la.processed
          << ", \"busy_ms\": " << fmt_double(la.busy_ms)
          << ", \"utilization\": " << fmt_double(util) << "}"
          << (i + 1 < lanes.size() ? ",\n" : "\n");
    }
    o << "  ]\n";
    o << "}\n";

    std::string path =
        opt.diag_out.empty() ? (opt.out + ".diag.json") : opt.diag_out;
    auto pos = path.find_last_of('/');
    if (pos != std::string::npos) {
        std::string d = path.substr(0, pos);
        if (!d.empty()) {
            std::error_code ec;
            std::filesystem::create_directories(d, ec);
        }
    }
    std::ofstream df(path, std::ios::trunc);
    if (!df) {
        std::fprintf(stderr,
                     "[hpx_synthetic_baseline] cannot open diag out: %s\n",
                     path.c_str());
        return;
    }
    df << o.str();
    df.close();
    std::printf("[hpx_synthetic_baseline] diag_out=%s\n", path.c_str());
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    std::string err;
    if (!parse_args(argc, argv, opt, err)) {
        std::fprintf(stderr, "[hpx_synthetic_baseline] error: %s\n",
                     err.c_str());
        return 2;
    }

    // --diag-out only takes effect with --diag. Warn (non-fatal) so a misused
    // flag is explicit rather than silently ignored; the run continues and no
    // diag file is written (the write_diag call below stays guarded by --diag).
    if (!opt.diag_out.empty() && !opt.diag) {
        std::fprintf(stderr,
                     "[hpx_synthetic_baseline] warning: --diag-out ignored "
                     "without --diag\n");
    }

    std::string workload;
    if (!opt.workload.empty()) {
        workload = opt.workload;
    } else if (opt.service_pattern == PATTERN_BIMODAL) {
        const int p = static_cast<int>(std::lround(opt.service_p_high * 100));
        workload = "bimodal_lo" + fmt_ms(opt.service_low) + "_hi" +
                   fmt_ms(opt.service_high) + "_p" + std::to_string(p) + "_c" +
                   std::to_string(opt.concurrency);
        if (opt.num_lanes > 1) {
            workload += "_l" + std::to_string(opt.num_lanes);
        }
    } else {
        workload = default_workload(opt.work_mode, opt.service_ms,
                                    opt.concurrency);
        // Encode lane count when scaling past one lane, so multi-lane runs
        // stay distinguishable. num_lanes==1 keeps the original names.
        if (opt.num_lanes > 1) {
            workload += "_l" + std::to_string(opt.num_lanes);
        }
    }
    // Opt-in HPX cooperative lane: tag auto-derived workload names with
    // "_hpxlane" so rows are self-documenting (user-supplied --workload is left
    // untouched). The boundary field carries the same distinction independently.
    const bool use_hpx_lane = (opt.lane_impl == LANE_IMPL_HPX);
    if (use_hpx_lane && opt.workload.empty()) {
        workload += "_hpxlane";
    }
    opt.boundary = use_hpx_lane ? BOUNDARY_HPXLANE : BOUNDARY;
    const std::string run_id = gen_run_id();

    // N independent serialized lanes, round-robined. lane_impl selects the
    // mechanism: ServiceLane (std::thread, blocking sleep -- the anchor) or
    // HpxLane (hpx::thread, cooperative sleep -- opt-in). Both are wrapped in a
    // LaneAdapter behind the LaneIface so the dispatch loops are impl-agnostic.
    std::vector<std::unique_ptr<LaneIface>> lanes;
    lanes.reserve(opt.num_lanes);
    for (int i = 0; i < opt.num_lanes; ++i) {
        if (use_hpx_lane) {
            lanes.push_back(std::make_unique<LaneAdapter<HpxLane>>(opt.diag));
        } else {
            lanes.push_back(std::make_unique<LaneAdapter<ServiceLane>>(opt.diag));
        }
    }

    // Warmup: same path, discarded, excluded from wall time.
    if (opt.warmup_requests > 0) {
        dispatch(lanes, opt, opt.warmup_requests, /*record=*/false);
    }

    const std::int64_t wall_start = now_ns();
    std::vector<Result> records =
        dispatch(lanes, opt, opt.requests, /*record=*/true);
    const std::int64_t wall_end = now_ns();

    // Sort by request_id (matches the Ray driver).
    std::sort(records.begin(), records.end(),
              [](const Result& a, const Result& b) {
                  return a.request_id < b.request_id;
              });

    // Ensure the output directory exists.
    {
        std::string dir = opt.out;
        auto pos = dir.find_last_of('/');
        if (pos != std::string::npos) {
            std::string d = dir.substr(0, pos);
            if (!d.empty()) {
                std::error_code ec;
                std::filesystem::create_directories(d, ec);
            }
        }
    }

    std::ofstream f(opt.out, std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "[hpx_synthetic_baseline] cannot open out: %s\n",
                     opt.out.c_str());
        return 1;
    }
    int completed = 0;
    for (const Result& r : records) {
        if (r.status == "completed") ++completed;
        // Recompute the per-request requested service time from the request
        // index (service_for is pure), so variable-service rows record their
        // own value without changing the shared Result struct.
        const int idx = idx_from_request_id(r.request_id);
        const double svc_req =
            (idx >= 0) ? service_for(opt, idx) : opt.service_ms;
        f << record_to_json(opt, run_id, workload, r, svc_req) << '\n';
    }
    f.close();

    std::string lane_ids;
    for (size_t i = 0; i < lanes.size(); ++i) {
        if (i) lane_ids += ",";
        lane_ids += lanes[i]->actor_id();
    }

    const double wall_s = (wall_end - wall_start) / 1e9;
    std::printf(
        "[hpx_synthetic_baseline] run_id=%s workload=%s boundary=%s "
        "retire_mode=%s warmup=%d num_lanes=%d client_threads=%d "
        "actor_ids=%s\n",
        run_id.c_str(), workload.c_str(), opt.boundary.c_str(),
        opt.retire_mode.c_str(),
        opt.warmup_requests, opt.num_lanes, opt.client_threads,
        lane_ids.c_str());
    std::printf(
        "[hpx_synthetic_baseline] requests=%zu completed=%d wall_s=%.3f "
        "out=%s\n",
        records.size(), completed, wall_s, opt.out.c_str());

    // Opt-in coordination diagnostic: separate compact JSON, normal output
    // above is unchanged.
    if (opt.diag) {
        write_diag(opt, run_id, workload, records, wall_s, completed);
    }
    return 0;
}
