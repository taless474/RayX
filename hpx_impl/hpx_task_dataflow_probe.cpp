// HPX task/dataflow mechanism probe (experiments/20) -- standalone, native-only.
//
// A CONTRACT-RELAXING mechanism probe, deliberately separate from the
// serving-control benchmark corpus. It does NOT use the JSONL benchmark schema,
// it is NOT a retire-mode/lane-sweep driver, and its numbers are NOT comparable
// to benchmark 06/10 or any other corpus entry. It exists to answer ONE question:
//
//   When synthetic work is served by HPX task/future/dataflow mechanisms instead
//   of a serialized lane, which serialized-lane CONTRACTS are preserved, relaxed,
//   or not applicable?
//
// Lineage: experiment 15 measured isolated HPX primitives (sleep overshoot, the
// hpx::async no-op dispatch floor); experiment 16 measured a contract-PRESERVING
// cooperative FIFO lane (HpxLane); this probe measures contract-RELAXING task /
// dataflow mechanisms on the SAME opt-in, separately-reported axis.
//
// Mechanisms compared (identical synthetic sleep work; only DISPATCH differs):
//   * service_lane  -- rayhpx::ServiceLane (std::thread, blocking sleep). The
//                      Ray-actor-like ANCHOR; all lane contracts hold. Unmodified.
//   * hpx_lane      -- rayhpx::HpxLane (hpx::thread, cooperative sleep). A
//                      contract-preserving HPX-thread FIFO lane. Unmodified.
//   * hpx_async     -- one hpx::async task per request. Scheduler-placed POOL: no
//                      stable per-lane identity, no FIFO guarantee, no lane queue.
//   * hpx_dataflow  -- a tiny per-request dependency (prepare -> service) via
//                      hpx::dataflow. Dependency-driven firing; same pool relaxation.
//   * hpx_async_then-- hpx::async(...).then(finalize): the continuation composes
//                      BELOW the caller-visible future (the point that HPX-native
//                      composition stays inside the backend, not the Python API).
//
// IDENTITY (honest, not faked): service_lane / hpx_lane emit their real lane
// actor_id ("act-hpx-" / "act-hpxl-"). The pool mechanisms emit a `pool_id` tag
// and lane_identity = "n/a" -- they do NOT fabricate a stable per-lane actor_id.
// `distinct_worker_ids` (how many HPX workers ran the pass) is reported only as
// "which worker ran it", never as a lane handle.
//
// ORDERING: FIFO is measured uniformly as the number of end_ns inversions vs
// submit order. A single serialized lane is strictly monotonic (0 inversions);
// scheduler-placed pools may complete out of submit order -- reported as a
// CONTRACT DIFFERENCE, not a bug.
//
// service_lane.hpp and hpx_lane.hpp are used UNMODIFIED (shared Request/Result/
// now_ns + the two lane classes). The pool mechanisms run the synthetic sleep via
// the cooperative hpx::this_thread::sleep_for (they execute on HPX workers, so a
// blocking std sleep would pin a worker); this matches HpxLane's timer and is the
// honest native-task service. Output: one compact JSON summary to --out (schema
// "hpx-task-dataflow-probe-1"). No per-request rows, no JSONL.

#include <hpx/hpx.hpp>
#include <hpx/hpx_main.hpp>

#include "hpx_lane.hpp"
#include "service_lane.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using rayhpx::HpxLane;
using rayhpx::now_ns;
using rayhpx::Request;
using rayhpx::Result;
using rayhpx::ServiceLane;
using rayhpx::WORK_MODE_SLEEP;

namespace {

constexpr char SCHEMA[] = "hpx-task-dataflow-probe-1";

// Fixed synthetic service-time targets (ms). 0 == no-op dispatch floor.
const std::vector<double> SERVICE_MS_LIST = {0.0, 1.0, 5.0, 20.0};

struct Options {
    int n = 200;       // requests per (mechanism, service_ms) cell
    std::string out;   // required: JSON summary path
};

bool parse_args(int argc, char** argv, Options& opt, std::string& err) {
    auto need_value = [&](int& i) -> const char* {
        if (i + 1 >= argc) return nullptr;
        return argv[++i];
    };
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a.rfind("--hpx:", 0) == 0) continue;  // HPX flags consumed pre-main
        if (a == "--n") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --n"; return false; }
            opt.n = std::stoi(v);
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
    if (opt.n < 1) { err = "--n must be >= 1"; return false; }
    return true;
}

// ---- small JSON + stats helpers (compact summary only) ------------------

std::string fmt_double(double v) {
    std::ostringstream os;
    os << std::setprecision(15) << v;
    return os.str();
}

// Nearest-rank-with-interpolation percentile (q in [0,100]); sorts a copy.
// Matches bench/analyze_jsonl.py::_percentile so the probe reads consistently.
double percentile(std::vector<double> v, double q) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    if (v.size() == 1) return v[0];
    const double rank = (q / 100.0) * (v.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(rank);
    const std::size_t hi = std::min(lo + 1, v.size() - 1);
    const double frac = rank - static_cast<double>(lo);
    return v[lo] + (v[hi] - v[lo]) * frac;
}

double mean_of(const std::vector<double>& v) {
    if (v.empty()) return 0.0;
    double s = 0.0;
    for (double x : v) s += x;
    return s / static_cast<double>(v.size());
}

// {p50,p90,p99,mean} for a vector.
std::string stat_block(const std::vector<double>& v) {
    std::ostringstream o;
    o << "{\"p50\": " << fmt_double(percentile(v, 50))
      << ", \"p90\": " << fmt_double(percentile(v, 90))
      << ", \"p99\": " << fmt_double(percentile(v, 99))
      << ", \"mean\": " << fmt_double(mean_of(v)) << "}";
    return o.str();
}

// ---- one serviced request (mechanism-agnostic record) -------------------

struct Rec {
    Result res;
    int worker = -1;  // HPX worker that ran it (pools); -1 for lanes (n/a)
};

std::chrono::nanoseconds ms_to_ns(double ms) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::duration<double, std::milli>(ms));
}

// Synthetic sleep service for the POOL mechanisms, run as an HPX task. Uses the
// COOPERATIVE hpx timer (it executes on an HPX worker; a blocking std sleep would
// pin a worker). Mirrors the work shape ServiceLane/HpxLane do internally, minus
// the lane queue. `pool_tag` is recorded in actor_id only so the reducer can read
// it back as pool_id -- it is a POOL tag, never a per-lane handle.
Rec service_rec(const Request& req, const char* pool_tag) {
    Rec rec;
    Result& r = rec.res;
    r.request_id = req.request_id;
    r.actor_id = pool_tag;          // read back as pool_id, not a lane actor_id
    r.submit_ns = req.submit_ns;
    r.start_ns = now_ns();
    if (req.service_ms_requested > 0.0) {
        hpx::this_thread::sleep_for(ms_to_ns(req.service_ms_requested));
    }
    r.end_ns = now_ns();
    r.status = "completed";
    r.chunks_completed = req.chunks;
    rec.worker = static_cast<int>(hpx::get_worker_thread_num());
    return rec;
}

std::vector<Request> make_reqs(int n, double service_ms) {
    std::vector<Request> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i) {
        Request q;
        char buf[32];
        std::snprintf(buf, sizeof buf, "req-%06d", i);
        q.request_id = buf;
        q.service_ms_requested = service_ms;
        q.work_mode = WORK_MODE_SLEEP;  // sleep-only by design (no spin in v1)
        v.push_back(std::move(q));
    }
    return v;
}

// ---- mechanism runners (each fills `out` in SUBMIT order; returns wall ms) ----

template <class Lane>
double run_lane(std::vector<Request> reqs, std::vector<Rec>& out,
                std::string& lane_id) {
    Lane lane;  // diag=false: original hot path
    lane_id = lane.actor_id();
    std::vector<hpx::future<Result>> futs;
    futs.reserve(reqs.size());
    const std::int64_t t0 = now_ns();
    for (auto& q : reqs) {
        q.submit_ns = now_ns();
        futs.push_back(lane.submit(std::move(q)));
    }
    hpx::wait_all(futs);
    const std::int64_t t1 = now_ns();
    for (auto& f : futs) out.push_back(Rec{f.get(), -1});
    return (t1 - t0) / 1e6;
}

double run_hpx_async(std::vector<Request> reqs, std::vector<Rec>& out,
                     const char* pool) {
    std::vector<hpx::future<Rec>> futs;
    futs.reserve(reqs.size());
    const std::int64_t t0 = now_ns();
    for (auto& q : reqs) {
        q.submit_ns = now_ns();
        Request rq = std::move(q);
        futs.push_back(hpx::async([rq, pool] { return service_rec(rq, pool); }));
    }
    hpx::wait_all(futs);
    const std::int64_t t1 = now_ns();
    for (auto& f : futs) out.push_back(f.get());
    return (t1 - t0) / 1e6;
}

// Tiny per-request dependency graph: a trivial "prepare" node whose future feeds
// the "service" node via hpx::dataflow (service fires only once prepare is ready).
double run_hpx_dataflow(std::vector<Request> reqs, std::vector<Rec>& out,
                        const char* pool) {
    std::vector<hpx::future<Rec>> futs;
    futs.reserve(reqs.size());
    const std::int64_t t0 = now_ns();
    for (auto& q : reqs) {
        q.submit_ns = now_ns();
        Request rq = std::move(q);
        hpx::future<int> prep = hpx::async([] { return 0; });
        futs.push_back(hpx::dataflow(
            [rq, pool](hpx::future<int> p) {
                p.get();  // consume the dependency
                return service_rec(rq, pool);
            },
            std::move(prep)));
    }
    hpx::wait_all(futs);
    const std::int64_t t1 = now_ns();
    for (auto& f : futs) out.push_back(f.get());
    return (t1 - t0) / 1e6;
}

// async + a continuation that runs BELOW the caller-visible future. The .then
// composition is invisible to whoever holds the returned future -- the point that
// HPX-native composition belongs inside the backend, not exposed as Python API.
double run_hpx_async_then(std::vector<Request> reqs, std::vector<Rec>& out,
                          const char* pool) {
    std::vector<hpx::future<Rec>> futs;
    futs.reserve(reqs.size());
    const std::int64_t t0 = now_ns();
    for (auto& q : reqs) {
        q.submit_ns = now_ns();
        Request rq = std::move(q);
        futs.push_back(
            hpx::async([rq, pool] { return service_rec(rq, pool); })
                .then([](hpx::future<Rec> f) {
                    Rec r = f.get();
                    r.res.recv_ns = now_ns();  // finalize below the future
                    return r;
                }));
    }
    hpx::wait_all(futs);
    const std::int64_t t1 = now_ns();
    for (auto& f : futs) out.push_back(f.get());
    return (t1 - t0) / 1e6;
}

// ---- reduce one cell to JSON --------------------------------------------

// end_ns inversions vs submit order: count pairs (i<j) where the later-submitted
// request finished EARLIER. 0 == strict FIFO completion. O(n^2) is fine at n<=200.
int end_ns_inversions(const std::vector<Rec>& recs) {
    int inv = 0;
    const std::size_t n = recs.size();
    for (std::size_t i = 0; i < n; ++i)
        for (std::size_t j = i + 1; j < n; ++j)
            if (recs[i].res.end_ns > recs[j].res.end_ns) ++inv;
    return inv;
}

std::string cell_json(const char* mechanism, const char* klass, double service_ms,
                      int n, const std::vector<Rec>& recs, double wall_ms,
                      const std::string& lane_id, const char* pool_id) {
    int completed = 0;
    std::vector<double> svc;          // observed service (end-start) ms
    std::vector<double> overshoot;    // pct vs requested (only service_ms>0)
    std::set<std::string> actor_ids;
    std::set<int> worker_ids;
    svc.reserve(recs.size());
    for (const Rec& rec : recs) {
        if (rec.res.status == "completed") ++completed;
        const double s = (rec.res.end_ns - rec.res.start_ns) / 1e6;
        svc.push_back(s);
        if (service_ms > 0.0)
            overshoot.push_back(100.0 * (s - service_ms) / service_ms);
        if (!rec.res.actor_id.empty()) actor_ids.insert(rec.res.actor_id);
        if (rec.worker >= 0) worker_ids.insert(rec.worker);
    }
    const int inv = end_ns_inversions(recs);
    const bool is_lane = (std::string(klass) == "lane");
    const double thr =
        wall_ms > 0.0 ? n / (wall_ms / 1000.0) : 0.0;

    std::ostringstream o;
    o << "    {\"mechanism\": \"" << mechanism << "\", \"class\": \"" << klass
      << "\", \"service_ms\": " << fmt_double(service_ms)
      << ", \"submitted\": " << n << ", \"completed\": " << completed
      << ", \"failed\": " << (n - completed)
      << ", \"wall_ms\": " << fmt_double(wall_ms)
      << ", \"throughput_ops_s\": " << fmt_double(thr)
      << ", \"service_observed_ms\": " << stat_block(svc)
      << ", \"overshoot_pct\": "
      << (overshoot.empty() ? std::string("null") : stat_block(overshoot))
      << ", \"inversions\": " << inv
      << ", \"fifo_preserved\": " << (inv == 0 ? "true" : "false")
      << ", \"lane_identity\": "
      << (is_lane ? ("\"" + lane_id + "\"") : std::string("\"n/a\""))
      << ", \"pool_id\": "
      << (pool_id ? ("\"" + std::string(pool_id) + "\"") : std::string("null"))
      << ", \"distinct_actor_ids\": "
      << (is_lane ? std::to_string(actor_ids.size()) : std::string("null"))
      << ", \"distinct_worker_ids\": "
      << (is_lane ? std::string("null") : std::to_string(worker_ids.size()))
      << "}";
    return o.str();
}

// ---- one mechanism dispatcher -------------------------------------------

double dispatch(const std::string& mechanism, std::vector<Request> reqs,
                std::vector<Rec>& out, std::string& lane_id, const char* pool) {
    if (mechanism == "service_lane")
        return run_lane<ServiceLane>(std::move(reqs), out, lane_id);
    if (mechanism == "hpx_lane")
        return run_lane<HpxLane>(std::move(reqs), out, lane_id);
    if (mechanism == "hpx_async")
        return run_hpx_async(std::move(reqs), out, pool);
    if (mechanism == "hpx_dataflow")
        return run_hpx_dataflow(std::move(reqs), out, pool);
    if (mechanism == "hpx_async_then")
        return run_hpx_async_then(std::move(reqs), out, pool);
    return 0.0;
}

struct Mech {
    const char* name;
    const char* klass;   // "lane" | "pool"
    const char* pool_id; // nullptr for lanes
};

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    std::string err;
    if (!parse_args(argc, argv, opt, err)) {
        std::fprintf(stderr, "[hpx_task_dataflow_probe] error: %s\n", err.c_str());
        return 2;
    }

    const std::size_t hpx_threads = hpx::get_num_worker_threads();

    const std::vector<Mech> mechs = {
        {"service_lane", "lane", nullptr},
        {"hpx_lane", "lane", nullptr},
        {"hpx_async", "pool", "hpx-async-pool"},
        {"hpx_dataflow", "pool", "hpx-dataflow-pool"},
        {"hpx_async_then", "pool", "hpx-async-then-pool"},
    };

    std::vector<std::string> cells;
    for (const Mech& m : mechs) {
        for (double sm : SERVICE_MS_LIST) {
            std::vector<Rec> recs;
            recs.reserve(opt.n);
            std::string lane_id;
            const double wall_ms =
                dispatch(m.name, make_reqs(opt.n, sm), recs, lane_id, m.pool_id);
            cells.push_back(cell_json(m.name, m.klass, sm, opt.n, recs, wall_ms,
                                      lane_id, m.pool_id));
        }
    }

    std::ostringstream o;
    o << "{\n";
    o << "  \"probe\": \"hpx_task_dataflow_probe\",\n";
    o << "  \"schema\": \"" << SCHEMA << "\",\n";
    o << "  \"isolated_mechanism_probe\": true,\n";
    o << "  \"hpx_threads\": " << hpx_threads << ",\n";
    o << "  \"config\": {\"n\": " << opt.n
      << ", \"work_mode\": \"sleep\", \"service_ms_list\": [0, 1, 5, 20]},\n";
    o << "  \"cells\": [\n";
    for (std::size_t i = 0; i < cells.size(); ++i) {
        o << cells[i] << (i + 1 < cells.size() ? ",\n" : "\n");
    }
    o << "  ]\n";
    o << "}\n";

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
        std::fprintf(stderr, "[hpx_task_dataflow_probe] cannot open out: %s\n",
                     opt.out.c_str());
        return 1;
    }
    f << o.str();
    f.close();

    std::printf("[hpx_task_dataflow_probe] hpx_threads=%zu n=%d mechanisms=%zu "
                "out=%s\n",
                hpx_threads, opt.n, mechs.size(), opt.out.c_str());
    return 0;
}
