// HPX-native lane-primitive feasibility microbenchmark (isolated probe).
//
// This is a STANDALONE PRIMITIVE PROBE, deliberately separate from the
// serving-control benchmark corpus. It does NOT use the JSONL benchmark schema,
// it is NOT a retire-mode/lane-sweep driver, and its numbers are NOT comparable
// to benchmark 06/10 or any of the experiments/benchmarks write-ups. It exists
// only to characterize two low-level HPX/std primitives in isolation, so we can
// reason about WHY the serving-control results look the way they do.
//
// It measures exactly two things, nothing else:
//
//   1. Sleep overshoot: how much longer than requested does a blocking
//      std::this_thread::sleep_for vs a cooperative hpx::this_thread::sleep_for
//      actually park, for 1 / 5 / 20 ms targets. (Calibration for the
//      sleep-fidelity caveat already documented in experiment 01.)
//
//   2. No-op dispatch throughput: ops/sec for
//        (a) the CURRENT ServiceLane reference path (service_lane.hpp, the same
//            single-serialized-lane mechanism the native baseline and rayx use),
//            with service_ms == 0 (pure dispatch, no synthetic work), vs
//        (b) a plain hpx::async no-op path (one trivial task per op).
//      These two are NOT a serving-lane-vs-serving-lane comparison: (a) is one
//      serialized FIFO consumer (the actor-like anchor); (b) is the HPX task
//      scheduler spreading trivial tasks across worker threads. (b) is SCHEDULER
//      TERRITORY, not a serving-lane result. The report must keep them distinct.
//
// service_lane.hpp is used UNMODIFIED. Timing uses the same steady_clock
// (rayhpx::now_ns) as the rest of the project.
//
// Output: one compact JSON summary to --out (machine-readable; see schema
// "lane-feasibility-1" below). No per-op rows, no JSONL.

#include <hpx/hpx.hpp>
#include <hpx/hpx_main.hpp>

#include "service_lane.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using rayhpx::now_ns;
using rayhpx::Request;
using rayhpx::Result;
using rayhpx::ServiceLane;
using rayhpx::WORK_MODE_SLEEP;

namespace {

constexpr char SCHEMA[] = "lane-feasibility-1";

struct Options {
    int ops = 50000;       // no-op dispatch ops per repeat
    int repeats = 5;       // timed dispatch repeats per path
    int sleep_samples = 200;  // samples per (primitive, target) sleep cell
    std::string out;       // required: JSON summary path
};

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
        if (a == "--ops") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --ops"; return false; }
            opt.ops = std::stoi(v);
        } else if (a == "--repeats") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --repeats"; return false; }
            opt.repeats = std::stoi(v);
        } else if (a == "--sleep-samples") {
            const char* v = need_value(i);
            if (!v) { err = "missing value for --sleep-samples"; return false; }
            opt.sleep_samples = std::stoi(v);
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
    if (opt.ops < 1) { err = "--ops must be >= 1"; return false; }
    if (opt.repeats < 1) { err = "--repeats must be >= 1"; return false; }
    if (opt.sleep_samples < 1) {
        err = "--sleep-samples must be >= 1";
        return false;
    }
    return true;
}

// ---- small JSON + stats helpers (compact summary only) ------------------

std::string fmt_double(double v) {
    std::ostringstream os;
    os << std::setprecision(15) << v;
    return os.str();
}

// Nearest-rank-with-interpolation percentile (q in [0,100]); sorts in place.
// Matches bench/analyze_jsonl.py::_percentile so the probe reads consistently.
double percentile(std::vector<double>& v, double q) {
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

double median_of(std::vector<double> v) { return percentile(v, 50); }

// {p50,p90,p99,mean} for a vector (copy: percentile sorts).
std::string stat_block(std::vector<double> v, bool with_mean = true) {
    const double p50 = percentile(v, 50);
    const double p90 = percentile(v, 90);
    const double p99 = percentile(v, 99);
    std::ostringstream o;
    o << "{\"p50\": " << fmt_double(p50) << ", \"p90\": " << fmt_double(p90)
      << ", \"p99\": " << fmt_double(p99);
    if (with_mean) o << ", \"mean\": " << fmt_double(mean_of(v));
    o << "}";
    return o.str();
}

std::string array_block(const std::vector<double>& v) {
    std::ostringstream o;
    o << "[";
    for (std::size_t i = 0; i < v.size(); ++i) {
        o << fmt_double(v[i]) << (i + 1 < v.size() ? ", " : "");
    }
    o << "]";
    return o.str();
}

// ---- 1. sleep overshoot --------------------------------------------------

// Take `samples` timed sleeps of `target_ms`, using either the blocking std
// primitive (parks the OS thread) or the cooperative HPX primitive (yields the
// HPX worker). Returns observed durations in ms.
std::vector<double> sample_sleep(bool use_hpx, double target_ms, int samples) {
    std::vector<double> observed;
    observed.reserve(samples);
    const auto dur = std::chrono::duration<double, std::milli>(target_ms);
    for (int i = 0; i < samples; ++i) {
        const std::int64_t t0 = now_ns();
        if (use_hpx) {
            hpx::this_thread::sleep_for(
                std::chrono::duration_cast<std::chrono::nanoseconds>(dur));
        } else {
            std::this_thread::sleep_for(dur);
        }
        const std::int64_t t1 = now_ns();
        observed.push_back((t1 - t0) / 1e6);
    }
    return observed;
}

std::string sleep_cell_json(const char* primitive, double target_ms,
                            int samples, std::vector<double> observed) {
    std::vector<double> overshoot;
    std::vector<double> pct;
    overshoot.reserve(observed.size());
    pct.reserve(observed.size());
    for (double o : observed) {
        overshoot.push_back(o - target_ms);
        pct.push_back(target_ms > 0.0 ? 100.0 * (o - target_ms) / target_ms : 0.0);
    }
    std::ostringstream o;
    o << "    {\"primitive\": \"" << primitive << "\", \"target_ms\": "
      << fmt_double(target_ms) << ", \"samples\": " << samples
      << ", \"observed_ms\": " << stat_block(observed)
      << ", \"overshoot_ms\": " << stat_block(overshoot)
      << ", \"pct_overshoot\": " << stat_block(pct, /*with_mean=*/false) << "}";
    return o.str();
}

// ---- 2. no-op dispatch throughput ---------------------------------------

// One timed pass of `ops` no-op submits through the ServiceLane reference path
// (service_ms == 0 -> the lane's no-op service). Single serialized FIFO consumer
// -- the actor-like anchor. Returns wall seconds for the pass.
double run_service_lane_pass(int ops) {
    ServiceLane lane;  // diag=false: original hot path, no extra clock reads
    std::vector<hpx::future<Result>> futs;
    futs.reserve(ops);
    const std::int64_t t0 = now_ns();
    for (int i = 0; i < ops; ++i) {
        Request req;
        req.request_id = "noop";
        req.service_ms_requested = 0.0;  // no synthetic work: pure dispatch
        req.work_mode = WORK_MODE_SLEEP;
        req.submit_ns = now_ns();
        futs.push_back(lane.submit(std::move(req)));
    }
    hpx::wait_all(futs);
    const std::int64_t t1 = now_ns();
    for (auto& f : futs) f.get();  // drain after timing (release promises)
    return (t1 - t0) / 1e9;
}

// One timed pass of `ops` plain hpx::async no-op tasks. SCHEDULER TERRITORY:
// trivial tasks spread across HPX worker threads, NOT a single serving lane.
// Kept here only as a primitive contrast, never as a serving-lane result.
double run_hpx_async_pass(int ops) {
    std::vector<hpx::future<int>> futs;
    futs.reserve(ops);
    const std::int64_t t0 = now_ns();
    for (int i = 0; i < ops; ++i) {
        futs.push_back(hpx::async([] { return 0; }));
    }
    hpx::wait_all(futs);
    const std::int64_t t1 = now_ns();
    for (auto& f : futs) f.get();  // drain after timing
    return (t1 - t0) / 1e9;
}

std::string dispatch_cell_json(const char* path, const char* note, int ops,
                               int repeats, double (*pass)(int)) {
    pass(ops);  // one untimed warmup pass (stabilize caches/threads)
    std::vector<double> thr;       // ops/sec per repeat
    std::vector<double> ns_per_op; // ns/op per repeat
    thr.reserve(repeats);
    ns_per_op.reserve(repeats);
    for (int r = 0; r < repeats; ++r) {
        const double wall_s = pass(ops);
        thr.push_back(wall_s > 0.0 ? ops / wall_s : 0.0);
        ns_per_op.push_back(wall_s > 0.0 ? (wall_s * 1e9) / ops : 0.0);
    }
    std::ostringstream o;
    o << "    {\"path\": \"" << path << "\", \"note\": \"" << note
      << "\", \"ops\": " << ops << ", \"repeats\": " << repeats
      << ", \"throughput_ops_s\": {\"per_repeat\": " << array_block(thr)
      << ", \"median\": " << fmt_double(median_of(thr)) << "}"
      << ", \"ns_per_op\": {\"per_repeat\": " << array_block(ns_per_op)
      << ", \"median\": " << fmt_double(median_of(ns_per_op)) << "}}";
    return o.str();
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    std::string err;
    if (!parse_args(argc, argv, opt, err)) {
        std::fprintf(stderr, "[hpx_lane_feasibility] error: %s\n", err.c_str());
        return 2;
    }

    const std::size_t hpx_threads = hpx::get_num_worker_threads();

    // --- 1. sleep overshoot ---
    const double targets[] = {1.0, 5.0, 20.0};
    std::vector<std::string> sleep_cells;
    for (double t : targets) {
        sleep_cells.push_back(sleep_cell_json(
            "std::this_thread::sleep_for", t, opt.sleep_samples,
            sample_sleep(/*use_hpx=*/false, t, opt.sleep_samples)));
    }
    for (double t : targets) {
        sleep_cells.push_back(sleep_cell_json(
            "hpx::this_thread::sleep_for", t, opt.sleep_samples,
            sample_sleep(/*use_hpx=*/true, t, opt.sleep_samples)));
    }

    // --- 2. no-op dispatch throughput ---
    std::vector<std::string> dispatch_cells;
    dispatch_cells.push_back(dispatch_cell_json(
        "service_lane",
        "current ServiceLane reference path (single serialized FIFO lane, "
        "service_ms=0); the actor-like anchor",
        opt.ops, opt.repeats, run_service_lane_pass));
    dispatch_cells.push_back(dispatch_cell_json(
        "hpx_async",
        "plain hpx::async no-op (scheduler territory: trivial tasks across "
        "worker threads); NOT a serving-lane result",
        opt.ops, opt.repeats, run_hpx_async_pass));

    // --- emit compact JSON summary ---
    std::ostringstream o;
    o << "{\n";
    o << "  \"probe\": \"hpx_native_lane_feasibility\",\n";
    o << "  \"schema\": \"" << SCHEMA << "\",\n";
    o << "  \"isolated_primitive_probe\": true,\n";
    o << "  \"hpx_threads\": " << hpx_threads << ",\n";
    o << "  \"config\": {\"ops\": " << opt.ops << ", \"repeats\": "
      << opt.repeats << ", \"sleep_samples\": " << opt.sleep_samples
      << ", \"sleep_targets_ms\": [1, 5, 20]},\n";
    o << "  \"sleep_overshoot\": [\n";
    for (std::size_t i = 0; i < sleep_cells.size(); ++i) {
        o << sleep_cells[i] << (i + 1 < sleep_cells.size() ? ",\n" : "\n");
    }
    o << "  ],\n";
    o << "  \"dispatch\": [\n";
    for (std::size_t i = 0; i < dispatch_cells.size(); ++i) {
        o << dispatch_cells[i] << (i + 1 < dispatch_cells.size() ? ",\n" : "\n");
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
        std::fprintf(stderr, "[hpx_lane_feasibility] cannot open out: %s\n",
                     opt.out.c_str());
        return 1;
    }
    f << o.str();
    f.close();

    std::printf(
        "[hpx_lane_feasibility] hpx_threads=%zu ops=%d repeats=%d "
        "sleep_samples=%d out=%s\n",
        hpx_threads, opt.ops, opt.repeats, opt.sleep_samples, opt.out.c_str());
    return 0;
}
