// rayx.runtime dispatch-decomposition probe (experiments/25) -- standalone,
// native-only, DIAGNOSTIC ONLY.
//
// Decomposes the per-operation dispatch path of the experimental `rayx.runtime`
// RuntimeLane and estimates the cost of the nested
//
//     hpx::async(exec_, task).get()
//
// inside the lane worker (python/src/rayx/runtime_lane.hpp, run()). The lane
// worker is itself an hpx::thread, so an INLINE `task(stop)` call would preserve
// the current cooperative semantics (park_ms still suspends cooperatively,
// checkpoint yields still run, FIFO unchanged); the open question is what the
// nested hop COSTS. This probe measures; it does not decide.
//
// The production runtime headers are included READ-ONLY and UNMODIFIED:
//     python/src/rayx/runtime_lane.hpp      (RuntimeLane, used as-is)
//     python/src/rayx/runtime_ops.hpp       (fixed registry: square / busy_sum)
//     python/src/rayx/runtime_ops_hpx.hpp   (HPX-side registry: park_ms)
// No pybind, no Python, no _rayx involvement. The probe is NOT part of the
// serving-control benchmark corpus, does NOT use the v1 benchmark JSONL schema,
// and its numbers are NOT comparable to any corpus entry. Observation-only,
// machine-specific: NO performance verdict, NO speedup claim, NO Ray comparison.
//
// Modes (same op bodies, same registered-op closures; only DISPATCH differs):
//   * inline_body       task(stop) called directly on an HPX thread -- body floor.
//   * async_get         hpx::async(exec, [task,stop]{ return task(stop); }).get()
//                       on an HPX thread -- the EXACT production dispatch idiom
//                       (runtime_lane.hpp run(), nested-async lines). The
//                       async_get - inline_body delta is the PRIMITIVE estimate
//                       of the nested-async per-op overhead (approximate).
//   * lane_nested_*     production RuntimeLane, unmodified -- full lane cost
//                       (queue push/pop, cv wakeup, token+promise, nested hop).
//   * lane_inline_*     InlineDispatchProbeLane (below): MEASUREMENT FORK ONLY;
//                       not production RuntimeLane; not a maintained alternate
//                       implementation. Identical worker loop with ONLY the
//                       nested async replaced by an inline task(stop) call. The
//                       lane_nested - lane_inline delta is the IN-LANE estimate
//                       of the same overhead (approximate).
//   Lane modes run two shapes: _seq (one-at-a-time submit+get latency) and
//   _batch (submit BATCH_SIZE then drain; per-op time = span / BATCH_SIZE).
//
// Ops (tiny, no real-duration parked sleeps -- exp24 owns parked timing):
//   square(7) -> 49; busy_sum(256) -> (256*255/2) & (2^31-1) = 32640 (single
//   chunk, no checkpoint reached); park_ms(0) -> echoes 0 (parks nothing).
//
// GATES are structural only (every op completes with the expected value; lane
// modes additionally FIFO end_ns inversions == 0 and the production lane's
// rt-hpx- id prefix). ALL timing is observation-only and is NEVER gated; the
// derived deltas are approximations, NOT exact subtractions (they still include
// closure-copy, scheduling jitter, and clock granularity).
//
// Output: one compact JSON summary on STDOUT (schema
// "rayx-runtime-dispatch-probe-1", probe-local); human progress goes to stderr.
// The experiments/25 runner curates the tracked aggregate.json.

#include <hpx/hpx.hpp>
#include <hpx/hpx_main.hpp>

#include "runtime_lane.hpp"      // production RuntimeLane -- READ-ONLY include
#include "runtime_ops.hpp"       // fixed HPX-free registry + RuntimeResult
#include "runtime_ops_hpx.hpp"   // HPX-side registry (park_ms)

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <memory>
#include <string>
#include <vector>

namespace {

// Monotonic ns clock. Mirrors rayhpx::now_ns (service_lane.hpp) without pulling
// the harness header into a runtime probe.
std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Probe-local mirror of _rayx.cpp make_op_task (timing + value/failure mapping,
// minus pybind). Kept shape-identical so lane cells run the same closure work the
// production submit path builds per operation.
rayx_runtime::RuntimeLane::OpTask make_probe_op_task(
        rayx_runtime::OpFn fn, rayx_runtime::OpArgs args, std::string actor) {
    return [fn = std::move(fn), args = std::move(args), actor = std::move(actor)](
               const rayx_runtime::StopCheckpoint& stop)
               -> rayx_runtime::RuntimeResult {
        rayx_runtime::RuntimeResult r;
        r.actor_id = actor;
        r.start_ns = now_ns();
        try {
            rayx_runtime::OpOutcome o = fn(args, stop);
            r.value = o.value;
            r.has_value = o.has_value;
            r.status = o.status;
            r.error = o.error;
        } catch (const std::exception& e) {
            r.status = "failed";
            r.error = e.what();
            r.has_value = false;
        } catch (...) {
            r.status = "failed";
            r.error = "operation failed with a non-std::exception";
            r.has_value = false;
        }
        r.end_ns = now_ns();
        return r;
    };
}

// ---------------------------------------------------------------------------
// InlineDispatchProbeLane: MEASUREMENT FORK ONLY; not production RuntimeLane;
// not a maintained alternate implementation. A minimal copy of
// rayx_runtime::RuntimeLane (runtime_lane.hpp) whose worker loop differs in
// EXACTLY ONE place: the nested hpx::async(exec_, task).get() dispatch is
// replaced by an inline task(stop) call (same defensive try/catch mapping).
// Everything else relevant to the measured path -- hpx::thread worker,
// hpx::mutex + hpx::condition_variable_any queue, per-item promise + cancel
// token, begin_service, the StopCheckpoint binding with its cooperative yield,
// mark_finished, and the active_/active_tok_ bookkeeping (same two lock
// acquisitions per item) -- is copied unchanged so the lane_nested - lane_inline
// delta isolates the dispatch difference as closely as a fork can.
// try_submit / stats / cancel_pending are omitted: neither lane mode exercises
// them, so their absence cannot bias the comparison. Lane ids use a distinct
// "rt-prbi-" prefix so a probe lane can never be mistaken for a production
// "rt-hpx-" operation lane. If RuntimeLane::run() changes, this fork is probe
// evidence for experiments/25 only and must NOT be treated as current.
// ---------------------------------------------------------------------------
class InlineDispatchProbeLane {
public:
    using OpTask = rayx_runtime::RuntimeLane::OpTask;

    InlineDispatchProbeLane() {
        actor_id_ = rayx_runtime::make_runtime_actor_id("rt-prbi-");
        worker_ = hpx::thread([this]() { run(); });
    }
    ~InlineDispatchProbeLane() = default;
    InlineDispatchProbeLane(const InlineDispatchProbeLane&) = delete;
    InlineDispatchProbeLane& operator=(const InlineDispatchProbeLane&) = delete;

    const std::string& actor_id() const { return actor_id_; }

    // Copied from RuntimeLane::submit (same promise/token/queue/notify work).
    hpx::future<rayx_runtime::RuntimeResult> submit(
            OpTask task, int checkpoint_count,
            std::shared_ptr<rayx_runtime::RuntimeCancelToken>* out_tok) {
        auto prom =
            std::make_shared<hpx::promise<rayx_runtime::RuntimeResult>>();
        hpx::future<rayx_runtime::RuntimeResult> fut = prom->get_future();
        auto tok = std::make_shared<rayx_runtime::RuntimeCancelToken>();
        tok->arm(prom, actor_id_);
        {
            std::lock_guard<hpx::mutex> lk(mu_);
            queue_.push_back(
                Item{std::move(task), std::move(prom), tok, checkpoint_count});
        }
        cv_.notify_one();
        if (out_tok) *out_tok = std::move(tok);
        return fut;
    }

    // Copied from RuntimeLane::stop_and_join (same contract: HPX thread, drains).
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
        std::shared_ptr<hpx::promise<rayx_runtime::RuntimeResult>> prom;
        std::shared_ptr<rayx_runtime::RuntimeCancelToken> tok;
        int checkpoint_count = 1;
    };

    // Copied from RuntimeLane::run(); the ONLY intended difference is the
    // inline dispatch (marked below).
    void run() {
        for (;;) {
            Item item;
            {
                std::unique_lock<hpx::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                item = std::move(queue_.front());
                queue_.pop_front();
                active_.store(true, std::memory_order_relaxed);
                active_tok_ = item.tok;
            }
            if (item.tok && !item.tok->begin_service(item.checkpoint_count)) {
                clear_active();
                continue;
            }
            std::shared_ptr<rayx_runtime::RuntimeCancelToken> tok = item.tok;
            rayx_runtime::StopCheckpoint stop =
                [tok](bool next_is_final) -> bool {
                    bool s = tok ? tok->stop_at_boundary(next_is_final) : false;
                    hpx::this_thread::yield();
                    return s;
                };
            rayx_runtime::RuntimeResult r;
            try {
                // INLINE DISPATCH -- the one intended difference from
                // RuntimeLane::run(), which does:
                //     r = hpx::async(exec_, [task = item.task, stop]() {
                //             return task(stop);
                //         }).get();
                r = item.task(stop);
            } catch (...) {
                r.actor_id = actor_id_;
                r.status = "failed";
                r.error = "runtime dispatch failed";
                r.has_value = false;
            }
            if (item.tok && r.status != "cancelled") item.tok->mark_finished();
            item.prom->set_value(std::move(r));
            clear_active();
        }
    }

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
    bool stop_ = false;
    std::atomic<bool> active_{false};
    std::shared_ptr<rayx_runtime::RuntimeCancelToken> active_tok_;
};

// ---------------------------------------------------------------------------
// Op specs: tiny registered ops resolved from the PRODUCTION registries.
// ---------------------------------------------------------------------------
struct OpSpec {
    std::string name;
    rayx_runtime::OpArgs args;
    std::int64_t expected;  // known closed-form result (gate, not timing)
    rayx_runtime::OpFn fn;
    int checkpoint_count = 1;
};

const rayx_runtime::OpEntry* find_op(const std::string& name) {
    auto it = rayx_runtime::registry().find(name);
    if (it != rayx_runtime::registry().end()) return &it->second;
    auto hit = rayx_runtime::hpx_registry().find(name);
    if (hit != rayx_runtime::hpx_registry().end()) return &hit->second;
    return nullptr;
}

std::vector<OpSpec> make_op_specs() {
    std::vector<OpSpec> specs;
    // square(7) = 49 (no-op floor: one multiply).
    // busy_sum(256) = (256*255/2) & (2^31-1) = 32640 (single chunk: 256 <=
    // BUSY_SUM_STRIDE, so checkpoint_count == 1 and stop is never polled).
    // park_ms(0) echoes 0 (parks nothing; checkpoint_count == 1) -- the parked
    // op at its no-park floor; real parked durations are exp24's axis.
    struct Row { const char* name; std::int64_t arg; std::int64_t expected; };
    const Row rows[] = {
        {"square", 7, 49},
        {"busy_sum", 256, 32640},
        {"park_ms", 0, 0},
    };
    for (const Row& row : rows) {
        const rayx_runtime::OpEntry* e = find_op(row.name);
        if (!e) {
            std::fprintf(stderr,
                         "[rayx_runtime_dispatch_probe] missing op: %s\n",
                         row.name);
            std::exit(2);
        }
        OpSpec s;
        s.name = row.name;
        s.args = {row.arg};
        s.expected = row.expected;
        s.fn = e->fn;
        s.checkpoint_count = e->checkpoint_count(s.args);
        specs.push_back(std::move(s));
    }
    return specs;
}

// ---------------------------------------------------------------------------
// Sampling / summary (observation-only; percentiles match the exp20 probe /
// bench/analyze_jsonl.py linear interpolation).
// ---------------------------------------------------------------------------
double percentile(std::vector<double>& xs, double q) {
    if (xs.empty()) return 0.0;
    std::sort(xs.begin(), xs.end());
    if (xs.size() == 1) return xs[0];
    const double k = (static_cast<double>(xs.size()) - 1.0) * (q / 100.0);
    const std::size_t f = static_cast<std::size_t>(k);
    const std::size_t c = std::min(f + 1, xs.size() - 1);
    if (f == c) return xs[f];
    return xs[f] + (xs[c] - xs[f]) * (k - static_cast<double>(f));
}

struct Cell {
    std::string mode;
    std::string op;
    int n = 0;            // retained samples (after warmup)
    int ops_total = 0;    // operations actually executed (incl. warmup)
    double min_ns = 0, p50_ns = 0, p90_ns = 0, p99_ns = 0, max_ns = 0,
           mean_ns = 0;
    // structural gates (results, never timing)
    bool all_completed = true;
    bool values_ok = true;
    int fifo_inversions = 0;   // lane modes only; 0 elsewhere
    bool lane_prefix_ok = true;  // lane modes only; true elsewhere
};

void summarize(Cell& c, std::vector<double>& samples) {
    c.n = static_cast<int>(samples.size());
    if (samples.empty()) return;
    double sum = 0;
    for (double v : samples) sum += v;
    c.mean_ns = sum / static_cast<double>(samples.size());
    c.p50_ns = percentile(samples, 50.0);
    c.p90_ns = percentile(samples, 90.0);
    c.p99_ns = percentile(samples, 99.0);
    c.min_ns = samples.front();   // sorted by percentile()
    c.max_ns = samples.back();
}

void check_result(Cell& c, const rayx_runtime::RuntimeResult& r,
                  std::int64_t expected) {
    if (r.status != "completed" || !r.has_value) c.all_completed = false;
    else if (!std::holds_alternative<std::int64_t>(r.value) ||
             std::get<std::int64_t>(r.value) != expected) c.values_ok = false;
}

// ---------------------------------------------------------------------------
// Mode runners. main() runs as an HPX thread (hpx_main wrapper), satisfying the
// lane submit/stop contracts and making the primitive .get()s cooperative.
// ---------------------------------------------------------------------------

// inline_body / async_get: the task closure is built ONCE per cell (both modes
// equally), so the loop isolates dispatch; async_get copies the task into the
// per-dispatch lambda exactly as the production worker loop does.
Cell run_primitive(bool nested, const OpSpec& spec, int iters, int warmup) {
    Cell c;
    c.mode = nested ? "async_get" : "inline_body";
    c.op = spec.name;
    c.ops_total = iters + warmup;
    hpx::execution::parallel_executor exec;  // same type as RuntimeLane::exec_
    rayx_runtime::RuntimeLane::OpTask task =
        make_probe_op_task(spec.fn, spec.args, "probe-primitive");
    // Probe-shaped StopCheckpoint: the lane binds a token poll + cooperative
    // yield; with no token this keeps the yield (the part with a cost) so both
    // primitive modes pay what a lane-bound checkpoint would.
    rayx_runtime::StopCheckpoint stop = [](bool) -> bool {
        hpx::this_thread::yield();
        return false;
    };
    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(iters));
    for (int i = 0; i < iters + warmup; ++i) {
        const std::int64_t t0 = now_ns();
        rayx_runtime::RuntimeResult r;
        if (nested) {
            r = hpx::async(exec, [task, stop]() { return task(stop); }).get();
        } else {
            r = task(stop);
        }
        const std::int64_t t1 = now_ns();
        check_result(c, r, spec.expected);
        if (i >= warmup) samples.push_back(static_cast<double>(t1 - t0));
    }
    summarize(c, samples);
    return c;
}

// One-at-a-time lane latency: per-op submit -> get, fresh task per submit (the
// production submit path also builds one closure per operation).
template <class Lane>
Cell run_lane_seq(const char* mode, const OpSpec& spec, int iters, int warmup,
                  const char* expect_prefix) {
    Cell c;
    c.mode = mode;
    c.op = spec.name;
    c.ops_total = iters + warmup;
    Lane lane;
    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(iters));
    std::int64_t last_end = 0;
    for (int i = 0; i < iters + warmup; ++i) {
        const std::int64_t t0 = now_ns();
        auto fut = lane.submit(
            make_probe_op_task(spec.fn, spec.args, lane.actor_id()),
            spec.checkpoint_count, nullptr);
        rayx_runtime::RuntimeResult r = fut.get();
        const std::int64_t t1 = now_ns();
        check_result(c, r, spec.expected);
        if (r.end_ns < last_end) ++c.fifo_inversions;
        last_end = r.end_ns;
        if (r.actor_id.rfind(expect_prefix, 0) != 0) c.lane_prefix_ok = false;
        if (i >= warmup) samples.push_back(static_cast<double>(t1 - t0));
    }
    lane.stop_and_join();
    summarize(c, samples);
    return c;
}

// Submit-all-then-drain: one sample per batch = span / batch_size (per-op
// figure with the worker wakeup amortized across the batch).
template <class Lane>
Cell run_lane_batch(const char* mode, const OpSpec& spec, int batches,
                    int warmup_batches, int batch_size,
                    const char* expect_prefix) {
    Cell c;
    c.mode = mode;
    c.op = spec.name;
    c.ops_total = (batches + warmup_batches) * batch_size;
    Lane lane;
    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(batches));
    for (int b = 0; b < batches + warmup_batches; ++b) {
        std::vector<hpx::future<rayx_runtime::RuntimeResult>> futs;
        futs.reserve(static_cast<std::size_t>(batch_size));
        const std::int64_t t0 = now_ns();
        for (int k = 0; k < batch_size; ++k) {
            futs.push_back(lane.submit(
                make_probe_op_task(spec.fn, spec.args, lane.actor_id()),
                spec.checkpoint_count, nullptr));
        }
        std::int64_t last_end = 0;
        for (auto& fut : futs) {
            rayx_runtime::RuntimeResult r = fut.get();
            check_result(c, r, spec.expected);
            if (r.end_ns < last_end) ++c.fifo_inversions;
            last_end = r.end_ns;
            if (r.actor_id.rfind(expect_prefix, 0) != 0)
                c.lane_prefix_ok = false;
        }
        const std::int64_t t1 = now_ns();
        if (b >= warmup_batches)
            samples.push_back(static_cast<double>(t1 - t0) /
                              static_cast<double>(batch_size));
    }
    lane.stop_and_join();
    summarize(c, samples);
    return c;
}

// ---------------------------------------------------------------------------
// JSON emission (compact, stdout-only; stderr carries human progress).
// ---------------------------------------------------------------------------
std::string cell_json(const Cell& c) {
    char buf[640];
    std::snprintf(
        buf, sizeof buf,
        "{\"mode\":\"%s\",\"op\":\"%s\",\"n\":%d,\"ops_total\":%d,"
        "\"min_ns\":%.1f,\"p50_ns\":%.1f,\"p90_ns\":%.1f,\"p99_ns\":%.1f,"
        "\"max_ns\":%.1f,\"mean_ns\":%.1f,"
        "\"all_completed\":%s,\"values_ok\":%s,"
        "\"fifo_inversions\":%d,\"lane_prefix_ok\":%s}",
        c.mode.c_str(), c.op.c_str(), c.n, c.ops_total,
        c.min_ns, c.p50_ns, c.p90_ns, c.p99_ns, c.max_ns, c.mean_ns,
        c.all_completed ? "true" : "false", c.values_ok ? "true" : "false",
        c.fifo_inversions, c.lane_prefix_ok ? "true" : "false");
    return std::string(buf);
}

struct Options {
    bool quick = false;
    int iters = 20000;
    int warmup = 1000;
    int batch_size = 64;
    int batches = 300;
    int warmup_batches = 10;
};

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next_int = [&](const char* flag) {
            if (i + 1 >= argc) {
                std::fprintf(stderr,
                             "[rayx_runtime_dispatch_probe] %s needs a value\n",
                             flag);
                std::exit(2);
            }
            return std::atoi(argv[++i]);
        };
        if (a == "--quick") opt.quick = true;
        else if (a == "--iters") opt.iters = next_int("--iters");
        else if (a == "--warmup") opt.warmup = next_int("--warmup");
        else if (a == "--batch-size") opt.batch_size = next_int("--batch-size");
        else if (a == "--batches") opt.batches = next_int("--batches");
        else {
            std::fprintf(stderr,
                         "[rayx_runtime_dispatch_probe] unknown flag: %s\n",
                         a.c_str());
            return 2;
        }
    }
    if (opt.quick) {
        opt.iters = 2000;
        opt.warmup = 200;
        opt.batches = 30;
        opt.warmup_batches = 3;
    }

    const std::size_t hpx_threads = hpx::get_os_thread_count();
    std::fprintf(stderr,
                 "[rayx_runtime_dispatch_probe] hpx_threads=%zu quick=%d "
                 "iters=%d warmup=%d batch_size=%d batches=%d (structural "
                 "gates only; ALL timing observation-only)\n",
                 hpx_threads, opt.quick ? 1 : 0, opt.iters, opt.warmup,
                 opt.batch_size, opt.batches);

    const std::vector<OpSpec> specs = make_op_specs();
    std::vector<Cell> cells;
    for (const OpSpec& s : specs) {
        cells.push_back(run_primitive(/*nested=*/false, s, opt.iters,
                                      opt.warmup));
        cells.push_back(run_primitive(/*nested=*/true, s, opt.iters,
                                      opt.warmup));
        cells.push_back(run_lane_seq<rayx_runtime::RuntimeLane>(
            "lane_nested_seq", s, opt.iters, opt.warmup, "rt-hpx-"));
        cells.push_back(run_lane_seq<InlineDispatchProbeLane>(
            "lane_inline_seq", s, opt.iters, opt.warmup, "rt-prbi-"));
        cells.push_back(run_lane_batch<rayx_runtime::RuntimeLane>(
            "lane_nested_batch", s, opt.batches, opt.warmup_batches,
            opt.batch_size, "rt-hpx-"));
        cells.push_back(run_lane_batch<InlineDispatchProbeLane>(
            "lane_inline_batch", s, opt.batches, opt.warmup_batches,
            opt.batch_size, "rt-prbi-"));
        std::fprintf(stderr, "[rayx_runtime_dispatch_probe] op=%s done\n",
                     s.name.c_str());
    }

    bool gates_ok = true;
    for (const Cell& c : cells) {
        if (!c.all_completed || !c.values_ok || c.fifo_inversions != 0 ||
            !c.lane_prefix_ok) {
            gates_ok = false;
            std::fprintf(stderr,
                         "[rayx_runtime_dispatch_probe] GATE FAIL %s/%s "
                         "(completed=%d values=%d inversions=%d prefix=%d)\n",
                         c.mode.c_str(), c.op.c_str(), c.all_completed,
                         c.values_ok, c.fifo_inversions, c.lane_prefix_ok);
        }
    }

    std::string out =
        "{\"probe\":\"rayx_runtime_dispatch_probe\","
        "\"schema\":\"rayx-runtime-dispatch-probe-1\","
        "\"hpx_threads\":" + std::to_string(hpx_threads) +
        ",\"num_lanes\":1,\"quick\":" + (opt.quick ? "true" : "false") +
        ",\"iters\":" + std::to_string(opt.iters) +
        ",\"warmup\":" + std::to_string(opt.warmup) +
        ",\"batch_size\":" + std::to_string(opt.batch_size) +
        ",\"batches\":" + std::to_string(opt.batches) +
        ",\"all_structural_gates_passed\":" + (gates_ok ? "true" : "false") +
        ",\"cells\":[";
    for (std::size_t i = 0; i < cells.size(); ++i) {
        if (i) out += ",";
        out += cell_json(cells[i]);
    }
    out += "]}";
    std::printf("%s\n", out.c_str());
    std::fprintf(stderr, "[rayx_runtime_dispatch_probe] gates=%s cells=%zu\n",
                 gates_ok ? "PASS" : "FAIL", cells.size());
    return gates_ok ? 0 : 1;
}
