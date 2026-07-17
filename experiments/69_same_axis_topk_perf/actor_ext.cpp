// exp69 -- EXPERIMENT-ONLY pybind11 extension hosted INSIDE a Ray actor worker process.
//
// SAME-AXIS PERFORMANCE PROBE over the accepted exp68 workload (LLM-shaped SYNTHETIC vocabulary-
// sharded top-k; NOT real inference). Two Ray actors A and B each host a connect-mode HPX locality
// IN-PROCESS (hpx::start, NO child) -- the exp66/67/68 mechanism, reused unchanged. Both comparison
// arms live in the SAME actor processes and call the SAME native local_topk / merge_topk:
//   * HPX arm  -- coordinate(): dispatch exp69_local_topk_action at the PEER locality, merge inside
//                 an HPX future CONTINUATION (native path; the exp68 load-bearing step, unchanged).
//   * Ray arm  -- the Python actor method fetches the peer's candidates over a Ray actor handle as
//                 (token_id, logit_bits) tuples via local_topk_pairs(), then calls
//                 merge_topk_native() here, so ONLY transport + Python boundary handling differ.
//
// Identity discipline: hpx_version_info() is callable BEFORE start so the controller asserts the
// verified waiter-fix build identity before any measurement. Native timings are NEVER taken here:
// the ONLY timed boundary is the controller's perf_counter_ns around one ray.get. NOT production,
// NOT rayx.runtime API, no ratio/speedup/winner claim, no model weights/tokenizer/GPU.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/version.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/runtime_local/get_os_thread_count.hpp>       // Slice 3 diag: HPX worker-thread count
#include <hpx/runtime_local/get_worker_thread_num.hpp>     // Slice 3 diag: worker-thread identity

#include <unistd.h>

#include <atomic>
#include <chrono>    // Slice 3 diag: same-process monotonic stage timing (coordinator-local only)
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "topk_action.hpp"  // ONE TU per binary: registers exp69 topk/pid actions + reply type

namespace py = pybind11;

namespace {

std::atomic<bool> g_started{false};

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

// Slice 3 diag: nanoseconds between two SAME-PROCESS steady_clock reads (coordinator-local only).
using steady_tp = std::chrono::steady_clock::time_point;
long long ns_since(steady_tp a, steady_tp b) {
    return static_cast<long long>(std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count());
}

std::string this_host() {
    char buf[256] = {0};
    if (::gethostname(buf, sizeof(buf) - 1) == 0) return std::string(buf);
    return "unknown";
}

std::map<std::string, std::string> ext_hpx_version_info() {
    std::map<std::string, std::string> out;
    out["hpx_version_full"] = hpx::full_version_as_string();
    out["hpx_complete_version"] = hpx::complete_version();
    return out;
}

void ext_start_connect(int hpx_threads, const std::vector<std::string>& extra_args) {
    if (g_started.load()) {
        throw std::runtime_error("HPX already started in this process (one runtime per process)");
    }
    std::vector<std::vector<char>> argv_store;
    argv_store.push_back(to_cstr("exp69_actor_ext"));
    argv_store.push_back(to_cstr("--hpx:threads=" + std::to_string(hpx_threads)));
    for (const std::string& a : extra_args) argv_store.push_back(to_cstr(a));
    std::vector<char*> argv;
    argv.reserve(argv_store.size() + 1);
    for (auto& v : argv_store) argv.push_back(v.data());
    argv.push_back(nullptr);
    int argc = static_cast<int>(argv_store.size());
    hpx::init_params params;
    params.mode = hpx::runtime_mode::connect;
    py::gil_scoped_release release;
    if (!hpx::start(nullptr, argc, argv.data(), params)) {
        throw std::runtime_error("hpx::start (connect) failed");
    }
    g_started.store(true);
}

int ext_stop_disconnect() {
    if (!g_started.load()) return 0;
    int rc = 0;
    {
        py::gil_scoped_release release;
        hpx::post([]() { hpx::disconnect(); });
        rc = hpx::stop();
    }
    g_started.store(false);
    return rc;
}

std::int64_t ext_pid() { return static_cast<std::int64_t>(::getpid()); }
std::string ext_hostname() { return this_host(); }

std::uint32_t ext_locality_id() {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
}

std::size_t ext_membership_count() {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([]() { return hpx::find_all_localities().size(); });
}

// Expose the deterministic rule so the controller can cross-check C++ vs Python float32 bits.
std::uint32_t ext_logit_bits(std::int64_t token_id, std::int64_t seed) {
    return exp69::logit_bits(token_id, seed);
}

// MECHANISM WITNESS accessor: exp69 top-k actions served by THIS process (monotonic; read by the
// controller outside timed windows only). Ray arm requests must leave it unchanged.
long long ext_action_serve_count() {
    return exp69_action_serve_count.load(std::memory_order_relaxed);
}

// ACTIVE-CALL WITNESS accessors (Slice 2 throughput): peak/current CONCURRENTLY executing peer HPX
// top-k actions in this process. Read/reset by the controller BETWEEN batches only. Inert for QD1.
long long ext_hpx_action_active_max() {
    return exp69_hpx_action_active_max.load(std::memory_order_relaxed);
}
long long ext_hpx_action_active_current() {
    return exp69_hpx_action_active_current.load(std::memory_order_relaxed);
}
void ext_reset_hpx_action_active() { exp69_reset_hpx_action_active(); }

// Candidates as ordered (token_id, logit_bits) tuples -- the ONE result/transport representation
// both arms return (approved payload contract; no float reconstruction anywhere in transport).
py::list cands_to_pairs(const std::vector<exp69::Cand>& v) {
    py::list out;
    for (auto const& c : v) out.append(py::make_tuple(c.first, c.second));
    return out;
}

std::vector<exp69::Cand> pairs_to_cands(const py::sequence& seq) {
    std::vector<exp69::Cand> v;
    v.reserve(static_cast<std::size_t>(py::len(seq)));
    for (auto item : seq) {
        auto t = py::cast<py::sequence>(item);
        v.emplace_back(py::cast<std::int64_t>(t[0]),
                       static_cast<std::uint32_t>(py::cast<std::uint64_t>(t[1])));
    }
    return v;
}

// Own shard's local top-k as (token_id, logit_bits) pairs, computed in-process (pure; no HPX).
// The Ray arm's fetch endpoint AND its own-shard step -- the SAME exp69::local_topk the HPX arm
// runs (native_compute_mismatch is gated on this).
py::list ext_local_topk_pairs(std::int64_t lo, std::int64_t hi, std::int64_t seed, int k) {
    std::vector<exp69::Cand> v;
    {
        py::gil_scoped_release release;
        v = exp69::local_topk(lo, hi, seed, k);
    }
    return cands_to_pairs(v);
}

// The Ray arm's merge: the SAME exp69::merge_topk the HPX continuation runs, entered from Python
// pairs (native_merge_mismatch is gated on this). No Python merge exists anywhere in exp69.
py::list ext_merge_topk_native(const py::sequence& own, const py::sequence& peer, int k) {
    std::vector<exp69::Cand> a = pairs_to_cands(own);
    std::vector<exp69::Cand> b = pairs_to_cands(peer);
    std::vector<exp69::Cand> merged;
    {
        py::gil_scoped_release release;
        merged = exp69::merge_topk(std::move(a), b, k);
    }
    return cands_to_pairs(merged);
}

struct Flags {
    std::atomic<bool> dispatched{false}, future_ready{false},
        continuation_executed{false}, result_delivered{false};
};

// The HPX arm: coordinate a distributed top-k with the PEER locality over HPX and merge through a
// future CONTINUATION (exp68's load-bearing composition, same action/future/.then witnesses).
//
// WAIT STRATEGY (differs from exp68 -- sustained-volume hardening): exp68 bounded the composition
// with an HPX timed wait (future::wait_for) on a run_as_hpx_thread trampoline, at ~14 calls/rep.
// At exp69 volume (hundreds of sequential calls) that exact strategy crashes the hosting process
// (SIGILL/SIGSEGV) on this build/platform; the Ray-free diagnostic (coordinate_diag) isolates the
// crash to the HPX timed wait -- untimed continuation waits are stable at 2000 and 5000 iterations.
// Production path here: hpx::post the composition onto the runtime, wait UNTIMED inside HPX
// (merged.get()), and enforce the caller bound with std::future::wait_for on the calling OS
// thread (GIL released). Bounded caller, no HPX timed suspension, no per-call trampoline. On
// timeout the caller returns not-ready; the posted thread finishes in the background (scalars are
// captured by value so nothing dangles).
//
// include_locals=false keeps the timed return payload to the logical result + witnesses so BOTH
// arms return the same fields; the untimed pre-gate passes include_locals=true for candidate lists.
struct CoordR {
    bool found = false, ready = false;
    std::vector<exp69::Cand> global, own_topk, peer_topk;
    std::int64_t peer_pid = -1;
    std::uint32_t peer_loc = 0xFFFFFFFFu;
    std::string peer_host;
    std::int64_t own_pid = -1;
    std::uint32_t own_loc = 0xFFFFFFFFu;
    std::string own_host;
    bool f_dispatched = false, f_future_ready = false,
         f_continuation = false, f_delivered = false;
};

py::dict ext_coordinate(std::uint32_t peer_loc, std::int64_t own_lo, std::int64_t own_hi,
                        std::int64_t peer_lo, std::int64_t peer_hi, std::int64_t seed, int k,
                        int bound_s, bool include_locals) {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    CoordR r;
    {
        py::gil_scoped_release release;
        auto prom = std::make_shared<std::promise<CoordR>>();
        auto fut = prom->get_future();
        hpx::post([=]() {  // scalars by value: the bounded caller may return first
            CoordR q;
            try {
                q.own_pid = static_cast<std::int64_t>(::getpid());
                q.own_loc = hpx::get_locality_id();
                q.own_host = this_host();
                q.own_topk = exp69::local_topk(own_lo, own_hi, seed, k);

                hpx::id_type target = hpx::invalid_id;
                for (auto const& id : hpx::find_all_localities()) {
                    if (hpx::naming::get_locality_id_from_id(id) == peer_loc) target = id;
                }
                if (target == hpx::invalid_id) {
                    prom->set_value(std::move(q));
                    return;
                }
                q.found = true;

                auto flags = std::make_shared<Flags>();
                auto own_copy = q.own_topk;
                // Dispatch the peer's local top-k over HPX, then MERGE inside a .then continuation
                // that runs on an HPX worker thread when the fetch future becomes ready.
                auto f = hpx::async<exp69_local_topk_action>(target, peer_lo, peer_hi, seed,
                                                             static_cast<std::int64_t>(k));
                flags->dispatched.store(f.valid());
                auto merged = f.then(
                    [own_copy, k, flags](hpx::future<exp69_topk_reply> ff) -> std::tuple<
                        std::vector<exp69::Cand>, exp69_topk_reply> {
                        flags->future_ready.store(ff.is_ready());
                        exp69_topk_reply reply = ff.get();
                        flags->continuation_executed.store(true);
                        auto global = exp69::merge_topk(own_copy, reply.cands, k);
                        return std::make_tuple(std::move(global), std::move(reply));
                    });
                auto out = merged.get();  // UNTIMED HPX suspension (stable at volume; see above)
                flags->result_delivered.store(true);
                q.ready = true;
                q.global = std::get<0>(out);
                exp69_topk_reply const& reply = std::get<1>(out);
                q.peer_topk = reply.cands;
                q.peer_pid = reply.pid;
                q.peer_loc = reply.locality;
                q.peer_host = reply.host;
                q.f_dispatched = flags->dispatched.load();
                q.f_future_ready = flags->future_ready.load();
                q.f_continuation = flags->continuation_executed.load();
                q.f_delivered = flags->result_delivered.load();
            } catch (...) {
                q.ready = false;  // reported not-ready; verification classifies the sample
            }
            prom->set_value(std::move(q));
        });
        // Caller bound at the std boundary (OS thread, GIL released): bounded; never hang.
        if (fut.wait_for(std::chrono::seconds(bound_s)) == std::future_status::ready) {
            r = fut.get();
        }
    }
    py::dict d;
    d["target_found"] = r.found;
    d["ready"] = r.ready;
    d["global_topk"] = cands_to_pairs(r.global);
    if (include_locals) {
        d["own_topk"] = cands_to_pairs(r.own_topk);
        d["peer_topk"] = cands_to_pairs(r.peer_topk);
    }
    d["peer_pid"] = r.peer_pid;
    d["peer_locality"] = r.peer_loc;
    d["peer_host"] = r.peer_host;
    d["own_pid"] = r.own_pid;
    d["own_locality"] = r.own_loc;
    d["own_host"] = r.own_host;
    py::dict comp;
    comp["action_dispatched"] = r.f_dispatched;
    comp["future_ready"] = r.f_future_ready;
    comp["continuation_executed"] = r.f_continuation;
    comp["result_delivered"] = r.f_delivered;
    d["hpx_composition"] = comp;
    return d;
}

// DIAGNOSTIC ONLY -- sustained-volume crash isolation for the HPX-arm compose path. NOT an arm,
// never called by ray_coordinate/hpx_coordinate, carries no timing meaning. Variants:
//   "run_waitfor" -- run_as_hpx_thread + hpx timed wait (mirrors ext_coordinate's strategy)
//   "run_get"     -- run_as_hpx_thread + UNTIMED merged.get() (isolates the hpx timed wait)
//   "post_get"    -- hpx::post + untimed merged.get() on the HPX thread; boundedness enforced by
//                    std::future::wait_for on the calling OS thread (no hpx timed suspension,
//                    no per-call external-thread trampoline)
py::dict ext_coordinate_diag(const std::string& mode, std::uint32_t peer_loc, std::int64_t own_lo,
                             std::int64_t own_hi, std::int64_t peer_lo, std::int64_t peer_hi,
                             std::int64_t seed, int k, int bound_s) {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    struct R {
        bool found = false, ready = false;
        std::vector<exp69::Cand> global;
        std::int64_t peer_pid = -1;
        bool f_continuation = false;
    };
    R r;
    // Capture scalars BY VALUE: the post_get timeout path lets the caller return while the posted
    // thread is still running, so the body must never reference the caller's frame.
    auto body = [=](auto&& waiter) -> R {
        R q;
        q.found = false;
        std::vector<exp69::Cand> own = exp69::local_topk(own_lo, own_hi, seed, k);
        hpx::id_type target = hpx::invalid_id;
        for (auto const& id : hpx::find_all_localities()) {
            if (hpx::naming::get_locality_id_from_id(id) == peer_loc) target = id;
        }
        if (target == hpx::invalid_id) return q;
        q.found = true;
        auto flags = std::make_shared<Flags>();
        auto f = hpx::async<exp69_local_topk_action>(target, peer_lo, peer_hi, seed,
                                                     static_cast<std::int64_t>(k));
        auto merged = f.then(
            [own, k, flags](hpx::future<exp69_topk_reply> ff)
                -> std::tuple<std::vector<exp69::Cand>, exp69_topk_reply> {
                exp69_topk_reply reply = ff.get();
                flags->continuation_executed.store(true);
                return std::make_tuple(exp69::merge_topk(own, reply.cands, k), std::move(reply));
            });
        if (!waiter(merged)) return q;  // bounded (strategy-specific); never hang
        auto out = merged.get();
        q.ready = true;
        q.global = std::get<0>(out);
        q.peer_pid = std::get<1>(out).pid;
        q.f_continuation = flags->continuation_executed.load();
        return q;
    };
    {
        py::gil_scoped_release release;
        if (mode == "run_waitfor") {
            r = hpx::run_as_hpx_thread([&]() -> R {
                return body([&](auto& fut) {
                    return fut.wait_for(std::chrono::seconds(bound_s))
                           == hpx::future_status::ready;
                });
            });
        } else if (mode == "run_get") {
            r = hpx::run_as_hpx_thread([&]() -> R {
                return body([](auto& fut) { fut.wait(); return true; });
            });
        } else if (mode == "post_get") {
            auto prom = std::make_shared<std::promise<R>>();
            auto fut = prom->get_future();
            hpx::post([body, prom]() {
                R q;
                try {
                    q = body([](auto& f2) { f2.wait(); return true; });
                } catch (...) {
                    q = R{};
                }
                prom->set_value(std::move(q));
            });
            if (fut.wait_for(std::chrono::seconds(bound_s)) != std::future_status::ready) {
                r = R{};  // bounded at the std boundary; posted thread finishes in background
            } else {
                r = fut.get();
            }
        } else {
            throw std::runtime_error("unknown diag mode: " + mode);
        }
    }
    py::dict d;
    d["mode"] = mode;
    d["target_found"] = r.found;
    d["ready"] = r.ready;
    d["global_topk"] = cands_to_pairs(r.global);
    d["peer_pid"] = r.peer_pid;
    d["continuation_executed"] = r.f_continuation;
    return d;
}

// Slice 3 (exp69) DIAGNOSTIC-ONLY timed HPX-arm coordinate. It MIRRORS ext_coordinate exactly --
// SAME own->dispatch->wait->merge ordering, SAME wait strategy (hpx::post + UNTIMED merged.get() on
// the HPX thread, bounded by std::future::wait_for on the calling OS thread) -- and additionally
// records SAME-PROCESS monotonic stage durations, coordinator worker-thread identities, and (via
// the timed peer action) PEER-LOCAL durations. Peer-local values are attribution-only and are NEVER
// subtracted from coordinator timestamps. The accepted ext_coordinate is untouched; this never runs
// on the accepted Slice 1/2 timed path. No timing here licenses any ratio/speedup/winner claim.
struct CoordTimedR {
    bool found = false, ready = false;
    std::vector<exp69::Cand> global, own_topk, peer_topk;
    std::int64_t peer_pid = -1;
    std::uint32_t peer_loc = 0xFFFFFFFFu;
    std::string peer_host;
    std::int64_t own_pid = -1;
    std::uint32_t own_loc = 0xFFFFFFFFu;
    std::string own_host;
    bool f_dispatched = false, f_future_ready = false, f_continuation = false, f_delivered = false;
    // Coordinator-local monotonic stage durations (ns); -1 == not reached this call.
    long long entry_to_post_ns = -1, post_to_start_ns = -1, own_topk_ns = -1, dispatch_ns = -1,
              dispatch_to_cont_ns = -1, reply_get_ns = -1, merge_ns = -1, cont_to_delivered_ns = -1,
              total_native_ns = -1;
    // Worker-thread identity: own-work worker vs continuation worker (same pool of hpx_worker_count).
    long long own_worker = -1, cont_worker = -1, hpx_worker_count = -1;
    bool same_worker = false;
    // PEER-LOCAL durations carried in the timed reply (NOT subtracted from coordinator clock).
    long long peer_local_topk_ns = -1, peer_action_body_ns = -1;
};

// Filled inside the .then continuation (a distinct HPX thread) and read after merged.get() returns.
struct ContStageTimed {
    long long dispatch_to_cont_ns = -1, reply_get_ns = -1, merge_ns = -1, cont_worker = -1;
    long long peer_local_topk_ns = -1, peer_action_body_ns = -1;
    std::int64_t peer_pid = -1;
    std::uint32_t peer_loc = 0xFFFFFFFFu;
    std::string peer_host;
    std::vector<exp69::Cand> peer_topk;
    steady_tp merge_end{};
    bool future_ready = false, continuation_executed = false;
};

py::dict ext_coordinate_diag_timed(std::uint32_t peer_loc, std::int64_t own_lo, std::int64_t own_hi,
                                   std::int64_t peer_lo, std::int64_t peer_hi, std::int64_t seed,
                                   int k, int bound_s, bool include_locals) {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    CoordTimedR r;
    {
        py::gil_scoped_release release;
        auto t_entry = std::chrono::steady_clock::now();
        auto prom = std::make_shared<std::promise<CoordTimedR>>();
        auto fut = prom->get_future();
        auto t_post = std::chrono::steady_clock::now();  // captured BEFORE post (no race on read-back)
        hpx::post([=]() {  // scalars + t_post by value: the bounded caller may return first
            auto body_start = std::chrono::steady_clock::now();
            CoordTimedR q;
            q.entry_to_post_ns = ns_since(t_entry, t_post);
            q.post_to_start_ns = ns_since(t_post, body_start);
            q.hpx_worker_count = static_cast<long long>(hpx::get_os_thread_count());
            try {
                q.own_pid = static_cast<std::int64_t>(::getpid());
                q.own_loc = hpx::get_locality_id();
                q.own_host = this_host();
                q.own_worker = static_cast<long long>(hpx::get_worker_thread_num());
                auto o0 = std::chrono::steady_clock::now();
                q.own_topk = exp69::local_topk(own_lo, own_hi, seed, k);
                q.own_topk_ns = ns_since(o0, std::chrono::steady_clock::now());

                hpx::id_type target = hpx::invalid_id;
                for (auto const& id : hpx::find_all_localities()) {
                    if (hpx::naming::get_locality_id_from_id(id) == peer_loc) target = id;
                }
                if (target == hpx::invalid_id) {
                    prom->set_value(std::move(q));
                    return;
                }
                q.found = true;

                auto flags = std::make_shared<Flags>();
                auto cs = std::make_shared<ContStageTimed>();
                auto own_copy = q.own_topk;
                auto d0 = std::chrono::steady_clock::now();
                auto f = hpx::async<exp69_local_topk_timed_action>(target, peer_lo, peer_hi, seed,
                                                                   static_cast<std::int64_t>(k));
                auto d1 = std::chrono::steady_clock::now();
                q.dispatch_ns = ns_since(d0, d1);
                flags->dispatched.store(f.valid());
                auto merged = f.then(
                    [own_copy, k, flags, cs, d1](hpx::future<exp69_topk_reply_timed> ff)
                        -> std::vector<exp69::Cand> {
                        auto c_entry = std::chrono::steady_clock::now();
                        // dispatch->continuation: coordinator-observed peer round trip (composite;
                        // NOT peer-local, NOT pure transport).
                        cs->dispatch_to_cont_ns = ns_since(d1, c_entry);
                        cs->future_ready = ff.is_ready();
                        flags->future_ready.store(ff.is_ready());
                        cs->cont_worker = static_cast<long long>(hpx::get_worker_thread_num());
                        auto g0 = std::chrono::steady_clock::now();
                        exp69_topk_reply_timed reply = ff.get();
                        cs->reply_get_ns = ns_since(g0, std::chrono::steady_clock::now());
                        flags->continuation_executed.store(true);
                        cs->continuation_executed = true;
                        auto m0 = std::chrono::steady_clock::now();
                        auto global = exp69::merge_topk(own_copy, reply.cands, k);
                        cs->merge_end = std::chrono::steady_clock::now();
                        cs->merge_ns = ns_since(m0, cs->merge_end);
                        cs->peer_topk = reply.cands;
                        cs->peer_pid = reply.pid;
                        cs->peer_loc = reply.locality;
                        cs->peer_host = reply.host;
                        cs->peer_local_topk_ns = reply.peer_local_topk_ns;
                        cs->peer_action_body_ns = reply.peer_action_body_ns;
                        return global;
                    });
                auto out = merged.get();  // UNTIMED HPX suspension (mirrors ext_coordinate exactly)
                auto delivered = std::chrono::steady_clock::now();
                flags->result_delivered.store(true);
                q.ready = true;
                q.global = std::move(out);
                q.peer_topk = cs->peer_topk;
                q.peer_pid = cs->peer_pid;
                q.peer_loc = cs->peer_loc;
                q.peer_host = cs->peer_host;
                q.dispatch_to_cont_ns = cs->dispatch_to_cont_ns;
                q.reply_get_ns = cs->reply_get_ns;
                q.merge_ns = cs->merge_ns;
                q.cont_worker = cs->cont_worker;
                q.same_worker = (q.own_worker >= 0 && q.own_worker == q.cont_worker);
                q.cont_to_delivered_ns = ns_since(cs->merge_end, delivered);
                q.peer_local_topk_ns = cs->peer_local_topk_ns;
                q.peer_action_body_ns = cs->peer_action_body_ns;
                q.f_dispatched = flags->dispatched.load();
                q.f_future_ready = flags->future_ready.load();
                q.f_continuation = flags->continuation_executed.load();
                q.f_delivered = flags->result_delivered.load();
                q.total_native_ns = ns_since(body_start, delivered);
            } catch (...) {
                q.ready = false;  // reported not-ready; verification classifies the sample
            }
            prom->set_value(std::move(q));
        });
        // Caller bound at the std boundary (OS thread, GIL released): bounded; never hang.
        if (fut.wait_for(std::chrono::seconds(bound_s)) == std::future_status::ready) {
            r = fut.get();
        }
    }
    py::dict d;
    d["target_found"] = r.found;
    d["ready"] = r.ready;
    d["global_topk"] = cands_to_pairs(r.global);
    if (include_locals) {
        d["own_topk"] = cands_to_pairs(r.own_topk);
        d["peer_topk"] = cands_to_pairs(r.peer_topk);
    }
    d["peer_pid"] = r.peer_pid;
    d["peer_locality"] = r.peer_loc;
    d["peer_host"] = r.peer_host;
    d["own_pid"] = r.own_pid;
    d["own_locality"] = r.own_loc;
    d["own_host"] = r.own_host;
    py::dict comp;
    comp["action_dispatched"] = r.f_dispatched;
    comp["future_ready"] = r.f_future_ready;
    comp["continuation_executed"] = r.f_continuation;
    comp["result_delivered"] = r.f_delivered;
    d["hpx_composition"] = comp;
    // Coordinator-local monotonic stage decomposition (ns). Same-process deltas only.
    py::dict st;
    st["entry_to_post_ns"] = r.entry_to_post_ns;
    st["post_to_start_ns"] = r.post_to_start_ns;         // posted-task queue delay (worker starvation)
    st["own_topk_ns"] = r.own_topk_ns;
    st["dispatch_ns"] = r.dispatch_ns;
    st["dispatch_to_cont_ns"] = r.dispatch_to_cont_ns;   // coordinator-observed peer round trip
    st["reply_get_ns"] = r.reply_get_ns;
    st["merge_ns"] = r.merge_ns;
    st["cont_to_delivered_ns"] = r.cont_to_delivered_ns;
    st["total_native_ns"] = r.total_native_ns;
    d["stage_ns"] = st;
    py::dict wk;
    wk["own_worker"] = r.own_worker;
    wk["cont_worker"] = r.cont_worker;
    wk["same_worker"] = r.same_worker;
    wk["hpx_worker_count"] = r.hpx_worker_count;
    d["workers"] = wk;
    py::dict pl;   // PEER-LOCAL (never subtracted from coordinator clock)
    pl["peer_local_topk_ns"] = r.peer_local_topk_ns;
    pl["peer_action_body_ns"] = r.peer_action_body_ns;
    pl["subtracted_from_coordinator"] = false;
    d["peer_local"] = pl;
    return d;
}

}  // namespace

PYBIND11_MODULE(exp69_actor_ext, m) {
    m.doc() = "exp69 EXPERIMENT-ONLY in-Ray-actor connect-mode HPX host + same-axis sharded top-k "
              "arms (LLM-shaped synthetic, not inference; not rayx.runtime API; no ratio claim)";
    m.def("hpx_version_info", &ext_hpx_version_info,
          "Runtime-observed HPX build identity (callable BEFORE start; identity gate input).");
    m.def("start_connect", &ext_start_connect, py::arg("hpx_threads"), py::arg("extra_args"));
    m.def("stop_disconnect", &ext_stop_disconnect, "Graceful leave: post(disconnect) + stop.");
    m.def("pid", &ext_pid);
    m.def("hostname", &ext_hostname);
    m.def("locality_id", &ext_locality_id);
    m.def("membership_count", &ext_membership_count);
    m.def("logit_bits", &ext_logit_bits, py::arg("token_id"), py::arg("seed"),
          "Deterministic float32 logit bit-pattern for (token_id, seed) -- cross-language check.");
    m.def("action_serve_count", &ext_action_serve_count,
          "Monotonic exp69 top-k actions served by this process (witness; read outside timing).");
    m.def("hpx_action_active_max", &ext_hpx_action_active_max,
          "Slice 2: peak CONCURRENTLY executing peer HPX top-k actions since last reset (witness).");
    m.def("hpx_action_active_current", &ext_hpx_action_active_current,
          "Slice 2: currently executing peer HPX top-k actions (witness; read between batches).");
    m.def("reset_hpx_action_active", &ext_reset_hpx_action_active,
          "Slice 2: reset the peer-HPX-action active MAX to current in-flight (BETWEEN batches only).");
    m.def("local_topk_pairs", &ext_local_topk_pairs, py::arg("lo"), py::arg("hi"), py::arg("seed"),
          py::arg("k"),
          "Own-shard local top-k over [lo,hi) as ordered (token_id, logit_bits) tuples (pure C++).");
    m.def("merge_topk_native", &ext_merge_topk_native, py::arg("own"), py::arg("peer"), py::arg("k"),
          "Merge two (token_id, logit_bits) pair lists with the SAME native merge the HPX "
          "continuation uses; returns the global top-k pairs (Ray-arm merge entry point).");
    m.def("coordinate", &ext_coordinate, py::arg("peer_loc"), py::arg("own_lo"), py::arg("own_hi"),
          py::arg("peer_lo"), py::arg("peer_hi"), py::arg("seed"), py::arg("k"),
          py::arg("bound_s") = 30, py::arg("include_locals") = false,
          "HPX arm: fetch the peer shard's local top-k over an HPX action and merge via a future "
          "continuation; returns global top-k pairs + witnesses + composition evidence.");
    m.def("coordinate_diag", &ext_coordinate_diag, py::arg("mode"), py::arg("peer_loc"),
          py::arg("own_lo"), py::arg("own_hi"), py::arg("peer_lo"), py::arg("peer_hi"),
          py::arg("seed"), py::arg("k"), py::arg("bound_s") = 30,
          "DIAGNOSTIC ONLY: wait-strategy variants for crash isolation; never an arm, no timing "
          "meaning.");
    m.def("coordinate_diag_timed", &ext_coordinate_diag_timed, py::arg("peer_loc"),
          py::arg("own_lo"), py::arg("own_hi"), py::arg("peer_lo"), py::arg("peer_hi"),
          py::arg("seed"), py::arg("k"), py::arg("bound_s") = 30, py::arg("include_locals") = false,
          "Slice 3 DIAGNOSTIC ONLY: mirrors coordinate() (same ordering + wait strategy) and returns "
          "the same result/witnesses PLUS coordinator-local monotonic stage_ns, worker identities, "
          "and peer-local durations (never subtracted). No ratio/speedup claim.");
    m.attr("__experiment__") = "exp69";
    m.attr("__gil_declaration__") = "gil_used_default";
}
