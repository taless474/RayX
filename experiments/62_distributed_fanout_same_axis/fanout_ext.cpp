// exp62 fanout_ext -- EXPERIMENT-ONLY pybind11 module that embeds an HPX runtime in the Python
// process. It supports two fanout/fanin paths over the SAME closed-int64 oracle:
//
//   * SINGLE-LOCALITY (Slice 1): N ordinary LOCAL hpx::async leaves on find_here, composed with
//     hpx::when_all + reduce. No connector, no parcelport.
//
//   * ONE-REMOTE-LOCALITY (Slice 2): with the embedded runtime started as the AGAS ROOT (extra_args
//     carry the TCP networking flags), a standalone connector joins as a remote locality and the
//     root dispatches N exp62_leaf_action leaves to it:
//
//        Python caller -> pybind/HPX root -> N hpx::async<exp62_leaf_action>(remote_loc, x, i)
//                      -> bounded is_ready watchdog + root reduce -> (composite int64, leaf records,
//                         timed_out_leaf_count) -> Python
//
//     Default fanout placement is all-remote: the root/coordinator runs NO leaves. With one remote
//     locality, ALL N leaves execute on the connector. There is NO single-node fallback: the remote
//     path refuses unless a remote locality actually joined.
//
// Each leaf reports the runtime-observed hpx::get_locality_id(), so records are shaped for the Slice
// 0 witness: (i, value, locality). The composite is checked against the Slice 0 oracle by the Python
// runner's composite_oracle_correct gate (no redundant in-ext assert).
//
// The HPX embedding (hpx::start / run_as_hpx_thread / finalize+stop, GIL released around the
// blocking call) mirrors the proven exp61 pattern. This is NOT rayx.runtime, NOT _rayx, NO Ray. It
// is MECHANISM validation only: NO same-axis, speedup, ratio, or "HPX beats Ray" claim.
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/async_combinators/when_all.hpp>
#include <hpx/include/dataflow.hpp>
#include <hpx/futures/future.hpp>
#include <hpx/naming_base/id_type.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>  // gethostname

#include "fanout_action.hpp"  // defines + HPX_PLAIN_ACTION-registers exp62_leaf_action (ONE TU here)

namespace py = pybind11;

namespace {

std::atomic<bool> g_started{false};

// Cached remote localities. Slice 2/3 cache ONE (await_remote); Slice 4a caches >=2 (await_remotes).
// Resolved ONCE and reused for every leaf, so no per-call AGAS lookup is folded into the Python-timed
// boundary. Ordered deterministically by ascending locality id. Empty until a connector joins.
std::vector<hpx::id_type> g_remote_ids;
std::vector<std::uint32_t> g_remote_locs;
bool g_have_remote{false};

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

// Bring up the embedded HPX runtime on background threads (does not take over the Python main
// thread). GIL released across hpx::start, mirroring exp61 / the _rayx extension.
//
// extra_args (Slice 2) is appended verbatim to the HPX argv so the runner can pass two-node
// networking flags (e.g. --hpx:hpx=IP:PORT, --hpx:agas=IP:PORT, --hpx:expect-connecting-localities)
// to make this embedded runtime the AGAS ROOT a connector can join. With no extra args (the Slice 1
// smoke) the startup is the single-locality console runtime as before.
void ext_start(int hpx_threads, const std::vector<std::string>& extra_args) {
    if (g_started.load()) {
        return;
    }
    std::vector<std::vector<char>> argv_store;
    argv_store.push_back(to_cstr("exp62_fanout_ext"));
    argv_store.push_back(to_cstr("--hpx:threads=" + std::to_string(hpx_threads)));
    for (const std::string& a : extra_args) {
        argv_store.push_back(to_cstr(a));
    }
    std::vector<char*> argv;
    argv.reserve(argv_store.size() + 1);
    for (auto& v : argv_store) {
        argv.push_back(v.data());
    }
    argv.push_back(nullptr);
    int argc = static_cast<int>(argv_store.size());
    hpx::init_params params;
    py::gil_scoped_release release;
    if (!hpx::start(nullptr, argc, argv.data(), params)) {
        throw std::runtime_error("hpx::start failed");
    }
    g_started.store(true);
}

// Clean teardown: finalize then stop, GIL released. Mirrors exp61 / _rayx.
void ext_shutdown() {
    if (!g_started.load()) {
        return;
    }
    {
        py::gil_scoped_release release;
        hpx::post([]() { hpx::finalize(); });
        hpx::stop();
    }
    g_started.store(false);
    g_have_remote = false;
    g_remote_ids.clear();
    g_remote_locs.clear();
}

// Leaf record returned to Python: (i, value, locality). locality is the runtime hpx::get_locality_id.
using LeafTuple = std::tuple<std::int64_t, std::int64_t, std::uint32_t>;
// Composition provenance appended to every fanout result so the Python runner can gate on HOW the
// fan-in was composed (pre-Slice-4b spike): watchdog label, whether the closure ran on an HPX thread,
// whether the SUCCESS path used is_ready polling, and whether the composition is HPX-native (future
// continuation, no success-path poll). hpx_native == !polled_in_success_path by construction.
// (composite_value, leaves, composition_primitive, reduce_primitive, n_localities, inner_fanout_n,
//  watchdog, ran_on_hpx_thread, polled_in_success_path, hpx_native_composition)
using FanoutResult = std::tuple<std::int64_t, std::vector<LeafTuple>, std::string, std::string,
                                std::uint32_t, std::int64_t, std::string, bool, bool, bool>;
// Slice 2 remote result adds timed_out_leaf_count before the composition-provenance tail.
// (..., inner_fanout_n, timed_out_leaf_count, watchdog, ran_on_hpx_thread, polled_in_success_path,
//  hpx_native_composition)
using RemoteResult = std::tuple<std::int64_t, std::vector<LeafTuple>, std::string, std::string,
                                std::uint32_t, std::int64_t, std::int64_t, std::string, bool, bool,
                                bool>;

// Reduce ready LOCAL leaf futures (LeafTuple already carries i, value, loc). acc in uint64 so the
// wrap is well-defined and matches the Slice 0 Python oracle.
std::pair<std::int64_t, std::vector<LeafTuple>> reduce_local_leaves(
    std::vector<hpx::future<LeafTuple>>& ready) {
    std::vector<LeafTuple> leaves;
    leaves.reserve(ready.size());
    std::uint64_t acc = 0;
    for (auto& f : ready) {
        LeafTuple t = f.get();
        acc += static_cast<std::uint64_t>(std::get<1>(t));
        leaves.push_back(t);
    }
    return {static_cast<std::int64_t>(acc), std::move(leaves)};
}

// Reduce ready REMOTE leaf_record futures. when_all/dataflow preserve input order, so the vector
// index IS the leaf index i. acc in uint64 (order-independent mod 2^64).
std::pair<std::int64_t, std::vector<LeafTuple>> reduce_remote_leaves(
    std::vector<hpx::future<exp62::leaf_record>>& ready) {
    std::vector<LeafTuple> leaves;
    leaves.reserve(ready.size());
    std::uint64_t acc = 0;
    std::int64_t i = 0;
    for (auto& f : ready) {
        exp62::leaf_record lr = f.get();
        acc += static_cast<std::uint64_t>(lr.value);
        leaves.push_back(LeafTuple{i, lr.value, lr.locality});
        ++i;
    }
    return {static_cast<std::int64_t>(acc), std::move(leaves)};
}

// The blocking op the Python caller times. Dispatches N ordinary LOCAL async leaves and folds the
// closed-int64 sum on an HPX thread. GIL released around the whole blocking HPX section.
//
// mode selects the LOCAL composition (both HPX-native, no is_ready poll -- this local path exists so
// the native continuation can be exercised OFF-CLUSTER via hpx-local-smoke):
//   * "when_all_get"          -- hpx::when_all(futs).get() then reduce (original Slice 1 shape).
//   * "when_all_then_reduce"  -- hpx::when_all(futs).then(reduce) composed as a continuation, one
//                                final .get() at the boundary.
FanoutResult ext_fanout_fanin(std::int64_t x, std::int64_t n, const std::string& mode) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    if (n < 0) {
        throw std::runtime_error("n must be >= 0");
    }
    if (mode != "when_all_get" && mode != "when_all_then_reduce") {
        throw std::runtime_error("unknown local composition mode: " + mode);
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([x, n, mode]() -> FanoutResult {
        std::vector<hpx::future<LeafTuple>> futs;
        futs.reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i < n; ++i) {
            futs.push_back(hpx::async([x, i]() -> LeafTuple {
                std::uint32_t loc = hpx::get_locality_id();
                return LeafTuple{i, exp62::leaf_value(x, i), loc};
            }));
        }
        std::pair<std::int64_t, std::vector<LeafTuple>> pr;
        std::string prim;
        if (mode == "when_all_then_reduce") {
            // Continuation-based compose; single .get() at the end. No is_ready polling.
            auto composed = hpx::when_all(std::move(futs)).then(
                [](auto&& allf) -> std::pair<std::int64_t, std::vector<LeafTuple>> {
                    std::vector<hpx::future<LeafTuple>> ready = allf.get();
                    return reduce_local_leaves(ready);
                });
            pr = composed.get();
            prim = "when_all_then_reduce";
        } else {
            std::vector<hpx::future<LeafTuple>> ready = hpx::when_all(futs).get();
            pr = reduce_local_leaves(ready);
            prim = "hpx::when_all";
        }
        // Local paths always run on an HPX thread, never poll, and are HPX-native.
        return FanoutResult{pr.first, std::move(pr.second), prim,
                            std::string("local_fold_sum_int64"), static_cast<std::uint32_t>(1), n,
                            std::string("none_local"), true, false, true};
    });
}

std::uint32_t ext_local_locality_id() {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
}

// ---- Slice 2/3 (one remote) + Slice 4a (>=2 remotes): remote-locality fanout (root side) --------

// MUST run on an HPX thread. Poll find_all_localities() until at least expected_count remote (non-
// here) localities have joined, then cache ALL currently-joined remotes deterministically (ascending
// locality id) and return the number cached. On a single local node (no connector) the remote set is
// always empty -- there is no way to fabricate a remote. The cache is replaced on each call.
std::int64_t await_remotes_on_hpx_thread(int expected_count, int timeout_s) {
    const hpx::id_type here = hpx::find_here();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    for (;;) {
        std::vector<std::pair<std::uint32_t, hpx::id_type>> remotes;
        for (const auto& l : hpx::find_all_localities()) {
            if (l != here) {
                remotes.emplace_back(hpx::naming::get_locality_id_from_id(l), l);
            }
        }
        if (static_cast<int>(remotes.size()) >= expected_count
            || std::chrono::steady_clock::now() >= deadline) {
            std::sort(remotes.begin(), remotes.end(),
                      [](const auto& a, const auto& b) { return a.first < b.first; });
            g_remote_ids.clear();
            g_remote_locs.clear();
            for (const auto& r : remotes) {
                g_remote_locs.push_back(r.first);
                g_remote_ids.push_back(r.second);
            }
            g_have_remote = !g_remote_ids.empty();
            return static_cast<std::int64_t>(g_remote_ids.size());
        }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

// Slice 2/3 compat: wait for >=1 remote, cache it, return the first remote locality id (or -1).
std::int64_t ext_await_remote(int timeout_s) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([timeout_s]() -> std::int64_t {
        std::int64_t cnt = await_remotes_on_hpx_thread(1, timeout_s);
        return cnt >= 1 ? static_cast<std::int64_t>(g_remote_locs.front()) : -1;
    });
}

// Slice 4a: wait for >=expected_count remotes, cache ALL joined remotes deterministically, and return
// the number cached. The caller fails closed if the returned count < expected_count.
std::int64_t ext_await_remotes(int expected_count, int timeout_s) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    if (expected_count < 1) {
        expected_count = 1;
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([expected_count, timeout_s]() -> std::int64_t {
        return await_remotes_on_hpx_thread(expected_count, timeout_s);
    });
}

std::int64_t ext_remote_locality_id() {  // first cached remote loc id, or -1
    return g_remote_locs.empty() ? -1 : static_cast<std::int64_t>(g_remote_locs.front());
}

std::vector<std::int64_t> ext_remote_locality_ids() {  // all cached remote loc ids (ascending)
    std::vector<std::int64_t> out;
    out.reserve(g_remote_locs.size());
    for (std::uint32_t l : g_remote_locs) {
        out.push_back(static_cast<std::int64_t>(l));
    }
    return out;
}

std::string ext_hostname() {
    char buf[256];
    if (::gethostname(buf, sizeof(buf)) == 0) {
        buf[sizeof(buf) - 1] = '\0';
        return std::string(buf);
    }
    return std::string("unknown");
}

// The blocking op the Python caller times in the remote phase. Dispatches N leaf actions to the
// CACHED REMOTE locality (all-remote: the root runs none), then composes with a BOUNDED watchdog so
// a slow/crashed remote leaf fails the island cleanly instead of hanging the allocation:
//
//   * Each leaf is an independent hpx::async<exp62_leaf_action>(remote, x, i) future.
//   * Cross-node watchdog: use a bounded fine-grained readiness poll over the individual leaf
//     futures (50 us sleep/yield between checks). Do NOT use hpx::when_all(...).wait_for(...) here:
//     on the TCP parcelport path that aggregate future can sleep until the timeout even when the
//     leaves have completed. Polling the leaf futures directly is the Rostam-validated path.
//   * On success, all futures are ready and are reduced normally.
//   * On timeout, do NOT call get() on unready futures; collect only ready leaves, count unready
//     leaves in timed_out_leaf_count, and let the Python artifact/gates fail closed.
//
// Refuses if no remote was resolved -- a single-node run can never masquerade as a remote one.
RemoteResult ext_fanout_fanin_remote(std::int64_t x, std::int64_t n, double dispatch_timeout_s,
                                     const std::string& mode) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    if (!g_have_remote) {
        throw std::runtime_error("no remote locality resolved: call await_remote() and ensure a "
                                 "connector joined (this is NOT a single-node fallback)");
    }
    if (n < 0) {
        throw std::runtime_error("n must be >= 0");
    }
    if (mode != "root_flat_gather_poll" && mode != "when_all_then_reduce"
        && mode != "dataflow_reduce") {
        throw std::runtime_error("unknown remote composition mode: " + mode);
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([x, n, dispatch_timeout_s, mode]() -> RemoteResult {
        // all-remote, round-robin across the cached remote localities (Slice 4a: >=2 remotes;
        // Slice 2/3: exactly one). The root runs NO leaves.
        auto make_futs = [x, n]() {
            std::vector<hpx::future<exp62::leaf_record>> futs;
            futs.reserve(static_cast<std::size_t>(n));
            const std::size_t r = g_remote_ids.size();
            for (std::int64_t i = 0; i < n; ++i) {
                const hpx::id_type& target = g_remote_ids[static_cast<std::size_t>(i) % r];
                futs.push_back(hpx::async<exp62_leaf_action>(target, x, i));
            }
            return futs;
        };
        const std::uint32_t n_loc = static_cast<std::uint32_t>(g_remote_ids.size());

        // ---- HPX-native composition (pre-Slice-4b spike): future continuation, one final wait ----
        // Compose the N remote leaf futures with hpx::when_all(...).then(reduce) (or hpx::dataflow),
        // then bound the WHOLE composition with a SINGLE cooperative timed wait on the composed
        // future (future::wait_for) -- NOT a 50us is_ready poll loop. On success wait_for returns
        // ready promptly and we .get() once; on timeout we fail closed (timed_out=n) and never .get()
        // a pending future (that would block past the watchdog).
        if (mode == "when_all_then_reduce" || mode == "dataflow_reduce") {
            auto futs = make_futs();
            hpx::future<std::pair<std::int64_t, std::vector<LeafTuple>>> composed;
            if (mode == "dataflow_reduce") {
                composed = hpx::dataflow(
                    [](std::vector<hpx::future<exp62::leaf_record>> ready)
                        -> std::pair<std::int64_t, std::vector<LeafTuple>> {
                        return reduce_remote_leaves(ready);
                    },
                    std::move(futs));
            } else {
                composed = hpx::when_all(std::move(futs)).then(
                    [](auto&& allf) -> std::pair<std::int64_t, std::vector<LeafTuple>> {
                        std::vector<hpx::future<exp62::leaf_record>> ready = allf.get();
                        return reduce_remote_leaves(ready);
                    });
            }
            std::int64_t value = 0;
            std::int64_t timed_out = 0;
            std::vector<LeafTuple> leaves;
            if (composed.wait_for(std::chrono::duration<double>(dispatch_timeout_s))
                == hpx::future_status::ready) {
                auto pr = composed.get();
                value = pr.first;
                leaves = std::move(pr.second);
            } else {
                timed_out = n;  // fail closed; never .get() a pending composed future
            }
            return RemoteResult{value, std::move(leaves), mode,
                                std::string("root_fold_sum_int64"), n_loc, n, timed_out,
                                std::string("composed_future_wait_for"), true, false, true};
        }

        // ---- Interim path (default): bounded is_ready poll over the INDIVIDUAL leaf futures. ----
        // The Slice 3 when_all(...).wait_for() watchdog missed the leaf-completion wakeup on the
        // cross-node parcelport path and blocked the full dispatch timeout every call even though all
        // leaves completed correctly (exp62 Slice 3 job 158801). This poll path is the Rostam-proven
        // Slice 4a shape; the native modes above are the spike to retire it. Kept for provenance and
        // as a fallback. The 50us sleep_for yields this HPX thread so parcelport completion handlers
        // run and mark futures ready; the fine interval keeps quantization far below the old 2 ms.
        auto futs = make_futs();
        std::vector<LeafTuple> leaves;
        std::uint64_t acc = 0;
        std::int64_t timed_out = 0;
        const auto deadline = std::chrono::steady_clock::now()
            + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                  std::chrono::duration<double>(dispatch_timeout_s));
        for (;;) {
            bool all_ready = true;
            for (auto& f : futs) {
                if (!f.is_ready()) { all_ready = false; break; }
            }
            if (all_ready || std::chrono::steady_clock::now() >= deadline) {
                break;
            }
            hpx::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        // Collect only the leaves that are ready; an unready leaf at the deadline is a timed-out leaf
        // and is NEVER .get()'d. timed_out_leaf_count > 0 makes the Python no_dispatch_timeout gate
        // fail closed and yields a failed artifact path.
        leaves.reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i < n; ++i) {
            hpx::future<exp62::leaf_record>& f = futs[static_cast<std::size_t>(i)];
            if (f.is_ready()) {
                exp62::leaf_record lr = f.get();
                acc += static_cast<std::uint64_t>(lr.value);
                leaves.push_back(LeafTuple{i, lr.value, lr.locality});
            } else {
                ++timed_out;
            }
        }
        return RemoteResult{static_cast<std::int64_t>(acc), std::move(leaves),
                            std::string("root_flat_gather_reduce"),
                            std::string("root_fold_sum_int64"), n_loc, n, timed_out,
                            std::string("bounded_is_ready_poll_50us"), true, true, false};
    });
}

}  // namespace

PYBIND11_MODULE(fanout_ext, m) {
    m.doc() = "exp62 fanout/fanin HPX embedding (EXPERIMENT-ONLY). Slice 1 single-locality smoke + "
              "Slice 2 one-remote-locality all-remote fanout. Not rayx.runtime, not _rayx, no Ray.";
    m.def("start", &ext_start, py::arg("hpx_threads") = 4,
          py::arg("extra_args") = std::vector<std::string>{},
          "Start the embedded HPX runtime (idempotent). extra_args are appended to the HPX argv for "
          "two-node networking (e.g. --hpx:hpx=IP:PORT); empty = single-locality smoke.");
    m.def("shutdown", &ext_shutdown);
    m.def("fanout_fanin", &ext_fanout_fanin, py::arg("x"), py::arg("n"),
          py::arg("mode") = std::string("when_all_get"),
          "Slice 1: single-locality local fanout/fanin (find_here). mode selects the local "
          "composition: 'when_all_get' (default) or the native 'when_all_then_reduce'.");
    m.def("fanout_fanin_remote", &ext_fanout_fanin_remote, py::arg("x"), py::arg("n"),
          py::arg("dispatch_timeout_s") = 30.0,
          py::arg("mode") = std::string("root_flat_gather_poll"),
          "Slice 2/4a: all-remote fanout/fanin to the cached remote localities, bounded watchdog; "
          "raises if no remote was resolved. mode: 'root_flat_gather_poll' (default interim poll), "
          "'when_all_then_reduce' or 'dataflow_reduce' (HPX-native continuation, no success-path "
          "poll, single future::wait_for bound).");
    m.def("await_remote", &ext_await_remote, py::arg("timeout_s") = 60,
          "Slice 2/3: wait for >=1 connector to join, cache it; returns the first remote locality id "
          "or -1 if none joined (no single-node fallback).");
    m.def("await_remotes", &ext_await_remotes, py::arg("expected_count"), py::arg("timeout_s") = 60,
          "Slice 4a: wait for >=expected_count remote localities to join, cache ALL joined remotes "
          "(ascending locality id); returns the number cached. Caller fails closed if < expected.");
    m.def("remote_locality_id", &ext_remote_locality_id,
          "The first cached remote locality id (-1 if none resolved).");
    m.def("remote_locality_ids", &ext_remote_locality_ids,
          "All cached remote locality ids (ascending; empty if none resolved).");
    m.def("local_locality_id", &ext_local_locality_id);
    m.def("hostname", &ext_hostname, "This process's hostname (provenance only).");
    m.attr("__experiment__") = "exp62";
    m.attr("__action_registration_name__") = "exp62_leaf_action";
#ifdef EXP62_BUILD_TYPE
    m.attr("__build_type__") = EXP62_BUILD_TYPE;
#endif
}
