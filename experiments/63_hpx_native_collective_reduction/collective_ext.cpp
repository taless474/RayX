// exp63 collective_ext -- EXPERIMENT-ONLY pybind11 module that embeds an HPX runtime in the Python
// process for the Slice 1b HPX PROGRESS ROOT-CAUSE micro-slice.
//
// COPIED/RENAMED from exp62 fanout_ext.cpp (exp62 -> exp63, module fanout_ext -> collective_ext,
// exp62_leaf_action -> exp63_leaf_action). Self-contained under exp63; includes NO exp62 header.
//
// It reproduces the exp62 job-158814 failure shape at the SAME closed-int64 oracle and lets the
// Python runner sweep runtime/composition config to ask ONE question: can a PASSIVE HPX future wait
// (when_all(...).then(reduce) bounded by a single future::wait_for) wake cross-node WITHOUT a
// success-path is_ready poll? The proven root_flat_gather_poll path is kept as the KNOWN-GOOD
// CONTROL, never relabeled native.
//
//   Python caller -> pybind/HPX root -> N hpx::async<exp63_leaf_action>(remote, x, i)
//                 -> composition per mode (root_flat_gather_poll | when_all_then_reduce |
//                    dataflow_reduce) with a bounded watchdog -> (composite int64, leaf records,
//                    timed_out_leaf_count, provenance) -> Python
//
// SLICE 1b ADDITIONS over exp62:
//   * background_yielder flag on fanout_fanin_remote: for the native passive-wait modes, spawn a
//     low-cost yielding HPX task that pumps the scheduler while the composed future is waited on --
//     the diagnostic probe for whether background parcel progress is the missing wakeup.
//   * hpx_config_provenance(): reports selected HPX config entries (parcel coalescing / array
//     optimization / TCP parcelport / background keys) via hpx::get_config_entry, "unknown" if
//     absent -- never fabricated.
//
// The HPX embedding (hpx::start / run_as_hpx_thread / finalize+stop, GIL released) mirrors the proven
// exp61/exp62 pattern. This is NOT rayx.runtime, NOT _rayx, NO Ray. MECHANISM/PROGRESS validation
// only: NO same-axis, speedup, ratio, or "HPX beats Ray" claim.
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <map>
#include <stdexcept>
#include <string>
#include <system_error>
#include <tuple>
#include <typeinfo>
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

#include "collective_action.hpp"  // defines + HPX_PLAIN_ACTION-registers exp63_leaf_action (ONE TU)

namespace py = pybind11;

namespace {

std::atomic<bool> g_started{false};

// Cached remote localities (>=1; Slice 1b uses >=2). Resolved ONCE and reused for every leaf so no
// per-call AGAS lookup is folded into the Python-timed boundary. Ordered by ascending locality id.
std::vector<hpx::id_type> g_remote_ids;
std::vector<std::uint32_t> g_remote_locs;
bool g_have_remote{false};

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

// Bring up the embedded HPX runtime on background threads. GIL released across hpx::start.
// extra_args is appended verbatim to the HPX argv so the runner can pass root networking + config
// flags (--hpx:hpx=IP:PORT, --hpx:agas=IP:PORT, --hpx:expect-connecting-localities, --hpx:bind=...).
void ext_start(int hpx_threads, const std::vector<std::string>& extra_args) {
    if (g_started.load()) {
        return;
    }
    std::vector<std::vector<char>> argv_store;
    argv_store.push_back(to_cstr("exp63_collective_ext"));
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

// Clean teardown: finalize then stop, GIL released.
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
// Remote fanout result. Composition provenance tail: watchdog label, ran-on-HPX-thread, whether the
// SUCCESS path polled is_ready, and whether the composition is HPX-native (hpx_native == !polled).
// (composite_value, leaves, composition_primitive, reduce_primitive, n_localities, inner_fanout_n,
//  timed_out_leaf_count, watchdog, ran_on_hpx_thread, polled_in_success_path, hpx_native_composition)
using RemoteResult = std::tuple<std::int64_t, std::vector<LeafTuple>, std::string, std::string,
                                std::uint32_t, std::int64_t, std::int64_t, std::string, bool, bool,
                                bool>;

// Reduce ready REMOTE leaf_record futures. when_all/dataflow preserve input order, so the vector
// index IS the leaf index i. acc in uint64 (order-independent mod 2^64).
std::pair<std::int64_t, std::vector<LeafTuple>> reduce_remote_leaves(
    std::vector<hpx::future<exp63::leaf_record>>& ready) {
    std::vector<LeafTuple> leaves;
    leaves.reserve(ready.size());
    std::uint64_t acc = 0;
    std::int64_t i = 0;
    for (auto& f : ready) {
        exp63::leaf_record lr = f.get();
        acc += static_cast<std::uint64_t>(lr.value);
        leaves.push_back(LeafTuple{i, lr.value, lr.locality});
        ++i;
    }
    return {static_cast<std::int64_t>(acc), std::move(leaves)};
}

std::uint32_t ext_local_locality_id() {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
}

// Selected HPX config-entry provenance. get_config_entry returns "unknown" for an absent key -- we
// never fabricate a value. Read-only; safe to call while the runtime is up.
std::map<std::string, std::string> ext_hpx_config_provenance() {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    static const char* keys[] = {
        "hpx.parcel.tcp.enable",
        "hpx.parcel.tcp.array_optimization",
        "hpx.parcel.tcp.zero_copy_optimization",
        "hpx.parcel.message_handlers",
        "hpx.parcel.tcp.parcel_pool_size",
        "hpx.max_background_threads",
        "hpx.threads",
        "hpx.parcel.bootstrap",
        "hpx.parcel.tcp.priority",
    };
    std::map<std::string, std::string> out;
    for (const char* k : keys) {
        out[std::string(k)] = hpx::get_config_entry(std::string(k), std::string("unknown"));
    }
    return out;
}

// ---- remote-locality resolution (root side) ---------------------------------------------------

// MUST run on an HPX thread. Poll find_all_localities() until at least expected_count remote (non-
// here) localities have joined, then cache ALL currently-joined remotes deterministically (ascending
// locality id) and return the number cached. On a single local node the remote set is always empty --
// there is no way to fabricate a remote. The cache is replaced on each call.
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

// Wait for >=expected_count remotes, cache ALL joined remotes deterministically, return the number
// cached. The caller fails closed if the returned count < expected_count.
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

// The blocking op the Python caller times. Dispatches N leaf actions to the CACHED REMOTE localities
// (all-remote round-robin: the root runs none), then composes with a BOUNDED watchdog per mode:
//
//   * root_flat_gather_poll (CONTROL): bounded fine-grained is_ready poll over the individual leaf
//     futures (50 us sleep/yield). Rostam-proven. polled_in_success_path=true, hpx_native=false.
//   * when_all_then_reduce / dataflow_reduce (NATIVE, UNVALIDATED cross-node): compose the N leaf
//     futures with a continuation, bound the WHOLE composition with a SINGLE future::wait_for -- NO
//     success-path poll. polled_in_success_path=false, hpx_native=true. On the TCP parcelport this
//     passive wait stalled to the timeout in exp62 job 158814; background_yielder is the Slice 1b
//     probe for whether pumping the scheduler wakes it.
//
// background_yielder: when true AND the mode is native, spawn a low-cost yielding HPX task that runs
// alongside the passive wait, giving the scheduler repeated opportunities to run background parcel
// progress. Stopped and joined right after the composed wait returns. No effect on the poll control.
//
// Refuses if no remote was resolved -- a single-node run can never masquerade as a remote one.
RemoteResult ext_fanout_fanin_remote(std::int64_t x, std::int64_t n, double dispatch_timeout_s,
                                     const std::string& mode, bool background_yielder) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    if (!g_have_remote) {
        throw std::runtime_error("no remote locality resolved: call await_remotes() and ensure "
                                 "connectors joined (this is NOT a single-node fallback)");
    }
    if (n < 0) {
        throw std::runtime_error("n must be >= 0");
    }
    if (mode != "root_flat_gather_poll" && mode != "when_all_then_reduce"
        && mode != "dataflow_reduce") {
        throw std::runtime_error("unknown remote composition mode: " + mode);
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread(
        [x, n, dispatch_timeout_s, mode, background_yielder]() -> RemoteResult {
            auto make_futs = [x, n]() {
                std::vector<hpx::future<exp63::leaf_record>> futs;
                futs.reserve(static_cast<std::size_t>(n));
                const std::size_t r = g_remote_ids.size();
                for (std::int64_t i = 0; i < n; ++i) {
                    const hpx::id_type& target = g_remote_ids[static_cast<std::size_t>(i) % r];
                    futs.push_back(hpx::async<exp63_leaf_action>(target, x, i));
                }
                return futs;
            };
            const std::uint32_t n_loc = static_cast<std::uint32_t>(g_remote_ids.size());

            // ---- HPX-native composition: future continuation, one final passive wait ----
            if (mode == "when_all_then_reduce" || mode == "dataflow_reduce") {
                auto futs = make_futs();
                hpx::future<std::pair<std::int64_t, std::vector<LeafTuple>>> composed;
                if (mode == "dataflow_reduce") {
                    composed = hpx::dataflow(
                        [](std::vector<hpx::future<exp63::leaf_record>> ready)
                            -> std::pair<std::int64_t, std::vector<LeafTuple>> {
                            return reduce_remote_leaves(ready);
                        },
                        std::move(futs));
                } else {
                    composed = hpx::when_all(std::move(futs)).then(
                        [](auto&& allf) -> std::pair<std::int64_t, std::vector<LeafTuple>> {
                            std::vector<hpx::future<exp63::leaf_record>> ready = allf.get();
                            return reduce_remote_leaves(ready);
                        });
                }

                // Slice 1b probe: pump the scheduler with a yielding task during the passive wait, so
                // background parcel progress gets a chance to run. Stopped + joined after the wait.
                std::atomic<bool> stop_yielder{false};
                hpx::future<void> yielder;
                if (background_yielder) {
                    yielder = hpx::async([&stop_yielder]() {
                        while (!stop_yielder.load(std::memory_order_relaxed)) {
                            hpx::this_thread::yield();
                        }
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

                if (background_yielder) {
                    stop_yielder.store(true, std::memory_order_relaxed);
                    yielder.get();
                }
                return RemoteResult{value, std::move(leaves), mode,
                                    std::string("root_fold_sum_int64"), n_loc, n, timed_out,
                                    std::string("composed_future_wait_for"), true, false, true};
            }

            // ---- Interim CONTROL (default): bounded is_ready poll over the INDIVIDUAL leaf futures.
            // The 50us sleep_for yields this HPX thread so parcelport completion handlers run and mark
            // futures ready; the fine interval keeps quantization low. This is the proven cross-node
            // control; the native modes above are the spike to retire it.
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
            leaves.reserve(static_cast<std::size_t>(n));
            for (std::int64_t i = 0; i < n; ++i) {
                hpx::future<exp63::leaf_record>& f = futs[static_cast<std::size_t>(i)];
                if (f.is_ready()) {
                    exp63::leaf_record lr = f.get();
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

// ---- Slice 1b-follow-up: INSTRUMENTED single-call native diagnostic (A1 rerun) -----------------
//
// A heavily-instrumented variant of the NATIVE branch, for ONE call, to pin down the exp62/job-158870
// "Operation not permitted" poison. It does NOT throw on failure: it CATCHES exceptions and reports
// the stage + type + message so the Python runner can classify (wait timeout vs get vs teardown vs
// EPERM). It uses SHARED futures so leaf readiness can be OBSERVED at timeout, and drains only the
// READY leaves (never blocks on / abandons a moved-from future). This is safer teardown + full
// instrumentation, NOT a true cancellation -- unready leaves at timeout are recorded as
// unsafe_timeout_abandonment (HPX has no clean force-cancel). No performance/Ray/same-axis claim.
//
// Returns a plain struct from the HPX thread; the caller converts it to a py::dict WITH the GIL held.
struct DiagResult {
    std::int64_t value = 0;
    bool have_value = false;
    std::vector<LeafTuple> leaves;
    std::string composition_primitive;
    std::string wait_for_status = "unknown";  // ready | timeout | deferred | unknown
    int leaf_futures_ready_count_at_timeout = -1;
    bool composed_future_ready_at_timeout = false;
    std::int64_t timed_out_leaf_count = 0;
    bool drained_ready_leaves = false;
    bool unsafe_timeout_abandonment = false;
    std::string exception_stage = "none";
    std::string exception_type;
    std::string exception_message;
    bool exception_code_present = false;  // true iff the caught exception was a std::system_error
    int exception_code_value = 0;         // errno/category value from system_error::code().value()
    std::string exception_code_category;  // system_error::code().category().name()
    std::string exception_diagnostic;         // hpx::diagnostic_information(...) if available
    bool exception_diagnostic_available = false;
    std::string last_stage = "before_dispatch";
    std::uint32_t n_localities = 0;
};

std::pair<std::int64_t, std::vector<LeafTuple>> reduce_shared_leaves(
    std::vector<hpx::shared_future<exp63::leaf_record>>& ready) {
    std::vector<LeafTuple> leaves;
    leaves.reserve(ready.size());
    std::uint64_t acc = 0;
    std::int64_t i = 0;
    for (auto& f : ready) {
        exp63::leaf_record lr = f.get();
        acc += static_cast<std::uint64_t>(lr.value);
        leaves.push_back(LeafTuple{i, lr.value, lr.locality});
        ++i;
    }
    return {static_cast<std::int64_t>(acc), std::move(leaves)};
}

py::dict ext_fanout_fanin_remote_diag(std::int64_t x, std::int64_t n, double dispatch_timeout_s,
                                      const std::string& mode, bool drain_ready_leaves) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    if (!g_have_remote) {
        throw std::runtime_error("no remote locality resolved: call await_remotes() first");
    }
    if (n < 0) {
        throw std::runtime_error("n must be >= 0");
    }
    if (mode != "when_all_then_reduce" && mode != "dataflow_reduce"
        && mode != "root_flat_gather_poll") {
        throw std::runtime_error("diag supports root_flat_gather_poll + native modes only: " + mode);
    }

    DiagResult dr;
    {
        py::gil_scoped_release release;
        dr = hpx::run_as_hpx_thread(
            [x, n, dispatch_timeout_s, mode, drain_ready_leaves]() -> DiagResult {
                DiagResult r;
                r.composition_primitive = mode;
                try {
                    r.last_stage = "before_dispatch";
                    std::vector<hpx::shared_future<exp63::leaf_record>> sfuts;
                    sfuts.reserve(static_cast<std::size_t>(n));
                    const std::size_t nrem = g_remote_ids.size();
                    for (std::int64_t i = 0; i < n; ++i) {
                        const hpx::id_type& target = g_remote_ids[static_cast<std::size_t>(i) % nrem];
                        sfuts.push_back(hpx::async<exp63_leaf_action>(target, x, i).share());
                    }
                    r.n_localities = static_cast<std::uint32_t>(nrem);
                    r.last_stage = "after_leaf_futures";

                    if (mode == "root_flat_gather_poll") {
                        // CONTROL branch: bounded is_ready poll over the shared leaf futures. Mirrors
                        // ext_fanout_fanin_remote's poll (same composition, 50us yield, deadline); it
                        // shares the SAME leaf-dispatch loop above, so an EPERM there is observable
                        // identically to the native modes. Behavior/composition/timeout unchanged.
                        const auto deadline = std::chrono::steady_clock::now()
                            + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                  std::chrono::duration<double>(dispatch_timeout_s));
                        for (;;) {
                            bool all_ready = true;
                            for (auto& sf : sfuts) {
                                if (!sf.is_ready()) { all_ready = false; break; }
                            }
                            if (all_ready || std::chrono::steady_clock::now() >= deadline) break;
                            hpx::this_thread::sleep_for(std::chrono::microseconds(50));
                        }
                        r.last_stage = "after_wait_for";
                        int ready_count = 0;
                        for (auto& sf : sfuts) {
                            if (sf.is_ready()) ++ready_count;
                        }
                        if (ready_count == static_cast<int>(n)) {
                            r.wait_for_status = "ready";
                            r.last_stage = "before_get";
                            std::vector<hpx::shared_future<exp63::leaf_record>> ready(
                                sfuts.begin(), sfuts.end());
                            auto pr = reduce_shared_leaves(ready);
                            r.last_stage = "after_get";
                            r.value = pr.first;
                            r.have_value = true;
                            r.leaves = std::move(pr.second);
                            r.timed_out_leaf_count = 0;
                        } else {
                            r.wait_for_status = "timeout";
                            r.leaf_futures_ready_count_at_timeout = ready_count;
                            r.timed_out_leaf_count = n - ready_count;
                            if (drain_ready_leaves) {
                                std::vector<LeafTuple> drained;
                                std::int64_t idx = 0;
                                for (auto& sf : sfuts) {
                                    if (sf.is_ready()) {
                                        exp63::leaf_record lr = sf.get();
                                        drained.push_back(LeafTuple{idx, lr.value, lr.locality});
                                    }
                                    ++idx;
                                }
                                r.leaves = std::move(drained);
                                r.drained_ready_leaves = true;
                            }
                            r.unsafe_timeout_abandonment = (ready_count < static_cast<int>(n));
                        }
                    } else {
                    // shared_futures are copyable -> compose over a COPY, keep sfuts to observe.
                    std::vector<hpx::shared_future<exp63::leaf_record>> compose_in = sfuts;
                    hpx::future<std::pair<std::int64_t, std::vector<LeafTuple>>> composed;
                    if (mode == "dataflow_reduce") {
                        composed = hpx::dataflow(
                            [](std::vector<hpx::shared_future<exp63::leaf_record>> ready)
                                -> std::pair<std::int64_t, std::vector<LeafTuple>> {
                                return reduce_shared_leaves(ready);
                            },
                            std::move(compose_in));
                    } else {
                        composed = hpx::when_all(std::move(compose_in)).then(
                            [](auto&& allf) -> std::pair<std::int64_t, std::vector<LeafTuple>> {
                                auto ready = allf.get();
                                return reduce_shared_leaves(ready);
                            });
                    }
                    r.last_stage = "after_composed_future";

                    hpx::future_status st =
                        composed.wait_for(std::chrono::duration<double>(dispatch_timeout_s));
                    r.last_stage = "after_wait_for";

                    if (st == hpx::future_status::ready) {
                        r.wait_for_status = "ready";
                        r.last_stage = "before_get";
                        auto pr = composed.get();
                        r.last_stage = "after_get";
                        r.value = pr.first;
                        r.have_value = true;
                        r.leaves = std::move(pr.second);
                        r.timed_out_leaf_count = 0;
                    } else if (st == hpx::future_status::deferred) {
                        r.wait_for_status = "deferred";
                    } else {
                        r.wait_for_status = "timeout";
                        // Observe leaf readiness (shared_futures remain valid; no moved-from teardown).
                        int ready_count = 0;
                        for (auto& sf : sfuts) {
                            if (sf.is_ready()) ++ready_count;
                        }
                        r.leaf_futures_ready_count_at_timeout = ready_count;
                        r.composed_future_ready_at_timeout = composed.is_ready();
                        r.timed_out_leaf_count = n - ready_count;
                        if (drain_ready_leaves) {
                            std::vector<LeafTuple> drained;
                            std::int64_t idx = 0;
                            for (auto& sf : sfuts) {
                                if (sf.is_ready()) {
                                    exp63::leaf_record lr = sf.get();  // ready -> won't block
                                    drained.push_back(LeafTuple{idx, lr.value, lr.locality});
                                }
                                ++idx;
                            }
                            r.leaves = std::move(drained);
                            r.drained_ready_leaves = true;
                        }
                        // Unready leaves remain and HPX has no clean force-cancel: honest flag.
                        r.unsafe_timeout_abandonment = (ready_count < static_cast<int>(n));
                    }
                    }  // end else (native compose+wait)
                } catch (const std::exception& e) {
                    r.exception_stage = r.last_stage;
                    r.exception_type = typeid(e).name();
                    r.exception_message = e.what();
                    // Capture errno origin when this is a std::system_error (the "Operation not
                    // permitted" poison) so the Python runner can confirm EPERM vs another code.
                    if (const auto* se = dynamic_cast<const std::system_error*>(&e)) {
                        r.exception_code_present = true;
                        r.exception_code_value = se->code().value();
                        r.exception_code_category = se->code().category().name();
                    }
                    // HPX embeds file/function/backtrace context; capture it if this HPX build
                    // exposes diagnostic_information(exception_ptr). Guarded: any failure leaves the
                    // diagnostic simply unavailable (never throws out of the catch).
                    try {
                        r.exception_diagnostic = hpx::diagnostic_information(std::current_exception());
                        r.exception_diagnostic_available = !r.exception_diagnostic.empty();
                    } catch (...) {
                        r.exception_diagnostic_available = false;
                    }
                } catch (...) {
                    r.exception_stage = r.last_stage;
                    r.exception_type = "non_std_exception";
                    r.exception_message = "unknown non-std exception";
                }
                return r;
            });
    }

    // GIL held again here; build the result dict.
    py::dict d;
    d["composition_primitive"] = dr.composition_primitive;
    d["value"] = dr.have_value ? py::cast(dr.value) : py::none();
    d["leaves"] = dr.leaves;
    d["wait_for_status"] = dr.wait_for_status;
    d["leaf_futures_ready_count_at_timeout"] = dr.leaf_futures_ready_count_at_timeout;
    d["composed_future_ready_at_timeout"] = dr.composed_future_ready_at_timeout;
    d["timed_out_leaf_count"] = dr.timed_out_leaf_count;
    d["drained_ready_leaves"] = dr.drained_ready_leaves;
    d["unsafe_timeout_abandonment"] = dr.unsafe_timeout_abandonment;
    d["exception_stage"] = dr.exception_stage;
    d["exception_type"] = dr.exception_type;
    d["exception_message"] = dr.exception_message;
    d["exception_code_value"] = dr.exception_code_present ? py::cast(dr.exception_code_value)
                                                          : py::none();
    d["exception_code_category"] = dr.exception_code_present ? py::cast(dr.exception_code_category)
                                                             : py::none();
    d["exception_diagnostic_information"] = dr.exception_diagnostic_available
                                                ? py::cast(dr.exception_diagnostic) : py::none();
    d["exception_diagnostic_available"] = dr.exception_diagnostic_available;
    d["last_stage"] = dr.last_stage;
    d["n_localities"] = dr.n_localities;
    return d;
}

// ---- Slice 2b: depth-2 STAR-of-partials fan-in ------------------------------------------------
//
// TOPOLOGY (honest): depth-2 star, NOT a general k-ary tree. The root partitions [0, n) into r
// CONTIGUOUS non-empty blocks (r = number of cached remote localities), dispatches ONE
// exp63_partial_action per remote locality (each folds its OWN block locally, no second hop), and
// composes the r remote PARTIAL futures with a VALIDATED native wait (dataflow_reduce default, or
// when_all_then_reduce). The root reduces r partials, NOT N leaves. This structurally bounds the root
// fan-in by remote-locality count; at N=8, r=2 it is mechanism/topology evidence, NOT a performance
// result. HAND-ROLLED (not hpx::collectives::reduce) so a departed locality surfaces as an exception
// through its future instead of poisoning a fixed-membership communicator.
//
// The wait_for(timeout) is a BOUNDED HARNESS WATCHDOG, not the success-path poll; on timeout the
// outstanding partial actions are abandoned in flight (HPX has no clean force-cancel) -- an honest
// caveat, bounded by the hardened connector lifetime.

using PartialTuple = std::tuple<std::int64_t, std::int64_t, std::int64_t, std::uint32_t>;
// (partial_sum, i_begin, i_count, locality)

struct TreeDiagResult {
    std::int64_t value = 0;
    bool have_value = false;
    std::vector<PartialTuple> partials;
    std::string composition_primitive = "tree_of_partials";
    std::string partial_topology = "depth2_star_of_partials_contiguous_blocks";
    std::string partial_collect_wait;
    std::string wait_for_status = "unknown";  // ready | timeout | deferred | unknown
    int partial_futures_ready_count_at_timeout = -1;
    bool composed_future_ready_at_timeout = false;
    std::int64_t timed_out_partial_count = 0;
    bool drained_ready_partials = false;
    bool unsafe_timeout_abandonment = false;
    std::string exception_stage = "none";
    std::string exception_type;
    std::string exception_message;
    bool exception_code_present = false;
    int exception_code_value = 0;
    std::string exception_code_category;
    std::string exception_diagnostic;
    bool exception_diagnostic_available = false;
    std::string last_stage = "before_dispatch";
    std::uint32_t n_localities = 0;
    std::int64_t partials_count = 0;
};

std::pair<std::int64_t, std::vector<PartialTuple>> reduce_partials(
    std::vector<hpx::shared_future<exp63::partial_record>>& ready) {
    std::vector<PartialTuple> out;
    out.reserve(ready.size());
    std::uint64_t acc = 0;
    for (auto& f : ready) {
        exp63::partial_record pr = f.get();
        acc += static_cast<std::uint64_t>(pr.partial_sum);
        out.push_back(PartialTuple{pr.partial_sum, pr.i_begin, pr.i_count, pr.locality});
    }
    return {static_cast<std::int64_t>(acc), std::move(out)};
}

py::dict ext_fanout_fanin_tree_remote_diag(std::int64_t x, std::int64_t n, double dispatch_timeout_s,
                                           const std::string& partial_collect_wait,
                                           bool drain_ready_partials) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    if (!g_have_remote) {
        throw std::runtime_error("no remote locality resolved: call await_remotes() first");
    }
    if (n < 0) {
        throw std::runtime_error("n must be >= 0");
    }
    if (partial_collect_wait != "dataflow_reduce" && partial_collect_wait != "when_all_then_reduce") {
        throw std::runtime_error(
            "tree collect wait must be dataflow_reduce or when_all_then_reduce: " + partial_collect_wait);
    }
    if (n < static_cast<std::int64_t>(g_remote_ids.size())) {
        throw std::runtime_error("n must be >= remote-locality count so every locality gets a leaf");
    }

    TreeDiagResult dr;
    {
        py::gil_scoped_release release;
        dr = hpx::run_as_hpx_thread(
            [x, n, dispatch_timeout_s, partial_collect_wait, drain_ready_partials]() -> TreeDiagResult {
                TreeDiagResult r;
                r.partial_collect_wait = partial_collect_wait;
                try {
                    r.last_stage = "before_dispatch";
                    const std::size_t nrem = g_remote_ids.size();
                    r.n_localities = static_cast<std::uint32_t>(nrem);
                    r.partials_count = static_cast<std::int64_t>(nrem);
                    // Contiguous block partition: first (n % r) blocks get one extra leaf. Each remote
                    // locality gets EXACTLY ONE block; the blocks tile [0, n) once.
                    const std::int64_t rr = static_cast<std::int64_t>(nrem);
                    const std::int64_t base = n / rr;
                    const std::int64_t rem = n % rr;
                    std::vector<hpx::shared_future<exp63::partial_record>> sfuts;
                    sfuts.reserve(nrem);
                    std::int64_t i_begin = 0;
                    for (std::int64_t j = 0; j < rr; ++j) {
                        const std::int64_t i_count = base + (j < rem ? 1 : 0);
                        const hpx::id_type& target = g_remote_ids[static_cast<std::size_t>(j)];
                        sfuts.push_back(
                            hpx::async<exp63_partial_action>(target, x, i_begin, i_count).share());
                        i_begin += i_count;
                    }
                    r.last_stage = "after_partial_futures";

                    // NATIVE compose of the r partial futures (dataflow_reduce default / when_all).
                    // shared_futures are copyable -> compose over a COPY, keep sfuts to observe.
                    std::vector<hpx::shared_future<exp63::partial_record>> compose_in = sfuts;
                    hpx::future<std::pair<std::int64_t, std::vector<PartialTuple>>> composed;
                    if (partial_collect_wait == "dataflow_reduce") {
                        composed = hpx::dataflow(
                            [](std::vector<hpx::shared_future<exp63::partial_record>> ready)
                                -> std::pair<std::int64_t, std::vector<PartialTuple>> {
                                return reduce_partials(ready);
                            },
                            std::move(compose_in));
                    } else {
                        composed = hpx::when_all(std::move(compose_in)).then(
                            [](auto&& allf) -> std::pair<std::int64_t, std::vector<PartialTuple>> {
                                auto ready = allf.get();
                                return reduce_partials(ready);
                            });
                    }
                    r.last_stage = "after_composed_future";

                    hpx::future_status st =
                        composed.wait_for(std::chrono::duration<double>(dispatch_timeout_s));
                    r.last_stage = "after_wait_for";

                    if (st == hpx::future_status::ready) {
                        r.wait_for_status = "ready";
                        r.last_stage = "before_get";
                        auto pr = composed.get();
                        r.last_stage = "after_get";
                        r.value = pr.first;
                        r.have_value = true;
                        r.partials = std::move(pr.second);
                        r.timed_out_partial_count = 0;
                    } else if (st == hpx::future_status::deferred) {
                        r.wait_for_status = "deferred";
                    } else {
                        r.wait_for_status = "timeout";
                        int ready_count = 0;
                        for (auto& sf : sfuts) {
                            if (sf.is_ready()) ++ready_count;
                        }
                        r.partial_futures_ready_count_at_timeout = ready_count;
                        r.composed_future_ready_at_timeout = composed.is_ready();
                        r.timed_out_partial_count = static_cast<std::int64_t>(nrem) - ready_count;
                        if (drain_ready_partials) {
                            std::vector<PartialTuple> drained;
                            for (auto& sf : sfuts) {
                                if (sf.is_ready()) {
                                    exp63::partial_record pr = sf.get();  // ready -> won't block
                                    drained.push_back(
                                        PartialTuple{pr.partial_sum, pr.i_begin, pr.i_count, pr.locality});
                                }
                            }
                            r.partials = std::move(drained);
                            r.drained_ready_partials = true;
                        }
                        // Unready partial actions remain in flight; HPX has no clean force-cancel.
                        r.unsafe_timeout_abandonment = (ready_count < static_cast<int>(nrem));
                    }
                } catch (const std::exception& e) {
                    r.exception_stage = r.last_stage;
                    r.exception_type = typeid(e).name();
                    r.exception_message = e.what();
                    if (const auto* se = dynamic_cast<const std::system_error*>(&e)) {
                        r.exception_code_present = true;
                        r.exception_code_value = se->code().value();
                        r.exception_code_category = se->code().category().name();
                    }
                    try {
                        r.exception_diagnostic = hpx::diagnostic_information(std::current_exception());
                        r.exception_diagnostic_available = !r.exception_diagnostic.empty();
                    } catch (...) {
                        r.exception_diagnostic_available = false;
                    }
                } catch (...) {
                    r.exception_stage = r.last_stage;
                    r.exception_type = "non_std_exception";
                    r.exception_message = "unknown non-std exception";
                }
                return r;
            });
    }

    // GIL held again here; build the result dict.
    py::dict d;
    d["composition_primitive"] = dr.composition_primitive;
    d["partial_topology"] = dr.partial_topology;
    d["partial_collect_wait"] = dr.partial_collect_wait;
    d["polled_in_success_path"] = false;
    d["hpx_native_composition"] = true;
    d["value"] = dr.have_value ? py::cast(dr.value) : py::none();
    d["partials"] = dr.partials;
    d["partials_count"] = dr.partials_count;
    d["wait_for_status"] = dr.wait_for_status;
    d["partial_futures_ready_count_at_timeout"] = dr.partial_futures_ready_count_at_timeout;
    d["composed_future_ready_at_timeout"] = dr.composed_future_ready_at_timeout;
    d["timed_out_partial_count"] = dr.timed_out_partial_count;
    d["drained_ready_partials"] = dr.drained_ready_partials;
    d["unsafe_timeout_abandonment"] = dr.unsafe_timeout_abandonment;
    d["exception_stage"] = dr.exception_stage;
    d["exception_type"] = dr.exception_type;
    d["exception_message"] = dr.exception_message;
    d["exception_code_value"] = dr.exception_code_present ? py::cast(dr.exception_code_value)
                                                          : py::none();
    d["exception_code_category"] = dr.exception_code_present ? py::cast(dr.exception_code_category)
                                                             : py::none();
    d["exception_diagnostic_information"] = dr.exception_diagnostic_available
                                                ? py::cast(dr.exception_diagnostic) : py::none();
    d["exception_diagnostic_available"] = dr.exception_diagnostic_available;
    d["last_stage"] = dr.last_stage;
    d["n_localities"] = dr.n_localities;
    return d;
}

}  // namespace

PYBIND11_MODULE(collective_ext, m) {
    m.doc() = "exp63 HPX progress root-cause embedding (EXPERIMENT-ONLY, Slice 1b). Reproduces the "
              "exp62 job-158814 passive-wait shape and sweeps composition/config. Not rayx.runtime, "
              "not _rayx, no Ray.";
    m.def("start", &ext_start, py::arg("hpx_threads") = 4,
          py::arg("extra_args") = std::vector<std::string>{},
          "Start the embedded HPX runtime (idempotent). extra_args are appended to the HPX argv for "
          "root networking + config (e.g. --hpx:hpx=IP:PORT, --hpx:bind=...).");
    m.def("shutdown", &ext_shutdown);
    m.def("fanout_fanin_remote", &ext_fanout_fanin_remote, py::arg("x"), py::arg("n"),
          py::arg("dispatch_timeout_s") = 30.0,
          py::arg("mode") = std::string("root_flat_gather_poll"),
          py::arg("background_yielder") = false,
          "All-remote fanout/fanin to the cached remote localities, bounded watchdog; raises if no "
          "remote was resolved. mode: 'root_flat_gather_poll' (proven interim poll CONTROL), "
          "'when_all_then_reduce' / 'dataflow_reduce' (HPX-native continuation, no success-path poll, "
          "single future::wait_for bound). background_yielder pumps the scheduler during the native "
          "passive wait (Slice 1b progress probe; no effect on the poll control).");
    m.def("fanout_fanin_remote_diag", &ext_fanout_fanin_remote_diag, py::arg("x"), py::arg("n"),
          py::arg("dispatch_timeout_s") = 8.0,
          py::arg("mode") = std::string("when_all_then_reduce"),
          py::arg("drain_ready_leaves") = true,
          "INSTRUMENTED single-call native diagnostic (A1 rerun). Native modes only. Does NOT throw: "
          "catches exceptions and returns a dict with wait_for_status, exception stage/type/message, "
          "leaf-ready-at-timeout, composed-ready-at-timeout, and unsafe_timeout_abandonment. Uses "
          "shared futures so leaf readiness is observable and only READY leaves are drained "
          "(safer teardown + full instrumentation, NOT a true cancellation).");
    m.def("fanout_fanin_tree_remote_diag", &ext_fanout_fanin_tree_remote_diag, py::arg("x"),
          py::arg("n"), py::arg("dispatch_timeout_s") = 8.0,
          py::arg("partial_collect_wait") = std::string("dataflow_reduce"),
          py::arg("drain_ready_partials") = true,
          "Slice 2b depth-2 STAR-of-partials fan-in (NOT a k-ary tree). Partitions [0,n) into r "
          "contiguous non-empty blocks, dispatches ONE exp63_partial_action per remote locality (each "
          "folds its own block locally), and composes the r remote PARTIAL futures with a VALIDATED "
          "native wait: partial_collect_wait 'dataflow_reduce' (default, fused HPX form) or "
          "'when_all_then_reduce'. Hand-rolled (no fixed communicator / membership state) so a departed "
          "locality surfaces as a future exception. wait_for is a BOUNDED harness watchdog, not a "
          "success-path poll; on timeout outstanding partials are abandoned in flight. Returns a dict "
          "with partials, composite value, wait_for_status, timed_out_partial_count, and provenance.");
    m.def("await_remotes", &ext_await_remotes, py::arg("expected_count"), py::arg("timeout_s") = 60,
          "Wait for >=expected_count remote localities to join, cache ALL joined remotes (ascending "
          "locality id); returns the number cached. Caller fails closed if < expected.");
    m.def("remote_locality_ids", &ext_remote_locality_ids,
          "All cached remote locality ids (ascending; empty if none resolved).");
    m.def("local_locality_id", &ext_local_locality_id);
    m.def("hpx_config_provenance", &ext_hpx_config_provenance,
          "Selected HPX config entries (parcel coalescing / array optimization / TCP parcelport / "
          "background keys) via get_config_entry; 'unknown' when absent (never fabricated).");
    m.def("hostname", &ext_hostname, "This process's hostname (provenance only).");
    m.attr("__experiment__") = "exp63";
    m.attr("__action_registration_name__") = "exp63_leaf_action";
#ifdef EXP63_BUILD_TYPE
    m.attr("__build_type__") = EXP63_BUILD_TYPE;
#endif
}
