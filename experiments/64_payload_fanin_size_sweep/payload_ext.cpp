// exp64 payload_ext -- EXPERIMENT-ONLY pybind11 module embedding an HPX runtime in the Python process
// for the Slice 1 PAYLOAD-FANIN mechanism smoke.
//
// COPIED/ADAPTED from exp63 collective_ext.cpp (exp63 -> exp64, module collective_ext -> payload_ext,
// exp63_leaf_action -> exp64_payload_leaf_action). Self-contained under exp64; includes NO exp62/exp63
// header. This is the AGAS ROOT that dispatches N exp64_payload_leaf_action leaves to joined connectors
// and gathers them with the PROVEN poll-mode control:
//
//   Python caller -> pybind/HPX root -> N hpx::async<exp64_payload_leaf_action>(remote, x, i, S)
//                 -> root_flat_gather_poll (bounded is_ready poll; NAIVE all-to-root gather baseline)
//                 -> (composite int64, per-leaf value/locality/S RESPONSE BYTES) -> Python
//
// EXP64 DISCIPLINE:
//   * Slice 1-4 used root_flat_gather_poll ONLY. Slice 5 Phase A ADDS native READINESS composition
//     (when_all_then_reduce / dataflow_reduce) as an opt-in `mode`, mirroring the exp63 known-good
//     pattern (shared leaf futures -> when_all/dataflow -> one bounded wait_for -> reduce). This
//     retires the success-path POLL for the readiness wait; it does NOT change the data-movement
//     topology: the root still gathers O(N*S) bytes (root_flat_gather_retained) because raw per-leaf
//     bytes must cross the Python boundary. So Phase A addresses only the "poll" half of the
//     hpx_poll_gather_baseline blocker; the "gather" half remains. NOT a collective, NOT "the answer".
//   * The timed op returns the raw payload BYTES to Python. The payload DIGEST is folded in Python
//     AFTER timing -- this module never reduces the payload (the native reduce folds only the SCALAR).
//   * serialize_buffer<char> is the transport-facing payload representation (see payload_action.hpp).
//   * py::bytes is built DIRECTLY from the leaf buffer's data()/size() (no intermediate std::string).
//     This reduces an avoidable root-side copy; it is NOT a zero-copy-to-Python claim (py::bytes copies).
//
// The HPX embedding (hpx::start / run_as_hpx_thread / finalize+stop, GIL released) mirrors the proven
// exp61/62/63 pattern. NOT rayx.runtime, NOT _rayx, NO Ray. Mechanism validation only: NO same-axis,
// speedup, ratio, or "HPX beats Ray" claim.
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/futures/future.hpp>
#include <hpx/async_combinators/when_all.hpp>  // Slice 5 Phase A: native when_all readiness composition
#include <hpx/include/dataflow.hpp>            // Slice 5 Phase A: native dataflow readiness composition
#include <hpx/naming_base/id_type.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>  // gethostname

#include "payload_action.hpp"  // defines + HPX_PLAIN_ACTION-registers exp64_payload_leaf_action (ONE TU)

namespace py = pybind11;

namespace {

std::atomic<bool> g_started{false};

// Cached remote localities (>=2 for the Slice 1 all-remote 4/4 shape). Resolved ONCE and reused for
// every leaf, so no per-call AGAS lookup is folded into the Python-timed boundary. Ascending loc id.
std::vector<hpx::id_type> g_remote_ids;
std::vector<std::uint32_t> g_remote_locs;
bool g_have_remote{false};

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

// Bring up the embedded HPX runtime on background threads. GIL released across hpx::start. extra_args
// is appended verbatim to the HPX argv (root networking + config flags from the runner).
void ext_start(int hpx_threads, const std::vector<std::string>& extra_args) {
    if (g_started.load()) {
        return;
    }
    std::vector<std::vector<char>> argv_store;
    argv_store.push_back(to_cstr("exp64_payload_ext"));
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

std::uint32_t ext_local_locality_id() {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
}

// Selected HPX config-entry provenance. get_config_entry returns "unknown" for an absent key -- never
// fabricated. Includes the parcelport transport / zero-copy / array-optimization / coalescing keys the
// exp64 payload artifacts record.
std::map<std::string, std::string> ext_hpx_config_provenance() {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    static const char* keys[] = {
        "hpx.parcel.bootstrap",
        "hpx.parcel.tcp.enable",
        "hpx.parcel.tcp.array_optimization",
        "hpx.parcel.tcp.zero_copy_optimization",
        "hpx.parcel.tcp.zero_copy_receive_optimization",
        "hpx.parcel.message_handlers",
        "hpx.parcel.tcp.parcel_pool_size",
        "hpx.parcel.max_message_size",
        "hpx.parcel.max_outbound_message_size",
        "hpx.max_background_threads",
        "hpx.max_idle_backoff_time",  // A3: scheduler idle-backoff disclosure (exp58 convention)
        "hpx.max_idle_loop_count",    // A3: scheduler idle-loop count disclosure
        "hpx.threads",
    };
    std::map<std::string, std::string> out;
    for (const char* k : keys) {
        out[std::string(k)] = hpx::get_config_entry(std::string(k), std::string("unknown"));
    }
    return out;
}

// ---- remote-locality resolution (root side) ---------------------------------------------------

// MUST run on an HPX thread. Poll find_all_localities() until >= expected_count remote (non-here)
// localities have joined, then cache ALL currently-joined remotes deterministically (ascending loc id)
// and return the number cached. On a single local node the remote set is always empty.
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

std::vector<std::int64_t> ext_remote_locality_ids() {
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

// ---- the timed payload fanin op ---------------------------------------------------------------

// One leaf's result carried back off the HPX thread: index, scalar value, executing locality, and the
// S response bytes held as an OWNING serialize_buffer<char> (moved out of a plain future, or copied out
// of a shared future). Carrying the buffer -- not a std::string -- lets the GIL section build py::bytes
// DIRECTLY from data()/size() with no intermediate std::string hop.
struct PayloadLeaf {
    std::int64_t i = 0;
    std::int64_t value = 0;
    std::uint32_t locality = 0;
    exp64::payload_buffer payload;   // owning S bytes; py::bytes built directly from data()/size()
    std::int64_t payload_len = 0;
};

// A4-progress-probe: ONE root-process monotonic clock. All A4 timestamps are ns since this steady_clock
// epoch (arbitrary), so they are COMPARABLE ONLY AMONG THEMSELVES -- never differenced against the Python
// perf_counter that measures the outer RTT. 0 means "not captured / N/A".
static inline std::int64_t steady_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

// A4-progress-probe cross-thread continuation timestamps. The when_all/dataflow continuation may run on a
// DIFFERENT HPX worker thread than the run_as_hpx_thread body and, in the timeout path, may fire AFTER the
// body returns. Held via shared_ptr so the storage outlives a late continuation (no stack use-after-free);
// atomics so the post-wait read is race-free even when the continuation never ran (sentinel 0 stays).
struct A4ProbeTs {
    std::atomic<std::int64_t> continuation_entered{0};
    std::atomic<std::int64_t> continuation_completed{0};
};

struct PayloadResult {
    std::int64_t composite = 0;   // int64 sum of leaf values (mod 2^64), order-independent
    std::vector<PayloadLeaf> leaves;
    std::uint32_t n_localities = 0;
    std::int64_t timed_out_leaf_count = 0;
    std::int64_t n = 0;
    std::int64_t payload_bytes = 0;
    // Slice 5 Phase A readiness-composition provenance.
    std::string composition_primitive = "root_flat_gather_poll";  // = readiness mode
    bool success_path_polling_used = true;   // true iff the SUCCESS path used the is_ready poll
    std::string wait_for_status = "unknown"; // ready | timeout | deferred | unknown
    std::string native_wait_variant = "n/a"; // n/a(poll control) | wait_for | yield_poll(diagnostic)
    // A4-progress-probe root-clock timestamps (ns; single steady_clock; 0 = not captured / N/A).
    std::int64_t t_dispatch_start_ns = 0;
    std::int64_t t_first_leaf_observed_ns = 0;   // controls only; 0/N/A for native (no perturbing probe)
    std::int64_t t_continuation_entered_ns = 0;  // when_all/dataflow continuation only
    std::int64_t t_continuation_completed_ns = 0;
    std::int64_t t_waiter_entered_ns = 0;        // Run1: root-clock instant just before the direct wait
    std::int64_t t_wait_returned_ns = 0;
};

// Run 1 blocked-waiter probe: cooperative delay each LOCAL leaf holds before returning, so the composed
// future becomes ready AFTER the body suspends in wait_for -- this makes the resume path (not the
// is_ready fast path) the thing under test. Recorded in provenance; not real work.
constexpr std::int64_t LOCAL_LEAF_DELAY_MS = 2;

// Slice 5 Phase A: native SCALAR reduce over ready shared leaf futures (order-independent int64 sum).
// The payload is NOT folded here -- raw bytes still cross the Python boundary; only the scalar is
// reduced natively, exactly mirroring exp63's reduce_shared_leaves shape.
std::int64_t reduce_shared_scalar(
    std::vector<hpx::shared_future<exp64::payload_leaf_record>>& ready) {
    std::uint64_t acc = 0;
    for (auto& f : ready) {
        acc += static_cast<std::uint64_t>(f.get().value);
    }
    return static_cast<std::int64_t>(acc);
}

// The blocking op the Python caller times. All-remote round-robin dispatch of N payload leaves to the
// CACHED REMOTE localities (the root runs none). Readiness is composed by `mode`:
//   * root_flat_gather_poll (control): bounded is_ready poll over PLAIN leaf futures (Slice 1-4 path).
//   * when_all_then_reduce / dataflow_reduce (Slice 5 Phase A NATIVE): SHARED leaf futures composed with
//     hpx::when_all(...).then(reduce) or hpx::dataflow, bounded by ONE wait_for; the SCALAR is reduced
//     natively (payload bytes are still gathered flat at the root afterward). This retires the
//     success-path POLL but NOT the flat gather (root_flat_gather_retained).
// The payload bytes are returned to Python; the digest is folded in Python AFTER timing. Refuses if no
// remote was resolved -- a single-node run can never masquerade as a remote one.
py::dict ext_fanout_fanin_payload_remote(std::int64_t x, std::int64_t n, std::int64_t payload_bytes,
                                         double dispatch_timeout_s, std::string mode) {
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
    if (payload_bytes < 0) {
        throw std::runtime_error("payload_bytes must be >= 0");
    }
    if (mode != "root_flat_gather_poll" && mode != "when_all_then_reduce"
        && mode != "dataflow_reduce" && mode != "when_all_then_reduce_yield"
        && mode != "sequential_leaf_wait"
        && mode != "local_when_all_then_reduce_wait_for") {
        throw std::runtime_error("mode must be root_flat_gather_poll | when_all_then_reduce | "
                                 "dataflow_reduce | when_all_then_reduce_yield | sequential_leaf_wait | "
                                 "local_when_all_then_reduce_wait_for: " + mode);
    }

    PayloadResult pr;
    {
        py::gil_scoped_release release;
        pr = hpx::run_as_hpx_thread(
            [x, n, payload_bytes, dispatch_timeout_s, mode]() -> PayloadResult {
                PayloadResult r;
                r.n = n;
                r.payload_bytes = payload_bytes;
                r.composition_primitive = mode;
                const std::size_t rr = g_remote_ids.size();
                r.n_localities = static_cast<std::uint32_t>(rr);
                r.leaves.reserve(static_cast<std::size_t>(n));

                if (mode == "root_flat_gather_poll") {
                    // CONTROL: bounded is_ready poll over PLAIN leaf futures. The 50us sleep_for yields
                    // this HPX thread so parcelport completion handlers run and mark futures ready.
                    // Proven cross-node control; NAIVE all-to-root gather baseline; poll on the success
                    // path. Payload harvested by MOVE out of the plain future (single copy into py::bytes).
                    r.success_path_polling_used = true;
                    std::vector<hpx::future<exp64::payload_leaf_record>> futs;
                    futs.reserve(static_cast<std::size_t>(n));
                    r.t_dispatch_start_ns = steady_now_ns();  // A4: root-clock dispatch start
                    for (std::int64_t i = 0; i < n; ++i) {
                        const hpx::id_type& target = g_remote_ids[static_cast<std::size_t>(i) % rr];
                        futs.push_back(
                            hpx::async<exp64_payload_leaf_action>(target, x, i, payload_bytes));
                    }
                    const auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                              std::chrono::duration<double>(dispatch_timeout_s));
                    for (;;) {
                        // A4: full scan (not short-circuit) so we can record first-leaf-observed
                        // non-perturbingly on the SAME is_ready calls the poll control already makes.
                        bool all_ready = true;
                        bool any_ready = false;
                        for (auto& f : futs) {
                            if (f.is_ready()) { any_ready = true; }
                            else { all_ready = false; }
                        }
                        if (any_ready && r.t_first_leaf_observed_ns == 0) {
                            r.t_first_leaf_observed_ns = steady_now_ns();  // A4: first leaf ready
                        }
                        if (all_ready || std::chrono::steady_clock::now() >= deadline) {
                            break;
                        }
                        hpx::this_thread::sleep_for(std::chrono::microseconds(50));
                    }
                    r.t_wait_returned_ns = steady_now_ns();  // A4: poll loop resolved
                    int ready_count = 0;
                    for (auto& f : futs) {
                        if (f.is_ready()) ++ready_count;
                    }
                    r.wait_for_status = (ready_count == static_cast<int>(n)) ? "ready" : "timeout";
                    std::uint64_t acc = 0;
                    for (std::int64_t i = 0; i < n; ++i) {
                        hpx::future<exp64::payload_leaf_record>& f = futs[static_cast<std::size_t>(i)];
                        if (f.is_ready()) {
                            exp64::payload_leaf_record lr = f.get();  // move out of plain future
                            acc += static_cast<std::uint64_t>(lr.value);
                            PayloadLeaf pl;
                            pl.i = i;
                            pl.value = lr.value;
                            pl.locality = lr.locality;
                            pl.payload_len = static_cast<std::int64_t>(lr.payload.size());
                            pl.payload = std::move(lr.payload);  // owning buffer moved, no extra copy
                            r.leaves.push_back(std::move(pl));
                        } else {
                            ++r.timed_out_leaf_count;
                        }
                    }
                    r.composite = static_cast<std::int64_t>(acc);
                } else if (mode == "sequential_leaf_wait") {
                    // POSITIVE CONTROL (A3, exp58-analog): dispatch N SHARED leaf futures, then BLOCK on
                    // each with a bounded wait_for sharing ONE deadline (a safe .get() analog). NO
                    // when_all/dataflow composition, NO is_ready poll -- this isolates whether an idle-
                    // backoff-off runtime wakes a BARE blocking wait from whether it also wakes the
                    // composed future. NOT a native-composition claim: the scalar is summed from the
                    // harvested leaves, not natively reduced.
                    r.success_path_polling_used = false;
                    r.native_wait_variant = "sequential_leaf_wait_for";
                    std::vector<hpx::shared_future<exp64::payload_leaf_record>> sfuts;
                    sfuts.reserve(static_cast<std::size_t>(n));
                    r.t_dispatch_start_ns = steady_now_ns();  // A4: root-clock dispatch start
                    for (std::int64_t i = 0; i < n; ++i) {
                        const hpx::id_type& target = g_remote_ids[static_cast<std::size_t>(i) % rr];
                        sfuts.push_back(
                            hpx::async<exp64_payload_leaf_action>(target, x, i, payload_bytes).share());
                    }
                    const auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                              std::chrono::duration<double>(dispatch_timeout_s));
                    std::uint64_t acc = 0;
                    std::int64_t ready_ct = 0;
                    for (std::int64_t idx = 0; idx < static_cast<std::int64_t>(sfuts.size()); ++idx) {
                        hpx::shared_future<exp64::payload_leaf_record>& sf =
                            sfuts[static_cast<std::size_t>(idx)];
                        const auto now = std::chrono::steady_clock::now();
                        if (now < deadline) {
                            sf.wait_for(deadline - now);  // bounded blocking wait, ONE shared deadline
                        }
                        if (sf.is_ready()) {  // pre-existing post-wait harvest check (NOT an added poll)
                            // A4: first-leaf-observed = when the FIRST per-leaf wait_for returns ready.
                            // Reuses the existing readiness check; adds NO extra polling to this control.
                            if (r.t_first_leaf_observed_ns == 0) {
                                r.t_first_leaf_observed_ns = steady_now_ns();
                            }
                            const exp64::payload_leaf_record& lr = sf.get();
                            acc += static_cast<std::uint64_t>(lr.value);
                            PayloadLeaf pl;
                            pl.i = idx;
                            pl.value = lr.value;
                            pl.locality = lr.locality;
                            pl.payload = lr.payload;  // copy owning buffer out of the shared state
                            pl.payload_len = static_cast<std::int64_t>(pl.payload.size());
                            r.leaves.push_back(std::move(pl));
                            ++ready_ct;
                        }
                    }
                    r.t_wait_returned_ns = steady_now_ns();  // A4: all per-leaf waits resolved
                    r.composite = static_cast<std::int64_t>(acc);
                    r.timed_out_leaf_count = n - ready_ct;
                    r.wait_for_status = (ready_ct == static_cast<std::int64_t>(n)) ? "ready" : "timeout";
                } else {
                    // NATIVE READINESS: SHARED leaf futures composed with when_all/dataflow. The WAIT
                    // depends on `mode`:
                    //   when_all_then_reduce / dataflow_reduce (CLAIM path): ONE bounded wait_for, NO
                    //     success-path poll. Mirrors exp63 ext_fanout_fanin_remote_diag.
                    //   when_all_then_reduce_yield (DIAGNOSTIC, NOT a claim path): a yielding is_ready
                    //     poll on the COMPOSED future -- used only to test whether yielding the scheduler
                    //     drives the passive native composition promptly (job 159418 showed wait_for slept
                    //     to the deadline). This variant DOES poll, so success_path_polling_used=true.
                    // Either way the SCALAR is reduced natively; payload bytes are harvested from the ready
                    // shared futures afterward (flat gather retained).
                    const bool yield_diag = (mode == "when_all_then_reduce_yield");
                    // Run 1 local no-parcelport control: LOCAL hpx::async leaves (no action, no
                    // serialization, no connector), same when_all().then(reduce), same direct wait_for.
                    const bool local_control = (mode == "local_when_all_then_reduce_wait_for");
                    r.success_path_polling_used = yield_diag;  // both claim + local BLOCK (false)
                    r.native_wait_variant =
                        yield_diag ? "yield_poll" : (local_control ? "local_wait_for" : "wait_for");
                    std::vector<hpx::shared_future<exp64::payload_leaf_record>> sfuts;
                    sfuts.reserve(static_cast<std::size_t>(n));
                    r.t_dispatch_start_ns = steady_now_ns();  // A4: root-clock dispatch start
                    for (std::int64_t i = 0; i < n; ++i) {
                        if (local_control) {
                            // Local leaf: same closed value + payload as the action, built IN-PROCESS on
                            // the root, after a small cooperative delay so readiness follows suspension.
                            // No remote action, no parcelport, no serialization hop.
                            sfuts.push_back(hpx::async([x, i, payload_bytes]()
                                                       -> exp64::payload_leaf_record {
                                hpx::this_thread::sleep_for(
                                    std::chrono::milliseconds(LOCAL_LEAF_DELAY_MS));
                                return exp64_payload_leaf(x, i, payload_bytes);
                            }).share());
                        } else {
                            const hpx::id_type& target = g_remote_ids[static_cast<std::size_t>(i) % rr];
                            sfuts.push_back(
                                hpx::async<exp64_payload_leaf_action>(target, x, i, payload_bytes).share());
                        }
                    }
                    // A4: cross-thread continuation timestamps. Held by shared_ptr so the storage
                    // outlives a continuation that fires AFTER the body returns (timeout path); the
                    // continuation captures a COPY of the shared_ptr. Two cheap atomic stores only.
                    auto a4 = std::make_shared<A4ProbeTs>();
                    std::vector<hpx::shared_future<exp64::payload_leaf_record>> compose_in = sfuts;
                    hpx::future<std::int64_t> composed;
                    if (mode == "dataflow_reduce") {
                        composed = hpx::dataflow(
                            [a4](std::vector<hpx::shared_future<exp64::payload_leaf_record>> ready)
                                -> std::int64_t {
                                a4->continuation_entered.store(steady_now_ns(),
                                                               std::memory_order_release);
                                std::int64_t v = reduce_shared_scalar(ready);
                                a4->continuation_completed.store(steady_now_ns(),
                                                                 std::memory_order_release);
                                return v;
                            },
                            std::move(compose_in));
                    } else {  // when_all_then_reduce and its yield diagnostic both compose via when_all
                        composed = hpx::when_all(std::move(compose_in)).then(
                            [a4](auto&& allf) -> std::int64_t {
                                a4->continuation_entered.store(steady_now_ns(),
                                                               std::memory_order_release);
                                auto ready = allf.get();
                                std::int64_t v = reduce_shared_scalar(ready);
                                a4->continuation_completed.store(steady_now_ns(),
                                                                 std::memory_order_release);
                                return v;
                            });
                    }

                    // WAIT on the composed future. yield_poll: bounded is_ready poll with a 50us
                    // sleep_for yield (diagnostic). wait_for: single bounded suspension (claim path).
                    bool ready = false;
                    if (yield_diag) {
                        const auto deadline = std::chrono::steady_clock::now()
                            + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                  std::chrono::duration<double>(dispatch_timeout_s));
                        while (!composed.is_ready()
                               && std::chrono::steady_clock::now() < deadline) {
                            hpx::this_thread::sleep_for(std::chrono::microseconds(50));
                        }
                        ready = composed.is_ready();
                        r.wait_for_status = ready ? "ready" : "timeout";
                    } else {
                        // Run1: root-clock instant immediately before the direct timed suspension. Lets
                        // Python prove the body actually SUSPENDED before readiness
                        // (waiter_suspended_before_ready = t_waiter_entered < t_continuation_completed) --
                        // otherwise a prompt result is an is_ready fast-path artifact, not a resume result.
                        r.t_waiter_entered_ns = steady_now_ns();
                        hpx::future_status st =
                            composed.wait_for(std::chrono::duration<double>(dispatch_timeout_s));
                        if (st == hpx::future_status::ready) {
                            ready = true;
                            r.wait_for_status = "ready";
                        } else if (st == hpx::future_status::deferred) {
                            r.wait_for_status = "deferred";
                        } else {
                            r.wait_for_status = "timeout";
                        }
                    }
                    // A4: wait resolved on the body thread; read the continuation atomics (acquire pairs
                    // with the continuation's release stores). Sentinel 0 stays if the continuation never
                    // ran (timeout/deferred) -> Python derivation fails closed. Never extends the wait.
                    r.t_wait_returned_ns = steady_now_ns();
                    r.t_continuation_entered_ns =
                        a4->continuation_entered.load(std::memory_order_acquire);
                    r.t_continuation_completed_ns =
                        a4->continuation_completed.load(std::memory_order_acquire);

                    if (ready) {
                        r.composite = composed.get();  // native scalar reduce
                        for (std::int64_t idx = 0; idx < static_cast<std::int64_t>(sfuts.size());
                             ++idx) {
                            const exp64::payload_leaf_record& lr =
                                sfuts[static_cast<std::size_t>(idx)].get();  // ready -> no block
                            PayloadLeaf pl;
                            pl.i = idx;
                            pl.value = lr.value;
                            pl.locality = lr.locality;
                            pl.payload = lr.payload;  // copy owning buffer out of the shared state
                            pl.payload_len = static_cast<std::int64_t>(pl.payload.size());
                            r.leaves.push_back(std::move(pl));
                        }
                        r.timed_out_leaf_count = 0;
                    } else {
                        // deferred or timeout: harvest ready leaves for provenance; composite left 0 and
                        // timed_out>0 so the correctness gates fail CLOSED.
                        std::int64_t ready_ct = 0;
                        for (std::int64_t idx = 0; idx < static_cast<std::int64_t>(sfuts.size());
                             ++idx) {
                            hpx::shared_future<exp64::payload_leaf_record>& sf =
                                sfuts[static_cast<std::size_t>(idx)];
                            if (sf.is_ready()) {
                                const exp64::payload_leaf_record& lr = sf.get();
                                PayloadLeaf pl;
                                pl.i = idx;
                                pl.value = lr.value;
                                pl.locality = lr.locality;
                                pl.payload = lr.payload;
                                pl.payload_len = static_cast<std::int64_t>(pl.payload.size());
                                r.leaves.push_back(std::move(pl));
                                ++ready_ct;
                            }
                        }
                        r.timed_out_leaf_count = n - ready_ct;
                    }
                }
                return r;
            });
    }

    // GIL held again: build the result dict. Each leaf's payload crosses the boundary as py::bytes,
    // built DIRECTLY from the owning buffer's data()/size() (no intermediate std::string).
    py::dict d;
    d["composite"] = pr.composite;
    d["n"] = pr.n;
    d["payload_bytes"] = pr.payload_bytes;
    d["n_localities"] = pr.n_localities;
    d["timed_out_leaf_count"] = pr.timed_out_leaf_count;
    d["composition_primitive"] = pr.composition_primitive;
    // Slice 5 Phase A readiness / topology provenance (authoritative from the runtime path taken).
    d["readiness_composition"] = pr.composition_primitive;
    d["success_path_polling_used"] = pr.success_path_polling_used;
    d["wait_for_status"] = pr.wait_for_status;
    d["native_wait_variant"] = pr.native_wait_variant;  // n/a | wait_for | yield_poll(diagnostic)
    d["root_flat_gather_retained"] = true;             // payload still gathered O(N*S) at the root
    d["payload_data_movement_topology"] = std::string("root_flat_gather");
    d["payload_bytes_cross_python_boundary"] = true;
    d["python_bytes_direct_copy"] = true;              // py::bytes from data()/size(), not via std::string
    // A4-progress-probe root-clock timestamps (ns; single steady_clock; comparable ONLY among themselves,
    // never against the Python RTT clock; 0 = not captured / N/A). t_artifact_assembly_ns is read here on
    // the main thread from the SAME clock. Python derives the continuation/waiter discriminator.
    d["progress_clock"] = std::string("steady_clock_root_process_ns");
    d["t_dispatch_start_ns"] = pr.t_dispatch_start_ns;
    d["t_first_leaf_observed_ns"] = pr.t_first_leaf_observed_ns;    // controls only; 0 => N/A (native)
    d["t_continuation_entered_ns"] = pr.t_continuation_entered_ns;  // native when_all/dataflow only
    d["t_continuation_completed_ns"] = pr.t_continuation_completed_ns;
    d["t_waiter_entered_ns"] = pr.t_waiter_entered_ns;             // Run1: 0 => N/A (poll/yield paths)
    d["t_wait_returned_ns"] = pr.t_wait_returned_ns;
    d["t_artifact_assembly_ns"] = steady_now_ns();
    // Run 1 blocked-waiter probe placement provenance. The local control runs IN-PROCESS on the root
    // (no parcelport); it is scored by leaves_all_local, NOT by the all-remote/balanced placement gates.
    const bool is_local_control = (mode == "local_when_all_then_reduce_wait_for");
    d["placement_class"] = std::string(is_local_control ? "local_control" : "remote_all");
    d["local_control"] = is_local_control;
    d["local_leaf_delay_ms"] = is_local_control ? LOCAL_LEAF_DELAY_MS : 0;
    py::list leaves;
    for (auto& lf : pr.leaves) {
        py::dict ld;
        ld["i"] = lf.i;
        ld["value"] = lf.value;
        ld["locality"] = lf.locality;
        ld["payload"] = py::bytes(lf.payload.data(),
                                  static_cast<py::size_t>(lf.payload.size()));  // direct, raw S bytes
        ld["payload_len"] = lf.payload_len;
        leaves.append(std::move(ld));
    }
    d["leaves"] = std::move(leaves);
    return d;
}

}  // namespace

PYBIND11_MODULE(payload_ext, m) {
    m.doc() = "exp64 HPX payload-fanin embedding (EXPERIMENT-ONLY, Slice 1). All-remote fanout/fanin "
              "with a serialize_buffer<char> RESPONSE payload gathered via root_flat_gather_poll "
              "(naive all-to-root gather baseline). Payload bytes are returned to Python; the digest "
              "is folded in Python after timing. Not rayx.runtime, not _rayx, no Ray.";
    m.def("start", &ext_start, py::arg("hpx_threads") = 4,
          py::arg("extra_args") = std::vector<std::string>{},
          "Start the embedded HPX runtime (idempotent). extra_args are appended to the HPX argv for "
          "root networking + config (e.g. --hpx:hpx=IP:PORT, --hpx:bind=...).");
    m.def("shutdown", &ext_shutdown);
    m.def("fanout_fanin_payload_remote", &ext_fanout_fanin_payload_remote,
          py::arg("x"), py::arg("n"), py::arg("payload_bytes"),
          py::arg("dispatch_timeout_s") = 8.0,
          py::arg("mode") = std::string("root_flat_gather_poll"),
          "All-remote payload fanout/fanin to the cached remote localities. mode selects READINESS "
          "composition: root_flat_gather_poll (bounded is_ready poll control), or the Slice 5 Phase A "
          "native modes when_all_then_reduce / dataflow_reduce (shared futures -> when_all/dataflow -> "
          "one wait_for; native SCALAR reduce). Each leaf returns (value, locality, S payload bytes) "
          "gathered flat at the root (root_flat_gather_retained). Returns a dict with composite, "
          "readiness_composition, success_path_polling_used, wait_for_status, per-leaf records (payload "
          "as bytes built directly from data()/size()), n_localities, timed_out. Raises if no remote "
          "was resolved. The digest is NOT folded here -- Python folds it after timing.");
    m.def("await_remotes", &ext_await_remotes, py::arg("expected_count"), py::arg("timeout_s") = 60,
          "Wait for >=expected_count remote localities to join, cache ALL joined remotes (ascending "
          "locality id); returns the number cached. Caller fails closed if < expected.");
    m.def("remote_locality_ids", &ext_remote_locality_ids,
          "All cached remote locality ids (ascending; empty if none resolved).");
    m.def("local_locality_id", &ext_local_locality_id);
    m.def("hpx_config_provenance", &ext_hpx_config_provenance,
          "Selected HPX config entries (parcelport bootstrap / TCP / zero-copy / array-optimization / "
          "coalescing / message-size / background keys) via get_config_entry; 'unknown' when absent.");
    m.def("hostname", &ext_hostname, "This process's hostname (provenance only).");
    m.attr("__experiment__") = "exp64";
    m.attr("__action_registration_name__") = "exp64_payload_leaf_action";
    m.attr("__payload_representation__") = "hpx::serialization::serialize_buffer<char>";
    m.attr("__hpx_composition_mode__") = "root_flat_gather_poll";  // default; per-call `mode` authoritative
    m.attr("__readiness_composition_modes__") =
        std::vector<std::string>{"root_flat_gather_poll", "when_all_then_reduce", "dataflow_reduce"};
    m.attr("__diagnostic_readiness_modes__") =
        std::vector<std::string>{"when_all_then_reduce_yield"};  // progress-diagnosis only, not a claim
    m.attr("__positive_control_modes__") =
        std::vector<std::string>{"sequential_leaf_wait"};  // A3 exp58-analog control, not a claim
    m.attr("__root_flat_gather_retained__") = true;  // Slice 5 Phase A retires poll, NOT the flat gather
#ifdef EXP64_BUILD_TYPE
    m.attr("__build_type__") = EXP64_BUILD_TYPE;
#endif
}
