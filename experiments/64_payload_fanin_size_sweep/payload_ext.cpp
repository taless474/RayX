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
//   * root_flat_gather_poll ONLY. No native when_all/passive composition (that stays in exp63). This
//     is the reliable poll-mode gather baseline, explicitly NOT "the HPX answer" and NOT a collective.
//   * The timed op returns the raw payload BYTES to Python. The payload DIGEST is folded in Python
//     AFTER timing -- this module never reduces the payload.
//   * serialize_buffer<char> is the transport-facing payload representation (see payload_action.hpp).
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
// RAW S response bytes (copied out of the serialize_buffer so no HPX type escapes to the GIL section).
struct PayloadLeaf {
    std::int64_t i = 0;
    std::int64_t value = 0;
    std::uint32_t locality = 0;
    std::string payload;          // raw S bytes
    std::int64_t payload_len = 0;
};

struct PayloadResult {
    std::int64_t composite = 0;   // int64 sum of leaf values (mod 2^64), order-independent
    std::vector<PayloadLeaf> leaves;
    std::uint32_t n_localities = 0;
    std::int64_t timed_out_leaf_count = 0;
    std::int64_t n = 0;
    std::int64_t payload_bytes = 0;
};

// The blocking op the Python caller times. All-remote round-robin dispatch of N payload leaves to the
// CACHED REMOTE localities (the root runs none), gathered with the bounded is_ready poll control. The
// payload bytes are returned to Python; the digest is folded in Python AFTER timing. Refuses if no
// remote was resolved -- a single-node run can never masquerade as a remote one.
py::dict ext_fanout_fanin_payload_remote(std::int64_t x, std::int64_t n, std::int64_t payload_bytes,
                                         double dispatch_timeout_s) {
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

    PayloadResult pr;
    {
        py::gil_scoped_release release;
        pr = hpx::run_as_hpx_thread(
            [x, n, payload_bytes, dispatch_timeout_s]() -> PayloadResult {
                PayloadResult r;
                r.n = n;
                r.payload_bytes = payload_bytes;
                const std::size_t rr = g_remote_ids.size();
                r.n_localities = static_cast<std::uint32_t>(rr);

                std::vector<hpx::future<exp64::payload_leaf_record>> futs;
                futs.reserve(static_cast<std::size_t>(n));
                for (std::int64_t i = 0; i < n; ++i) {
                    const hpx::id_type& target = g_remote_ids[static_cast<std::size_t>(i) % rr];
                    futs.push_back(
                        hpx::async<exp64_payload_leaf_action>(target, x, i, payload_bytes));
                }

                // root_flat_gather_poll: bounded is_ready poll over the individual leaf futures. The
                // 50us sleep_for yields this HPX thread so parcelport completion handlers run and mark
                // futures ready. Proven cross-node control; NAIVE all-to-root gather baseline.
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

                std::uint64_t acc = 0;
                r.leaves.reserve(static_cast<std::size_t>(n));
                for (std::int64_t i = 0; i < n; ++i) {
                    hpx::future<exp64::payload_leaf_record>& f = futs[static_cast<std::size_t>(i)];
                    if (f.is_ready()) {
                        exp64::payload_leaf_record lr = f.get();
                        acc += static_cast<std::uint64_t>(lr.value);
                        PayloadLeaf pl;
                        pl.i = i;
                        pl.value = lr.value;
                        pl.locality = lr.locality;
                        pl.payload.assign(lr.payload.data(), lr.payload.data() + lr.payload.size());
                        pl.payload_len = static_cast<std::int64_t>(lr.payload.size());
                        r.leaves.push_back(std::move(pl));
                    } else {
                        ++r.timed_out_leaf_count;
                    }
                }
                r.composite = static_cast<std::int64_t>(acc);
                return r;
            });
    }

    // GIL held again: build the result dict. Each leaf's payload crosses the boundary as py::bytes.
    py::dict d;
    d["composite"] = pr.composite;
    d["n"] = pr.n;
    d["payload_bytes"] = pr.payload_bytes;
    d["n_localities"] = pr.n_localities;
    d["timed_out_leaf_count"] = pr.timed_out_leaf_count;
    d["composition_primitive"] = std::string("root_flat_gather_poll");
    py::list leaves;
    for (auto& lf : pr.leaves) {
        py::dict ld;
        ld["i"] = lf.i;
        ld["value"] = lf.value;
        ld["locality"] = lf.locality;
        ld["payload"] = py::bytes(lf.payload);  // raw S bytes, unmodified
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
          "All-remote payload fanout/fanin to the cached remote localities via root_flat_gather_poll "
          "(bounded is_ready poll). Each leaf returns (value, locality, S payload bytes). Returns a "
          "dict with composite, per-leaf records (payload as bytes), n_localities, timed_out. Raises "
          "if no remote was resolved. The digest is NOT folded here -- Python folds it after timing.");
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
    m.attr("__hpx_composition_mode__") = "root_flat_gather_poll";
#ifdef EXP64_BUILD_TYPE
    m.attr("__build_type__") = EXP64_BUILD_TYPE;
#endif
}
