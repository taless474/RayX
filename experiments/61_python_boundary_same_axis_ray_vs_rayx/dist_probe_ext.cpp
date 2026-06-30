// exp61 dist_probe_ext -- EXPERIMENT-ONLY pybind11 module that embeds an HPX runtime in the
// Python process and dispatches the fixed closed-int64 `exp61_dist_probe_action` to the LOCAL
// HPX locality (find_here), returning the result to Python. This retires the Slice-0 risk:
//
//     Python caller -> pybind/HPX extension -> fixed HPX closed-int64 action -> result -> Python
//
// timed by the Python caller around the blocking call. The HPX embedding (hpx::start /
// run_as_hpx_thread / finalize+stop, GIL released around the blocking call) mirrors the proven
// pattern in python/src/rayx/_rayx.cpp.
//
// Slice 0 is SINGLE-LOCALITY (find_here): no connector, no parcelport, no distributed locality.
//
// Slice 2A adds an ADDITIVE, two-node-CAPABLE root/remote API (start() can take extra HPX
// networking args; await_remote()/dist_probe_remote()/remote_locality_id() dispatch the SAME fixed
// action to a JOINED remote locality served by the standalone dist_probe_connector binary). The
// single-locality smoke path above is UNCHANGED: with no extra args and no connector, await_remote
// finds no remote and dist_probe_remote refuses -- there is NO way to fabricate a remote locally.
//
// This is NOT rayx.runtime, NOT _rayx, NO Ray, and does NOT add distributed actions to any
// shipped RayX API. It makes NO same-axis, speedup, ratio, or "HPX beats Ray" claim.
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/naming_base/id_type.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>   // gethostname

#include "shared_dist_probe.hpp"

namespace py = pybind11;

namespace {

std::atomic<bool> g_started{false};

// cached remote locality (Slice 2A). Resolved ONCE by await_remote and reused for every action, so
// no per-call AGAS lookup is folded into the Python-timed boundary. Empty until a connector joins.
hpx::id_type g_remote_id;
bool g_have_remote{false};
std::uint32_t g_remote_loc{0};

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

// Bring up the embedded HPX runtime on background threads (does not take over the Python main
// thread). Mirrors _rayx's start_process_hpx: GIL released across hpx::start.
//
// `extra_args` (Slice 2A) is appended verbatim to the HPX argv so the runner can pass two-node
// networking flags (e.g. --hpx:hpx=IP:PORT, --hpx:agas=IP:PORT, --hpx:expect-connecting-localities)
// to make this embedded runtime the AGAS ROOT a connector can join. With no extra args (the
// Slice-0 smoke) the startup is byte-for-byte the single-locality console runtime as before.
void ext_start(int hpx_threads, const std::vector<std::string>& extra_args) {
    if (g_started.load()) {
        return;
    }
    std::vector<std::vector<char>> argv_store;
    argv_store.push_back(to_cstr("exp61_dist_probe_ext"));
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

// Clean teardown. Mirrors _rayx's stop_process_hpx: finalize then stop, GIL released.
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
    g_remote_loc = 0;
    g_remote_id = hpx::id_type{};
}

// The blocking op the Python caller times: dispatch the fixed action to the local locality and
// return the closed-int64 to Python. GIL released around the blocking HPX call.
std::int64_t ext_dist_probe(std::int64_t x) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([x]() {
        return hpx::async<exp61_dist_probe_action>(hpx::find_here(), x).get();
    });
}

std::uint32_t ext_local_locality_id() {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
}

// ---- Slice 2A: two-node remote path (root side) --------------------------------------------------
// Poll find_all_localities() until a second locality (the joined connector) appears, then cache its
// id ONCE for reuse. Returns the remote locality id, or -1 if no remote joined within timeout_s.
// On a single local node (no connector) this ALWAYS returns -1 -- there is no way to fabricate a
// remote locally.
std::int64_t ext_await_remote(int timeout_s) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([timeout_s]() -> std::int64_t {
        const hpx::id_type here = hpx::find_here();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
        do {
            std::vector<hpx::id_type> locs = hpx::find_all_localities();
            for (const auto& l : locs) {
                if (l != here) {
                    g_remote_id = l;
                    g_remote_loc = hpx::naming::get_locality_id_from_id(l);
                    g_have_remote = true;
                    return static_cast<std::int64_t>(g_remote_loc);
                }
            }
            hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
        } while (std::chrono::steady_clock::now() < deadline);
        return -1;
    });
}

// The blocking op the Python caller times in the hpx-connected phase: dispatch the fixed action to
// the CACHED REMOTE locality (NOT find_here) and return the closed-int64. Refuses if no remote was
// resolved -- so a single-node run can never masquerade as a remote one.
std::int64_t ext_dist_probe_remote(std::int64_t x) {
    if (!g_started.load()) {
        throw std::runtime_error("HPX not started: call start() first");
    }
    if (!g_have_remote) {
        throw std::runtime_error("no remote locality resolved: call await_remote() and ensure a "
                                 "connector joined (this is NOT a single-node fallback)");
    }
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([x]() {
        return hpx::async<exp61_dist_probe_action>(g_remote_id, x).get();
    });
}

// The cached remote locality id (-1 if none resolved). Used as the explicit node_tag in the oracle.
std::int64_t ext_remote_locality_id() {
    return g_have_remote ? static_cast<std::int64_t>(g_remote_loc) : -1;
}

// This process's hostname (provenance only; not a placement proof).
std::string ext_hostname() {
    char buf[256];
    if (::gethostname(buf, sizeof(buf)) == 0) {
        buf[sizeof(buf) - 1] = '\0';
        return std::string(buf);
    }
    return std::string("unknown");
}

}  // namespace

PYBIND11_MODULE(dist_probe_ext, m) {
    m.doc() = "exp61 experiment-only HPX dist_probe embedding (Slice 0 smoke; not rayx.runtime, "
              "not _rayx, no Ray, single-locality)";
    m.attr("experiment_only") = true;
    m.attr("gil_released_around_blocking_call") = true;
    m.attr("dist_probe_xor") = py::int_(static_cast<std::int64_t>(exp61::DIST_PROBE_XOR));
    m.attr("two_node_capable") = true;  // Slice 2A: remote API present (still needs a joined connector)
    m.def("start", &ext_start, py::arg("hpx_threads") = 4,
          py::arg("extra_args") = std::vector<std::string>{},
          "Start the embedded HPX runtime (idempotent). extra_args are appended to the HPX argv "
          "for two-node networking (e.g. --hpx:hpx=IP:PORT); empty = single-locality smoke.");
    m.def("shutdown", &ext_shutdown, "Finalize + stop the embedded HPX runtime (idempotent).");
    m.def("dist_probe", &ext_dist_probe, py::arg("x"),
          "Dispatch the fixed closed-int64 action to the local locality; returns the result.");
    m.def("local_locality_id", &ext_local_locality_id,
          "The embedded runtime's locality id (0 for the single-locality Slice-0 smoke).");
    m.def("await_remote", &ext_await_remote, py::arg("timeout_s") = 60,
          "Slice 2A: wait for a connector to join, cache its locality id; returns the remote "
          "locality id or -1 if none joined (no single-node fallback).");
    m.def("dist_probe_remote", &ext_dist_probe_remote, py::arg("x"),
          "Slice 2A: dispatch the fixed closed-int64 action to the CACHED REMOTE locality; raises "
          "if no remote was resolved.");
    m.def("remote_locality_id", &ext_remote_locality_id,
          "The cached remote locality id (-1 if none resolved).");
    m.def("hostname", &ext_hostname, "This process's hostname (provenance only).");
    m.def("oracle",
          [](std::int64_t x, std::uint32_t tag) { return exp61::dist_probe_oracle(x, tag); },
          py::arg("x"), py::arg("node_tag"), "Closed-int64 oracle (correctness only).");
}
