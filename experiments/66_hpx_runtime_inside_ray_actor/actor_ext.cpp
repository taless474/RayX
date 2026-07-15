// exp66 -- EXPERIMENT-ONLY pybind11 extension hosted INSIDE one Ray actor worker process.
//
// The Ray actor imports this module and calls start_connect(): hpx::start with
// runtime_mode::connect brings a NETWORKING (TCP parcelport) HPX runtime up on background
// threads of the SAME process -- no child process is launched, which is the point of exp66.
// The module also registers the fixed exp66 actions (ONE TU per binary), so this locality both
// SERVES probes dispatched at it from the standalone prober and can DISPATCH the closed-oracle
// probe at the prober. All blocking calls release the GIL; actions are pure C++ and never touch
// the GIL -- that independence is exactly what the busy-Python progress gate tests.
//
// Identity discipline: hpx_version_info() is callable BEFORE start (exp64 Slice 5V pattern) so
// the controller asserts the verified waiter-fix build identity before any measurement.
// NOT production code, NOT rayx.runtime API, no performance/ratio/winner claim, no inference.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/version.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "probe_action.hpp"  // ONE TU per binary: registers exp66_probe_action / exp66_pid_action

namespace py = pybind11;

namespace {

std::atomic<bool> g_started{false};

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

long long wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Runtime-observed HPX build identity via the PUBLIC version API; callable before start().
std::map<std::string, std::string> ext_hpx_version_info() {
    std::map<std::string, std::string> out;
    out["hpx_version_full"] = hpx::full_version_as_string();
    out["hpx_complete_version"] = hpx::complete_version();
    return out;
}

// Join an existing island as a CONNECT-mode locality on background threads of THIS process.
void ext_start_connect(int hpx_threads, const std::vector<std::string>& extra_args) {
    if (g_started.load()) {
        throw std::runtime_error("HPX already started in this process (one runtime per process)");
    }
    std::vector<std::vector<char>> argv_store;
    argv_store.push_back(to_cstr("exp66_actor_ext"));
    argv_store.push_back(to_cstr("--hpx:threads=" + std::to_string(hpx_threads)));
    for (const std::string& a : extra_args) {
        argv_store.push_back(to_cstr(a));
    }
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

// Graceful connector leave from the hosting process: exp49-proven post(disconnect)+stop.
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

std::string ext_hostname() {
    char buf[256] = {0};
    if (::gethostname(buf, sizeof(buf) - 1) == 0) return std::string(buf);
    return "unknown";
}

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

// Actor->prober closed-oracle dispatch: bounded wait, .get() only when ready (exp50 discipline).
py::dict ext_dispatch_probe(std::int64_t x, std::uint32_t target_loc, int bound_s) {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    struct R {
        bool found = false, ready = false, oracle = false;
        std::int64_t result = -1;
        std::uint32_t executed_on = 0xFFFFFFFFu;
        long long t0 = 0, t1 = 0;
    } r;
    {
        py::gil_scoped_release release;
        r = hpx::run_as_hpx_thread([&]() -> R {
            R q;
            hpx::id_type target = hpx::invalid_id;
            for (auto const& id : hpx::find_all_localities()) {
                if (hpx::naming::get_locality_id_from_id(id) == target_loc) target = id;
            }
            if (target == hpx::invalid_id) return q;
            q.found = true;
            q.t0 = wall_ms();
            auto f = hpx::async<exp66_probe_action>(target, x);
            q.ready = f.wait_for(std::chrono::seconds(bound_s)) == hpx::future_status::ready;
            q.t1 = wall_ms();
            if (q.ready) {
                q.result = f.get();
                q.executed_on = exp66::executed_locality_from(q.result, x);
                q.oracle = (q.result == exp66::probe_value(x, target_loc));
            }
            return q;
        });
    }
    py::dict d;
    d["target_found"] = r.found;
    d["ready"] = r.ready;
    d["result"] = r.result;
    d["executed_on"] = r.executed_on;
    d["oracle_match"] = r.oracle;
    d["dispatch_wall_ms"] = r.t0;
    d["return_wall_ms"] = r.t1;
    d["x"] = x;
    return d;
}

}  // namespace

PYBIND11_MODULE(exp66_actor_ext, m) {
    m.doc() = "exp66 EXPERIMENT-ONLY in-Ray-actor connect-mode HPX host (not rayx.runtime API)";
    m.def("hpx_version_info", &ext_hpx_version_info,
          "Runtime-observed HPX build identity (callable BEFORE start; identity gate input).");
    m.def("start_connect", &ext_start_connect, py::arg("hpx_threads"), py::arg("extra_args"),
          "Join the island as a connect-mode locality on background threads of THIS process.");
    m.def("stop_disconnect", &ext_stop_disconnect,
          "Graceful leave: post(disconnect) + stop. Returns hpx::stop rc.");
    m.def("pid", &ext_pid, "This process's pid (in-process identity proof input).");
    m.def("hostname", &ext_hostname, "This process's hostname (provenance).");
    m.def("locality_id", &ext_locality_id, "This locality's id (expect > 0 for connect mode).");
    m.def("membership_count", &ext_membership_count, "find_all_localities().size() witness.");
    m.def("dispatch_probe", &ext_dispatch_probe, py::arg("x"), py::arg("target_loc"),
          py::arg("bound_s") = 10,
          "Dispatch the closed-oracle action AT target_loc; bounded wait; oracle-checked.");
    m.attr("__experiment__") = "exp66";
    // Minimal build-declaration field (design: only what Python cannot infer). This TU does
    // NOT opt out of the GIL; a future free-threaded (t-ABI) slice D build would declare
    // "mod_gil_not_used" and pass py::mod_gil_not_used() to PYBIND11_MODULE instead.
    m.attr("__gil_declaration__") = "gil_used_default";
}
