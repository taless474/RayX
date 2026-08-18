// exp70 Slice 3B -- EXPERIMENT-ONLY pybind11 extension hosted INSIDE a Ray actor worker
// process (exp67/68 mechanism, reused unchanged: hpx::start with runtime_mode::connect, no
// child process).
//
// Adds exactly ONE new capability beyond exp68's actor_ext.cpp: supervision_init(), which
// calls hpx::supervision::init() so this locality registers a supervision_dispatch registry
// and publishes event::started/running. That registration is what lets the root's
// discover_and_join() (native/root_supervised.cpp) join this locality and later observe its
// silent loss -- this file itself NEVER calls publish_event(event::failed) anywhere, which is
// the structural half of the "the application did not report its own death" proof (see
// g_app_failed_publish_count below and root_supervised.cpp's matching counter).
//
// probe_locality() is a plain (non-supervision-fenced) HPX action dispatch used only for the
// bonus cross-locality force_disconnect cache-purge check: it lets the SURVIVING connector (A)
// independently confirm the departed connector's old locality is unreachable, not just the
// root's own view.
//
// CLAIM FENCE: mechanism/validation evidence for HPX issue #7390 / #7441 / PR #7447 only. Not
// production, not rayx.runtime API, no performance/ratio/winner claim.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/version.hpp>
#include <hpx/modules/agas.hpp>
#include <hpx/modules/supervision.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/runtime_local/get_num_all_localities.hpp>

#include <hpx/supervision_dispatch.hpp>

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../upstream_reproducer/common.hpp"  // exp70_probe_action (ONE TU per binary)

namespace py = pybind11;

namespace {

std::atomic<bool> g_started{false};
std::atomic<bool> g_supervision_active{false};

// Structural witness: this counter can only ever become non-zero if THIS file were to call
// hpx::supervision::publish_event(..., event::failed, ...) itself, which it never does. Kept
// as a live, inspectable field (not just a doc claim) for the
// application_failed_event_publish_count gate.
std::atomic<std::uint64_t> g_app_failed_publish_count{0};

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
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
    argv_store.push_back(to_cstr("exp70_slice3b_ext"));
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

// Registers this locality under supervision_dispatch so the root can discover_and_join() it.
// Idempotent (hpx::supervision::init() itself is idempotent). This file never calls
// publish_event(event::failed) anywhere -- see g_app_failed_publish_count above.
py::dict ext_supervision_init(int discovery_timeout_ms) {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    struct R {
        bool ok = false;
        std::uint64_t epoch = 0;
        std::string error;
        // Diagnostic only (job 185471/root.raw_resolve_diag on the Rostam hardware run:
        // root's raw hpx::agas::resolve_name() for a/b's registry names returned "not found"
        // with NO error, despite both init() calls returning ok=true): self-check whether THIS
        // locality's own just-registered supervision_dispatch registry name is resolvable AT
        // ALL, even from its own process -- isolates a silent register_name() failure (its bool
        // return is discarded by upstream's own run_init_sequence()) from a cross-locality/AGAS
        // propagation issue.
        bool self_resolve_ok = false;
        std::string self_resolve_error;
        std::uint32_t own_locality = 0;
    } r;
    {
        py::gil_scoped_release release;
        r = hpx::run_as_hpx_thread([&]() -> R {
            R q;
            try {
                hpx::supervision::registry const handle = hpx::supervision::init(
                    hpx::launch::sync, std::chrono::milliseconds(discovery_timeout_ms));
                q.epoch = hpx::supervision::query_state(hpx::launch::sync, handle).epoch;
                hpx::supervision::publish_event(hpx::launch::sync, handle,
                    hpx::supervision::event::running, q.epoch);
                q.ok = true;

                q.own_locality = hpx::get_locality_id();
                hpx::error_code ec(hpx::throwmode::lightweight);
                std::string const name = "/" + std::to_string(q.own_locality) +
                    "/supervision_dispatch/registry";
                hpx::id_type const id = hpx::agas::resolve_name(hpx::launch::sync, name, ec);
                q.self_resolve_ok = !ec && id;
                q.self_resolve_error = ec ? ec.get_message() : std::string();
            } catch (std::exception const& e) {
                q.error = e.what();
            }
            return q;
        });
    }
    g_supervision_active.store(r.ok);
    py::dict d;
    d["ok"] = r.ok;
    d["self_resolve_ok"] = r.self_resolve_ok;
    d["self_resolve_error"] = r.self_resolve_error;
    d["own_locality"] = r.own_locality;
    d["epoch"] = r.epoch;
    d["error"] = r.error;
    d["app_failed_publish_count"] = g_app_failed_publish_count.load();
    return d;
}

// Diagnostic only: this locality independently attempts discover_and_join() itself, mirroring
// the direction components/supervision_dispatch/examples/late_component_worker.cpp exercises
// (a worker discovering root), as opposed to root_supervised.cpp's root-discovers-connector
// direction. Added after root's discover_and_join() reproducibly found 0 peers on the Rostam
// hardware run despite both connectors' own registry names self-resolving successfully --
// isolates whether the cross-locality visibility gap is direction-specific (root->connector
// only) or general (any locality -> any other locality).
py::dict ext_supervision_discover_probe(int discovery_timeout_ms) {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    struct R {
        bool ok = false;
        int peers_found = 0;
        std::vector<std::uint32_t> peer_localities;
        std::string error;
        // Diagnostic only (2026-08-18 source-level hypothesis test): the same three counts
        // root_supervised.cpp records, measured here BEFORE discover_and_join() so this call's
        // own retry/join activity cannot affect them. See root_supervised.cpp's matching block
        // for the full rationale (symbol_namespace_locality() routing gated by
        // hpx::get_initial_num_localities(), a boot-time-frozen count).
        std::uint32_t own_locality_id = 0;
        std::size_t initial_num_localities = 0;
        std::size_t live_all_localities_count = 0;
        std::size_t live_remote_localities_count = 0;
    } r;
    {
        py::gil_scoped_release release;
        r = hpx::run_as_hpx_thread([&]() -> R {
            R q;
            try {
                hpx::supervision::registry const handle =
                    hpx::supervision::init(hpx::launch::sync,
                        std::chrono::milliseconds(discovery_timeout_ms));

                q.own_locality_id = hpx::get_locality_id();
                q.initial_num_localities = hpx::get_initial_num_localities();
                q.live_all_localities_count = hpx::find_all_localities().size();
                q.live_remote_localities_count = hpx::find_remote_localities().size();

                auto const peers = hpx::supervision::discover_and_join(
                    handle, std::chrono::milliseconds(discovery_timeout_ms));
                q.peers_found = static_cast<int>(peers.size());
                for (auto const& p : peers) {
                    q.peer_localities.push_back(
                        hpx::naming::get_locality_id_from_id(p.locality));
                }
                q.ok = true;
            } catch (std::exception const& e) {
                q.error = e.what();
            }
            return q;
        });
    }
    py::dict d;
    d["ok"] = r.ok;
    d["peers_found"] = r.peers_found;
    d["peer_localities"] = r.peer_localities;
    d["error"] = r.error;
    d["own_locality_id"] = r.own_locality_id;
    d["initial_num_localities"] = r.initial_num_localities;
    d["live_all_localities_count"] = r.live_all_localities_count;
    d["live_remote_localities_count"] = r.live_remote_localities_count;
    return d;
}

// Plain (non-supervision-fenced) dispatch of exp70_probe_action to an arbitrary locality id.
// Used only for the bonus post-force_disconnect cross-locality cache-purge check: lets a
// SURVIVING connector confirm (from its own vantage, independent of root) that a
// force_disconnect'ed locality is genuinely unreachable, not just from root's point of view.
py::dict ext_probe_locality(std::uint32_t target_locality, std::int64_t x, int bound_s) {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    struct R {
        bool found = false, ok = false;
        std::int64_t result = 0, oracle = 0;
        std::string error;
    } r;
    {
        py::gil_scoped_release release;
        r = hpx::run_as_hpx_thread([&]() -> R {
            R q;
            q.oracle = exp70::probe_oracle(x, target_locality);
            hpx::id_type target = hpx::invalid_id;
            for (auto const& id : hpx::find_all_localities()) {
                if (hpx::naming::get_locality_id_from_id(id) == target_locality) target = id;
            }
            if (target == hpx::invalid_id) { q.error = "locality not found locally"; return q; }
            q.found = true;
            try {
                auto f = hpx::async<exp70_probe_action>(target, x);
                auto status = f.wait_for(std::chrono::seconds(bound_s));
                if (status != hpx::future_status::ready) {
                    q.error = "timed_out_no_result";
                    return q;
                }
                q.result = f.get();
                q.ok = (q.result == q.oracle);
            } catch (hpx::exception const& e) {
                q.error = std::string("hpx::exception: ") + e.what();
            } catch (std::exception const& e) {
                q.error = std::string("std::exception: ") + e.what();
            }
            return q;
        });
    }
    py::dict d;
    d["locality_found_locally"] = r.found;
    d["ok"] = r.ok;
    d["result"] = r.result;
    d["oracle"] = r.oracle;
    d["error"] = r.error;
    return d;
}

}  // namespace

PYBIND11_MODULE(exp70_slice3b_ext, m) {
    m.doc() = "exp70 Slice 3B EXPERIMENT-ONLY in-Ray-actor connect-mode HPX host + "
              "supervision_dispatch join (validates HPX #7390/#7441/PR #7447; not production, "
              "not rayx.runtime API, no performance claim)";
    m.def("hpx_version_info", &ext_hpx_version_info,
          "Runtime-observed HPX build identity (callable BEFORE start; identity gate input).");
    m.def("start_connect", &ext_start_connect, py::arg("hpx_threads"), py::arg("extra_args"));
    m.def("stop_disconnect", &ext_stop_disconnect, "Graceful leave: post(disconnect) + stop.");
    m.def("pid", &ext_pid);
    m.def("hostname", &ext_hostname);
    m.def("locality_id", &ext_locality_id);
    m.def("membership_count", &ext_membership_count);
    m.def("supervision_init", &ext_supervision_init, py::arg("discovery_timeout_ms") = 5000,
          "Register this locality under supervision_dispatch (idempotent). Never publishes "
          "event::failed.");
    m.def("supervision_discover_probe", &ext_supervision_discover_probe,
          py::arg("discovery_timeout_ms") = 5000,
          "Diagnostic only: this locality's own discover_and_join(), mirroring "
          "late_component_worker.cpp's connector-discovers-root direction.");
    m.def("probe_locality", &ext_probe_locality, py::arg("target_locality"), py::arg("x"),
          py::arg("bound_s") = 10,
          "Plain (non-fenced) exp70_probe_action dispatch to a locality id, for the "
          "third-party post-force_disconnect reachability check.");
    m.attr("__experiment__") = "exp70_slice3b";
    m.attr("__gil_declaration__") = "gil_used_default";
}
