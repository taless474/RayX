// exp53 -- Ray-supervised HPX island RESTART under the whole-island-fatal policy (standalone HPX
// binary).
//
// EXPERIMENTAL, NARROW. NOT RayX production code, NOT linked into the `_rayx` Python extension. From
// HPX's point of view this binary contains NO new mechanism: the failure path is exp50's mid-flight
// connector loss, and the clean path is exp52's connect-mode clean island. exp53 only proves, at the
// Ray level, that a durable supervisor can DISCARD a poisoned HPX island and launch a FRESH clean
// one. Island #1 and island #2 are TWO INDEPENDENT HPX runtimes, not one runtime recovering.
//
// One binary, two roles selected by --role; the root branches on --island-mode:
//   * f_root --island-mode failure : admit the victim connector, invoke the long dist_sleep_probe,
//        classify the long-action future (the loss WITNESS), write failure_root.json, then idle
//        INSIDE hpx_main until the supervisor SIGKILLs it. It MUST NOT call hpx::finalize() /
//        hpx::disconnect() and MUST NOT return normally from hpx_main in the expected path -- a
//        return would enter finalize and HANG on the dead connector (exp51), confounding the probe.
//        The idle cap (120 s) is a SAFETY guard only; the expected exit is SIGKILL (signal 9).
//   * f_root --island-mode clean   : exp52 clean path -- admit connector, serve one closed-int64
//        dist_probe (folding locality id in as remote-proof), write served1.ok, wait for connector
//        locality absence (graceful-leave gate), write clean_root.json, hpx::finalize() cleanly.
//   * f_connect --connector-kind victim : join, write connect.joined1, run connect-mode, NEVER
//        disconnect -- expects to be SIGKILLed.
//   * f_connect --connector-kind clean  : exp52 graceful path -- join, wait served1.ok, then
//        hpx::post([]{ hpx::disconnect(); }); hpx::stop();, write connect.disconnected1.
//
// CLOSED-VALUE DISCIPLINE: actions return closed int64, NEVER a managed hpx::id_type, so no
// global-reference decref parcel is owed back at shutdown.
//
// CLAIM FENCE (see ray_supervised_island_restart.md): single-node; loopback TCP; closed-int64 action
// only; Ray = bootstrap/supervision/restart plane only; HPX = execution/data plane inside each
// island; whole-island-fatal policy exercised; NOT HPX fault tolerance; not in-place recovery; no
// AGAS stale-locality repair; no Ray actor-failure-recovery claim beyond this controlled supervisor
// kill/restart; island #2 is a FRESH independent HPX runtime, not repaired island #1; no multi-node;
// no general fabric; no performance/speedup/throughput/latency; no production/public API; no endpoint
// seam; no Ray replacement; no "HPX faster than Ray"; no "RayX makes Ray faster".

#include <hpx/hpx_init.hpp>
#include <hpx/hpx_start.hpp>   // hpx::start (non-blocking connector path)
#include <hpx/hpx.hpp>
#include <hpx/future.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/runtime.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/modules/program_options.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/runtime_local/runtime_local_fwd.hpp>
#include <hpx/naming_base/id_type.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>  // getpid

namespace {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

std::string g_bootdir = ".";

void write_text(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
}

std::string bquote(bool b) { return b ? "true" : "false"; }

std::string int_or_null(long v, bool present) {
    return present ? std::to_string(v) : std::string("null");
}

}  // namespace

// ===== fixed registered actions (closed int64 -> int64) ====================
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_probe, dist_probe_action)

// Long probe: writes action_started as its FIRST statement so the supervisor can SIGKILL the
// connector while the body is provably executing on it; then chunk-sleeps (capped). Closed int64.
std::int64_t dist_sleep_probe(std::int64_t x, std::int64_t millis) {
    std::uint32_t loc = hpx::get_locality_id();
    write_text(g_bootdir + "/action_started",
               std::string("{\"locality_id\":") + std::to_string(loc) +
                   ",\"pid\":" + std::to_string(static_cast<long>(::getpid())) + "}\n");
    std::int64_t remaining = std::min<std::int64_t>(std::max<std::int64_t>(millis, 0), 60000);
    while (remaining > 0) {
        std::int64_t step = std::min<std::int64_t>(remaining, 100);
        hpx::this_thread::sleep_for(std::chrono::milliseconds(step));
        remaining -= step;
    }
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_sleep_probe, dist_sleep_probe_action)

// ===== root-side helpers ===================================================

std::vector<hpx::id_type> wait_two(int timeout_s, bool& reached_two) {
    std::vector<hpx::id_type> locs;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        locs = hpx::find_all_localities();
        if (locs.size() >= 2) { reached_two = true; return locs; }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    reached_two = false;
    return locs;
}

// Graceful-leave gate (clean island): the root must observe the connector's disconnect before it
// finalizes, otherwise it could wedge on collective shutdown (the exp50 failure mode).
void wait_id_absent(std::uint32_t id, int timeout_s) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        bool present = false;
        for (auto const& l : hpx::find_all_localities())
            if (hpx::naming::get_locality_id_from_id(l) == id) { present = true; break; }
        if (!present) return;
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

bool find_remote(const std::vector<hpx::id_type>& locs, hpx::id_type& remote, std::uint32_t& rid) {
    hpx::id_type here_id = hpx::find_here();
    for (auto const& l : locs) {
        if (l != here_id) {
            remote = l;
            rid = hpx::naming::get_locality_id_from_id(l);
            return true;
        }
    }
    return false;
}

// ===== failure-mode root ====================================================
// Admit victim, dispatch the long action, CLASSIFY the loss (the witness), write failure_root.json,
// then idle INSIDE hpx_main until SIGKILL. Never finalize; never return normally in the expected
// path. The idle cap is a safety guard only -- if it elapses we flag the anomaly and let hpx_main
// return (which will then finalize-hang and be killed), so a missed kill is VISIBLE, not silent.
int run_failure_root(std::int64_t x, std::int64_t sleep_ms, int wait_bound, int step_timeout,
                     int idle_cap_s, std::uint32_t here, long pid) {
    write_text(g_bootdir + "/root.ready", "ready\n");

    bool reached_two = false;
    std::vector<hpx::id_type> locs = wait_two(step_timeout, reached_two);
    hpx::id_type remote;
    std::uint32_t remote_id = 0;
    bool have_remote = reached_two && find_remote(locs, remote, remote_id);

    // Dispatch the long action; bounded wait -> classify. The connector is SIGKILLed mid-flight by
    // the supervisor once action_started appears. exp50 Case A: this typically TIMES OUT at the
    // bound (no exception enum). A timed_out / threw outcome is the loss WITNESS.
    std::string outcome = "no_remote";
    bool dispatched = false;
    if (have_remote) {
        dispatched = true;
        hpx::future<std::int64_t> f = hpx::async<dist_sleep_probe_action>(remote, x, sleep_ms);
        if (f.wait_for(std::chrono::seconds(wait_bound)) != hpx::future_status::ready) {
            outcome = "timed_out";
        } else {
            try { (void) f.get(); outcome = "returned"; }
            catch (...) { outcome = "threw"; }
        }
    }
    // loss observed iff the long-action future did NOT return normally (mid-flight loss landed).
    bool loss_observed = dispatched && (outcome != "returned");

    std::string j = "{";
    j += "\"role\":\"f_root\",\"island_mode\":\"failure\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"connector_remote_locality\":" + int_or_null(remote_id, have_remote) + ",";
    j += "\"long_action_dispatched\":" + bquote(dispatched) + ",";
    j += "\"long_action_outcome\":\"" + outcome + "\",";
    j += "\"loss_observed_by_root\":" + bquote(loss_observed);
    j += "}\n";
    write_text(g_bootdir + "/failure_root.json", j);  // <-- the witness the supervisor waits for

    // Idle INSIDE hpx_main until SIGKILL. We deliberately DO NOT finalize/disconnect: a collective
    // shutdown against the dead connector would hang (exp51). The supervisor kills us mid-idle.
    int cap = std::min(std::max(idle_cap_s, 1), 600);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(cap);
    while (std::chrono::steady_clock::now() < deadline)
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));

    // ANOMALY: idle cap elapsed without a SIGKILL. Flag it so a missed kill is visible. Returning
    // here enters finalize (which will hang) -- recorded as a non-clean outcome by the runner.
    write_text(g_bootdir + "/failure_root_idle_cap_elapsed", "elapsed\n");
    return hpx::finalize();  // anomaly path only; expected path never reaches here
}

// ===== clean-mode root (exp52) ==============================================
int run_clean_root(std::int64_t x, int wait_bound, int step_timeout, std::uint32_t here, long pid) {
    write_text(g_bootdir + "/root.ready", "ready\n");

    bool reached_two = false;
    std::vector<hpx::id_type> locs = wait_two(step_timeout, reached_two);
    hpx::id_type remote;
    std::uint32_t remote_id = 0;
    bool have_remote = reached_two && find_remote(locs, remote, remote_id);

    bool served = false, proved = false;
    std::string oc = "no_remote";
    if (have_remote) {
        hpx::future<std::int64_t> f = hpx::async<dist_probe_action>(remote, x);
        if (f.wait_for(std::chrono::seconds(wait_bound)) == hpx::future_status::ready) {
            try {
                std::int64_t r = f.get();
                std::int64_t oracle =
                    (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(remote_id) << 1);
                served = true;
                proved = (r == oracle) && (remote_id != here);
                oc = "returned";
            } catch (...) { oc = "threw"; }
        } else {
            oc = "timed_out";
        }
        if (proved) {
            write_text(g_bootdir + "/served1.ok", "served\n");  // connector waits on this
            wait_id_absent(remote_id, step_timeout);            // graceful-leave gate
        }
    }

    std::string j = "{";
    j += "\"role\":\"f_root\",\"island_mode\":\"clean\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"connector_remote_locality\":" + int_or_null(remote_id, have_remote) + ",";
    j += "\"action_outcome\":\"" + oc + "\",";
    j += "\"action_served\":" + bquote(served) + ",";
    j += "\"action_proved_remote\":" + bquote(proved);
    j += "}\n";
    write_text(g_bootdir + "/clean_root.json", j);

    return hpx::finalize();  // clean path: connector already left -> clean finalize
}

// ===== connector (hpx::start non-blocking) =================================
int run_connector(int argc, char** argv,
                  const hpx::program_options::options_description& desc) {
    std::string bootdir = ".";
    std::string kind = "clean";
    int index = 1;
    int serve_timeout = 25;
    int victim_idle = 120;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--bootstrap" && i + 1 < argc) bootdir = argv[++i];
        else if (a.rfind("--bootstrap=", 0) == 0) bootdir = a.substr(12);
        else if (a == "--connector-kind" && i + 1 < argc) kind = argv[++i];
        else if (a.rfind("--connector-kind=", 0) == 0) kind = a.substr(17);
        else if (a == "--connector-index" && i + 1 < argc) index = std::atoi(argv[++i]);
        else if (a.rfind("--connector-index=", 0) == 0) index = std::atoi(a.substr(18).c_str());
        else if (a == "--serve-timeout" && i + 1 < argc) serve_timeout = std::atoi(argv[++i]);
        else if (a.rfind("--serve-timeout=", 0) == 0) serve_timeout = std::atoi(a.substr(16).c_str());
        else if (a == "--victim-idle" && i + 1 < argc) victim_idle = std::atoi(argv[++i]);
        else if (a.rfind("--victim-idle=", 0) == 0) victim_idle = std::atoi(a.substr(14).c_str());
    }
    g_bootdir = bootdir;
    const std::string idx = std::to_string(index);

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::connect;
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(bootdir + "/connect.joined" + idx, "{\"started\":false}\n");
        return 2;
    }

    const long pid = static_cast<long>(::getpid());
    std::uint32_t hereloc = hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    write_text(bootdir + "/connect.joined" + idx,
               "{\"index\":" + idx + ",\"pid\":" + std::to_string(pid) +
                   ",\"locality_id\":" + std::to_string(hereloc) +
                   ",\"kind\":\"" + kind + "\"}\n");

    if (kind == "victim") {
        // Ungraceful-loss subject: stay alive (the runtime services the long action on background
        // HPX threads) and NEVER disconnect -- the supervisor SIGKILLs this process. The cap bounds
        // a runaway; if the kill somehow does not land, fall back to a teardown and flag it.
        int idle = std::min(std::max(victim_idle, 0), 600);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(idle);
        while (std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        write_text(bootdir + "/victim_survived_idle" + idx, "survived\n");
        hpx::post([]() { hpx::disconnect(); });
        hpx::stop();
        return 0;
    }

    // kind == clean: exp52 graceful path.
    const std::string served_path = bootdir + "/served" + idx + ".ok";
    bool served = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(serve_timeout);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream f(served_path);
        if (f.good()) { served = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    int rc = 0;
    bool clean = true;
    std::string err;
    try {
        hpx::post([]() { hpx::disconnect(); });
        rc = hpx::stop();
    } catch (const std::exception& e) {
        clean = false;
        err = e.what();
    } catch (...) {
        clean = false;
        err = "unknown";
    }
    write_text(bootdir + "/connect.disconnected" + idx,
               "{\"index\":" + idx + ",\"clean\":" + bquote(clean) +
                   ",\"rc\":" + std::to_string(rc) + ",\"served\":" + bquote(served) +
                   ",\"teardown\":\"post(disconnect)+stop\",\"error\":\"" + err + "\"}\n");
    return clean ? 0 : 1;
}

int hpx_main(hpx::program_options::variables_map& vm) {
    const std::string mode = vm["island-mode"].as<std::string>();
    const std::int64_t x = vm["x"].as<std::int64_t>();
    const std::int64_t sleep_ms = vm["sleep-ms"].as<std::int64_t>();
    const int wait_bound = vm["wait-bound"].as<int>();
    const int step_timeout = vm["step-timeout"].as<int>();
    const int idle_cap = vm["idle-cap"].as<int>();
    const std::uint32_t here = hpx::get_locality_id();
    const long pid = static_cast<long>(::getpid());

    if (mode == "failure")
        return run_failure_root(x, sleep_ms, wait_bound, step_timeout, idle_cap, here, pid);
    return run_clean_root(x, wait_bound, step_timeout, here, pid);
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp53 ray-supervised island-restart options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("f_root"), "f_root | f_connect")
        ("island-mode", po::value<std::string>()->default_value("clean"), "failure | clean (f_root)")
        ("x", po::value<std::int64_t>()->default_value(7), "closed int64 action input")
        ("sleep-ms", po::value<std::int64_t>()->default_value(8000),
            "dist_sleep_probe duration (capped 60000)")
        ("wait-bound", po::value<int>()->default_value(15),
            "bounded wait_for seconds for the action future")
        ("step-timeout", po::value<int>()->default_value(20),
            "seconds to wait for join / graceful-leave steps")
        ("idle-cap", po::value<int>()->default_value(120),
            "failure-root idle SAFETY cap seconds before anomaly fallback (expected exit is SIGKILL)")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "rendezvous / result directory (single-node shared filesystem)")
        ("connector-kind", po::value<std::string>()->default_value("clean"),
            "victim | clean (f_connect)")
        ("connector-index", po::value<int>()->default_value(1), "f_connect index")
        ("serve-timeout", po::value<int>()->default_value(25),
            "clean connector seconds to wait for its served signal")
        ("victim-idle", po::value<int>()->default_value(120),
            "victim connector idle seconds before fallback teardown (capped 600)");
    // clang-format on

    std::string role = "f_root";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--role" && i + 1 < argc) role = argv[i + 1];
        else if (a.rfind("--role=", 0) == 0) role = a.substr(7);
        else if (a == "--bootstrap" && i + 1 < argc) g_bootdir = argv[i + 1];
        else if (a.rfind("--bootstrap=", 0) == 0) g_bootdir = a.substr(12);
    }

    if (role == "f_connect") {
        return run_connector(argc, argv, desc) == 0 ? 0 : 1;
    }

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::console;  // f_root: AGAS root / locality 0
    return hpx::init(argc, argv, params);
}
