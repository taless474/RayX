// exp54 -- autonomous Ray-side poisoned-island DETECTION (standalone HPX binary).
//
// EXPERIMENTAL, NARROW. NOT RayX production code, NOT linked into the `_rayx` Python extension. From
// HPX's point of view this binary contains NO new mechanism: the failure path is exp50's mid-flight
// connector loss and the clean path is exp52/exp53. exp54 is a DETECTION-LOGIC experiment: the Ray
// supervisor cannot query HPX for authoritative locality health (exp51 found no such API), so it
// infers island poisoning from OS process liveness + bounded progress/completion markers.
//
// PRIMARY CORRECTION vs exp53: island #1's connector death is OBSERVED, not CAUSED, by the
// supervisor. The connector SELF-CRASHES (std::abort) a fixed delay after join, so the supervisor
// observes an UNCAUSED death via Popen.poll() rather than confirming a death it initiated.
//
// Roles (--role; root branches on --island-mode):
//   * f_root --island-mode failure : admit connector, dispatch the long dist_sleep_probe, classify
//        it as a DIAGNOSTIC, then idle INSIDE hpx_main until SIGKILL. It writes NO clean-completion
//        marker, and MUST NOT finalize/disconnect or return normally in the expected path (a return
//        would enter finalize and hang on the dead connector -- exp51). Idle cap is a safety guard.
//   * f_root --island-mode clean   : exp52/53 clean path -- serve one closed-int64 dist_probe (fold
//        locality id in as remote-proof), write served1.ok, wait for connector locality absence
//        (graceful-leave gate), write clean_root.json ONLY on success, hpx::finalize() cleanly.
//   * f_connect --connector-kind self_crash : join, write connect.joined1, wait --crash-delay-ms,
//        then std::abort() (ungraceful SELF-crash; expected observed exit SIGABRT / signal 6).
//   * f_connect --connector-kind clean      : exp52/53 graceful path -- join, wait served1.ok, then
//        hpx::post([]{ hpx::disconnect(); }); hpx::stop();, write connect.disconnected1.
//   * f_connect --connector-kind victim     : non-primary second arm (join, idle, never disconnect;
//        expects external SIGKILL). NOT used for the headline autonomous-detection claim.
//
// CLOSED-VALUE DISCIPLINE: actions return closed int64, NEVER a managed hpx::id_type.
//
// CLAIM FENCE (see ray_autonomous_poison_detection.md): single-node; loopback TCP; closed-int64
// action only; Ray = bootstrap/supervision/restart/detection plane only; HPX = execution/data plane
// inside each island; supervisor-owned poisoned-island detection from OS process liveness + bounded
// progress markers; connector death observed, not caused by the supervisor, in the primary arm; not
// HPX fault tolerance; not in-place recovery; no AGAS stale-locality repair; no Ray
// actor-failure-recovery claim; island #2 is a fresh independent HPX runtime, not repaired island
// #1; no multi-node; no general fabric; no performance/speedup/throughput/latency; no
// production/public API; no endpoint seam; no Ray replacement; no "HPX faster than Ray"; no "RayX
// makes Ray faster".

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
#include <cstdlib>     // std::abort
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

// Long probe: writes action_started as its FIRST statement (DIAGNOSTIC only; the supervisor's
// classifier NEVER reads it), then chunk-sleeps (capped). Closed int64.
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
// Dispatch the long action, idle INSIDE hpx_main until SIGKILL. Writes NO clean-completion marker
// (so the supervisor's "clean_completion_within_T" stays false). Never finalize/return in the
// expected path. The connector SELF-CRASHES; the root just keeps running with stale state until the
// supervisor reaps it. The idle cap is a safety guard whose elapse is a flagged anomaly.
int run_failure_root(std::int64_t x, std::int64_t sleep_ms, int wait_bound, int step_timeout,
                     int idle_cap_s, std::uint32_t here, long pid) {
    write_text(g_bootdir + "/root.ready", "ready\n");

    bool reached_two = false;
    std::vector<hpx::id_type> locs = wait_two(step_timeout, reached_two);
    hpx::id_type remote;
    std::uint32_t remote_id = 0;
    bool have_remote = reached_two && find_remote(locs, remote, remote_id);

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

    // DIAGNOSTIC witness only (the supervisor's classifier does NOT read this file). Importantly,
    // this is NOT a clean-completion marker -- clean_root.json is never written in failure mode.
    std::string j = "{";
    j += "\"role\":\"f_root\",\"island_mode\":\"failure\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"connector_remote_locality\":" + int_or_null(remote_id, have_remote) + ",";
    j += "\"long_action_dispatched\":" + bquote(dispatched) + ",";
    j += "\"long_action_outcome\":\"" + outcome + "\"";
    j += "}\n";
    write_text(g_bootdir + "/failure_root_diag.json", j);

    int cap = std::min(std::max(idle_cap_s, 1), 600);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(cap);
    while (std::chrono::steady_clock::now() < deadline)
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));

    // ANOMALY: idle cap elapsed without a SIGKILL. Returning enters finalize (which hangs); the
    // runner records this as a non-clean outcome.
    write_text(g_bootdir + "/failure_root_idle_cap_elapsed", "elapsed\n");
    return hpx::finalize();  // anomaly path only; expected path never reaches here
}

// ===== clean-mode root (exp52/53) ===========================================
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
            write_text(g_bootdir + "/served1.ok", "served\n");
            wait_id_absent(remote_id, step_timeout);  // graceful-leave gate
        }
    }

    // clean-completion marker -- written ONLY on success. Its ABSENCE is the supervisor's progress
    // signal. (A non-proved run does NOT write this file.)
    if (proved) {
        std::string j = "{";
        j += "\"role\":\"f_root\",\"island_mode\":\"clean\",";
        j += "\"here_locality\":" + std::to_string(here) + ",";
        j += "\"pid\":" + std::to_string(pid) + ",";
        j += "\"connector_remote_locality\":" + std::to_string(remote_id) + ",";
        j += "\"action_outcome\":\"" + oc + "\",";
        j += "\"action_proved_remote\":true";
        j += "}\n";
        write_text(g_bootdir + "/clean_root.json", j);
    }

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
    int crash_delay_ms = 2000;
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
        else if (a == "--crash-delay-ms" && i + 1 < argc) crash_delay_ms = std::atoi(argv[++i]);
        else if (a.rfind("--crash-delay-ms=", 0) == 0) crash_delay_ms = std::atoi(a.substr(17).c_str());
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

    if (kind == "self_crash") {
        // PRIMARY ungraceful failure: the connector terminates ITSELF a fixed delay after join, so
        // the supervisor OBSERVES an uncaused death (not one it initiated). std::abort() raises
        // SIGABRT (signal 6); the runtime's background threads die with the process. NO disconnect.
        int delay = std::min(std::max(crash_delay_ms, 0), 60000);
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        std::abort();  // ungraceful self-crash; no return
    }

    if (kind == "victim") {
        // Non-primary second arm: idle, never disconnect, expects an external SIGKILL.
        int idle = std::min(std::max(victim_idle, 0), 600);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(idle);
        while (std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        write_text(bootdir + "/victim_survived_idle" + idx, "survived\n");
        hpx::post([]() { hpx::disconnect(); });
        hpx::stop();
        return 0;
    }

    // kind == clean: exp52/53 graceful path.
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
    po::options_description desc("exp54 autonomous poison-detection options");
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
            "failure-root idle SAFETY cap seconds (expected exit is SIGKILL)")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "rendezvous / result directory (single-node shared filesystem)")
        ("connector-kind", po::value<std::string>()->default_value("clean"),
            "self_crash | clean | victim (f_connect)")
        ("connector-index", po::value<int>()->default_value(1), "f_connect index")
        ("serve-timeout", po::value<int>()->default_value(25),
            "clean connector seconds to wait for its served signal")
        ("victim-idle", po::value<int>()->default_value(120),
            "victim connector idle seconds before fallback teardown (capped 600)")
        ("crash-delay-ms", po::value<int>()->default_value(2000),
            "self_crash connector delay after join before std::abort (capped 60000)");
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
