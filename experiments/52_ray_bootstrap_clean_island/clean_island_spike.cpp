// exp52 -- Ray-orchestrated HPX bootstrap, CLEAN-PATH island (standalone HPX binary).
//
// EXPERIMENTAL, NARROW. NOT RayX production code, NOT linked into the `_rayx` Python extension.
// This binary is a THIN reuse of the exp49/51 connect-mode CLEAN path. From HPX's point of view
// this is the SAME mechanism already validated in exp49 -- HPX cannot observe whether its parent
// process is a shell, a Python runner, or a Ray actor. exp52 only adds that a Ray actor is the
// launcher/supervisor (the role mpirun / srun / hpxrun.py normally play). The HPX action still
// travels HPX->HPX over the parcelport; Ray carries only bootstrap metadata (AGAS endpoint, ports,
// rendezvous dir, role/index, timeouts) and never the action result.
//
// One binary, two roles selected by --role:
//   * f_root    : hpx::init/hpx_main console (AGAS root, locality 0). Admits one connector, serves
//                 one closed-int64 dist_probe (folding the executing locality id in as remote-proof),
//                 writes served1.ok, waits for the connector locality to be absent (graceful leave),
//                 then hpx::finalize() and exits cleanly.
//   * f_connect : hpx::start(nullptr) connect-mode locality (--connector-kind clean). Writes
//                 connect.joined1, waits for served1.ok, then leaves via the exp49 empirical
//                 teardown `hpx::post([]{ hpx::disconnect(); }); hpx::stop();`, writes
//                 connect.disconnected1, exits cleanly.
//
// CLOSED-VALUE DISCIPLINE: the action returns a closed int64, NEVER a managed hpx::id_type, so no
// global-reference decref parcel is owed back at shutdown -- the clean teardown stays clean.
//
// CLAIM FENCE (see ray_bootstrap_clean_island.md): single-node; loopback TCP; closed-int64 action
// only; Ray = bootstrap/supervision plane only; HPX = execution/data plane inside one island; clean
// path only; whole-island-fatal policy assumed, NOT exercised; no failure injection; no endpoint
// seam; no production/public API; no performance/speedup/throughput/latency; no multi-node; no
// general fabric; no fault tolerance; no Ray replacement; no "HPX faster than Ray"; no "RayX makes
// Ray faster".

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

// ===== fixed registered action (closed int64 -> int64) =====================
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_probe, dist_probe_action)

// ===== root-side helpers ===================================================

// Bounded-poll until at least two localities are visible (the connector has joined).
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

// Bounded-poll until a specific locality id is no longer visible (graceful leave gate). This is
// LOAD-BEARING: the root must observe the connector's disconnect before it finalizes, otherwise it
// could wedge on collective shutdown (the exp50 failure mode). The clean path waits here first.
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

bool invoke_short(hpx::id_type remote, std::uint32_t remote_id, std::int64_t x, int wait_bound_s,
                  std::string& outcome) {
    hpx::future<std::int64_t> f = hpx::async<dist_probe_action>(remote, x);
    if (f.wait_for(std::chrono::seconds(wait_bound_s)) != hpx::future_status::ready) {
        outcome = "timed_out";
        return false;
    }
    try {
        std::int64_t r = f.get();
        std::int64_t oracle = (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(remote_id) << 1);
        outcome = "returned";
        return r == oracle;
    } catch (...) {
        outcome = "threw";
        return false;
    }
}

int run_f_root(std::int64_t x, int wait_bound, int step_timeout, std::uint32_t here, long pid) {
    write_text(g_bootdir + "/root.ready", "ready\n");

    bool reached_two = false;
    std::vector<hpx::id_type> locs = wait_two(step_timeout, reached_two);

    hpx::id_type remote;
    std::uint32_t remote_id = 0;
    bool have_remote = false;
    if (reached_two) {
        hpx::id_type here_id = hpx::find_here();
        for (auto const& l : locs) {
            if (l != here_id) {
                remote = l;
                remote_id = hpx::naming::get_locality_id_from_id(l);
                have_remote = true;
                break;
            }
        }
    }

    bool served = false, proved = false;
    std::string oc = "no_remote";
    if (have_remote) {
        bool match = invoke_short(remote, remote_id, x, wait_bound, oc);
        served = (oc == "returned");
        proved = served && match && (remote_id != here);
        if (proved) {
            write_text(g_bootdir + "/served1.ok", "served\n");  // connector waits on this
            wait_id_absent(remote_id, step_timeout);            // graceful-leave gate
        }
    }

    std::string j = "{";
    j += "\"role\":\"f_root\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"connector_remote_locality\":" + int_or_null(remote_id, have_remote) + ",";
    j += "\"action_outcome\":\"" + oc + "\",";
    j += "\"action_served\":" + bquote(served) + ",";
    j += "\"action_proved_remote\":" + bquote(proved);
    j += "}\n";
    write_text(g_bootdir + "/root_result.json", j);

    return hpx::finalize();  // clean path: connector already left -> clean finalize
}

// ===== connector (hpx::start non-blocking; drives its own lifecycle) =======
int run_connector(int argc, char** argv,
                  const hpx::program_options::options_description& desc) {
    std::string bootdir = ".";
    std::string kind = "clean";
    int index = 1;
    int serve_timeout = 25;
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

    // Wait for the root's served signal, then leave via the exp49 empirical teardown.
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
    const std::int64_t x = vm["x"].as<std::int64_t>();
    const int wait_bound = vm["wait-bound"].as<int>();
    const int step_timeout = vm["step-timeout"].as<int>();
    const std::uint32_t here = hpx::get_locality_id();
    const long pid = static_cast<long>(::getpid());
    return run_f_root(x, wait_bound, step_timeout, here, pid);
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp52 ray-bootstrap clean-island options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("f_root"), "f_root | f_connect")
        ("x", po::value<std::int64_t>()->default_value(7), "closed int64 action input")
        ("wait-bound", po::value<int>()->default_value(15),
            "bounded wait_for seconds for the action future")
        ("step-timeout", po::value<int>()->default_value(20),
            "seconds to wait for join / graceful-leave steps")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "rendezvous / result directory (single-node shared filesystem)")
        ("connector-kind", po::value<std::string>()->default_value("clean"),
            "clean (f_connect)")
        ("connector-index", po::value<int>()->default_value(1), "f_connect index")
        ("serve-timeout", po::value<int>()->default_value(25),
            "connector seconds to wait for its served signal");
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
