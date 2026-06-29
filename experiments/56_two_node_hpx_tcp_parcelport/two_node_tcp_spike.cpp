// exp56 -- Ray-free TWO-NODE HPX TCP parcelport connect-mode probe (standalone binary).
//
// EXPERIMENTAL, NARROW, MECHANISM-FEASIBILITY ONLY. NOT RayX production code, NOT linked into the
// `_rayx` Python extension, NOT a Ray demo. Derived from the exp49 connect-mode core but SELF-CONTAINED
// in exp56. It tests whether two plain HPX processes on TWO DIFFERENT nodes can form one HPX
// distributed runtime over the TCP parcelport (connect mode) and run one fixed registered closed-int64
// action from the root locality to the connector locality on the other node.
//
// Two roles select on --role:
//   * root    : console (AGAS root), launched with --hpx:expect-connecting-localities. Polls
//               find_all_localities() until the connector joins, invokes one dist_probe action on it,
//               structurally proves remote execution, releases the connector, observes its graceful
//               leave, then finalizes.
//   * connect : connect-mode late joiner started NON-BLOCKING via hpx::start (so it controls its own
//               leave). Attests its identity, waits to be served, then post(disconnect)+stop.
//
// REMOTE PROOF is three-way and the action stays closed-int64:
//   (1) oracle: result == (x ^ 0x52415958) + (remote_loc << 1)        [in the action return]
//   (2) locality id: remote_locality_id != root_locality_id           [from find_all_localities]
//   (3) physical host: connector hostname/endpoint != root's          [SIDE-CHANNEL attestation marker]
// (3) is carried ONLY by the per-locality self-attestation file (attest_<role>.json); the hostname/IP
// is NEVER encoded into the action result. The orchestrator compares the two attestation files.
//
// CLAIM FENCE (see two_node_hpx_tcp_parcelport.md): first two-node probe only; TCP parcelport only;
// closed-int64 action only; Ray-free; no performance/speedup/throughput/latency claim (the structural
// settle is readiness, not latency); no HPX fault tolerance; no Ray actor-failure recovery; no
// production/public API; no object store; no arbitrary Python; no Ray replacement; no general fabric
// claim from one TCP two-node probe; no MPI/LCI performance-path claim. Future distributed-fabric
// direction only.

#include <hpx/hpx_init.hpp>
#include <hpx/hpx_start.hpp>   // hpx::start (non-blocking connector path)
#include <hpx/hpx.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/runtime.hpp>
#include <hpx/include/run_as.hpp>  // hpx::run_as_hpx_thread
#include <hpx/modules/program_options.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/naming_base/id_type.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>  // getpid, gethostname

namespace {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

void write_text(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
}

std::string bquote(bool b) { return b ? "true" : "false"; }

std::string json_escape(const std::string& s) {
    std::string o;
    for (char c : s) {
        if (c == '"' || c == '\\') { o.push_back('\\'); o.push_back(c); }
        else if (c == '\n') { o += "\\n"; }
        else { o.push_back(c); }
    }
    return o;
}

std::string get_hostname() {
    char buf[256];
    if (::gethostname(buf, sizeof(buf)) == 0) { buf[sizeof(buf) - 1] = '\0'; return std::string(buf); }
    return std::string("unknown");
}

// The advertised parcelport endpoint is exactly what this process was told to bind via --hpx:hpx.
// Reading it from argv is robust and is what the connector advertises to AGAS for the root to reach.
std::string advertised_endpoint(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a.rfind("--hpx:hpx=", 0) == 0) return a.substr(10);
        if (a == "--hpx:hpx" && i + 1 < argc) return argv[i + 1];
    }
    return "";
}

// Per-locality self-attestation side marker -- FOR PROOF ONLY. Never encoded into the action result.
void write_attestation(const std::string& bootdir, const std::string& role, std::uint32_t loc,
                       long pid, const std::string& adv_endpoint) {
    std::string j = "{";
    j += "\"role\":\"" + role + "\",";
    j += "\"locality_id\":" + std::to_string(loc) + ",";
    j += "\"hostname\":\"" + json_escape(get_hostname()) + "\",";
    j += "\"advertised_hpx_endpoint\":\"" + json_escape(adv_endpoint) + "\",";
    j += "\"pid\":" + std::to_string(pid) + "}\n";
    write_text(bootdir + "/attest_" + role + ".json", j);
}

// Set in main() before hpx::init so the root's hpx_main can attest its advertised endpoint.
std::string g_adv_endpoint;

bool wait_drop_to_one(int timeout_s) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        if (hpx::find_all_localities().size() <= 1) return true;
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

}  // namespace

// ===== the one fixed registered action (identical in every locality) =======
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_probe, dist_probe_action)

// ===== root (console; admits one connector, serves one action, observes leave) =====
int run_root(std::int64_t x, const std::string& bootdir, int ready_timeout, int leave_timeout,
             std::uint32_t here, long pid) {
    write_attestation(bootdir, "root", here, pid, g_adv_endpoint);
    write_text(bootdir + "/root.ready", "ready\n");

    // Structural settle: ready -> two localities visible (NOT a latency/perf metric).
    auto t_ready = std::chrono::steady_clock::now();
    std::vector<hpx::id_type> locs;
    bool reached_two = false;
    auto deadline = t_ready + std::chrono::seconds(ready_timeout);
    while (std::chrono::steady_clock::now() < deadline) {
        locs = hpx::find_all_localities();
        if (locs.size() >= 2) { reached_two = true; break; }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    long settle_ms = reached_two
        ? std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t_ready).count()
        : -1;

    std::int64_t result = 0, oracle = 0;
    std::uint32_t remote_id = 0, executed_on = 0;
    bool invoked = false, match = false, proved_remote = false;

    if (reached_two) {
        hpx::id_type here_id = hpx::find_here();
        hpx::id_type remote;
        bool have_remote = false;
        for (auto const& l : locs) {
            if (l != here_id) { remote = l; have_remote = true; break; }
        }
        if (have_remote) {
            remote_id = hpx::naming::get_locality_id_from_id(remote);
            try {
                result = hpx::async<dist_probe_action>(remote, x).get();
                invoked = true;
                oracle = (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(remote_id) << 1);
                match = (result == oracle);
                executed_on = static_cast<std::uint32_t>((result - (x ^ DIST_PROBE_XOR)) >> 1);
                proved_remote = invoked && match && (executed_on == remote_id) && (executed_on != here);
            } catch (...) {
                invoked = false;
            }
        }
    }

    // release the connector so it can leave gracefully, then observe the drop
    if (proved_remote) write_text(bootdir + "/served1.ok", "served\n");
    bool observed_leave = reached_two ? wait_drop_to_one(leave_timeout) : false;

    std::string j = "{";
    j += "\"role\":\"root\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"localities_seen\":" + std::to_string(locs.size()) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"settle_ms\":" + std::to_string(settle_ms) + ",";
    j += "\"remote_locality\":" + std::to_string(remote_id) + ",";
    j += "\"x\":" + std::to_string(x) + ",";
    j += "\"invoked\":" + bquote(invoked) + ",";
    j += "\"result\":" + std::to_string(result) + ",";
    j += "\"oracle\":" + std::to_string(oracle) + ",";
    j += "\"match\":" + bquote(match) + ",";
    j += "\"executed_on_locality\":" + std::to_string(executed_on) + ",";
    j += "\"remote_locality_id_differs\":" + bquote(reached_two && remote_id != here) + ",";
    j += "\"proved_remote_by_oracle\":" + bquote(proved_remote) + ",";
    j += "\"observed_connector_leave\":" + bquote(observed_leave);
    j += "}\n";
    write_text(bootdir + "/root_result.json", j);

    return hpx::finalize();
}

// ===== connect (non-blocking hpx::start; attests, waits to be served, self-disconnects) =====
int run_connector_start(int argc, char** argv,
                        const hpx::program_options::options_description& desc) {
    std::string bootdir = ".";
    int serve_timeout = 30;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--bootstrap" && i + 1 < argc) bootdir = argv[++i];
        else if (a.rfind("--bootstrap=", 0) == 0) bootdir = a.substr(12);
        else if (a == "--serve-timeout" && i + 1 < argc) serve_timeout = std::atoi(argv[++i]);
        else if (a.rfind("--serve-timeout=", 0) == 0) serve_timeout = std::atoi(a.substr(16).c_str());
    }
    const std::string adv = advertised_endpoint(argc, argv);

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::connect;
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(bootdir + "/connect.joined1", "{\"started\":false}\n");
        return 2;
    }

    const long pid = static_cast<long>(::getpid());
    std::uint32_t hereloc = hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    write_attestation(bootdir, "connect", hereloc, pid, adv);
    write_text(bootdir + "/connect.joined1",
               "{\"locality_id\":" + std::to_string(hereloc) + ",\"pid\":" + std::to_string(pid) +
                   "}\n");

    // wait (bounded) to be served before leaving; main thread is not an HPX thread here
    const std::string served_path = bootdir + "/served1.ok";
    bool served = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(serve_timeout);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream f(served_path);
        if (f.good()) { served = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // graceful leave: post(disconnect) onto an HPX thread, then stop() from this main thread
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
    write_text(bootdir + "/connect.disconnected1",
               "{\"clean\":" + bquote(clean) + ",\"rc\":" + std::to_string(rc) +
                   ",\"served\":" + bquote(served) + ",\"teardown\":\"post(disconnect)+stop\"," +
                   "\"error\":\"" + json_escape(err) + "\"}\n");
    return clean ? 0 : 1;
}

int hpx_main(hpx::program_options::variables_map& vm) {
    const std::string role = vm["role"].as<std::string>();
    const std::int64_t x = vm["x"].as<std::int64_t>();
    const std::string bootdir = vm["bootstrap"].as<std::string>();
    const int ready_timeout = vm["ready-timeout"].as<int>();
    const int leave_timeout = vm["leave-timeout"].as<int>();
    const std::uint32_t here = hpx::get_locality_id();
    const long pid = static_cast<long>(::getpid());

    if (role == "root") {
        return run_root(x, bootdir, ready_timeout, leave_timeout, here, pid);
    }
    return hpx::finalize();  // unknown role: don't hang
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp56 two-node HPX TCP parcelport probe options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("root"), "root | connect")
        ("x", po::value<std::int64_t>()->default_value(7), "closed int64 action input")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "shared-FS directory for rendezvous / result / attestation files")
        ("ready-timeout", po::value<int>()->default_value(60),
            "seconds the root waits for the connector to join (generous for real network)")
        ("leave-timeout", po::value<int>()->default_value(60),
            "seconds the root waits for the connector's graceful leave")
        ("serve-timeout", po::value<int>()->default_value(60),
            "connect: seconds to wait for the served signal before leaving");
    // clang-format on

    std::string role = "root";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--role" && i + 1 < argc) role = argv[i + 1];
        else if (a.rfind("--role=", 0) == 0) role = a.substr(7);
    }
    g_adv_endpoint = advertised_endpoint(argc, argv);

    // connect drives its own lifecycle via the non-blocking hpx::start path (self-disconnect).
    if (role == "connect") {
        return run_connector_start(argc, argv, desc) == 0 ? 0 : 1;
    }

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::console;  // root: AGAS root / locality 0
    return hpx::init(argc, argv, params);
}
