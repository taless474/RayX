// exp57 Slice 0 -- TWO-NODE HPX TCP parcelport connect-mode probe with DIAGNOSTIC TIMESTAMP MARKERS.
//
// EXPERIMENTAL, NARROW, MECHANISM-FEASIBILITY ONLY. NOT RayX production code, NOT linked into the
// `_rayx` Python extension, NOT a Ray demo. This is a byte-faithful COPY of the exp56
// two_node_tcp_spike.cpp (which is unchanged) with ADDITIVE, DIAGNOSTIC-ONLY monotonic timestamp
// markers. The closed-int64 action semantics, the three-way remote proof, the rendezvous/attestation
// files, and the connect-mode lifecycle are all IDENTICAL to exp56. Slice 0's only purpose is to
// ATTRIBUTE the ~30 s two-node readiness duration observed in exp56 (loopback was ~100 ms), so the
// added markers split the in-binary timeline into phase boundaries.
//
// CLOCK-DOMAIN DISCIPLINE (critical, see ray_slurm_supervised_two_node_island.md / settle_attribution):
//   * Every *_ms field below is a process-local steady_clock delta measured since main() entry on THIS
//     process (g_t0). The root runs on node A and the connector on node B; their clocks are NOT
//     synchronized. ROOT timings and CONNECTOR timings live in SEPARATE clock domains and MUST NOT be
//     subtracted across nodes. The authoritative settle span is the ROOT-local
//     (root_wait_two_start_ms -> root_first_observed_two_localities_ms) delta, which is single-clock.
//   * The orchestrator's marker-visibility times are a THIRD clock (the launcher's) and are recorded
//     separately in the runner; they are shared-FS visibility latencies, never AGAS settle.
//
// Two roles select on --role:
//   * root    : console (AGAS root), launched with --hpx:expect-connecting-localities. Polls
//               find_all_localities() until the connector joins, invokes one dist_probe action on it,
//               structurally proves remote execution, releases the connector, observes its graceful
//               leave, then finalizes.
//   * connect : connect-mode late joiner started NON-BLOCKING via hpx::start (so it controls its own
//               leave). Attests its identity, waits to be served, then post(disconnect)+stop.
//
// REMOTE PROOF is three-way and the action stays closed-int64 (UNCHANGED from exp56):
//   (1) oracle: result == (x ^ 0x52415958) + (remote_loc << 1)        [in the action return]
//   (2) locality id: remote_locality_id != root_locality_id           [from find_all_localities]
//   (3) physical host: connector hostname/endpoint != root's          [SIDE-CHANNEL attestation marker]
//
// CLAIM FENCE: first Ray/Slurm-supervised two-node island arc (Slice 0 is the Ray-free settle
// diagnostic; Ray is NOT present in this binary or in --phase settle-diag); TCP parcelport only;
// closed-int64 action only; no performance/speedup/throughput/latency claim (the settle is a
// STRUCTURAL READINESS duration and is SUSPICIOUS until attributed -- it must NOT be reused for
// restart/detector timing); no HPX fault tolerance; no Ray actor-failure recovery; no production/
// public API; no object store; no arbitrary Python; no Ray replacement; no general fabric claim; no
// MPI/LCI performance-path claim. Future distributed-fabric direction only.

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

#include <unistd.h>        // getpid, gethostname, close
#include <cerrno>          // errno (Slice A1 connector AGAS TCP pre-probe)
#include <fcntl.h>         // fcntl O_NONBLOCK (Slice A1 pre-probe)
#include <sys/socket.h>    // socket, connect, getsockopt (Slice A1 pre-probe)
#include <sys/select.h>    // select (Slice A1 pre-probe)
#include <netinet/in.h>    // sockaddr_in (Slice A1 pre-probe)
#include <arpa/inet.h>     // inet_pton (Slice A1 pre-probe)

namespace {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

// ---- DIAGNOSTIC-ONLY process-local monotonic clock (Slice 0 addition) ------------------------------
// g_t0 is captured at the very start of main(); every *_ms marker is a delta since g_t0 on THIS
// process. Single-clock per process; never compared across nodes. -1 means "phase not reached".
std::chrono::steady_clock::time_point g_t0;
long now_ms() {
    return static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - g_t0).count());
}
long g_hpx_main_entry_ms = -1;  // set at hpx_main entry (root path)

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

// ---- Slice A1: connector-side AGAS TCP reachability check (NO HPX runtime touched) -----------------
// One bounded non-blocking TCP connect attempt to numeric host:port. Returns true iff the AGAS
// endpoint is accepting connections. Used to wait for the root's AGAS to be listening BEFORE the
// connector calls hpx::start(connect), so HPX bootstrap is never attempted against a dead endpoint
// (it is NOT an HPX bootstrap retry). per_attempt_ms bounds a single attempt (defeats a hung connect
// on an unreachable route); ECONNREFUSED (port not up yet) returns false quickly so the caller polls.
bool tcp_can_connect(const std::string& host, int port, int per_attempt_ms = 1000) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<std::uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) { ::close(fd); return false; }
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    bool ok = false;
    int r = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (r == 0) {
        ok = true;
    } else if (errno == EINPROGRESS) {
        fd_set wf;
        FD_ZERO(&wf);
        FD_SET(fd, &wf);
        timeval tv;
        tv.tv_sec = per_attempt_ms / 1000;
        tv.tv_usec = (per_attempt_ms % 1000) * 1000;
        if (::select(fd + 1, nullptr, &wf, nullptr, &tv) > 0) {
            int soerr = 0;
            socklen_t len = sizeof(soerr);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &len) == 0 && soerr == 0) ok = true;
        }
    }
    ::close(fd);
    return ok;
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

    // DIAGNOSTIC root-local monotonic markers (Slice 0). All ms since main() entry on THIS process.
    const long t_root_ready_ms = now_ms();
    long t_wait_two_start_ms = -1, t_first_two_ms = -1;
    long t_action_dispatch_ms = -1, t_action_result_ms = -1;
    long t_observed_leave_ms = -1, t_finalize_start_ms = -1;

    // Structural settle: ready -> two localities visible (NOT a latency/perf metric).
    auto t_ready = std::chrono::steady_clock::now();
    t_wait_two_start_ms = now_ms();
    std::vector<hpx::id_type> locs;
    bool reached_two = false;
    auto deadline = t_ready + std::chrono::seconds(ready_timeout);
    while (std::chrono::steady_clock::now() < deadline) {
        locs = hpx::find_all_localities();
        if (locs.size() >= 2) { reached_two = true; break; }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (reached_two) t_first_two_ms = now_ms();
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
                t_action_dispatch_ms = now_ms();
                result = hpx::async<dist_probe_action>(remote, x).get();
                t_action_result_ms = now_ms();
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
    if (observed_leave) t_observed_leave_ms = now_ms();

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

    // DIAGNOSTIC root timing marker (Slice 0). root_finalize_done_ms is NOT capturable from inside
    // hpx_main (finalize only signals); main() records it after hpx::init returns (root_finalize_done
    // .json). All values are root-process-local ms since main() entry; clock_domain says so explicitly.
    t_finalize_start_ms = now_ms();
    std::string tj = "{";
    tj += "\"role\":\"root\",";
    tj += "\"clock_domain\":\"root_in_binary\",";
    tj += "\"reference\":\"ms since main() entry on the root process (node A)\",";
    tj += "\"hpx_main_entry_ms\":" + std::to_string(g_hpx_main_entry_ms) + ",";
    tj += "\"root_ready_ms\":" + std::to_string(t_root_ready_ms) + ",";
    tj += "\"root_wait_two_start_ms\":" + std::to_string(t_wait_two_start_ms) + ",";
    tj += "\"root_first_observed_two_localities_ms\":" + std::to_string(t_first_two_ms) + ",";
    tj += "\"root_action_dispatch_ms\":" + std::to_string(t_action_dispatch_ms) + ",";
    tj += "\"root_action_result_ms\":" + std::to_string(t_action_result_ms) + ",";
    tj += "\"root_observed_connector_leave_ms\":" + std::to_string(t_observed_leave_ms) + ",";
    tj += "\"root_finalize_start_ms\":" + std::to_string(t_finalize_start_ms) + ",";
    tj += "\"settle_ms\":" + std::to_string(settle_ms);
    tj += "}\n";
    write_text(bootdir + "/root_timing.json", tj);

    return hpx::finalize();
}

// ===== connect (non-blocking hpx::start; attests, waits to be served, self-disconnects) =====
int run_connector_start(int argc, char** argv,
                        const hpx::program_options::options_description& desc) {
    std::string bootdir = ".";
    int serve_timeout = 30;
    std::string preprobe_host;       // Slice A1: connector AGAS TCP pre-probe (opt-in)
    int preprobe_port = 0;
    int preprobe_timeout_ms = 60000;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--bootstrap" && i + 1 < argc) bootdir = argv[++i];
        else if (a.rfind("--bootstrap=", 0) == 0) bootdir = a.substr(12);
        else if (a == "--serve-timeout" && i + 1 < argc) serve_timeout = std::atoi(argv[++i]);
        else if (a.rfind("--serve-timeout=", 0) == 0) serve_timeout = std::atoi(a.substr(16).c_str());
        else if (a == "--agas-preprobe-host" && i + 1 < argc) preprobe_host = argv[++i];
        else if (a.rfind("--agas-preprobe-host=", 0) == 0) preprobe_host = a.substr(21);
        else if (a == "--agas-preprobe-port" && i + 1 < argc) preprobe_port = std::atoi(argv[++i]);
        else if (a.rfind("--agas-preprobe-port=", 0) == 0) preprobe_port = std::atoi(a.substr(21).c_str());
        else if (a == "--agas-preprobe-timeout-ms" && i + 1 < argc)
            preprobe_timeout_ms = std::atoi(argv[++i]);
        else if (a.rfind("--agas-preprobe-timeout-ms=", 0) == 0)
            preprobe_timeout_ms = std::atoi(a.substr(27).c_str());
    }
    const std::string adv = advertised_endpoint(argc, argv);

    // DIAGNOSTIC connector-local monotonic markers (Slice 0). Connector has NO hpx_main (it uses
    // hpx::start); the closest safe local points are recorded with that limitation noted in the JSON.
    long t_join_issued_ms = -1, t_joined_ms = -1, t_served_ms = -1;
    long t_disconnect_start_ms = -1, t_disconnect_done_ms = -1;

    // ---- Slice A1: connector-side AGAS TCP pre-probe BEFORE hpx::start(connect) ----------------------
    // OPT-IN (preserves Slice 0 behavior when the flags are absent). Bounded plain-TCP poll to the
    // root's numeric AGAS endpoint so HPX bootstrap is never attempted against a not-yet-listening
    // AGAS. This is the substitute for shared-FS readiness gating; it is NOT an HPX bootstrap retry.
    bool preprobe_active = (!preprobe_host.empty() && preprobe_port > 0);
    bool agas_preprobe_ok = false;
    bool agas_preprobe_timeout = false;
    long agas_preprobe_ms = -1;
    if (preprobe_active) {
        const long t_pp0 = now_ms();
        const auto pp_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(preprobe_timeout_ms);
        while (std::chrono::steady_clock::now() < pp_deadline) {
            if (tcp_can_connect(preprobe_host, preprobe_port)) { agas_preprobe_ok = true; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(150));  // bounded poll interval
        }
        agas_preprobe_ms = now_ms() - t_pp0;
        agas_preprobe_timeout = !agas_preprobe_ok;
        std::string pj = "{";
        pj += "\"agas_preprobe_ok\":" + bquote(agas_preprobe_ok) + ",";
        pj += "\"agas_preprobe_timeout\":" + bquote(agas_preprobe_timeout) + ",";
        pj += "\"agas_preprobe_ms\":" + std::to_string(agas_preprobe_ms) + ",";
        pj += "\"host\":\"" + json_escape(preprobe_host) + "\",";
        pj += "\"port\":" + std::to_string(preprobe_port) + ",";
        pj += "\"timeout_ms\":" + std::to_string(preprobe_timeout_ms);
        pj += "}\n";
        write_text(bootdir + "/connect_preprobe.json", pj);
        if (agas_preprobe_ok) {
            write_text(bootdir + "/connect.preprobe_ok", "ok\n");
        } else {
            // bounded pre-probe timed out -> DO NOT call hpx::start; exit cleanly nonzero.
            write_text(bootdir + "/connect.joined1",
                       "{\"started\":false,\"reason\":\"agas_preprobe_timeout\"}\n");
            return 3;
        }
    }

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::connect;
    t_join_issued_ms = now_ms();  // closest equivalent: hpx::start initiates the connect-mode join
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(bootdir + "/connect.joined1", "{\"started\":false}\n");
        return 2;
    }

    const long pid = static_cast<long>(::getpid());
    std::uint32_t hereloc = hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    t_joined_ms = now_ms();  // runtime is up and this process has a locality id (it is part of AGAS)
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
    if (served) t_served_ms = now_ms();

    // ---- graceful leave: the PROVEN HPX idiom -- post(disconnect) then stop() ------------------------
    // REVERTED from the Slice A1 await-disconnect-before-stop attempt: a Rostam settle-diag run showed
    // that awaiting hpx::disconnect() before hpx::stop() DEADLOCKS. hpx::disconnect() and hpx::stop()
    // are coupled -- the posted disconnect needs shutdown progress driven by hpx::stop() on the main
    // thread, so blocking main to wait for disconnect completion is a circular wait (it hit the bound,
    // forced stop, and marked the leave unclean). Authoritative clean-leave confirmation is the
    // ROOT-side observed_connector_leave (AGAS-level, cross-node), NOT a connector-local flag. We only
    // record a lightweight "disconnect initiated" stamp; we do NOT claim connector-local completion.
    int rc = 0;
    bool clean = true;
    std::string err;
    t_disconnect_start_ms = now_ms();
    write_text(bootdir + "/connect.disconnect_initiated.json",
               "{\"disconnect_initiated_ms\":" + std::to_string(t_disconnect_start_ms) +
                   ",\"teardown\":\"post(disconnect)+stop\",\"note\":\"connector-local INITIATION only; "
                   "authoritative graceful-leave confirmation is the root-side observed_connector_leave "
                   "(AGAS/cross-node). HPX has no public disconnect-future and disconnect/stop are "
                   "coupled, so connector-local completion is not claimed.\"}\n");
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
    t_disconnect_done_ms = now_ms();
    write_text(bootdir + "/connect.disconnected1",
               "{\"clean\":" + bquote(clean) + ",\"rc\":" + std::to_string(rc) +
                   ",\"served\":" + bquote(served) + ",\"teardown\":\"post(disconnect)+stop\"," +
                   "\"error\":\"" + json_escape(err) + "\"}\n");

    // DIAGNOSTIC connector timing marker (Slice 0). SEPARATE clock domain from the root (node B
    // process clock). Field names follow the plan; hpx_main_entry_ms / connector_join_issued_ms map
    // to the closest safe local point because the connector path has no hpx_main.
    std::string tj = "{";
    tj += "\"role\":\"connect\",";
    tj += "\"clock_domain\":\"connector_in_binary\",";
    tj += "\"reference\":\"ms since main() entry on the connector process (node B); NOT comparable "
          "to the root clock\",";
    tj += "\"hpx_main_entry_ms\":" + std::to_string(t_join_issued_ms) + ",";
    tj += "\"connector_join_issued_ms\":" + std::to_string(t_join_issued_ms) + ",";
    tj += "\"connector_joined_ms\":" + std::to_string(t_joined_ms) + ",";
    tj += "\"connector_served_ms\":" + std::to_string(t_served_ms) + ",";
    tj += "\"connector_disconnect_start_ms\":" + std::to_string(t_disconnect_start_ms) + ",";
    tj += "\"connector_disconnect_done_ms\":" + std::to_string(t_disconnect_done_ms) + ",";
    tj += "\"agas_preprobe_active\":" + bquote(preprobe_active) + ",";
    tj += "\"agas_preprobe_ok\":" + bquote(agas_preprobe_ok) + ",";
    tj += "\"agas_preprobe_timeout\":" + bquote(agas_preprobe_timeout) + ",";
    tj += "\"agas_preprobe_ms\":" + std::to_string(agas_preprobe_ms) + ",";
    tj += "\"limitation\":\"connect uses hpx::start (no hpx_main); hpx_main_entry_ms and "
          "connector_join_issued_ms are the post-start initiation point, connector_joined_ms is when "
          "a locality id was first observable\"";
    tj += "}\n";
    write_text(bootdir + "/connect_timing.json", tj);
    return clean ? 0 : 1;
}

int hpx_main(hpx::program_options::variables_map& vm) {
    g_hpx_main_entry_ms = now_ms();  // DIAGNOSTIC: root-local hpx_main entry (Slice 0)
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
    g_t0 = std::chrono::steady_clock::now();  // DIAGNOSTIC: process-local monotonic reference (Slice 0)
    namespace po = hpx::program_options;
    po::options_description desc("exp57 two-node HPX TCP parcelport probe options (Slice 0 diagnostic)");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("root"), "root | connect")
        ("x", po::value<std::int64_t>()->default_value(7), "closed int64 action input")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "shared-FS directory for rendezvous / result / attestation / timing files")
        ("ready-timeout", po::value<int>()->default_value(60),
            "seconds the root waits for the connector to join (generous for real network)")
        ("leave-timeout", po::value<int>()->default_value(60),
            "seconds the root waits for the connector's graceful leave")
        ("serve-timeout", po::value<int>()->default_value(60),
            "connect: seconds to wait for the served signal before leaving")
        ("agas-preprobe-host", po::value<std::string>()->default_value(""),
            "connect (Slice A1): AGAS IP to bounded TCP pre-probe before hpx::start(connect); "
            "opt-in -- absent keeps Slice 0 behavior")
        ("agas-preprobe-port", po::value<int>()->default_value(0),
            "connect (Slice A1): AGAS port for the TCP pre-probe")
        ("agas-preprobe-timeout-ms", po::value<int>()->default_value(60000),
            "connect (Slice A1): bounded TCP pre-probe timeout in ms");
    // clang-format on

    std::string role = "root";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--role" && i + 1 < argc) role = argv[i + 1];
        else if (a.rfind("--role=", 0) == 0) role = a.substr(7);
    }
    g_adv_endpoint = advertised_endpoint(argc, argv);

    // bootdir for the post-init root_finalize_done marker (parsed here so main() can write it)
    std::string bootdir = ".";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--bootstrap" && i + 1 < argc) bootdir = argv[i + 1];
        else if (a.rfind("--bootstrap=", 0) == 0) bootdir = a.substr(12);
    }

    // connect drives its own lifecycle via the non-blocking hpx::start path (self-disconnect).
    if (role == "connect") {
        return run_connector_start(argc, argv, desc) == 0 ? 0 : 1;
    }

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::console;  // root: AGAS root / locality 0
    int rc = hpx::init(argc, argv, params);

    // DIAGNOSTIC: root_finalize_done is only observable AFTER hpx::init returns (the runtime has fully
    // stopped). Written here as a small separate marker; same root_in_binary clock domain.
    if (role == "root") {
        write_text(bootdir + "/root_finalize_done.json",
                   "{\"clock_domain\":\"root_in_binary\",\"root_finalize_done_ms\":" +
                       std::to_string(now_ms()) + ",\"init_rc\":" + std::to_string(rc) + "}\n");
    }
    return rc;
}
