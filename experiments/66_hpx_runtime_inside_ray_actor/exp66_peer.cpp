// exp66 -- standalone EXPERIMENT-ONLY island peers for the HPX-inside-one-Ray-actor probe.
//
// TWO roles in one binary (exp65 pattern):
//   * --role root    WORK-FREE island root (console mode / AGAS root / locality 0), separately
//                    supervised by the Python controller. It hosts NO application work: its
//                    hpx_main only heartbeats root.alive, observes membership counts
//                    (introspection, not work), and finalizes on the root.done completion
//                    sentinel -- the exp63/64/65 user-space lifecycle protocol. The controller
//                    verifies no probe action ever executed on locality 0 (oracle-by-value).
//   * --role prober  Standalone connect-mode locality. On the controller's per-phase trigger
//                    files it dispatches the fixed exp66 pid/oracle actions AT the Ray-actor
//                    locality (read from actor.loc) and writes one result JSON per phase --
//                    the external evidence that the actor-hosted runtime progresses while the
//                    actor's Python thread is idle and while it is busy (GIL held).
//
// Wait discipline: bounded wait_for, .get() ONLY when ready (exp50). This experiment REQUIRES
// the verified waiter-fix build (exp64 Slice 5V); the runtime-observed HPX identity is written
// into every marker so the controller can assert it. Durations are observational only.
//
// NOT production code, NOT rayx.runtime API, no performance/ratio/winner claim, no inference.

#include <hpx/hpx.hpp>
#include <hpx/hpx_init.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/version.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <vector>

#include "probe_action.hpp"  // ONE TU per binary: registers exp66_probe_action / exp66_pid_action

namespace {

long long wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void write_text(const std::string& path, const std::string& content) {
    std::ofstream f(path + ".tmp", std::ios::trunc);
    f << content;
    f.close();
    ::rename((path + ".tmp").c_str(), path.c_str());
}

bool file_exists(const std::string& p) {
    struct stat st{};
    return ::stat(p.c_str(), &st) == 0;
}

long long mtime_ns(const std::string& p) {
    struct stat st{};
    if (::stat(p.c_str(), &st) != 0) return -1;
#ifdef __APPLE__
    return static_cast<long long>(st.st_mtimespec.tv_sec) * 1000000000LL +
           st.st_mtimespec.tv_nsec;
#else
    return static_cast<long long>(st.st_mtim.tv_sec) * 1000000000LL + st.st_mtim.tv_nsec;
#endif
}

std::string jesc(const std::string& s) {
    std::string o;
    for (char c : s) {
        if (c == '"' || c == '\\') { o += '\\'; o += c; }
        else if (c == '\n') o += "\\n";
        else o += c;
    }
    return o;
}

std::string host_name() {
    char buf[256] = {0};
    if (::gethostname(buf, sizeof(buf) - 1) == 0) return std::string(buf);
    return "unknown";
}

std::string stamp(const std::string& who) {
    std::ostringstream o;
    o << "\"who\":\"" << who << "\",\"pid\":" << static_cast<long>(::getpid())
      << ",\"hostname\":\"" << jesc(host_name()) << "\",\"wall_ms\":" << wall_ms()
      << ",\"hpx_complete_version\":\"" << jesc(hpx::complete_version()) << "\"";
    return o.str();
}

std::string g_bootdir = ".";
int g_leave_timeout_s = 20;

// ---- root role (WORK-FREE; hpx::init/hpx_main path) --------------------------------------

int root_hpx_main(hpx::program_options::variables_map&) {
    const std::string ready = g_bootdir + "/root.ready";
    const std::string alive = g_bootdir + "/root.alive";
    const std::string done = g_bootdir + "/root.done";
    write_text(ready, "{" + stamp("root") + ",\"locality_id\":" +
                          std::to_string(hpx::get_locality_id()) + "}\n");
    std::size_t max_membership = 1;
    // Heartbeat + membership WITNESS only (introspection). No application work, no probe
    // action targets this locality; the controller verifies that by oracle value.
    // On root.done the root does NOT finalize immediately: it keeps heartbeating and waits
    // (bounded) for every connector to leave gracefully -- the exp65 root ordering, so
    // finalize never races a connector's post(disconnect)+stop.
    bool done_seen = false;
    bool leave_observed = false;
    auto leave0 = std::chrono::steady_clock::now();
    for (;;) {
        write_text(alive, "alive\n");
        std::size_t n = hpx::find_all_localities().size();
        if (n > max_membership) max_membership = n;
        if (!done_seen && file_exists(done)) {
            done_seen = true;
            leave0 = std::chrono::steady_clock::now();
        }
        if (done_seen) {
            if (n == 1) {
                leave_observed = true;
                break;
            }
            if (std::chrono::steady_clock::now() - leave0 >
                std::chrono::seconds(g_leave_timeout_s)) {
                break;  // bounded: never hang on a connector that fails to leave
            }
        }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    std::size_t final_membership = hpx::find_all_localities().size();
    write_text(g_bootdir + "/root.final",
               "{" + stamp("root") + ",\"max_membership\":" + std::to_string(max_membership) +
                   ",\"final_membership\":" + std::to_string(final_membership) +
                   ",\"leave_observed\":" + (leave_observed ? "true" : "false") + "}\n");
    return hpx::finalize();
}

// ---- prober role (connect mode; hpx::start, main thread stays non-HPX) --------------------

hpx::id_type find_locality(std::uint32_t loc) {
    for (auto const& id : hpx::find_all_localities()) {
        if (hpx::naming::get_locality_id_from_id(id) == loc) return id;
    }
    return hpx::invalid_id;
}

// One phase: dispatch pid + oracle actions AT the actor locality; bounded waits; result JSON.
std::string run_phase(const std::string& phase, std::uint32_t actor_loc, std::int64_t x) {
    return hpx::run_as_hpx_thread([&]() -> std::string {
        std::ostringstream o;
        o << "{" << stamp("prober") << ",\"phase\":\"" << phase << "\""
          << ",\"actor_locality\":" << actor_loc;
        hpx::id_type target = find_locality(actor_loc);
        if (target == hpx::invalid_id) {
            o << ",\"ok\":false,\"error\":\"actor locality not found\"}";
            return o.str();
        }
        const long long t0 = wall_ms();
        auto fpid = hpx::async<exp66_pid_action>(target);
        auto fval = hpx::async<exp66_probe_action>(target, x);
        bool ready_pid = fpid.wait_for(std::chrono::seconds(10)) == hpx::future_status::ready;
        bool ready_val = fval.wait_for(std::chrono::seconds(10)) == hpx::future_status::ready;
        const long long t1 = wall_ms();
        std::int64_t pid_result = ready_pid ? fpid.get() : -1;   // .get() only when ready
        std::int64_t val_result = ready_val ? fval.get() : -1;
        std::uint32_t executed_on =
            ready_val ? exp66::executed_locality_from(val_result, x) : 0xFFFFFFFFu;
        bool oracle_match = ready_val && val_result == exp66::probe_value(x, actor_loc);
        o << ",\"x\":" << x << ",\"dispatch_wall_ms\":" << t0 << ",\"return_wall_ms\":" << t1
          << ",\"both_ready\":" << ((ready_pid && ready_val) ? "true" : "false")
          << ",\"pid_result\":" << pid_result << ",\"probe_result\":" << val_result
          << ",\"executed_on\":" << executed_on
          << ",\"oracle_match\":" << (oracle_match ? "true" : "false")
          << ",\"ok\":" << ((ready_pid && ready_val && oracle_match) ? "true" : "false") << "}";
        return o.str();
    });
}

int run_prober(int argc, char** argv, const hpx::program_options::options_description& desc,
               int deadman_s, std::int64_t x) {
    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::connect;
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(g_bootdir + "/prober.joined", "{" + stamp("prober") + ",\"started\":false}\n");
        return 2;
    }
    std::uint32_t here = hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    write_text(g_bootdir + "/prober.joined",
               "{" + stamp("prober") + ",\"locality_id\":" + std::to_string(here) +
                   ",\"started\":true}\n");

    const std::string done = g_bootdir + "/root.done";
    const std::string alive = g_bootdir + "/root.alive";
    std::vector<std::string> pending = {"idle", "busy"};
    std::string reason = "unknown";
    long long last_alive = mtime_ns(alive);
    auto dead0 = std::chrono::steady_clock::now();
    for (;;) {
        for (auto it = pending.begin(); it != pending.end();) {
            const std::string go = g_bootdir + "/probe_" + *it + ".go";
            if (file_exists(go)) {
                // actor.loc: the Ray actor's locality id, written by the controller.
                std::uint32_t actor_loc = 0;
                std::ifstream f(g_bootdir + "/actor.loc");
                f >> actor_loc;
                write_text(g_bootdir + "/probe_" + *it + ".result",
                           run_phase(*it, actor_loc, x) + "\n");
                it = pending.erase(it);
            } else {
                ++it;
            }
        }
        if (file_exists(done)) { reason = "root_completion_signal"; break; }
        long long m = mtime_ns(alive);
        if (m != last_alive) { last_alive = m; dead0 = std::chrono::steady_clock::now(); }
        if (std::chrono::steady_clock::now() - dead0 > std::chrono::seconds(deadman_s)) {
            reason = "deadman_timeout";
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    // Graceful leave: exp49-proven post(disconnect)+stop idiom.
    std::string err;
    bool clean = true;
    try {
        hpx::post([]() { hpx::disconnect(); });
    } catch (std::exception const& e) { clean = false; err = e.what(); }
    int rc = hpx::stop();
    write_text(g_bootdir + "/prober.disconnected",
               "{" + stamp("prober") + ",\"clean\":" + (clean && rc == 0 ? "true" : "false") +
                   ",\"shutdown_reason\":\"" + reason + "\",\"stop_rc\":" + std::to_string(rc) +
                   ",\"teardown\":\"post(disconnect)+stop\",\"error\":\"" + jesc(err) + "\"}\n");
    return (clean && rc == 0 && reason == "root_completion_signal") ? 0 : 3;
}

}  // namespace

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp66_peer options");
    desc.add_options()
        ("role", po::value<std::string>()->default_value("root"), "root | prober")
        ("bootstrap", po::value<std::string>()->default_value("."), "rendezvous dir")
        ("deadman", po::value<int>()->default_value(40), "prober: root.alive stall deadman (s)")
        ("leave-timeout", po::value<int>()->default_value(20),
         "root: bounded wait (s) for connectors to leave after root.done")
        ("x", po::value<long long>()->default_value(7), "prober: closed-oracle input");

    std::string role = "root";
    std::string boot = ".";
    int deadman = 40;
    int leave_timeout = 20;
    long long x = 7;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto val = [&](const char* k) -> const char* {
            std::string kk = std::string("--") + k + "=";
            if (a.rfind(kk, 0) == 0) return a.c_str() + kk.size();
            if (a == std::string("--") + k && i + 1 < argc) return argv[++i];
            return nullptr;
        };
        if (const char* v = val("role")) role = v;
        else if (const char* v = val("bootstrap")) boot = v;
        else if (const char* v = val("deadman")) deadman = std::atoi(v);
        else if (const char* v = val("leave-timeout")) leave_timeout = std::atoi(v);
        else if (const char* v = val("x")) x = std::atoll(v);
    }
    g_bootdir = boot;
    g_leave_timeout_s = leave_timeout;

    if (role == "prober") {
        return run_prober(argc, argv, desc, deadman, x);
    }
    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::console;  // WORK-FREE root: AGAS root / locality 0
    return hpx::init(&root_hpx_main, argc, argv, params);
}
