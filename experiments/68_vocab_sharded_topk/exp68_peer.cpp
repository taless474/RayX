// exp68 -- standalone EXPERIMENT-ONLY island root for the vocab-sharded top-k probe.
//
// ONE role: --role root  WORK-FREE island root (console mode / AGAS root / locality 0), separately
//   supervised by the Python controller. It hosts NO application work: its hpx_main only heartbeats
//   root.alive, observes membership counts (introspection, not work; expect it to reach 3 = root +
//   actor A + actor B), and finalizes on the root.done completion sentinel -- the exp63/64/65/66/67
//   user-space lifecycle protocol. On root.done it does NOT finalize immediately: it keeps
//   heartbeating and waits (bounded) for EVERY connector to leave gracefully (membership back to 1)
//   so finalize never races an actor's post(disconnect)+stop. The controller verifies BY VALUE that
//   no application (top-k) action ever executed on locality 0.
//
// exp68 has NO prober role: the two Ray-actor localities are the top-k endpoints, and the
// load-bearing proof is actor-to-actor (A<->B) sharded top-k, driven from the actors' runtimes.
//
// Registers the exp68 actions (ONE TU) so the root binary matches the island's action table.
// REQUIRES the verified waiter-fix HPX build. Durations observational only. NOT production code.

#include <hpx/hpx.hpp>
#include <hpx/hpx_init.hpp>
#include <hpx/version.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <sys/stat.h>

#include "topk_action.hpp"  // ONE TU per binary: registers exp68 topk/pid actions

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

int root_hpx_main(hpx::program_options::variables_map&) {
    const std::string ready = g_bootdir + "/root.ready";
    const std::string alive = g_bootdir + "/root.alive";
    const std::string done = g_bootdir + "/root.done";
    write_text(ready, "{" + stamp("root") + ",\"locality_id\":" +
                          std::to_string(hpx::get_locality_id()) + "}\n");
    std::size_t max_membership = 1;
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
            if (n == 1) { leave_observed = true; break; }
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

}  // namespace

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp68_peer options");
    desc.add_options()
        ("role", po::value<std::string>()->default_value("root"), "root (only supported role)")
        ("bootstrap", po::value<std::string>()->default_value("."), "rendezvous dir")
        ("leave-timeout", po::value<int>()->default_value(20),
         "root: bounded wait (s) for connectors to leave after root.done");

    std::string role = "root";
    std::string boot = ".";
    int leave_timeout = 20;
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
        else if (const char* v = val("leave-timeout")) leave_timeout = std::atoi(v);
    }
    g_bootdir = boot;
    g_leave_timeout_s = leave_timeout;

    if (role != "root") {
        std::fprintf(stderr, "exp68_peer: unsupported role '%s' (only 'root')\n", role.c_str());
        return 2;
    }
    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::console;  // WORK-FREE root: AGAS root / locality 0
    return hpx::init(&root_hpx_main, argc, argv, params);
}
