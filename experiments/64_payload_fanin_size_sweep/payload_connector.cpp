// exp64 payload_connector -- EXPERIMENT-ONLY standalone HPX connect-mode locality for the Slice 1
// payload-fanin mechanism smoke.
//
// COPIED/RENAMED from exp63 collective_connector.cpp (exp63 -> exp64, exp63_leaf_action ->
// exp64_payload_leaf_action, EXP63_* -> EXP64_*). Self-contained under exp64; includes NO exp62/exp63
// header.
//
// ROLE: this binary is a REMOTE locality (node B / node C). The exp64 ROOT is the embedded HPX runtime
// inside the Python process (payload_ext), NOT a binary. This connector joins the root's AGAS in
// runtime_mode::connect, registers the SAME exp64_payload_leaf_action (via payload_action.hpp), and
// stays alive as a served remote locality the root dispatches N payload leaf actions to. It does NO
// timing of its own: the single measurement clock lives on the Python caller side.
//
// It preserves the exp61/62/63 connector role: connect-mode start, optional bounded AGAS TCP
// pre-probe, shared-FS attestation + rendezvous, post(disconnect)+stop clean leave, Linux-only
// getsockopt(TCP_NODELAY) + sched_getaffinity attestation. NOT linked into python/CMakeLists.txt,
// links no RayX/production code, NO Ray, adds NO distributed action to any shipped rayx.runtime API.
//
// CLAIM FENCE: Slice 1 mechanism only. Being a reachable remote locality is a MECHANISM, not a
// performance result. No same-axis comparison, no speedup, no ratio, no "HPX beats Ray". The closed
// oracle proves intended closed-value execution on a locality id, NOT physical node placement by
// itself (placement proof needs hostnames/node ids/hard gates on the runner side).

#include <hpx/hpx_start.hpp>
#include <hpx/hpx.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/modules/program_options.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>        // getpid, gethostname, close
#include <cerrno>          // errno (AGAS TCP pre-probe)
#include <fcntl.h>         // fcntl O_NONBLOCK
#include <sys/socket.h>    // socket, connect, getsockopt, getpeername
#include <sys/select.h>    // select
#include <netinet/in.h>    // sockaddr_in
#include <netinet/tcp.h>   // TCP_NODELAY
#include <arpa/inet.h>     // inet_pton, inet_ntop
#include <ifaddrs.h>       // getifaddrs (self-detect this node's selected-subnet IP)
#include <dirent.h>        // opendir/readdir /proc/self/fd (enumerate live sockets)
#include <sys/stat.h>      // fstat / S_ISSOCK (socket-fd filter)
#include <sched.h>         // sched_getaffinity / cpu_set_t (effective-affinity attestation)

#include "payload_action.hpp"  // defines + HPX_PLAIN_ACTION-registers exp64_payload_leaf_action (ONE TU)

#ifndef EXP64_BUILD_TYPE
#define EXP64_BUILD_TYPE "unknown"
#endif
#ifndef EXP64_COMPILER_ID
#define EXP64_COMPILER_ID "unknown"
#endif
#ifndef EXP64_COMPILER_VERSION
#define EXP64_COMPILER_VERSION "unknown"
#endif

namespace {

using clk = std::chrono::steady_clock;

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

// Self-detect THIS node's first IPv4 whose dotted prefix matches `prefix` (e.g. "10.42.5."). The
// connector runs on the remote node, so this is node B's own IP -- NOT the root IP. Empty when nothing
// matches (then we do not inject --hpx:hpx).
std::string first_ip_for_subnet(const std::string& prefix) {
    if (prefix.empty()) return "";
    struct ifaddrs* ifaddr = nullptr;
    if (::getifaddrs(&ifaddr) != 0) return "";
    std::string found;
    for (struct ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr || ifa->ifa_addr->sa_family != AF_INET) continue;
        char buf[INET_ADDRSTRLEN] = {0};
        auto* sin = reinterpret_cast<sockaddr_in*>(ifa->ifa_addr);
        if (::inet_ntop(AF_INET, &sin->sin_addr, buf, sizeof(buf)) != nullptr) {
            std::string ip(buf);
            if (ip.rfind(prefix, 0) == 0) { found = ip; break; }
        }
    }
    ::freeifaddrs(ifaddr);
    return found;
}

bool argv_has_hpx_endpoint(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--hpx:hpx" || a.rfind("--hpx:hpx=", 0) == 0) return true;
    }
    return false;
}

// connect-side AGAS TCP reachability check (bounded, non-blocking).
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

std::string advertised_endpoint(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a.rfind("--hpx:hpx=", 0) == 0) return a.substr(10);
        if (a == "--hpx:hpx" && i + 1 < argc) return argv[i + 1];
    }
    return "";
}

std::string ip_of_endpoint(const std::string& ep) {
    auto pos = ep.rfind(':');
    return (pos == std::string::npos) ? ep : ep.substr(0, pos);
}

// REAL TCP_NODELAY attestation: enumerate THIS process's own fds (/proc/self/fd, Linux), keep TCP
// sockets whose PEER IP is the HPX root parcelport/AGAS host, run getsockopt(TCP_NODELAY) on each.
// FAIL-CLOSED on non-Linux, no match, or getsockopt failure. We never fabricate a "verified" value.
struct nodelay_attestation {
    bool attested = false;
    bool nodelay = false;
    int matched = 0;
    std::string source;
    std::string scope = "hpx_root_parcelport_peer_sockets";
    std::string error;
};

std::string effective_cpuset_array() {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (::sched_getaffinity(0, sizeof(set), &set) != 0) return "";
    std::string a = "[";
    bool first = true;
    for (int c = 0; c < CPU_SETSIZE; ++c) {
        if (CPU_ISSET(c, &set)) {
            if (!first) a += ",";
            a += std::to_string(c);
            first = false;
        }
    }
    a += "]";
    return a;
#else
    return "";
#endif
}

nodelay_attestation probe_peer_nodelay(const std::string& peer_ip) {
    nodelay_attestation r;
    if (peer_ip.empty()) {
        r.error = "no peer ip to match (pass --agas-preprobe-host)";
        return r;
    }
#if defined(__linux__)
    DIR* d = ::opendir("/proc/self/fd");
    if (d == nullptr) { r.error = "opendir /proc/self/fd failed"; return r; }
    int agg = -1;  // tri-state: -1 none yet, 1 all-nodelay, 0 at-least-one-without
    struct dirent* ent = nullptr;
    while ((ent = ::readdir(d)) != nullptr) {
        int fd = std::atoi(ent->d_name);
        if (fd <= 2) continue;
        struct stat st {};
        if (::fstat(fd, &st) != 0 || !S_ISSOCK(st.st_mode)) continue;
        sockaddr_in peer {};
        socklen_t plen = sizeof(peer);
        if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &plen) != 0) continue;
        if (peer.sin_family != AF_INET) continue;
        char buf[INET_ADDRSTRLEN] = {0};
        if (::inet_ntop(AF_INET, &peer.sin_addr, buf, sizeof(buf)) == nullptr) continue;
        if (std::string(buf) != peer_ip) continue;
        int val = 0;
        socklen_t vlen = sizeof(val);
        if (::getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, &vlen) != 0) continue;
        ++r.matched;
        int nd = (val != 0) ? 1 : 0;
        agg = (agg < 0) ? nd : (agg & nd);
    }
    ::closedir(d);
    if (r.matched > 0) {
        r.attested = true;
        r.nodelay = (agg == 1);
        r.source = "getsockopt";
    } else {
        r.error = "no established TCP socket to peer " + peer_ip + " found at attest time";
    }
#else
    (void) peer_ip;
    r.error = "nodelay getsockopt probe is linux-only (/proc/self/fd)";
#endif
    return r;
}

}  // namespace

// connect: join the root's AGAS, attest as the remote locality, wait for the root's served/leave
// marker (or serve-timeout), then disconnect cleanly. This is the ONLY role of this binary.
int run_connector(int argc, char** argv,
                  const hpx::program_options::options_description& desc) {
    std::string bootdir = ".";
    int serve_timeout = 120;
    std::string preprobe_host;
    int preprobe_port = 0;
    int preprobe_timeout_ms = 60000;
    std::string prefer_subnet;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--bootstrap" && i + 1 < argc) bootdir = argv[++i];
        else if (a.rfind("--bootstrap=", 0) == 0) bootdir = a.substr(12);
        else if (a == "--serve-timeout" && i + 1 < argc) serve_timeout = std::atoi(argv[++i]);
        else if (a.rfind("--serve-timeout=", 0) == 0) serve_timeout = std::atoi(a.substr(16).c_str());
        else if (a == "--prefer-subnet" && i + 1 < argc) prefer_subnet = argv[++i];
        else if (a.rfind("--prefer-subnet=", 0) == 0) prefer_subnet = a.substr(16);
        else if (a == "--agas-preprobe-host" && i + 1 < argc) preprobe_host = argv[++i];
        else if (a.rfind("--agas-preprobe-host=", 0) == 0) preprobe_host = a.substr(21);
        else if (a == "--agas-preprobe-port" && i + 1 < argc) preprobe_port = std::atoi(argv[++i]);
        else if (a.rfind("--agas-preprobe-port=", 0) == 0) preprobe_port = std::atoi(a.substr(21).c_str());
        else if (a == "--agas-preprobe-timeout-ms" && i + 1 < argc)
            preprobe_timeout_ms = std::atoi(argv[++i]);
        else if (a.rfind("--agas-preprobe-timeout-ms=", 0) == 0)
            preprobe_timeout_ms = std::atoi(a.substr(27).c_str());
    }

    // if --prefer-subnet is set and the launcher did NOT already pin --hpx:hpx, self-detect THIS
    // node's matching IP (NOT the root IP) and inject --hpx:hpx=<own_ip>:0 so the connector binds its
    // own interface. Augmented argv goes to HPX; original argv stays intact for everything else.
    std::vector<std::string> own_args(argv, argv + argc);
    std::string self_ip;
    bool self_injected = false;
    if (!prefer_subnet.empty() && !argv_has_hpx_endpoint(argc, argv)) {
        self_ip = first_ip_for_subnet(prefer_subnet);
        if (!self_ip.empty()) {
            own_args.push_back("--hpx:hpx=" + self_ip + ":0");
            self_injected = true;
        }
    }
    std::vector<char*> aug_argv;
    aug_argv.reserve(own_args.size() + 1);
    for (auto& s : own_args) aug_argv.push_back(const_cast<char*>(s.c_str()));
    aug_argv.push_back(nullptr);
    int aug_argc = static_cast<int>(own_args.size());

    const std::string adv = advertised_endpoint(aug_argc, aug_argv.data());
    const std::string adv_ip = self_injected ? self_ip : ip_of_endpoint(adv);

    // optional bounded TCP pre-probe of the root AGAS endpoint before hpx::start(connect), so an
    // unreachable root fails fast and clean instead of hanging the join.
    bool preprobe_active = (!preprobe_host.empty() && preprobe_port > 0);
    bool agas_preprobe_ok = false, agas_preprobe_timeout = false;
    if (preprobe_active) {
        const auto pp_deadline = clk::now() + std::chrono::milliseconds(preprobe_timeout_ms);
        while (clk::now() < pp_deadline) {
            if (tcp_can_connect(preprobe_host, preprobe_port)) { agas_preprobe_ok = true; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
        agas_preprobe_timeout = !agas_preprobe_ok;
        if (!agas_preprobe_ok) {
            write_text(bootdir + "/connect.joined1",
                       "{\"started\":false,\"reason\":\"agas_preprobe_timeout\"}\n");
            return 3;
        }
        write_text(bootdir + "/connect.preprobe_ok", "ok\n");
    }

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::connect;
    if (!hpx::start(nullptr, aug_argc, aug_argv.data(), params)) {
        write_text(bootdir + "/connect.joined1", "{\"started\":false}\n");
        return 2;
    }

    const long pid = static_cast<long>(::getpid());
    std::uint32_t hereloc = hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    const std::string conn_cpuset_eff = effective_cpuset_array();

    auto write_attest = [&](const nodelay_attestation& nd) {
        std::string peer = preprobe_host;
        if (preprobe_port > 0) peer += ":" + std::to_string(preprobe_port);
        std::string j = "{";
        j += "\"role\":\"connect\",";
        j += "\"locality_id\":" + std::to_string(hereloc) + ",";
        j += "\"hostname\":\"" + json_escape(get_hostname()) + "\",";
        j += "\"advertised_hpx_endpoint\":\"" + json_escape(adv) + "\",";
        j += "\"advertised_ip\":\"" + json_escape(adv_ip) + "\",";
        j += "\"prefer_subnet\":\"" + json_escape(prefer_subnet) + "\",";
        j += "\"self_bound_own_subnet_ip\":" + bquote(self_injected) + ",";
        j += "\"action_registration_name\":\"exp64_payload_leaf_action\",";
        j += "\"connector_cpuset_effective\":" +
             (conn_cpuset_eff.empty() ? std::string("null") : conn_cpuset_eff) + ",";
        j += "\"connector_affinity_source\":\"" +
             std::string(conn_cpuset_eff.empty() ? "" : "sched_getaffinity") + "\",";
        j += "\"tcp_nodelay_attested\":" + bquote(nd.attested) + ",";
        j += "\"tcp_nodelay\":" + bquote(nd.nodelay) + ",";
        j += "\"tcp_nodelay_source\":\"" +
             json_escape(nd.attested ? nd.source : std::string("")) + "\",";
        j += "\"tcp_nodelay_scope\":\"" + json_escape(nd.scope) + "\",";
        j += "\"tcp_nodelay_matched_sockets\":" + std::to_string(nd.matched) + ",";
        j += "\"tcp_nodelay_peer\":\"" + json_escape(peer) + "\",";
        j += "\"tcp_nodelay_error\":\"" + json_escape(nd.error) + "\",";
        j += "\"hpx_parcelport_transport\":\"" +
             std::string(nd.matched > 0 ? "tcp" : "unknown") + "\",";
        j += "\"pid\":" + std::to_string(pid) + "}\n";
        write_text(bootdir + "/attest_connect.json", j);
    };

    {
        nodelay_attestation pre;
        pre.error = "pre-serve: parcelport connection not warm yet";
        write_attest(pre);
    }
    write_text(bootdir + "/connect.joined1",
               "{\"locality_id\":" + std::to_string(hereloc) + ",\"pid\":" + std::to_string(pid) +
                   ",\"hostname\":\"" + json_escape(get_hostname()) + "\"}\n");

    // stay alive as a served remote locality until the root signals done (served1.ok) or timeout.
    const std::string served_path = bootdir + "/served1.ok";
    bool served = false;
    auto deadline = clk::now() + std::chrono::seconds(serve_timeout);
    while (clk::now() < deadline) {
        std::ifstream f(served_path);
        if (f.good()) { served = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // serve-end: the root dispatched the leaf actions during timing, so the parcelport peer connection
    // is warm -- sample the REAL TCP_NODELAY on it now and overwrite the attestation.
    write_attest(probe_peer_nodelay(preprobe_host));

    int rc = 0;
    bool clean = true;
    std::string err;
    try {
        hpx::post([]() { hpx::disconnect(); });
        rc = hpx::stop();
    } catch (const std::exception& e) { clean = false; err = e.what(); }
    catch (...) { clean = false; err = "unknown"; }
    write_text(bootdir + "/connect.disconnected1",
               "{\"clean\":" + bquote(clean) + ",\"rc\":" + std::to_string(rc) +
                   ",\"served\":" + bquote(served) + ",\"teardown\":\"post(disconnect)+stop\"," +
                   "\"agas_preprobe_active\":" + bquote(preprobe_active) +
                   ",\"agas_preprobe_ok\":" + bquote(agas_preprobe_ok) +
                   ",\"agas_preprobe_timeout\":" + bquote(agas_preprobe_timeout) +
                   ",\"build_type\":\"" EXP64_BUILD_TYPE "\"" +
                   ",\"error\":\"" + json_escape(err) + "\"}\n");
    return clean ? 0 : 1;
}

// --probe-info: print hostname + advertised endpoint WITHOUT starting HPX or networking, so the runner
// can sanity-check the binary off-cluster (e.g. local) without a two-node allocation.
int run_probe_info(int argc, char** argv) {
    const std::string adv = advertised_endpoint(argc, argv);
    std::string j = "{";
    j += "\"role\":\"probe-info\",";
    j += "\"hostname\":\"" + json_escape(get_hostname()) + "\",";
    j += "\"advertised_hpx_endpoint\":\"" + json_escape(adv) + "\",";
    j += "\"advertised_ip\":\"" + json_escape(ip_of_endpoint(adv)) + "\",";
    j += "\"action_registration_name\":\"exp64_payload_leaf_action\",";
    j += "\"build_type\":\"" EXP64_BUILD_TYPE "\",";
    j += "\"compiler_id\":\"" EXP64_COMPILER_ID "\",";
    j += "\"compiler_version\":\"" EXP64_COMPILER_VERSION "\",";
    j += "\"hpx_started\":false}\n";
    std::printf("%s", j.c_str());
    return 0;
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp64 payload connector (remote locality) options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("connect"), "connect | probe-info")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "shared-FS directory for rendezvous / attestation files")
        ("serve-timeout", po::value<int>()->default_value(120),
            "connect: seconds to stay alive waiting for the served signal before leaving")
        ("prefer-subnet", po::value<std::string>()->default_value(""),
            "connect: self-bind this node's first IP with this prefix (e.g. 10.42.5.) as --hpx:hpx, "
            "unless --hpx:hpx is already given (bind own node IP, not the root IP)")
        ("agas-preprobe-host", po::value<std::string>()->default_value(""),
            "connect: AGAS IP to bounded TCP pre-probe before hpx::start(connect); opt-in")
        ("agas-preprobe-port", po::value<int>()->default_value(0),
            "connect: AGAS port for the TCP pre-probe")
        ("agas-preprobe-timeout-ms", po::value<int>()->default_value(60000),
            "connect: bounded TCP pre-probe timeout in ms");
    // clang-format on

    std::string role = "connect";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--role" && i + 1 < argc) role = argv[i + 1];
        else if (a.rfind("--role=", 0) == 0) role = a.substr(7);
    }

    if (role == "probe-info") {
        return run_probe_info(argc, argv);
    }
    return run_connector(argc, argv, desc) == 0 ? 0 : 1;
}
