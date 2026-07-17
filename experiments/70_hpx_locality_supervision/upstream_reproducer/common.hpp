// exp70 upstream_reproducer -- shared helpers + the ONE registered action (EXPERIMENT-ONLY).
//
// Minimal two-process loopback reduction of the exp63 connector-lifetime problem
// (experiments/63_hpx_native_collective_reduction/connector_lifetime_hardening.md). HPX-only:
// no Ray, no Python, no pybind11, no RayX runtime code, no repo helper outside this directory.
//
// REGISTRATION DISCIPLINE (exp63 model): this header BOTH defines exp70_probe AND registers
// exp70_probe_action via HPX_PLAIN_ACTION. It must be included in EXACTLY ONE TRANSLATION UNIT
// PER BINARY (root.cpp, connector.cpp). Two separate binaries => the action is registered
// exactly once per binary.
//
// CLAIM FENCE: lifecycle mechanism evidence only. No performance, no Ray comparison, no
// production, no fabric claim. The value oracle proves intended closed-int64 execution on a
// locality id, not physical placement.
#pragma once

#include <hpx/include/actions.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace exp70 {

using steady = std::chrono::steady_clock;

inline long long steady_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               steady::now().time_since_epoch()).count();
}

inline double now_unix() {
    return std::chrono::duration<double>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// Single small trunc write, exp63 marker style. Markers are one-line flat JSON so the shell
// driver can extract fields with sed.
inline void write_text(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
}

inline bool file_exists(const std::string& path) {
    struct stat st {};
    return ::stat(path.c_str(), &st) == 0;
}

// mtime in whole seconds (portable st_mtime). The exp63 deadman only needs "advanced since
// last look"; root witness bumps are seconds apart, so second granularity suffices.
inline time_t file_mtime(const std::string& path) {
    struct stat st {};
    if (::stat(path.c_str(), &st) != 0) return 0;
    return st.st_mtime;
}

inline std::string json_escape(const std::string& s) {
    std::string o;
    for (char c : s) {
        if (c == '"' || c == '\\') { o.push_back('\\'); o.push_back(c); }
        else if (c == '\n') { o += "\\n"; }
        else if (c == '\r') { o += "\\r"; }
        else if (c == '\t') { o += "\\t"; }
        else { o.push_back(c); }
    }
    return o;
}

inline std::string bquote(bool b) { return b ? "true" : "false"; }

inline std::string get_hostname() {
    char buf[256];
    if (::gethostname(buf, sizeof(buf)) == 0) { buf[sizeof(buf) - 1] = '\0'; return std::string(buf); }
    return std::string("unknown");
}

// Hard wall-clock backstop: a detached plain OS thread (never an HPX thread, never an HPX
// timed wait) that force-exits the process if the case runs past hard_timeout_s. It writes a
// marker first so the driver can distinguish a watchdog exit (rc 86) from a crash. Normal
// process exit simply ends the detached thread.
inline void start_hard_timeout(int hard_timeout_s, const std::string& bootdir,
                               const std::string& role) {
    std::thread([hard_timeout_s, bootdir, role]() {
        std::this_thread::sleep_for(std::chrono::seconds(hard_timeout_s));
        write_text(bootdir + "/" + role + ".hard_timeout",
                   "{\"role\":\"" + role + "\",\"hard_timeout_s\":" +
                       std::to_string(hard_timeout_s) + ",\"unix\":" +
                       std::to_string(now_unix()) + "}\n");
        std::_Exit(86);
    }).detach();
}

// Tiny flag parser for "--name value" / "--name=value" argv forms (exp63/exp65 style; HPX
// receives the same argv via desc_cmdline so these flags are also declared to HPX).
inline std::string arg_value(int argc, char** argv, const std::string& name,
                             const std::string& dflt) {
    const std::string eq = name + "=";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == name && i + 1 < argc) return argv[i + 1];
        if (a.rfind(eq, 0) == 0) return a.substr(eq.size());
    }
    return dflt;
}

inline int arg_int(int argc, char** argv, const std::string& name, int dflt) {
    const std::string v = arg_value(argc, argv, name, "");
    return v.empty() ? dflt : std::atoi(v.c_str());
}

// Closed-int64 oracle (exp65 dist_probe shape): the result encodes the executing locality, so
// the caller can verify both the value and where it ran.
constexpr std::int64_t kProbeXor = 0x5A5A;

inline std::int64_t probe_oracle(std::int64_t x, std::uint32_t locality) {
    return (x ^ kProbeXor) + (static_cast<std::int64_t>(locality) << 1);
}

}  // namespace exp70

// The one minimal registered plain action. Runs ON the target locality and folds its own
// runtime locality id into the value.
inline std::int64_t exp70_probe(std::int64_t x) {
    return exp70::probe_oracle(x, hpx::get_locality_id());
}
HPX_PLAIN_ACTION(exp70_probe, exp70_probe_action)
