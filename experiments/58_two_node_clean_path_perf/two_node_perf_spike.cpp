// exp58 Slice 1 -- TWO-NODE HPX TCP parcelport CLEAN-PATH PERFORMANCE-CHARACTERIZATION spike.
//
// EXPERIMENTAL, NARROW, MECHANISM + CLEAN-PATH PERF ONLY. NOT RayX production code, NOT linked into
// the `_rayx` Python extension, NOT a Ray demo, NO Ray anywhere in this binary. This is a COPY IN
// SPIRIT of the exp57/exp56 two-node HPX TCP mechanism (root + connect roles, TCP parcelport, closed-
// int64 `dist_probe` action, three-way remote proof, clean connector leave observed by the root, root
// finalize, NO failure/restart). exp57 is NOT modified. The ONE behavioral change vs exp57 is that the
// root, instead of serving a SINGLE action, runs a Class-B timing characterization on the SAME remote
// id: a depth-1 serialized RTT floor (prewarm -> W warmups -> K measured) plus pipelined throughput at
// queue depths [8,32,128]. The connector path is unchanged (it is just a live remote locality).
//
// CLOCK / TIMING DISCIPLINE (see perf_characterization.md):
//   * Class-B timing is HIGH-RESOLUTION MONOTONIC (std::chrono::steady_clock nanoseconds). NO Class-B
//     millisecond fields. Per-action durations are recorded in ns; an aggregate K-loop duration and an
//     aggregate-derived mean are recorded as a cross-check; timestamp-call overhead is estimated.
//   * The timing loop runs INSIDE hpx_main (an HPX thread), so hpx::future suspend/resume on the root
//     HPX thread is INCLUDED in every measured delta (future_get_resume_included=true).
//   * The remote hpx::id_type is resolved ONCE via find_all_localities() and reused for every action
//     (remote_id_cached=true; per_iteration_agas_lookup=false).
//   * NO shared-FS marker wait is inside the timed steady-state loop.
//
// CLAIM FENCE: clean-path characterization only; exp59 is failure/restart. TCP parcelport only; closed-
// int64 action only. Depth-1 numbers are a SERIALIZED RTT FLOOR at queue depth 1 (including HPX future
// suspend/resume AND, unless idle backoff is disabled, scheduler wake-from-idle latency) -- NOT general
// per-action cost, NOT pure network RTT. Pipeline throughput may include HPX TCP parcelport coalescing
// and parcel-pool scheduling -- it is path characterization, not pure action cost; pipeline amortized
// time is NOT a latency. No HPX fault tolerance; no Ray failure recovery; no production/public API; no
// object store; no arbitrary Python; no Ray replacement; no network/fabric performance claim; no single-
// run speedup claim. Rostam/allocation-specific only.

#include <hpx/hpx_init.hpp>
#include <hpx/hpx_start.hpp>   // hpx::start (non-blocking connector path)
#include <hpx/hpx.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/lcos.hpp>     // hpx::wait_all (pipeline drain without result-vector allocation)
#include <hpx/include/runtime.hpp>
#include <hpx/include/run_as.hpp>   // hpx::run_as_hpx_thread
#include <hpx/modules/program_options.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/runtime_local/config_entry.hpp>   // hpx::get_config_entry (scheduler/parcelport disclosure)
#include <hpx/naming_base/id_type.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>        // getpid, gethostname, close
#include <cerrno>          // errno (connector AGAS TCP pre-probe, copied from exp57)
#include <fcntl.h>         // fcntl O_NONBLOCK
#include <sys/socket.h>    // socket, connect, getsockopt
#include <sys/select.h>    // select
#include <netinet/in.h>    // sockaddr_in
#include <arpa/inet.h>     // inet_pton

// ---- build-identity macros (injected by CMake target_compile_definitions) --------------------------
#ifndef EXP58_BUILD_TYPE
#define EXP58_BUILD_TYPE "unknown"
#endif
#ifndef EXP58_COMPILER_ID
#define EXP58_COMPILER_ID "unknown"
#endif
#ifndef EXP58_COMPILER_VERSION
#define EXP58_COMPILER_VERSION "unknown"
#endif

namespace {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

using clk = std::chrono::steady_clock;
inline clk::time_point ns_now() { return clk::now(); }
inline long long ns_delta(clk::time_point a, clk::time_point b) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count();
}

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

// emit a config-entry value as a JSON string, or null when HPX has no such key (disclosure honesty).
std::string cfg_or_null(const std::string& key) {
    std::string v = hpx::get_config_entry(key, "\x01__absent__");
    if (v == "\x01__absent__") return "null";
    return "\"" + json_escape(v) + "\"";
}
std::string cfg_raw(const std::string& key) { return hpx::get_config_entry(key, ""); }

std::string get_hostname() {
    char buf[256];
    if (::gethostname(buf, sizeof(buf)) == 0) { buf[sizeof(buf) - 1] = '\0'; return std::string(buf); }
    return std::string("unknown");
}

// ---- connector-side AGAS TCP reachability check (copied from exp57; NO HPX runtime touched) ---------
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

std::string g_adv_endpoint;  // set in main() before hpx::init so root's hpx_main can attest it

bool wait_drop_to_one(int timeout_s) {
    auto deadline = clk::now() + std::chrono::seconds(timeout_s);
    while (clk::now() < deadline) {
        if (hpx::find_all_localities().size() <= 1) return true;
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

std::vector<int> parse_depths(const std::string& csv) {
    std::vector<int> out;
    std::stringstream ss(csv);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        if (tok.empty()) continue;
        int v = std::atoi(tok.c_str());
        if (v > 0) out.push_back(v);
    }
    return out;
}

// nearest-rank percentile over an ALREADY-SORTED ascending vector of ns durations. -1 when empty.
long long pct_ns(const std::vector<long long>& sorted, double p) {
    if (sorted.empty()) return -1;
    long long n = static_cast<long long>(sorted.size());
    long long k = static_cast<long long>(std::ceil(p * static_cast<double>(n))) - 1;
    if (k < 0) k = 0;
    if (k >= n) k = n - 1;
    return sorted[static_cast<size_t>(k)];
}

std::string array_ns(const std::vector<long long>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) s.push_back(',');
        s += std::to_string(v[i]);
    }
    s.push_back(']');
    return s;
}

// derived perf-build tier (Release primary; RelWithDebInfo secondary; Debug invalid).
void build_tier(const std::string& bt, std::string& tier, bool& perf_valid) {
    if (bt == "Release") { tier = "primary"; perf_valid = true; }
    else if (bt == "RelWithDebInfo") { tier = "secondary_relwithdebinfo"; perf_valid = true; }
    else if (bt == "Debug") { tier = "invalid_debug"; perf_valid = false; }
    else { tier = "unknown"; perf_valid = false; }
}

}  // namespace

// ===== the one fixed registered action (identical in every locality; UNCHANGED from exp56/57) =======
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_probe, dist_probe_action)

// ===== root: admit one connector, run the Class-B characterization on the cached remote id, release ==
int run_root_perf(std::int64_t x, const std::string& bootdir, int ready_timeout, int leave_timeout,
                  std::int64_t K, std::int64_t W, const std::vector<int>& depths,
                  std::uint32_t here, long pid) {
    write_attestation(bootdir, "root", here, pid, g_adv_endpoint);
    write_text(bootdir + "/root.ready", "ready\n");

    // ---- structural settle: ready -> two localities visible (NOT a Class-B perf metric) -------------
    std::vector<hpx::id_type> locs;
    bool reached_two = false;
    auto deadline = clk::now() + std::chrono::seconds(ready_timeout);
    while (clk::now() < deadline) {
        locs = hpx::find_all_localities();
        if (locs.size() >= 2) { reached_two = true; break; }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // ---- remote id resolution ONCE; cache + reuse for every action (no per-iteration AGAS lookup) ---
    hpx::id_type here_id = hpx::find_here();
    hpx::id_type remote;
    bool have_remote = false;
    std::uint32_t remote_id = 0;
    if (reached_two) {
        for (auto const& l : locs) {
            if (l != here_id) { remote = l; have_remote = true; break; }
        }
        if (have_remote) remote_id = hpx::naming::get_locality_id_from_id(remote);
    }
    const std::int64_t oracle = (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(remote_id) << 1);

    // ---- clock disclosure + timestamp-overhead estimate ---------------------------------------------
    const std::string clock_type = "std::chrono::steady_clock";
    const double clock_resolution_ns =
        1e9 * static_cast<double>(clk::period::num) / static_cast<double>(clk::period::den);
    long long ts_overhead_ns = -1;
    {
        const int N = 4096;
        volatile long long sink = 0;
        auto o0 = ns_now();
        for (int i = 0; i < N; ++i) { sink += ns_now().time_since_epoch().count(); }
        auto o1 = ns_now();
        (void) sink;
        ts_overhead_ns = ns_delta(o0, o1) / N;
    }

    // ---- the measurement (ONLY when we have a proven-remote cached id) -------------------------------
    long long prewarm_ns = -1, first_action_ns = -1, agg_loop_ns = -1, agg_mean_ns = -1;
    std::vector<long long> raw;                  // K steady-state per-action ns
    std::int64_t depth1_correct = 0;
    std::int64_t depth1_first_result = 0;
    std::uint32_t depth1_executed_on = 0;
    bool proved_remote = false;

    struct PipeRow { int depth; long long total_ns; double aps; long long amort_ns;
                     std::int64_t correct; bool first_last_ok; };
    std::vector<PipeRow> pipe;

    if (have_remote) {
        // (1) one deliberate prewarm action -- one-shot, may include TCP connection + AGAS resolution.
        {
            auto t0 = ns_now();
            std::int64_t r = hpx::async<dist_probe_action>(remote, x).get();
            prewarm_ns = ns_delta(t0, ns_now());
            (void) r;
        }
        // (2) W warmup actions, dropped from steady-state stats; first_action_ns = first warmup.
        for (std::int64_t i = 0; i < W; ++i) {
            auto t0 = ns_now();
            std::int64_t r = hpx::async<dist_probe_action>(remote, x).get();
            long long d = ns_delta(t0, ns_now());
            if (i == 0) first_action_ns = d;
            (void) r;
        }
        // (3) K measured serialized actions -- the depth-1 QD1 RTT floor. NO shared-FS wait inside.
        raw.reserve(static_cast<size_t>(std::max<std::int64_t>(K, 0)));
        auto agg0 = ns_now();
        for (std::int64_t i = 0; i < K; ++i) {
            auto t0 = ns_now();
            std::int64_t r = hpx::async<dist_probe_action>(remote, x).get();
            long long d = ns_delta(t0, ns_now());
            raw.push_back(d);
            if (r == oracle) ++depth1_correct;
            if (i == 0) {
                depth1_first_result = r;
                depth1_executed_on =
                    static_cast<std::uint32_t>((r - (x ^ DIST_PROBE_XOR)) >> 1);
            }
        }
        agg_loop_ns = ns_delta(agg0, ns_now());
        if (K > 0) agg_mean_ns = agg_loop_ns / K;
        proved_remote = (depth1_correct == K) && (K > 0) && (depth1_executed_on == remote_id)
                        && (depth1_executed_on != here);

        // (4) pipelined throughput at the requested queue depths. Issue D without per-action waiting,
        //     drain with hpx::wait_all (no result-vector/continuation allocation), THEN verify.
        for (int D : depths) {
            std::vector<hpx::future<std::int64_t>> futs;
            futs.reserve(static_cast<size_t>(D));
            auto p0 = ns_now();
            for (int i = 0; i < D; ++i) futs.push_back(hpx::async<dist_probe_action>(remote, x));
            hpx::wait_all(futs);
            long long total_ns = ns_delta(p0, ns_now());   // includes the issue loop (recorded as such)

            std::int64_t correct = 0, first_res = 0, last_res = 0;
            for (int i = 0; i < D; ++i) {
                std::int64_t v = futs[static_cast<size_t>(i)].get();
                if (v == oracle) ++correct;
                if (i == 0) first_res = v;
                if (i == D - 1) last_res = v;
            }
            double aps = total_ns > 0 ? (static_cast<double>(D) * 1e9 / static_cast<double>(total_ns))
                                      : -1.0;
            long long amort = D > 0 ? total_ns / D : -1;
            pipe.push_back({D, total_ns, aps, amort, correct,
                            (first_res == oracle && last_res == oracle)});
        }
    }

    // ---- release the connector (this is the FIRST shared-FS write of the run; it is OUTSIDE every
    //      timed region above), then observe its graceful leave -------------------------------------
    if (proved_remote) write_text(bootdir + "/served1.ok", "served\n");
    bool observed_leave = reached_two ? wait_drop_to_one(leave_timeout) : false;

    // ---- sorted copy for percentiles ---------------------------------------------------------------
    std::vector<long long> sorted = raw;
    std::sort(sorted.begin(), sorted.end());
    long long d1_min = sorted.empty() ? -1 : sorted.front();
    long long d1_max = sorted.empty() ? -1 : sorted.back();
    long long d1_mean = -1;
    if (!raw.empty()) {
        long long s = 0;
        for (long long v : raw) s += v;
        d1_mean = s / static_cast<long long>(raw.size());
    }

    // ---- scheduler idle-backoff disclosure ----------------------------------------------------------
    const std::string idle_backoff = cfg_raw("hpx.max_idle_backoff_time");
    const std::string idle_loop = cfg_raw("hpx.max_idle_loop_count");
    const bool idle_disabled = (idle_backoff == "0");
    const std::string idle_mode = idle_disabled ? "disabled"
                                 : (idle_backoff.empty() ? "unknown" : "recorded_only");

    // ---- perf build tier ----------------------------------------------------------------------------
    std::string tier; bool perf_valid = false;
    build_tier(EXP58_BUILD_TYPE, tier, perf_valid);

    // ---- emit the single root perf result JSON ------------------------------------------------------
    std::ostringstream j;
    j << "{";
    // Class A -- structural
    j << "\"role\":\"root\",";
    j << "\"here_locality\":" << here << ",";
    j << "\"remote_locality\":" << remote_id << ",";
    j << "\"reached_two\":" << bquote(reached_two) << ",";
    j << "\"have_remote\":" << bquote(have_remote) << ",";
    j << "\"remote_locality_id_differs\":" << bquote(reached_two && have_remote && remote_id != here)
      << ",";
    j << "\"x\":" << x << ",";
    j << "\"oracle\":" << oracle << ",";
    j << "\"depth1_first_result\":" << depth1_first_result << ",";
    j << "\"depth1_executed_on_locality\":" << depth1_executed_on << ",";
    j << "\"proved_remote_by_oracle\":" << bquote(proved_remote) << ",";
    j << "\"observed_connector_leave\":" << bquote(observed_leave) << ",";
    // timing context
    j << "\"timing_loop_context\":\"hpx_thread\",";
    j << "\"future_get_resume_included\":true,";
    j << "\"remote_id_cached\":true,";
    j << "\"per_iteration_agas_lookup\":false,";
    j << "\"steady_state_contains_shared_fs_marker_wait\":false,";
    j << "\"plain_exists_critical_waits\":false,";
    j << "\"nfs_marker_artifact_excluded_from_perf\":true,";
    j << "\"connection_establishment_component_labeled\":true,";
    j << "\"first_action_includes_tcp_connection_and_agas_resolution_possible\":true,";
    // clock disclosure
    j << "\"clock_type\":\"" << clock_type << "\",";
    j << "\"clock_resolution_ns\":" << clock_resolution_ns << ",";
    j << "\"timestamp_overhead_ns\":" << ts_overhead_ns << ",";
    // first-action decomposition
    j << "\"prewarm_action_duration_ns\":" << prewarm_ns << ",";
    j << "\"first_action_duration_ns\":" << first_action_ns << ",";
    // depth-1 RTT floor block
    j << "\"remote_action_rtt_floor_depth1\":{";
    j << "\"metric_name\":\"remote_action_rtt_floor_depth1\",";
    j << "\"alias\":\"closed_int64_remote_action_overhead_floor_qd1\",";
    j << "\"queue_depth\":1,";
    j << "\"K\":" << K << ",\"W\":" << W << ",";
    j << "\"steady_count\":" << raw.size() << ",";
    j << "\"correct_count\":" << depth1_correct << ",";
    j << "\"aggregate_loop_duration_ns\":" << agg_loop_ns << ",";
    j << "\"aggregate_mean_action_ns\":" << agg_mean_ns << ",";
    j << "\"min_ns\":" << d1_min << ",\"mean_ns\":" << d1_mean << ",";
    j << "\"p50_ns\":" << pct_ns(sorted, 0.50) << ",";
    j << "\"p90_ns\":" << pct_ns(sorted, 0.90) << ",";
    j << "\"p99_ns\":" << pct_ns(sorted, 0.99) << ",";
    j << "\"max_ns\":" << d1_max << ",";
    j << "\"note\":\"serialized RTT floor at queue depth 1; includes HPX future suspend/resume on the "
         "root HPX thread; may include scheduler wake-from-idle latency unless idle backoff disabled; "
         "NOT general per-action cost; NOT pure network RTT\",";
    j << "\"per_action_duration_ns_raw\":" << array_ns(raw);
    j << "},";
    // pipeline block
    j << "\"remote_action_pipeline\":[";
    for (size_t i = 0; i < pipe.size(); ++i) {
        const PipeRow& r = pipe[i];
        if (i) j << ",";
        j << "{";
        j << "\"queue_depth\":" << r.depth << ",";
        j << "\"pipeline_actions\":" << r.depth << ",";
        j << "\"pipeline_total_duration_ns\":" << r.total_ns << ",";
        j << "\"pipeline_actions_per_sec\":" << r.aps << ",";
        j << "\"pipeline_amortized_action_time_ns\":" << r.amort_ns << ",";
        j << "\"pipeline_correct_count\":" << r.correct << ",";
        j << "\"pipeline_remote_proof_first_last\":" << bquote(r.first_last_ok) << ",";
        j << "\"hpx_wait_primitive\":\"hpx::wait_all\",";
        j << "\"pipeline_wait_all_includes_allocation_overhead\":false,";
        j << "\"issue_loop_included_in_total\":true,";
        j << "\"note\":\"amortized time = makespan/N, NOT a latency; tail-gated; may include HPX TCP "
             "parcelport coalescing + parcel-pool scheduling\"";
        j << "}";
    }
    j << "],";
    // scheduler tuning
    j << "\"scheduler_tuning\":{";
    j << "\"hpx_max_idle_backoff_time\":" << cfg_or_null("hpx.max_idle_backoff_time") << ",";
    j << "\"hpx_max_idle_loop_count\":" << cfg_or_null("hpx.max_idle_loop_count") << ",";
    j << "\"idle_backoff_controlled\":" << bquote(idle_disabled) << ",";
    j << "\"idle_backoff_mode\":\"" << idle_mode << "\",";
    j << "\"scheduler_idle_backoff_may_affect_qd1\":" << bquote(!idle_disabled);
    (void) idle_loop;
    j << "},";
    // parcelport / TCP disclosure (config-derived; null when HPX has no such key)
    j << "\"parcelport_config\":{";
    j << "\"parcel_coalescing_enabled\":" << cfg_or_null("hpx.parcel.message_handlers") << ",";
    j << "\"parcel_coalescing_interval\":"
      << cfg_or_null("hpx.parcel.coalescing.interval") << ",";
    j << "\"parcel_coalescing_buffer_size\":"
      << cfg_or_null("hpx.parcel.coalescing.num_messages") << ",";
    j << "\"parcel_coalescing_mode\":\"unknown\",";
    j << "\"tcp_enable\":" << cfg_or_null("hpx.parcel.tcp.enable") << ",";
    j << "\"tcp_nodelay_expected\":true,";
    j << "\"tcp_nodelay_verified\":false,";
    j << "\"tcp_nodelay_note\":\"HPX TCP parcelport normally sets socket no_delay; not directly "
         "config-verifiable here, so Nagle/delayed-ACK remains a possible QD1 confound\",";
    j << "\"parcel_pool_threads\":" << cfg_or_null("hpx.threadpools.parcel_pool_size") << ",";
    j << "\"parcel_pool_affinity\":\"unknown\",";
    j << "\"parcel_pool_threading_config\":"
      << cfg_or_null("hpx.threadpools.parcel_pool_size") << ",";
    j << "\"parcel_pool_contends_with_worker_cores\":\"unknown\"";
    j << "},";
    // build / runtime
    j << "\"spike_build_type\":\"" << EXP58_BUILD_TYPE << "\",";
    j << "\"perf_build_tier\":\"" << tier << "\",";
    j << "\"perf_valid\":" << bquote(perf_valid) << ",";
    j << "\"compiler_id\":\"" << EXP58_COMPILER_ID << "\",";
    j << "\"compiler_version\":\"" << EXP58_COMPILER_VERSION << "\",";
    j << "\"hpx_version\":null,";   // runner fills via --hpx:version (kept out of binary to limit deps)
    j << "\"allocator\":null,";
    j << "\"hpx_threads\":" << cfg_or_null("hpx.os_threads") << ",";
    j << "\"hpx_bind\":null,";
    // loopback control fields (reserved; not run in Slice 1)
    j << "\"loopback_control_available\":false,";
    j << "\"loopback_control_run\":false,";
    j << "\"loopback_control_purpose\":\"decompose HPX parcel-stack/scheduler software cost from the "
         "inter-node network leg via a same-node two-locality TCP run (reserved; not run in Slice 1)\",";
    j << "\"pid\":" << pid;
    j << "}\n";
    write_text(bootdir + "/perf_root_result.json", j.str());

    return hpx::finalize();
}

// ===== connect (non-blocking hpx::start; attests, waits to be served, self-disconnects) =============
// COPIED FROM exp57 (clean-leave idiom: post(disconnect)+stop; authoritative leave is the ROOT-side
// observed_connector_leave). The connector is just a live remote locality for the action loop.
int run_connector_start(int argc, char** argv,
                        const hpx::program_options::options_description& desc) {
    std::string bootdir = ".";
    int serve_timeout = 60;
    std::string preprobe_host;
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

    bool preprobe_active = (!preprobe_host.empty() && preprobe_port > 0);
    bool agas_preprobe_ok = false, agas_preprobe_timeout = false;
    if (preprobe_active) {
        const auto pp_deadline =
            clk::now() + std::chrono::milliseconds(preprobe_timeout_ms);
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

    const std::string served_path = bootdir + "/served1.ok";
    bool served = false;
    auto deadline = clk::now() + std::chrono::seconds(serve_timeout);
    while (clk::now() < deadline) {
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
    } catch (const std::exception& e) { clean = false; err = e.what(); }
    catch (...) { clean = false; err = "unknown"; }
    write_text(bootdir + "/connect.disconnected1",
               "{\"clean\":" + bquote(clean) + ",\"rc\":" + std::to_string(rc) +
                   ",\"served\":" + bquote(served) + ",\"teardown\":\"post(disconnect)+stop\"," +
                   "\"agas_preprobe_active\":" + bquote(preprobe_active) +
                   ",\"agas_preprobe_ok\":" + bquote(agas_preprobe_ok) +
                   ",\"agas_preprobe_timeout\":" + bquote(agas_preprobe_timeout) +
                   ",\"error\":\"" + json_escape(err) + "\"}\n");
    write_text(bootdir + "/connect_timing.json",
               "{\"role\":\"connect\",\"served\":" + bquote(served) +
                   ",\"note\":\"connector is a live remote locality only; no Class-B timing on this "
                   "side; root holds the single-clock measurement\"}\n");
    return clean ? 0 : 1;
}

int hpx_main(hpx::program_options::variables_map& vm) {
    const std::string role = vm["role"].as<std::string>();
    const std::int64_t x = vm["x"].as<std::int64_t>();
    const std::string bootdir = vm["bootstrap"].as<std::string>();
    const int ready_timeout = vm["ready-timeout"].as<int>();
    const int leave_timeout = vm["leave-timeout"].as<int>();
    const std::int64_t K = vm["k"].as<std::int64_t>();
    const std::int64_t W = vm["w"].as<std::int64_t>();
    const std::vector<int> depths = parse_depths(vm["pipeline-depths"].as<std::string>());
    const std::uint32_t here = hpx::get_locality_id();
    const long pid = static_cast<long>(::getpid());

    if (role == "root") {
        return run_root_perf(x, bootdir, ready_timeout, leave_timeout, K, W, depths, here, pid);
    }
    return hpx::finalize();  // unknown role: don't hang
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp58 two-node HPX TCP clean-path perf spike options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("root"), "root | connect")
        ("x", po::value<std::int64_t>()->default_value(7), "closed int64 action input")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "shared-FS directory for rendezvous / result / attestation files")
        ("ready-timeout", po::value<int>()->default_value(60),
            "seconds the root waits for the connector to join")
        ("leave-timeout", po::value<int>()->default_value(60),
            "seconds the root waits for the connector's graceful leave")
        ("serve-timeout", po::value<int>()->default_value(120),
            "connect: seconds to stay alive waiting for the served signal before leaving")
        ("k", po::value<std::int64_t>()->default_value(1000),
            "measured steady-state actions per island (depth-1 QD1 RTT floor)")
        ("w", po::value<std::int64_t>()->default_value(100),
            "warmup actions, dropped from steady-state stats")
        ("pipeline-depths", po::value<std::string>()->default_value("8,32,128"),
            "comma-separated pipeline queue depths")
        ("agas-preprobe-host", po::value<std::string>()->default_value(""),
            "connect: AGAS IP to bounded TCP pre-probe before hpx::start(connect); opt-in")
        ("agas-preprobe-port", po::value<int>()->default_value(0),
            "connect: AGAS port for the TCP pre-probe")
        ("agas-preprobe-timeout-ms", po::value<int>()->default_value(60000),
            "connect: bounded TCP pre-probe timeout in ms");
    // clang-format on

    std::string role = "root";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--role" && i + 1 < argc) role = argv[i + 1];
        else if (a.rfind("--role=", 0) == 0) role = a.substr(7);
    }
    g_adv_endpoint = advertised_endpoint(argc, argv);

    if (role == "connect") {
        return run_connector_start(argc, argv, desc) == 0 ? 0 : 1;
    }

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::console;  // root: AGAS root / locality 0
    return hpx::init(argc, argv, params);
}
