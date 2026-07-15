// exp65 -- demand-triggered connect-mode admission spike (standalone binary).
//
// EXPERIMENTAL, NARROW, MECHANISM EVIDENCE ONLY. NOT RayX production code, NOT linked into
// the `_rayx` Python extension, NOT a Ray demo, NOT a performance experiment. Single node,
// loopback TCP only.
//
// Question: can the connect-mode island run DEMAND-ORDERED admission -- root starts ALONE
// (no --hpx:localities, no expected connector count anywhere), proves local HPX progress
// while zero connectors exist, keeps doing local liveness work through a deliberate
// zero-connector dwell, and only AFTER the controller's demand event is one connect-mode
// locality launched, discovered by membership SET-DIFFERENCE, served ONE bounded
// oracle-verifiable remote action, and released -- after which the root proves it is still
// operational (one more local oracle action) and finalizes cleanly? A no-demand control arm
// proves the same root finalizes cleanly with zero connectors ever joining.
//
// Lineage (all patterns proven earlier, reused deliberately):
//   * oracle action + console/connect roles + post(disconnect)+stop teardown: exp49
//   * bounded wait_for dispatch classification (never .get() after timeout): exp50
//   * root heartbeat (root.alive mtime) + completion sentinel (root.done), serve-timeout as
//     DEADMAN ONLY, never the normal release path: exp63 connector-lifetime hardening
//
// CLAIM FENCE (see demand_triggered_admission.md): no Ray / Ray actors; no
// HPX-inside-Ray-worker result; no elasticity-under-load; no concurrent membership churn;
// no failure-recovery result; no lazy-TCP-socket-establishment result; no performance
// comparison; no multi-node claim. Durations recorded are observational only.
//
// Honest structural caveat: the root still boots with --hpx:expect-connecting-localities
// (exp49 proved connect mode refuses late joiners without it). Demand-driven admission
// operates WITHIN that pre-declared willingness; this experiment removes the predetermined
// COUNT and the assemble-before-measure GATE, not the boolean intent flag.

#include <hpx/hpx_init.hpp>
#include <hpx/hpx_start.hpp>   // hpx::start (non-blocking, connector path)
#include <hpx/hpx.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/runtime.hpp>
#include <hpx/include/run_as.hpp>  // hpx::run_as_hpx_thread (HPX calls from main thread)
#include <hpx/modules/program_options.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/naming_base/id_type.hpp>
#include <hpx/version.hpp>  // waiter-fix verification: runtime-observed HPX build identity

#include <algorithm>
#include <atomic>   // waiter-fix verification: readiness-witness timestamp store
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <memory>   // waiter-fix verification: shared_ptr witness lifetime
#include <string>
#include <thread>   // std::this_thread on the connector's (non-HPX) main thread
#include <vector>

#include <sys/stat.h>  // ::stat for root.alive mtime (connector deadman)
#include <unistd.h>    // getpid, gethostname

namespace {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

// Steady-clock epoch captured at process start so every marker carries a per-process
// steady_ms alongside the comparable same-host wall_ms.
const std::chrono::steady_clock::time_point g_t0 = std::chrono::steady_clock::now();

// Full argv captured in main() before HPX parses anything (marker-level argv audit).
std::string g_argv_json = "[]";

long long wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

long long steady_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - g_t0)
        .count();
}

// Waiter-fix verification probe clock: ns since the per-process steady epoch. Comparable ONLY
// among this process's own probe timestamps (never across processes, never against wall_ms).
long long steady_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now() - g_t0)
        .count();
}

void write_text(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
}

std::string bquote(bool b) { return b ? "true" : "false"; }

std::string jesc(const std::string& s) {
    std::string o;
    for (char c : s) {
        if (c == '"' || c == '\\') o += '\\';
        if (c == '\n') { o += "\\n"; continue; }
        o += c;
    }
    return o;
}

std::string argv_to_json(int argc, char** argv) {
    std::string j = "[";
    for (int i = 0; i < argc; ++i) {
        if (i) j += ",";
        j += "\"" + jesc(argv[i]) + "\"";
    }
    return j + "]";
}

// Hostname attestation for placement proof (exp65 cross-node slice): every marker records
// the host the writing process actually ran on. Cached once per process.
const std::string& host_name() {
    static const std::string h = [] {
        char buf[256] = {0};
        if (::gethostname(buf, sizeof(buf) - 1) != 0) return std::string("unknown");
        return std::string(buf);
    }();
    return h;
}

// Common marker prefix: role, pid, host, wall/steady timestamps.
std::string stamp(const std::string& role, long pid) {
    return "\"role\":\"" + role + "\",\"pid\":" + std::to_string(pid) +
           ",\"host\":\"" + jesc(host_name()) + "\"" +
           ",\"wall_ms\":" + std::to_string(wall_ms()) +
           ",\"steady_ms\":" + std::to_string(steady_ms());
}

std::string ids_json(const std::vector<std::uint32_t>& ids) {
    std::string j = "[";
    for (std::size_t i = 0; i < ids.size(); ++i) {
        if (i) j += ",";
        j += std::to_string(ids[i]);
    }
    return j + "]";
}

bool file_exists(const std::string& path) {
    std::ifstream f(path);
    return f.good();
}

// root.alive mtime in nanoseconds (0 if missing). Sub-second resolution is required: the
// root heartbeats every tick (~200 ms), so second-granularity mtimes would look stale.
long long mtime_ns(const std::string& path) {
    struct stat st;
    if (::stat(path.c_str(), &st) != 0) return 0;
#ifdef __APPLE__
    return static_cast<long long>(st.st_mtimespec.tv_sec) * 1000000000LL +
           st.st_mtimespec.tv_nsec;
#else
    return static_cast<long long>(st.st_mtim.tv_sec) * 1000000000LL + st.st_mtim.tv_nsec;
#endif
}

}  // namespace

// ===== the one fixed registered action (identical to exp49) ================
// Registered identically in every locality because all processes run THIS binary.
// Closed int64 -> int64; folds the EXECUTING locality id into the result as the oracle.
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_probe, dist_probe_action)

// ===== root helpers =========================================================

namespace {

std::vector<std::uint32_t> snapshot_ids() {
    std::vector<std::uint32_t> ids;
    for (auto const& l : hpx::find_all_localities()) {
        ids.push_back(hpx::naming::get_locality_id_from_id(l));
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

struct LocalCall {
    bool ok = false;
    std::int64_t result = 0, oracle = 0;
    std::uint32_t executed_on = 0;
};

// One oracle-verified LOCAL call: dispatch the registered action to find_here() and require
// the executing locality to be the root itself. Local progress proof, not a remote call.
LocalCall local_oracle_call(std::int64_t x, std::uint32_t here) {
    LocalCall c;
    try {
        c.result = hpx::async<dist_probe_action>(hpx::find_here(), x).get();
        c.oracle = (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(here) << 1);
        c.executed_on = static_cast<std::uint32_t>((c.result - (x ^ DIST_PROBE_XOR)) >> 1);
        c.ok = (c.result == c.oracle) && (c.executed_on == here);
    } catch (...) {
        c.ok = false;
    }
    return c;
}

struct RemoteDispatch {
    std::string status = "not_attempted";  // returned | timed_out | threw | not_attempted
    bool match = false, proved_remote = false;
    bool detected_before_bound = false;
    int wait_slices = 0;
    std::int64_t result = 0, oracle = 0;
    std::uint32_t executed_on = 0, remote_id = 0;
    std::string error_type, error_what;
    // Waiter-fix verification probe provenance (full_bound_instrumented only; 0 = not captured).
    // Timestamps are per-process steady_ns(): comparable only among themselves.
    std::string wait_probe = "sliced";
    long long t_dispatch_ns = 0;        // just before hpx::async
    long long t_wait_entered_ns = 0;    // just before the ONE full-bound wait_for
    long long t_wait_returned_ns = 0;   // just after that wait_for returned
    long long t_ready_witness_ns = 0;   // readiness-witness continuation fired (0 = never)
    // Race-free suspension precondition: sf.is_ready() sampled AT wait entry. false proves
    // the future became ready only AFTER the waiter entered its wait (no dependence on when
    // the witness continuation gets scheduled); true is the is_ready fast path, which cannot
    // test the resume path either way.
    bool wait_entry_future_ready = false;
};

// ONE bounded remote dispatch, exp50 discipline: bounded wait; .get() ONLY when ready
// (a timed-out future is never .get()-ed, so the root can never block on a lost peer).
// The bound is applied as 100 ms wait_for SLICES rather than one full-bound wait_for:
// validation runs showed a single wait_for(bound) on this build parking for its FULL
// timeout and only then observing the (long-ready) future -- admission->served equaled the
// bound exactly (15000 ms at bound 15, 2000 ms at bound 2, oracle matched in both). Slicing
// keeps the wait strictly bounded, records early detection (exp50's detected_before_bound
// semantics), and removes that deadline-wake artifact. OBSERVATION ONLY; no timing claim.
//
// wait_probe == "full_bound_instrumented" (waiter-fix verification re-check ONLY; default
// "sliced" keeps the exp65 semantics above byte-identical): deliberately reintroduces the
// ONE full-bound wait_for that produced the original observation, plus a readiness-witness
// continuation on the shared state so the artifact can prove the future became ready while
// the waiter was still suspended. Classification happens in the verification analyzer, not
// here. OBSERVATION ONLY; no timing claim; never the default.
RemoteDispatch bounded_remote_dispatch(const hpx::id_type& remote, std::int64_t x,
                                       std::uint32_t here, int bound_s,
                                       const std::string& wait_probe = "sliced") {
    RemoteDispatch d;
    d.remote_id = hpx::naming::get_locality_id_from_id(remote);
    d.wait_probe = wait_probe;
    try {
        bool ready = false;
        if (wait_probe == "full_bound_instrumented") {
            d.t_dispatch_ns = steady_ns();
            auto sf = hpx::async<dist_probe_action>(remote, x).share();
            // Readiness witness: fires when the shared state becomes ready, independent of
            // the suspended waiter below. shared_ptr keeps the slot alive even if the
            // continuation outlives this frame (never-ready path); witness_f is held (not
            // waited on) so the continuation is not abandoned before harvest.
            auto witness = std::make_shared<std::atomic<long long>>(0);
            auto witness_f = sf.then([witness](auto&&) {
                witness->store(steady_ns(), std::memory_order_release);
            });
            d.t_wait_entered_ns = steady_ns();
            d.wait_entry_future_ready = sf.is_ready();     // race-free suspension precondition
            ready = (sf.wait_for(std::chrono::seconds(bound_s)) ==
                     hpx::future_status::ready);           // the ONE full-bound suspension
            d.t_wait_returned_ns = steady_ns();
            // Let the witness continuation land before harvesting its timestamp: on a build
            // where the waiter wakes ON readiness it can win the race against the witness
            // continuation, and a same-instant harvest reads 0 -- a false instrumentation
            // failure. Bounded side-channel settle; never part of the timed observation.
            if (ready) {
                witness_f.wait_for(std::chrono::seconds(2));
            }
            d.t_ready_witness_ns = witness->load(std::memory_order_acquire);
            d.wait_slices = 0;  // no slicing on this path
            // "detected early" analog: returned ready materially before the full bound.
            d.detected_before_bound =
                ready && (d.t_wait_returned_ns - d.t_wait_entered_ns <
                          static_cast<long long>(bound_s) * 1000000000LL / 2);
            if (ready) {
                d.result = sf.get();
                d.status = "returned";
                d.oracle =
                    (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(d.remote_id) << 1);
                d.match = (d.result == d.oracle);
                d.executed_on =
                    static_cast<std::uint32_t>((d.result - (x ^ DIST_PROBE_XOR)) >> 1);
                d.proved_remote = d.match && (d.executed_on == d.remote_id) &&
                                  (d.executed_on != here);
            } else {
                d.status = "timed_out";
            }
            return d;
        }
        auto f = hpx::async<dist_probe_action>(remote, x);
        const int max_slices = bound_s * 10;  // 100 ms slices
        for (int i = 0; i < max_slices; ++i) {
            ++d.wait_slices;
            if (f.wait_for(std::chrono::milliseconds(100)) == hpx::future_status::ready) {
                ready = true;
                break;
            }
        }
        d.detected_before_bound = ready && (d.wait_slices < max_slices);
        if (ready) {
            d.result = f.get();
            d.status = "returned";
            d.oracle = (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(d.remote_id) << 1);
            d.match = (d.result == d.oracle);
            d.executed_on =
                static_cast<std::uint32_t>((d.result - (x ^ DIST_PROBE_XOR)) >> 1);
            d.proved_remote = d.match && (d.executed_on == d.remote_id) &&
                              (d.executed_on != here);
        } else {
            d.status = "timed_out";
        }
    } catch (hpx::exception const& e) {
        d.status = "threw";
        d.error_type = "hpx::exception";
        d.error_what = e.what();
    } catch (std::exception const& e) {
        d.status = "threw";
        d.error_type = "std::exception";
        d.error_what = e.what();
    } catch (...) {
        d.status = "threw";
        d.error_type = "unknown";
    }
    return d;
}

}  // namespace

// ===== root =================================================================
// Arms share one code path; the ONLY branch is what ends the available-wait loop:
//   demand    : a NEW locality appears (set-difference vs the pre-demand baseline)
//   no_demand : the controller's no_demand.finish marker appears
// There is deliberately NO expected-count parameter anywhere in this function.
int run_root(hpx::program_options::variables_map& vm) {
    const std::string arm = vm["arm"].as<std::string>();
    const std::string bootdir = vm["bootstrap"].as<std::string>();
    const std::int64_t x = vm["x"].as<std::int64_t>();
    const int local_k = vm["local-k"].as<int>();
    const int wait_timeout =
        (arm == "demand") ? vm["admit-timeout"].as<int>() : vm["finish-timeout"].as<int>();
    const int leave_timeout = vm["leave-timeout"].as<int>();
    const int dispatch_bound = vm["dispatch-bound"].as<int>();
    const int tick_ms = vm["tick-ms"].as<int>();
    const std::string wait_probe = vm["wait-probe"].as<std::string>();

    const std::uint32_t here = hpx::get_locality_id();
    const long pid = static_cast<long>(::getpid());
    const std::string alive_path = bootdir + "/root.alive";

    // --- 1. startup snapshot: the root must be ALONE -------------------------------------
    std::vector<std::uint32_t> ids_start = snapshot_ids();
    write_text(bootdir + "/root.ready",
               "{" + stamp("root", pid) + ",\"arm\":\"" + arm + "\",\"here_locality\":" +
                   std::to_string(here) + ",\"membership_ids\":" + ids_json(ids_start) +
                   ",\"membership_count\":" + std::to_string(ids_start.size()) + "}\n");

    // --- 2. pre-demand local phase: K oracle-verified LOCAL calls ------------------------
    int local_ok = 0;
    for (int i = 0; i < local_k; ++i) {
        if (local_oracle_call(x + i, here).ok) ++local_ok;
    }
    bool all_local_ok = (local_ok == local_k);
    std::vector<std::uint32_t> ids_baseline = snapshot_ids();  // pre-demand baseline
    write_text(bootdir + "/local_phase.done",
               "{" + stamp("root", pid) + ",\"k\":" + std::to_string(local_k) +
                   ",\"local_ok_count\":" + std::to_string(local_ok) +
                   ",\"all_local_ok\":" + bquote(all_local_ok) +
                   ",\"membership_ids\":" + ids_json(ids_baseline) +
                   ",\"membership_count\":" + std::to_string(ids_baseline.size()) + "}\n");

    // --- 3. available-wait: heartbeat + local liveness work, NO count gate ---------------
    bool admitted = false, finish_seen = false;
    std::uint32_t new_id = 0;
    hpx::id_type new_locality;
    long liveness_ok = 0, heartbeat_count = 0;
    long long first_liveness_wall = 0, last_liveness_wall = 0;
    std::size_t max_ever = std::max(ids_start.size(), ids_baseline.size());

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(wait_timeout);
    while (std::chrono::steady_clock::now() < deadline) {
        // exp63 contract: heartbeat FIRST, so the connector deadman never fires while the
        // root is alive and ticking.
        write_text(alive_path, "alive " + std::to_string(++heartbeat_count) + "\n");

        // Local liveness work: the root stays a working HPX locality during the dwell.
        if (local_oracle_call(x + 1000 + heartbeat_count, here).ok) {
            ++liveness_ok;
            last_liveness_wall = wall_ms();
            if (first_liveness_wall == 0) first_liveness_wall = last_liveness_wall;
        }

        if (arm == "demand") {
            // Set-difference discovery vs the pre-demand baseline (exp50 pattern) -- NOT an
            // expected-count wait. The first id not present in the baseline is the admitted
            // locality.
            std::vector<hpx::id_type> locs = hpx::find_all_localities();
            max_ever = std::max(max_ever, locs.size());
            for (auto const& l : locs) {
                std::uint32_t lid = hpx::naming::get_locality_id_from_id(l);
                if (std::find(ids_baseline.begin(), ids_baseline.end(), lid) ==
                    ids_baseline.end()) {
                    admitted = true;
                    new_id = lid;
                    new_locality = l;
                    break;
                }
            }
            if (admitted) break;
        } else {
            max_ever = std::max(max_ever, hpx::find_all_localities().size());
            if (file_exists(bootdir + "/no_demand.finish")) {
                finish_seen = true;
                break;
            }
        }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(tick_ms));
    }

    // --- 4. demand arm: admission record, ONE bounded remote dispatch, release -----------
    RemoteDispatch disp;
    bool leave_observed = false;
    std::size_t membership_after_leave = 0;
    if (admitted) {
        std::vector<std::uint32_t> ids_adm = snapshot_ids();
        write_text(bootdir + "/admission_detected",
                   "{" + stamp("root", pid) + ",\"baseline_ids\":" + ids_json(ids_baseline) +
                       ",\"new_locality_id\":" + std::to_string(new_id) +
                       ",\"membership_ids\":" + ids_json(ids_adm) +
                       ",\"liveness_ok_before_admission\":" + std::to_string(liveness_ok) +
                       ",\"first_liveness_wall_ms\":" + std::to_string(first_liveness_wall) +
                       ",\"last_liveness_wall_ms\":" + std::to_string(last_liveness_wall) +
                       "}\n");

        write_text(alive_path, "alive pre-dispatch\n");  // heartbeat before dispatch (exp63)
        disp = bounded_remote_dispatch(new_locality, x + 42, here, dispatch_bound, wait_probe);
        write_text(bootdir + "/served",
                   "{" + stamp("root", pid) + ",\"x\":" + std::to_string(x + 42) +
                       ",\"dispatch_status\":\"" + disp.status + "\"" +
                       ",\"result\":" + std::to_string(disp.result) +
                       ",\"oracle\":" + std::to_string(disp.oracle) +
                       ",\"match\":" + bquote(disp.match) +
                       ",\"executed_on_locality\":" + std::to_string(disp.executed_on) +
                       ",\"remote_locality\":" + std::to_string(disp.remote_id) +
                       ",\"proved_remote\":" + bquote(disp.proved_remote) +
                       ",\"detected_before_bound\":" + bquote(disp.detected_before_bound) +
                       ",\"wait_slices_used\":" + std::to_string(disp.wait_slices) +
                       ",\"wait_probe\":\"" + jesc(disp.wait_probe) + "\"" +
                       ",\"dispatch_bound_s\":" + std::to_string(dispatch_bound) +
                       ",\"t_dispatch_ns\":" + std::to_string(disp.t_dispatch_ns) +
                       ",\"t_wait_entered_ns\":" + std::to_string(disp.t_wait_entered_ns) +
                       ",\"t_wait_returned_ns\":" + std::to_string(disp.t_wait_returned_ns) +
                       ",\"t_ready_witness_ns\":" + std::to_string(disp.t_ready_witness_ns) +
                       ",\"wait_entry_future_ready\":" + bquote(disp.wait_entry_future_ready) +
                       ",\"error_type\":\"" + jesc(disp.error_type) + "\"" +
                       ",\"error_what\":\"" + jesc(disp.error_what) + "\"}\n");

        // Completion sentinel ONLY after the dispatch outcome is captured (exp63 contract:
        // no more remote dispatches will occur once root.done exists).
        write_text(bootdir + "/root.done", "done\n");

        // Id-specific departure wait (exp52 lesson: gate on the admitted id going absent,
        // not on a bare count), bounded.
        auto lv_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(leave_timeout);
        while (std::chrono::steady_clock::now() < lv_deadline) {
            std::vector<std::uint32_t> ids_now = snapshot_ids();
            if (std::find(ids_now.begin(), ids_now.end(), new_id) == ids_now.end()) {
                leave_observed = true;
                membership_after_leave = ids_now.size();
                break;
            }
            hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        write_text(bootdir + "/leave_observed",
                   "{" + stamp("root", pid) + ",\"observed\":" + bquote(leave_observed) +
                       ",\"membership_count_after\":" +
                       std::to_string(membership_after_leave) + "}\n");
    }

    // --- 5. final local oracle action: root is STILL operational -------------------------
    LocalCall fin = local_oracle_call(x + 9000, here);
    write_text(bootdir + "/final_local.done",
               "{" + stamp("root", pid) + ",\"ok\":" + bquote(fin.ok) +
                   ",\"result\":" + std::to_string(fin.result) +
                   ",\"oracle\":" + std::to_string(fin.oracle) + "}\n");

    // --- 6. full result record ------------------------------------------------------------
    std::string j = "{";
    j += stamp("root", pid) + ",";
    j += "\"arm\":\"" + arm + "\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"argv\":" + g_argv_json + ",";
    j += "\"membership_at_start\":" + ids_json(ids_start) + ",";
    j += "\"membership_baseline\":" + ids_json(ids_baseline) + ",";
    j += "\"all_local_ok\":" + bquote(all_local_ok) + ",";
    j += "\"local_ok_count\":" + std::to_string(local_ok) + ",";
    j += "\"liveness_ok\":" + std::to_string(liveness_ok) + ",";
    j += "\"heartbeat_count\":" + std::to_string(heartbeat_count) + ",";
    j += "\"max_localities_ever\":" + std::to_string(max_ever) + ",";
    j += "\"admission_detected\":" + bquote(admitted) + ",";
    j += "\"admitted_locality_id\":" + std::to_string(new_id) + ",";
    j += "\"no_demand_finish_seen\":" + bquote(finish_seen) + ",";
    j += "\"dispatch_status\":\"" + disp.status + "\",";
    j += "\"proved_remote\":" + bquote(disp.proved_remote) + ",";
    j += "\"leave_observed\":" + bquote(leave_observed) + ",";
    j += "\"final_local_ok\":" + bquote(fin.ok) + ",";
    // Waiter-fix verification provenance: probe selection, per-process probe timestamps, and the
    // runtime-observed HPX build identity (public version API; commit embedded when recorded).
    j += "\"wait_probe\":\"" + jesc(disp.wait_probe) + "\",";
    j += "\"dispatch_bound_s\":" + std::to_string(dispatch_bound) + ",";
    j += "\"t_dispatch_ns\":" + std::to_string(disp.t_dispatch_ns) + ",";
    j += "\"t_wait_entered_ns\":" + std::to_string(disp.t_wait_entered_ns) + ",";
    j += "\"t_wait_returned_ns\":" + std::to_string(disp.t_wait_returned_ns) + ",";
    j += "\"t_ready_witness_ns\":" + std::to_string(disp.t_ready_witness_ns) + ",";
    j += "\"wait_entry_future_ready\":" + bquote(disp.wait_entry_future_ready) + ",";
    j += "\"hpx_version_full\":\"" + jesc(hpx::full_version_as_string()) + "\",";
    j += "\"hpx_complete_version\":\"" + jesc(hpx::complete_version()) + "\",";
    // Structural by construction: the only remote dispatch site in this binary runs strictly
    // after set-difference admission; there is no pre-admission remote path to take.
    j += "\"remote_work_started_before_admission\":false";
    j += "}\n";
    write_text(bootdir + "/root_result.json", j);

    return hpx::finalize();
}

// ===== connector ============================================================
// Connect-mode locality driven from its own (non-HPX) main thread via hpx::start, exp49
// p2b pattern. Lifetime contract is exp63's: leave on the root.done COMPLETION SENTINEL;
// the serve-timeout is a DEADMAN that fires only if root.alive goes silent.
int run_connector(int argc, char** argv,
                  const hpx::program_options::options_description& desc) {
    std::string bootdir = ".";
    int serve_timeout = 30;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--bootstrap" && i + 1 < argc) bootdir = argv[++i];
        else if (a.rfind("--bootstrap=", 0) == 0) bootdir = a.substr(12);
        else if (a == "--serve-timeout" && i + 1 < argc) serve_timeout = std::atoi(argv[++i]);
        else if (a.rfind("--serve-timeout=", 0) == 0)
            serve_timeout = std::atoi(a.substr(16).c_str());
    }
    const long pid = static_cast<long>(::getpid());

    // Process-start attestation BEFORE hpx::start: proves when this process came to exist
    // (the controller checks spawned.wall_ms > demand.wall_ms).
    write_text(bootdir + "/connector.spawned",
               "{" + stamp("connector", pid) + ",\"argv\":" + g_argv_json + "}\n");

    hpx::init_params params;
    params.desc_cmdline = desc;  // so HPX accepts --role/--bootstrap/--serve-timeout
    params.mode = hpx::runtime_mode::connect;
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(bootdir + "/connector.joined",
                   "{" + stamp("connector", pid) + ",\"started\":false}\n");
        return 2;
    }

    std::uint32_t hereloc = hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    write_text(bootdir + "/connector.joined",
               "{" + stamp("connector", pid) +
                   ",\"locality_id\":" + std::to_string(hereloc) + "}\n");

    // Deadman wait (main thread, NOT an HPX thread): leave on root.done; the serve-timeout
    // deadman fires only if root.alive stops advancing for serve_timeout seconds.
    const std::string done_path = bootdir + "/root.done";
    const std::string alive_path = bootdir + "/root.alive";
    std::string reason = "unknown";
    long long last_alive = mtime_ns(alive_path);
    auto deadman_start = std::chrono::steady_clock::now();
    for (;;) {
        if (file_exists(done_path)) {
            reason = "root_completion_signal";
            break;
        }
        long long m = mtime_ns(alive_path);
        if (m != last_alive) {
            last_alive = m;
            deadman_start = std::chrono::steady_clock::now();
        }
        if (std::chrono::steady_clock::now() - deadman_start >
            std::chrono::seconds(serve_timeout)) {
            reason = "serve_timeout_expired";
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Graceful leave: the exp49-proven idiom for an hpx::start connect locality. POST the
    // disconnect onto an HPX thread, then join shutdown from THIS main thread via stop().
    // (Bare disconnect() throws off-HPX-thread; awaiting the posted disconnect before stop()
    // deadlocks -- exp57 settle-diag finding. Do not "improve" this sequence.)
    int rc = 0;
    bool clean = true;
    std::string err;
    try {
        hpx::post([]() { hpx::disconnect(); });
        rc = hpx::stop();
    } catch (const std::exception& e) {
        clean = false;
        reason = "error";
        err = e.what();
    } catch (...) {
        clean = false;
        reason = "error";
        err = "unknown";
    }
    write_text(bootdir + "/connector.disconnected",
               "{" + stamp("connector", pid) + ",\"clean\":" + bquote(clean) +
                   ",\"rc\":" + std::to_string(rc) + ",\"shutdown_reason\":\"" + reason +
                   "\",\"teardown\":\"post(disconnect)+stop\",\"error\":\"" + jesc(err) +
                   "\"}\n");
    return clean ? 0 : 1;
}

// ===== entry ================================================================

int hpx_main(hpx::program_options::variables_map& vm) {
    // Only the root reaches hpx_main (the connector uses hpx::start(nullptr)).
    return run_root(vm);
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp65 demand-triggered admission spike options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("root"),
            "root | connector")
        ("arm", po::value<std::string>()->default_value("demand"),
            "root arm: demand | no_demand")
        ("x", po::value<std::int64_t>()->default_value(7),
            "closed int64 action input base")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "directory for markers / result files")
        ("local-k", po::value<int>()->default_value(5),
            "root: pre-demand local oracle call count")
        ("admit-timeout", po::value<int>()->default_value(60),
            "root demand arm: max seconds in the available-wait loop")
        ("finish-timeout", po::value<int>()->default_value(30),
            "root no_demand arm: max seconds waiting for no_demand.finish")
        ("leave-timeout", po::value<int>()->default_value(20),
            "root: max seconds waiting for the admitted locality to depart")
        ("dispatch-bound", po::value<int>()->default_value(15),
            "root: bounded wait_for seconds on the one remote dispatch")
        ("wait-probe", po::value<std::string>()->default_value("sliced"),
            "root dispatch wait probe: sliced (default; 100ms wait_for slices, unchanged "
            "exp65 semantics) | full_bound_instrumented (waiter-fix verification re-check: "
            "ONE full-bound wait_for plus readiness-witness continuation; observation only)")
        ("tick-ms", po::value<int>()->default_value(200),
            "root: available-wait tick period (heartbeat + liveness)")
        ("serve-timeout", po::value<int>()->default_value(30),
            "connector: DEADMAN seconds of root.alive silence (not the release path)");
    // clang-format on

    g_argv_json = argv_to_json(argc, argv);

    // Role must be known BEFORE init/start so connect mode is selected on the connector.
    std::string role = "root";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--role" && i + 1 < argc) role = argv[i + 1];
        else if (a.rfind("--role=", 0) == 0) role = a.substr(7);
    }

    if (role == "connector") {
        return run_connector(argc, argv, desc);
    }

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::console;  // root: AGAS root / locality 0
    return hpx::init(argc, argv, params);
}
