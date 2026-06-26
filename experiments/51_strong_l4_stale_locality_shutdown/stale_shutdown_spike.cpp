// exp51 -- Ray-free strong-L4 STALE-LOCALITY SHUTDOWN / CLEANUP / RECOVERY-BOUNDARY
// characterization (standalone binary).
//
// EXPERIMENTAL, NARROW, MECHANISM-CHARACTERIZATION ONLY. NOT RayX production code, NOT linked
// into the `_rayx` Python extension, NOT a Ray demo. exp50 showed that an UNGRACEFUL non-root
// connect-mode locality loss (SIGKILL): (a) leaves AGAS/locality state STALE, (b) still lets the
// root admit and serve a fresh connector by SET-DIFFERENCE targeting, yet (c) makes the root HANG
// at collective shutdown/finalize (it did not self-terminate; no HPX exception enum was thrown).
//
// exp51 asks the next question, still Ray-free and single-node: after that ungraceful loss leaves
// stale state, is there ANY HPX-side bounded-finalize or local-cache cleanup path that lets the
// root shut down cleanly -- or is the safe policy external WHOLE-ISLAND restart? This is NOT a
// search for fault tolerance; the likely design answer is already that no public AGAS
// stale-locality eviction API exists and the island is the failure unit. The value of the probe
// is to (1) LOCALIZE where the root hangs (the orchestrator captures a backtrace), (2) confirm
// whether bounded finalize helps on THIS build, (3) confirm whether local-cache cleanup helps,
// and (4) establish external whole-island restart as the recovery boundary if cleanup fails.
//
// One binary, two role families selected by --role; the root branches on --probe:
//   * f_root  : hpx::init/hpx_main console (AGAS root, locality 0), --hpx:expect-connecting-
//               localities, --hpx:threads=2.
//       --probe P1  : reproduce the exp50 loss + set-difference re-admit, write `reached_finalize`,
//                     then call PUBLIC BOUNDED finalize hpx::finalize(shutdown_timeout_us,
//                     localwait_us). The collective gather has no public per-locality timeout, so
//                     this is expected to HANG; the orchestrator owns the real wall bound, captures
//                     a backtrace, then SIGKILLs. (Negative result is the point.)
//       --probe P2  : like P1, but BEFORE the kill it snapshots the victim's gid + endpoints while
//                     it is still alive, and AFTER loss+re-admit (from this HPX thread) attempts
//                     LOCAL-CACHE cleanup -- hpx::agas::remove_resolved_locality(dead_gid) and
//                     parcelhandler::remove_from_connection_cache(dead_gid, endpoints) -- each in
//                     try/catch, then bounded finalize. These are INTERNAL-ish/LOCAL-CACHE calls,
//                     NOT supported public AGAS eviction. REFUTATION EXPECTATION: the dead locality
//                     remains in the authoritative AGAS locality namespace, so the shutdown gather
//                     re-targets / waits on it anyway and clearing local caches does not cure the
//                     hang.
//       --probe clean_island : NO loss. Admit one clean connector, serve one dist_probe, let it
//                     gracefully disconnect, then finalize cleanly. Used as the FRESH island for
//                     the P3 whole-island-restart POLICY phase (external restart yields a clean
//                     island -- this is NOT repair of the poisoned root).
//   * f_connect : hpx::start(nullptr) connect-mode locality.
//       --connector-kind victim : join, idle, NEVER disconnect -- expects to be SIGKILLed.
//       --connector-kind clean  : exp49 graceful path (wait served signal, post(disconnect)+stop).
//
// NOTE on hpx::terminate(): hpx::terminate() is the in-process NON-GRACEFUL analog of killing the
// island -- it bypasses clean collective shutdown (calls std::terminate on all localities), so it
// is NOT a clean recovery mechanism and is deliberately NOT used as a cleanup path here. NOTE on
// resiliency: HPX resiliency / task-replay modules are not tested here and do not imply
// membership / locality-loss recovery in this experiment.
//
// CLOSED-VALUE DISCIPLINE: every registered action returns a closed int64, NEVER a managed
// hpx::id_type, so no global-reference decref parcel is ever owed back to a (possibly dead)
// locality at shutdown. Whatever teardown behavior we observe is attributable to the locality loss
// and the cleanup attempt, not to reference-counting traffic to a corpse.
//
// CLAIM FENCE (see strong_l4_stale_locality_shutdown.md): Ray-free; single-node; loopback TCP
// only; stale-locality shutdown / cleanup characterization only; SIGKILLed connector is a crash
// analog, not a real Ray actor; NO fault-tolerance claim; no crash-recovery generalization; no
// AGAS-root-loss recovery; cleanup APIs used are LOCAL-CACHE/internal-ish and NOT public AGAS
// eviction; whole-island restart is EXTERNAL supervision, not HPX fault tolerance; no Ray
// actor/bootstrap claim yet; no performance/speedup/throughput/latency; no multi-node; no general
// fabric; no production/public API; no Ray replacement; no "HPX faster than Ray"; no "RayX makes
// Ray faster".

#include <hpx/hpx_init.hpp>
#include <hpx/hpx_start.hpp>   // hpx::start (non-blocking connector path)
#include <hpx/hpx.hpp>
#include <hpx/future.hpp>      // hpx::future / future_status / wait_for
#include <hpx/exception.hpp>   // hpx::exception, hpx::get_error
#include <hpx/include/actions.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/runtime.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/include/agas.hpp>                          // agas::resolve_locality / remove_resolved_locality
#include <hpx/modules/program_options.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_distributed/find_here.hpp>
#include <hpx/runtime_distributed/applier.hpp>           // applier::get_applier()
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/runtime_local/runtime_local_fwd.hpp>
#include <hpx/naming_base/id_type.hpp>
#if defined(HPX_HAVE_NETWORKING)
#include <hpx/parcelset/parcelhandler.hpp>               // parcelhandler::remove_from_connection_cache
#include <hpx/parcelset_base/locality.hpp>               // parcelset::endpoints_type
#endif

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>  // getpid

namespace {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

// Process-global rendezvous dir. Set in main() from --bootstrap BEFORE init/start so the long
// action's body (which runs on the connector) can write its in-flight marker. Process CONFIG, not
// an action argument -- the action value model stays closed int64.
std::string g_bootdir = ".";

void write_text(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
}

std::string bquote(bool b) { return b ? "true" : "false"; }

std::string qornull(const std::string& s, bool present) {
    if (!present) return "null";
    std::string out = "\"";
    for (char c : s) {
        if (c == '"' || c == '\\') out += '\\';
        if (c == '\n') { out += "\\n"; continue; }
        out += c;
    }
    out += "\"";
    return out;
}

std::string int_or_null(long v, bool present) {
    return present ? std::to_string(v) : std::string("null");
}

std::string ids_json(const std::vector<std::uint32_t>& ids) {
    std::string j = "[";
    for (std::size_t i = 0; i < ids.size(); ++i) {
        if (i) j += ",";
        j += std::to_string(ids[i]);
    }
    j += "]";
    return j;
}

std::vector<std::uint32_t> locality_ids(const std::vector<hpx::id_type>& locs) {
    std::vector<std::uint32_t> ids;
    ids.reserve(locs.size());
    for (auto const& l : locs)
        ids.push_back(hpx::naming::get_locality_id_from_id(l));
    std::sort(ids.begin(), ids.end());
    return ids;
}

}  // namespace

// ===== fixed registered actions (closed int64 -> int64) ====================
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_probe, dist_probe_action)

// Long probe: writes an in-flight marker as its FIRST statement so the orchestrator can SIGKILL the
// connector while the body is provably executing on it; then chunk-sleeps (capped); then returns
// the locality-folded oracle IF it ever completes. Closed int64 in/out.
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

// Set-difference re-admit target: a locality id not in `exclude` (and != self).
bool wait_new_locality(const std::set<std::uint32_t>& exclude, std::uint32_t here,
                       int timeout_s, hpx::id_type& out_id, std::uint32_t& out_loc) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        for (auto const& l : hpx::find_all_localities()) {
            std::uint32_t id = hpx::naming::get_locality_id_from_id(l);
            if (id != here && exclude.find(id) == exclude.end()) {
                out_id = l;
                out_loc = id;
                return true;
            }
        }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
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

// Bounded short probe + match; never get() after a timeout.
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

// Bounded long probe; classify returned|threw|timed_out without get() after a timeout.
std::string invoke_long(hpx::id_type remote, std::int64_t x, std::int64_t millis, int wait_bound_s) {
    hpx::future<std::int64_t> f = hpx::async<dist_sleep_probe_action>(remote, x, millis);
    if (f.wait_for(std::chrono::seconds(wait_bound_s)) != hpx::future_status::ready)
        return "timed_out";
    try { (void) f.get(); return "returned"; }
    catch (...) { return "threw"; }
}

#if defined(HPX_HAVE_NETWORKING)
struct CleanupOutcome {
    bool attempted = false;
    bool gid_snapshotted = false;
    bool endpoints_snapshotted = false;
    bool rrl_called = false, rrl_returned = false, rrl_threw = false;
    std::string rrl_exc;
    bool rfc_called = false, rfc_returned = false, rfc_threw = false;
    std::string rfc_exc;
};

// LOCAL-CACHE cleanup attempt (P2). Internal-ish, NOT public AGAS eviction. Each call wrapped so a
// throw/abort is a RECORDED outcome, not a harness failure. Runs on the calling HPX thread.
void try_local_cleanup(const hpx::naming::gid_type& dead_gid,
                       const hpx::parcelset::endpoints_type& dead_eps, CleanupOutcome& o) {
    o.attempted = true;
    try {
        hpx::agas::remove_resolved_locality(dead_gid);
        o.rrl_called = true;
        o.rrl_returned = true;
    } catch (const std::exception& e) {
        o.rrl_called = true; o.rrl_threw = true; o.rrl_exc = e.what();
    } catch (...) {
        o.rrl_called = true; o.rrl_threw = true; o.rrl_exc = "unknown";
    }
    try {
        hpx::applier::get_applier().get_parcel_handler().remove_from_connection_cache(
            dead_gid, dead_eps);
        o.rfc_called = true;
        o.rfc_returned = true;
    } catch (const std::exception& e) {
        o.rfc_called = true; o.rfc_threw = true; o.rfc_exc = e.what();
    } catch (...) {
        o.rfc_called = true; o.rfc_threw = true; o.rfc_exc = "unknown";
    }
}

void write_cleanup_json(const std::string& bootdir, const CleanupOutcome& o) {
    std::string j = "{";
    j += "\"attempted\":" + bquote(o.attempted) + ",";
    j += "\"dead_gid_snapshotted\":" + bquote(o.gid_snapshotted) + ",";
    j += "\"dead_endpoints_snapshotted\":" + bquote(o.endpoints_snapshotted) + ",";
    j += "\"remove_resolved_locality_called\":" + bquote(o.rrl_called) + ",";
    j += "\"remove_resolved_locality_returned\":" + bquote(o.rrl_returned) + ",";
    j += "\"remove_resolved_locality_threw\":" + bquote(o.rrl_threw) + ",";
    j += "\"remove_resolved_locality_exception\":" + qornull(o.rrl_exc, o.rrl_threw) + ",";
    j += "\"remove_from_connection_cache_called\":" + bquote(o.rfc_called) + ",";
    j += "\"remove_from_connection_cache_returned\":" + bquote(o.rfc_returned) + ",";
    j += "\"remove_from_connection_cache_threw\":" + bquote(o.rfc_threw) + ",";
    j += "\"remove_from_connection_cache_exception\":" + qornull(o.rfc_exc, o.rfc_threw);
    j += "}\n";
    write_text(bootdir + "/cleanup_result.json", j);
}
#endif

// Reproduce the exp50 ungraceful loss + set-difference re-admit, then (for P2) attempt local-cache
// cleanup, then write the result + `reached_finalize` marker, then call PUBLIC BOUNDED finalize.
// The orchestrator owns the real wall bound and captures a backtrace if finalize hangs.
int run_loss_then_finalize(const std::string& probe, std::int64_t x, std::int64_t sleep_ms,
                           int wait_bound, int step_timeout, double finalize_timeout_us,
                           double finalize_localwait_us, std::uint32_t here, long pid) {
    write_text(g_bootdir + "/root.ready", "ready\n");

    bool reached_two = false;
    std::vector<hpx::id_type> locs = wait_two(step_timeout, reached_two);
    std::vector<std::uint32_t> pre_kill = locality_ids(locs);
    std::set<std::uint32_t> pre_kill_set(pre_kill.begin(), pre_kill.end());

    hpx::id_type remote1;
    std::uint32_t remote1_id = 0;
    bool have_remote = false;
    if (reached_two) {
        hpx::id_type here_id = hpx::find_here();
        for (auto const& l : locs) {
            if (l != here_id) {
                remote1 = l;
                remote1_id = hpx::naming::get_locality_id_from_id(l);
                have_remote = true;
                break;
            }
        }
    }

#if defined(HPX_HAVE_NETWORKING)
    // P2: snapshot the victim's gid + endpoints WHILE IT IS STILL ALIVE (before the kill).
    CleanupOutcome cleanup;
    hpx::naming::gid_type dead_gid;
    hpx::parcelset::endpoints_type dead_eps;
    if (probe == "P2" && have_remote) {
        dead_gid = remote1.get_gid();
        cleanup.gid_snapshotted = true;
        try {
            dead_eps = hpx::agas::resolve_locality(dead_gid);
            cleanup.endpoints_snapshotted = !dead_eps.empty();
        } catch (...) {
            cleanup.endpoints_snapshotted = false;
        }
    }
#endif

    // Loss: invoke the long action; the orchestrator SIGKILLs the victim once action_started shows.
    std::string future_outcome = "no_remote";
    if (have_remote) future_outcome = invoke_long(remote1, x, sleep_ms, wait_bound);

    // Stale check: did AGAS drop the dead locality, or retain it?
    std::vector<std::uint32_t> after = pre_kill;
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline) {
            after = locality_ids(hpx::find_all_localities());
            hpx::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }
    bool dead_still_present =
        have_remote && (std::find(after.begin(), after.end(), remote1_id) != after.end());

    // Re-admit by set-difference.
    hpx::id_type new_id;
    std::uint32_t new_loc = 0;
    bool new_seen = wait_new_locality(pre_kill_set, here, step_timeout, new_id, new_loc);
    bool c2_served = false, c2_proved = false, readmitted = false;
    if (new_seen) {
        std::string oc;
        bool match = invoke_short(new_id, new_loc, x, wait_bound, oc);
        c2_served = (oc == "returned");
        c2_proved = c2_served && match && (new_loc != here);
        readmitted = c2_proved;
        if (readmitted) {
            write_text(g_bootdir + "/served2.ok", "served\n");
            wait_id_absent(new_loc, step_timeout);
        }
    }

#if defined(HPX_HAVE_NETWORKING)
    // P2: attempt local-cache cleanup BEFORE finalize (runtime fully up). REFUTATION EXPECTATION:
    // the dead locality is still authoritative in the locality namespace, so this should NOT cure
    // the finalize hang.
    if (probe == "P2") {
        try_local_cleanup(dead_gid, dead_eps, cleanup);
        write_cleanup_json(g_bootdir, cleanup);
    }
#endif

    std::string j = "{";
    j += "\"probe\":\"" + probe + "\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"pre_kill_localities\":" + ids_json(pre_kill) + ",";
    j += "\"connector1_remote_locality\":" + int_or_null(remote1_id, have_remote) + ",";
    j += "\"root_future_outcome\":\"" + future_outcome + "\",";
    j += "\"localities_after_loss\":" + std::to_string(after.size()) + ",";
    j += "\"localities_after_loss_ids\":" + ids_json(after) + ",";
    j += "\"dead_locality_still_present\":" + bquote(dead_still_present) + ",";
    j += "\"new_locality_after_loss_seen\":" + bquote(new_seen) + ",";
    j += "\"connector2_remote_locality\":" + int_or_null(new_loc, new_seen) + ",";
    j += "\"connector2_served\":" + bquote(c2_served) + ",";
    j += "\"connector2_proved_remote\":" + bquote(c2_proved) + ",";
    j += "\"root_readmitted_after_loss\":" + bquote(readmitted) + ",";
    j += "\"finalize_shutdown_timeout_us\":" + std::to_string(static_cast<long long>(finalize_timeout_us));
    j += "}\n";
    write_text(g_bootdir + "/" + probe + "_root_result.json", j);

    // Mark that we REACHED finalize (so the orchestrator can separate a finalize hang from an
    // earlier hang), then call PUBLIC BOUNDED finalize. The collective gather has no public
    // per-locality timeout, so against a stale dead locality this is expected to HANG.
    write_text(g_bootdir + "/reached_finalize", "reached\n");
    return hpx::finalize(finalize_timeout_us, finalize_localwait_us);
}

// P3 FRESH island (policy phase): NO loss. Admit one clean connector, serve one dist_probe, let it
// gracefully disconnect, then finalize CLEANLY. Demonstrates that external whole-island restart
// yields a clean island -- it does NOT repair the poisoned root.
int run_clean_island(std::int64_t x, int wait_bound, int step_timeout, std::uint32_t here,
                     long pid) {
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
    if (have_remote) {
        std::string oc;
        bool match = invoke_short(remote, remote_id, x, wait_bound, oc);
        served = (oc == "returned");
        proved = served && match && (remote_id != here);
        if (proved) {
            write_text(g_bootdir + "/served1.ok", "served\n");  // clean connector waits on this
            wait_id_absent(remote_id, step_timeout);            // let it self-disconnect
        }
    }

    std::string j = "{";
    j += "\"probe\":\"clean_island\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"fresh_connector_served\":" + bquote(served) + ",";
    j += "\"fresh_connector_proved_remote\":" + bquote(proved);
    j += "}\n";
    write_text(g_bootdir + "/clean_island_result.json", j);

    write_text(g_bootdir + "/reached_finalize", "reached\n");
    return hpx::finalize();  // fresh island: expected to finalize cleanly
}

// ===== connector (hpx::start non-blocking; drives its own lifecycle) =======
int run_connector(int argc, char** argv,
                  const hpx::program_options::options_description& desc) {
    std::string bootdir = ".";
    std::string kind = "victim";
    int index = 1;
    int serve_timeout = 25;
    int victim_idle = 30;
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

    if (kind == "victim") {
        int idle = std::min(std::max(victim_idle, 0), 120);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(idle);
        while (std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        write_text(bootdir + "/victim_survived_idle" + idx, "survived\n");
        hpx::post([]() { hpx::disconnect(); });
        hpx::stop();
        return 0;
    }

    // kind == clean: exp49 graceful path. Wait for the served signal, then post(disconnect)+stop.
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
    const std::string probe = vm["probe"].as<std::string>();
    const std::int64_t x = vm["x"].as<std::int64_t>();
    const std::int64_t sleep_ms = vm["sleep-ms"].as<std::int64_t>();
    const int wait_bound = vm["wait-bound"].as<int>();
    const int step_timeout = vm["step-timeout"].as<int>();
    const double fin_to_us = vm["finalize-timeout-us"].as<double>();
    const double fin_lw_us = vm["finalize-localwait-us"].as<double>();
    const std::uint32_t here = hpx::get_locality_id();
    const long pid = static_cast<long>(::getpid());

    if (probe == "clean_island")
        return run_clean_island(x, wait_bound, step_timeout, here, pid);
    return run_loss_then_finalize(probe, x, sleep_ms, wait_bound, step_timeout, fin_to_us, fin_lw_us,
                                  here, pid);
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp51 strong-L4 stale-locality shutdown options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("f_root"), "f_root | f_connect")
        ("probe", po::value<std::string>()->default_value("P1"),
            "P1 | P2 | clean_island (f_root)")
        ("x", po::value<std::int64_t>()->default_value(7), "closed int64 action input")
        ("sleep-ms", po::value<std::int64_t>()->default_value(8000),
            "dist_sleep_probe duration (capped 60000)")
        ("wait-bound", po::value<int>()->default_value(15),
            "bounded wait_for seconds for the connector-loss / serve future")
        ("step-timeout", po::value<int>()->default_value(20),
            "seconds to wait for join / re-admit / drop steps")
        ("finalize-timeout-us", po::value<double>()->default_value(5000000.0),
            "public hpx::finalize shutdown_timeout (microseconds); local thread-drain timeout")
        ("finalize-localwait-us", po::value<double>()->default_value(-1.0),
            "public hpx::finalize localwait (microseconds)")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "rendezvous / result directory")
        ("connector-kind", po::value<std::string>()->default_value("victim"),
            "victim | clean (f_connect)")
        ("connector-index", po::value<int>()->default_value(1), "f_connect index")
        ("serve-timeout", po::value<int>()->default_value(25),
            "clean connector seconds to wait for its served signal")
        ("victim-idle", po::value<int>()->default_value(30),
            "victim connector idle seconds before fallback teardown (capped 120)");
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
