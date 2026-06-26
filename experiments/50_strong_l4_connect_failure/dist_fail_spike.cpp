// exp50 -- Ray-free strong-L4 UNGRACEFUL connector-loss characterization (standalone binary).
//
// EXPERIMENTAL, NARROW, MECHANISM-CHARACTERIZATION ONLY. NOT RayX production code, NOT linked
// into the `_rayx` Python extension, NOT a Ray demo. exp49 proved the GRACEFUL connect-mode
// lifecycle (join -> remote action -> self-`disconnect()` via post(disconnect)+stop -> root
// re-admits a second connector). exp50 asks the next question, still Ray-free and single-node:
// what happens when a connect-mode locality disappears UNGRACEFULLY (SIGKILL), the way a Ray
// actor process might crash? A SIGKILLed connector is a crash ANALOG, not a real Ray actor.
//
// One binary, two role families selected by --role:
//   * f_root    : hpx::init/hpx_main console (AGAS root, locality 0) running with
//                 --hpx:expect-connecting-localities and --hpx:threads=2. Branches on --case
//                 A|B|C. Serves the victim connector #1, then re-admits a clean connector #2
//                 by SET-DIFFERENCE targeting (NOT a hard-coded dead id). Writes
//                 case_<X>_root_result.json, then hpx::finalize().
//   * f_connect : hpx::start(nullptr) connect-mode locality, branching on --connector-kind:
//                   victim : join, idle, NEVER disconnect -- expects to be SIGKILLed.
//                   clean  : exp49 graceful path (wait served signal, post(disconnect)+stop).
//
// CLOSED-VALUE DISCIPLINE (and why it matters for clean teardown): every registered action
// returns a closed int64, NEVER a managed hpx::id_type. So no global-reference credit/decref
// parcel is ever owed back to a (possibly dead) locality at shutdown. The only ids in play are
// the UNMANAGED locality ids from find_all_localities(), which carry no reference credit. This
// is deliberately why the probe is closed-value: it makes the ungraceful-loss teardown clean
// to characterize instead of hanging on decref traffic to a corpse.
//
// CLAIM FENCE (see strong_l4_connect_failure.md): Ray-free; single-node; loopback TCP only;
// ungraceful connect-mode connector-loss CHARACTERIZATION only; SIGKILLed connector is a crash
// analog, not a real Ray actor; NO fault-tolerance claim (even on survival/readmit, phrase
// narrowly); no crash-recovery generalization; no AGAS-root-loss recovery; no Ray
// actor/bootstrap claim yet; no performance/speedup/throughput/latency; no multi-node; no
// general fabric; no production/public API; no Ray replacement; no "HPX faster than Ray"; no
// "RayX makes Ray faster".

#include <hpx/hpx_init.hpp>
#include <hpx/hpx_start.hpp>   // hpx::start (non-blocking connector path)
#include <hpx/hpx.hpp>
#include <hpx/future.hpp>      // hpx::future / future_status / wait_for
#include <hpx/exception.hpp>   // hpx::exception, hpx::get_error
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
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>  // getpid

namespace {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

// Process-global rendezvous dir. Set in main() from --bootstrap BEFORE init/start so the
// long action's body (which runs on the connector) can write its in-flight marker. This is
// process CONFIG, not an action argument -- the action value model stays closed int64.
std::string g_bootdir = ".";

void write_text(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
}

std::string bquote(bool b) { return b ? "true" : "false"; }

// JSON quoted-string or literal null.
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

// Best-effort name for the common hpx::error enumerators we expect from a broken parcelport /
// dropped locality; falls back to the numeric code for anything else.
std::string hpx_error_name(int v) {
    switch (v) {
        case 1:  return "no_success";
        case 6:  return "network_error";
        case 12: return "invalid_status";
        case 13: return "bad_parameter";
        case 18: return "lock_error";
        case 20: return "no_registered_console";
        case 24: return "deadlock";
        case 28: return "yield_aborted";
        case 31: return "serialization_error";
        case 32: return "unhandled_exception";
        case 33: return "kernel_error";
        case 34: return "broken_task";
        case 41: return "no_state";
        case 42: return "broken_promise";
        case 44: return "future_cancelled";
        default: return "hpx_error_" + std::to_string(v);
    }
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
// Short probe reused from exp49: folds the executing locality id into the result as remote
// proof. Used for the case-B success path and for every re-admit dispatch.
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_probe, dist_probe_action)

// Long probe: writes an in-flight marker as its FIRST statement so the orchestrator can SIGKILL
// the connector while the body is provably executing on it; then chunk-sleeps (capped); then
// returns the same locality-folded oracle IF it ever completes (it usually will not, because
// the connector is killed mid-flight). Closed int64 in/out; millis is config, not a payload.
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

// Bounded-poll until at least two localities are visible.
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

// Bounded-poll until a locality id NOT in `exclude` (and != self) appears. This is the
// set-difference re-admit target: it finds the GENUINELY NEW connector #2 rather than a
// hard-coded id, so it is robust to AGAS retaining the dead locality OR reusing its id.
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

// Bounded-poll until a specific locality id is no longer visible (used to let connector #2
// finish its graceful self-disconnect before the root finalizes).
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

struct Connector1Outcome {
    std::string future_outcome = "no_remote";   // returned|threw|timed_out|no_remote
    bool served1_ok = false;                     // case B short-action success
    bool detected_before_wait_bound = false;
    bool detected_at_full_bound = false;
    bool timed_out_at_bound = false;
    bool has_exc = false;
    std::string exc_name;
    long exc_value = 0;
    std::string exc_text;
};

// Invoke the long action with a BOUNDED wait_for; classify returned/threw/timed_out. NEVER
// call get() after a timeout (that could block forever). On throw, capture the hpx::error enum
// (name + numeric) before the std::exception text -- the enum is the diagnostic of HOW the
// parcelport surfaced the loss. NOTE: the root process may instead be self-terminated by HPX's
// fatal-error path here; that is detected by the orchestrator (no result JSON), not in C++.
Connector1Outcome invoke_long_bounded(hpx::id_type remote, std::uint32_t remote_id,
                                      std::int64_t x, std::int64_t millis, int wait_bound_s) {
    Connector1Outcome o;
    hpx::future<std::int64_t> f = hpx::async<dist_sleep_probe_action>(remote, x, millis);
    hpx::future_status st = f.wait_for(std::chrono::seconds(wait_bound_s));
    if (st == hpx::future_status::ready) {
        try {
            std::int64_t r = f.get();
            std::int64_t oracle =
                (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(remote_id) << 1);
            o.future_outcome = (r == oracle) ? "returned" : "returned";
            o.detected_before_wait_bound = false;  // completed normally; no loss observed
        } catch (hpx::exception const& e) {
            o.future_outcome = "threw";
            o.has_exc = true;
            o.exc_value = static_cast<long>(static_cast<int>(hpx::get_error(e)));
            o.exc_name = hpx_error_name(static_cast<int>(o.exc_value));
            o.exc_text = e.what();
            o.detected_before_wait_bound = true;  // parcelport surfaced an error before bound
        } catch (std::exception const& e) {
            o.future_outcome = "threw";
            o.has_exc = true;
            o.exc_name = "std_exception";
            o.exc_value = -1;
            o.exc_text = e.what();
            o.detected_before_wait_bound = true;
        } catch (...) {
            o.future_outcome = "threw";
            o.has_exc = true;
            o.exc_name = "unknown";
            o.exc_value = -1;
            o.detected_before_wait_bound = true;
        }
    } else {
        o.future_outcome = "timed_out";
        o.timed_out_at_bound = true;
        o.detected_at_full_bound = true;  // loss "noticed" only by hitting the bound
    }
    return o;
}

// Short bounded probe (used by case C against the already-killed locality, and as the generic
// re-admit dispatch). Reuses the same classification + exception capture.
Connector1Outcome invoke_short_bounded(hpx::id_type remote, std::uint32_t remote_id,
                                       std::int64_t x, int wait_bound_s, bool& match) {
    Connector1Outcome o;
    match = false;
    hpx::future<std::int64_t> f = hpx::async<dist_probe_action>(remote, x);
    hpx::future_status st = f.wait_for(std::chrono::seconds(wait_bound_s));
    if (st == hpx::future_status::ready) {
        try {
            std::int64_t r = f.get();
            std::int64_t oracle =
                (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(remote_id) << 1);
            match = (r == oracle);
            o.future_outcome = "returned";
            o.detected_before_wait_bound = false;
        } catch (hpx::exception const& e) {
            o.future_outcome = "threw";
            o.has_exc = true;
            o.exc_value = static_cast<long>(static_cast<int>(hpx::get_error(e)));
            o.exc_name = hpx_error_name(static_cast<int>(o.exc_value));
            o.exc_text = e.what();
            o.detected_before_wait_bound = true;
        } catch (std::exception const& e) {
            o.future_outcome = "threw";
            o.has_exc = true;
            o.exc_name = "std_exception";
            o.exc_value = -1;
            o.exc_text = e.what();
            o.detected_before_wait_bound = true;
        } catch (...) {
            o.future_outcome = "threw";
            o.has_exc = true;
            o.exc_name = "unknown";
            o.exc_value = -1;
            o.detected_before_wait_bound = true;
        }
    } else {
        o.future_outcome = "timed_out";
        o.timed_out_at_bound = true;
        o.detected_at_full_bound = true;
    }
    return o;
}

int run_f_root(const std::string& kase, std::int64_t x, std::int64_t sleep_ms, int wait_bound,
               int step_timeout, std::uint32_t here, long pid) {
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

    Connector1Outcome c1;
    if (!have_remote) {
        c1.future_outcome = "no_remote";
    } else if (kase == "A") {
        // Primary: invoke the long action; the orchestrator SIGKILLs the connector mid-flight
        // once the action_started marker appears.
        c1 = invoke_long_bounded(remote1, remote1_id, x, sleep_ms, wait_bound);
    } else if (kase == "B") {
        // Bracketing: serve one short action successfully, signal served1.ok, THEN the
        // orchestrator SIGKILLs the connector (ungraceful, no disconnect). Loss is probed by
        // the re-admit below, not by this (successful) future.
        bool match = false;
        Connector1Outcome served = invoke_short_bounded(remote1, remote1_id, x, wait_bound, match);
        c1 = served;
        c1.served1_ok = (served.future_outcome == "returned") && match;
        if (c1.served1_ok) write_text(g_bootdir + "/served1.ok", "served\n");
    } else {  // case C, best-effort: connector is killed ~immediately after join; dispatch may
              // race whether TCP/AGAS noticed. Short fixed beat, then a bounded probe attempt.
        hpx::this_thread::sleep_for(std::chrono::milliseconds(400));
        bool match = false;
        c1 = invoke_short_bounded(remote1, remote1_id, x, wait_bound, match);
    }

    // Did AGAS drop the dead locality, or retain it stale? (Bounded look; expectation: stale.)
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

    // Re-admit by SET-DIFFERENCE: find a locality id not present before the kill.
    hpx::id_type new_id;
    std::uint32_t new_loc = 0;
    bool new_seen = wait_new_locality(pre_kill_set, here, step_timeout, new_id, new_loc);

    bool c2_served = false, c2_proved = false, readmitted = false;
    if (new_seen) {
        bool match = false;
        Connector1Outcome r = invoke_short_bounded(new_id, new_loc, x, wait_bound, match);
        c2_served = (r.future_outcome == "returned");
        c2_proved = c2_served && match && (new_loc != here);
        readmitted = c2_served && match && c2_proved;
        if (readmitted) {
            write_text(g_bootdir + "/served2.ok", "served\n");
            wait_id_absent(new_loc, step_timeout);  // let connector #2 self-disconnect cleanly
        }
    }

    std::string j = "{";
    j += "\"case\":\"" + kase + "\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"pre_kill_localities\":" + ids_json(pre_kill) + ",";
    j += "\"connector1_remote_locality\":" + int_or_null(remote1_id, have_remote) + ",";
    j += "\"root_future_outcome\":\"" + c1.future_outcome + "\",";
    j += "\"served1_ok\":" + bquote(c1.served1_ok) + ",";
    j += "\"future_exception_hpx_error\":" + qornull(c1.exc_name, c1.has_exc) + ",";
    j += "\"future_exception_hpx_error_value\":" + int_or_null(c1.exc_value, c1.has_exc) + ",";
    j += "\"future_exception_text\":" + qornull(c1.exc_text, c1.has_exc) + ",";
    j += "\"detected_before_wait_bound\":" + bquote(c1.detected_before_wait_bound) + ",";
    j += "\"detected_at_full_bound\":" + bquote(c1.detected_at_full_bound) + ",";
    j += "\"timed_out_at_bound\":" + bquote(c1.timed_out_at_bound) + ",";
    j += "\"localities_after_loss\":" + std::to_string(after.size()) + ",";
    j += "\"localities_after_loss_ids\":" + ids_json(after) + ",";
    j += "\"dead_locality_still_present\":" + bquote(dead_still_present) + ",";
    j += "\"new_locality_after_loss_seen\":" + bquote(new_seen) + ",";
    j += "\"connector2_remote_locality\":" + int_or_null(new_loc, new_seen) + ",";
    j += "\"connector2_served\":" + bquote(c2_served) + ",";
    j += "\"connector2_proved_remote\":" + bquote(c2_proved) + ",";
    j += "\"root_readmitted_after_loss\":" + bquote(readmitted);
    j += "}\n";
    write_text(g_bootdir + "/case_" + kase + "_root_result.json", j);

    return hpx::finalize();
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
        // Ungraceful-loss subject: stay alive (runtime services the incoming long action on
        // background HPX threads) and NEVER disconnect -- the orchestrator SIGKILLs this
        // process. The std::min cap bounds a runaway. If the kill somehow does not land, fall
        // back to a graceful teardown and flag it, so a missed kill is visible rather than a
        // contaminated "graceful" result.
        int idle = std::min(std::max(victim_idle, 0), 120);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(idle);
        while (std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        write_text(bootdir + "/victim_survived_idle" + idx, "survived\n");
        hpx::post([]() { hpx::disconnect(); });
        hpx::stop();
        return 0;
    }

    // kind == clean: exp49 graceful re-admit connector. Wait for the root's served signal,
    // then leave via the empirically-correct post(disconnect)+stop teardown.
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
    const std::string kase = vm["case"].as<std::string>();
    const std::int64_t x = vm["x"].as<std::int64_t>();
    const std::int64_t sleep_ms = vm["sleep-ms"].as<std::int64_t>();
    const int wait_bound = vm["wait-bound"].as<int>();
    const int step_timeout = vm["step-timeout"].as<int>();
    const std::uint32_t here = hpx::get_locality_id();
    const long pid = static_cast<long>(::getpid());
    return run_f_root(kase, x, sleep_ms, wait_bound, step_timeout, here, pid);
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp50 strong-L4 connect-failure options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("f_root"), "f_root | f_connect")
        ("case", po::value<std::string>()->default_value("A"), "A | B | C (f_root)")
        ("x", po::value<std::int64_t>()->default_value(7), "closed int64 action input")
        ("sleep-ms", po::value<std::int64_t>()->default_value(8000),
            "dist_sleep_probe duration (capped 60000)")
        ("wait-bound", po::value<int>()->default_value(15),
            "bounded wait_for seconds for the connector-loss future")
        ("step-timeout", po::value<int>()->default_value(20),
            "seconds to wait for join / re-admit / drop steps")
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
