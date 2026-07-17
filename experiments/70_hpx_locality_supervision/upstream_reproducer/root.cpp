// exp70 upstream_reproducer ROOT -- embedded HPX root with late connectors enabled
// (EXPERIMENT-ONLY, HPX-only).
//
// Reduced from the exp63 embedded root (collective_ext.cpp ext_start/ext_shutdown: hpx::start
// driven from a non-HPX main thread, teardown via post(finalize)+stop) and the exp63 runner's
// lifecycle protocol (run_exp63_collective.py _touch_heartbeat/_write_completion). The bounded
// dispatch uses the exp69 "post_get" pattern (hpx::post + untimed .get() on the HPX runtime,
// bound enforced with std::future::wait_for on the calling OS thread) because HPX timed waits
// (future::wait_for) crashed a local macOS host at volume in exp69; no HPX timed wait is used
// anywhere in this binary.
//
// Cases (selected with --case):
//   late-dispatch-current-behavior  Root dispatches action 1, then WAITS until the connector
//                                   has completed its fixed-serve-window stop path (proven by
//                                   the connector.stopping/connector.stopped markers), then
//                                   attempts action 2 to the SAVED locality id and records the
//                                   exact outcome. No root.alive witness, no root.done -- this
//                                   is the pre-hardening exp63 behavior.
//   external-lifecycle-workaround   Reduced exp63 hardening: root bumps the root.alive witness
//                                   BEFORE EVERY dispatch (dispatch-driven, not periodic),
//                                   stays idle longer than the case-1 serve window between the
//                                   two dispatches, writes the root.done completion witness
//                                   only after the final dispatch is verified, then observes
//                                   the connector's graceful root_completion_signal leave.
//
// CLAIM FENCE: lifecycle mechanism evidence only. No performance, Ray, production, or fabric
// claim.

#include <hpx/hpx_start.hpp>
#include <hpx/hpx.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/modules/program_options.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/version.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <typeinfo>
#include <vector>

#include <unistd.h>  // getpid

#include "common.hpp"  // defines + HPX_PLAIN_ACTION-registers exp70_probe_action (ONE TU)

namespace {

using namespace exp70;

// Outcome of one bounded remote dispatch. status: returned | threw | timed_out_no_result.
// throw_site: async_call (exception out of hpx::async itself -- exp63's "before_dispatch"
// stage) | future_get (exception delivered through the future).
struct DispatchOutcome {
    std::string status = "not_attempted";
    std::int64_t result = 0;
    std::int64_t oracle = 0;
    bool match = false;
    std::uint32_t executed_on = 0;
    bool proved_remote = false;
    std::string throw_site;
    std::string error_type;         // hpx::exception | std::system_error | std::exception | unknown
    std::string error_type_detail;  // typeid name of the caught exception
    std::string error_what;
    int hpx_error_code = -1;        // hpx::exception::get_error() when applicable
    int system_error_code = -1;     // std::error_code::value() when applicable
    std::string system_error_category;
    long long elapsed_ms = 0;
};

std::string outcome_json(const DispatchOutcome& d) {
    std::string j = "{";
    j += "\"status\":\"" + d.status + "\",";
    j += "\"result\":" + std::to_string(d.result) + ",";
    j += "\"oracle\":" + std::to_string(d.oracle) + ",";
    j += "\"match\":" + bquote(d.match) + ",";
    j += "\"executed_on\":" + std::to_string(d.executed_on) + ",";
    j += "\"proved_remote\":" + bquote(d.proved_remote) + ",";
    j += "\"throw_site\":\"" + json_escape(d.throw_site) + "\",";
    j += "\"error_type\":\"" + json_escape(d.error_type) + "\",";
    j += "\"error_type_detail\":\"" + json_escape(d.error_type_detail) + "\",";
    j += "\"error_what\":\"" + json_escape(d.error_what) + "\",";
    j += "\"hpx_error_code\":" + std::to_string(d.hpx_error_code) + ",";
    j += "\"system_error_code\":" + std::to_string(d.system_error_code) + ",";
    j += "\"system_error_category\":\"" + json_escape(d.system_error_category) + "\",";
    j += "\"elapsed_ms\":" + std::to_string(d.elapsed_ms);
    j += "}";
    return j;
}

// ONE bounded remote dispatch, exp69 post_get pattern: the work runs as a posted HPX thread
// doing an UNTIMED .get(); boundedness is enforced with std::future::wait_for on this plain OS
// thread. Captures scalars by value: on timeout the caller returns while the posted thread may
// still be running, so the body must never reference this frame.
DispatchOutcome bounded_probe(const hpx::id_type& target, std::uint32_t target_locality,
                              std::uint32_t here, std::int64_t x, int bound_s) {
    DispatchOutcome d;
    d.oracle = probe_oracle(x, target_locality);
    auto prom = std::make_shared<std::promise<DispatchOutcome>>();
    auto fut = prom->get_future();
    hpx::post([prom, target, target_locality, here, x]() {
        DispatchOutcome q;
        q.oracle = probe_oracle(x, target_locality);
        const long long t0 = steady_ns();
        q.throw_site = "async_call";
        try {
            auto f = hpx::async<exp70_probe_action>(target, x);
            q.throw_site = "future_get";
            q.result = f.get();
            q.throw_site = "";
            q.status = "returned";
            q.match = (q.result == q.oracle);
            q.executed_on = static_cast<std::uint32_t>((q.result - (x ^ kProbeXor)) >> 1);
            q.proved_remote = q.match && (q.executed_on == target_locality) &&
                              (q.executed_on != here);
        } catch (hpx::exception const& e) {
            q.status = "threw";
            q.error_type = "hpx::exception";
            q.error_type_detail = typeid(e).name();
            q.error_what = e.what();
            q.hpx_error_code = static_cast<int>(e.get_error());
            q.system_error_code = e.code().value();
            q.system_error_category = e.code().category().name();
        } catch (std::system_error const& e) {
            q.status = "threw";
            q.error_type = "std::system_error";
            q.error_type_detail = typeid(e).name();
            q.error_what = e.what();
            q.system_error_code = e.code().value();
            q.system_error_category = e.code().category().name();
        } catch (std::exception const& e) {
            q.status = "threw";
            q.error_type = "std::exception";
            q.error_type_detail = typeid(e).name();
            q.error_what = e.what();
        } catch (...) {
            q.status = "threw";
            q.error_type = "unknown";
        }
        q.elapsed_ms = (steady_ns() - t0) / 1000000;
        try { prom->set_value(std::move(q)); } catch (...) {}  // promise may be gone: ignore
    });
    if (fut.wait_for(std::chrono::seconds(bound_s)) != std::future_status::ready) {
        d.status = "timed_out_no_result";
        d.elapsed_ms = static_cast<long long>(bound_s) * 1000;
        return d;  // bounded at the std boundary; the posted thread finishes in background
    }
    return fut.get();
}

// Membership snapshot as (locality_id, id_type) pairs, taken on an HPX thread.
std::vector<std::pair<std::uint32_t, hpx::id_type>> snapshot_localities() {
    return hpx::run_as_hpx_thread([]() {
        std::vector<std::pair<std::uint32_t, hpx::id_type>> out;
        for (auto const& l : hpx::find_all_localities()) {
            out.emplace_back(hpx::naming::get_locality_id_from_id(l), l);
        }
        return out;
    });
}

}  // namespace

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp70 upstream_reproducer root options");
    // clang-format off
    desc.add_options()
        ("case", po::value<std::string>()->default_value("late-dispatch-current-behavior"),
            "late-dispatch-current-behavior | external-lifecycle-workaround")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "shared directory for marker files (loopback: a local temp dir)")
        ("join-timeout-s", po::value<int>()->default_value(30),
            "seconds to wait for the connector to join")
        ("dispatch-bound-s", po::value<int>()->default_value(10),
            "std-boundary bound for each remote dispatch")
        ("idle-dwell-s", po::value<int>()->default_value(6),
            "workaround case: root idle seconds between dispatch 1 and dispatch 2 "
            "(must exceed the case-1 serve window)")
        ("stop-wait-timeout-s", po::value<int>()->default_value(20),
            "current-behavior case: seconds to wait for the connector.stopped marker")
        ("leave-timeout-s", po::value<int>()->default_value(20),
            "workaround case: seconds to wait for the connector's graceful leave")
        ("hard-timeout-s", po::value<int>()->default_value(90),
            "hard wall-clock backstop; the process force-exits (rc 86) past this");
    // clang-format on

    const std::string kase = arg_value(argc, argv, "--case", "late-dispatch-current-behavior");
    const std::string bootdir = arg_value(argc, argv, "--bootstrap", ".");
    const int join_timeout_s = arg_int(argc, argv, "--join-timeout-s", 30);
    const int dispatch_bound_s = arg_int(argc, argv, "--dispatch-bound-s", 10);
    const int idle_dwell_s = arg_int(argc, argv, "--idle-dwell-s", 6);
    const int stop_wait_timeout_s = arg_int(argc, argv, "--stop-wait-timeout-s", 20);
    const int leave_timeout_s = arg_int(argc, argv, "--leave-timeout-s", 20);
    const int hard_timeout_s = arg_int(argc, argv, "--hard-timeout-s", 90);
    const bool workaround = (kase == "external-lifecycle-workaround");

    start_hard_timeout(hard_timeout_s, bootdir, "root");

    hpx::init_params params;
    params.desc_cmdline = desc;
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(bootdir + "/root.started", "{\"started\":false}\n");
        return 2;
    }
    const long pid = static_cast<long>(::getpid());
    const std::uint32_t here =
        hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    write_text(bootdir + "/root.started",
               "{\"role\":\"root\",\"started\":true,\"pid\":" + std::to_string(pid) +
                   ",\"case\":\"" + kase + "\",\"locality_id\":" + std::to_string(here) +
                   ",\"hostname\":\"" + json_escape(get_hostname()) +
                   "\",\"hpx_version\":\"" + json_escape(hpx::full_version_as_string()) +
                   "\",\"hpx_git_commit\":\"" HPX_HAVE_GIT_COMMIT "\",\"unix\":" +
                   std::to_string(now_unix()) + "}\n");

    // Teardown + final result live in a lambda so every early-exit path records provenance.
    // Root teardown is the exp63 embedded-root idiom: post(finalize) then stop.
    auto finish = [&](int rc, const std::string& note, bool sequence_complete) {
        bool teardown_clean = true;
        std::string teardown_err;
        int stop_rc = -1;
        try {
            hpx::post([]() { hpx::finalize(); });
            stop_rc = hpx::stop();
        } catch (const std::exception& e) { teardown_clean = false; teardown_err = e.what(); }
        catch (...) { teardown_clean = false; teardown_err = "unknown"; }
        write_text(bootdir + "/root.teardown",
                   "{\"clean\":" + bquote(teardown_clean) + ",\"stop_rc\":" +
                       std::to_string(stop_rc) + ",\"error\":\"" + json_escape(teardown_err) +
                       "\"}\n");
        write_text(bootdir + "/root_result.json",
                   "{\"role\":\"root\",\"case\":\"" + kase + "\",\"rc\":" + std::to_string(rc) +
                       ",\"sequence_complete\":" + bquote(sequence_complete) +
                       ",\"note\":\"" + json_escape(note) + "\",\"teardown_clean\":" +
                       bquote(teardown_clean) + ",\"unix\":" + std::to_string(now_unix()) +
                       "}\n");
        return (rc == 0 && !teardown_clean) ? 6 : rc;
    };

    // --- 1. wait for the connector to join (set-difference on locality ids, exp65 pattern) --
    std::uint32_t conn_locality = 0;
    hpx::id_type conn_id;
    bool joined = false;
    {
        const auto deadline = steady::now() + std::chrono::seconds(join_timeout_s);
        while (steady::now() < deadline) {
            for (auto const& [lid, id] : snapshot_localities()) {
                if (lid != here) { conn_locality = lid; conn_id = id; joined = true; break; }
            }
            if (joined) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    write_text(bootdir + "/root.observed_join",
               "{\"joined\":" + bquote(joined) + ",\"connector_locality_id\":" +
                   std::to_string(conn_locality) + ",\"unix\":" + std::to_string(now_unix()) +
                   "}\n");
    if (!joined) return finish(3, "connector never joined", false);

    // Dispatch-driven activity witness (workaround case only): bump root.alive BEFORE the
    // dispatch, exactly the exp63 _touch_heartbeat semantics. NOT a periodic heartbeat.
    auto witness_before_dispatch = [&]() {
        if (workaround) write_text(bootdir + "/root.alive", std::to_string(now_unix()) + "\n");
    };

    // --- 2. action 1 -------------------------------------------------------------------------
    witness_before_dispatch();
    DispatchOutcome a1 = bounded_probe(conn_id, conn_locality, here, 41, dispatch_bound_s);
    const bool a1_ok = (a1.status == "returned") && a1.proved_remote;
    write_text(bootdir + "/root.action1",
               "{\"ok\":" + bquote(a1_ok) + ",\"outcome\":" + outcome_json(a1) +
                   ",\"unix\":" + std::to_string(now_unix()) + "}\n");
    if (!a1_ok) return finish(5, "action 1 did not complete verified", false);

    DispatchOutcome a2;
    bool stopping_seen = false, stopped_seen = false;

    if (!workaround) {
        // --- 3a. current behavior: wait for the connector to COMPLETE its stop path ----------
        // Ordering proof is marker-based, not sleep-based: dispatch 2 happens only after the
        // connector.stopped marker (written after the connector's hpx::stop() returned) is
        // observed on disk.
        const auto deadline = steady::now() + std::chrono::seconds(stop_wait_timeout_s);
        while (steady::now() < deadline) {
            if (!stopping_seen && file_exists(bootdir + "/connector.stopping"))
                stopping_seen = true;
            if (file_exists(bootdir + "/connector.stopped")) { stopped_seen = true; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        write_text(bootdir + "/root.observed_stop",
                   "{\"stopping_seen\":" + bquote(stopping_seen) + ",\"stopped_seen\":" +
                       bquote(stopped_seen) + ",\"unix\":" + std::to_string(now_unix()) + "}\n");
        if (!stopped_seen)
            return finish(4, "connector.stopped marker never appeared; race not constructed",
                          false);

        // --- 4a. action 2 to the SAVED id of the departed connector --------------------------
        write_text(bootdir + "/root.action2_attempt",
                   "{\"after_connector_stopped\":true,\"unix\":" + std::to_string(now_unix()) +
                       "}\n");
        a2 = bounded_probe(conn_id, conn_locality, here, 43, dispatch_bound_s);
        write_text(bootdir + "/root.action2",
                   "{\"ok\":" + bquote(a2.status == "returned" && a2.proved_remote) +
                       ",\"attempted_after_connector_stopped\":true,\"outcome\":" +
                       outcome_json(a2) + ",\"unix\":" + std::to_string(now_unix()) + "}\n");
        return finish(0, "current-behavior sequence complete", true);
    }

    // --- 3b. workaround: idle LONGER than the case-1 serve window, no witness during idle ----
    write_text(bootdir + "/root.idle_begin",
               "{\"idle_dwell_s\":" + std::to_string(idle_dwell_s) + ",\"unix\":" +
                   std::to_string(now_unix()) + "}\n");
    std::this_thread::sleep_for(std::chrono::seconds(idle_dwell_s));
    write_text(bootdir + "/root.idle_end", "{\"unix\":" + std::to_string(now_unix()) + "}\n");

    // --- 4b. action 2, witness first (dispatch-driven) ---------------------------------------
    witness_before_dispatch();
    write_text(bootdir + "/root.action2_attempt",
               "{\"after_connector_stopped\":false,\"unix\":" + std::to_string(now_unix()) +
                   "}\n");
    a2 = bounded_probe(conn_id, conn_locality, here, 43, dispatch_bound_s);
    const bool a2_ok = (a2.status == "returned") && a2.proved_remote;
    write_text(bootdir + "/root.action2",
               "{\"ok\":" + bquote(a2_ok) +
                   ",\"attempted_after_connector_stopped\":false,\"outcome\":" +
                   outcome_json(a2) + ",\"unix\":" + std::to_string(now_unix()) + "}\n");
    if (!a2_ok) return finish(5, "action 2 did not complete verified", false);

    // --- 5b. explicit completion witness ONLY after the final remote work is verified --------
    write_text(bootdir + "/root.done", std::to_string(now_unix()) + "\n");

    // --- 6b. observe the graceful leave: marker + membership --------------------------------
    bool leave_marker_seen = false, membership_shrunk = false;
    {
        const auto deadline = steady::now() + std::chrono::seconds(leave_timeout_s);
        while (steady::now() < deadline) {
            if (!leave_marker_seen && file_exists(bootdir + "/connector.stopped"))
                leave_marker_seen = true;
            bool present = false;
            for (auto const& [lid, id] : snapshot_localities()) {
                if (lid == conn_locality) present = true;
            }
            if (!present) membership_shrunk = true;
            if (leave_marker_seen && membership_shrunk) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    write_text(bootdir + "/root.observed_leave",
               "{\"leave_marker_seen\":" + bquote(leave_marker_seen) +
                   ",\"membership_shrunk\":" + bquote(membership_shrunk) + ",\"unix\":" +
                   std::to_string(now_unix()) + "}\n");
    if (!leave_marker_seen || !membership_shrunk)
        return finish(7, "graceful leave not fully observed", false);

    return finish(0, "workaround sequence complete", true);
}
