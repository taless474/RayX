// exp70 upstream_reproducer CONNECTOR -- connect-mode HPX locality (EXPERIMENT-ONLY, HPX-only).
//
// Reduced from the exp63 connect-mode connector (collective_connector.cpp): hpx::start with
// runtime_mode::connect driven from a non-HPX main thread, the same wait-loop structure, and
// the exp49-proven graceful leave (post(disconnect) then hpx::stop()). Loopback-only: the
// exp63 subnet self-bind, AGAS TCP pre-probe, and TCP_NODELAY attestation are intentionally
// omitted.
//
// Lifetime contract by case (selected with --case):
//   late-dispatch-current-behavior  PRE-hardening exp63 semantics: a FIXED local serve window
//                                   measured from join on the connector's own steady clock.
//                                   When it expires the connector begins and completes its
//                                   normal stop path, even if the root may still dispatch.
//   external-lifecycle-workaround   Reduced exp63 hardening: leave on the root.done COMPLETION
//                                   witness; the serve window becomes a DEADMAN that fires
//                                   only if the root.alive activity witness has not advanced
//                                   for --deadman-s seconds, measured on the connector's OWN
//                                   monotonic (steady) clock.
//
// Markers: connector.joined -> connector.stopping (stop path entered) -> connector.stopped
// (hpx::stop() returned). The root's case-1 ordering proof polls these markers.
//
// CLAIM FENCE: lifecycle mechanism evidence only. No performance, Ray, production, or fabric
// claim.

#include <hpx/hpx_start.hpp>
#include <hpx/hpx.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/modules/program_options.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/version.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#include <unistd.h>  // getpid

#include "common.hpp"  // defines + HPX_PLAIN_ACTION-registers exp70_probe_action (ONE TU)

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    using namespace exp70;

    po::options_description desc("exp70 upstream_reproducer connector options");
    // clang-format off
    desc.add_options()
        ("case", po::value<std::string>()->default_value("late-dispatch-current-behavior"),
            "late-dispatch-current-behavior | external-lifecycle-workaround")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "shared directory for marker files (loopback: a local temp dir)")
        ("serve-window-s", po::value<int>()->default_value(3),
            "current-behavior case: fixed local serve window from join (the pre-hardening "
            "exp63 lifetime boundary)")
        ("deadman-s", po::value<int>()->default_value(15),
            "workaround case: monotonic deadman; fires only after this many seconds of "
            "root.alive silence")
        ("hard-timeout-s", po::value<int>()->default_value(90),
            "hard wall-clock backstop; the process force-exits (rc 86) past this");
    // clang-format on

    const std::string kase = arg_value(argc, argv, "--case", "late-dispatch-current-behavior");
    const std::string bootdir = arg_value(argc, argv, "--bootstrap", ".");
    const int serve_window_s = arg_int(argc, argv, "--serve-window-s", 3);
    const int deadman_s = arg_int(argc, argv, "--deadman-s", 15);
    const int hard_timeout_s = arg_int(argc, argv, "--hard-timeout-s", 90);
    const bool workaround = (kase == "external-lifecycle-workaround");

    start_hard_timeout(hard_timeout_s, bootdir, "connector");

    hpx::init_params params;
    params.desc_cmdline = desc;
    params.mode = hpx::runtime_mode::connect;
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(bootdir + "/connector.joined", "{\"started\":false}\n");
        return 2;
    }

    const long pid = static_cast<long>(::getpid());
    const std::uint32_t hereloc =
        hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    write_text(bootdir + "/connector.joined",
               "{\"role\":\"connector\",\"started\":true,\"pid\":" + std::to_string(pid) +
                   ",\"case\":\"" + kase + "\",\"locality_id\":" + std::to_string(hereloc) +
                   ",\"hostname\":\"" + json_escape(get_hostname()) +
                   "\",\"hpx_version\":\"" + json_escape(hpx::full_version_as_string()) +
                   "\",\"hpx_git_commit\":\"" HPX_HAVE_GIT_COMMIT "\",\"unix\":" +
                   std::to_string(now_unix()) + "}\n");

    // ---- serve loop -------------------------------------------------------------------------
    // Both cases poll on the connector's own plain OS main thread (100 ms, std sleep -- no HPX
    // timed wait), exactly the exp63 connector wait-loop structure.
    const std::string done_path = bootdir + "/root.done";
    const std::string served_path = bootdir + "/served1.ok";  // legacy exp63 completion name
    const std::string alive_path = bootdir + "/root.alive";

    std::string exit_reason = "unknown";
    bool root_completion_signaled = false;
    bool deadman_expired = false;
    bool serve_window_expired = false;
    int witness_advances = 0;
    double root_completion_unix = 0.0;
    double observed_completion_unix = 0.0;

    if (!workaround) {
        // PRE-hardening semantics: fixed serve window from join, connector's own steady clock.
        // (The legacy served1.ok completion check is kept for shape fidelity; in case 1 the
        // root intentionally never writes it.)
        const auto window_end = steady::now() + std::chrono::seconds(serve_window_s);
        for (;;) {
            if (file_exists(served_path)) {
                root_completion_signaled = true;
                observed_completion_unix = now_unix();
                exit_reason = "root_completion_signal";
                break;
            }
            if (steady::now() >= window_end) {
                serve_window_expired = true;
                exit_reason = "serve_window_expired";
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    } else {
        // Reduced exp63 hardening: completion witness OR monotonic deadman on root silence.
        // The deadman window resets whenever the root.alive mtime ADVANCES (dispatch-driven
        // activity witness); it fires only after deadman_s seconds without any advance.
        time_t last_seen_alive_mtime = 0;
        auto last_progress = steady::now();  // deadman window starts at join
        for (;;) {
            if (file_exists(done_path)) {
                root_completion_signaled = true;
                std::ifstream f(done_path);
                f >> root_completion_unix;
                observed_completion_unix = now_unix();
                exit_reason = "root_completion_signal";
                break;
            }
            if (file_exists(served_path)) {  // legacy completion signal
                root_completion_signaled = true;
                observed_completion_unix = now_unix();
                exit_reason = "root_completion_signal";
                break;
            }
            const time_t m = file_mtime(alive_path);
            if (m != 0 && m != last_seen_alive_mtime) {
                last_seen_alive_mtime = m;
                last_progress = steady::now();  // fresh activity witness -> reset the deadman
                ++witness_advances;
            }
            if (steady::now() - last_progress >= std::chrono::seconds(deadman_s)) {
                deadman_expired = true;
                exit_reason = "deadman_expired_root_silent";
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // ---- stop path --------------------------------------------------------------------------
    // Marker BEFORE the stop path begins, so the root can prove "entered stop path" ordering.
    write_text(bootdir + "/connector.stopping",
               "{\"reason\":\"" + exit_reason + "\",\"unix\":" + std::to_string(now_unix()) +
                   "}\n");

    int rc = 0;
    bool clean = true;
    std::string err;
    try {
        hpx::post([]() { hpx::disconnect(); });  // exp49-proven graceful leave
        rc = hpx::stop();
    } catch (const std::exception& e) { clean = false; err = e.what(); }
    catch (...) { clean = false; err = "unknown"; }

    const std::string shutdown_reason = !clean ? std::string("error") : exit_reason;
    const bool normal_completion = clean && root_completion_signaled;
    write_text(bootdir + "/connector.stopped",
               "{\"clean\":" + bquote(clean) + ",\"rc\":" + std::to_string(rc) +
                   ",\"teardown\":\"post(disconnect)+stop\"" +
                   ",\"case\":\"" + kase + "\"" +
                   ",\"connector_shutdown_reason\":\"" + shutdown_reason + "\"" +
                   ",\"normal_completion\":" + bquote(normal_completion) +
                   ",\"root_completion_signaled\":" + bquote(root_completion_signaled) +
                   ",\"serve_window_expired\":" + bquote(serve_window_expired) +
                   ",\"deadman_expired\":" + bquote(deadman_expired) +
                   ",\"witness_advances\":" + std::to_string(witness_advances) +
                   ",\"serve_window_s\":" + std::to_string(serve_window_s) +
                   ",\"deadman_s\":" + std::to_string(deadman_s) +
                   ",\"root_completion_unix\":" + std::to_string(root_completion_unix) +
                   ",\"observed_completion_unix\":" + std::to_string(observed_completion_unix) +
                   ",\"error\":\"" + json_escape(err) + "\"}\n");
    return clean ? 0 : 1;
}
