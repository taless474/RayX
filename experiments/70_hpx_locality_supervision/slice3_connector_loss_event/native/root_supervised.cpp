// exp70 Slice 3B ROOT -- exercises CURRENT upstream HPX supervision + hpx::force_disconnect
// against the silent crash of a late-connecting, Ray-actor-hosted connector locality (HPX
// issue #7390 / #7441 / merged PR #7447, "Adding hpx::force_disconnect").
//
// Reuses the Slice 3 topology unmodified: this standalone, separately-supervised, WORK-FREE
// root (locality 0) plus two (later three) Ray-actor-hosted connect-mode HPX localities
// (connector_ext.cpp, the exp67/68 in-Ray-actor hosting mechanism). Root plays the
// "launcher"/observer role late_component_launcher.cpp plays upstream, adapted from
// process::execute()-spawned OS workers to Ray-actor-hosted connectors.
//
// THREE SEPARATE RESPONSIBILITIES this binary keeps observably distinct (see the user-facing
// exp70 write-up for the full rationale):
//   1. HPX runtime/supervision detects+classifies the silent connector crash. This binary
//      NEVER calls publish_event(event::failed) anywhere (see g_app_failed_publish_count,
//      which therefore can only ever read 0) -- classification is entirely
//      components/supervision_dispatch's own failure_detection_loop() background sweep,
//      started automatically by hpx::supervision::init().
//   2. supervision_dispatch fences the failed incarnation -- observed here via
//      hpx::supervision::check_admission(), a pure local read of already-resident latch
//      state with no cross-binary action-registration risk (unlike the templated
//      dispatch_work<Action>()/fenced_action<> wrapper, which this file also attempts as a
//      best-effort DIAGNOSTIC ONLY, given real uncertainty about whether a template-based
//      fenced action instantiated only in the launcher binary is guaranteed registered in a
//      separately-compiled connector binary; see probe_fenced_diagnostic() below).
//   3. This root explicitly, deliberately invokes hpx::force_disconnect() as the recovery
//      action, ONLY after step 2 has been observed -- never automatically, never speculatively.
//
// "Useful HPX work" (steps 5/14 of the exp70 Slice 3B spec) is plain_probe(): an ordinary,
// non-supervision-fenced hpx::async<exp70_probe_action> dispatch (same closed-form oracle
// upstream_reproducer/root.cpp already uses), so the identity/result verification never
// depends on the templated fenced-dispatch path above.
//
// File-marker rendezvous protocol (exp63/65/68 idiom; all one-line flat JSON):
//   root.started              -- pid/locality/hpx version + HPX_HAVE_SUPERVISION/
//                                 HPX_HAVE_FORCE_DISCONNECT compile-time flags
//   root.joined                -- discovered peer a/b locality ids, join epochs, pre-crash
//                                  is_connecting()
//   root.work_verified          -- pre-crash plain-probe results for a and b +
//                                  application_failed_event_publish_count (0, live-read)
//   root.fencing_observed        -- check_admission()/query_state() polling timeline,
//                                  runtime_failure_classification_observed,
//                                  stale_incarnation_fenced, best-effort dispatch_work
//                                  diagnostic
//   root.force_disconnect        -- force_disconnect() invocation record (only if fenced)
//   root.force_disconnect_effect -- resolve()/membership/plain-probe evidence of its effect
//   root.ready_for_replacement    -- signal: python may now launch connector C
//   root_result.json             -- full gate-field aggregate (see the exp70 write-up)
//
// Reads (written by the python driver): <bootdir>/native.roles.json, rewritten once after the
// replacement joins to add "c".
//
// CLAIM FENCE: mechanism/validation evidence for the CURRENT upstream supervision_dispatch +
// force_disconnect API only. No performance, Ray-vs-HPX, production, or general-fabric claim.
// force_disconnect is asserted only against a late-connecting (is_connecting==true) locality;
// this binary makes no claim about non-late-connecting or console/root loss (see Slice 4B).

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/init_runtime/finalize.hpp>
#include <hpx/modules/agas.hpp>
#include <hpx/modules/program_options.hpp>
#include <hpx/modules/runtime_distributed.hpp>
#include <hpx/modules/supervision.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_num_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/version.hpp>

#include <hpx/supervision_dispatch.hpp>

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <unistd.h>  // getpid

#include "../../upstream_reproducer/common.hpp"  // exp70_probe_action (ONE TU per binary)

namespace {

using namespace exp70;

// Structural witness: can only ever become non-zero if this file were to call
// hpx::supervision::publish_event(..., event::failed, ...) itself. It never does -- grep this
// file for "event::failed" and it will not be found outside this comment.
std::atomic<std::uint64_t> g_app_failed_publish_count{0};

std::string g_bootdir = ".";

// ---- tiny JSON array/object builders (flat; matches exp70 house style) --------------------

std::string jkv(const std::string& k, const std::string& v_json) {
    return "\"" + k + "\":" + v_json;
}
std::string jstr(const std::string& s) { return "\"" + json_escape(s) + "\""; }
// Template + enable_if rather than a plain jnum(long long) overload: an integral argument of
// any width/signedness (int, long, uint32_t, the mapped_type of std::map<..., unsigned int>,
// ...) is an EXACT match for the template (no conversion needed), so it is preferred over
// jnum(double) unambiguously. A plain long long overload alongside jnum(double) left every
// non-long-long integral caller ambiguous under GCC (both candidates require a conversion,
// neither is a better match) -- caught only by the actual Rostam GCC build, not by local
// clang tooling that had no working include paths for this freshly-added file.
template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
std::string jnum(T v) { return std::to_string(static_cast<long long>(v)); }
std::string jnum(double v) { std::ostringstream o; o << v; return o.str(); }
std::string jbool(bool b) { return bquote(b); }

// ---- plain (non-fenced) probe: the "useful HPX work" oracle --------------------------------

struct PlainProbe {
    bool found = false, ok = false;
    std::int64_t result = 0, oracle = 0;
    std::string status;  // "returned" | "not_found" | "timed_out" | "threw"
    std::string error;
};

std::string plain_probe_json(const PlainProbe& p) {
    return "{" + jkv("found", jbool(p.found)) + "," + jkv("ok", jbool(p.ok)) + "," +
           jkv("result", jnum(p.result)) + "," + jkv("oracle", jnum(p.oracle)) + "," +
           jkv("status", jstr(p.status)) + "," + jkv("error", jstr(p.error)) + "}";
}

// Runs entirely on an HPX thread. `all_localities` is passed in (already snapshotted) so
// repeated calls in a tight loop do not each re-snapshot membership.
PlainProbe plain_probe_on_hpx_thread(std::uint32_t target_locality, std::int64_t x, int bound_s) {
    PlainProbe p;
    p.oracle = probe_oracle(x, target_locality);
    hpx::id_type target = hpx::invalid_id;
    for (auto const& id : hpx::find_all_localities()) {
        if (hpx::naming::get_locality_id_from_id(id) == target_locality) target = id;
    }
    if (target == hpx::invalid_id) { p.status = "not_found"; return p; }
    p.found = true;
    try {
        auto f = hpx::async<exp70_probe_action>(target, x);
        auto status = f.wait_for(std::chrono::seconds(bound_s));
        if (status != hpx::future_status::ready) { p.status = "timed_out"; return p; }
        p.result = f.get();
        p.status = "returned";
        p.ok = (p.result == p.oracle);
    } catch (hpx::exception const& e) {
        p.status = "threw";
        p.error = std::string("hpx::exception: ") + e.what();
    } catch (std::exception const& e) {
        p.status = "threw";
        p.error = std::string("std::exception: ") + e.what();
    }
    return p;
}

PlainProbe plain_probe(std::uint32_t target_locality, std::int64_t x, int bound_s) {
    return hpx::run_as_hpx_thread(
        [=]() { return plain_probe_on_hpx_thread(target_locality, x, bound_s); });
}

// ---- fencing observation --------------------------------------------------------------

struct FenceAttempt {
    double t_unix = 0.0;
    std::string dispatch_outcome;  // "admitted" | "rejected_fenced"
    std::string last_event;        // supervision::event as a string
    std::uint64_t event_seq = 0;
};

std::string event_name(hpx::supervision::event ev) {
    using E = hpx::supervision::event;
    switch (ev) {
        case E::unknown: return "unknown";
        case E::started: return "started";
        case E::running: return "running";
        case E::suspending: return "suspending";
        case E::completed: return "completed";
        case E::failed: return "failed";
        case E::losing_locality: return "losing_locality";
    }
    return "unrecognized";
}

std::string fence_attempt_json(const FenceAttempt& a) {
    return "{" + jkv("t_unix", jnum(a.t_unix)) + "," +
           jkv("dispatch_outcome", jstr(a.dispatch_outcome)) + "," +
           jkv("last_event", jstr(a.last_event)) + "," +
           jkv("event_seq", jnum(static_cast<long long>(a.event_seq))) + "}";
}

// One poll iteration on an HPX thread: check_admission() (the gating oracle -- a pure local
// read, no action-dispatch/registration risk) + query_state() (records what the runtime
// itself classified the peer as, independent of check_admission's own latch read).
FenceAttempt fence_probe_on_hpx_thread(hpx::id_type peer_locality, std::uint64_t peer_epoch) {
    FenceAttempt a;
    a.t_unix = now_unix();
    auto const outcome = hpx::supervision::check_admission(peer_locality, peer_epoch);
    a.dispatch_outcome =
        (outcome == hpx::supervision::dispatch_outcome::rejected_fenced) ? "rejected_fenced"
                                                                          : "admitted";
    hpx::error_code ec(hpx::throwmode::lightweight);
    auto const st = hpx::supervision::query_state(peer_locality, ec);
    a.last_event = ec ? "query_error" : event_name(st.last_event);
    a.event_seq = st.event_sequence_number;
    return a;
}

FenceAttempt fence_probe(hpx::id_type peer_locality, std::uint64_t peer_epoch) {
    return hpx::run_as_hpx_thread(
        [=]() { return fence_probe_on_hpx_thread(peer_locality, peer_epoch); });
}

// Best-effort DIAGNOSTIC ONLY, never gating: attempts the templated
// hpx::supervision::dispatch_work<exp70_probe_action>() fenced-dispatch path. Recorded
// separately from check_admission()'s result because dispatch_work<>'s fenced_action<> wrapper
// is a class template instantiated per-TU; whether HPX's action registration guarantees a
// matching instantiation is available in a SEPARATELY COMPILED connector binary that never
// itself calls dispatch_work() is not something this experiment independently verified against
// upstream (the worked late_component_launcher.cpp/late_component_worker.cpp pair never
// exercises the cross-binary case either -- late_component_worker.cpp never calls
// dispatch_work). Any outcome (including a link/dispatch-time error) is recorded verbatim and
// never fails the run.
struct DispatchWorkDiagnostic {
    bool attempted = false;
    std::string outcome;  // "target_fenced" | "returned" | "other_error" | "not_attempted"
    std::string detail;
};

std::string dispatch_work_diag_json(const DispatchWorkDiagnostic& d) {
    return "{" + jkv("attempted", jbool(d.attempted)) + "," + jkv("outcome", jstr(d.outcome)) +
           "," + jkv("detail", jstr(d.detail)) + "}";
}

DispatchWorkDiagnostic dispatch_work_diagnostic(hpx::id_type peer_locality,
                                                std::uint64_t peer_epoch, std::int64_t x) {
    return hpx::run_as_hpx_thread([=]() {
        DispatchWorkDiagnostic d;
        d.attempted = true;
        try {
            auto f = hpx::supervision::dispatch_work<exp70_probe_action>(
                peer_locality, peer_epoch, x);
            auto status = f.wait_for(std::chrono::seconds(5));
            if (status != hpx::future_status::ready) {
                d.outcome = "other_error";
                d.detail = "timed_out_no_result";
                return d;
            }
            f.get();
            d.outcome = "returned";
        } catch (hpx::exception const& e) {
            d.outcome = (e.get_error() == hpx::error::target_fenced) ? "target_fenced"
                                                                      : "other_error";
            d.detail = e.what();
        } catch (std::exception const& e) {
            d.outcome = "other_error";
            d.detail = e.what();
        }
        return d;
    });
}

// ---- membership / roles.json --------------------------------------------------------------

std::size_t membership_count() {
    return hpx::run_as_hpx_thread([]() { return hpx::find_all_localities().size(); });
}

// Extremely small hand-rolled reader for the two-or-three-key flat int map python writes to
// native.roles.json, e.g. {"a": 1, "b": 2} or {"a": 1, "b": 2, "c": 3}. Avoids a JSON library
// dependency for a trivial, fully-controlled schema (exp70/exp63 style).
bool read_roles(const std::string& path, std::map<std::string, std::uint32_t>& out) {
    std::ifstream f(path);
    if (!f) return false;
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    out.clear();
    std::size_t pos = 0;
    while (true) {
        auto qpos = content.find('"', pos);
        if (qpos == std::string::npos) break;
        auto qend = content.find('"', qpos + 1);
        if (qend == std::string::npos) break;
        std::string key = content.substr(qpos + 1, qend - qpos - 1);
        auto colon = content.find(':', qend);
        if (colon == std::string::npos) break;
        auto numend = colon + 1;
        while (numend < content.size() && (content[numend] == ' ')) ++numend;
        auto numstart = numend;
        while (numend < content.size() && isdigit(static_cast<unsigned char>(content[numend])))
            ++numend;
        if (numend == numstart) break;
        out[key] = static_cast<std::uint32_t>(std::stoul(content.substr(numstart, numend - numstart)));
        pos = numend;
    }
    return !out.empty();
}

hpx::id_type locality_for(std::uint32_t locality_id) {
    return hpx::run_as_hpx_thread([=]() {
        for (auto const& id : hpx::find_all_localities()) {
            if (hpx::naming::get_locality_id_from_id(id) == locality_id) return id;
        }
        return hpx::invalid_id;
    });
}

}  // namespace

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp70 slice3b root options");
    desc.add_options()
        ("bootstrap", po::value<std::string>()->default_value("."), "rendezvous dir")
        ("join-timeout-s", po::value<int>()->default_value(30),
         "seconds to wait for both initial connectors (a, b) to appear")
        ("replacement-timeout-s", po::value<int>()->default_value(30),
         "seconds to wait for the replacement connector (c) to appear")
        ("discovery-timeout-ms", po::value<int>()->default_value(5000),
         "hpx::supervision discover_and_join()/init() timeout")
        ("fd-poll-ms", po::value<int>()->default_value(500),
         "failure_detection_loop() poll timeout override "
         "(hpx::supervision::testing::set_failure_detection_poll_timeout_for_testing); "
         "bounds test wall-clock time instead of the 60s upstream default")
        ("fence-wait-timeout-s", po::value<int>()->default_value(60),
         "bounded wait for check_admission() to observe rejected_fenced after the crash")
        ("hard-timeout-s", po::value<int>()->default_value(180),
         "hard wall-clock backstop; the process force-exits (rc 86) past this");

    g_bootdir = arg_value(argc, argv, "--bootstrap", ".");
    const int join_timeout_s = arg_int(argc, argv, "--join-timeout-s", 30);
    const int replacement_timeout_s = arg_int(argc, argv, "--replacement-timeout-s", 30);
    const int discovery_timeout_ms = arg_int(argc, argv, "--discovery-timeout-ms", 5000);
    const int fd_poll_ms = arg_int(argc, argv, "--fd-poll-ms", 500);
    const int fence_wait_timeout_s = arg_int(argc, argv, "--fence-wait-timeout-s", 60);
    const int hard_timeout_s = arg_int(argc, argv, "--hard-timeout-s", 180);

    start_hard_timeout(hard_timeout_s, g_bootdir, "root");

    std::vector<std::string> const cfg = {"hpx.expect_connecting_localities!=1"};
    hpx::init_params params;
    params.desc_cmdline = desc;
    params.cfg = cfg;
    params.mode = hpx::runtime_mode::console;
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(g_bootdir + "/root.started", "{\"started\":false}\n");
        return 2;
    }

    const long pid = static_cast<long>(::getpid());
    const std::uint32_t here = hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });

#if defined(HPX_HAVE_SUPERVISION)
    const bool have_supervision = true;
#else
    const bool have_supervision = false;
#endif
#if defined(HPX_HAVE_FORCE_DISCONNECT)
    const bool have_force_disconnect = true;
#else
    const bool have_force_disconnect = false;
#endif

    write_text(g_bootdir + "/root.started",
        "{" + jkv("role", jstr("root")) + "," + jkv("started", jbool(true)) + "," +
        jkv("pid", jnum(pid)) + "," + jkv("locality_id", jnum(here)) + "," +
        jkv("hostname", jstr(get_hostname())) + "," +
        jkv("hpx_version", jstr(hpx::full_version_as_string())) + "," +
        jkv("hpx_git_commit", jstr(HPX_HAVE_GIT_COMMIT)) + "," +
        jkv("hpx_have_supervision", jbool(have_supervision)) + "," +
        jkv("hpx_have_force_disconnect", jbool(have_force_disconnect)) + "," +
        jkv("unix", jnum(now_unix())) + "}\n");

    auto finish = [&](int rc, const std::string& note) {
        hpx::run_as_hpx_thread([]() {
            hpx::error_code ec(hpx::throwmode::lightweight);
            hpx::supervision::finalize();
        });
        write_text(g_bootdir + "/root.finish_note",
            "{" + jkv("rc", jnum(rc)) + "," + jkv("note", jstr(note)) + "}\n");
        hpx::post([]() { hpx::finalize(); });
        int stop_rc = hpx::stop();
        return (rc == 0 && stop_rc != 0) ? 6 : rc;
    };

    // native/CMakeLists.txt refuses to configure without HPX_WITH_SUPERVISION (which also
    // enables HPX_HAVE_FORCE_DISCONNECT), and finish() above already uses
    // hpx::supervision::finalize() unconditionally -- so this file cannot compile at all
    // against an HPX build lacking supervision. There is deliberately no separate runtime
    // fallback branch for that case: it would be dead code masquerading as a second guard.

    // ---- 1. supervision init: shorten the failure-detection poll timeout FIRST -------------
    hpx::run_as_hpx_thread([&]() {
        hpx::supervision::testing::set_failure_detection_poll_timeout_for_testing(
            std::chrono::milliseconds(fd_poll_ms));
    });
    hpx::supervision::registry const handle = hpx::run_as_hpx_thread([&]() {
        return hpx::supervision::init(
            hpx::launch::sync, std::chrono::milliseconds(discovery_timeout_ms));
    });
    std::uint64_t const root_epoch = hpx::run_as_hpx_thread([&]() {
        return hpx::supervision::query_state(hpx::launch::sync, handle).epoch;
    });
    hpx::run_as_hpx_thread([&]() {
        hpx::supervision::publish_event(
            hpx::launch::sync, handle, hpx::supervision::event::running, root_epoch);
    });

    // ---- 2. wait for a, b to appear (membership root+a+b == 3) -----------------------------
    {
        auto const deadline = steady::now() + std::chrono::seconds(join_timeout_s);
        while (steady::now() < deadline && membership_count() != 3) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    if (membership_count() != 3) return finish(3, "a/b never both appeared");

    // native.roles.json is written by the python driver only after it has ALSO called
    // supervision_init() on both actors (a slower path than the raw HPX membership check
    // above), so a single read here races the write and can fail even though the file is
    // about to appear -- bounded retry, not a one-shot check, mirroring the membership wait
    // immediately above it. (First hardware attempt, job 185467: a one-shot read failed with
    // "native.roles.json missing a/b" well inside join_timeout_s while python was still
    // finishing b's supervision_init call.)
    std::map<std::string, std::uint32_t> roles;
    {
        auto const deadline = steady::now() + std::chrono::seconds(join_timeout_s);
        while (steady::now() < deadline) {
            if (read_roles(g_bootdir + "/native.roles.json", roles) && roles.count("a") &&
                roles.count("b")) {
                break;
            }
            roles.clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    if (!roles.count("a") || !roles.count("b")) {
        return finish(4, "native.roles.json missing a/b");
    }

    // Diagnostic only (2026-08-18 source-level hypothesis test, exp70 continuation): measures
    // whether hpx::get_initial_num_localities() -- a boot-time-frozen count, documented as "the
    // number of localities which were registered at startup" -- stays at 1 for a root that
    // boots alone, which would explain discover_and_join()'s zero-peer result via
    // symbol_namespace_locality()'s routing (libs/full/agas_base/src/symbol_namespace.cpp):
    // a "/<locality_id>/..." key is only routed directly to that locality if
    // locality_id < get_initial_num_localities() or the caller IS that locality; otherwise it
    // falls through to hash(key) % get_initial_num_localities(), which is always 0 when that
    // count is 1 -- i.e. always routed back to root itself. Recorded BEFORE discover_and_join()
    // so this measurement cannot be affected by that call's own retry loop below.
    std::size_t const initial_num_localities = hpx::get_initial_num_localities();
    std::size_t const live_all_localities_count = hpx::run_as_hpx_thread(
        [&]() { return hpx::find_all_localities().size(); });
    std::size_t const remote_localities_seen = hpx::run_as_hpx_thread(
        [&]() { return hpx::find_remote_localities().size(); });
    write_text(g_bootdir + "/root.locality_counts_diag",
        "{" + jkv("role", jstr("root")) + "," + jkv("own_locality_id", jnum(here)) + "," +
        jkv("initial_num_localities", jnum(static_cast<long long>(initial_num_localities))) +
        "," + jkv("live_all_localities_count",
            jnum(static_cast<long long>(live_all_localities_count))) + "," +
        jkv("live_remote_localities_count",
            jnum(static_cast<long long>(remote_localities_seen))) + "," +
        jkv("unix", jnum(now_unix())) + "}\n");

    // Diagnostic only: raw hpx::agas::resolve_name() against the EXACT name pattern
    // registry::register_name()/discover_peers() both use
    // ("/" + locality_id + "/supervision_dispatch/registry"), bypassing the registry client
    // wrapper entirely. Isolates "the AGAS symbol itself is not visible from root's locality"
    // from any behavior specific to discover_peers()'s registry-client construction.
    struct RawResolve { bool ok = false; std::string error; };
    auto raw_resolve = [&](std::uint32_t lid) {
        return hpx::run_as_hpx_thread([lid]() {
            RawResolve r;
            hpx::error_code ec(hpx::throwmode::lightweight);
            std::string const name =
                "/" + std::to_string(lid) + "/supervision_dispatch/registry";
            hpx::id_type const id = hpx::agas::resolve_name(hpx::launch::sync, name, ec);
            r.ok = !ec && id;
            r.error = ec ? ec.get_message() : std::string();
            return r;
        });
    };
    RawResolve const raw_a = raw_resolve(roles["a"]);
    RawResolve const raw_b = raw_resolve(roles["b"]);
    write_text(g_bootdir + "/root.raw_resolve_diag",
        "{" + jkv("a_resolved", jbool(raw_a.ok)) + "," + jkv("a_error", jstr(raw_a.error)) +
        "," + jkv("b_resolved", jbool(raw_b.ok)) + "," + jkv("b_error", jstr(raw_b.error)) +
        "," + jkv("unix", jnum(now_unix())) + "}\n");

    // ---- 3. discover_and_join(): create local registry entries for a, b -------------------
    // discover_and_join() is documented safe to call repeatedly (idempotent for
    // already-joined peers), so retry across join_timeout_s rather than a single shot: job
    // 185469's first hardware attempt returned zero peers on one immediate call even though
    // both a and b's own supervision_init() (which registers their registry symbol name) had
    // already returned successfully well before this point.
    std::vector<hpx::supervision::discovered_peer> peers;
    {
        auto const deadline = steady::now() + std::chrono::seconds(join_timeout_s);
        for (;;) {
            peers = hpx::run_as_hpx_thread([&]() {
                return hpx::supervision::discover_and_join(
                    handle, std::chrono::milliseconds(discovery_timeout_ms));
            });
            bool have_a = false, have_b = false;
            for (auto const& p : peers) {
                std::uint32_t const lid = hpx::naming::get_locality_id_from_id(p.locality);
                have_a = have_a || (lid == roles["a"]);
                have_b = have_b || (lid == roles["b"]);
            }
            if ((have_a && have_b) || steady::now() >= deadline) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    hpx::supervision::discovered_peer peer_a, peer_b;
    bool found_a = false, found_b = false;
    for (auto const& p : peers) {
        std::uint32_t const lid = hpx::naming::get_locality_id_from_id(p.locality);
        if (lid == roles["a"]) { peer_a = p; found_a = true; }
        if (lid == roles["b"]) { peer_b = p; found_b = true; }
    }

    // Unconditional diagnostic (not gated on success/failure): what discover_and_join() and
    // roles.json parsing actually produced. Written BEFORE the found_a/found_b early-return so
    // a discovery failure is directly diagnosable from the marker alone, without needing a
    // rebuild+rerun cycle to add logging after the fact.
    {
        std::string peers_json = "[";
        for (std::size_t i = 0; i < peers.size(); ++i) {
            if (i) peers_json += ",";
            peers_json += "{" + jkv("locality_id",
                jnum(hpx::naming::get_locality_id_from_id(peers[i].locality))) + "," +
                jkv("join_epoch", jnum(static_cast<long long>(peers[i].join_epoch))) + "}";
        }
        peers_json += "]";
        write_text(g_bootdir + "/root.discover_diag",
            "{" + jkv("remote_localities_seen_before_discover",
                jnum(static_cast<long long>(remote_localities_seen))) + "," +
            jkv("peers_found_count", jnum(static_cast<long long>(peers.size()))) + "," +
            jkv("peers", peers_json) + "," +
            jkv("expected_a_locality", jnum(roles["a"])) + "," +
            jkv("expected_b_locality", jnum(roles["b"])) + "," +
            jkv("found_a", jbool(found_a)) + "," + jkv("found_b", jbool(found_b)) + "," +
            jkv("discovery_timeout_ms", jnum(discovery_timeout_ms)) + "," +
            jkv("unix", jnum(now_unix())) + "}\n");
    }

    if (!found_a || !found_b) return finish(5, "discover_and_join did not join both a and b");

    // ---- prove eligibility: is_connecting() BEFORE the crash (plan step 3) ----------------
    bool const b_is_connecting_before = hpx::run_as_hpx_thread(
        [&]() { return hpx::agas::is_connecting(peer_b.locality.get_gid()); });
    bool const a_is_connecting_before = hpx::run_as_hpx_thread(
        [&]() { return hpx::agas::is_connecting(peer_a.locality.get_gid()); });

    write_text(g_bootdir + "/root.joined",
        "{" + jkv("a_locality", jnum(roles["a"])) + "," +
        jkv("b_locality", jnum(roles["b"])) + "," +
        jkv("a_join_epoch", jnum(static_cast<long long>(peer_a.join_epoch))) + "," +
        jkv("b_join_epoch", jnum(static_cast<long long>(peer_b.join_epoch))) + "," +
        jkv("a_is_connecting_before_crash", jbool(a_is_connecting_before)) + "," +
        jkv("b_is_connecting_before_crash", jbool(b_is_connecting_before)) + "," +
        jkv("unix", jnum(now_unix())) + "}\n");
    if (!b_is_connecting_before) return finish(6, "b is not is_connecting-eligible before crash");

    // ---- 4. pre-crash useful work (plan steps 5) -------------------------------------------
    PlainProbe const pa = plain_probe(roles["a"], 41, 10);
    PlainProbe const pb = plain_probe(roles["b"], 42, 10);
    write_text(g_bootdir + "/root.work_verified",
        "{" + jkv("a", plain_probe_json(pa)) + "," + jkv("b", plain_probe_json(pb)) + "," +
        jkv("application_failed_event_publish_count",
            jnum(static_cast<long long>(g_app_failed_publish_count.load()))) + "," +
        jkv("unix", jnum(now_unix())) + "}\n");
    if (!pa.ok || !pb.ok) return finish(7, "pre-crash work verification failed");

    // ---- 5/6/7/8/9. wait (bounded) for python to hard-kill b, then observe classification+
    // fencing. No synchronization signal is needed for "b was killed": this loop simply polls
    // check_admission()/query_state() from the moment work is verified until either
    // rejected_fenced appears or fence_wait_timeout_s elapses; python independently kills b at
    // its own pace and the two are correlated post-hoc via wall-clock timestamps.
    std::vector<FenceAttempt> timeline;
    bool fenced = false;
    {
        auto const deadline = steady::now() + std::chrono::seconds(fence_wait_timeout_s);
        while (steady::now() < deadline) {
            FenceAttempt a = fence_probe(peer_b.locality, peer_b.join_epoch);
            bool const this_fenced = (a.dispatch_outcome == "rejected_fenced");
            timeline.push_back(a);
            if (this_fenced) { fenced = true; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }
    bool const runtime_classified_failed =
        !timeline.empty() && timeline.back().last_event == "failed";

    DispatchWorkDiagnostic const dw_diag = fenced
        ? dispatch_work_diagnostic(peer_b.locality, peer_b.join_epoch, 99)
        : DispatchWorkDiagnostic{};

    {
        std::string tl = "[";
        for (std::size_t i = 0; i < timeline.size(); ++i) {
            if (i) tl += ",";
            tl += fence_attempt_json(timeline[i]);
        }
        tl += "]";
        write_text(g_bootdir + "/root.fencing_observed",
            "{" + jkv("stale_incarnation_fenced", jbool(fenced)) + "," +
            jkv("fenced_outcome_is_specific", jbool(fenced)) + "," +
            jkv("runtime_failure_classification_observed", jbool(runtime_classified_failed)) +
            "," + jkv("application_failed_event_publish_count",
                jnum(static_cast<long long>(g_app_failed_publish_count.load()))) + "," +
            jkv("dispatch_work_diagnostic", dispatch_work_diag_json(dw_diag)) + "," +
            jkv("attempts", tl) + "," + jkv("attempts_count", jnum(static_cast<long long>(timeline.size()))) +
            "," + jkv("unix", jnum(now_unix())) + "}\n");
    }
    if (!fenced) return finish(8, "check_admission never observed rejected_fenced within bound");

    // ---- 10. force_disconnect, ONLY now that fencing is observed --------------------------
    long long fd_rc = -2;
    std::string fd_error;
    double const fd_t0 = now_unix();
    {
        hpx::error_code ec(hpx::throwmode::lightweight);
        int const rc = hpx::run_as_hpx_thread([&]() {
            hpx::error_code ec2(hpx::throwmode::lightweight);
            int const r = hpx::force_disconnect(peer_b.locality, ec2);
            ec = ec2;
            return r;
        });
        fd_rc = rc;
        fd_error = ec ? ec.get_message() : std::string();
    }
    double const fd_elapsed_s = now_unix() - fd_t0;
    bool const fd_ok = (fd_rc == 0);
    write_text(g_bootdir + "/root.force_disconnect",
        "{" + jkv("target_locality", jnum(roles["b"])) + "," +
        jkv("target_join_epoch", jnum(static_cast<long long>(peer_b.join_epoch))) + "," +
        jkv("caller_locality", jnum(here)) + "," +
        jkv("force_disconnect_invoked", jbool(true)) + "," +
        jkv("force_disconnect_completed", jbool(fd_ok)) + "," +
        jkv("rc", jnum(fd_rc)) + "," + jkv("error", jstr(fd_error)) + "," +
        jkv("elapsed_s", jnum(fd_elapsed_s)) + "," + jkv("unix", jnum(now_unix())) + "}\n");

    // ---- 11. effect verification ------------------------------------------------------------
    bool const resolve_fails_after = hpx::run_as_hpx_thread([&]() {
        hpx::error_code ec2(hpx::throwmode::lightweight);
        hpx::naming::address const addr =
            hpx::agas::resolve(hpx::launch::sync, peer_b.locality, ec2);
        return static_cast<bool>(ec2) || !addr;
    });
    std::size_t const membership_after_fd = membership_count();
    PlainProbe const post_fd_probe_from_root = plain_probe(roles["b"], 77, 5);
    write_text(g_bootdir + "/root.force_disconnect_effect",
        "{" + jkv("force_disconnect_effect_observed", jbool(fd_ok)) + "," +
        jkv("agas_resolve_fails_after", jbool(resolve_fails_after)) + "," +
        jkv("membership_after", jnum(static_cast<long long>(membership_after_fd))) + "," +
        jkv("membership_shrank", jbool(membership_after_fd == 2)) + "," +
        jkv("post_force_disconnect_dispatch_from_root", plain_probe_json(post_fd_probe_from_root)) +
        "," + jkv("post_force_disconnect_dispatch_failed", jbool(!post_fd_probe_from_root.ok)) +
        "," + jkv("unix", jnum(now_unix())) + "}\n");

    // ---- 12. signal python: replacement connector c may now be launched -------------------
    write_text(g_bootdir + "/root.ready_for_replacement",
        "{" + jkv("ready", jbool(true)) + "," + jkv("unix", jnum(now_unix())) + "}\n");

    // ---- 13/14/15. wait for c, discover_and_join again, dispatch + verify -----------------
    {
        auto const deadline = steady::now() + std::chrono::seconds(replacement_timeout_s);
        while (steady::now() < deadline && membership_count() != 3) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    if (membership_count() != 3) return finish(9, "replacement c never appeared");

    // Same read/write race as the a/b roles read above -- bounded retry, not one-shot.
    {
        auto const deadline = steady::now() + std::chrono::seconds(replacement_timeout_s);
        while (steady::now() < deadline) {
            if (read_roles(g_bootdir + "/native.roles.json", roles) && roles.count("c")) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    if (!roles.count("c")) {
        return finish(10, "native.roles.json missing c after replacement");
    }

    auto const peers2 = hpx::run_as_hpx_thread([&]() {
        return hpx::supervision::discover_and_join(
            handle, std::chrono::milliseconds(discovery_timeout_ms));
    });
    hpx::supervision::discovered_peer peer_c;
    bool found_c = false;
    for (auto const& p : peers2) {
        if (hpx::naming::get_locality_id_from_id(p.locality) == roles["c"]) {
            peer_c = p;
            found_c = true;
        }
    }
    if (!found_c) return finish(11, "discover_and_join did not join replacement c");

    PlainProbe const pc = plain_probe(roles["c"], 43, 10);

    bool const replacement_locality_distinct =
        hpx::naming::get_locality_id_from_id(peer_c.locality) !=
        hpx::naming::get_locality_id_from_id(peer_b.locality);
    bool const replacement_epoch_distinct = peer_c.join_epoch != peer_b.join_epoch;

    // ---- final stale-vs-replacement disambiguation (plan step 15) -------------------------
    FenceAttempt const stale_recheck = fence_probe(peer_b.locality, peer_b.join_epoch);
    bool const stale_still_rejected = (stale_recheck.dispatch_outcome == "rejected_fenced");
    PlainProbe const stale_plain_recheck = plain_probe(roles["b"], 100, 5);

    write_text(g_bootdir + "/root_result.json",
        "{" + jkv("role", jstr("root")) + "," + jkv("rc", jnum(0)) + "," +
        jkv("connector_late_join_proven", jbool(b_is_connecting_before)) + "," +
        jkv("pre_crash_work_ok", jbool(pa.ok && pb.ok)) + "," +
        jkv("hard_crash_used", jbool(true)) + "," +
        jkv("graceful_disconnect_used", jbool(false)) + "," +
        jkv("application_failed_event_publish_count",
            jnum(static_cast<long long>(g_app_failed_publish_count.load()))) + "," +
        jkv("runtime_failure_classification_observed", jbool(runtime_classified_failed)) + "," +
        jkv("failed_epoch_or_incarnation_identified",
            jbool(runtime_classified_failed && timeline.back().event_seq > 0)) + "," +
        jkv("stale_incarnation_fenced", jbool(fenced)) + "," +
        jkv("fenced_outcome_is_specific", jbool(fenced)) + "," +
        jkv("dispatch_work_diagnostic", dispatch_work_diag_json(dw_diag)) + "," +
        jkv("force_disconnect_invoked", jbool(true)) + "," +
        jkv("force_disconnect_completed", jbool(fd_ok)) + "," +
        jkv("force_disconnect_effect_observed",
            jbool(fd_ok && (resolve_fails_after || membership_after_fd == 2 ||
                            !post_fd_probe_from_root.ok))) + "," +
        jkv("replacement_joined", jbool(pc.ok)) + "," +
        jkv("replacement_incarnation_distinct",
            jbool(replacement_locality_distinct && replacement_epoch_distinct)) + "," +
        jkv("replacement_work_ok", jbool(pc.ok)) + "," +
        jkv("stale_incarnation_not_confused_with_replacement",
            jbool(stale_still_rejected && !stale_plain_recheck.ok && pc.ok &&
                  replacement_locality_distinct)) + "," +
        jkv("b_join_epoch", jnum(static_cast<long long>(peer_b.join_epoch))) + "," +
        jkv("c_join_epoch", jnum(static_cast<long long>(peer_c.join_epoch))) + "," +
        jkv("b_locality", jnum(roles["b"])) + "," + jkv("c_locality", jnum(roles["c"])) + "," +
        jkv("unix", jnum(now_unix())) + "}\n");

    return finish(0, "slice3b sequence complete");
}
