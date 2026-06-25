// Cooperative gate / barrier witness spike (experiments/41) -- standalone,
// native-only, EXPERIMENT-ONLY, STRUCTURAL GATES ONLY.
//
// Question: can multiple HPX child tasks make *coordinated cooperative progress*
// through a shared synchronization point? Each child arrives, suspends mid-
// execution on a shared gate, the scheduler runs the others, the last arriver
// opens the gate, all resume and complete. The load-bearing witness is the
// SUCCESS case at --hpx:threads=1: with one OS worker, a non-cooperative (OS-
// level blocking) gate wait would pin the single worker and deadlock before all
// children could arrive. A clean pass at one worker therefore witnesses
// cooperative SUSPENSION + RESUME + scheduler INTERLEAVING -- NOT parallelism.
//
// Explicit non-claims: NO parallelism, NO multi-worker requirement, NO speedup,
// NO throughput / latency / performance, NO endpoint bridge, NO Runtime registry
// op, NO HPX socket serving, NO async socket I/O, NO parcelport / AGAS / multi-
// node. This is an experiment probe only; it is NOT promoted to a Runtime op and
// NOT wired to the endpoint bridge. The subprocess timeout in the experiments/41
// wrapper (NOT this binary) is the real anti-hang guarantee for a true
// cooperative-scheduling mechanism failure; the in-HPX watchdog here only rescues
// LOGIC-failure rounds (some expected participant never arrives).
//
// Primitives (deliberately limited; NO hpx::latch / hpx::barrier /
// hpx::counting_semaphore):
//   * hpx::async                 -- launch child tasks
//   * hpx::when_all              -- join all launched children
//   * hpx::promise<void>         -- the gate (LOCAL promise, as used elsewhere
//                                   in this repo, e.g. runtime_lane.hpp)
//   * hpx::shared_future<void>   -- the shared gate handle (multi-get)
//   * hpx::this_thread::sleep_for-- cooperative coordinator poll wait
//   * std::atomic<int>           -- arrival / release / opener bookkeeping
//   * std::chrono                -- deadlines
// hpx::this_thread::yield and hpx::future::wait_for are PROBED (availability
// reported in the JSON) but the witness does NOT rely on either: children use a
// plain cooperative gate.get(); the coordinator uses sleep_for.
//
// main() runs as an HPX thread (hpx_main wrapper), so the coordinator poll wait
// is cooperative. Output: one compact JSON object on STDOUT (schema
// "rayx-barrier-witness-spike-1"); human progress goes to stderr. The
// experiments/41 wrapper invokes this binary per --hpx:threads value under a hard
// subprocess timeout and curates aggregate.json.

#include <hpx/hpx.hpp>
#include <hpx/hpx_main.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

// Which forced-failure shape a round exercises (or NONE for the success case).
enum class Case { Success, FailSkip, FailThrow, LaunchFewer };

const char* case_name(Case c) {
    switch (c) {
        case Case::Success:     return "success";
        case Case::FailSkip:    return "fail_skip";
        case Case::FailThrow:   return "fail_throw";
        case Case::LaunchFewer: return "launch_fewer";
    }
    return "unknown";
}

// Gate opener attribution. Per the approved refinement, the coordinator opens
// the gate ONLY on the deadline/failure path; on the success path the last
// arriving child is the sole opener. Attribution is therefore deterministic and
// never races at --hpx:threads>=2.
enum Opener { OPENER_NONE = 0, OPENER_LAST = 1, OPENER_WATCHDOG = 2 };

const char* opener_name(int o) {
    switch (o) {
        case OPENER_LAST:     return "last_arriver";
        case OPENER_WATCHDOG: return "watchdog";
        default:              return "none";
    }
}

struct RoundResult {
    Case        kase;
    int         participants_expected = 0;
    int         children_launched = 0;
    int         arrived = 0;
    int         released = 0;
    bool        gate_opened = false;
    int         opener = OPENER_NONE;
    bool        watchdog_opened = false;
    int         joined = 0;
    int         child_errors = 0;
    bool        success = false;       // barrier fully completed via last arriver
    bool        clean_exit = false;    // reached end of round, all children joined
    bool        as_expected = false;   // matched this case's expected signature
};

// Run one independent barrier round inside the live HPX runtime. Fresh promise /
// atomics each call, so no hpx::start/stop churn between rounds.
RoundResult run_round(Case kase, int participants_expected, int children_launched,
                      int deadline_ms, int poll_ms) {
    RoundResult r;
    r.kase = kase;
    r.participants_expected = participants_expected;
    r.children_launched = children_launched;

    hpx::promise<void> gate_prom;
    hpx::shared_future<void> gate = gate_prom.get_future().share();

    std::atomic<int> arrived{0};
    std::atomic<int> released{0};
    std::atomic<bool> opened{false};
    std::atomic<int> opener{OPENER_NONE};

    // Idempotent single-set gate open. CAS guarantees exactly one set_value();
    // the CAS winner records the opener. The success path only ever calls this
    // from the last arriving child; the coordinator calls it only on deadline.
    auto open_gate = [&](int who) {
        bool expect = false;
        if (opened.compare_exchange_strong(expect, true)) {
            opener.store(who);
            gate_prom.set_value();
        }
    };

    // Child body. STRICT ordering: arrive -> (open iff last) -> wait -> release.
    // The forced THROW happens INSIDE this callable so the exception is captured
    // in the child future and surfaced through when_all(...).get(), never on the
    // main thread.
    auto child = [&](int idx) {
        if (kase == Case::FailSkip && idx == 0) {
            return;  // never arrives; never waits; never releases
        }
        if (kase == Case::FailThrow && idx == 0) {
            throw std::runtime_error("forced child failure before arrival");
        }
        const int got = arrived.fetch_add(1) + 1;
        if (got == participants_expected) {
            open_gate(OPENER_LAST);  // last arriver opens on the success path
        }
        gate.get();                  // cooperative suspend until the gate opens
        released.fetch_add(1);
    };

    std::vector<hpx::future<void>> futs;
    futs.reserve(static_cast<std::size_t>(children_launched));
    for (int i = 0; i < children_launched; ++i) {
        futs.push_back(hpx::async(child, i));
    }

    // Cooperative coordinator watchdog. Polls the arrival counter with a
    // cooperative sleep_for (yields the worker so children run, critical at
    // hpx_threads=1). Opens the gate ONLY if the deadline is hit -- i.e. some
    // expected participant never arrived. On the success path the loop exits
    // because arrived>=N and the coordinator does NOT open (the last arriver
    // already did), so the opener is unambiguous.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(deadline_ms);
    bool hit_deadline = false;
    while (arrived.load() < participants_expected) {
        if (std::chrono::steady_clock::now() >= deadline) {
            hit_deadline = true;
            break;
        }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
    }
    if (hit_deadline) {
        open_gate(OPENER_WATCHDOG);  // logic-failure rescue; no orphaned child
    }

    // Join every launched child. A throwing child becomes ready-with-exception;
    // its get() rethrows here and is counted, never left unjoined.
    std::vector<hpx::future<void>> done = hpx::when_all(std::move(futs)).get();
    for (auto& f : done) {
        try {
            f.get();
        } catch (...) {
            ++r.child_errors;
        }
        ++r.joined;
    }

    r.arrived = arrived.load();
    r.released = released.load();
    r.gate_opened = opened.load();
    r.opener = opener.load();
    r.watchdog_opened = (r.opener == OPENER_WATCHDOG);
    r.clean_exit = (r.joined == children_launched);
    r.success = (r.arrived == participants_expected &&
                 r.released == participants_expected &&
                 r.opener == OPENER_LAST && r.child_errors == 0 &&
                 r.clean_exit);

    // Expected structural signature per case (deterministic).
    switch (kase) {
        case Case::Success:
            r.as_expected = r.success;
            break;
        case Case::FailSkip:
            r.as_expected = (r.arrived == participants_expected - 1 &&
                             r.released == participants_expected - 1 &&
                             r.opener == OPENER_WATCHDOG && r.child_errors == 0 &&
                             !r.success && r.clean_exit);
            break;
        case Case::FailThrow:
            r.as_expected = (r.arrived == participants_expected - 1 &&
                             r.released == participants_expected - 1 &&
                             r.opener == OPENER_WATCHDOG && r.child_errors == 1 &&
                             !r.success && r.clean_exit);
            break;
        case Case::LaunchFewer:
            r.as_expected = (r.arrived == children_launched &&
                             r.released == children_launched &&
                             r.children_launched < participants_expected &&
                             r.opener == OPENER_WATCHDOG && r.child_errors == 0 &&
                             !r.success && r.clean_exit);
            break;
    }
    return r;
}

std::string round_json(const RoundResult& r, std::size_t hpx_threads,
                        bool yield_available, bool wait_for_available) {
    char buf[768];
    std::snprintf(
        buf, sizeof buf,
        "{\"hpx_threads\":%zu,\"case\":\"%s\","
        "\"participants_expected\":%d,\"children_launched\":%d,"
        "\"arrived\":%d,\"released\":%d,\"gate_opened\":%s,"
        "\"opener\":\"%s\",\"watchdog_opened\":%s,\"joined\":%d,"
        "\"child_errors\":%d,\"success\":%s,\"clean_exit\":%s,"
        "\"as_expected\":%s,\"yield_available\":%s,\"wait_for_available\":%s}",
        hpx_threads, case_name(r.kase), r.participants_expected,
        r.children_launched, r.arrived, r.released,
        r.gate_opened ? "true" : "false", opener_name(r.opener),
        r.watchdog_opened ? "true" : "false", r.joined, r.child_errors,
        r.success ? "true" : "false", r.clean_exit ? "true" : "false",
        r.as_expected ? "true" : "false",
        yield_available ? "true" : "false",
        wait_for_available ? "true" : "false");
    return std::string(buf);
}

}  // namespace

int main(int argc, char** argv) {
    int participants = 8;
    int deadline_ms = 500;
    int poll_ms = 2;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next_int = [&](const char* flag) {
            if (i + 1 >= argc) {
                std::fprintf(stderr,
                             "[hpx_barrier_witness_spike] %s needs a value\n",
                             flag);
                std::exit(2);
            }
            return std::atoi(argv[++i]);
        };
        if (a == "--participants") participants = next_int("--participants");
        else if (a == "--deadline-ms") deadline_ms = next_int("--deadline-ms");
        else if (a == "--poll-ms") poll_ms = next_int("--poll-ms");
        else {
            std::fprintf(stderr,
                         "[hpx_barrier_witness_spike] unknown flag: %s\n",
                         a.c_str());
            return 2;
        }
    }
    if (participants < 2) {
        std::fprintf(stderr,
                     "[hpx_barrier_witness_spike] --participants must be >= 2 "
                     "(N=1 demonstrates no interleaving)\n");
        return 2;
    }

    // Primitive availability probes. The witness relies on NEITHER (children use
    // gate.get(); coordinator uses sleep_for) -- these only report what compiled
    // and ran, as the experiment plan asks.
    bool yield_available = false;
    hpx::this_thread::yield();
    yield_available = true;  // compiled + executed

    bool wait_for_available = false;
    {
        hpx::promise<void> p;
        hpx::future<void> f = p.get_future();
        p.set_value();
        hpx::future_status st = f.wait_for(std::chrono::milliseconds(0));
        wait_for_available = (st == hpx::future_status::ready);
    }

    const std::size_t hpx_threads = hpx::get_os_thread_count();
    std::fprintf(stderr,
                 "[hpx_barrier_witness_spike] hpx_threads=%zu participants=%d "
                 "deadline_ms=%d poll_ms=%d yield_available=%d "
                 "wait_for_available=%d (structural gates only; no timing "
                 "claim; subprocess timeout owns true-hang safety)\n",
                 hpx_threads, participants, deadline_ms, poll_ms,
                 yield_available ? 1 : 0, wait_for_available ? 1 : 0);

    const int fewer = participants - 2;  // >= 0 since participants >= 2 (== 0
                                         // only at N==2: still < N, watchdog opens)
    struct Plan { Case kase; int launched; };
    const Plan plan[] = {
        {Case::Success,     participants},
        {Case::FailSkip,    participants},
        {Case::FailThrow,   participants},
        {Case::LaunchFewer, fewer},
    };

    std::vector<RoundResult> rounds;
    for (const Plan& p : plan) {
        RoundResult r = run_round(p.kase, participants, p.launched, deadline_ms,
                                  poll_ms);
        std::fprintf(stderr,
                     "[hpx_barrier_witness_spike] case=%-12s arrived=%d/%d "
                     "released=%d opener=%s child_errors=%d success=%d "
                     "as_expected=%d\n",
                     case_name(r.kase), r.arrived, r.participants_expected,
                     r.released, opener_name(r.opener), r.child_errors,
                     r.success ? 1 : 0, r.as_expected ? 1 : 0);
        rounds.push_back(r);
    }

    bool all_as_expected = true;
    for (const RoundResult& r : rounds) {
        if (!r.as_expected) all_as_expected = false;
    }

    std::string out =
        "{\"probe\":\"hpx_barrier_witness_spike\","
        "\"schema\":\"rayx-barrier-witness-spike-1\","
        "\"hpx_threads\":" + std::to_string(hpx_threads) +
        ",\"participants_expected\":" + std::to_string(participants) +
        ",\"deadline_ms\":" + std::to_string(deadline_ms) +
        ",\"poll_ms\":" + std::to_string(poll_ms) +
        ",\"yield_available\":" + (yield_available ? "true" : "false") +
        ",\"wait_for_available\":" + (wait_for_available ? "true" : "false") +
        ",\"all_as_expected\":" + (all_as_expected ? "true" : "false") +
        ",\"rounds\":[";
    for (std::size_t i = 0; i < rounds.size(); ++i) {
        if (i) out += ",";
        out += round_json(rounds[i], hpx_threads, yield_available,
                          wait_for_available);
    }
    out += "]}";
    std::printf("%s\n", out.c_str());

    std::fprintf(stderr, "[hpx_barrier_witness_spike] all_as_expected=%s\n",
                 all_as_expected ? "PASS" : "FAIL");
    return all_as_expected ? 0 : 1;
}
