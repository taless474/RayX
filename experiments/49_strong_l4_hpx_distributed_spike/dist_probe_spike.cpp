// exp49 -- Ray-free strong-L4 HPX distributed-runtime spike (standalone binary).
//
// EXPERIMENTAL, NARROW, MECHANISM-FEASIBILITY ONLY. This is NOT RayX production code,
// NOT linked into the `_rayx` Python extension, and NOT a Ray demo. It tests, before Ray
// enters the picture, whether two plain processes on ONE node can form one HPX distributed
// runtime over loopback TCP and run one registered HPX action from locality 0 to locality 1.
//
// Two phases share THIS one binary (so the action is registered identically in every
// locality) and are selected by --role:
//   * p1_root / p1_worker : Phase 1, COORDINATED launch (fixed --hpx:localities=2).
//   * p2_root / p2_connect: Phase 2, CONNECT-MODE late join (runtime_mode::connect).
//
// CLAIM FENCE (see strong_l4_hpx_distributed_spike.md): strong-L4 mechanism feasibility
// only; Ray-free; single-node; loopback TCP only; no Ray actor/bootstrap claim; no
// performance/throughput/latency; no multi-node; no general fabric; no fault tolerance;
// no production/public API; no Ray replacement; no "HPX faster than Ray".
//
// The action returns (x ^ 0x52415958) + (get_locality_id() << 1): the EXECUTING locality id
// is folded into the result so the caller can structurally prove the body ran on the remote
// locality (id 1), not locally (id 0) -- the peer-specific-value idea reused from the
// endpoint ping. Closed int64 in/out only; no arbitrary payloads, no callbacks, no store.

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
#include <hpx/runtime_local/runtime_local_fwd.hpp>  // hpx::is_running
#include <hpx/naming_base/id_type.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>  // atoi
#include <fstream>
#include <string>
#include <thread>   // std::this_thread on the connector's (non-HPX) main thread
#include <vector>

#include <unistd.h>  // getpid

namespace {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

void write_text(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
}

std::string bquote(bool b) { return b ? "true" : "false"; }

}  // namespace

// ===== the one fixed registered action ====================================
// Global scope; registered identically in every locality because both processes run THIS
// binary. Closed int64 -> int64. Folds in the executing locality id as remote-proof.
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(dist_probe, dist_probe_action)

// ===== root logic (shared by p1_root and p2_root) =========================
// Bounded-poll find_all_localities() until two localities are visible, pick the non-local
// one, invoke the action on it, check the oracle, and write a per-phase result JSON.
int run_root(const std::string& role, std::int64_t x, const std::string& bootdir,
             int ready_timeout, std::uint32_t here, long pid) {
    if (role == "p2_root") {
        // Publish readiness so the orchestrator can launch the late joiner AFTER the
        // AGAS root is up (connect mode is sequenced, not simultaneous).
        write_text(bootdir + "/bootstrap.json",
                   std::string("{\"agas_role\":\"root\",\"locality_id\":") +
                       std::to_string(here) + ",\"pid\":" + std::to_string(pid) + "}\n");
        write_text(bootdir + "/root.ready", "ready\n");
    }

    std::vector<hpx::id_type> locs;
    bool reached_two = false;
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(ready_timeout);
    while (std::chrono::steady_clock::now() < deadline) {
        locs = hpx::find_all_localities();
        if (locs.size() >= 2) {
            reached_two = true;
            break;
        }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::int64_t result = 0, oracle = 0;
    std::uint32_t remote_id = 0, executed_on = 0;
    bool invoked = false, match = false, proved_remote = false;

    if (reached_two) {
        hpx::id_type here_id = hpx::find_here();
        hpx::id_type remote;
        bool have_remote = false;
        for (auto const& l : locs) {
            if (l != here_id) {
                remote = l;
                have_remote = true;
                break;
            }
        }
        if (have_remote) {
            remote_id = hpx::naming::get_locality_id_from_id(remote);
            try {
                result = hpx::async<dist_probe_action>(remote, x).get();
                invoked = true;
                oracle = (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(remote_id) << 1);
                match = (result == oracle);
                executed_on =
                    static_cast<std::uint32_t>((result - (x ^ DIST_PROBE_XOR)) >> 1);
                proved_remote =
                    invoked && match && (executed_on == remote_id) && (executed_on != here);
            } catch (...) {
                invoked = false;
            }
        }
    }

    const std::string phase = (role == "p1_root") ? "phase1" : "phase2";
    std::string j = "{";
    j += "\"role\":\"" + role + "\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += "\"localities_seen\":" + std::to_string(locs.size()) + ",";
    j += "\"reached_two\":" + bquote(reached_two) + ",";
    j += "\"remote_locality\":" + std::to_string(remote_id) + ",";
    j += "\"x\":" + std::to_string(x) + ",";
    j += "\"invoked\":" + bquote(invoked) + ",";
    j += "\"result\":" + std::to_string(result) + ",";
    j += "\"oracle\":" + std::to_string(oracle) + ",";
    j += "\"match\":" + bquote(match) + ",";
    j += "\"executed_on_locality\":" + std::to_string(executed_on) + ",";
    j += "\"proved_remote\":" + bquote(proved_remote);
    j += "}\n";
    write_text(bootdir + "/" + phase + "_result.json", j);

    return hpx::finalize();  // collective shutdown (Phase 1) / root teardown (Phase 2)
}

// ===== Phase 2b: connect-mode independent-disconnect lifecycle =============
// The connector self-disconnects (hpx::disconnect) instead of being torn down collectively,
// and the root must REMAIN FUNCTIONAL afterward -- proven by serving a SECOND connector.

struct DispatchOutcome {
    bool reached_two = false, invoked = false, match = false, proved_remote = false;
    std::uint32_t remote_id = 0, executed_on = 0;
    std::int64_t result = 0, oracle = 0;
    std::size_t localities_seen = 0;
};

// Bounded-poll until two localities are visible, pick the non-self one, invoke dist_probe,
// and check the oracle (same logic the p1/p2 root uses, factored for reuse across connectors).
DispatchOutcome wait_and_dispatch(std::int64_t x, std::uint32_t here, int timeout_s) {
    DispatchOutcome o;
    std::vector<hpx::id_type> locs;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        locs = hpx::find_all_localities();
        if (locs.size() >= 2) { o.reached_two = true; break; }
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    o.localities_seen = locs.size();
    if (o.reached_two) {
        hpx::id_type here_id = hpx::find_here();
        hpx::id_type remote;
        bool have_remote = false;
        for (auto const& l : locs) {
            if (l != here_id) { remote = l; have_remote = true; break; }
        }
        if (have_remote) {
            o.remote_id = hpx::naming::get_locality_id_from_id(remote);
            try {
                o.result = hpx::async<dist_probe_action>(remote, x).get();
                o.invoked = true;
                o.oracle = (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(o.remote_id) << 1);
                o.match = (o.result == o.oracle);
                o.executed_on =
                    static_cast<std::uint32_t>((o.result - (x ^ DIST_PROBE_XOR)) >> 1);
                o.proved_remote =
                    o.invoked && o.match && (o.executed_on == o.remote_id) &&
                    (o.executed_on != here);
            } catch (...) {
                o.invoked = false;
            }
        }
    }
    return o;
}

// Bounded-poll until the locality count drops back to <= 1 (the peer disconnected).
bool wait_drop_to_one(int timeout_s) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        if (hpx::find_all_localities().size() <= 1) return true;
        hpx::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

std::string conn_json(const std::string& key, const DispatchOutcome& o) {
    std::string j = "\"" + key + "\":{";
    j += "\"reached_two\":" + bquote(o.reached_two) + ",";
    j += "\"localities_seen\":" + std::to_string(o.localities_seen) + ",";
    j += "\"remote_locality\":" + std::to_string(o.remote_id) + ",";
    j += "\"result\":" + std::to_string(o.result) + ",";
    j += "\"oracle\":" + std::to_string(o.oracle) + ",";
    j += "\"invoked\":" + bquote(o.invoked) + ",";
    j += "\"match\":" + bquote(o.match) + ",";
    j += "\"executed_on_locality\":" + std::to_string(o.executed_on) + ",";
    j += "\"proved_remote\":" + bquote(o.proved_remote);
    j += "}";
    return j;
}

// p2b_root: console that admits TWO sequential connectors, serves each one remote action,
// observes each self-disconnect, then finalizes. Started via hpx::init (hpx_main).
int run_p2b_root(std::int64_t x, const std::string& bootdir, int per_step_timeout,
                 std::uint32_t here, long pid) {
    write_text(bootdir + "/root.ready", "ready\n");

    // Connector #1: wait, dispatch, release (served1.ok), then watch it leave.
    DispatchOutcome o1 = wait_and_dispatch(x, here, per_step_timeout);
    if (o1.reached_two) write_text(bootdir + "/served1.ok", "served\n");
    bool observed_after_1 = o1.reached_two ? wait_drop_to_one(per_step_timeout) : false;

    // Connector #2: the load-bearing re-admit. If the root were left non-functional by the
    // first disconnect, this second join/serve would fail.
    DispatchOutcome o2 = wait_and_dispatch(x, here, per_step_timeout);
    if (o2.reached_two) write_text(bootdir + "/served2.ok", "served\n");
    bool observed_after_2 = o2.reached_two ? wait_drop_to_one(per_step_timeout) : false;

    bool root_served_second = o2.invoked && o2.match && o2.proved_remote;

    std::string j = "{";
    j += "\"role\":\"p2b_root\",";
    j += "\"here_locality\":" + std::to_string(here) + ",";
    j += "\"pid\":" + std::to_string(pid) + ",";
    j += conn_json("connector1", o1) + ",";
    j += "\"root_observed_after_connector1\":" + bquote(observed_after_1) + ",";
    j += conn_json("connector2", o2) + ",";
    j += "\"root_observed_after_connector2\":" + bquote(observed_after_2) + ",";
    j += "\"root_served_second_connector\":" + bquote(root_served_second);
    j += "}\n";
    write_text(bootdir + "/phase2b_root_result.json", j);

    return hpx::finalize();  // clean root teardown after both connectors have left
}

// p2b_connect: a connect-mode locality started NON-BLOCKING via hpx::start, driving its own
// lifecycle from its main thread -- join, wait to be served, then hpx::disconnect() itself.
// Deliberately uses hpx::start (not --hpx:run-hpx-main) so the connector controls its leave.
int run_connector_start(int argc, char** argv,
                        const hpx::program_options::options_description& desc) {
    // Manual argv scan: with hpx::start(nullptr) there is no hpx_main / variables_map to read
    // our app options from. (desc is still passed to HPX below so it accepts these options.)
    std::string bootdir = ".";
    int index = 1;
    int serve_timeout = 20;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--bootstrap" && i + 1 < argc) bootdir = argv[++i];
        else if (a.rfind("--bootstrap=", 0) == 0) bootdir = a.substr(12);
        else if (a == "--connector-index" && i + 1 < argc) index = std::atoi(argv[++i]);
        else if (a.rfind("--connector-index=", 0) == 0) index = std::atoi(a.substr(18).c_str());
        else if (a == "--serve-timeout" && i + 1 < argc) serve_timeout = std::atoi(argv[++i]);
        else if (a.rfind("--serve-timeout=", 0) == 0) serve_timeout = std::atoi(a.substr(16).c_str());
    }
    const std::string idx = std::to_string(index);

    hpx::init_params params;
    params.desc_cmdline = desc;  // so HPX accepts --bootstrap/--connector-index/--serve-timeout
    params.mode = hpx::runtime_mode::connect;
    if (!hpx::start(nullptr, argc, argv, params)) {
        write_text(bootdir + "/connect.joined" + idx, "{\"started\":false}\n");
        return 2;
    }

    const long pid = static_cast<long>(::getpid());
    // get_locality_id() via an HPX thread (called from the connector's main thread).
    std::uint32_t hereloc = hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
    write_text(bootdir + "/connect.joined" + idx,
               "{\"index\":" + idx + ",\"pid\":" + std::to_string(pid) +
                   ",\"locality_id\":" + std::to_string(hereloc) + "}\n");

    // Wait (bounded) for the root's per-connector served signal before leaving. Main thread
    // is NOT an HPX thread here, so use std::this_thread.
    const std::string served_path = bootdir + "/served" + idx + ".ok";
    bool served = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(serve_timeout);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream f(served_path);
        if (f.good()) { served = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Graceful leave. hpx::disconnect() is documented as the TERMINAL call for a disconnecting
    // locality ("blocks and waits for this locality to finish executing ... should be the last
    // HPX-function called"). So we call it ALONE -- no trailing is_running()/stop(), which
    // would run after the runtime has begun tearing down and is what produced an unclean leave
    // (and a broken-pipe on the root) in the first attempt. Empirical teardown = "disconnect".
    // Teardown for a hpx::start(nullptr) connect-mode locality, mirroring the production
    // finalize+stop pattern (hpx::post([]{hpx::finalize();}); hpx::stop();) but substituting
    // disconnect: POST hpx::disconnect() onto an HPX thread (it "can be called from an HPX
    // thread only", and run_as_hpx_thread would never return once it tears down its own
    // runtime), then hpx::stop() from THIS main thread to join the runtime shutdown and exit
    // cleanly. The root observes the locality drop out cleanly (no broken pipe).
    int rc = 0;
    bool clean = true;
    std::string teardown = "post(disconnect)+stop";
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
                   ",\"teardown\":\"" + teardown + "\",\"error\":\"" + err + "\"}\n");
    return clean ? 0 : 1;
}

int hpx_main(hpx::program_options::variables_map& vm) {
    const std::string role = vm["role"].as<std::string>();
    const std::int64_t x = vm["x"].as<std::int64_t>();
    const std::string bootdir = vm["bootstrap"].as<std::string>();
    const int ready_timeout = vm["ready-timeout"].as<int>();
    const int idle_s = vm["idle"].as<int>();

    const std::uint32_t here = hpx::get_locality_id();
    const long pid = static_cast<long>(::getpid());

    // First-class finding: did hpx_main run on THIS locality/role? The orchestrator reads
    // these markers to report whether hpx_main runs only on the console in coordinated mode.
    write_text(bootdir + "/hpxmain_" + role + "_loc" + std::to_string(here) + ".entered",
               "pid=" + std::to_string(pid) + "\n");

    if (role == "p1_worker") {
        // Coordinated mode: hpx_main is expected to run ONLY on the console (locality 0).
        // If it runs here it's unexpected; do NOT finalize (that would tear down the
        // collective out from under the console). The marker above records that it ran.
        return 0;  // deliberately not hpx::finalize()
    }

    if (role == "p1_root" || role == "p2_root") {
        return run_root(role, x, bootdir, ready_timeout, here, pid);
    }

    if (role == "p2b_root") {
        return run_p2b_root(x, bootdir, ready_timeout, here, pid);
    }

    if (role == "p2_connect") {
        // Connect-mode late joiner: record identity, idle so the root can dispatch the
        // action, then DISCONNECT (leave the running app) -- not finalize.
        write_text(bootdir + "/connect.joined",
                   std::string("{\"locality_id\":") + std::to_string(here) +
                       ",\"pid\":" + std::to_string(pid) + "}\n");
        hpx::this_thread::sleep_for(std::chrono::seconds(idle_s));
        int rc = 0;
        bool clean = true;
        try {
            rc = hpx::disconnect();
        } catch (...) {
            clean = false;
        }
        write_text(bootdir + "/connect.disconnected",
                   std::string("{\"clean\":") + bquote(clean) +
                       ",\"rc\":" + std::to_string(rc) + "}\n");
        return rc;
    }

    return hpx::finalize();  // unknown role: don't hang
}

int main(int argc, char* argv[]) {
    namespace po = hpx::program_options;
    po::options_description desc("exp49 strong-L4 distributed spike options");
    // clang-format off
    desc.add_options()
        ("role", po::value<std::string>()->default_value("p1_root"),
            "p1_root | p1_worker | p2_root | p2_connect | p2b_root | p2b_connect")
        ("x", po::value<std::int64_t>()->default_value(7),
            "closed int64 action input")
        ("bootstrap", po::value<std::string>()->default_value("."),
            "directory for rendezvous / result files")
        ("ready-timeout", po::value<int>()->default_value(20),
            "seconds to wait for the second locality (root steps)")
        ("idle", po::value<int>()->default_value(5),
            "p2_connect idle seconds before disconnect")
        ("connector-index", po::value<int>()->default_value(1),
            "p2b_connect index (1 or 2)")
        ("serve-timeout", po::value<int>()->default_value(20),
            "p2b_connect seconds to wait for its served signal");
    // clang-format on

    // The role must be known BEFORE hpx::init so connect mode can be selected on the
    // connecting process. HPX parses --hpx:* itself; --role is our app option.
    std::string role = "p1_root";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--role" && i + 1 < argc) {
            role = argv[i + 1];
        } else if (a.rfind("--role=", 0) == 0) {
            role = a.substr(7);
        }
    }

    // p2b_connect drives its OWN lifecycle via the non-blocking hpx::start path (so it can
    // self-disconnect from its main thread), so it does NOT go through hpx::init / hpx_main.
    if (role == "p2b_connect") {
        return run_connector_start(argc, argv, desc) == 0 ? 0 : 1;
    }

    hpx::init_params params;
    params.desc_cmdline = desc;
    // Make the runtime mode explicit per role rather than relying on address auto-detect:
    //   p1_root  -> console (AGAS root / locality 0)
    //   p1_worker-> worker  (coordinated worker that registers with the root)
    //   p2_connect-> connect (late dynamic join, passive)
    //   p2_root / p2b_root -> default_ (console; with --hpx:expect-connecting-localities)
    if (role == "p2_connect") {
        params.mode = hpx::runtime_mode::connect;
    } else if (role == "p1_worker") {
        params.mode = hpx::runtime_mode::worker;
    } else if (role == "p1_root") {
        params.mode = hpx::runtime_mode::console;
    }
    return hpx::init(argc, argv, params);
}
