// exp68 -- EXPERIMENT-ONLY pybind11 extension hosted INSIDE a Ray actor worker process.
//
// LLM-SHAPED SYNTHETIC WORKLOAD (not real inference). Two Ray actors A and B each host a
// connect-mode HPX locality IN-PROCESS (hpx::start, NO child) -- the exp67 mechanism, reused
// unchanged. Each owns a disjoint vocabulary shard. The load-bearing exp68 step: a coordinator
// computes its own shard's local top-k, dispatches exp68_local_topk_action at the PEER locality
// over HPX, and merges the two candidate lists through an HPX FUTURE CONTINUATION (.then) inside
// the HPX runtime. The global top-k + peer identity witnesses are returned to the controller,
// which checks them BIT-EXACTLY against an independent global oracle. Ray never carries the peer
// shard or the peer's local top-k (the actors hold no Ray handle to each other).
//
// Identity discipline: hpx_version_info() is callable BEFORE start so the controller asserts the
// verified waiter-fix build identity before any measurement. NOT production, NOT rayx.runtime API,
// no performance/ratio/winner claim, no model weights/tokenizer/GPU.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/version.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "topk_action.hpp"  // ONE TU per binary: registers exp68 topk/pid actions + reply type

namespace py = pybind11;

namespace {

std::atomic<bool> g_started{false};

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

std::string this_host() {
    char buf[256] = {0};
    if (::gethostname(buf, sizeof(buf) - 1) == 0) return std::string(buf);
    return "unknown";
}

std::map<std::string, std::string> ext_hpx_version_info() {
    std::map<std::string, std::string> out;
    out["hpx_version_full"] = hpx::full_version_as_string();
    out["hpx_complete_version"] = hpx::complete_version();
    return out;
}

void ext_start_connect(int hpx_threads, const std::vector<std::string>& extra_args) {
    if (g_started.load()) {
        throw std::runtime_error("HPX already started in this process (one runtime per process)");
    }
    std::vector<std::vector<char>> argv_store;
    argv_store.push_back(to_cstr("exp68_actor_ext"));
    argv_store.push_back(to_cstr("--hpx:threads=" + std::to_string(hpx_threads)));
    for (const std::string& a : extra_args) argv_store.push_back(to_cstr(a));
    std::vector<char*> argv;
    argv.reserve(argv_store.size() + 1);
    for (auto& v : argv_store) argv.push_back(v.data());
    argv.push_back(nullptr);
    int argc = static_cast<int>(argv_store.size());
    hpx::init_params params;
    params.mode = hpx::runtime_mode::connect;
    py::gil_scoped_release release;
    if (!hpx::start(nullptr, argc, argv.data(), params)) {
        throw std::runtime_error("hpx::start (connect) failed");
    }
    g_started.store(true);
}

int ext_stop_disconnect() {
    if (!g_started.load()) return 0;
    int rc = 0;
    {
        py::gil_scoped_release release;
        hpx::post([]() { hpx::disconnect(); });
        rc = hpx::stop();
    }
    g_started.store(false);
    return rc;
}

std::int64_t ext_pid() { return static_cast<std::int64_t>(::getpid()); }
std::string ext_hostname() { return this_host(); }

std::uint32_t ext_locality_id() {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([]() { return hpx::get_locality_id(); });
}

std::size_t ext_membership_count() {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    py::gil_scoped_release release;
    return hpx::run_as_hpx_thread([]() { return hpx::find_all_localities().size(); });
}

// Expose the deterministic rule so the controller can cross-check C++ vs Python float32 bits.
std::uint32_t ext_logit_bits(std::int64_t token_id, std::int64_t seed) {
    return exp68::logit_bits(token_id, seed);
}

py::list cands_to_py(const std::vector<exp68::Cand>& v) {
    py::list out;
    for (auto const& c : v) {
        py::dict d;
        d["token_id"] = c.first;
        d["logit_bits"] = c.second;
        d["logit"] = exp68::bits_f32(c.second);
        out.append(d);
    }
    return out;
}

// Own shard's local top-k, computed in-process (pure; no HPX peer). Slice B input.
py::list ext_local_topk(std::int64_t lo, std::int64_t hi, std::int64_t seed, int k) {
    std::vector<exp68::Cand> v;
    {
        py::gil_scoped_release release;
        v = exp68::local_topk(lo, hi, seed, k);
    }
    return cands_to_py(v);
}

struct Flags {
    std::atomic<bool> dispatched{false}, future_ready{false},
        continuation_executed{false}, result_delivered{false};
};

// The load-bearing exp68 op: coordinate a distributed top-k with the PEER locality over HPX and
// merge through a future CONTINUATION. Returns the global top-k, the coordinator's own local top-k,
// the peer's local top-k, identity witnesses for both, and HPX-composition evidence flags.
py::dict ext_coordinate(std::uint32_t peer_loc, std::int64_t own_lo, std::int64_t own_hi,
                        std::int64_t peer_lo, std::int64_t peer_hi, std::int64_t seed, int k,
                        int bound_s) {
    if (!g_started.load()) throw std::runtime_error("HPX not started");
    struct R {
        bool found = false, ready = false;
        std::vector<exp68::Cand> global, own_topk, peer_topk;
        std::int64_t peer_pid = -1;
        std::uint32_t peer_loc = 0xFFFFFFFFu;
        std::string peer_host;
        std::int64_t own_pid = -1;
        std::uint32_t own_loc = 0xFFFFFFFFu;
        std::string own_host;
        bool f_dispatched = false, f_future_ready = false,
             f_continuation = false, f_delivered = false;
    } r;
    {
        py::gil_scoped_release release;
        r = hpx::run_as_hpx_thread([&]() -> R {
            R q;
            q.own_pid = static_cast<std::int64_t>(::getpid());
            q.own_loc = hpx::get_locality_id();
            q.own_host = this_host();
            q.own_topk = exp68::local_topk(own_lo, own_hi, seed, k);

            hpx::id_type target = hpx::invalid_id;
            for (auto const& id : hpx::find_all_localities()) {
                if (hpx::naming::get_locality_id_from_id(id) == peer_loc) target = id;
            }
            if (target == hpx::invalid_id) return q;
            q.found = true;

            auto flags = std::make_shared<Flags>();
            auto own_copy = q.own_topk;
            // Dispatch the peer's local top-k over HPX, then MERGE inside a .then continuation that
            // runs on an HPX worker thread when the fetch future becomes ready.
            auto f = hpx::async<exp68_local_topk_action>(target, peer_lo, peer_hi, seed,
                                                         static_cast<std::int64_t>(k));
            flags->dispatched.store(f.valid());
            auto merged = f.then(
                [own_copy, k, flags](hpx::future<exp68_topk_reply> ff) -> std::tuple<
                    std::vector<exp68::Cand>, exp68_topk_reply> {
                    flags->future_ready.store(ff.is_ready());
                    exp68_topk_reply reply = ff.get();
                    flags->continuation_executed.store(true);
                    auto global = exp68::merge_topk(own_copy, reply.cands, k);
                    return std::make_tuple(std::move(global), std::move(reply));
                });
            auto status = merged.wait_for(std::chrono::seconds(bound_s));
            if (status != hpx::future_status::ready) return q;  // bounded; never hang
            auto out = merged.get();
            flags->result_delivered.store(true);
            q.ready = true;
            q.global = std::get<0>(out);
            exp68_topk_reply const& reply = std::get<1>(out);
            q.peer_topk = reply.cands;
            q.peer_pid = reply.pid;
            q.peer_loc = reply.locality;
            q.peer_host = reply.host;
            q.f_dispatched = flags->dispatched.load();
            q.f_future_ready = flags->future_ready.load();
            q.f_continuation = flags->continuation_executed.load();
            q.f_delivered = flags->result_delivered.load();
            return q;
        });
    }
    py::dict d;
    d["target_found"] = r.found;
    d["ready"] = r.ready;
    d["global_topk"] = cands_to_py(r.global);
    d["own_topk"] = cands_to_py(r.own_topk);
    d["peer_topk"] = cands_to_py(r.peer_topk);
    d["peer_pid"] = r.peer_pid;
    d["peer_locality"] = r.peer_loc;
    d["peer_host"] = r.peer_host;
    d["own_pid"] = r.own_pid;
    d["own_locality"] = r.own_loc;
    d["own_host"] = r.own_host;
    py::dict comp;
    comp["action_dispatched"] = r.f_dispatched;
    comp["future_ready"] = r.f_future_ready;
    comp["continuation_executed"] = r.f_continuation;
    comp["result_delivered"] = r.f_delivered;
    d["hpx_composition"] = comp;
    return d;
}

}  // namespace

PYBIND11_MODULE(exp68_actor_ext, m) {
    m.doc() = "exp68 EXPERIMENT-ONLY in-Ray-actor connect-mode HPX host + sharded top-k "
              "(LLM-shaped synthetic, not inference; not rayx.runtime API)";
    m.def("hpx_version_info", &ext_hpx_version_info,
          "Runtime-observed HPX build identity (callable BEFORE start; identity gate input).");
    m.def("start_connect", &ext_start_connect, py::arg("hpx_threads"), py::arg("extra_args"));
    m.def("stop_disconnect", &ext_stop_disconnect, "Graceful leave: post(disconnect) + stop.");
    m.def("pid", &ext_pid);
    m.def("hostname", &ext_hostname);
    m.def("locality_id", &ext_locality_id);
    m.def("membership_count", &ext_membership_count);
    m.def("logit_bits", &ext_logit_bits, py::arg("token_id"), py::arg("seed"),
          "Deterministic float32 logit bit-pattern for (token_id, seed) -- cross-language check.");
    m.def("local_topk", &ext_local_topk, py::arg("lo"), py::arg("hi"), py::arg("seed"), py::arg("k"),
          "Own-shard local top-k over [lo,hi): list of {token_id, logit_bits, logit}.");
    m.def("coordinate", &ext_coordinate, py::arg("peer_loc"), py::arg("own_lo"), py::arg("own_hi"),
          py::arg("peer_lo"), py::arg("peer_hi"), py::arg("seed"), py::arg("k"),
          py::arg("bound_s") = 10,
          "Fetch the peer shard's local top-k over HPX and merge via a future continuation; "
          "returns global top-k + witnesses + composition evidence.");
    m.attr("__experiment__") = "exp68";
    m.attr("__gil_declaration__") = "gil_used_default";
}
