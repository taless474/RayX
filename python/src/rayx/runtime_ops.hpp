// rayx Phase 1 runtime: fixed native operation registry + runtime result type.
//
// This header is part of the EXPERIMENTAL `rayx.runtime` prototype (Phase 1,
// Slice 0). It is deliberately SEPARATE from the shipped harness lane core
// (hpx_impl/service_lane.hpp): it does NOT touch rayhpx::Request / rayhpx::Result
// / ServiceLane / HpxLane or the native baseline, and it adds NO field to the
// frozen v1 benchmark row. It defines:
//
//   * a fixed, C++-side operation registry (square / add / boom) -- NO Python
//     callables, NO arbitrary execution: a string op id resolves to a
//     pre-compiled native functor over a closed arg type set (int only in
//     Phase 1);
//   * RuntimeResult, the runtime's OWN result struct carrying the core
//     measurement-row fields PLUS the operation value (kept separate from the
//     row at the Python boundary);
//   * the `rt-hpx-` actor_id generator (distinct from the harness `act-hpx-` /
//     `act-hpxl-` prefixes).
//
// Slice 0 has NO cancellation, so OpFn takes no cancel token (that arrives in
// Slice 2). Dispatch (hpx::async on an executor) lives in _rayx.cpp.

#ifndef RAYX_RUNTIME_OPS_HPP
#define RAYX_RUNTIME_OPS_HPP

#include <algorithm>
#include <climits>
#include <cstdint>
#include <functional>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace rayx_runtime {

// Phase 1 closed value type: int only (int64). float / bytes are a later (P6)
// extension, not Phase-1 surface.
using OpValue = std::int64_t;

// One operation outcome: a value (when has_value) plus a row-status/error. A
// native operation that throws is mapped (in _rayx.cpp) to status="failed" with
// has_value=false; a checkpointed op (busy_sum) that honors a cooperative running
// cancel returns status="cancelled", has_value=false.
struct OpOutcome {
    OpValue value = 0;
    bool has_value = false;
    std::string status = "completed";  // "completed" | "failed" | "cancelled"
    std::string error;                 // empty == null
};

// Cooperative-cancellation checkpoint predicate (Slice 2a). The LANE binds this to
// the cancel token (and performs the cooperative hpx::this_thread::yield()); a
// checkpointed op (busy_sum) calls it at each chunk boundary with next_is_final
// true for the last chunk. Returns true if the op must STOP NOW (running cancel).
// Non-checkpointed ops (square/add/boom) ignore it. Kept as a plain std::function
// so this header stays HPX-free (no HPX header, no token type).
using StopCheckpoint = std::function<bool(bool next_is_final)>;

// busy_sum checkpoint stride + value modulus. STRIDE is SHARED: it drives both the
// engine/lane checkpoint COUNT (used to arm running-cancellability) and the
// busy_sum loop's chunk size, so the two always agree. MASK = 2^31 - 1.
inline constexpr std::int64_t BUSY_SUM_STRIDE = 8192;
inline constexpr std::uint64_t BUSY_SUM_MASK = 0x7FFFFFFFULL;

// Checkpoint count for a busy_sum over `n` steps: ceil(n / STRIDE), at least 1.
// A count of 1 means "not running-cancellable" (no chunk boundary): begin_service
// arms cancellable_ = (count > 1). Clamped to INT_MAX defensively for huge n.
inline int busy_sum_checkpoints(std::int64_t n) {
    if (n <= BUSY_SUM_STRIDE) return 1;
    const std::int64_t c = (n + BUSY_SUM_STRIDE - 1) / BUSY_SUM_STRIDE;
    return c > static_cast<std::int64_t>(INT_MAX) ? INT_MAX : static_cast<int>(c);
}

// Slice 2a operation signature: validated int args + a lane-bound StopCheckpoint
// in, OpOutcome out. Args are arity/type-validated at the Python boundary; a
// defensive arity re-check happens in _rayx.cpp. Non-checkpointed ops ignore stop.
using OpFn = std::function<OpOutcome(const std::vector<std::int64_t>& args,
                                     const StopCheckpoint& stop)>;

struct OpEntry {
    int arity;
    OpFn fn;
    // Number of cancellation checkpoints this call will reach, from its args. The
    // engine passes it to RuntimeCancelToken::begin_service so running-cancel is
    // armed only when the op actually has a boundary to stop at (> 1). Instantaneous
    // ops are always 1.
    std::function<int(const std::vector<std::int64_t>& args)> checkpoint_count;
};

// Every instantaneous op reaches exactly one (notional) checkpoint -> count 1.
inline int one_checkpoint(const std::vector<std::int64_t>&) { return 1; }

// The fixed registry, built once. `square` / `add` are real native operations;
// `boom` is a tiny throwing operation that exercises the failure-path mapping
// (status="failed"); `busy_sum` is the checkpointed iterative op that exercises
// queued + cooperative running cancellation (Slice 2a).
inline const std::unordered_map<std::string, OpEntry>& registry() {
    static const std::unordered_map<std::string, OpEntry> r = {
        {"square", OpEntry{1,
             [](const std::vector<std::int64_t>& a, const StopCheckpoint&) {
                 OpOutcome o;
                 o.value = a[0] * a[0];
                 o.has_value = true;
                 return o;
             },
             one_checkpoint}},
        {"add", OpEntry{2,
             [](const std::vector<std::int64_t>& a, const StopCheckpoint&) {
                 OpOutcome o;
                 o.value = a[0] + a[1];
                 o.has_value = true;
                 return o;
             },
             one_checkpoint}},
        {"boom", OpEntry{0,
             [](const std::vector<std::int64_t>&, const StopCheckpoint&)
                 -> OpOutcome {
                 throw std::runtime_error(
                     "boom: intentional failure for failure-path testing");
             },
             one_checkpoint}},
        {"busy_sum", OpEntry{1,
             // Real native iterative work: acc = (Σ_{i=0}^{n-1} i) mod 2^31, with
             // per-step masking so acc stays < 2^31 (overflow-safe, deterministic;
             // equals (n*(n-1)/2) mod 2^31). The loop mirrors the harness chunk
             // loop: for each STRIDE-sized chunk, poll stop(next_is_final) BEFORE
             // the chunk's work; the final boundary clears running-cancellability
             // before the last segment. NO sleep -- this is on-core work. n >= 0 is
             // guaranteed by the Python boundary.
             [](const std::vector<std::int64_t>& a, const StopCheckpoint& stop)
                 -> OpOutcome {
                 const std::int64_t n = a[0];
                 const int n_chk = busy_sum_checkpoints(n);
                 std::uint64_t acc = 0;
                 for (int c = 0; c < n_chk; ++c) {
                     if (c > 0 && stop(/*next_is_final=*/c == n_chk - 1)) {
                         OpOutcome o;  // honored running cancel at this boundary
                         o.has_value = false;
                         o.status = "cancelled";
                         return o;
                     }
                     const std::int64_t begin =
                         static_cast<std::int64_t>(c) * BUSY_SUM_STRIDE;
                     const std::int64_t end =
                         std::min<std::int64_t>(begin + BUSY_SUM_STRIDE, n);
                     for (std::int64_t i = begin; i < end; ++i)
                         acc = (acc + static_cast<std::uint64_t>(i)) & BUSY_SUM_MASK;
                 }
                 OpOutcome o;
                 o.value = static_cast<OpValue>(acc);
                 o.has_value = true;
                 o.status = "completed";
                 return o;
             },
             [](const std::vector<std::int64_t>& a) {
                 return busy_sum_checkpoints(a[0]);
             }}},
    };
    return r;
}

// {name: arity} view of the registry, for Python-boundary validation
// (unknown id / wrong arity rejected before the C++ crossing).
inline const std::unordered_map<std::string, int>& op_arities() {
    static const std::unordered_map<std::string, int> a = [] {
        std::unordered_map<std::string, int> m;
        for (const auto& kv : registry()) m[kv.first] = kv.second.arity;
        return m;
    }();
    return a;
}

// The runtime's OWN result struct -- NOT rayhpx::Result (which stays frozen).
// Carries the core timing/identity/status fields plus the operation value;
// _rayx.cpp / the Python layer keep value and row strictly separate.
struct RuntimeResult {
    std::string actor_id;
    std::int64_t start_ns = 0;
    std::int64_t end_ns = 0;
    std::string status = "completed";
    std::string error;  // empty == null
    OpValue value = 0;
    bool has_value = false;
};

// Runtime actor_id: "rt-hpx-" + 8 lowercase hex chars (e.g. rt-hpx-9f3a1c07).
// Distinct from the harness "act-hpx-" (ServiceLane) / "act-hpxl-" (HpxLane)
// prefixes -- the runtime is its own namespace. Own generator because
// ServiceLane::make_actor_id is private. Stable from Slice 0 (Runtime-level id)
// into Slice 1 (per-RuntimeLane id).
inline std::string make_runtime_actor_id() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 15);
    const char* hex = "0123456789abcdef";
    std::string id = "rt-hpx-";
    for (int i = 0; i < 8; ++i) id += hex[dist(gen)];
    return id;
}

}  // namespace rayx_runtime

#endif  // RAYX_RUNTIME_OPS_HPP
