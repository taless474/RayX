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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace rayx_runtime {

// Closed value channel (value-model V3): int64 OR double. The variant is the
// INTERNAL representation only -- args are marshalled into it C++-side from Python
// ints/floats (see _rayx.cpp), and results are converted back to Python int/float in
// RuntimeFuture.result() on the Python thread. It is NEVER serialized or exposed as a
// std::variant across pybind or any ABI. bytes is deliberately NOT an alternative
// (gated hardest -- a heap payload + object-store-drift risk).
using OpValue = std::variant<std::int64_t, double>;
using OpArgs = std::vector<OpValue>;

// Closed argument/result type tag. The enum lets the registry carry typed signatures
// (arg_types/result_type) so the Python boundary validates per-arg types. Tags are
// APPEND-ONLY (ABI-shaped): never renumber. V3 ships Int64 + Double; Bytes stays
// reserved (not defined, so no op can declare it).
enum class OpType : std::uint8_t {
    Int64 = 0,
    Double = 1,
    // Bytes = 2,  // reserved -- gated hardest; not shipped
};

// Stable string name for an OpType, used to serialize typed signatures to Python in
// runtime_op_table(). Names are part of the Python-boundary contract -- keep stable.
inline const char* op_type_name(OpType t) {
    switch (t) {
        case OpType::Int64: return "int64";
        case OpType::Double: return "double";
    }
    return "unknown";  // unreachable for shipped tags; defensive only
}

// Typed argument extraction (value-model V3). The PUBLIC path is type-validated at
// the Python boundary, so these always succeed there; they are the NATIVE defensive
// backstop for the private/raw-_RuntimeEngine bypass. A wrong tag throws
// std::invalid_argument, which make_op_task maps to a status="failed" row (never a
// crash) -- same posture as the fanout_sum defensive guard. is_int64/is_double are
// non-throwing predicates used by the defensive checkpoint_count lambdas (which run
// BEFORE the task/future exist and therefore cannot rely on failed-row mapping).
inline bool is_int64(const OpArgs& a, std::size_t i) {
    return i < a.size() && std::holds_alternative<std::int64_t>(a[i]);
}
inline bool is_double(const OpArgs& a, std::size_t i) {
    return i < a.size() && std::holds_alternative<double>(a[i]);
}
inline std::int64_t as_int64(const OpArgs& a, std::size_t i, const char* op) {
    if (!is_int64(a, i))
        throw std::invalid_argument(std::string("operation '") + op +
            "' argument " + std::to_string(i) + " must be int64");
    return std::get<std::int64_t>(a[i]);
}
inline double as_double(const OpArgs& a, std::size_t i, const char* op) {
    if (!is_double(a, i))
        throw std::invalid_argument(std::string("operation '") + op +
            "' argument " + std::to_string(i) + " must be double");
    return std::get<double>(a[i]);
}

// One operation outcome: a value (when has_value) plus a row-status/error. A
// native operation that throws is mapped (in _rayx.cpp) to status="failed" with
// has_value=false; a checkpointed op (busy_sum) that honors a cooperative running
// cancel returns status="cancelled", has_value=false.
struct OpOutcome {
    OpValue value{};  // default: int64{0}; meaningful only when has_value
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
// Ceil is computed as (n - 1) / STRIDE + 1 (n > STRIDE here, so n - 1 is safe):
// the naive (n + STRIDE - 1) / STRIDE overflows int64 for n near INT64_MAX,
// which IS reachable from Python (any int64 n >= 0 passes validation, and this
// runs at submit time).
inline int busy_sum_checkpoints(std::int64_t n) {
    if (n <= BUSY_SUM_STRIDE) return 1;
    const std::int64_t c = (n - 1) / BUSY_SUM_STRIDE + 1;
    return c > static_cast<std::int64_t>(INT_MAX) ? INT_MAX : static_cast<int>(c);
}

// Shared masked checkpoint loop for busy_sum (op) and busy_get (actor method):
// the single source of truth for the chunked on-core work both use, so the two
// cannot drift. Accumulates acc = (Σ_{i=0}^{n-1} i) & BUSY_SUM_MASK in
// BUSY_SUM_STRIDE-sized chunks, polling stop(next_is_final) BEFORE each chunk
// except the first (k > 0), with next_is_final = (k == n_chk - 1) clearing
// running-cancellability before the last segment. Returns false on an honored
// running cancel (acc is then meaningless), true on completion. NO sleep --
// genuine on-core work; n >= 0 is guaranteed by the Python boundary.
inline bool run_masked_checkpoint_loop(std::int64_t n, const StopCheckpoint& stop,
                                       std::uint64_t& acc) {
    const int n_chk = busy_sum_checkpoints(n);
    acc = 0;
    for (int k = 0; k < n_chk; ++k) {
        if (k > 0 && stop(/*next_is_final=*/k == n_chk - 1))
            return false;  // honored running cancel at this chunk boundary
        const std::int64_t begin =
            static_cast<std::int64_t>(k) * BUSY_SUM_STRIDE;
        const std::int64_t end = std::min<std::int64_t>(begin + BUSY_SUM_STRIDE, n);
        for (std::int64_t i = begin; i < end; ++i)
            acc = (acc + static_cast<std::uint64_t>(i)) & BUSY_SUM_MASK;
    }
    return true;
}

// --- park_ms HPX-free metadata (body lives in runtime_ops_hpx.hpp) -----------
//
// park_ms(ms) is the PARKED / cooperative-wait work shape: the parked analog of
// the CPU-bound busy_sum diagnostic. The HPX body (chunked cooperative
// hpx::this_thread::sleep_for) lives HPX-side in runtime_ops_hpx.hpp; only the
// pure bounds/checkpoint metadata lives here so this registry header stays
// HPX-free. Synthetic diagnostic work only -- NOT real I/O, NOT inference, NOT a
// serving claim, and NO performance claim.

// Upper bound on `ms`, enforced at the Python boundary (mirror: PARK_MS_MAX in
// runtime/_validate.py -- keep the two in sync). Generous for tests/demos while
// keeping any parked lane bounded by construction.
inline constexpr std::int64_t PARK_MS_MAX = 60'000;  // 60 s

// Chunk stride for the cooperative park: the body parks in PARK_MS_STRIDE-ms
// chunks, polling the StopCheckpoint before each chunk after the first, so a
// running cancel (and shutdown's cancel_pending) stops a park at the NEXT chunk
// boundary -- bounding cancel/shutdown latency to one chunk, never the full park.
inline constexpr std::int64_t PARK_MS_STRIDE = 10;

// Checkpoint count for a park over `ms` milliseconds: ceil(ms / STRIDE), at
// least 1 (ms <= STRIDE -> 1 -> queued-cancelable only; ms = 0 parks nothing).
// Same overflow-safe (ms - 1) / STRIDE + 1 ceil form as busy_sum_checkpoints.
// The Python boundary caps ms at PARK_MS_MAX so the count stays small; the form
// stays overflow-safe for raw-bypass inputs anyway.
inline int park_ms_checkpoints(std::int64_t ms) {
    if (ms <= PARK_MS_STRIDE) return 1;
    const std::int64_t c = (ms - 1) / PARK_MS_STRIDE + 1;
    return c > static_cast<std::int64_t>(INT_MAX) ? INT_MAX : static_cast<int>(c);
}

// --- fanout_sum HPX-free metadata/kernel (body lives in runtime_ops_hpx.hpp) ---
//
// fanout_sum(n, parts) is the first internally-composed op (see
// docs/design/rayx_runtime_internal_composition_note.md, Candidate B / P1). The
// HPX composition (hpx::async per part + when_all) lives HPX-side in
// runtime_ops_hpx.hpp; only these PURE, HPX-free pieces live here so the registry
// header stays HPX-free.

// Upper bound on `parts`, enforced at the Python boundary (mirror: PARTS_MAX in
// runtime/_validate.py). Bounds the internal hpx::async fan-out so an absurd part
// count cannot spawn an unbounded number of tasks.
inline constexpr int FANOUT_PARTS_MAX = 1024;

// Masked partial sum over a half-open range: (Σ_{i=begin}^{end-1} i) with the same
// per-step BUSY_SUM_MASK as busy_sum, so a disjoint contiguous cover of [0, n)
// folds (under the mask) to exactly busy_sum(n). An empty range (begin >= end,
// e.g. when parts > n) contributes 0. Pure/HPX-free: the HPX body just schedules
// these across parts and combines the results.
inline std::uint64_t masked_range_sum(std::int64_t begin, std::int64_t end) {
    std::uint64_t acc = 0;
    for (std::int64_t i = begin; i < end; ++i)
        acc = (acc + static_cast<std::uint64_t>(i)) & BUSY_SUM_MASK;
    return acc;
}

// Checkpoint count for fanout_sum. P1 design is launch-all + when_all, so the op is
// QUEUED-cancelable only: there is no honest running-cancel boundary once every part
// has been launched. Always 1 -> begin_service never arms running-cancellability, so
// an active cancel() returns false (see the design note's P1 cancellation story). A
// future P2 bounded-wave variant would instead return ceil(parts / FANOUT_WAVE_SIZE).
inline int fanout_sum_checkpoints(std::int64_t /*parts*/) { return 1; }

// Operation signature: typed args (OpArgs = vector<variant<int64,double>>) + a
// lane-bound StopCheckpoint in, OpOutcome out. Args are arity/type-validated at the
// Python boundary and marshalled into OpArgs in _rayx.cpp; the op body extracts each
// arg with as_int64/as_double (defensive backstop for the raw bypass). A defensive
// arity re-check also happens in _rayx.cpp. Non-checkpointed ops ignore stop.
using OpFn = std::function<OpOutcome(const OpArgs& args,
                                     const StopCheckpoint& stop)>;

struct OpEntry {
    int arity;
    OpFn fn;
    // Number of cancellation checkpoints this call will reach, from its args. The
    // engine passes it to RuntimeCancelToken::begin_service so running-cancel is
    // armed only when the op actually has a boundary to stop at (> 1). Instantaneous
    // ops are always 1. This runs BEFORE the task/future exist, so a checkpointed op
    // must read its args DEFENSIVELY (is_int64/is_double -> real count, else 1) and
    // let the op body produce the failed row on a wrong tag.
    std::function<int(const OpArgs& args)> checkpoint_count;
    // Typed signature (closed value model: int64 / finite double). arg_types has
    // exactly `arity` entries; result_type is the declared result tag (for `boom`,
    // which always throws, the declared type is moot but kept int64 for
    // uniformity). Exposed to Python via runtime_op_table() so the boundary
    // validates per-arg types + domains (int64 range; double strict-float
    // finiteness) BEFORE the crossing. These are metadata only -- the closed
    // OpValue variant is the value channel.
    std::vector<OpType> arg_types;
    OpType result_type = OpType::Int64;
};

// Every instantaneous op reaches exactly one (notional) checkpoint -> count 1.
// Ignores args, so it never extracts a typed value (no wrong-tag concern).
inline int one_checkpoint(const OpArgs&) { return 1; }

// The fixed registry, built once. `square` / `add` are real native operations;
// `boom` is a tiny throwing operation that exercises the failure-path mapping
// (status="failed"); `busy_sum` is the checkpointed iterative op that exercises
// queued + cooperative running cancellation (Slice 2a).
inline const std::unordered_map<std::string, OpEntry>& registry() {
    static const std::unordered_map<std::string, OpEntry> r = {
        {"square", OpEntry{1,
             [](const OpArgs& a, const StopCheckpoint&) {
                 const std::int64_t x = as_int64(a, 0, "square");
                 OpOutcome o;
                 o.value = x * x;  // int64 -> OpValue (variant) via assignment
                 o.has_value = true;
                 return o;
             },
             one_checkpoint,
             {OpType::Int64}, OpType::Int64}},
        {"add", OpEntry{2,
             [](const OpArgs& a, const StopCheckpoint&) {
                 OpOutcome o;
                 o.value = as_int64(a, 0, "add") + as_int64(a, 1, "add");
                 o.has_value = true;
                 return o;
             },
             one_checkpoint,
             {OpType::Int64, OpType::Int64}, OpType::Int64}},
        {"boom", OpEntry{0,
             [](const OpArgs&, const StopCheckpoint&)
                 -> OpOutcome {
                 throw std::runtime_error(
                     "boom: intentional failure for failure-path testing");
             },
             one_checkpoint,
             {}, OpType::Int64}},
        {"busy_sum", OpEntry{1,
             // Real native iterative work: acc = (Σ_{i=0}^{n-1} i) mod 2^31, with
             // per-step masking so acc stays < 2^31 (overflow-safe, deterministic;
             // equals (n*(n-1)/2) mod 2^31). The chunk loop is the shared
             // run_masked_checkpoint_loop above (also used by the actor busy_get):
             // per STRIDE-sized chunk, poll stop(next_is_final) BEFORE the chunk's
             // work; the final boundary clears running-cancellability before the
             // last segment. NO sleep -- this is on-core work. n >= 0 is
             // guaranteed by the Python boundary.
             [](const OpArgs& a, const StopCheckpoint& stop)
                 -> OpOutcome {
                 const std::int64_t n = as_int64(a, 0, "busy_sum");
                 std::uint64_t acc = 0;
                 if (!run_masked_checkpoint_loop(n, stop, acc)) {
                     OpOutcome o;  // honored running cancel at a chunk boundary
                     o.has_value = false;
                     o.status = "cancelled";
                     return o;
                 }
                 OpOutcome o;
                 o.value = static_cast<std::int64_t>(acc);  // -> OpValue (int64)
                 o.has_value = true;
                 o.status = "completed";
                 return o;
             },
             // Defensive checkpoint_count: runs BEFORE the task/future exist, so it
             // cannot produce a failed row. On the valid (int64) tag it returns the
             // real count; on a wrong tag (only reachable via the raw bypass) it
             // returns 1 (queued-cancelable only) and lets the body's as_int64
             // produce the failed row -- never a throw-to-Python here.
             [](const OpArgs& a) {
                 return is_int64(a, 0)
                     ? busy_sum_checkpoints(std::get<std::int64_t>(a[0]))
                     : 1;
             },
             {OpType::Int64}, OpType::Int64}},
        {"scale_double", OpEntry{2,
             // First double op (value-model V3): scale_double(x, factor) = x * factor,
             // double in / double out. Instantaneous (one_checkpoint), so it never
             // extracts a typed arg in checkpoint_count. Deterministic for exactly
             // representable doubles; no internal HPX fan-out (float sum is not
             // associative) and no performance intent.
             [](const OpArgs& a, const StopCheckpoint&) {
                 const double x = as_double(a, 0, "scale_double");
                 const double factor = as_double(a, 1, "scale_double");
                 OpOutcome o;
                 o.value = x * factor;  // double -> OpValue (variant)
                 o.has_value = true;
                 return o;
             },
             one_checkpoint,
             {OpType::Double, OpType::Double}, OpType::Double}},
    };
    return r;
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
    OpValue value{};    // default: int64{0}; meaningful only when has_value
    bool has_value = false;
};

// Runtime actor_id: `prefix` + 16 lowercase hex chars
// (e.g. rt-hpx-9f3a1c07b2d4e601). The default prefix "rt-hpx-" is the
// operation-lane id namespace, distinct from the harness "act-hpx-" (ServiceLane) /
// "act-hpxl-" (HpxLane) prefixes. Own generator because ServiceLane::make_actor_id
// is private. Stable from Slice 0 (Runtime-level id) into Slice 1 (per-RuntimeLane
// id).
//
// The `prefix` parameter is ADDITIVE and DEFAULTS to "rt-hpx-", so every existing
// call site (operation lanes) is unchanged byte-for-byte. A future local-actor lane
// will pass "rt-act-" to give actor method rows / lane_stats a distinct id
// namespace; no caller passes a non-default prefix yet.
//
// Entropy: 16 hex chars carry a REAL 64-bit random value, not 16 chars printed from
// a 32-bit PRNG seed. We draw two 32-bit std::random_device words and combine them
// into a std::uint64_t, so the effective id space is 2^64 (birthday-safe well past
// the realistic actor/lane count) rather than the 2^32 a single-seed mt19937 would
// have given. Drawing straight from random_device keeps this STATELESS -- no shared
// mutable PRNG, no global counter, no thread-safety concern -- and is not a UUID.
inline std::string make_runtime_actor_id(const std::string& prefix = "rt-hpx-") {
    std::random_device rd;
    const std::uint64_t hi = static_cast<std::uint64_t>(rd());
    const std::uint64_t lo = static_cast<std::uint64_t>(rd());
    const std::uint64_t v = (hi << 32) | lo;
    const char* hex = "0123456789abcdef";
    std::string id = prefix;
    // Most-significant nibble first; all 16 nibbles emitted, so the suffix is always
    // zero-padded to exactly 16 lowercase hex chars.
    for (int shift = 60; shift >= 0; shift -= 4)
        id += hex[(v >> shift) & 0xFULL];
    return id;
}

}  // namespace rayx_runtime

#endif  // RAYX_RUNTIME_OPS_HPP
