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
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
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

// --- chain_sum_* shared stage kernel + metadata (exp39) -----------------------
//
// exp39 is the latency/decomposition slice (single actor, single node, synthetic
// int64). A dependent "stage" applies `quantum` units of the SAME masked on-core
// work as busy_sum/fanout_sum and folds it into the running value x:
//
//   chain_stage(x, q) = (x + masked_range_sum(0, q)) & BUSY_SUM_MASK
//
// so a chain of S stages from `seed` is deterministic, int64, overflow-safe, and
// data-dependent (each stage reads the previous value). chain_stage is the SINGLE
// source of truth for BOTH native variants -- the plain C++ loop op chain_sum_loop
// (registry(), the in-process floor) and the hpx::future::then continuation chain
// chain_sum_then (hpx_registry()) -- so the two cannot drift and the experiment's
// three-way equal-work invariant (loop == then == Python left-fold of the one-stage
// loop) holds by construction. Synthetic CPU diagnostic only: NO performance claim,
// NOT inference, and NOT framed as "HPX scheduling wins" (a linear dependent chain
// has no parallelism -- the concurrency/overlap story is the separate exp40).

// Upper bounds enforced at the Python boundary (mirror: CHAIN_STEPS_MAX /
// CHAIN_QUANTUM_MAX in runtime/_validate.py -- keep in sync). STEPS bounds the chain
// length (so chain_sum_then cannot build an unbounded continuation chain and the
// Python-mediated fold issues a bounded number of submits); QUANTUM bounds per-stage
// on-core work. The chain ops are queued-cancelable only (no running cancel), so the
// product is kept modest by construction to bound an uninterruptible call / teardown.
inline constexpr std::int64_t CHAIN_STEPS_MAX = 10'000;
inline constexpr std::int64_t CHAIN_QUANTUM_MAX = 100'000;

// Upper bound on the chain_fanout `count` argument (number of INDEPENDENT child
// chains fanned out via hpx::async, exp40 probe), enforced at the Python boundary
// (mirror: CHAIN_FANOUT_K_MAX in runtime/_validate.py -- keep in sync). Bounds the
// internal hpx::async fan-out so an absurd child count cannot spawn an unbounded
// number of tasks, exactly the FANOUT_PARTS_MAX posture for the launch-all op.
inline constexpr std::int64_t CHAIN_FANOUT_K_MAX = 256;

// One dependent stage. masked_range_sum(0, q) is q units of the shared masked work;
// folding it into x under BUSY_SUM_MASK keeps the result < 2^31 and dependent on x.
// q == 0 -> empty range -> 0 -> identity add (stage returns x unchanged); q < 0 is
// rejected at the Python boundary and re-checked defensively by the op bodies. Pure /
// HPX-free so both the registry() loop op and the hpx_registry() then op call it.
inline std::int64_t chain_stage(std::int64_t x, std::int64_t q) {
    const std::uint64_t acc =
        (static_cast<std::uint64_t>(x) + masked_range_sum(0, q)) & BUSY_SUM_MASK;
    return static_cast<std::int64_t>(acc);
}

// --- diamond_fanin HPX-free metadata (body lives in runtime_ops_hpx.hpp) ------
//
// diamond_fanin(seed, quantum) is the exp46 fixed non-linear (diamond) DAG op. Its
// HPX body (runtime_ops_hpx.hpp) expresses A -> {B, C} -> D with hpx::shared_future +
// two .then continuations + an hpx::dataflow JOIN, so the cross-edge (D depends on BOTH
// B and C) is resolved below the Python/Runtime boundary. It reuses chain_stage (the
// single source of truth) and CHAIN_QUANTUM_MAX (no new constant), so each node is
// byte-identical to chain_sum_loop(x, 1, quantum) and the closed value is Python-checkable:
//
//   A = chain_stage(seed,            quantum)
//   B = chain_stage(A + 1,           quantum)
//   C = chain_stage(A + 2,           quantum)
//   diamond_fanin(seed, quantum) = D = chain_stage((B + C) & BUSY_SUM_MASK, quantum)
//
// (B + C) is commutative, so the value is independent of the two arms' completion order.
// ONE fixed diamond, NOT general DAG scheduling. The hpx::dataflow is REPRESENTATIONAL
// (it expresses the join natively); it is NOT what makes the op a single boundary
// crossing -- any native body would be. NO speedup/throughput/latency/overlap/
// parallelism/Ray claim. Only the bounds reuse lives here so this header stays HPX-free.

// --- barrier_fanin (exp44) HPX-free metadata + diagnostic witness -------------
//
// barrier_fanin(seed, leaves, quantum) is the exp44 "witnessed barrier-gated fan-in"
// op. Its HPX body (runtime_ops_hpx.hpp) launches `leaves` bare-hpx::async children
// that all compute chain_stage(seed+j, quantum), mutually RENDEZVOUS on a shared
// cooperative gate, join with when_all, and reduce with a scheduled .then. The value
// is value-neutral of the gate, so:
//
//   barrier_fanin(seed, leaves, quantum)
//     == ( Σ_{j=0}^{leaves-1} chain_stage(seed+j, quantum) ) & BUSY_SUM_MASK
//     == chain_fanout(seed, leaves, /*steps=*/1, quantum)
//
// so the result is Python-checkable WITHOUT any HPX type. The gate only forces the
// mutual dependency that makes a clean completion at --hpx:threads=1 load-bearing
// (cooperative suspend/resume on ONE default-pool OS worker; a non-cooperative
// interior would deadlock). This is a synthetic barrier/all-reduce shape, NOT general
// DAG scheduling -- NO speedup/throughput/latency/parallelism claim.

// Upper bound on `leaves`, enforced at the Python boundary (mirror: FANIN_LEAVES_MAX in
// runtime/_validate.py -- keep in sync) and re-checked defensively in the op body. Small
// so the gated interior + witness stay bounded; well under CHAIN_FANOUT_K_MAX.
inline constexpr std::int64_t FANIN_LEAVES_MAX = 64;

// Internal cooperative-watchdog deadline / poll stride for the barrier_fanin body
// (defense-in-depth only). GENEROUS deadline (>= exp41's 500 ms, with CI headroom) so a
// healthy success run is opened by the LAST ARRIVER, never the watchdog: a watchdog open
// on a success run is treated as a structural FAILURE upstream. NOT a timing knob and NOT
// the anti-hang guarantee -- the experiment's external subprocess timeout owns that.
inline constexpr int BARRIER_FANIN_WATCHDOG_MS = 5'000;
inline constexpr int BARRIER_FANIN_POLL_MS = 2;

// Debug-only structural witness for the LAST barrier_fanin execution. THIS IS THE ONLY
// SIDE-EFFECTING REGISTRY OP: every other registry()/hpx_registry() op is a pure value
// function (value out, no global state). barrier_fanin additionally writes this witness
// for structural test/experiment gating. It does NOT touch OpOutcome, RuntimeResult,
// lane_stats(), or the v1 JSONL schema. The snapshot is mutex-guarded (no torn read);
// "racy" means only that a reader may observe a stale / cross-call value. Tests and the
// experiment are SINGLE-IN-FLIGHT (one barrier_fanin at a time), under which `seq`
// identifies the execution. The process-global slot is acceptable because Runtime is a
// process singleton. This is NOT scheduler state, NOT a synchronization primitive, NOT
// placement control, and NOT public scheduler introspection.
struct BarrierFaninWitness {
    std::int64_t seq = 0;             // monotonic id of the last recorded execution
    int observed_os_workers = 0;      // hpx::get_os_thread_count() at execution
    int leaves_requested = 0;
    int arrived_count = 0;            // leaves that reached the gate
    int released_count = 0;           // leaves that passed the gate and returned
    std::string opener = "none";      // "last_arriver" | "watchdog" | "none"
    bool reduction_after_all_leaves = false;  // released == leaves at reduction entry
    int ordering_violations = 0;      // defensive invariant breaches; expected 0
    bool clean_exit = false;          // joined_count == leaves
    bool watchdog_opened = false;     // opener == "watchdog"
    int joined_count = 0;             // leaves joined (when_all)
    // OBSERVATION-ONLY (never a pass/fail gate): peak count of leaves simultaneously
    // suspended at gate.get(). This is COORDINATED SUSPENSION -- NOT parallel execution,
    // NOT throughput, NOT worker-level concurrency. At --hpx:threads=1 it can still be
    // large because HPX cooperatively suspends the gated tasks on a single OS worker.
    int max_simultaneously_suspended_leaves = 0;
};

inline std::mutex& barrier_fanin_witness_mutex() {
    static std::mutex m;
    return m;
}

inline BarrierFaninWitness& barrier_fanin_witness_slot() {
    static BarrierFaninWitness w;
    return w;
}

// Record one execution's witness under the mutex (single write site; the op body calls
// this exactly once at completion). Stamps a fresh monotonic `seq`.
inline void record_barrier_fanin_witness(BarrierFaninWitness w) {
    std::lock_guard<std::mutex> g(barrier_fanin_witness_mutex());
    BarrierFaninWitness& slot = barrier_fanin_witness_slot();
    w.seq = slot.seq + 1;
    slot = w;
}

// Snapshot the last witness under the mutex (no torn read). Returns by value.
inline BarrierFaninWitness read_barrier_fanin_witness() {
    std::lock_guard<std::mutex> g(barrier_fanin_witness_mutex());
    return barrier_fanin_witness_slot();
}

// --- exp47 overlap_probe debug-only structural witness ------------------------
//
// overlap_probe(seed, quantum, mode) is the SECOND side-effecting registry op (after
// barrier_fanin): it launches two INDEPENDENT bare-hpx::async arms ("barrier_fanin
// without the gate"), joins with when_all, and returns a closed int64. Its body
// additionally writes this witness for structural test/experiment gating. It does NOT
// touch OpOutcome, RuntimeResult, lane_stats(), or the v1 JSONL schema. The snapshot is
// mutex-guarded (no torn read); "racy" means only that a reader may observe a stale /
// cross-call value. Tests and the experiment are SINGLE-IN-FLIGHT (one overlap_probe at a
// time), under which `seq` identifies the execution. This is NOT scheduler state, NOT a
// synchronization primitive, NOT placement control, and NOT public scheduler
// introspection. The witness observes that both arms are ACTIVE / IN FLIGHT within the
// bracketed arm compute -- it does NOT assert both are executing CPU instructions at the
// same instant (in the cooperative-yielding one-worker case they interleave on one
// worker). "worker_parallel" means only "distinct workers OBSERVED" -- a candidate, never
// a proof of speedup/throughput/latency/general parallel execution.

// Structural caps for the overlap witness (NOT value constants; they bound the witness
// memory only and never affect the returned value).
inline constexpr int OVERLAP_ARMS = 2;                 // fixed two-arm fork
inline constexpr int OVERLAP_WORKER_ID_SET_CAP = 4;    // per-arm deduped worker-id set cap
inline constexpr int OVERLAP_CHUNK_EVENTS_CAP = 8;     // per-arm chunk events kept in trace

// One entry in the bounded overlap event trace. Ordered by `seq` (a monotonic counter
// stamped within the single op execution), NOT by wall clock -- no timestamp is recorded.
struct OverlapEvent {
    int arm_id = 0;            // 0 .. OVERLAP_ARMS-1
    std::string phase;        // "enter" | "chunk" | "leave"
    std::int64_t seq = 0;     // monotonic event order within this execution
    long long worker_id = -1; // hpx::get_worker_thread_num() sample; -1 == unknown/off-worker
};

struct OverlapWitness {
    std::int64_t seq = 0;            // monotonic id of the last recorded execution
    int mode = 0;                    // 0 non-yielding / 1 chunked-yielding arm kernel
    int observed_os_workers = 0;     // hpx::get_os_thread_count() -- CONTEXT ONLY, not the
                                     // disambiguator (it is the static pool size)
    int arms_launched = 0;           // OVERLAP_ARMS
    int arms_completed = 0;
    // Peak count of arms simultaneously IN FLIGHT (entered, not yet left) within the
    // bracketed compute. >= 2 means both arms were in flight together. This is an
    // OBSERVATION, never a pass/fail gate; in the one-worker yielding case it reflects
    // cooperative interleaving, NOT OS-thread parallelism.
    int max_in_flight = 0;
    bool both_in_flight = false;     // max_in_flight >= 2
    std::int64_t per_arm_enter_seq[OVERLAP_ARMS] = {-1, -1};  // event seq at enter, -1 if unseen
    std::int64_t per_arm_leave_seq[OVERLAP_ARMS] = {-1, -1};  // event seq at leave, -1 if unseen
    int per_arm_chunk_event_count[OVERLAP_ARMS] = {0, 0};
    // Per-arm deduped set (capped) of observed hpx::get_worker_thread_num() samples at
    // entry + each chunk. HPX threads may MIGRATE between workers after a suspension
    // point, so a set (not a single sample) is recorded and never over-read.
    std::vector<long long> per_arm_worker_ids[OVERLAP_ARMS];
    bool per_arm_worker_ids_overflowed[OVERLAP_ARMS] = {false, false};
    int ordering_violations = 0;     // defensive invariant breaches; expected 0
    bool clean_exit = false;         // both arms entered & left exactly once; all completed
    // "serial" | "cooperative_interleaving" | "worker_parallel" | "inconclusive".
    // inconclusive covers unknown/overflowed worker sets or malformed traces.
    std::string classification = "inconclusive";
    int event_count = 0;
    bool event_trace_overflowed = false;
    std::vector<OverlapEvent> events;
};

inline std::mutex& overlap_witness_mutex() {
    static std::mutex m;
    return m;
}

inline OverlapWitness& overlap_witness_slot() {
    static OverlapWitness w;
    return w;
}

// Record one execution's witness under the mutex (single write site; the op body calls
// this exactly once at completion, AFTER both arms have joined). Stamps a fresh `seq`.
inline void record_overlap_witness(OverlapWitness w) {
    std::lock_guard<std::mutex> g(overlap_witness_mutex());
    OverlapWitness& slot = overlap_witness_slot();
    w.seq = slot.seq + 1;
    slot = std::move(w);
}

// Snapshot the last witness under the mutex (no torn read). Returns by value.
inline OverlapWitness read_overlap_witness() {
    std::lock_guard<std::mutex> g(overlap_witness_mutex());
    return overlap_witness_slot();
}

// Operation signature: typed args (OpArgs = vector<variant<int64,double>>) + a
// lane-bound StopCheckpoint in, OpOutcome out. Args are arity/type-validated at the
// Python boundary and marshalled into OpArgs in _rayx.cpp; the op body extracts each
// arg with as_int64/as_double (defensive backstop for the raw bypass). A defensive
// arity re-check also happens in _rayx.cpp. Non-checkpointed ops ignore stop.
using OpFn = std::function<OpOutcome(const OpArgs& args,
                                     const StopCheckpoint& stop)>;

// How the serialized RuntimeLane worker runs a task. Inline = call task(stop)
// directly on the worker (no hpx::async hop) -- ONLY for instantaneous,
// non-parking, non-composed work. Async = keep hpx::async(exec_, task).get()
// (cooperative HPX suspension) for parking / checkpointed / internally-composed
// work, where suspending the worker lets sibling lanes overlap. The DEFAULT is
// Async (conservative): any unclassified op/method stays on the existing path.
// This is internal dispatch metadata only -- it is NOT derived from
// checkpoint_count (a small park_ms is checkpoint_count==1 yet MUST stay Async),
// adds no Python surface, and does not change the row/value model.
enum class DispatchPolicy { Inline, Async };

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
    // Internal lane-dispatch policy (last field, defaulted). Inline ops run
    // directly on the lane worker; everything else (default) keeps the async
    // hop. Defaulted so existing aggregate initializers are unchanged -- only
    // the instantaneous ops below opt into Inline.
    DispatchPolicy policy = DispatchPolicy::Async;
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
             {OpType::Int64}, OpType::Int64, DispatchPolicy::Inline}},
        {"add", OpEntry{2,
             [](const OpArgs& a, const StopCheckpoint&) {
                 OpOutcome o;
                 o.value = as_int64(a, 0, "add") + as_int64(a, 1, "add");
                 o.has_value = true;
                 return o;
             },
             one_checkpoint,
             {OpType::Int64, OpType::Int64}, OpType::Int64, DispatchPolicy::Inline}},
        {"boom", OpEntry{0,
             [](const OpArgs&, const StopCheckpoint&)
                 -> OpOutcome {
                 throw std::runtime_error(
                     "boom: intentional failure for failure-path testing");
             },
             one_checkpoint,
             {}, OpType::Int64, DispatchPolicy::Inline}},
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
             {OpType::Double, OpType::Double}, OpType::Double, DispatchPolicy::Inline}},
        {"chain_sum_loop", OpEntry{3,
             // exp39 native in-process FLOOR: apply chain_stage in a plain C++ loop
             // over `steps` dependent stages on the lane worker -- NO per-stage
             // hpx::future::then. One submitted op, one outer async hop (default
             // Async policy). Deterministic; shares chain_stage with chain_sum_then,
             // so the three-way equal-work invariant holds by construction. Also the
             // ONE-STAGE unit for the experiment's Python-mediated fold (steps == 1).
             [](const OpArgs& a, const StopCheckpoint&) -> OpOutcome {
                 const std::int64_t seed = as_int64(a, 0, "chain_sum_loop");
                 const std::int64_t steps = as_int64(a, 1, "chain_sum_loop");
                 const std::int64_t quantum = as_int64(a, 2, "chain_sum_loop");
                 // Defensive native guard (the public Python boundary already rejects
                 // these; this protects the private/native bypass). Throw is mapped by
                 // make_op_task to a status="failed" row, never a crash.
                 if (steps < 0 || steps > CHAIN_STEPS_MAX ||
                     quantum < 0 || quantum > CHAIN_QUANTUM_MAX) {
                     throw std::invalid_argument(
                         "chain_sum_loop requires 0 <= steps <= "
                         + std::to_string(CHAIN_STEPS_MAX)
                         + " and 0 <= quantum <= "
                         + std::to_string(CHAIN_QUANTUM_MAX));
                 }
                 std::int64_t x = seed;  // steps == 0 -> returns seed unchanged
                 for (std::int64_t k = 0; k < steps; ++k)
                     x = chain_stage(x, quantum);
                 OpOutcome o;
                 o.value = x;  // int64 -> OpValue
                 o.has_value = true;
                 o.status = "completed";
                 return o;
             },
             one_checkpoint,  // queued-cancelable only (count 1)
             {OpType::Int64, OpType::Int64, OpType::Int64}, OpType::Int64}},
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
