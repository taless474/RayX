// rayx runtime: HPX-side registry for internally-composed registered operations.
//
// Part of the EXPERIMENTAL `rayx.runtime` prototype. This header is the HPX-side
// companion to the deliberately HPX-free runtime_ops.hpp: it holds the operation
// entries whose BODIES use HPX composition (hpx::async / when_all) and therefore
// cannot live in the HPX-free registry header. The structural rule
// (docs/design/rayx_runtime_internal_composition_note.md, Candidate B):
//
//   * runtime_ops.hpp stays HPX-free and keeps only pure helpers/constants
//     (FANOUT_PARTS_MAX, masked_range_sum, fanout_sum_checkpoints);
//   * the composed OpEntry (arity + typed signature + HPX body + checkpoint
//     metadata) lives HERE, in hpx_registry();
//   * _rayx.cpp builds the typed Python-boundary table (runtime_op_table) directly
//     from registry() + hpx_registry() and checks BOTH registries at dispatch.
//
// The op bodies reuse the SAME OpEntry / OpFn / OpOutcome / StopCheckpoint types
// from runtime_ops.hpp; only the body lambdas pull in HPX. Nothing here exposes an
// HPX type to Python, emits a child row, or creates a Python-visible future: the
// internal futures are an implementation detail of one operation.

#ifndef RAYX_RUNTIME_OPS_HPX_HPP
#define RAYX_RUNTIME_OPS_HPX_HPP

#include "runtime_ops.hpp"  // OpEntry/OpFn/OpOutcome/StopCheckpoint, masked_range_sum,
                            // fanout_sum_checkpoints, FANOUT_PARTS_MAX, BUSY_SUM_MASK

#include <hpx/hpx.hpp>  // hpx::async, hpx::when_all, hpx::future, hpx::this_thread::sleep_for,
                        // hpx::promise, hpx::shared_future, hpx::get_os_thread_count

#include <atomic>     // std::atomic (barrier_fanin gate bookkeeping)
#include <chrono>     // std::chrono::milliseconds (park_ms chunk, barrier_fanin watchdog)
#include <cstdint>
#include <stdexcept>  // std::invalid_argument (defensive native arg guard)
#include <string>
#include <unordered_map>
#include <vector>

namespace rayx_runtime {

// The HPX-side registry. Two entries: the internally-composed fanout_sum and the
// cooperative parked park_ms. Both INTENTIONALLY inherit the default
// DispatchPolicy::Async (no policy field set below): park_ms must keep the
// cooperative hop so a parked lane frees its worker, and fanout_sum composes
// internal hpx::async children that rely on running inside the async body.
// Neither may run inline on the lane worker. First:
//
//   fanout_sum(n, parts) = (Σ_{i=0}^{n-1} i) mod 2^31  ==  busy_sum(n)
//
// P1 design (launch-all): split [0, n) into `parts` contiguous, disjoint ranges,
// launch each masked partial via hpx::async, combine with hpx::when_all(...).get()
// plus a masked fold. The lane already runs this body inside hpx::async(exec_,
// task).get(), so the nested async/.get() here are COOPERATIVE HPX suspensions on
// the lane worker (no OS-thread blocking). Sum-mod is associative, so the result is
// independent of `parts` and the fan-out order. The `stop` checkpoint is UNUSED:
// fanout_sum_checkpoints(parts) == 1, so begin_service never arms running-cancel ->
// the op is queued-cancelable only and an active cancel() returns false. We do NOT
// poll stop() after launching all parts, because by then no work could be saved
// (the honesty posture in the design note's P1 cancellation story).
//
// Range split is division-based (no n*(k+1) multiply) so the index math cannot
// overflow: part k has length base + (k < rem ? 1 : 0), a balanced contiguous cover
// of [0, n). When parts > n some trailing ranges are empty (partial 0), which is a
// valid cover. n >= 0 and 1 <= parts <= FANOUT_PARTS_MAX are guaranteed by the
// public Python boundary (runtime/_validate.py); the body ALSO re-checks them
// defensively so the private/native bypass (e.g. constructing `_RuntimeEngine`
// directly) cannot reach the `n / parts` divide with parts == 0 (UB). A defensive
// throw is mapped by make_op_task (_rayx.cpp) to a `status="failed"` row, never a
// crash.
inline const std::unordered_map<std::string, OpEntry>& hpx_registry() {
    static const std::unordered_map<std::string, OpEntry> r = {
        {"fanout_sum", OpEntry{2,
             [](const OpArgs& a, const StopCheckpoint&)
                 -> OpOutcome {
                 const std::int64_t n = as_int64(a, 0, "fanout_sum");
                 const std::int64_t parts = as_int64(a, 1, "fanout_sum");
                 // Defensive native guard. The public Python path already rejects
                 // these at the boundary; this protects the private/native bypass.
                 // parts < 1 would make `n / parts` a divide-by-zero (UB), so reject
                 // before any arithmetic; n < 0 and parts > FANOUT_PARTS_MAX are
                 // rejected too for parity. The throw is caught by make_op_task and
                 // mapped to a status="failed" row -- never a crash.
                 if (n < 0 || parts < 1 || parts > FANOUT_PARTS_MAX) {
                     throw std::invalid_argument(
                         "fanout_sum requires n >= 0 and 1 <= parts <= "
                         + std::to_string(FANOUT_PARTS_MAX));
                 }
                 // Launch all parts: each computes a masked partial over its range.
                 std::vector<hpx::future<std::uint64_t>> futs;
                 futs.reserve(static_cast<std::size_t>(parts));
                 const std::int64_t base = n / parts;  // parts >= 1 guaranteed
                 const std::int64_t rem = n % parts;
                 std::int64_t begin = 0;
                 for (std::int64_t k = 0; k < parts; ++k) {
                     const std::int64_t len = base + (k < rem ? 1 : 0);
                     const std::int64_t end = begin + len;
                     futs.push_back(hpx::async([begin, end]() {
                         return masked_range_sum(begin, end);
                     }));
                     begin = end;
                 }
                 // Combine: when_all(...).get() yields the now-ready futures; fold
                 // their masked partials back under the mask. This is the only place
                 // the body waits, and it is a cooperative suspension of the lane
                 // worker (not an OS-thread block).
                 std::vector<hpx::future<std::uint64_t>> done =
                     hpx::when_all(futs).get();
                 std::uint64_t acc = 0;
                 for (auto& f : done)
                     acc = (acc + f.get()) & BUSY_SUM_MASK;
                 OpOutcome o;
                 o.value = static_cast<std::int64_t>(acc);  // -> OpValue (int64)
                 o.has_value = true;
                 o.status = "completed";
                 return o;
             },
             // Defensive checkpoint_count (always 1 for P1 launch-all): guard the tag
             // read so a wrong-tag raw bypass returns 1 here and lets the body's
             // as_int64 produce the failed row, never a throw-to-Python.
             [](const OpArgs& a) {
                 return is_int64(a, 1)
                     ? fanout_sum_checkpoints(std::get<std::int64_t>(a[1]))
                     : 1;  // P1: always 1 (queued-only)
             },
             // Typed signature: fanout_sum(int64 n, int64 parts) -> int64.
             {OpType::Int64, OpType::Int64}, OpType::Int64}},
        // park_ms(ms) -> int64: PARKED / cooperative-wait synthetic work, the
        // parked analog of the CPU-bound busy_sum diagnostic. Parks in
        // PARK_MS_STRIDE-ms chunks via hpx::this_thread::sleep_for -- a
        // COOPERATIVE suspension of the HPX thread running the body (NEVER
        // std::this_thread::sleep_for, which would pin an OS worker) -- polling
        // stop(next_is_final) BEFORE each chunk after the first, exactly the
        // busy_sum chunk contract: a running cancel (and shutdown's
        // cancel_pending) stops the park at the next chunk boundary (<= one
        // stride, never the full park), and the final boundary clears
        // running-cancellability before the last chunk. Completion ECHOES ms
        // back (deterministic value path); ms = 0 parks nothing
        // (checkpoint_count == 1, queued-cancelable only). 0 <= ms <=
        // PARK_MS_MAX is guaranteed by the Python boundary and re-checked
        // defensively for the raw/native bypass (throw -> status="failed" row
        // via make_op_task, never a crash). Synthetic diagnostic only: NO
        // performance claim, NOT real I/O or inference.
        {"park_ms", OpEntry{1,
             [](const OpArgs& a, const StopCheckpoint& stop)
                 -> OpOutcome {
                 const std::int64_t ms = as_int64(a, 0, "park_ms");
                 if (ms < 0 || ms > PARK_MS_MAX) {
                     throw std::invalid_argument(
                         "park_ms requires 0 <= ms <= "
                         + std::to_string(PARK_MS_MAX));
                 }
                 const int n_chk = park_ms_checkpoints(ms);
                 std::int64_t remaining = ms;
                 for (int k = 0; k < n_chk; ++k) {
                     if (k > 0 && stop(/*next_is_final=*/k == n_chk - 1)) {
                         OpOutcome o;  // honored running cancel at this boundary
                         o.has_value = false;
                         o.status = "cancelled";
                         return o;
                     }
                     const std::int64_t chunk =
                         remaining < PARK_MS_STRIDE ? remaining : PARK_MS_STRIDE;
                     if (chunk > 0) {
                         hpx::this_thread::sleep_for(
                             std::chrono::milliseconds(chunk));
                     }
                     remaining -= chunk;
                 }
                 OpOutcome o;
                 o.value = ms;  // completion echoes the requested ms (int64)
                 o.has_value = true;
                 o.status = "completed";
                 return o;
             },
             // Defensive checkpoint_count, mirroring busy_sum: the real count on
             // the valid int64 tag, else 1 (queued-cancelable only) -- the body's
             // as_int64 then produces the failed row; never a throw-to-Python.
             [](const OpArgs& a) {
                 return is_int64(a, 0)
                     ? park_ms_checkpoints(std::get<std::int64_t>(a[0]))
                     : 1;
             },
             // Typed signature: park_ms(int64 ms) -> int64.
             {OpType::Int64}, OpType::Int64}},
        // chain_sum_then(seed, steps, quantum) -> int64: exp39 native HPX-continuation
        // variant. Express the SAME dependent S-stage chain as an hpx::future::then
        // continuation chain, each stage PINNED to hpx::launch::async so it is a
        // genuinely SCHEDULED continuation (not inlined onto the completing thread).
        // This is a DELIBERATELY VISIBLE / pessimistic measurement of scheduled
        // hpx::future::then shared-state + continuation cost -- NOT the theoretical
        // lower bound of HPX composition (hpx::launch::sync, sender/receiver
        // composition, or the plain chain_sum_loop are different design-space points;
        // launch::sync in particular would inline the continuations and collapse this
        // toward the loop, erasing the very cost the variant exists to isolate). The
        // body runs inside the lane's hpx::async(exec_, task).get(), so the terminal
        // fut.get() here is a COOPERATIVE suspension of the lane worker (no OS-thread
        // block), exactly the fanout_sum posture. Shares chain_stage with
        // chain_sum_loop so the three-way equal-work invariant holds by construction.
        // Queued-cancelable only (launch-all-style: checkpoint_count == 1).
        {"chain_sum_then", OpEntry{3,
             [](const OpArgs& a, const StopCheckpoint&) -> OpOutcome {
                 const std::int64_t seed = as_int64(a, 0, "chain_sum_then");
                 const std::int64_t steps = as_int64(a, 1, "chain_sum_then");
                 const std::int64_t quantum = as_int64(a, 2, "chain_sum_then");
                 // Defensive native guard (public boundary already rejects these;
                 // protects the private/native bypass). Throw -> status="failed" row.
                 if (steps < 0 || steps > CHAIN_STEPS_MAX ||
                     quantum < 0 || quantum > CHAIN_QUANTUM_MAX) {
                     throw std::invalid_argument(
                         "chain_sum_then requires 0 <= steps <= "
                         + std::to_string(CHAIN_STEPS_MAX)
                         + " and 0 <= quantum <= "
                         + std::to_string(CHAIN_QUANTUM_MAX));
                 }
                 // Build the dependent continuation chain: start from a ready seed,
                 // then fold `steps` stages, each scheduled via
                 // .then(hpx::launch::async, ...). steps == 0 leaves the ready seed.
                 hpx::future<std::int64_t> fut = hpx::make_ready_future(seed);
                 for (std::int64_t k = 0; k < steps; ++k) {
                     fut = fut.then(hpx::launch::async,
                         [quantum](hpx::future<std::int64_t> prev) {
                             return chain_stage(prev.get(), quantum);
                         });
                 }
                 OpOutcome o;
                 o.value = fut.get();  // cooperative suspension of the lane worker
                 o.has_value = true;
                 o.status = "completed";
                 return o;
             },
             // Queued-cancelable only: the launched .then chain has no honest running
             // boundary in this slice. Always 1 (ignores args -> no tag read).
             one_checkpoint,
             // Typed signature: chain_sum_then(int64 seed, int64 steps, int64 quantum)
             // -> int64.
             {OpType::Int64, OpType::Int64, OpType::Int64}, OpType::Int64}},
        // chain_fanout(seed, count, steps, quantum) -> int64: exp40 HPX-NATIVE
        // intra-op overlap probe (the load-bearing HPX arm; the RayX lane-level arm
        // submits K independent chain_sum_loop ops in Python and needs no new op). One
        // submitted op fans out `count` INDEPENDENT child chains -- child j runs the
        // SAME chain_stage loop as chain_sum_loop over `steps` stages from a derived
        // seed_j -- via bare hpx::async on the default HPX pool, joins them with
        // hpx::when_all, and folds deterministically under BUSY_SUM_MASK. This mirrors
        // the existing fanout_sum launch-all + when_all pattern (so it stays inside the
        // fixed registered native operation model) but the children are independent
        // chains rather than masked range partials. The body runs inside the lane's
        // hpx::async(exec_, task).get(), so the when_all(...).get() here is a
        // COOPERATIVE suspension of the lane worker (no OS-thread block), exactly the
        // fanout_sum posture -- the children overlap on the worker pool while the lane
        // worker is suspended at the join, which is the HPX scheduler placement the
        // probe is built to observe. NO Python callback, NO Python object, NO Ray
        // baseline, NO HPX fabric/parcelport/AGAS. Synthetic CPU diagnostic only: NO
        // performance claim. Queued-cancelable only (launch-all-style: count == 1).
        //
        // Equal-work invariant (the credibility gate, NOT timing):
        //   chain_fanout(seed, K, S, q)
        //     == ( Σ_{j=0}^{K-1} chain_sum_loop(seed + j, S, q) ) & BUSY_SUM_MASK
        // so the folded value is the deterministic masked fold of K independent
        // reference chains -- checkable from Python without any HPX type.
        {"chain_fanout", OpEntry{4,
             [](const OpArgs& a, const StopCheckpoint&) -> OpOutcome {
                 const std::int64_t seed = as_int64(a, 0, "chain_fanout");
                 const std::int64_t count = as_int64(a, 1, "chain_fanout");
                 const std::int64_t steps = as_int64(a, 2, "chain_fanout");
                 const std::int64_t quantum = as_int64(a, 3, "chain_fanout");
                 // Defensive native guard (public boundary already rejects these;
                 // protects the private/native bypass). Throw -> status="failed" row.
                 if (count < 1 || count > CHAIN_FANOUT_K_MAX ||
                     steps < 0 || steps > CHAIN_STEPS_MAX ||
                     quantum < 0 || quantum > CHAIN_QUANTUM_MAX) {
                     throw std::invalid_argument(
                         "chain_fanout requires 1 <= count <= "
                         + std::to_string(CHAIN_FANOUT_K_MAX)
                         + ", 0 <= steps <= " + std::to_string(CHAIN_STEPS_MAX)
                         + ", and 0 <= quantum <= "
                         + std::to_string(CHAIN_QUANTUM_MAX));
                 }
                 // Launch `count` INDEPENDENT child chains on the default HPX pool. Each
                 // child applies chain_stage `steps` times from seed_j = seed + j; the
                 // add is done in uint64 so a large seed cannot trip signed overflow
                 // (well-defined, deterministic, and == seed + j for the in-range seeds
                 // the boundary/tests use). chain_stage is the SAME kernel chain_sum_loop
                 // uses, so per-child work is byte-identical to a reference chain.
                 std::vector<hpx::future<std::int64_t>> futs;
                 futs.reserve(static_cast<std::size_t>(count));
                 for (std::int64_t j = 0; j < count; ++j) {
                     const std::int64_t seed_j = static_cast<std::int64_t>(
                         static_cast<std::uint64_t>(seed)
                         + static_cast<std::uint64_t>(j));
                     futs.push_back(hpx::async([seed_j, steps, quantum]() {
                         std::int64_t x = seed_j;  // steps == 0 -> seed_j unchanged
                         for (std::int64_t k = 0; k < steps; ++k)
                             x = chain_stage(x, quantum);
                         return x;
                     }));
                 }
                 // Join: when_all(...).get() is the only wait, a cooperative suspension
                 // of the lane worker (not an OS-thread block). Fold the K child
                 // endpoints under BUSY_SUM_MASK -- masked add is associative, so the
                 // result is independent of completion order.
                 std::vector<hpx::future<std::int64_t>> done =
                     hpx::when_all(futs).get();
                 std::uint64_t acc = 0;
                 for (auto& f : done)
                     acc = (acc + static_cast<std::uint64_t>(f.get()))
                           & BUSY_SUM_MASK;
                 OpOutcome o;
                 o.value = static_cast<std::int64_t>(acc);  // -> OpValue (int64)
                 o.has_value = true;
                 o.status = "completed";
                 return o;
             },
             // Queued-cancelable only (launch-all: no honest running boundary once the
             // children are launched). Always 1 (ignores args -> no tag read).
             one_checkpoint,
             // Typed signature: chain_fanout(int64 seed, int64 count, int64 steps,
             // int64 quantum) -> int64.
             {OpType::Int64, OpType::Int64, OpType::Int64, OpType::Int64},
             OpType::Int64}},
        // barrier_fanin(seed, leaves, quantum) -> int64: exp44 "witnessed barrier-gated
        // fan-in". The KEYSTONE integration of exp39 (boundary cost), exp40 (intra-op
        // overlap) and exp41 (cooperative interleaving) BEHIND ONE Python/Runtime crossing.
        // It launches `leaves` INDEPENDENT bare-hpx::async children ON THE DEFAULT POOL (no
        // custom executor/pool -- so --hpx:threads=1 constrains them to ONE OS worker), each
        // computing chain_stage(seed+j, quantum); the children then mutually RENDEZVOUS on a
        // shared cooperative hpx::promise<void> gate (the last arriver opens it), join with
        // when_all, and reduce with a scheduled .then(launch::async). The body runs inside
        // the lane's hpx::async(exec_, task).get(), so every interior get() (each gate.get()
        // and the terminal reduction get()) is a COOPERATIVE suspension of the lane worker,
        // never an OS-thread block. The single load-bearing fact: at --hpx:threads=1 with one
        // observed OS worker, a clean completion REQUIRES cooperative suspend/resume of the
        // gated leaves on that one worker -- a non-cooperative interior would pin the worker
        // on the first gate wait and DEADLOCK. The value is value-neutral of the gate, so it
        // equals chain_fanout(seed, leaves, 1, quantum) (Python-checkable). DIAGNOSTIC
        // SIDE-EFFECT: unlike every other registry op, this one writes the mutex-guarded
        // BarrierFaninWitness (debug-only structural evidence). It does NOT touch OpOutcome,
        // RuntimeResult, lane_stats(), or the v1 JSONL schema. Queued-cancelable only
        // (one_checkpoint): the gate self-opens at full arrival so the op is always short;
        // there is no running-cancel boundary and none is wired. Synthetic barrier/all-reduce
        // shape, NOT general DAG scheduling: NO speedup/throughput/latency/parallelism claim.
        {"barrier_fanin", OpEntry{3,
             [](const OpArgs& a, const StopCheckpoint&) -> OpOutcome {
                 const std::int64_t seed = as_int64(a, 0, "barrier_fanin");
                 const std::int64_t leaves = as_int64(a, 1, "barrier_fanin");
                 const std::int64_t quantum = as_int64(a, 2, "barrier_fanin");
                 // Defensive native guard (public boundary already rejects these; protects
                 // the private/native bypass). Throw -> status="failed" row, never a crash.
                 if (leaves < 1 || leaves > FANIN_LEAVES_MAX ||
                     quantum < 0 || quantum > CHAIN_QUANTUM_MAX) {
                     throw std::invalid_argument(
                         "barrier_fanin requires 1 <= leaves <= "
                         + std::to_string(FANIN_LEAVES_MAX)
                         + " and 0 <= quantum <= "
                         + std::to_string(CHAIN_QUANTUM_MAX));
                 }
                 const int n = static_cast<int>(leaves);

                 // Per-call cooperative gate + structural bookkeeping (fresh each call,
                 // exp41 posture: a LOCAL hpx::promise, no hpx::latch/barrier).
                 hpx::promise<void> gate_prom;
                 hpx::shared_future<void> gate = gate_prom.get_future().share();
                 std::atomic<int> arrived{0};
                 std::atomic<int> released{0};
                 std::atomic<int> in_suspend{0};
                 std::atomic<int> max_in_suspend{0};
                 std::atomic<int> ordering_violations{0};
                 std::atomic<bool> opened{false};
                 std::atomic<int> opener{0};  // 0 none, 1 last_arriver, 2 watchdog
                 std::atomic<bool> reduction_saw_all{false};

                 // Idempotent single-set gate open (CAS winner records the opener).
                 auto open_gate = [&](int who) {
                     bool expect = false;
                     if (opened.compare_exchange_strong(expect, true)) {
                         opener.store(who);
                         gate_prom.set_value();
                     }
                 };
                 auto bump_max = [&](int v) {
                     int cur = max_in_suspend.load();
                     while (v > cur &&
                            !max_in_suspend.compare_exchange_weak(cur, v)) { /*retry*/ }
                 };

                 // Leaf j: compute -> arrive -> (last opens) -> cooperative suspend on the
                 // gate -> release -> return its value. The forced ordering makes the gate
                 // a genuine rendezvous: no leaf returns until every leaf has arrived.
                 auto leaf = [&](int j) -> std::int64_t {
                     const std::int64_t seed_j = static_cast<std::int64_t>(
                         static_cast<std::uint64_t>(seed)
                         + static_cast<std::uint64_t>(j));
                     const std::int64_t v = chain_stage(seed_j, quantum);
                     const int got = arrived.fetch_add(1) + 1;
                     if (got == n) open_gate(1);  // last arriver opens (success path)
                     const int cur = in_suspend.fetch_add(1) + 1;
                     bump_max(cur);
                     gate.get();  // cooperative suspend until the gate opens
                     in_suspend.fetch_sub(1);
                     if (!opened.load()) ordering_violations.fetch_add(1);  // defensive
                     released.fetch_add(1);
                     return v;
                 };

                 // Launch the leaves with BARE hpx::async on the DEFAULT pool (the
                 // load-bearing requirement: --hpx:threads sizes this pool).
                 std::vector<hpx::future<std::int64_t>> futs;
                 futs.reserve(static_cast<std::size_t>(n));
                 for (int j = 0; j < n; ++j)
                     futs.push_back(hpx::async(leaf, j));

                 // Cooperative watchdog (defense-in-depth ONLY): poll arrival with a
                 // cooperative sleep_for (yields the worker so leaves run, critical at
                 // threads=1) and open the gate ONLY if the generous deadline is hit. On a
                 // healthy run the LAST ARRIVER opens first and this never fires; a watchdog
                 // open on a success run is a structural FAILURE upstream.
                 const auto deadline = std::chrono::steady_clock::now()
                     + std::chrono::milliseconds(BARRIER_FANIN_WATCHDOG_MS);
                 while (arrived.load() < n) {
                     if (std::chrono::steady_clock::now() >= deadline) {
                         open_gate(2);  // watchdog rescue (never on the success path)
                         break;
                     }
                     hpx::this_thread::sleep_for(
                         std::chrono::milliseconds(BARRIER_FANIN_POLL_MS));
                 }

                 // Dependent reduction as a SCHEDULED continuation: runs only after every
                 // leaf future is ready (released). Folds the K leaf values under
                 // BUSY_SUM_MASK (associative -> order-independent).
                 hpx::future<std::int64_t> result =
                     hpx::when_all(futs).then(hpx::launch::async,
                         [&reduction_saw_all, &released, &ordering_violations, n]
                         (hpx::future<std::vector<hpx::future<std::int64_t>>> done_f)
                             -> std::int64_t {
                             std::vector<hpx::future<std::int64_t>> done = done_f.get();
                             const bool saw_all = (released.load() == n);
                             reduction_saw_all.store(saw_all);
                             if (!saw_all) ordering_violations.fetch_add(1);
                             std::uint64_t acc = 0;
                             for (auto& f : done)
                                 acc = (acc + static_cast<std::uint64_t>(f.get()))
                                       & BUSY_SUM_MASK;
                             return static_cast<std::int64_t>(acc);
                         });
                 const std::int64_t value = result.get();  // terminal cooperative suspend

                 // Record the structural witness (the ONLY side effect; single guarded
                 // write site). joined_count == released on the success path (no faults).
                 BarrierFaninWitness w;
                 w.observed_os_workers =
                     static_cast<int>(hpx::get_os_thread_count());
                 w.leaves_requested = n;
                 w.arrived_count = arrived.load();
                 w.released_count = released.load();
                 const int who = opener.load();
                 w.opener = (who == 1 ? "last_arriver"
                                      : (who == 2 ? "watchdog" : "none"));
                 w.reduction_after_all_leaves = reduction_saw_all.load();
                 w.ordering_violations = ordering_violations.load();
                 w.joined_count = released.load();
                 w.watchdog_opened = (who == 2);
                 w.clean_exit = (w.joined_count == n);
                 w.max_simultaneously_suspended_leaves = max_in_suspend.load();
                 record_barrier_fanin_witness(w);

                 OpOutcome o;
                 o.value = value;  // -> OpValue (int64)
                 o.has_value = true;
                 o.status = "completed";
                 return o;
             },
             // Queued-cancelable only (launch-all-style: the gate self-opens at full
             // arrival so the op is short; no running-cancel boundary). Always 1.
             one_checkpoint,
             // Typed signature: barrier_fanin(int64 seed, int64 leaves, int64 quantum)
             // -> int64.
             {OpType::Int64, OpType::Int64, OpType::Int64}, OpType::Int64}},
    };
    return r;
}

}  // namespace rayx_runtime

#endif  // RAYX_RUNTIME_OPS_HPX_HPP
