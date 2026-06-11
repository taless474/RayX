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

#include <hpx/hpx.hpp>  // hpx::async, hpx::when_all, hpx::future, hpx::this_thread::sleep_for

#include <chrono>     // std::chrono::milliseconds (park_ms chunk)
#include <cstdint>
#include <stdexcept>  // std::invalid_argument (defensive native arg guard)
#include <string>
#include <unordered_map>
#include <vector>

namespace rayx_runtime {

// The HPX-side registry. Two entries: the internally-composed fanout_sum and the
// cooperative parked park_ms. First:
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
    };
    return r;
}

}  // namespace rayx_runtime

#endif  // RAYX_RUNTIME_OPS_HPX_HPP
