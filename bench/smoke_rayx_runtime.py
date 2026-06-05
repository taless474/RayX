#!/usr/bin/env python3
"""Slice 2c contract smoke for the experimental `rayx.runtime` prototype.

Exercises the Phase 1 registered-native-operation VALUE PATH, the HPX-native FIFO
lanes, and the Slice 2a cooperative cancellation core over a built `_rayx`
extension: `Runtime(num_lanes=...)` + the fixed registry (square / add / boom /
busy_sum) + `RuntimeFuture` (+ `cancel()` / `cancelled()`) + `OperationResult` + the
error classes + the shared process-runtime guard + per-lane round-robin dispatch via
`hpx::async(...).get()` + `num_lanes()` / `lane_stats()`. Asserts the *contract*
(value/row separation, row key set, actor_id prefix, error/cancel mapping,
consume-once, single-runtime exclusion, round-robin spread, lane_stats shape,
per-lane stable rt-hpx- ids, busy_sum value, checkpoint-count-armed queued + running
cancel, cancel-on-shutdown, lane_stats under load) -- not timing values (the cancel
tests gate on lane_stats `active` / `queue_depth` rather than sleeping). Mirrors
bench/smoke_rayx.py: a PASS line per section, exit 0 on success, exit 1 with a
reason on the first failed assertion.

Slice 2c adds the non-consuming collection APIs on top of the 2a cancellation core
and 2b bounded admission: `Runtime.get` (collect in input order, no fail-fast),
`Runtime.wait` (blocking `hpx::wait_some` / `timeout=0` poll, with strict
`num_returns` / entry-type / `timeout` validation shared by both modes), and
`Runtime.as_completed` (yields the input `RuntimeFuture` handles, each once). It emits
no JSONL and runs no benchmark matrix. Requires the rayx extension to be BUILT
(python/src/rayx/_rayx*.so); it adds python/src to sys.path automatically.

Usage:
    python bench/smoke_rayx_runtime.py
Exit code 0 == pass, 1 == fail.
"""
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

import rayx
from rayx import Engine
from rayx.runtime import (
    Runtime,
    RuntimeFuture,
    OperationResult,
    RuntimeOperationError,
    OperationFailedError,
    OperationCancelledError,
    QueueFullError,
)

# The exact runtime row key set (Slice 0). See design doc §9: core
# measurement-row fields only -- a strict subset of the harness row keys.
RUNTIME_ROW_FIELDS = {
    "actor_id",
    "submit_ns",
    "start_ns",
    "end_ns",
    "total_ms",
    "queue_wait_ms",
    "service_ms_observed",
    "status",
    "error",
}
# Harness-facade echoes that must NOT appear in a runtime row.
HARNESS_ECHO_FIELDS = ("label", "chunks", "chunk_delay_ms", "chunks_completed")
HEX = set("0123456789abcdef")


def _fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def _expect(exc_type, fn, where):
    try:
        fn()
    except exc_type:
        return
    except Exception as e:  # noqa: BLE001
        _fail(f"{where}: expected {exc_type.__name__}, got "
              f"{type(e).__name__}: {e}")
    _fail(f"{where}: expected {exc_type.__name__}, none raised")


def _wait_until(pred, timeout_s=5.0, where="condition"):
    """Gate on a lane_stats state (active / queue_depth) instead of sleeping.

    Polls ``pred`` until true or the timeout elapses (then fails). Used to make the
    cancellation tests deterministic without wall-clock guesses: wait for a
    busy_sum to be ``active`` (running) or a sibling to be ``queue_depth >= 1``
    before exercising cancel.
    """
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        if pred():
            return
        time.sleep(0.001)
    _fail(f"timed out after {timeout_s}s waiting for {where}")


def _check_runtime_row(row, where):
    if set(row) != RUNTIME_ROW_FIELDS:
        _fail(f"{where}: row keys {sorted(row)} != "
              f"{sorted(RUNTIME_ROW_FIELDS)}")
    if "value" in row:
        _fail(f"{where}: row must not contain a 'value' key")
    for echo in HARNESS_ECHO_FIELDS:
        if echo in row:
            _fail(f"{where}: row must not contain harness echo field {echo!r}")
    aid = row["actor_id"]
    if not aid.startswith("rt-hpx-"):
        _fail(f"{where}: actor_id {aid!r} must start with 'rt-hpx-'")
    if aid.startswith("act-"):
        _fail(f"{where}: actor_id {aid!r} must NOT start with 'act-'")
    suffix = aid[len("rt-hpx-"):]
    if len(suffix) != 8 or any(c not in HEX for c in suffix):
        _fail(f"{where}: actor_id suffix {suffix!r} must be 8 lowercase hex")


def section_value_path_and_contract():
    with Runtime(hpx_threads=2) as rt:
        # --- square(2) -> 4 ---
        res = rt.submit_operation("square", 2).result()
        if not isinstance(res, OperationResult):
            _fail("square: result() must return an OperationResult")
        if res.value != 4:
            _fail(f"square(2).value == {res.value!r}, expected 4")
        if res.row["status"] != "completed":
            _fail(f"square: status {res.row['status']!r}, expected 'completed'")
        if res.row["error"] is not None:
            _fail(f"square: error {res.row['error']!r}, expected None")
        if not (res.row["start_ns"] <= res.row["end_ns"]):
            _fail("square: start_ns must be <= end_ns")
        if res.row["service_ms_observed"] < 0.0 or res.row["total_ms"] < 0.0:
            _fail("square: service_ms_observed/total_ms must be >= 0")
        _check_runtime_row(res.row, "square")
        sample_aid = res.row["actor_id"]

        # --- add(2, 3) -> 5 ---
        res_add = rt.submit_operation("add", 2, 3).result()
        if res_add.value != 5:
            _fail(f"add(2, 3).value == {res_add.value!r}, expected 5")
        _check_runtime_row(res_add.row, "add")

        # --- value / row separation + re-readability (NOT consume-once) ---
        res_sq = rt.submit_operation("square", 5).result()
        if res_sq.value != 25 or res_sq.value != 25:
            _fail("square(5).value must be 25 and idempotently re-readable")
        if res_sq.row is not res_sq.row:
            _fail("OperationResult.row must be re-readable (stable)")
        if "value" in res_sq.row:
            _fail("value must be separate from row (no 'value' key)")

        # --- failure path: boom -> failed result, .row safe, .value raises ---
        res_boom = rt.submit_operation("boom").result()
        if not isinstance(res_boom, OperationResult):
            _fail("boom: result() must return an OperationResult, not raise")
        if res_boom.row["status"] != "failed":
            _fail(f"boom: status {res_boom.row['status']!r}, expected 'failed'")
        if not res_boom.row["error"]:
            _fail("boom: failed row must carry a populated 'error'")
        _check_runtime_row(res_boom.row, "boom")
        # .row is inspectable AND idempotently re-readable for a failed result
        # (stable identity), even though .value re-raises.
        if res_boom.row is not res_boom.row:
            _fail("boom: failed .row must be re-readable (stable identity)")
        _expect(OperationFailedError, lambda: res_boom.value,
                "boom .value (first read)")
        _expect(OperationFailedError, lambda: res_boom.value,
                "boom .value (idempotent re-read)")

        # --- ready(): bool before retire, raises after retire ---
        f = rt.submit_operation("square", 3)
        r0 = f.ready()
        if not isinstance(r0, bool):
            _fail(f"ready() before retire must return bool, got {type(r0).__name__}")
        # --- consume-once: second result() raises (structural misuse) ---
        f.result()
        _expect(RuntimeError, f.result, "consume-once second result()")
        # ready() after the future is retired raises (mirrors result()'s guard).
        _expect(RuntimeError, f.ready, "ready() after retire")

        # --- boundary validation (rejected before any RuntimeFuture) ---
        _expect(TypeError, lambda: rt.submit_operation(123),
                "non-string op id")
        _expect(ValueError, lambda: rt.submit_operation("nope", 2),
                "unknown operation id")
        _expect(ValueError, lambda: rt.submit_operation("add", 2),
                "wrong arity (too few)")
        _expect(ValueError, lambda: rt.submit_operation("add", 2, 3, 4),
                "wrong arity (too many)")
        _expect(ValueError, lambda: rt.submit_operation("square"),
                "wrong arity (zero)")
        _expect(TypeError, lambda: rt.submit_operation("square", "x"),
                "non-int arg (str)")
        _expect(TypeError, lambda: rt.submit_operation("square", 2.5),
                "non-int arg (float)")
        _expect(TypeError, lambda: rt.submit_operation("square", True),
                "non-int arg (bool)")

        # --- single-runtime guard (while this Runtime is live) ---
        _expect(RuntimeError, lambda: Runtime(),
                "second active Runtime")
        _expect(RuntimeError, lambda: Engine(),
                "Engine while Runtime is live")

        print(f"PASS: value path + contract (sample actor_id={sample_aid})")


def section_num_lanes_api():
    # default num_lanes == 1; explicit honored; invalid rejected at the Python
    # boundary (before any process guard is claimed, so these do not leak the
    # single-runtime singleton).
    with Runtime() as rt:
        if rt.num_lanes() != 1:
            _fail(f"default Runtime().num_lanes() == {rt.num_lanes()}, expected 1")
    with Runtime(num_lanes=3) as rt3:
        if rt3.num_lanes() != 3:
            _fail(f"Runtime(num_lanes=3).num_lanes() == {rt3.num_lanes()}, "
                  "expected 3")
    _expect(ValueError, lambda: Runtime(num_lanes=0), "num_lanes=0")
    _expect(ValueError, lambda: Runtime(num_lanes=-1), "num_lanes=-1")
    _expect(TypeError, lambda: Runtime(num_lanes=True), "num_lanes=bool")
    _expect(TypeError, lambda: Runtime(num_lanes=1.0), "num_lanes=float")
    _expect(TypeError, lambda: Runtime(num_lanes="2"), "num_lanes=str")
    # Post-shutdown guards: num_lanes() / lane_stats() raise after shutdown
    # (consistent with submit_operation's post-shutdown guard).
    rt_closed = Runtime(num_lanes=2)
    if rt_closed.num_lanes() != 2:
        _fail("num_lanes() must be 2 before shutdown")
    _ = rt_closed.lane_stats()  # callable while live
    rt_closed.shutdown()
    _expect(RuntimeError, rt_closed.num_lanes, "num_lanes() after shutdown")
    _expect(RuntimeError, rt_closed.lane_stats, "lane_stats() after shutdown")
    print("PASS: num_lanes API (default 1; explicit 3; invalid rejected; "
          "num_lanes()/lane_stats() raise after shutdown)")


def _check_lane_stat(s, where):
    if set(s) != {"actor_id", "queue_depth", "active"}:
        _fail(f"{where}: lane_stats keys {sorted(s)} != "
              "['active', 'actor_id', 'queue_depth']")
    if isinstance(s["queue_depth"], bool) or not isinstance(s["queue_depth"], int):
        _fail(f"{where}: queue_depth must be int, got "
              f"{type(s['queue_depth']).__name__}")
    if not isinstance(s["active"], bool):
        _fail(f"{where}: active must be bool, got {type(s['active']).__name__}")
    aid = s["actor_id"]
    if not aid.startswith("rt-hpx-"):
        _fail(f"{where}: lane actor_id {aid!r} must start with 'rt-hpx-'")
    if aid.startswith("act-"):
        _fail(f"{where}: lane actor_id {aid!r} must NOT start with 'act-'")
    suffix = aid[len("rt-hpx-"):]
    if len(suffix) != 8 or any(c not in HEX for c in suffix):
        _fail(f"{where}: lane actor_id suffix {suffix!r} must be 8 lowercase hex")


def section_round_robin_and_stats():
    N = 3
    with Runtime(num_lanes=N) as rt:
        if rt.num_lanes() != N:
            _fail(f"num_lanes() == {rt.num_lanes()}, expected {N}")

        # Fresh runtime: lane_stats has one well-typed row per lane, all idle, with
        # distinct rt-hpx- lane ids.
        stats0 = rt.lane_stats()
        if len(stats0) != N:
            _fail(f"lane_stats len {len(stats0)} != num_lanes {N}")
        lane_ids = []
        for i, s in enumerate(stats0):
            _check_lane_stat(s, f"fresh lane {i}")
            if s["queue_depth"] != 0 or s["active"] is not False:
                _fail(f"fresh lane {i} must be idle (queue_depth 0, active False)")
            lane_ids.append(s["actor_id"])
        if len(set(lane_ids)) != N:
            _fail(f"lane actor_ids must be distinct per lane: {lane_ids}")

        # Lane ids stable across snapshots.
        if [s["actor_id"] for s in rt.lane_stats()] != lane_ids:
            _fail("lane actor_ids must be stable across lane_stats snapshots")

        # ready() on a multi-lane future: bool before retire, raises after retire
        # (mirrors result()'s guard; same contract as the single-lane Slice 0 case).
        rf = rt.submit_operation("square", 4)
        r_before = rf.ready()
        if not isinstance(r_before, bool):
            _fail(f"multi-lane ready() before retire must be bool, got "
                  f"{type(r_before).__name__}")
        res_ready = rf.result()
        if res_ready.value != 16:
            _fail(f"multi-lane ready-case square(4).value == {res_ready.value!r}, "
                  "expected 16")
        _expect(RuntimeError, rf.ready, "multi-lane ready() after retire")

        # Round-robin spread over 2N submissions: submission i -> lane (i % N), so
        # exactly N distinct actor_ids and row[i].actor_id == row[i+N].actor_id.
        futs = [rt.submit_operation("square", i) for i in range(2 * N)]
        results = [f.result() for f in futs]
        seen = [res.row["actor_id"] for res in results]
        if len(set(seen)) != N:
            _fail(f"round-robin must touch exactly {N} lanes, saw {set(seen)}")
        for i in range(N):
            if seen[i] != seen[i + N]:
                _fail(f"round-robin: actor at {i} ({seen[i]!r}) != at {i + N} "
                      f"({seen[i + N]!r})")
        if set(seen) != set(lane_ids):
            _fail(f"round-robin actor_ids {set(seen)} != lane_stats ids "
                  f"{set(lane_ids)}")

        # The exact 9-field runtime row contract holds with num_lanes > 1: run the
        # same checker used on the single-lane Slice 0 path against a multi-lane
        # result (one completed row is enough -- the row builder is lane-agnostic).
        _check_runtime_row(results[0].row, f"multi-lane row (num_lanes={N})")

        # Values still correct across lanes; failure mapping preserved.
        if rt.submit_operation("square", 6).result().value != 36:
            _fail("multi-lane square(6) != 36")
        if rt.submit_operation("add", 2, 3).result().value != 5:
            _fail("multi-lane add(2, 3) != 5")
        res_boom = rt.submit_operation("boom").result()
        if res_boom.row["status"] != "failed":
            _fail("multi-lane boom must be 'failed'")
        _expect(OperationFailedError, lambda: res_boom.value,
                "multi-lane boom .value")

        # Drained (all futures retired): every lane idle again.
        for i, s in enumerate(rt.lane_stats()):
            if s["queue_depth"] != 0 or s["active"] is not False:
                _fail(f"post-drain lane {i} must be idle "
                      f"(queue_depth {s['queue_depth']}, active {s['active']})")
    print(f"PASS: round-robin spread + lane_stats (num_lanes={N}; distinct "
          "stable rt-hpx- lane ids; multi-lane ready()/9-field row; idle "
          "before/after drain)")


def section_single_lane_batch():
    # Cheap STRUCTURAL coverage on one lane: submit a sequence of operations and
    # retire them in submission order, asserting each value. This is NOT a FIFO
    # proof -- with sub-millisecond ops there is no observable service-order
    # signal; true FIFO-under-load needs a controllable slow op (busy_sum, Slice 2).
    with Runtime(num_lanes=1) as rt:
        seq = []
        for i in range(8):
            seq.append((rt.submit_operation("square", i), i * i))
            seq.append((rt.submit_operation("add", i, 1), i + 1))
        for fut, expected in seq:
            got = fut.result().value
            if got != expected:
                _fail(f"single-lane batch: got {got!r}, expected {expected!r}")
    print("PASS: single-lane structural batch (16 ops retired in order; "
          "values correct -- not a FIFO-under-load proof)")


def section_busy_sum_value():
    with Runtime(num_lanes=1) as rt:
        for N in (0, 1, 1000, 20000, 100000):
            exp = (N * (N - 1) // 2) % (2 ** 31)
            res = rt.submit_operation("busy_sum", N).result()
            if res.row["status"] != "completed":
                _fail(f"busy_sum({N}) status {res.row['status']!r}, expected completed")
            if res.value != exp:
                _fail(f"busy_sum({N}).value == {res.value!r}, expected {exp}")
            _check_runtime_row(res.row, f"busy_sum({N})")
        # n < 0 rejected at the Python boundary (before any RuntimeFuture).
        _expect(ValueError, lambda: rt.submit_operation("busy_sum", -1),
                "busy_sum(-1)")
        _expect(ValueError, lambda: rt.submit_operation("busy_sum", -1000000),
                "busy_sum(negative)")
        # bool still rejected as a non-int (int subclass).
        _expect(TypeError, lambda: rt.submit_operation("busy_sum", True),
                "busy_sum(bool)")
    print("PASS: busy_sum value ((n*(n-1)/2) mod 2^31; n>=0 boundary; 9-field row)")


def section_short_busy_sum_cancel_invariant():
    # A checkpoint_count == 1 op (busy_sum with n <= STRIDE=8192) is NOT
    # running-cancellable. The blocker bug would surface as cancel()==True followed
    # by status=="completed"; assert that invariant over a repeat loop (cancel
    # races the instant run), then a deterministic post-completion check.
    N1 = 1000  # < STRIDE -> checkpoint_count == 1
    exp1 = (N1 * (N1 - 1) // 2) % (2 ** 31)
    with Runtime(num_lanes=1) as rt:
        for _ in range(64):
            f = rt.submit_operation("busy_sum", N1)
            settled = f.cancel()
            res = f.result()
            st = res.row["status"]
            if settled != (st == "cancelled"):
                _fail(f"short busy_sum invariant violated: cancel()={settled} "
                      f"but status={st!r} (must never be True+completed)")
            if st == "completed":
                if res.value != exp1:
                    _fail(f"short busy_sum completed value {res.value!r} != {exp1}")
            elif st == "cancelled":
                _expect(OperationCancelledError, lambda r=res: r.value,
                        "short busy_sum cancelled .value")
            else:
                _fail(f"short busy_sum unexpected status {st!r}")
        # Deterministic: a completed count==1 op cannot be cancelled afterward.
        g = rt.submit_operation("busy_sum", N1)
        rg = g.result()
        if rg.row["status"] != "completed" or rg.value != exp1:
            _fail("short busy_sum should complete with the correct value")
        if g.cancel() is not False:
            _fail("cancel() after completion must return False")
        if g.cancelled() is not False:
            _fail("cancelled() must be False for a completed op")
    print("PASS: short busy_sum (checkpoint_count==1) never cancels-then-completes; "
          "post-completion cancel returns False")


def section_running_cancel():
    with Runtime(num_lanes=1) as rt:
        f = rt.submit_operation("busy_sum", 100_000_000)  # count >> 1; runs a while
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "busy_sum active")
        if f.cancel() is not True:
            _fail("running cancel of a long busy_sum must return True")
        res = f.result()
        if res.row["status"] != "cancelled":
            _fail(f"running-cancelled busy_sum status {res.row['status']!r}, "
                  "expected 'cancelled'")
        _expect(OperationCancelledError, lambda: res.value, "running-cancel .value")
        if res.row["error"] is not None:
            _fail("cancelled row error should be None")
        if f.cancelled() is not True:
            _fail("cancelled() must be True after a running cancel")
    print("PASS: running cancel on long busy_sum (cancel True; status cancelled; "
          ".value raises OperationCancelledError)")


def section_queued_cancel():
    with Runtime(num_lanes=1) as rt:
        hold = rt.submit_operation("busy_sum", 100_000_000)  # holds lane 0
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        q = rt.submit_operation("square", 7)  # queued behind the holder
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 1, 5.0,
                    "square queued")
        if q.cancel() is not True:
            _fail("queued cancel of the square must return True")
        res = q.result()
        if res.row["status"] != "cancelled":
            _fail(f"queued-cancelled square status {res.row['status']!r}, "
                  "expected 'cancelled'")
        _expect(OperationCancelledError, lambda: res.value, "queued-cancel .value")
        hold.cancel()  # release the lane so context exit is instant
    print("PASS: queued cancel (square behind busy_sum; cancel True; status "
          "cancelled; .value raises)")


def section_shutdown_cancels_running():
    rt = Runtime(num_lanes=1)
    f = rt.submit_operation("busy_sum", 2_000_000_000)  # would run seconds in full
    _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "busy_sum active")
    t0 = time.perf_counter()
    rt.shutdown()
    dt = time.perf_counter() - t0
    if dt >= 1.0:
        _fail(f"shutdown took {dt:.3f}s; expected prompt cancel (~1 checkpoint "
              "stride), not a full drain")
    if f.cancelled() is not True:
        _fail("shutdown must cancel the in-flight op (cancelled() True)")
    res = f.result()  # promise was fulfilled (cancelled) during the shutdown drain
    if res.row["status"] != "cancelled":
        _fail(f"shutdown-cancelled op status {res.row['status']!r}, "
              "expected 'cancelled'")
    print(f"PASS: shutdown cancels a running busy_sum and exits promptly "
          f"({dt * 1000:.0f}ms)")


def section_lane_stats_under_load():
    with Runtime(num_lanes=1) as rt:
        s0 = rt.lane_stats()[0]
        if s0["active"] is not False or s0["queue_depth"] != 0:
            _fail("fresh lane must be idle (active False, queue_depth 0)")
        hold = rt.submit_operation("busy_sum", 100_000_000)
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        if rt.lane_stats()[0]["active"] is not True:
            _fail("lane must report active=True while busy_sum runs")
        rt.submit_operation("square", 3)  # queued sibling
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 1, 5.0,
                    "sibling queued")
        hold.cancel()  # stop the holder so the lane drains the (queued) sibling
        _wait_until(lambda: (rt.lane_stats()[0]["active"] is False
                             and rt.lane_stats()[0]["queue_depth"] == 0),
                    5.0, "idle after drain")
    print("PASS: lane_stats under load (active True during busy_sum; queue_depth>=1 "
          "with a queued sibling; returns to idle after drain)")


def section_cancelled_semantics():
    with Runtime(num_lanes=1) as rt:
        # Completed op: cancel() -> False; cancelled() -> False before & after retire.
        f = rt.submit_operation("add", 2, 3)
        if f.cancelled() is not False:
            _fail("cancelled() must be False before a settled cancel")
        r = f.result()
        if r.value != 5:
            _fail("add(2, 3) should be 5")
        if f.cancel() is not False:
            _fail("cancel() after completion must return False")
        if f.cancelled() is not False:
            _fail("cancelled() must be False for a completed op (after retire)")
        # Cancelled queued op: cancelled() valid both before and after retire.
        hold = rt.submit_operation("busy_sum", 100_000_000)
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        q = rt.submit_operation("square", 9)
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 1, 5.0, "queued")
        if q.cancel() is not True:
            _fail("queued cancel must return True")
        if q.cancelled() is not True:
            _fail("cancelled() must be True before retire after a settled cancel")
        rq = q.result()
        if q.cancelled() is not True:
            _fail("cancelled() must remain True after retire")
        if rq.row["status"] != "cancelled":
            _fail("queued-cancelled status must be 'cancelled'")
        hold.cancel()
    print("PASS: cancel()/cancelled() semantics (completed->False; cancelled->True "
          "before & after retire)")


def section_failed_terminal_cancel():
    # 2a hardening: a FAILED result is terminal -- a later cancel() must return
    # False (not claim to cancel an already-fulfilled future) and cancelled() stays
    # False, while .row stays readable and .value raises OperationFailedError.
    with Runtime(num_lanes=1) as rt:
        f = rt.submit_operation("boom")
        res = f.result()
        if res.row["status"] != "failed":
            _fail(f"boom status {res.row['status']!r}, expected 'failed'")
        if not res.row["error"]:
            _fail("failed row must carry a populated 'error'")
        if res.row is not res.row:
            _fail("failed .row must be stably readable")
        _expect(OperationFailedError, lambda: res.value, "failed .value")
        if f.cancel() is not False:
            _fail("cancel() after a failed (terminal) result must return False")
        if f.cancelled() is not False:
            _fail("cancelled() must be False for a failed result")
    print("PASS: failed result is terminal (boom failed; cancel()->False; "
          "cancelled()->False; .row readable; .value raises OperationFailedError)")


def section_admission_validation():
    with Runtime(num_lanes=1, max_queue_depth_per_lane=None) as rt:
        if rt.submit_operation("square", 2).result().value != 4:
            _fail("unbounded (None) runtime should admit and run")
    with Runtime(num_lanes=1, max_queue_depth_per_lane=4) as rt:
        if rt.submit_operation("add", 2, 3).result().value != 5:
            _fail("capped runtime should admit under the cap")
    # Invalid caps rejected at the Python boundary (before any process guard).
    _expect(ValueError, lambda: Runtime(max_queue_depth_per_lane=0), "cap=0")
    _expect(ValueError, lambda: Runtime(max_queue_depth_per_lane=-1), "cap=-1")
    _expect(TypeError, lambda: Runtime(max_queue_depth_per_lane=True), "cap=bool")
    _expect(TypeError, lambda: Runtime(max_queue_depth_per_lane=2.0), "cap=float")
    _expect(TypeError, lambda: Runtime(max_queue_depth_per_lane="2"), "cap=str")
    print("PASS: admission validation (None/positive ok; 0/neg ValueError; "
          "bool/float/str TypeError)")


def section_admission_full_reject():
    with Runtime(num_lanes=1, max_queue_depth_per_lane=2) as rt:
        hold = rt.submit_operation("busy_sum", 100_000_000)  # occupies service slot
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        a = rt.submit_operation("square", 1)  # queued (depth 1)
        b = rt.submit_operation("square", 2)  # queued (depth 2 == cap)
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] == 2, 5.0, "queue full")
        depth_before = rt.lane_stats()[0]["queue_depth"]
        # Next submit must be rejected: a full lane raises QueueFullError, and the
        # rejected call creates no future (the _expect confirms it raised, so nothing
        # was returned) and does not change queue_depth.
        _expect(QueueFullError, lambda: rt.submit_operation("square", 3),
                "submit when lane full")
        depth_after = rt.lane_stats()[0]["queue_depth"]
        if depth_after != depth_before:
            _fail(f"rejected submit changed queue_depth {depth_before}->{depth_after}")
        a.cancel()
        b.cancel()
        hold.cancel()
    print("PASS: full-lane rejection (cap=2; third submit raises QueueFullError; "
          "queue_depth unchanged; no future created)")


def section_admission_drain_reopens():
    with Runtime(num_lanes=1, max_queue_depth_per_lane=1) as rt:
        hold = rt.submit_operation("busy_sum", 100_000_000)
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        a = rt.submit_operation("square", 1)  # queued (depth 1 == cap)
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] == 1, 5.0, "queue full")
        _expect(QueueFullError, lambda: rt.submit_operation("square", 2),
                "full before drain")
        # Drain: cancel the holder + the queued op -> lane returns to idle and
        # admission reopens.
        a.cancel()
        hold.cancel()
        _wait_until(lambda: (rt.lane_stats()[0]["active"] is False
                             and rt.lane_stats()[0]["queue_depth"] == 0),
                    5.0, "idle after drain")
        if rt.submit_operation("square", 5).result().value != 25:
            _fail("admission should reopen after the lane drains")
    print("PASS: drain reopens admission (full -> QueueFullError; after drain a "
          "submit succeeds)")


def section_admission_per_lane():
    # Per-lane cap, not global. num_lanes=2, cap=1; round-robin maps submission i to
    # lane (i % 2) and rr_ advances on every submit (admitted or rejected).
    with Runtime(num_lanes=2, max_queue_depth_per_lane=1) as rt:
        h0 = rt.submit_operation("busy_sum", 100_000_000)  # rr0 -> lane 0 (running)
        h1 = rt.submit_operation("busy_sum", 100_000_000)  # rr1 -> lane 1 (running)
        _wait_until(lambda: all(s["active"] for s in rt.lane_stats()), 5.0,
                    "both lanes active")
        q0 = rt.submit_operation("square", 1)  # rr2 -> lane 0 (queued, depth 1==cap)
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] == 1, 5.0,
                    "lane 0 queued")
        q1 = rt.submit_operation("square", 2)  # rr3 -> lane 1 (queued) -- must admit
        _wait_until(lambda: rt.lane_stats()[1]["queue_depth"] == 1, 5.0,
                    "lane 1 queued (per-lane cap: lane 1 not full)")
        _expect(QueueFullError, lambda: rt.submit_operation("square", 3),
                "rr4 -> lane 0 full")
        _expect(QueueFullError, lambda: rt.submit_operation("square", 4),
                "rr5 -> lane 1 full")
        for f in (q0, q1, h0, h1):
            f.cancel()
    print("PASS: per-lane admission (num_lanes=2 cap=1; filling lane 0 does not "
          "reject lane 1; each lane caps independently)")


def section_admission_cancelled_still_counts():
    # A cancelled-but-not-yet-popped queued item still occupies its admission slot
    # (queue_depth) until the worker pops/skips it. Deterministic here because the
    # holder busy_sum (gated running) keeps the worker busy, so it cannot pop the
    # cancelled item -- the depth provably stays put.
    with Runtime(num_lanes=1, max_queue_depth_per_lane=1) as rt:
        hold = rt.submit_operation("busy_sum", 100_000_000)
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        q = rt.submit_operation("square", 1)  # queued, depth 1 == cap
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] == 1, 5.0, "queued")
        if q.cancel() is not True:
            _fail("queued cancel should return True")
        if q.cancelled() is not True:
            _fail("cancelled() True after a settled queued cancel")
        # The cancelled item is still in the queue (worker is busy with the holder),
        # so it still counts toward the cap and admission stays full.
        if rt.lane_stats()[0]["queue_depth"] != 1:
            _fail("cancelled-but-queued item must still count toward queue_depth")
        _expect(QueueFullError, lambda: rt.submit_operation("square", 2),
                "still full while a cancelled item occupies the slot")
        hold.cancel()  # release the holder -> worker drains/skips the cancelled item
    print("PASS: cancelled-but-not-yet-popped queued item still counts toward "
          "queue_depth (admission stays full until the worker pops/skips it)")


def section_unretired_shutdown_and_reinit():
    # Context-manager shutdown with UNRETIRED futures: submit several ops, never
    # call .result(), then exit the context. shutdown() stop_and_joins the lanes,
    # which drains the queue and fulfills every promise -- so no broken promise,
    # hang, or crash even though Python dropped the futures.
    with Runtime(num_lanes=2) as rt:
        for i in range(8):
            rt.submit_operation("square", i)  # future dropped, never retired
    # Sequential multi-lane re-init: Runtime -> shutdown -> Runtime (different lane
    # count) exercises the HPX stop/restart path + guard release/reacquire.
    with Runtime(num_lanes=2) as rt2:
        if rt2.submit_operation("add", 4, 5).result().value != 9:
            _fail("re-init: add(4, 5) must be 9 (num_lanes=2)")
    with Runtime(num_lanes=3) as rt3:
        if rt3.num_lanes() != 3:
            _fail("re-init: second runtime num_lanes must be 3")
        if rt3.submit_operation("square", 7).result().value != 49:
            _fail("re-init: square(7) must be 49 (num_lanes=3)")
    print("PASS: unretired-future shutdown drains cleanly; sequential "
          "multi-lane Runtime re-init")


def section_lifecycle():
    # submit-after-shutdown: the Python `_closed` guard rejects new work once a
    # Runtime is shut down (before any C++ crossing).
    rt = Runtime(hpx_threads=1)
    if rt.submit_operation("square", 4).result().value != 16:
        _fail("lifecycle: square(4) must be 16 before shutdown")
    rt.shutdown()
    _expect(RuntimeError, lambda: rt.submit_operation("square", 2),
            "submit after shutdown")
    rt.shutdown()  # idempotent: a second shutdown must not raise

    # sequential re-init: Runtime -> shutdown -> Runtime exercises the HPX
    # stop/restart path (stop_process_hpx then start_process_hpx again) and the
    # process-runtime guard release/reacquire.
    with Runtime(hpx_threads=1) as rt2:
        if rt2.submit_operation("add", 7, 8).result().value != 15:
            _fail("lifecycle: add(7, 8) must be 15 after Runtime re-init")
    print("PASS: lifecycle (submit-after-shutdown; idempotent shutdown; "
          "sequential Runtime re-init)")


def section_runtime_blocks_engine():
    # Reverse direction: an Engine holds the process guard; a Runtime must raise.
    with Engine(num_lanes=1, hpx_threads=1):
        _expect(RuntimeError, lambda: Runtime(),
                "Runtime while Engine is live")
    print("PASS: Engine <-> Runtime mutual exclusion")


def section_error_hierarchy():
    if not issubclass(OperationFailedError, RuntimeOperationError):
        _fail("OperationFailedError must subclass RuntimeOperationError")
    if not issubclass(OperationCancelledError, RuntimeOperationError):
        _fail("OperationCancelledError must subclass RuntimeOperationError")
    if not issubclass(RuntimeOperationError, RuntimeError):
        _fail("RuntimeOperationError must subclass RuntimeError")
    print("PASS: error class hierarchy")


def section_namespace_isolation():
    # The runtime must NOT leak into the harness top-level __all__.
    expected = {"Engine", "Future", "SyntheticActor", "QueueFullError",
                "hpx_smoke"}
    if set(rayx.__all__) != expected:
        _fail(f"rayx.__all__ changed: {sorted(rayx.__all__)} != "
              f"{sorted(expected)}")
    if "Runtime" in rayx.__all__:
        _fail("Runtime must not be exported via rayx.__all__")
    # Provisional names exist where they belong: under rayx.runtime.
    if not (RuntimeFuture and OperationResult):
        _fail("rayx.runtime must export RuntimeFuture / OperationResult")
    print("PASS: namespace isolation (rayx.__all__ unchanged)")


# ---- Slice 2c: collection APIs (get / wait / as_completed) ----------------

def section_get_collection():
    with Runtime(num_lanes=2) as rt:
        # single -> one OperationResult
        one = rt.get(rt.submit_operation("square", 6))
        if not isinstance(one, OperationResult) or one.value != 36:
            _fail("get(single) must return an OperationResult with value 36")
        _check_runtime_row(one.row, "get single")
        # list -> list[OperationResult] in input order, same length
        fs = [rt.submit_operation("square", i) for i in range(5)]
        res = rt.get(fs)
        if not (isinstance(res, list) and len(res) == 5):
            _fail("get(list) must return a list of the same length")
        if [r.value for r in res] != [i * i for i in range(5)]:
            _fail("get(list) values must be in input order")
        if not all(isinstance(r, OperationResult) for r in res):
            _fail("get(list) elements must all be OperationResult")
        # tuple accepted, order preserved
        tup = rt.get(tuple(rt.submit_operation("add", i, 1) for i in range(3)))
        if [r.value for r in tup] != [1, 2, 3]:
            _fail("get(tuple) must work and preserve order")
        # consume-once: a second get / result on the same future raises
        g = rt.submit_operation("square", 8)
        if rt.get(g).value != 64:
            _fail("get(single) value mismatch")
        _expect(RuntimeError, lambda: rt.get(g), "get twice (consume-once)")
        _expect(RuntimeError, lambda: g.result(), "result after get (consume-once)")
    print("PASS: get (single -> OperationResult; list/tuple -> input-order "
          "OperationResults; consume-once)")


def section_get_mixed_no_fail_fast():
    # A mixed completed / failed / cancelled batch: get retires ALL of them in
    # input order (no fail-fast); only the per-element .value raises. Deterministic
    # via a busy_sum holder + queued cancel (no sleeps).
    with Runtime(num_lanes=1) as rt:
        holder = rt.submit_operation("busy_sum", 100_000_000)  # holds lane 0
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        a = rt.submit_operation("square", 5)   # queued -> will be cancelled
        b = rt.submit_operation("boom")        # queued -> will fail
        c = rt.submit_operation("square", 9)   # queued -> will complete
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 3, 5.0,
                    "a/b/c queued")
        if a.cancel() is not True:
            _fail("queued cancel of 'a' must return True")
        holder.cancel()  # release the lane so b/c run
        res = rt.get([a, b, c])  # no fail-fast: all three retired, input order
        if len(res) != 3:
            _fail("get must collect all three results (no fail-fast)")
        if res[0].row["status"] != "cancelled":
            _fail(f"a status {res[0].row['status']!r}, expected 'cancelled'")
        _expect(OperationCancelledError, lambda: res[0].value, "a.value")
        if res[1].row["status"] != "failed":
            _fail(f"b status {res[1].row['status']!r}, expected 'failed'")
        _expect(OperationFailedError, lambda: res[1].value, "b.value")
        if res[2].row["status"] != "completed" or res[2].value != 81:
            _fail("c must complete with value 81")
        for r in res:
            _check_runtime_row(r.row, "mixed get row")
        rt.get([holder])  # retire the (cancelled) holder so the lane drains clean
    print("PASS: get mixed completed/failed/cancelled (no fail-fast; input order; "
          "per-element .value raises; rows readable)")


def section_wait_contract():
    # Blocking wait is non-consuming and partitions correctly; timeout=0 polls.
    with Runtime(num_lanes=2) as rt:
        fs = [rt.submit_operation("square", i) for i in range(6)]
        ready, rest = rt.wait(fs, num_returns=len(fs))
        if len(ready) != len(fs) or rest:
            _fail("blocking wait(num_returns=len) must return all as ready")
        # non-consuming: the same futures still retire afterward
        res = rt.get(fs)
        if [r.value for r in res] != [i * i for i in range(6)]:
            _fail("wait must be non-consuming (get after wait returns values)")
    with Runtime(num_lanes=2) as rt:
        fs = [rt.submit_operation("square", i) for i in range(4)]
        ready, rest = rt.wait(fs, num_returns=1)  # wait-any
        if len(ready) < 1 or len(ready) + len(rest) != 4:
            _fail("wait-any must return >=1 ready and a full partition")
        # ready/rest are the SAME future objects (identity preserved)
        for f in ready + rest:
            if f not in fs:
                _fail("wait must return the same future objects")
        rt.get(fs)
    # timeout=0 poll: a true readiness partition, deterministic via holder+cancel.
    with Runtime(num_lanes=1) as rt:
        holder = rt.submit_operation("busy_sum", 100_000_000)
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        q = rt.submit_operation("square", 7)
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 1, 5.0, "q queued")
        r0, p0 = rt.wait([holder, q], num_returns=1, timeout=0)
        if q in r0:
            _fail("poll: a still-queued future must be pending before cancel")
        if q.cancel() is not True:
            _fail("queued cancel must return True")
        r1, p1 = rt.wait([holder, q], num_returns=1, timeout=0)
        if q not in r1 or holder in r1:
            _fail("poll: after a queued cancel, q is ready and holder is pending")
        holder.cancel()
        rt.get([holder, q])
    print("PASS: wait (blocking all/any non-consuming; same future objects; "
          "timeout=0 poll partition via queued cancel)")


def section_wait_validation():
    # Shared validation across both modes (blocking + poll): empty, num_returns
    # (strict int / range), entry type, duplicate, timeout, retired.
    with Runtime(num_lanes=1) as rt:
        f = rt.submit_operation("square", 3)
        g = rt.submit_operation("square", 4)
        _expect(ValueError, lambda: rt.wait([]), "wait empty")
        # num_returns: strict int (bool rejected), no float coercion, range-checked
        _expect(TypeError, lambda: rt.wait([f], num_returns=True), "num_returns bool")
        _expect(TypeError, lambda: rt.wait([f], num_returns=1.0), "num_returns float")
        _expect(ValueError, lambda: rt.wait([f], num_returns=0), "num_returns 0")
        _expect(ValueError, lambda: rt.wait([f, g], num_returns=3),
                "num_returns > len")
        # wrong-type entry -> deliberate TypeError in BOTH modes
        _expect(TypeError, lambda: rt.wait([object()]), "wrong type blocking")
        _expect(TypeError, lambda: rt.wait([object()], timeout=0), "wrong type poll")
        # duplicate future -> ValueError in BOTH modes
        _expect(ValueError, lambda: rt.wait([f, f]), "duplicate blocking")
        _expect(ValueError, lambda: rt.wait([f, f], timeout=0), "duplicate poll")
        # timeout value contracts
        _expect(NotImplementedError, lambda: rt.wait([f], timeout=0.5), "timeout>0")
        _expect(ValueError, lambda: rt.wait([f], timeout=-1), "timeout negative")
        _expect(ValueError, lambda: rt.wait([f], timeout=float("nan")), "timeout nan")
        _expect(ValueError, lambda: rt.wait([f], timeout=float("inf")), "timeout inf")
        _expect(TypeError, lambda: rt.wait([f], timeout=True), "timeout bool")
        _expect(TypeError, lambda: rt.wait([f], timeout="0"), "timeout str")
        # retired future -> raises in BOTH modes (f, g were never consumed above)
        rt.get([f, g])
        _expect(RuntimeError, lambda: rt.wait([f]), "retired blocking")
        _expect(RuntimeError, lambda: rt.wait([g], timeout=0), "retired poll")
    print("PASS: wait validation (empty; num_returns strict int/range; wrong-type "
          "TypeError; duplicate ValueError; timeout modes; retired raises)")


def section_as_completed():
    # Yields the input RuntimeFuture handles (not results), each exactly once, and
    # is non-consuming until the caller retires.
    with Runtime(num_lanes=2) as rt:
        fs = [rt.submit_operation("square", i) for i in range(6)]
        seen = []
        for fut in rt.as_completed(fs):
            if fut not in fs:
                _fail("as_completed yielded an unknown future")
            if not isinstance(fut, RuntimeFuture):
                _fail("as_completed must yield RuntimeFuture handles, not results")
            if fut.ready() is not True:
                _fail("as_completed must yield a ready (but not retired) future")
            seen.append(fut)
        if len(seen) != len(fs) or any(seen.count(x) != 1 for x in fs):
            _fail("as_completed must yield each input future exactly once")
        res = [f.result() for f in fs]  # caller retires AFTER the loop
        if sorted(r.value for r in res) != sorted(i * i for i in range(6)):
            _fail("as_completed futures must retire to the right values")
    # mixed completed / failed / cancelled
    with Runtime(num_lanes=1) as rt:
        holder = rt.submit_operation("busy_sum", 100_000_000)
        _wait_until(lambda: rt.lane_stats()[0]["active"], 5.0, "holder active")
        a = rt.submit_operation("square", 5)
        b = rt.submit_operation("boom")
        c = rt.submit_operation("square", 9)
        _wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 3, 5.0, "queued")
        a.cancel()
        holder.cancel()
        got = list(rt.as_completed([a, b, c]))
        if {id(x) for x in got} != {id(a), id(b), id(c)}:
            _fail("as_completed(mixed) must yield all three exactly once")
        want = {id(a): "cancelled", id(b): "failed", id(c): "completed"}
        for fut in got:
            if fut.result().row["status"] != want[id(fut)]:
                _fail("as_completed(mixed) wrong terminal status")
        rt.get([holder])
    # duplicate / wrong-type propagate from wait (generator raises when iterated)
    with Runtime(num_lanes=1) as rt:
        f = rt.submit_operation("square", 1)
        _expect(ValueError, lambda: list(rt.as_completed([f, f])),
                "as_completed duplicate")
        _expect(TypeError, lambda: list(rt.as_completed([object()])),
                "as_completed wrong type")
        rt.get([f])
    print("PASS: as_completed (yields each future handle once; non-consuming until "
          "result(); mixed statuses; duplicate/wrong-type raise)")


def main():
    section_namespace_isolation()
    section_error_hierarchy()
    section_value_path_and_contract()
    section_num_lanes_api()
    section_round_robin_and_stats()
    section_single_lane_batch()
    section_busy_sum_value()
    section_short_busy_sum_cancel_invariant()
    section_running_cancel()
    section_queued_cancel()
    section_shutdown_cancels_running()
    section_lane_stats_under_load()
    section_cancelled_semantics()
    section_failed_terminal_cancel()
    section_admission_validation()
    section_admission_full_reject()
    section_admission_drain_reopens()
    section_admission_per_lane()
    section_admission_cancelled_still_counts()
    section_get_collection()
    section_get_mixed_no_fail_fast()
    section_wait_contract()
    section_wait_validation()
    section_as_completed()
    section_unretired_shutdown_and_reinit()
    section_lifecycle()
    section_runtime_blocks_engine()
    print("ALL PASS: rayx.runtime Slice 2c smoke")
    return 0


if __name__ == "__main__":
    sys.exit(main())
