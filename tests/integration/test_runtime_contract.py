"""Integration contract tests for rayx.runtime (require built _rayx + HPX).

A SMALL, high-signal subset -- bench/smoke_rayx_runtime.py remains the comprehensive
contract test; this suite intentionally does not re-encode every smoke section. The
whole module skips cleanly via importorskip if the extension is not built.

Determinism: cancellation / admission tests gate on lane_stats() polling (the
`wait_until` fixture), never on sleeps, and assert no timing values.
"""
import pytest

# Skips the whole module cleanly if _rayx is not built (ImportError -> skip).
rayx_runtime = pytest.importorskip("rayx.runtime")

import rayx  # noqa: E402  (rayx is already imported by the importorskip above)
from rayx.runtime import (  # noqa: E402
    OperationCancelledError,
    OperationFailedError,
    OperationResult,
    QueueFullError,
    Runtime,
    RuntimeFuture,
)
from rayx.runtime._validate import ROW_FIELDS  # noqa: E402  canonical row contract

EXPECTED_ALL = {
    "Runtime",
    "RuntimeFuture",
    "OperationResult",
    "ActorHandle",
    "RuntimeOperationError",
    "OperationFailedError",
    "OperationCancelledError",
    "QueueFullError",
}
FORBIDDEN_ROW_FIELDS = ("value", "label", "chunks", "chunk_delay_ms",
                        "chunks_completed")


def _any_active(rt):
    return any(s["active"] for s in rt.lane_stats())


# --- 1. public API shape ----------------------------------------------------


def test_runtime_all_exact():
    assert set(rayx_runtime.__all__) == EXPECTED_ALL


def test_runtime_not_in_top_level_all():
    assert "Runtime" not in rayx.__all__


def test_no_top_level_get_wait():
    assert not hasattr(rayx, "get")
    assert not hasattr(rayx, "wait")


def test_no_module_level_get_wait_on_runtime():
    assert not hasattr(rayx_runtime, "get")
    assert not hasattr(rayx_runtime, "wait")


# --- 2. value / row contract ------------------------------------------------


def test_square_value_and_row_contract():
    with Runtime() as rt:
        res = rt.submit_operation("square", 2).result()
        assert isinstance(res, OperationResult)
        assert res.value == 4
        assert set(res.row) == set(ROW_FIELDS)        # exactly the 9 fields
        assert "value" not in res.row
        for field in FORBIDDEN_ROW_FIELDS:
            assert field not in res.row
        assert res.row["actor_id"].startswith("rt-hpx-")


# --- 3. failure contract ----------------------------------------------------


def test_boom_failure_contract():
    with Runtime() as rt:
        res = rt.submit_operation("boom").result()
        assert isinstance(res, OperationResult)
        assert res.row["status"] == "failed"
        assert res.row["error"]            # row stays readable, error populated
        with pytest.raises(OperationFailedError):
            _ = res.value
        # `except OperationFailedError` catches correctly
        caught = False
        try:
            _ = res.value
        except OperationFailedError:
            caught = True
        assert caught


# --- 4. cancellation contract -----------------------------------------------


def test_running_cancel_contract(wait_until):
    with Runtime(num_lanes=1) as rt:
        fut = rt.submit_operation("busy_sum", 2_000_000_000)  # long, checkpointed
        wait_until(lambda: _any_active(rt), where="busy_sum active")
        assert fut.cancel() is True
        res = fut.result()
        assert res.row["status"] == "cancelled"
        with pytest.raises(OperationCancelledError):
            _ = res.value


def test_busy_sum_huge_n_submit_and_cancel():
    """Overflow/arming guard: submitting busy_sum(INT64_MAX) is safe and cancelable.

    busy_sum_checkpoints computes ceil(n / STRIDE) at SUBMIT time. The naive
    (n + STRIDE - 1) / STRIDE form overflowed int64 for n near INT64_MAX -- UB,
    reachable from Python (any int64 n >= 0 passes validation); in practice the
    wrap produced a negative count whose zero-iteration body made the huge op
    bogusly "complete" instantly with value 0. This protects the hardened
    (n - 1) / STRIDE + 1 form: the count clamps to INT_MAX, so the op is armed
    running-cancellable and is genuinely huge. The test therefore relies on
    cancellation and MUST NEVER wait for completion (2**63 steps): cancel() is
    True whether it settles queued or running (clamped count > 1 guarantees a
    boundary ahead either way), and the future retires status="cancelled". No
    sleeps, no timing assertions; the also-shared actor busy_get path is covered
    via the same helper.
    """
    with Runtime(num_lanes=1) as rt:
        fut = rt.submit_operation("busy_sum", 2**63 - 1)
        assert fut.cancel() is True          # queued or running: both settle True
        res = fut.result()                   # queued: immediate; running: <= one
        assert res.row["status"] == "cancelled"   # chunk boundary away
        with pytest.raises(OperationCancelledError):
            _ = res.value


# --- 4b. park_ms: parked/cooperative work shape ------------------------------
#
# park_ms(ms) is the PARKED analog of the CPU-bound busy_sum diagnostic: it
# occupies the lane's service slot while the HPX thread running it suspends
# COOPERATIVELY (chunked hpx::this_thread::sleep_for, PARK_MS_STRIDE chunks), so
# a parked lane does not pin an OS worker. All tests below are structural /
# stats-gated: NO wall-clock assertions, NO duration measurements, NO performance
# claims. Large-duration holds rely on the same several-orders-of-magnitude
# engineering margin as the busy_sum/busy_get tests (a 60 s park cannot retire
# within a microsecond-scale test window) -- margin, not formal proof.

_PARK_BIG_MS = 60_000  # PARK_MS_MAX: a held lane parks for 60 s unless cancelled


def test_park_ms_completes_and_echoes():
    with Runtime() as rt:
        res = rt.submit_operation("park_ms", 1).result()
        assert res.row["status"] == "completed"
        assert res.value == 1                      # completion echoes ms
        assert isinstance(res.value, int)
        assert tuple(sorted(res.row)) == tuple(sorted(ROW_FIELDS))


def test_park_ms_zero_terminal_cancel_false():
    # ms=0 parks nothing (checkpoint_count == 1, queued-cancelable only); once
    # the result is terminal, cancel() must report False, never claim a cancel.
    with Runtime() as rt:
        f = rt.submit_operation("park_ms", 0)
        res = f.result()
        assert res.row["status"] == "completed" and res.value == 0
        assert f.cancel() is False


def test_park_ms_running_cancel_stats_gated(wait_until):
    """cancel() is True once the park is observed active; row retires cancelled.

    HONEST SCOPE: gating on active removes the start-side race; the finish side
    rests on the engineering margin that a 60 s park cannot complete within the
    microsecond gap between the observation and cancel(). No wall-clock value is
    asserted anywhere: the stop lands at the next PARK_MS_STRIDE chunk boundary,
    and only the terminal row/value contract is checked.
    """
    with Runtime(num_lanes=1) as rt:
        f = rt.submit_operation("park_ms", _PARK_BIG_MS)
        wait_until(lambda: _any_active(rt), where="park_ms active")
        assert f.cancel() is True
        res = f.result()
        assert res.row["status"] == "cancelled"
        with pytest.raises(OperationCancelledError):
            _ = res.value


def test_park_ms_queued_cancel(wait_until):
    with Runtime(num_lanes=1) as rt:
        holder = rt.submit_operation("park_ms", _PARK_BIG_MS)
        wait_until(lambda: _any_active(rt), where="holder active")
        queued = rt.submit_operation("park_ms", 1)   # FIFO: behind the holder
        assert queued.cancel() is True               # queued: settles immediately
        res = queued.result()
        assert res.row["status"] == "cancelled"
        with pytest.raises(OperationCancelledError):
            _ = res.value
        assert holder.cancel() is True               # release the lane fast
        assert holder.result().row["status"] == "cancelled"


def test_park_overlap_structural_hpx_threads_1(wait_until):
    """Cooperative parking yields the ONE OS worker: structural overlap proof.

    With hpx_threads=1 there is a single HPX worker OS thread. A park that
    BLOCKED its thread (std::this_thread::sleep_for) would starve every other
    lane until it finished, so the tiny op on the second lane could never
    retire. The tiny op retiring while the park lane is still active is
    therefore a structural, state-gated proof that the park suspends
    cooperatively -- no wall-clock values are asserted. HONEST SCOPE: "still
    active" rides the usual margin (the 60 s park cannot have finished within
    the tiny op's window); round-robin places submissions 0 and 1 on different
    lanes (the round-robin contract is covered by the smoke). This is the
    runtime-internal demonstration of the parked/cooperative regime; the
    quantitative overlap observation note is a deferred follow-up slice.
    """
    with Runtime(num_lanes=2, hpx_threads=1) as rt:
        park = rt.submit_operation("park_ms", _PARK_BIG_MS)   # lane A
        wait_until(lambda: _any_active(rt), where="park active")
        tiny = rt.submit_operation("square", 7)               # lane B
        assert tiny.result().value == 49     # retires WHILE the park holds A
        assert _any_active(rt)               # the park lane is still active
        assert park.cancel() is True         # fast teardown
        assert park.result().row["status"] == "cancelled"


def test_park_ms_shutdown_bounded_and_fulfills(wait_until):
    """Shutdown with a parked op in flight: clean, and the future still retires.

    shutdown()'s cancel_pending stops the park at its next chunk boundary (at
    most one PARK_MS_STRIDE), so teardown is bounded by a chunk, never the full
    60 s park -- asserted only as "shutdown returns and the future retires with
    a valid terminal row" (no duration measurement, no exact-status assertion).
    """
    rt = Runtime(num_lanes=1)
    f = rt.submit_operation("park_ms", _PARK_BIG_MS)
    wait_until(lambda: _any_active(rt), where="park active")
    rt.shutdown()                            # must not hang for the full park
    res = f.result()                         # retire after shutdown
    assert res.row["status"] in ("completed", "cancelled")
    if res.row["status"] == "cancelled":
        with pytest.raises(OperationCancelledError):
            _ = res.value
    else:
        assert res.value == _PARK_BIG_MS


# --- 5. bounded admission contract ------------------------------------------


def test_bounded_admission_queue_full(wait_until):
    # Single lane, cap=1: hold the lane with a long busy_sum, fill the one queue
    # slot, then the next submit must be rejected with QueueFullError.
    with Runtime(num_lanes=1, max_queue_depth_per_lane=1) as rt:
        holder = rt.submit_operation("busy_sum", 2_000_000_000)
        wait_until(lambda: _any_active(rt), where="holder active")
        rt.submit_operation("square", 1)  # fills the single queue slot
        wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 1,
                   where="queue slot filled")
        with pytest.raises(QueueFullError) as ei:
            rt.submit_operation("square", 2)
        assert ei.type is QueueFullError   # the rayx.runtime.QueueFullError class
        holder.cancel()                    # release the lane; context exit drains


# --- 6. collection API subset -----------------------------------------------


def test_get_no_fail_fast_mixed(wait_until):
    with Runtime(num_lanes=1) as rt:
        holder = rt.submit_operation("busy_sum", 2_000_000_000)
        wait_until(lambda: _any_active(rt), where="holder active")
        a = rt.submit_operation("square", 5)   # queued -> cancelled
        b = rt.submit_operation("boom")        # queued -> failed
        c = rt.submit_operation("square", 9)   # queued -> completed
        wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 3,
                   where="a/b/c queued")
        assert a.cancel() is True
        holder.cancel()                        # release the lane so b/c run
        res = rt.get([a, b, c])                # input order, no fail-fast
        assert [type(r) for r in res] == [OperationResult] * 3
        assert res[0].row["status"] == "cancelled"
        assert res[1].row["status"] == "failed"
        assert res[2].row["status"] == "completed" and res[2].value == 81
        rt.get([holder])                       # retire the cancelled holder


def test_wait_is_non_consuming():
    with Runtime(num_lanes=2) as rt:
        fs = [rt.submit_operation("square", i) for i in range(4)]
        ready, rest = rt.wait(fs, num_returns=1)
        assert len(ready) >= 1
        assert len(ready) + len(rest) == 4
        # non-consuming: the same futures still retire afterward
        assert sorted(r.value for r in rt.get(fs)) == [0, 1, 4, 9]


def test_as_completed_yields_futures_each_once():
    with Runtime(num_lanes=2) as rt:
        fs = [rt.submit_operation("square", i) for i in range(5)]
        seen = []
        for fut in rt.as_completed(fs):
            assert isinstance(fut, RuntimeFuture)   # handles, not results
            seen.append(fut)
        assert {id(x) for x in seen} == {id(x) for x in fs}
        assert len(seen) == 5
        assert sorted(f.result().value for f in fs) == [0, 1, 4, 9, 16]


# --- 7. constructor / wait argument validation subset -----------------------


def test_invalid_num_lanes():
    with pytest.raises(ValueError):
        Runtime(num_lanes=0)
    with pytest.raises(TypeError):
        Runtime(num_lanes=True)


def test_invalid_max_queue_depth_per_lane():
    with pytest.raises(ValueError):
        Runtime(max_queue_depth_per_lane=0)
    with pytest.raises(TypeError):
        Runtime(max_queue_depth_per_lane=1.5)


def test_wait_argument_validation():
    with Runtime() as rt:
        f = rt.submit_operation("square", 1)
        g = rt.submit_operation("square", 2)
        with pytest.raises(TypeError):
            rt.wait([object()])                  # wrong-type entry
        with pytest.raises(ValueError):
            rt.wait([f, f])                      # duplicate future
        with pytest.raises(TypeError):
            rt.wait([f], num_returns=True)       # bad num_returns
        rt.get([f, g])                           # retire both
        with pytest.raises(RuntimeError):
            rt.wait([f])                         # retired future


# --- 8. fanout_sum (internal-composition op, P1 launch-all) ------------------


def _closed_form(n):
    return (n * (n - 1) // 2) % (2 ** 31)


def test_fanout_sum_value_matches_closed_form_and_busy_sum():
    with Runtime(num_lanes=1, hpx_threads=2) as rt:
        for n, parts in [(0, 1), (1, 1), (1, 5), (10, 1), (10, 3), (10, 10),
                         (10, 1024), (100_000, 7), (100_000, 1)]:
            res = rt.submit_operation("fanout_sum", n, parts).result()
            assert res.row["status"] == "completed"
            assert res.value == _closed_form(n)
            # same value as busy_sum(n) -- same closed form, different mechanism
            assert res.value == rt.submit_operation("busy_sum", n).result().value


def test_fanout_sum_value_independent_of_parts():
    with Runtime(num_lanes=1, hpx_threads=2) as rt:
        # Small n so the sweep can include parts == n and parts > n while staying
        # within PARTS_MAX (1024); plus a large-n moderate-parts check separately.
        n = 100
        vals = {rt.submit_operation("fanout_sum", n, p).result().value
                for p in (1, 2, 7, n, n + 1, 1024)}   # incl. parts == n and parts > n
        assert vals == {_closed_form(n)}
        big = {rt.submit_operation("fanout_sum", 50_000, p).result().value
               for p in (1, 2, 7, 64, 1024)}
        assert big == {_closed_form(50_000)}


def test_fanout_sum_row_contract():
    with Runtime(num_lanes=1) as rt:
        res = rt.submit_operation("fanout_sum", 1000, 4).result()
        assert isinstance(res, OperationResult)
        assert set(res.row) == set(ROW_FIELDS)        # exactly the 9 fields
        assert "value" not in res.row
        for field in FORBIDDEN_ROW_FIELDS:
            assert field not in res.row
        assert res.row["actor_id"].startswith("rt-hpx-")


def test_fanout_sum_boundary_validation():
    with Runtime() as rt:
        with pytest.raises(ValueError):
            rt.submit_operation("fanout_sum", 10)            # wrong arity
        with pytest.raises(TypeError):
            rt.submit_operation("fanout_sum", 10, True)      # bool parts
        with pytest.raises(ValueError):
            rt.submit_operation("fanout_sum", -1, 2)         # n < 0
        with pytest.raises(ValueError):
            rt.submit_operation("fanout_sum", 10, 0)         # parts < 1
        with pytest.raises(ValueError):
            rt.submit_operation("fanout_sum", 10, 1025)      # parts > PARTS_MAX


def test_fanout_sum_collection_compat():
    with Runtime(num_lanes=2, hpx_threads=2) as rt:
        fs = [rt.submit_operation("fanout_sum", n, 4) for n in (10, 20, 30)]
        got = rt.get(fs)                                  # input order, no fail-fast
        assert [r.value for r in got] == [_closed_form(n) for n in (10, 20, 30)]
        gs = [rt.submit_operation("fanout_sum", n, 3) for n in (5, 6, 7)]
        ready, rest = rt.wait(gs, num_returns=1)          # non-consuming
        assert len(ready) >= 1 and len(ready) + len(rest) == 3
        seen = [f for f in rt.as_completed(gs)]
        assert {id(x) for x in seen} == {id(x) for x in gs}


def test_fanout_sum_queued_cancel(wait_until):
    with Runtime(num_lanes=1) as rt:
        holder = rt.submit_operation("busy_sum", 2_000_000_000)  # hold lane 0
        wait_until(lambda: _any_active(rt), where="holder active")
        q = rt.submit_operation("fanout_sum", 1000, 4)           # queued behind it
        wait_until(lambda: rt.lane_stats()[0]["queue_depth"] >= 1,
                   where="fanout queued")
        assert q.cancel() is True                                # queued cancel wins
        holder.cancel()                                          # release the lane
        res = q.result()
        assert res.row["status"] == "cancelled"
        with pytest.raises(OperationCancelledError):
            _ = res.value
        rt.get([holder])                                         # retire holder


def test_fanout_sum_queued_only_cancel_invariant():
    # checkpoint_count == 1 (P1 launch-all): a cancel() that WINS must mean
    # status == "cancelled" (a queued cancel); once the op is in service an active
    # cancel() returns False and the op completes normally -- never True+completed.
    # Race cancel against the instant run over a loop, then a deterministic
    # post-completion False check.
    with Runtime(num_lanes=1, hpx_threads=2) as rt:
        for _ in range(64):
            f = rt.submit_operation("fanout_sum", 20_000, 8)
            settled = f.cancel()
            res = f.result()
            st = res.row["status"]
            assert settled == (st == "cancelled")            # never True+completed
            if st == "completed":
                assert res.value == _closed_form(20_000)
            else:
                with pytest.raises(OperationCancelledError):
                    _ = res.value
        gf = rt.submit_operation("fanout_sum", 20_000, 8)
        gr = gf.result()
        assert gr.row["status"] == "completed"
        assert gr.value == _closed_form(20_000)
        assert gf.cancel() is False                          # terminal -> False
        assert gf.cancelled() is False


def test_fanout_sum_native_bypass_invalid_args_fail_safely():
    # The PUBLIC path (Runtime.submit_operation) rejects invalid args at the Python
    # boundary before any future (test_fanout_sum_boundary_validation). This test
    # drives the PRIVATE/native bypass -- constructing the raw `_RuntimeEngine` and
    # submitting unvalidated args -- to prove the defensive C++ guard in the
    # fanout_sum body turns invalid args (especially `parts == 0`, which would
    # otherwise be a `n / parts` divide-by-zero / UB) into a clean status="failed"
    # row rather than a crash. Valid args still compute through the bypass.
    from rayx._rayx import _RuntimeEngine
    eng = _RuntimeEngine(hpx_threads=1, num_lanes=1, max_queue_depth_per_lane=-1)
    try:
        for args in ([10, 0], [-1, 2], [10, 1025]):   # parts==0, n<0, parts>MAX
            row = eng.submit_operation("fanout_sum", args).result()
            assert row["status"] == "failed"
            assert row["has_value"] is False
            assert row["error"]                       # populated, readable
        ok = eng.submit_operation("fanout_sum", [10, 3]).result()
        assert ok["status"] == "completed" and ok["value"] == _closed_form(10)
    finally:
        eng.shutdown()


# --- value-model V1: typed signatures + int64 range -------------------------


def test_runtime_op_table_typed_signatures():
    # The registry view is the typed-signature shape (value-model V1/V3):
    # {op_id: {"arg_types": [...], "result_type": ...}}, arity == len(arg_types).
    # V3 adds scale_double (double,double -> double); the others stay int64.
    from rayx._rayx import runtime_op_table
    tbl = runtime_op_table()
    expected = {
        "square": {"arg_types": ["int64"], "result_type": "int64"},
        "add": {"arg_types": ["int64", "int64"], "result_type": "int64"},
        "boom": {"arg_types": [], "result_type": "int64"},
        "busy_sum": {"arg_types": ["int64"], "result_type": "int64"},
        "fanout_sum": {"arg_types": ["int64", "int64"], "result_type": "int64"},
        "scale_double": {"arg_types": ["double", "double"],
                         "result_type": "double"},
        "park_ms": {"arg_types": ["int64"], "result_type": "int64"},
        # exp39: chain_sum_loop / chain_sum_then share (seed, steps, quantum) -> int64.
        "chain_sum_loop": {"arg_types": ["int64", "int64", "int64"],
                           "result_type": "int64"},
        "chain_sum_then": {"arg_types": ["int64", "int64", "int64"],
                           "result_type": "int64"},
        # exp40: chain_fanout (seed, count, steps, quantum) -> int64.
        "chain_fanout": {"arg_types": ["int64", "int64", "int64", "int64"],
                         "result_type": "int64"},
        # exp44: barrier_fanin (seed, leaves, quantum) -> int64 (witnessed
        # barrier-gated fan-in; the one side-effecting registry op).
        "barrier_fanin": {"arg_types": ["int64", "int64", "int64"],
                          "result_type": "int64"},
    }
    assert set(tbl) == set(expected)
    for op, sig in expected.items():
        assert set(tbl[op]) == {"arg_types", "result_type"}
        assert tbl[op]["arg_types"] == sig["arg_types"]
        assert tbl[op]["result_type"] == sig["result_type"]


def test_int64_out_of_range_rejected_at_boundary():
    # Python ints are arbitrary-precision; a value that does not fit int64 is
    # rejected at the Python boundary with ValueError BEFORE any future is created
    # (intentional V1 behavior change vs the old opaque pybind cast failure).
    int64_max = 2 ** 63 - 1
    int64_min = -(2 ** 63)
    with Runtime() as rt:
        with pytest.raises(ValueError):
            rt.submit_operation("square", int64_max + 1)
        with pytest.raises(ValueError):
            rt.submit_operation("add", 1, int64_min - 1)
        # The inclusive endpoints are accepted and still compute (value channel
        # unchanged: int64 in -> int out).
        assert rt.submit_operation("add", int64_min, 0).result().value == int64_min
        assert rt.submit_operation("square", 0).result().value == 0


# --- value-model V3: scale_double (double channel) --------------------------


def test_scale_double_value_and_type():
    # scale_double(x, factor) = x * factor; double in / double out. Use exactly
    # representable doubles so equality is exact (no tolerance, no float-assoc trap).
    with Runtime() as rt:
        res = rt.submit_operation("scale_double", 1.5, 2.0).result()
        assert isinstance(res, OperationResult)
        assert res.value == 3.0
        assert isinstance(res.value, float)          # double -> Python float
        assert rt.submit_operation("scale_double", -0.5, 4.0).result().value == -2.0
        assert rt.submit_operation("scale_double", 123.0, 0.0).result().value == 0.0


def test_scale_double_row_contract():
    with Runtime() as rt:
        res = rt.submit_operation("scale_double", 2.5, 2.0).result()
        assert res.value == 5.0
        assert set(res.row) == set(ROW_FIELDS)        # exactly the 9 fields
        assert "value" not in res.row                 # no value, no type field
        for field in FORBIDDEN_ROW_FIELDS:
            assert field not in res.row
        assert res.row["status"] == "completed"
        assert res.row["actor_id"].startswith("rt-hpx-")


def test_scale_double_boundary_validation():
    # All rejected at the Python boundary BEFORE any future is created.
    with Runtime() as rt:
        with pytest.raises(ValueError):
            rt.submit_operation("scale_double", 1.5)              # wrong arity
        with pytest.raises(TypeError):
            rt.submit_operation("scale_double", 2, 2.0)          # int (no widening)
        with pytest.raises(TypeError):
            rt.submit_operation("scale_double", True, 2.0)       # bool
        with pytest.raises(ValueError):
            rt.submit_operation("scale_double", float("nan"), 2.0)
        with pytest.raises(ValueError):
            rt.submit_operation("scale_double", float("inf"), 2.0)
        with pytest.raises(ValueError):
            rt.submit_operation("scale_double", 1.0, float("-inf"))


def test_scale_double_collection_compat():
    with Runtime() as rt:
        fs = [rt.submit_operation("scale_double", x, 2.0) for x in (1.0, 2.0, 3.0)]
        got = rt.get(fs)
        assert [r.value for r in got] == [2.0, 4.0, 6.0]
        gs = [rt.submit_operation("scale_double", x, 0.5) for x in (4.0, 8.0)]
        assert sorted(f.result().value for f in rt.as_completed(gs)) == [2.0, 4.0]


def test_int64_ops_unchanged_under_v3():
    # The existing int64 ops keep exact behavior with the variant channel.
    with Runtime() as rt:
        assert rt.submit_operation("square", 7).result().value == 49
        assert isinstance(rt.submit_operation("square", 7).result().value, int)
        assert rt.submit_operation("add", 3, 4).result().value == 7
        assert rt.submit_operation("busy_sum", 100000).result().value == _closed_form(
            100000)
        assert rt.submit_operation("fanout_sum", 100000, 8).result().value == \
            _closed_form(100000)


def test_native_bypass_wrong_tag_fails_safely():
    # PUBLIC path is type-validated; this drives the PRIVATE/raw bypass to prove the
    # native typed extraction is the backstop: a wrong-tag arg maps to a status=
    # "failed" row (op-body as_int64/as_double throw -> make_op_task catch), and a
    # wrong-tag arg to a CHECKPOINTED op flows through the DEFENSIVE checkpoint_count
    # (returns 1) into the same failed row -- NOT a Python exception at submit.
    from rayx._rayx import _RuntimeEngine
    eng = _RuntimeEngine(hpx_threads=1, num_lanes=1, max_queue_depth_per_lane=-1)
    try:
        # square expects int64; a double reaches the body and as_int64 throws.
        row = eng.submit_operation("square", [1.5]).result()
        assert row["status"] == "failed" and row["has_value"] is False and row["error"]
        # scale_double expects double; an int reaches the body and as_double throws.
        row = eng.submit_operation("scale_double", [2, 3]).result()
        assert row["status"] == "failed" and row["has_value"] is False and row["error"]
        # busy_sum is checkpointed; a double must NOT raise at submit (defensive
        # checkpoint_count) -- it retires as a failed row from the body.
        row = eng.submit_operation("busy_sum", [1.5]).result()
        assert row["status"] == "failed" and row["has_value"] is False and row["error"]
        # Valid calls still compute through the bypass (int64 and double).
        assert eng.submit_operation("square", [6]).result()["value"] == 36
        assert eng.submit_operation("scale_double", [1.5, 2.0]).result()["value"] == 3.0
    finally:
        eng.shutdown()


def test_mixed_int64_double_collection_roundtrip():
    # Heterogeneous values round-trip through ONE collection path: the variant value
    # channel yields a Python int for the int64 op and a Python float for the double
    # op, in input order, each with the exact 9-field row (no value/type field).
    with Runtime() as rt:
        results = rt.get([
            rt.submit_operation("square", 2),
            rt.submit_operation("scale_double", 1.5, 2.0),
        ])
        assert [r.value for r in results] == [4, 3.0]
        assert isinstance(results[0].value, int)        # int64 -> Python int
        assert isinstance(results[1].value, float)      # double -> Python float
        for r in results:
            assert set(r.row) == set(ROW_FIELDS)         # exactly the 9 fields
            assert "value" not in r.row
