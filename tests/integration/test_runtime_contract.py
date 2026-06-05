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
