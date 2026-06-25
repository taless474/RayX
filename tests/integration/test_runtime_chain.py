"""Integration contract tests for the exp39 chain_sum_* ops (``chain_sum_loop`` /
``chain_sum_then``). Require the built ``_rayx`` extension + HPX; the whole module
skips cleanly via ``importorskip`` if the extension is not built.

These gate the experiment's credibility, NOT its timing: the THREE-WAY equal-work
invariant (``chain_sum_loop`` == ``chain_sum_then`` == Python left-fold of the
one-stage ``chain_sum_loop``), determinism, the value/row contract, edge cases, the
queued-only cancellation posture, and future ``get`` / ``wait`` / ``as_completed``
parity. No timing values are asserted -- latency/decomposition lives in the exp39
driver, never in the contract tests.
"""
import pytest

# Skips the whole module cleanly if _rayx is not built (ImportError -> skip).
rayx_runtime = pytest.importorskip("rayx.runtime")

from rayx.runtime import (  # noqa: E402
    OperationCancelledError,
    OperationResult,
    Runtime,
)
from rayx.runtime._validate import ROW_FIELDS  # noqa: E402  canonical row contract
from rayx._rayx import runtime_op_table  # noqa: E402  native registry view

CHAIN_OPS = ["chain_sum_loop", "chain_sum_then"]


def _any_active(rt):
    return any(s["active"] for s in rt.lane_stats())


def _mediated_fold(rt, seed, steps, quantum):
    """The mediated_samelane path: Python left-fold of S one-stage chain_sum_loop
    calls into the same runtime, feeding each result forward."""
    x = seed
    for _ in range(steps):
        x = rt.submit_operation("chain_sum_loop", x, 1, quantum).result().value
    return x


# --- 1. op-table / signature structural (guards any op-set golden) -----------


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_ops_registered_arity3(op):
    table = dict(runtime_op_table())
    assert op in table
    assert table[op]["arg_types"] == ["int64", "int64", "int64"]
    assert table[op]["result_type"] == "int64"


# --- 2. three-way equal-work invariant (the credibility gate) ---------------


@pytest.mark.parametrize("seed,steps,quantum", [
    (0, 0, 0),       # empty chain -> seed
    (0, 1, 0),       # one identity stage
    (7, 1, 64),      # one real stage
    (3, 5, 100),
    (123, 8, 257),
    (-5, 4, 33),     # negative seed (folded under the mask on the first stage)
])
def test_three_way_equal_work(seed, steps, quantum):
    with Runtime() as rt:
        loop_v = rt.submit_operation(
            "chain_sum_loop", seed, steps, quantum).result().value
        then_v = rt.submit_operation(
            "chain_sum_then", seed, steps, quantum).result().value
        med_v = _mediated_fold(rt, seed, steps, quantum)
        assert loop_v == then_v == med_v


# --- 3. determinism ---------------------------------------------------------


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_deterministic(op):
    with Runtime() as rt:
        a = rt.submit_operation(op, 9, 6, 128).result().value
        b = rt.submit_operation(op, 9, 6, 128).result().value
        assert a == b


# --- 4. value / row contract ------------------------------------------------


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_value_and_row_contract(op):
    with Runtime() as rt:
        res = rt.submit_operation(op, 2, 3, 50).result()
        assert isinstance(res, OperationResult)
        assert isinstance(res.value, int)
        assert set(res.row) == set(ROW_FIELDS)     # exactly the 9 row fields
        assert "value" not in res.row
        assert res.row["status"] == "completed"
        assert res.row["actor_id"].startswith("rt-hpx-")


# --- 5. edge cases ----------------------------------------------------------


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_zero_steps_returns_seed(op):
    with Runtime() as rt:
        assert rt.submit_operation(op, 42, 0, 999).result().value == 42


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_zero_quantum_is_identity(op):
    # quantum == 0 -> masked_range_sum(0,0) == 0 -> each stage is identity, so any
    # number of stages preserves the seed.
    with Runtime() as rt:
        assert rt.submit_operation(op, 17, 4, 0).result().value == 17


# --- 6. queued-only cancellation posture ------------------------------------


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_queued_cancel(op, wait_until):
    # Hold a single serial lane with a long checkpointed busy_sum, queue a chain op
    # behind it, and cancel the queued chain -> it settles cancelled immediately
    # (queued cancel always works). Then release the holder fast via its running
    # cancel. Gated on lane_stats(), never on sleeps; no timing value asserted.
    with Runtime(num_lanes=1) as rt:
        holder = rt.submit_operation("busy_sum", 2_000_000_000)
        wait_until(lambda: _any_active(rt), where="holder active")
        queued = rt.submit_operation(op, 1, 8, 100)
        assert queued.cancel() is True
        assert queued.cancelled() is True
        res = queued.result()                       # consume-once: capture, then probe
        assert res.row["status"] == "cancelled"
        with pytest.raises(OperationCancelledError):
            _ = res.value
        holder.cancel()
        assert holder.result().row["status"] == "cancelled"


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_terminal_cancel_false(op):
    # Once a chain op is terminal, cancel() must report False (queued-cancelable
    # only; it never claims a cancel after completion).
    with Runtime() as rt:
        f = rt.submit_operation(op, 5, 3, 64)
        f.result()
        assert f.cancel() is False


# --- 7. future get / wait / as_completed parity -----------------------------


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_wait_and_get(op):
    with Runtime() as rt:
        futs = [rt.submit_operation(op, i, 3, 32) for i in range(4)]
        ready, not_ready = rt.wait(futs, num_returns=len(futs))  # block for all
        assert len(ready) == len(futs)
        assert not not_ready
        results = rt.get(futs)
        assert all(isinstance(r, OperationResult) for r in results)
        assert all(isinstance(r.value, int) for r in results)


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_as_completed(op):
    with Runtime() as rt:
        futs = [rt.submit_operation(op, i, 2, 16) for i in range(4)]
        seen = 0
        for fut in rt.as_completed(futs):           # yields futures, not results
            assert isinstance(fut.result(), OperationResult)
            seen += 1
        assert seen == len(futs)
