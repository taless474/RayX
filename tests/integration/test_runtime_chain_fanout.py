"""Integration contract tests for the exp40 ``chain_fanout`` op (the HPX-native
intra-op overlap arm). Require the built ``_rayx`` extension + HPX; the whole module
skips cleanly via ``importorskip`` if the extension is not built.

These gate the experiment's CREDIBILITY, NOT its timing: the equal-work FOLD invariant
(``chain_fanout(seed, K, S, q)`` == masked fold of K independent
``chain_sum_loop(seed+j, S, q)`` results), determinism, the value/row contract, edge
cases, the queued-only cancellation posture, and future ``get`` / ``wait`` /
``as_completed`` parity. No timing values are asserted -- overlap/latency lives in the
exp40 probe driver, never in the contract tests.
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

BUSY_SUM_MASK = 0x7FFFFFFF  # mirror runtime_ops.hpp; the fold modulus


def _any_active(rt):
    return any(s["active"] for s in rt.lane_stats())


def _reference_fold(rt, seed, count, steps, quantum):
    """The deterministic reference: masked fold of K independent reference chains, each
    computed by the native chain_sum_loop on seed_j = seed + j. This is the value the
    HPX-native chain_fanout must reproduce."""
    acc = 0
    for j in range(count):
        v = rt.submit_operation("chain_sum_loop", seed + j, steps, quantum).result().value
        acc = (acc + v) & BUSY_SUM_MASK
    return acc


# --- 1. op-table / signature structural (guards the op-set golden) -----------


def test_chain_fanout_registered_arity4():
    table = dict(runtime_op_table())
    assert "chain_fanout" in table
    assert table["chain_fanout"]["arg_types"] == ["int64", "int64", "int64", "int64"]
    assert table["chain_fanout"]["result_type"] == "int64"


# --- 2. equal-work fold invariant (the credibility gate) --------------------


@pytest.mark.parametrize("seed,count,steps,quantum", [
    (0, 1, 0, 0),      # single child, empty chain -> seed
    (0, 1, 1, 0),      # single child, one identity stage
    (7, 4, 1, 64),     # 4 children, one real stage each
    (3, 8, 5, 100),
    (123, 8, 8, 257),
    (12345, 16, 6, 128),
])
def test_fanout_equals_masked_fold_of_reference_chains(seed, count, steps, quantum):
    with Runtime() as rt:
        fanout_v = rt.submit_operation(
            "chain_fanout", seed, count, steps, quantum).result().value
        ref_v = _reference_fold(rt, seed, count, steps, quantum)
        assert fanout_v == ref_v


# --- 3. determinism ---------------------------------------------------------


def test_chain_fanout_deterministic():
    with Runtime() as rt:
        a = rt.submit_operation("chain_fanout", 9, 8, 6, 128).result().value
        b = rt.submit_operation("chain_fanout", 9, 8, 6, 128).result().value
        assert a == b


# --- 4. value / row contract ------------------------------------------------


def test_chain_fanout_value_and_row_contract():
    with Runtime() as rt:
        res = rt.submit_operation("chain_fanout", 2, 8, 3, 50).result()
        assert isinstance(res, OperationResult)
        assert isinstance(res.value, int)
        assert set(res.row) == set(ROW_FIELDS)     # exactly the 9 row fields
        assert "value" not in res.row
        assert res.row["status"] == "completed"
        assert res.row["actor_id"].startswith("rt-hpx-")


# --- 5. edge cases ----------------------------------------------------------


def test_chain_fanout_count_one_matches_single_chain():
    # count == 1 -> the fold is just chain_sum_loop(seed, S, q) (already < 2^31 after a
    # real stage, so the mask is identity here).
    with Runtime() as rt:
        f1 = rt.submit_operation("chain_fanout", 42, 1, 4, 64).result().value
        loop = rt.submit_operation("chain_sum_loop", 42, 4, 64).result().value
        assert f1 == loop


def test_chain_fanout_zero_steps_folds_seeds():
    # steps == 0 -> each child returns seed_j == seed + j; the op folds them under the
    # mask. Small seed so seed + j is exact on both sides.
    with Runtime() as rt:
        v = rt.submit_operation("chain_fanout", 10, 4, 0, 999).result().value
        assert v == ((10 + 11 + 12 + 13) & BUSY_SUM_MASK)


# --- 6. queued-only cancellation posture ------------------------------------


def test_chain_fanout_queued_cancel(wait_until):
    # Hold a single serial lane with a long checkpointed busy_sum, queue a chain_fanout
    # behind it, and cancel the queued op -> it settles cancelled immediately (queued
    # cancel always works). Gated on lane_stats(), never on sleeps; no timing asserted.
    with Runtime(num_lanes=1) as rt:
        holder = rt.submit_operation("busy_sum", 2_000_000_000)
        wait_until(lambda: _any_active(rt), where="holder active")
        queued = rt.submit_operation("chain_fanout", 1, 8, 8, 100)
        assert queued.cancel() is True
        assert queued.cancelled() is True
        res = queued.result()                       # consume-once: capture, then probe
        assert res.row["status"] == "cancelled"
        with pytest.raises(OperationCancelledError):
            _ = res.value
        holder.cancel()
        assert holder.result().row["status"] == "cancelled"


def test_chain_fanout_terminal_cancel_false():
    # Once terminal, cancel() must report False (queued-cancelable only).
    with Runtime() as rt:
        f = rt.submit_operation("chain_fanout", 5, 8, 3, 64)
        f.result()
        assert f.cancel() is False


# --- 7. future get / wait / as_completed parity -----------------------------


def test_chain_fanout_wait_and_get():
    with Runtime() as rt:
        futs = [rt.submit_operation("chain_fanout", i, 8, 3, 32) for i in range(4)]
        ready, not_ready = rt.wait(futs, num_returns=len(futs))  # block for all
        assert len(ready) == len(futs)
        assert not not_ready
        results = rt.get(futs)
        assert all(isinstance(r, OperationResult) for r in results)
        assert all(isinstance(r.value, int) for r in results)


def test_chain_fanout_as_completed():
    with Runtime() as rt:
        futs = [rt.submit_operation("chain_fanout", i, 8, 2, 16) for i in range(4)]
        seen = 0
        for fut in rt.as_completed(futs):           # yields futures, not results
            assert isinstance(fut.result(), OperationResult)
            seen += 1
        assert seen == len(futs)
