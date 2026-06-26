"""Integration contract tests for the exp46 ``diamond_fanin`` op (one fixed non-linear
diamond DAG: A -> {B, C} -> D, with the cross-edge join resolved natively below the
boundary). Require the built ``_rayx`` extension + HPX; the whole module skips cleanly
via ``importorskip`` if the extension is not built.

These gate the op's CREDIBILITY, NOT timing. The counts are analytically predictable;
the contract here is value FAITHFULNESS through the real Runtime/lane boundary:

  * the op-table signature is the 2-int64 golden;
  * ``diamond_fanin(seed, quantum)`` equals BOTH a pure-Python oracle AND a native
    node-by-node decomposition into ``chain_sum_loop`` nodes (value triangulation only --
    the fair Path B boundary-count artifact lives in the experiment runner, not here);
  * the result is deterministic and stable at ``hpx_threads`` 1 and 2 (no overlap or
    timing is asserted -- the canonical run is single-worker on purpose).
"""
import pytest

# Skips the whole module cleanly if _rayx is not built (ImportError -> skip).
rayx_runtime = pytest.importorskip("rayx.runtime")

from rayx.runtime import Runtime  # noqa: E402
from rayx.runtime._validate import CHAIN_QUANTUM_MAX  # noqa: E402
from rayx._rayx import runtime_op_table  # noqa: E402  native registry view

MASK = 0x7FFFFFFF  # mirror BUSY_SUM_MASK


# --- pure-Python oracle (mirrors native masking exactly) ---------------------

def _masked_range_sum(begin, end):
    acc = 0
    for i in range(begin, end):
        acc = (acc + i) & MASK
    return acc


def _chain_stage(x, q):
    return ((x & ((1 << 64) - 1)) + _masked_range_sum(0, q)) & MASK


def _diamond_oracle(seed, q):
    a = _chain_stage(seed, q)
    b = _chain_stage(a + 1, q)
    c = _chain_stage(a + 2, q)
    return _chain_stage((b + c) & MASK, q)


# --- 1. op-table / signature structural (guards the op-set golden) -----------

def test_diamond_fanin_in_op_table():
    table = runtime_op_table()
    assert "diamond_fanin" in table
    sig = table["diamond_fanin"]
    assert sig["arg_types"] == ["int64", "int64"]
    assert sig["result_type"] == "int64"


# --- 2. value faithfulness: oracle == native == Path-B decomposition ---------

@pytest.mark.parametrize("seed,quantum", [
    (0, 0),
    (3, 16),
    (3, 64),
    (1, 64),
    (7, 16),
    (123456, 1000),
    (-9, 50),
])
def test_diamond_fanin_matches_oracle_and_decomposition(seed, quantum):
    with Runtime(hpx_threads=1) as rt:
        native = rt.submit_operation("diamond_fanin", seed, quantum).result().value
        # Native node-by-node decomposition (value triangulation only): each node is one
        # chain_sum_loop (== one chain_stage). This checks VALUE faithfulness, not boundary
        # counts -- the fair Path B boundary-count artifact lives in the experiment runner
        # (experiments/46_diamond_join_dag/run_diamond_join_dag.py), not here.
        a = rt.submit_operation("chain_sum_loop", seed, 1, quantum).result().value
        b = rt.submit_operation("chain_sum_loop", a + 1, 1, quantum).result().value
        c = rt.submit_operation("chain_sum_loop", a + 2, 1, quantum).result().value
        d = rt.submit_operation(
            "chain_sum_loop", (b + c) & MASK, 1, quantum).result().value
        assert native == d                       # native coarse == native decomposition
        assert native == _diamond_oracle(seed, quantum)  # == pure-Python oracle


def test_diamond_fanin_is_deterministic():
    with Runtime(hpx_threads=1) as rt:
        vals = {rt.submit_operation("diamond_fanin", 3, 64).result().value
                for _ in range(5)}
        assert len(vals) == 1


def test_diamond_fanin_value_stable_across_threads():
    # The value is order-free ((B+C) commutative), so it must not depend on worker count.
    with Runtime(hpx_threads=1) as rt1:
        v1 = rt1.submit_operation("diamond_fanin", 3, 64).result().value
    with Runtime(hpx_threads=2) as rt2:
        v2 = rt2.submit_operation("diamond_fanin", 3, 64).result().value
    assert v1 == v2 == _diamond_oracle(3, 64)


# --- 3. domain + value-model edges ------------------------------------------

def test_diamond_fanin_zero_quantum_completes():
    # quantum == 0 -> every node is the identity add; the diamond still composes cleanly.
    with Runtime(hpx_threads=1) as rt:
        res = rt.submit_operation("diamond_fanin", 9, 0).result()
        assert res.status == "completed"
        assert res.value == _diamond_oracle(9, 0)


def test_diamond_fanin_quantum_at_max():
    with Runtime(hpx_threads=2) as rt:
        v = rt.submit_operation(
            "diamond_fanin", 0, CHAIN_QUANTUM_MAX).result().value
        assert v == _diamond_oracle(0, CHAIN_QUANTUM_MAX)


@pytest.mark.parametrize("bad", [
    (3, -1),                       # quantum < 0
    (3, CHAIN_QUANTUM_MAX + 1),    # quantum > max
])
def test_diamond_fanin_boundary_rejects(bad):
    with Runtime(hpx_threads=1) as rt:
        with pytest.raises(ValueError):
            rt.submit_operation("diamond_fanin", *bad)
