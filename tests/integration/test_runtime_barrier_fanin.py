"""Integration contract tests for the exp44 ``barrier_fanin`` op (the witnessed
barrier-gated fan-in keystone). Require the built ``_rayx`` extension + HPX; the whole
module skips cleanly via ``importorskip`` if the extension is not built.

These gate the experiment's CREDIBILITY, NOT its timing:
  * the value oracle ``barrier_fanin(seed, leaves, quantum) ==
    chain_fanout(seed, leaves, 1, quantum)`` (gate is value-neutral);
  * the load-bearing ``hpx_threads=1`` structural witness (one observed OS worker; all
    leaves arrived/released; opened by the last arriver, NOT the watchdog; no ordering
    violations; reduction ran after all leaves; clean exit);
  * shutdown-during-a-batch teardown is clean (no orphan, no hang).
No timing values are asserted. ``max_simultaneously_suspended_leaves`` is recorded but
never asserted (coordinated suspension, not parallelism).
"""
import pytest

# Skips the whole module cleanly if _rayx is not built (ImportError -> skip).
rayx_runtime = pytest.importorskip("rayx.runtime")

from rayx.runtime import Runtime  # noqa: E402
from rayx.runtime._validate import FANIN_LEAVES_MAX  # noqa: E402
from rayx._rayx import runtime_op_table  # noqa: E402  native registry view


# --- 1. op-table / signature structural (guards the op-set golden) -----------

def test_barrier_fanin_in_op_table():
    table = runtime_op_table()
    assert "barrier_fanin" in table
    sig = table["barrier_fanin"]
    assert sig["arg_types"] == ["int64", "int64", "int64"]
    assert sig["result_type"] == "int64"


# --- 2. value oracle: barrier_fanin == chain_fanout(., ., 1, .) --------------

@pytest.mark.parametrize("seed,leaves,quantum", [
    (0, 2, 0),
    (5, 8, 100),
    (7, 4, 16),
    (123456, 16, 1000),
    (-9, 3, 50),
])
def test_barrier_fanin_matches_chain_fanout_oracle(seed, leaves, quantum):
    with Runtime(hpx_threads=2) as rt:
        bf = rt.submit_operation("barrier_fanin", seed, leaves, quantum).result().value
        cf = rt.submit_operation("chain_fanout", seed, leaves, 1, quantum).result().value
        assert bf == cf


def test_barrier_fanin_is_deterministic():
    with Runtime(hpx_threads=2) as rt:
        vals = {rt.submit_operation("barrier_fanin", 11, 12, 64).result().value
                for _ in range(5)}
        assert len(vals) == 1


# --- 3. LOAD-BEARING: hpx_threads=1 structural witness (leaves >= 2) ---------

@pytest.mark.parametrize("leaves", [2, 4, 8, 16])
def test_barrier_fanin_threads1_witness_gates(leaves):
    """The keystone gate: at one observed HPX OS worker, a clean completion of the
    mutually-gated interior witnesses cooperative suspend/resume on a single worker."""
    with Runtime(hpx_threads=1) as rt:
        res = rt.submit_operation("barrier_fanin", 3, leaves, 64).result()
        assert res.status == "completed"
        w = rt.barrier_fanin_witness()
        # Single-in-flight: this snapshot is the call we just made.
        assert w["leaves_requested"] == leaves
        assert w["observed_os_workers"] == 1            # #1: single-worker premise
        assert w["arrived_count"] == leaves
        assert w["released_count"] == leaves
        assert w["opener"] == "last_arriver"            # #3: not the watchdog
        assert w["watchdog_opened"] is False            # #3: no masked broken path
        assert w["ordering_violations"] == 0
        assert w["reduction_after_all_leaves"] is True
        assert w["clean_exit"] is True
        # Observation-only: recorded, NOT asserted (coordinated suspension, not parallelism).
        assert "max_simultaneously_suspended_leaves" in w


def test_barrier_fanin_witness_seq_advances():
    with Runtime(hpx_threads=1) as rt:
        rt.submit_operation("barrier_fanin", 1, 4, 16).result()
        s1 = rt.barrier_fanin_witness()["seq"]
        rt.submit_operation("barrier_fanin", 1, 4, 16).result()
        s2 = rt.barrier_fanin_witness()["seq"]
        assert s2 == s1 + 1


# --- 4. domain + value-model edges ------------------------------------------

def test_barrier_fanin_leaves_one_completes():
    # leaves == 1 is valid at the op level: the single leaf opens its own gate and returns.
    with Runtime(hpx_threads=1) as rt:
        v = rt.submit_operation("barrier_fanin", 9, 1, 32).result().value
        cf = rt.submit_operation("chain_fanout", 9, 1, 1, 32).result().value
        assert v == cf
        w = rt.barrier_fanin_witness()
        assert w["arrived_count"] == 1 and w["released_count"] == 1


def test_barrier_fanin_leaves_at_max():
    with Runtime(hpx_threads=2) as rt:
        bf = rt.submit_operation(
            "barrier_fanin", 0, FANIN_LEAVES_MAX, 8).result().value
        cf = rt.submit_operation(
            "chain_fanout", 0, FANIN_LEAVES_MAX, 1, 8).result().value
        assert bf == cf


@pytest.mark.parametrize("bad", [
    (0, 0, 16),                       # leaves < 1
    (0, FANIN_LEAVES_MAX + 1, 16),    # leaves > max
    (0, 4, -1),                       # quantum < 0
])
def test_barrier_fanin_boundary_rejects(bad):
    with Runtime(hpx_threads=1) as rt:
        with pytest.raises(ValueError):
            rt.submit_operation("barrier_fanin", *bad)


# --- 5. shutdown-during-a-batch: clean teardown, no orphan, no hang ----------

def test_barrier_fanin_shutdown_during_batch_is_clean():
    """Submit a batch and shut the runtime down without draining the futures. Teardown
    must complete (no hang) and not orphan a worker; barrier_fanin ops are short (the
    gate self-opens at full arrival), so shutdown's cancel+drain is bounded."""
    rt = Runtime(hpx_threads=2)
    futs = [rt.submit_operation("barrier_fanin", i, 8, 200) for i in range(16)]
    # Intentionally do NOT resolve all futures before shutting down.
    rt.shutdown()
    # Reaching here without hanging is the deterministic assertion; the witness accessor
    # must now refuse cleanly (runtime is shut down).
    with pytest.raises(RuntimeError):
        rt.barrier_fanin_witness()
    assert futs  # batch was submitted
