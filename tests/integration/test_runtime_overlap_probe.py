"""Integration contract tests for the exp47 ``overlap_probe`` op (in-process
nested-overlap observation: two INDEPENDENT bare-``hpx::async`` arms + ``when_all``
below ONE Runtime/lane boundary -- "barrier_fanin without the gate"). Require the built
``_rayx`` extension + HPX; the whole module skips cleanly via ``importorskip`` if the
extension is not built.

These gate the op's CREDIBILITY through the real Runtime/lane boundary, NOT timing or
scheduling. The LOAD-BEARING result is faithfulness/liveness: the nested HPX async arms
schedule, join, and return the bit-identical closed ``int64``; the witness shape and the
deterministic structural gates hold. The scheduler-sensitive overlap classification
(serial / cooperative_interleaving / worker_parallel) is value-checked for membership in
the closed set only -- it is NEVER asserted to a specific value here (that is reported by
the experiment runner, which isolates ``hpx_threads`` regimes in subprocesses).

No wall-clock value is asserted. "in flight" means active within the bracketed arm
compute; "worker_parallel" means only "distinct workers observed", never parallel-speedup.
"""
import pytest

# Skips the whole module cleanly if _rayx is not built (ImportError -> skip).
rayx_runtime = pytest.importorskip("rayx.runtime")

from rayx.runtime import Runtime  # noqa: E402
from rayx.runtime._validate import CHAIN_QUANTUM_MAX  # noqa: E402
from rayx._rayx import runtime_op_table  # noqa: E402  native registry view

MASK = 0x7FFFFFFF  # mirror BUSY_SUM_MASK
U64 = (1 << 64) - 1
_CLASSES = {"serial", "cooperative_interleaving", "worker_parallel", "inconclusive"}


# --- pure-Python oracle (mirrors native masking exactly) ---------------------

def _masked_range_sum(begin, end):
    acc = 0
    for i in range(begin, end):
        acc = (acc + (i & U64)) & MASK
    return acc


def _chain_stage(x, q):
    return ((x & U64) + _masked_range_sum(0, q)) & MASK


def _overlap_oracle(seed, q):
    # Two independent arms over seed and seed+1, masked-folded; mode-independent.
    return (_chain_stage(seed, q) + _chain_stage(seed + 1, q)) & MASK


# --- 1. op-table / signature structural (guards the op-set golden) -----------

def test_overlap_probe_in_op_table():
    table = runtime_op_table()
    assert "overlap_probe" in table
    sig = table["overlap_probe"]
    assert sig["arg_types"] == ["int64", "int64", "int64"]
    assert sig["result_type"] == "int64"


# --- 2. value faithfulness: oracle == native == chain_fanout, mode-independent ---

@pytest.mark.parametrize("seed,quantum", [
    (0, 0),
    (3, 100),
    (7, 9000),
    (1, 16384),
    (123456, 50000),
    (-9, 50),
])
@pytest.mark.parametrize("mode", [0, 1])
def test_overlap_probe_matches_oracle_and_fanout(seed, quantum, mode):
    ref = _overlap_oracle(seed, quantum)
    with Runtime(hpx_threads=2) as rt:
        native = rt.submit_operation(
            "overlap_probe", seed, quantum, mode).result().value
        # Same closed value as the existing fixed fan-out op (different mechanism).
        cf = rt.submit_operation(
            "chain_fanout", seed, 2, 1, quantum).result().value
        assert native == ref            # native coarse == pure-Python oracle
        assert native == cf             # == chain_fanout(seed, 2, 1, quantum)


def test_overlap_probe_value_is_mode_independent():
    with Runtime(hpx_threads=2) as rt:
        v0 = rt.submit_operation("overlap_probe", 5, 16384, 0).result().value
        v1 = rt.submit_operation("overlap_probe", 5, 16384, 1).result().value
        assert v0 == v1 == _overlap_oracle(5, 16384)


def test_overlap_probe_is_deterministic():
    with Runtime(hpx_threads=2) as rt:
        vals = {rt.submit_operation("overlap_probe", 3, 9000, 1).result().value
                for _ in range(5)}
        assert len(vals) == 1


def test_overlap_probe_value_stable_across_threads():
    # The value is order-free (masked add commutes), so it must not depend on workers.
    with Runtime(hpx_threads=1) as rt1:
        v1 = rt1.submit_operation("overlap_probe", 3, 16384, 1).result().value
    with Runtime(hpx_threads=2) as rt2:
        v2 = rt2.submit_operation("overlap_probe", 3, 16384, 1).result().value
    assert v1 == v2 == _overlap_oracle(3, 16384)


# --- 3. witness: deterministic structural gates (NOT the classification) -----

@pytest.mark.parametrize("hpx_threads", [1, 2])
@pytest.mark.parametrize("mode", [0, 1])
def test_overlap_probe_witness_structural_gates(hpx_threads, mode):
    with Runtime(hpx_threads=hpx_threads) as rt:
        res = rt.submit_operation("overlap_probe", 3, 16384, mode).result()
        assert res.status == "completed"
        w = rt.overlap_witness()
        # Single-in-flight: this snapshot is the call we just made.
        assert w["mode"] == mode
        assert w["arms_launched"] == 2
        assert w["arms_completed"] == 2
        assert w["clean_exit"] is True
        assert w["ordering_violations"] == 0
        # Each arm entered and left exactly once, in order.
        for j in (0, 1):
            assert w["per_arm_enter_seq"][j] >= 0
            assert w["per_arm_leave_seq"][j] > w["per_arm_enter_seq"][j]
        # both_in_flight is consistent with max_in_flight (derived, not timing).
        assert w["both_in_flight"] == (w["max_in_flight"] >= 2)
        # Classification is REPORTED only: assert membership, never a specific value.
        assert w["classification"] in _CLASSES
        # Event trace is well-formed: every event is one of the three phases.
        for e in w["events"]:
            assert e["phase"] in ("enter", "chunk", "leave")
            assert e["arm_id"] in (0, 1)


def test_overlap_probe_witness_seq_advances():
    with Runtime(hpx_threads=1) as rt:
        rt.submit_operation("overlap_probe", 1, 9000, 1).result()
        s1 = rt.overlap_witness()["seq"]
        rt.submit_operation("overlap_probe", 1, 9000, 1).result()
        s2 = rt.overlap_witness()["seq"]
        assert s2 == s1 + 1


def test_overlap_probe_mode0_threads1_is_not_in_flight_overlap():
    # REPORTED (not a hard gate elsewhere), but deterministic enough to check here: a
    # non-yielding kernel on ONE worker cannot be preempted, so the two arms cannot be
    # in flight together -> max_in_flight stays 1 and classification is "serial".
    with Runtime(hpx_threads=1) as rt:
        rt.submit_operation("overlap_probe", 3, 16384, 0).result()
        w = rt.overlap_witness()
        assert w["observed_os_workers"] == 1
        assert w["max_in_flight"] == 1
        assert w["both_in_flight"] is False
        assert w["classification"] == "serial"


# --- 4. domain + value-model edges ------------------------------------------

def test_overlap_probe_zero_quantum_completes():
    with Runtime(hpx_threads=1) as rt:
        for mode in (0, 1):
            res = rt.submit_operation("overlap_probe", 9, 0, mode).result()
            assert res.status == "completed"
            assert res.value == _overlap_oracle(9, 0)


def test_overlap_probe_quantum_at_max():
    with Runtime(hpx_threads=2) as rt:
        v = rt.submit_operation(
            "overlap_probe", 0, CHAIN_QUANTUM_MAX, 1).result().value
        assert v == _overlap_oracle(0, CHAIN_QUANTUM_MAX)


@pytest.mark.parametrize("bad", [
    (3, -1, 0),                       # quantum < 0
    (3, CHAIN_QUANTUM_MAX + 1, 0),    # quantum > max
    (3, 64, 2),                       # mode not in {0, 1}
    (3, 64, -1),                      # mode not in {0, 1}
])
def test_overlap_probe_boundary_rejects(bad):
    with Runtime(hpx_threads=1) as rt:
        with pytest.raises(ValueError):
            rt.submit_operation("overlap_probe", *bad)


def test_overlap_witness_refuses_after_shutdown():
    rt = Runtime(hpx_threads=1)
    rt.submit_operation("overlap_probe", 1, 9000, 1).result()
    rt.shutdown()
    with pytest.raises(RuntimeError):
        rt.overlap_witness()
