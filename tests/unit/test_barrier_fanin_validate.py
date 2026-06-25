"""Pure unit tests for the exp44 ``barrier_fanin`` boundary validation.

Import-light: uses the ``validate`` module loaded BY FILE PATH (see conftest), so this
suite runs WITHOUT the ``_rayx`` extension or HPX, in the lightweight repo-sanity CI
job. Mirrors the native defensive re-check in ``runtime_ops_hpx.hpp`` at the
pure-function level: ``barrier_fanin(seed, leaves, quantum)`` is a 3-``int64``
signature with an unbounded-within-int64 ``seed``, ``leaves`` in
``[1, FANIN_LEAVES_MAX]``, and a bounded non-negative ``quantum``.
"""
import pytest

# validate_call takes the op_table as a parameter, so the pure test needs no native
# registry -- a representative 3-int64 signature mirrors _rayx.runtime_op_table().
_FANIN_TABLE = {
    "barrier_fanin": {"arg_types": ["int64", "int64", "int64"],
                      "result_type": "int64"},
}


def test_barrier_fanin_accepts_valid_triple(validate):
    assert validate.validate_call(
        "barrier_fanin", [5, 8, 100], _FANIN_TABLE) == [5, 8, 100]


def test_barrier_fanin_leaves_one_accepted(validate):
    # leaves == 1 is a valid op-level argument (no interleaving to witness, but the
    # boundary accepts it; the load-bearing experiment/test uses leaves >= 2).
    assert validate.validate_call(
        "barrier_fanin", [0, 1, 16], _FANIN_TABLE) == [0, 1, 16]


def test_barrier_fanin_zero_quantum_accepted(validate):
    # quantum == 0 -> each leaf's chain_stage is the identity add (value-neutral stage).
    assert validate.validate_call(
        "barrier_fanin", [7, 4, 0], _FANIN_TABLE) == [7, 4, 0]


def test_barrier_fanin_wrong_arity(validate):
    with pytest.raises(ValueError):
        validate.validate_call("barrier_fanin", [1, 2], _FANIN_TABLE)        # 3 expected
    with pytest.raises(ValueError):
        validate.validate_call("barrier_fanin", [1, 2, 3, 4], _FANIN_TABLE)  # 3 expected


def test_barrier_fanin_non_int_rejected(validate):
    with pytest.raises(TypeError):
        validate.validate_call("barrier_fanin", [1, 2.0, 3], _FANIN_TABLE)
    with pytest.raises(TypeError):
        validate.validate_call("barrier_fanin", [1, 2, "3"], _FANIN_TABLE)


def test_barrier_fanin_bool_rejected(validate):
    # bool is a non-int for the closed int64 channel (mirrors the other ops).
    with pytest.raises(TypeError):
        validate.validate_call("barrier_fanin", [1, True, 3], _FANIN_TABLE)


def test_barrier_fanin_leaves_below_one_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call("barrier_fanin", [0, 0, 16], _FANIN_TABLE)
    with pytest.raises(ValueError):
        validate.validate_call("barrier_fanin", [0, -1, 16], _FANIN_TABLE)


def test_barrier_fanin_leaves_over_max_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "barrier_fanin", [0, validate.FANIN_LEAVES_MAX + 1, 16], _FANIN_TABLE)


def test_barrier_fanin_leaves_at_max_accepted(validate):
    out = validate.validate_call(
        "barrier_fanin", [0, validate.FANIN_LEAVES_MAX, 16], _FANIN_TABLE)
    assert out == [0, validate.FANIN_LEAVES_MAX, 16]


def test_barrier_fanin_negative_quantum_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call("barrier_fanin", [0, 4, -1], _FANIN_TABLE)


def test_barrier_fanin_quantum_over_max_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "barrier_fanin", [0, 4, validate.CHAIN_QUANTUM_MAX + 1], _FANIN_TABLE)


def test_barrier_fanin_quantum_at_max_accepted(validate):
    out = validate.validate_call(
        "barrier_fanin", [0, 4, validate.CHAIN_QUANTUM_MAX], _FANIN_TABLE)
    assert out == [0, 4, validate.CHAIN_QUANTUM_MAX]


def test_barrier_fanin_seed_is_unbounded_within_int64(validate):
    # seed (arg 0) takes any int64; only leaves/quantum are domain-bounded.
    assert validate.validate_call(
        "barrier_fanin", [validate.INT64_MAX, 4, 16], _FANIN_TABLE) == [
            validate.INT64_MAX, 4, 16]
    assert validate.validate_call(
        "barrier_fanin", [validate.INT64_MIN, 4, 16], _FANIN_TABLE) == [
            validate.INT64_MIN, 4, 16]


def test_barrier_fanin_seed_out_of_int64_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "barrier_fanin", [validate.INT64_MAX + 1, 4, 16], _FANIN_TABLE)
