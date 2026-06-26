"""Pure unit tests for the exp46 ``diamond_fanin`` boundary validation.

Import-light: uses the ``validate`` module loaded BY FILE PATH (see conftest), so this
suite runs WITHOUT the ``_rayx`` extension or HPX, in the lightweight repo-sanity CI
job. Mirrors the native defensive re-check in ``runtime_ops_hpx.hpp`` at the
pure-function level: ``diamond_fanin(seed, quantum)`` is a 2-``int64`` signature with an
unbounded-within-int64 ``seed`` and a bounded non-negative ``quantum`` (reusing the
chain ops' ``CHAIN_QUANTUM_MAX``).
"""
import pytest

# validate_call takes the op_table as a parameter, so the pure test needs no native
# registry -- a representative 2-int64 signature mirrors _rayx.runtime_op_table().
_DIAMOND_TABLE = {
    "diamond_fanin": {"arg_types": ["int64", "int64"], "result_type": "int64"},
}


def test_diamond_fanin_accepts_valid_pair(validate):
    assert validate.validate_call(
        "diamond_fanin", [3, 64], _DIAMOND_TABLE) == [3, 64]


def test_diamond_fanin_zero_quantum_accepted(validate):
    # quantum == 0 -> each node's chain_stage is the identity add (value-neutral stage).
    assert validate.validate_call(
        "diamond_fanin", [7, 0], _DIAMOND_TABLE) == [7, 0]


def test_diamond_fanin_wrong_arity(validate):
    with pytest.raises(ValueError):
        validate.validate_call("diamond_fanin", [1], _DIAMOND_TABLE)        # 2 expected
    with pytest.raises(ValueError):
        validate.validate_call("diamond_fanin", [1, 2, 3], _DIAMOND_TABLE)  # 2 expected


def test_diamond_fanin_non_int_rejected(validate):
    with pytest.raises(TypeError):
        validate.validate_call("diamond_fanin", [1, 2.0], _DIAMOND_TABLE)
    with pytest.raises(TypeError):
        validate.validate_call("diamond_fanin", [1, "2"], _DIAMOND_TABLE)


def test_diamond_fanin_bool_rejected(validate):
    # bool is a non-int for the closed int64 channel (mirrors the other ops).
    with pytest.raises(TypeError):
        validate.validate_call("diamond_fanin", [True, 64], _DIAMOND_TABLE)
    with pytest.raises(TypeError):
        validate.validate_call("diamond_fanin", [3, True], _DIAMOND_TABLE)


def test_diamond_fanin_negative_quantum_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call("diamond_fanin", [3, -1], _DIAMOND_TABLE)


def test_diamond_fanin_quantum_over_max_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "diamond_fanin", [3, validate.CHAIN_QUANTUM_MAX + 1], _DIAMOND_TABLE)


def test_diamond_fanin_quantum_at_max_accepted(validate):
    out = validate.validate_call(
        "diamond_fanin", [3, validate.CHAIN_QUANTUM_MAX], _DIAMOND_TABLE)
    assert out == [3, validate.CHAIN_QUANTUM_MAX]


def test_diamond_fanin_seed_is_unbounded_within_int64(validate):
    # seed (arg 0) takes any int64; only quantum is domain-bounded.
    assert validate.validate_call(
        "diamond_fanin", [validate.INT64_MAX, 64], _DIAMOND_TABLE) == [
            validate.INT64_MAX, 64]
    assert validate.validate_call(
        "diamond_fanin", [validate.INT64_MIN, 64], _DIAMOND_TABLE) == [
            validate.INT64_MIN, 64]


def test_diamond_fanin_seed_out_of_int64_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "diamond_fanin", [validate.INT64_MAX + 1, 64], _DIAMOND_TABLE)
