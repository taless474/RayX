"""Pure unit tests for the exp40 ``chain_fanout`` boundary validation.

Import-light: uses the ``validate`` module loaded BY FILE PATH (see conftest), so this
suite runs WITHOUT the ``_rayx`` extension or HPX, in the lightweight repo-sanity CI
job. Mirrors the native defensive re-check in ``runtime_ops_hpx.hpp`` at the
pure-function level: ``chain_fanout(seed, count, steps, quantum)`` is a 4-``int64``
signature with an unbounded-within-int64 ``seed``, ``count`` in
``[1, CHAIN_FANOUT_K_MAX]``, and bounded non-negative ``steps`` / ``quantum``.
"""
import pytest

# validate_call takes the op_table as a parameter, so the pure test needs no native
# registry -- a representative 4-int64 signature mirrors _rayx.runtime_op_table().
_FANOUT_TABLE = {
    "chain_fanout": {"arg_types": ["int64", "int64", "int64", "int64"],
                     "result_type": "int64"},
}


def test_chain_fanout_accepts_valid_quad(validate):
    assert validate.validate_call(
        "chain_fanout", [5, 8, 64, 100], _FANOUT_TABLE) == [5, 8, 64, 100]


def test_chain_fanout_count_one_accepted(validate):
    # count == 1 is the single-child baseline (T1 in the overlap ratio).
    assert validate.validate_call(
        "chain_fanout", [0, 1, 4, 16], _FANOUT_TABLE) == [0, 1, 4, 16]


def test_chain_fanout_zero_steps_and_quantum_accepted(validate):
    # steps == 0 (each child returns its seed_j) and quantum == 0 (identity stage).
    assert validate.validate_call(
        "chain_fanout", [7, 4, 0, 0], _FANOUT_TABLE) == [7, 4, 0, 0]


def test_chain_fanout_wrong_arity(validate):
    with pytest.raises(ValueError):
        validate.validate_call("chain_fanout", [1, 2, 3], _FANOUT_TABLE)        # 4
    with pytest.raises(ValueError):
        validate.validate_call("chain_fanout", [1, 2, 3, 4, 5], _FANOUT_TABLE)  # 4


def test_chain_fanout_non_int_rejected(validate):
    with pytest.raises(TypeError):
        validate.validate_call("chain_fanout", [1, 2.0, 3, 4], _FANOUT_TABLE)   # float
    with pytest.raises(TypeError):
        validate.validate_call("chain_fanout", [1, True, 3, 4], _FANOUT_TABLE)  # bool


def test_chain_fanout_count_below_one_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call("chain_fanout", [0, 0, 4, 16], _FANOUT_TABLE)
    with pytest.raises(ValueError):
        validate.validate_call("chain_fanout", [0, -1, 4, 16], _FANOUT_TABLE)


def test_chain_fanout_count_over_max_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "chain_fanout", [0, validate.CHAIN_FANOUT_K_MAX + 1, 4, 16],
            _FANOUT_TABLE)


def test_chain_fanout_count_at_max_accepted(validate):
    out = validate.validate_call(
        "chain_fanout", [0, validate.CHAIN_FANOUT_K_MAX, 4, 16], _FANOUT_TABLE)
    assert out == [0, validate.CHAIN_FANOUT_K_MAX, 4, 16]


def test_chain_fanout_negative_steps_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call("chain_fanout", [0, 2, -1, 16], _FANOUT_TABLE)


def test_chain_fanout_negative_quantum_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call("chain_fanout", [0, 2, 4, -1], _FANOUT_TABLE)


def test_chain_fanout_steps_over_max_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "chain_fanout", [0, 2, validate.CHAIN_STEPS_MAX + 1, 16], _FANOUT_TABLE)


def test_chain_fanout_quantum_over_max_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "chain_fanout", [0, 2, 4, validate.CHAIN_QUANTUM_MAX + 1], _FANOUT_TABLE)


def test_chain_fanout_steps_and_quantum_at_max_accepted(validate):
    out = validate.validate_call(
        "chain_fanout",
        [0, 2, validate.CHAIN_STEPS_MAX, validate.CHAIN_QUANTUM_MAX], _FANOUT_TABLE)
    assert out == [0, 2, validate.CHAIN_STEPS_MAX, validate.CHAIN_QUANTUM_MAX]


def test_chain_fanout_seed_is_unbounded_within_int64(validate):
    # seed (arg 0) carries no domain bound beyond the generic int64 range.
    assert validate.validate_call(
        "chain_fanout", [validate.INT64_MAX, 2, 1, 1], _FANOUT_TABLE) \
        == [validate.INT64_MAX, 2, 1, 1]
    assert validate.validate_call(
        "chain_fanout", [validate.INT64_MIN, 2, 1, 1], _FANOUT_TABLE) \
        == [validate.INT64_MIN, 2, 1, 1]
    with pytest.raises(ValueError):
        validate.validate_call(
            "chain_fanout", [validate.INT64_MAX + 1, 2, 1, 1], _FANOUT_TABLE)
