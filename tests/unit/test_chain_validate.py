"""Pure unit tests for the exp39 chain_sum_* boundary validation
(``chain_sum_loop`` / ``chain_sum_then``).

Import-light: uses the ``validate`` module loaded BY FILE PATH (see conftest), so this
suite runs WITHOUT the ``_rayx`` extension or HPX, in the lightweight repo-sanity CI
job. Mirrors the native defensive re-check in ``runtime_ops.hpp`` /
``runtime_ops_hpx.hpp`` at the pure-function level: both ops share an identical
3-``int64`` signature ``(seed, steps, quantum)`` with bounded non-negative
``steps`` / ``quantum`` and an unbounded-within-int64 ``seed``.
"""
import pytest

# chain_sum_loop / chain_sum_then share an identical 3-int64 signature; the validator
# takes the op_table as a parameter, so the pure tests need no native registry.
_CHAIN_TABLE = {
    "chain_sum_loop": {"arg_types": ["int64", "int64", "int64"],
                       "result_type": "int64"},
    "chain_sum_then": {"arg_types": ["int64", "int64", "int64"],
                       "result_type": "int64"},
}

CHAIN_OPS = ["chain_sum_loop", "chain_sum_then"]


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_accepts_valid_triple(validate, op):
    assert validate.validate_call(op, [5, 3, 100], _CHAIN_TABLE) == [5, 3, 100]


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_zero_steps_and_quantum_accepted(validate, op):
    # steps == 0 (empty chain -> seed) and quantum == 0 (identity stage) are valid.
    assert validate.validate_call(op, [7, 0, 0], _CHAIN_TABLE) == [7, 0, 0]


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_wrong_arity(validate, op):
    with pytest.raises(ValueError):
        validate.validate_call(op, [1, 2], _CHAIN_TABLE)        # expects 3
    with pytest.raises(ValueError):
        validate.validate_call(op, [1, 2, 3, 4], _CHAIN_TABLE)  # expects 3


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_non_int_rejected(validate, op):
    with pytest.raises(TypeError):
        validate.validate_call(op, [1, 2.0, 3], _CHAIN_TABLE)   # float steps
    with pytest.raises(TypeError):
        validate.validate_call(op, [1, True, 3], _CHAIN_TABLE)  # bool (int subclass)


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_negative_steps_rejected(validate, op):
    with pytest.raises(ValueError):
        validate.validate_call(op, [0, -1, 10], _CHAIN_TABLE)


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_negative_quantum_rejected(validate, op):
    with pytest.raises(ValueError):
        validate.validate_call(op, [0, 1, -1], _CHAIN_TABLE)


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_steps_over_max_rejected(validate, op):
    with pytest.raises(ValueError):
        validate.validate_call(op, [0, validate.CHAIN_STEPS_MAX + 1, 10],
                               _CHAIN_TABLE)


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_quantum_over_max_rejected(validate, op):
    with pytest.raises(ValueError):
        validate.validate_call(op, [0, 1, validate.CHAIN_QUANTUM_MAX + 1],
                               _CHAIN_TABLE)


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_steps_and_quantum_at_max_accepted(validate, op):
    out = validate.validate_call(
        op, [0, validate.CHAIN_STEPS_MAX, validate.CHAIN_QUANTUM_MAX], _CHAIN_TABLE)
    assert out == [0, validate.CHAIN_STEPS_MAX, validate.CHAIN_QUANTUM_MAX]


@pytest.mark.parametrize("op", CHAIN_OPS)
def test_chain_seed_is_unbounded_within_int64(validate, op):
    # seed (arg 0) carries no domain bound beyond the generic int64 range.
    assert validate.validate_call(op, [validate.INT64_MAX, 1, 1], _CHAIN_TABLE) \
        == [validate.INT64_MAX, 1, 1]
    assert validate.validate_call(op, [validate.INT64_MIN, 1, 1], _CHAIN_TABLE) \
        == [validate.INT64_MIN, 1, 1]
    with pytest.raises(ValueError):
        validate.validate_call(op, [validate.INT64_MAX + 1, 1, 1], _CHAIN_TABLE)
