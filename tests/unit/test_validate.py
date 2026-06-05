"""Pure unit tests for rayx.runtime validators (no rayx / _rayx import).

The ``validate`` and ``op_table`` fixtures load ``_validate.py`` by file path; see
tests/unit/conftest.py. Mirrors the validation contract exercised end-to-end by
bench/smoke_rayx_runtime.py, but at the pure-function level with no HPX.
"""
import math

import pytest

# --- validate_timeout -------------------------------------------------------


def test_validate_timeout_zero_accepted(validate):
    assert validate.validate_timeout(0) == 0.0


def test_validate_timeout_positive_accepted(validate):
    assert validate.validate_timeout(2) == 2.0
    assert validate.validate_timeout(0.25) == 0.25


def test_validate_timeout_bool_rejected(validate):
    with pytest.raises(TypeError):
        validate.validate_timeout(True)


def test_validate_timeout_non_numeric_rejected(validate):
    with pytest.raises(TypeError):
        validate.validate_timeout("0")


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf, -1, -0.5])
def test_validate_timeout_value_errors(validate, bad):
    with pytest.raises(ValueError):
        validate.validate_timeout(bad)


# --- validate_call ----------------------------------------------------------


def test_validate_call_unknown_op(validate, op_table):
    with pytest.raises(ValueError):
        validate.validate_call("nope", [1], op_table)


def test_validate_call_wrong_arity(validate, op_table):
    with pytest.raises(ValueError):
        validate.validate_call("add", [1], op_table)        # add expects 2
    with pytest.raises(ValueError):
        validate.validate_call("square", [1, 2], op_table)  # square expects 1


def test_validate_call_non_int_rejected(validate, op_table):
    with pytest.raises(TypeError):
        validate.validate_call("square", [1.5], op_table)
    with pytest.raises(TypeError):
        validate.validate_call("square", ["7"], op_table)


def test_validate_call_bool_rejected(validate, op_table):
    with pytest.raises(TypeError):
        validate.validate_call("square", [True], op_table)


def test_validate_call_busy_sum_negative(validate, op_table):
    with pytest.raises(ValueError):
        validate.validate_call("busy_sum", [-1], op_table)


def test_validate_call_valid_returns_int_args(validate, op_table):
    assert validate.validate_call("square", [7], op_table) == [7]
    assert validate.validate_call("add", [3, 4], op_table) == [3, 4]
    assert validate.validate_call("boom", [], op_table) == []
    assert validate.validate_call("busy_sum", [0], op_table) == [0]
