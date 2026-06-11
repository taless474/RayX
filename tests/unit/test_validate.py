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


# --- int64 typed signature (value-model V1) ---------------------------------


def test_validate_call_int64_bounds_accepted(validate, op_table):
    # The inclusive int64 endpoints are valid and pass through unchanged.
    assert validate.validate_call("square", [validate.INT64_MAX], op_table) == [
        validate.INT64_MAX]
    assert validate.validate_call("square", [validate.INT64_MIN], op_table) == [
        validate.INT64_MIN]
    assert validate.validate_call(
        "add", [validate.INT64_MIN, validate.INT64_MAX], op_table) == [
            validate.INT64_MIN, validate.INT64_MAX]


def test_validate_call_int64_out_of_range_rejected(validate, op_table):
    # Python ints are arbitrary-precision; one past either endpoint -> ValueError
    # at the boundary (no future is created).
    with pytest.raises(ValueError):
        validate.validate_call("square", [validate.INT64_MAX + 1], op_table)
    with pytest.raises(ValueError):
        validate.validate_call("square", [validate.INT64_MIN - 1], op_table)
    with pytest.raises(ValueError):
        validate.validate_call("add", [0, validate.INT64_MAX + 1], op_table)


def test_validate_call_arity_derived_from_arg_types(validate, op_table):
    # Arity comes from len(arg_types), so a typed table with the wrong count still
    # rejects exactly as the old arity-only table did.
    assert validate.validate_call("fanout_sum", [10, 3], op_table) == [10, 3]
    with pytest.raises(ValueError):
        validate.validate_call("fanout_sum", [10], op_table)


def test_validate_call_unsupported_declared_type_rejected(validate):
    # Defensive: a declared type the boundary cannot enforce must fail loudly rather
    # than letting an unvalidated arg cross (V3 validates int64 + double only).
    bad_table = {"weird": {"arg_types": ["bytes"], "result_type": "bytes"}}
    with pytest.raises(ValueError):
        validate.validate_call("weird", [1], bad_table)


# --- double typed signature (value-model V3) --------------------------------


def test_validate_call_double_accepts_float(validate, op_table):
    assert validate.validate_call("scale_double", [1.5, 2.0], op_table) == [1.5, 2.0]
    assert validate.validate_call("scale_double", [-0.5, 4.0], op_table) == [-0.5, 4.0]
    assert validate.validate_call("scale_double", [3.0, 0.0], op_table) == [3.0, 0.0]


def test_validate_call_double_rejects_int_no_widening(validate, op_table):
    # No implicit int->float widening: a Python int for a double arg is a TypeError.
    with pytest.raises(TypeError):
        validate.validate_call("scale_double", [2, 2.0], op_table)
    with pytest.raises(TypeError):
        validate.validate_call("scale_double", [1.5, 3], op_table)


def test_validate_call_double_rejects_bool(validate, op_table):
    with pytest.raises(TypeError):
        validate.validate_call("scale_double", [True, 2.0], op_table)


def test_validate_call_double_rejects_str(validate, op_table):
    with pytest.raises(TypeError):
        validate.validate_call("scale_double", ["1.5", 2.0], op_table)


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_validate_call_double_rejects_non_finite(validate, op_table, bad):
    with pytest.raises(ValueError):
        validate.validate_call("scale_double", [bad, 2.0], op_table)


def test_validate_call_double_wrong_arity(validate, op_table):
    with pytest.raises(ValueError):
        validate.validate_call("scale_double", [1.5], op_table)        # needs 2


# --- fanout_sum boundary validation -----------------------------------------


def test_validate_call_fanout_sum_wrong_arity(validate, op_table):
    with pytest.raises(ValueError):
        validate.validate_call("fanout_sum", [10], op_table)        # needs 2
    with pytest.raises(ValueError):
        validate.validate_call("fanout_sum", [10, 2, 3], op_table)  # too many


def test_validate_call_fanout_sum_non_int_rejected(validate, op_table):
    with pytest.raises(TypeError):
        validate.validate_call("fanout_sum", [10.0, 2], op_table)
    with pytest.raises(TypeError):
        validate.validate_call("fanout_sum", [10, "2"], op_table)


def test_validate_call_fanout_sum_bool_rejected(validate, op_table):
    with pytest.raises(TypeError):
        validate.validate_call("fanout_sum", [True, 2], op_table)
    with pytest.raises(TypeError):
        validate.validate_call("fanout_sum", [10, False], op_table)


def test_validate_call_fanout_sum_negative_n(validate, op_table):
    with pytest.raises(ValueError):
        validate.validate_call("fanout_sum", [-1, 2], op_table)


def test_validate_call_fanout_sum_parts_below_one(validate, op_table):
    with pytest.raises(ValueError):
        validate.validate_call("fanout_sum", [10, 0], op_table)
    with pytest.raises(ValueError):
        validate.validate_call("fanout_sum", [10, -5], op_table)


def test_validate_call_fanout_sum_parts_above_max(validate, op_table):
    assert validate.PARTS_MAX == 1024
    with pytest.raises(ValueError):
        validate.validate_call("fanout_sum", [10, validate.PARTS_MAX + 1], op_table)


def test_validate_call_fanout_sum_valid_returns_int_args(validate, op_table):
    assert validate.validate_call("fanout_sum", [10, 3], op_table) == [10, 3]
    assert validate.validate_call("fanout_sum", [0, 1], op_table) == [0, 1]
    # parts > n is allowed (empty trailing ranges contribute 0).
    assert validate.validate_call("fanout_sum", [5, 10], op_table) == [5, 10]
    # parts == PARTS_MAX is the inclusive upper bound.
    assert validate.validate_call(
        "fanout_sum", [10, validate.PARTS_MAX], op_table) == [10, validate.PARTS_MAX]


# --- validate_call: park_ms (parked/cooperative-wait diagnostic) -------------
#
# park_ms(ms) is a single-int64-arg op exactly like busy_sum at the typed-
# signature level; only its 0 <= ms <= PARK_MS_MAX domain guard is
# park_ms-specific. PARK_MS_MAX mirrors the native constant in runtime_ops.hpp.


def test_validate_call_park_ms_wrong_type(validate, op_table):
    with pytest.raises(TypeError):
        validate.validate_call("park_ms", ["x"], op_table)


def test_validate_call_park_ms_bool_rejected(validate, op_table):
    # bool is an int subclass; the strict int64 validator must reject it.
    with pytest.raises(TypeError):
        validate.validate_call("park_ms", [True], op_table)


def test_validate_call_park_ms_negative(validate, op_table):
    with pytest.raises(ValueError):
        validate.validate_call("park_ms", [-1], op_table)


def test_validate_call_park_ms_over_cap(validate, op_table):
    assert validate.PARK_MS_MAX == 60_000
    with pytest.raises(ValueError):
        validate.validate_call("park_ms", [validate.PARK_MS_MAX + 1], op_table)


def test_validate_call_park_ms_valid_returns_int_args(validate, op_table):
    assert validate.validate_call("park_ms", [0], op_table) == [0]
    assert validate.validate_call("park_ms", [1], op_table) == [1]
    # ms == PARK_MS_MAX is the inclusive upper bound.
    assert validate.validate_call(
        "park_ms", [validate.PARK_MS_MAX], op_table) == [validate.PARK_MS_MAX]


# --- validate_actor_call: counter.busy_get (read-only checkpointed method) --
#
# Local actor-metadata table including busy_get. Built inline (rather than via the
# conftest ``actor_table`` fixture) so this file is self-contained: busy_get is a
# single-int64-arg method exactly like add/reset/busy_sum at the typed-signature
# level; only its work_n >= 0 domain guard is busy_get-specific.
_BUSY_GET_TABLE = {
    "counter": {
        "init_arg_types": ["int64"],
        "methods": {
            "busy_get": {"arg_types": ["int64"], "result_type": "int64"},
        },
    }
}


def test_validate_actor_call_busy_get_wrong_type(validate):
    with pytest.raises(TypeError):
        validate.validate_actor_call("counter", "busy_get", ["x"], _BUSY_GET_TABLE)


def test_validate_actor_call_busy_get_bool_rejected(validate):
    # bool is an int subclass; the strict int64 validator must reject it (no widening).
    with pytest.raises(TypeError):
        validate.validate_actor_call("counter", "busy_get", [True], _BUSY_GET_TABLE)


def test_validate_actor_call_busy_get_negative(validate):
    with pytest.raises(ValueError):
        validate.validate_actor_call("counter", "busy_get", [-1], _BUSY_GET_TABLE)


def test_validate_actor_call_busy_get_valid_returns_int_args(validate):
    # Non-negative ints pass and are returned unchanged for the native marshaller.
    assert validate.validate_actor_call(
        "counter", "busy_get", [0], _BUSY_GET_TABLE) == [0]
    assert validate.validate_actor_call(
        "counter", "busy_get", [100_000], _BUSY_GET_TABLE) == [100_000]
