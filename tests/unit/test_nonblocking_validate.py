"""Pure unit tests for ``validate_nonblocking_options`` (the experimental
non-blocking-op-lane constructor-option validator).

Import-light: uses the ``validate`` module loaded BY FILE PATH (see conftest), so this
suite runs WITHOUT the ``_rayx`` extension or HPX, in the lightweight repo-sanity CI
job. The validator is a pure Python-boundary check that returns the ``(flag, mi)`` pair
the native ``_RuntimeEngine`` receives; it does not touch the runtime or any lane.
"""
import pytest


# --- disabled (default) path --------------------------------------------------


def test_disabled_default_returns_false_and_one(validate):
    assert validate.validate_nonblocking_options(False, None) == (False, 1)


def test_disabled_with_explicit_cap_is_a_usage_error(validate):
    # The cap is meaningless without the mode -> reject rather than silently ignore.
    with pytest.raises(ValueError):
        validate.validate_nonblocking_options(False, 4)


# --- enabled path -------------------------------------------------------------


def test_enabled_defaults_inflight_to_eight(validate):
    assert validate.validate_nonblocking_options(True, None) == (True, 8)


def test_enabled_with_explicit_positive_cap(validate):
    assert validate.validate_nonblocking_options(True, 1) == (True, 1)
    assert validate.validate_nonblocking_options(True, 16) == (True, 16)


# --- type / domain rejections -------------------------------------------------


def test_flag_must_be_bool_not_int(validate):
    # bool is an int subclass; an int (even 1/0) for the flag is a usage error.
    with pytest.raises(TypeError):
        validate.validate_nonblocking_options(1, None)
    with pytest.raises(TypeError):
        validate.validate_nonblocking_options(0, None)


def test_inflight_must_be_int_not_bool(validate):
    with pytest.raises(TypeError):
        validate.validate_nonblocking_options(True, True)


def test_inflight_must_be_int_not_float(validate):
    with pytest.raises(TypeError):
        validate.validate_nonblocking_options(True, 4.0)


def test_inflight_must_be_positive(validate):
    with pytest.raises(ValueError):
        validate.validate_nonblocking_options(True, 0)
    with pytest.raises(ValueError):
        validate.validate_nonblocking_options(True, -3)
