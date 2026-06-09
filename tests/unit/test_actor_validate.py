"""Pure unit tests for the actor-create / actor-call Python-boundary validators.

Import-light: uses the ``validate`` module loaded BY FILE PATH (see conftest), so this
suite runs WITHOUT the ``_rayx`` extension or HPX, in the lightweight repo-sanity CI
job. The ``actor_table`` fixture is a representative typed metadata table mirroring
``_rayx.runtime_actor_table()``; the validators take it as a parameter, so no native
registry is needed.
"""
import pytest


# --- valid paths return the validated args ----------------------------------


def test_valid_create_returns_validated_args(validate, actor_table):
    assert validate.validate_actor_create("counter", (0,), actor_table) == [0]
    assert validate.validate_actor_create("counter", (7,), actor_table) == [7]


def test_valid_method_call_returns_validated_args(validate, actor_table):
    assert validate.validate_actor_call("counter", "add", (5,), actor_table) == [5]
    assert validate.validate_actor_call("counter", "get", (), actor_table) == []
    assert validate.validate_actor_call("counter", "reset", (2,), actor_table) == [2]


# --- create-path rejections --------------------------------------------------


def test_unknown_actor_type_create(validate, actor_table):
    with pytest.raises(ValueError):
        validate.validate_actor_create("nope", (0,), actor_table)


def test_wrong_init_arity(validate, actor_table):
    with pytest.raises(ValueError):
        validate.validate_actor_create("counter", (), actor_table)
    with pytest.raises(ValueError):
        validate.validate_actor_create("counter", (1, 2), actor_table)


def test_wrong_init_type(validate, actor_table):
    with pytest.raises(TypeError):
        validate.validate_actor_create("counter", ("x",), actor_table)


def test_bool_init_rejected(validate, actor_table):
    with pytest.raises(TypeError):
        validate.validate_actor_create("counter", (True,), actor_table)


def test_int64_init_out_of_range(validate, actor_table):
    with pytest.raises(ValueError):
        validate.validate_actor_create("counter", (2 ** 63,), actor_table)
    with pytest.raises(ValueError):
        validate.validate_actor_create("counter", (-(2 ** 63) - 1,), actor_table)


# --- call-path rejections ----------------------------------------------------


def test_unknown_actor_type_call(validate, actor_table):
    with pytest.raises(ValueError):
        validate.validate_actor_call("nope", "add", (1,), actor_table)


def test_unknown_method(validate, actor_table):
    with pytest.raises(ValueError):
        validate.validate_actor_call("counter", "frobnicate", (), actor_table)


def test_wrong_method_arity(validate, actor_table):
    with pytest.raises(ValueError):
        validate.validate_actor_call("counter", "add", (), actor_table)
    with pytest.raises(ValueError):
        validate.validate_actor_call("counter", "get", (1,), actor_table)


def test_wrong_method_type(validate, actor_table):
    with pytest.raises(TypeError):
        validate.validate_actor_call("counter", "add", ("x",), actor_table)


def test_bool_method_arg_rejected(validate, actor_table):
    with pytest.raises(TypeError):
        validate.validate_actor_call("counter", "reset", (True,), actor_table)
