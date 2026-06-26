"""Pure unit tests for the exp47 ``overlap_probe`` boundary validation.

Import-light: uses the ``validate`` module loaded BY FILE PATH (see conftest), so this
suite runs WITHOUT the ``_rayx`` extension or HPX, in the lightweight repo-sanity CI
job. Mirrors the native defensive re-check in ``runtime_ops_hpx.hpp`` at the
pure-function level: ``overlap_probe(seed, quantum, mode)`` is a 3-``int64`` signature
with an unbounded-within-int64 ``seed``, a bounded non-negative ``quantum`` (reusing the
chain ops' ``CHAIN_QUANTUM_MAX``), and a ``mode`` restricted to ``{0, 1}`` (the arm
kernel selector -- it never changes the returned value).
"""
import pytest

# validate_call takes the op_table as a parameter, so the pure test needs no native
# registry -- a representative 3-int64 signature mirrors _rayx.runtime_op_table().
_OVERLAP_TABLE = {
    "overlap_probe": {"arg_types": ["int64", "int64", "int64"],
                      "result_type": "int64"},
}


def test_overlap_probe_accepts_valid_triple(validate):
    assert validate.validate_call(
        "overlap_probe", [3, 64, 0], _OVERLAP_TABLE) == [3, 64, 0]
    assert validate.validate_call(
        "overlap_probe", [3, 64, 1], _OVERLAP_TABLE) == [3, 64, 1]


def test_overlap_probe_zero_quantum_accepted(validate):
    # quantum == 0 -> each arm's chain_stage is the identity add (value-neutral stage).
    assert validate.validate_call(
        "overlap_probe", [7, 0, 0], _OVERLAP_TABLE) == [7, 0, 0]
    assert validate.validate_call(
        "overlap_probe", [7, 0, 1], _OVERLAP_TABLE) == [7, 0, 1]


def test_overlap_probe_wrong_arity(validate):
    with pytest.raises(ValueError):
        validate.validate_call("overlap_probe", [1, 2], _OVERLAP_TABLE)        # 3 expected
    with pytest.raises(ValueError):
        validate.validate_call("overlap_probe", [1, 2, 0, 0], _OVERLAP_TABLE)  # 3 expected


def test_overlap_probe_non_int_rejected(validate):
    with pytest.raises(TypeError):
        validate.validate_call("overlap_probe", [1, 2.0, 0], _OVERLAP_TABLE)
    with pytest.raises(TypeError):
        validate.validate_call("overlap_probe", [1, 2, "0"], _OVERLAP_TABLE)


def test_overlap_probe_bool_rejected(validate):
    # bool is a non-int for the closed int64 channel (mirrors the other ops).
    with pytest.raises(TypeError):
        validate.validate_call("overlap_probe", [True, 64, 0], _OVERLAP_TABLE)
    with pytest.raises(TypeError):
        validate.validate_call("overlap_probe", [3, 64, True], _OVERLAP_TABLE)


def test_overlap_probe_negative_quantum_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call("overlap_probe", [3, -1, 0], _OVERLAP_TABLE)


def test_overlap_probe_quantum_over_max_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "overlap_probe", [3, validate.CHAIN_QUANTUM_MAX + 1, 0], _OVERLAP_TABLE)


def test_overlap_probe_quantum_at_max_accepted(validate):
    out = validate.validate_call(
        "overlap_probe", [3, validate.CHAIN_QUANTUM_MAX, 1], _OVERLAP_TABLE)
    assert out == [3, validate.CHAIN_QUANTUM_MAX, 1]


@pytest.mark.parametrize("bad_mode", [-1, 2, 3, 100])
def test_overlap_probe_mode_out_of_set_rejected(validate, bad_mode):
    # mode must be exactly 0 or 1; any other int64 is a domain error (ValueError).
    with pytest.raises(ValueError):
        validate.validate_call("overlap_probe", [3, 64, bad_mode], _OVERLAP_TABLE)


def test_overlap_probe_seed_is_unbounded_within_int64(validate):
    # seed (arg 0) takes any int64; only quantum/mode are domain-bounded.
    assert validate.validate_call(
        "overlap_probe", [validate.INT64_MAX, 64, 0], _OVERLAP_TABLE) == [
            validate.INT64_MAX, 64, 0]
    assert validate.validate_call(
        "overlap_probe", [validate.INT64_MIN, 64, 1], _OVERLAP_TABLE) == [
            validate.INT64_MIN, 64, 1]


def test_overlap_probe_seed_out_of_int64_rejected(validate):
    with pytest.raises(ValueError):
        validate.validate_call(
            "overlap_probe", [validate.INT64_MAX + 1, 64, 0], _OVERLAP_TABLE)
