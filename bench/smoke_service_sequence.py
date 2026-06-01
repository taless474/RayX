#!/usr/bin/env python3
"""Golden-sequence check for the deterministic service-time mapping.

Pins ``bench/service_sequence.py``::``service_for`` against frozen golden values
so the three runtime implementations of the request-index -> service_ms mapping
-- ``bench/run_ray_baseline.py`` and ``bench/run_hpx_python_baseline.py`` (which
both import this helper) and the native C++ ``hpx_impl/hpx_synthetic_baseline.cpp``
::``service_for`` (a hand-mirrored copy, checked separately in the native CI
workflow) -- cannot drift silently.

Dependency-free: standard library only. Does NOT import Ray or the rayx
extension, so it runs in lightweight CI. Style mirrors the other bench smokes:
a PASS line per section, exit 0 on success, exit 1 with a reason on first fail.

Golden values are for the bimodal cell used in experiments 02/06/07:
``service-low=1, service-high=20, service-p-high=0.1, seed=0``.

Usage:
    python bench/smoke_service_sequence.py
Exit code 0 == pass, 1 == fail.
"""
import argparse
import os
import sys

_BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
if _BENCH_DIR not in sys.path:
    sys.path.insert(0, _BENCH_DIR)

from service_sequence import service_for


def _fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def _args(**kw):
    # The drivers pass an argparse.Namespace; service_for is duck-typed on it.
    base = dict(service_pattern="bimodal", service_ms=0.0, seed=0,
                service_low=1.0, service_high=20.0, service_p_high=0.1)
    base.update(kw)
    return argparse.Namespace(**base)


# Frozen golden output for service-low=1, service-high=20, p_high=0.1, seed=0.
GOLDEN_BIMODAL_0_19 = [20.0, 1.0, 1.0, 20.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
                       1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
GOLDEN_HIGH_POSITIONS_0_19 = [0, 3]
GOLDEN_HIGH_COUNT_FIRST_1000 = 92


def check_fixed():
    # fixed -> always service_ms, independent of idx (and of bimodal params).
    a = _args(service_pattern="fixed", service_ms=5.0)
    for idx in (0, 1, 7, 100, 999):
        v = service_for(idx, a)
        if v != 5.0:
            _fail(f"fixed: service_for({idx}) = {v}, expected 5.0")
    # service_ms=0 (no-op) likewise.
    a0 = _args(service_pattern="fixed", service_ms=0.0)
    if service_for(3, a0) != 0.0:
        _fail("fixed: service_for(3) with service_ms=0 should be 0.0")
    print("PASS: fixed -> constant service_ms for all indices")


def check_bimodal_golden():
    a = _args()  # bimodal lo1 hi20 p0.1 seed0
    seq = [service_for(idx, a) for idx in range(20)]
    if seq != GOLDEN_BIMODAL_0_19:
        _fail(f"bimodal idx0..19 = {seq}, expected {GOLDEN_BIMODAL_0_19}")
    highs = [i for i, v in enumerate(seq) if v == 20.0]
    if highs != GOLDEN_HIGH_POSITIONS_0_19:
        _fail(f"bimodal high positions 0..19 = {highs}, expected "
              f"{GOLDEN_HIGH_POSITIONS_0_19}")
    count = sum(1 for idx in range(1000) if service_for(idx, a) == 20.0)
    if count != GOLDEN_HIGH_COUNT_FIRST_1000:
        _fail(f"bimodal high count in first 1000 = {count}, expected "
              f"{GOLDEN_HIGH_COUNT_FIRST_1000}")
    print("PASS: bimodal lo1/hi20/p0.1/seed0 -> golden sequence (idx0..19, "
          f"high@{GOLDEN_HIGH_POSITIONS_0_19}, {count}/1000 high)")


def check_seed_changes_sequence():
    # A different seed must produce a different sequence (guards against the
    # seed being silently ignored). Shape-only: just assert non-equality.
    a0 = _args(seed=0)
    a1 = _args(seed=1)
    s0 = [service_for(i, a0) for i in range(50)]
    s1 = [service_for(i, a1) for i in range(50)]
    if s0 == s1:
        _fail("bimodal: seed=0 and seed=1 produced identical sequences")
    print("PASS: seed changes the bimodal sequence")


def main():
    check_fixed()
    check_bimodal_golden()
    check_seed_changes_sequence()
    print("OK: service-sequence golden check passed")


if __name__ == "__main__":
    main()
