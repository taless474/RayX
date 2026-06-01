"""Deterministic per-request service-time sequence (single source of truth).

This is the canonical request-index -> service_ms mapping shared by the Python
benchmark drivers (``bench/run_ray_baseline.py`` and
``bench/run_hpx_python_baseline.py``). The native C++ baseline
(``hpx_impl/hpx_synthetic_baseline.cpp``::``service_for``) implements the SAME
function, so the same ``(seed, request_index)`` yields the same
``service_ms_requested`` sequence across Ray, native HPX, and rayx -- which is
what makes the cross-engine comparison fair.

Dependency-free (standard library only): importable in lightweight CI without
Ray or the rayx extension. ``bench/smoke_service_sequence.py`` pins the output
against frozen golden values so the three implementations cannot drift silently.

``service_for`` is duck-typed on an ``args``-like object exposing
``service_pattern``, ``service_ms``, ``seed``, ``service_low``,
``service_high``, and ``service_p_high`` (an ``argparse.Namespace`` works).
"""

_MASK64 = (1 << 64) - 1


def service_for(idx, args):
    """Per-request requested service time in ms (a pure function of idx).

    fixed   -> always args.service_ms.
    bimodal -> deterministic splitmix64 draw keyed by (seed, idx): if the draw
               in [0,1) is < service_p_high use service_high, else service_low.
               No stateful RNG, so the same seed reproduces the same sequence,
               and (because the mix is portable integer arithmetic and the draw
               normalization is a power-of-two scale) the sequence is
               engine-independent -- the native C++ ``service_for`` produces the
               identical value. Decorrelated from round-robin lane/actor
               assignment, unlike a periodic cycle.
    """
    if args.service_pattern == "fixed":
        return args.service_ms
    z = (args.seed + idx * 0x9E3779B97F4A7C15) & _MASK64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK64
    z = z ^ (z >> 31)
    draw = z / 2 ** 64
    return args.service_high if draw < args.service_p_high else args.service_low
