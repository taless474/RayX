"""rayx.runtime pure validators + row-field constant (import-light; stdlib-only).

Extracted from ``rayx/runtime/__init__.py`` so the Python-boundary validation logic
can be imported and unit-tested WITHOUT the ``_rayx`` extension or HPX (the package
``__init__`` imports ``_rayx`` at module load). This module deliberately has **no**
``from .._rayx``, **no** ``import rayx``, and **no** relative imports (only ``math``),
so it is safe to load by file path in lightweight (repo-sanity) unit tests.

``validate_call`` takes the operation table (``{op_id: arity}``) as a **parameter**
rather than reading a module global, which is exactly what makes it testable without
the native registry: ``__init__`` passes the real C++ ``_OP_TABLE``; tests pass a
representative dict.
"""

import math

__all__ = ["ROW_FIELDS", "validate_timeout", "validate_call"]

# The exact runtime measurement-row key set. Documented in
# docs/design/rayx_phase1_registered_operation_api.md §9: the core measurement-row
# fields only -- a strict subset of the harness row's keys, with identical timing
# semantics. NO harness-facade echoes (label / chunks / chunk_delay_ms /
# chunks_completed) and NO `value` key (the value lives on OperationResult.value,
# never in the row). This is the canonical contract anchor for the runtime row.
ROW_FIELDS = (
    "actor_id",
    "submit_ns",
    "start_ns",
    "end_ns",
    "total_ms",
    "queue_wait_ms",
    "service_ms_observed",
    "status",
    "error",
)


def validate_call(op_id, args, op_table):
    """Validate ``op_id`` + ``args`` at the Python boundary, before the crossing.

    ``op_table`` is the ``{op_id: arity}`` registry view. Unknown op id ->
    ``ValueError``; wrong arity -> ``ValueError``; a non-``int`` argument (``bool``
    rejected explicitly, as an ``int`` subclass) -> ``TypeError``. Returns the
    validated list of ``int`` args. Mirrors the harness ``_validate_*`` boundary
    discipline; no ``RuntimeFuture`` is created on rejection.
    """
    if not isinstance(op_id, str):
        raise TypeError(f"op_id must be str, got {type(op_id).__name__}")
    if op_id not in op_table:
        raise ValueError(
            f"unknown operation id {op_id!r}; registered operations: "
            f"{sorted(op_table)}")
    arity = op_table[op_id]
    if len(args) != arity:
        raise ValueError(
            f"operation {op_id!r} expects {arity} argument(s), got {len(args)}")
    out = []
    for i, a in enumerate(args):
        # bool is an int subclass; reject it explicitly (almost always a mistake).
        if isinstance(a, bool) or not isinstance(a, int):
            raise TypeError(
                f"operation {op_id!r} argument {i} must be int, got "
                f"{type(a).__name__}")
        out.append(int(a))
    # Per-op argument-domain checks (fail fast at the boundary, like the harness
    # _validate_* helpers). busy_sum's step count must be non-negative.
    if op_id == "busy_sum" and out[0] < 0:
        raise ValueError(f"operation 'busy_sum' argument 0 (n) must be >= 0, "
                         f"got {out[0]}")
    return out


def validate_timeout(timeout):
    """Validate a non-``None`` :meth:`rayx.runtime.Runtime.wait` ``timeout`` (seconds).

    Mirrors the harness ``_validate_timeout`` (replicated, not imported, to keep
    ``rayx.runtime`` decoupled from harness internals). ``None`` means block and is
    handled by the caller (never reaches here). Otherwise ``timeout`` must be a
    finite real number (``int`` / ``float``, **not** ``bool``) ``>= 0``; ``NaN`` /
    ``inf`` / negative / non-numeric / ``bool`` all raise here, before any future is
    inspected. Only ``0`` (a non-blocking poll) is supported on HPX v1.11.0 -- a
    finite **positive** timeout is rejected by :meth:`Runtime.wait` (no non-consuming
    timed multi-future wait primitive). This validates the *value*; the caller
    decides poll vs reject.
    """
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError(
            f"timeout must be a real number of seconds or None, got "
            f"{type(timeout).__name__}")
    t = float(timeout)
    if not math.isfinite(t):
        raise ValueError(f"timeout must be finite (or None), got {timeout!r}")
    if t < 0.0:
        raise ValueError(f"timeout must be >= 0 (or None), got {timeout!r}")
    return t
