"""rayx.runtime pure validators + row-field constant (import-light; stdlib-only).

Extracted from ``rayx/runtime/__init__.py`` so the Python-boundary validation logic
can be imported and unit-tested WITHOUT the ``_rayx`` extension or HPX (the package
``__init__`` imports ``_rayx`` at module load). This module deliberately has **no**
``from .._rayx``, **no** ``import rayx``, and **no** relative imports (only ``math``),
so it is safe to load by file path in lightweight (repo-sanity) unit tests.

``validate_call`` takes the operation table (the typed-signature view
``{op_id: {"arg_types": [type, ...], "result_type": type}}``) as a **parameter**
rather than reading a module global, which is exactly what makes it testable without
the native registry: ``__init__`` passes the real C++ ``_OP_TABLE``; tests pass a
representative dict.

The closed typed value model (int64 / finite double) is a Python-boundary /
type-substrate concern, NOT an HPX mechanism one: this module only adds typed
per-argument validation (driven by the registry's declared ``arg_types``) plus the
explicit domain checks at the boundary (``int64`` range; strict-``float`` typing and
finiteness for ``double``). The type set is closed -- no ``bytes``, no implicit
``int`` -> ``float`` widening, and no open/extensible value channel.
"""

import math

__all__ = [
    "ROW_FIELDS",
    "PARTS_MAX",
    "PARK_MS_MAX",
    "CHAIN_STEPS_MAX",
    "CHAIN_QUANTUM_MAX",
    "CHAIN_FANOUT_K_MAX",
    "FANIN_LEAVES_MAX",
    "INT64_MIN",
    "INT64_MAX",
    "validate_timeout",
    "validate_call",
    "validate_actor_create",
    "validate_actor_call",
    "validate_nonblocking_options",
]

# Upper bound on the fanout_sum `parts` argument, enforced at the Python boundary.
# Mirror of FANOUT_PARTS_MAX in python/src/rayx/runtime_ops.hpp -- keep the two in
# sync. Bounds the op's internal hpx::async fan-out so an absurd part count cannot
# spawn an unbounded number of tasks.
PARTS_MAX = 1024

# Upper bound on the park_ms `ms` argument, enforced at the Python boundary.
# Mirror of PARK_MS_MAX in python/src/rayx/runtime_ops.hpp -- keep the two in sync.
# Keeps any cooperatively parked lane bounded by construction (60 s).
PARK_MS_MAX = 60_000

# Upper bounds on the chain_sum_* (exp39) `steps` / `quantum` arguments, enforced at
# the Python boundary. Mirror of CHAIN_STEPS_MAX / CHAIN_QUANTUM_MAX in
# python/src/rayx/runtime_ops.hpp -- keep the two in sync. STEPS bounds the dependent
# chain length (so chain_sum_then cannot build an unbounded continuation chain and the
# Python-mediated fold issues a bounded number of submits); QUANTUM bounds per-stage
# on-core work. The chain ops are queued-cancelable only, so the product is kept
# modest by construction to bound an uninterruptible call / teardown.
CHAIN_STEPS_MAX = 10_000
CHAIN_QUANTUM_MAX = 100_000

# Upper bound on the chain_fanout (exp40) `count` argument -- the number of independent
# child chains the op fans out via hpx::async. Mirror of CHAIN_FANOUT_K_MAX in
# python/src/rayx/runtime_ops.hpp -- keep the two in sync. Bounds the internal fan-out
# so an absurd child count cannot spawn an unbounded number of tasks (the FANOUT_PARTS_MAX
# posture for the launch-all op).
CHAIN_FANOUT_K_MAX = 256

# Upper bound on the barrier_fanin (exp44) `leaves` argument -- the number of bare-hpx::async
# children that mutually rendezvous on the cooperative gate. Mirror of FANIN_LEAVES_MAX in
# python/src/rayx/runtime_ops.hpp -- keep the two in sync. Small so the gated interior +
# diagnostic witness stay bounded; well under CHAIN_FANOUT_K_MAX.
FANIN_LEAVES_MAX = 64

# Inclusive int64 range. Python ints are arbitrary-precision, so a value that does not
# fit a C++ std::int64_t must be rejected EXPLICITLY at the boundary (deterministic,
# well-messaged) rather than relying on an opaque pybind cast failure at the crossing.
# This is the int64 leg of the closed value model (int64 / finite double).
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


def _validate_int64(label, i, a):
    """Validate one declared-``int64`` argument: ``bool``/non-``int`` -> ``TypeError``;
    out of ``[INT64_MIN, INT64_MAX]`` -> ``ValueError``. Returns the ``int`` value.

    ``label`` is the already-formatted context noun for messages (e.g.
    ``operation 'square'`` for ops, ``actor 'counter' init`` / ``method 'add'`` for
    actors). It replaces the former hard-coded ``operation`` wording so the same
    validators serve both the op and actor boundaries; ``validate_call`` passes
    ``operation '<op_id>'`` so op-path messages are unchanged."""
    # bool is an int subclass; reject it explicitly (almost always a mistake).
    if isinstance(a, bool) or not isinstance(a, int):
        raise TypeError(
            f"{label} argument {i} must be int, got {type(a).__name__}")
    if a < INT64_MIN or a > INT64_MAX:
        raise ValueError(
            f"{label} argument {i} (int64) is out of range "
            f"[{INT64_MIN}, {INT64_MAX}], got {a}")
    return int(a)


def _validate_double(label, i, a):
    """Validate one declared-``double`` argument (value-model V3): strict ``float``
    only -- ``bool``/``int``/anything non-``float`` -> ``TypeError`` (NO implicit
    int->float widening); ``NaN``/``inf``/``-inf`` -> ``ValueError``. Returns the
    ``float``. ``label`` is the formatted context noun (see :func:`_validate_int64`)."""
    # bool is an int subclass and int is NOT a float; require a real float. (The
    # explicit bool check keeps the message clear; `not isinstance(a, float)` alone
    # would also reject bool and int.)
    if isinstance(a, bool) or not isinstance(a, float):
        raise TypeError(
            f"{label} argument {i} must be float, got "
            f"{type(a).__name__}")
    if not math.isfinite(a):
        raise ValueError(
            f"{label} argument {i} (double) must be finite, got {a!r}")
    return a


# Per-type boundary validators, keyed by the registry's declared type name. V3 ships
# int64 + double; bytes is deliberately absent (no op declares it), and an unknown
# declared type fails loudly below rather than passing silently.
_TYPE_VALIDATORS = {
    "int64": _validate_int64,
    "double": _validate_double,
}

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

    ``op_table`` is the typed-signature registry view
    ``{op_id: {"arg_types": [type, ...], "result_type": type}}``. Unknown op id ->
    ``ValueError``; wrong arity (``len(args) != len(arg_types)``) -> ``ValueError``;
    an argument of the wrong Python type for its declared type (``bool`` rejected
    explicitly, as an ``int`` subclass) -> ``TypeError``; an ``int64`` argument
    outside ``[INT64_MIN, INT64_MAX]`` -> ``ValueError``; a ``double`` argument that is
    non-finite -> ``ValueError`` (value-model V3). Returns the validated Python args
    for the typed native marshaller: an ``int64`` arg stays a Python ``int`` and a
    ``double`` arg stays a Python ``float`` (the native side marshals each into the
    typed OpValue channel). Mirrors the harness ``_validate_*`` boundary discipline;
    public validation runs here, before any ``RuntimeFuture`` is created on rejection.
    """
    if not isinstance(op_id, str):
        raise TypeError(f"op_id must be str, got {type(op_id).__name__}")
    if op_id not in op_table:
        raise ValueError(
            f"unknown operation id {op_id!r}; registered operations: "
            f"{sorted(op_table)}")
    arg_types = op_table[op_id]["arg_types"]
    arity = len(arg_types)
    if len(args) != arity:
        raise ValueError(
            f"operation {op_id!r} expects {arity} argument(s), got {len(args)}")
    out = []
    for i, (a, t) in enumerate(zip(args, arg_types)):
        validator = _TYPE_VALIDATORS.get(t)
        if validator is None:
            # Defensive: the closed value model ships int64 / finite double only,
            # so a declared type with no validator means the registry advertised a
            # type the boundary cannot enforce. Fail loudly rather than letting an
            # unvalidated arg cross.
            raise ValueError(
                f"operation {op_id!r} argument {i} has unsupported declared type "
                f"{t!r}; this build validates: {sorted(_TYPE_VALIDATORS)}")
        # Pass the formatted label so messages stay exactly "operation '<op_id>'
        # argument N ..." (byte-identical to before the validators were generalized).
        out.append(validator(f"operation {op_id!r}", i, a))
    # Per-op argument-domain checks (fail fast at the boundary, like the harness
    # _validate_* helpers). busy_sum's step count must be non-negative.
    if op_id == "busy_sum" and out[0] < 0:
        raise ValueError(f"operation 'busy_sum' argument 0 (n) must be >= 0, "
                         f"got {out[0]}")
    # fanout_sum(n, parts): n >= 0; parts in [1, PARTS_MAX]. parts > n is allowed
    # (trailing ranges are empty and contribute 0). Arity (2) and the int/non-bool
    # checks above are already enforced generically; these are the domain bounds.
    if op_id == "fanout_sum":
        n, parts = out[0], out[1]
        if n < 0:
            raise ValueError(f"operation 'fanout_sum' argument 0 (n) must be >= 0, "
                             f"got {n}")
        if parts < 1:
            raise ValueError(f"operation 'fanout_sum' argument 1 (parts) must be "
                             f">= 1, got {parts}")
        if parts > PARTS_MAX:
            raise ValueError(f"operation 'fanout_sum' argument 1 (parts) must be "
                             f"<= {PARTS_MAX}, got {parts}")
    # park_ms(ms): the parked/cooperative-wait diagnostic. 0 <= ms <= PARK_MS_MAX
    # (the strict int/bool/int64-range handling above is the generic validator;
    # these are the domain bounds, mirroring the native re-check).
    if op_id == "park_ms":
        if out[0] < 0:
            raise ValueError(f"operation 'park_ms' argument 0 (ms) must be >= 0, "
                             f"got {out[0]}")
        if out[0] > PARK_MS_MAX:
            raise ValueError(f"operation 'park_ms' argument 0 (ms) must be <= "
                             f"{PARK_MS_MAX}, got {out[0]}")
    # chain_sum_loop / chain_sum_then (exp39): chain_sum_*(seed, steps, quantum).
    # seed (arg 0) is any int64; steps and quantum are bounded non-negative. Arity (3)
    # and the int/non-bool/int64-range checks above are generic; these are the per-op
    # domain bounds, mirrored by the native defensive re-check in the op bodies.
    if op_id in ("chain_sum_loop", "chain_sum_then"):
        steps, quantum = out[1], out[2]
        if steps < 0:
            raise ValueError(f"operation {op_id!r} argument 1 (steps) must be >= 0, "
                             f"got {steps}")
        if steps > CHAIN_STEPS_MAX:
            raise ValueError(f"operation {op_id!r} argument 1 (steps) must be <= "
                             f"{CHAIN_STEPS_MAX}, got {steps}")
        if quantum < 0:
            raise ValueError(f"operation {op_id!r} argument 2 (quantum) must be >= 0, "
                             f"got {quantum}")
        if quantum > CHAIN_QUANTUM_MAX:
            raise ValueError(f"operation {op_id!r} argument 2 (quantum) must be <= "
                             f"{CHAIN_QUANTUM_MAX}, got {quantum}")
    # chain_fanout (exp40): chain_fanout(seed, count, steps, quantum). seed (arg 0) is
    # any int64; count (arg 1) is in [1, CHAIN_FANOUT_K_MAX]; steps/quantum bounded
    # non-negative as for the chain_sum_* ops. Arity (4) and the int/non-bool/int64-range
    # checks above are generic; these are the per-op domain bounds, mirrored by the
    # native defensive re-check in the chain_fanout body.
    if op_id == "chain_fanout":
        count, steps, quantum = out[1], out[2], out[3]
        if count < 1:
            raise ValueError(f"operation 'chain_fanout' argument 1 (count) must be "
                             f">= 1, got {count}")
        if count > CHAIN_FANOUT_K_MAX:
            raise ValueError(f"operation 'chain_fanout' argument 1 (count) must be "
                             f"<= {CHAIN_FANOUT_K_MAX}, got {count}")
        if steps < 0:
            raise ValueError(f"operation 'chain_fanout' argument 2 (steps) must be "
                             f">= 0, got {steps}")
        if steps > CHAIN_STEPS_MAX:
            raise ValueError(f"operation 'chain_fanout' argument 2 (steps) must be "
                             f"<= {CHAIN_STEPS_MAX}, got {steps}")
        if quantum < 0:
            raise ValueError(f"operation 'chain_fanout' argument 3 (quantum) must be "
                             f">= 0, got {quantum}")
        if quantum > CHAIN_QUANTUM_MAX:
            raise ValueError(f"operation 'chain_fanout' argument 3 (quantum) must be "
                             f"<= {CHAIN_QUANTUM_MAX}, got {quantum}")
    # barrier_fanin (exp44): barrier_fanin(seed, leaves, quantum). seed (arg 0) is any
    # int64; leaves (arg 1) is in [1, FANIN_LEAVES_MAX]; quantum (arg 2) is bounded
    # non-negative as for the chain ops. Arity (3) and the int/non-bool/int64-range checks
    # above are generic; these are the per-op domain bounds, mirrored by the native
    # defensive re-check in the barrier_fanin body.
    if op_id == "barrier_fanin":
        leaves, quantum = out[1], out[2]
        if leaves < 1:
            raise ValueError(f"operation 'barrier_fanin' argument 1 (leaves) must be "
                             f">= 1, got {leaves}")
        if leaves > FANIN_LEAVES_MAX:
            raise ValueError(f"operation 'barrier_fanin' argument 1 (leaves) must be "
                             f"<= {FANIN_LEAVES_MAX}, got {leaves}")
        if quantum < 0:
            raise ValueError(f"operation 'barrier_fanin' argument 2 (quantum) must be "
                             f">= 0, got {quantum}")
        if quantum > CHAIN_QUANTUM_MAX:
            raise ValueError(f"operation 'barrier_fanin' argument 2 (quantum) must be "
                             f"<= {CHAIN_QUANTUM_MAX}, got {quantum}")
    return out


def _validate_typed_args(label, args, arg_types):
    """Arity + per-argument type validation against a typed signature, reusing
    ``_TYPE_VALIDATORS``. Shared by the actor create/call boundary validators (and
    structurally identical to ``validate_call``'s per-arg loop, kept separate so the
    op path stays untouched). ``label`` is the formatted context noun. Wrong arity ->
    ``ValueError``; wrong type -> ``TypeError``; out-of-range ``int64`` / non-finite
    ``double`` -> ``ValueError``. Returns the validated args list."""
    arity = len(arg_types)
    if len(args) != arity:
        raise ValueError(f"{label} expects {arity} argument(s), got {len(args)}")
    out = []
    for i, (a, t) in enumerate(zip(args, arg_types)):
        validator = _TYPE_VALIDATORS.get(t)
        if validator is None:
            raise ValueError(
                f"{label} argument {i} has unsupported declared type {t!r}; "
                f"this build validates: {sorted(_TYPE_VALIDATORS)}")
        out.append(validator(label, i, a))
    return out


def validate_actor_create(actor_type, args, actor_table):
    """Validate an actor-create call at the Python boundary, before the native
    crossing and before any actor lane/state is built.

    ``actor_table`` is the typed actor metadata view
    ``{actor_type: {"init_arg_types": [type, ...], "methods": {...}}}`` (the shape of
    ``_rayx.runtime_actor_table()``). Unknown actor type -> ``ValueError``; wrong init
    arity -> ``ValueError``; an init arg of the wrong Python type (``bool`` rejected
    as an ``int`` subclass) -> ``TypeError``; an ``int64`` init arg out of
    ``[INT64_MIN, INT64_MAX]`` -> ``ValueError``. Returns the validated init args for
    the native marshaller. Mirrors :func:`validate_call`'s boundary discipline; runs
    before any native ``create_actor`` call on rejection."""
    if not isinstance(actor_type, str):
        raise TypeError(f"actor_type must be str, got {type(actor_type).__name__}")
    if actor_type not in actor_table:
        raise ValueError(
            f"unknown actor type {actor_type!r}; registered actor types: "
            f"{sorted(actor_table)}")
    init_types = actor_table[actor_type]["init_arg_types"]
    return _validate_typed_args(f"actor {actor_type!r} init", args, init_types)


def validate_actor_call(actor_type, method_id, args, actor_table):
    """Validate an actor-method call at the Python boundary, before the native
    crossing and before any ``RuntimeFuture`` is created.

    ``actor_table`` is as in :func:`validate_actor_create`. Unknown actor type ->
    ``ValueError``; unknown method -> ``ValueError``; wrong method arity ->
    ``ValueError``; a method arg of the wrong Python type (``bool`` rejected) ->
    ``TypeError``; an ``int64`` arg out of range -> ``ValueError``. A per-method
    argument-domain check rejects a negative ``busy_get`` ``work_n`` (``ValueError``),
    mirroring the op-level ``busy_sum`` ``n >= 0`` guard in :func:`validate_call`.
    Returns the validated method args for the native marshaller."""
    if not isinstance(actor_type, str):
        raise TypeError(f"actor_type must be str, got {type(actor_type).__name__}")
    if not isinstance(method_id, str):
        raise TypeError(f"method_id must be str, got {type(method_id).__name__}")
    if actor_type not in actor_table:
        raise ValueError(
            f"unknown actor type {actor_type!r}; registered actor types: "
            f"{sorted(actor_table)}")
    methods = actor_table[actor_type]["methods"]
    if method_id not in methods:
        raise ValueError(
            f"unknown method {method_id!r} for actor type {actor_type!r}; "
            f"registered methods: {sorted(methods)}")
    arg_types = methods[method_id]["arg_types"]
    out = _validate_typed_args(f"method {method_id!r}", args, arg_types)
    # Per-method argument-domain check (mirror the op-level busy_sum guard in
    # validate_call): busy_get's synthetic on-core work count must be non-negative.
    if method_id == "busy_get" and out[0] < 0:
        raise ValueError(f"method 'busy_get' argument 0 (work_n) must be >= 0, "
                         f"got {out[0]}")
    return out


def validate_nonblocking_options(experimental_nonblocking_op_lanes,
                                 max_inflight_per_lane):
    """Validate the EXPERIMENTAL non-blocking-op-lane constructor options at the
    Python boundary, before the native crossing. Returns ``(flag, mi)`` where ``flag``
    is the bool to pass to the native ``nonblocking_op_lanes`` and ``mi`` is the int to
    pass to native ``max_inflight_per_lane``.

    ``experimental_nonblocking_op_lanes`` must be a real ``bool`` (not an ``int``).
    ``max_inflight_per_lane`` must be ``None`` or a positive ``int`` (``bool`` rejected
    as an ``int`` subclass):

      * disabled (flag ``False``): ``max_inflight_per_lane`` must be ``None`` (the cap
        is meaningless without the mode); a value with the mode off is a usage error.
        ``mi`` defaults to ``1``.
      * enabled (flag ``True``): ``None`` -> a default of ``8``; else the positive int.

    This mirrors the ``num_lanes`` / ``max_queue_depth_per_lane`` boundary discipline:
    fully validated here, before any ``_RuntimeEngine`` is constructed on rejection.
    The non-blocking mode applies to STATELESS operation lanes only -- actor lanes are
    always serial -- and relaxes only per-lane completion order, never the value/row
    schema or the ``RuntimeFuture`` contract.
    """
    # Default per-lane in-flight cap when the experimental mode is enabled without an
    # explicit cap. Small and bounded so a single non-blocking lane cannot dispatch an
    # unbounded number of concurrent Async bodies.
    default_inflight = 8
    if not isinstance(experimental_nonblocking_op_lanes, bool):
        raise TypeError(
            "experimental_nonblocking_op_lanes must be bool, got "
            f"{type(experimental_nonblocking_op_lanes).__name__}")
    if max_inflight_per_lane is not None:
        if (isinstance(max_inflight_per_lane, bool)
                or not isinstance(max_inflight_per_lane, int)):
            raise TypeError(
                "max_inflight_per_lane must be None or int, got "
                f"{type(max_inflight_per_lane).__name__}")
        if max_inflight_per_lane < 1:
            raise ValueError(
                "max_inflight_per_lane must be None or >= 1, got "
                f"{max_inflight_per_lane}")
    if not experimental_nonblocking_op_lanes:
        if max_inflight_per_lane is not None:
            raise ValueError(
                "max_inflight_per_lane is only valid when "
                "experimental_nonblocking_op_lanes=True")
        return False, 1
    return True, (default_inflight if max_inflight_per_lane is None
                  else max_inflight_per_lane)


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
