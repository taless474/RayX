"""rayx.runtime: experimental Phase 1 registered-native-operation runtime.

**Experimental and deliberately SEPARATE** from the shipped rayx harness
(``Engine`` / ``SyntheticActor`` / ``Future``). This is the Phase 1 "RayX runtime
prototype": it runs *registered native operations* on an HPX-native path and
returns a user **value** plus a measurement **row** as two SEPARATE things
(:attr:`OperationResult.value` vs :attr:`OperationResult.row`). It is **not** Ray,
**not** Ray-compatible, has no object store, no ``ObjectRef``, runs no arbitrary
Python, and makes no performance / "HPX beats Ray" claim.

**Slice 2a/2b scope.** ``Runtime`` + a fixed C++ operation registry
(``square`` / ``add`` / ``boom`` / ``busy_sum``, plus the internally-composed
``fanout_sum``) + ``RuntimeFuture`` +
``OperationResult`` + the error classes + a shared process-runtime guard (mutually
exclusive with the harness ``Engine``) + ``num_lanes`` HPX-native FIFO
``RuntimeLane`` workers with round-robin selection, per-lane ``rt-hpx-`` ids, and
``lane_stats()`` + **cooperative cancellation** (2a): ``RuntimeFuture.cancel()`` /
``cancelled()``, deterministic **queued** and checkpoint-count-armed **running**
cancel (via the checkpointed ``busy_sum`` built-in), a cancelled
``OperationResult`` whose ``.value`` raises :class:`OperationCancelledError`, and a
shutdown that **cancels outstanding queued/in-flight work** before draining +
**bounded admission** (2b): a per-lane ``max_queue_depth_per_lane`` cap whose full
lane makes ``submit_operation`` raise :class:`QueueFullError`.

**Collection APIs** (Slice 2c): :meth:`Runtime.get` (retire/collect in input order,
no fail-fast), :meth:`Runtime.wait` (blocking ``hpx::wait_some`` / ``timeout=0``
non-blocking poll, non-consuming), and :meth:`Runtime.as_completed` -- mirroring the
harness collection semantics without touching harness code. The runtime emits no
JSONL and is not a benchmark driver.

This subpackage is **not** exported through top-level ``rayx.__all__``; import it
explicitly as ``from rayx.runtime import Runtime``.
"""

import time

# Requires the built _rayx extension. Importing the rayx parent package already
# raises a build-actionable error if _rayx is missing, so a plain import here is
# enough (and surfaces the same loader error otherwise).
from .._rayx import (
    _RuntimeEngine,
    _RuntimeFuture,
    runtime_actor_table,
    runtime_op_table,
)

# Pure, import-light pieces live in sibling submodules so they can be unit-tested
# WITHOUT the _rayx extension (see _errors.py / _validate.py). Re-exported below so
# the public names stay on rayx.runtime exactly as before.
from ._errors import (
    OperationCancelledError,
    OperationFailedError,
    QueueFullError,
    RuntimeOperationError,
)
from ._validate import (
    ROW_FIELDS,
    validate_actor_call,
    validate_actor_create,
    validate_call,
    validate_timeout,
)

__all__ = [
    "Runtime",
    "RuntimeFuture",
    "OperationResult",
    "ActorHandle",
    "RuntimeOperationError",
    "OperationFailedError",
    "OperationCancelledError",
    "QueueFullError",
]

# Typed-signature snapshot of the fixed C++ registry, read once at import:
# {op_id: {"arg_types": [type, ...], "result_type": type}} (closed value model:
# int64 / finite double). Passed to validate_call() to check op id, arity
# (== len(arg_types)), and each arg's declared type + domain (int64 range; double
# strict-float finiteness) at the Python boundary BEFORE the C++ crossing.
# (validate_call lives in _validate.py and takes the table as a parameter so it
# stays unit-testable without the native registry.)
_OP_TABLE = dict(runtime_op_table())

# Typed-metadata snapshot of the fixed C++ actor registry, read once at import (the
# actor analogue of _OP_TABLE): {actor_type: {"init_arg_types": [type, ...],
# "methods": {method_id: {"arg_types": [type, ...], "result_type": type}}}}. Passed
# to validate_actor_create / validate_actor_call to check actor type, method id,
# arity, and each arg's declared type + int64 range at the Python boundary BEFORE the
# native crossing. (The validators take the table as a parameter so they stay
# unit-testable without the native registry.)
_ACTOR_TABLE = dict(runtime_actor_table())


class OperationResult:
    """Retired payload of one runtime operation: a value AND a measurement row.

    The two are kept strictly separate: :attr:`value` is the user value,
    :attr:`row` is the core measurement-row dict (never containing ``value``).
    Unlike :class:`RuntimeFuture`, an ``OperationResult`` is **not** consume-once:
    :attr:`value` and :attr:`row` are idempotently re-readable.

    :attr:`row` is always safe to inspect (for completed, failed, and -- later --
    cancelled outcomes). :attr:`value` returns the value only when the operation
    completed; on a failed result it raises :class:`OperationFailedError`, and on
    a cancelled result :class:`OperationCancelledError` (idempotently, every read).
    """

    __slots__ = ("_value", "_has_value", "_status", "_error", "_row")

    def __init__(self, value, has_value, status, error, row):
        self._value = value
        self._has_value = has_value
        self._status = status
        self._error = error
        self._row = row

    @property
    def value(self):
        if self._status == "completed" and self._has_value:
            return self._value
        if self._status == "cancelled":
            raise OperationCancelledError(
                "operation was cancelled; no value is available "
                f"(status={self._status!r})")
        # failed, or any non-completed result without a value
        detail = f", error={self._error!r}" if self._error else ""
        raise OperationFailedError(
            f"operation did not complete; no value is available "
            f"(status={self._status!r}{detail})")

    @property
    def row(self):
        return self._row

    @property
    def status(self):
        return self._status

    def __repr__(self):
        return f"<rayx.runtime.OperationResult status={self._status!r}>"


class RuntimeFuture:
    """Handle for one in-flight runtime operation.

    Carries the Python-side ``submit_ns`` captured at submission so that
    :meth:`result` can report client-observed ``total_ms`` (including the
    pybind11/GIL crossing). Consume-once: :meth:`result` may be called only once.
    """

    __slots__ = ("_rf", "_submit_ns", "_retired")

    def __init__(self, rf, submit_ns):
        self._rf = rf
        self._submit_ns = submit_ns
        self._retired = False

    def ready(self):
        """Non-blocking: True if the result can be retired without blocking.

        Raises if this future was already retired via :meth:`result`.
        """
        return self._rf.ready()

    def cancel(self):
        """Request cancellation of this operation; returns whether it settled one.

        Runs on the calling (Python) thread with **no HPX hop**. Returns ``True``
        iff this call settles a cancellation: the op was still **queued** (its lane
        will skip it) or **running** a checkpointed op (``busy_sum``) with a boundary
        still ahead (it stops at the next checkpoint). Returns ``False`` otherwise --
        already completed/cancelled, or running an instantaneous / short
        (``checkpoint_count == 1``) op that has no boundary to stop at. Safe to call
        after :meth:`result` (a terminal outcome returns ``False``). A cancelled
        operation retires with ``row["status"] == "cancelled"`` and its
        :attr:`OperationResult.value` raises :class:`OperationCancelledError`.
        """
        return self._rf.cancel()

    def cancelled(self):
        """Non-consuming: ``True`` once cancellation is guaranteed.

        Valid **before and after** :meth:`result` (unlike :meth:`ready`, it does not
        raise after retire). Reflects a settled queued cancel or a requested running
        stop; it does not itself retire the future.
        """
        return self._rf.cancelled()

    def result(self, recv_ns=None):
        """Retire this future and return its :class:`OperationResult`.

        Consume-once: a second call raises (structural misuse). Does **not** raise
        on a failed operation *outcome* -- it returns an ``OperationResult`` whose
        ``.row`` is inspectable and whose ``.value`` raises.
        """
        raw = self._rf.result()  # consume-once; raises if already retired
        self._retired = True
        if recv_ns is None:
            recv_ns = time.perf_counter_ns()
        total_ms = (recv_ns - self._submit_ns) / 1e6
        service_ms = raw["service_ms_observed"]
        # queue_wait is approximate (submit_ns is the Python clock, start_ns the
        # C++ clock -- different domains), so we do not subtract them; total_ms is
        # client-authoritative. Mirrors the harness Future.result().
        queue_wait_ms = total_ms - service_ms
        if queue_wait_ms < 0.0:
            queue_wait_ms = 0.0
        row = {
            "actor_id": raw["actor_id"],
            "submit_ns": self._submit_ns,
            "start_ns": raw["start_ns"],
            "end_ns": raw["end_ns"],
            "total_ms": total_ms,
            "queue_wait_ms": queue_wait_ms,
            "service_ms_observed": service_ms,
            "status": raw["status"],
            "error": raw["error"],
        }
        return OperationResult(
            value=raw["value"],
            has_value=raw["has_value"],
            status=raw["status"],
            error=raw["error"],
            row=row,
        )

    def __repr__(self):
        state = "retired" if self._retired else "pending"
        return f"<rayx.runtime.RuntimeFuture {state} submit_ns={self._submit_ns}>"


class ActorHandle:
    """Handle to one local stateful native actor (experimental ``rayx.runtime``).

    Created by :meth:`Runtime.create_actor`. Holds the actor's opaque ``actor_id``
    and registered ``actor_type`` plus a back-reference to its :class:`Runtime`.
    Methods are dispatched by **explicit registered id** via :meth:`call` over a
    *fixed* native method set -- there is deliberately **no** ``.remote()`` and **no**
    attribute-style ``actor.method(...)`` (which would imply an open/arbitrary method
    set). A method call returns the same :class:`RuntimeFuture` as an operation, so
    :meth:`Runtime.get` / :meth:`Runtime.wait` / :meth:`Runtime.as_completed` and
    cancellation all work on actor-method futures unchanged. This is **not** a Ray
    actor handle: no object store, no ``.remote()``, no arbitrary Python methods.
    """

    __slots__ = ("_runtime", "_actor_id", "_actor_type")

    def __init__(self, runtime, actor_id, actor_type):
        self._runtime = runtime
        self._actor_id = actor_id
        self._actor_type = actor_type

    @property
    def actor_id(self):
        """The actor's opaque ``rt-act-`` id (also stamped on each method row)."""
        return self._actor_id

    @property
    def actor_type(self):
        """The actor's registered type id (e.g. ``"counter"``)."""
        return self._actor_type

    def stats(self):
        """Non-consuming per-actor observability snapshot (debugging only).

        Returns one ``{"actor_id", "queue_depth", "active"}`` dict for THIS
        actor's dedicated lane — the actor analogue of one
        :meth:`Runtime.lane_stats` element, with the same three fields and the
        same semantics: ``queue_depth`` is the queued-but-not-started method-call
        count (the in-service call has been popped and is **not** counted), and
        ``active`` is whether a method is currently in the service slot.

        Point-in-time and **racy**, exactly like ``lane_stats()``: values can
        change the instant this returns. It is fully non-consuming — it touches
        no future and changes no call/cancel/admission semantics — and is for
        debugging and test-gating only: **not** scheduler state, **not**
        placement control, **not** a synchronization primitive, and not part of
        any JSONL schema. ``Runtime.lane_stats()`` remains **op-lanes-only**;
        actor lanes are visible only through this per-handle snapshot. Raises
        ``RuntimeError`` if the owning runtime is shut down (consistent with
        :meth:`call`).
        """
        if self._runtime._closed:
            raise RuntimeError("Runtime is shut down")
        return self._runtime._engine.actor_stats(self._actor_id)

    def call(self, method_id, *args):
        """Dispatch a registered method on this actor; return a :class:`RuntimeFuture`.

        ``method_id`` is a registered method of this actor's type (e.g. ``"add"`` /
        ``"get"`` / ``"reset"`` for ``"counter"``); ``*args`` are its typed arguments,
        validated at the Python boundary against the actor table BEFORE the native
        crossing (unknown method / wrong arity -> ``ValueError``; wrong type ->
        ``TypeError``; ``int64`` out of range -> ``ValueError``), capturing
        ``submit_ns`` like :meth:`Runtime.submit_operation`. A full actor lane makes
        the call raise :class:`QueueFullError`. The returned future is an ordinary
        :class:`RuntimeFuture` (FIFO per actor; cancelable -- queued always; the MVP
        methods are instantaneous, so running-cancel does not apply). Raises if the
        owning runtime is shut down.
        """
        if self._runtime._closed:
            raise RuntimeError("Runtime is shut down")
        validated = validate_actor_call(self._actor_type, method_id, args,
                                        _ACTOR_TABLE)
        submit_ns = time.perf_counter_ns()
        rf = self._runtime._engine.call_actor_method(self._actor_id, method_id,
                                                     validated)
        # Bounded admission: the capped native path returns None when the actor lane
        # is full (no future/token/promise created). Mirror submit_operation: raise
        # the runtime QueueFullError here.
        if rf is None:
            raise QueueFullError(
                "actor lane is full (max_queue_depth_per_lane reached); "
                "the call was rejected and created no future")
        return RuntimeFuture(rf, submit_ns)

    def __repr__(self):
        return (f"<rayx.runtime.ActorHandle actor_type={self._actor_type!r} "
                f"actor_id={self._actor_id!r}>")


class Runtime:
    """Explicit, process-singleton HPX-native runtime for registered operations.

    Only one active ``Runtime`` is allowed per process, and it is **mutually
    exclusive with the harness** :class:`rayx.Engine` / ``SyntheticActor`` (they
    share one process HPX-runtime guard): constructing one while the other is live
    raises. Usable as a context manager.

    The constructor takes ``num_lanes`` (the number of HPX-native FIFO
    ``RuntimeLane`` workers; default 1), ``hpx_threads`` (the HPX worker count), and
    ``max_queue_depth_per_lane`` (Slice 2b bounded admission; ``None`` = unbounded
    default, else a positive **per-lane** cap on queued-but-not-started operations --
    a full lane makes :meth:`submit_operation` raise :class:`QueueFullError`).
    Submissions round-robin across the lanes, and each lane has its own stable
    ``rt-hpx-`` ``actor_id`` (visible on the result row and via :meth:`lane_stats`).
    It does **not** accept ``lane_impl`` (the runtime is HPX-native by construction;
    there is no backend selector).
    """

    def __init__(self, num_lanes=1, hpx_threads=1, max_queue_depth_per_lane=None):
        # bool is an int subclass; reject it explicitly (almost always a mistake).
        if isinstance(num_lanes, bool) or not isinstance(num_lanes, int):
            raise TypeError(
                f"num_lanes must be int, got {type(num_lanes).__name__}")
        if num_lanes < 1:
            raise ValueError(f"num_lanes must be >= 1, got {num_lanes}")
        # Bounded admission (Slice 2b): None = unbounded (default), passed to C++ as
        # the -1 sentinel; otherwise a positive int per-lane cap. Validated here at
        # the Python boundary, before the crossing -- mirrors num_lanes / the harness
        # facade. Per-lane cap only (never a global cap).
        if max_queue_depth_per_lane is None:
            cap = -1
        else:
            if (isinstance(max_queue_depth_per_lane, bool)
                    or not isinstance(max_queue_depth_per_lane, int)):
                raise TypeError(
                    "max_queue_depth_per_lane must be None or int, got "
                    f"{type(max_queue_depth_per_lane).__name__}")
            if max_queue_depth_per_lane < 1:
                raise ValueError(
                    "max_queue_depth_per_lane must be None or >= 1, got "
                    f"{max_queue_depth_per_lane}")
            cap = max_queue_depth_per_lane
        self._engine = _RuntimeEngine(hpx_threads=hpx_threads,
                                      num_lanes=num_lanes,
                                      max_queue_depth_per_lane=cap)
        self._closed = False

    def submit_operation(self, op_id, *args):
        """Submit one registered native operation; returns a :class:`RuntimeFuture`.

        ``op_id`` is a registered native operation name (``"square"`` / ``"add"``
        / ``"boom"`` / ``"busy_sum"`` / ``"fanout_sum"`` / ``"park_ms"`` ->
        ``int64``; ``"scale_double"`` -> ``double``); ``*args`` are its typed
        arguments, whose accepted Python type
        is per the op's declared signature: ``int64`` args take a Python ``int``
        (``bool`` rejected; out of ``[-2**63, 2**63-1]`` -> ``ValueError``), and
        ``double`` args take a strict Python ``float`` (``bool``/``int`` rejected --
        no implicit widening; ``NaN``/``inf`` -> ``ValueError``). Validates at the
        Python boundary (unknown id / wrong arity -> ``ValueError``; wrong type ->
        ``TypeError``; ``busy_sum`` requires ``n >= 0``; ``fanout_sum`` requires
        ``n >= 0`` and ``1 <= parts <= 1024``; ``park_ms`` requires
        ``0 <= ms <= 60_000`` -> ``ValueError``) before the single
        Python->C++ crossing, capturing ``submit_ns`` like the harness
        ``Engine.submit``. The returned :class:`RuntimeFuture` is cancelable (queued
        always; running only for checkpointed ops such as ``busy_sum`` and the
        cooperative parked ``park_ms`` -- the launch-all ``fanout_sum`` and the
        instantaneous ``scale_double`` are queued-cancelable only).
        """
        if self._closed:
            raise RuntimeError("Runtime is shut down")
        validated = validate_call(op_id, args, _OP_TABLE)
        submit_ns = time.perf_counter_ns()
        rf = self._engine.submit_operation(op_id, validated)
        # Bounded admission: the capped C++ path returns None when the target lane is
        # full (no future/token/promise created). Raise the runtime QueueFullError
        # here -- mirrors the harness facade turning a None submit into QueueFullError.
        if rf is None:
            raise QueueFullError(
                "target lane is full (max_queue_depth_per_lane reached); "
                "the submit was rejected and created no future")
        return RuntimeFuture(rf, submit_ns)

    def create_actor(self, actor_type, *args):
        """Create a local stateful native actor; return an :class:`ActorHandle`.

        ``actor_type`` is a registered native actor type (``"counter"`` -> state
        ``int64`` with methods ``add`` / ``get`` / ``reset``); ``*args`` are its init
        arguments, validated at the Python boundary against the actor table BEFORE the
        native crossing (unknown type / wrong init arity -> ``ValueError``; wrong type
        -> ``TypeError``; ``int64`` out of range -> ``ValueError``). Actor creation
        itself returns **no** future / row -- it builds the actor's native state and
        its dedicated HPX-native FIFO lane and returns a handle whose
        :meth:`ActorHandle.call` dispatches registered methods (each returning an
        ordinary :class:`RuntimeFuture`). The actor lives for the runtime's lifetime
        (there is no per-actor release in this slice) and is torn down at
        :meth:`shutdown`. It is **not** a Ray actor: no object store, no ``.remote()``,
        no arbitrary Python methods.
        """
        if self._closed:
            raise RuntimeError("Runtime is shut down")
        validated = validate_actor_create(actor_type, args, _ACTOR_TABLE)
        actor_id = self._engine.create_actor(actor_type, validated)
        return ActorHandle(self, actor_id, actor_type)

    def wait(self, futures, num_returns=1, timeout=None):
        """Partition ``futures`` into ``(ready, not_ready)`` by readiness.

        Returns a partition of the SAME :class:`RuntimeFuture` objects (each keeps
        its Python-side ``submit_ns``). **Non-consuming**: this is readiness /
        retirement coordination, NOT result consumption -- you still call
        :meth:`RuntimeFuture.result` / :meth:`get` once to retire a ready future.
        The given futures must not have been retired yet, and each must appear at
        most once (a duplicate raises ``ValueError``). A failed or cancelled
        operation is ready as soon as its row exists (a queued cancel is ready
        immediately; a running chunk-boundary cancel stays pending until the lane
        reaches the boundary), so ``wait`` treats completed, failed, and cancelled
        results uniformly.

        ``timeout`` is in **seconds** and selects the mode (mirrors
        :meth:`rayx.Engine.wait`):

        * ``timeout=None`` (default) -- **block** until at least ``num_returns`` of
          ``futures`` are ready, inside C++/HPX with the GIL released
          (``hpx::wait_some``); NOT a Python busy-poll. ``num_returns=1`` gives
          wait-any semantics.
        * ``timeout=0`` -- **non-blocking poll**: return ``(ready_now, pending_now)``
          immediately by probing each future's non-consuming readiness. ``ready``
          holds every currently-ready future (a true readiness partition, so it may
          hold fewer OR more than ``num_returns``); ``num_returns`` is still
          range-checked for contract parity with the blocking path.
        * ``timeout`` finite and **> 0** -- raises ``NotImplementedError``: HPX
          v1.11.0 has no non-consuming timed multi-future wait primitive. Use
          ``timeout=0`` to poll or ``timeout=None`` to block.

        ``num_returns`` must be an ``int`` (``bool`` rejected, no float coercion)
        with ``1 <= num_returns <= len(futures)``; a non-``RuntimeFuture`` entry
        raises ``TypeError``. A negative / ``NaN`` / ``inf`` / ``bool`` /
        non-numeric ``timeout`` raises ``TypeError`` / ``ValueError``. All input
        validation (empty list, ``num_returns``, entry type, ``timeout``) is shared
        by the blocking and polling modes and happens before any future is inspected.

        **Concurrency (accepted non-guarantees, same as the harness):**

        * *Per-future access is not thread-safe.* ``wait`` is non-consuming and safe
          for normal single-driver use, but do **not** call
          :meth:`RuntimeFuture.ready` / :meth:`RuntimeFuture.result` on the *same*
          future from another Python thread while it is inside this ``wait`` (the
          blocking path briefly moves the underlying HPX future out during the
          GIL-released ``hpx::wait_some``). :meth:`RuntimeFuture.cancel` **remains
          safe** -- it uses the independent cancel token, not the future.
        * *Concurrent shutdown during a blocked wait is unsupported.* Calling
          :meth:`shutdown` from another thread while this ``wait`` (or a
          :meth:`RuntimeFuture.result`) is blocked is not supported (it could stop
          HPX out from under the blocked wait). Normal usage is a single driver
          owning the runtime lifetime.
        """
        if not futures:
            raise ValueError("wait() requires a non-empty list of futures")
        # Shared input validation (both modes): num_returns is a strict int (bool
        # rejected; NO float coercion), range-checked; every entry must be a
        # RuntimeFuture -> a deliberate TypeError, not a stray AttributeError off
        # ._rf. Done up front so a bad input never yields a partial partition and
        # both the blocking and polling paths reject identically.
        if isinstance(num_returns, bool) or not isinstance(num_returns, int):
            raise TypeError(
                f"num_returns must be int, got {type(num_returns).__name__}")
        if num_returns < 1:
            raise ValueError(f"num_returns must be >= 1, got {num_returns}")
        if num_returns > len(futures):
            raise ValueError(
                f"num_returns must be <= len(futures) ({len(futures)}), "
                f"got {num_returns}")
        for i, fut in enumerate(futures):
            if not isinstance(fut, RuntimeFuture):
                raise TypeError(
                    f"wait() futures[{i}] must be a RuntimeFuture, got "
                    f"{type(fut).__name__}")
        if timeout is None:
            # Blocking path: hpx::wait_some in C++, GIL released. Duplicate /
            # retired / shut-down are rejected in C++ before any future is moved
            # out (entry type + num_returns already validated above).
            ready_idx = set(self._engine.wait([f._rf for f in futures],
                                              num_returns))
            ready, not_ready = [], []
            for i, fut in enumerate(futures):
                (ready if i in ready_idx else not_ready).append(fut)
            return ready, not_ready
        t = validate_timeout(timeout)
        if t > 0.0:
            raise NotImplementedError(
                "bounded finite-timeout wait is not supported on HPX v1.11.0: "
                "there is no non-consuming timed multi-future wait primitive. "
                "Use timeout=0 for a non-blocking poll or timeout=None to block.")
        # timeout == 0: non-blocking poll. Pure-Python over the non-consuming
        # RuntimeFuture.ready(); no C++ crossing into wait_some, no block. The
        # remaining structural guards (runtime running, no duplicate, no retired)
        # mirror what the blocking path delegates to C++.
        if self._closed:
            raise RuntimeError("Runtime is shut down")
        seen = set()
        for fut in futures:
            if id(fut._rf) in seen:
                raise ValueError(
                    "wait() received the same RuntimeFuture more than once; each "
                    "future may appear at most once")
            seen.add(id(fut._rf))
        # Readiness probe: _rf.ready() is non-consuming and raises on a retired
        # future (same guard as the blocking path). Raising here happens before we
        # return, so a bad input never yields a partial partition.
        ready, not_ready = [], []
        for fut in futures:
            (ready if fut._rf.ready() else not_ready).append(fut)
        return ready, not_ready

    def as_completed(self, futures):
        """Yield the given :class:`RuntimeFuture` objects as they become ready.

        Ergonomic generator over :meth:`wait`: copies ``futures`` into an internal
        in-flight list, then repeatedly blocks on ``wait(inflight, num_returns=1)``
        -- so the block happens inside C++/HPX with the GIL released
        (``hpx::wait_some``), NOT a Python busy-poll -- yielding the ready futures
        from each sweep and continuing with the still-not-ready ones until all are
        exhausted.

        Yields the ORIGINAL :class:`RuntimeFuture` objects (each preserving its
        ``submit_ns``), NOT :class:`OperationResult`\\ s; the caller is responsible
        for calling :meth:`RuntimeFuture.result`. Each input future is yielded
        exactly once. Failed / cancelled futures are yielded like any other (ready
        once their row exists); the caller's ``.result().value`` is what raises.
        """
        inflight = list(futures)
        while inflight:
            ready, inflight = self.wait(inflight, num_returns=1)
            yield from ready

    def get(self, futures, recv_ns=None):
        """Retire one :class:`RuntimeFuture` or a list/tuple of them; return result(s).

        A single ``RuntimeFuture`` returns one :class:`OperationResult`; a list or
        tuple returns ``list[OperationResult]`` **in input order** (same length).
        It is **not** ``ray.get`` -- RayX has no object store; this returns the
        RayX :class:`OperationResult` (value + measurement row), not a remote object.

        Like :meth:`RuntimeFuture.result`, it **consumes/retires** each future
        exactly once: a second ``get()`` / ``result()`` on the same future raises
        (the once-only guard). **No fail-fast**: a failed or cancelled operation
        still yields an ``OperationResult`` whose ``.row`` is inspectable -- only the
        per-element ``.value`` raises (:class:`OperationFailedError` /
        :class:`OperationCancelledError`) when accessed. An all-or-nothing check is
        the caller's to make. ``recv_ns`` is an optional client receive timestamp
        (``time.perf_counter_ns()``) forwarded to each ``result(recv_ns=...)``; like
        ``result``, it is only correct for an already-ready future.
        """
        if isinstance(futures, RuntimeFuture):
            return futures.result(recv_ns=recv_ns)
        return [f.result(recv_ns=recv_ns) for f in futures]

    def num_lanes(self):
        """Number of HPX-native ``RuntimeLane`` workers this runtime owns."""
        if self._closed:
            raise RuntimeError("Runtime is shut down")
        return self._engine.num_lanes()

    def lane_stats(self):
        """Per-lane observability snapshot (debugging only).

        Returns a list (in stable lane order) of ``{actor_id, queue_depth,
        active}`` dicts: ``actor_id`` is the lane's ``rt-hpx-`` id, ``queue_depth``
        is the queued-but-not-started count, and ``active`` is whether the lane is
        currently in a service slot. Non-consuming snapshot -- it touches no future
        and changes no submit/service semantics; values can change the instant after
        it returns. Raises if the runtime is shut down. Mirrors
        :meth:`rayx.Engine.lane_stats`; not scheduler state, not placement control,
        not part of the JSONL schema.
        """
        if self._closed:
            raise RuntimeError("Runtime is shut down")
        return self._engine.lane_stats()

    def shutdown(self):
        """Stop the runtime and release the process HPX-runtime guard."""
        if self._closed:
            return
        self._closed = True
        self._engine.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.shutdown()
        return False

    def __del__(self):
        if not getattr(self, "_closed", True):
            try:
                self.shutdown()
            except Exception:
                pass
