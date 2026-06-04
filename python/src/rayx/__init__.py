"""rayx: thin Python frontend over the HPX synthetic service lanes.

Minimal Engine/Future API backed by the HPX runtime (started as a library
inside the compiled ``_rayx`` extension). The Python layer owns client-side
timing (``submit_ns``/``total_ms`` on ``time.perf_counter_ns``, including the
pybind11/GIL crossing); the C++ layer owns service timing (steady_clock).

Boundary for any driver over this module: ``hpx-python-frontend``.

Example::

    from rayx import Engine
    engine = Engine(num_lanes=4, hpx_threads=4)
    row = engine.submit(service_ms=5).result()
    engine.shutdown()
"""

from collections.abc import Iterator
import math
import time
import warnings

try:
    from ._rayx import _Engine, _Future, hpx_smoke
except ImportError as exc:
    # _rayx is the compiled pybind11/HPX extension; rayx cannot work without it.
    # Covers both "not built" (ModuleNotFoundError) and "built but won't load"
    # (missing HPX shared libraries / Python ABI mismatch -> ImportError). Give a
    # build-actionable message but preserve the original loader error as the
    # cause (`from exc`); do NOT stub a fake module or mask post-import errors.
    raise ImportError(
        "RayX native extension '_rayx' is not available. Build HPX v1.11.0 and "
        "the pybind extension before importing rayx: with HPX_PREFIX set to "
        "your HPX install prefix, configure with `cmake -S python -B "
        "python/build -DPYBIND11_FINDPYTHON=ON "
        "-DCMAKE_PREFIX_PATH=\"${HPX_PREFIX};$(python -m pybind11 --cmakedir)\"`"
        ", then `cmake --build python/build`. If _rayx is built but fails to "
        "load, check it was built for this Python (ABI) and that the HPX "
        "shared libraries are findable. See docs/hpx_build_notes.md."
    ) from exc

__all__ = ["Engine", "Future", "SyntheticActor", "QueueFullError", "hpx_smoke"]


class QueueFullError(RuntimeError):
    """Raised by :meth:`Engine.submit` when bounded admission rejects a request.

    Only raised when the :class:`Engine` was built with a finite
    ``max_queue_depth_per_lane`` and the round-robin **target lane** already
    holds that many queued-but-not-started requests. It is raised **before** any
    :class:`Future` is created, so a rejected request has no Future, no result
    row, and no JSONL row. Subclasses :class:`RuntimeError`, so existing
    ``except RuntimeError`` still catches it, while a load-shedding caller can
    catch ``QueueFullError`` specifically (and distinguish it from the
    ``RuntimeError`` raised after :meth:`Engine.shutdown`).

    This is **local, per-lane admission by rejection** — not Ray Serve
    backpressure, not distributed flow control, not a scheduler, and not blocking
    backpressure (the call returns immediately by raising; it never blocks waiting
    for space).
    """


def _validate_label(label: "str | None") -> "str | None":
    """Validate an optional client-side request label.

    ``label`` must be ``None`` or a ``str``; anything else raises ``TypeError``
    (validated before the C++ crossing). The label is client-side metadata only
    -- see :class:`Future` -- so this is the single point that enforces its type.
    """
    if label is not None and not isinstance(label, str):
        raise TypeError(
            f"label must be str or None, got {type(label).__name__}")
    return label


def _validate_max_queue_depth_per_lane(value: "int | None") -> int:
    """Validate the per-lane bounded-admission cap and map it to the C++ sentinel.

    ``None`` (default) means **unbounded** and maps to ``-1`` (the C++ sentinel
    that preserves the original behavior exactly). Otherwise it must be an ``int``
    >= 1 (``bool`` is rejected even though it is an ``int`` subclass); ``0``,
    negatives, floats, and other types raise. Returns the int passed to
    ``_Engine`` (``-1`` for ``None``).
    """
    if value is None:
        return -1
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            "max_queue_depth_per_lane must be a positive int or None, got "
            f"{type(value).__name__}")
    if value < 1:
        raise ValueError(
            f"max_queue_depth_per_lane must be >= 1 or None, got {value}")
    return value


def _validate_lane_impl(value: str) -> str:
    """Validate the lane backend selector and return it unchanged.

    ``"std"`` (default) selects the std::thread ``ServiceLane`` stable comparison
    anchor; ``"hpx"`` selects the opt-in cooperative HPX-thread lane. Both honor
    the same lane contract (FIFO, actor_id, queue_depth/active, bounded admission,
    queued/running cancellation) and leave the result-row/JSONL schema unchanged;
    the choice is visible only through the lane's ``actor_id`` prefix
    (``act-hpx-`` vs ``act-hpxl-``). A non-``str`` raises ``TypeError``; an
    unrecognized string raises ``ValueError``.
    """
    if not isinstance(value, str):
        raise TypeError(
            f'lane_impl must be "std" or "hpx", got {type(value).__name__}')
    if value not in ("std", "hpx"):
        raise ValueError(f'lane_impl must be "std" or "hpx", got {value!r}')
    return value


# The synthetic service work modes the native lane supports (service_lane.hpp):
# "sleep" parks the lane in a blocking sleep_for (waiting/parked service time);
# "spin" keeps it busy on-core in a no-yield loop until the requested duration
# elapses (CPU-bound active service time). Both are synthetic service-time
# SHAPES, not payload/function execution -- service_ms sets the target duration
# either way.
_WORK_MODES = ("sleep", "spin")


def _validate_work_mode(work_mode: str) -> str:
    """Validate the synthetic service ``work_mode`` before the C++ crossing.

    Must be one of :data:`_WORK_MODES` (``"sleep"`` or ``"spin"``); a non-``str``
    raises ``TypeError`` and an unrecognized string raises ``ValueError``. This
    is the single enforcement point (every facade submit path funnels through
    :meth:`Engine.submit` / :meth:`Engine.submit_batch`), so a typo fails fast at
    the Python boundary instead of letting the native lane return a
    ``status="failed"`` row for an unsupported mode. Unlike ``label``,
    ``work_mode`` crosses into C++ (it selects the lane's service path), but --
    also unlike ``label`` -- it is NOT echoed in the result row: it is an
    execution input whose effect is already visible in ``service_ms_observed``.
    """
    if not isinstance(work_mode, str):
        raise TypeError(
            f"work_mode must be str, got {type(work_mode).__name__}")
    if work_mode not in _WORK_MODES:
        raise ValueError(
            f"work_mode must be one of {_WORK_MODES}, got {work_mode!r}")
    return work_mode


def _validate_batch_service_ms(
        service_ms: "float | list | tuple",
        count: "int | None") -> "list[float] | None":
    """Resolve the batch ``service_ms`` argument into scalar vs varied form.

    Returns ``None`` for the **scalar** form (a single ``service_ms`` applied to
    ``count`` requests -- unchanged behavior). Returns a ``list[float]`` of
    per-request service times for the **varied** form, when ``service_ms`` is a
    ``list``/``tuple``.

    Varied-form rules, all enforced here before the C++ crossing:

    * the list/tuple must be **non-empty** (else ``ValueError``);
    * every element must be a real number -- ``int`` or ``float``, **not**
      ``bool`` (else ``TypeError``);
    * every element must be **finite and strictly positive** -- no ``NaN`` /
      ``inf``, no zero, no negative (else ``ValueError``). The degenerate ``0``
      no-op is only the scalar ``service_ms=0`` form, never a list element;
    * if ``count`` is also passed it must equal ``len(service_ms)`` (else
      ``ValueError``); when omitted, ``count`` is inferred from the length.

    This keeps single-request ``submit`` / ``remote`` scalar-only and confines
    variable service time to the batch paths.
    """
    if not isinstance(service_ms, (list, tuple)):
        return None  # scalar form, unchanged
    if len(service_ms) == 0:
        raise ValueError("service_ms list/tuple must be non-empty")
    out: "list[float]" = []
    for i, x in enumerate(service_ms):
        # bool is an int subclass; reject it explicitly (almost always a mistake)
        if isinstance(x, bool) or not isinstance(x, (int, float)):
            raise TypeError(
                f"service_ms[{i}] must be a real number (int/float), got "
                f"{type(x).__name__}")
        xf = float(x)
        if not math.isfinite(xf):
            raise ValueError(f"service_ms[{i}] must be finite, got {x!r}")
        if xf <= 0.0:
            raise ValueError(
                f"service_ms[{i}] must be > 0 in the varied batch form "
                f"(use the scalar service_ms=0 for a no-op), got {x!r}")
        out.append(xf)
    if count is not None and count != len(out):
        raise ValueError(
            f"count ({count}) must equal len(service_ms) ({len(out)}) when a "
            "service_ms list/tuple is given (omit count to infer it)")
    return out


def _validate_chunks(chunks: int) -> int:
    """Validate the chunk count for chunked synthetic service.

    Must be an ``int`` >= 1; ``bool`` is rejected (it is an ``int`` subclass and
    almost always a mistake) and so is any non-``int``. ``chunks=1`` is the
    unchunked default. Validated before the C++ crossing, like
    :func:`_validate_work_mode`, so a bad value fails fast at the boundary.
    """
    if isinstance(chunks, bool) or not isinstance(chunks, int):
        raise TypeError(f"chunks must be an int, got {type(chunks).__name__}")
    if chunks < 1:
        raise ValueError(f"chunks must be >= 1, got {chunks}")
    return chunks


def _validate_chunk_delay_ms(chunk_delay_ms: float) -> float:
    """Validate the parked inter-chunk delay (ms) for chunked service.

    Must be a finite real number (``int``/``float``, **not** ``bool``) >= 0.
    ``0.0`` (default) means no inter-chunk gap. Validated before the C++ crossing.
    The gap is a parked wait inserted between chunks (``chunks-1`` gaps), so with
    ``chunk_delay_ms > 0`` a request's ``service_ms_observed`` (lifecycle span)
    includes these parked gaps, not just active service.
    """
    if isinstance(chunk_delay_ms, bool) or not isinstance(
            chunk_delay_ms, (int, float)):
        raise TypeError(
            f"chunk_delay_ms must be a real number, got "
            f"{type(chunk_delay_ms).__name__}")
    v = float(chunk_delay_ms)
    if not math.isfinite(v):
        raise ValueError(f"chunk_delay_ms must be finite, got {chunk_delay_ms!r}")
    if v < 0.0:
        raise ValueError(f"chunk_delay_ms must be >= 0, got {chunk_delay_ms!r}")
    return v


def _validate_timeout(timeout: float) -> float:
    """Validate a non-``None`` ``Engine.wait`` ``timeout`` (seconds).

    ``timeout`` is in **seconds** (Ray/Python convention). ``None`` means block
    (handled by the caller, never reaches here). Otherwise it must be a finite
    real number (``int``/``float``, **not** ``bool``) >= 0; ``NaN`` / ``inf`` /
    negative / non-numeric / ``bool`` all raise here, before any future is
    inspected. Only ``0`` (a non-blocking poll) is actually supported on top of
    HPX v1.11.0 -- a finite **positive** timeout is rejected by the caller
    (:meth:`Engine.wait`), because HPX v1.11.0 has no non-consuming timed
    multi-future wait primitive (see ``docs/reference/rayx_frontend_design.md``
    §9). This validates the *value*; the caller decides poll vs reject.
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


class Future:
    """Handle for one in-flight request.

    Carries the Python-side ``submit_ns`` captured at submission so that
    ``result()`` can report client-observed ``total_ms`` (which includes the
    pybind11/GIL crossing in both directions).

    May also carry an optional client-side ``label`` (a ``str`` supplied at
    submit). The label is pure client metadata: it never crosses into C++, never
    influences execution, is not stored, and is not part of the benchmark JSONL.
    ``result()`` echoes it back as ``row["label"]`` (``None`` when not supplied),
    useful for mapping a retired row back to a user-level request id.
    """

    __slots__ = ("_cf", "_submit_ns", "_retired", "_label", "_chunks",
                 "_chunk_delay_ms")

    def __init__(self, cf: "_Future", submit_ns: int, label: "str | None" = None,
                 chunks: int = 1, chunk_delay_ms: float = 0.0):
        self._cf = cf
        self._submit_ns = submit_ns
        # Python-only flag flipped True after a successful result(); used by
        # __repr__ so it never probes a consumed _cf (which would raise).
        self._retired = False
        # Client-side metadata only (never crosses C++); echoed by result().
        self._label = label
        # Chunked-service parameters used for this request, echoed by result()
        # (parallel to label). The chunk count/delay DO cross into C++ (they
        # drive the lane's chunk loop), but the row echo is from this Python copy
        # -- the C++ Result carries no chunk field. Defaults (1, 0.0) for
        # single-step and for all batch-submitted futures (batch is unchunked).
        self._chunks = chunks
        self._chunk_delay_ms = chunk_delay_ms

    def ready(self) -> bool:
        """Non-blocking: True if the result can be retired without blocking.

        Raises if this Future was already retired via ``result()``. This is a
        cheap test/building block -- do NOT spin on it in a Python loop; use
        :meth:`Engine.wait` to block in C++/HPX with the GIL released, or
        ``Engine.wait(futures, timeout=0)`` for a non-consuming readiness
        partition over many Futures at once. (There is deliberately no
        ``done()`` alias -- ``ready()`` is the single non-blocking readiness
        predicate; the honest answer on a retired Future is "raise", not a quiet
        boolean.)
        """
        return self._cf.ready()

    def cancelled(self) -> bool:
        """Non-blocking, non-consuming: is this request's cancellation settled?

        Returns ``True`` once :meth:`Engine.cancel` (or
        :meth:`SyntheticActor.cancel`) has settled a cancellation for this
        request -- either a **queued** cancel (skipped before its lane started)
        or a **running** stop requested at a chunk boundary of a chunked request
        (the lane will stop at the next boundary). This can become ``True``
        *before* the cancelled row is ready; use :meth:`ready` / :meth:`result`
        for readiness/retirement. Returns ``False`` otherwise -- including for a
        request that completed normally and for a non-cancelable (batch-submitted)
        future. Unlike :meth:`ready` / :meth:`result`, this does NOT raise after
        retire and does NOT consume the Future: it reads the cancellation state
        only, so it is valid before and after ``result()``.
        """
        return self._cf.cancelled()

    def result(self, recv_ns: int | None = None) -> dict:
        """Retire this Future and return its measured row.

        Retiring **consumes** the Future: ``result()`` may be called only once.
        A second call raises ``RuntimeError`` (and ``ready()`` likewise raises
        after retire), instead of surfacing a raw HPX error.

        ``recv_ns`` is an optional caller-supplied receive timestamp (a
        ``time.perf_counter_ns()`` value). The as-completed retire path passes
        ONE shared ``recv_ns`` for every Future retired in a single
        :meth:`Engine.wait` sweep, matching native ``batch_wait`` (one batch
        ``recv_ns`` per sweep). When omitted it is captured here, as before.
        Passing ``recv_ns`` is only correct for an already-ready Future
        (``ready()`` is True); otherwise ``result()`` blocks and the supplied
        timestamp would predate completion.
        """
        raw = self._cf.result()  # blocks; GIL released in C++ during the wait
        self._retired = True     # consumed; __repr__ now reports "retired"
        if recv_ns is None:
            recv_ns = time.perf_counter_ns()
        total_ms = (recv_ns - self._submit_ns) / 1e6
        service_ms = raw["service_ms_observed"]
        # queue_wait is approximate here, like Ray: submit_ns (Python clock) and
        # start_ns (C++ clock) are different clock domains, so we do not
        # subtract them. total_ms is the client-authoritative latency.
        queue_wait_ms = total_ms - service_ms
        if queue_wait_ms < 0.0:
            queue_wait_ms = 0.0
        return {
            "actor_id": raw["actor_id"],
            "submit_ns": self._submit_ns,
            "start_ns": raw["start_ns"],
            "end_ns": raw["end_ns"],
            "total_ms": total_ms,
            "queue_wait_ms": queue_wait_ms,
            "service_ms_observed": service_ms,
            "status": raw["status"],
            "error": raw["error"],
            # Client-side label echo: the supplied str, or None when omitted.
            # Facade-only -- not part of the benchmark JSONL schema.
            "label": self._label,
            # Chunked-service echo (facade rows): the chunk count and parked
            # inter-chunk delay used for this request (1 / 0.0 when unchunked and
            # for batch-submitted futures). With chunk_delay_ms > 0,
            # service_ms_observed above is lifecycle/lane-occupancy time (active
            # service + the chunks-1 parked gaps), NOT active-only service time.
            "chunks": self._chunks,
            "chunk_delay_ms": self._chunk_delay_ms,
            # Lane-determined: how many active chunks ACTUALLY ran. Unlike
            # "chunks" (the requested count, echoed from the Python copy above),
            # this comes from the C++ Result -- the client cannot know where an
            # early stop landed. == chunks on a normal finish; 0 on a queued
            # cancel (nothing ran); 1 <= chunks_completed < chunks on a running
            # (chunk-boundary) cancel. For batch-submitted futures (unchunked,
            # never cancelable) it is 1, matching chunks.
            "chunks_completed": raw["chunks_completed"],
        }

    def __repr__(self) -> str:
        """Debug-friendly, non-blocking, non-consuming repr.

        Shows ``pending``/``ready`` for a live future and ``retired`` after a
        successful ``result()``. For a live future it does a single
        non-blocking ``_cf.ready()`` probe; once ``_retired`` is set it must
        NOT touch ``_cf`` (the result was moved out and probing would raise).
        """
        if self._retired:
            state = "retired"
        else:
            state = "ready" if self._cf.ready() else "pending"
        return f"<rayx.Future {state} submit_ns={self._submit_ns}>"


class Engine:
    """Process-singleton HPX-backed engine with ``num_lanes`` serialized lanes.

    Only one active Engine is allowed per process; constructing a second before
    calling ``shutdown()`` on the first raises. Usable as a context manager.

    ``max_queue_depth_per_lane`` (default ``None`` = unbounded, exactly the
    original behavior) optionally enables **bounded admission**: each lane admits
    at most that many queued-but-not-started requests, and :meth:`submit` raises
    :class:`QueueFullError` when the round-robin target lane is full. It is a
    local, per-lane admission-by-rejection mechanism (not Ray Serve backpressure,
    distributed flow control, a scheduler, or blocking backpressure); see
    :meth:`submit` and :class:`QueueFullError`.

    ``lane_impl`` selects the lane backend: ``"std"`` (default) is the
    std::thread ``ServiceLane`` stable comparison anchor; ``"hpx"`` is the opt-in
    cooperative HPX-thread lane. Both honor the identical lane contract (FIFO,
    actor_id, queue_depth/active, bounded admission, queued/running cancellation)
    and leave every result-row/JSONL field unchanged -- the only visible
    difference is the lane's ``actor_id`` prefix (``act-hpx-`` vs ``act-hpxl-``).
    No HPX internals are exposed to Python by either choice.
    """

    def __init__(self, num_lanes: int = 1, hpx_threads: int = 1,
                 max_queue_depth_per_lane: "int | None" = None,
                 lane_impl: str = "std"):
        # max_queue_depth_per_lane: None (default) = unbounded, preserving the
        # original behavior exactly. A positive int bounds each lane's queued-but-
        # not-started depth; submit() then raises QueueFullError when the target
        # lane is full. Validated here (None / positive int) and passed to C++ as
        # -1 (None) or the int. Stored so submit_batch can refuse under a cap.
        self._max_queue_depth_per_lane = max_queue_depth_per_lane
        cap = _validate_max_queue_depth_per_lane(max_queue_depth_per_lane)
        # lane_impl: "std" (default) keeps the ServiceLane anchor; "hpx" opts into
        # the cooperative HpxLane backend. Validated (str / known value) before the
        # process-singleton HPX runtime is touched, then forwarded to _Engine.
        lane_impl = _validate_lane_impl(lane_impl)
        self._engine = _Engine(num_lanes=num_lanes, hpx_threads=hpx_threads,
                               max_queue_depth_per_lane=cap,
                               lane_impl=lane_impl)
        self._closed = False

    def submit(self, service_ms: float = 0.0, work_mode: str = "sleep",
               label: "str | None" = None, chunks: int = 1,
               chunk_delay_ms: float = 0.0) -> Future:
        """Submit one synthetic request to a service lane; returns a :class:`Future`.

        Captures the Python-side ``submit_ns`` (``time.perf_counter_ns``) before
        the single Python->C++ crossing, so the returned Future can later report
        client-observed ``total_ms``.

        ``work_mode`` selects the synthetic service-time shape (``"sleep"`` or
        ``"spin"``; an unrecognized value raises ``ValueError``, a non-``str``
        raises ``TypeError``). ``"sleep"`` (default) parks the lane in a blocking
        sleep for ``service_ms`` (waiting/parked service time); ``"spin"`` keeps
        the lane busy on-core for ``service_ms`` (CPU-bound active service time).
        Both are synthetic service-time shapes, not payload execution; the effect
        shows in ``service_ms_observed`` and the result-row schema is unchanged
        (``work_mode`` is not echoed in the row).

        ``label`` is an optional client-side request annotation (``str`` or
        ``None``; a bad type raises ``TypeError``). It is pure client metadata --
        it never crosses into C++, never influences execution, and is not part of
        the benchmark JSONL -- and is echoed back by ``result()`` as
        ``row["label"]`` (``None`` when omitted). Not accepted by
        :meth:`submit_batch`.

        ``chunks`` / ``chunk_delay_ms`` are the **chunked synthetic service**
        controls (v1 streaming). ``chunks`` (an ``int`` >= 1, default ``1``)
        splits the **total** active ``service_ms`` into that many equal active
        steps of ``service_ms / chunks`` each (using ``work_mode``);
        ``chunk_delay_ms`` (finite, >= 0, default ``0``) is a **parked** gap
        (blocking sleep, both modes) inserted between consecutive chunks
        (``chunks-1`` gaps), modelling token-like cadence. This is still synthetic
        service-time control -- **not** real token streaming, not payload
        execution, no per-chunk rows/events: the request still returns **one**
        :class:`Future` and **one** final row. The lane is occupied for the whole
        chunked lifecycle (FIFO preserved), so with ``chunk_delay_ms > 0`` the
        row's ``service_ms_observed`` is lifecycle/lane-occupancy time (active
        service **plus** the parked gaps), **not** active-only service. The row
        echoes ``chunks`` / ``chunk_delay_ms``. ``chunks=1, chunk_delay_ms=0``
        (default) reproduces the unchunked single-step behavior exactly. Chunking
        is **single-request only** -- :meth:`submit_batch` does not accept it.

        **Bounded admission.** If the :class:`Engine` was built with a finite
        ``max_queue_depth_per_lane``, this raises :class:`QueueFullError` when the
        round-robin **target lane** already holds that many queued-but-not-started
        requests (the in-service request is not counted). The rejection is raised
        **before** any Future is created -- no Future, no result row, no JSONL row
        -- and the round-robin rotation still advances (a rejected call consumes
        its lane turn, so one full lane never stalls submissions to other lanes).
        With the default ``max_queue_depth_per_lane=None`` no admission check runs.
        """
        _validate_label(label)
        _validate_work_mode(work_mode)
        n_chunks = _validate_chunks(chunks)
        delay = _validate_chunk_delay_ms(chunk_delay_ms)
        submit_ns = time.perf_counter_ns()
        cf = self._engine.submit(float(service_ms), work_mode, n_chunks, delay)
        if cf is None:
            # Bounded admission: the C++ engine returns None when a per-lane cap
            # is set and the round-robin target lane is full. Raise BEFORE building
            # a Future -- a rejected request has no Future, no row, no JSONL row.
            raise QueueFullError(
                "submit rejected: the target lane already holds "
                f"max_queue_depth_per_lane={self._max_queue_depth_per_lane} "
                "queued-but-not-started requests (active request not counted). "
                "Bounded admission by rejection; retire/drain work or retry.")
        return Future(cf, submit_ns, label=label, chunks=n_chunks,
                      chunk_delay_ms=delay)

    def submit_batch(
        self, service_ms: "float | list[float] | tuple[float, ...]" = 0.0,
        count: "int | None" = None, work_mode: str = "sleep"
    ) -> list[Future]:
        """Submit a batch with a single Python->C++ crossing; two forms.

        **Scalar form** -- ``submit_batch(service_ms=5, count=100)`` -- submits
        ``count`` requests that all share one ``service_ms`` (``count`` defaults
        to 1). **Varied form** -- ``submit_batch(service_ms=[1, 5, 1, 10])`` --
        submits one request per element of the list/tuple, each with its **own**
        service time, in input order; ``count`` is inferred from the length (and,
        if also passed, must equal it, else ``ValueError``). The varied form lets
        a single bulk submission model a heterogeneous / skewed synthetic service
        workload. See :func:`_validate_batch_service_ms` for the exact rules
        (non-empty; finite, strictly-positive real elements; no ``bool``).

        Both forms keep the same **bulk semantics**: one Python->C++ crossing
        enqueues every request (the varied form uses the native
        ``submit_batch_varied`` path, **not** a Python loop over :meth:`submit`),
        and every returned :class:`Future` shares one ``submit_ns`` captured
        before the FFI call -- so per-request ``total_ms`` is queue-shaped
        (requests deeper in the batch include their queue position) and the
        meaningful batch signal is submission overhead / throughput, not clean
        steady-state per-request latency.

        ``work_mode`` selects the synthetic service-time shape for every request
        in the batch (``"sleep"`` or ``"spin"``, same meaning and validation as
        :meth:`submit`). Single-request ``service_ms`` stays scalar-only; only
        the batch paths take a list. Like :meth:`submit`, the batch path does not
        accept a ``label``. The result-row schema is unchanged -- each row already
        reports its own ``service_ms_observed``.

        **Bounded admission.** ``submit_batch`` does **not** participate in
        bounded admission in v1: if the :class:`Engine` was built with a finite
        ``max_queue_depth_per_lane``, this raises ``RuntimeError`` (it refuses
        loudly rather than silently bypass the per-lane cap). Bounded admission is
        a :meth:`submit`-only feature; build the Engine without a cap to use the
        batch throughput path.
        """
        if self._max_queue_depth_per_lane is not None:
            # Bounded admission is a submit()-only feature in v1. The batch path
            # is the unbounded bulk/throughput probe (non-cancelable, multi-lane);
            # rather than silently bypass the cap, refuse loudly so the protection
            # cannot be defeated by accident. A plain RuntimeError (not
            # QueueFullError) because no lane is "full" here -- the operation is
            # simply unsupported under a cap. Use submit() for bounded admission,
            # or build the Engine without max_queue_depth_per_lane for batches.
            raise RuntimeError(
                "submit_batch is not supported when max_queue_depth_per_lane is "
                f"set (={self._max_queue_depth_per_lane}); bounded admission is a "
                "submit()-only feature in v1. Use submit() under a cap, or build "
                "the Engine without max_queue_depth_per_lane for batch submits.")
        _validate_work_mode(work_mode)
        svc_list = _validate_batch_service_ms(service_ms, count)
        submit_ns = time.perf_counter_ns()
        if svc_list is None:
            # Scalar form (unchanged): one service_ms applied to `count` requests
            # (count defaults to 1). C++ rejects count < 1 (-> ValueError).
            n = 1 if count is None else int(count)
            cfs = self._engine.submit_batch(float(service_ms), n, work_mode)
        else:
            # Varied form: one request per element with its own service time, a
            # single crossing via the native varied-batch path.
            cfs = self._engine.submit_batch_varied(svc_list, work_mode)
        return [Future(cf, submit_ns) for cf in cfs]

    def cancel(self, future: Future) -> bool:
        """Cancel a single submitted request: queued skip, or stop at a chunk boundary.

        Honest cancellation with two settling cases. Returns ``True`` iff **this
        call** settles a cancellation:

        * **Queued** -- the request had not started; its lane will skip service
          entirely (no service time spent).
        * **Running, chunked, with a boundary still ahead** -- the lane will stop
          at its **next chunk boundary**. ``True`` here means *guaranteed to
          stop*, **not** ready-now: the cancelled row appears only once the lane
          reaches that boundary. An active chunk's work and an in-progress parked
          inter-chunk gap are **never** interrupted.

        Returns ``False`` when no cancellation can be settled: the request already
        completed, already had a stop requested, is a started single-chunk
        (``chunks=1``) request, or is already running its **final** chunk (no
        boundary left). The queued-vs-running / boundary races are resolved in C++
        under the request's own lock, so exactly one of {serviced, cancelled}
        wins and the result promise is fulfilled exactly once.

        ``cancel()`` is **not** a retire: a cancelled Future still becomes ready
        and must still be consumed once via :meth:`Future.result` /
        :meth:`get`, which returns a measurement row with ``status="cancelled"``
        (preserving the submit-time ``label``). A queued cancel yields
        ``chunks_completed == 0``; a running cancel yields
        ``1 <= chunks_completed < chunks``. It raises for an already-retired
        Future (consumed via ``result``/``get``), after engine :meth:`shutdown`,
        and for a **non-cancelable** Future -- only single-request :meth:`submit`
        (and the actor ``remote`` / ``serve.remote`` forwards) are cancelable;
        batch-submitted futures are not (no batch-cancel in this slice).

        This is **not** Ray task/object cancellation: it cancels one synthetic
        request on an in-process HPX lane (queued skip or chunk-boundary early
        stop); it does not cancel a task graph, drop an object-store value, or
        interrupt an in-progress active chunk. The chunked model is synthetic
        cadence, not real token streaming.
        """
        return self._engine.cancel(future._cf)

    def wait(self, futures: list[Future], num_returns: int = 1,
             timeout: "float | None" = None) -> tuple[list[Future], list[Future]]:
        """Partition ``futures`` into ``(ready, not_ready)`` by readiness.

        Returns a partition of the SAME :class:`Future` objects -- so each keeps
        its Python-side ``submit_ns``. **Non-consuming**: this is readiness /
        retirement coordination, NOT result consumption -- you still call
        :meth:`Future.result` / :meth:`get` once to retire a ready Future. The
        given futures must not have been retired yet, and each must appear at most
        once (a duplicate raises ``ValueError``).

        ``timeout`` is in **seconds** (Ray/Python convention) and selects the mode:

        * ``timeout=None`` (default) -- **block** until at least ``num_returns`` of
          ``futures`` are ready, inside C++/HPX with the GIL released
          (``hpx::wait_some``); NOT a Python busy-poll. Mirrors
          ``ray.wait(num_returns=k)`` and the native ``batch_wait`` primitive.
          ``num_returns=1`` gives wait-any semantics.
        * ``timeout=0`` -- **non-blocking poll**: return ``(ready_now,
          pending_now)`` immediately by probing each Future's (non-consuming)
          readiness; nothing blocks. ``ready`` holds every currently-ready Future
          (it is a true readiness partition, so it may contain fewer OR more than
          ``num_returns``); ``num_returns`` is still range-checked
          (``1 <= num_returns <= len(futures)``) for contract parity with the
          blocking path. A *cancelled* Future is ready only once its row exists: a
          queued cancel is ready immediately, but a **running** chunk-boundary
          cancel stays pending until the lane reaches the boundary and retires the
          cancelled row.
        * ``timeout`` finite and **> 0** -- raises ``NotImplementedError``. A
          bounded finite-timeout wait needs a non-consuming *timed multi-future*
          wait, which HPX v1.11.0 does not provide (no ``wait_some_for`` /
          ``wait_some_until``; ``when_some`` would move/strand the inputs on
          timeout; busy-polling is deliberately avoided). See
          ``docs/reference/rayx_frontend_design.md`` §9. Use ``timeout=0`` to poll
          or ``timeout=None`` to block.

        A negative, ``NaN``, ``inf``, ``bool``, or non-numeric ``timeout`` raises
        ``TypeError`` / ``ValueError`` before any Future is inspected. A retired
        Future raises through the same guard as the blocking path (its
        ``ready()`` probe rejects a consumed Future).
        """
        if not futures:
            raise ValueError("wait() requires a non-empty list of futures")
        if timeout is None:
            # Blocking path (unchanged): hpx::wait_some in C++, GIL released.
            ready_idx = set(self._engine.wait([f._cf for f in futures],
                                              int(num_returns)))
            ready, not_ready = [], []
            for i, fut in enumerate(futures):
                (ready if i in ready_idx else not_ready).append(fut)
            return ready, not_ready
        t = _validate_timeout(timeout)
        if t > 0.0:
            raise NotImplementedError(
                "bounded finite-timeout wait is not supported on HPX v1.11.0: "
                "there is no non-consuming timed multi-future wait primitive "
                "(no hpx::wait_some_for / wait_some_until; when_some would "
                "move/strand the inputs on timeout; busy-polling is avoided). "
                "Use timeout=0 for a non-blocking poll or timeout=None to block. "
                "See docs/reference/rayx_frontend_design.md §9.")
        # timeout == 0: non-blocking poll. Pure-Python over the existing
        # non-consuming _Future.ready() -- no C++ crossing into wait_some, no
        # block. Replicate the blocking path's structural guards (engine running,
        # num_returns range, no duplicate, no retired) so both modes reject the
        # same bad input.
        if self._closed:
            raise RuntimeError("Engine is shut down")
        nr = int(num_returns)
        if nr < 1:
            raise ValueError("num_returns must be >= 1")
        if nr > len(futures):
            raise ValueError("num_returns must be <= len(futures)")
        seen = set()
        for fut in futures:
            cf = fut._cf  # AttributeError here if not a Future (wrong-type guard)
            if id(cf) in seen:
                raise ValueError(
                    "wait() received the same Future more than once; each "
                    "Future may appear at most once")
            seen.add(id(cf))
        # Readiness probe: cf.ready() is non-consuming and raises on a retired
        # Future (same guard as the blocking path). Raising here happens before
        # we return, so a bad input never yields a partial partition and never
        # consumes a Future.
        ready, not_ready = [], []
        for fut in futures:
            (ready if fut._cf.ready() else not_ready).append(fut)
        return ready, not_ready

    def as_completed(self, futures: list[Future]) -> Iterator[Future]:
        """Yield the given :class:`Future` objects as they become ready.

        Ergonomic generator over :meth:`wait`: copies ``futures`` into an
        internal in-flight list, then repeatedly blocks on
        ``wait(inflight, num_returns=1)`` -- so the block happens inside
        C++/HPX with the GIL released (``hpx::wait_some``), NOT a Python
        busy-poll -- yielding the ready Futures from each sweep and continuing
        with the still-not-ready ones until all are exhausted.

        Yields the ORIGINAL :class:`Future` objects (each preserving its
        ``submit_ns``); the caller is responsible for calling
        ``future.result()``. Each input future is yielded exactly once.

        This is a convenience wrapper, NOT the benchmark ``batch_wait`` retire
        path: it does not share one ``recv_ns`` across a ready sweep. Drivers
        that need per-sweep shared-``recv_ns`` fairness keep their explicit
        :meth:`wait` loop.
        """
        inflight = list(futures)
        while inflight:
            ready, inflight = self.wait(inflight, num_returns=1)
            yield from ready

    def get(self, futures: "Future | list[Future]",
            recv_ns: int | None = None) -> "dict | list[dict]":
        """Retire one :class:`Future` or a list of them; return measured row(s).

        Ray-flavored convenience over :meth:`Future.result`: a single ``Future``
        returns one row dict; a list returns a list of row dicts **in input
        order**. It is **not** ``ray.get`` -- RayX has no object store and no
        computed user value, so this returns RayX **measurement rows** (the
        per-request timing dict), not a function result.

        Like :meth:`Future.result`, it **consumes/retires** each Future: a given
        Future may be retired only once, so a second ``get()`` / ``result()`` on
        it raises (the once-only guard). ``recv_ns`` is an optional client
        receive timestamp (``time.perf_counter_ns()``) forwarded to each
        ``result(recv_ns=...)``; like ``result``, it is only correct for an
        already-ready Future.

        This is an ergonomic retire/collect helper, **not** the benchmark
        ``batch_wait`` retire path: it does not coordinate one shared ``recv_ns``
        across a wait sweep, so drivers that need per-sweep shared-``recv_ns``
        fairness keep their explicit :meth:`wait` loop.
        """
        if isinstance(futures, Future):
            return futures.result(recv_ns=recv_ns)
        return [f.result(recv_ns=recv_ns) for f in futures]

    def num_lanes(self) -> int:
        return self._engine.num_lanes()

    def lane_stats(self) -> "list[dict]":
        """Observability snapshot of the service lanes (debugging only).

        Returns one dict per lane, in **stable lane order** (the same order
        :meth:`submit` round-robins over)::

            [{"actor_id": "act-hpx-...", "queue_depth": 3, "active": True},
             {"actor_id": "act-hpx-...", "queue_depth": 0, "active": False}]

        * ``queue_depth`` -- requests **queued but not yet started** on that lane
          (the in-service request, if any, is not counted).
        * ``active`` -- ``True`` once the lane has **popped** a request and is in
          its service lifecycle, until that request's row is fulfilled (or a
          queued-cancel skip clears it); ``False`` when the lane is idle.

        This is for **observability/debugging only**. It is a **snapshot** -- the
        values can change the instant after it returns -- and **non-consuming**:
        it does not touch any :class:`Future` and does not affect
        ``submit``/``wait``/``cancel``/``result`` semantics. It is **not** Ray
        scheduler state, **not** placement control (lane choice stays internal
        round-robin; you cannot submit to a chosen lane), **not** a
        synchronization primitive, and **not** part of the benchmark JSONL /
        analyzer schema. Raises ``RuntimeError`` after :meth:`shutdown` (the lanes
        are destroyed), consistent with the other coordination APIs.
        """
        return self._engine.lane_stats()

    def shutdown(self) -> None:
        """Graceful drain: block until all queued/in-flight submitted work
        completes and every Future is fulfilled, then stop the HPX runtime.

        Work is drained to completion; shutdown itself never cancels or drops it
        (use ``Engine.cancel`` to cancel a still-queued request beforehand), so
        shutdown latency can scale with the outstanding queued service time.
        Futures submitted before shutdown stay valid afterward -- ``ready()`` is
        ``True`` and ``result()`` retires them -- while new work (``submit`` /
        ``submit_batch`` / ``cancel`` / ``wait`` / ``as_completed``) raises after
        shutdown.
        """
        if self._closed:
            return
        self._closed = True
        self._engine.shutdown()

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc) -> bool:
        self.shutdown()
        return False

    def __del__(self):
        if not getattr(self, "_closed", True):
            warnings.warn(
                "rayx.Engine was not shut down explicitly; cleaning up in "
                "__del__",
                ResourceWarning,
            )
            try:
                self.shutdown()
            except Exception:
                pass


class _SyntheticServeMethod:
    """Bound handle for :class:`SyntheticActor`'s single synthetic operation.

    Mirrors Ray's ``actor.method.remote(...)`` shape for exactly ONE fixed
    synthetic operation (``serve``); there are no other methods. It just
    forwards to the owning actor, so it carries no state and inherits the
    actor's consume-once / wait / get / shutdown semantics unchanged. Private:
    obtained only via the :attr:`SyntheticActor.serve` property, never imported.
    """

    __slots__ = ("_actor",)

    def __init__(self, actor: "SyntheticActor"):
        self._actor = actor

    def remote(self, service_ms: float = 0.0, work_mode: str = "sleep",
               label: "str | None" = None, chunks: int = 1,
               chunk_delay_ms: float = 0.0) -> Future:
        """Submit one synthetic request; returns a :class:`Future`.

        Forwards to :meth:`SyntheticActor.remote` (→ :meth:`Engine.submit`),
        including the optional client-side ``label`` and the chunked-service
        controls ``chunks`` / ``chunk_delay_ms`` (single-request only).
        """
        return self._actor.remote(
            service_ms=service_ms, work_mode=work_mode, label=label,
            chunks=chunks, chunk_delay_ms=chunk_delay_ms)

    def remote_batch(
        self, service_ms: "float | list[float] | tuple[float, ...]" = 0.0,
        count: "int | None" = None, work_mode: str = "sleep"
    ) -> list[Future]:
        """Submit a batch of synthetic requests; returns a list of
        :class:`Future`.

        Forwards to :meth:`SyntheticActor.remote_batch`
        (→ :meth:`Engine.submit_batch`), inheriting both the scalar
        (``service_ms``, ``count``) and varied (``service_ms=[...]``) forms and
        the same bulk semantics.
        """
        return self._actor.remote_batch(
            service_ms=service_ms, count=count, work_mode=work_mode)

    def __repr__(self) -> str:
        return "<rayx SyntheticActor.serve — single fixed synthetic operation>"


class SyntheticActor:
    """Ray-flavored ergonomic facade over a single :class:`Engine`.

    Exposes the recognizable Ray idiom -- an actor handle whose call returns a
    future:

        from rayx import SyntheticActor
        with SyntheticActor(num_lanes=2, hpx_threads=4) as actor:
            rows = [f.result() for f in
                    (actor.remote(service_ms=5) for _ in range(20))]

    What this is, precisely:

    * A thin wrapper that forwards ``remote(...)`` to ``Engine.submit(...)``;
      it adds no measurable overhead over the underlying Engine.
    * It runs the **synthetic native C++ service lane** (blocking sleep), the
      same work the native HPX baseline runs. Boundary: ``hpx-python-frontend``.

    What this is NOT:

    * Not a general Ray actor. It has a single fixed synthetic operation
      (``remote``), not arbitrary named methods.
    * It does **not** run arbitrary Python functions -- only native C++
      synthetic work is dispatched to the lane.
    * No object store, scheduler, fault tolerance, autoscaling, distributed
      placement, supervision, or named-actor registry.

    Like :class:`Engine`, it owns one HPX runtime, so only one active
    ``SyntheticActor`` (or ``Engine``) is allowed per process; constructing a
    second before ``shutdown()`` raises. Usable as a context manager.
    """

    def __init__(self, num_lanes: int = 1, hpx_threads: int = 1,
                 max_queue_depth_per_lane: "int | None" = None,
                 lane_impl: str = "std"):
        # Forwards the optional per-lane bounded-admission cap to the underlying
        # Engine: remote() then raises QueueFullError when the target lane is full,
        # and remote_batch() / serve.remote_batch() refuse under the cap (both via
        # Engine). Default None = unbounded (unchanged behavior).
        #
        # lane_impl ("std" default / "hpx") selects the lane backend and is
        # forwarded to Engine (validated there); "hpx" opts into the cooperative
        # HpxLane. The contract and result rows are identical -- only the lane
        # actor_id prefix differs (act-hpx- vs act-hpxl-).
        self._engine = Engine(num_lanes=num_lanes, hpx_threads=hpx_threads,
                              max_queue_depth_per_lane=max_queue_depth_per_lane,
                              lane_impl=lane_impl)

    def remote(self, service_ms: float = 0.0, work_mode: str = "sleep",
               label: "str | None" = None, chunks: int = 1,
               chunk_delay_ms: float = 0.0) -> Future:
        """Submit one synthetic request; returns a :class:`Future`.

        Forwards directly to :meth:`Engine.submit`, including the optional
        client-side ``label`` (echoed by ``result()`` as ``row["label"]``) and
        the chunked synthetic-service controls ``chunks`` / ``chunk_delay_ms``
        (single-request only; see :meth:`Engine.submit`). If the actor was built
        with a finite ``max_queue_depth_per_lane``, this raises
        :class:`QueueFullError` when the target lane is full (bounded admission).
        """
        return self._engine.submit(
            service_ms=service_ms, work_mode=work_mode, label=label,
            chunks=chunks, chunk_delay_ms=chunk_delay_ms)

    @property
    def serve(self) -> "_SyntheticServeMethod":
        """Ray-like method-style handle for the single synthetic operation.

        ``actor.serve.remote(service_ms=...)`` /
        ``actor.serve.remote_batch(service_ms=..., count=...)`` mirror Ray's
        ``worker.method.remote(...)`` shape, forwarding to :meth:`remote` /
        :meth:`remote_batch` and returning the SAME :class:`Future` /
        ``list[Future]`` (so ``wait`` / ``as_completed`` / ``get`` / ``result``
        and graceful-drain shutdown all behave identically).

        There is exactly ONE method, ``serve``; RayX does **not** support
        arbitrary actor methods, so any other dotted access (``actor.foo``)
        naturally raises ``AttributeError``. This is **optional sugar** --
        :meth:`remote` remains the primary API, and the benchmark driver uses
        ``remote``/``submit``, not this façade.
        """
        return _SyntheticServeMethod(self)

    def remote_batch(
        self, service_ms: "float | list[float] | tuple[float, ...]" = 0.0,
        count: "int | None" = None, work_mode: str = "sleep"
    ) -> list[Future]:
        """Submit a batch of synthetic requests in one crossing; returns a list
        of :class:`Future`.

        Ergonomic façade that forwards directly to :meth:`Engine.submit_batch`,
        inheriting its scalar form (``service_ms``, ``count``) and varied form
        (``service_ms=[1, 5, 1, 10]`` -- one request per element with its own
        service time, ``count`` inferred). Both keep the same bulk semantics: one
        Python->C++ crossing enqueues all requests, and every returned Future
        shares one Python-side ``submit_ns``, so per-request ``total_ms`` is
        queue-shaped (requests deeper in the batch include their queue position).
        The meaningful batch signal is submission overhead / throughput, not
        steady-state per-request latency.

        Like :meth:`remote`, this runs **native synthetic C++ work only** (the
        blocking-sleep or on-core-spin service lane). It is **not** a general Ray
        actor batch API: it dispatches the single fixed synthetic operation, not
        arbitrary Python functions or named actor methods.
        """
        return self._engine.submit_batch(
            service_ms=service_ms, count=count, work_mode=work_mode)

    def wait(self, futures: list[Future], num_returns: int = 1,
             timeout: "float | None" = None) -> tuple[list[Future], list[Future]]:
        """Forward to :meth:`Engine.wait` (readiness partition over Futures).

        ``timeout`` (seconds) is passed through unchanged: ``None`` blocks, ``0``
        is a non-blocking poll, and a finite ``> 0`` raises ``NotImplementedError``
        -- see :meth:`Engine.wait`.
        """
        return self._engine.wait(futures, num_returns=num_returns,
                                 timeout=timeout)

    def as_completed(self, futures: list[Future]) -> Iterator[Future]:
        """Forward to :meth:`Engine.as_completed` (yield Futures as ready)."""
        return self._engine.as_completed(futures)

    def get(self, futures: "Future | list[Future]",
            recv_ns: int | None = None) -> "dict | list[dict]":
        """Forward to :meth:`Engine.get` (retire one/list of Futures -> row(s))."""
        return self._engine.get(futures, recv_ns=recv_ns)

    def cancel(self, future: Future) -> bool:
        """Forward to :meth:`Engine.cancel` (queued skip, or a running chunked
        request stops at its next chunk boundary; True iff this call settles a
        cancellation). Works for ``remote`` / ``serve.remote`` futures."""
        return self._engine.cancel(future)

    def num_lanes(self) -> int:
        return self._engine.num_lanes()

    def lane_stats(self) -> "list[dict]":
        """Forward to :meth:`Engine.lane_stats` (observability snapshot of the
        service lanes: one ``{actor_id, queue_depth, active}`` dict per lane).
        Debugging only -- snapshot can race, non-consuming, not scheduler state /
        placement control / JSONL schema; raises after :meth:`shutdown`."""
        return self._engine.lane_stats()

    def shutdown(self) -> None:
        """Forward to :meth:`Engine.shutdown` (graceful drain; Futures submitted
        before shutdown remain valid and retirable afterward)."""
        self._engine.shutdown()

    def __enter__(self) -> "SyntheticActor":
        return self

    def __exit__(self, *exc) -> bool:
        self.shutdown()
        return False
