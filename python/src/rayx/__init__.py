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

import time
import warnings

from ._rayx import _Engine, _Future, hpx_smoke

__all__ = ["Engine", "Future", "SyntheticActor", "hpx_smoke"]


class Future:
    """Handle for one in-flight request.

    Carries the Python-side ``submit_ns`` captured at submission so that
    ``result()`` can report client-observed ``total_ms`` (which includes the
    pybind11/GIL crossing in both directions).
    """

    __slots__ = ("_cf", "_submit_ns")

    def __init__(self, cf: "_Future", submit_ns: int):
        self._cf = cf
        self._submit_ns = submit_ns

    def ready(self) -> bool:
        """Non-blocking: True if the result can be retired without blocking.

        Raises if this Future was already retired via ``result()``. This is a
        cheap test/building block -- do NOT spin on it in a Python loop; use
        :meth:`Engine.wait` to block in C++/HPX with the GIL released.
        """
        return self._cf.ready()

    def result(self, recv_ns: int = None) -> dict:
        """Retire this Future and return its measured row.

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
        }


class Engine:
    """Process-singleton HPX-backed engine with ``num_lanes`` serialized lanes.

    Only one active Engine is allowed per process; constructing a second before
    calling ``shutdown()`` on the first raises. Usable as a context manager.
    """

    def __init__(self, num_lanes: int = 1, hpx_threads: int = 1):
        self._engine = _Engine(num_lanes=num_lanes, hpx_threads=hpx_threads)
        self._closed = False

    def submit(self, service_ms: float = 0.0, work_mode: str = "sleep") -> Future:
        submit_ns = time.perf_counter_ns()
        cf = self._engine.submit(float(service_ms), work_mode)
        return Future(cf, submit_ns)

    def submit_batch(
        self, service_ms: float = 0.0, count: int = 1, work_mode: str = "sleep"
    ) -> list:
        """Submit ``count`` requests with a single Python->C++ crossing.

        Bulk semantics: every returned :class:`Future` shares one ``submit_ns``
        captured before the batch FFI call, so per-request ``total_ms`` is
        queue-shaped (requests deeper in the batch include their queue
        position). For batch mode the meaningful signal is submission
        overhead / throughput, not clean steady-state per-request latency.
        """
        submit_ns = time.perf_counter_ns()
        cfs = self._engine.submit_batch(float(service_ms), int(count), work_mode)
        return [Future(cf, submit_ns) for cf in cfs]

    def wait(self, futures, num_returns: int = 1):
        """Block until at least ``num_returns`` of ``futures`` are ready.

        Returns ``(ready, not_ready)`` as a partition of the SAME :class:`Future`
        objects -- so each keeps its Python-side ``submit_ns``. The wait blocks
        inside C++/HPX with the GIL released (``hpx::wait_some``); it is NOT a
        Python busy-poll. Mirrors ``ray.wait(num_returns=k)`` and the native
        ``batch_wait`` primitive: retire the ready ones (optionally with a shared
        ``recv_ns``) and keep the rest in flight. ``num_returns=1`` gives
        wait_any semantics. The given futures must not have been retired yet.
        """
        if not futures:
            raise ValueError("wait() requires a non-empty list of futures")
        ready_idx = set(self._engine.wait([f._cf for f in futures],
                                          int(num_returns)))
        ready, not_ready = [], []
        for i, fut in enumerate(futures):
            (ready if i in ready_idx else not_ready).append(fut)
        return ready, not_ready

    def num_lanes(self) -> int:
        return self._engine.num_lanes()

    def shutdown(self) -> None:
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

    def __init__(self, num_lanes: int = 1, hpx_threads: int = 1):
        self._engine = Engine(num_lanes=num_lanes, hpx_threads=hpx_threads)

    def remote(self, service_ms: float = 0.0, work_mode: str = "sleep") -> Future:
        """Submit one synthetic request; returns a :class:`Future`.

        Forwards directly to :meth:`Engine.submit`.
        """
        return self._engine.submit(service_ms=service_ms, work_mode=work_mode)

    def remote_batch(
        self, service_ms: float = 0.0, count: int = 1, work_mode: str = "sleep"
    ) -> list:
        """Submit ``count`` synthetic requests in one batch; returns a list of
        :class:`Future`.

        Ergonomic façade that forwards directly to :meth:`Engine.submit_batch`,
        with the same bulk semantics: one Python->C++ crossing enqueues all
        ``count`` requests, and every returned Future shares one Python-side
        ``submit_ns``, so per-request ``total_ms`` is queue-shaped (requests
        deeper in the batch include their queue position). The meaningful batch
        signal is submission overhead / throughput, not steady-state per-request
        latency.

        Like :meth:`remote`, this runs **native synthetic C++ work only** (the
        blocking-sleep service lane). It is **not** a general Ray actor batch
        API: it dispatches the single fixed synthetic operation, not arbitrary
        Python functions or named actor methods.
        """
        return self._engine.submit_batch(
            service_ms=service_ms, count=count, work_mode=work_mode)

    def wait(self, futures, num_returns: int = 1):
        """Forward to :meth:`Engine.wait` (as-completed wait over Futures)."""
        return self._engine.wait(futures, num_returns=num_returns)

    def num_lanes(self) -> int:
        return self._engine.num_lanes()

    def shutdown(self) -> None:
        self._engine.shutdown()

    def __enter__(self) -> "SyntheticActor":
        return self

    def __exit__(self, *exc) -> bool:
        self.shutdown()
        return False
