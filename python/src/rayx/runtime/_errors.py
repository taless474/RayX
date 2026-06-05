"""rayx.runtime error classes (import-light; stdlib-only).

Extracted from ``rayx/runtime/__init__.py`` so the exception hierarchy can be
imported WITHOUT the ``_rayx`` extension or HPX (the package ``__init__`` imports
``_rayx`` at module load). This module deliberately has **no** ``from .._rayx``,
**no** ``import rayx``, and **no** relative imports, so it is safe to load by file
path in lightweight (repo-sanity) unit tests.

The public home of these classes is ``rayx.runtime``: ``__init__`` re-exports them,
and each class pins ``__module__ = "rayx.runtime"`` so reprs / tracebacks / pickle
all reference the public location, not this private submodule.
"""

__all__ = [
    "QueueFullError",
    "RuntimeOperationError",
    "OperationFailedError",
    "OperationCancelledError",
]


class QueueFullError(RuntimeError):
    """Raised by :meth:`rayx.runtime.Runtime.submit_operation` when the lane is full.

    Bounded admission (Slice 2b): when a ``Runtime`` is constructed with
    ``max_queue_depth_per_lane`` and the round-robin-selected lane already holds that
    many queued-but-not-started operations, the submit is **rejected** -- it creates
    no ``RuntimeFuture``, no promise, and no cancel token, and does not change the
    lane's queue depth. Subclasses :class:`RuntimeError`, mirroring the harness
    ``rayx.QueueFullError`` precedent (a **distinct** class -- this one is
    ``rayx.runtime.QueueFullError``, not the harness error; they are not aliased).
    """

    __module__ = "rayx.runtime"


class RuntimeOperationError(RuntimeError):
    """Base for runtime operation value-read failures.

    Subclasses :class:`RuntimeError` (mirroring the harness
    ``QueueFullError(RuntimeError)`` precedent) so existing ``except RuntimeError``
    still catches the whole family. Raised only on a ``.value`` read of a
    non-completed ``OperationResult`` -- boundary validation (unknown op id, wrong
    arity, non-int args) keeps the builtin ``ValueError`` / ``TypeError``.
    """

    __module__ = "rayx.runtime"


class OperationFailedError(RuntimeOperationError):
    """Raised by ``OperationResult.value`` when ``status == "failed"``."""

    __module__ = "rayx.runtime"


class OperationCancelledError(RuntimeOperationError):
    """Raised by ``OperationResult.value`` when ``status == "cancelled"``.

    A cancelled operation (queued-cancel skip, or a cooperative running cancel at a
    checkpoint boundary) retires with ``row["status"] == "cancelled"`` and no value,
    so reading ``.value`` raises this (idempotently, every read).
    """

    __module__ = "rayx.runtime"
