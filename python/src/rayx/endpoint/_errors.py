"""rayx.endpoint error classes (experimental endpoint seam; import-light, stdlib-only).

Separated from ``rayx/endpoint/__init__.py`` so the exception hierarchy can be imported
WITHOUT the ``_rayx`` extension or HPX (the package ``__init__`` imports ``_rayx`` at
module load). This module deliberately has **no** ``from .._rayx``, **no** ``import
rayx``, and **no** relative imports, so it is safe to load by file path in lightweight
(repo-sanity) unit tests.

The public home of these classes is ``rayx.endpoint``: ``__init__`` re-exports them, and
each pins ``__module__ = "rayx.endpoint"`` so reprs / tracebacks / pickle reference the
public location, not this private submodule.

Slice 0 is the **Ray bootstrap + HPX endpoint identity seam** only: NO transport, NO
fabric, NO cross-process native delivery, NO performance claim.
"""

__all__ = [
    "EndpointError",
    "EndpointValidationError",
    "EndpointNotFoundError",
    "EndpointClosedError",
    "EndpointProtocolError",
    "EndpointUnreachableError",
    "EndpointTimeoutError",
    "EndpointRuntimeUnavailableError",
    "EndpointOperationError",
]


class EndpointError(RuntimeError):
    """Base for all ``rayx.endpoint`` errors.

    Subclasses :class:`RuntimeError` (mirroring the ``rayx.runtime`` error precedent) so
    existing ``except RuntimeError`` still catches the whole family. Also raised directly
    when ``Endpoint()`` cannot start because an ``Engine``/``Runtime`` already owns the
    one process HPX runtime (they are mutually exclusive).
    """

    __module__ = "rayx.endpoint"


class EndpointValidationError(EndpointError, ValueError):
    """Malformed endpoint token or peer metadata at the Python boundary.

    Also subclasses :class:`ValueError` so callers can catch either family. Raised before
    any native crossing (bad id format, wrong metadata shape/types, bad nonce).
    """

    __module__ = "rayx.endpoint"


class EndpointNotFoundError(EndpointError):
    """No live endpoint with the given id is registered **in this process**.

    This slice has a process-local registry only: the (same-process) peer id was never
    registered or has already been closed/torn down.
    """

    __module__ = "rayx.endpoint"


class EndpointClosedError(EndpointError):
    """Operation on a closed endpoint/connection, or against a peer that has closed."""

    __module__ = "rayx.endpoint"


class EndpointProtocolError(EndpointError):
    """Peer metadata advertises an incompatible ``proto_version``."""

    __module__ = "rayx.endpoint"


class EndpointUnreachableError(EndpointError):
    """The peer cannot be reached.

    Raised for a peer on another node (``host`` mismatch -- no multi-node), a
    transport-disabled cross-process peer (``transport_kind == "none"``), or a
    transport-enabled peer whose listener is not accepting (nothing listening at its
    ``transport_addr``). No delivery is attempted / completed.
    """

    __module__ = "rayx.endpoint"


class EndpointTimeoutError(EndpointError):
    """A cross-process connect or ping exceeded its bounded deadline.

    The A1 transport uses a single deadline covering dial + send + recv; if it is
    exceeded the socket is closed and this is raised. Distinct from
    :class:`EndpointUnreachableError` (nothing listening) so callers can tell a slow/stuck
    peer from an absent one.
    """

    __module__ = "rayx.endpoint"


class EndpointRuntimeUnavailableError(EndpointError):
    """The endpoint->Runtime bridge could not reach a usable ``rayx.runtime.Runtime``.

    Raised by the experimental bridge (``Connection._call_op``) when the peer process has
    no ``Runtime`` attached, or its ``Runtime`` is shutting down (draining). The endpoint
    itself was resolved fine; there is simply no live Runtime to dispatch the op.
    """

    __module__ = "rayx.endpoint"


class EndpointOperationError(EndpointError):
    """The bridged Runtime operation could not be completed.

    Raised by the experimental bridge for an unknown op-code, an operation whose body
    failed/was cancelled, or an unexpected internal error during dispatch. (A wrong
    arity/type is reported as :class:`EndpointValidationError` instead.)
    """

    __module__ = "rayx.endpoint"
