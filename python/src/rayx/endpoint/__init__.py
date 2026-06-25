"""rayx.endpoint -- experimental Ray bootstrap + endpoint identity seam + A1 transport.

EXPERIMENTAL and deliberately NARROW:

  * In ONE process, ``Endpoint`` A can ``connect`` to ``Endpoint`` B through the
    process-local native registry and run one fixed typed int64 ``ping`` handshake
    against B's seam, computed INLINE (no HPX call in the ping path).
  * With ``Endpoint(transport=True)``, a peer in **another process on the same node** is
    reached over the **A1 transport**: plain native local IPC (AF_UNIX). The
    cross-process ``ping`` returns the **same** peer-specific transform as the local
    path. This is NOT HPX transport, NOT HPX serving, NOT HPX async I/O, NOT a fabric,
    NOT parcelport / AGAS, NOT multi-node, and carries NO performance claim. HPX is not
    the delivery mechanism. See ``docs/design/endpoint_transport_slice.md``.
  * Without transport (the default), a cross-process peer is reported cleanly as
    :class:`EndpointUnreachableError`, preserving the prior identity-seam behavior. A peer
    on another node is always :class:`EndpointUnreachableError` (single-node only).
  * Endpoint identities are minted natively (``rtb-ep-<16 hex>``) and exchanged as plain
    serializable metadata -- intended to travel through Ray/Python for **bootstrap
    only**.

It is NOT an object store, NOT an ``ObjectRef``. The ping payload is a fixed typed
``int64`` only -- no arbitrary payloads, no Python callbacks, no serialization blobs.

HPX lifecycle (**Variant 2: an Endpoint is HPX-free**): constructing an ``Endpoint`` does
**not** start HPX -- the A1 ping is native IPC computed inline. HPX is owned solely by a
``rayx.runtime.Runtime`` / ``rayx.Engine`` through the shared process-level owner.
``Endpoint.close()`` unregisters the seam and releases any transport share; it never touches
HPX. :func:`shutdown` is retained for compatibility as an idempotent no-op cleanup helper
(it does **not** stop HPX and does **not** raise).

Coexistence with ``rayx.runtime.Runtime``: an ``Endpoint`` and a ``Runtime`` **can coexist**
in one process (the Runtime owns HPX; the Endpoint runs native IPC alongside it), in any
construction order. ``rayx.Engine`` stays **exclusive**: an ``Endpoint`` cannot be
constructed while an ``Engine`` is active (raises :class:`EndpointError`), and an ``Engine``
cannot start while an ``Endpoint``/``Runtime`` is active. (Endpoint -> Runtime request
dispatch is future work; this layer only establishes coexistence.)

Synchronization (precise): the shared process HPX owner serializes attach/detach and the
HPX lifecycle; the CPython GIL serializes Python construction calls. No free-threaded /
no-GIL behavior is assumed or claimed.
"""

import os
import socket

from .._rayx import (  # noqa: E402  native seam (built extension required)
    _Endpoint,
    _endpoint_connect_local,
    _endpoint_connect_unix,
    _endpoint_shutdown,
    _process_hpx_active as _process_hpx_active_native,
    _ENDPOINT_PROTO_VERSION,
    _BRIDGE_OP_SQUARE,
    _BRIDGE_OP_ADD,
    _BRIDGE_OP_FANOUT_SUM,
)
from . import _validate
from ._errors import (
    EndpointError,
    EndpointValidationError,
    EndpointNotFoundError,
    EndpointClosedError,
    EndpointProtocolError,
    EndpointUnreachableError,
    EndpointTimeoutError,
    EndpointRuntimeUnavailableError,
    EndpointOperationError,
)

__all__ = [
    "Endpoint",
    "Connection",
    "connect",
    "shutdown",
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

# Default bounded deadline for a cross-process connect+ping (seconds). The A1 transport
# uses ONE deadline covering dial+send+recv; override per call via connect(..., timeout=).
ENDPOINT_DEFAULT_TIMEOUT_S = 5.0

# Native ping status codes (mirror endpoint_seam.hpp PingStatus). 0/1/2 are used by the
# same-process path; 3/4/5 are added by the cross-process transport.
_PING_OK = 0
_PING_PEER_CLOSED = 1
_PING_CONN_CLOSED = 2
_PING_NOT_FOUND = 3
_PING_PROTO_ERR = 4
_PING_TIMEOUT = 5

# Native cross-process connect status (mirror _endpoint_connect_unix).
_CONNECT_OK = 0
_CONNECT_TIMEOUT = 5
_CONNECT_UNREACHABLE = 6

# Bridge v1 CALL_OP status codes (mirror rayx_endpoint::CallStatus in endpoint_seam.hpp).
_CALL_OK = 0
_CALL_PEER_CLOSED = 1
_CALL_CONN_CLOSED = 2
_CALL_NOT_FOUND = 3
_CALL_PROTO_ERR = 4
_CALL_TIMEOUT = 5
_CALL_RUNTIME_ABSENT = 6
_CALL_RUNTIME_DRAINING = 7
_CALL_OP_UNKNOWN = 8
_CALL_OP_BADARGS = 9
_CALL_OP_FAILED = 10
_CALL_INTERNAL = 11

# Bridge closed op table: op_id -> (native op_code, arity). EXPERIMENTAL, private/test-only.
# v1: the instantaneous int64 ops square/add. v2 adds fanout_sum (a "composed-op structural
# bridge": its body uses nested HPX composition hpx::async + hpx::when_all and returns the
# correct typed result -- this proves faithful delivery into HPX-thread execution context,
# NOT scheduling/overlap/parallelism value). See docs/design/endpoint_runtime_bridge.md.
_BRIDGE_OPS = {
    "square": (_BRIDGE_OP_SQUARE, 1),
    "add": (_BRIDGE_OP_ADD, 2),
    "fanout_sum": (_BRIDGE_OP_FANOUT_SUM, 2),
}

# int64 range for bridge arguments (the closed value model; v1 op codes are int64-only).
_INT64_MIN = -(2 ** 63)
_INT64_MAX = 2 ** 63 - 1


class Connection:
    """A live, process-local connection to a peer endpoint's seam.

    Obtained from :func:`connect`. The only operation is the fixed typed :meth:`ping`.
    A same-process peer is resolved through the registry and computed inline; a
    cross-process (same-node) peer is reached over the A1 AF_UNIX transport. Either way
    the response is the same peer-specific transform (no HPX in the path).
    """

    def __init__(self, native):
        self._native = native
        self._closed = False

    def ping(self, nonce):
        """Run the peer-specific typed int64 handshake against the peer seam; return the
        deterministic transform
        ``nonce ^ ENDPOINT_PING_XOR ^ endpoint_id_hash(peer_id)``.

        The response depends on the resolved peer's identity (structural evidence the
        call is parameterized by the peer -- NOT cryptographic proof and NOT security).
        Raises :class:`EndpointValidationError` for a bad nonce; :class:`EndpointClosedError`
        if this connection or the peer endpoint is closed (or the peer's listener went
        away); :class:`EndpointNotFoundError` if the peer process is listening but the id
        is not registered there; :class:`EndpointProtocolError` on a wire proto mismatch;
        :class:`EndpointTimeoutError` if the bounded deadline is exceeded.
        """
        try:
            _validate.validate_nonce(nonce)
        except (ValueError, TypeError) as e:
            raise EndpointValidationError(str(e)) from e
        if self._closed:
            raise EndpointClosedError("connection is closed")
        status, value = self._native.ping(nonce)
        if status == _PING_OK:
            return value
        if status == _PING_PEER_CLOSED:
            raise EndpointClosedError("peer endpoint is closed")
        if status == _PING_CONN_CLOSED:
            raise EndpointClosedError("connection is closed (peer unreachable)")
        if status == _PING_NOT_FOUND:
            raise EndpointNotFoundError(
                "peer process is listening but the endpoint id is not registered there")
        if status == _PING_PROTO_ERR:
            raise EndpointProtocolError("peer rejected the request proto_version")
        if status == _PING_TIMEOUT:
            raise EndpointTimeoutError("ping timed out")
        raise EndpointError(f"unexpected ping status {status}")  # defensive

    def _call_op(self, op_id, *args):
        """EXPERIMENTAL, PRIVATE endpoint->Runtime bridge call (v1). NOT public API.

        Invoke one fixed registered Runtime operation in the peer's process and return its
        typed result. v1 supports only the instantaneous int64 ops ``square(int64)`` and
        ``add(int64, int64)``; the peer process must have a live ``rayx.runtime.Runtime``.
        This is **plumbing** -- it proves the cross-process endpoint->Runtime seam, not HPX
        value, and carries no performance claim.

        Raises :class:`EndpointValidationError` (unknown op / wrong arity / non-int64 arg),
        :class:`EndpointRuntimeUnavailableError` (no Runtime attached, or it is shutting
        down), :class:`EndpointOperationError` (op failed / unknown op-code / internal),
        :class:`EndpointClosedError`, :class:`EndpointNotFoundError`,
        :class:`EndpointProtocolError`, or :class:`EndpointTimeoutError`.
        """
        if op_id not in _BRIDGE_OPS:
            raise EndpointValidationError(
                f"unknown bridge op {op_id!r}; v1 supports {sorted(_BRIDGE_OPS)}")
        op_code, arity = _BRIDGE_OPS[op_id]
        if len(args) != arity:
            raise EndpointValidationError(
                f"bridge op {op_id!r} expects {arity} argument(s), got {len(args)}")
        iargs = []
        for i, a in enumerate(args):
            if isinstance(a, bool) or not isinstance(a, int):
                raise EndpointValidationError(
                    f"bridge op {op_id!r} argument {i} must be a non-bool int (int64)")
            if not (_INT64_MIN <= a <= _INT64_MAX):
                raise EndpointValidationError(
                    f"bridge op {op_id!r} argument {i} is out of int64 range")
            iargs.append(a)
        if self._closed:
            raise EndpointClosedError("connection is closed")
        status, value = self._native.call_op(op_code, iargs)
        if status == _CALL_OK:
            return value
        if status in (_CALL_PEER_CLOSED, _CALL_CONN_CLOSED):
            raise EndpointClosedError("connection/peer is closed (peer unreachable)")
        if status == _CALL_NOT_FOUND:
            raise EndpointNotFoundError(
                "peer process is listening but the endpoint id is not registered there")
        if status == _CALL_PROTO_ERR:
            raise EndpointProtocolError("peer rejected the request proto_version")
        if status == _CALL_TIMEOUT:
            raise EndpointTimeoutError("bridge call timed out")
        if status in (_CALL_RUNTIME_ABSENT, _CALL_RUNTIME_DRAINING):
            raise EndpointRuntimeUnavailableError(
                "no live rayx.runtime.Runtime is attached in the peer process "
                "(absent or shutting down)")
        if status == _CALL_OP_UNKNOWN:
            raise EndpointOperationError(f"peer does not support bridge op {op_id!r}")
        if status == _CALL_OP_BADARGS:
            raise EndpointValidationError(f"peer rejected arguments for op {op_id!r}")
        if status == _CALL_OP_FAILED:
            raise EndpointOperationError(f"bridged operation {op_id!r} failed")
        if status == _CALL_INTERNAL:
            raise EndpointOperationError(
                f"internal error dispatching bridged operation {op_id!r}")
        raise EndpointError(f"unexpected bridge call status {status}")  # defensive

    def close(self):
        """Close the connection (idempotent)."""
        if not self._closed:
            self._closed = True
            self._native.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class Endpoint:
    """A process-local endpoint with a stable opaque identity (Variant 2: HPX-free).

    Construction mints ``rtb-ep-<16 hex>``, registers the seam, and (with
    ``transport=True``) takes a refcounted share of the process-local AF_UNIX transport
    listener. It does **not** start HPX -- an Endpoint never owns HPX (see the module
    coexistence rule; HPX is owned by a ``rayx.runtime.Runtime`` / ``rayx.Engine``). Use
    :meth:`metadata` to obtain the plain dict to hand to a peer through Ray/Python
    bootstrap, and :func:`connect` to reach a (same- or cross-process) peer.
    """

    def __init__(self, transport=False):
        try:
            self._native = _Endpoint(bool(transport))
        except RuntimeError as e:
            # Expected native failures: the mutual-exclusion guard (an Engine/Runtime
            # already owns HPX), an HPX start failure, or (transport=True) a listener
            # bring-up failure (e.g. socket path too long). Surface as a typed error.
            raise EndpointError(str(e)) from e
        self._id = self._native.id()
        self._pid = self._native.pid()
        self._proto = self._native.proto_version()
        self._host = socket.gethostname()
        self._transport_kind = self._native.transport_kind()
        self._transport_addr = self._native.transport_addr()
        self._closed = False

    @property
    def id(self):
        """The stable opaque endpoint identity (``rtb-ep-<16 hex>``)."""
        return self._id

    def metadata(self):
        """Return the plain, serializable bootstrap metadata dict.

        Exactly ``{endpoint_id, pid, host, proto_version, transport_kind,
        transport_addr}`` -- no HPX type, no ``ObjectRef``. ``host``/``pid`` are the
        node/process discriminators; ``transport_kind``/``transport_addr`` describe the
        A1 connect path (``"none"``/``""`` when transport is off, ``"unix"``/socket-path
        when on). Raises :class:`EndpointClosedError` after :meth:`close`.
        """
        if self._closed:
            raise EndpointClosedError("endpoint is closed")
        return {"endpoint_id": self._id, "pid": self._pid,
                "host": self._host, "proto_version": self._proto,
                "transport_kind": self._transport_kind,
                "transport_addr": self._transport_addr}

    def close(self):
        """Unregister the seam and release this endpoint's transport share, if any
        (idempotent). Variant 2: an Endpoint is HPX-free, so this never touches HPX --
        the last *transport* endpoint to close stops the process-local AF_UNIX listener,
        but HPX (if up) is owned by a Runtime/Engine and is unaffected."""
        if not self._closed:
            self._closed = True
            self._native.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def connect(peer_metadata, timeout=ENDPOINT_DEFAULT_TIMEOUT_S):
    """Open a :class:`Connection` to the peer described by ``peer_metadata``.

    ``timeout`` (seconds) bounds a cross-process connect and its pings with a single
    deadline. Validation and resolution order:
      1. :class:`EndpointValidationError` -- malformed metadata/token.
      2. :class:`EndpointProtocolError` -- incompatible ``proto_version``.
      3. :class:`EndpointUnreachableError` -- peer on another node (``host`` mismatch),
         or a transport-disabled cross-process peer (``transport_kind == "none"``), or a
         transport peer whose listener is not accepting.
      4. :class:`EndpointTimeoutError` -- cross-process connect exceeded ``timeout``.
      5. :class:`EndpointNotFoundError` -- same-process token that is not a live local
         endpoint (never registered or already closed).

    Same-process peers resolve through the process-local registry (no socket); a
    cross-process peer on the same node with ``transport_kind == "unix"`` is reached over
    the A1 AF_UNIX transport (plain local IPC, not HPX).
    """
    try:
        meta = _validate.validate_metadata(peer_metadata)
    except (ValueError, TypeError) as e:
        raise EndpointValidationError(str(e)) from e

    if meta["proto_version"] != _ENDPOINT_PROTO_VERSION:
        raise EndpointProtocolError(
            f"peer proto_version {meta['proto_version']} != this build's "
            f"{_ENDPOINT_PROTO_VERSION}")

    if meta["host"] != socket.gethostname():
        raise EndpointUnreachableError(
            f"peer endpoint {meta['endpoint_id']} is on another node "
            f"(host={meta['host']!r}); A1 is single-node only (no multi-node transport)")

    if meta["pid"] == os.getpid():
        # Same process: resolve through the registry and compute inline (no socket).
        native = _endpoint_connect_local(meta["endpoint_id"])
        if native is None:
            raise EndpointNotFoundError(
                f"no live local endpoint {meta['endpoint_id']} registered in this "
                f"process")
        return Connection(native)

    # Cross-process, same node: dispatch on the advertised transport.
    kind = meta["transport_kind"]
    if kind == "none":
        raise EndpointUnreachableError(
            f"peer endpoint {meta['endpoint_id']} is in another process with transport "
            f"disabled (transport_kind='none'); construct it with Endpoint(transport=True)"
            f" to enable cross-process delivery")
    if kind == "unix":
        deadline_ms = max(1, int(timeout * 1000))
        status, native = _endpoint_connect_unix(
            meta["endpoint_id"], meta["transport_addr"], deadline_ms)
        if status == _CONNECT_OK:
            return Connection(native)
        if status == _CONNECT_TIMEOUT:
            raise EndpointTimeoutError(
                f"timed out connecting to peer endpoint {meta['endpoint_id']} at "
                f"{meta['transport_addr']!r} after {timeout}s")
        if status == _CONNECT_UNREACHABLE:
            raise EndpointUnreachableError(
                f"peer endpoint {meta['endpoint_id']} is not listening at "
                f"{meta['transport_addr']!r}")
        raise EndpointError(f"unexpected connect status {status}")  # defensive
    # "tcp" is a documented fallback shape only; A1 does not implement it.
    raise EndpointUnreachableError(
        f"transport_kind {kind!r} is not supported in this build (A1 implements 'unix')")


def shutdown():
    """Idempotent endpoint cleanup helper (Variant 2 -- retained for compatibility).

    Endpoints are HPX-free, so this **never** stops HPX and **never** raises. Normal
    cleanup is per :meth:`Endpoint.close`; this only clears closed-id tombstones (and a
    stray transport listener) when no endpoints remain. Safe to call at any time.
    """
    _endpoint_shutdown()


def _process_hpx_active():
    """Debug/test gate: True iff the process HPX runtime is up (owned by a
    ``Runtime``/``Engine``). Endpoints are HPX-free and never make this True."""
    return _process_hpx_active_native()
