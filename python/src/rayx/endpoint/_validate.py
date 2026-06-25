"""rayx.endpoint pure validators (experimental endpoint seam; import-light, stdlib-only).

Separated from ``rayx/endpoint/__init__.py`` so the Python-boundary validation can be
imported and unit-tested WITHOUT the ``_rayx`` extension or HPX. This module has **no**
``from .._rayx``, **no** ``import rayx``, and **no** relative imports (only ``re``), so
it is safe to load by file path in lightweight (repo-sanity) unit tests.

Following the ``rayx.runtime._validate`` precedent, the validators here raise the
**builtin** :class:`ValueError` / :class:`TypeError`; the public ``rayx.endpoint`` layer
re-raises those as :class:`EndpointValidationError` (which subclasses ``ValueError``).
This keeps the pure module dependency-free while the public surface stays typed.

Closed model: an endpoint id is ``rtb-ep-<16 lowercase hex>``; peer metadata is exactly
``{endpoint_id, pid, host, proto_version, transport_kind, transport_addr}``; a ping nonce
is an ``int64`` (``bool`` rejected as an ``int`` subclass). ``transport_kind`` is one of
``"none"`` / ``"unix"`` / ``"tcp"``; ``transport_addr`` is a string (empty iff kind is
``"none"``). A1 implements ``"none"`` and ``"unix"``; ``"tcp"`` is accepted as a valid
shape but is a documented fallback only (not produced by this build).
"""

import re

__all__ = [
    "ENDPOINT_PROTO_VERSION",
    "ENDPOINT_ID_RE",
    "METADATA_KEYS",
    "TRANSPORT_KINDS",
    "INT64_MIN",
    "INT64_MAX",
    "validate_endpoint_id",
    "validate_metadata",
    "validate_nonce",
    "endpoint_id_hash",
]

# Mirror of ENDPOINT_PROTO_VERSION in python/src/rayx/endpoint_seam.hpp -- keep in sync.
# Bumped to 2 for the A1 six-key metadata shape (added transport_kind/transport_addr).
ENDPOINT_PROTO_VERSION = 2

# rtb-ep- + exactly 16 lowercase hex (mirror ENDPOINT_ID_PREFIX + make_endpoint_id()).
ENDPOINT_ID_RE = re.compile(r"^rtb-ep-[0-9a-f]{16}$")

# Exact metadata shape exchanged through Ray/Python for bootstrap (plain/serializable).
METADATA_KEYS = ("endpoint_id", "pid", "host", "proto_version",
                 "transport_kind", "transport_addr")

# Valid transport kinds. "none" = no listener (cross-process -> Unreachable, the
# transport-disabled default); "unix" = AF_UNIX local IPC (A1); "tcp" = documented
# fallback shape only (not implemented in A1).
TRANSPORT_KINDS = ("none", "unix", "tcp")

# Inclusive int64 range for the ping nonce (the closed value model's int64 leg).
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


def validate_endpoint_id(token):
    """Validate an endpoint id token. Returns it unchanged. ``TypeError`` if not a str;
    ``ValueError`` if it is not ``rtb-ep-<16 lowercase hex>``."""
    if not isinstance(token, str):
        raise TypeError(f"endpoint id must be str, got {type(token).__name__}")
    if not ENDPOINT_ID_RE.match(token):
        raise ValueError(
            f"endpoint id must match 'rtb-ep-<16 lowercase hex>', got {token!r}")
    return token


def validate_metadata(meta):
    """Validate peer endpoint metadata. Returns a normalized dict with exactly
    ``METADATA_KEYS``. ``TypeError`` for a non-dict / wrong field types (``bool``
    rejected as ``int``); ``ValueError`` for missing/extra keys, a bad id, an
    out-of-range ``pid``/``proto_version``, or an empty ``host``.

    Validation is structural only -- a well-formed ``proto_version`` that differs from
    this build's is NOT rejected here (the public layer raises EndpointProtocolError so
    a mismatch is distinguishable from malformed input)."""
    if not isinstance(meta, dict):
        raise TypeError(f"endpoint metadata must be dict, got {type(meta).__name__}")
    keys = set(meta)
    if keys != set(METADATA_KEYS):
        raise ValueError(
            f"endpoint metadata keys must be exactly {sorted(METADATA_KEYS)}, "
            f"got {sorted(keys)}")
    validate_endpoint_id(meta["endpoint_id"])
    pid = meta["pid"]
    if isinstance(pid, bool) or not isinstance(pid, int):
        raise TypeError(f"endpoint metadata 'pid' must be int, "
                        f"got {type(pid).__name__}")
    if pid < 0:
        raise ValueError(f"endpoint metadata 'pid' must be >= 0, got {pid}")
    host = meta["host"]
    if not isinstance(host, str):
        raise TypeError(f"endpoint metadata 'host' must be str, "
                        f"got {type(host).__name__}")
    if not host:
        raise ValueError("endpoint metadata 'host' must be non-empty")
    proto = meta["proto_version"]
    if isinstance(proto, bool) or not isinstance(proto, int):
        raise TypeError(f"endpoint metadata 'proto_version' must be int, "
                        f"got {type(proto).__name__}")
    if proto < 0:
        raise ValueError(
            f"endpoint metadata 'proto_version' must be >= 0, got {proto}")
    kind = meta["transport_kind"]
    if not isinstance(kind, str):
        raise TypeError(f"endpoint metadata 'transport_kind' must be str, "
                        f"got {type(kind).__name__}")
    if kind not in TRANSPORT_KINDS:
        raise ValueError(
            f"endpoint metadata 'transport_kind' must be one of "
            f"{list(TRANSPORT_KINDS)}, got {kind!r}")
    addr = meta["transport_addr"]
    if not isinstance(addr, str):
        raise TypeError(f"endpoint metadata 'transport_addr' must be str, "
                        f"got {type(addr).__name__}")
    if kind == "none" and addr != "":
        raise ValueError("endpoint metadata 'transport_addr' must be empty when "
                         "transport_kind is 'none'")
    if kind != "none" and not addr:
        raise ValueError("endpoint metadata 'transport_addr' must be non-empty when "
                         f"transport_kind is {kind!r}")
    return {"endpoint_id": meta["endpoint_id"], "pid": pid,
            "host": host, "proto_version": proto,
            "transport_kind": kind, "transport_addr": addr}


def validate_nonce(nonce):
    """Validate a ping nonce. Returns it unchanged. ``TypeError`` if not an ``int``
    (``bool`` rejected); ``ValueError`` if outside the inclusive ``int64`` range."""
    if isinstance(nonce, bool) or not isinstance(nonce, int):
        raise TypeError(f"ping nonce must be int, got {type(nonce).__name__}")
    if nonce < INT64_MIN or nonce > INT64_MAX:
        raise ValueError(
            f"ping nonce must be in [{INT64_MIN}, {INT64_MAX}], got {nonce}")
    return nonce


# FNV-1a 64-bit constants (the standard parameters).
_FNV_OFFSET_BASIS = 0xcbf29ce484222325
_FNV_PRIME = 0x100000001b3
_U64_MASK = (1 << 64) - 1


def endpoint_id_hash(endpoint_id):
    """FNV-1a 64-bit over the endpoint-id bytes, returned as a SIGNED ``int64``.

    Mirror of ``endpoint_id_hash`` in ``python/src/rayx/endpoint_seam.hpp`` -- the
    constants and algorithm MUST stay in sync, and the signed reinterpretation here
    matches the C++ ``static_cast<std::int64_t>`` so ``nonce ^ ENDPOINT_PING_XOR ^
    endpoint_id_hash(peer_id)`` is identical on both sides for any ``int64`` nonce.

    This is a structural per-endpoint mixin that makes the ping response depend on the
    resolved peer's identity -- it is NOT a cryptographic hash and NOT security.
    """
    h = _FNV_OFFSET_BASIS
    for b in endpoint_id.encode("ascii"):
        h ^= b
        h = (h * _FNV_PRIME) & _U64_MASK
    # Reinterpret the 64-bit pattern as signed int64 (match C++ static_cast).
    return h - (1 << 64) if h >= (1 << 63) else h
