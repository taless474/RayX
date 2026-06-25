"""Integration contract tests for the experimental endpoint identity seam.

Require the built ``_rayx`` extension + HPX; the whole module skips cleanly via
``importorskip`` if the extension is not built. These gate the **structural** seam, NOT
performance: identity minting, the in-process peer-specific handshake, the typed error
taxonomy (including the clean cross-process ``EndpointUnreachableError`` -- there is no
transport in this slice), the HPX-free endpoint lifecycle (Variant 2: an Endpoint never
owns HPX), and the coexistence rule with ``rayx.runtime.Runtime`` (they may share a
process; HPX is owned by the Runtime) plus ``rayx.Engine`` exclusivity. No timing values
are asserted.

A function-scoped autouse fixture calls ``rayx.endpoint.shutdown()`` after each test --
now an idempotent no-op cleanup helper (Variant 2 endpoints are HPX-free, so there is no
HPX to hand back) -- which keeps the test order independent.
"""
import os
import socket

import pytest

rayx_endpoint = pytest.importorskip("rayx.endpoint")

import rayx.endpoint as ep_mod  # noqa: E402  (module-level shutdown / _process_hpx_active)
from rayx.endpoint import (  # noqa: E402
    Endpoint,
    Connection,
    connect,
    shutdown,
    EndpointError,
    EndpointValidationError,
    EndpointNotFoundError,
    EndpointClosedError,
    EndpointProtocolError,
    EndpointUnreachableError,
)
from rayx.endpoint._validate import (  # noqa: E402
    ENDPOINT_ID_RE,
    ENDPOINT_PROTO_VERSION,
    endpoint_id_hash,
)
from rayx._rayx import _ENDPOINT_PING_XOR as PING_XOR  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_endpoint_mode():
    """Run the idempotent endpoint cleanup helper after each test. Variant 2 endpoints are
    HPX-free, so this never stops HPX and never raises; it just clears endpoint tombstones
    (and any stray transport listener) so test order stays independent."""
    yield
    shutdown()


def _expected_ping(peer_id, nonce):
    return nonce ^ PING_XOR ^ endpoint_id_hash(peer_id)


def _local_unknown_meta():
    """Well-formed metadata for a SAME-process id that is not registered -> NotFound."""
    return {"endpoint_id": "rtb-ep-0000000000000000", "pid": os.getpid(),
            "host": socket.gethostname(), "proto_version": ENDPOINT_PROTO_VERSION,
            "transport_kind": "none", "transport_addr": ""}


# --- 1. identity -------------------------------------------------------------


def test_endpoint_mints_stable_unique_id():
    with Endpoint() as a, Endpoint() as b:
        assert ENDPOINT_ID_RE.match(a.id)
        assert ENDPOINT_ID_RE.match(b.id)
        assert a.id != b.id          # distinct
        assert a.id == a.id          # stable across reads
        assert a.metadata()["endpoint_id"] == a.id


def test_metadata_shape_and_serializable():
    import json
    with Endpoint() as a:
        meta = a.metadata()
        assert set(meta) == {"endpoint_id", "pid", "host", "proto_version",
                             "transport_kind", "transport_addr"}
        assert meta["pid"] == os.getpid()
        assert meta["proto_version"] == ENDPOINT_PROTO_VERSION
        # Default Endpoint() has transport OFF.
        assert meta["transport_kind"] == "none"
        assert meta["transport_addr"] == ""
        assert json.loads(json.dumps(meta)) == meta   # plain/serializable


# --- 2. in-process peer-specific handshake (the seam) -----------------------


@pytest.mark.parametrize("nonce", [0, 1, 123456789, 2 ** 40, -7])
def test_in_process_ping_returns_peer_specific_transform(nonce):
    with Endpoint() as a, Endpoint() as b:
        conn = connect(b.metadata())
        assert isinstance(conn, Connection)
        # Response mixes B's id hash -> reached B's seam, peer-parameterized.
        assert conn.ping(nonce) == _expected_ping(b.id, nonce)
        conn.close()


def test_ping_is_peer_specific_b_vs_c():
    # Same nonce against two different peers must give DIFFERENT valid responses.
    with Endpoint() as a, Endpoint() as b, Endpoint() as c:
        nonce = 424242
        pb = connect(b.metadata()).ping(nonce)
        pc = connect(c.metadata()).ping(nonce)
        assert pb == _expected_ping(b.id, nonce)
        assert pc == _expected_ping(c.id, nonce)
        assert pb != pc                      # peer identity changes the response


def test_connect_to_self_endpoint_works_in_process():
    with Endpoint() as a:
        conn = connect(a.metadata())
        assert conn.ping(5) == _expected_ping(a.id, 5)


# --- 3. typed failure modes -------------------------------------------------


def test_bad_metadata_raises_validation():
    with Endpoint():
        with pytest.raises(EndpointValidationError):
            connect({"not": "metadata"})


def test_bad_token_raises_validation():
    with Endpoint():
        m = _local_unknown_meta()
        m["endpoint_id"] = "garbage"
        with pytest.raises(EndpointValidationError):
            connect(m)


def test_unknown_local_endpoint_raises_not_found():
    with Endpoint():
        with pytest.raises(EndpointNotFoundError):
            connect(_local_unknown_meta())


def test_cross_process_endpoint_raises_unreachable():
    # A well-formed peer in another process (different pid) with transport DISABLED
    # (the default): there is no listener, so this is reported cleanly WITHOUT any
    # delivery attempt. (The A1 transport path is covered in test_endpoint_transport.py.)
    with Endpoint() as a:
        meta = dict(a.metadata())
        assert meta["transport_kind"] == "none"
        meta["pid"] = meta["pid"] + 1   # pretend it lives in another process
        with pytest.raises(EndpointUnreachableError):
            connect(meta)


def test_proto_mismatch_raises_protocol_error():
    with Endpoint() as a:
        meta = dict(a.metadata())
        meta["proto_version"] = ENDPOINT_PROTO_VERSION + 1000
        with pytest.raises(EndpointProtocolError):
            connect(meta)


def test_ping_after_peer_closed_raises_closed():
    a = Endpoint()
    b = Endpoint()
    conn = connect(b.metadata())
    b.close()
    with pytest.raises(EndpointClosedError):
        conn.ping(1)
    a.close()


def test_ping_after_connection_closed_raises_closed():
    with Endpoint() as a, Endpoint() as b:
        conn = connect(b.metadata())
        conn.close()
        with pytest.raises(EndpointClosedError):
            conn.ping(1)


def test_metadata_after_endpoint_closed_raises_closed():
    a = Endpoint()
    a.close()
    with pytest.raises(EndpointClosedError):
        a.metadata()


def test_bad_nonce_raises_validation():
    with Endpoint() as a:
        conn = connect(a.metadata())
        with pytest.raises(EndpointValidationError):
            conn.ping(1.5)
        with pytest.raises(EndpointValidationError):
            conn.ping(True)


# --- 4. HPX-free endpoint lifecycle + idempotent shutdown -------------------


def test_endpoint_only_process_has_no_hpx():
    # Variant 2: an endpoint is HPX-free, so an endpoint-only process never brings HPX up.
    assert not ep_mod._process_hpx_active()
    a = Endpoint()
    b = Endpoint()
    assert not ep_mod._process_hpx_active()   # constructing endpoints does NOT start HPX
    a.close()
    assert not ep_mod._process_hpx_active()
    b.close()
    assert not ep_mod._process_hpx_active()   # still no HPX after the last close


def test_many_endpoints_coexist_without_hpx():
    eps = [Endpoint() for _ in range(4)]
    ids = {e.id for e in eps}
    assert len(ids) == 4
    conn = connect(eps[2].metadata())
    assert conn.ping(9) == _expected_ping(eps[2].id, 9)
    conn.close()
    assert not ep_mod._process_hpx_active()   # endpoints never own HPX
    for e in eps:
        e.close()


def test_shutdown_is_noop_with_live_endpoints():
    # Variant 2: shutdown() is an idempotent cleanup helper, NOT an HPX handback. It must
    # not raise just because endpoints are live; normal cleanup is per Endpoint.close().
    a = Endpoint()
    shutdown()                               # no raise, no effect on the live endpoint
    assert connect(a.metadata()).ping(2) == _expected_ping(a.id, 2)
    a.close()


def test_shutdown_idempotent_noop():
    a = Endpoint()
    a.close()
    assert not ep_mod._process_hpx_active()
    shutdown()
    shutdown()                               # idempotent no-op
    assert not ep_mod._process_hpx_active()


def test_close_is_idempotent():
    a = Endpoint()
    a.close()
    a.close()                            # no raise
    owner = Endpoint()
    conn = connect(owner.metadata())
    conn.close()
    conn.close()                         # no raise
    owner.close()


# --- 5. coexistence with rayx.runtime (Variant 2 shared HPX owner) ----------


def test_endpoint_then_runtime_coexist():
    from rayx.runtime import Runtime
    with Endpoint() as a:
        with Runtime() as rt:                # a Runtime can start alongside an Endpoint
            assert ep_mod._process_hpx_active()   # the Runtime owns HPX, not the Endpoint
            assert rt.submit_operation("square", 6).result().value == 36
            assert connect(a.metadata()).ping(4) == _expected_ping(a.id, 4)
    assert not ep_mod._process_hpx_active()  # Runtime shutdown stopped HPX


def test_runtime_then_endpoint_coexist():
    from rayx.runtime import Runtime
    with Runtime() as rt:
        with Endpoint() as a:                # an Endpoint can start alongside a Runtime
            assert ep_mod._process_hpx_active()
            assert connect(a.metadata()).ping(7) == _expected_ping(a.id, 7)
            assert rt.submit_operation("square", 5).result().value == 25


def test_close_endpoint_first_runtime_still_works():
    from rayx.runtime import Runtime
    with Runtime() as rt:
        a = Endpoint()
        a.close()                            # closing the endpoint must not disturb HPX
        assert ep_mod._process_hpx_active()
        assert rt.submit_operation("square", 8).result().value == 64


def test_shutdown_runtime_first_endpoint_still_works():
    from rayx.runtime import Runtime
    a = Endpoint()
    rt = Runtime()
    rt.shutdown()                            # the Runtime hands HPX back...
    assert not ep_mod._process_hpx_active()  # ...and the Endpoint never needed it
    assert connect(a.metadata()).ping(1) == _expected_ping(a.id, 1)  # endpoint still live
    a.close()


def test_runtime_restarts_after_full_detach():
    from rayx.runtime import Runtime
    with Endpoint() as a, Runtime() as rt:
        assert rt.submit_operation("square", 3).result().value == 9
    # Both detached; a fresh Runtime can start again (existing singleton lifecycle).
    with Runtime() as rt2:
        assert rt2.submit_operation("square", 4).result().value == 16


# --- 6. Engine exclusivity (unchanged by Variant 2) -------------------------


def test_engine_excludes_runtime():
    import rayx
    from rayx.runtime import Runtime
    with rayx.Engine(num_lanes=1, hpx_threads=1):
        with pytest.raises(RuntimeError):
            Runtime()


def test_runtime_excludes_engine():
    import rayx
    from rayx.runtime import Runtime
    with Runtime():
        with pytest.raises(RuntimeError):
            rayx.Engine(num_lanes=1, hpx_threads=1)


def test_engine_excludes_endpoint():
    import rayx
    with rayx.Engine(num_lanes=1, hpx_threads=1):
        with pytest.raises(EndpointError):
            Endpoint()


def test_endpoint_excludes_engine():
    import rayx
    with Endpoint():
        with pytest.raises(RuntimeError):
            rayx.Engine(num_lanes=1, hpx_threads=1)
