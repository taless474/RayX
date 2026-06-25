"""Integration tests for the A1 endpoint transport (plain native local IPC, AF_UNIX).

Require the built ``_rayx`` extension + HPX; the module skips cleanly via ``importorskip``
if the extension is not built. These gate the **structural** A1 transport, NOT performance:
genuine cross-process delivery between two LOCAL processes (via ``multiprocessing``, no Ray
needed), the invariance property (a cross-process ping equals the same-peer local-path
transform), the typed failure taxonomy, and deterministic cleanup. **No timing values are
asserted** (the one timeout test asserts only that a bounded deadline raises the timeout
error, never a duration).

HPX is NOT exercised as a data path here: A1 delivery is plain AF_UNIX IPC with the ping
computed inline. See docs/design/endpoint_transport_slice.md.
"""
import multiprocessing as mp
import os
import socket
import stat
import sys

import pytest

rayx_endpoint = pytest.importorskip("rayx.endpoint")

import rayx.endpoint as ep_mod  # noqa: E402
from rayx.endpoint import (  # noqa: E402
    Endpoint,
    connect,
    shutdown,
    EndpointClosedError,
    EndpointNotFoundError,
    EndpointProtocolError,
    EndpointUnreachableError,
    EndpointTimeoutError,
)
from rayx.endpoint._validate import endpoint_id_hash, ENDPOINT_PROTO_VERSION  # noqa: E402
from rayx._rayx import _ENDPOINT_PING_XOR as PING_XOR  # noqa: E402

# Short, fixed socket dir (NOT $TMPDIR -- macOS $TMPDIR can exceed AF_UNIX sun_path).
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "python", "src")
_SOCK_DIR = "/tmp/rayx-ep-itest"
_CTX = mp.get_context("spawn")  # avoid forking a process that already started HPX


@pytest.fixture(autouse=True)
def _reset_endpoint_mode():
    os.environ["RAYX_ENDPOINT_SOCK_DIR"] = _SOCK_DIR
    yield
    shutdown()  # idempotent when endpoint mode is inactive


def _expected_ping(peer_id, nonce):
    return nonce ^ PING_XOR ^ endpoint_id_hash(peer_id)


# --- child entry points (top-level so spawn can import them) -----------------


def _child_endpoint(conn, src, sock_dir):
    """A transport-enabled endpoint in its own process. Sends its metadata, then on the
    'close' command tears down (close + shutdown) and confirms 'closed'."""
    try:
        os.environ["RAYX_ENDPOINT_SOCK_DIR"] = sock_dir
        if src not in sys.path:
            sys.path.insert(0, src)
        from rayx.endpoint import Endpoint as _EP, shutdown as _sd
        ep = _EP(transport=True)
        conn.send(ep.metadata())
        conn.recv()  # wait for "close"
        ep.close()
        _sd()
        conn.send("closed")
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        conn.close()


def _child_two_endpoints(conn, src, sock_dir):
    """Two transport endpoints in one process (one shared listener). Closes the FIRST on
    'close1' (listener stays alive via the second), then both on 'close_all'."""
    try:
        os.environ["RAYX_ENDPOINT_SOCK_DIR"] = sock_dir
        if src not in sys.path:
            sys.path.insert(0, src)
        from rayx.endpoint import Endpoint as _EP, shutdown as _sd
        ep1 = _EP(transport=True)
        ep2 = _EP(transport=True)
        conn.send((ep1.metadata(), ep2.metadata()))
        conn.recv()           # "close1"
        ep1.close()           # listener stays up because ep2 is still live
        conn.send("closed1")
        conn.recv()           # "close_all"
        ep2.close()
        _sd()
        conn.send("closed_all")
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        conn.close()


def _child_raw_request_then_close(conn, sock_dir, path, target_id):
    """Connect to a live rayx listener, send ONE valid request, then close immediately
    WITHOUT reading the response -- exercising a server-side write to a closed peer
    (SIGPIPE robustness). Uses the import-light wire codec."""
    try:
        if sock_dir:
            os.environ["RAYX_ENDPOINT_SOCK_DIR"] = sock_dir
        sys.path.insert(0, _SRC)
        from rayx.endpoint import _wire
        c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        c.connect(path)
        c.sendall(_wire.encode_request(_wire.WIRE_PROTO_VERSION, target_id, 1))
        c.close()             # close before reading -> server write hits a closed peer
        conn.send("sent")
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        conn.close()


def _child_staller(conn, sock_dir):
    """A RAW AF_UNIX server (no rayx) that accepts but NEVER replies -- used to drive a
    bounded ping timeout. Sends (path, pid); stops on any command."""
    srv = None
    path = None
    accepted = []
    try:
        os.makedirs(sock_dir, exist_ok=True)
        path = os.path.join(sock_dir, f"stall-{os.getpid()}.sock")
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(path)
        srv.listen(8)
        srv.settimeout(0.1)
        conn.send((path, os.getpid()))
        while True:
            try:
                c, _ = srv.accept()
                accepted.append(c)  # keep open, never read/reply
            except socket.timeout:
                pass
            if conn.poll(0):
                conn.recv()
                break
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        for c in accepted:
            try:
                c.close()
            except OSError:
                pass
        if srv is not None:
            srv.close()
        if path is not None:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
        conn.close()


def _spawn_endpoint_child():
    parent, child = _CTX.Pipe()
    p = _CTX.Process(target=_child_endpoint, args=(child, _SRC, _SOCK_DIR))
    p.start()
    meta = parent.recv()
    assert isinstance(meta, dict), f"child failed to start: {meta!r}"
    return p, parent, meta


# --- 1. cross-process delivery + invariance ---------------------------------


def test_cross_process_ping_matches_local_transform():
    p, parent, meta = _spawn_endpoint_child()
    try:
        assert meta["transport_kind"] == "unix"
        assert meta["pid"] != os.getpid()           # genuinely another process
        conn = connect(meta)
        for nonce in (0, 1, 123456789, -7, 2 ** 40):
            assert conn.ping(nonce) == _expected_ping(meta["endpoint_id"], nonce)
        conn.close()
    finally:
        parent.send("close")
        assert parent.recv() == "closed"
        p.join(timeout=10)


def test_cross_process_two_peers_are_peer_specific():
    p1, c1, m1 = _spawn_endpoint_child()
    p2, c2, m2 = _spawn_endpoint_child()
    try:
        assert m1["endpoint_id"] != m2["endpoint_id"]
        nonce = 424242
        v1 = connect(m1).ping(nonce)
        v2 = connect(m2).ping(nonce)
        assert v1 == _expected_ping(m1["endpoint_id"], nonce)
        assert v2 == _expected_ping(m2["endpoint_id"], nonce)
        assert v1 != v2
    finally:
        for c, p in ((c1, p1), (c2, p2)):
            c.send("close")
            c.recv()
            p.join(timeout=10)


# --- 2. typed failure modes -------------------------------------------------


def test_not_listening_raises_unreachable():
    # Cross-process metadata pointing at a unix path with nothing listening.
    meta = {"endpoint_id": "rtb-ep-0123456789abcdef", "pid": os.getpid() + 1,
            "host": socket.gethostname(), "proto_version": ENDPOINT_PROTO_VERSION,
            "transport_kind": "unix",
            "transport_addr": os.path.join(_SOCK_DIR, "ep-nonexistent-9999999.sock")}
    with pytest.raises(EndpointUnreachableError):
        connect(meta, timeout=1.0)


def test_stale_id_on_live_listener_raises_not_found():
    p, parent, meta = _spawn_endpoint_child()
    try:
        stale = dict(meta)
        stale["endpoint_id"] = "rtb-ep-ffffffffffffffff"  # not registered in the child
        conn = connect(stale)                              # listener is up -> probe ok
        with pytest.raises(EndpointNotFoundError):
            conn.ping(7)
    finally:
        parent.send("close")
        parent.recv()
        p.join(timeout=10)


def test_peer_closed_raises_closed():
    p, parent, meta = _spawn_endpoint_child()
    try:
        conn = connect(meta)                # reachable now (probe succeeds)
        parent.send("close")                # child closes endpoint + listener
        assert parent.recv() == "closed"
        with pytest.raises(EndpointClosedError):
            conn.ping(1)                    # listener gone -> dial fails -> closed
    finally:
        p.join(timeout=10)


def test_timeout_raises_timeout():
    parent, child = _CTX.Pipe()
    p = _CTX.Process(target=_child_staller, args=(child, _SOCK_DIR))
    p.start()
    try:
        info = parent.recv()
        assert isinstance(info, tuple) and not (info and info[0] == "error"), info
        path, spid = info
        meta = {"endpoint_id": "rtb-ep-0123456789abcdef", "pid": spid,
                "host": socket.gethostname(), "proto_version": ENDPOINT_PROTO_VERSION,
                "transport_kind": "unix", "transport_addr": path}
        conn = connect(meta, timeout=0.3)   # probe: staller accepts -> ok
        with pytest.raises(EndpointTimeoutError):
            conn.ping(123)                  # staller never replies -> bounded timeout
    finally:
        parent.send("stop")
        p.join(timeout=10)


def test_wrong_host_raises_unreachable():
    meta = {"endpoint_id": "rtb-ep-0123456789abcdef", "pid": os.getpid() + 1,
            "host": "definitely-not-this-host", "proto_version": ENDPOINT_PROTO_VERSION,
            "transport_kind": "unix", "transport_addr": "/tmp/rayx-ep/whatever.sock"}
    with pytest.raises(EndpointUnreachableError):
        connect(meta)


def test_proto_mismatch_raises_protocol():
    meta = {"endpoint_id": "rtb-ep-0123456789abcdef", "pid": os.getpid() + 1,
            "host": socket.gethostname(), "proto_version": ENDPOINT_PROTO_VERSION + 1000,
            "transport_kind": "unix", "transport_addr": "/tmp/rayx-ep/whatever.sock"}
    with pytest.raises(EndpointProtocolError):
        connect(meta)


def test_transport_disabled_cross_process_unreachable():
    # A transport-OFF peer in another process must stay cleanly unreachable.
    meta = {"endpoint_id": "rtb-ep-0123456789abcdef", "pid": os.getpid() + 1,
            "host": socket.gethostname(), "proto_version": ENDPOINT_PROTO_VERSION,
            "transport_kind": "none", "transport_addr": ""}
    with pytest.raises(EndpointUnreachableError):
        connect(meta)


# --- 2b. closed-peer vs stale-id on a STILL-LIVE listener (A1.1) -------------


def test_closed_peer_vs_stale_id_on_live_listener():
    # One child process, two transport endpoints sharing one listener. Close the FIRST
    # endpoint; the listener stays up via the second. Then:
    #   * pinging the CLOSED id -> EndpointClosedError (tombstone),
    #   * pinging a NEVER-registered id on the same live listener -> EndpointNotFoundError.
    parent, child = _CTX.Pipe()
    p = _CTX.Process(target=_child_two_endpoints, args=(child, _SRC, _SOCK_DIR))
    p.start()
    try:
        payload = parent.recv()
        assert isinstance(payload, tuple) and isinstance(payload[0], dict), payload
        meta1, meta2 = payload
        conn1 = connect(meta1)                # reachable now (probe ok)
        parent.send("close1")
        assert parent.recv() == "closed1"     # ep1 closed; listener alive via ep2

        with pytest.raises(EndpointClosedError):
            conn1.ping(1)                     # CLOSED id on a live listener

        stale = dict(meta2)
        stale["endpoint_id"] = "rtb-ep-aaaaaaaaaaaaaaaa"  # never registered
        with pytest.raises(EndpointNotFoundError):
            connect(stale).ping(2)            # ABSENT id on the same live listener
    finally:
        parent.send("close_all")
        parent.recv()
        p.join(timeout=10)


# --- 2c. SIGPIPE robustness (server write to a closed peer) ------------------


def test_server_survives_peer_closing_before_reading_response():
    p, parent, meta = _spawn_endpoint_child()
    try:
        path = meta["transport_addr"]
        # Raw client: send a valid request, then close before reading -> the server's
        # response write goes to a closed peer. Must NOT kill the server process.
        rp, rc = _CTX.Pipe()
        rproc = _CTX.Process(target=_child_raw_request_then_close,
                             args=(rc, _SOCK_DIR, path, meta["endpoint_id"]))
        rproc.start()
        assert rp.recv() == "sent"
        rproc.join(timeout=10)
        # The endpoint process is still alive and serving: a normal ping still works.
        assert connect(meta).ping(5) == _expected_ping(meta["endpoint_id"], 5)
    finally:
        parent.send("close")
        assert parent.recv() == "closed"
        p.join(timeout=10)


# --- 2d. socket directory permission enforcement (A1.1) ----------------------


def test_sock_dir_permissions_enforced_when_preexisting_loose():
    d = "/tmp/rayx-ep-permtest"
    os.makedirs(d, exist_ok=True)
    os.chmod(d, 0o777)                         # pre-existing, group/world accessible
    os.environ["RAYX_ENDPOINT_SOCK_DIR"] = d   # override the autouse default for this test
    try:
        a = Endpoint(transport=True)           # listener start must tighten the dir
        try:
            st = os.stat(d)
            assert stat.S_ISDIR(st.st_mode)
            assert st.st_uid == os.geteuid()
            assert stat.S_IMODE(st.st_mode) == 0o700   # tightened to owner-only
        finally:
            a.close()
    finally:
        os.environ["RAYX_ENDPOINT_SOCK_DIR"] = _SOCK_DIR


# --- 3. cleanup / lifecycle -------------------------------------------------


def test_child_socket_file_removed_on_close():
    p, parent, meta = _spawn_endpoint_child()
    sock_path = os.path.join(_SOCK_DIR, f"ep-{meta['pid']}.sock")
    assert os.path.exists(sock_path)        # listener is up
    parent.send("close")
    assert parent.recv() == "closed"
    p.join(timeout=10)
    assert not os.path.exists(sock_path)    # unlinked on close


def test_local_listener_socket_removed_and_runtime_starts_after():
    from rayx.runtime import Runtime
    a = Endpoint(transport=True)
    sock_path = os.path.join(_SOCK_DIR, f"ep-{os.getpid()}.sock")
    assert os.path.exists(sock_path)
    assert not ep_mod._process_hpx_active()  # Variant 2: a transport endpoint is HPX-free
    a.close()                               # last transport endpoint -> listener stops
    assert not os.path.exists(sock_path)
    assert not ep_mod._process_hpx_active()
    with Runtime() as rt:                   # Runtime owns HPX; it can start any time
        assert rt.submit_operation("square", 7).result().value == 49


def test_transport_endpoint_coexists_with_runtime():
    # An Endpoint(transport=True) and a Runtime share one process: the Runtime owns HPX,
    # the A1 AF_UNIX transport runs alongside it (plain native IPC, not HPX).
    from rayx.runtime import Runtime
    with Runtime() as rt, Endpoint(transport=True) as a:
        assert ep_mod._process_hpx_active()  # HPX up because of the Runtime
        sock_path = os.path.join(_SOCK_DIR, f"ep-{os.getpid()}.sock")
        assert os.path.exists(sock_path)     # listener is up
        # Same-process registry fast path still works while the Runtime is live.
        assert connect(a.metadata()).ping(11) == _expected_ping(a.id, 11)
        assert rt.submit_operation("square", 9).result().value == 81
    assert not os.path.exists(sock_path)     # endpoint close stopped the listener
    assert not ep_mod._process_hpx_active()  # runtime exit stopped HPX


def test_same_process_local_path_still_works_with_transport_on():
    # transport=True must not break the same-process registry fast path.
    with Endpoint(transport=True) as a:
        assert connect(a.metadata()).ping(5) == _expected_ping(a.id, 5)
