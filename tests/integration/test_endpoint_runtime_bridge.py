"""Integration tests for the experimental endpoint->Runtime bridge (v1 + v2).

Require the built ``_rayx`` extension + HPX; the module skips cleanly via ``importorskip``
if the extension is not built. These gate the **structural** bridge, NOT performance: a
cross-process endpoint request triggers ONE fixed registered Runtime op (closed CALL_OP
frame) in the server process and returns a typed result. **No timing values are asserted.**

Bridge v1 is PLUMBING: it proves the seam (cross-process request -> CALL_OP -> live
same-process Runtime -> typed result). Bridge v2 (the **composed-op structural bridge**)
adds ``fanout_sum``, whose body uses nested HPX composition (``hpx::async`` +
``hpx::when_all``): it proves the bridged path reaches a body that runs in a valid
HPX-thread execution context and executes that composition correctly -- it does **not**
prove scheduling value, overlap, parallelism, or performance (at ``hpx_threads=1`` the child
tasks run cooperatively/sequentially and none of those are claimed). The bridge supports the
instantaneous int64 ops ``square``/``add`` and the composed ``fanout_sum``. AF_UNIX I/O stays
native; HPX is entered only to dispatch the op. See docs/design/endpoint_runtime_bridge.md.
"""
import multiprocessing as mp
import os
import socket
import sys
import threading

import pytest

rayx_endpoint = pytest.importorskip("rayx.endpoint")

import rayx.endpoint as ep_mod  # noqa: E402
from rayx.endpoint import (  # noqa: E402
    Endpoint,
    connect,
    shutdown,
    EndpointRuntimeUnavailableError,
    EndpointValidationError,
    EndpointOperationError,
)
from rayx.endpoint._validate import endpoint_id_hash  # noqa: E402
from rayx._rayx import _ENDPOINT_PING_XOR as PING_XOR  # noqa: E402
from rayx._rayx import _BRIDGE_FANOUT_N_MAX as FANOUT_N_MAX  # noqa: E402

_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "python", "src")
_SOCK_DIR = "/tmp/rayx-ep-bridge-itest"
_CTX = mp.get_context("spawn")  # never fork a process that already started HPX


@pytest.fixture(autouse=True)
def _reset_endpoint_mode():
    os.environ["RAYX_ENDPOINT_SOCK_DIR"] = _SOCK_DIR
    yield
    shutdown()  # idempotent no-op cleanup (endpoints are HPX-free)


def _expected_ping(peer_id, nonce):
    return nonce ^ PING_XOR ^ endpoint_id_hash(peer_id)


# --- child entry points (top-level so spawn can import them) -----------------


def _child_runtime_endpoint(conn, src, sock_dir):
    """Server process: a live Runtime + a transport endpoint sharing the process. Sends its
    metadata, serves bridge calls until 'close', then tears down cleanly."""
    try:
        os.environ["RAYX_ENDPOINT_SOCK_DIR"] = sock_dir
        if src not in sys.path:
            sys.path.insert(0, src)
        from rayx.runtime import Runtime
        from rayx.endpoint import Endpoint as _EP, shutdown as _sd
        rt = Runtime()
        ep = _EP(transport=True)
        conn.send(ep.metadata())
        conn.recv()  # wait for "close"
        ep.close()
        rt.shutdown()
        _sd()
        conn.send("closed")
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        conn.close()


def _child_endpoint_no_runtime(conn, src, sock_dir):
    """Server process with a transport endpoint but NO Runtime -> bridge calls must report
    runtime-unavailable."""
    try:
        os.environ["RAYX_ENDPOINT_SOCK_DIR"] = sock_dir
        if src not in sys.path:
            sys.path.insert(0, src)
        from rayx.endpoint import Endpoint as _EP, shutdown as _sd
        ep = _EP(transport=True)
        conn.send(ep.metadata())
        conn.recv()
        ep.close()
        _sd()
        conn.send("closed")
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        conn.close()


def _spawn(target):
    parent, child = _CTX.Pipe()
    p = _CTX.Process(target=target, args=(child, _SRC, _SOCK_DIR))
    p.start()
    meta = parent.recv()
    assert isinstance(meta, dict), f"child failed to start: {meta!r}"
    return p, parent, meta


def _teardown(p, parent):
    parent.send("close")
    assert parent.recv() == "closed"
    p.join(timeout=10)
    assert not p.is_alive()


# --- 1. same-process bridge --------------------------------------------------


def test_same_process_bridge_matches_direct_runtime():
    from rayx.runtime import Runtime
    with Runtime() as rt, Endpoint() as b:
        conn = connect(b.metadata())
        assert conn._call_op("square", 7) == 49
        assert conn._call_op("add", 3, 4) == 7
        # The bridged result equals the direct Runtime result.
        assert conn._call_op("square", 7) == rt.submit_operation("square", 7).result().value
        # A local ping still works on the same connection while the Runtime is live.
        assert conn.ping(5) == _expected_ping(b.id, 5)
        conn.close()


def test_same_process_runtime_absent_raises():
    # Endpoint up, no Runtime in this process -> typed runtime-unavailable.
    with Endpoint() as a:
        conn = connect(a.metadata())
        assert conn.ping(1) == _expected_ping(a.id, 1)  # ping unaffected
        with pytest.raises(EndpointRuntimeUnavailableError):
            conn._call_op("square", 7)


def test_bridge_arg_validation():
    from rayx.runtime import Runtime
    with Runtime(), Endpoint() as b:
        conn = connect(b.metadata())
        with pytest.raises(EndpointValidationError):
            conn._call_op("nope", 1)             # unknown bridge op
        with pytest.raises(EndpointValidationError):
            conn._call_op("square", 1, 2)        # wrong arity
        with pytest.raises(EndpointValidationError):
            conn._call_op("square", 1.5)         # non-int64 arg
        with pytest.raises(EndpointValidationError):
            conn._call_op("square", True)        # bool rejected
        with pytest.raises(EndpointValidationError):
            conn._call_op("square", 2 ** 63)     # out of int64 range


# --- 2. cross-process bridge -------------------------------------------------


def test_cross_process_bridge_square_and_add():
    p, parent, meta = _spawn(_child_runtime_endpoint)
    try:
        assert meta["transport_kind"] == "unix"
        assert meta["pid"] != os.getpid()        # genuinely another process
        conn = connect(meta)
        assert conn._call_op("square", 7) == 49
        assert conn._call_op("add", 3, 4) == 7
        assert conn._call_op("square", -5) == 25
        conn.close()
    finally:
        _teardown(p, parent)


def test_cross_process_runtime_absent_raises():
    p, parent, meta = _spawn(_child_endpoint_no_runtime)
    try:
        conn = connect(meta)
        with pytest.raises(EndpointRuntimeUnavailableError):
            conn._call_op("square", 7)
        # Ping still works against the runtime-less peer (transport is independent of HPX).
        assert conn.ping(3) == _expected_ping(meta["endpoint_id"], 3)
        conn.close()
    finally:
        _teardown(p, parent)


# --- 2b. composed-op structural bridge (v2: fanout_sum) ---------------------
#
# Bridge v2 carries the registered `fanout_sum(n, parts)` op, whose body uses nested HPX
# composition (hpx::async + hpx::when_all). These tests prove faithful delivery into a valid
# HPX-thread execution context where that composition runs and returns the correct typed
# result. They assert NO scheduling/overlap/parallelism/timing -- at hpx_threads=1 the child
# tasks run cooperatively/sequentially, so there is no overlap to observe and none is claimed.


def test_bridged_fanout_sum_matches_direct():
    from rayx.runtime import Runtime
    with Runtime() as rt, Endpoint() as b:
        conn = connect(b.metadata())
        for n, parts in [(0, 1), (1, 1), (10, 4), (1000, 8)]:
            direct = rt.submit_operation("fanout_sum", n, parts).result().value
            assert conn._call_op("fanout_sum", n, parts) == direct
        conn.close()


def test_bridged_fanout_sum_known_value():
    # Sum_{i<10} i = 45 (masked); a hand-checked deterministic value. No timing.
    from rayx.runtime import Runtime
    with Runtime(), Endpoint() as b:
        conn = connect(b.metadata())
        assert conn._call_op("fanout_sum", 10, 4) == 45
        conn.close()


def test_bridged_fanout_sum_parts_invariant_correctness_only():
    # CORRECTNESS / partition-recombination check ONLY: the fold is invariant to how many
    # internal parts the range is split into (sum-mod is associative). This is a regression
    # guard, NOT evidence of HPX scheduling -- a sequential split-and-sum would pass it too.
    from rayx.runtime import Runtime
    with Runtime(), Endpoint() as b:
        conn = connect(b.metadata())
        n = 5000
        vals = [conn._call_op("fanout_sum", n, parts) for parts in (1, 4, 8)]
        assert len(set(vals)) == 1  # same result regardless of partition width
        conn.close()


def test_bridged_fanout_sum_invalid_semantics_fail():
    # Invalid semantics are delegated to the op body -> failed row -> OP_FAILED.
    from rayx.runtime import Runtime
    with Runtime(), Endpoint() as b:
        conn = connect(b.metadata())
        with pytest.raises(EndpointOperationError):
            conn._call_op("fanout_sum", 100, 0)    # parts < 1 -> op body fails
        with pytest.raises(EndpointOperationError):
            conn._call_op("fanout_sum", -1, 4)     # n < 0 -> op body fails
        conn.close()


def test_bridged_fanout_sum_over_cap_rejected():
    # The bridge caps `n` as a shutdown/drain bound (NOT a perf knob): over-cap -> BADARGS.
    from rayx.runtime import Runtime
    with Runtime(), Endpoint() as b:
        conn = connect(b.metadata())
        with pytest.raises(EndpointValidationError):
            conn._call_op("fanout_sum", FANOUT_N_MAX + 1, 4)
        # At the cap boundary it is accepted and returns the correct value.
        assert conn._call_op("fanout_sum", 4, 2) == 6  # 0+1+2+3 = 6, small in-range case
        conn.close()


def test_cross_process_bridged_fanout_sum():
    p, parent, meta = _spawn(_child_runtime_endpoint)
    try:
        conn = connect(meta)
        assert conn._call_op("fanout_sum", 10, 4) == 45
        assert conn._call_op("fanout_sum", 1000, 1) == conn._call_op("fanout_sum", 1000, 8)
        conn.close()
    finally:
        _teardown(p, parent)


# --- 3. accept-thread exception survival + A1 regression --------------------


def _raw_call_op(path, proto, target_id, op_code, args):
    """Send ONE raw CALL_OP frame and return the decoded response tuple. Uses the
    import-light wire codec so we can craft malformed/edge requests the Python API rejects."""
    from rayx.endpoint import _wire
    c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    c.connect(path)
    try:
        c.sendall(_wire.encode_call_op(proto, target_id, op_code, args))
        buf = b""
        while len(buf) < _wire.WIRE_CALLOP_RESP_SIZE:
            chunk = c.recv(_wire.WIRE_CALLOP_RESP_SIZE - len(buf))
            if not chunk:
                break
            buf += chunk
        return _wire.decode_call_resp(buf)
    finally:
        c.close()


def test_accept_thread_survives_malformed_calls():
    from rayx.endpoint import _wire
    p, parent, meta = _spawn(_child_runtime_endpoint)
    try:
        path = meta["transport_addr"]
        tid = meta["endpoint_id"]
        proto = _wire.WIRE_PROTO_VERSION
        # Unknown op-code -> CALL_OP_UNKNOWN (8); listener must survive.
        _proto, status, _tag, _bits = _raw_call_op(path, proto, tid, 999, [(0, 1)])
        assert status == 8  # CALL_OP_UNKNOWN
        # Wrong arity for square (op_code 1, argc=2) -> CALL_OP_BADARGS (9).
        _proto, status, _tag, _bits = _raw_call_op(path, proto, tid, 1, [(0, 3), (0, 4)])
        assert status == 9  # CALL_OP_BADARGS
        # Double-tagged arg (v1 is int64-only) -> CALL_OP_BADARGS (9).
        _proto, status, _tag, _bits = _raw_call_op(path, proto, tid, 1, [(1, 0)])
        assert status == 9
        # After all those errors a normal bridge call still succeeds (listener alive).
        conn = connect(meta)
        assert conn._call_op("add", 10, 5) == 15
        # ...and A1 ping still works on the same peer.
        assert conn.ping(9) == _expected_ping(tid, 9)
        conn.close()
    finally:
        _teardown(p, parent)


# --- 4. Runtime shutdown race / stress (same process) -----------------------


def test_runtime_shutdown_stress_no_crash_or_hang():
    # Hammer the bridge from a worker thread while the main thread repeatedly brings a
    # Runtime up and down. Every call must EITHER return 49 OR raise the deterministic
    # runtime-unavailable error -- never crash, hang, or return a wrong value.
    a = Endpoint()
    conn = connect(a.metadata())
    results = []
    bad = []
    stop = threading.Event()

    def worker():
        while not stop.is_set():
            try:
                results.append(conn._call_op("square", 7))
            except EndpointRuntimeUnavailableError:
                pass  # expected whenever no Runtime is attached / draining
            except (EndpointOperationError, Exception) as exc:  # noqa: BLE001
                bad.append(repr(exc))

    t = threading.Thread(target=worker)
    t.start()
    try:
        from rayx.runtime import Runtime
        for _ in range(8):
            with Runtime():
                for _ in range(3):
                    pass
    finally:
        stop.set()
        t.join(timeout=15)
    assert not t.is_alive()                      # no hang
    assert bad == [], bad                        # no crash / no unexpected error
    assert all(r == 49 for r in results)         # never a wrong value
    conn.close()
    a.close()
