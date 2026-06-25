#!/usr/bin/env python3
"""exp43: local one-shot AF_UNIX endpoint *ping* round-trip floor, WITHOUT Runtime
dispatch, using the real endpoint API path -- plus a same-shape Python AF_UNIX echo
control.

This is an OS/IPC endpoint-transport microprobe, NOT HPX-mechanism evidence. EP0 and
EP1 deliberately have ZERO HPX in the path (an `Endpoint` is HPX-free; with no Runtime
the process HPX runtime never starts), and the ping is a fixed typed int64 transform
computed inline on the serving side. There is no Runtime, no bridge, no op body.

Paths (all use the REAL endpoint API -- no spoofed pid, no private transport helpers):

  EP0  same-process endpoint ping (registry/local path, no socket):
         ep = Endpoint()                       # transport OFF
         conn = connect(ep.metadata())         # meta["pid"] == os.getpid()
         conn.ping(nonce)
       connect() takes the `meta["pid"] == os.getpid()` branch
       (rayx/endpoint/__init__.py -> _endpoint_connect_local) and ping() is computed
       INLINE on the calling thread. No socket, no accept thread, no Runtime, HPX
       inactive. This is the near-zero-work local-path floor.

  EP1  cross-process endpoint ping (one-shot dial-per-call AF_UNIX):
         child: ep = Endpoint(transport=True)  # Endpoint ONLY, no Runtime
         parent: connect(child_meta) -> pid != getpid() -> AF_UNIX remote path
         parent: conn.ping(nonce)              # EACH call dials afresh
       The remote Connection is a probed handle, NOT a persistent fd: every ping is a
       fresh dial + 39-byte request + child accept-thread INLINE ping compute +
       16-byte response + teardown. No Runtime in child, HPX inactive on both sides.

  EPraw  same-shape PYTHON AF_UNIX echo control (NOT endpoint behavior, NOT an OS floor):
         child: plain socket.AF_UNIX stream server, accept/recv/send/close per call
         parent: per call -> connect + 39-byte request + 16-byte fixed reply + close
       No rayx, no framing, no registry, no HPX. EPraw uses the SAME one-shot
       connect/accept/recv/send/close envelope and 39B/16B byte sizes as EP1, but its
       server AND client are interpreted PYTHON while EP1's serving side is a NATIVE C++
       accept thread. Because of that, EPraw is NOT a lower bound on EP1, does NOT isolate
       kernel AF_UNIX cost, and is NOT a minimal C/native floor.

Interpretation (observation-only):
  * EP1 is a ONE-SHOT dial-per-call endpoint ping round trip. The dominant term is
    likely socket/connect/accept + fd setup/teardown each call, NOT moving 55 bytes.
    Call it that -- NOT "AF_UNIX transport cost", NOT fabric/persistent-transport cost,
    NOT endpoint->Runtime cost.
  * EP1 - EP0 is the cross-process one-shot endpoint round-trip path difference vs the
    same-process inline floor (end-to-end, observation-only).
  * EP1 - EPraw is retained ONLY as a cross-implementation observation: it compares two
    different implementations (native C++ vs Python) of the same-shaped one-shot round
    trip and is dominated by Python-vs-native server/client differences plus the EPraw
    accept-poll loop. It is NOT rayx endpoint overhead, NOT listener/framing/registry
    overhead, NOT an above-the-OS-floor reading, and NOT fabric evidence.
  * Do NOT mechanically subtract EP1 from exp42 P2: exp42 P2's child runs a live HPX
    Runtime + workers contending with its accept thread; exp43 EP1's child has no HPX
    workers. EP1 is a runtime-less lower reference, not a clean subtractor.
  * Sequential single-in-flight maximizes idle gaps, so each EP1/EPraw call may pay OS
    scheduler wake-up latency on the blocked accept()/recv(). EP1 is NOT a stable layer
    constant; classify deltas vs run-to-run jitter (IQR), not the timer floor alone.
  * Darwin/macOS (or whatever OS this runs on) AF_UNIX connect/accept profiles differ
    from Linux; numbers are OS-local and NON-TRANSFERABLE.

NON-CLAIMS: no endpoint->Runtime bridge claim, no HPX mechanism/scheduling/value/design
result, no speedup, no throughput, no general latency claim, no Ray comparison, no
distributed fabric, no persistent transport/channel claim, no parcelport, no AGAS, no
multi-node, no public endpoint-call API beyond the existing ping, no exact decomposition
of exp42 P2, no mechanical P2-EP1 subtraction.

Run (laptop -- generates aggregate.json beside this file):
  PYTHONPATH=python/src python \
    experiments/43_endpoint_transport_ping_floor/run_endpoint_ping_floor.py --smoke
"""
import argparse
import gc
import json
import multiprocessing as mp
import os
import platform
import socket
import sys
import time

# Fixed nonce used in the timed loop (a stable int64 value).
PING_NONCE = 0x0123456789ABCDEF
# A handful of distinct nonces for the pre-timing correctness sweep.
CORRECTNESS_NONCES = [0, 1, -1, 0x0123456789ABCDEF, -0x0123456789ABCDEF, 2 ** 62]

# Raw control wire shapes (match the endpoint PING frame sizes; CONTENT is arbitrary --
# EPraw is NOT the endpoint protocol, it is a same-shape Python round-trip of the same
# byte sizes; it is NOT a raw OS floor).
RAW_REQ_LEN = 39
RAW_RESP_LEN = 16
RAW_REQ_BYTES = b"X" * RAW_REQ_LEN
RAW_RESP_BYTES = bytes(range(RAW_RESP_LEN))  # fixed, deterministic 16-byte reply

# rayx endpoint transport listener socket dir (owner-only is enforced natively).
_SOCK_DIR = os.environ.setdefault(
    "RAYX_ENDPOINT_SOCK_DIR", "/tmp/rayx-ep-exp43")
# Separate short dir for the raw AF_UNIX control socket (kept away from the rayx dir so
# rayx's owner-only dir enforcement is not perturbed). Short path: AF_UNIX path limit.
_RAW_DIR = os.environ.get("RAYX_EXP43_RAW_DIR", "/tmp/rayx-ep-exp43-raw")
_CTX = mp.get_context("spawn")  # never fork a process that already touched native state


# --- ping oracle (mirror of the native transform) ---------------------------


def _oracle(nonce, peer_id, ping_xor, id_hash):
    """nonce ^ ENDPOINT_PING_XOR ^ endpoint_id_hash(peer_id), as Python int (the native
    side returns the same int64; we compare value-equality)."""
    return nonce ^ ping_xor ^ id_hash(peer_id)


# --- timing helpers (consistent with exp42) ---------------------------------


def _pct(xs, p):
    """Nearest-rank percentile of a non-empty list (no numpy)."""
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def _summ(samples):
    """median/p25/p75/iqr (ns, ints) over a non-empty sample list."""
    med = _pct(samples, 50)
    p25 = _pct(samples, 25)
    p75 = _pct(samples, 75)
    return {"median_ns": int(med), "p25_ns": int(p25), "p75_ns": int(p75),
            "iqr_ns": int(p75 - p25)}


def _measure(call, reps, warmup):
    """Time `call()` reps+warmup times (warmup discarded), single-in-flight, GC off in
    the measured loop. Returns (samples_ns, last_value)."""
    last = None
    for _ in range(warmup):
        last = call()
    samples = []
    gc_was = gc.isenabled()
    gc.disable()
    try:
        for _ in range(reps):
            t0 = time.perf_counter_ns()
            last = call()
            t1 = time.perf_counter_ns()
            samples.append(t1 - t0)
    finally:
        if gc_was:
            gc.enable()
    return samples, last


def _empty_loop_floor_ns(reps):
    """Floor of an empty perf_counter_ns delta loop (timer + loop overhead)."""
    samples = []
    gc_was = gc.isenabled()
    gc.disable()
    try:
        for _ in range(reps):
            t0 = time.perf_counter_ns()
            t1 = time.perf_counter_ns()
            samples.append(t1 - t0)
    finally:
        if gc_was:
            gc.enable()
    return int(_pct(samples, 50))


def classify_delta(med_a, iqr_a, med_b, iqr_b, empty_floor, end_to_end=False,
                   cross_impl=False):
    """Classify median(b)-median(a) against the timer floor and run-to-run jitter.
    `pooled_iqr` is the conservative max of the two paths' IQRs: a delta smaller than
    that sits inside normal run-to-run variation and is NOT reported as signal.
    `end_to_end=True` flags an EP1-based delta whose above-jitter value is still an
    end-to-end one-shot observation, NOT transport/fabric cost. `cross_impl=True` flags
    an EP1-vs-EPraw delta that compares two DIFFERENT implementations (native C++ vs
    Python) of the same-shaped round trip: it is a cross-implementation observation only,
    NOT overhead and NOT a raw-OS-floor relationship."""
    delta = med_b - med_a
    pooled_iqr = max(iqr_a, iqr_b)
    below_resolution = abs(delta) <= empty_floor
    within_jitter = abs(delta) <= pooled_iqr
    meaningful = not below_resolution and not within_jitter
    if below_resolution:
        status = "below_resolution"
    elif within_jitter:
        status = "within_jitter"
    elif cross_impl:
        status = "cross_implementation_observation_only"
    elif end_to_end:
        status = "end_to_end_observation_only"
    else:
        status = "above_jitter_observation_only"
    return {
        "delta_ns": int(delta), "pooled_iqr_ns": int(pooled_iqr),
        "below_resolution": bool(below_resolution),
        "within_jitter": bool(within_jitter),
        "meaningful_above_jitter": bool(meaningful),
        "interpretation_status": status,
    }


# --- EP1 child: a transport Endpoint ONLY (no Runtime, HPX inactive) ---------


def _ep1_child(conn, src, sock_dir):
    """EP1 server: ONE transport Endpoint, no Runtime. Sends its bootstrap metadata and
    its HPX-active / runtime-created controls, then serves cross-process pings (via the
    endpoint accept thread) until told to close. HPX must stay inactive here."""
    try:
        os.environ["RAYX_ENDPOINT_SOCK_DIR"] = sock_dir
        if src not in sys.path:
            sys.path.insert(0, src)
        from rayx.endpoint import Endpoint, shutdown as _sd, _process_hpx_active
        ep = Endpoint(transport=True)
        conn.send({
            "metadata": ep.metadata(),
            "hpx_active": bool(_process_hpx_active()),
            "runtime_created": False,
        })
        conn.recv()  # block until parent says "close"
        # Re-report HPX state after serving (it must still be inactive).
        post_hpx = bool(_process_hpx_active())
        ep.close()
        _sd()
        conn.send({"status": "closed", "post_hpx_active": post_hpx})
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        conn.close()


# --- EPraw child: a plain AF_UNIX echo server (no rayx, no HPX) ---------------


def _raw_child(conn, raw_dir):
    """EPraw server: a plain blocking AF_UNIX stream server. One-shot per call:
    accept/recv/send/close. Returns a fixed 16-byte reply. No rayx, no framing, no
    registry, no HPX."""
    srv = None
    sock_path = os.path.join(raw_dir, f"epraw-{os.getpid()}.sock")
    try:
        os.makedirs(raw_dir, mode=0o700, exist_ok=True)
        os.chmod(raw_dir, 0o700)
        if os.path.exists(sock_path):
            os.unlink(sock_path)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(sock_path)
        srv.listen(16)
        srv.settimeout(0.2)  # poll the control pipe between accepts
        conn.send({"sock_path": sock_path})
        while True:
            if conn.poll(0):
                if conn.recv() == "close":
                    break
            try:
                c, _ = srv.accept()
            except socket.timeout:
                continue
            try:
                # Drain the fixed-size request (best effort), then reply fixed bytes.
                need = RAW_REQ_LEN
                while need > 0:
                    chunk = c.recv(need)
                    if not chunk:
                        break
                    need -= len(chunk)
                c.sendall(RAW_RESP_BYTES)
            finally:
                c.close()
        conn.send({"status": "closed"})
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        if srv is not None:
            srv.close()
        try:
            os.unlink(sock_path)
        except OSError:
            pass
        conn.close()


def _raw_ping(sock_path):
    """One-shot raw AF_UNIX round trip: fresh connect + 39-byte request + 16-byte reply
    + close. Returns the reply bytes."""
    c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    c.connect(sock_path)
    try:
        c.sendall(RAW_REQ_BYTES)
        data = b""
        while len(data) < RAW_RESP_LEN:
            chunk = c.recv(RAW_RESP_LEN - len(data))
            if not chunk:
                break
            data += chunk
        return data
    finally:
        c.close()


# --- child spawn / teardown --------------------------------------------------


def _spawn(target, *args):
    parent, child = _CTX.Pipe()
    p = _CTX.Process(target=target, args=(child, *args))
    p.start()
    msg = parent.recv()
    return p, parent, msg


def _teardown(p, pipe):
    pipe.send("close")
    ack = pipe.recv()
    p.join(timeout=10)
    alive = p.is_alive()
    ok = (not alive) and isinstance(ack, dict) and ack.get("status") == "closed"
    return ok, ack, alive


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--out", default=None,
                    help="aggregate.json path (default: beside this script)")
    ap.add_argument("--no-raw", action="store_true",
                    help="skip the EPraw raw AF_UNIX control (EP1 then undifferentiated)")
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop run (structural only)")
    args = ap.parse_args()

    if args.smoke:
        reps, warmup = 30, 8
    else:
        reps, warmup = args.reps, args.warmup
    include_raw = not args.no_raw

    here = os.path.dirname(os.path.abspath(__file__))
    out_path = args.out or os.path.join(here, "aggregate.json")
    src = os.path.join(os.path.dirname(os.path.dirname(here)), "python", "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    from rayx._rayx import _ENDPOINT_PING_XOR
    from rayx.endpoint import Endpoint, connect, shutdown, _process_hpx_active
    from rayx.endpoint._validate import endpoint_id_hash

    empty_floor = _empty_loop_floor_ns(max(reps, 50))

    measurements = []     # one row per path
    correctness_fail = []
    control_fail = []

    def record(path, samples, peer_id, value, runtimes_active, extra):
        s = _summ(samples)
        ref = _oracle(PING_NONCE, peer_id, _ENDPOINT_PING_XOR, endpoint_id_hash)
        equal_ok = (value == ref)
        if not equal_ok:
            correctness_fail.append(
                f"{path}: ping({PING_NONCE}) -> {value} != oracle {ref}")
        row = {
            "path": path, "reps": reps, "warmup": warmup,
            "runtimes_active": runtimes_active,
            "machine_cpu_count": os.cpu_count(),
            "median_ns": s["median_ns"], "p25_ns": s["p25_ns"],
            "p75_ns": s["p75_ns"], "iqr_ns": s["iqr_ns"],
            "value": value, "oracle": ref, "result_equal_ok": equal_ok,
            "below_resolution": bool(s["median_ns"] <= empty_floor),
        }
        row.update(extra)
        measurements.append(row)
        return row

    # Parent HPX control: must be inactive (no Runtime constructed anywhere here).
    parent_hpx_active = bool(_process_hpx_active())
    if parent_hpx_active:
        control_fail.append("parent _process_hpx_active() is True before any path")

    def correctness_sweep(conn, peer_id, label):
        """Pre-timing: every oracle nonce must match (structural gate, not timing)."""
        for nz in CORRECTNESS_NONCES:
            got = conn.ping(nz)
            ref = _oracle(nz, peer_id, _ENDPOINT_PING_XOR, endpoint_id_hash)
            if got != ref:
                correctness_fail.append(
                    f"{label}: ping({nz}) -> {got} != oracle {ref}")

    # ---- EP0: same-process endpoint ping (no socket, no Runtime) ----
    with Endpoint() as ep0:                       # transport OFF
        conn = connect(ep0.metadata())            # same pid -> registry/local path
        try:
            correctness_sweep(conn, ep0.id, "EP0")
            samp, val = _measure(lambda: conn.ping(PING_NONCE), reps, warmup)
            record("EP0", samp, ep0.id, val, 0, {
                "kind": "same_process_inline",
                "hpx_active_serving_side": bool(_process_hpx_active()),
            })
        finally:
            conn.close()
    if bool(_process_hpx_active()):
        control_fail.append("EP0 left process HPX active (must stay inactive)")

    # ---- EP1: cross-process one-shot dial-per-call endpoint ping ----
    p, pipe, hello = _spawn(_ep1_child, src, _SOCK_DIR)
    child_hpx_active = None
    child_runtime_created = None
    ep1_clean = False
    ep1_ack = None
    try:
        if not isinstance(hello, dict) or "metadata" not in hello:
            control_fail.append(f"EP1 child did not start: {hello!r}")
        else:
            child_meta = hello["metadata"]
            child_hpx_active = bool(hello["hpx_active"])
            child_runtime_created = bool(hello["runtime_created"])
            if child_meta["pid"] == os.getpid():            # defensive
                control_fail.append("EP1 child shares parent pid (not a separate proc)")
            if child_meta["transport_kind"] != "unix":
                control_fail.append("EP1 child did not advertise AF_UNIX transport")
            if child_hpx_active:
                control_fail.append("EP1 child _process_hpx_active() is True")
            if child_runtime_created:
                control_fail.append("EP1 child created a Runtime (must not)")
            conn = connect(child_meta)                      # pid != getpid -> AF_UNIX
            try:
                correctness_sweep(conn, child_meta["endpoint_id"], "EP1")
                samp, val = _measure(lambda: conn.ping(PING_NONCE), reps, warmup)
                record("EP1", samp, child_meta["endpoint_id"], val, 0, {
                    "kind": "cross_process_one_shot_dial_per_call",
                    "child_hpx_active": child_hpx_active,
                    "child_runtime_created": child_runtime_created,
                    "transport_kind": child_meta["transport_kind"],
                })
            finally:
                conn.close()
    finally:
        ep1_clean, ep1_ack, ep1_alive = _teardown(p, pipe)
        if not ep1_clean:
            control_fail.append(f"EP1 child teardown not clean (ack={ep1_ack!r})")
        # The child must have stayed HPX-inactive across serving, too.
        if isinstance(ep1_ack, dict) and ep1_ack.get("post_hpx_active"):
            control_fail.append("EP1 child became HPX-active while serving")

    # ---- EPraw: same-shape Python AF_UNIX echo control (no rayx, no HPX; NOT an OS floor) ----
    raw_row = None
    raw_clean = None
    if include_raw:
        rp, rpipe, rhello = _spawn(_raw_child, _RAW_DIR)
        raw_ack = None
        try:
            if not isinstance(rhello, dict) or "sock_path" not in rhello:
                control_fail.append(f"EPraw child did not start: {rhello!r}")
            else:
                sock_path = rhello["sock_path"]
                # Pre-timing correctness: reply must equal the fixed 16 bytes.
                first = _raw_ping(sock_path)
                if first != RAW_RESP_BYTES:
                    correctness_fail.append(
                        f"EPraw: reply {first!r} != fixed {RAW_RESP_BYTES!r}")
                samp, _ = _measure(lambda: _raw_ping(sock_path), reps, warmup)
                s = _summ(samp)
                raw_row = {
                    "path": "EPraw", "reps": reps, "warmup": warmup,
                    "runtimes_active": 0, "machine_cpu_count": os.cpu_count(),
                    "median_ns": s["median_ns"], "p25_ns": s["p25_ns"],
                    "p75_ns": s["p75_ns"], "iqr_ns": s["iqr_ns"],
                    "kind": "raw_os_one_shot_af_unix",
                    "reply_equal_ok": (first == RAW_RESP_BYTES),
                    "req_len": RAW_REQ_LEN, "resp_len": RAW_RESP_LEN,
                    "below_resolution": bool(s["median_ns"] <= empty_floor),
                }
                measurements.append(raw_row)
        finally:
            raw_clean, raw_ack, raw_alive = _teardown(rp, rpipe)
            if not raw_clean:
                control_fail.append(f"EPraw child teardown not clean (ack={raw_ack!r})")

    shutdown()  # idempotent endpoint cleanup (HPX-free, never stops HPX)

    # ---- deltas + IQR/jitter classification (observation-only) ----
    def row_for(path):
        for r in measurements:
            if r["path"] == path:
                return r
        return None

    ep0 = row_for("EP0")
    ep1 = row_for("EP1")
    epr = row_for("EPraw")

    deltas = {}
    if ep0 and ep1:
        deltas["EP1_minus_EP0"] = classify_delta(
            ep0["median_ns"], ep0["iqr_ns"], ep1["median_ns"], ep1["iqr_ns"],
            empty_floor, end_to_end=True)
    if ep1 and epr:
        deltas["EP1_minus_EPraw"] = classify_delta(
            epr["median_ns"], epr["iqr_ns"], ep1["median_ns"], ep1["iqr_ns"],
            empty_floor, cross_impl=True)

    structural_pass = (
        not correctness_fail
        and not control_fail
        and all(r.get("result_equal_ok", True) for r in measurements)
        and parent_hpx_active is False
        and (child_hpx_active is False)
        and (child_runtime_created is False)
        and ep1_clean
        and (raw_clean in (None, True))
    )

    aggregate = {
        "experiment": "exp43_endpoint_transport_ping_floor",
        "schema": "rayx-endpoint-ping-floor-1",
        "claim": (
            "characterizes the local one-shot endpoint ping round-trip floor WITHOUT "
            "Runtime dispatch, using the real endpoint API path (EP0 same-process inline; "
            "EP1 cross-process one-shot dial-per-call). EPraw is included ONLY as a "
            "same-shape Python AF_UNIX echo control, NOT as a raw OS lower bound"),
        "ep1_framing": (
            "EP1 is a ONE-SHOT dial-per-call endpoint ping round trip: every ping is a "
            "fresh socket/connect/accept/send/recv/close. This is NOT 'AF_UNIX transport "
            "cost', NOT fabric/persistent-transport cost, NOT endpoint->Runtime cost. The "
            "dominant term is likely connection setup/teardown, not moving 55 bytes."),
        "epraw_framing": (
            "EPraw is a SAME-SHAPE PYTHON AF_UNIX echo control (plain accept/recv/send/"
            "close, no rayx, no HPX). It uses the same one-shot connect/accept/recv/send/"
            "close envelope and 39B/16B byte sizes as EP1. IMPORTANT: EPraw's server AND "
            "client are interpreted PYTHON while EP1's serving side is a NATIVE C++ accept "
            "thread, so EPraw is NOT a raw OS lower floor, is NOT a minimal C/native socket "
            "floor, does NOT isolate kernel AF_UNIX cost, and is NOT guaranteed to be lower "
            "than EP1 (the observed EP1 - EPraw sign is negative purely because the Python "
            "accept-poll/recv/send loop is slower than the native one). EP1 - EPraw is "
            "retained ONLY as a cross-implementation observation, dominated by Python-vs-"
            "native server/client differences and the EPraw accept-poll loop; it is NOT "
            "rayx endpoint overhead, NOT listener/framing/registry overhead, and NOT fabric "
            "evidence."),
        "non_claims": [
            "no endpoint->Runtime bridge claim",
            "no HPX mechanism / scheduling / value / design result",
            "no speedup",
            "no throughput claim",
            "no general latency claim",
            "no Ray comparison",
            "no distributed fabric",
            "no persistent transport / channel claim",
            "no parcelport",
            "no AGAS",
            "no multi-node",
            "no public endpoint-call API beyond the existing ping",
            "no exact decomposition of exp42 P2",
            "no mechanical P2 - EP1 subtraction",
            "no EP1 - EPraw overhead claim",
            "no raw OS floor claim (EPraw is a same-shape Python control, not an OS floor)",
            "EP1 is NOT a stable layer constant: sequential single-in-flight calls may "
            "include OS scheduler wake-up latency on the blocked accept()/recv()",
            "OS-local timing (see machine.platform); NON-TRANSFERABLE across OSes",
        ],
        "caveats": {
            "os_scheduler_wakeup": (
                "Sequential single-in-flight pings maximize idle gaps; each EP1/EPraw call "
                "may pay OS wake-up latency for a blocked accept()/recv() on the server. "
                "Analogous to exp42's HPX idle-backoff caveat, but located in the kernel/OS "
                "scheduler. IQR-significance is the primary guard against over-reading."),
            "non_transferable_os": (
                "AF_UNIX connect/accept cost profiles differ by OS (Darwin/macOS vs Linux). "
                "These are OS-local observations and are non-transferable."),
            "vs_exp42_p2": (
                "exp42 P2's child runs a live HPX Runtime + worker threads contending with "
                "its accept thread; exp43 EP1's child has no HPX workers. Do NOT subtract "
                "EP1 from P2 to get 'Runtime dispatch cost' -- EP1 is a runtime-less lower "
                "reference only."),
            "distributed_fabric_fence": (
                "exp43 does NOT inform the distributed-fabric/transport question directly. "
                "One-shot local AF_UNIX is not a persistent inter-node parcelport."),
        },
        "paths": {
            "EP0": "Endpoint(); connect(self.metadata()); conn.ping(nonce)  "
                   "[same pid -> registry/inline, no socket, no Runtime, HPX inactive]",
            "EP1": "child Endpoint(transport=True) ONLY; connect(child_meta); "
                   "conn.ping(nonce)  [pid != getpid -> one-shot dial-per-call AF_UNIX, "
                   "no Runtime, HPX inactive both sides]",
            "EPraw": "child plain socket.AF_UNIX echo server; per call connect + 39B "
                     "request + 16B fixed reply + close  [same-shape PYTHON control, no "
                     "rayx, no HPX; NOT a raw OS floor / NOT a lower bound on EP1]",
        },
        "config": {
            "ping_nonce": PING_NONCE,
            "correctness_nonces": CORRECTNESS_NONCES,
            "reps": reps, "warmup": warmup,
            "include_raw": include_raw,
            "raw_req_len": RAW_REQ_LEN, "raw_resp_len": RAW_RESP_LEN,
            "sequential_single_in_flight": True, "gc_disabled_in_loop": True,
            "hpx_threads_sweep": "none (no HPX starts in any exp43 path)",
        },
        "controls": {
            "parent_hpx_active": parent_hpx_active,
            "child_hpx_active": child_hpx_active,
            "child_runtime_created": child_runtime_created,
            "ep1_child_clean_exit": bool(ep1_clean),
            "epraw_child_clean_exit": (None if raw_clean is None else bool(raw_clean)),
            "runtime_created": False,
        },
        "machine": {
            "platform": platform.platform(),
            "machine_cpu_count": os.cpu_count(),
            "sock_dir": _SOCK_DIR,
            "raw_sock_dir": _RAW_DIR,
        },
        "floors": {"empty_loop_floor_ns": empty_floor},
        "measurements": measurements,
        "deltas": deltas,
        "correctness_failures": correctness_fail,
        "control_failures": control_fail,
        "overall_structural_pass": bool(structural_pass),
    }

    with open(out_path, "w") as f:
        json.dump(aggregate, f, indent=2)
        f.write("\n")

    # Console summary.
    print(f"exp43 endpoint-ping-floor: wrote {out_path}")
    print(f"  empty_loop_floor_ns={empty_floor}  reps={reps} warmup={warmup}  "
          f"include_raw={include_raw}")
    print("  [one-shot dial-per-call; deltas judged vs pooled IQR, observation-only]")
    for r in measurements:
        print(f"    {r['path']:>6}  median={r['median_ns']:>10} ns  "
              f"iqr={r['iqr_ns']:>9}  below_res={r['below_resolution']}")
    for name, d in deltas.items():
        print(f"    {name:>16} = {d['delta_ns']:>10} ns  iqr<={d['pooled_iqr_ns']:>9}  "
              f"-> {d['interpretation_status']}")
    print(f"  controls: parent_hpx={parent_hpx_active} child_hpx={child_hpx_active} "
          f"child_runtime={child_runtime_created} "
          f"ep1_clean={ep1_clean} raw_clean={raw_clean}")
    print(f"  STRUCTURAL: {'PASS' if structural_pass else 'FAIL'} "
          f"(correctness_failures={len(correctness_fail)}, "
          f"control_failures={len(control_fail)})")
    if correctness_fail:
        for m in correctness_fail:
            print(f"    CORRECTNESS: {m}")
    if control_fail:
        for m in control_fail:
            print(f"    CONTROL: {m}")
    if not structural_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
