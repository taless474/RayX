#!/usr/bin/env python3
"""exp42: local observation-only boundary/path characterization for an existing
composed Runtime op, across three REAL API paths into the same fixed op body.

This experiment does NOT measure HPX, scheduling, overlap, parallelism, transport,
or performance superiority. It characterizes *local path differences* between three
ways an existing registered op (`fanout_sum(n, parts=4)`) is reached, using ONLY the
real public/private API behavior -- no faked sockets, no spoofed pid, no private
lower-level transport helpers.

Paths (corrected decomposition -- see bridge_boundary_cost.md "Same-pid short-circuit"):

  P0  direct Runtime submit:
        rt.submit_operation("fanout_sum", n, 4).result().value
      Python/pybind submit -> lane enqueue -> run_as_hpx_thread -> op body ->
      future.get -> RuntimeFuture retirement. ONE Runtime active.

  P1  same-process in-process bridge dispatch (NO socket):
        ep = Endpoint(transport=True)              # in the SAME process as rt
        conn = connect(ep.metadata())              # meta["pid"] == os.getpid()
        conn._call_op("fanout_sum", n, 4)
      Because the peer pid equals this pid, connect() resolves through the
      in-process registry (rayx/endpoint/__init__.py connect(): the
      `meta["pid"] == os.getpid()` branch -> _endpoint_connect_local), and _call_op
      dispatches through the SAME-PROCESS drain-gated bridge into the SAME live
      Runtime. There is NO AF_UNIX socket, NO accept thread, NO frame encode/decode,
      NO struct.unpack here. ONE Runtime active.

  P2  cross-process local AF_UNIX endpoint bridge:
        child process owns its OWN Runtime + Endpoint(transport=True);
        parent connect(child_meta) -> pid != getpid() -> AF_UNIX remote path;
        conn._call_op("fanout_sum", n, 4)
      pybind _call_op -> frame encode -> AF_UNIX round-trip -> child accept thread ->
      child drain gate -> bridge dispatch into the CHILD Runtime -> op body ->
      response frame -> decode. TWO Runtimes active (parent's stays up).

Interpretation (corrected, observation-only):
  * P1 - P0 is the IN-PROCESS bridge-dispatch path difference (bridge arg
    marshalling + drain gate + bridge_dispatch vs direct submit/RuntimeFuture
    retirement). It is NOT socket cost.
  * P2 - P1 bundles AF_UNIX delivery + accept-thread hop + response framing +
    second-process scheduling + a second HPX runtime on the same machine. It does
    NOT isolate transport.

Diagnostics (self-consistency only, NOT quality gates):
  * empty perf_counter loop floor + per-path `square` floor (a near-zero-work op);
  * flatness gate: bridge fixed-path cost should be roughly independent of n, so if
    P1-P0 systematically scales with n the measurement is contaminated -> withhold
    amortization interpretation;
  * below-resolution rule: deltas at/below the timer/loop/square floor are not
    reported as meaningful signal.

NON-CLAIMS: no same-process socket, no isolated socket/transport cost, no HPX
mechanism/scheduling result, no HPX value, no speedup/throughput/latency claim, no
Ray comparison, no transport/fabric/parcelport/AGAS/multi-node, no public
endpoint-call API. Sequential single-in-flight only; laptop timing is NOT evidence.

Run (laptop -- generates aggregate.json beside this file):
  PYTHONPATH=python/src python \
    experiments/42_endpoint_bridge_boundary_cost/run_bridge_boundary_cost.py --smoke
"""
import argparse
import gc
import json
import multiprocessing as mp
import os
import platform
import sys
import time

PARTS = 4              # fixed composed-op partition width (NOT a perf knob)
SQUARE_X = 7           # near-zero-work floor argument
_SOCK_DIR = os.environ.setdefault(
    "RAYX_ENDPOINT_SOCK_DIR", "/tmp/rayx-ep-exp42")
_CTX = mp.get_context("spawn")  # never fork a process that already started HPX


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
    """Time `call()` reps+warmup times (warmup discarded), single-in-flight,
    GC off inside the measured loop. Returns (samples_ns, last_value)."""
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


# --- child server process (top-level so spawn can import it) -----------------


def _child_server(conn, src, sock_dir, hpx_threads):
    """P2 server: a live Runtime + transport Endpoint sharing the process. Sends its
    bootstrap metadata, then serves bridge calls (via the endpoint accept thread)
    until told to close. The child Runtime is where P2's op body actually runs."""
    try:
        os.environ["RAYX_ENDPOINT_SOCK_DIR"] = sock_dir
        if src not in sys.path:
            sys.path.insert(0, src)
        from rayx.runtime import Runtime
        from rayx.endpoint import Endpoint, shutdown as _sd
        rt = Runtime(hpx_threads=hpx_threads)
        ep = Endpoint(transport=True)
        conn.send(ep.metadata())
        conn.recv()  # block until parent says "close"
        ep.close()
        rt.shutdown()
        _sd()
        conn.send("closed")
    except Exception as exc:  # noqa: BLE001
        conn.send(("error", repr(exc)))
    finally:
        conn.close()


def _spawn_child(hpx_threads, src):
    parent, child = _CTX.Pipe()
    p = _CTX.Process(target=_child_server,
                     args=(child, src, _SOCK_DIR, hpx_threads))
    p.start()
    meta = parent.recv()
    if not isinstance(meta, dict):
        p.join(timeout=5)
        sys.exit(f"FAIL child server did not start: {meta!r}")
    return p, parent, meta


def _teardown_child(p, parent):
    parent.send("close")
    ack = parent.recv()
    p.join(timeout=10)
    if p.is_alive() or ack != "closed":
        sys.exit(f"FAIL child teardown (ack={ack!r}, alive={p.is_alive()})")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ns", default="0,100,1000,10000,100000",
                    help="comma-separated fanout_sum n sweep")
    ap.add_argument("--hpx-threads", default="1,4",
                    help="comma-separated HPX worker-thread sweep")
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--out", default=None,
                    help="aggregate.json path (default: beside this script)")
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop sweep (structural only; overrides grid)")
    args = ap.parse_args()

    if args.smoke:
        ns = [0, 100, 1000]
        threads = [1, 2]
        reps, warmup = 30, 8
    else:
        ns = [int(x) for x in args.ns.split(",")]
        threads = [int(x) for x in args.hpx_threads.split(",")]
        reps, warmup = args.reps, args.warmup

    here = os.path.dirname(os.path.abspath(__file__))
    out_path = args.out or os.path.join(here, "aggregate.json")
    src = os.path.join(os.path.dirname(os.path.dirname(here)), "python", "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    from rayx.runtime import Runtime
    from rayx.endpoint import Endpoint, connect, shutdown

    empty_floor = _empty_loop_floor_ns(max(reps, 50))

    measurements = []   # one row per (path, op, n, hpx_threads)
    square_floor = {}   # (path, hpx_threads) -> median_ns
    correctness_fail = []

    def record(path, op, n, T, samples, value, ref_value, runtimes_active):
        s = _summ(samples)
        equal_ok = (value == ref_value)
        if not equal_ok:
            correctness_fail.append(
                f"{path} {op} n={n} T={T}: {value} != ref {ref_value}")
        floor = max(empty_floor, square_floor.get((path, T), 0))
        row = {
            "path": path, "op": op, "n": n, "parts": PARTS,
            "reps": reps, "warmup": warmup, "hpx_threads": T,
            "runtimes_active": runtimes_active,
            "total_workers": runtimes_active * T,
            "machine_cpu_count": os.cpu_count(),
            "median_ns": s["median_ns"], "p25_ns": s["p25_ns"],
            "p75_ns": s["p75_ns"], "iqr_ns": s["iqr_ns"],
            "value": value, "result_equal_ok": equal_ok,
            "below_resolution": bool(s["median_ns"] <= floor),
        }
        measurements.append(row)
        return row

    for T in threads:
        # ---- Parent process owns ONE Runtime (serves P0 + P1) ----
        with Runtime(hpx_threads=T) as rt:
            # Reference values from the direct path (the correctness oracle).
            ref_fanout = {n: rt.submit_operation("fanout_sum", n, PARTS)
                          .result().value for n in ns}
            ref_square = rt.submit_operation("square", SQUARE_X).result().value

            # --- P0: direct Runtime submit ---
            sq, sqv = _measure(
                lambda: rt.submit_operation("square", SQUARE_X).result().value,
                reps, warmup)
            square_floor[("P0", T)] = int(_pct(sq, 50))
            record("P0", "square", SQUARE_X, T, sq, sqv, ref_square, 1)
            for n in ns:
                samp, val = _measure(
                    lambda n=n: rt.submit_operation("fanout_sum", n, PARTS)
                    .result().value, reps, warmup)
                record("P0", "fanout_sum", n, T, samp, val, ref_fanout[n], 1)

            # --- P1: same-process in-process bridge dispatch (NO socket) ---
            with Endpoint(transport=True) as ep:
                conn = connect(ep.metadata())   # same pid -> registry, no socket
                try:
                    sq, sqv = _measure(
                        lambda: conn._call_op("square", SQUARE_X), reps, warmup)
                    square_floor[("P1", T)] = int(_pct(sq, 50))
                    record("P1", "square", SQUARE_X, T, sq, sqv, ref_square, 1)
                    for n in ns:
                        samp, val = _measure(
                            lambda n=n: conn._call_op("fanout_sum", n, PARTS),
                            reps, warmup)
                        record("P1", "fanout_sum", n, T, samp, val,
                               ref_fanout[n], 1)
                finally:
                    conn.close()

            # --- P2: cross-process AF_UNIX bridge into a CHILD Runtime ---
            # The parent Runtime `rt` stays alive here -> runtimes_active == 2.
            p, pipe, child_meta = _spawn_child(T, src)
            try:
                if child_meta["pid"] == os.getpid():        # defensive
                    sys.exit("FAIL child shares parent pid (not a separate proc)")
                if child_meta["transport_kind"] != "unix":
                    sys.exit("FAIL child did not advertise AF_UNIX transport")
                conn = connect(child_meta)                  # pid != getpid -> AF_UNIX
                try:
                    sq, sqv = _measure(
                        lambda: conn._call_op("square", SQUARE_X), reps, warmup)
                    square_floor[("P2", T)] = int(_pct(sq, 50))
                    record("P2", "square", SQUARE_X, T, sq, sqv, ref_square, 2)
                    for n in ns:
                        samp, val = _measure(
                            lambda n=n: conn._call_op("fanout_sum", n, PARTS),
                            reps, warmup)
                        record("P2", "fanout_sum", n, T, samp, val,
                               ref_fanout[n], 2)
                finally:
                    conn.close()
            finally:
                _teardown_child(p, pipe)
        shutdown()  # idempotent endpoint cleanup (HPX-free, never stops HPX)

    # ---- Deltas + IQR-significance + flatness diagnostics (per hpx_threads) ----
    def row_for(path, op, n, T):
        for r in measurements:
            if (r["path"], r["op"], r["n"], r["hpx_threads"]) == (path, op, n, T):
                return r
        return None

    def med(path, op, n, T):
        r = row_for(path, op, n, T)
        return r["median_ns"] if r else None

    def _rank(xs):
        order = sorted(range(len(xs)), key=lambda i: xs[i])
        ranks = [0] * len(xs)
        for r, i in enumerate(order):
            ranks[i] = r
        return ranks

    def _spearman(xs, ys):
        """Coarse Spearman rank correlation (small N, no ties handling)."""
        rx, ry = _rank(xs), _rank(ys)
        n = len(xs)
        mx, my = sum(rx) / n, sum(ry) / n
        num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
        dx = sum((rx[i] - mx) ** 2 for i in range(n)) ** 0.5
        dy = sum((ry[i] - my) ** 2 for i in range(n)) ** 0.5
        return num / (dx * dy) if dx > 0 and dy > 0 else 0.0

    def classify_delta(med_a, iqr_a, med_b, iqr_b, contaminated=False,
                       confounded=False):
        """Classify median(b)-median(a) against the timer floor and run-to-run
        jitter. `pooled_iqr` is the conservative max of the two paths' IQRs: a delta
        smaller than that sits inside normal run-to-run variation and is NOT reported
        as signal. `confounded=True` (P2) flags an end-to-end path whose above-jitter
        deltas are still NOT transport/socket cost. `contaminated=True` (flatness
        failed) withholds interpretation entirely."""
        delta = med_b - med_a
        pooled_iqr = max(iqr_a, iqr_b)
        below_resolution = abs(delta) <= empty_floor
        within_jitter = abs(delta) <= pooled_iqr
        meaningful = not below_resolution and not within_jitter
        if contaminated:
            status = "withheld"
        elif below_resolution:
            status = "below_resolution"
        elif within_jitter:
            status = "within_jitter"
        elif confounded:
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

    # PRIMARY boundary carrier: `square` (near-zero-work; no internal HPX async/when_all
    # to add common-mode op cost). These are the cleanest pure-boundary deltas.
    square_boundary = []
    for T in threads:
        r0 = row_for("P0", "square", SQUARE_X, T)
        r1 = row_for("P1", "square", SQUARE_X, T)
        r2 = row_for("P2", "square", SQUARE_X, T)
        square_boundary.append({
            "hpx_threads": T,
            "P1_minus_P0": classify_delta(r0["median_ns"], r0["iqr_ns"],
                                          r1["median_ns"], r1["iqr_ns"]),
            "P2_minus_P1": classify_delta(r1["median_ns"], r1["iqr_ns"],
                                          r2["median_ns"], r2["iqr_ns"],
                                          confounded=True),
        })

    # Flatness (coarse self-consistency on the COMPOSED op): P1-P0 for fanout_sum should
    # not scale with n. Coarse detector (do not over-trust): flag contamination when the
    # trend is monotone-up OR strongly rank-correlated with n AND the spread clears one
    # minimal in-process bridged call.
    flatness = []
    flat_contaminated = {}
    for T in threads:
        spread_floor = max(empty_floor, square_floor.get(("P1", T), 0))
        d1_by_n = [med("P1", "fanout_sum", n, T) - med("P0", "fanout_sum", n, T)
                   for n in ns]
        spread = max(d1_by_n) - min(d1_by_n)
        monotone_up = all(d1_by_n[i] <= d1_by_n[i + 1]
                          for i in range(len(d1_by_n) - 1)) and spread > 0
        rho = _spearman(ns, d1_by_n)
        contaminated = (monotone_up or rho >= 0.9) and spread > spread_floor
        flat_contaminated[T] = contaminated
        flatness.append({
            "hpx_threads": T,
            "P1_minus_P0_by_n": [int(x) for x in d1_by_n],
            "n_sweep": ns,
            "spread_ns": int(spread),
            "spread_floor_ns": int(spread_floor),
            "monotone_increasing_with_n": bool(monotone_up),
            "spearman_rho_n_vs_delta": round(rho, 3),
            "detector_note": ("COARSE self-consistency check, not a strong test; "
                              "IQR-significance is the primary interpretation guard"),
            "flatness_ok": bool(not contaminated),
        })

    # COMPOSED-op deltas (fanout_sum): correctness carrier + amortization illustration
    # only. Each is IQR-classified and withheld when its thread's flatness failed.
    deltas = []
    for T in threads:
        cont = flat_contaminated[T]
        for n in ns:
            r0 = row_for("P0", "fanout_sum", n, T)
            r1 = row_for("P1", "fanout_sum", n, T)
            r2 = row_for("P2", "fanout_sum", n, T)
            deltas.append({
                "hpx_threads": T, "n": n,
                "P1_minus_P0": classify_delta(r0["median_ns"], r0["iqr_ns"],
                                              r1["median_ns"], r1["iqr_ns"],
                                              contaminated=cont),
                "P2_minus_P1": classify_delta(r1["median_ns"], r1["iqr_ns"],
                                              r2["median_ns"], r2["iqr_ns"],
                                              contaminated=cont, confounded=True),
            })

    flatness_ok = all(f["flatness_ok"] for f in flatness)
    # Is there ANY in-process boundary signal that clears both the timer floor and
    # run-to-run jitter? Lead with the clean square carrier.
    square_p1p0_meaningful = any(
        sb["P1_minus_P0"]["meaningful_above_jitter"] for sb in square_boundary)
    noise_floor_ok = square_p1p0_meaningful or any(
        d["P1_minus_P0"]["meaningful_above_jitter"] for d in deltas)
    # Amortization is ILLUSTRATION ONLY on the composed op, and only meaningful if a
    # fixed in-process boundary cost is itself distinguishable from jitter (square) and
    # the composed-op flatness is self-consistent.
    if not flatness_ok:
        amortization = "withheld"
    elif not square_p1p0_meaningful:
        amortization = "weakened"  # no fixed boundary cost above jitter to amortize
    else:
        amortization = "applied_illustration_only"

    structural_pass = (not correctness_fail
                       and all(r["result_equal_ok"] for r in measurements))

    aggregate = {
        "experiment": "exp42_endpoint_bridge_boundary_cost",
        "schema": "rayx-bridge-boundary-cost-1",
        "claim": ("characterizes local observation-only path differences between "
                  "direct Runtime submit (P0), same-process in-process bridge "
                  "dispatch (P1, no socket), and cross-process local AF_UNIX "
                  "endpoint bridge (P2). The PRIMARY pure-boundary timing carrier is "
                  "the near-zero-work `square` op; `fanout_sum` is the composed-op "
                  "correctness carrier and an amortization illustration only"),
        "primary_boundary_carrier": "square",
        "fanout_sum_role": ("composed-op correctness carrier + amortization "
                            "illustration only; NOT the primary boundary timing "
                            "carrier (its body runs hpx::async x parts + when_all, "
                            "common-mode across paths but extra op cost)"),
        "p2_interpretation": (
            "END-TO-END cross-process path observation only. P2 timing is heavily "
            "confounded by second-process scheduling, a second HPX Runtime, possible "
            "worker-pool oversubscription, the child accept thread, AF_UNIX delivery, "
            "and response framing. It is NOT transport cost and NOT socket cost."),
        "interpretation_guards": (
            "A delta is reported as signal only when it clears BOTH the empty-loop "
            "timer floor AND run-to-run jitter (pooled IQR of the two paths). Deltas "
            "within IQR are 'within_jitter' and are NOT interpreted; flatness is a "
            "coarse secondary check only."),
        "non_claims": [
            "no same-process socket (P1 is in-process registry dispatch, no AF_UNIX)",
            "no isolated socket-cost claim",
            "no isolated transport-cost claim (P2-P1 bundles many costs)",
            "no HPX mechanism / scheduling / design result",
            "no HPX value",
            "no speedup / throughput / latency claim",
            "no Ray comparison",
            "no transport / fabric / parcelport / AGAS / multi-node",
            "no public endpoint-call API",
            "no stable layer-constant claim: sequential single-in-flight calls may "
            "include HPX idle-worker wake-up/backoff jitter (see caveats)",
            "laptop timing is observation-only, NOT evidence",
        ],
        "caveats": {
            "hpx_idle_backoff": (
                "Each call enters HPX via hpx::run_as_hpx_thread just to ENQUEUE onto "
                "the persistent lane worker; sequential single-in-flight calls can let "
                "HPX workers idle between calls, so measured us-level path differences "
                "may include HPX idle-backoff / wake-up jitter. No HPX ini tuning is "
                "applied here -- timing is observation-only and is NOT a stable layer "
                "constant. IQR-significance is the primary guard against over-reading."),
            "fanout_carrier": (
                "fanout_sum adds common-mode internal HPX task spawn/join to every "
                "path; it cancels in the delta but makes the boundary signal less "
                "clean than the square carrier."),
        },
        "same_pid_short_circuit": (
            "P1 uses connect(self.metadata()); rayx/endpoint/__init__.py connect() "
            "takes the `meta['pid'] == os.getpid()` branch -> _endpoint_connect_local "
            "(in-process registry, no socket), and Connection._call_op dispatches "
            "through the same-process drain-gated bridge into the same Runtime. No "
            "pid is spoofed and no lower-level transport helper is called."),
        "paths": {
            "P0": "rt.submit_operation('fanout_sum', n, 4).result().value",
            "P1": "Endpoint(transport=True); connect(self.metadata()); "
                  "conn._call_op('fanout_sum', n, 4)  [same pid -> no socket]",
            "P2": "child Runtime+Endpoint(transport=True); connect(child_meta); "
                  "conn._call_op('fanout_sum', n, 4)  [pid != getpid -> AF_UNIX]",
        },
        "config": {
            "op": "fanout_sum", "parts": PARTS, "floor_op": "square",
            "n_sweep": ns, "hpx_threads_sweep": threads,
            "reps": reps, "warmup": warmup,
            "sequential_single_in_flight": True, "gc_disabled_in_loop": True,
        },
        "machine": {
            "platform": platform.platform(),
            "machine_cpu_count": os.cpu_count(),
            "bind_passed": False,
            "hpx_bind_note": ("RayX/Runtime passes only hpx_threads (--hpx:threads); "
                              "binding is HPX default and placement is unknown -> no "
                              "NUMA/socket attribution"),
            "sock_dir": _SOCK_DIR,
        },
        "floors": {
            "empty_loop_floor_ns": empty_floor,
            "square_floor_per_path": {f"{p}/T{t}": v
                                      for (p, t), v in sorted(square_floor.items())},
        },
        "measurements": measurements,
        "square_boundary_primary": square_boundary,
        "fanout_deltas": deltas,
        "diagnostics": {
            "square_p1_minus_p0_meaningful_above_jitter": bool(square_p1p0_meaningful),
            "flatness": flatness,
            "flatness_ok": flatness_ok,
            "noise_floor_ok": noise_floor_ok,
            "amortization_interpretation": amortization,
        },
        "correctness_failures": correctness_fail,
        "overall_structural_pass": bool(structural_pass),
    }

    with open(out_path, "w") as f:
        json.dump(aggregate, f, indent=2)
        f.write("\n")

    # Console summary.
    print(f"exp42 boundary-cost: wrote {out_path}")
    print(f"  empty_loop_floor_ns={empty_floor}  n_sweep={ns}  threads={threads}  "
          f"reps={reps} warmup={warmup}")
    print("  [PRIMARY carrier = square; deltas judged vs pooled IQR, not just timer]")
    for sb in square_boundary:
        T = sb["hpx_threads"]
        d1, d2 = sb["P1_minus_P0"], sb["P2_minus_P1"]
        print(f"  --- hpx_threads={T} (square boundary) ---")
        print(f"    P1-P0={d1['delta_ns']:>9} ns  iqr<={d1['pooled_iqr_ns']:>7}  "
              f"-> {d1['interpretation_status']}")
        print(f"    P2-P1={d2['delta_ns']:>9} ns  iqr<={d2['pooled_iqr_ns']:>7}  "
              f"-> {d2['interpretation_status']}")
    print("  [composed-op fanout_sum (correctness + amortization illustration only)]")
    for T in threads:
        print(f"  --- hpx_threads={T} ---")
        for n in ns:
            p0 = med("P0", "fanout_sum", n, T)
            p1 = med("P1", "fanout_sum", n, T)
            p2 = med("P2", "fanout_sum", n, T)
            print(f"    n={n:>7}  P0={p0:>10}  P1={p1:>10}  P2={p2:>10}  "
                  f"P1-P0={p1 - p0:>9}  P2-P1={p2 - p1:>9} ns")
    fok = "FLAT(coarse)" if flatness_ok else "NOT-FLAT(contaminated)"
    print(f"  square_P1-P0_meaningful_above_jitter={square_p1p0_meaningful}  "
          f"flatness={fok}  noise_floor_ok={noise_floor_ok}  "
          f"amortization={amortization}")
    print(f"  STRUCTURAL: {'PASS' if structural_pass else 'FAIL'} "
          f"(result_equal across all paths; correctness_failures="
          f"{len(correctness_fail)})")
    if not structural_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
