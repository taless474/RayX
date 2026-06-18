#!/usr/bin/env python3
"""exp31 -- runtime control-plane-under-load probe (observation-only).

QUESTION (not a benchmark, not a Ray comparison, not a CPU-overlap study):
when every HPX worker is busy running CPU-bound Async ``busy_sum`` work, do the
RayX ``rayx.runtime`` *control-plane* operations that cross
``hpx::run_as_hpx_thread`` stay bounded, or do they balloon because lane
consumers, control hops, and op bodies all share the one default HPX pool?

Two classes of control op, by their exposure to pool saturation:

  * HPX-hop-exposed (need a worker slot on the default pool to make progress):
      - ``Runtime.submit_operation``  (enqueue/admission)
      - ``Runtime.lane_stats``
      - ``Runtime.create_actor``
      - ``Runtime.release_actor``
  * Hop-free NEGATIVE CONTROL (signalled from the Python thread via the atomic
    cancel token, NO run_as_hpx_thread hop -- see runtime_cancel.hpp):
      - ``RuntimeFuture.cancel``

We measure each op's *Python-call wall latency* on an IDLE runtime and again
while the data plane is SATURATED, in the SAME process/runtime (same pool, only
the load differs), and report idle-vs-saturated p50/p90/p99 and the saturation
ratio. The hop-free ``cancel`` should stay ~flat regardless of load; if it does
and the hop ops balloon, the inflation is HPX scheduling latency, not Python.

INTERPRETATION (this is a DECISION GATE, not a verdict about HPX performance):
  * hop ops bounded + cancel flat  -> named pools / priorities NOT motivated.
  * hop ops balloon + cancel flat  -> next slice tests HPX control-hop PRIORITY
                                      before named pools.
Named pools / resource partitioning stays deferred either way.

All timings are observation-only and machine-specific. The checkpoint yield
cadence (busy_sum yields every STRIDE=8192 iterations) is a confound: it sets
how often a queued control hop can be scheduled, so results are specific to that
fixed stride. ``busy_sum`` bodies are native C++ and DO NOT hold the GIL during
the loop, so the Python thread issuing control calls is not GIL-blocked by load.

No JSONL / corpus artifact is written; only a compact summary is printed.

Because the HPX runtime is a process-global singleton (one active Runtime per
process), each cell runs in its own pristine subprocess; the parent stays
HPX-free and only aggregates.

Usage:
    python experiments/31_runtime_control_plane_under_load/run_runtime_control_plane_under_load.py --quick
    python experiments/31_runtime_control_plane_under_load/run_runtime_control_plane_under_load.py --full

Exit code: 0 == structural gates passed (or cleanly skipped if _rayx is
unavailable), 1 == a structural gate failed.
"""
import argparse
import json
import math
import os
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

# busy_sum checkpoint stride (mirrors BUSY_SUM_STRIDE in runtime_ops.hpp); used
# only to report the derived checkpoint_count of the load op (a confound note),
# never to change behavior.
BUSY_SUM_STRIDE = 8192
CELL_PREFIX = "CELL_JSON "


def _pct(xs, q):
    """Nearest-rank percentile (q in [0,1]) over a list of floats. xs non-empty."""
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    idx = int(math.ceil(q * len(s))) - 1
    idx = max(0, min(len(s) - 1, idx))
    return s[idx]


def _summ(xs):
    """Compact summary stats (ms) for a latency sample."""
    return {
        "n": len(xs),
        "min": min(xs),
        "p50": _pct(xs, 0.50),
        "p90": _pct(xs, 0.90),
        "p99": _pct(xs, 0.99),
        "max": max(xs),
    }


def _wait_until(pred, timeout_s, where):
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        if pred():
            return True
        time.sleep(0.001)
    return False


# --------------------------------------------------------------------------- #
# Cell body (runs inside a pristine subprocess; touches HPX)                   #
# --------------------------------------------------------------------------- #
def _measure_controls(rt, RuntimeFuture, ActorHandle, reps, warmup):
    """Measure control-op call latency (ms). Returns (metrics, controls_valid,
    pending_futures, actor_prefix_ok). submit/cancel create throwaway futures
    that may be queued (un-retirable until load drains), so they are returned in
    `pending` for the caller to retire at teardown."""
    submit_lat, stats_lat, cancel_lat, create_lat, release_lat = [], [], [], [], []
    pending = []
    valid = True
    actor_prefix_ok = True

    # ---- warmup (untimed) ----
    for _ in range(warmup):
        f = rt.submit_operation("square", 7)
        pending.append(f)
        rt.lane_stats()
        g = rt.submit_operation("square", 7)
        g.cancel()
        pending.append(g)
        a = rt.create_actor("counter", 0)
        rt.release_actor(a)

    # ---- submit / admission latency ----
    for _ in range(reps):
        t0 = time.perf_counter()
        f = rt.submit_operation("square", 7)
        t1 = time.perf_counter()
        submit_lat.append((t1 - t0) * 1e3)
        if not isinstance(f, RuntimeFuture):
            valid = False
        f.cancel()  # hop-free; keep the lane from running the probe op
        pending.append(f)

    # ---- lane_stats latency ----
    for _ in range(reps):
        t0 = time.perf_counter()
        ls = rt.lane_stats()
        t1 = time.perf_counter()
        stats_lat.append((t1 - t0) * 1e3)
        if not (isinstance(ls, list) and ls
                and all(set(d.keys()) >= {"actor_id", "queue_depth", "active"}
                        for d in ls)):
            valid = False

    # ---- cancel latency (hop-free negative control) ----
    for _ in range(reps):
        f = rt.submit_operation("square", 7)
        t0 = time.perf_counter()
        r = f.cancel()
        t1 = time.perf_counter()
        cancel_lat.append((t1 - t0) * 1e3)
        if not isinstance(r, bool):
            valid = False
        pending.append(f)

    # ---- create_actor / release_actor latency ----
    for _ in range(reps):
        t0 = time.perf_counter()
        a = rt.create_actor("counter", 0)
        t1 = time.perf_counter()
        create_lat.append((t1 - t0) * 1e3)
        if not isinstance(a, ActorHandle):
            valid = False
        if not a.actor_id.startswith("rt-act-"):
            actor_prefix_ok = False
        t2 = time.perf_counter()
        ret = rt.release_actor(a)
        t3 = time.perf_counter()
        release_lat.append((t3 - t2) * 1e3)
        if ret is not None:
            valid = False

    metrics = {
        "submit": _summ(submit_lat),
        "lane_stats": _summ(stats_lat),
        "cancel": _summ(cancel_lat),
        "create_actor": _summ(create_lat),
        "release_actor": _summ(release_lat),
    }
    return metrics, valid, pending, actor_prefix_ok


def run_cell(cfg):
    """Run one (hpx_threads, num_lanes, load_lanes, work_n) cell. Returns a dict."""
    result = {"config": cfg, "skipped": False, "error": None,
              "gates": {}, "idle": {}, "saturated": {}}
    try:
        from rayx.runtime import Runtime, RuntimeFuture, ActorHandle
    except Exception as e:  # _rayx not built -> clean skip
        result["skipped"] = True
        result["error"] = f"{type(e).__name__}: {e}"
        return result

    ht = cfg["hpx_threads"]
    lanes = cfg["num_lanes"]
    load = cfg["load_lanes"]
    work_n = cfg["work_n"]
    reps = cfg["reps"]
    warmup = cfg["warmup"]
    result["checkpoint_count_load"] = (work_n + BUSY_SUM_STRIDE - 1) // BUSY_SUM_STRIDE

    # value-correctness reference (a small COMPLETED busy_sum, not the load)
    vn = 100_000
    expected = (vn * (vn - 1) // 2) % (2 ** 31)

    rt = Runtime(num_lanes=lanes, hpx_threads=ht)
    pending = []
    load_futs = []
    try:
        # ---- structural: busy_sum value + lane-id prefix (idle) ----
        vres = rt.submit_operation("busy_sum", vn).result()
        result["gates"]["busy_sum_value_ok"] = (
            vres.status == "completed" and vres.value == expected)
        ls0 = rt.lane_stats()
        result["gates"]["lane_ids_valid"] = (
            len(ls0) == lanes
            and all(d["actor_id"].startswith("rt-hpx-") for d in ls0))

        # ---- IDLE control latency ----
        idle_m, idle_ok, idle_pend, idle_actor_ok = _measure_controls(
            rt, RuntimeFuture, ActorHandle, reps, warmup)
        result["idle"] = idle_m
        pending += idle_pend

        # ---- establish saturation: long oversized busy_sum on `load` lanes ----
        load_futs = [rt.submit_operation("busy_sum", work_n) for _ in range(load)]
        saturated = _wait_until(
            lambda: sum(1 for d in rt.lane_stats() if d["active"]) >= load,
            10.0, "saturation")
        result["saturation_established"] = saturated
        # Structural gate: if the data plane never saturated, the saturated
        # timings are not a valid idle-vs-load comparison, so fail the cell loudly
        # rather than report meaningless numbers.
        result["gates"]["saturation_established"] = bool(saturated)
        result["active_lanes_at_probe"] = sum(
            1 for d in rt.lane_stats() if d["active"])

        # ---- SATURATED control latency ----
        sat_m, sat_ok, sat_pend, sat_actor_ok = _measure_controls(
            rt, RuntimeFuture, ActorHandle, reps, warmup)
        result["saturated"] = sat_m
        pending += sat_pend

        result["gates"]["controls_valid"] = bool(idle_ok and sat_ok)
        result["gates"]["actor_prefix_ok"] = bool(idle_actor_ok and sat_actor_ok)

        # ---- teardown: cancel load (running-cancel), then retire everything ----
        for f in load_futs:
            f.cancel()
        statuses = []
        for f in load_futs + pending:
            try:
                statuses.append(f.result().status)
            except Exception:
                statuses.append("retire_error")
        result["gates"]["load_retired_clean"] = all(
            s in ("cancelled", "completed") for s in statuses)
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
        result["gates"]["cell_completed"] = False
    finally:
        rt.shutdown()
    result["gates"].setdefault("cell_completed", True)
    return result


# --------------------------------------------------------------------------- #
# Orchestrator (parent; HPX-free)                                             #
# --------------------------------------------------------------------------- #
def get_cells(mode):
    cpu = os.cpu_count() or 1
    if mode == "quick":
        # single small cell: 2x oversubscription so load > hpx_threads.
        ht = min(2, cpu)
        return [{"hpx_threads": ht, "num_lanes": 2 * ht, "load_lanes": 2 * ht,
                 "work_n": 2_000_000_000, "reps": 25, "warmup": 5}]
    # full: hpx_threads in {1,2,4} (<= cores) x load multiplier {1,2,4}
    cells = []
    seen = set()
    for ht in sorted({1, min(2, cpu), min(4, cpu)}):
        for mult in (1, 2, 4):
            lanes = mult * ht
            key = (ht, lanes)
            if key in seen:
                continue
            seen.add(key)
            cells.append({"hpx_threads": ht, "num_lanes": lanes,
                          "load_lanes": lanes, "work_n": 4_000_000_000,
                          "reps": 80, "warmup": 10})
    return cells


def _run_cell_subprocess(cfg):
    proc = subprocess.run(
        [sys.executable, os.path.abspath(__file__), "--_cell", json.dumps(cfg)],
        capture_output=True, text=True, timeout=300)
    for line in proc.stdout.splitlines():
        if line.startswith(CELL_PREFIX):
            return json.loads(line[len(CELL_PREFIX):])
    return {"config": cfg, "skipped": False, "error": "no cell result",
            "stderr": proc.stderr[-2000:], "gates": {"cell_completed": False}}


def _fmt_cell(res):
    cfg = res["config"]
    head = (f"cell hpx_threads={cfg['hpx_threads']} num_lanes={cfg['num_lanes']} "
            f"load_lanes={cfg['load_lanes']} reps={cfg['reps']} "
            f"work_n={cfg['work_n']} checkpoint_count_load="
            f"{res.get('checkpoint_count_load', '?')}")
    lines = [head]
    if res.get("error"):
        lines.append(f"  ERROR: {res['error']}")
    sat = (f"  saturation_established={res.get('saturation_established')} "
           f"active_lanes_at_probe={res.get('active_lanes_at_probe')}")
    lines.append(sat)
    idle, satm = res.get("idle", {}), res.get("saturated", {})
    lines.append(f"  {'op':<14}{'idle_p50':>10}{'sat_p50':>10}"
                 f"{'ratio_p50':>11}{'sat_p99':>10}  (ms)")
    for op in ("submit", "lane_stats", "cancel", "create_actor", "release_actor"):
        if op in idle and op in satm:
            ip, sp = idle[op]["p50"], satm[op]["p50"]
            ratio = (sp / ip) if ip > 0 else float("inf")
            tag = "  <- hop-free control" if op == "cancel" else ""
            lines.append(f"  {op:<14}{ip:>10.4f}{sp:>10.4f}"
                         f"{ratio:>10.2f}x{satm[op]['p99']:>10.4f}{tag}")
    return "\n".join(lines)


def _interpret(results):
    """Tentative, hedged reading. HOP ops vs the hop-free cancel control.

    A control plane is judged by ABSOLUTE saturated latency, not by ratio: a
    large p50 ratio over a microsecond-scale baseline is scheduling jitter, not
    a balloon. So "balloon" requires BOTH a meaningful absolute saturated p99
    (>= ABS_GATE_MS) AND a real inflation (ratio >= RATIO_GATE). The hop-free
    cancel is the negative control: it is "immune" if it never reaches a
    meaningful absolute latency under load (as a hop-free op should not)."""
    hop_ops = ("submit", "lane_stats", "create_actor", "release_actor")
    RATIO_GATE = 3.0    # supporting inflation factor (not a trigger on its own)
    ABS_GATE_MS = 5.0   # meaningful absolute saturated p99 (the real trigger)
    balloon = False
    worst_hop_p99 = 0.0
    cancel_immune = True
    for res in results:
        idle, satm = res.get("idle", {}), res.get("saturated", {})
        for op in hop_ops:
            if op in idle and op in satm:
                ip, sp99 = idle[op]["p50"], satm[op]["p99"]
                worst_hop_p99 = max(worst_hop_p99, sp99)
                ratio = (satm[op]["p50"] / ip) if ip > 0 else float("inf")
                if sp99 >= ABS_GATE_MS and ratio >= RATIO_GATE:
                    balloon = True
        if "cancel" in satm and satm["cancel"]["p99"] >= ABS_GATE_MS:
            cancel_immune = False
    tail = (f" (worst HPX-hop saturated p99 = {worst_hop_p99:.4f} ms vs "
            f"{ABS_GATE_MS:.1f} ms gate)")
    if balloon and cancel_immune:
        return ("READING (observation-only, this run/machine): HPX-hop control "
                "ops reach a meaningful absolute latency under load while the "
                "hop-free cancel stays bounded -> the inflation is HPX "
                "scheduling latency. NEXT SLICE: test HPX control-hop PRIORITY "
                "before named pools." + tail)
    if balloon and not cancel_immune:
        return ("READING (observation-only): both HPX-hop ops AND the hop-free "
                "cancel reached meaningful latency -> cannot attribute to HPX "
                "scheduling alone (Python/GIL/machine noise suspected). Treat "
                "as INCONCLUSIVE; re-run on a quiescent machine." + tail)
    return ("READING (observation-only, this run/machine): HPX-hop control ops "
            "stayed bounded (well under the meaningful-latency gate) and the "
            "hop-free cancel stayed flat -> named pools / priorities are NOT "
            "motivated by this evidence. Expected if busy_sum's cooperative "
            "checkpoint yields keep control hops schedulable. Decision gate "
            "result: STOP (do not build priorities or pools on this evidence)."
            + tail)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="single small cell for fast local validation")
    ap.add_argument("--full", action="store_true",
                    help="fuller hpx_threads x load sweep")
    ap.add_argument("--_cell", default=None,
                    help=argparse.SUPPRESS)  # internal: run one cell, print JSON
    args = ap.parse_args()

    if args._cell is not None:
        res = run_cell(json.loads(args._cell))
        print(CELL_PREFIX + json.dumps(res))
        return 0

    mode = "full" if args.full else "quick"
    cells = get_cells(mode)
    print(f"exp31 runtime control-plane-under-load probe ({mode} mode, "
          f"{len(cells)} cell(s), cpu_count={os.cpu_count()})")
    print("observation-only / machine-specific / no Ray comparison / "
          "no perf/sizing claim\n")

    results = [_run_cell_subprocess(c) for c in cells]

    if all(r.get("skipped") for r in results):
        print("SKIP: _rayx extension unavailable; nothing measured.")
        return 0

    all_gates_ok = True
    for res in results:
        print(_fmt_cell(res))
        gates = res.get("gates", {})
        gate_str = "  gates: " + ", ".join(
            f"{k}={v}" for k, v in sorted(gates.items()))
        print(gate_str)
        if res.get("stderr"):
            print("  stderr:\n" + res["stderr"])
        if not all(gates.values()):
            all_gates_ok = False
        print()

    print(_interpret(results))
    print()
    if all_gates_ok:
        print("STRUCTURAL GATES: PASS")
        return 0
    print("STRUCTURAL GATES: FAIL")
    return 1


if __name__ == "__main__":
    sys.exit(main())
