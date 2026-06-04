#!/usr/bin/env python3
"""Experiment 17 runner: lane backlog observability via Engine.lane_stats().

Uses the rayx observability snapshot `Engine.lane_stats()` -- one
`{actor_id, queue_depth, active}` row per ServiceLane -- to characterize live
lane backlog and active state under offered load. The question:

> Under a burst that overfills the lanes, does `lane_stats()` honestly show the
> backlog distributed round-robin and draining FIFO, and what does it show for
> queued requests that are cancelled before they start?

Observability ONLY. This runner adds NO new RayX API, does NOT change
ServiceLane semantics, the benchmark JSONL schema, or the analyzer, and does NOT
touch HpxLane / experiment 16. `lane_stats()` is a SNAPSHOT and is NON-consuming
(it touches no future), so the lanes drain on their own while we sample; we
retire the futures afterwards only to confirm the per-request outcomes.

Backlog model (per cell): warm up, then burst-submit N=lanes*8 sleep requests
WITHOUT retiring -> a deep FIFO backlog builds on each round-robin lane (one in
service per lane, the rest queued). Then sample `lane_stats()` on a ~1 ms cadence
until every lane is idle, recording (t_rel_ms, per-lane queue_depth/active,
total_queue, num_active). Finally retire all futures (input order) to read the
per-request status/service.

Scenarios:
  * drain      -- baseline; submit the burst and watch it drain.
  * cancel_tail -- immediately cancel the tail half (idx [N/2, N)). Those sit deep
                  in each lane's queue (positions >= 4 in an 8-deep lane), so the
                  lanes cannot have started them in the ~ms it takes to submit:
                  every such cancel is structurally a queued cancel (True).

THE cancel_tail SUBTLETY (made explicit in gates and report): cancel() does NOT
remove an item from the lane deque. queue_depth therefore STILL counts cancelled-
but-not-yet-popped items -- so the first all-active snapshot shows the SAME
total_queue == N - lanes whether or not the tail was cancelled. The visible effect
of cancellation is a FASTER drain / skipped service when the lane later pops those
cancelled items, NOT an immediate queue_depth drop. This reinforces that
lane_stats() is observability only, not a control/synchronization API.

Structural, timing-robust gates (per run):
  G1 round-robin backlog -- at the first all-active snapshot: num_active == lanes,
     per-lane queue_depth spread <= 1, total_queue == N - lanes.
  G2 monotonic drain    -- the recorded total_queue sequence is non-increasing
     (no work is ever re-queued; lanes only pop).
  G3 reaches idle       -- the final snapshot has num_active == 0, total_queue == 0.
  G4 outcomes           -- drain: completed == N, cancelled == 0. cancel_tail:
     cancelled == N/2 (each ~0 service), completed == N/2 (real service).
  G5 round-robin over all -- per-lane assigned-request counts (from row actor_id,
     cancelled rows included) span <= 1 across exactly `lanes` lanes.

Raw per-run JSON written here is a small EXPERIMENT-LOCAL scratch file under
results/ (gitignored), NOT the v1 benchmark schema. Tracked evidence: the curated
aggregate.json beside this script + the markdown report. One subprocess per cell
(the rayx Engine owns one HPX runtime per process).

Usage (repo root, venv active, _rayx built):
    python experiments/17_lane_stats_observability/run_lane_stats_observability.py
    python experiments/17_lane_stats_observability/run_lane_stats_observability.py --quick
    # internal: --worker runs a single cell (used via subprocess)
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
if _RAYX_SRC not in sys.path:
    sys.path.insert(0, _RAYX_SRC)

WORK_MODE = "sleep"          # experiment is sleep-only by design
LANES = (2, 4)
SCENARIOS = ("drain", "cancel_tail")
HPX_THREADS = 4
SERVICE_MS = 40.0            # long enough that the burst stays backlogged while we sample
REQUESTS_PER_LANE = 8        # N = lanes * 8 -> perfectly round-robin-balanced
REPEATS = 3

SAMPLE_INTERVAL_S = 0.001    # ~1 ms cadence: "periodically", not a timing assertion
WALL_CAP_S = 10.0            # generous bound; cells drain in well under 1 s
MAX_SAMPLES = 200_000        # hard upper bound on the sampling loop
TRAJECTORY_POINTS = 12       # downsample the curve for the curated aggregate


def _p50(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _downsample(samples, k):
    """Pick <= k roughly-even (t_rel_ms, total_queue, num_active) points, always
    including the first and last sample, for a compact curated trajectory."""
    n = len(samples)
    if n <= k:
        idxs = range(n)
    else:
        idxs = sorted({round(i * (n - 1) / (k - 1)) for i in range(k)})
    return [{"t_rel_ms": round(samples[i]["t_rel_ms"], 3),
             "total_queue": samples[i]["total_queue"],
             "num_active": samples[i]["num_active"]} for i in idxs]


# --------------------------------------------------------------------------
# Worker: run ONE cell in its own process and write a compact JSON summary.
# --------------------------------------------------------------------------
def _worker(args):
    from rayx import Engine
    from collections import Counter

    n = args.requests
    L = args.num_lanes
    cancel_k = n // 2 if args.scenario == "cancel_tail" else 0

    with Engine(num_lanes=L, hpx_threads=args.hpx_threads) as engine:
        # Warmup: a few quick requests drained so the measured path is warm and
        # every lane worker thread has run at least once.
        for f in [engine.submit(service_ms=1.0, work_mode=WORK_MODE)
                  for _ in range(max(2 * L, 4))]:
            f.result()

        # Burst-submit the whole batch WITHOUT retiring -> deep FIFO backlog.
        futs = [engine.submit(service_ms=args.service_ms, work_mode=WORK_MODE,
                              label=f"r{i}") for i in range(n)]

        # cancel_tail: cancel idx [N/2, N) immediately. Deep-queue targets, so
        # every cancel is a queued cancel (True). cancel() does NOT dequeue them.
        cancel_targets = list(range(n - cancel_k, n))
        cancel_result = {i: engine.cancel(futs[i]) for i in cancel_targets}

        # Sample lane_stats() (non-consuming) on a ~1 ms cadence until idle.
        samples = []
        t0 = time.perf_counter_ns()
        first_active_idx = None
        for it in range(MAX_SAMPLES):
            st = engine.lane_stats()
            t_rel_ms = (time.perf_counter_ns() - t0) / 1e6
            qd = [int(r["queue_depth"]) for r in st]
            ac = [bool(r["active"]) for r in st]
            total_q = sum(qd)
            n_active = sum(1 for a in ac if a)
            samples.append({"t_rel_ms": t_rel_ms, "queue_depth": qd, "active": ac,
                            "total_queue": total_q, "num_active": n_active})
            if first_active_idx is None and n_active == L:
                first_active_idx = len(samples) - 1
            if it > 0 and n_active == 0 and total_q == 0:
                break
            if t_rel_ms > WALL_CAP_S * 1000.0:
                break
            time.sleep(SAMPLE_INTERVAL_S)

        # Retire every future (cancelled + completed) in input order; the lanes
        # are idle by now, so result() does not block.
        rows = engine.get(futs)

    # ---- Reductions (gate-relevant facts), computed here so the scratch file
    # stays compact even when the sampling loop took thousands of samples. ----
    fa = samples[first_active_idx] if first_active_idx is not None else None
    balance_spread = (max(fa["queue_depth"]) - min(fa["queue_depth"])) if fa else None

    monotonic_ok, monotonic_violation = True, None
    prev = None
    for s in samples:
        if prev is not None and s["total_queue"] > prev:
            monotonic_ok = False
            monotonic_violation = {"prev": prev, "next": s["total_queue"],
                                   "t_rel_ms": round(s["t_rel_ms"], 3)}
            break
        prev = s["total_queue"]

    final = samples[-1]
    reached_idle = (final["num_active"] == 0 and final["total_queue"] == 0)

    completed = [r for r in rows if r["status"] == "completed"]
    cancelled = [r for r in rows if r["status"] == "cancelled"]
    lane_counts = Counter(r["actor_id"] for r in rows)

    out = {
        "scenario": args.scenario, "num_lanes": L, "requests": n,
        "service_ms": args.service_ms, "hpx_threads": args.hpx_threads,
        "work_mode": WORK_MODE, "cancel_k": cancel_k,
        "cancel_all_true": all(v is True for v in cancel_result.values()),
        "num_samples": len(samples),
        "drain_span_ms": round(final["t_rel_ms"] - samples[0]["t_rel_ms"], 3),
        "first_active": {
            "sample_idx": first_active_idx,
            "t_rel_ms": round(fa["t_rel_ms"], 3) if fa else None,
            "queue_depth": fa["queue_depth"] if fa else None,
            "active": fa["active"] if fa else None,
            "total_queue": fa["total_queue"] if fa else None,
            "num_active": fa["num_active"] if fa else None,
        },
        "balance_spread": balance_spread,
        "expected_total_queue": n - L,
        "monotonic_ok": monotonic_ok,
        "monotonic_violation": monotonic_violation,
        "reached_idle": reached_idle,
        "final": {"t_rel_ms": round(final["t_rel_ms"], 3),
                  "queue_depth": final["queue_depth"], "active": final["active"],
                  "total_queue": final["total_queue"],
                  "num_active": final["num_active"]},
        "trajectory": _downsample(samples, TRAJECTORY_POINTS),
        "rows": {
            "completed": len(completed), "cancelled": len(cancelled),
            "completed_service_ms_p50":
                _p50([r["service_ms_observed"] for r in completed]),
            "cancelled_service_ms_p50":
                _p50([r["service_ms_observed"] for r in cancelled]),
        },
        "lane_counts": dict(lane_counts),
        "lane_count_spread":
            (max(lane_counts.values()) - min(lane_counts.values()))
            if lane_counts else None,
        "lanes_seen": len(lane_counts),
    }
    with open(args.out, "w") as fh:
        json.dump(out, fh)
    return 0


# --------------------------------------------------------------------------
# Orchestrator: gates + aggregation across repeats.
# --------------------------------------------------------------------------
def _load(path):
    with open(path) as fh:
        return json.load(fh)


def _gate(w):
    """Structural, timing-robust gates. Returns a list of failure strings."""
    fails = []
    L, n, scenario = w["num_lanes"], w["requests"], w["scenario"]

    # G1: round-robin backlog at the first all-active snapshot.
    fa = w["first_active"]
    if fa["sample_idx"] is None:
        fails.append("never observed all lanes active")
    else:
        if fa["num_active"] != L:
            fails.append(f"first-active num_active {fa['num_active']} != {L}")
        if w["balance_spread"] is None or w["balance_spread"] > 1:
            fails.append(f"first-active queue_depth spread {w['balance_spread']} > 1")
        if fa["total_queue"] != n - L:
            fails.append(f"first-active total_queue {fa['total_queue']} != {n - L} "
                         f"(N - lanes); cancel must NOT drop queued slots")

    # G2: monotonic, non-increasing total_queue (FIFO drain, no re-queue).
    if not w["monotonic_ok"]:
        fails.append(f"total_queue not non-increasing: {w['monotonic_violation']}")

    # G3: lanes reach idle.
    if not w["reached_idle"]:
        fails.append(f"never reached idle: final={w['final']}")

    # G4: per-request outcomes.
    comp, canc = w["rows"]["completed"], w["rows"]["cancelled"]
    csvc, dsvc = (w["rows"]["cancelled_service_ms_p50"],
                  w["rows"]["completed_service_ms_p50"])
    svc_cap = max(2.0, 0.2 * w["service_ms"])
    if scenario == "drain":
        if comp != n or canc != 0:
            fails.append(f"drain outcomes completed={comp} cancelled={canc} "
                         f"(expected {n}/0)")
    else:  # cancel_tail
        if not w["cancel_all_true"]:
            fails.append("cancel_tail: some tail cancel returned False (not queued)")
        if canc != n // 2 or comp != n - n // 2:
            fails.append(f"cancel_tail outcomes completed={comp} cancelled={canc} "
                         f"(expected {n - n // 2}/{n // 2})")
        if canc and (csvc is None or csvc > svc_cap):
            fails.append(f"cancel_tail cancelled service p50 {csvc} > {svc_cap}")
    if comp and (dsvc is None or dsvc < 0.5 * w["service_ms"]):
        fails.append(f"completed service p50 {dsvc} < {0.5 * w['service_ms']}")

    # G5: round-robin over ALL requests (cancelled rows carry their lane id).
    if w["lanes_seen"] != L:
        fails.append(f"lanes_seen {w['lanes_seen']} != {L}")
    if w["lane_count_spread"] is None or w["lane_count_spread"] > 1:
        fails.append(f"lane request-count spread {w['lane_count_spread']} > 1")

    return fails


def _cell(scenario, lanes, raw_dir, repeats, service_ms, all_fails):
    n = lanes * REQUESTS_PER_LANE
    reps = []
    for rep in range(repeats):
        fname = f"{scenario}_l{lanes}_r{rep}.json"
        out = os.path.join(raw_dir, fname)
        subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--worker",
             "--scenario", scenario, "--num-lanes", str(lanes),
             "--hpx-threads", str(HPX_THREADS), "--requests", str(n),
             "--service-ms", str(service_ms), "--out", out],
            check=True, cwd=_REPO_ROOT, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE)
        w = _load(out)
        fails = _gate(w)
        if fails:
            all_fails.append((fname, fails))
        reps.append(w)

    rep0 = reps[0]  # representative concrete snapshots/trajectory
    return {
        "scenario": scenario, "num_lanes": lanes, "requests": n,
        "service_ms": service_ms, "work_mode": WORK_MODE,
        "cancel_k": rep0["cancel_k"], "repeats": repeats,
        "expected_total_queue": n - lanes,
        "first_active_num_active": rep0["first_active"]["num_active"],
        "first_active_total_queue": rep0["first_active"]["total_queue"],
        "first_active_balance_spread": rep0["balance_spread"],
        "monotonic_ok": all(w["monotonic_ok"] for w in reps),
        "reached_idle": all(w["reached_idle"] for w in reps),
        "drain_span_ms_p50": _p50([w["drain_span_ms"] for w in reps]),
        "num_samples_p50": int(statistics.median(w["num_samples"] for w in reps)),
        "completed": int(statistics.median(w["rows"]["completed"] for w in reps)),
        "cancelled": int(statistics.median(w["rows"]["cancelled"] for w in reps)),
        "completed_service_ms_p50":
            _p50([w["rows"]["completed_service_ms_p50"] for w in reps]),
        "cancelled_service_ms_p50":
            _p50([w["rows"]["cancelled_service_ms_p50"] for w in reps]),
        "lane_count_spread": int(statistics.median(
            w["lane_count_spread"] for w in reps)),
        "representative_first_active_snapshot": {
            "queue_depth": rep0["first_active"]["queue_depth"],
            "active": rep0["first_active"]["active"],
            "total_queue": rep0["first_active"]["total_queue"],
            "num_active": rep0["first_active"]["num_active"],
        },
        "representative_final_snapshot": {
            "queue_depth": rep0["final"]["queue_depth"],
            "active": rep0["final"]["active"],
            "total_queue": rep0["final"]["total_queue"],
            "num_active": rep0["final"]["num_active"],
        },
        "representative_trajectory": rep0["trajectory"],
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run a single cell and write its JSON summary")
    ap.add_argument("--scenario")
    ap.add_argument("--num-lanes", type=int)
    ap.add_argument("--hpx-threads", type=int, default=HPX_THREADS)
    ap.add_argument("--requests", type=int)
    ap.add_argument("--service-ms", type=float, default=SERVICE_MS)
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (lanes {2}, both scenarios, 1 repeat, "
                         "smaller N); no aggregate.json")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker(args)

    lanes_set, repeats, rpl = LANES, REPEATS, REQUESTS_PER_LANE
    if args.quick:
        lanes_set, repeats = (2,), 1

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results",
                           f"lane_stats_observability_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp17] raw -> {raw_dir}")
    print(f"[exp17] work_mode={WORK_MODE} lanes={lanes_set} scenarios={SCENARIOS} "
          f"service_ms={SERVICE_MS} N=lanes*{rpl} repeats={repeats}")

    cells, all_fails = [], []
    for scenario in SCENARIOS:
        for lanes in lanes_set:
            c = _cell(scenario, lanes, raw_dir, repeats, SERVICE_MS, all_fails)
            cells.append(c)
            print(f"  {scenario:11s} L{lanes:<2d} "
                  f"first_active(active={c['first_active_num_active']}, "
                  f"total_q={c['first_active_total_queue']}/"
                  f"{c['expected_total_queue']}, spread={c['first_active_balance_spread']}) "
                  f"mono={c['monotonic_ok']} idle={c['reached_idle']} "
                  f"done={c['completed']} canc={c['cancelled']} "
                  f"drain={c['drain_span_ms_p50']}ms")

    aggregate = {
        "experiment": "lane_stats_observability",
        "boundary": "hpx-python-frontend",
        "instrument": "Engine.lane_stats() observability snapshot (per-lane "
                      "{actor_id, queue_depth, active}); non-consuming, can race",
        "submit": "facade Engine.submit (single-request); burst of N=lanes*8 "
                  "submitted without retiring -> deep FIFO backlog per lane",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("Observability ONLY: lane_stats() is a snapshot, non-consuming, "
                 "and NOT scheduler state / placement control / a synchronization "
                 "primitive / part of the v1 JSONL schema. cancel() does NOT remove "
                 "a queued item from the deque, so queue_depth still counts a "
                 "cancelled-but-not-yet-popped request (first-active total_queue is "
                 "N-lanes with or without cancel); cancellation shows up as faster "
                 "drain / skipped service when the lane later pops it, not an "
                 "immediate queue_depth drop. Raw per-run JSON is an experiment-"
                 "local scratch format under results/ (gitignored), NOT the v1 "
                 "benchmark schema."),
        "scenarios": {
            "drain": "burst submit, watch the backlog drain FIFO to idle",
            "cancel_tail": "burst submit, immediately cancel idx [N/2,N) (deep "
                           "queue -> all queued cancels), then watch drain",
        },
        "matrix": {"work_mode": WORK_MODE, "num_lanes": list(lanes_set),
                   "scenarios": list(SCENARIOS), "service_ms": SERVICE_MS,
                   "requests_per_lane": rpl, "hpx_threads": HPX_THREADS,
                   "repeats": repeats},
        "gates": [
            "G1 first all-active snapshot: num_active==lanes, queue_depth spread<=1, "
            "total_queue==N-lanes",
            "G2 total_queue non-increasing (FIFO drain, no re-queue)",
            "G3 reaches idle (num_active==0, total_queue==0)",
            "G4 outcomes: drain N/0; cancel_tail N/2 completed + N/2 cancelled (~0 "
            "service), all tail cancels queued (True)",
            "G5 round-robin over all requests: lane request-count spread<=1 across "
            "exactly `lanes` lanes",
        ],
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp17] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp17] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp17] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp17] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
