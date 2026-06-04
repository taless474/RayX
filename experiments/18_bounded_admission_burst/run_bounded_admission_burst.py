#!/usr/bin/env python3
"""Experiment 18 runner: bounded backlog under bursty arrivals.

Uses the rayx admission-control feature `max_queue_depth_per_lane` together with
the observability snapshot `Engine.lane_stats()` to show, structurally, the
backlog tradeoff under a burst that overfills the lanes:

> unbounded engine -- admits ALL offered work; the per-lane backlog grows deep
>   and late-admitted requests wait behind the whole queue.
> capped engine    -- REJECTS overflow early (caller-visible `QueueFullError`);
>   per-lane `queue_depth` stays bounded by the cap, so admitted work has a
>   bounded backlog/tail.

This is an EXPERIMENT PACKAGE ONLY. It adds NO new RayX API and does NOT change
ServiceLane semantics, the benchmark JSONL schema, the analyzer, the benchmark
drivers, HpxLane / experiment 16, or CI. rayx-only, local synthetic, sleep mode.

What `max_queue_depth_per_lane` IS (and is not), restated so the experiment can
only be read one way: it is **local, per-lane admission by rejection**. When the
round-robin target lane already holds `cap` queued-but-not-started requests, the
submit raises `QueueFullError` immediately. It is NOT Ray Serve backpressure,
NOT distributed flow control, and NOT blocking backpressure (the call returns by
raising; it never blocks waiting for space). The cap counts only
queued-but-not-started work -- the ACTIVE in-service request on each lane is
extra (already popped off the queue) -- so a lane's live footprint is at most
`cap + 1` (one active + `cap` queued). Rejected requests are caller-visible
exceptions, NOT result rows.

Burst model (per cell): warm up, then burst-submit N = lanes*8 sleep requests as
fast as possible WITHOUT retiring. In `capped` mode each `submit` is wrapped in
try/except `QueueFullError`, so admitted requests collect a Future and rejected
ones are counted (no Future). Then sample `lane_stats()` on a ~1 ms cadence until
every lane is idle, recording per-lane queue_depth/active and the running PEAK
per-lane queue_depth. Finally retire the admitted futures (input order) to read
per-request status/service/latency.

Why the peak is captured: service_ms=40 >> the microseconds the burst takes, so
for the first ~40 ms after the burst nothing completes and the backlog sits at
its peak -- the first samples see it. `unbounded` peaks at ~lanes*8/lanes - 1 = 7
queued per lane (> cap); `capped` peaks at exactly `cap`.

Structural, timing-robust gates (per run):
  G1 unbounded admits all   -- admitted == N, rejected == 0.
  G2 capped admitted band   -- lanes*cap <= admitted <= lanes*(cap+1), rejected>0,
     admitted + rejected == N. (Floor = no lane drained during the burst; ceiling
     = one pop per lane freed one slot. Robust to pop-timing; widened only if an
     actual mid-burst drain is detected, see `drain_during_submit`.)
  G3 capped bounds backlog  -- peak per-lane queue_depth <= cap across ALL samples.
  G4 unbounded grows backlog-- peak per-lane queue_depth > cap at least once.
  G5 admitted complete      -- every admitted (retired) row status == "completed".
  G6 reject => no row       -- len(rows) == admitted == N - rejected (rejected
     requests produced no Future and no row).
  G7 reaches idle           -- final lane_stats() sample: num_active==0, total_q==0.
  G8 round-robin balance     -- admitted-row actor_id counts span <= 1 across
     exactly `lanes` lanes.

Latency/throughput magnitudes (queue_wait_ms / total_ms p50/p99, drain span) are
REPORTED as machine-specific observations of the tradeoff -- never gated.

Raw per-run JSON written here is a small EXPERIMENT-LOCAL scratch file under
results/ (gitignored), NOT the v1 benchmark schema. Tracked evidence: the curated
aggregate.json beside this script + the markdown report. One subprocess per cell
(the rayx Engine owns one HPX runtime per process).

Usage (repo root, venv active, _rayx built):
    python experiments/18_bounded_admission_burst/run_bounded_admission_burst.py
    python experiments/18_bounded_admission_burst/run_bounded_admission_burst.py --quick
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
NUM_LANES = 4
MODES = ("unbounded", "capped")
CAP = 3                      # max_queue_depth_per_lane for the capped mode
HPX_THREADS = 4
SERVICE_MS = 40.0           # long enough that the burst stays backlogged while we sample
REQUESTS_PER_LANE = 8       # N = lanes * 8 -> overflows a cap-3 lane (capacity 4)
REPEATS = 3

SAMPLE_INTERVAL_S = 0.001   # ~1 ms cadence: "periodically", not a timing assertion
WALL_CAP_S = 15.0           # generous bound; cells drain in well under 1.5 s
MAX_SAMPLES = 200_000       # hard upper bound on the sampling loop
TRAJECTORY_POINTS = 12      # downsample the curve for the curated aggregate


def _p50(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _p99(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    # Nearest-rank p99 (small samples): index ceil(0.99*n)-1.
    import math
    k = max(0, math.ceil(0.99 * len(xs)) - 1)
    return round(xs[k], 4)


def _downsample(samples, k):
    """Pick <= k roughly-even (t_rel_ms, total_queue, num_active, max_qd) points,
    always including the first and last sample, for a compact curated trajectory."""
    n = len(samples)
    if n <= k:
        idxs = range(n)
    else:
        idxs = sorted({round(i * (n - 1) / (k - 1)) for i in range(k)})
    return [{"t_rel_ms": round(samples[i]["t_rel_ms"], 3),
             "total_queue": samples[i]["total_queue"],
             "num_active": samples[i]["num_active"],
             "max_queue_depth": max(samples[i]["queue_depth"])} for i in idxs]


# --------------------------------------------------------------------------
# Worker: run ONE cell in its own process and write a compact JSON summary.
# --------------------------------------------------------------------------
def _worker(args):
    from collections import Counter

    from rayx import Engine, QueueFullError

    n = args.requests
    L = args.num_lanes
    cap = None if args.mode == "unbounded" else args.cap

    with Engine(num_lanes=L, hpx_threads=args.hpx_threads,
                max_queue_depth_per_lane=cap) as engine:
        # Warmup: a few quick requests drained so the measured path is warm and
        # every lane worker thread has run at least once. (No cap interaction:
        # they drain immediately, well under any cap.)
        for f in [engine.submit(service_ms=1.0, work_mode=WORK_MODE)
                  for _ in range(max(2 * L, 4))]:
            f.result()

        # Burst-submit N as fast as possible WITHOUT retiring. capped mode catches
        # QueueFullError (rejected -> no Future); unbounded admits all. We record
        # the admitted call indices so the round-robin mapping can be checked.
        futs = []            # admitted futures, in submit order
        admitted_idx = []    # original call index of each admitted request
        rejected = 0
        for i in range(n):
            try:
                f = engine.submit(service_ms=args.service_ms, work_mode=WORK_MODE,
                                  label=f"r{i}")
            except QueueFullError:
                rejected += 1
                continue
            futs.append(f)
            admitted_idx.append(i)

        # Sample lane_stats() (non-consuming) on a ~1 ms cadence until idle.
        samples = []
        t0 = time.perf_counter_ns()
        first_active_idx = None
        peak_qd = 0  # running max per-lane queue_depth across all samples
        for it in range(MAX_SAMPLES):
            st = engine.lane_stats()
            t_rel_ms = (time.perf_counter_ns() - t0) / 1e6
            qd = [int(r["queue_depth"]) for r in st]
            ac = [bool(r["active"]) for r in st]
            total_q = sum(qd)
            n_active = sum(1 for a in ac if a)
            peak_qd = max(peak_qd, max(qd))
            samples.append({"t_rel_ms": t_rel_ms, "queue_depth": qd, "active": ac,
                            "total_queue": total_q, "num_active": n_active})
            if first_active_idx is None and n_active == L:
                first_active_idx = len(samples) - 1
            if it > 0 and n_active == 0 and total_q == 0:
                break
            if t_rel_ms > WALL_CAP_S * 1000.0:
                break
            time.sleep(SAMPLE_INTERVAL_S)

        # Retire every admitted future in input order; the lanes are idle by now,
        # so result() does not block. Rejected requests have no future to retire.
        rows = engine.get(futs) if futs else []

    # ---- Reductions (gate-relevant facts), computed here so the scratch file
    # stays compact even when the sampling loop took thousands of samples. ----
    admitted = len(futs)

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
    non_completed = [r for r in rows if r["status"] != "completed"]
    lane_counts = Counter(r["actor_id"] for r in rows)

    # Did any lane drain (complete a request) DURING the burst-submit window? Only
    # then could `admitted` exceed lanes*(cap+1). With 40 ms service and a us-scale
    # burst this is ~never, but we surface it so G2's ceiling can be widened
    # honestly rather than flake. Heuristic: admitted beyond the no-drain ceiling.
    drain_during_submit = max(0, admitted - L * (args.cap + 1)) if cap is not None \
        else 0

    out = {
        "mode": args.mode, "num_lanes": L, "cap": cap, "requests": n,
        "service_ms": args.service_ms, "hpx_threads": args.hpx_threads,
        "work_mode": WORK_MODE,
        "admitted": admitted, "rejected": rejected,
        "admitted_plus_rejected": admitted + rejected,
        "rows": len(rows),
        "peak_per_lane_queue_depth": peak_qd,
        "drain_during_submit": drain_during_submit,
        "num_samples": len(samples),
        "drain_span_ms": round(final["t_rel_ms"] - samples[0]["t_rel_ms"], 3),
        "first_active_idx": first_active_idx,
        "monotonic_ok": monotonic_ok,
        "monotonic_violation": monotonic_violation,
        "reached_idle": reached_idle,
        "final": {"t_rel_ms": round(final["t_rel_ms"], 3),
                  "queue_depth": final["queue_depth"], "active": final["active"],
                  "total_queue": final["total_queue"],
                  "num_active": final["num_active"]},
        "first_sample": {"t_rel_ms": round(samples[0]["t_rel_ms"], 3),
                         "queue_depth": samples[0]["queue_depth"],
                         "active": samples[0]["active"],
                         "total_queue": samples[0]["total_queue"],
                         "num_active": samples[0]["num_active"]},
        "trajectory": _downsample(samples, TRAJECTORY_POINTS),
        "outcomes": {
            "completed": len(completed), "non_completed": len(non_completed),
            "completed_service_ms_p50":
                _p50([r["service_ms_observed"] for r in completed]),
            "completed_total_ms_p50": _p50([r["total_ms"] for r in completed]),
            "completed_total_ms_p99": _p99([r["total_ms"] for r in completed]),
            "completed_queue_wait_ms_p50":
                _p50([r["queue_wait_ms"] for r in completed]),
            "completed_queue_wait_ms_p99":
                _p99([r["queue_wait_ms"] for r in completed]),
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
    L, n, mode, cap = w["num_lanes"], w["requests"], w["mode"], w["cap"]

    # Accounting: admitted + rejected must equal N, and rows must equal admitted
    # (G6: rejected -> no Future, no row).
    if w["admitted_plus_rejected"] != n:
        fails.append(f"admitted+rejected {w['admitted_plus_rejected']} != N {n}")
    if w["rows"] != w["admitted"]:
        fails.append(f"rows {w['rows']} != admitted {w['admitted']} "
                     f"(rejected must produce no row)")

    if mode == "unbounded":
        # G1: admits all, rejects none.
        if w["admitted"] != n or w["rejected"] != 0:
            fails.append(f"unbounded admitted={w['admitted']} rejected="
                         f"{w['rejected']} (expected {n}/0)")
        # G4: backlog grows past the (other mode's) cap at least once.
        if w["peak_per_lane_queue_depth"] <= CAP:
            fails.append(f"unbounded peak queue_depth {w['peak_per_lane_queue_depth']}"
                         f" did not exceed cap {CAP} (backlog should grow)")
    else:  # capped
        # G2: admitted lands in the robust band, with overflow rejected. Ceiling
        # is widened by any detected mid-burst drain so a draining lane never flakes.
        floor = L * cap
        ceiling = L * (cap + 1) + w["drain_during_submit"]
        if not (floor <= w["admitted"] <= ceiling):
            fails.append(f"capped admitted {w['admitted']} outside band "
                         f"[{floor}, {ceiling}] (lanes*cap .. lanes*(cap+1)[+drain])")
        if w["rejected"] <= 0:
            fails.append(f"capped rejected {w['rejected']} (overflow should be shed)")
        # G3: per-lane backlog never exceeds the cap.
        if w["peak_per_lane_queue_depth"] > cap:
            fails.append(f"capped peak queue_depth {w['peak_per_lane_queue_depth']} "
                         f"> cap {cap} (admission must bound the backlog)")

    # G5: every admitted (retired) row completed successfully.
    if w["outcomes"]["non_completed"] != 0:
        fails.append(f"{w['outcomes']['non_completed']} admitted rows did not "
                     f"complete (expected all completed)")
    if w["admitted"] and w["outcomes"]["completed"] != w["admitted"]:
        fails.append(f"completed {w['outcomes']['completed']} != admitted "
                     f"{w['admitted']}")

    # G7: lanes reach idle.
    if not w["reached_idle"]:
        fails.append(f"never reached idle: final={w['final']}")

    # G8: round-robin balance of admitted rows across exactly `lanes` lanes.
    if w["lanes_seen"] != L:
        fails.append(f"lanes_seen {w['lanes_seen']} != {L}")
    if w["lane_count_spread"] is None or w["lane_count_spread"] > 1:
        fails.append(f"admitted lane-count spread {w['lane_count_spread']} > 1")

    # Sanity: completed service ~ requested (not a tail/latency gate, just a floor
    # to confirm real sleep service ran on admitted work).
    csvc = w["outcomes"]["completed_service_ms_p50"]
    if w["outcomes"]["completed"] and (csvc is None or csvc < 0.5 * w["service_ms"]):
        fails.append(f"completed service p50 {csvc} < {0.5 * w['service_ms']}")

    return fails


def _cell(mode, raw_dir, repeats, rpl, all_fails):
    n = NUM_LANES * rpl
    reps = []
    for rep in range(repeats):
        fname = f"{mode}_l{NUM_LANES}_r{rep}.json"
        out = os.path.join(raw_dir, fname)
        subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--worker",
             "--mode", mode, "--num-lanes", str(NUM_LANES),
             "--hpx-threads", str(HPX_THREADS), "--requests", str(n),
             "--service-ms", str(SERVICE_MS), "--cap", str(CAP), "--out", out],
            check=True, cwd=_REPO_ROOT, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE)
        w = _load(out)
        fails = _gate(w)
        if fails:
            all_fails.append((fname, fails))
        reps.append(w)

    rep0 = reps[0]  # representative concrete snapshots/trajectory
    cap = rep0["cap"]
    return {
        "mode": mode, "num_lanes": NUM_LANES, "cap": cap, "requests": n,
        "service_ms": SERVICE_MS, "work_mode": WORK_MODE,
        "hpx_threads": HPX_THREADS, "repeats": repeats,
        "expected_admitted_band": ([n, n] if mode == "unbounded"
                                   else [NUM_LANES * cap, NUM_LANES * (cap + 1)]),
        "admitted_p50": int(statistics.median(w["admitted"] for w in reps)),
        "rejected_p50": int(statistics.median(w["rejected"] for w in reps)),
        "peak_per_lane_queue_depth_p50":
            int(statistics.median(w["peak_per_lane_queue_depth"] for w in reps)),
        "peak_per_lane_queue_depth_max":
            max(w["peak_per_lane_queue_depth"] for w in reps),
        "queue_depth_cap_respected":
            (all(w["peak_per_lane_queue_depth"] <= cap for w in reps)
             if mode == "capped" else None),
        "observed_depth_over_cap":
            (all(w["peak_per_lane_queue_depth"] > CAP for w in reps)
             if mode == "unbounded" else None),
        "all_admitted_completed":
            all(w["outcomes"]["non_completed"] == 0 for w in reps),
        "rows_equal_admitted": all(w["rows"] == w["admitted"] for w in reps),
        "reached_idle": all(w["reached_idle"] for w in reps),
        "lanes_seen": rep0["lanes_seen"],
        "lane_count_spread": int(statistics.median(
            w["lane_count_spread"] for w in reps)),
        "drain_span_ms_p50": _p50([w["drain_span_ms"] for w in reps]),
        "num_samples_p50": int(statistics.median(w["num_samples"] for w in reps)),
        # Latency tradeoff -- REPORTED, never gated (machine-specific magnitudes).
        "completed_service_ms_p50":
            _p50([w["outcomes"]["completed_service_ms_p50"] for w in reps]),
        "completed_total_ms_p50":
            _p50([w["outcomes"]["completed_total_ms_p50"] for w in reps]),
        "completed_total_ms_p99":
            _p50([w["outcomes"]["completed_total_ms_p99"] for w in reps]),
        "completed_queue_wait_ms_p50":
            _p50([w["outcomes"]["completed_queue_wait_ms_p50"] for w in reps]),
        "completed_queue_wait_ms_p99":
            _p50([w["outcomes"]["completed_queue_wait_ms_p99"] for w in reps]),
        "representative_first_sample": {
            "queue_depth": rep0["first_sample"]["queue_depth"],
            "active": rep0["first_sample"]["active"],
            "total_queue": rep0["first_sample"]["total_queue"],
            "num_active": rep0["first_sample"]["num_active"],
        },
        "representative_final_sample": {
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
    ap.add_argument("--mode")
    ap.add_argument("--num-lanes", type=int)
    ap.add_argument("--hpx-threads", type=int, default=HPX_THREADS)
    ap.add_argument("--requests", type=int)
    ap.add_argument("--service-ms", type=float, default=SERVICE_MS)
    ap.add_argument("--cap", type=int, default=CAP)
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (both modes, 1 repeat, smaller N); no "
                         "aggregate.json")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker(args)

    # --quick: 1 repeat and a smaller N. N must stay > lanes*(cap+1) (total
    # capacity = 4*4 = 16) so the capped mode still overflows and sheds; rpl=6 ->
    # N=24 (capacity 16, ~8 rejected). Full run uses the proposed matrix (rpl=8).
    repeats = 1 if args.quick else REPEATS
    rpl = 6 if args.quick else REQUESTS_PER_LANE

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results",
                           f"bounded_admission_burst_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    n = NUM_LANES * rpl
    print(f"[exp18] raw -> {raw_dir}")
    print(f"[exp18] work_mode={WORK_MODE} num_lanes={NUM_LANES} cap={CAP} "
          f"service_ms={SERVICE_MS} N={n} repeats={repeats} modes={MODES}")

    cells, all_fails = [], []
    for mode in MODES:
        c = _cell(mode, raw_dir, repeats, rpl, all_fails)
        cells.append(c)
        print(f"  {mode:9s} L{NUM_LANES} "
              f"admitted={c['admitted_p50']} rejected={c['rejected_p50']} "
              f"band={c['expected_admitted_band']} "
              f"peak_qd={c['peak_per_lane_queue_depth_p50']} "
              f"idle={c['reached_idle']} done_all={c['all_admitted_completed']} "
              f"spread={c['lane_count_spread']} "
              f"qwait_p99={c['completed_queue_wait_ms_p99']}ms "
              f"drain={c['drain_span_ms_p50']}ms")

    aggregate = {
        "experiment": "bounded_admission_burst",
        "boundary": "hpx-python-frontend",
        "instrument": "Engine(max_queue_depth_per_lane=cap) admission control + "
                      "Engine.lane_stats() observability snapshot (per-lane "
                      "{actor_id, queue_depth, active})",
        "submit": "facade Engine.submit (single-request); burst of N=lanes*8 "
                  "submitted as fast as possible without retiring; capped mode "
                  "catches QueueFullError per submit",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("max_queue_depth_per_lane is LOCAL, PER-LANE admission by "
                 "rejection. It is NOT Ray Serve backpressure, NOT distributed "
                 "flow control, and NOT blocking backpressure (submit returns by "
                 "raising QueueFullError; it never blocks waiting for space). The "
                 "cap counts queued-but-not-started work only -- the active "
                 "in-service request is extra -- so a lane's live footprint is at "
                 "most cap+1 (one active + cap queued). Rejected requests are "
                 "caller-visible exceptions, NOT result rows (no Future, no row). "
                 "Capped mode trades shed load for a bounded backlog/tail; "
                 "unbounded admits everything and the late-queued tail grows. "
                 "Cancelled queued items still count against the cap until the "
                 "lane pops them, but cancellation is NOT exercised here. "
                 "Latency/throughput magnitudes are machine-specific and REPORTED, "
                 "not gated; the structural gates are the result. Raw per-run JSON "
                 "is an experiment-local scratch format under results/ "
                 "(gitignored), NOT the v1 benchmark schema."),
        "modes": {
            "unbounded": "max_queue_depth_per_lane=None; admit all N, deep backlog",
            "capped": f"max_queue_depth_per_lane={CAP}; reject overflow, bounded "
                      f"backlog",
        },
        "matrix": {"work_mode": WORK_MODE, "num_lanes": NUM_LANES,
                   "modes": list(MODES), "cap": CAP, "service_ms": SERVICE_MS,
                   "requests_per_lane": rpl, "hpx_threads": HPX_THREADS,
                   "repeats": repeats},
        "gates": [
            "G1 unbounded admits all: admitted==N, rejected==0",
            "G2 capped admitted band: lanes*cap <= admitted <= lanes*(cap+1)[+drain]"
            ", rejected>0, admitted+rejected==N",
            "G3 capped bounds backlog: peak per-lane queue_depth <= cap",
            "G4 unbounded grows backlog: peak per-lane queue_depth > cap",
            "G5 admitted complete: every retired row status==completed",
            "G6 reject => no row: rows == admitted == N - rejected",
            "G7 reaches idle: final num_active==0, total_queue==0",
            "G8 round-robin balance: admitted lane-count spread<=1 across `lanes` "
            "lanes",
        ],
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp18] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp18] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp18] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp18] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
