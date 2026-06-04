#!/usr/bin/env python3
"""Experiment 19 runner: bounded backlog under SUSTAINED offered load.

Experiment 18 asked one question with a single burst: when a one-shot burst
overfills the lanes, does `max_queue_depth_per_lane` bound the per-lane backlog?
Experiment 19 asks the sustained-flow version of the same question with a
*continuous producer*:

> Under a continuous submitter at a fixed offered rate, does
>   `max_queue_depth_per_lane` keep per-lane backlog bounded by shedding overload,
>   while the unbounded engine accumulates queue when offered load exceeds the
>   lanes' service rate?

This is an EXPERIMENT PACKAGE ONLY. It adds NO new RayX API and does NOT change
ServiceLane semantics, the Python API, the benchmark drivers, the analyzer, the
benchmark JSONL schema, HpxLane / experiment 16, or CI. rayx-only, local
synthetic, sleep mode. It reuses the existing `Engine(max_queue_depth_per_lane=
cap)` admission control, the `Engine.lane_stats()` observability snapshot, and
the caller-visible `QueueFullError` from experiments 17/18.

What `max_queue_depth_per_lane` IS (restated so this can only be read one way):
**local, per-lane admission by rejection**. When the round-robin target lane
already holds `cap` queued-but-not-started requests, `submit` raises
`QueueFullError` immediately. It is NOT Ray Serve backpressure, NOT distributed
flow control, and NOT blocking backpressure (the call returns by raising; it
never blocks waiting for space). The cap counts only queued-but-not-started work
-- the ACTIVE in-service request on each lane is extra (already popped) -- so a
lane's live footprint is at most `cap + 1`. Rejected requests are caller-visible
exceptions, NOT result rows (no Future, no row).

Offered-load model (per cell): warm up and drain, then run a duration-based
producer that ATTEMPTS a submit at a fixed inter-arrival interval (a fixed
schedule `next_t += interval`, so the offered rate is independent of per-iteration
cost as long as that cost stays under the interval). In `capped` mode each submit
is wrapped in try/except `QueueFullError`: admitted requests collect a Future and
rejected ones are counted (no Future). The producer does NOT retire futures while
running, so the live per-lane backlog is exactly arrivals-minus-serviced. Once per
attempt we sample `lane_stats()` and track the running PEAK per-lane queue_depth
and the PEAK total_queue. After the producer stops we drain (retire) the admitted
futures in input order, then confirm the lanes reach idle.

Two offered rates, relative to the lanes' observed service rate. With
`num_lanes=4` and a sleep `service_ms=40` that lands near ~50 ms observed (the
~25% sleep overshoot), per-lane capacity is one active + `cap` queued and the
aggregate service rate is roughly `num_lanes / observed_service ~= 4 / 0.050 ~=
80 req/s`:
  below capacity -- one submit every 20 ms ~= 50 req/s (util ~0.6): both modes
    keep backlog low; capped sheds ~nothing.
  over  capacity -- one submit every  5 ms ~= 200 req/s (>> 80): the UNBOUNDED
    engine's per-lane backlog grows past the cap and keeps growing for the whole
    window; the CAPPED engine pins per-lane queue_depth at the cap and sheds the
    overflow as caller-visible QueueFullError.
(Offered rates are nominal targets; the host's sleep granularity can stretch the
interval. The structural gates below assert direction-of-change, not a rate, so
they are robust to that -- the runner reports the realized attempt rate.)

Structural, timing-robust gates (per run; load/bound-aware):
  G1 accounting           -- admitted + rejected == attempted; unbounded rejects 0.
  G2 reject => no row      -- rows == admitted == attempted - rejected.
  G3 admitted complete     -- every admitted (retired) row status == "completed".
  G4 reaches idle          -- final lane_stats() sample: num_active==0, total_q==0.
  G5 lane balance          -- completed rows span exactly `lanes` lanes and the
     least-loaded lane handled >= half the busiest (min*2 >= max).
  G6 capped bounds backlog -- (capped modes) peak per-lane queue_depth <= cap,
     across ALL samples.
  G7 below-capacity capped near-zero shed -- rejected <= ceil(REJECT_TOL_FRAC *
     attempted) (offered load is under the service rate, so almost nothing sheds).
  G8 over-capacity capped sheds -- rejected > 0 (overflow is shed).
  G9 over-capacity unbounded grows backlog -- peak per-lane queue_depth > cap at
     least once (the unbounded queue overruns the cap under sustained overload).

Latency/throughput magnitudes (queue_wait_ms / total_ms p50/p99, realized attempt
rate, drain span) are REPORTED as machine-specific observations of the tradeoff --
never gated.

Raw per-run JSON written here is a small EXPERIMENT-LOCAL scratch file under
results/ (gitignored), NOT the v1 benchmark schema. Tracked evidence: the curated
aggregate.json beside this script + the markdown report. One subprocess per cell
(the rayx Engine owns one HPX runtime per process).

Usage (repo root, venv active, _rayx built):
    python experiments/19_bounded_admission_offered_load/run_bounded_admission_offered_load.py
    python experiments/19_bounded_admission_offered_load/run_bounded_admission_offered_load.py --quick
    # internal: --worker runs a single cell (used via subprocess)
"""
import argparse
import json
import math
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
CAP = 3                      # max_queue_depth_per_lane for the capped modes
HPX_THREADS = 4
SERVICE_MS = 40.0           # ~50 ms observed (sleep overshoot) -> ~80 req/s/4 lanes
REPEATS = 3

# Cells: (name, load, cap). cap=None -> unbounded engine; cap=CAP -> capped.
CELLS = (
    ("below_capacity_unbounded", "below", None),
    ("below_capacity_capped",    "below", CAP),
    ("over_capacity_unbounded",  "over",  None),
    ("over_capacity_capped",     "over",  CAP),
)
# Fixed inter-arrival interval per offered-load regime (ms).
INTERVAL_MS = {"below": 20.0, "over": 5.0}      # ~50 req/s vs ~200 req/s
DURATION_MS_FULL = 800.0
DURATION_MS_QUICK = 300.0

REJECT_TOL_FRAC = 0.05      # below-capacity capped: near-zero shed tolerance
IDLE_POLL_MAX = 2000        # post-drain idle-confirm samples (1 ms cadence)
IDLE_POLL_INTERVAL_S = 0.001
TRAJECTORY_POINTS = 12      # downsample the live curve for the curated aggregate


def _p50(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _p99(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    # Nearest-rank p99 (small samples): index ceil(0.99*n)-1.
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

    L = args.num_lanes
    cap = None if args.cap < 0 else args.cap
    interval_s = args.interval_ms / 1000.0
    duration_s = args.duration_ms / 1000.0

    with Engine(num_lanes=L, hpx_threads=args.hpx_threads,
                max_queue_depth_per_lane=cap) as engine:
        # Warmup: a few quick requests drained so the measured path is warm and
        # every lane worker thread has run at least once. They drain immediately
        # (service 1 ms), well under any cap, so no admission interaction.
        for f in [engine.submit(service_ms=1.0, work_mode=WORK_MODE)
                  for _ in range(max(2 * L, 4))]:
            f.result()

        # Sustained producer: ATTEMPT a submit at the fixed interval for `duration`
        # WITHOUT retiring. capped mode catches QueueFullError (rejected -> no
        # Future); unbounded admits all. Once per attempt, sample lane_stats()
        # (non-consuming) and track the running peak per-lane queue_depth and peak
        # total_queue. The schedule is absolute (next_t += interval) so the offered
        # rate does not drift with per-iteration cost.
        futs = []            # admitted futures, in submit order
        admitted_idx = []    # original attempt index of each admitted request
        attempted = 0
        rejected = 0
        samples = []
        peak_qd = 0          # running max per-lane queue_depth across all samples
        peak_total_q = 0     # running max total queue_depth across all samples
        t0 = time.perf_counter_ns()
        t_start = time.perf_counter()
        end_t = t_start + duration_s
        next_t = t_start
        i = 0
        while True:
            now = time.perf_counter()
            if now >= end_t:
                break
            if now < next_t:
                time.sleep(next_t - now)
            # One offered request.
            attempted += 1
            try:
                f = engine.submit(service_ms=args.service_ms, work_mode=WORK_MODE,
                                  label=f"r{i}")
                futs.append(f)
                admitted_idx.append(i)
            except QueueFullError:
                rejected += 1
            i += 1
            # Live backlog snapshot.
            st = engine.lane_stats()
            t_rel_ms = (time.perf_counter_ns() - t0) / 1e6
            qd = [int(r["queue_depth"]) for r in st]
            ac = [bool(r["active"]) for r in st]
            total_q = sum(qd)
            n_active = sum(1 for a in ac if a)
            peak_qd = max(peak_qd, max(qd))
            peak_total_q = max(peak_total_q, total_q)
            samples.append({"t_rel_ms": t_rel_ms, "queue_depth": qd, "active": ac,
                            "total_queue": total_q, "num_active": n_active})
            next_t += interval_s

        producer_span_ms = (time.perf_counter() - t_start) * 1000.0

        # Retire every admitted future in input order; this drains the backlog the
        # producer built up. Rejected requests have no future to retire.
        rows = engine.get(futs) if futs else []

        # Confirm the lanes reach idle after the drain (the last lane may still be
        # clearing its active flag the instant get() returns). 1 ms cadence, bounded.
        idle_final = None
        for _ in range(IDLE_POLL_MAX):
            st = engine.lane_stats()
            qd = [int(r["queue_depth"]) for r in st]
            ac = [bool(r["active"]) for r in st]
            idle_final = {"queue_depth": qd, "active": ac, "total_queue": sum(qd),
                          "num_active": sum(1 for a in ac if a)}
            if idle_final["num_active"] == 0 and idle_final["total_queue"] == 0:
                break
            time.sleep(IDLE_POLL_INTERVAL_S)

    # ---- Reductions (gate-relevant facts) ----
    admitted = len(futs)
    realized_rate = (attempted / (producer_span_ms / 1000.0)
                     if producer_span_ms > 0 else None)

    completed = [r for r in rows if r["status"] == "completed"]
    non_completed = [r for r in rows if r["status"] != "completed"]
    lane_counts = Counter(r["actor_id"] for r in rows)
    if lane_counts:
        lane_min = min(lane_counts.values())
        lane_max = max(lane_counts.values())
    else:
        lane_min = lane_max = None

    reached_idle = (idle_final is not None and idle_final["num_active"] == 0
                    and idle_final["total_queue"] == 0)

    first = samples[0] if samples else None
    last = samples[-1] if samples else None

    out = {
        "mode": args.mode, "load": args.load, "num_lanes": L, "cap": cap,
        "service_ms": args.service_ms, "hpx_threads": args.hpx_threads,
        "work_mode": WORK_MODE, "interval_ms": args.interval_ms,
        "duration_ms": args.duration_ms,
        "attempted": attempted, "admitted": admitted, "rejected": rejected,
        "admitted_plus_rejected": admitted + rejected,
        "rows": len(rows),
        "peak_per_lane_queue_depth": peak_qd,
        "peak_total_queue": peak_total_q,
        "num_samples": len(samples),
        "producer_span_ms": round(producer_span_ms, 3),
        "realized_attempt_rate_per_s":
            round(realized_rate, 2) if realized_rate is not None else None,
        "reached_idle": reached_idle,
        "idle_final": idle_final,
        "first_sample": ({"t_rel_ms": round(first["t_rel_ms"], 3),
                          "queue_depth": first["queue_depth"],
                          "active": first["active"],
                          "total_queue": first["total_queue"],
                          "num_active": first["num_active"]} if first else None),
        "last_sample": ({"t_rel_ms": round(last["t_rel_ms"], 3),
                         "queue_depth": last["queue_depth"],
                         "active": last["active"],
                         "total_queue": last["total_queue"],
                         "num_active": last["num_active"]} if last else None),
        "trajectory": _downsample(samples, TRAJECTORY_POINTS) if samples else [],
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
        "lane_count_min": lane_min,
        "lane_count_max": lane_max,
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
    """Structural, timing-robust gates (load/bound-aware). Returns failures."""
    fails = []
    L = w["num_lanes"]
    load = w["load"]
    bound = "unbounded" if w["cap"] is None else "capped"
    attempted = w["attempted"]

    # G1 accounting: admitted + rejected == attempted; unbounded sheds nothing.
    if w["admitted_plus_rejected"] != attempted:
        fails.append(f"admitted+rejected {w['admitted_plus_rejected']} != "
                     f"attempted {attempted}")
    if bound == "unbounded" and w["rejected"] != 0:
        fails.append(f"unbounded rejected {w['rejected']} (expected 0)")

    # G2 reject => no row.
    if w["rows"] != w["admitted"]:
        fails.append(f"rows {w['rows']} != admitted {w['admitted']} "
                     f"(rejected must produce no row)")

    # G3 admitted complete.
    if w["outcomes"]["non_completed"] != 0:
        fails.append(f"{w['outcomes']['non_completed']} admitted rows did not "
                     f"complete (expected all completed)")
    if w["admitted"] and w["outcomes"]["completed"] != w["admitted"]:
        fails.append(f"completed {w['outcomes']['completed']} != admitted "
                     f"{w['admitted']}")

    # G4 reaches idle.
    if not w["reached_idle"]:
        fails.append(f"never reached idle: final={w['idle_final']}")

    # G5 lane balance: exactly `lanes` lanes seen and min >= half of max.
    if w["lanes_seen"] != L:
        fails.append(f"lanes_seen {w['lanes_seen']} != {L}")
    if w["admitted"] and (w["lane_count_min"] is None
                          or w["lane_count_min"] * 2 < w["lane_count_max"]):
        fails.append(f"lane balance: min {w['lane_count_min']} < half of max "
                     f"{w['lane_count_max']}")

    # G6 capped bounds backlog.
    if bound == "capped" and w["peak_per_lane_queue_depth"] > w["cap"]:
        fails.append(f"capped peak queue_depth {w['peak_per_lane_queue_depth']} "
                     f"> cap {w['cap']} (admission must bound the backlog)")

    # G7/G8 capped shed behaviour, load-specific.
    if bound == "capped" and load == "below":
        tol = math.ceil(REJECT_TOL_FRAC * attempted)
        if w["rejected"] > tol:
            fails.append(f"below-capacity capped rejected {w['rejected']} > tol "
                         f"{tol} (offered load is under the service rate)")
    if bound == "capped" and load == "over":
        if w["rejected"] <= 0:
            fails.append(f"over-capacity capped rejected {w['rejected']} "
                         f"(overflow should be shed)")

    # G9 over-capacity unbounded grows backlog past the cap.
    if bound == "unbounded" and load == "over":
        if w["peak_per_lane_queue_depth"] <= CAP:
            fails.append(f"over-capacity unbounded peak queue_depth "
                         f"{w['peak_per_lane_queue_depth']} did not exceed cap "
                         f"{CAP} (sustained overload should grow the queue)")

    # Sanity: completed service ~ requested (a floor, not a tail/latency gate).
    csvc = w["outcomes"]["completed_service_ms_p50"]
    if w["outcomes"]["completed"] and (csvc is None or csvc < 0.5 * w["service_ms"]):
        fails.append(f"completed service p50 {csvc} < {0.5 * w['service_ms']}")

    return fails


def _cell(name, load, cap, raw_dir, repeats, duration_ms, all_fails):
    interval_ms = INTERVAL_MS[load]
    cap_arg = -1 if cap is None else cap
    reps = []
    for rep in range(repeats):
        fname = f"{name}_r{rep}.json"
        out = os.path.join(raw_dir, fname)
        subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--worker",
             "--mode", name, "--load", load, "--num-lanes", str(NUM_LANES),
             "--hpx-threads", str(HPX_THREADS), "--service-ms", str(SERVICE_MS),
             "--cap", str(cap_arg), "--interval-ms", str(interval_ms),
             "--duration-ms", str(duration_ms), "--out", out],
            check=True, cwd=_REPO_ROOT, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE)
        w = _load(out)
        fails = _gate(w)
        if fails:
            all_fails.append((fname, fails))
        reps.append(w)

    rep0 = reps[0]  # representative concrete snapshots/trajectory
    return {
        "mode": name, "load": load, "num_lanes": NUM_LANES, "cap": cap,
        "service_ms": SERVICE_MS, "work_mode": WORK_MODE,
        "hpx_threads": HPX_THREADS, "interval_ms": interval_ms,
        "duration_ms": duration_ms, "repeats": repeats,
        "attempted_p50": int(statistics.median(w["attempted"] for w in reps)),
        "admitted_p50": int(statistics.median(w["admitted"] for w in reps)),
        "rejected_p50": int(statistics.median(w["rejected"] for w in reps)),
        "realized_attempt_rate_per_s_p50":
            _p50([w["realized_attempt_rate_per_s"] for w in reps]),
        "peak_per_lane_queue_depth_p50":
            int(statistics.median(w["peak_per_lane_queue_depth"] for w in reps)),
        "peak_per_lane_queue_depth_max":
            max(w["peak_per_lane_queue_depth"] for w in reps),
        "peak_total_queue_p50":
            int(statistics.median(w["peak_total_queue"] for w in reps)),
        "queue_depth_cap_respected":
            (all(w["peak_per_lane_queue_depth"] <= w["cap"] for w in reps)
             if cap is not None else None),
        "observed_depth_over_cap":
            (all(w["peak_per_lane_queue_depth"] > CAP for w in reps)
             if cap is None and load == "over" else None),
        "all_admitted_completed":
            all(w["outcomes"]["non_completed"] == 0 for w in reps),
        "rows_equal_admitted": all(w["rows"] == w["admitted"] for w in reps),
        "reached_idle": all(w["reached_idle"] for w in reps),
        "lanes_seen": rep0["lanes_seen"],
        "lane_count_min_p50": int(statistics.median(
            w["lane_count_min"] for w in reps)) if rep0["lane_count_min"]
            is not None else None,
        "lane_count_max_p50": int(statistics.median(
            w["lane_count_max"] for w in reps)) if rep0["lane_count_max"]
            is not None else None,
        "producer_span_ms_p50": _p50([w["producer_span_ms"] for w in reps]),
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
        "representative_first_sample": rep0["first_sample"],
        "representative_last_sample": rep0["last_sample"],
        "representative_idle_final": rep0["idle_final"],
        "representative_trajectory": rep0["trajectory"],
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run a single cell and write its JSON summary")
    ap.add_argument("--mode")
    ap.add_argument("--load")
    ap.add_argument("--num-lanes", type=int)
    ap.add_argument("--hpx-threads", type=int, default=HPX_THREADS)
    ap.add_argument("--service-ms", type=float, default=SERVICE_MS)
    ap.add_argument("--cap", type=int, default=-1,
                    help="per-lane cap; -1 == unbounded (None)")
    ap.add_argument("--interval-ms", type=float)
    ap.add_argument("--duration-ms", type=float)
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="shorter producer window, 1 repeat; no aggregate.json")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker(args)

    repeats = 1 if args.quick else REPEATS
    duration_ms = DURATION_MS_QUICK if args.quick else DURATION_MS_FULL

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results",
                           f"bounded_admission_offered_load_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp19] raw -> {raw_dir}")
    print(f"[exp19] work_mode={WORK_MODE} num_lanes={NUM_LANES} cap={CAP} "
          f"service_ms={SERVICE_MS} duration_ms={duration_ms} repeats={repeats} "
          f"intervals(ms)={INTERVAL_MS}")

    cells, all_fails = [], []
    for name, load, cap in CELLS:
        c = _cell(name, load, cap, raw_dir, repeats, duration_ms, all_fails)
        cells.append(c)
        print(f"  {name:26s} attempted={c['attempted_p50']:4d} "
              f"admitted={c['admitted_p50']:4d} rejected={c['rejected_p50']:4d} "
              f"rate={c['realized_attempt_rate_per_s_p50']}/s "
              f"peak_qd={c['peak_per_lane_queue_depth_p50']} "
              f"idle={c['reached_idle']} done_all={c['all_admitted_completed']} "
              f"lanes={c['lanes_seen']} "
              f"qwait_p99={c['completed_queue_wait_ms_p99']}ms")

    aggregate = {
        "experiment": "bounded_admission_offered_load",
        "boundary": "hpx-python-frontend",
        "instrument": "Engine(max_queue_depth_per_lane=cap) admission control + "
                      "Engine.lane_stats() observability snapshot (per-lane "
                      "{actor_id, queue_depth, active})",
        "submit": "facade Engine.submit (single-request); a duration-based "
                  "producer ATTEMPTS one submit per fixed inter-arrival interval "
                  "without retiring; capped mode catches QueueFullError per submit",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("Sustained offered-load companion to experiment 18's single "
                 "burst. max_queue_depth_per_lane is LOCAL, PER-LANE admission by "
                 "rejection. It is NOT Ray Serve backpressure, NOT distributed "
                 "flow control, and NOT blocking backpressure (submit returns by "
                 "raising QueueFullError; it never blocks waiting for space). The "
                 "cap counts queued-but-not-started work only -- the active "
                 "in-service request is extra -- so a lane's live footprint is at "
                 "most cap+1. Rejected requests are caller-visible exceptions, NOT "
                 "result rows (no Future, no row). Under sustained OVER-capacity "
                 "offered load the unbounded per-lane backlog grows past the cap "
                 "and keeps growing; the capped engine pins per-lane queue_depth at "
                 "the cap and sheds the overflow. Offered rates are nominal targets "
                 "(the host sleep granularity can stretch the interval); the gates "
                 "assert direction-of-change, not a rate, and the runner reports "
                 "the realized attempt rate. Latency/throughput magnitudes are "
                 "machine-specific and REPORTED, not gated. Raw per-run JSON is an "
                 "experiment-local scratch format under results/ (gitignored), NOT "
                 "the v1 benchmark schema."),
        "modes": {
            "below_capacity_unbounded":
                "cap=None, ~20 ms interval (~50 req/s, under service rate)",
            "below_capacity_capped":
                f"cap={CAP}, ~20 ms interval; sheds ~nothing",
            "over_capacity_unbounded":
                "cap=None, ~5 ms interval (~200 req/s); backlog grows past cap",
            "over_capacity_capped":
                f"cap={CAP}, ~5 ms interval; pins queue_depth at cap, sheds overflow",
        },
        "matrix": {"work_mode": WORK_MODE, "num_lanes": NUM_LANES, "cap": CAP,
                   "service_ms": SERVICE_MS, "hpx_threads": HPX_THREADS,
                   "interval_ms": INTERVAL_MS, "duration_ms": duration_ms,
                   "repeats": repeats,
                   "cells": [c[0] for c in CELLS]},
        "gates": [
            "G1 accounting: admitted+rejected==attempted; unbounded rejects 0",
            "G2 reject => no row: rows == admitted == attempted - rejected",
            "G3 admitted complete: every retired row status==completed",
            "G4 reaches idle: final num_active==0, total_queue==0",
            "G5 lane balance: completed rows span `lanes` lanes, min*2 >= max",
            "G6 capped bounds backlog: peak per-lane queue_depth <= cap",
            "G7 below-capacity capped near-zero shed: rejected <= "
            "ceil(REJECT_TOL_FRAC*attempted)",
            "G8 over-capacity capped sheds: rejected > 0",
            "G9 over-capacity unbounded grows backlog: peak per-lane "
            "queue_depth > cap",
        ],
        "reject_tol_frac": REJECT_TOL_FRAC,
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp19] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp19] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp19] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp19] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
