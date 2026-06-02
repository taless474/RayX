#!/usr/bin/env python3
"""Experiment 08 runner: rayx-frontend sleep-vs-spin coordination sweep.

Drives the EXISTING rayx Python-frontend driver (bench/run_hpx_python_baseline.py)
across a bounded sleep-vs-spin lane sweep, summarizes each run with the EXISTING
analyzer (bench/analyze_jsonl.py), and writes a curated aggregate.json beside this
script. It adds NO new RayX API, NO driver change, and NO schema change: the lane
(actor_id) distribution is computed by reading the existing per-row actor_id, not
by extending any schema.

One driver subprocess per (work_mode, lanes, service_ms, repeat) cell -- the rayx
Engine owns one HPX runtime per process, so each run must be its own process
(HPX-as-a-library is not restarted in-process).

Raw per-run JSONL goes under results/ (gitignored); only the curated aggregate.json
+ the markdown report are tracked. Gates per run: completed == requests, unique
request_ids, and (spin only) service-time fidelity |service_ms_p50 - service_ms|
within tolerance.

Usage (from repo root, venv active, _rayx built):
    python experiments/08_spin_vs_sleep_coordination/run_spin_vs_sleep.py
    python experiments/08_spin_vs_sleep_coordination/run_spin_vs_sleep.py --quick
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from collections import Counter

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
_BENCH = os.path.join(_REPO_ROOT, "bench")
if _BENCH not in sys.path:
    sys.path.insert(0, _BENCH)

import analyze_jsonl  # noqa: E402  (reuse the canonical summarizer)

DRIVER = os.path.join(_BENCH, "run_hpx_python_baseline.py")

# Bounded but useful matrix. concurrency >= max lanes so every lane is offered
# load (underload -> saturation is driven by lane count, not by starvation).
WORK_MODES = ("sleep", "spin")
LANES = (1, 2, 4, 8)
SERVICE_MS = (1, 5)
CONCURRENCY = 16
HPX_THREADS = 4
REQUESTS = 500
WARMUP = 50
REPEATS = 3
# Spin is CPU-bound on the metrics clock, so it should track service_ms tightly.
SPIN_FIDELITY_TOL = 0.05  # 5% on the p50 (shape gate, not a perf assertion)


def _run_one(work_mode, lanes, service_ms, out_path):
    """Run the driver once; return (summary, lane_counts, rows_meta)."""
    cmd = [
        sys.executable, DRIVER,
        "--api", "engine",
        "--work-mode", work_mode,
        "--num-lanes", str(lanes),
        "--service-ms", str(service_ms),
        "--concurrency", str(CONCURRENCY),
        "--hpx-threads", str(HPX_THREADS),
        "--requests", str(REQUESTS),
        "--warmup-requests", str(WARMUP),
        "--out", out_path,
    ]
    subprocess.run(cmd, check=True, cwd=_REPO_ROOT,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    rows = analyze_jsonl.load(out_path)
    summary = analyze_jsonl.summarize(rows)
    lane_counts = Counter(r["actor_id"] for r in rows)
    req_ids = [r["request_id"] for r in rows]
    meta = {"n_rows": len(rows), "n_unique_ids": len(set(req_ids))}
    return summary, lane_counts, meta


def _gate(work_mode, service_ms, summary, meta):
    """Integrity + (spin) fidelity gates. Returns a list of failure strings."""
    fails = []
    if meta["n_rows"] != REQUESTS:
        fails.append(f"rows {meta['n_rows']} != {REQUESTS}")
    if meta["n_unique_ids"] != REQUESTS:
        fails.append(f"unique ids {meta['n_unique_ids']} != {REQUESTS}")
    if summary["completed"] != REQUESTS:
        fails.append(f"completed {summary['completed']} != {REQUESTS}")
    if summary["failed"]:
        fails.append(f"failed={summary['failed']}")
    if work_mode == "spin" and service_ms > 0:
        p50 = summary["service_ms_p50"]
        if abs(p50 - service_ms) > SPIN_FIDELITY_TOL * service_ms:
            fails.append(f"spin svc_p50 {p50} off target {service_ms} "
                         f"(> {SPIN_FIDELITY_TOL:.0%})")
    return fails


def _median(xs):
    return round(statistics.median(xs), 4)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="Smaller smoke matrix (lanes {1,4}, svc {1}, 1 repeat, "
                         "fewer requests) to validate the runner end to end.")
    ap.add_argument("--keep-raw", action="store_true",
                    help="Keep the raw per-run JSONL (default: kept under "
                         "results/, which is gitignored).")
    args = ap.parse_args(argv)

    lanes_set, svc_set, repeats, requests, warmup = (
        LANES, SERVICE_MS, REPEATS, REQUESTS, WARMUP)
    if args.quick:
        lanes_set, svc_set, repeats = (1, 4), (1,), 1
        globals()["REQUESTS"] = 80
        globals()["WARMUP"] = 10
        requests = 80

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"spin_vs_sleep_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)

    cells = []
    all_fails = []
    print(f"[exp08] raw -> {raw_dir}")
    print(f"[exp08] matrix: work_mode={WORK_MODES} lanes={lanes_set} "
          f"service_ms={svc_set} concurrency={CONCURRENCY} "
          f"hpx_threads={HPX_THREADS} requests={REQUESTS} repeats={repeats}")

    for work_mode in WORK_MODES:
        for service_ms in svc_set:
            for lanes in lanes_set:
                thr, t50, t99, s50, s99, q50 = [], [], [], [], [], []
                lanes_seen, lane_min, lane_max = [], [], []
                for rep in range(repeats):
                    fname = (f"{work_mode}_svc{service_ms}_l{lanes}"
                             f"_r{rep}.jsonl")
                    out_path = os.path.join(raw_dir, fname)
                    summary, lane_counts, meta = _run_one(
                        work_mode, lanes, service_ms, out_path)
                    fails = _gate(work_mode, service_ms, summary, meta)
                    if fails:
                        all_fails.append((fname, fails))
                    thr.append(summary["throughput_req_s"])
                    t50.append(summary["total_ms_p50"])
                    t99.append(summary["total_ms_p99"])
                    s50.append(summary["service_ms_p50"])
                    s99.append(summary["service_ms_p99"])
                    q50.append(summary["queue_wait_ms_p50"])
                    lanes_seen.append(len(lane_counts))
                    lane_min.append(min(lane_counts.values()))
                    lane_max.append(max(lane_counts.values()))

                thr_med = _median(thr)
                ideal = lanes * (1000.0 / service_ms)  # req/s at full efficiency
                per_lane_eff = round(thr_med / ideal, 4) if ideal else None
                cells.append({
                    "work_mode": work_mode,
                    "service_ms": service_ms,
                    "lanes": lanes,
                    "concurrency": CONCURRENCY,
                    "hpx_threads": HPX_THREADS,
                    "requests": REQUESTS,
                    "repeats": repeats,
                    "throughput_req_s": thr_med,
                    "throughput_min": round(min(thr), 1),
                    "throughput_max": round(max(thr), 1),
                    "per_lane_efficiency": per_lane_eff,
                    "total_ms_p50": _median(t50),
                    "total_ms_p99": _median(t99),
                    "service_ms_p50": _median(s50),
                    "service_ms_p99": _median(s99),
                    "queue_wait_ms_p50": _median(q50),
                    "lanes_seen": int(statistics.median(lanes_seen)),
                    "lane_min_count": int(statistics.median(lane_min)),
                    "lane_max_count": int(statistics.median(lane_max)),
                })
                c = cells[-1]
                print(f"  {work_mode:5s} svc={service_ms} L{lanes:<2d} "
                      f"thr={c['throughput_req_s']:>8.1f} "
                      f"eff={c['per_lane_efficiency']:.3f} "
                      f"svc_p50={c['service_ms_p50']:.4f} "
                      f"t_p50={c['total_ms_p50']:.3f} "
                      f"lanes={c['lanes_seen']} "
                      f"lane[{c['lane_min_count']}-{c['lane_max_count']}]")

    aggregate = {
        "experiment": "spin_vs_sleep_coordination",
        "boundary": "hpx-python-frontend",
        "api": "engine",
        "retire_mode": "one_by_one",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "matrix": {
            "work_mode": list(WORK_MODES),
            "num_lanes": list(lanes_set),
            "service_ms": list(svc_set),
            "concurrency": CONCURRENCY,
            "hpx_threads": HPX_THREADS,
            "requests": REQUESTS,
            "warmup_requests": warmup,
            "repeats": repeats,
        },
        "cells": cells,
    }
    agg_path = os.path.join(_HERE, "aggregate.json")
    if not args.quick:
        with open(agg_path, "w") as f:
            json.dump(aggregate, f, indent=2)
        print(f"[exp08] wrote {agg_path}")
    else:
        print("[exp08] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp08] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp08] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
