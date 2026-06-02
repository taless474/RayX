#!/usr/bin/env python3
"""Experiment 09 runner: spin core-boundary lane x hpx-threads sweep.

Follow-up to experiment 08. Sharpens the high-lane flattening finding by sweeping
num_lanes AROUND the L8 knee and varying hpx_threads, primarily under
work_mode="spin", with a small set of sleep control cells. The key question:
does the spin knee MOVE when hpx_threads changes (worker/core scheduling
pressure) or stay fixed (some other ceiling)?

Reuses the EXISTING rayx driver (bench/run_hpx_python_baseline.py --api engine)
and the EXISTING analyzer (bench/analyze_jsonl.py). Adds NO RayX API, NO driver
change, NO schema change: lane (actor_id) distribution is read from the existing
per-row actor_id. One driver subprocess per (work_mode, threads, lanes,
service_ms, repeat) cell (the rayx Engine owns one HPX runtime per process).

concurrency is fixed at 16 (per the experiment design): the knee-vs-threads
comparison is made across thread counts at the SAME (lanes, concurrency), so the
fixed-window offered-depth confound (offered_depth = concurrency/lanes, which
falls to ~1 at L16) cancels out of that comparison. offered_depth is recorded
per cell so high-lane absolute efficiency can be read with that caveat.

Raw per-run JSONL -> results/ (gitignored). Tracked evidence: the curated
aggregate.json beside this script + the markdown report.

Usage (repo root, venv active, _rayx built):
    python experiments/09_spin_core_boundary_sweep/run_spin_core_boundary_sweep.py
    python experiments/09_spin_core_boundary_sweep/run_spin_core_boundary_sweep.py --quick
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

CONCURRENCY = 16            # fixed (see module docstring)
REQUESTS = 500
WARMUP = 50
REPEATS = 3
SPIN_FIDELITY_TOL = 0.05   # 5% on service_ms_p50 (shape gate, not a perf assert)

# Primary spin sweep: lanes around the L8 knee x worker-thread counts x service.
SPIN_LANES = (4, 6, 8, 10, 12, 16)
SPIN_THREADS = (2, 4, 8)
SPIN_SERVICE_MS = (1, 5)

# Small sleep control set (svc=5): show sleep does not knee and is
# thread-insensitive. Each tuple is (lanes, hpx_threads).
SLEEP_CONTROLS = ((4, 4), (8, 4), (12, 4), (8, 2), (8, 8))
SLEEP_SERVICE_MS = 5


def _run_one(work_mode, lanes, threads, service_ms, requests, warmup, out_path):
    cmd = [
        sys.executable, DRIVER,
        "--api", "engine",
        "--work-mode", work_mode,
        "--num-lanes", str(lanes),
        "--hpx-threads", str(threads),
        "--service-ms", str(service_ms),
        "--concurrency", str(CONCURRENCY),
        "--requests", str(requests),
        "--warmup-requests", str(warmup),
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


def _gate(work_mode, service_ms, summary, meta, requests):
    fails = []
    if meta["n_rows"] != requests:
        fails.append(f"rows {meta['n_rows']} != {requests}")
    if meta["n_unique_ids"] != requests:
        fails.append(f"unique ids {meta['n_unique_ids']} != {requests}")
    if summary["completed"] != requests:
        fails.append(f"completed {summary['completed']} != {requests}")
    if summary["failed"]:
        fails.append(f"failed={summary['failed']}")
    if work_mode == "spin" and service_ms > 0:
        p50 = summary["service_ms_p50"]
        if abs(p50 - service_ms) > SPIN_FIDELITY_TOL * service_ms:
            fails.append(f"spin svc_p50 {p50} off target {service_ms}")
    return fails


def _median(xs):
    return round(statistics.median(xs), 4)


def _cell(work_mode, lanes, threads, service_ms, raw_dir, repeats, requests,
          warmup, all_fails):
    thr, t50, t90, t99 = [], [], [], []
    s50, s90, s99, q50 = [], [], [], []
    lanes_seen, lane_min, lane_max = [], [], []
    for rep in range(repeats):
        fname = (f"{work_mode}_t{threads}_svc{service_ms}_l{lanes}"
                 f"_r{rep}.jsonl")
        out_path = os.path.join(raw_dir, fname)
        summary, lane_counts, meta = _run_one(
            work_mode, lanes, threads, service_ms, requests, warmup, out_path)
        fails = _gate(work_mode, service_ms, summary, meta, requests)
        if fails:
            all_fails.append((fname, fails))
        thr.append(summary["throughput_req_s"])
        t50.append(summary["total_ms_p50"])
        t90.append(summary["total_ms_p90"])
        t99.append(summary["total_ms_p99"])
        s50.append(summary["service_ms_p50"])
        s90.append(summary["service_ms_p90"])
        s99.append(summary["service_ms_p99"])
        q50.append(summary["queue_wait_ms_p50"])
        lanes_seen.append(len(lane_counts))
        lane_min.append(min(lane_counts.values()))
        lane_max.append(max(lane_counts.values()))
    thr_med = _median(thr)
    ideal = lanes * (1000.0 / service_ms)
    return {
        "work_mode": work_mode,
        "service_ms": service_ms,
        "hpx_threads": threads,
        "lanes": lanes,
        "concurrency": CONCURRENCY,
        "offered_depth": round(CONCURRENCY / lanes, 3),
        "requests": requests,
        "repeats": repeats,
        "throughput_req_s": thr_med,
        "throughput_min": round(min(thr), 1),
        "throughput_max": round(max(thr), 1),
        "per_lane_efficiency": round(thr_med / ideal, 4) if ideal else None,
        "total_ms_p50": _median(t50),
        "total_ms_p90": _median(t90),
        "total_ms_p99": _median(t99),
        "service_ms_p50": _median(s50),
        "service_ms_p90": _median(s90),
        "service_ms_p99": _median(s99),
        "queue_wait_ms_p50": _median(q50),
        "lanes_seen": int(statistics.median(lanes_seen)),
        "lane_min_count": int(statistics.median(lane_min)),
        "lane_max_count": int(statistics.median(lane_max)),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="Tiny smoke matrix (spin lanes {4,8} x threads {4} x "
                         "svc {1}, 1 repeat, few requests); no aggregate.json.")
    args = ap.parse_args(argv)

    spin_lanes, spin_threads, spin_svc = SPIN_LANES, SPIN_THREADS, SPIN_SERVICE_MS
    sleep_controls, sleep_svc = SLEEP_CONTROLS, SLEEP_SERVICE_MS
    repeats, requests, warmup = REPEATS, REQUESTS, WARMUP
    if args.quick:
        spin_lanes, spin_threads, spin_svc = (4, 8), (4,), (1,)
        sleep_controls = ((8, 4),)
        repeats, requests, warmup = 1, 80, 10

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"spin_core_boundary_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)

    cells = []
    all_fails = []
    print(f"[exp09] raw -> {raw_dir}")
    print(f"[exp09] spin: lanes={spin_lanes} threads={spin_threads} "
          f"svc={spin_svc} | sleep controls={sleep_controls} svc={sleep_svc} | "
          f"concurrency={CONCURRENCY} requests={requests} repeats={repeats}")

    print("-- spin sweep --")
    for service_ms in spin_svc:
        for threads in spin_threads:
            for lanes in spin_lanes:
                c = _cell("spin", lanes, threads, service_ms, raw_dir, repeats,
                          requests, warmup, all_fails)
                cells.append(c)
                print(f"  spin t{threads} svc={service_ms} L{lanes:<2d} "
                      f"thr={c['throughput_req_s']:>8.1f} "
                      f"eff={c['per_lane_efficiency']:.3f} "
                      f"depth={c['offered_depth']:.2f} "
                      f"svc_p50={c['service_ms_p50']:.4f} "
                      f"t_p99={c['total_ms_p99']:.3f} "
                      f"lane[{c['lane_min_count']}-{c['lane_max_count']}]")

    print("-- sleep controls --")
    for (lanes, threads) in sleep_controls:
        c = _cell("sleep", lanes, threads, sleep_svc, raw_dir, repeats,
                  requests, warmup, all_fails)
        cells.append(c)
        print(f"  sleep t{threads} svc={sleep_svc} L{lanes:<2d} "
              f"thr={c['throughput_req_s']:>8.1f} "
              f"eff={c['per_lane_efficiency']:.3f} "
              f"svc_p50={c['service_ms_p50']:.4f} "
              f"t_p99={c['total_ms_p99']:.3f} "
              f"lane[{c['lane_min_count']}-{c['lane_max_count']}]")

    aggregate = {
        "experiment": "spin_core_boundary_sweep",
        "boundary": "hpx-python-frontend",
        "api": "engine",
        "retire_mode": "one_by_one",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("concurrency fixed at 16; offered_depth = concurrency/lanes "
                 "(falls to ~1 at L16). The knee-vs-threads comparison is made "
                 "across hpx_threads at the same (lanes, concurrency)."),
        "matrix": {
            "spin": {"lanes": list(spin_lanes), "hpx_threads": list(spin_threads),
                     "service_ms": list(spin_svc)},
            "sleep_controls": [list(t) for t in sleep_controls],
            "sleep_service_ms": sleep_svc,
            "concurrency": CONCURRENCY, "requests": requests,
            "warmup_requests": warmup, "repeats": repeats,
        },
        "cells": cells,
    }
    agg_path = os.path.join(_HERE, "aggregate.json")
    if not args.quick:
        with open(agg_path, "w") as f:
            json.dump(aggregate, f, indent=2)
        print(f"[exp09] wrote {agg_path}")
    else:
        print("[exp09] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp09] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp09] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
