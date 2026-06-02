#!/usr/bin/env python3
"""Benchmark 09 runner: chunked synthetic service, cross-driver (Ray/HPX/rayx).

Drives the THREE existing benchmark drivers across a bounded chunked-service
matrix and rolls each run up with the canonical analyzer -- no new API, no schema
change, no analyzer change. It only invokes:

    bench/run_ray_baseline.py           (Ray actor baseline; sleep-only)
    hpx_impl/build/hpx_synthetic_baseline  (HPX-native; --work-mode sleep)
    bench/run_hpx_python_baseline.py    (rayx frontend; --api engine, sleep)
    bench/analyze_jsonl.py              (aggregate rollup, schema "1")

All three now expose --chunks / --chunk-delay-ms (single-submit modes): the TOTAL
active service_ms is split into `chunks` equal active steps with `chunks-1` parked
`chunk_delay_ms` gaps. This is synthetic timing only -- NOT real token streaming,
NOT model inference, NOT Ray Serve, no object-store/task semantics, no per-chunk
events: one request -> one row.

Retire mode is one_by_one and work mode is sleep across all engines (Ray is
sleep-only), so the comparison is apples-to-apples; the only backend difference is
the sleep-fidelity overshoot (Ray ~5% vs HPX ~25%, per experiment 01).

Per-run JSONL is scratch under results/ (gitignored). Tracked evidence: the
curated aggregate.json beside this script + the markdown report. One driver
subprocess per (engine, lanes, chunks, delay, repeat) cell.

Usage (repo root, venv active; _rayx built; native binary built; ray installed):
    python benchmarks/09_chunked_service_cross_driver/run_chunked_cross_driver.py
    python benchmarks/09_chunked_service_cross_driver/run_chunked_cross_driver.py --quick
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
PY = sys.executable
NATIVE_BIN = os.path.join(_REPO_ROOT, "hpx_impl", "build", "hpx_synthetic_baseline")
RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
ANALYZER = os.path.join("bench", "analyze_jsonl.py")

ENGINES = ("ray", "hpx", "rayx")
LANES = (1, 4)
SERVICE_MS = 20.0          # TOTAL active service per request
CHUNKS = (1, 4, 8)
DELAYS = (0.0, 2.0)        # parked inter-chunk gap (ms)
REPEATS = 2
REQUESTS = 100
WARMUP = 10
NUM_CPUS = 4               # Ray: >= max actors; HPX threads for hpx/rayx

ENGINE_BOUNDARY = {
    "ray": "ray-actor-process",
    "hpx": "hpx-intra-locality",
    "rayx": "hpx-python-frontend",
}

# Metrics carried from each analyzer summary into the aggregate.
METRICS = (
    "throughput_req_s",
    "total_ms_p50", "total_ms_p90", "total_ms_p99",
    "queue_wait_ms_p50", "queue_wait_ms_p90", "queue_wait_ms_p99",
    "service_ms_p50", "service_ms_p90", "service_ms_p99",
)


# --------------------------------------------------------------------------
def _driver_cmd(engine, lanes, chunks, delay, requests, out):
    sm, ch, dl = str(SERVICE_MS), str(chunks), str(delay)
    if engine == "ray":
        return [PY, "bench/run_ray_baseline.py", "--service-ms", sm,
                "--chunks", ch, "--chunk-delay-ms", dl,
                "--num-actors", str(lanes), "--concurrency", str(lanes),
                "--requests", str(requests), "--warmup-requests", str(WARMUP),
                "--retire-mode", "one_by_one", "--num-cpus", str(NUM_CPUS),
                "--out", out]
    if engine == "hpx":
        return [NATIVE_BIN, "--service-ms", sm, "--chunks", ch,
                "--chunk-delay-ms", dl, "--num-lanes", str(lanes),
                "--concurrency", str(lanes), "--requests", str(requests),
                "--warmup-requests", str(WARMUP), "--retire-mode", "one_by_one",
                "--work-mode", "sleep", "--out", out]
    return [PY, "bench/run_hpx_python_baseline.py", "--api", "engine",
            "--service-ms", sm, "--chunks", ch, "--chunk-delay-ms", dl,
            "--num-lanes", str(lanes), "--hpx-threads", str(NUM_CPUS),
            "--concurrency", str(lanes), "--requests", str(requests),
            "--warmup-requests", str(WARMUP), "--retire-mode", "one_by_one",
            "--work-mode", "sleep", "--out", out]


def _env():
    e = dict(os.environ)
    e["PYTHONPATH"] = RAYX_SRC + os.pathsep + e.get("PYTHONPATH", "")
    return e


def _analyze(path):
    proc = subprocess.run([PY, ANALYZER, path], cwd=_REPO_ROOT,
                          capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError("analyzer failed: " + proc.stderr.strip()[-200:])
    return json.loads(proc.stdout)


def _raw_check(path, requests, chunks, delay):
    """Structural checks on the raw JSONL: all completed, echo chunks/delay,
    schema 1. Returns failure strings (empty == ok) and the completed count."""
    fails = []
    with open(path) as fh:
        rows = [json.loads(ln) for ln in fh if ln.strip()]
    if len(rows) != requests:
        fails.append(f"rows {len(rows)} != requests {requests}")
    completed = sum(1 for r in rows if r.get("status") == "completed")
    if completed != len(rows):
        fails.append(f"completed {completed} != rows {len(rows)} (not all completed)")
    for r in rows:
        if r.get("schema_version") != "1":
            fails.append(f"schema_version {r.get('schema_version')!r} != '1'")
            break
    for r in rows:
        if int(r.get("chunks", -1)) != chunks or \
                float(r.get("chunk_delay_ms", -1)) != float(delay):
            fails.append(f"row chunks/delay ({r.get('chunks')},"
                         f"{r.get('chunk_delay_ms')}) != ({chunks},{delay})")
            break
    return fails, completed


def _run_repeat(engine, lanes, chunks, delay, requests, out, all_fails, where):
    cmd = _driver_cmd(engine, lanes, chunks, delay, requests, out)
    proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True,
                          env=_env())
    if proc.returncode != 0:
        all_fails.append((where, ["driver exit %d: %s" %
                                  (proc.returncode, proc.stderr.strip()[-200:])]))
        return None
    raw_fails, completed = _raw_check(out, requests, chunks, delay)
    if raw_fails:
        all_fails.append((where, raw_fails))
    summary = _analyze(out)
    if summary.get("schema_version") != "1":
        all_fails.append((where, [f"analyzer schema {summary.get('schema_version')!r}"]))
    return summary


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


# --------------------------------------------------------------------------
def _cross_cell_gates(cells, service_ms):
    """Loose, overshoot-aware bands (NOT tight timing asserts).

    (A) delay=0: splitting active service into more chunks must not blow up
        observed active service -- it stays ~flat (only per-sleep overshoot
        accumulates). Banded per engine/lanes: max/min service_ms_p50 < 1.8x.
    (B) delay>0: lifecycle grows by ~ (chunks-1)*delay vs the same cell at
        delay=0; gated as a wide band (sleep overshoot inflates the gap). For
        chunks==1 (no gaps) the delta must be ~0.
    """
    fails = []
    by = {(c["engine"], c["lanes"], c["chunks"], c["delay"]): c for c in cells}

    for engine in sorted({c["engine"] for c in cells}):
        for lanes in sorted({c["lanes"] for c in cells}):
            vals = [by[(engine, lanes, k, 0.0)]["service_ms_p50"]
                    for k in sorted({c["chunks"] for c in cells})
                    if (engine, lanes, k, 0.0) in by]
            vals = [v for v in vals if v]
            if vals and max(vals) > 1.8 * min(vals):
                fails.append(f"{engine} L{lanes} delay=0: active service "
                             f"varies with chunks {vals} (max > 1.8x min)")

    for (engine, lanes, chunks, delay), c in by.items():
        if delay <= 0:
            continue
        base = by.get((engine, lanes, chunks, 0.0))
        if not base or base["service_ms_p50"] is None or c["service_ms_p50"] is None:
            continue
        d = c["service_ms_p50"] - base["service_ms_p50"]
        expected = (chunks - 1) * delay
        if chunks == 1:
            if abs(d) > 3.0:
                fails.append(f"{engine} L{lanes} c1: delay added {d:.2f}ms but "
                             "chunks=1 has no gaps (expected ~0)")
        else:
            if not (0.4 * expected <= d <= 3.0 * expected):
                fails.append(f"{engine} L{lanes} c{chunks} d{delay}: lifecycle "
                             f"delta {d:.2f}ms outside loose band for expected "
                             f"~{expected}ms")
    return fails


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (lanes {1}, chunks {1,4}, delays {0,2}, "
                         "1 repeat, 20 requests) over all 3 engines; no "
                         "aggregate.json written")
    ap.add_argument("--engines", default=",".join(ENGINES),
                    help="comma list subset of ray,hpx,rayx (default all)")
    args = ap.parse_args(argv)

    engines = tuple(e for e in args.engines.split(",") if e in ENGINES)
    lanes_set, chunks_set, delays = LANES, CHUNKS, DELAYS
    repeats, requests = REPEATS, REQUESTS
    if args.quick:
        lanes_set, chunks_set, repeats, requests = (1,), (1, 4), 1, 20

    # Availability checks (skip an engine cleanly if its toolchain is absent).
    avail = []
    for e in engines:
        if e == "hpx" and not os.path.exists(NATIVE_BIN):
            print(f"[bench09] SKIP engine hpx: native binary not built")
            continue
        if e == "ray" and subprocess.run(
                [PY, "-c", "import ray"], capture_output=True).returncode != 0:
            print(f"[bench09] SKIP engine ray: ray not installed")
            continue
        if e == "rayx" and subprocess.run(
                [PY, "-c", f"import sys;sys.path.insert(0,{RAYX_SRC!r});import rayx"],
                capture_output=True).returncode != 0:
            print(f"[bench09] SKIP engine rayx: _rayx extension not importable")
            continue
        avail.append(e)

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"chunked_cross_driver_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    n_runs = len(avail) * len(lanes_set) * len(chunks_set) * len(delays) * repeats
    print(f"[bench09] raw -> {raw_dir}")
    print(f"[bench09] engines={avail} lanes={lanes_set} service_ms={SERVICE_MS} "
          f"chunks={chunks_set} delays={delays} repeats={repeats} "
          f"requests={requests} -> {n_runs} runs")

    cells, all_fails = [], []
    for engine in avail:
        for lanes in lanes_set:
            for chunks in chunks_set:
                for delay in delays:
                    per = {m: [] for m in METRICS}
                    completed_min = None
                    for rep in range(repeats):
                        fn = f"{engine}_l{lanes}_c{chunks}_d{int(delay)}_r{rep}.jsonl"
                        out = os.path.join(raw_dir, fn)
                        where = fn
                        summary = _run_repeat(engine, lanes, chunks, delay,
                                              requests, out, all_fails, where)
                        if summary is None:
                            continue
                        for m in METRICS:
                            per[m].append(summary.get(m))
                        c = summary.get("completed")
                        completed_min = c if completed_min is None else min(completed_min, c)
                    cell = {"engine": engine, "lanes": lanes, "chunks": chunks,
                            "delay": delay, "completed_min": completed_min}
                    for m in METRICS:
                        cell[m] = _med(per[m])
                    cells.append(cell)
                    print(f"  {engine:4s} L{lanes} c{chunks} d{int(delay)} "
                          f"thr={cell['throughput_req_s']} "
                          f"svc_p50={cell['service_ms_p50']} "
                          f"tot_p50={cell['total_ms_p50']} "
                          f"completed={completed_min}")

    cross = _cross_cell_gates(cells, SERVICE_MS)
    for f in cross:
        all_fails.append(("cross-cell", [f]))

    aggregate = {
        "benchmark": "chunked_service_cross_driver",
        "engines": {e: ENGINE_BOUNDARY[e] for e in avail},
        "retire_mode": "one_by_one",
        "work_mode": "sleep (all engines; Ray is sleep-only)",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("Chunked synthetic service across Ray / HPX-native / rayx: total "
                 "active service_ms split into `chunks` active sleeps with "
                 "`chunks-1` parked `chunk_delay_ms` gaps. Synthetic timing only "
                 "-- NOT token streaming, NOT inference, NOT Ray Serve, no "
                 "object-store/task semantics, no per-chunk events; one row per "
                 "request. service_ms_observed is lifecycle/lane-occupancy time "
                 "(active + parked gaps), not active-only when delay>0. Backends "
                 "differ in sleep overshoot (Ray ~5% vs HPX ~25%, experiment 01); "
                 "magnitudes are machine-specific. Cells are medians across "
                 "repeats. Raw per-run JSONL is scratch under results/ "
                 "(gitignored); benchmark JSONL schema stays version 1."),
        "matrix": {"engines": list(avail), "lanes": list(lanes_set),
                   "service_ms": SERVICE_MS, "chunks": list(chunks_set),
                   "chunk_delay_ms": list(delays), "repeats": repeats,
                   "requests": requests, "warmup_requests": WARMUP},
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[bench09] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[bench09] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[bench09] GATES FAILED ({len(all_fails)}):")
        for where, fails in all_fails:
            print(f"  {where}: {fails}")
        return 1
    print(f"[bench09] all gates passed; {len(cells)} cells, {n_runs} runs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
