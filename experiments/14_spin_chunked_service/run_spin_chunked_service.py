#!/usr/bin/env python3
"""Experiment 14 runner: SPIN chunked synthetic service, HPX-native + rayx.

Characterizes chunked synthetic service under work_mode="spin" (active, CPU-bound)
for the two HPX-backed engines ONLY -- HPX-native and the rayx Python frontend --
deliberately WITHOUT Ray. Removing the sleep timer (benchmark 09 was sleep-only,
all three engines) strips the ~25% blocking-sleep overshoot so chunk behaviour is
read on the clean active-CPU axis: does splitting the same total active service
into more chunks preserve it, and where do chunk/lane/core pressure start to bite?

Drives the TWO existing benchmark drivers across a bounded matrix and rolls each
run up with the canonical analyzer -- no new API, no schema change, no analyzer
change. It only invokes:

    hpx_impl/build/hpx_synthetic_baseline  (HPX-native; --work-mode spin)
    bench/run_hpx_python_baseline.py       (rayx frontend; --api engine, spin)
    bench/analyze_jsonl.py                 (aggregate rollup, schema "1")

Each request's TOTAL active service_ms is split into `chunks` equal active spin
steps with `chunks-1` parked `chunk_delay_ms` gaps. Synthetic timing only -- NOT
real token streaming, NOT model inference, NOT Ray Serve, NOT a Ray comparison, no
object-store/task semantics, no per-chunk events: one request -> one row.

service_ms_observed (= end_ns - start_ns, C++ steady clock) is the per-request
LIFECYCLE span: active spin plus the parked inter-chunk gaps. At delay=0 that is
active-only; at delay>0 it is lane-occupancy time (active + parked gaps).

Per-run JSONL is scratch under results/ (gitignored). Tracked evidence: the
curated aggregate.json beside this script + the markdown report. One driver
subprocess per (engine, lanes, chunks, delay, repeat) cell.

Usage (repo root, venv active; _rayx built; native binary built):
    python experiments/14_spin_chunked_service/run_spin_chunked_service.py
    python experiments/14_spin_chunked_service/run_spin_chunked_service.py --quick
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

PY = sys.executable
NATIVE_BIN = os.path.join(_REPO_ROOT, "hpx_impl", "build", "hpx_synthetic_baseline")
RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
RAYX_DRIVER = os.path.join(_BENCH, "run_hpx_python_baseline.py")

ENGINES = ("hpx", "rayx")
WORK_MODE = "spin"          # this experiment is spin-only by design
LANES = (1, 4, 8)
HPX_THREADS = 4
SERVICE_MS = 8.0            # TOTAL active service per request
CHUNKS = (1, 2, 4, 8)
DELAYS = (0.0, 2.0)        # parked inter-chunk gap (ms)
REPEATS = 2
REQUESTS = 100
WARMUP = 10

ENGINE_BOUNDARY = {
    "hpx": "hpx-intra-locality",
    "rayx": "hpx-python-frontend",
}

# Loose, shape-not-magnitude bands (NOT exact timing asserts).
SPIN_LOW = 0.85            # spin delay=0 service_ms_p50 >= 0.85 * target
SPIN_HIGH = 1.40          # ... and <= 1.40 * target (chunk overhead headroom)
CHUNK_INVAR = 1.40        # delay=0: max service_ms_p50 < 1.40x min across chunks
DELAY_LO, DELAY_HI = 0.5, 2.5   # delay>0 lifecycle delta band vs (chunks-1)*delay
C1_DELAY_TOL = 2.0        # chunks=1 has no gaps -> |delta| must stay under this


def _env():
    e = dict(os.environ)
    e["PYTHONPATH"] = RAYX_SRC + os.pathsep + e.get("PYTHONPATH", "")
    return e


def _driver_cmd(engine, lanes, chunks, delay, requests, out):
    sm, ch, dl = str(SERVICE_MS), str(chunks), str(delay)
    if engine == "hpx":
        # Pin native HPX worker threads to match rayx's hpx_threads, so the
        # cross-engine comparison is apples-to-apples (the native binary passes
        # --hpx:... flags through to the runtime). Without this, native would use
        # its default worker-thread count and the spin throughput/efficiency
        # comparison at high lanes would be confounded by thread count, not a real
        # engine difference (service fidelity is identical either way).
        return [NATIVE_BIN, f"--hpx:threads={HPX_THREADS}",
                "--service-ms", sm, "--chunks", ch,
                "--chunk-delay-ms", dl, "--num-lanes", str(lanes),
                "--concurrency", str(lanes), "--requests", str(requests),
                "--warmup-requests", str(WARMUP), "--retire-mode", "one_by_one",
                "--work-mode", WORK_MODE, "--out", out]
    return [PY, RAYX_DRIVER, "--api", "engine", "--service-ms", sm,
            "--chunks", ch, "--chunk-delay-ms", dl, "--num-lanes", str(lanes),
            "--hpx-threads", str(HPX_THREADS), "--concurrency", str(lanes),
            "--requests", str(requests), "--warmup-requests", str(WARMUP),
            "--retire-mode", "one_by_one", "--work-mode", WORK_MODE, "--out", out]


def _raw_gate(rows, requests, chunks, delay):
    """Per-run structural gates on the raw JSONL. Returns failure strings."""
    fails = []
    if len(rows) != requests:
        fails.append(f"rows {len(rows)} != requests {requests}")
    if len({r.get("request_id") for r in rows}) != len(rows):
        fails.append("duplicate request_id")
    completed = sum(1 for r in rows if r.get("status") == "completed")
    if completed != len(rows):
        fails.append(f"completed {completed} != rows {len(rows)}")
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
    for r in rows:
        if r.get("work_mode") != WORK_MODE:
            fails.append(f"work_mode {r.get('work_mode')!r} != {WORK_MODE!r}")
            break
    if any(r.get("service_ms_observed", 0.0) < 0 for r in rows):
        fails.append("negative service_ms_observed")
    return fails


def _run_repeat(engine, lanes, chunks, delay, requests, out, all_fails, where):
    cmd = _driver_cmd(engine, lanes, chunks, delay, requests, out)
    proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True,
                          env=_env())
    if proc.returncode != 0:
        all_fails.append((where, ["driver exit %d: %s" %
                                  (proc.returncode, proc.stderr.strip()[-200:])]))
        return None
    rows = analyze_jsonl.load(out)
    raw_fails = _raw_gate(rows, requests, chunks, delay)
    if raw_fails:
        all_fails.append((where, raw_fails))
    summary = analyze_jsonl.summarize(rows)
    if summary.get("schema_version") != "1":
        all_fails.append((where, [f"analyzer schema {summary.get('schema_version')!r}"]))
    lane_counts = Counter(r["actor_id"] for r in rows)
    return summary, lane_counts


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


METRICS = (
    "throughput_req_s",
    "total_ms_p50", "total_ms_p90", "total_ms_p99",
    "queue_wait_ms_p50", "queue_wait_ms_p90", "queue_wait_ms_p99",
    "service_ms_p50", "service_ms_p90", "service_ms_p99",
)


def _cell(engine, lanes, chunks, delay, raw_dir, repeats, requests, all_fails):
    per = {m: [] for m in METRICS}
    lanes_seen, lane_min, lane_max, completed_min = [], [], [], None
    for rep in range(repeats):
        fname = f"{engine}_l{lanes}_c{chunks}_d{int(delay)}_r{rep}.jsonl"
        out = os.path.join(raw_dir, fname)
        res = _run_repeat(engine, lanes, chunks, delay, requests, out,
                          all_fails, fname)
        if res is None:
            continue
        summary, lane_counts = res
        for m in METRICS:
            per[m].append(summary.get(m))
        c = summary.get("completed")
        completed_min = c if completed_min is None else min(completed_min, c)
        lanes_seen.append(len(lane_counts))
        lane_min.append(min(lane_counts.values()))
        lane_max.append(max(lane_counts.values()))
        # round-robin lane balance gate (per run)
        if len(lane_counts) != lanes:
            all_fails.append((fname, [f"lanes_seen {len(lane_counts)} != {lanes}"]))
        elif max(lane_counts.values()) - min(lane_counts.values()) > 1:
            all_fails.append((fname, [f"lane imbalance "
                                      f"{min(lane_counts.values())}-"
                                      f"{max(lane_counts.values())}"]))
    ideal = lanes * (1000.0 / SERVICE_MS)
    thr = _med(per["throughput_req_s"])
    cell = {"engine": engine, "lanes": lanes, "chunks": chunks, "delay": delay,
            "service_ms": SERVICE_MS, "requests": requests, "repeats": repeats,
            "completed_min": completed_min,
            "per_lane_efficiency": round(thr / ideal, 4) if (thr and ideal) else None,
            "lanes_seen": int(statistics.median(lanes_seen)) if lanes_seen else None,
            "lane_min_count": int(statistics.median(lane_min)) if lane_min else None,
            "lane_max_count": int(statistics.median(lane_max)) if lane_max else None}
    for m in METRICS:
        cell[m] = _med(per[m])
    return cell


def _cross_cell_gates(cells):
    """Cross-cell shape gates (loose, overshoot-aware), keyed by cell.

    (A) spin delay=0: splitting the same total active service into more chunks
        must NOT change observed active service much -- it stays chunk-invariant
        within a band, AND each cell's service_ms_p50 stays near the target.
    (B) delay>0 vs delay=0 at the same (engine,lanes,chunks): lifecycle grows by
        approximately (chunks-1)*delay -- a LOOSE band (the parked gap carries its
        own timer overshoot). chunks==1 has no gaps, so the delta must be ~0.
    """
    fails = []
    by = {(c["engine"], c["lanes"], c["chunks"], c["delay"]): c for c in cells}
    chunk_set = sorted({c["chunks"] for c in cells})

    for engine in sorted({c["engine"] for c in cells}):
        for lanes in sorted({c["lanes"] for c in cells}):
            vals = [by[(engine, lanes, k, 0.0)]["service_ms_p50"]
                    for k in chunk_set if (engine, lanes, k, 0.0) in by]
            vals = [v for v in vals if v]
            if not vals:
                continue
            if max(vals) > CHUNK_INVAR * min(vals):
                fails.append(f"{engine} L{lanes} spin delay=0: active service "
                             f"varies with chunks {vals} (max > {CHUNK_INVAR}x min)")
            lo, hi = SPIN_LOW * SERVICE_MS, SPIN_HIGH * SERVICE_MS
            off = [round(v, 3) for v in vals if not (lo <= v <= hi)]
            if off:
                fails.append(f"{engine} L{lanes} spin delay=0: service_ms_p50 "
                             f"{off} outside [{lo:.2f},{hi:.2f}] target band")

    for (engine, lanes, chunks, delay), c in by.items():
        if delay <= 0:
            continue
        base = by.get((engine, lanes, chunks, 0.0))
        if not base or base["service_ms_p50"] is None or c["service_ms_p50"] is None:
            continue
        d = c["service_ms_p50"] - base["service_ms_p50"]
        expected = (chunks - 1) * delay
        if chunks == 1:
            if abs(d) > C1_DELAY_TOL:
                fails.append(f"{engine} L{lanes} c1: delay added {d:.2f}ms but "
                             "chunks=1 has no gaps (expected ~0)")
        elif not (DELAY_LO * expected <= d <= DELAY_HI * expected):
            fails.append(f"{engine} L{lanes} c{chunks} d{delay}: lifecycle delta "
                         f"{d:.2f}ms outside loose band for expected ~{expected}ms")
    return fails


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (lanes {1,4}, chunks {1,4}, delays {0,2}, "
                         "1 repeat, 24 requests) over both engines; no "
                         "aggregate.json written")
    ap.add_argument("--engines", default=",".join(ENGINES),
                    help="comma list subset of hpx,rayx (default both)")
    args = ap.parse_args(argv)

    engines = tuple(e for e in args.engines.split(",") if e in ENGINES)
    lanes_set, chunks_set, delays = LANES, CHUNKS, DELAYS
    repeats, requests = REPEATS, REQUESTS
    if args.quick:
        lanes_set, chunks_set, repeats, requests = (1, 4), (1, 4), 1, 24

    avail = []
    for e in engines:
        if e == "hpx" and not os.path.exists(NATIVE_BIN):
            print("[exp14] SKIP engine hpx: native binary not built")
            continue
        if e == "rayx" and subprocess.run(
                [PY, "-c", f"import sys;sys.path.insert(0,{RAYX_SRC!r});import rayx"],
                capture_output=True).returncode != 0:
            print("[exp14] SKIP engine rayx: _rayx extension not importable")
            continue
        avail.append(e)

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"spin_chunked_service_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    n_runs = len(avail) * len(lanes_set) * len(chunks_set) * len(delays) * repeats
    print(f"[exp14] raw -> {raw_dir}")
    print(f"[exp14] engines={avail} work_mode={WORK_MODE} lanes={lanes_set} "
          f"hpx_threads={HPX_THREADS} service_ms={SERVICE_MS} chunks={chunks_set} "
          f"delays={delays} repeats={repeats} requests={requests} -> {n_runs} runs")

    cells, all_fails = [], []
    for engine in avail:
        for lanes in lanes_set:
            for chunks in chunks_set:
                for delay in delays:
                    c = _cell(engine, lanes, chunks, delay, raw_dir, repeats,
                              requests, all_fails)
                    cells.append(c)
                    print(f"  {engine:4s} L{lanes:<2d} c{chunks} d{int(delay)} "
                          f"thr={c['throughput_req_s']} "
                          f"svc_p50={c['service_ms_p50']} "
                          f"svc_p99={c['service_ms_p99']} "
                          f"tot_p50={c['total_ms_p50']} "
                          f"eff={c['per_lane_efficiency']} "
                          f"lane[{c['lane_min_count']}-{c['lane_max_count']}] "
                          f"completed={c['completed_min']}")

    for f in _cross_cell_gates(cells):
        all_fails.append(("cross-cell", [f]))

    aggregate = {
        "experiment": "spin_chunked_service",
        "engines": {e: ENGINE_BOUNDARY[e] for e in avail},
        "work_mode": WORK_MODE,
        "retire_mode": "one_by_one",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("SPIN chunked synthetic service, HPX-native + rayx ONLY (no Ray). "
                 "Total active service_ms split into `chunks` active spin steps "
                 "with `chunks-1` parked `chunk_delay_ms` gaps. Spin removes the "
                 "blocking-sleep timer overshoot, so delay=0 isolates active-CPU "
                 "chunk behaviour. Synthetic timing only -- NOT token streaming, "
                 "NOT inference, NOT Ray Serve, NOT a Ray comparison, no "
                 "object-store/task semantics, no per-chunk events; one row per "
                 "request. service_ms_observed is active-only at delay=0 and "
                 "lifecycle/lane-occupancy (active + parked gaps) at delay>0. L8 "
                 "behaviour reads against the experiment 08/09 core-boundary / "
                 "oversubscription findings. Cells are medians across repeats; raw "
                 "per-run JSONL is scratch under results/ (gitignored); benchmark "
                 "JSONL schema stays version 1; magnitudes are machine-specific."),
        "matrix": {"engines": list(avail), "work_mode": WORK_MODE,
                   "lanes": list(lanes_set), "hpx_threads": HPX_THREADS,
                   "service_ms": SERVICE_MS, "chunks": list(chunks_set),
                   "chunk_delay_ms": list(delays), "repeats": repeats,
                   "requests": requests, "warmup_requests": WARMUP},
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp14] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp14] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp14] GATES FAILED ({len(all_fails)}):")
        for where, fails in all_fails:
            print(f"  {where}: {fails}")
        return 1
    print(f"[exp14] all gates passed; {len(cells)} cells, {n_runs} runs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
