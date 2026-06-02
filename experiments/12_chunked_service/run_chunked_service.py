#!/usr/bin/env python3
"""Experiment 12 runner: chunked synthetic service.

Validates and characterizes the v1 chunked synthetic-service primitive added to
the rayx frontend: engine.submit(service_ms, chunks, chunk_delay_ms, work_mode).
A request services in `chunks` equal active steps (total active = service_ms,
split chunks ways) separated by `chunks-1` PARKED inter-chunk gaps of
chunk_delay_ms. It is synthetic timing only -- NOT real token streaming, not
payload execution, no per-chunk rows/events: one request -> one future -> one
final row that echoes chunks/chunk_delay_ms.

This is an EXPERIMENT-LOCAL runner (the benchmark driver stays unchunked). Each
cell submits N single requests (a backlog), drains as-completed, and records one
row per request. The lane-side `service_ms_observed` (end_ns - start_ns, C++
steady clock) is the per-request LIFECYCLE span -- active service plus the parked
inter-chunk gaps -- and is queue-position-independent, so it is the clean signal
for chunk characterization.

NO new RayX API beyond the implemented chunked single-submit, NO driver change,
NO result-row / benchmark-JSONL schema change. The per-run JSONL written here is
an experiment-local scratch format (under results/, gitignored), not the v1
benchmark schema. Tracked evidence: the curated aggregate.json + this report.

One subprocess per cell (the rayx Engine owns one HPX runtime per process).

Usage (repo root, venv active, _rayx built):
    python experiments/12_chunked_service/run_chunked_service.py
    python experiments/12_chunked_service/run_chunked_service.py --quick
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
_BENCH = os.path.join(_REPO_ROOT, "bench")
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
for p in (_BENCH, _RAYX_SRC):
    if p not in sys.path:
        sys.path.insert(0, p)

import analyze_jsonl  # noqa: E402  (reuse the canonical percentile helper)

WORK_MODES = ("sleep", "spin")
LANES = (1, 4, 8)
HPX_THREADS = 4
SERVICE_MS = 8.0           # TOTAL active service per request
CHUNKS = (1, 2, 4, 8)
CHUNK_DELAYS = (0.0, 2.0)  # parked inter-chunk gap (ms)
REQUESTS = 48
REPEATS = 3


# --------------------------------------------------------------------------
# Worker: run ONE cell in its own process and write experiment-local JSONL.
# --------------------------------------------------------------------------
def _worker(args):
    from rayx import Engine

    n = args.requests
    with Engine(num_lanes=args.num_lanes, hpx_threads=args.hpx_threads) as engine:
        # Warmup: a few quick requests drained so the measured path is warm.
        for f in [engine.submit(service_ms=1.0, work_mode=args.work_mode)
                  for _ in range(max(2 * args.num_lanes, 4))]:
            f.result()

        # Submit the batch as single chunked requests (a backlog), then drain
        # as-completed and reconstruct input order.
        futs = [engine.submit(service_ms=args.service_ms, chunks=args.chunks,
                              chunk_delay_ms=args.chunk_delay_ms,
                              work_mode=args.work_mode, label=f"r{i}")
                for i in range(n)]
        idx_of = {id(f): i for i, f in enumerate(futs)}
        rows = [None] * n
        inflight = list(futs)
        while inflight:
            ready, inflight = engine.wait(inflight, num_returns=1)
            recv_ns = time.perf_counter_ns()
            for f in ready:
                r = f.result(recv_ns=recv_ns)
                i = idx_of[id(f)]
                rows[i] = {
                    "idx": i,
                    "label": f"r{i}",
                    "label_echoed": r["label"],
                    "chunks": r["chunks"],
                    "chunk_delay_ms": r["chunk_delay_ms"],
                    "req_chunks": args.chunks,
                    "req_chunk_delay_ms": args.chunk_delay_ms,
                    "actor_id": r["actor_id"],
                    "status": r["status"],
                    "submit_ns": r["submit_ns"],
                    "total_ms": r["total_ms"],
                    "service_ms_observed": r["service_ms_observed"],
                    "work_mode": args.work_mode,
                    "lanes": args.num_lanes,
                }

    with open(args.out, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return 0


# --------------------------------------------------------------------------
# Orchestrator helpers
# --------------------------------------------------------------------------
def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _load(path):
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _gate(rows, n, num_lanes, work_mode, chunks, chunk_delay_ms, service_ms):
    """Per-cell structural gates. Returns failure strings."""
    fails = []
    # (1) one submitted request -> exactly one final row (no per-chunk rows).
    if len(rows) != n:
        fails.append(f"rows {len(rows)} != {n} (expected one row per request)")
    # (3) all complete (no cancellation in this experiment).
    if any(r["status"] != "completed" for r in rows):
        fails.append("not all rows completed")
    # (2) row echoes chunks / chunk_delay_ms matching what was requested.
    for r in rows:
        if r["chunks"] != chunks:
            fails.append(f"row chunks {r['chunks']} != requested {chunks}")
            break
        if r["chunk_delay_ms"] != chunk_delay_ms:
            fails.append(f"row chunk_delay_ms {r['chunk_delay_ms']} != "
                         f"{chunk_delay_ms}")
            break
    # (4) label preserved.
    if any(r["label_echoed"] != r["label"] for r in rows):
        fails.append("label not preserved on some row")
    # (7) round-robin lane balance.
    from collections import Counter
    lc = Counter(r["actor_id"] for r in rows)
    if len(lc) != num_lanes:
        fails.append(f"lanes_seen {len(lc)} != {num_lanes}")
    if lc and max(lc.values()) - min(lc.values()) > 1:
        fails.append(f"lane imbalance {min(lc.values())}-{max(lc.values())}")
    # observed lifecycle is non-negative.
    if any(r["service_ms_observed"] < 0 for r in rows):
        fails.append("negative service_ms_observed")
    return fails


def _cell(work_mode, lanes, chunks, delay, raw_dir, repeats, requests,
          service_ms, all_fails):
    svc50, tot50, lanes_seen, lane_min, lane_max = [], [], [], [], []
    for rep in range(repeats):
        fname = f"{work_mode}_l{lanes}_c{chunks}_d{int(delay)}_r{rep}.jsonl"
        out = os.path.join(raw_dir, fname)
        subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--worker",
             "--work-mode", work_mode, "--num-lanes", str(lanes),
             "--hpx-threads", str(HPX_THREADS), "--requests", str(requests),
             "--service-ms", str(service_ms), "--chunks", str(chunks),
             "--chunk-delay-ms", str(delay), "--out", out],
            check=True, cwd=_REPO_ROOT, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE)
        rows = _load(out)
        fails = _gate(rows, requests, lanes, work_mode, chunks, delay, service_ms)
        if fails:
            all_fails.append((fname, fails))
        svc50.append(analyze_jsonl._pcts(
            [r["service_ms_observed"] for r in rows])["p50"])
        tot50.append(analyze_jsonl._pcts([r["total_ms"] for r in rows])["p50"])
        from collections import Counter
        lc = Counter(r["actor_id"] for r in rows)
        lanes_seen.append(len(lc))
        lane_min.append(min(lc.values()))
        lane_max.append(max(lc.values()))
    return {
        "work_mode": work_mode, "lanes": lanes, "chunks": chunks,
        "chunk_delay_ms": delay, "service_ms": service_ms, "requests": requests,
        "repeats": repeats,
        "service_ms_observed_p50": _med(svc50),
        "total_ms_p50": _med(tot50),
        "lanes_seen": int(statistics.median(lanes_seen)),
        "lane_min_count": int(statistics.median(lane_min)),
        "lane_max_count": int(statistics.median(lane_max)),
    }


def _cross_cell_gates(cells, service_ms):
    """Cross-cell structural checks (loose, overshoot-aware), keyed by cell.

    (5) spin, delay=0: splitting the same total active service into more chunks
        must NOT change observed active service much (band, not exact).
    (6) delay>0 vs delay=0 at the same (mode,lanes,chunks): the lifecycle grows
        by approximately (chunks-1)*chunk_delay_ms -- gated as a LOOSE band to
        avoid fragile timing (the parked gap carries the sleep-timer overshoot).
        For chunks==1 there are no gaps, so the delta must be ~0.
    """
    fails = []
    by = {(c["work_mode"], c["lanes"], c["chunks"], c["chunk_delay_ms"]): c
          for c in cells}

    # (5) spin delay=0 chunk-invariance of active service (per lanes).
    for lanes in sorted({c["lanes"] for c in cells}):
        vals = [by[("spin", lanes, k, 0.0)]["service_ms_observed_p50"]
                for k in sorted({c["chunks"] for c in cells})
                if ("spin", lanes, k, 0.0) in by]
        vals = [v for v in vals if v]
        if vals and max(vals) > 1.6 * min(vals):
            fails.append(f"spin L{lanes} delay=0: active varies with chunks "
                         f"{vals} (max>1.6x min)")

    # (6) delay effect band.
    for (mode, lanes, chunks, delay), c in by.items():
        if delay <= 0:
            continue
        base = by.get((mode, lanes, chunks, 0.0))
        if not base:
            continue
        d = c["service_ms_observed_p50"] - base["service_ms_observed_p50"]
        expected = (chunks - 1) * delay
        if chunks == 1:
            if abs(d) > 2.0:
                fails.append(f"{mode} L{lanes} c1: delay added {d:.2f}ms but "
                             "chunks=1 has no gaps (expected ~0)")
        else:
            if not (0.5 * expected <= d <= 2.5 * expected):
                fails.append(f"{mode} L{lanes} c{chunks} d{delay}: lifecycle "
                             f"delta {d:.2f}ms outside loose band for expected "
                             f"~{expected}ms")
    return fails


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run a single cell and write its JSONL")
    ap.add_argument("--work-mode")
    ap.add_argument("--num-lanes", type=int)
    ap.add_argument("--hpx-threads", type=int, default=HPX_THREADS)
    ap.add_argument("--requests", type=int, default=REQUESTS)
    ap.add_argument("--service-ms", type=float, default=SERVICE_MS)
    ap.add_argument("--chunks", type=int)
    ap.add_argument("--chunk-delay-ms", type=float)
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (sleep+spin, lanes {1,4}, chunks {1,4}, "
                         "delays {0,2}, 1 repeat, 24 requests); no aggregate.json")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker(args)

    lanes_set, chunks_set, delays, repeats, requests = (
        LANES, CHUNKS, CHUNK_DELAYS, REPEATS, REQUESTS)
    if args.quick:
        lanes_set, chunks_set, repeats, requests = (1, 4), (1, 4), 1, 24

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"chunked_service_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp12] raw -> {raw_dir}")
    print(f"[exp12] work_mode={WORK_MODES} lanes={lanes_set} chunks={chunks_set} "
          f"delays={delays} service_ms={SERVICE_MS} requests={requests} "
          f"repeats={repeats}")

    cells, all_fails = [], []
    for work_mode in WORK_MODES:
        for lanes in lanes_set:
            for chunks in chunks_set:
                for delay in delays:
                    c = _cell(work_mode, lanes, chunks, delay, raw_dir, repeats,
                              requests, SERVICE_MS, all_fails)
                    cells.append(c)
                    print(f"  {work_mode:5s} L{lanes:<2d} chunks={chunks} "
                          f"delay={delay:.0f} svc_obs_p50={c['service_ms_observed_p50']:>7.3f} "
                          f"tot_p50={c['total_ms_p50']:>8.2f} "
                          f"lane[{c['lane_min_count']}-{c['lane_max_count']}]")

    cross = _cross_cell_gates(cells, SERVICE_MS)
    for f in cross:
        all_fails.append(("cross-cell", [f]))

    aggregate = {
        "experiment": "chunked_service",
        "boundary": "hpx-python-frontend",
        "submit": "facade Engine.submit single-request, chunked "
                  "(service_ms total active split over chunks; parked inter-chunk gap)",
        "retire": "as-completed Engine.wait, per-sweep recv_ns, input order",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("Chunked synthetic service: synthetic timing only, NOT real "
                 "token streaming. One request -> one row. With chunk_delay_ms>0, "
                 "service_ms_observed is lifecycle/lane-occupancy time (active "
                 "service + the chunks-1 PARKED gaps), not active-only; the parked "
                 "gap carries the sleep-timer overshoot. Raw per-run JSONL is an "
                 "experiment-local scratch format under results/ (gitignored), "
                 "NOT the v1 benchmark schema."),
        "matrix": {"work_mode": list(WORK_MODES), "num_lanes": list(lanes_set),
                   "chunks": list(chunks_set), "chunk_delay_ms": list(delays),
                   "service_ms": SERVICE_MS, "requests": requests,
                   "hpx_threads": HPX_THREADS, "repeats": repeats},
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp12] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp12] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp12] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp12] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
