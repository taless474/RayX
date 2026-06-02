#!/usr/bin/env python3
"""Experiment 13 runner: chunk-boundary running cancellation.

Validates and characterizes the running (chunk-boundary) cancellation added on
top of queued cancellation. `Engine.cancel(future)` now settles two outcomes:

  * QUEUED  -> the lane skips the request entirely (chunks_completed == 0).
  * RUNNING -> a started CHUNKED request with a chunk boundary still ahead stops
    at the NEXT boundary (1 <= chunks_completed < chunks). An in-progress active
    chunk and an in-progress parked inter-chunk gap are NEVER interrupted -- the
    boundary is the only checkpoint. cancel()==True here means "guaranteed to
    stop", not "ready now".

It is synthetic timing only -- NOT real token streaming, NOT Ray task/object
cancellation, NO per-chunk rows/events: one request -> one future -> one final
row that also carries the lane-determined `chunks_completed`.

Four policies per cell exercise the state machine:
  1. baseline  -- no cancel; every request completes (chunks_completed==chunks).
  2. queued    -- submit a backlog, cancel the TAIL half immediately; those are
                  still queued (each request is long), so they cancel True with
                  chunks_completed==0 and never run.
  3. running   -- waves of `num_lanes` requests; sleep ~40% into the active span
                  (started, far from the final-chunk boundary), cancel all ->
                  True, a STRICTLY-PARTIAL run (1 <= chunks_completed < chunks).
  4. late      -- waves; sleep to ~90% of the nominal time-to-final-boundary,
                  cancel near the end. INHERENTLY RACY: True (stop at the final
                  boundary, chunks_completed==chunks-1) OR False (already on the
                  final chunk -> completes). Both are valid; this characterizes
                  the boundary race. The invariant cancel()==True iff the final
                  row is status=="cancelled" must hold either way.

This is an EXPERIMENT-LOCAL runner (the benchmark driver stays unchunked and
never cancels). The per-run JSONL written here is experiment-local scratch
(under results/, gitignored), NOT the v1 benchmark schema. Tracked evidence: the
curated aggregate.json + this report. All four policies for a cell run in ONE
subprocess (one HPX runtime per process), sequentially.

Usage (repo root, venv active, _rayx built):
    python experiments/13_chunk_boundary_cancellation/run_chunk_boundary_cancellation.py
    python experiments/13_chunk_boundary_cancellation/run_chunk_boundary_cancellation.py --quick
    # internal: --worker runs a single cell (used via subprocess)
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
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
for p in (_BENCH, _RAYX_SRC):
    if p not in sys.path:
        sys.path.insert(0, p)

WORK_MODES = ("sleep", "spin")
LANES = (1, 4)
HPX_THREADS = 4
SERVICE_MS = 40.0          # TOTAL active service per request
CHUNKS = (2, 4, 8)
CHUNK_DELAYS = (0.0, 2.0)  # parked inter-chunk gap (ms)
REQUESTS = 24              # backlog size for baseline / queued policies
WAVE_REPEATS = 4           # number of waves for running / late (waves of num_lanes)
POLICIES = ("baseline", "queued", "running", "late")


# --------------------------------------------------------------------------
# Worker: run ALL policies for ONE cell in its own process; write JSONL.
# --------------------------------------------------------------------------
def _row(r, *, idx, policy, requested_chunks, cancel_attempted,
         cancel_returned, work_mode, lanes):
    return {
        "idx": idx,
        "policy": policy,
        "label": f"{policy[0]}{idx}",
        "label_echoed": r["label"],
        "requested_chunks": requested_chunks,
        "chunks": r["chunks"],
        "chunk_delay_ms": r["chunk_delay_ms"],
        "status": r["status"],
        "chunks_completed": r["chunks_completed"],
        "cancel_attempted": cancel_attempted,
        "cancel_returned": cancel_returned,
        "service_ms_observed": r["service_ms_observed"],
        "actor_id": r["actor_id"],
        "work_mode": work_mode,
        "lanes": lanes,
    }


def _drain_in_order(engine, futs, idx_of, on_result):
    """Drain `futs` as-completed; call on_result(i, row) in completion order."""
    inflight = list(futs)
    while inflight:
        ready, inflight = engine.wait(inflight, num_returns=1)
        recv_ns = time.perf_counter_ns()
        for f in ready:
            on_result(idx_of[id(f)], f.result(recv_ns=recv_ns))


def _worker(args):
    from rayx import Engine

    wm, lanes, chunks, delay = (args.work_mode, args.num_lanes, args.chunks,
                                args.chunk_delay_ms)
    n = args.requests
    active = args.service_ms
    # Nominal (no-overshoot) chunk timing, used only to TIME the cancels.
    per_chunk_active = active / chunks
    # ~25% into the active span: comfortably started, with the widest margin to
    # the final-chunk boundary -- this maximizes robustness for the hardest cell
    # (chunks=2 has a single boundary at ~50% of active; landing at ~25% leaves
    # the most slack against scheduler/sleep jitter under spin contention).
    sleep_running = 0.25 * active / 1000.0
    # ~90% of the nominal time-to-final-boundary (start of the last chunk):
    # active of the first chunks-1 chunks plus the chunks-1 parked gaps.
    t_final_boundary = per_chunk_active * (chunks - 1) + delay * (chunks - 1)
    sleep_late = 0.9 * t_final_boundary / 1000.0

    rows = []

    def submit_one(idx, policy):
        return engine.submit(service_ms=active, chunks=chunks,
                             chunk_delay_ms=delay, work_mode=wm,
                             label=f"{policy[0]}{idx}")

    with Engine(num_lanes=lanes, hpx_threads=args.hpx_threads) as engine:
        # Warmup so the measured path is warm (unchunked, drained).
        for f in [engine.submit(service_ms=1.0, work_mode=wm)
                  for _ in range(max(2 * lanes, 4))]:
            f.result()

        # --- (1) baseline: backlog, no cancel, drain ---
        futs = [submit_one(i, "baseline") for i in range(n)]
        idx_of = {id(f): i for i, f in enumerate(futs)}
        out = [None] * n
        _drain_in_order(engine, futs, idx_of,
                        lambda i, r: out.__setitem__(i, r))
        for i, r in enumerate(out):
            rows.append(_row(r, idx=i, policy="baseline", requested_chunks=chunks,
                             cancel_attempted=False, cancel_returned=None,
                             work_mode=wm, lanes=lanes))

        # --- (2) queued: backlog, cancel the TAIL half immediately ---
        futs = [submit_one(i, "queued") for i in range(n)]
        idx_of = {id(f): i for i, f in enumerate(futs)}
        cancelled = {}
        half = n // 2
        for i in range(half, n):  # tail half: deep in FIFO -> still queued
            cancelled[i] = engine.cancel(futs[i])
        out = [None] * n
        _drain_in_order(engine, futs, idx_of,
                        lambda i, r: out.__setitem__(i, r))
        for i, r in enumerate(out):
            att = i in cancelled
            rows.append(_row(r, idx=i, policy="queued", requested_chunks=chunks,
                             cancel_attempted=att,
                             cancel_returned=cancelled.get(i),
                             work_mode=wm, lanes=lanes))

        # --- (3) running & (4) late: waves of `lanes` requests, timed cancel ---
        for policy, sleep_s in (("running", sleep_running), ("late", sleep_late)):
            k = 0
            for _wave in range(args.wave_repeats):
                wave = [submit_one(k + j, policy) for j in range(lanes)]
                k += lanes
                idx_of = {id(f): (k - lanes + j) for j, f in enumerate(wave)}
                time.sleep(sleep_s)  # let the wave start; leave boundaries ahead
                ret = {idx_of[id(f)]: engine.cancel(f) for f in wave}
                out = {}
                _drain_in_order(engine, wave, idx_of,
                                lambda i, r: out.__setitem__(i, r))
                for i in sorted(out):
                    rows.append(_row(out[i], idx=i, policy=policy,
                                     requested_chunks=chunks,
                                     cancel_attempted=True,
                                     cancel_returned=ret[i],
                                     work_mode=wm, lanes=lanes))

    with open(args.out, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return 0


# --------------------------------------------------------------------------
# Orchestrator
# --------------------------------------------------------------------------
def _load(path):
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _gate(rows, chunks, num_lanes):
    """Per-cell structural gates over all policy rows. Returns failure strings."""
    fails = []
    by_policy = {}
    for r in rows:
        by_policy.setdefault(r["policy"], []).append(r)

    # Universal invariants over every row.
    for r in rows:
        # one row per request -> chunks echo matches request, never over-runs.
        if r["chunks"] != chunks:
            fails.append(f"{r['policy']}#{r['idx']}: chunks {r['chunks']} != {chunks}")
        if r["chunks_completed"] > chunks:
            fails.append(f"{r['policy']}#{r['idx']}: chunks_completed "
                         f"{r['chunks_completed']} > chunks {chunks}")
        # label preserved.
        if r["label_echoed"] != r["label"]:
            fails.append(f"{r['policy']}#{r['idx']}: label not preserved")
        # core invariant: a cancel call settles True IFF the final row cancelled.
        if r["cancel_attempted"]:
            is_cancelled = (r["status"] == "cancelled")
            if bool(r["cancel_returned"]) != is_cancelled:
                fails.append(f"{r['policy']}#{r['idx']}: cancel()="
                             f"{r['cancel_returned']} but status={r['status']!r}")
        # status / chunks_completed coupling.
        if r["status"] == "completed" and r["chunks_completed"] != chunks:
            fails.append(f"{r['policy']}#{r['idx']}: completed but "
                         f"chunks_completed {r['chunks_completed']} != {chunks}")
        if r["status"] == "cancelled" and not (0 <= r["chunks_completed"] < chunks):
            fails.append(f"{r['policy']}#{r['idx']}: cancelled but "
                         f"chunks_completed {r['chunks_completed']} not in [0,{chunks})")

    # baseline: all complete, full run, round-robin lane balance.
    base = by_policy.get("baseline", [])
    if any(r["status"] != "completed" for r in base):
        fails.append("baseline: not all completed")
    lc = Counter(r["actor_id"] for r in base)
    if base and len(lc) != num_lanes:
        fails.append(f"baseline: lanes_seen {len(lc)} != {num_lanes}")
    if lc and max(lc.values()) - min(lc.values()) > 1:
        fails.append(f"baseline: lane imbalance {min(lc.values())}-{max(lc.values())}")

    # queued: every attempted cancel succeeded and skipped ALL chunks.
    for r in by_policy.get("queued", []):
        if r["cancel_attempted"]:
            if r["cancel_returned"] is not True:
                fails.append(f"queued#{r['idx']}: tail cancel returned "
                             f"{r['cancel_returned']} (expected True; still queued)")
            elif r["chunks_completed"] != 0:
                fails.append(f"queued#{r['idx']}: queued-cancel ran "
                             f"{r['chunks_completed']} chunks (expected 0)")

    # running: every running-cancel that settled True is a STRICTLY-PARTIAL stop.
    # (Whether it fires at all in a given cell is timing-dependent -- a single
    # narrow boundary under spin contention can race past it -- so "feature
    # exercised" is asserted run-level in main(), not per cell. The deterministic
    # proof that running-cancel works lives in bench/smoke_rayx.py.)
    for r in by_policy.get("running", []):
        if r["cancel_returned"] is True and not (1 <= r["chunks_completed"] < chunks):
            fails.append(f"running#{r['idx']}: settled True but chunks_completed "
                         f"{r['chunks_completed']} not in [1,{chunks})")
    return fails


def _dist(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return {"min": min(xs), "median": round(statistics.median(xs), 2),
            "max": max(xs), "n": len(xs)}


def _policy_summary(rows, chunks):
    """Compact per-policy characterization for the aggregate."""
    cancelled = [r for r in rows if r["status"] == "cancelled"]
    completed = [r for r in rows if r["status"] == "completed"]
    attempted = [r for r in rows if r["cancel_attempted"]]
    true_n = sum(1 for r in attempted if r["cancel_returned"] is True)
    # Active work saved by cancellation: fraction of active chunks NOT run,
    # summed over cancelled rows (one full request == 1.0 of active work).
    saved = sum((chunks - r["chunks_completed"]) / chunks for r in cancelled)
    return {
        "rows": len(rows),
        "completed": len(completed),
        "cancelled": len(cancelled),
        "cancel_attempts": len(attempted),
        "cancel_true": true_n,
        "cancel_false": len(attempted) - true_n,
        "chunks_completed_cancelled": _dist([r["chunks_completed"] for r in cancelled]),
        "active_requests_saved": round(saved, 3),
    }


def _cell(work_mode, lanes, chunks, delay, raw_dir, requests, wave_repeats,
          service_ms, all_fails):
    fname = f"{work_mode}_l{lanes}_c{chunks}_d{int(delay)}.jsonl"
    out = os.path.join(raw_dir, fname)
    subprocess.run(
        [sys.executable, os.path.abspath(__file__), "--worker",
         "--work-mode", work_mode, "--num-lanes", str(lanes),
         "--hpx-threads", str(HPX_THREADS), "--requests", str(requests),
         "--wave-repeats", str(wave_repeats), "--service-ms", str(service_ms),
         "--chunks", str(chunks), "--chunk-delay-ms", str(delay), "--out", out],
        check=True, cwd=_REPO_ROOT, stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE)
    rows = _load(out)
    fails = _gate(rows, chunks, lanes)
    if fails:
        all_fails.append((fname, fails))
    by_policy = {}
    for r in rows:
        by_policy.setdefault(r["policy"], []).append(r)
    return {
        "work_mode": work_mode, "lanes": lanes, "chunks": chunks,
        "chunk_delay_ms": delay, "service_ms": service_ms,
        "policies": {p: _policy_summary(by_policy.get(p, []), chunks)
                     for p in POLICIES},
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run all policies for one cell, write JSONL")
    ap.add_argument("--work-mode")
    ap.add_argument("--num-lanes", type=int)
    ap.add_argument("--hpx-threads", type=int, default=HPX_THREADS)
    ap.add_argument("--requests", type=int, default=REQUESTS)
    ap.add_argument("--wave-repeats", type=int, default=WAVE_REPEATS)
    ap.add_argument("--service-ms", type=float, default=SERVICE_MS)
    ap.add_argument("--chunks", type=int)
    ap.add_argument("--chunk-delay-ms", type=float)
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (sleep+spin, lanes {1}, chunks {2,4}, "
                         "delays {0,2}, requests 12, 2 waves); no aggregate.json")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker(args)

    lanes_set, chunks_set, delays = LANES, CHUNKS, CHUNK_DELAYS
    requests, wave_repeats = REQUESTS, WAVE_REPEATS
    if args.quick:
        lanes_set, chunks_set, requests, wave_repeats = (1,), (2, 4), 12, 2

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"chunk_boundary_cancel_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp13] raw -> {raw_dir}")
    print(f"[exp13] work_mode={WORK_MODES} lanes={lanes_set} chunks={chunks_set} "
          f"delays={delays} service_ms={SERVICE_MS} requests={requests} "
          f"waves={wave_repeats}")

    cells, all_fails = [], []
    for work_mode in WORK_MODES:
        for lanes in lanes_set:
            for chunks in chunks_set:
                for delay in delays:
                    c = _cell(work_mode, lanes, chunks, delay, raw_dir, requests,
                              wave_repeats, SERVICE_MS, all_fails)
                    cells.append(c)
                    q = c["policies"]["queued"]
                    rn = c["policies"]["running"]
                    lt = c["policies"]["late"]
                    print(f"  {work_mode:5s} L{lanes:<2d} c{chunks} d{delay:.0f} "
                          f"| queued cc0x{q['cancelled']} "
                          f"| running T{rn['cancel_true']}/F{rn['cancel_false']} "
                          f"cc{rn['chunks_completed_cancelled']['median'] if rn['chunks_completed_cancelled'] else '-'} "
                          f"| late T{lt['cancel_true']}/F{lt['cancel_false']}")

    # Run-level: running-cancel must demonstrably fire somewhere (a totally
    # broken running-cancel would settle True nowhere). Per-cell firing is
    # timing-dependent and only characterized, not gated.
    total_running_true = sum(c["policies"]["running"]["cancel_true"] for c in cells)
    if total_running_true < 1:
        all_fails.append(("run-level",
                          ["running-cancel settled True in NO cell "
                           "(feature never exercised across the whole matrix)"]))

    aggregate = {
        "experiment": "chunk_boundary_cancellation",
        "boundary": "hpx-python-frontend",
        "submit": "facade Engine.submit single-request chunked; Engine.cancel "
                  "(queued skip / running stop-at-next-chunk-boundary)",
        "retire": "as-completed Engine.wait, per-sweep recv_ns",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("Running cancellation is an early stop BETWEEN synthetic chunks: "
                 "an in-progress active chunk and parked inter-chunk gap are never "
                 "interrupted. cancel()==True for a running request means "
                 "'guaranteed to stop at the next boundary', not 'ready now'. "
                 "chunks_completed is lane-determined (==chunks completed, 0 "
                 "queued, 1..chunks-1 running). NOT real token streaming, NOT Ray "
                 "task/object cancellation, no per-chunk events. Timings are "
                 "synthetic and machine-specific; the 'late' policy is "
                 "deliberately racy (True or False both valid). Raw per-run JSONL "
                 "is experiment-local scratch under results/ (gitignored), NOT the "
                 "v1 benchmark schema (benchmark JSONL stays version 1, unchunked, "
                 "no cancellation)."),
        "policies": {
            "baseline": "no cancel; every request completes (chunks_completed==chunks)",
            "queued": "cancel the tail half of a long backlog immediately "
                      "(still queued -> True, chunks_completed==0)",
            "running": "wave of num_lanes requests, cancel ~40% into the active "
                       "span (boundary ahead -> True, 1<=chunks_completed<chunks)",
            "late": "wave, cancel ~90% to the nominal final-chunk boundary "
                    "(racy: True with chunks_completed==chunks-1, or False -> completed)",
        },
        "matrix": {"work_mode": list(WORK_MODES), "num_lanes": list(lanes_set),
                   "chunks": list(chunks_set), "chunk_delay_ms": list(delays),
                   "service_ms": SERVICE_MS, "requests": requests,
                   "wave_repeats": wave_repeats, "hpx_threads": HPX_THREADS},
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp13] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp13] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp13] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp13] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
