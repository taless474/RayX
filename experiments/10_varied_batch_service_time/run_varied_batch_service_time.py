#!/usr/bin/env python3
"""Experiment 10 runner: true bulk-varied synthetic service time.

Exercises the new varied batch path -- engine.submit_batch(service_ms=[...]) --
to characterize how lane assignment, completion latency, and tails behave when a
SINGLE true batch crossing mixes short and long synthetic durations. This is the
feature's real purpose: heterogeneous durations inside one batch crossing, NOT a
Python loop over scalar submits.

Design:
  * Submission is ONE true submit_batch crossing (varied list, or scalar for the
    uniform baseline). The facade stamps every returned future with ONE shared
    Python submit_ns -- the runner verifies this (gate), which is what
    distinguishes the true batch path from a Python loop over submit().
  * Retirement is as-completed (Engine.wait, per-sweep recv_ns), with rows
    reconstructed in input order. So total_ms reflects true completion latency
    (isolating the lane-queue / convoy effect), not client retire order.

Reuses the facade and analyze_jsonl._pcts for percentiles. NO new RayX API, NO
driver change, NO result-row / benchmark-JSONL schema change: the per-request
short/long class is derived from the known input pattern, and the raw per-run
JSONL written here is an EXPERIMENT-LOCAL scratch format (under results/,
gitignored), not the v1 benchmark schema.

One subprocess per cell (the rayx Engine owns one HPX runtime per process).

Usage (repo root, venv active, _rayx built):
    python experiments/10_varied_batch_service_time/run_varied_batch_service_time.py
    python experiments/10_varied_batch_service_time/run_varied_batch_service_time.py --quick
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
LANES = (4, 8)
HPX_THREADS = 4
BATCH_SIZE = 128
REPEATS = 3
# (short_ms, long_ms) per pattern; "uniform" is the scalar baseline.
PATTERNS = ("uniform5", "alt_1_10", "block_sl_1_10", "block_ls_1_10",
            "rare_long_1_20")


def _pattern_spec(name, n):
    """Return (kind, requested_list_or_scalar, short_ms, long_ms) for a pattern.

    kind is 'scalar' (uniform baseline) or 'varied'. ``n`` is the batch size.
    short_ms/long_ms are the two duration classes (None for uniform).
    """
    if name == "uniform5":
        return "scalar", 5.0, None, None
    if name == "alt_1_10":
        return "varied", [1.0 if i % 2 == 0 else 10.0 for i in range(n)], 1.0, 10.0
    if name == "block_sl_1_10":
        return "varied", [1.0] * (n // 2) + [10.0] * (n - n // 2), 1.0, 10.0
    if name == "block_ls_1_10":
        return "varied", [10.0] * (n // 2) + [1.0] * (n - n // 2), 1.0, 10.0
    if name == "rare_long_1_20":
        return "varied", [20.0 if i % 10 == 0 else 1.0 for i in range(n)], 1.0, 20.0
    raise ValueError(f"unknown pattern {name!r}")


# --------------------------------------------------------------------------
# Worker: run ONE cell in its own process and write experiment-local JSONL.
# --------------------------------------------------------------------------
def _worker(args):
    from rayx import Engine
    kind, requested, short_ms, long_ms = _pattern_spec(args.pattern,
                                                       args.batch_size)
    if kind == "scalar":
        requested = [float(requested)] * args.batch_size

    with Engine(num_lanes=args.num_lanes, hpx_threads=args.hpx_threads) as engine:
        # Warmup: a discarded scalar batch so the measured path is warm.
        for f in engine.submit_batch(service_ms=1.0,
                                     count=max(2 * args.num_lanes, 4),
                                     work_mode=args.work_mode):
            f.result()

        # ONE true batch crossing (varied list or scalar). All returned futures
        # share one Python submit_ns (verified downstream).
        if kind == "scalar":
            futures = engine.submit_batch(service_ms=5.0, count=args.batch_size,
                                          work_mode=args.work_mode)
        else:
            futures = engine.submit_batch(service_ms=requested,
                                          work_mode=args.work_mode)

        # As-completed retire with per-sweep recv_ns; reconstruct input order.
        idx_of = {id(f): i for i, f in enumerate(futures)}
        rows = [None] * len(futures)
        inflight = list(futures)
        while inflight:
            ready, inflight = engine.wait(inflight, num_returns=1)
            recv_ns = time.perf_counter_ns()
            for f in ready:
                r = f.result(recv_ns=recv_ns)
                i = idx_of[id(f)]
                req = requested[i]
                klass = ("uniform" if kind == "scalar"
                         else ("long" if req == long_ms else "short"))
                rows[i] = {
                    "idx": i,
                    "service_ms_requested": req,
                    "klass": klass,
                    "actor_id": r["actor_id"],
                    "submit_ns": r["submit_ns"],
                    "end_ns": r["end_ns"],
                    "total_ms": r["total_ms"],
                    "queue_wait_ms": r["queue_wait_ms"],
                    "service_ms_observed": r["service_ms_observed"],
                    "status": r["status"],
                    "work_mode": args.work_mode,
                    "pattern": args.pattern,
                }

    with open(args.out, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return 0


# --------------------------------------------------------------------------
# Orchestrator helpers
# --------------------------------------------------------------------------
def _pcts(vals):
    p = analyze_jsonl._pcts(vals)
    return p["p50"], p["p90"], p["p99"]


def _med(xs):
    return round(statistics.median(xs), 4)


def _load(path):
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _gate(rows, name, num_lanes, batch_size, work_mode, pattern):
    fails = []
    if len(rows) != batch_size:
        fails.append(f"rows {len(rows)} != {batch_size}")
    if sum(1 for r in rows if r["status"] == "completed") != batch_size:
        fails.append("not all completed")
    # Gate: one shared submit_ns across the whole batch (true single crossing,
    # not a Python loop over submit()).
    if len({r["submit_ns"] for r in rows}) != 1:
        fails.append(f"submit_ns not shared "
                     f"({len({r['submit_ns'] for r in rows})} distinct)")
    # Gate: input order preserved. Each lane is a single FIFO thread, so sorting
    # a lane's rows by end_ns (C++ steady_clock completion) must yield strictly
    # increasing idx (= submission order). This is a STRUCTURAL relative-order
    # check, robust to timing drift / oversubscription stalls (a stall delays a
    # completion but cannot reorder requests within a lane) -- NOT an exact-timing
    # assertion. Combined with round-robin (idx i -> lane i % num_lanes), it
    # confirms the i-th returned future ran the i-th requested duration.
    from collections import defaultdict
    by_lane = defaultdict(list)
    for r in rows:
        by_lane[r["actor_id"]].append(r)
    for lane, rs in by_lane.items():
        idxs = [r["idx"] for r in sorted(rs, key=lambda r: r["end_ns"])]
        if idxs != sorted(idxs):
            fails.append(f"order: lane …{lane[-4:]} not FIFO by idx")
            break
    # Gate: round-robin lane balance.
    from collections import Counter
    lc = Counter(r["actor_id"] for r in rows)
    if len(lc) != num_lanes:
        fails.append(f"lanes_seen {len(lc)} != {num_lanes}")
    if max(lc.values()) - min(lc.values()) > 1:
        fails.append(f"lane imbalance {min(lc.values())}-{max(lc.values())}")
    # Gate: short/long service separation (skip uniform).
    if pattern != "uniform5":
        sv = [r["service_ms_observed"] for r in rows if r["klass"] == "short"]
        lv = [r["service_ms_observed"] for r in rows if r["klass"] == "long"]
        if sv and lv and _med(lv) < 2 * _med(sv):
            fails.append(f"weak short/long separation "
                         f"(short~{_med(sv)} long~{_med(lv)})")
    return fails


def _cell(work_mode, lanes, pattern, raw_dir, repeats, batch_size, all_fails):
    thr, comp, t50, t90, t99, s50, s90, s99 = ([] for _ in range(8))
    sh_t50, lo_t50, sh_s50, lo_s50 = [], [], [], []
    lanes_seen, lane_min, lane_max = [], [], []
    for rep in range(repeats):
        fname = f"{work_mode}_l{lanes}_{pattern}_r{rep}.jsonl"
        out = os.path.join(raw_dir, fname)
        subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--worker",
             "--work-mode", work_mode, "--num-lanes", str(lanes),
             "--hpx-threads", str(HPX_THREADS), "--batch-size", str(batch_size),
             "--pattern", pattern, "--out", out],
            check=True, cwd=_REPO_ROOT, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE)
        rows = _load(out)
        fails = _gate(rows, fname, lanes, batch_size, work_mode, pattern)
        if fails:
            all_fails.append((fname, fails))

        totals = [r["total_ms"] for r in rows]
        svcs = [r["service_ms_observed"] for r in rows]
        span_start = min(r["submit_ns"] for r in rows)
        span_end = max(r["submit_ns"] + r["total_ms"] * 1e6 for r in rows)
        thr.append(len(rows) / max((span_end - span_start) / 1e9, 1e-9))
        comp.append(max(totals))  # batch completion ~ last completion latency
        a, b, c = _pcts(totals); t50.append(a); t90.append(b); t99.append(c)
        a, b, c = _pcts(svcs); s50.append(a); s90.append(b); s99.append(c)
        sh = [r["total_ms"] for r in rows if r["klass"] == "short"]
        lo = [r["total_ms"] for r in rows if r["klass"] == "long"]
        shs = [r["service_ms_observed"] for r in rows if r["klass"] == "short"]
        los = [r["service_ms_observed"] for r in rows if r["klass"] == "long"]
        sh_t50.append(_pcts(sh)[0] if sh else None)
        lo_t50.append(_pcts(lo)[0] if lo else None)
        sh_s50.append(_pcts(shs)[0] if shs else None)
        lo_s50.append(_pcts(los)[0] if los else None)
        from collections import Counter
        lc = Counter(r["actor_id"] for r in rows)
        lanes_seen.append(len(lc)); lane_min.append(min(lc.values()))
        lane_max.append(max(lc.values()))

    def med_opt(xs):
        xs = [x for x in xs if x is not None]
        return _med(xs) if xs else None

    return {
        "work_mode": work_mode, "pattern": pattern, "lanes": lanes,
        "hpx_threads": HPX_THREADS, "batch_size": batch_size, "repeats": repeats,
        "throughput_req_s": _med(thr),
        "batch_complete_ms": _med(comp),
        "total_ms_p50": _med(t50), "total_ms_p90": _med(t90),
        "total_ms_p99": _med(t99),
        "service_ms_p50": _med(s50), "service_ms_p90": _med(s90),
        "service_ms_p99": _med(s99),
        "short_total_ms_p50": med_opt(sh_t50),
        "long_total_ms_p50": med_opt(lo_t50),
        "short_service_ms_p50": med_opt(sh_s50),
        "long_service_ms_p50": med_opt(lo_s50),
        "lanes_seen": int(statistics.median(lanes_seen)),
        "lane_min_count": int(statistics.median(lane_min)),
        "lane_max_count": int(statistics.median(lane_max)),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run a single cell and write its JSONL")
    ap.add_argument("--work-mode"); ap.add_argument("--num-lanes", type=int)
    ap.add_argument("--hpx-threads", type=int, default=HPX_THREADS)
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    ap.add_argument("--pattern"); ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (sleep+spin, lanes {4}, 3 patterns, 1 "
                         "repeat, batch 32); no aggregate.json")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker(args)

    lanes_set, patterns, repeats, batch = LANES, PATTERNS, REPEATS, BATCH_SIZE
    if args.quick:
        lanes_set = (4,)
        patterns = ("uniform5", "block_ls_1_10", "alt_1_10")
        repeats, batch = 1, 32

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"varied_batch_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp10] raw -> {raw_dir}")
    print(f"[exp10] work_mode={WORK_MODES} lanes={lanes_set} patterns={patterns} "
          f"batch_size={batch} hpx_threads={HPX_THREADS} repeats={repeats}")

    cells, all_fails = [], []
    for work_mode in WORK_MODES:
        for lanes in lanes_set:
            for pattern in patterns:
                c = _cell(work_mode, lanes, pattern, raw_dir, repeats, batch,
                          all_fails)
                cells.append(c)
                st = (f"sh_t={c['short_total_ms_p50']}/lo_t={c['long_total_ms_p50']}"
                      if c["short_total_ms_p50"] is not None else "uniform")
                print(f"  {work_mode:5s} L{lanes:<2d} {pattern:15s} "
                      f"thr={c['throughput_req_s']:>8.1f} "
                      f"complete={c['batch_complete_ms']:>8.2f} "
                      f"t_p50={c['total_ms_p50']:>7.2f} t_p99={c['total_ms_p99']:>7.2f} "
                      f"lanes={c['lanes_seen']} {st}")

    aggregate = {
        "experiment": "varied_batch_service_time",
        "boundary": "hpx-python-frontend",
        "submit": "submit_batch (one true crossing; varied list or scalar)",
        "retire": "as-completed Engine.wait, per-sweep recv_ns, input order",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("Raw per-run JSONL is an experiment-local scratch format under "
                 "results/ (gitignored), NOT the v1 benchmark schema. total_ms is "
                 "queue-shaped: all requests share one batch submit_ns, so a "
                 "request's latency includes draining the requests ahead of it on "
                 "its round-robin lane."),
        "matrix": {"work_mode": list(WORK_MODES), "num_lanes": list(lanes_set),
                   "patterns": list(patterns), "batch_size": batch,
                   "hpx_threads": HPX_THREADS, "repeats": repeats},
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp10] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp10] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp10] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp10] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
