#!/usr/bin/env python3
"""Benchmark 10 runner: RayX batch lane bulk-enqueue A/B (internal diagnostics).

A/Bs the rayx batch enqueue strategy on no-op (service_ms=0) batches, where
enqueue overhead dominates:

  * BULK (shipped default)  -- group a batch's requests per lane and push each
    lane's group under ONE lock + ONE notify (ServiceLane::submit_bulk).
  * SINGLE (original)       -- one ServiceLane::submit per request (one lock +
    one notify each).

This does NOT go through the JSONL benchmark driver / analyzer: the two
strategies are an INTERNAL enqueue difference that is not observable in the
per-request JSONL (identical input order, round-robin actor_id, and schema v1).
So the A/B is driven in-process through the internal, undocumented diagnostics on
the raw _Engine:

    engine._engine._set_bulk_enqueue(True|False)     -- select the strategy
    engine._engine._submit_batch_cost_probe(count)   -- producer-side ns split
                                                        (enqueue / pybind-wrap / drain)

plus an end-to-end submit_batch()+get() wall-clock throughput measure. No public
API, no JSONL schema change, no analyzer change, no new benchmark driver.

Synthetic timing only -- NOT HPX #4703 scheduler batching, NOT a Ray result, NOT
real inference, NOT a general workload speedup. The win is no-op/tiny-batch
enqueue overhead; magnitudes are machine-specific.

Tracked evidence: the curated aggregate.json beside this script + the markdown
report. No scratch JSONL is produced (timings are in-process).

Usage (repo root, venv active, _rayx built):
    python benchmarks/10_rayx_bulk_enqueue/run_bulk_enqueue_ab.py
    python benchmarks/10_rayx_bulk_enqueue/run_bulk_enqueue_ab.py --quick
"""
import argparse
import json
import os
import statistics as st
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

HERE = os.path.dirname(os.path.abspath(__file__))
AGG = os.path.join(HERE, "aggregate.json")
MACHINE = "macOS laptop, 10 cores (4 P + 6 E), single locality"


def _throughput(engine, count, service_ms, reps):
    """End-to-end submit_batch()+get() throughput (req/s), median of `reps`."""
    out = []
    for _ in range(reps):
        t0 = time.perf_counter_ns()
        futs = engine.submit_batch(service_ms=service_ms, count=count)
        engine.get(futs)  # drain/retire all so the cell fully settles
        out.append(count / ((time.perf_counter_ns() - t0) / 1e9))
    return st.median(out)


def _producer_ns(engine, count, reps):
    """Producer-side ns split via the internal cost probe (no-op only)."""
    rows = [engine._engine._submit_batch_cost_probe(count) for _ in range(reps)]
    return (st.median(r["enqueue_ns"] for r in rows) / count,
            st.median(r["pybind_wrap_ns"] for r in rows) / count)


def measure(engine, count, service_ms, bulk, probe_reps, thr_reps):
    engine._engine._set_bulk_enqueue(bulk)
    cell = {
        "lanes": engine.num_lanes(),
        "count": count,
        "service_ms": service_ms,
        "mode": "bulk" if bulk else "single",
        "throughput_req_s": round(_throughput(engine, count, service_ms, thr_reps), 1),
    }
    if service_ms == 0:
        enq, wrap = _producer_ns(engine, count, probe_reps)
        cell["enqueue_ns_per_req"] = round(enq, 2)
        cell["pybind_wrap_ns_per_req"] = round(wrap, 2)
        cell["producer_ns_per_req"] = round(enq + wrap, 2)
    return cell


def run(quick):
    from rayx import Engine
    lanes_list = [1, 4] if quick else [1, 4, 8]
    counts = [1000] if quick else [1000, 10000]
    probe_reps = 5 if quick else 9
    thr_reps = 5 if quick else 7

    cells = []
    for lanes in lanes_list:
        with Engine(num_lanes=lanes, hpx_threads=4) as engine:
            engine._engine._submit_batch_cost_probe(2000)  # warmup
            for count in counts:
                for bulk in (False, True):
                    cells.append(measure(engine, count, 0, bulk, probe_reps, thr_reps))
        # One tiny SANITY cell per lane count is overkill; do it once at 4 lanes
        # (where the no-op win was largest): with real service the enqueue saving
        # is irrelevant -- bulk and single throughput should match (service-bound).
        if lanes == 4 and not quick:
            with Engine(num_lanes=4, hpx_threads=4) as engine:
                for bulk in (False, True):
                    cells.append(measure(engine, 200, 5, bulk, probe_reps, thr_reps=3))
    return {
        "benchmark": "rayx_bulk_enqueue",
        "boundary": "hpx-python-frontend",
        "method": ("in-process A/B via internal diagnostics "
                   "(_set_bulk_enqueue, _submit_batch_cost_probe) + "
                   "submit_batch()+get() throughput; NOT the JSONL driver/analyzer "
                   "(the bulk-vs-single difference is internal and not observable "
                   "in the per-request JSONL)"),
        "machine": MACHINE,
        "note": ("Per-lane BULK batch enqueue (one lock+notify per lane) vs "
                 "original ONE-BY-ONE enqueue (one lock+notify per request), on "
                 "no-op (service_ms=0) batches where enqueue overhead dominates. "
                 "Bulk preserves input order, round-robin actor_id, shared Python "
                 "submit_ns, scalar/varied forms, batch chunking rejection, batch "
                 "non-cancellation, and result-row/JSONL schema v1 -- it only "
                 "changes how requests are pushed onto the lanes. NOT an HPX #4703 "
                 "scheduler result, NOT Ray, NOT inference, NOT a general workload "
                 "speedup; magnitudes are machine-specific. enqueue/wrap ns are "
                 "producer-side (cost probe); throughput is end-to-end "
                 "submit_batch()+get(), median across repeats."),
        "matrix": {
            "service_ms": [0, 5],
            "lanes": lanes_list,
            "count": counts,
            "modes": ["single", "bulk"],
            "probe_reps": probe_reps,
            "throughput_reps": thr_reps,
            "sanity_cell": "lanes=4, count=200, service_ms=5 (effect should vanish)",
        },
        "cells": cells,
    }


def _print_table(agg):
    print(f"\nbenchmark: {agg['benchmark']}  ({agg['machine']})")
    # Pair single vs bulk per (lanes, count, service_ms).
    by_key = {}
    for c in agg["cells"]:
        by_key.setdefault((c["lanes"], c["count"], c["service_ms"]), {})[c["mode"]] = c
    print(f"{'lanes':>5} {'count':>6} {'svc':>4} "
          f"{'enq/req single':>14} {'enq/req bulk':>12} {'enq x':>6} "
          f"{'thr single':>11} {'thr bulk':>11} {'thr gain':>9}")
    for (lanes, count, svc), pair in sorted(by_key.items()):
        s, b = pair.get("single"), pair.get("bulk")
        es = s.get("enqueue_ns_per_req"); eb = b.get("enqueue_ns_per_req")
        ex = f"{es/eb:.1f}x" if es and eb else "-"
        es_s = f"{es:.1f}" if es is not None else "-"
        eb_s = f"{eb:.1f}" if eb is not None else "-"
        gain = 100 * (b["throughput_req_s"] / s["throughput_req_s"] - 1)
        print(f"{lanes:>5} {count:>6} {svc:>4} {es_s:>14} {eb_s:>12} {ex:>6} "
              f"{s['throughput_req_s']:>11,.0f} {b['throughput_req_s']:>11,.0f} "
              f"{gain:>+8.1f}%")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--quick", action="store_true",
                   help="tiny subset (lanes 1,4; count 1k; no aggregate written)")
    args = p.parse_args()
    agg = run(args.quick)
    _print_table(agg)
    if args.quick:
        print("\n--quick: aggregate.json NOT written (smoke subset).")
        return
    with open(AGG, "w") as f:
        json.dump(agg, f, indent=2)
        f.write("\n")
    print(f"\nwrote {os.path.relpath(AGG, REPO_ROOT)}")


if __name__ == "__main__":
    main()
