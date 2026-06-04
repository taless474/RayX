#!/usr/bin/env python3
"""Experiment 20 runner: HPX task/dataflow mechanism probe (native-only).

OPT-IN, EXPLICITLY-INCOMPARABLE mechanism probe. It drives the standalone native
binary `hpx_task_dataflow_probe`, which serves the SAME synthetic sleep work
through five dispatch mechanisms and reports which serialized-lane CONTRACTS each
preserves, relaxes, or makes not-applicable:

  * service_lane  -- rayhpx::ServiceLane (std::thread, blocking sleep). The
    Ray-actor-like ANCHOR; all lane contracts hold. Used UNMODIFIED.
  * hpx_lane      -- rayhpx::HpxLane (hpx::thread, cooperative sleep). A
    contract-preserving HPX-thread FIFO lane. Used UNMODIFIED.
  * hpx_async     -- one hpx::async task per request (scheduler-placed POOL).
  * hpx_dataflow  -- a tiny per-request prepare->service hpx::dataflow graph.
  * hpx_async_then-- hpx::async(...).then(finalize): composition BELOW the
    caller-visible future (the reason HPX-native composition stays in the backend,
    not the Python API).

Lineage: exp15 (isolated HPX primitives: sleep overshoot + hpx::async no-op
dispatch floor) -> exp16 (contract-PRESERVING cooperative FIFO HpxLane) -> exp20
(contract-RELAXING task/dataflow pools). Same opt-in, separately-reported axis.

This is NOT a rayx Python feature, NOT a benchmark-corpus entry, and NOT a
serving-lane-vs-serving-lane throughput result for the pool mechanisms (those are
SCHEDULER TERRITORY: trivial tasks spread across worker threads, per exp15's
framing -- never read as "task backend is faster at serving"). The native binary
emits a compact experiment-local JSON (schema "hpx-task-dataflow-probe-1"); it
does NOT touch the v1 benchmark JSONL schema or the analyzer.

Identity is honest, not faked: lanes emit their real actor_id ("act-hpx-" /
"act-hpxl-"); pools emit a `pool_id` tag and `lane_identity="n/a"` and report
`distinct_worker_ids` only as "which HPX worker ran it", never as a lane handle.
FIFO is measured as end_ns inversions vs submit order (0 == strict FIFO).

Matrix: mechanisms x service_ms {0,1,5,20}, work_mode=sleep (no spin in v1),
hpx_threads=4, N=200 full, 3 repeats. --quick: N=40, 1 repeat, no aggregate.json.

Raw per-run JSON is scratch under results/ (gitignored). Tracked evidence: the
curated aggregate.json beside this script + the markdown report. Magnitudes are
machine-specific and REPORTED, never gated.

Usage (repo root; native binary built):
    python experiments/20_hpx_task_dataflow_probe/run_hpx_task_dataflow_probe.py
    python experiments/20_hpx_task_dataflow_probe/run_hpx_task_dataflow_probe.py --quick
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
NATIVE_BIN = os.path.join(_REPO_ROOT, "hpx_impl", "build", "hpx_task_dataflow_probe")

PROBE_SCHEMA = "hpx-task-dataflow-probe-1"
HPX_THREADS = 4
SERVICE_MS = (0.0, 1.0, 5.0, 20.0)
N_FULL = 200
N_QUICK = 40
REPEATS_FULL = 3
REPEATS_QUICK = 1

# Mechanisms and their class. Lanes preserve every lane contract; pools relax /
# n-a the lane-specific ones (see CONTRACT_COVERAGE below).
MECHS = (
    ("service_lane", "lane"),
    ("hpx_lane", "lane"),
    ("hpx_async", "pool"),
    ("hpx_dataflow", "pool"),
    ("hpx_async_then", "pool"),
)

# Expected lane-identity prefix per lane mechanism (structural identity gate).
LANE_PREFIX = {"service_lane": "act-hpx-", "hpx_lane": "act-hpxl-"}

# The eight serialized-lane contracts the probe reports coverage for.
CONTRACTS = (
    "one_result_per_request",
    "future_ownership_compatible",
    "stable_actor_id",
    "fifo_lane_order",
    "queue_depth",
    "active",
    "per_lane_admission_cap",
    "lane_targeted_cancellation",
)

# Declared contract coverage by mechanism class. The two UNIVERSAL contracts
# (one_result_per_request, future_ownership_compatible) hold for every mechanism;
# the lane-specific ones are preserved by serialized lanes and relaxed/n-a for the
# scheduler-placed pools. `fifo_lane_order` is cross-checked against the MEASURED
# inversions per cell (a defensive consistency gate), not taken on faith.
_LANE_COVERAGE = {c: "preserved" for c in CONTRACTS}
_POOL_COVERAGE = {
    "one_result_per_request": "preserved",
    "future_ownership_compatible": "preserved",
    "stable_actor_id": "relaxed",      # pool_id tag, not a per-lane handle
    "fifo_lane_order": "relaxed",      # scheduler-placed; completion reorders
    "queue_depth": "n/a",              # no per-lane queue to measure
    "active": "n/a",                   # no single in-service-per-lane notion
    "per_lane_admission_cap": "n/a",   # no lane queue to cap
    "lane_targeted_cancellation": "n/a",  # no lane to target a queued skip at
}
CONTRACT_COVERAGE = {
    name: (_LANE_COVERAGE if klass == "lane" else _POOL_COVERAGE)
    for name, klass in MECHS
}


def _p50(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _load(path):
    with open(path) as fh:
        return json.load(fh)


def _run_repeat(n, out_path):
    """One native-binary invocation (a fresh HPX runtime); returns parsed JSON."""
    subprocess.run(
        [NATIVE_BIN, f"--hpx:threads={HPX_THREADS}", "--n", str(n),
         "--out", out_path],
        check=True, cwd=_REPO_ROOT, stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE)
    return _load(out_path)


def _cell_key(c):
    return (c["mechanism"], c["service_ms"])


def _aggregate_cell(mechanism, klass, service_ms, reps):
    """Aggregate the same (mechanism, service_ms) cell across repeats."""
    cells = [c for rep in reps for c in rep["cells"]
             if c["mechanism"] == mechanism and c["service_ms"] == service_ms]
    n = cells[0]["submitted"]
    inversions = [c["inversions"] for c in cells]
    return {
        "mechanism": mechanism, "class": klass, "service_ms": service_ms,
        "submitted": n,
        "completed_all": all(c["completed"] == n for c in cells),
        "failed_total": sum(c["failed"] for c in cells),
        "inversions_p50": int(statistics.median(inversions)),
        "inversions_max": max(inversions),
        "fifo_preserved_all": all(c["fifo_preserved"] for c in cells),
        "lane_identity": cells[0]["lane_identity"],
        "pool_id": cells[0]["pool_id"],
        "distinct_actor_ids": cells[0]["distinct_actor_ids"],
        "distinct_worker_ids_max":
            max((c["distinct_worker_ids"] for c in cells
                 if c["distinct_worker_ids"] is not None), default=None),
        # Timing -- REPORTED, never gated (machine-specific magnitudes).
        "throughput_ops_s_p50": _p50([c["throughput_ops_s"] for c in cells]),
        "service_observed_ms_p50":
            _p50([c["service_observed_ms"]["p50"] for c in cells]),
        "service_observed_ms_p99":
            _p50([c["service_observed_ms"]["p99"] for c in cells]),
        "overshoot_pct_p50":
            _p50([c["overshoot_pct"]["p50"] for c in cells
                  if c["overshoot_pct"] is not None]) if service_ms > 0 else None,
    }


def _gate(cell, reps_schema_ok):
    """Structural gates (timing-robust). Returns a list of failure strings."""
    fails = []
    m, klass, sm = cell["mechanism"], cell["class"], cell["service_ms"]

    # G1 every mechanism returns exactly N well-formed results (no failures).
    if not cell["completed_all"]:
        fails.append(f"{m} sm={sm}: not all repeats completed N={cell['submitted']}")
    if cell["failed_total"] != 0:
        fails.append(f"{m} sm={sm}: {cell['failed_total']} failed results")

    # G3 lanes preserve FIFO; pools' ordering is REPORTED, not gated.
    if klass == "lane" and not cell["fifo_preserved_all"]:
        fails.append(f"{m} sm={sm}: lane FIFO broken "
                     f"(inversions max={cell['inversions_max']})")

    # G4 identity: lanes carry a real prefixed actor_id + no pool_id; pools carry
    # lane_identity == "n/a" + a pool_id and never a stable per-lane actor_id.
    if klass == "lane":
        pref = LANE_PREFIX[m]
        if not str(cell["lane_identity"]).startswith(pref):
            fails.append(f"{m} sm={sm}: lane_identity {cell['lane_identity']!r} "
                         f"missing prefix {pref!r}")
        if cell["pool_id"] is not None:
            fails.append(f"{m} sm={sm}: lane unexpectedly has pool_id "
                         f"{cell['pool_id']!r}")
        if cell["distinct_actor_ids"] != 1:
            fails.append(f"{m} sm={sm}: lane distinct_actor_ids "
                         f"{cell['distinct_actor_ids']} != 1")
    else:
        if cell["lane_identity"] != "n/a":
            fails.append(f"{m} sm={sm}: pool lane_identity "
                         f"{cell['lane_identity']!r} != 'n/a' (must not fake a lane)")
        if not cell["pool_id"]:
            fails.append(f"{m} sm={sm}: pool missing pool_id")

    # G6 service actually ran for service_ms > 0 (loose overshoot/sanity band).
    if sm > 0.0:
        svc = cell["service_observed_ms_p50"]
        if svc is None or not (0.5 * sm <= svc <= 3.0 * sm):
            fails.append(f"{m} sm={sm}: observed service p50 {svc} outside "
                         f"[{0.5 * sm}, {3.0 * sm}]")

    # G7 probe emitted its own compact schema, not the v1 benchmark JSONL.
    if not reps_schema_ok:
        fails.append(f"{m} sm={sm}: probe schema is not {PROBE_SCHEMA!r}")

    return fails


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (N=40, 1 repeat); no aggregate.json")
    args = ap.parse_args(argv)

    if not os.path.exists(NATIVE_BIN):
        print(f"[exp20] native binary not found: {NATIVE_BIN}\n"
              f"        build it: cmake --build hpx_impl/build "
              f"--target hpx_task_dataflow_probe")
        return 2

    n = N_QUICK if args.quick else N_FULL
    repeats = REPEATS_QUICK if args.quick else REPEATS_FULL

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results",
                           f"hpx_task_dataflow_probe_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp20] raw -> {raw_dir}")
    print(f"[exp20] hpx_threads={HPX_THREADS} work_mode=sleep N={n} "
          f"repeats={repeats} service_ms={list(SERVICE_MS)} "
          f"mechanisms={[m for m, _ in MECHS]}")

    reps = []
    for rep in range(repeats):
        out = os.path.join(raw_dir, f"probe_r{rep}.json")
        reps.append(_run_repeat(n, out))
    reps_schema_ok = all(r.get("schema") == PROBE_SCHEMA for r in reps)

    cells, all_fails = [], []
    for mechanism, klass in MECHS:
        for sm in SERVICE_MS:
            c = _aggregate_cell(mechanism, klass, sm, reps)
            fails = _gate(c, reps_schema_ok)
            if fails:
                all_fails.extend(fails)
            cells.append(c)

    # G5 contract-coverage completeness: every (mechanism x contract) is one of
    # {preserved, relaxed, n/a}, none blank.
    valid = {"preserved", "relaxed", "n/a"}
    for mechanism, _ in MECHS:
        cov = CONTRACT_COVERAGE[mechanism]
        for contract in CONTRACTS:
            if cov.get(contract) not in valid:
                all_fails.append(f"contract table: {mechanism}.{contract} = "
                                 f"{cov.get(contract)!r} not in {valid}")

    # Console summary (one line per mechanism at service_ms=5: the readable cell).
    for mechanism, klass in MECHS:
        c = next(x for x in cells
                 if x["mechanism"] == mechanism and x["service_ms"] == 5.0)
        ident = c["lane_identity"] if klass == "lane" else c["pool_id"]
        print(f"  {mechanism:15s} {klass:4s} sm=5 "
              f"fifo={str(c['fifo_preserved_all']):5s} "
              f"inv_p50={c['inversions_p50']:4d} "
              f"ident={ident} workers={c['distinct_worker_ids_max']} "
              f"svc_p50={c['service_observed_ms_p50']}ms "
              f"thr={c['throughput_ops_s_p50']}/s")

    aggregate = {
        "experiment": "hpx_task_dataflow_probe",
        "probe_schema": PROBE_SCHEMA,
        "kind": "native-only, opt-in, contract-relaxing mechanism probe "
                "(NOT a benchmark-corpus entry, NOT a rayx feature)",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "lineage": {
            "exp15": "isolated HPX primitives: sleep overshoot + hpx::async no-op "
                     "dispatch floor",
            "exp16": "contract-PRESERVING cooperative FIFO HpxLane (opt-in, tagged)",
            "exp20": "contract-RELAXING hpx::async / hpx::dataflow pools (this probe)",
        },
        "note": ("Serves identical synthetic sleep work through serialized lanes "
                 "vs scheduler-placed HPX task/dataflow pools, to show which "
                 "serialized-lane contracts are preserved/relaxed/n-a. Lanes "
                 "(service_lane/hpx_lane) preserve every contract; the pools keep "
                 "the UNIVERSAL contracts (one result row per request, per-request "
                 "future ownership) but RELAX identity (pool_id, lane_identity "
                 "n/a) and FIFO (measured end_ns inversions > 0), and the per-lane "
                 "contracts (queue_depth, active, per-lane cap, lane-targeted "
                 "cancellation) are N/A with no lane queue to measure or cap -- "
                 "NOT faked. Pool throughput is scheduler-spread parallelism "
                 "across workers, NOT single-lane serving throughput, and must not "
                 "be read as 'task backend is faster at serving' (exp15 framing). "
                 "Out-of-order completion for pools is a CONTRACT DIFFERENCE, not a "
                 "bug. ServiceLane and HpxLane are used UNMODIFIED; no rayx Python "
                 "API, no HPX .then/dataflow exposed to Python, no ServiceLane "
                 "replacement. Timing magnitudes are machine-specific and REPORTED, "
                 "not gated. Raw per-run JSON is experiment-local scratch under "
                 "results/ (gitignored), NOT the v1 benchmark JSONL."),
        "matrix": {"work_mode": "sleep", "hpx_threads": HPX_THREADS,
                   "service_ms": list(SERVICE_MS), "n": n, "repeats": repeats,
                   "mechanisms": [m for m, _ in MECHS]},
        "gates": [
            "G1 every mechanism returns exactly N well-formed results (0 failed)",
            "G2 every per-request future fulfilled once and retired cleanly "
            "(binary wait_all/get completes, exit 0)",
            "G3 service_lane and hpx_lane preserve FIFO (end_ns inversions == 0)",
            "G4 lanes emit real prefixed actor_id (no pool_id); pools emit "
            "lane_identity=='n/a' + pool_id (no faked per-lane actor_id)",
            "G5 contract-coverage table complete (every cell preserved|relaxed|n/a)",
            "G6 service ran for service_ms>0 (observed p50 in a loose band)",
            "G7 probe emits compact schema (hpx-task-dataflow-probe-1), not v1 JSONL",
        ],
        "contracts": list(CONTRACTS),
        "contract_coverage": CONTRACT_COVERAGE,
        "universal_contracts": [
            "one_result_per_request", "future_ownership_compatible"],
        "lane_specific_contracts": [
            "stable_actor_id", "fifo_lane_order", "queue_depth", "active",
            "per_lane_admission_cap", "lane_targeted_cancellation"],
        "cells": cells,
    }

    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp20] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp20] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp20] GATES FAILED ({len(all_fails)}):")
        for f in all_fails:
            print(f"  {f}")
        return 1
    print(f"[exp20] all gates passed; {len(cells)} cells, "
          f"{len(MECHS)} mechanisms x {len(SERVICE_MS)} service_ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
