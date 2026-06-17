#!/usr/bin/env python3
"""Experiment 24 runner: rayx.runtime parked-overlap mechanism (park_ms).

What park_ms is: the runtime's PARKED / cooperative-wait synthetic registered
op -- the parked-wait analog of the CPU-bound busy_sum diagnostic. The body
parks in 10 ms chunks via hpx::this_thread::sleep_for, a COOPERATIVE suspension
of the HPX thread running it (never std::this_thread::sleep_for), cancellable
at chunk boundaries, capped at 60 s. Completion echoes the requested ms back as
the int64 value. It is synthetic diagnostic work only: NOT real I/O, NOT
inference, and it carries NO performance claim.

The mechanism this experiment demonstrates (observation-only): each
rayx.runtime RuntimeLane worker is one long-lived hpx::thread multiplexed onto
the hpx_threads-sized HPX worker pool. A lane parked inside park_ms suspends
cooperatively and FREES its OS-level HPX worker, so many parked lanes can make
progress with few HPX workers -- num_lanes x park_ms of requested parked work
can drain in about ONE park duration even at hpx_threads=1. This demonstrates
that parked HPX threads can suspend cooperatively, allowing other lanes to make
progress with fewer HPX worker threads. Contrast busy_sum: CPU-bound work does
NOT yield, so its concurrent progress is bounded by the worker pool (the exp22
spin analog). No busy_sum cells are run here; the contrast is mechanism
framing, not a measured comparison.

GATE POLICY (firm gates are STRUCTURAL only; every timing is observed, never
gated -- park-related checks never assert wall-clock values):
  G1 retire      every submitted future retires via get(); results == offered
  G2 status      every status is terminal and valid ("completed" / "failed" /
                 "cancelled"); this matrix cancels nothing, so completed ==
                 offered (cancelled == 0, failed == 0)
  G3 value echo  every completed value == the requested park_ms (int64 echo)
  G4 lane ids    rt-hpx- prefix on every row; lanes_seen == num_lanes
                 (round-robin puts exactly one park on each lane)
  G5 idle drain  lane_stats() returns to queue_depth == 0 / active == False
                 after the drain (bounded poll, no sleep)
The run finishing at all is the no-deadlock/no-hang evidence; there is no
timing gate anywhere.

OBSERVATION-ONLY (mechanism evidence, NEVER a gate, NO performance verdict, NO
speedup / "HPX beats Ray" claim): makespan_ms, expected_serial_ms = offered x
park_ms, and overlap_factor = expected_serial_ms / makespan_ms (~1 means the
parks did not overlap; ~num_lanes means they did). Machine-specific.

Scope: rayx.runtime only (registered native ops over HPX-native FIFO
RuntimeLanes). NOT the harness Engine / lane_impl backends (exp16/20-23 are a
different axis), NOT the v1 benchmark JSONL schema (no analyzer / driver / CI
change), not Ray Serve, not an object store, not arbitrary Python, not real
inference, and no Ray comparison of any kind. hpx_threads varies via
short-lived sequential Runtime contexts (each Runtime owns one
hpx::start..hpx::stop cycle, as bench/smoke_rayx_runtime.py already exercises),
so no subprocess plumbing is needed.

Usage (repo root, venv active, _rayx built):
    python experiments/24_runtime_parked_overlap/run_runtime_parked_overlap.py
    python experiments/24_runtime_parked_overlap/run_runtime_parked_overlap.py --quick
    # overrides: --hpx-threads "1,2" --num-lanes "1,8,32" --park-ms 250 --repeats N
"""
import argparse
import json
import os
import statistics
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
if _RAYX_SRC not in sys.path:
    sys.path.insert(0, _RAYX_SRC)

LANE_PREFIX = "rt-hpx-"
TERMINAL_STATUSES = ("completed", "failed", "cancelled")

# Swept axes. Laptop-safe: with cooperative overlap every cell drains in about
# one park duration, and even a hypothetical fully-serial worst case is bounded
# at num_lanes x park_ms (32 x 250 ms = 8 s), far below PARK_MS_MAX.
HPX_THREADS_FULL = (1, 2)
HPX_THREADS_QUICK = (1, 2)
NUM_LANES_FULL = (1, 8, 32)
NUM_LANES_QUICK = (1, 8)
PARK_MS_FULL = 250
PARK_MS_QUICK = 100
REPEATS_FULL = 3
REPEATS_QUICK = 1

# Safety bound on the post-drain idle poll (no sleep / timing assertion): after
# get() returns, the lane's service slot clears almost immediately; the cap only
# prevents a pathological spin if it never does (reported as a G5 failure).
IDLE_POLL_CAP = 200_000


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _poll_idle(rt):
    """Bounded poll (no sleep) until every lane reports idle; False if capped."""
    for _ in range(IDLE_POLL_CAP):
        stats = rt.lane_stats()
        if all((not s["active"]) and s["queue_depth"] == 0 for s in stats):
            return True
    return False


def _run_cell(Runtime, hpx_threads, num_lanes, park_ms, repeats):
    """One matrix cell: one park_ms per lane (round-robin), drained via get().

    Records firm-gate facts (retire / terminal status / value echo / lane ids /
    idle drain) plus observation-only makespan vs the serial expectation.
    """
    offered = num_lanes  # round-robin submission lands exactly one park per lane
    per_rep = []
    for _ in range(repeats):
        with Runtime(num_lanes=num_lanes, hpx_threads=hpx_threads) as rt:
            t0 = time.perf_counter_ns()
            futs = [rt.submit_operation("park_ms", park_ms)
                    for _ in range(offered)]
            results = rt.get(futs)
            t1 = time.perf_counter_ns()
            idle_ok = _poll_idle(rt)
        makespan_ms = (t1 - t0) / 1e6
        statuses = [r.status for r in results]
        completed = sum(1 for s in statuses if s == "completed")
        cancelled = sum(1 for s in statuses if s == "cancelled")
        failed = sum(1 for s in statuses if s == "failed")
        terminal_ok = all(s in TERMINAL_STATUSES for s in statuses)
        echo_ok = all(r.value == park_ms for r in results
                      if r.status == "completed")
        actor_ids = [r.row["actor_id"] for r in results]
        prefix_ok = all(isinstance(a, str) and a.startswith(LANE_PREFIX)
                        for a in actor_ids)
        per_rep.append({
            "results": len(results), "completed": completed,
            "cancelled": cancelled, "failed": failed,
            "terminal_ok": terminal_ok, "echo_ok": echo_ok,
            "prefix_ok": prefix_ok, "lanes_seen": len(set(actor_ids)),
            "idle_ok": idle_ok, "makespan_ms": makespan_ms,
        })

    expected_serial_ms = offered * park_ms
    makespans = [r["makespan_ms"] for r in per_rep]
    return {
        "hpx_threads": hpx_threads, "num_lanes": num_lanes,
        "park_ms": park_ms, "offered": offered, "repeats": repeats,
        # firm-gate facts (hold across every repeat)
        "retired_all": all(r["results"] == offered for r in per_rep),
        "all_terminal_valid": all(r["terminal_ok"] for r in per_rep),
        "completed_all": all(r["completed"] == offered for r in per_rep),
        "cancelled_total": sum(r["cancelled"] for r in per_rep),
        "failed_total": sum(r["failed"] for r in per_rep),
        "value_echo_ok": all(r["echo_ok"] for r in per_rep),
        "prefix_ok": all(r["prefix_ok"] for r in per_rep),
        "lanes_seen_ok": all(r["lanes_seen"] == num_lanes for r in per_rep),
        "idle_after_drain": all(r["idle_ok"] for r in per_rep),
        # observation-only (machine-specific; never gated)
        "expected_serial_ms": expected_serial_ms,
        "obs_makespan_ms_p50": _med(makespans),
        "obs_overlap_factor_p50": _med(
            [expected_serial_ms / m if m > 0 else None for m in makespans]),
    }


def _gate(cells):
    """Firm structural gates only (G1-G5); timing is never inspected here."""
    fails = []
    for c in cells:
        tag = f"ht{c['hpx_threads']}/L{c['num_lanes']}"
        if not c["retired_all"]:
            fails.append(f"{tag}: G1 retire (results != offered)")
        if not c["all_terminal_valid"]:
            fails.append(f"{tag}: G2 non-terminal/invalid status")
        if not c["completed_all"]:
            fails.append(f"{tag}: G2 completion (cancelled={c['cancelled_total']}"
                         f" failed={c['failed_total']})")
        if not c["value_echo_ok"]:
            fails.append(f"{tag}: G3 value echo != park_ms")
        if not c["prefix_ok"]:
            fails.append(f"{tag}: G4 actor_id prefix mismatch")
        if not c["lanes_seen_ok"]:
            fails.append(f"{tag}: G4 lanes_seen != num_lanes")
        if not c["idle_after_drain"]:
            fails.append(f"{tag}: G5 lanes not idle after drain")
    return fails


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--hpx-threads", dest="hpx_threads",
                    help='override swept HPX worker counts, e.g. "1,2"')
    ap.add_argument("--num-lanes", dest="num_lanes",
                    help='override swept lane counts, e.g. "1,8,32"')
    ap.add_argument("--park-ms", dest="park_ms", type=int,
                    help="requested park per request (ms)")
    ap.add_argument("--repeats", type=int)
    ap.add_argument("--quick", action="store_true",
                    help="smaller matrix (num_lanes 1,8; park 100 ms; 1 repeat);"
                         " no aggregate.json written")
    args = ap.parse_args(argv)

    if args.hpx_threads:
        hpx_threads_list = tuple(int(x) for x in args.hpx_threads.split(","))
    else:
        hpx_threads_list = HPX_THREADS_QUICK if args.quick else HPX_THREADS_FULL
    if args.num_lanes:
        num_lanes_list = tuple(int(x) for x in args.num_lanes.split(","))
    else:
        num_lanes_list = NUM_LANES_QUICK if args.quick else NUM_LANES_FULL
    park_ms = args.park_ms if args.park_ms is not None else (
        PARK_MS_QUICK if args.quick else PARK_MS_FULL)
    repeats = args.repeats if args.repeats is not None else (
        REPEATS_QUICK if args.quick else REPEATS_FULL)

    from rayx.runtime import Runtime

    print(f"[exp24] hpx_threads={hpx_threads_list} num_lanes={num_lanes_list} "
          f"park_ms={park_ms} repeats={repeats} quick={args.quick} "
          f"(structural gates only; overlap is OBSERVATION, never gated)")

    cells = []
    for ht in hpx_threads_list:
        for L in num_lanes_list:
            c = _run_cell(Runtime, ht, L, park_ms, repeats)
            cells.append(c)
            print(f"  ht={ht} L={L:2d} offered={c['offered']:2d} "
                  f"completed_all={c['completed_all']} "
                  f"[obs] makespan_p50={c['obs_makespan_ms_p50']} ms "
                  f"(serial expectation {c['expected_serial_ms']} ms, "
                  f"overlap_factor~{c['obs_overlap_factor_p50']})")

    fails = _gate(cells)
    if not fails:
        print("[exp24] structural gates passed (G1-G5): every park retired "
              "completed with the echoed value on its own lane; overlap is "
              "reported as observation only")
    else:
        print("[exp24] STRUCTURAL GATE FAILURES:")
        for f in fails:
            print(f"  - {f}")

    aggregate = {
        "experiment": "runtime_parked_overlap",
        "schema": "rayx-runtime-parked-overlap-1",
        "schema_note": "experiment-local; NOT the v1 benchmark JSONL schema",
        "boundary": "rayx-runtime-hpx-native",
        "goal": "demonstrate the cooperative parked-work mechanism in "
                "rayx.runtime: lanes parked in park_ms (hpx::this_thread::"
                "sleep_for) suspend cooperatively and free their HPX worker, "
                "so many parked lanes drain in about one park duration even at "
                "hpx_threads=1. Mechanism/structure evidence; overlap is "
                "observed, never gated.",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "matrix": {
            "hpx_threads": list(hpx_threads_list),
            "num_lanes": list(num_lanes_list),
            "park_ms": park_ms,
            "requests_per_lane": 1,
            "repeats": repeats,
        },
        "firm_gates": {
            "G1": "retire: every submitted future retires via get(); "
                  "results == offered",
            "G2": "status: every status terminal and valid; completed == "
                  "offered (nothing is cancelled in this matrix)",
            "G3": "value echo: every completed value == requested park_ms",
            "G4": "lane ids: rt-hpx- prefix on every row; lanes_seen == "
                  "num_lanes",
            "G5": "idle drain: lane_stats() back to queue_depth 0 / active "
                  "False after drain",
        },
        "note": "Structural gates only; makespan_ms / expected_serial_ms / "
                "overlap_factor are OBSERVATION-ONLY and machine-specific -- "
                "no performance verdict, no speedup claim, no 'HPX beats Ray' "
                "claim, no Ray comparison. park_ms is synthetic cooperative "
                "parked work (chunked hpx::this_thread::sleep_for), not real "
                "I/O and not inference. rayx.runtime only: registered native "
                "ops over HPX-native FIFO RuntimeLanes; NOT the harness "
                "Engine/lane_impl backends, no analyzer / benchmark-JSONL / "
                "driver / CI change.",
        "all_structural_gates_passed": not fails,
        "gate_failures": fails,
        "cells": cells,
    }

    if args.quick:
        print("[exp24] --quick: aggregate.json NOT written (smoke only)")
    else:
        agg_path = os.path.join(_HERE, "aggregate.json")
        with open(agg_path, "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp24] curated aggregate -> {agg_path}")

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
