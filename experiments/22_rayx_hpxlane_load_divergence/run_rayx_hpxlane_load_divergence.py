#!/usr/bin/env python3
"""Experiment 22 runner: where rayx lane_impl="std" / "hpx" DIVERGE under load.

Exp21 already proved CONTRACT PARITY between the two rayx lane backends
(``Engine(lane_impl="std")`` = std::thread ``ServiceLane`` anchor;
``Engine(lane_impl="hpx")`` = cooperative HPX-thread ``HpxLane``). This
experiment asks the follow-on question parity left open: given identical
semantics, WHERE do the two backends structurally diverge under load, and why is
that divergence a SCHEDULING-MECHANISM fact rather than a speedup?

The mechanism (rayx-only, synthetic):
  * ServiceLane = num_lanes independent std::thread workers. Their parallelism is
    set by the OS / core count and is INDEPENDENT of hpx_threads.
  * HpxLane = num_lanes hpx::thread lane-workers multiplexed onto the
    hpx_threads-sized HPX worker pool. NON-yielding work can only truly run
    hpx_threads-wide.
  * Parked sleep yields the HPX worker; spin (work_mode="spin") does NOT (busy
    on-core). work_mode="spin" here is a synthetic CPU-bound diagnostic /
    calibration mode used to expose the scheduling bound -- it is NOT the serving
    design.

Predicted (interpreted, mostly OBSERVATIONAL):
  * sleep -> CONVERGENT: both overlap parked waits (ServiceLane via OS threads,
    HpxLane via cooperative yield, which overlaps even at hpx_threads=1).
  * spin  -> DIVERGENT: ServiceLane overlaps across cores regardless of
    hpx_threads; HpxLane's concurrent progress is bounded near hpx_threads.

GATE POLICY (firm gates are STRUCTURAL only; the divergence story is observed,
never gated):
  G1 completion        rows == offered, one row per request (no loss under load)
  G2 per-lane FIFO     within each lane, end_ns is monotonic in submit order
  G3 lane_stats sanity active + queue_depth observed under a controlled SLEEP
                       load, idle after drain (run once per backend on a sleep
                       probe so it is not subject to spin worker starvation)
  G4 actor_id prefix   act-hpx- for std, act-hpxl- for hpx, on every row
  G5 completion parity same offered/completed per (hpx_threads,num_lanes,workload)

OBSERVATION-ONLY (mechanism evidence, NEVER a gate, NO performance / speedup /
"HPX beats Ray" claim): drain_wall_ms, overlap_ratio, max_active_lanes (sampled
from lane_stats()), max_total_queue, and the inferred sleep-convergence /
spin-bounded pattern. lane_stats().active is valid RayX observability but is NOT
a perfect proof of true HPX worker-level concurrency -- under spin the stats call
itself needs HPX-worker access and can be delayed by saturated workers, which is
exactly why the spin bound is reported, not gated.

Scope: rayx-only. This is the opt-in serialized rayx HpxLane backend
(lane_impl="hpx"), NOT the exp16 native single-lane mechanism probe and NOT the
exp20 hpx::async / hpx::dataflow task-pool probe. No analyzer / benchmark-JSONL
schema / driver / CI / public Future-ownership change; no HPX internals exposed.
Not Ray Serve, not a Ray object store, not real model inference, not an
"HPX beats Ray" claim.

Because the HPX runtime is a PROCESS resource (one hpx::start per process, fixed
worker count), each (backend, hpx_threads) pair runs in its OWN subprocess; within
it num_lanes / workload vary via short-lived Engine contexts. Raw per-worker JSON
is experiment-local scratch under results/ (gitignored), NOT the v1 benchmark
schema. Tracked evidence: the curated aggregate.json beside this script plus the
markdown report.

Usage (repo root, venv active, _rayx built):
    python experiments/22_rayx_hpxlane_load_divergence/run_rayx_hpxlane_load_divergence.py
    python experiments/22_rayx_hpxlane_load_divergence/run_rayx_hpxlane_load_divergence.py --quick
    # overrides: --hpx-threads "1,2,4" --num-lanes "2,4,8" --workload both --repeats N
    # internal: --worker --backend {std,hpx} --hpx-threads N
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
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
if _RAYX_SRC not in sys.path:
    sys.path.insert(0, _RAYX_SRC)

BACKENDS = ("std", "hpx")
EXPECTED_PREFIX = {"std": "act-hpx-", "hpx": "act-hpxl-"}
WORKLOADS = ("sleep", "spin")

# Swept axes. hpx_threads MUST be a subprocess axis (the HPX runtime is a process
# resource: one hpx::start per process with a fixed worker count, so a later
# in-process Engine cannot change it). num_lanes / workload vary in-process.
HPX_THREADS_FULL = (1, 2, 4)
HPX_THREADS_QUICK = (1, 2)
NUM_LANES_FULL = (2, 4, 8)
NUM_LANES_QUICK = (2, 4)
REPEATS_FULL = 3
REPEATS_QUICK = 1

REQS_PER_LANE = 2          # >= 2 so each lane has a real per-lane FIFO sequence
SLEEP_MS_FULL = 120.0      # parked service per request (yields the HPX worker)
SLEEP_MS_QUICK = 80.0
SPIN_MS_FULL = 40.0        # busy-on-core service per request (does NOT yield)
SPIN_MS_QUICK = 30.0

# Safety bound on the in-flight sampling loop; it normally exits the instant every
# future is ready (no timing assertion). lane_stats() for the hpx backend hops onto
# an HPX worker, so under spin saturation it naturally paces at request-completion
# rate -- the loop cannot busy-spin faster than the workers free up.
SAMPLE_CAP = 2_000_000


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _load(path):
    with open(path) as fh:
        return json.load(fh)


def _poll(fn, tries=2_000_000):
    """Bounded poll (no sleep / timing assertion) until fn() is truthy."""
    for _ in range(tries):
        v = fn()
        if v:
            return v
    raise RuntimeError("poll: condition never became true")


def _fifo_inversions(rows):
    """Per-lane FIFO check. rows are in SUBMIT order (get() preserves input order),
    and submission is round-robin, so the rows for one actor_id are that lane's
    requests in submit order. A serialized FIFO lane services them one at a time,
    so end_ns must be monotonic non-decreasing within each lane. Returns the count
    of adjacent (submit-order) pairs whose end_ns strictly decreases."""
    last_end = {}
    inversions = 0
    for r in rows:
        aid = r["actor_id"]
        e = r["end_ns"]
        if aid in last_end and e < last_end[aid]:
            inversions += 1
        last_end[aid] = e
    return inversions


# --------------------------------------------------------------------------
# Worker: one (backend, hpx_threads) process. Sweeps workload x num_lanes.
# --------------------------------------------------------------------------
def _run_cell(Engine, backend, hpx_threads, num_lanes, workload, service_ms,
              repeats, prefix):
    """One matrix cell: a burst of REQS_PER_LANE per lane, round-robined. Records
    firm-gate facts (completion / FIFO / prefix) plus observation-only mechanism
    fields (drain wall, overlap ratio, sampled concurrency)."""
    offered = num_lanes * REQS_PER_LANE
    per_rep = []
    for _ in range(repeats):
        with Engine(num_lanes=num_lanes, hpx_threads=hpx_threads,
                    lane_impl=backend) as e:
            t0 = time.perf_counter_ns()
            futs = [e.submit(service_ms=service_ms, work_mode=workload,
                             label=str(i)) for i in range(offered)]
            # Sample lane occupancy while the burst is in flight. Exits as soon as
            # every future is ready; SAMPLE_CAP is only a safety net.
            max_active = 0
            max_total_queue = 0
            samples = 0
            for _s in range(SAMPLE_CAP):
                stats = e.lane_stats()
                active_ct = sum(1 for s in stats if s["active"])
                tq = sum(int(s["queue_depth"]) for s in stats)
                if active_ct > max_active:
                    max_active = active_ct
                if tq > max_total_queue:
                    max_total_queue = tq
                samples += 1
                if all(f.ready() for f in futs):
                    break
            rows = e.get(futs)
            t1 = time.perf_counter_ns()
        wall_ms = (t1 - t0) / 1e6
        completed = sum(1 for r in rows if r["status"] == "completed")
        prefix_ok = all(isinstance(r["actor_id"], str)
                        and r["actor_id"].startswith(prefix) for r in rows)
        lanes_seen = len({r["actor_id"] for r in rows})
        inv = _fifo_inversions(rows)
        # overlap_ratio ~ effective concurrent lanes: total requested service
        # divided by wall. ~num_lanes when lanes overlap; ~hpx_threads when a
        # non-yielding (spin) HpxLane is bound by the worker pool. OBSERVATION.
        overlap_ratio = (offered * service_ms) / wall_ms if wall_ms > 0 else None
        per_rep.append({
            "offered": offered, "rows": len(rows), "completed": completed,
            "lanes_seen": lanes_seen, "prefix_ok": prefix_ok,
            "fifo_inversions": inv, "wall_ms": wall_ms,
            "overlap_ratio": overlap_ratio, "max_active_lanes": max_active,
            "max_total_queue": max_total_queue, "samples": samples,
            "service_ms_observed_p50": _med(
                [r["service_ms_observed"] for r in rows]),
        })

    # Firm-gate facts: hold across every repeat. Observation fields: summarized
    # (median wall / overlap; the highest concurrency observed across repeats).
    g1 = all(r["rows"] == offered and r["completed"] == offered for r in per_rep)
    g2 = all(r["fifo_inversions"] == 0 for r in per_rep)
    g4 = all(r["prefix_ok"] for r in per_rep)
    lanes_ok = all(r["lanes_seen"] == num_lanes for r in per_rep)
    return {
        "backend": backend, "hpx_threads": hpx_threads, "num_lanes": num_lanes,
        "workload": workload, "service_ms": service_ms, "repeats": repeats,
        "offered": offered,
        # firm-gate facts
        "completed_all": g1, "fifo_inversions_zero": g2, "prefix_ok": g4,
        "lanes_seen_ok": lanes_ok,
        "completed_min": min(r["completed"] for r in per_rep),
        "fifo_inversions_max": max(r["fifo_inversions"] for r in per_rep),
        # observation-only
        "obs_drain_wall_ms_p50": _med([r["wall_ms"] for r in per_rep]),
        "obs_overlap_ratio_p50": _med([r["overlap_ratio"] for r in per_rep]),
        "obs_max_active_lanes": max(r["max_active_lanes"] for r in per_rep),
        "obs_max_total_queue": max(r["max_total_queue"] for r in per_rep),
        "obs_service_ms_observed_p50": _med(
            [r["service_ms_observed_p50"] for r in per_rep]),
    }


def _g3_sleep_probe(Engine, backend, hpx_threads, prefix):
    """G3 (firm): lane_stats() reports active + queue_depth under load and returns
    to idle after drain. Run on a SLEEP load so the stats snapshot is not subject
    to spin worker-pool starvation (a parked sleep yields the HPX worker, so the
    hpx-backend stats hop returns promptly). Mirrors exp21's lane_stats scenario."""
    fails = []
    # Shape on a fresh multi-lane engine: idle, distinct actor_ids, right keys.
    sl = 4
    with Engine(num_lanes=sl, hpx_threads=hpx_threads, lane_impl=backend) as e:
        fresh = e.lane_stats()
    keys_ok = all(set(r.keys()) >= {"actor_id", "queue_depth", "active"}
                  for r in fresh) and len(fresh) == sl
    if not keys_ok:
        fails.append("g3: fresh lane_stats shape/keys wrong")
    if any(r["active"] for r in fresh) or any(r["queue_depth"] for r in fresh):
        fails.append("g3: fresh engine not idle")
    if len({r["actor_id"] for r in fresh}) != sl:
        fails.append("g3: fresh actor_ids not distinct")
    if not all(isinstance(r["actor_id"], str) and r["actor_id"].startswith(prefix)
               for r in fresh):
        fails.append("g3: fresh actor_id prefix mismatch")

    # Under load on a single lane: occupier active + a queued backlog, then idle.
    with Engine(num_lanes=1, hpx_threads=hpx_threads, lane_impl=backend) as e:
        occ = e.submit(service_ms=300.0, work_mode="sleep")
        _poll(lambda: e.lane_stats()[0]["active"] or None)
        queued = [e.submit(service_ms=50.0, work_mode="sleep") for _ in range(2)]
        loaded = e.lane_stats()
        active_observed = bool(loaded[0]["active"])
        depth_observed = int(loaded[0]["queue_depth"])
        e.get([occ, *queued])  # drain
        idle = _poll(lambda: e.lane_stats()
                     if (not e.lane_stats()[0]["active"]
                         and e.lane_stats()[0]["queue_depth"] == 0) else None)
        idle_ok = (not idle[0]["active"]) and idle[0]["queue_depth"] == 0
    if not active_observed:
        fails.append("g3: occupier never observed active under load")
    if depth_observed <= 0:
        fails.append("g3: no queued depth observed under load")
    if not idle_ok:
        fails.append("g3: did not return to idle after drain")
    return {
        "pass": not fails, "fails": fails, "stat_keys": sorted(fresh[0].keys()),
        "active_observed_under_load": active_observed,
        "queue_depth_observed_under_load": depth_observed,
        "returned_to_idle": idle_ok,
    }


def _worker(args):
    from rayx import Engine

    backend = args.backend
    hpx_threads = args.hpx_threads
    prefix = EXPECTED_PREFIX[backend]
    num_lanes = QUICK_LANES if args.quick else NUM_LANES_FULL
    repeats = REPEATS_QUICK if args.quick else REPEATS_FULL
    sleep_ms = SLEEP_MS_QUICK if args.quick else SLEEP_MS_FULL
    spin_ms = SPIN_MS_QUICK if args.quick else SPIN_MS_FULL
    svc = {"sleep": sleep_ms, "spin": spin_ms}

    cells = []
    for workload in WORKLOADS:
        for L in num_lanes:
            cells.append(_run_cell(Engine, backend, hpx_threads, L, workload,
                                   svc[workload], repeats, prefix))
    g3 = _g3_sleep_probe(Engine, backend, hpx_threads, prefix)

    out = {
        "backend": backend, "expected_prefix": prefix,
        "hpx_threads": hpx_threads, "quick": bool(args.quick),
        "num_lanes": list(num_lanes), "repeats": repeats,
        "service_ms": svc, "g3_lane_stats_sanity": g3, "cells": cells,
    }
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2)
    return 0


# --------------------------------------------------------------------------
# Orchestrator: spawn one subprocess per (backend, hpx_threads); gate G1-G5.
# --------------------------------------------------------------------------
def _gate(workers, hpx_threads_list):
    """Firm structural gates only (G1-G5). workers: {(backend,ht): summary}."""
    fails = []
    # G1-G4 per backend/ht: every cell + the G3 sleep probe.
    for (b, ht), s in workers.items():
        if s["expected_prefix"] != EXPECTED_PREFIX[b]:
            fails.append(f"{b}/ht{ht}: expected_prefix {s['expected_prefix']!r}")
        if not s["g3_lane_stats_sanity"]["pass"]:
            fails.append(f"{b}/ht{ht}: G3 lane_stats sanity: "
                         f"{s['g3_lane_stats_sanity']['fails']}")
        for c in s["cells"]:
            tag = f"{b}/ht{ht}/L{c['num_lanes']}/{c['workload']}"
            if not c["completed_all"]:
                fails.append(f"{tag}: G1 completion "
                             f"(completed_min={c['completed_min']}/{c['offered']})")
            if not c["fifo_inversions_zero"]:
                fails.append(f"{tag}: G2 FIFO inversions "
                             f"(max={c['fifo_inversions_max']})")
            if not c["prefix_ok"]:
                fails.append(f"{tag}: G4 actor_id prefix mismatch")
            if not c["lanes_seen_ok"]:
                fails.append(f"{tag}: lanes_seen != num_lanes")
    # G5 cross-backend completion parity: same offered/completed per matching cell.
    for ht in hpx_threads_list:
        std = {(c["num_lanes"], c["workload"]): c
               for c in workers[("std", ht)]["cells"]}
        hpx = {(c["num_lanes"], c["workload"]): c
               for c in workers[("hpx", ht)]["cells"]}
        if set(std) != set(hpx):
            fails.append(f"ht{ht}: backends ran different cell sets")
            continue
        for key in std:
            cs, ch = std[key], hpx[key]
            if cs["offered"] != ch["offered"]:
                fails.append(f"ht{ht}/{key}: G5 offered mismatch "
                             f"{cs['offered']} != {ch['offered']}")
            if cs["completed_min"] != cs["offered"] or \
                    ch["completed_min"] != ch["offered"]:
                fails.append(f"ht{ht}/{key}: G5 completion parity "
                             f"(std={cs['completed_min']}, hpx={ch['completed_min']}, "
                             f"offered={cs['offered']})")
    return fails


def _observations(workers, hpx_threads_list):
    """Non-gated mechanism evidence: per-cell drain wall / overlap / sampled
    concurrency for each backend, plus the inferred convergence/divergence pattern.
    NONE of this is a gate; it carries no performance / speedup claim."""
    cells = []
    for (b, ht), s in sorted(workers.items()):
        for c in s["cells"]:
            cells.append({
                "backend": b, "hpx_threads": ht, "num_lanes": c["num_lanes"],
                "workload": c["workload"], "offered": c["offered"],
                "drain_wall_ms_p50": c["obs_drain_wall_ms_p50"],
                "overlap_ratio_p50": c["obs_overlap_ratio_p50"],
                "max_active_lanes": c["obs_max_active_lanes"],
                "max_total_queue": c["obs_max_total_queue"],
                "service_ms_observed_p50": c["obs_service_ms_observed_p50"],
            })

    def _ratio(b, ht, L, wl):
        for c in workers[(b, ht)]["cells"]:
            if c["num_lanes"] == L and c["workload"] == wl:
                return c["obs_overlap_ratio_p50"], c["obs_max_active_lanes"]
        return None, None

    # Inferred pattern at the largest lane count, per hpx_threads (observation):
    # sleep should overlap (ratio climbs toward num_lanes for BOTH backends);
    # spin should reveal HpxLane bounded near hpx_threads while ServiceLane is not.
    L = max(workers[("std", hpx_threads_list[0])]["num_lanes"])
    pattern = []
    for ht in hpx_threads_list:
        s_sleep, s_sleep_act = _ratio("std", ht, L, "sleep")
        h_sleep, h_sleep_act = _ratio("hpx", ht, L, "sleep")
        s_spin, s_spin_act = _ratio("std", ht, L, "spin")
        h_spin, h_spin_act = _ratio("hpx", ht, L, "spin")
        pattern.append({
            "hpx_threads": ht, "num_lanes": L,
            "sleep": {"std_overlap_ratio": s_sleep, "hpx_overlap_ratio": h_sleep,
                      "std_max_active_lanes": s_sleep_act,
                      "hpx_max_active_lanes": h_sleep_act},
            "spin": {"std_overlap_ratio": s_spin, "hpx_overlap_ratio": h_spin,
                     "std_max_active_lanes": s_spin_act,
                     "hpx_max_active_lanes": h_spin_act},
        })
    return {"cells": cells, "inferred_pattern_at_max_lanes": pattern}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run one (backend, hpx_threads) sweep")
    ap.add_argument("--backend", choices=BACKENDS)
    ap.add_argument("--hpx-threads", dest="hpx_threads")
    ap.add_argument("--num-lanes", dest="num_lanes_override",
                    help='override swept lane counts, e.g. "2,4,8"')
    ap.add_argument("--workload", choices=("sleep", "spin", "both"), default="both")
    ap.add_argument("--repeats", type=int)
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="smaller matrix (hpx_threads 1,2; num_lanes 2,4; "
                         "1 repeat); no aggregate.json written")
    args = ap.parse_args(argv)

    # Resolve the swept axes (worker reads the same globals; the orchestrator may
    # override them via env for the worker subprocesses).
    global WORKLOADS, QUICK_LANES, NUM_LANES_FULL, REPEATS_FULL, REPEATS_QUICK
    if args.workload != "both":
        WORKLOADS = (args.workload,)
    if args.num_lanes_override:
        lanes = tuple(int(x) for x in args.num_lanes_override.split(","))
        NUM_LANES_FULL = lanes
        QUICK_LANES = lanes
    else:
        QUICK_LANES = NUM_LANES_QUICK
    if args.repeats is not None:
        REPEATS_FULL = args.repeats
        REPEATS_QUICK = args.repeats

    if args.worker:
        args.hpx_threads = int(args.hpx_threads)
        return _worker(args)

    if args.hpx_threads:
        hpx_threads_list = tuple(int(x) for x in args.hpx_threads.split(","))
    else:
        hpx_threads_list = HPX_THREADS_QUICK if args.quick else HPX_THREADS_FULL

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results",
                           f"rayx_hpxlane_load_divergence_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp22] raw -> {raw_dir}")
    print(f"[exp22] backends={BACKENDS} hpx_threads={hpx_threads_list} "
          f"workloads={WORKLOADS} quick={args.quick} "
          f"(structural gates only; divergence is OBSERVATION, never gated)")

    workers = {}
    for ht in hpx_threads_list:
        for backend in BACKENDS:
            out = os.path.join(raw_dir, f"{backend}_ht{ht}.json")
            cmd = [sys.executable, os.path.abspath(__file__), "--worker",
                   "--backend", backend, "--hpx-threads", str(ht),
                   "--workload", args.workload, "--out", out]
            if args.quick:
                cmd.append("--quick")
            if args.num_lanes_override:
                cmd += ["--num-lanes", args.num_lanes_override]
            if args.repeats is not None:
                cmd += ["--repeats", str(args.repeats)]
            proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True,
                                  text=True)
            if proc.returncode != 0:
                sys.stderr.write(proc.stderr)
                raise SystemExit(f"[exp22] worker {backend}/ht{ht} failed")
            s = workers[(backend, ht)] = _load(out)
            g3 = "ok" if s["g3_lane_stats_sanity"]["pass"] else "FAIL"
            ncells = len(s["cells"])
            print(f"  {backend:3s} ht={ht} cells={ncells} g3={g3} "
                  f"prefix={s['expected_prefix']}")

    fails = _gate(workers, hpx_threads_list)
    obs = _observations(workers, hpx_threads_list)

    aggregate = {
        "experiment": "rayx_hpxlane_load_divergence",
        "schema": "rayx-hpxlane-load-divergence-1",
        "schema_note": "experiment-local; NOT the v1 benchmark JSONL schema",
        "boundary": "hpx-python-frontend",
        "goal": "where rayx lane_impl='std' (std::thread ServiceLane) and "
                "lane_impl='hpx' (cooperative HpxLane) STRUCTURALLY DIVERGE under "
                "load, after exp21 proved contract parity. Mechanism/structure "
                "evidence; the divergence is observed, never gated.",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "backends": list(BACKENDS),
        "expected_prefix": EXPECTED_PREFIX,
        "matrix": {
            "hpx_threads": list(hpx_threads_list),
            "num_lanes_full": list(NUM_LANES_FULL),
            "num_lanes_quick": list(NUM_LANES_QUICK),
            "workloads": list(WORKLOADS),
            "reqs_per_lane": REQS_PER_LANE,
            "repeats_full": REPEATS_FULL, "repeats_quick": REPEATS_QUICK,
            "sleep_ms": {"full": SLEEP_MS_FULL, "quick": SLEEP_MS_QUICK},
            "spin_ms": {"full": SPIN_MS_FULL, "quick": SPIN_MS_QUICK},
            "subprocess_axis": "one process per (backend, hpx_threads) because "
                               "the HPX runtime is process-fixed",
        },
        "firm_gates": {
            "G1": "completion: rows == offered, one row per request",
            "G2": "per-lane FIFO: end_ns monotonic in submit order (inversions==0)",
            "G3": "lane_stats sanity: active + queue_depth under a controlled sleep "
                  "load, idle after drain",
            "G4": "actor_id prefix: act-hpx- (std) / act-hpxl- (hpx)",
            "G5": "cross-backend completion parity per matching cell",
        },
        "note": "Structural gates only; the spin scheduling-bound divergence is "
                "OBSERVATION-ONLY, never gated -- lane_stats().active is valid RayX "
                "observability but not a perfect proof of true HPX worker-level "
                "concurrency, and under spin the stats call itself can be delayed "
                "by saturated workers. No speedup claim, no 'HPX beats Ray' claim. "
                "work_mode='spin' is a synthetic CPU-bound diagnostic/calibration "
                "mode, NOT the serving design. rayx-only opt-in HpxLane backend "
                "(lane_impl='hpx'); NOT the exp16 native single-lane probe and NOT "
                "the exp20 hpx::async/hpx::dataflow task-pool probe. Not Ray Serve, "
                "not a Ray object store, not real inference. No analyzer / "
                "benchmark-JSONL-schema / driver / CI / Future-ownership change; no "
                "HPX internals exposed. Raw per-worker JSON is experiment-local "
                "scratch under results/ (gitignored).",
        "all_structural_gates_passed": not fails,
        "gate_failures": fails,
        "g3_lane_stats_sanity": {
            f"{b}_ht{ht}": workers[(b, ht)]["g3_lane_stats_sanity"]["pass"]
            for (b, ht) in sorted(workers)
        },
        "observations_non_gating": obs,
    }

    if not fails:
        print("[exp22] structural gates passed (G1-G5): contract holds under load "
              "for both backends; divergence reported as observation only")
    else:
        print("[exp22] STRUCTURAL GATE FAILURES:")
        for f in fails:
            print(f"  - {f}")

    # Compact observation readout (NON-gating) at the largest lane count.
    for p in obs["inferred_pattern_at_max_lanes"]:
        print(f"  [obs] ht={p['hpx_threads']} L={p['num_lanes']} "
              f"sleep overlap std={p['sleep']['std_overlap_ratio']} "
              f"hpx={p['sleep']['hpx_overlap_ratio']} | "
              f"spin overlap std={p['spin']['std_overlap_ratio']} "
              f"hpx={p['spin']['hpx_overlap_ratio']} "
              f"(spin max_active std={p['spin']['std_max_active_lanes']} "
              f"hpx={p['spin']['hpx_max_active_lanes']})")

    if args.quick:
        print("[exp22] --quick: aggregate.json NOT written (smoke only)")
    else:
        agg_path = os.path.join(_HERE, "aggregate.json")
        with open(agg_path, "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp22] curated aggregate -> {agg_path}")

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
