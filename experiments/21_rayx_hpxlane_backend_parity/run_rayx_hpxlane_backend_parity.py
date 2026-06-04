#!/usr/bin/env python3
"""Experiment 21 runner: RayX lane-contract parity across lane_impl backends.

Compares rayx ``Engine(lane_impl="std")`` (the std::thread ``ServiceLane`` stable
comparison anchor) against ``Engine(lane_impl="hpx")`` (the opt-in cooperative
HPX-thread ``HpxLane``) and verifies that BOTH honor the SAME RayX lane contract:

  * all admitted requests complete unless intentionally cancelled
  * exactly one result row per admitted request
  * get / wait / as_completed retirement
  * chunked synthetic service
  * queued cancellation (skip before service -> chunks_completed == 0)
  * running cancellation at a chunk boundary (0 < chunks_completed < chunks)
  * bounded admission + QueueFullError (rejected submit raises; admitted completes)
  * lane_stats() shape and semantics (active / queue_depth under load -> idle)

The only intended observable difference is the lane ``actor_id`` PREFIX:
``act-hpx-`` for std / ``ServiceLane``, ``act-hpxl-`` for hpx / ``HpxLane``.

PARITY / SEMANTICS first, NOT performance. Timing is recorded as an OBSERVATION
only and is NEVER a gate; this makes no speedup claim and no "HPX beats Ray"
claim. It does not change the analyzer, the benchmark JSONL schema, the benchmark
drivers, CI, or the public Future ownership model, and exposes no HPX internals.
It is not Ray Serve, not a Ray object store, and not real model inference.

This exercises the rayx-backend ``HpxLane`` (an opt-in serialized lane,
``lane_impl="hpx"``), NOT the experiment-20 ``hpx::async`` / ``hpx::dataflow``
task-pool mechanism probe -- those pools are deliberately not drop-in rayx lane
backends.

Each backend runs in its OWN subprocess (the rayx Engine owns one HPX runtime per
process). Within a backend's process, each scenario uses its own short-lived
Engine context, exactly as bench/smoke_rayx.py does. Raw per-backend JSON is a
small experiment-local scratch file under results/ (gitignored), NOT the v1
benchmark schema. Tracked evidence: the curated aggregate.json beside this script
plus the markdown report.

Usage (repo root, venv active, _rayx built):
    python experiments/21_rayx_hpxlane_backend_parity/run_rayx_hpxlane_backend_parity.py
    python experiments/21_rayx_hpxlane_backend_parity/run_rayx_hpxlane_backend_parity.py --quick
    # internal: --worker --backend {std,hpx} runs one backend's battery
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
HPX_THREADS = 4
OCC_MS = 120.0          # spin occupier hold: >> microseconds to fill/probe/cancel
SVC_MS = 4.0            # tiny synthetic service for completion scenarios
CHUNK_CANCEL_MS = 240.0  # running-cancel request total: 6 chunks of ~40ms
CHUNK_CANCEL_N = 6       # boundaries ahead at the ~50ms start probe (final ~200ms)

# (multi_lanes, basic_n, fan_n, stats_lanes). The cancellation / bounded-admission
# scenarios are lane-routing sensitive and run at num_lanes=1 (as smoke does), so
# only the round-robin completion + lane_stats-shape scenarios vary with this.
FULL = {"multi_lanes": 4, "basic_n": 24, "fan_n": 12, "stats_lanes": 4}
QUICK = {"multi_lanes": 2, "basic_n": 8, "fan_n": 6, "stats_lanes": 2}

SCENARIO_DESCS = {
    "basic": "round-robin burst via get(): all complete, one row each, prefix + "
             "lane balance",
    "wait": "wait(num_returns=1) partitions ready/rest, non-consuming, all retire",
    "as_completed": "as_completed yields each future once, all complete",
    "chunked": "chunked service completes, echoes chunks/chunk_delay_ms, full run",
    "queued_cancel": "victim queued behind a spin occupier -> cancel True, "
                     "status=cancelled, chunks_completed == 0",
    "running_cancel": "started chunked request -> cancel True, status=cancelled, "
                      "0 < chunks_completed < chunks",
    "bounded_admission": "cap full -> QueueFullError (no-op on depth); admitted + "
                         "occupier complete",
    "lane_stats": "shape {actor_id,queue_depth,active}; active+depth under load; "
                  "returns to idle after drain",
}


def _p50(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _load(path):
    with open(path) as fh:
        return json.load(fh)


def _poll(fn, tries=2_000_000):
    """Bounded poll (no sleep/timing assertion) until fn() is truthy; returns it."""
    for _ in range(tries):
        v = fn()
        if v:
            return v
    raise RuntimeError("poll: condition never became true")


def _check_stat_shape(rows, expected_lanes, prefix):
    """Shape/type checks for a lane_stats() snapshot. Returns (ok, fails)."""
    fails = []
    if not isinstance(rows, list):
        return False, [f"lane_stats not a list: {type(rows).__name__}"]
    if len(rows) != expected_lanes:
        fails.append(f"lane_stats {len(rows)} rows != {expected_lanes}")
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            fails.append(f"row[{i}] not a dict")
            continue
        for k in ("actor_id", "queue_depth", "active"):
            if k not in r:
                fails.append(f"row[{i}] missing {k!r}")
        if not isinstance(r.get("actor_id"), str) or not r["actor_id"]:
            fails.append(f"row[{i}] actor_id not a non-empty str")
        elif not r["actor_id"].startswith(prefix):
            fails.append(f"row[{i}] actor_id {r['actor_id']!r} not prefixed {prefix!r}")
        if isinstance(r.get("queue_depth"), bool) or not isinstance(
                r.get("queue_depth"), int):
            fails.append(f"row[{i}] queue_depth not int")
        if not isinstance(r.get("active"), bool):
            fails.append(f"row[{i}] active not bool")
    return (not fails), fails


# --------------------------------------------------------------------------
# Worker: run ONE backend's whole parity battery in its own process.
# --------------------------------------------------------------------------
def _worker(args):
    from collections import Counter
    from rayx import Engine, QueueFullError

    backend = args.backend
    prefix = EXPECTED_PREFIX[backend]
    cfg = QUICK if args.quick else FULL
    L = cfg["multi_lanes"]
    scen = {}

    def _prefix_ok(rows):
        return all(isinstance(r["actor_id"], str)
                   and r["actor_id"].startswith(prefix) for r in rows)

    # ---- basic: round-robin burst via get() ----------------------------------
    fails = []
    n = cfg["basic_n"]
    with Engine(num_lanes=L, hpx_threads=HPX_THREADS, lane_impl=backend) as e:
        futs = [e.submit(service_ms=SVC_MS, label=f"b{i}") for i in range(n)]
        rows = e.get(futs)
    completed = sum(1 for r in rows if r["status"] == "completed")
    lane_counts = Counter(r["actor_id"] for r in rows)
    spread = (max(lane_counts.values()) - min(lane_counts.values())) if lane_counts else None
    if len(rows) != n:
        fails.append(f"basic: {len(rows)} rows != {n}")
    if completed != n:
        fails.append(f"basic: {completed} completed != {n}")
    if not _prefix_ok(rows):
        fails.append("basic: actor_id prefix mismatch")
    if len(lane_counts) != L:
        fails.append(f"basic: lanes_seen {len(lane_counts)} != {L}")
    if spread is None or spread > 1:
        fails.append(f"basic: lane request-count spread {spread} > 1")
    scen["basic"] = {
        "pass": not fails, "fails": fails, "requests": n, "rows": len(rows),
        "completed": completed, "lanes_seen": len(lane_counts),
        "lane_count_spread": spread,
        "actor_id_sample": rows[0]["actor_id"] if rows else None,
        "service_ms_observed_p50": _p50([r["service_ms_observed"] for r in rows]),
    }

    # ---- wait: non-consuming ready/rest partition ----------------------------
    fails = []
    n = cfg["fan_n"]
    with Engine(num_lanes=L, hpx_threads=HPX_THREADS, lane_impl=backend) as e:
        futs = [e.submit(service_ms=SVC_MS) for _ in range(n)]
        ready, rest = e.wait(futs, num_returns=1)
        rows = e.get(futs)  # wait is non-consuming -> every future still retirable
    completed = sum(1 for r in rows if r["status"] == "completed")
    if len(ready) < 1:
        fails.append("wait: first sweep returned no ready futures")
    if len(ready) + len(rest) != n:
        fails.append(f"wait: partition {len(ready)}+{len(rest)} != {n}")
    if completed != n:
        fails.append(f"wait: {completed} completed != {n} (wait consumed a future?)")
    if not _prefix_ok(rows):
        fails.append("wait: actor_id prefix mismatch")
    scen["wait"] = {"pass": not fails, "fails": fails, "requests": n,
                    "ready_first_sweep": len(ready), "completed": completed}

    # ---- as_completed: each future yielded once, all complete ----------------
    fails = []
    n = cfg["fan_n"]
    with Engine(num_lanes=L, hpx_threads=HPX_THREADS, lane_impl=backend) as e:
        futs = [e.submit(service_ms=SVC_MS, label=f"a{i}") for i in range(n)]
        seen = [f.result() for f in e.as_completed(futs)]
    completed = sum(1 for r in seen if r["status"] == "completed")
    if len(seen) != n:
        fails.append(f"as_completed: yielded {len(seen)} != {n}")
    if completed != n:
        fails.append(f"as_completed: {completed} completed != {n}")
    if not _prefix_ok(seen):
        fails.append("as_completed: actor_id prefix mismatch")
    scen["as_completed"] = {"pass": not fails, "fails": fails, "requests": n,
                            "yielded": len(seen), "completed": completed}

    # ---- chunked: full run, echoes chunks/chunk_delay_ms ----------------------
    fails = []
    with Engine(num_lanes=1, hpx_threads=HPX_THREADS, lane_impl=backend) as e:
        row = e.submit(service_ms=8.0, work_mode="sleep", chunks=4,
                       chunk_delay_ms=1.0, label="chk").result()
    if row["status"] != "completed":
        fails.append(f"chunked: status {row['status']!r} != completed")
    if row["chunks"] != 4 or row["chunk_delay_ms"] != 1.0:
        fails.append(f"chunked: echo chunks={row['chunks']} "
                     f"delay={row['chunk_delay_ms']} (want 4 / 1.0)")
    if row["chunks_completed"] != 4:
        fails.append(f"chunked: chunks_completed {row['chunks_completed']} != 4")
    if not row["actor_id"].startswith(prefix):
        fails.append("chunked: actor_id prefix mismatch")
    scen["chunked"] = {"pass": not fails, "fails": fails, "status": row["status"],
                       "chunks": row["chunks"], "chunk_delay_ms": row["chunk_delay_ms"],
                       "chunks_completed": row["chunks_completed"]}

    # ---- queued cancellation: chunks_completed == 0 --------------------------
    fails = []
    with Engine(num_lanes=1, hpx_threads=HPX_THREADS, lane_impl=backend) as e:
        occ = e.submit(service_ms=OCC_MS, work_mode="spin")
        _poll(lambda: e.lane_stats()[0]["active"] or None)
        victim = e.submit(service_ms=SVC_MS, label="qv")
        did = e.cancel(victim)
        cancelled_flag = victim.cancelled()
        row = victim.result()
        occ.result()  # drain the occupier
    cc = row["chunks_completed"]
    if did is not True:
        fails.append(f"queued_cancel: cancel() returned {did!r} != True")
    if cancelled_flag is not True:
        fails.append("queued_cancel: cancelled() not True")
    if row["status"] != "cancelled":
        fails.append(f"queued_cancel: status {row['status']!r} != cancelled")
    if cc != 0:
        fails.append(f"queued_cancel: chunks_completed {cc} != 0 (not a queued skip)")
    scen["queued_cancel"] = {"pass": not fails, "fails": fails,
                             "cancel_returned": did, "status": row["status"],
                             "chunks_completed": cc}

    # ---- running cancellation at a chunk boundary: 0 < cc < chunks -----------
    fails = []
    with Engine(num_lanes=1, hpx_threads=HPX_THREADS, lane_impl=backend) as e:
        f = e.submit(service_ms=CHUNK_CANCEL_MS, work_mode="sleep",
                     chunks=CHUNK_CANCEL_N, chunk_delay_ms=0.0, label="rv")
        time.sleep(0.05)  # started; many boundaries remain (~40ms/chunk)
        did = e.cancel(f)
        cancelled_flag = f.cancelled()
        row = f.result()
    cc, ch = row["chunks_completed"], row["chunks"]
    if did is not True:
        fails.append(f"running_cancel: cancel() returned {did!r} != True")
    if cancelled_flag is not True:
        fails.append("running_cancel: cancelled() not True")
    if row["status"] != "cancelled":
        fails.append(f"running_cancel: status {row['status']!r} != cancelled")
    if not (0 < cc < ch):
        fails.append(f"running_cancel: chunks_completed {cc} not in (0, {ch})")
    scen["running_cancel"] = {"pass": not fails, "fails": fails,
                              "cancel_returned": did, "status": row["status"],
                              "chunks_completed": cc, "chunks": ch}

    # ---- bounded admission: QueueFullError + admitted/occupier complete ------
    fails = []
    cap = 2
    with Engine(num_lanes=1, hpx_threads=HPX_THREADS, lane_impl=backend,
                max_queue_depth_per_lane=cap) as e:
        occ = e.submit(service_ms=OCC_MS, work_mode="spin")
        _poll(lambda: e.lane_stats()[0]["active"] or None)
        admitted = [e.submit(service_ms=SVC_MS, work_mode="spin")
                    for _ in range(cap)]  # fill the queue to the cap
        depth_before = e.lane_stats()[0]["queue_depth"]
        raised, wrong_exc = False, None
        try:
            e.submit(service_ms=SVC_MS, work_mode="spin")  # lane full -> reject
        except QueueFullError:
            raised = True
        except Exception as ex:  # noqa: BLE001
            wrong_exc = type(ex).__name__
        depth_after = e.lane_stats()[0]["queue_depth"]
        rows = e.get([occ, *admitted])
    admitted_completed = sum(1 for r in rows if r["status"] == "completed")
    if not raised:
        fails.append(f"bounded_admission: QueueFullError not raised "
                     f"(wrong_exc={wrong_exc})")
    if depth_before != cap:
        fails.append(f"bounded_admission: depth_before {depth_before} != cap {cap}")
    if depth_after != depth_before:
        fails.append("bounded_admission: rejected submit changed queue_depth")
    if admitted_completed != len(rows):
        fails.append(f"bounded_admission: {admitted_completed}/{len(rows)} completed")
    scen["bounded_admission"] = {
        "pass": not fails, "fails": fails, "cap": cap,
        "queue_full_raised": raised, "wrong_exception": wrong_exc,
        "depth_before": depth_before, "depth_after": depth_after,
        "admitted_completed": admitted_completed, "admitted_total": len(rows),
    }

    # ---- lane_stats: shape (multi-lane) + active/depth under load -> idle -----
    fails = []
    sl = cfg["stats_lanes"]
    with Engine(num_lanes=sl, hpx_threads=HPX_THREADS, lane_impl=backend) as e:
        fresh = e.lane_stats()
    shape_ok, shape_fails = _check_stat_shape(fresh, sl, prefix)
    fails += shape_fails
    if any(r["active"] for r in fresh) or any(r["queue_depth"] for r in fresh):
        fails.append("lane_stats: fresh engine not idle")
    if len({r["actor_id"] for r in fresh}) != sl:
        fails.append("lane_stats: fresh actor_ids not distinct")
    with Engine(num_lanes=1, hpx_threads=HPX_THREADS, lane_impl=backend) as e:
        occ = e.submit(service_ms=OCC_MS, work_mode="spin")
        victims = [e.submit(service_ms=SVC_MS, work_mode="spin", label=f"s{i}")
                   for i in range(3)]
        loaded = _poll(lambda: e.lane_stats() if e.lane_stats()[0]["active"] else None)
        active_observed = bool(loaded[0]["active"])
        depth_observed = sum(int(r["queue_depth"]) for r in loaded)
        for v in victims:
            e.cancel(v)
            v.result()
        occ.result()
        idle = _poll(lambda: e.lane_stats()
                     if (not e.lane_stats()[0]["active"]
                         and e.lane_stats()[0]["queue_depth"] == 0) else None)
        idle_ok = (not idle[0]["active"]) and idle[0]["queue_depth"] == 0
    if not active_observed:
        fails.append("lane_stats: occupier never observed active")
    if depth_observed <= 0:
        fails.append("lane_stats: no queued depth observed under load")
    if not idle_ok:
        fails.append("lane_stats: did not return to idle after drain")
    scen["lane_stats"] = {
        "pass": not fails, "fails": fails, "shape_ok": shape_ok, "stats_lanes": sl,
        "stat_keys": sorted(fresh[0].keys()) if fresh else [],
        "active_observed_under_load": active_observed,
        "queue_depth_observed_under_load": depth_observed,
        "returned_to_idle": idle_ok,
    }

    out = {
        "backend": backend, "expected_prefix": prefix, "multi_lanes": L,
        "quick": bool(args.quick), "scenarios": scen,
        "all_pass": all(s["pass"] for s in scen.values()),
    }
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2)
    return 0


# --------------------------------------------------------------------------
# Orchestrator: run both backends, gate parity, curate aggregate.json.
# --------------------------------------------------------------------------
def _gate_parity(summaries):
    fails = []
    for b in BACKENDS:
        s = summaries[b]
        if s["expected_prefix"] != EXPECTED_PREFIX[b]:
            fails.append(f"{b}: expected_prefix {s['expected_prefix']!r}")
        if not s["all_pass"]:
            bad = [k for k, v in s["scenarios"].items() if not v["pass"]]
            for k in bad:
                fails.append(f"{b}: scenario {k} failed: {s['scenarios'][k]['fails']}")
    # The seam must actually switch lanes: the two backends report different
    # prefixes, and the observed sample matches each backend's expected prefix.
    if summaries["std"]["expected_prefix"] == summaries["hpx"]["expected_prefix"]:
        fails.append("std and hpx report the same actor_id prefix")
    for b in BACKENDS:
        sample = summaries[b]["scenarios"]["basic"].get("actor_id_sample")
        if not (isinstance(sample, str) and sample.startswith(EXPECTED_PREFIX[b])):
            fails.append(f"{b}: observed actor_id sample {sample!r} not prefixed "
                         f"{EXPECTED_PREFIX[b]!r}")
    # Parity: identical scenario set, and both backends agree on every pass/fail.
    if set(summaries["std"]["scenarios"]) != set(summaries["hpx"]["scenarios"]):
        fails.append("backends ran different scenario sets")
    else:
        for k in summaries["std"]["scenarios"]:
            ps, ph = (summaries["std"]["scenarios"][k]["pass"],
                      summaries["hpx"]["scenarios"][k]["pass"])
            if ps != ph:
                fails.append(f"scenario {k}: std pass={ps} != hpx pass={ph}")
    return fails


def _evidence(summaries):
    ev = {}
    for b in BACKENDS:
        sc = summaries[b]["scenarios"]
        ev[b] = {
            "expected_prefix": summaries[b]["expected_prefix"],
            "actor_id_sample": sc["basic"].get("actor_id_sample"),
            "scenario_pass": {k: v["pass"] for k, v in sc.items()},
            "all_pass": summaries[b]["all_pass"],
            "completion": {
                "basic_requests": sc["basic"]["requests"],
                "basic_completed": sc["basic"]["completed"],
                "basic_rows": sc["basic"]["rows"],
                "basic_lane_count_spread": sc["basic"]["lane_count_spread"],
                "as_completed_yielded": sc["as_completed"]["yielded"],
            },
            "cancellation": {
                "queued_chunks_completed": sc["queued_cancel"]["chunks_completed"],
                "running_chunks_completed": sc["running_cancel"]["chunks_completed"],
                "running_chunks": sc["running_cancel"]["chunks"],
            },
            "bounded_admission": {
                "cap": sc["bounded_admission"]["cap"],
                "queue_full_raised": sc["bounded_admission"]["queue_full_raised"],
                "depth_before_reject": sc["bounded_admission"]["depth_before"],
                "depth_after_reject": sc["bounded_admission"]["depth_after"],
                "admitted_completed": sc["bounded_admission"]["admitted_completed"],
                "admitted_total": sc["bounded_admission"]["admitted_total"],
            },
            "lane_stats": {
                "stat_keys": sc["lane_stats"]["stat_keys"],
                "shape_ok": sc["lane_stats"]["shape_ok"],
                "active_observed_under_load":
                    sc["lane_stats"]["active_observed_under_load"],
                "queue_depth_observed_under_load":
                    sc["lane_stats"]["queue_depth_observed_under_load"],
                "returned_to_idle": sc["lane_stats"]["returned_to_idle"],
            },
            "timing_observations_non_gating": {
                "basic_service_ms_observed_p50":
                    sc["basic"]["service_ms_observed_p50"],
            },
        }
    return ev


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run one backend's battery and write its JSON")
    ap.add_argument("--backend", choices=BACKENDS)
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="smaller matrix (multi_lanes 2, smaller N); no "
                         "aggregate.json written")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker(args)

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results",
                           f"rayx_hpxlane_backend_parity_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp21] raw -> {raw_dir}")
    print(f"[exp21] backends={BACKENDS} quick={args.quick} "
          f"(parity/semantics only; timing non-gating)")

    summaries = {}
    for backend in BACKENDS:
        out = os.path.join(raw_dir, f"{backend}.json")
        cmd = [sys.executable, os.path.abspath(__file__), "--worker",
               "--backend", backend, "--out", out]
        if args.quick:
            cmd.append("--quick")
        proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True)
        if proc.returncode != 0:
            sys.stderr.write(proc.stderr)
            raise SystemExit(f"[exp21] backend {backend} worker failed")
        s = summaries[backend] = _load(out)
        passes = sum(1 for v in s["scenarios"].values() if v["pass"])
        print(f"  {backend:3s} prefix={s['expected_prefix']:9s} "
              f"sample={s['scenarios']['basic']['actor_id_sample']} "
              f"scenarios={passes}/{len(s['scenarios'])} all_pass={s['all_pass']}")

    fails = _gate_parity(summaries)
    aggregate = {
        "experiment": "rayx_hpxlane_backend_parity",
        "schema_version": 1,
        "boundary": "hpx-python-frontend",
        "goal": "semantic parity (NOT performance) between rayx lane_impl='std' "
                "(std::thread ServiceLane) and lane_impl='hpx' (cooperative "
                "HpxLane): both honor the same RayX lane contract",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "backends": list(BACKENDS),
        "expected_prefix": EXPECTED_PREFIX,
        "matrix": {
            "full": FULL, "quick": QUICK, "hpx_threads": HPX_THREADS,
            "cancel_scenarios_num_lanes": 1,
            "occupier_ms": OCC_MS, "service_ms": SVC_MS,
        },
        "scenarios": SCENARIO_DESCS,
        "note": "Parity/semantics only; timing is an observation, NEVER a gate; "
                "no speedup claim and no 'HPX beats Ray' claim. This is the "
                "opt-in rayx HpxLane serialized backend (lane_impl='hpx'), NOT the "
                "experiment-20 hpx::async / hpx::dataflow task-pool probe. Not Ray "
                "Serve, not a Ray object store, not real inference. No analyzer / "
                "benchmark-JSONL-schema / driver / CI / Future-ownership change; no "
                "HPX internals exposed. Raw per-backend JSON is experiment-local "
                "scratch under results/ (gitignored), NOT the v1 benchmark schema.",
        "all_parity_gates_passed": not fails,
        "gate_failures": fails,
        "evidence": _evidence(summaries),
    }

    if not fails:
        print("[exp21] parity gates passed: both backends honor the same RayX "
              "lane contract; only the actor_id prefix differs "
              "(std act-hpx- vs hpx act-hpxl-)")
    else:
        print("[exp21] PARITY GATE FAILURES:")
        for f in fails:
            print(f"  - {f}")

    if args.quick:
        print("[exp21] --quick: aggregate.json NOT written (smoke only)")
    else:
        agg_path = os.path.join(_HERE, "aggregate.json")
        with open(agg_path, "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp21] curated aggregate -> {agg_path}")

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
