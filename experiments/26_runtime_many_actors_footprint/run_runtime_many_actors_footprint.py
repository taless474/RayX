#!/usr/bin/env python3
"""Experiment 26 runner: many-actors stress / idle-footprint observation note.

Uses ONLY the public ``rayx.runtime`` API (``Runtime`` / ``create_actor`` /
``ActorHandle.call`` / ``ActorHandle.stats`` / ``Runtime.release_actor`` /
``lane_stats`` / ``shutdown``) to exercise the create -> use -> release ->
shutdown -> reinit lifecycle for MANY local native actors, and to observe
(never gate) what that costs on this machine. This is the Stage-1 evidence
note that became possible once ``Runtime.release_actor`` landed.

STRUCTURAL GATES (the result; independent of timing/memory):
  G1 create     exactly N actors; ids unique; every id matches rt-act-<16 hex>
  G2 calls      when calls_per_actor=1, every add(1) returns 1; every future
                retires completed (no cancelled/failed -- nothing is in flight
                at teardown in this design)
  G3 idle stats every actor's stats() reads queue_depth==0 / active==False
                after calls retire
  G4 lane_stats Runtime.lane_stats() stays op-lanes-only (exactly num_lanes
                rt-hpx- entries) with N actors live
  G5 release    (release path) release_actor returns None for every actor;
                afterwards call()/stats() raise RuntimeError on every handle
  G6 teardown   shutdown() clean on BOTH paths (after release-all, and with N
                live actors on the shutdown-only path); then a fresh Runtime
                creates one actor and completes one call (guard released)
  G7 cycle      (recycling-cycle cell) both create/release waves succeed with
                valid, wave-unique ids -- the RSS trajectory itself is
                observation, never a gate

OBSERVATION-ONLY (machine/allocator-specific; NEVER a gate, NO performance or
capacity claim, NO Ray comparison, NO "HPX beats Ray", NOT a benchmark
verdict): elapsed create/call/release/shutdown ms (+ derived per-actor ms) and
the best-effort RSS checkpoints below.

FOOTPRINT STRATEGY (best-effort, deliberately modest):
  * current RSS via ``ps -o rss= -p <pid>`` (KiB on macOS and Linux); if ps is
    unavailable the fields are null and no gate is affected;
  * ``resource.getrusage().ru_maxrss`` is reported as the PEAK only -- it is a
    high-water mark and never decreases, so it cannot show after-release /
    after-shutdown drops (bytes on macOS, KiB on Linux; normalized here);
  * checkpoints: baseline (pre-Runtime) -> after_runtime_start (the HPX-hat
    refinement separating one-time HPX runtime cost from per-actor cost) ->
    after_create (calls retired) -> after_release (release path) ->
    after_shutdown;
  * values rounded to 0.1 MB. Process-level RSS is ALLOCATOR-MEDIATED: HPX
    pools thread stacks/descriptors and malloc may retain freed memory, so a
    NON-DROP after release is NOT proof of a leak, and a drop is NOT proof of
    guaranteed reclamation. macOS memory compression makes RSS an undercount
    and adds wobble. All memory numbers are observation-only.

RECYCLING-CYCLE CELL (one extra observation, not multiplied across the
matrix): create N -> release all -> create N again -> release all, sampling
RSS between steps. A second-wave plateau near the first-wave level suggests
pool reuse (expected); repeated growth across waves would be the concerning
signal worth escalating. Default N=64, calls_per_actor=1, hpx_threads=1.

Each matrix cell runs in its OWN subprocess so every RSS baseline is pristine
(no warm HPX/malloc pools inherited from a previous cell); the worker prints
one JSON object on stdout. No raw JSONL, no results/ output; the tracked
evidence is the curated aggregate.json beside this script plus the markdown
report.

Usage (repo root, venv active, _rayx built):
    python experiments/26_runtime_many_actors_footprint/run_runtime_many_actors_footprint.py
    python experiments/26_runtime_many_actors_footprint/run_runtime_many_actors_footprint.py --quick
    # overrides: --actor-counts "1,16,64,256" --hpx-threads "1,2" --repeats N
    #            (512 actors is OPT-IN ONLY via --actor-counts "...,512")
    # internal: --worker / --cycle ...
"""
import argparse
import json
import os
import re
import resource
import statistics
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
if _RAYX_SRC not in sys.path:
    sys.path.insert(0, _RAYX_SRC)

ACTOR_ID_RE = re.compile(r"rt-act-[0-9a-f]{16}")

ACTOR_COUNTS_FULL = (1, 16, 64, 256)   # 512 is opt-in via --actor-counts only
ACTOR_COUNTS_QUICK = (1, 16)
CALLS_FULL = (0, 1)
CALLS_QUICK = (1,)
PATHS = ("release-all-then-shutdown", "shutdown-only")
HPX_THREADS_FULL = (1, 2)
HPX_THREADS_QUICK = (1,)
REPEATS_FULL = 2
REPEATS_QUICK = 1
CYCLE_COUNT_FULL = 64
CYCLE_COUNT_QUICK = 16


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 3) if xs else None


def _rss_mb():
    """Best-effort current RSS in MB via ps (KiB on macOS and Linux); None if
    unavailable. Never raises; a None never affects a structural gate."""
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())],
                             capture_output=True, text=True, timeout=10)
        if out.returncode != 0 or not out.stdout.strip():
            return None
        return round(int(out.stdout.split()[0]) / 1024.0, 1)
    except Exception:
        return None


def _ru_maxrss_mb():
    """Peak RSS (high-water mark; never decreases). bytes on macOS, KiB on
    Linux -- normalized to MB, rounded to 0.1."""
    v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    div = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
    return round(v / div, 1)


def _gate_ids(handles, n, fails, where):
    ids = [h.actor_id for h in handles]
    if len(handles) != n:
        fails.append(f"{where}: created {len(handles)} != {n}")
    if len(set(ids)) != len(ids):
        fails.append(f"{where}: actor ids not unique")
    if not all(ACTOR_ID_RE.fullmatch(i) for i in ids):
        fails.append(f"{where}: actor id format violation")


def _create_wave(rt, n, calls_per_actor, fails, where):
    """Create n actors (+ optional one retired call each); return (handles,
    elapsed_create_ms, elapsed_call_ms, status counts)."""
    t0 = time.perf_counter_ns()
    handles = [rt.create_actor("counter", 0) for _ in range(n)]
    elapsed_create_ms = (time.perf_counter_ns() - t0) / 1e6
    _gate_ids(handles, n, fails, where)
    elapsed_call_ms = None
    counts = {"completed": 0, "cancelled": 0, "failed": 0}
    if calls_per_actor:
        t0 = time.perf_counter_ns()
        futs = [h.call("add", 1) for h in handles]
        results = rt.get(futs)
        elapsed_call_ms = (time.perf_counter_ns() - t0) / 1e6
        for r in results:
            st = r.status
            counts[st] = counts.get(st, 0) + 1
            if st != "completed" or r.value != 1:
                fails.append(f"{where}: call did not complete with value 1 "
                             f"(status={st})")
                break
    return handles, elapsed_create_ms, elapsed_call_ms, counts


def _release_wave(rt, handles, fails, where):
    """Release every handle (gating None + post-release errors); return
    elapsed_release_ms."""
    t0 = time.perf_counter_ns()
    for h in handles:
        if rt.release_actor(h) is not None:
            fails.append(f"{where}: release_actor did not return None")
    elapsed_release_ms = (time.perf_counter_ns() - t0) / 1e6
    for h in handles:
        try:
            h.call("get")
            fails.append(f"{where}: call after release did not raise")
            break
        except RuntimeError:
            pass
        try:
            h.stats()
            fails.append(f"{where}: stats after release did not raise")
            break
        except RuntimeError:
            pass
    return elapsed_release_ms


def _reinit_gate(fails):
    from rayx.runtime import Runtime
    try:
        with Runtime(num_lanes=1) as rt2:
            a = rt2.create_actor("counter", 0)
            if a.call("add", 1).result().value != 1:
                fails.append("reinit: post-teardown actor call != 1")
    except Exception as e:  # noqa: BLE001 -- gate evidence, not control flow
        fails.append(f"reinit: fresh Runtime failed: {e!r}")


def _worker(args):
    """One matrix cell in a pristine process."""
    from rayx.runtime import Runtime

    fails = []
    rss_baseline = _rss_mb()
    rt = Runtime(num_lanes=1, hpx_threads=args.hpx_threads)
    rss_after_runtime_start = _rss_mb()

    handles, elapsed_create_ms, elapsed_call_ms, counts = _create_wave(
        rt, args.actor_count, args.calls, fails, "create")

    # G3: every actor idle after its calls retired (also a mass exercise of the
    # per-actor observability path).
    for h in handles:
        s = h.stats()
        if s["queue_depth"] != 0 or s["active"] is not False:
            fails.append("stats: actor not idle after retire")
            break

    # G4: op-lane observability uncontaminated by N live actor lanes.
    ls = rt.lane_stats()
    if len(ls) != rt.num_lanes() or not all(
            x["actor_id"].startswith("rt-hpx-") for x in ls):
        fails.append("lane_stats: not op-lanes-only with actors live")

    rss_after_create = _rss_mb()

    elapsed_release_ms = None
    rss_after_release = None
    if args.path == "release-all-then-shutdown":
        elapsed_release_ms = _release_wave(rt, handles, fails, "release")
        rss_after_release = _rss_mb()

    t0 = time.perf_counter_ns()
    rt.shutdown()
    elapsed_shutdown_ms = (time.perf_counter_ns() - t0) / 1e6
    rss_after_shutdown = _rss_mb()

    _reinit_gate(fails)

    out = {
        "kind": "cell",
        "actor_count": args.actor_count,
        "hpx_threads": args.hpx_threads,
        "calls_per_actor": args.calls,
        "release_path": args.path,
        "gate_failures": fails,
        "completed": counts["completed"], "cancelled": counts["cancelled"],
        "failed": counts["failed"],
        "elapsed_create_ms": round(elapsed_create_ms, 3),
        "elapsed_call_ms": (round(elapsed_call_ms, 3)
                            if elapsed_call_ms is not None else None),
        "elapsed_release_ms": (round(elapsed_release_ms, 3)
                               if elapsed_release_ms is not None else None),
        "elapsed_shutdown_ms": round(elapsed_shutdown_ms, 3),
        "per_actor_create_ms": round(elapsed_create_ms / args.actor_count, 4),
        "per_actor_release_ms": (
            round(elapsed_release_ms / args.actor_count, 4)
            if elapsed_release_ms is not None else None),
        "rss_mb_baseline": rss_baseline,
        "rss_mb_after_runtime_start": rss_after_runtime_start,
        "rss_mb_after_create": rss_after_create,
        "rss_mb_after_release": rss_after_release,
        "rss_mb_after_shutdown": rss_after_shutdown,
        "ru_maxrss_mb": _ru_maxrss_mb(),
    }
    print(json.dumps(out))
    return 0


def _cycle_worker(args):
    """Recycling-cycle cell: create N -> release -> create N -> release, with
    RSS sampled between steps. The trajectory is OBSERVATION; only the
    lifecycle steps themselves are gated (G7)."""
    from rayx.runtime import Runtime

    fails = []
    rt = Runtime(num_lanes=1, hpx_threads=args.hpx_threads)
    rss_before_cycle = _rss_mb()  # post-runtime-start, pre-wave baseline

    w1, _, _, _ = _create_wave(rt, args.actor_count, args.calls, fails,
                               "cycle wave1 create")
    rss_after_first_create = _rss_mb()
    _release_wave(rt, w1, fails, "cycle wave1 release")
    rss_after_first_release = _rss_mb()

    w2, _, _, _ = _create_wave(rt, args.actor_count, args.calls, fails,
                               "cycle wave2 create")
    rss_after_second_create = _rss_mb()
    _release_wave(rt, w2, fails, "cycle wave2 release")
    rss_after_second_release = _rss_mb()

    rt.shutdown()
    _reinit_gate(fails)

    out = {
        "kind": "cycle",
        "actor_count": args.actor_count,
        "hpx_threads": args.hpx_threads,
        "calls_per_actor": args.calls,
        "gate_failures": fails,
        "rss_mb_before_cycle": rss_before_cycle,
        "rss_mb_after_first_create": rss_after_first_create,
        "rss_mb_after_first_release": rss_after_first_release,
        "rss_mb_after_second_create": rss_after_second_create,
        "rss_mb_after_second_release": rss_after_second_release,
        "ru_maxrss_mb": _ru_maxrss_mb(),
    }
    print(json.dumps(out))
    return 0


# --------------------------------------------------------------------------
# Orchestrator
# --------------------------------------------------------------------------
def _run_sub(extra):
    cmd = [sys.executable, os.path.abspath(__file__)] + extra
    proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(
            "[exp26] worker failed (see stderr above). If rayx/_rayx failed "
            "to import, build it first: cmake --build python/build")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true", help="internal: one cell")
    ap.add_argument("--cycle", action="store_true",
                    help="internal: recycling-cycle cell")
    ap.add_argument("--actor-count", dest="actor_count", type=int)
    ap.add_argument("--hpx-threads", dest="hpx_threads_one", type=int)
    ap.add_argument("--calls", type=int)
    ap.add_argument("--path", choices=PATHS)
    ap.add_argument("--actor-counts", dest="actor_counts",
                    help='override swept counts, e.g. "1,16,64,256" '
                         "(512 is opt-in only, via this flag)")
    ap.add_argument("--hpx-threads-list", "--hpx-threads-sweep",
                    dest="hpx_threads_list",
                    help='override swept HPX worker counts, e.g. "1,2"')
    ap.add_argument("--repeats", type=int)
    ap.add_argument("--quick", action="store_true",
                    help="small matrix (counts 1,16; calls 1; ht 1; 1 repeat; "
                         "cycle N=16); no aggregate.json written")
    args = ap.parse_args(argv)

    if args.worker or args.cycle:
        args.hpx_threads = args.hpx_threads_one
        return (_cycle_worker if args.cycle else _worker)(args)

    if args.actor_counts:
        counts = tuple(int(x) for x in args.actor_counts.split(","))
    else:
        counts = ACTOR_COUNTS_QUICK if args.quick else ACTOR_COUNTS_FULL
    if args.hpx_threads_list:
        hts = tuple(int(x) for x in args.hpx_threads_list.split(","))
    else:
        hts = HPX_THREADS_QUICK if args.quick else HPX_THREADS_FULL
    calls_list = CALLS_QUICK if args.quick else CALLS_FULL
    repeats = args.repeats if args.repeats is not None else (
        REPEATS_QUICK if args.quick else REPEATS_FULL)
    cycle_n = CYCLE_COUNT_QUICK if args.quick else CYCLE_COUNT_FULL

    print(f"[exp26] actor_counts={counts} hpx_threads={hts} "
          f"calls={calls_list} paths={PATHS} repeats={repeats} "
          f"cycle_n={cycle_n} quick={args.quick} (structural gates only; all "
          f"timing/RSS observation-only, best-effort)")

    all_fails = []
    cells = []
    for ht in hts:
        for n in counts:
            for calls in calls_list:
                for path in PATHS:
                    reps = []
                    for _ in range(repeats):
                        reps.append(_run_sub(
                            ["--worker", "--actor-count", str(n),
                             "--hpx-threads", str(ht), "--calls", str(calls),
                             "--path", path]))
                    for r in reps:
                        all_fails.extend(r["gate_failures"])
                    cell = {
                        "actor_count": n, "hpx_threads": ht,
                        "calls_per_actor": calls, "release_path": path,
                        "repeats": repeats,
                        "gates_ok": all(not r["gate_failures"] for r in reps),
                        "completed": reps[0]["completed"],
                        "cancelled": reps[0]["cancelled"],
                        "failed": reps[0]["failed"],
                    }
                    for k in ("elapsed_create_ms", "elapsed_call_ms",
                              "elapsed_release_ms", "elapsed_shutdown_ms",
                              "per_actor_create_ms", "per_actor_release_ms",
                              "rss_mb_baseline", "rss_mb_after_runtime_start",
                              "rss_mb_after_create", "rss_mb_after_release",
                              "rss_mb_after_shutdown", "ru_maxrss_mb"):
                        cell[k] = _med([r[k] for r in reps])
                    cells.append(cell)
                    rel = (f" release={cell['elapsed_release_ms']}ms"
                           if cell["elapsed_release_ms"] is not None else "")
                    print(f"  ht={ht} N={n:3d} calls={calls} "
                          f"{path:24s} ok={cell['gates_ok']} "
                          f"[obs] create={cell['elapsed_create_ms']}ms{rel} "
                          f"shutdown={cell['elapsed_shutdown_ms']}ms "
                          f"rss start->create="
                          f"{cell['rss_mb_after_runtime_start']}->"
                          f"{cell['rss_mb_after_create']}MB")

    cycle = _run_sub(["--cycle", "--actor-count", str(cycle_n),
                      "--hpx-threads", "1", "--calls", "1"])
    all_fails.extend(cycle["gate_failures"])
    print(f"  cycle N={cycle_n} ok={not cycle['gate_failures']} [obs] rss "
          f"before={cycle['rss_mb_before_cycle']} "
          f"w1create={cycle['rss_mb_after_first_create']} "
          f"w1release={cycle['rss_mb_after_first_release']} "
          f"w2create={cycle['rss_mb_after_second_create']} "
          f"w2release={cycle['rss_mb_after_second_release']} MB")

    if not all_fails:
        print("[exp26] structural gates passed (G1-G7): create/use/stats/"
              "release/shutdown/reinit clean at every cell; footprint and "
              "timing reported as observation only")
    else:
        print("[exp26] STRUCTURAL GATE FAILURES:")
        for f in all_fails:
            print(f"  - {f}")

    aggregate = {
        "experiment": "runtime_many_actors_footprint",
        "schema": "rayx-runtime-many-actors-footprint-1",
        "schema_note": "experiment-local; NOT the v1 benchmark JSONL schema",
        "boundary": "rayx-runtime-public-api",
        "goal": "many-actors lifecycle evidence over the public rayx.runtime "
                "API now that Runtime.release_actor exists: prove the "
                "create/use/release/shutdown/reinit contract holds at N "
                "actors, and OBSERVE (never gate) per-actor creation/release/"
                "teardown time and best-effort RSS footprint, including a "
                "create->release->create recycling cycle.",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "matrix": {
            "actor_counts": list(counts), "hpx_threads": list(hts),
            "calls_per_actor": list(calls_list), "release_paths": list(PATHS),
            "repeats": repeats, "num_op_lanes": 1,
            "cycle_cell": {"actor_count": cycle_n, "hpx_threads": 1,
                           "calls_per_actor": 1, "waves": 2},
            "note_512": "512 actors is opt-in via --actor-counts only; not "
                        "part of the default full run",
        },
        "firm_gates": {
            "G1": "exactly N actors; ids unique; rt-act-<16 hex> format",
            "G2": "calls=1: every add(1) returns 1; all futures completed",
            "G3": "every actor stats() idle after retire",
            "G4": "lane_stats() op-lanes-only with N actors live",
            "G5": "release path: release_actor None for every actor; "
                  "post-release call/stats raise on every handle",
            "G6": "shutdown clean on both paths; fresh Runtime + actor + "
                  "call works after teardown",
            "G7": "recycling cycle: both waves create/release cleanly with "
                  "valid wave-unique ids",
        },
        "note": "Structural gates only; ALL timing and ALL memory numbers are "
                "OBSERVATION-ONLY and machine/allocator-specific -- no "
                "performance verdict, no capacity or production-serving "
                "claim, no Ray comparison, no 'HPX beats Ray'. RSS is "
                "best-effort process-level ps output (null when unavailable) "
                "and ALLOCATOR-MEDIATED: HPX pools thread stacks/descriptors "
                "and malloc may retain freed memory, so a non-drop after "
                "release is NOT proof of a leak and a drop is NOT proof of "
                "guaranteed reclamation; macOS memory compression makes RSS "
                "an undercount with wobble. ru_maxrss is the peak high-water "
                "mark only (it never decreases). No runtime source, "
                "RuntimeLane, analyzer, JSONL, or CI change; no per-actor cap "
                "knob and no async_rw_mutex work -- those remain open design "
                "questions, not decisions.",
        "all_structural_gates_passed": not all_fails,
        "gate_failures": all_fails,
        "cells": cells,
        "recycling_cycle_observation": cycle,
    }

    if args.quick:
        print("[exp26] --quick: aggregate.json NOT written (smoke only)")
    else:
        agg_path = os.path.join(_HERE, "aggregate.json")
        with open(agg_path, "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp26] curated aggregate -> {agg_path}")

    return 1 if all_fails else 0


if __name__ == "__main__":
    sys.exit(main())
