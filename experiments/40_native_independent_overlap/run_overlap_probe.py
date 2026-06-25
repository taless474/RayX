#!/usr/bin/env python3
"""exp40 DIAGNOSTIC PROBE (not the full matrix): does independent native work overlap
inside one RayX runtime?

This is the small probe that gates whether the full exp40 matrix is worth running. It
measures ONE (arm, config) per process invocation so each HPX lifecycle is clean (no
repeated hpx::start/stop), and reports a fixed-CORE overlap ratio -- NOT a thread-count
speedup -- exactly as approved:

  * Arm A -- RayX lane-level overlap (BASELINE / control, Ray-shaped mapping):
      K independent chain_sum_loop(seed+j, S, q) submissions, gathered in Python.
      Run twice across num_lanes (e.g. 1 vs 8) at fixed hpx_threads to confirm lane-level
      overlap appears only when lanes > 1. Arm A pays K outer hpx::async hops AND K
      Python/pybind/RuntimeFuture retirements; both gather paths (as_completed, bulk
      wait(num_returns=K)) are timed so the retirement cost is visible.

  * Arm B -- HPX-native intra-op overlap (LOAD-BEARING HPX arm):
      one chain_fanout(seed, K, S, q) submission; the op fans out K children via bare
      hpx::async on the default HPX pool and joins with when_all. Run across hpx_threads
      (e.g. 1 vs 8) at num_lanes=1 to confirm the children overlap on the worker pool
      from a SINGLE submitted op. Arm B pays ONE outer op retirement + K internal HPX
      children, and NO inter-child Python retirement.

Primary metric (fixed cores): overlap_ratio = K * T1 / TK, where T1 is the one-chain
(count==1) wall and TK the K-chain wall, both p50. serial -> ~1; ideal overlap -> ~min(K,
available_workers). Also normalized_wall = TK / T1 and throughput = K / TK.

Scope / non-claims (unchanged): single actor, single node, synthetic CPU work, closed
int64; NO Python callback, NO Ray-mediated baseline, NO HPX fabric / parcelport / AGAS,
NO tokenizer / vLLM / SGLang / Ray Serve, NO no-GIL claim, NO general "HPX beats Ray"
claim. RayX sets only --hpx:threads; binding is HPX default; placement unknown -- so NO
NUMA/socket claims. Numbers are observation-only and machine-specific. p99 is omitted
(reps are small; p99 would be ~max).

Probe (laptop / Rostam), one invocation per (arm, config):
  PYTHONPATH=python/src python experiments/40_native_independent_overlap/run_overlap_probe.py \
      --arm A --hpx-threads 8 --num-lanes 1 --count 8 --steps 64 --quantum 16384
"""
import argparse
import gc
import json
import sys
import time

from rayx.runtime import Runtime

SEED = 12345  # fixed so the run is deterministic


def _pct(xs, p):
    """Nearest-rank percentile of a non-empty list (no numpy dependency)."""
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


# ---- Arm A: RayX lane-level (K independent chain_sum_loop submissions) -------


def _run_armA_chain(rt, count, steps, quantum, gather):
    """One Arm-A batch of `count` independent chains. Returns (wall_ms, sum_service_ms,
    fold_value). `gather` is "as_completed" or "bulk". The fold value lets the caller
    cross-check Arm A against Arm B / the reference (masked fold of the K endpoints)."""
    mask = 0x7FFFFFFF
    t0 = time.perf_counter_ns()
    futs = [rt.submit_operation("chain_sum_loop", SEED + j, steps, quantum)
            for j in range(count)]
    svc = 0.0
    acc = 0
    if gather == "as_completed":
        for fut in rt.as_completed(futs):
            res = fut.result()
            svc += res.row["service_ms_observed"]
            acc = (acc + res.value) & mask
    else:  # bulk wait(num_returns=K) then collect in submission order
        ready, not_ready = rt.wait(futs, num_returns=len(futs))
        assert not not_ready
        for res in rt.get(futs):
            svc += res.row["service_ms_observed"]
            acc = (acc + res.value) & mask
    t1 = time.perf_counter_ns()
    return (t1 - t0) / 1e6, svc, acc


# ---- Arm B: HPX-native intra-op (one chain_fanout submission) ----------------


def _run_armB_fanout(rt, count, steps, quantum):
    """One Arm-B chain_fanout op over `count` children. Returns (wall_ms, service_ms,
    fold_value)."""
    t0 = time.perf_counter_ns()
    res = rt.submit_operation("chain_fanout", SEED, count, steps, quantum).result()
    t1 = time.perf_counter_ns()
    return (t1 - t0) / 1e6, res.row["service_ms_observed"], res.value


def _measure(fn, reps, warmup):
    """Run fn() reps+warmup times (warmup discarded), GC off inside the measured loop.
    fn returns (wall_ms, service_ms, value). Returns (walls, svcs, value)."""
    for _ in range(warmup):
        fn()
    walls, svcs, val = [], [], None
    gc_was = gc.isenabled()
    gc.disable()
    try:
        for _ in range(reps):
            w, s, v = fn()
            walls.append(w)
            svcs.append(s)
            if val is None:
                val = v
            elif v != val:
                gc.enable()
                sys.exit(f"FAIL non-deterministic value: {v} != {val}")
    finally:
        if gc_was:
            gc.enable()
    return walls, svcs, val


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--arm", choices=("A", "B"), required=True)
    ap.add_argument("--hpx-threads", type=int, default=8)
    ap.add_argument("--num-lanes", type=int, default=1)
    ap.add_argument("--count", type=int, default=8, help="K independent chains")
    ap.add_argument("--steps", type=int, default=64)
    ap.add_argument("--quantum", type=int, default=16384)
    ap.add_argument("--reps", type=int, default=50)
    ap.add_argument("--warmup", type=int, default=15)
    ap.add_argument("--out", default=None, help="optional JSONL path (one row appended)")
    args = ap.parse_args()

    K = args.count
    # available_workers caps ideal overlap: Arm A is bounded by min(lanes, threads);
    # Arm B places children across the worker pool (num_lanes irrelevant), bounded by
    # hpx_threads. Reported for context only -- NOT a capacity claim.
    workers = (min(args.num_lanes, args.hpx_threads) if args.arm == "A"
               else args.hpx_threads)

    env = {
        "experiment": "exp40_probe",
        "arm": args.arm,
        "hpx_threads": args.hpx_threads,
        "num_lanes": args.num_lanes,
        "count_K": K,
        "steps": args.steps,
        "quantum": args.quantum,
        "reps": args.reps,
        "warmup": args.warmup,
        "seed": SEED,
        "hpx_bind": "RayX sets only --hpx:threads; binding is HPX default; "
                    "placement unknown",
        "available_workers_for_overlap": workers,
        "metric": "fixed-core overlap_ratio = K * T1 / TK (p50 client wall); "
                  "observation-only, machine-specific; no NUMA/socket claim",
    }
    print("exp40 probe env:", json.dumps(env))

    rows = []
    with Runtime(num_lanes=args.num_lanes, hpx_threads=args.hpx_threads) as rt:
        if args.arm == "A":
            gathers = ("as_completed", "bulk")
        else:
            gathers = (None,)
        for gather in gathers:
            if args.arm == "A":
                t1_walls, t1_svcs, t1_val = _measure(
                    lambda: _run_armA_chain(rt, 1, args.steps, args.quantum, gather),
                    args.reps, args.warmup)
                tk_walls, tk_svcs, tk_val = _measure(
                    lambda: _run_armA_chain(rt, K, args.steps, args.quantum, gather),
                    args.reps, args.warmup)
                label = f"A/{gather}"
            else:
                t1_walls, t1_svcs, t1_val = _measure(
                    lambda: _run_armB_fanout(rt, 1, args.steps, args.quantum),
                    args.reps, args.warmup)
                tk_walls, tk_svcs, tk_val = _measure(
                    lambda: _run_armB_fanout(rt, K, args.steps, args.quantum),
                    args.reps, args.warmup)
                label = "B"

            t1 = _pct(t1_walls, 50)
            tk = _pct(tk_walls, 50)
            row = {
                "label": label,
                "arm": args.arm,
                "gather": gather,
                "hpx_threads": args.hpx_threads,
                "num_lanes": args.num_lanes,
                "count_K": K,
                "T1_p50_ms": round(t1, 4),
                "T1_p90_ms": round(_pct(t1_walls, 90), 4),
                "TK_p50_ms": round(tk, 4),
                "TK_p90_ms": round(_pct(tk_walls, 90), 4),
                "overlap_ratio": round(K * t1 / tk, 3) if tk > 0 else None,
                "normalized_wall_TK_over_T1": round(tk / t1, 3) if t1 > 0 else None,
                "throughput_chains_per_s": round(K / (tk / 1e3), 1) if tk > 0 else None,
                "TK_sum_service_p50_ms": round(_pct(tk_svcs, 50), 4),
                "T1_value": t1_val,
                "TK_value": tk_val,
            }
            rows.append(row)
            print(f"  [{label:>14}] T1={row['T1_p50_ms']:8.4f}  "
                  f"TK={row['TK_p50_ms']:8.4f}  overlap={row['overlap_ratio']}  "
                  f"norm_wall={row['normalized_wall_TK_over_T1']}  "
                  f"thr={row['throughput_chains_per_s']}/s  "
                  f"TK_svc_sum={row['TK_sum_service_p50_ms']:8.4f}  (ideal<= {workers})")

    # Arm A: surface the as_completed-vs-bulk retirement difference explicitly.
    if args.arm == "A" and len(rows) == 2:
        ac = next(r for r in rows if r["gather"] == "as_completed")
        bk = next(r for r in rows if r["gather"] == "bulk")
        print(f"  Arm A retirement read: TK as_completed={ac['TK_p50_ms']:.4f} ms vs "
              f"bulk wait={bk['TK_p50_ms']:.4f} ms  "
              f"(delta={ac['TK_p50_ms'] - bk['TK_p50_ms']:+.4f} ms)")

    if args.out:
        with open(args.out, "a") as f:
            for row in rows:
                f.write(json.dumps({**env, **row}) + "\n")
        print(f"  appended {len(rows)} row(s) to {args.out}")

    print("STRUCTURAL: values deterministic across reps "
          "(equal-work fold gate covered by tests/integration/test_runtime_chain_fanout.py)")


if __name__ == "__main__":
    main()
