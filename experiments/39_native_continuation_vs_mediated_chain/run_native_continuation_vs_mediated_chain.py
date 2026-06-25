#!/usr/bin/env python3
"""exp39 driver: native loop vs hpx::future::then chain vs mediated boundary.

Latency / DECOMPOSITION slice (single actor, single node, synthetic int64). Three
variants over a (steps S, quantum q) sweep, all into ONE same-process runtime:

  * chain_sum_loop    -- one submitted op, plain C++ loop over S stages on the lane
                         worker (in-process native FLOOR; NO per-stage continuation).
  * chain_sum_then    -- one submitted op, S stages as an hpx::future::then chain,
                         each stage scheduled via hpx::launch::async. A deliberately
                         VISIBLE / pessimistic measurement of scheduled
                         hpx::future::then shared-state + continuation cost -- NOT the
                         theoretical lower bound of HPX composition (launch::sync,
                         sender/receiver, or the plain loop are different points).
  * mediated_samelane -- Python left-fold of S calls to chain_sum_loop(_, 1, q) into
                         the SAME runtime, feeding each result forward (repeated
                         Python / pybind / RuntimeFuture / lane-admission boundary).

Headline metric: CLIENT-SIDE per-chain wall time (time.perf_counter_ns) around the
full chain, for all three variants -- the per-step boundary cost lives OUTSIDE the
native JSONL row by construction, so the row alone would measure the wrong thing. The
runtime rows are kept (loop / then: one row per chain; mediated: summed service) for
decomposition only.

Decomposition (reported at p50 client wall, NOT a winner):
  mediated - loop  ~ repeated boundary overhead
  then - loop      ~ scheduled hpx::future::then continuation / shared-state overhead
  mediated - then  ~ net benefit of staying native while still paying continuation cost

Honest framing: a linear DEPENDENT chain has NO parallelism, so this is NOT an "HPX
scheduling wins" result -- the concurrency / overlap story is the separate exp40.

Large-q sanity: as q grows, relative differences should SHRINK because stage work
dominates the fixed boundary / continuation overheads. If the absolute or relative
gap grows, or the ordering becomes unstable, the result is measuring additional
effects (scheduler contention, allocator pressure, affinity, cache behavior) -- the
writeup must say so rather than treat it as failure.

Run (laptop smoke):
  PYTHONPATH=python/src python experiments/39_.../run_native_continuation_vs_mediated_chain.py --smoke
Run (full, e.g. Rostam):
  PYTHONPATH=python/src .venv/bin/python experiments/39_.../run_native_continuation_vs_mediated_chain.py \
      --steps 1,2,4,8,16,32,64 --quanta 0,16,64,256,1024,4096,16384,65536 \
      --reps 200 --warmup 30 --out results/exp39.jsonl
"""
import argparse
import gc
import json
import statistics
import sys
import time

from rayx.runtime import Runtime

VARIANTS = ("chain_sum_loop", "chain_sum_then", "mediated_samelane")
SEED = 12345  # fixed so the equal-work gate is deterministic across variants/reps


def _pct(xs, p):
    """Nearest-rank percentile of a non-empty list (no numpy dependency)."""
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def _run_loop(rt, S, q):
    """One chain_sum_loop chain. Returns (wall_ns, value, service_ms)."""
    t0 = time.perf_counter_ns()
    res = rt.submit_operation("chain_sum_loop", SEED, S, q).result()
    t1 = time.perf_counter_ns()
    return t1 - t0, res.value, res.row["service_ms_observed"]


def _run_then(rt, S, q):
    """One chain_sum_then chain. Returns (wall_ns, value, service_ms)."""
    t0 = time.perf_counter_ns()
    res = rt.submit_operation("chain_sum_then", SEED, S, q).result()
    t1 = time.perf_counter_ns()
    return t1 - t0, res.value, res.row["service_ms_observed"]


def _run_mediated(rt, S, q):
    """One mediated_samelane chain: S Python-driven one-stage submits, folded.

    Returns (wall_ns, value, summed_service_ms). The summed native service is the
    decomposition complement of the client wall: (wall - summed_service) is the
    repeated Python / pybind / RuntimeFuture / lane-admission boundary cost.
    """
    t0 = time.perf_counter_ns()
    x = SEED
    svc = 0.0
    for _ in range(S):
        res = rt.submit_operation("chain_sum_loop", x, 1, q).result()
        x = res.value
        svc += res.row["service_ms_observed"]
    t1 = time.perf_counter_ns()
    return t1 - t0, x, svc


_DISPATCH = {
    "chain_sum_loop": _run_loop,
    "chain_sum_then": _run_then,
    "mediated_samelane": _run_mediated,
}


def _equal_work_gate(rt, S, q):
    """Three-way equal-work invariant for this (S, q). Aborts the run on mismatch --
    the decomposition is meaningless if the variants do not compute the same value."""
    vals = {v: _DISPATCH[v](rt, S, q)[1] for v in VARIANTS}
    if len(set(vals.values())) != 1:
        sys.exit(f"FAIL equal-work invariant at S={S} q={q}: {vals}")
    return next(iter(vals.values()))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--steps", default="1,2,4,8,16,32",
                    help="comma-separated chain lengths S")
    ap.add_argument("--quanta", default="0,16,64,256,1024,4096,16384",
                    help="comma-separated per-stage work q")
    ap.add_argument("--reps", type=int, default=120, help="measured reps per cell")
    ap.add_argument("--warmup", type=int, default=20, help="discarded warmup reps")
    ap.add_argument("--hpx-threads", type=int, default=1)
    ap.add_argument("--num-lanes", type=int, default=1)
    ap.add_argument("--out", default=None, help="JSONL output path (rows); optional")
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop sweep (overrides steps/quanta/reps/warmup)")
    args = ap.parse_args()

    if args.smoke:
        steps = [1, 4, 16]
        quanta = [0, 64, 4096]
        reps, warmup = 15, 5
    else:
        steps = [int(x) for x in args.steps.split(",")]
        quanta = [int(x) for x in args.quanta.split(",")]
        reps, warmup = args.reps, args.warmup

    out = open(args.out, "w") if args.out else None
    summary = []  # one dict per (S, q) cell

    # Environment provenance: RayX exposes hpx_threads + num_lanes; thread
    # binding/affinity is the HPX default (RayX does not pass --hpx:bind), reported
    # here so the run is reproducible.
    env = {"hpx_threads": args.hpx_threads, "num_lanes": args.num_lanes,
           "hpx_bind": "HPX default (RayX sets only --hpx:threads)",
           "reps": reps, "warmup": warmup, "seed": SEED,
           "launch_policy": "hpx::launch::async (chain_sum_then; deliberately "
                            "visible scheduled-continuation cost, not HPX floor)"}
    print("exp39 env:", json.dumps(env))

    with Runtime(num_lanes=args.num_lanes, hpx_threads=args.hpx_threads) as rt:
        for S in steps:
            for q in quanta:
                expected = _equal_work_gate(rt, S, q)  # gate before timing
                walls = {v: [] for v in VARIANTS}
                svcs = {v: [] for v in VARIANTS}
                # Warmup (discarded): pay HPX pool / first-touch one-time costs.
                for v in VARIANTS:
                    for _ in range(warmup):
                        _DISPATCH[v](rt, S, q)
                # Measured: GC off inside the timing loop so a collection cannot
                # land in a client wall sample; re-enabled immediately after.
                gc_was = gc.isenabled()
                gc.disable()
                try:
                    for v in VARIANTS:
                        fn = _DISPATCH[v]
                        for r in range(reps):
                            wall_ns, val, svc_ms = fn(rt, S, q)
                            walls[v].append(wall_ns / 1e6)  # -> ms
                            svcs[v].append(svc_ms)
                            if val != expected:
                                gc.enable()
                                sys.exit(f"FAIL non-deterministic value v={v} "
                                         f"S={S} q={q}: {val} != {expected}")
                            if out:
                                out.write(json.dumps({
                                    "variant": v, "steps": S, "quantum": q, "rep": r,
                                    "wall_ms": wall_ns / 1e6,
                                    "service_ms_observed": svc_ms,
                                    "value": val,
                                }) + "\n")
                finally:
                    if gc_was:
                        gc.enable()
                cell = {"steps": S, "quantum": q, "value": expected}
                for v in VARIANTS:
                    cell[v] = {"p50": _pct(walls[v], 50),
                               "p90": _pct(walls[v], 90),
                               "p99": _pct(walls[v], 99),
                               "svc_p50": _pct(svcs[v], 50)}
                lo, th, md = (cell["chain_sum_loop"]["p50"],
                              cell["chain_sum_then"]["p50"],
                              cell["mediated_samelane"]["p50"])
                cell["decomp_p50_ms"] = {
                    "mediated_minus_loop": md - lo,   # repeated boundary overhead
                    "then_minus_loop": th - lo,       # scheduled future::then overhead
                    "mediated_minus_then": md - th,   # net stay-native benefit
                }
                summary.append(cell)
                print(f"S={S:>4} q={q:>6}  loop={lo:8.4f}  then={th:8.4f}  "
                      f"med={md:8.4f}  | med-loop={md-lo:8.4f}  "
                      f"then-loop={th-lo:8.4f}  med-then={md-th:8.4f}  (p50 ms)")

    if out:
        out.close()

    # Large-q convergence read (soft, not pass/fail): relative spread of the three
    # p50s at the largest q for each S. Shrinking spread as q grows is expected; a
    # growing/unstable spread means "measuring more than boundary/continuation cost".
    print("\nlarge-q convergence read (relative p50 spread = (max-min)/min):")
    qmax = max(quanta)
    for S in steps:
        cells = [c for c in summary if c["steps"] == S and c["quantum"] == qmax]
        if not cells:
            continue
        ps = [cells[0][v]["p50"] for v in VARIANTS]
        lo = min(ps)
        rel = (max(ps) - lo) / lo if lo > 0 else float("nan")
        print(f"  S={S:>4} q={qmax}: p50s={[round(p,4) for p in ps]}  "
              f"rel_spread={rel:.3f}")

    print("\nSTRUCTURAL GATES: PASS (three-way equal-work held at every cell; "
          "values deterministic across reps)")


if __name__ == "__main__":
    main()
