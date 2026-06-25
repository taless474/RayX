#!/usr/bin/env python3
"""exp40 FULL MATRIX driver: independent-chain overlap inside one RayX runtime.

Two arms over a fixed-core (hpx_threads held constant per invocation) (K, q) grid, all
into ONE same-process runtime per (num_lanes) group:

  * Arm A -- RayX lane-level overlap (BASELINE / CONTROL, Ray-shaped mapping):
      K independent chain_sum_loop(SEED+j, S, q) submissions, gathered in Python.
      Includes K outer hpx::async hops AND K Python/pybind/RuntimeFuture retirements.
      Two gather variants reported: "as_completed" and bulk "wait(num_returns=K)".

  * Arm B -- HPX-native intra-op overlap (LOAD-BEARING HPX arm):
      one chain_fanout(SEED, K, S, q) op; the op fans out K children via bare
      hpx::async on the default HPX pool and joins with hpx::when_all. ONE outer op
      retirement + K internal HPX children; NO inter-child Python retirement.

Primary metric (fixed cores): overlap_ratio = K * T1 / TK, where T1 is the count==1
wall and TK the count==K wall, both p50 client wall. serial -> ~1; ideal -> ~min(K,
hpx_threads). Each cell measures BOTH T1 and TK in the SAME runtime, so the ratio is
within-runtime (no cross-lifecycle T1 assumption).

STRUCTURAL GATES (pass/fail; the driver ABORTS the run on any failure -- timing is
meaningless if the variants do not compute the same value):
  * determinism: T1/TK values stable across all reps;
  * Arm A: exactly K results, K distinct seeds (SEED+j);
  * equal-work: each arm's folded TK value == a Python reference fold of K independent
    chain_sum_loop chains -- so Arm A folded == Arm B folded == reference;
  * op table includes chain_fanout.

SCOPE / NON-CLAIMS (unchanged): single actor, single node, synthetic CPU work, closed
int64. NO Ray-mediated baseline (Arm A is RayX-internal lane routing, NOT Ray), NO
multi-node, NO Ray transport, NO HPX fabric / parcelport / AGAS, NO tokenizer / vLLM /
SGLang / Ray Serve / real inference, NO no-GIL, NO general "HPX beats Ray" claim.
Binding is HPX default; RayX passes only --hpx:threads=N; placement unknown -> NO
NUMA/socket attribution. Observation-only, machine-specific.

Run (laptop smoke -- structural gates only; laptop build may closed-form the kernel so
do NOT read laptop timing as evidence):
  PYTHONPATH=python/src python experiments/40_native_independent_overlap/run_independent_overlap.py --smoke

Full primary (Rostam, authoritative): see native_independent_overlap.md "How to run".
"""
import argparse
import gc
import json
import sys
import time

from rayx.runtime import Runtime
from rayx._rayx import runtime_op_table

SEED = 12345               # fixed so values are deterministic across reps/cells
BUSY_SUM_MASK = 0x7FFFFFFF  # mirror runtime_ops.hpp; the fold modulus


def _pct(xs, p):
    """Nearest-rank percentile of a non-empty list (no numpy dependency)."""
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def _reference_fold(rt, count, steps, quantum):
    """Deterministic reference: masked fold of `count` independent chain_sum_loop
    chains on SEED+j. The value both arms must reproduce (equal-work gate)."""
    acc = 0
    for j in range(count):
        v = rt.submit_operation("chain_sum_loop", SEED + j, steps, quantum).result().value
        acc = (acc + v) & BUSY_SUM_MASK
    return acc


def _run_armA(rt, count, steps, quantum, gather):
    """One Arm-A batch: `count` independent chains, gathered in Python. Returns
    (wall_ms, sum_service_ms, folded_value). Asserts exactly `count` distinct-seed
    futures are gathered (no missing/duplicate)."""
    t0 = time.perf_counter_ns()
    futs = [rt.submit_operation("chain_sum_loop", SEED + j, steps, quantum)
            for j in range(count)]
    svc = 0.0
    acc = 0
    seen = 0
    if gather == "as_completed":
        for fut in rt.as_completed(futs):
            res = fut.result()
            svc += res.row["service_ms_observed"]
            acc = (acc + res.value) & BUSY_SUM_MASK
            seen += 1
    else:  # bulk wait(num_returns=count) then collect in submission order
        ready, not_ready = rt.wait(futs, num_returns=len(futs))
        if not_ready:
            sys.exit(f"FAIL Arm A bulk wait left {len(not_ready)} not-ready (count={count})")
        for res in rt.get(futs):
            svc += res.row["service_ms_observed"]
            acc = (acc + res.value) & BUSY_SUM_MASK
            seen += 1
    t1 = time.perf_counter_ns()
    if seen != count:
        sys.exit(f"FAIL Arm A gathered {seen} != count {count} ({gather})")
    return (t1 - t0) / 1e6, svc, acc


def _run_armB(rt, count, steps, quantum):
    """One Arm-B chain_fanout op over `count` children. Returns (wall_ms, service_ms,
    folded_value)."""
    t0 = time.perf_counter_ns()
    res = rt.submit_operation("chain_fanout", SEED, count, steps, quantum).result()
    t1 = time.perf_counter_ns()
    return (t1 - t0) / 1e6, res.row["service_ms_observed"], res.value


def _measure(fn, reps, warmup, ctx):
    """Run fn() reps+warmup times (warmup discarded), GC off inside the measured loop.
    fn returns (wall_ms, service_ms, value). Aborts on a non-deterministic value.
    Returns (walls, svcs, value)."""
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
                sys.exit(f"FAIL non-deterministic value {ctx}: {v} != {val}")
    finally:
        if gc_was:
            gc.enable()
    return walls, svcs, val


def _armA_lanes(rule, K, hpx_threads):
    """num_lanes for an Arm-A cell. 'minKT' = min(K, hpx_threads) (lanes are not the
    cap until K exceeds workers); an integer string forces a fixed lane count (used for
    the serial-lane control, num_lanes=1)."""
    if rule == "minKT":
        return min(K, hpx_threads)
    return int(rule)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hpx-threads", type=int, default=8,
                    help="HPX worker threads (held constant for this invocation)")
    ap.add_argument("--ks", default="1,2,4,8,16", help="comma-separated chain counts K")
    ap.add_argument("--quanta", default="256,4096,16384,65536",
                    help="comma-separated per-stage work q")
    ap.add_argument("--steps", type=int, default=64)
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--arms", default="A,B", help="comma-separated subset of A,B")
    ap.add_argument("--armA-num-lanes", default="minKT",
                    help="'minKT' (=min(K,hpx_threads)) or an int (forced lane count, "
                         "e.g. 1 for the Arm A serial-lane control)")
    ap.add_argument("--label", default="primary",
                    help="row tag (e.g. primary / secondary / controlA_serial_lane)")
    ap.add_argument("--out", default=None, help="JSONL output path (append)")
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop sweep (structural gates only; overrides grid)")
    args = ap.parse_args()

    if args.smoke:
        ks = [1, 2, 4]
        quanta = [256, 4096]
        reps, warmup = 8, 3
    else:
        ks = [int(x) for x in args.ks.split(",")]
        quanta = [int(x) for x in args.quanta.split(",")]
        reps, warmup = args.reps, args.warmup
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    S = args.steps
    T = args.hpx_threads

    # op-table structural gate (before any runtime is built).
    if "chain_fanout" not in dict(runtime_op_table()):
        sys.exit("FAIL op table missing chain_fanout")

    env = {
        "experiment": "exp40", "label": args.label, "hpx_threads": T,
        "steps": S, "ks": ks, "quanta": quanta, "reps": reps, "warmup": warmup,
        "seed": SEED, "armA_num_lanes_rule": args.armA_num_lanes,
        "hpx_bind": "RayX sets only --hpx:threads; binding is HPX default; "
                    "placement unknown",
        "metric": "fixed-core overlap_ratio = K * T1 / TK (p50 client wall); "
                  "observation-only, machine-specific; no NUMA/socket claim",
        "scope": "single actor/node, synthetic CPU int64; Arm A = RayX lane baseline "
                 "(K outer async hops + K Python retirements); Arm B = HPX-native "
                 "intra-op (1 outer retirement + K internal HPX children)",
    }
    print("exp40 env:", json.dumps(env))

    # Build the cell list, then group by num_lanes so one Runtime serves all cells that
    # share a lane count (minimizes hpx::start/stop cycles). Each cell: (arm, K, q,
    # gather, num_lanes). Arm A runs both gather variants; Arm B has gather=None.
    cells = []
    for arm in arms:
        for K in ks:
            nl = _armA_lanes(args.armA_num_lanes, K, T) if arm == "A" else 1
            gathers = ("as_completed", "bulk") if arm == "A" else (None,)
            for q in quanta:
                for g in gathers:
                    cells.append((arm, K, q, g, nl))
    lane_groups = sorted({c[4] for c in cells})

    out = open(args.out, "a") if args.out else None
    n_pass = 0
    for nl in lane_groups:
        with Runtime(num_lanes=nl, hpx_threads=T) as rt:
            for (arm, K, q, gather, cell_nl) in cells:
                if cell_nl != nl:
                    continue
                # Equal-work reference (structural gate), computed once per cell.
                ref = _reference_fold(rt, K, S, q)
                if arm == "A":
                    t1_fn = lambda K=K, q=q, gather=gather: _run_armA(rt, 1, S, q, gather)
                    tk_fn = lambda K=K, q=q, gather=gather: _run_armA(rt, K, S, q, gather)
                    label = f"A/{gather}"
                else:
                    t1_fn = lambda K=K, q=q: _run_armB(rt, 1, S, q)
                    tk_fn = lambda K=K, q=q: _run_armB(rt, K, S, q)
                    label = "B"
                ctx = f"{label} K={K} q={q} lanes={nl} T={T}"
                t1_walls, _t1s, t1_val = _measure(t1_fn, reps, warmup, "T1 " + ctx)
                tk_walls, tk_svcs, tk_val = _measure(tk_fn, reps, warmup, "TK " + ctx)
                # Equal-work gates: both the count==1 and count==K folded values must
                # match the independent Python reference fold.
                ref1 = _reference_fold(rt, 1, S, q)
                if t1_val != ref1:
                    sys.exit(f"FAIL equal-work T1 {ctx}: {t1_val} != ref {ref1}")
                if tk_val != ref:
                    sys.exit(f"FAIL equal-work TK {ctx}: {tk_val} != ref {ref}")

                t1 = _pct(t1_walls, 50)
                tk = _pct(tk_walls, 50)
                cap = min(K, T)  # ideal overlap cap (workers; Arm A also bounded by nl)
                row = {
                    **{k: env[k] for k in ("experiment", "label", "hpx_threads")},
                    "arm": arm, "variant": label, "gather": gather,
                    "K": K, "quantum": q, "steps": S, "num_lanes": nl,
                    "T1_p50_ms": round(t1, 4),
                    "TK_p50_ms": round(tk, 4),
                    "TK_p90_ms": round(_pct(tk_walls, 90), 4),
                    "TK_p99_ms": round(_pct(tk_walls, 99), 4),
                    "overlap_ratio": round(K * t1 / tk, 4) if tk > 0 else None,
                    "normalized_wall_TK_over_T1": round(tk / t1, 4) if t1 > 0 else None,
                    "throughput_chains_per_s": round(K / (tk / 1e3), 1) if tk > 0 else None,
                    "efficiency_vs_min_K_T": round((K * t1 / tk) / cap, 4)
                    if tk > 0 and cap > 0 else None,
                    "TK_sum_service_p50_ms": round(_pct(tk_svcs, 50), 4),
                    "ideal_cap_min_K_T": cap,
                    "TK_value": tk_val, "ref_value": ref,
                }
                n_pass += 1
                if out:
                    out.write(json.dumps(row) + "\n")
                print(f"  [{label:>14}] K={K:>2} q={q:>6} lanes={nl} T={T}  "
                      f"T1={t1:8.4f} TK={tk:8.4f}  overlap={row['overlap_ratio']:6}  "
                      f"eff={row['efficiency_vs_min_K_T']:6}  "
                      f"normW={row['normalized_wall_TK_over_T1']:6}  "
                      f"thr={row['throughput_chains_per_s']}/s  (ideal<= {cap})")

    if out:
        out.close()
    print(f"\nSTRUCTURAL GATES: PASS ({n_pass} cells; values deterministic across reps; "
          f"Arm A K distinct seeds + exactly K futures; each arm's fold == reference)")


if __name__ == "__main__":
    main()
