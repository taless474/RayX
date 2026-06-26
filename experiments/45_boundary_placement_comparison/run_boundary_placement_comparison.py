#!/usr/bin/env python3
"""exp45: fixed-granularity boundary-placement comparison for the SAME closed
fan-in/reduction workload, across two boundary placements. STRUCTURAL / COUNT evidence
only -- NOT a performance result.

The workload is a fan-in of `L` independent leaves, each a single `chain_stage`, folded
under BUSY_SUM_MASK. It is computed two ways that must return the BIT-IDENTICAL closed
int64:

  Path A -- native / coarse (one Python/Runtime boundary crossing pair):
      rt.submit_operation("barrier_fanin", seed, L, quantum).result().value
    One submission, one retirement. The L leaves, the cooperative gate, when_all, and the
    scheduled .then masked fold all run NATIVELY, below the boundary. The barrier_fanin
    witness is checked for STRUCTURAL VALIDITY only (it is not the subject here).

  Path B -- Python-mediated (many crossings), existing fixed ops only:
      for j in range(L): leaf_j = rt.submit_operation("chain_sum_loop", seed+j, 1, quantum)
                                    .result().value          # one submit + one retire each
      result_b = fold leaf values under BUSY_SUM_MASK in Python
    `chain_sum_loop(seed+j, 1, quantum)` applies chain_stage ONCE -> exactly a barrier_fanin
    leaf. The Python masked fold crosses NO Runtime boundary (reported as python_fold_ops,
    not as a crossing). No new op, no arbitrary Python execution, no object store.

Oracle (pure Python, triangulation): mirrors native masking EXACTLY (per-step mask in
`masked_range_sum`, uint64 wrap in `chain_stage`), then folds. Gate:
    result_a == result_b == oracle   for every L.

What is counted (deterministic, machine-independent -- the LOAD-BEARING result):
    Path A: runtime_submissions=1, runtime_retirements=1, boundary_crossings=2  (O(1))
    Path B: runtime_submissions=L, runtime_retirements=L, boundary_crossings=2L (O(L)),
            python_fold_ops=L-1
Counts are OBSERVED (incremented at each submit/retire) and then gated against the exact
formulas, not asserted by fiat.

Timing is OPT-IN (`--timing`), default OFF, observation-only and TAUTOLOGICAL: fewer pybind
transits cost less pybind time. It is NOT a speedup / throughput / latency / HPX / Ray /
scheduling result and must not be cited as one.

NON-CLAIMS: no speedup; no throughput; no latency/performance claim (timing is opt-in,
observation-only, tautological); no HPX faster than Ray; no Ray comparison; no real
inference; no endpoint/fabric claim; no parcelport; no AGAS; no multi-node; no ObjectRef /
object-store semantics; no arbitrary Python execution; no parallelism/scheduling claim (the
gate is incidental here); and NO claim that Python orchestration is "bad" (it is a
legitimate, flexible pattern -- exp45 only COUNTS its Runtime-boundary transits for this
workload).

Run (laptop -- generates aggregate.json beside this file):
  PYTHONPATH=python/src python \
    experiments/45_boundary_placement_comparison/run_boundary_placement_comparison.py --smoke
"""
import argparse
import gc
import json
import os
import platform
import sys
import time

MASK = 0x7FFFFFFF      # mirror BUSY_SUM_MASK in python/src/rayx/runtime_ops.hpp
U64 = (1 << 64) - 1
SEED = 3               # stable default (any int64; small non-negative keeps it clean)
QUANTUM = 64           # stable default per-leaf on-core work (matches exp44)


# --- pure-Python oracle (mirrors native masking EXACTLY) ---------------------

def masked_range_sum_py(begin, end):
    """Mirror of masked_range_sum(begin, end) in runtime_ops.hpp: per-step mask (NOT a
    final mask -- they differ once the triangular number exceeds 2^31)."""
    acc = 0
    for i in range(begin, end):
        acc = (acc + (i & U64)) & MASK
    return acc


def chain_stage_py(x, q):
    """Mirror of chain_stage(x, q) in runtime_ops.hpp: (uint64(x) + masked_range_sum(0,q))
    & MASK."""
    return ((x & U64) + masked_range_sum_py(0, q)) & MASK


def oracle(seed, leaves, quantum):
    """The same fan-in/reduction as barrier_fanin(seed, leaves, quantum), pure Python:
    fold chain_stage(seed+j, quantum) over j under BUSY_SUM_MASK."""
    acc = 0
    for j in range(leaves):
        acc = (acc + chain_stage_py(seed + j, quantum)) & MASK
    return acc


# --- boundary-crossing counter ----------------------------------------------

class Counter:
    """Counts Python/Runtime boundary transits: one per submit_operation (Python->native)
    and one per RuntimeFuture retirement (.result(), native->Python)."""
    def __init__(self):
        self.submissions = 0
        self.retirements = 0

    @property
    def boundary_crossings(self):
        return self.submissions + self.retirements


def submit_and_get(rt, counter, op_id, *args):
    """One counted submit + retire round trip."""
    counter.submissions += 1
    fut = rt.submit_operation(op_id, *args)
    val = fut.result().value
    counter.retirements += 1
    return val


# --- the two paths -----------------------------------------------------------

def path_a(rt, seed, leaves, quantum):
    """Native/coarse: one barrier_fanin op. Returns (result, counter, witness_ok)."""
    c = Counter()
    result = submit_and_get(rt, c, "barrier_fanin", seed, leaves, quantum)
    w = rt.barrier_fanin_witness()  # single-in-flight snapshot of the call above
    witness_ok = bool(
        w["leaves_requested"] == leaves
        and w["arrived_count"] == leaves
        and w["released_count"] == leaves
        and w["opener"] == "last_arriver"
        and w["watchdog_opened"] is False
        and w["ordering_violations"] == 0
        and w["reduction_after_all_leaves"] is True
        and w["clean_exit"] is True)
    return result, c, witness_ok


def path_b(rt, seed, leaves, quantum):
    """Python-mediated: L chain_sum_loop leaves + a Python masked fold. Returns
    (result, counter, python_fold_ops)."""
    c = Counter()
    leaf_vals = [submit_and_get(rt, c, "chain_sum_loop", seed + j, 1, quantum)
                 for j in range(leaves)]
    # Fold L values with L-1 binary masked adds (the Python fold crosses NO Runtime
    # boundary; it is reported as python_fold_ops, not a boundary crossing).
    acc = leaf_vals[0]
    fold_ops = 0
    for v in leaf_vals[1:]:
        acc = (acc + v) & MASK
        fold_ops += 1
    return (acc & MASK), c, fold_ops


# --- optional timing (opt-in, observation-only, TAUTOLOGICAL) ----------------

def _pct(xs, p):
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def _summ(samples):
    med, p25, p75 = _pct(samples, 50), _pct(samples, 25), _pct(samples, 75)
    return {"median_ns": int(med), "p25_ns": int(p25), "p75_ns": int(p75),
            "iqr_ns": int(p75 - p25)}


def _empty_loop_floor_ns(reps):
    samples = []
    was = gc.isenabled(); gc.disable()
    try:
        for _ in range(reps):
            t0 = time.perf_counter_ns(); t1 = time.perf_counter_ns()
            samples.append(t1 - t0)
    finally:
        if was:
            gc.enable()
    return int(_pct(samples, 50))


def _measure(call, reps, warmup):
    for _ in range(warmup):
        call()
    samples = []
    was = gc.isenabled(); gc.disable()
    try:
        for _ in range(reps):
            t0 = time.perf_counter_ns()
            call()
            t1 = time.perf_counter_ns()
            samples.append(t1 - t0)
    finally:
        if was:
            gc.enable()
    return samples


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--leaves", default="1,2,4,8,16,32",
                    help="comma-separated L sweep (default 1,2,4,8,16,32)")
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--quantum", type=int, default=QUANTUM)
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--timing", action="store_true",
                    help="opt-in observation-only timing (TAUTOLOGICAL; not a perf claim)")
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--out", default=None)
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop sweep L in {1,2,4} (structural only)")
    args = ap.parse_args()

    if args.smoke:
        leaves_sweep = [1, 2, 4]
        reps, warmup = 30, 8
    else:
        leaves_sweep = [int(x) for x in args.leaves.split(",")]
        reps, warmup = args.reps, args.warmup

    here = os.path.dirname(os.path.abspath(__file__))
    out_path = args.out or os.path.join(here, "aggregate.json")
    src = os.path.join(os.path.dirname(os.path.dirname(here)), "python", "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    from rayx.runtime import Runtime

    seed, quantum = args.seed, args.quantum
    rows = []
    correctness_fail = []
    count_fail = []
    witness_fail = []

    empty_floor = _empty_loop_floor_ns(max(reps, 50)) if args.timing else None

    with Runtime(hpx_threads=args.hpx_threads) as rt:
        for L in leaves_sweep:
            ref = oracle(seed, L, quantum)
            ra, ca, witness_ok = path_a(rt, seed, L, quantum)
            rb, cb, fold_ops = path_b(rt, seed, L, quantum)

            equal_ok = (ra == ref and rb == ref)
            if not equal_ok:
                correctness_fail.append(
                    f"L={L}: result_a={ra} result_b={rb} oracle={ref}")
            # Counts are OBSERVED above; gate them against the exact formulas.
            a_counts_ok = (ca.submissions == 1 and ca.retirements == 1
                           and ca.boundary_crossings == 2)
            b_counts_ok = (cb.submissions == L and cb.retirements == L
                           and cb.boundary_crossings == 2 * L and fold_ops == L - 1)
            if not a_counts_ok:
                count_fail.append(
                    f"L={L} Path A counts {ca.submissions}/{ca.retirements}/"
                    f"{ca.boundary_crossings} != 1/1/2")
            if not b_counts_ok:
                count_fail.append(
                    f"L={L} Path B counts {cb.submissions}/{cb.retirements}/"
                    f"{cb.boundary_crossings} (fold {fold_ops}) != {L}/{L}/{2 * L} "
                    f"(fold {L - 1})")
            if not witness_ok:
                witness_fail.append(f"L={L}: barrier_fanin witness not structurally valid")

            row = {
                "leaves": L, "seed": seed, "quantum": quantum,
                "result_a": ra, "result_b": rb, "oracle": ref,
                "result_equal_ok": bool(equal_ok),
                "path_a": {
                    "runtime_submissions": ca.submissions,
                    "runtime_retirements": ca.retirements,
                    "boundary_crossings": ca.boundary_crossings,
                    "witness_structural_ok": bool(witness_ok),
                },
                "path_b": {
                    "runtime_submissions": cb.submissions,
                    "runtime_retirements": cb.retirements,
                    "boundary_crossings": cb.boundary_crossings,
                    "python_fold_ops": fold_ops,
                },
                "a_counts_match_formula": bool(a_counts_ok),    # expect 1/1/2
                "b_counts_match_formula": bool(b_counts_ok),    # expect L/L/2L, fold L-1
            }

            if args.timing:
                sa = _measure(
                    lambda L=L: rt.submit_operation(
                        "barrier_fanin", seed, L, quantum).result().value, reps, warmup)

                def _run_b(L=L):
                    vals = [rt.submit_operation("chain_sum_loop", seed + j, 1, quantum)
                            .result().value for j in range(L)]
                    acc = vals[0]
                    for v in vals[1:]:
                        acc = (acc + v) & MASK
                    return acc
                sb = _measure(_run_b, reps, warmup)
                row["timing_observation_only"] = {
                    "note": ("TAUTOLOGICAL: fewer pybind transits cost less pybind time; "
                             "NOT a speedup/throughput/latency/HPX/Ray/scheduling claim"),
                    "reps": reps, "warmup": warmup,
                    "empty_loop_floor_ns": empty_floor,
                    "path_a": _summ(sa), "path_b": _summ(sb),
                }
            rows.append(row)

    structural_pass = (not correctness_fail and not count_fail and not witness_fail
                       and all(r["result_equal_ok"] for r in rows)
                       and all(r["a_counts_match_formula"] for r in rows)
                       and all(r["b_counts_match_formula"] for r in rows))

    aggregate = {
        "experiment": "exp45_boundary_placement_comparison",
        "schema": "rayx-boundary-placement-1",
        "claim": (
            "exp45 compares two boundary placements for the same closed fan-in/reduction "
            "workload: one coarse Runtime operation (barrier_fanin) versus Python-mediated "
            "orchestration of fixed Runtime operations (chain_sum_loop per leaf plus a "
            "Python masked fold). It shows the two are value-equivalent while the coarse "
            "native path uses one Runtime submission/retirement, O(1) Python/Runtime "
            "crossings, and the Python-mediated path uses L submissions/retirements, O(L) "
            "crossings. This is boundary-placement/orchestration-count evidence only."),
        "boundary_crossing_formulas": {
            "path_a": "submissions=1, retirements=1, boundary_crossings=2 (O(1), "
                      "independent of L)",
            "path_b": "submissions=L, retirements=L, boundary_crossings=2L (O(L)); "
                      "python_fold_ops=L-1 cross NO Runtime boundary",
        },
        "equivalence": ("result_a == result_b == pure-Python oracle for every L "
                        "(triangulated; the oracle mirrors per-step native masking)"),
        "non_claims": [
            "no speedup", "no throughput",
            "no latency/performance claim (timing is opt-in, observation-only, tautological)",
            "no HPX faster than Ray", "no Ray comparison", "no real inference",
            "no endpoint/fabric claim", "no parcelport", "no AGAS", "no multi-node",
            "no ObjectRef/object-store semantics", "no arbitrary Python execution",
            "no parallelism/scheduling claim (the barrier_fanin gate is incidental here)",
            "no claim that Python orchestration is 'bad' (it is a legitimate, flexible "
            "pattern; exp45 only COUNTS its Runtime-boundary transits for this workload)",
        ],
        "config": {
            "seed": seed, "quantum": quantum, "hpx_threads": args.hpx_threads,
            "leaves_sweep": leaves_sweep,
            "timing_enabled": bool(args.timing),
            "leaf_unit": "chain_sum_loop(seed+j, 1, quantum) == one chain_stage == a "
                         "barrier_fanin leaf (no new op)",
        },
        "machine": {
            "platform": platform.platform(),
            "machine_cpu_count": os.cpu_count(),
        },
        "rows": rows,
        "correctness_failures": correctness_fail,
        "count_failures": count_fail,
        "witness_failures": witness_fail,
        "overall_structural_pass": bool(structural_pass),
    }

    with open(out_path, "w") as f:
        json.dump(aggregate, f, indent=2)
        f.write("\n")

    # Console summary -- LEAD with equivalence + counts, never timing.
    print(f"exp45 boundary-placement: wrote {out_path}")
    print(f"  seed={seed} quantum={quantum} hpx_threads={args.hpx_threads} "
          f"L_sweep={leaves_sweep} timing={'ON' if args.timing else 'off (default)'}")
    print("  [equivalence + boundary-crossing COUNTS are the load-bearing result]")
    print(f"  {'L':>4}  {'result':>12}  {'eq':>3}  "
          f"{'A:sub/ret/xing':>16}  {'B:sub/ret/xing':>16}  {'B:fold':>7}")
    for r in rows:
        a, b = r["path_a"], r["path_b"]
        print(f"  {r['leaves']:>4}  {r['result_a']:>12}  "
              f"{'OK' if r['result_equal_ok'] else 'XX':>3}  "
              f"{a['runtime_submissions']}/{a['runtime_retirements']}/"
              f"{a['boundary_crossings']:>10}  "
              f"{b['runtime_submissions']}/{b['runtime_retirements']}/"
              f"{b['boundary_crossings']:>9}  {b['python_fold_ops']:>7}")
    if args.timing:
        print("  [timing: observation-only + TAUTOLOGICAL; NOT a performance claim]")
        for r in rows:
            t = r["timing_observation_only"]
            print(f"    L={r['leaves']:>3}  A_median={t['path_a']['median_ns']:>10} ns  "
                  f"B_median={t['path_b']['median_ns']:>10} ns")
    print(f"  STRUCTURAL: {'PASS' if structural_pass else 'FAIL'} "
          f"(correctness={len(correctness_fail)}, counts={len(count_fail)}, "
          f"witness={len(witness_fail)})")
    for m in correctness_fail + count_fail + witness_fail:
        print(f"    {m}")
    if not structural_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
