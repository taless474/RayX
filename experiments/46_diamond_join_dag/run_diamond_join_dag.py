#!/usr/bin/env python3
"""exp46: boundary-placement comparison for ONE FIXED non-linear (diamond/join) DAG.
STRUCTURAL / COUNT evidence only -- NOT a performance result.

The workload is a fixed diamond DAG A -> {B, C} -> D, where the sink D depends on BOTH
arms (the cross-edge join). Each node is a single chain_stage; the closed int64 is:

    A = chain_stage(seed,            quantum)
    B = chain_stage(A + 1,           quantum)
    C = chain_stage(A + 2,           quantum)
    D = chain_stage((B + C) & MASK,  quantum)        # depends on B AND C
    result = D                                       # (B + C) commutative -> order-free

It is computed two ways that must return the BIT-IDENTICAL closed int64:

  Path A -- native / coarse (one Python/Runtime boundary crossing pair):
      rt.submit_operation("diamond_fanin", seed, quantum).result().value
    One submission, one retirement. The root fork (shared_future), the two arms (.then),
    and the cross-edge join (hpx::dataflow) all run NATIVELY, below the boundary. ZERO
    intermediate node values cross back to Python (only the final D).

  Path B -- Python-mediated (many crossings), existing fixed ops only, FAIR FORM:
      A  = chain_sum_loop(seed,            1, q).result()    # submit + retire
      fB = chain_sum_loop(A + 1,           1, q)             # submit B and C together
      fC = chain_sum_loop(A + 2,           1, q)             #   (both need only A)
      B, C = fB.result(), fC.result()                        # retire both
      D  = chain_sum_loop((B + C) & MASK,  1, q).result()    # submit only after B,C held
    `chain_sum_loop(x, 1, q)` applies chain_stage ONCE -> exactly one diamond node. B and
    C are NOT artificially serialized (both submitted before either is retired). Because
    the registered ops are value-in/value-out with NO ObjectRef/object store, Python MUST
    hold each parent value to feed its children: A, B, and C are materialized in Python
    (3 = N-1 intermediate values); the sink D is the final output, not an intermediate.

Oracle (pure Python, triangulation): mirrors native masking EXACTLY. Gate:
    result_a == result_b == oracle.

ANALYTICALLY PREDICTABLE COUNTS (the run CONFIRMS them; it does not discover them):
    Path A: runtime_submissions=1, runtime_retirements=1, boundary_crossings=2,
            intermediate_materializations=0
    Path B: runtime_submissions=N=4, runtime_retirements=4, boundary_crossings=2N=8,
            intermediate_materializations=N-1=3
Counts are OBSERVED (incremented at each submit/retire) and then gated against the exact
formulas. The 2-vs-8 saving is NOT caused by hpx::dataflow -- any native coarse body has
one crossing pair. hpx::dataflow is REPRESENTATIONAL: it expresses and resolves the
diamond cross-edge join below the boundary in HPX-native form. This run makes NO overlap/
concurrency claim (canonical --hpx-threads=1).

Timing is OPT-IN (`--timing`), default OFF, observation-only and TAUTOLOGICAL: fewer
pybind transits cost less pybind time. NOT a speedup/throughput/latency/HPX/Ray claim.

NON-CLAIMS: no speedup; no throughput; no latency/performance claim; no HPX faster than
Ray; no Ray comparison; no real inference; no endpoint/fabric claim; no parcelport; no
AGAS; no multi-node; no ObjectRef/object-store semantics; no arbitrary Python execution;
no parallelism/overlap/scheduling claim; not a proof about arbitrary DAGs (ONE fixed
diamond); and NO claim that Python orchestration is "bad" (it is a legitimate, flexible
pattern -- exp46 only COUNTS its Runtime-boundary transits for this fixed workload).

Run (laptop -- generates aggregate.json beside this file):
  PYTHONPATH=python/src python \
    experiments/46_diamond_join_dag/run_diamond_join_dag.py --smoke
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
NODES = 4              # fixed diamond node count: A, B, C, D


# --- pure-Python oracle (mirrors native masking EXACTLY) ---------------------

def masked_range_sum_py(begin, end):
    acc = 0
    for i in range(begin, end):
        acc = (acc + (i & U64)) & MASK
    return acc


def chain_stage_py(x, q):
    return ((x & U64) + masked_range_sum_py(0, q)) & MASK


def oracle(seed, quantum):
    """The same fixed diamond as diamond_fanin(seed, quantum), pure Python."""
    a = chain_stage_py(seed, quantum)
    b = chain_stage_py(a + 1, quantum)
    c = chain_stage_py(a + 2, quantum)
    return chain_stage_py((b + c) & MASK, quantum)


# --- boundary-crossing counter ----------------------------------------------

class Counter:
    """Counts Python/Runtime boundary transits: one per submit_operation (Python->native)
    and one per RuntimeFuture retirement (.result(), native->Python). Also counts
    intermediate node values materialized in Python (a retired non-sink value that feeds a
    downstream submit)."""
    def __init__(self):
        self.submissions = 0
        self.retirements = 0
        self.intermediate_materializations = 0

    @property
    def boundary_crossings(self):
        return self.submissions + self.retirements


# --- the two paths -----------------------------------------------------------

def path_a(rt, seed, quantum):
    """Native/coarse: one diamond_fanin op. Returns (result, counter)."""
    c = Counter()
    c.submissions += 1
    fut = rt.submit_operation("diamond_fanin", seed, quantum)
    val = fut.result().value
    c.retirements += 1
    # The final D crosses back once; NO intermediate node value is materialized in Python.
    return val, c


def path_b(rt, seed, quantum):
    """Python-mediated, FAIR FORM: 4 chain_sum_loop nodes. B and C are submitted before
    either is retired (both depend only on A). Returns (result, counter)."""
    c = Counter()
    # A: submit + retire (its value is needed to derive B and C).
    c.submissions += 1
    fa = rt.submit_operation("chain_sum_loop", seed, 1, quantum)
    a = fa.result().value
    c.retirements += 1
    c.intermediate_materializations += 1  # A held in Python to feed B, C
    # B and C: submit BOTH before retiring either (fair form; both need only A).
    c.submissions += 1
    fb = rt.submit_operation("chain_sum_loop", a + 1, 1, quantum)
    c.submissions += 1
    fc = rt.submit_operation("chain_sum_loop", a + 2, 1, quantum)
    b = fb.result().value
    c.retirements += 1
    c.intermediate_materializations += 1  # B held in Python to feed D
    cc = fc.result().value
    c.retirements += 1
    c.intermediate_materializations += 1  # C held in Python to feed D
    # D: the cross-edge join -- can be submitted only after BOTH B and C are materialized.
    c.submissions += 1
    fd = rt.submit_operation("chain_sum_loop", (b + cc) & MASK, 1, quantum)
    d = fd.result().value
    c.retirements += 1
    # D is the final output, NOT an intermediate materialization.
    return (d & MASK), c


# --- optional timing (opt-in, observation-only, TAUTOLOGICAL) ----------------

def _pct(xs, p):
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def _summ(samples):
    med, p25, p75 = _pct(samples, 50), _pct(samples, 25), _pct(samples, 75)
    return {"median_ns": int(med), "p25_ns": int(p25), "p75_ns": int(p75),
            "iqr_ns": int(p75 - p25)}


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
    ap.add_argument("--seeds", default="1,3,7",
                    help="comma-separated seed sweep (default 1,3,7)")
    ap.add_argument("--quanta", default="16,64",
                    help="comma-separated quantum sweep (default 16,64)")
    ap.add_argument("--hpx-threads", type=int, default=1,
                    help="canonical 1 (no overlap is claimed; value is order-free)")
    ap.add_argument("--timing", action="store_true",
                    help="opt-in observation-only timing (TAUTOLOGICAL; not a perf claim)")
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--out", default=None)
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop sweep seed=3 x quantum in {16,64} (structural only)")
    args = ap.parse_args()

    if args.smoke:
        seeds = [3]
        quanta = [16, 64]
        reps, warmup = 30, 8
    else:
        seeds = [int(x) for x in args.seeds.split(",")]
        quanta = [int(x) for x in args.quanta.split(",")]
        reps, warmup = args.reps, args.warmup

    here = os.path.dirname(os.path.abspath(__file__))
    out_path = args.out or os.path.join(here, "aggregate.json")
    src = os.path.join(os.path.dirname(os.path.dirname(here)), "python", "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    from rayx.runtime import Runtime

    rows = []
    correctness_fail = []
    count_fail = []

    with Runtime(hpx_threads=args.hpx_threads) as rt:
        for seed in seeds:
            for quantum in quanta:
                ref = oracle(seed, quantum)
                ra, ca = path_a(rt, seed, quantum)
                rb, cb = path_b(rt, seed, quantum)

                equal_ok = (ra == ref and rb == ref)
                if not equal_ok:
                    correctness_fail.append(
                        f"seed={seed} q={quantum}: result_a={ra} result_b={rb} "
                        f"oracle={ref}")
                # Counts are OBSERVED above; gate against the exact (analytic) formulas.
                a_counts_ok = (ca.submissions == 1 and ca.retirements == 1
                               and ca.boundary_crossings == 2
                               and ca.intermediate_materializations == 0)
                b_counts_ok = (cb.submissions == NODES and cb.retirements == NODES
                               and cb.boundary_crossings == 2 * NODES
                               and cb.intermediate_materializations == NODES - 1)
                if not a_counts_ok:
                    count_fail.append(
                        f"seed={seed} q={quantum} Path A counts {ca.submissions}/"
                        f"{ca.retirements}/{ca.boundary_crossings} "
                        f"interm={ca.intermediate_materializations} != 1/1/2 interm=0")
                if not b_counts_ok:
                    count_fail.append(
                        f"seed={seed} q={quantum} Path B counts {cb.submissions}/"
                        f"{cb.retirements}/{cb.boundary_crossings} "
                        f"interm={cb.intermediate_materializations} != "
                        f"{NODES}/{NODES}/{2 * NODES} interm={NODES - 1}")

                row = {
                    "seed": seed, "quantum": quantum,
                    "result_a": ra, "result_b": rb, "oracle": ref,
                    "result_equal_ok": bool(equal_ok),
                    "path_a": {
                        "runtime_submissions": ca.submissions,
                        "runtime_retirements": ca.retirements,
                        "boundary_crossings": ca.boundary_crossings,
                        "intermediate_materializations": ca.intermediate_materializations,
                    },
                    "path_b": {
                        "runtime_submissions": cb.submissions,
                        "runtime_retirements": cb.retirements,
                        "boundary_crossings": cb.boundary_crossings,
                        "intermediate_materializations": cb.intermediate_materializations,
                    },
                    "a_counts_match_formula": bool(a_counts_ok),  # expect 1/1/2, interm 0
                    "b_counts_match_formula": bool(b_counts_ok),  # expect 4/4/8, interm 3
                }

                if args.timing:
                    sa = _measure(
                        lambda s=seed, q=quantum: rt.submit_operation(
                            "diamond_fanin", s, q).result().value, reps, warmup)

                    def _run_b(s=seed, q=quantum):
                        a = rt.submit_operation(
                            "chain_sum_loop", s, 1, q).result().value
                        fb = rt.submit_operation("chain_sum_loop", a + 1, 1, q)
                        fc = rt.submit_operation("chain_sum_loop", a + 2, 1, q)
                        b, c = fb.result().value, fc.result().value
                        return rt.submit_operation(
                            "chain_sum_loop", (b + c) & MASK, 1, q).result().value
                    sb = _measure(_run_b, reps, warmup)
                    row["timing_observation_only"] = {
                        "note": ("TAUTOLOGICAL: fewer pybind transits cost less pybind "
                                 "time; NOT a speedup/throughput/latency/HPX/Ray claim"),
                        "reps": reps, "warmup": warmup,
                        "path_a": _summ(sa), "path_b": _summ(sb),
                    }
                rows.append(row)

    structural_pass = (not correctness_fail and not count_fail
                       and all(r["result_equal_ok"] for r in rows)
                       and all(r["a_counts_match_formula"] for r in rows)
                       and all(r["b_counts_match_formula"] for r in rows))

    aggregate = {
        "experiment": "exp46_diamond_join_dag",
        "schema": "rayx-diamond-join-1",
        "claim": (
            "exp46 compares two boundary placements for one fixed non-linear "
            "(diamond/join) dependency DAG: a single coarse Runtime op (diamond_fanin) "
            "that resolves the cross-edge join natively below the Python/Runtime boundary "
            "via HPX shared_future continuations + hpx::dataflow, versus a Python-mediated "
            "decomposition into fixed value-in/value-out Runtime ops (chain_sum_loop per "
            "node) that must materialize each intermediate node value in Python to satisfy "
            "data dependencies. Both return the bit-identical closed int64 (triangulated "
            "against a pure-Python oracle). The coarse path is 1/1/2 with 0 intermediate "
            "materializations; the Python-mediated path is 4/4/8 with 3. The counts are "
            "analytically predictable; the run is a faithfulness/liveness confirmation, "
            "not a discovery. This is boundary-placement / orchestration-count evidence "
            "for one fixed diamond DAG only."),
        "counts_are_analytically_predictable": True,
        "boundary_crossing_formulas": {
            "path_a": "submissions=1, retirements=1, boundary_crossings=2, "
                      "intermediate_materializations=0 (independent of the DAG)",
            "path_b": "submissions=N=4, retirements=4, boundary_crossings=2N=8, "
                      "intermediate_materializations=N-1=3 (A, B, C; sink D is the output)",
        },
        "dataflow_is_representational": (
            "The 2-vs-8 crossing difference is NOT caused by hpx::dataflow -- any native "
            "coarse body has one crossing pair. shared_future/.then/hpx::dataflow matter "
            "because they express and resolve the diamond cross-edge join below the "
            "boundary in HPX-native form. No overlap/concurrency claim (canonical "
            "--hpx-threads=1)."),
        "equivalence": ("result_a == result_b == pure-Python oracle for every (seed, "
                        "quantum); the oracle mirrors per-step native masking"),
        "non_claims": [
            "no speedup", "no throughput",
            "no latency/performance claim (timing is opt-in, observation-only, tautological)",
            "no HPX faster than Ray", "no Ray comparison", "no real inference",
            "no endpoint/fabric claim", "no parcelport", "no AGAS", "no multi-node",
            "no ObjectRef/object-store semantics", "no arbitrary Python execution",
            "no parallelism/overlap/scheduling claim (canonical --hpx-threads=1)",
            "not a proof about arbitrary DAGs (one fixed diamond)",
            "no claim that Python orchestration is 'bad' (it is a legitimate, flexible "
            "pattern; exp46 only COUNTS its Runtime-boundary transits for this workload)",
        ],
        "config": {
            "hpx_threads": args.hpx_threads,
            "seeds": seeds, "quanta": quanta,
            "nodes": NODES,
            "timing_enabled": bool(args.timing),
            "node_unit": "chain_sum_loop(x, 1, quantum) == one chain_stage == a "
                         "diamond node (no new op)",
        },
        "machine": {
            "platform": platform.platform(),
            "machine_cpu_count": os.cpu_count(),
        },
        "rows": rows,
        "correctness_failures": correctness_fail,
        "count_failures": count_fail,
        "overall_structural_pass": bool(structural_pass),
    }

    with open(out_path, "w") as f:
        json.dump(aggregate, f, indent=2)
        f.write("\n")

    # Console summary -- LEAD with equivalence + counts, never timing.
    print(f"exp46 diamond-join: wrote {out_path}")
    print(f"  hpx_threads={args.hpx_threads} seeds={seeds} quanta={quanta} "
          f"timing={'ON' if args.timing else 'off (default)'}")
    print("  [equivalence + boundary-crossing COUNTS are the load-bearing result;"
          " counts are analytically predictable]")
    print(f"  {'seed':>5} {'q':>5}  {'result':>12}  {'eq':>3}  "
          f"{'A:sub/ret/xing/interm':>22}  {'B:sub/ret/xing/interm':>22}")
    for r in rows:
        a, b = r["path_a"], r["path_b"]
        print(f"  {r['seed']:>5} {r['quantum']:>5}  {r['result_a']:>12}  "
              f"{'OK' if r['result_equal_ok'] else 'XX':>3}  "
              f"{a['runtime_submissions']}/{a['runtime_retirements']}/"
              f"{a['boundary_crossings']}/{a['intermediate_materializations']:>11}  "
              f"{b['runtime_submissions']}/{b['runtime_retirements']}/"
              f"{b['boundary_crossings']}/{b['intermediate_materializations']:>11}")
    if args.timing:
        print("  [timing: observation-only + TAUTOLOGICAL; NOT a performance claim]")
        for r in rows:
            t = r["timing_observation_only"]
            print(f"    seed={r['seed']:>3} q={r['quantum']:>4}  "
                  f"A_median={t['path_a']['median_ns']:>10} ns  "
                  f"B_median={t['path_b']['median_ns']:>10} ns")
    print(f"  STRUCTURAL: {'PASS' if structural_pass else 'FAIL'} "
          f"(correctness={len(correctness_fail)}, counts={len(count_fail)})")
    for m in correctness_fail + count_fail:
        print(f"    {m}")
    if not structural_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
