#!/usr/bin/env python3
"""exp47: in-process HPX nested-overlap OBSERVATION through the RayX Runtime/lane boundary.
STRUCTURAL / OBSERVATIONAL evidence only -- NOT a performance result.

The workload is ONE fixed two-arm independent fork launched INSIDE a single coarse Runtime
op (``overlap_probe``): two INDEPENDENT bare-``hpx::async`` arms on the default HPX pool,
joined with ``when_all`` below the one Python/Runtime boundary -- "barrier_fanin without
the gate". The closed int64 is mode-INDEPENDENT:

    leaf0 = chain_stage(seed,     quantum)
    leaf1 = chain_stage(seed + 1, quantum)
    result = (leaf0 + leaf1) & MASK            == chain_fanout(seed, 2, 1, quantum)

mode selects only the ARM KERNEL SHAPE (never the value):
  * mode 0 -- non-yielding: one flat chain_stage sweep, no interior yield.
  * mode 1 -- chunked-yielding: the SAME masked sum split into BUSY_SUM_STRIDE (8192)
    chunks with an hpx::this_thread::yield() at each chunk boundary INSIDE the compute.

LOAD-BEARING RESULT (deterministic): nested HPX async work launched inside one Runtime op
schedules, joins, and returns the bit-identical closed int64 correctly through the real
Runtime/lane boundary. This is `overall_structural_pass` and does NOT depend on any
scheduler-sensitive observation.

REPORTED (scheduler-sensitive, separate fields `observation_met` / `observation_warnings`):
whether the debug-only OverlapWitness observed both arms IN FLIGHT together within the
bracketed arm compute, classified from per-chunk hpx::get_worker_thread_num() samples as:
  * serial                  -- max_in_flight < 2 (no in-flight overlap observed)
  * cooperative_interleaving-- max_in_flight >= 2, worker-id evidence consistent with ONE
                               worker (cooperative interleaving; NOT OS-thread parallelism)
  * worker_parallel         -- max_in_flight >= 2, DISTINCT workers observed (candidate)
  * inconclusive            -- unknown/overflowed worker sets or malformed trace
The headline expectation is mode 1 @ hpx_threads=1 -> cooperative_interleaving; it is
REPORTED (warned if not observed), never folded into overall_structural_pass.

PROCESS MODEL: HPX starts ONCE per process at a fixed --hpx:threads and is not re-init'd
in-process, so each hpx_threads regime runs in its OWN SUBPROCESS (this script re-execs
itself with --child --child-threads=N). The parent aggregates the children's rows.

NON-CLAIMS: no speedup; no throughput; no latency/performance claim; no HPX faster than
Ray; no Ray comparison; no real inference; no endpoint/fabric claim; no parcelport; no
AGAS; no multi-node; no ObjectRef/object-store semantics; no arbitrary Python execution;
no arbitrary-parallelism / general-DAG claim; no scheduler control; no placement control;
cooperative suspension is NOT OS-thread parallelism; "worker_parallel" means only "distinct
workers observed"; the witness is racy/debug-only, not scheduler state, not a sync
primitive; no claim that Python orchestration is "bad". No wall-clock value is asserted.

Run (laptop -- generates aggregate.json beside this file):
  PYTHONPATH=python/src python \
    experiments/47_native_overlap_observation/run_native_overlap_observation.py --smoke
"""
import argparse
import json
import os
import platform
import subprocess
import sys

MASK = 0x7FFFFFFF      # mirror BUSY_SUM_MASK in python/src/rayx/runtime_ops.hpp
U64 = (1 << 64) - 1
ARMS = 2               # fixed two-arm fork
BUSY_SUM_STRIDE = 8192  # mirror the native chunk stride (mode-1 yields per ceil(q/stride))
_CLASSES = ("serial", "cooperative_interleaving", "worker_parallel", "inconclusive")


# --- pure-Python oracle (mirrors native masking EXACTLY) ---------------------

def masked_range_sum_py(begin, end):
    acc = 0
    for i in range(begin, end):
        acc = (acc + (i & U64)) & MASK
    return acc


def chain_stage_py(x, q):
    return ((x & U64) + masked_range_sum_py(0, q)) & MASK


def oracle(seed, quantum):
    """The same fixed two-arm fork as overlap_probe(seed, quantum, *), pure Python."""
    return (chain_stage_py(seed, quantum) + chain_stage_py(seed + 1, quantum)) & MASK


# --- child: run one hpx_threads regime in an isolated process ----------------

def run_child(child_threads, seeds, quanta):
    """Construct one Runtime(hpx_threads=child_threads), run both modes across the sweep,
    read the witness after each call, and print the rows as JSON to stdout. HPX is started
    once for this process at child_threads workers (the reason this is a subprocess)."""
    from rayx.runtime import Runtime

    rows = []
    with Runtime(hpx_threads=child_threads) as rt:
        for seed in seeds:
            for quantum in quanta:
                ref = oracle(seed, quantum)
                for mode in (0, 1):
                    val = rt.submit_operation(
                        "overlap_probe", seed, quantum, mode).result().value
                    w = rt.overlap_witness()  # single-in-flight: this call's witness
                    # Cross-check value against the existing fixed fan-out op too.
                    cf = rt.submit_operation(
                        "chain_fanout", seed, 2, 1, quantum).result().value
                    value_ok = (val == ref and cf == ref)
                    # Deterministic structural gates (never scheduler-sensitive).
                    structural_ok = (
                        w["arms_launched"] == ARMS
                        and w["arms_completed"] == ARMS
                        and w["clean_exit"] is True
                        and w["ordering_violations"] == 0
                        and w["both_in_flight"] == (w["max_in_flight"] >= 2)
                        and w["classification"] in _CLASSES
                        and all(w["per_arm_leave_seq"][j] > w["per_arm_enter_seq"][j] >= 0
                                for j in range(ARMS)))
                    rows.append({
                        "hpx_threads": child_threads,
                        "seed": seed, "quantum": quantum, "mode": mode,
                        "result": val, "oracle": ref, "chain_fanout": cf,
                        "value_ok": bool(value_ok),
                        "structural_ok": bool(structural_ok),
                        "observed_os_workers": w["observed_os_workers"],
                        "max_in_flight": w["max_in_flight"],
                        "both_in_flight": bool(w["both_in_flight"]),
                        "classification": w["classification"],
                        "per_arm_worker_ids": w["per_arm_worker_ids"],
                        "per_arm_chunk_event_count": w["per_arm_chunk_event_count"],
                        "event_count": w["event_count"],
                        "event_trace_overflowed": bool(w["event_trace_overflowed"]),
                    })
    print(json.dumps(rows))
    return 0


# --- parent: spawn one subprocess per hpx_threads regime, aggregate ----------

def spawn_child(child_threads, seeds, quanta, src):
    env = dict(os.environ)
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, os.path.abspath(__file__),
           "--child", "--child-threads", str(child_threads),
           "--seeds", ",".join(str(s) for s in seeds),
           "--quanta", ",".join(str(q) for q in quanta)]
    out = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
    if out.returncode != 0:
        sys.stderr.write(out.stderr)
        raise RuntimeError(
            f"child hpx_threads={child_threads} failed (rc={out.returncode})")
    # The child prints exactly one JSON line (its rows); tolerate incidental stderr noise.
    line = [ln for ln in out.stdout.splitlines() if ln.strip().startswith("[")]
    if not line:
        raise RuntimeError(
            f"child hpx_threads={child_threads} produced no rows:\n{out.stdout}")
    return json.loads(line[-1])


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seeds", default="3,7", help="comma-separated seeds (default 3,7)")
    ap.add_argument("--quanta", default="9000,16384,49152",
                    help="comma-separated quanta; use > 8192 so mode 1 actually yields "
                         "(default 9000,16384,49152)")
    ap.add_argument("--threads", default="1,2",
                    help="comma-separated hpx_threads regimes, each run in its OWN "
                         "subprocess (default 1,2)")
    ap.add_argument("--out", default=None)
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop sweep: seeds=3 x quanta in {9000,16384} x threads "
                         "{1,2} (structural only)")
    # Internal child mode (not for direct use).
    ap.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--child-threads", type=int, default=1, help=argparse.SUPPRESS)
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(os.path.dirname(os.path.dirname(here)), "python", "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    seeds = [int(x) for x in args.seeds.split(",")]
    quanta = [int(x) for x in args.quanta.split(",")]

    # Child path: run one regime in this process and emit rows.
    if args.child:
        return run_child(args.child_threads, seeds, quanta)

    if args.smoke:
        seeds = [3]
        quanta = [9000, 16384]
        threads = [1, 2]
    else:
        threads = [int(x) for x in args.threads.split(",")]

    out_path = args.out or os.path.join(here, "aggregate.json")

    rows = []
    for t in threads:
        rows.extend(spawn_child(t, seeds, quanta, src))

    # --- deterministic structural pass (load-bearing) ---
    value_fail = [r for r in rows if not r["value_ok"]]
    structural_fail = [r for r in rows if not r["structural_ok"]]
    overall_structural_pass = (not value_fail and not structural_fail)

    # --- scheduler-sensitive observations (reported, NOT a gate) ---
    # Expectation registry: (mode, hpx_threads) -> expected classification.
    expectations = {
        (1, 1): "cooperative_interleaving",   # the headline: yielding arms interleave
    }
    observation_met = {}
    observation_warnings = []
    for (mode, t), expected in expectations.items():
        matching = [r for r in rows if r["mode"] == mode and r["hpx_threads"] == t]
        met = bool(matching) and all(
            r["classification"] == expected and r["max_in_flight"] >= 2
            for r in matching)
        observation_met[f"mode{mode}_threads{t}"] = {
            "expected_classification": expected, "met": met,
            "observed": sorted({r["classification"] for r in matching}),
        }
        if not met:
            observation_warnings.append(
                f"mode{mode}/threads{t}: expected {expected} (max_in_flight>=2); "
                f"observed {sorted({r['classification'] for r in matching})} "
                f"(scheduler-sensitive -- reported, not a structural failure)")
    # Also surface, observation-only, what mode 0 / threads>=2 produced.
    observed_by_regime = {}
    for r in rows:
        key = f"mode{r['mode']}_threads{r['hpx_threads']}"
        observed_by_regime.setdefault(key, set()).add(r["classification"])
    observed_by_regime = {k: sorted(v) for k, v in observed_by_regime.items()}

    aggregate = {
        "experiment": "exp47_native_overlap_observation",
        "schema": "rayx-native-overlap-1",
        "claim": (
            "exp47's load-bearing result is Runtime/lane-boundary faithfulness/liveness: "
            "two INDEPENDENT bare-hpx::async arms launched inside a single coarse Runtime "
            "op (overlap_probe) and joined with when_all below the one Python/Runtime "
            "boundary schedule, join, and return the bit-identical closed int64 "
            "(triangulated against a pure-Python oracle AND chain_fanout) -- this holds "
            "deterministically (overall_structural_pass). Separately, WHEN the debug-only "
            "witness observes both arms in flight together within the bracketed arm "
            "compute (max_in_flight>=2), exp47 CLASSIFIES that overlap from per-chunk "
            "hpx::get_worker_thread_num() samples as cooperative_interleaving (consistent "
            "with one observed worker -- NOT OS-thread parallelism) or worker_parallel "
            "(distinct workers observed). The overlap observation is conditioned on the "
            "yielding arm-kernel shape and the host scheduler; it is reported separately "
            "(observation_met / observation_warnings) and no overlap is claimed for any "
            "regime where observation_met is false. In-process, structural, "
            "observation-only."),
        "load_bearing_is_structural_pass": True,
        "non_claims": [
            "no speedup", "no throughput", "no latency/performance claim",
            "no HPX faster than Ray", "no Ray comparison", "no real inference",
            "no endpoint/fabric claim", "no parcelport", "no AGAS", "no multi-node",
            "no ObjectRef/object-store semantics", "no arbitrary Python execution",
            "no arbitrary-parallelism / general-DAG claim",
            "no scheduler control", "no placement control",
            "cooperative suspension is NOT OS-thread parallelism",
            "'worker_parallel' means only 'distinct workers observed', not parallel speedup",
            "the witness is racy/debug-only, not scheduler state, not a sync primitive",
            "no claim that Python orchestration is 'bad'",
            "no wall-clock value is asserted",
        ],
        "config": {
            "threads_regimes": threads, "seeds": seeds, "quanta": quanta,
            "arms": ARMS, "busy_sum_stride": BUSY_SUM_STRIDE,
            "process_model": "one subprocess per hpx_threads regime (HPX is started once "
                             "per process at a fixed --hpx:threads and not re-init'd)",
            "value_unit": "overlap_probe(seed, quantum, mode) == chain_fanout(seed, 2, 1, "
                          "quantum); mode selects the arm kernel only (value-independent)",
        },
        "machine": {
            "platform": platform.platform(),
            "machine_cpu_count": os.cpu_count(),
        },
        "rows": rows,
        "value_failures": [
            f"threads={r['hpx_threads']} seed={r['seed']} q={r['quantum']} "
            f"mode={r['mode']}: result={r['result']} oracle={r['oracle']} "
            f"chain_fanout={r['chain_fanout']}" for r in value_fail],
        "structural_failures": [
            f"threads={r['hpx_threads']} seed={r['seed']} q={r['quantum']} "
            f"mode={r['mode']}: structural gate failed (clean_exit/ordering/arms)"
            for r in structural_fail],
        "overall_structural_pass": bool(overall_structural_pass),
        "observation_met": observation_met,
        "observation_warnings": observation_warnings,
        "observed_classification_by_regime": observed_by_regime,
    }

    with open(out_path, "w") as f:
        json.dump(aggregate, f, indent=2)
        f.write("\n")

    # Console summary -- LEAD with the deterministic structural pass, then observations.
    print(f"exp47 native-overlap: wrote {out_path}")
    print(f"  threads={threads} seeds={seeds} quanta={quanta}")
    print("  [load-bearing: value==oracle + witness structural gates through the "
          "Runtime/lane boundary]")
    print(f"  {'thr':>3} {'seed':>5} {'q':>6} {'mode':>4}  {'result':>11}  {'val':>3}  "
          f"{'struct':>6}  {'osw':>3}  {'maxIF':>5}  {'classification':>22}")
    for r in rows:
        print(f"  {r['hpx_threads']:>3} {r['seed']:>5} {r['quantum']:>6} {r['mode']:>4}  "
              f"{r['result']:>11}  {'OK' if r['value_ok'] else 'XX':>3}  "
              f"{'OK' if r['structural_ok'] else 'XX':>6}  "
              f"{r['observed_os_workers']:>3}  {r['max_in_flight']:>5}  "
              f"{r['classification']:>22}")
    print(f"  STRUCTURAL: {'PASS' if overall_structural_pass else 'FAIL'} "
          f"(value_fail={len(value_fail)}, structural_fail={len(structural_fail)})")
    print("  [observations are scheduler-sensitive and REPORTED separately]")
    for k, v in observation_met.items():
        print(f"    observation {k}: expected={v['expected_classification']} "
              f"met={v['met']} observed={v['observed']}")
    for wmsg in observation_warnings:
        print(f"    WARN {wmsg}")
    if not overall_structural_pass:
        for m in aggregate["value_failures"] + aggregate["structural_failures"]:
            print(f"    {m}")
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main() or 0)
