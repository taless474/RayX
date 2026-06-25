#!/usr/bin/env python3
"""exp44: witnessed barrier-gated fan-in Runtime op (keystone integration of
exp39/40/41). Structural gates only -- NO timing/performance evidence.

This runner sweeps `--hpx:threads` and, for EACH value, runs the `barrier_fanin` op in a
FRESH CHILD PROCESS under a HARD subprocess timeout. Why a child + external timeout: a
genuine cooperative-scheduling failure of the mutually-gated interior would hang the HPX
runtime (at one OS worker, a non-cooperative gate wait would pin the only worker and the
gate could never open). The external subprocess timeout is the ONLY real anti-hang
guarantee -- it converts such a hang into a deterministic FAIL. The op's internal
watchdog is defense-in-depth only; a watchdog-opened SUCCESS run is a structural FAILURE.

Per child + per leaves config the child reports the debug-only structural witness
(`Runtime.barrier_fanin_witness()`) plus the value oracle check
`barrier_fanin(seed, leaves, quantum) == chain_fanout(seed, leaves, 1, quantum)`.

Load-bearing signal: at `--hpx:threads=1` with ONE observed HPX OS worker, a clean
completion of the barrier-gated interior (arrived==released==leaves, opener==last_arriver,
watchdog_opened==false, ordering_violations==0, reduction_after_all_leaves==true,
clean_exit==true) witnesses cooperative suspend/resume on a single default-pool worker.

Allowed claim / non-claims: see barrier_fanin_witness.md. NO speedup, NO throughput, NO
latency, NO HPX-faster-than-Ray, NO Ray comparison, NO parallelism-required claim, NO
general DAG scheduler claim, NO scheduler introspection, NO public scheduler API, NO
arbitrary Python callbacks, NO ObjectRef/object store, NO endpoint/fabric, NO parcelport,
NO AGAS, NO multi-node, NO persistent transport.

Run (laptop -- generates aggregate.json beside this file):
  PYTHONPATH=python/src python \
    experiments/44_barrier_fanin_witness/run_barrier_fanin_witness.py --smoke
"""
import argparse
import json
import os
import platform
import subprocess
import sys

SEED = 3            # fixed seed (any int64; value is seed-derived per leaf)
QUANTUM = 64        # per-leaf on-core work units (chain_stage), value-neutral of the gate


def _child(threads, leaves_list, quantum):
    """Child entry: build a Runtime at `threads`, run barrier_fanin for each leaves
    config, and print one JSON object on stdout. Lives in its own process so the parent's
    external timeout is the true anti-hang guard."""
    from rayx.runtime import Runtime
    rows = []
    with Runtime(hpx_threads=threads) as rt:
        for leaves in leaves_list:
            value = rt.submit_operation(
                "barrier_fanin", SEED, leaves, quantum).result().value
            witness = rt.barrier_fanin_witness()  # single-in-flight snapshot
            oracle = rt.submit_operation(
                "chain_fanout", SEED, leaves, 1, quantum).result().value
            rows.append({
                "leaves": leaves,
                "value": value,
                "oracle": oracle,
                "oracle_ok": (value == oracle),
                "witness": witness,
            })
    print(json.dumps({"threads": threads, "rows": rows}))


def _gate_row(threads, row):
    """Structural gates for one (threads, leaves) row. Returns (ok, reasons)."""
    reasons = []
    w = row["witness"]
    leaves = row["leaves"]
    if not row["oracle_ok"]:
        reasons.append(f"oracle {row['value']} != chain_fanout {row['oracle']}")
    # Common structural invariants (all thread counts).
    if w["arrived_count"] != leaves:
        reasons.append(f"arrived {w['arrived_count']} != {leaves}")
    if w["released_count"] != leaves:
        reasons.append(f"released {w['released_count']} != {leaves}")
    if w["opener"] != "last_arriver":
        reasons.append(f"opener={w['opener']!r} (expected last_arriver)")
    if w["watchdog_opened"]:
        reasons.append("watchdog_opened on a success run (masked broken path)")
    if w["ordering_violations"] != 0:
        reasons.append(f"ordering_violations={w['ordering_violations']}")
    if not w["reduction_after_all_leaves"]:
        reasons.append("reduction_after_all_leaves=false")
    if not w["clean_exit"]:
        reasons.append("clean_exit=false")
    # Load-bearing single-worker premise: ONLY hard-gated at threads=1 (leaves>=2).
    if threads == 1:
        if w["observed_os_workers"] != 1:
            reasons.append(
                f"observed_os_workers={w['observed_os_workers']} != 1 (single-worker "
                f"premise)")
        if leaves < 2:
            reasons.append("threads=1 load-bearing row must use leaves>=2")
    return (not reasons), reasons


def run_one(threads, leaves_list, quantum, timeout_s, here, src):
    """Run the child at `threads` under a hard external timeout. A timeout (true-hang
    safety) becomes a deterministic FAIL, never a hang."""
    env = dict(os.environ)
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, os.path.abspath(__file__), "--child", str(threads),
           "--leaves", ",".join(str(x) for x in leaves_list),
           "--quantum", str(quantum)]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout_s, env=env)
    except subprocess.TimeoutExpired:
        return {"threads": threads, "timed_out": True, "rows": [], "rows_ok": False,
                "reasons": [f"subprocess exceeded {timeout_s}s safety bound -> FAIL "
                            f"(cooperative-scheduling hang)"]}
    if proc.returncode != 0:
        return {"threads": threads, "timed_out": False, "rows": [], "rows_ok": False,
                "reasons": [f"child exit {proc.returncode}: "
                            f"{proc.stderr.strip()[-400:]}"]}
    out = None
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            out = json.loads(line)
    if out is None:
        return {"threads": threads, "timed_out": False, "rows": [], "rows_ok": False,
                "reasons": [f"no JSON from child; stderr: {proc.stderr.strip()[-400:]}"]}
    reasons = []
    all_ok = True
    for row in out["rows"]:
        ok, why = _gate_row(threads, row)
        all_ok = all_ok and ok
        reasons.extend(f"leaves={row['leaves']}: {r}" for r in why)
    return {"threads": threads, "timed_out": False, "rows": out["rows"],
            "rows_ok": all_ok, "reasons": reasons,
            "observed_os_workers": (out["rows"][0]["witness"]["observed_os_workers"]
                                    if out["rows"] else None)}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--child", type=int, default=None,
                    help=argparse.SUPPRESS)  # internal child entry
    ap.add_argument("--threads", default="1,2,4",
                    help="--hpx:threads values to sweep (default: 1,2,4)")
    ap.add_argument("--leaves", default="2,4,8,16",
                    help="leaves configs (default: 2,4,8,16; load-bearing uses >=2)")
    ap.add_argument("--quantum", type=int, default=QUANTUM)
    ap.add_argument("--timeout-s", type=int, default=60,
                    help="hard per-child subprocess safety bound (NOT timing evidence)")
    ap.add_argument("--out", default=None,
                    help="aggregate.json path (default: beside this script)")
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop sweep (threads 1,2; leaves 2,4)")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(os.path.dirname(os.path.dirname(here)), "python", "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    # Child entry: run the op and print the witness JSON, nothing else.
    if args.child is not None:
        leaves_list = [int(x) for x in args.leaves.split(",")]
        _child(args.child, leaves_list, args.quantum)
        return 0

    if args.smoke:
        threads = [1, 2]
        leaves_list = [2, 4]
    else:
        threads = [int(x) for x in args.threads.split(",")]
        leaves_list = [int(x) for x in args.leaves.split(",")]

    out_path = args.out or os.path.join(here, "aggregate.json")
    results = [run_one(t, leaves_list, args.quantum, args.timeout_s, here, src)
               for t in threads]

    # overall_structural_pass: every run's rows pass AND a threads=1 run is present and
    # passed (the load-bearing single-worker case must exist and hold).
    all_rows_ok = all(r["rows_ok"] for r in results)
    t1 = next((r for r in results if r["threads"] == 1), None)
    t1_ok = bool(t1 and t1["rows_ok"] and not t1["timed_out"])
    overall = bool(all_rows_ok and t1_ok)

    aggregate = {
        "experiment": "exp44_barrier_fanin_witness",
        "schema": "rayx-barrier-fanin-witness-1",
        "op": "barrier_fanin(seed, leaves, quantum) -> int64",
        "claim": (
            "exp44 shows the RayX Runtime boundary and lane execution model preserve "
            "cooperative HPX scheduling for a barrier-gated fan-in interior: a single "
            "coarse-grained registered operation, reached through one Python/Runtime "
            "boundary crossing, launches K bare-hpx::async leaves that mutually "
            "rendezvous on a shared cooperative gate, joins them with when_all, and "
            "reduces them with a scheduled .then continuation -- completing correctly and "
            "load-bearing at --hpx:threads=1 with one observed HPX OS worker, with "
            "structural witness evidence: arrived==released==leaves, opener==last_arriver, "
            "watchdog_opened==false, ordering_violations==0, reduction_after_all_leaves=="
            "true. A non-cooperative interior would deadlock at one worker."),
        "load_bearing_signal": (
            "clean completion at --hpx:threads=1 with observed_os_workers==1 and the "
            "structural witness gates -- cooperative suspend/resume of the gated leaves "
            "on a single default-pool worker; a non-cooperative interior would deadlock"),
        "non_claims": [
            "no speedup", "no throughput", "no latency/performance claim",
            "no HPX faster than Ray", "no Ray comparison",
            "no parallelism-required claim", "no general DAG scheduler claim",
            "no general scheduler introspection", "no public scheduler API",
            "no arbitrary Python callbacks", "no ObjectRef/object-store semantics",
            "no endpoint/fabric claim", "no parcelport", "no AGAS", "no multi-node",
            "no persistent transport",
            "max_simultaneously_suspended_leaves is coordinated suspension, NOT "
            "parallelism / throughput / worker-level concurrency",
        ],
        "anti_hang": (
            "external per-child subprocess timeout = true-hang safety; the op's internal "
            "watchdog is defense-in-depth only and a watchdog-opened success run is a "
            "structural FAILURE"),
        "config": {
            "seed": SEED, "quantum": args.quantum,
            "threads_sweep": threads, "leaves_sweep": leaves_list,
            "timeout_s_safety_bound": args.timeout_s,
        },
        "machine": {
            "platform": platform.platform(),
            "machine_cpu_count": os.cpu_count(),
        },
        "runs": results,
        "overall_structural_pass": overall,
    }

    with open(out_path, "w") as f:
        json.dump(aggregate, f, indent=2)
        f.write("\n")

    print(f"exp44 barrier_fanin_witness: wrote {out_path}")
    print(f"  seed={SEED} quantum={args.quantum} threads={threads} "
          f"leaves={leaves_list} timeout_s={args.timeout_s}")
    for r in results:
        tag = ("TIMEOUT" if r["timed_out"]
               else ("PASS" if r["rows_ok"] else "FAIL"))
        ow = r.get("observed_os_workers")
        print(f"  --- hpx_threads={r['threads']}  observed_os_workers={ow}  -> {tag}")
        for reason in r["reasons"]:
            print(f"        {reason}")
    print(f"  STRUCTURAL: {'PASS' if overall else 'FAIL'} "
          f"(all rows ok={all_rows_ok}; threads=1 load-bearing ok={t1_ok})")
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
