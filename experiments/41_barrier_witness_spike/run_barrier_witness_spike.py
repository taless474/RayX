#!/usr/bin/env python3
"""exp41 wrapper -- cooperative gate/barrier witness spike (EXPERIMENT-ONLY).

Runs the standalone HPX probe `hpx_impl/build/hpx_barrier_witness_spike` once per
--hpx:threads value, each invocation under a HARD subprocess timeout. The probe
itself runs four structural rounds (success, fail_skip, fail_throw, launch_fewer)
and emits one compact JSON object on stdout.

Why the subprocess timeout matters (approved refinement #2): the probe's in-HPX
watchdog can only rescue LOGIC-failure rounds (some expected participant never
arrives -- the children still suspend cooperatively). It CANNOT rescue a true
cooperative-scheduling MECHANISM failure at --hpx:threads=1: if a gate wait ever
blocked the single OS worker, the in-HPX watchdog (also an HPX thread on that
worker) would never run and the process would hang. The external subprocess
timeout here is the only real anti-hang guarantee -- it converts such a hang into
a deterministic FAIL for that thread-count run rather than hanging the whole
experiment.

The timeout is a SAFETY BOUND only; it is NOT timing/performance evidence. This
probe makes NO parallelism, speedup, throughput, or latency claim. The witness is
cooperative suspension + resume + scheduler interleaving through a shared sync
point; success at --hpx:threads=1 is the load-bearing structural signal.

Usage:
  python experiments/41_barrier_witness_spike/run_barrier_witness_spike.py \
      [--threads 1 2] [--participants 8] [--timeout-s 30] \
      [--out experiments/41_barrier_witness_spike/aggregate.json]
"""
import argparse
import json
import os
import subprocess
import sys

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BIN = os.path.join(REPO, "hpx_impl", "build", "hpx_barrier_witness_spike")


def run_one(threads, participants, timeout_s):
    """Run the probe once at a given --hpx:threads under a hard external timeout.

    Returns a dict: the probe's parsed JSON plus a wrapper verdict. A subprocess
    timeout (true-hang safety) becomes a deterministic FAIL, never a hang."""
    cmd = [BIN, f"--hpx:threads={threads}", "--participants", str(participants)]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return {
            "hpx_threads": threads,
            "wrapper_verdict": "FAIL_TIMEOUT",
            "timed_out": True,
            "note": (f"subprocess exceeded {timeout_s}s safety bound; treated as "
                     "cooperative-scheduling mechanism failure / hang"),
        }
    parsed = None
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            parsed = json.loads(line)
    verdict = "PASS" if (proc.returncode == 0 and parsed is not None and
                         parsed.get("all_as_expected")) else "FAIL"
    out = {
        "hpx_threads": threads,
        "wrapper_verdict": verdict,
        "timed_out": False,
        "returncode": proc.returncode,
    }
    if parsed is not None:
        out["probe"] = parsed
    if verdict != "PASS":
        out["stderr_tail"] = "\n".join(proc.stderr.splitlines()[-5:])
    return out


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--threads", type=int, nargs="+", default=[1, 2],
                    help="--hpx:threads values to sweep (default: 1 2)")
    ap.add_argument("--participants", type=int, default=8)
    ap.add_argument("--timeout-s", type=int, default=30,
                    help="hard per-invocation subprocess safety bound (NOT a "
                         "performance figure)")
    ap.add_argument("--out", default=None,
                    help="optional aggregate.json path to write")
    args = ap.parse_args()

    if not os.path.exists(BIN):
        sys.exit(f"FAIL missing probe binary: {BIN}\n"
                 f"build it first: ninja -C hpx_impl/build "
                 f"hpx_barrier_witness_spike")

    runs = []
    for t in args.threads:
        r = run_one(t, args.participants, args.timeout_s)
        runs.append(r)
        succ = None
        if "probe" in r:
            succ = next((rd for rd in r["probe"]["rounds"]
                         if rd["case"] == "success"), None)
        print(f"  hpx_threads={t:>2}  verdict={r['wrapper_verdict']:<12} "
              f"success_round={'PASS' if (succ and succ['success']) else 'n/a'}")

    # Load-bearing gate: the single-worker success round must pass, AND every
    # thread-count run must be all_as_expected with no timeout.
    t1 = next((r for r in runs if r["hpx_threads"] == 1), None)
    t1_success = bool(
        t1 and "probe" in t1 and
        any(rd["case"] == "success" and rd["success"]
            for rd in t1["probe"]["rounds"]))
    all_pass = all(r["wrapper_verdict"] == "PASS" for r in runs)
    overall = bool(all_pass and t1_success)

    aggregate = {
        "experiment": "exp41_barrier_witness_spike",
        "schema": "rayx-barrier-witness-spike-1",
        "claim": ("multiple HPX tasks make coordinated cooperative progress "
                  "through a shared gate: each suspends mid-execution, the "
                  "scheduler runs the others, the last arriver opens the gate, "
                  "all resume and complete"),
        "non_claims": ["no parallelism", "no multi-worker requirement",
                       "no speedup", "no throughput/latency/performance",
                       "no Runtime registry op", "no endpoint bridge",
                       "no HPX socket serving / async socket I/O",
                       "no parcelport / AGAS / multi-node"],
        "load_bearing_signal": ("success at --hpx:threads=1 (a non-cooperative "
                                "gate would deadlock the single worker)"),
        "anti_hang": ("external subprocess timeout = true-hang safety; in-HPX "
                      "watchdog only rescues logic-failure rounds"),
        "timeout_s_safety_bound": args.timeout_s,
        "participants": args.participants,
        "threads_swept": args.threads,
        "single_worker_success": t1_success,
        "overall_pass": overall,
        "runs": runs,
    }

    print(f"\nexp41 overall: {'PASS' if overall else 'FAIL'} "
          f"(single_worker_success={t1_success}, all_runs_pass={all_pass})")

    if args.out:
        with open(args.out, "w") as f:
            json.dump(aggregate, f, indent=2)
            f.write("\n")
        print(f"  wrote {args.out}")

    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
