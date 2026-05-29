#!/usr/bin/env python3
"""Runner + reducer for experiment 06: --diag decomposition of the high-lane
FIFO-retire ceiling.

Runs the native hpx_synthetic_baseline on the documented exp02 ceiling cell
(bimodal 1/20 ms, p_high=0.1, seed 0, 16 lanes, concurrency 32, 1000 requests,
warmup 20, single client thread, --hpx:threads=4) across three retire modes,
3 repeats each, with --diag on. Raw per-request JSONL is scratch under
results/06_diag_fifo_ceiling/; the preserved evidence is the 9 diag-1 JSON
files under experiments/06_diag_fifo_ceiling_analysis/diag/ plus the curated
aggregate.json this script writes next to itself.

Reduction (per mode):
  * throughput: median of the 3 repeats, plus min/max
  * representative repeat: the run whose throughput is the median for that mode
  * from that representative repeat:
      - phases_ms push/pickup/service/completion p50/p90/p99
      - queue_depth_at_enqueue p50/p90/p99/max
      - lane utilization min/median/max across lanes
      - per-lane processed min/max across lanes

This does NOT change source and asserts nothing about absolute numbers; it runs
the cell, parses diag-1, and prints a markdown summary table for the note.

Usage:
    python experiments/06_diag_fifo_ceiling_analysis/run_diag_ceiling.py
    python .../run_diag_ceiling.py --bin PATH --repeats 3
Exit 0 == all runs produced valid diag-1 output.
"""
import argparse
import json
import os
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
DEFAULT_BIN = os.path.join(REPO_ROOT, "hpx_impl", "build", "hpx_synthetic_baseline")
DIAG_DIR = os.path.join(HERE, "diag")
SCRATCH_DIR = os.path.join(REPO_ROOT, "results", "06_diag_fifo_ceiling")
AGGREGATE = os.path.join(HERE, "aggregate.json")

# The documented exp02 ceiling cell. Fixed across every run; only the retire
# mode varies. --hpx:threads is consumed by the HPX runtime (hpx_main.hpp),
# not the program's own parser, so it is passed through verbatim.
CELL_ARGS = [
    "--service-pattern", "bimodal",
    "--service-low", "1",
    "--service-high", "20",
    "--service-p-high", "0.1",
    "--seed", "0",
    "--num-lanes", "16",
    "--concurrency", "32",
    "--requests", "1000",
    "--warmup-requests", "20",
    "--client-threads", "1",
    "--hpx:threads=4",
]

# (key, label, extra retire args, context-only?)
MODES = [
    ("one_by_one", "one_by_one", ["--retire-mode", "one_by_one"], False),
    ("batch_wait", "batch_wait --wait-batch 8",
     ["--retire-mode", "batch_wait", "--wait-batch", "8"], False),
    ("submit_all", "submit_all_get_all (upper-bound/context only)",
     ["--retire-mode", "submit_all_get_all"], True),
]


def build_cmd(binary, mode_key, extra, rep):
    out = os.path.join(SCRATCH_DIR, f"{mode_key}_r{rep}.jsonl")
    diag = os.path.join(DIAG_DIR, f"{mode_key}_r{rep}.diag.json")
    cmd = [binary, *CELL_ARGS, *extra, "--out", out, "--diag", "--diag-out", diag]
    return cmd, diag


def run_one(binary, mode_key, extra, rep):
    cmd, diag = build_cmd(binary, mode_key, extra, rep)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    ok = proc.returncode == 0 and os.path.exists(diag)
    status = "OK" if ok else f"FAIL(exit={proc.returncode})"
    print(f"  [{mode_key} r{rep}] {status}")
    if not ok:
        print(f"    cmd: {' '.join(cmd)}")
        print(f"    stderr: {proc.stderr.strip()}")
        return None
    with open(diag) as f:
        return json.load(f)


def lane_stats(diag):
    utils = [la["utilization"] for la in diag["lanes"]]
    procs = [la["processed"] for la in diag["lanes"]]
    return {
        "n_lanes": len(diag["lanes"]),
        "util_min": min(utils), "util_median": statistics.median(utils),
        "util_max": max(utils),
        "processed_min": min(procs), "processed_max": max(procs),
    }


def reduce_mode(mode_key, label, context_only, diags):
    tputs = [d["throughput_req_s"] for d in diags]
    tput_median = statistics.median(tputs)
    # representative repeat: the run whose throughput equals the median. With an
    # odd repeat count statistics.median returns an actual sample value, so this
    # index is exact.
    rep_idx = tputs.index(tput_median)
    rep = diags[rep_idx]
    ph = rep["phases_ms"]
    qd = rep["queue_depth_at_enqueue"]

    def phase(name):
        p = ph[name]
        return {"p50": p["p50"], "p90": p["p90"], "p99": p["p99"]}

    return {
        "mode": mode_key,
        "label": label,
        "context_only": context_only,
        "throughput_req_s": {
            "median": tput_median, "min": min(tputs), "max": max(tputs),
            "all": tputs,
        },
        "representative_repeat": rep_idx + 1,
        "phases_ms": {
            "push": phase("push"), "pickup": phase("pickup"),
            "service": phase("service"), "completion": phase("completion"),
        },
        "queue_depth_at_enqueue": {
            "p50": qd["p50"], "p90": qd["p90"], "p99": qd["p99"], "max": qd["max"],
        },
        "lanes": lane_stats(rep),
    }


def print_table(rows):
    print("\n=== throughput (req/s) ===")
    print("| mode | median | min | max |")
    print("|---|---|---|---|")
    for r in rows:
        t = r["throughput_req_s"]
        print(f"| {r['label']} | {t['median']:.1f} | {t['min']:.1f} | {t['max']:.1f} |")

    print("\n=== phase decomposition, representative repeat (ms) ===")
    print("| mode | phase | p50 | p90 | p99 |")
    print("|---|---|---|---|---|")
    for r in rows:
        for name in ("push", "pickup", "service", "completion"):
            p = r["phases_ms"][name]
            print(f"| {r['mode']} | {name} | {p['p50']:.3f} | {p['p90']:.3f} | {p['p99']:.3f} |")

    print("\n=== queue depth + lane balance, representative repeat ===")
    print("| mode | qdepth p50 | p90 | p99 | max | util min | util med | util max | proc min | proc max |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        q = r["queue_depth_at_enqueue"]
        l = r["lanes"]
        print(f"| {r['mode']} | {q['p50']:.0f} | {q['p90']:.0f} | {q['p99']:.0f} | {q['max']:.0f} "
              f"| {l['util_min']:.3f} | {l['util_median']:.3f} | {l['util_max']:.3f} "
              f"| {l['processed_min']} | {l['processed_max']} |")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bin", default=DEFAULT_BIN)
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()
    if not os.path.exists(args.bin):
        print(f"FAIL: binary not found: {args.bin} (build it first)")
        sys.exit(1)
    os.makedirs(DIAG_DIR, exist_ok=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)

    rows = []
    all_ok = True
    for mode_key, label, extra, context_only in MODES:
        print(f"mode: {label}")
        diags = []
        for rep in range(1, args.repeats + 1):
            d = run_one(args.bin, mode_key, extra, rep)
            if d is None:
                all_ok = False
            else:
                diags.append(d)
        if len(diags) == args.repeats:
            rows.append(reduce_mode(mode_key, label, context_only, diags))
        else:
            print(f"  (skipping reduction for {mode_key}: missing repeats)")

    out = {
        "experiment": "06_diag_fifo_ceiling_analysis",
        "cell": {
            "service_pattern": "bimodal", "service_low_ms": 1,
            "service_high_ms": 20, "service_p_high": 0.1, "seed": 0,
            "num_lanes": 16, "concurrency": 32, "requests": 1000,
            "warmup_requests": 20, "client_threads": 1, "hpx_threads": 4,
        },
        "repeats": args.repeats,
        "modes": rows,
    }
    with open(AGGREGATE, "w") as f:
        json.dump(out, f, indent=2)
        f.write("\n")
    print(f"\nwrote {AGGREGATE}")
    print_table(rows)
    if not all_ok:
        print("\nFAIL: one or more runs did not produce diag output")
        sys.exit(1)
    print("\nOK: all runs produced valid diag-1 output")


if __name__ == "__main__":
    main()
