#!/usr/bin/env python3
"""Runner + reducer for experiment 07: RayX FIFO one_by_one vs windowed
as-completed batch_wait on the bimodal ceiling cell.

Drives bench/run_hpx_python_baseline.py (rayx Python frontend, --api engine)
on the documented L16/c32 bimodal cell, two retire modes, 3 repeats each, then
reduces the per-request JSONL into a curated aggregate.json next to this file.

Raw per-request JSONL is scratch under results/_rayx_ascompleted/ (gitignored);
the preserved evidence is aggregate.json + the write-up. This does NOT use Ray
and does NOT run a matrix.

Reduction (per mode):
  * throughput: the per-repeat values, the median, and min/max
  * representative repeat: the run whose throughput is the median for that mode
  * from that representative repeat: total_ms p50/p90/p99 and service_ms p50
Plus the throughput lift batch_wait/one_by_one (on medians).

Usage:
    # run all 6, then reduce:
    python experiments/07_rayx_as_completed/run_rayx_as_completed.py
    # reduce existing scratch JSONL only (no rerun):
    python .../run_rayx_as_completed.py --reduce-only
"""
import argparse
import glob
import json
import os
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
DRIVER = os.path.join(REPO_ROOT, "bench", "run_hpx_python_baseline.py")
ANALYZER = os.path.join(REPO_ROOT, "bench", "analyze_jsonl.py")
SCRATCH = os.path.join(REPO_ROOT, "results", "_rayx_ascompleted")
AGGREGATE = os.path.join(HERE, "aggregate.json")

# Use the venv interpreter if present (the _rayx extension is built against it);
# fall back to the current interpreter otherwise.
VENV_PY = os.path.join(REPO_ROOT, ".venv", "bin", "python")
PYEXE = VENV_PY if os.path.exists(VENV_PY) else sys.executable

# The documented bimodal ceiling cell (matches experiments 02/06). Fixed across
# every run; only the retire mode varies.
CELL = [
    "--api", "engine",
    "--work-mode", "sleep",
    "--service-pattern", "bimodal",
    "--service-low", "1",
    "--service-high", "20",
    "--service-p-high", "0.1",
    "--seed", "0",
    "--num-lanes", "16",
    "--concurrency", "32",
    "--requests", "1000",
    "--warmup-requests", "20",
    "--hpx-threads", "4",
]
CELL_META = {
    "api": "engine", "work_mode": "sleep", "service_pattern": "bimodal",
    "service_low_ms": 1, "service_high_ms": 20, "service_p_high": 0.1,
    "seed": 0, "num_lanes": 16, "concurrency": 32, "requests": 1000,
    "warmup_requests": 20, "hpx_threads": 4,
}

# (mode key/label, file prefix, extra retire args)
MODES = [
    ("one_by_one", "obo", ["--retire-mode", "one_by_one"]),
    ("batch_wait", "bw", ["--retire-mode", "batch_wait", "--wait-batch", "8"]),
]
REPEATS = 3


def run_all():
    os.makedirs(SCRATCH, exist_ok=True)
    for mode, pre, extra in MODES:
        for r in range(1, REPEATS + 1):
            out = os.path.join(SCRATCH, f"{pre}_r{r}.jsonl")
            cmd = [PYEXE, DRIVER, *CELL, *extra, "--out", out]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                print(f"FAIL: {mode} r{r} exited {proc.returncode}\n{proc.stderr}")
                sys.exit(1)
            print(f"  ran {mode} r{r} -> {os.path.relpath(out, REPO_ROOT)}")


def _summary(path):
    out = subprocess.check_output([PYEXE, ANALYZER, path], text=True)
    return json.loads(out)


def reduce_mode(mode, pre):
    paths = sorted(glob.glob(os.path.join(SCRATCH, f"{pre}_r*.jsonl")))
    if len(paths) != REPEATS:
        print(f"FAIL: expected {REPEATS} {pre}_r*.jsonl, found {len(paths)} "
              f"(run without --reduce-only first)")
        sys.exit(1)
    rows = [_summary(p) for p in paths]
    tputs = [round(r["throughput_req_s"], 1) for r in rows]
    median = statistics.median(tputs)
    rep = rows[tputs.index(median)]  # representative = median-throughput repeat
    return {
        "mode": mode,
        "throughput_req_s": {
            "repeats": tputs, "median": median,
            "min": min(tputs), "max": max(tputs),
        },
        "representative_repeat_index": tputs.index(median) + 1,
        "total_ms": {
            "p50": round(rep["total_ms_p50"], 3),
            "p90": round(rep["total_ms_p90"], 3),
            "p99": round(rep["total_ms_p99"], 3),
        },
        "service_ms_p50": round(rep["service_ms_p50"], 3),
    }


def reduce_all():
    modes = [reduce_mode(mode, pre) for mode, pre, _ in MODES]
    by = {m["mode"]: m for m in modes}
    obo = by["one_by_one"]["throughput_req_s"]["median"]
    bw = by["batch_wait"]["throughput_req_s"]["median"]
    out = {
        "experiment": "07_rayx_as_completed",
        "driver": "bench/run_hpx_python_baseline.py",
        "boundary": "hpx-python-frontend",
        "cell": CELL_META,
        "repeats": REPEATS,
        "modes": modes,
        "lift_batch_wait_over_one_by_one": {
            "ratio": round(bw / obo, 3),
            "percent": round(100 * (bw / obo - 1), 1),
        },
        "native_anchors": {
            "fifo_one_by_one_req_s": 1384,
            "fifo_completion_ms_p50": 21.9,
            "batch_wait_req_s": 2558,
            "batch_wait_completion_ms_p50": 0.003,
        },
    }
    with open(AGGREGATE, "w") as f:
        json.dump(out, f, indent=2)
        f.write("\n")
    print(f"wrote {os.path.relpath(AGGREGATE, REPO_ROOT)}")
    for m in modes:
        t = m["throughput_req_s"]
        print(f"  {m['mode']:>12}: median {t['median']} req/s "
              f"(repeats {t['repeats']}), total p50={m['total_ms']['p50']} ms")
    print(f"  lift batch_wait/one_by_one = {out['lift_batch_wait_over_one_by_one']['ratio']}x "
          f"(+{out['lift_batch_wait_over_one_by_one']['percent']}%)")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--reduce-only", action="store_true",
                    help="skip the runs; reduce existing scratch JSONL only")
    args = ap.parse_args()
    if not args.reduce_only:
        run_all()
    reduce_all()


if __name__ == "__main__":
    main()
