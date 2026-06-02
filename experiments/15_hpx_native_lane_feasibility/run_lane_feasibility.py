#!/usr/bin/env python3
"""Experiment 15 runner: HPX-native lane-primitive feasibility probe.

ISOLATED PRIMITIVE PROBE. This drives the standalone
`hpx_impl/build/hpx_lane_feasibility` binary, which reuses `service_lane.hpp`
UNMODIFIED to measure two low-level primitives in isolation:

  1. sleep overshoot: std::this_thread::sleep_for vs hpx::this_thread::sleep_for
     at 1 / 5 / 20 ms targets.
  2. no-op dispatch throughput: the current ServiceLane reference path
     (single serialized FIFO lane, service_ms=0) vs a plain hpx::async no-op
     path (scheduler territory, not a serving lane).

These results are NOT comparable to benchmark 06/10 or any serving-control
write-up. There is NO benchmark JSONL schema here, NO retire modes, NO Ray, NO
rayx Python frontend -- just the native primitive probe. The runner:

  * builds the binary command (hpx_threads=4),
  * invokes it once (full or --quick subset),
  * validates the emitted "lane-feasibility-1" JSON,
  * applies LOOSE shape gates (presence + sane sign/range, not exact timing),
  * writes the curated aggregate.json beside this script (full run only).

Raw probe JSON is scratch under results/ (gitignored). Tracked evidence: the
curated aggregate.json + the markdown report.

Usage (repo root; native binary built):
    python experiments/15_hpx_native_lane_feasibility/run_lane_feasibility.py
    python experiments/15_hpx_native_lane_feasibility/run_lane_feasibility.py --quick
"""
import argparse
import json
import os
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
NATIVE_BIN = os.path.join(_REPO_ROOT, "hpx_impl", "build", "hpx_lane_feasibility")

HPX_THREADS = 4
OPS = 50000
REPEATS = 5
SLEEP_SAMPLES = 200

# --quick subset (laptop smoke): tiny ops/sample counts, fewer repeats.
QUICK_OPS = 2000
QUICK_REPEATS = 2
QUICK_SLEEP_SAMPLES = 20

EXPECTED_SLEEP_PRIMITIVES = {
    "std::this_thread::sleep_for",
    "hpx::this_thread::sleep_for",
}
EXPECTED_SLEEP_TARGETS = {1.0, 5.0, 20.0}
EXPECTED_DISPATCH_PATHS = {"service_lane", "hpx_async"}


def _binary_cmd(ops, repeats, sleep_samples, out):
    return [NATIVE_BIN, f"--hpx:threads={HPX_THREADS}",
            "--ops", str(ops), "--repeats", str(repeats),
            "--sleep-samples", str(sleep_samples), "--out", out]


def _gate(probe, ops, repeats, sleep_samples):
    """Loose presence/shape gates on the probe JSON. Returns failure strings.

    Deliberately shape-not-magnitude: we assert the probe ran the requested
    cells and produced sane (non-negative, finite) numbers, NOT specific timings
    (those are machine-specific and not a serving-control result).
    """
    fails = []
    if probe.get("schema") != "lane-feasibility-1":
        fails.append(f"schema {probe.get('schema')!r} != 'lane-feasibility-1'")
    if probe.get("isolated_primitive_probe") is not True:
        fails.append("isolated_primitive_probe flag missing/false")
    if probe.get("hpx_threads") != HPX_THREADS:
        fails.append(f"hpx_threads {probe.get('hpx_threads')} != {HPX_THREADS}")

    sleep_cells = probe.get("sleep_overshoot", [])
    seen_prims, seen_targets = set(), set()
    for c in sleep_cells:
        seen_prims.add(c.get("primitive"))
        seen_targets.add(float(c.get("target_ms", -1)))
        if c.get("samples") != sleep_samples:
            fails.append(f"sleep samples {c.get('samples')} != {sleep_samples}")
        obs = c.get("observed_ms", {})
        tgt = float(c.get("target_ms", 0))
        # Observed sleep must be at least the requested target (overshoot >= 0
        # within clock noise) -- a sleep can't return early in any meaningful way.
        if obs.get("p50", -1) < tgt - 0.1:
            fails.append(f"{c.get('primitive')} @ {tgt}ms observed p50 "
                         f"{obs.get('p50')} < target (sleep returned early?)")
        os_ms = c.get("overshoot_ms", {})
        if os_ms.get("p99", 0) < -0.1:
            fails.append(f"{c.get('primitive')} @ {tgt}ms negative overshoot p99")
    if seen_prims != EXPECTED_SLEEP_PRIMITIVES:
        fails.append(f"sleep primitives {seen_prims} != {EXPECTED_SLEEP_PRIMITIVES}")
    if seen_targets != EXPECTED_SLEEP_TARGETS:
        fails.append(f"sleep targets {seen_targets} != {EXPECTED_SLEEP_TARGETS}")
    if len(sleep_cells) != len(EXPECTED_SLEEP_PRIMITIVES) * len(EXPECTED_SLEEP_TARGETS):
        fails.append(f"sleep cells {len(sleep_cells)} != 6")

    dispatch_cells = probe.get("dispatch", [])
    seen_paths = set()
    for c in dispatch_cells:
        seen_paths.add(c.get("path"))
        if c.get("ops") != ops:
            fails.append(f"{c.get('path')} ops {c.get('ops')} != {ops}")
        if c.get("repeats") != repeats:
            fails.append(f"{c.get('path')} repeats {c.get('repeats')} != {repeats}")
        thr = c.get("throughput_ops_s", {})
        per = thr.get("per_repeat", [])
        if len(per) != repeats:
            fails.append(f"{c.get('path')} per_repeat {len(per)} != {repeats}")
        if thr.get("median", 0) <= 0 or any(x <= 0 for x in per):
            fails.append(f"{c.get('path')} non-positive throughput {per}")
    if seen_paths != EXPECTED_DISPATCH_PATHS:
        fails.append(f"dispatch paths {seen_paths} != {EXPECTED_DISPATCH_PATHS}")
    return fails


def _print_summary(probe):
    print("  sleep overshoot (observed vs target):")
    for c in probe.get("sleep_overshoot", []):
        obs = c.get("observed_ms", {})
        pct = c.get("pct_overshoot", {})
        print(f"    {c['primitive']:<32s} {c['target_ms']:>4.0f}ms -> "
              f"obs p50={obs.get('p50'):.4f} p99={obs.get('p99'):.4f} "
              f"(+{pct.get('p50'):.1f}% / +{pct.get('p99'):.1f}% p99)")
    print("  no-op dispatch throughput (median ops/s; NOT comparable to each "
          "other):")
    for c in probe.get("dispatch", []):
        thr = c.get("throughput_ops_s", {})
        nspo = c.get("ns_per_op", {})
        print(f"    {c['path']:<14s} median={thr.get('median'):.0f} ops/s "
              f"({nspo.get('median'):.0f} ns/op)")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="tiny subset (ops=2000, repeats=2, sleep_samples=20); "
                         "no aggregate.json written")
    args = ap.parse_args(argv)

    if not os.path.exists(NATIVE_BIN):
        print("[exp15] SKIP: native binary not built "
              "(build hpx_impl/build first)")
        return 0

    ops, repeats, sleep_samples = OPS, REPEATS, SLEEP_SAMPLES
    if args.quick:
        ops, repeats, sleep_samples = QUICK_OPS, QUICK_REPEATS, QUICK_SLEEP_SAMPLES

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"lane_feasibility_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    out = os.path.join(raw_dir, "lane_feasibility.json")

    print(f"[exp15] raw -> {raw_dir}")
    print(f"[exp15] hpx_threads={HPX_THREADS} ops={ops} repeats={repeats} "
          f"sleep_samples={sleep_samples}")

    cmd = _binary_cmd(ops, repeats, sleep_samples, out)
    proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"[exp15] FAIL: probe exit {proc.returncode}: "
              f"{proc.stderr.strip()[-400:]}")
        return 1

    with open(out) as fh:
        probe = json.load(fh)
    _print_summary(probe)

    fails = _gate(probe, ops, repeats, sleep_samples)

    aggregate = {
        "experiment": "hpx_native_lane_feasibility",
        "isolated_primitive_probe": True,
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("ISOLATED HPX-native lane-PRIMITIVE probe. Measures only (1) "
                 "sleep overshoot for std::this_thread::sleep_for vs "
                 "hpx::this_thread::sleep_for at 1/5/20ms, and (2) no-op dispatch "
                 "throughput for the current ServiceLane reference path "
                 "(single serialized FIFO lane, service_ms=0) vs a plain "
                 "hpx::async no-op path. NOT comparable to benchmark 06/10 or the "
                 "serving-control corpus; NO benchmark JSONL schema, NO retire "
                 "modes, NO Ray, NO rayx frontend. ServiceLane remains the stable "
                 "actor-like anchor; hpx::async is scheduler territory, not a "
                 "serving-lane result. spin is unrelated here -- it stays a "
                 "CPU-bound diagnostic/calibration axis elsewhere. service_lane.hpp "
                 "is used unmodified. Magnitudes are machine-specific; raw probe "
                 "JSON is scratch under results/ (gitignored)."),
        "config": {"hpx_threads": HPX_THREADS, "ops": ops, "repeats": repeats,
                   "sleep_samples": sleep_samples,
                   "sleep_targets_ms": [1, 5, 20]},
        "probe": probe,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp15] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp15] --quick: aggregate.json NOT written (smoke only)")

    if fails:
        print(f"[exp15] GATES FAILED ({len(fails)}):")
        for f in fails:
            print(f"  {f}")
        return 1
    print("[exp15] all gates passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
