#!/usr/bin/env python3
"""Experiment 16 runner: std lane vs HPX cooperative lane (mechanism probe).

OPT-IN, EXPLICITLY-INCOMPARABLE lane-MECHANISM probe. It compares the native
binary's two serialized-lane implementations under an otherwise identical
single-lane workload:

  * --lane-impl std : rayhpx::ServiceLane -- the stable Ray-actor-like ANCHOR
    (std::thread consumer, BLOCKING std::this_thread::sleep_for).
  * --lane-impl hpx : rayhpx::HpxLane -- the "HPX cooperative lane" (hpx::thread
    consumer, hpx::mutex/condition_variable_any FIFO, COOPERATIVE
    hpx::this_thread::sleep_for that yields the HPX worker while parked).

Both preserve actor-like FIFO (one consumer, one request at a time, submission
order). The ONLY deliberate differences are the consumer thread type, the queue
suspension primitive, and the parked-sleep timer. This probe asks one question:
"what changes if a serialized lane uses HPX-native scheduling/timer primitives
while preserving actor-like FIFO semantics?"

This is NOT comparable to benchmark 06/10 or any prior package: those held the
lane mechanism fixed (always ServiceLane); this VARIES it. The hpx-lane rows
carry a distinct boundary ("hpx-intra-locality-hpxlane") and an "_hpxlane"
workload tag so they never fold into the corpus. HpxLane is NOT a replacement
for ServiceLane and this is NOT a general HPX-scheduler result.

Matrix (native binary only): lane_impl {std,hpx} x service_ms {0,1,5,20},
work_mode=sleep, retire one_by_one, num_lanes=1, concurrency=1, hpx_threads=4.
Spin is intentionally excluded (cooperative timing has no effect on a busy-wait).

Per-run JSONL is scratch under results/ (gitignored). Tracked evidence: the
curated aggregate.json beside this script + the markdown report. Magnitudes are
machine-specific.

Usage (repo root; native binary built):
    python experiments/16_hpx_lane_mechanism_probe/run_lane_mechanism.py
    python experiments/16_hpx_lane_mechanism_probe/run_lane_mechanism.py --quick
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from collections import Counter

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
_BENCH = os.path.join(_REPO_ROOT, "bench")
if _BENCH not in sys.path:
    sys.path.insert(0, _BENCH)

import analyze_jsonl  # noqa: E402  (reuse the canonical summarizer)

PY = sys.executable
NATIVE_BIN = os.path.join(_REPO_ROOT, "hpx_impl", "build", "hpx_synthetic_baseline")

LANE_IMPLS = ("std", "hpx")
WORK_MODE = "sleep"          # sleep-only by design (cooperative timing axis)
HPX_THREADS = 4
SERVICE_MS = (0.0, 1.0, 5.0, 20.0)
REPEATS = 5
REQUESTS = 200
WARMUP = 20

# Per-impl expected row identity (self-documenting; used as structural gates).
EXPECTED = {
    "std": {"boundary": "hpx-intra-locality", "actor_prefix": "act-hpx-"},
    "hpx": {"boundary": "hpx-intra-locality-hpxlane", "actor_prefix": "act-hpxl-"},
}

# Go/no-go (design §5 criterion): the cooperative hpx lane should show a clear,
# reproducible sleep-overshoot DIFFERENCE vs the std lane at the longer service
# times, in the SAME DIRECTION as experiment 15 (hpx lower). The pass condition
# is direction (hpx strictly lower at every assessed service_ms), NOT a fixed
# magnitude -- the magnitude is machine- and duration-dependent and is reported
# per cell as `rel_improvement`. MATERIAL_REL flags cells whose improvement is
# also large (informational only, not part of the verdict).
GONOGO_SERVICE_MS = 5.0   # assess overshoot at service_ms >= this
MATERIAL_REL = 0.30       # informational: flag a >=30% relative improvement


def _driver_cmd(impl, service_ms, requests, out):
    return [NATIVE_BIN, f"--hpx:threads={HPX_THREADS}",
            "--lane-impl", impl, "--service-ms", str(service_ms),
            "--num-lanes", "1", "--concurrency", "1",
            "--requests", str(requests), "--warmup-requests", str(WARMUP),
            "--retire-mode", "one_by_one", "--work-mode", WORK_MODE,
            "--out", out]


def _raw_gate(rows, impl, requests):
    """Per-run structural gates on the raw JSONL. Returns failure strings."""
    fails = []
    exp = EXPECTED[impl]
    if len(rows) != requests:
        fails.append(f"rows {len(rows)} != requests {requests}")
    if len({r.get("request_id") for r in rows}) != len(rows):
        fails.append("duplicate request_id")
    completed = sum(1 for r in rows if r.get("status") == "completed")
    if completed != len(rows):
        fails.append(f"completed {completed} != rows {len(rows)}")
    for r in rows:
        if r.get("schema_version") != "1":
            fails.append(f"schema_version {r.get('schema_version')!r} != '1'")
            break
    for r in rows:
        if r.get("work_mode") != WORK_MODE:
            fails.append(f"work_mode {r.get('work_mode')!r} != {WORK_MODE!r}")
            break
    for r in rows:
        if r.get("boundary") != exp["boundary"]:
            fails.append(f"boundary {r.get('boundary')!r} != {exp['boundary']!r}")
            break
    # Single serialized lane, actor_id prefix matches the impl (FIFO anchor / id).
    actors = {r.get("actor_id") for r in rows}
    if len(actors) != 1:
        fails.append(f"expected 1 actor_id, saw {len(actors)}")
    if any(not str(a).startswith(exp["actor_prefix"]) for a in actors):
        fails.append(f"actor_id prefix != {exp['actor_prefix']!r}: {actors}")
    if any(r.get("service_ms_observed", 0.0) < 0 for r in rows):
        fails.append("negative service_ms_observed")
    return fails


def _run_repeat(impl, service_ms, requests, out, all_fails, where):
    cmd = _driver_cmd(impl, service_ms, requests, out)
    proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        all_fails.append((where, ["driver exit %d: %s" %
                                  (proc.returncode, proc.stderr.strip()[-200:])]))
        return None
    rows = analyze_jsonl.load(out)
    raw_fails = _raw_gate(rows, impl, requests)
    if raw_fails:
        all_fails.append((where, raw_fails))
    summary = analyze_jsonl.summarize(rows)
    if summary.get("schema_version") != "1":
        all_fails.append((where, [f"analyzer schema {summary.get('schema_version')!r}"]))
    lane_counts = Counter(r["actor_id"] for r in rows)
    return summary, lane_counts


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


METRICS = (
    "throughput_req_s",
    "total_ms_p50", "total_ms_p90", "total_ms_p99",
    "queue_wait_ms_p50",
    "service_ms_p50", "service_ms_p90", "service_ms_p99",
)


def _cell(impl, service_ms, raw_dir, repeats, requests, all_fails):
    per = {m: [] for m in METRICS}
    completed_min, lanes_seen = None, []
    for rep in range(repeats):
        fname = f"{impl}_s{int(service_ms)}_r{rep}.jsonl"
        out = os.path.join(raw_dir, fname)
        res = _run_repeat(impl, service_ms, requests, out, all_fails, fname)
        if res is None:
            continue
        summary, lane_counts = res
        for m in METRICS:
            per[m].append(summary.get(m))
        c = summary.get("completed")
        completed_min = c if completed_min is None else min(completed_min, c)
        lanes_seen.append(len(lane_counts))
    cell = {"lane_impl": impl, "service_ms": service_ms, "requests": requests,
            "repeats": repeats, "completed_min": completed_min,
            "lanes_seen": int(statistics.median(lanes_seen)) if lanes_seen else None}
    for m in METRICS:
        cell[m] = _med(per[m])
    # Sleep overshoot (only meaningful for service_ms > 0).
    p50 = cell["service_ms_p50"]
    if service_ms > 0 and p50 is not None:
        cell["overshoot_ms_p50"] = round(p50 - service_ms, 4)
        cell["pct_overshoot_p50"] = round(100.0 * (p50 - service_ms) / service_ms, 3)
    else:
        cell["overshoot_ms_p50"] = None
        cell["pct_overshoot_p50"] = None
    return cell


def _assess_go_no_go(cells):
    """Compute the go/no-go verdict block. FIFO/structural pass is enforced by
    the raw gates (separately); here we assess the experiment-15 prediction:
    the cooperative hpx lane shows materially lower sleep overshoot at the
    longer service times. Returns (verdict_dict, notes_list)."""
    by = {(c["lane_impl"], c["service_ms"]): c for c in cells}
    comparisons = []
    lower = []
    for sm in SERVICE_MS:
        if sm < GONOGO_SERVICE_MS:
            continue
        s = by.get(("std", sm))
        h = by.get(("hpx", sm))
        if not s or not h:
            continue
        so, ho = s.get("pct_overshoot_p50"), h.get("pct_overshoot_p50")
        if so is None or ho is None:
            continue
        rel = None if so == 0 else round((so - ho) / so, 4)
        ok = (so > 0) and (ho < so)  # direction: hpx strictly lower
        comparisons.append({"service_ms": sm, "std_pct_overshoot": so,
                            "hpx_pct_overshoot": ho, "rel_improvement": rel,
                            "hpx_lower": ok,
                            "material": rel is not None and rel >= MATERIAL_REL})
        lower.append(ok)
    overshoot_prediction = bool(lower) and all(lower)
    return {
        "criterion": (f"hpx-lane p50 sleep overshoot strictly LOWER than std-lane "
                      f"at every service_ms >= {GONOGO_SERVICE_MS} (direction; "
                      f"design §5). Magnitude reported per cell as rel_improvement "
                      f"(material flag at >= {MATERIAL_REL})."),
        "overshoot_prediction_met": overshoot_prediction,
        "comparisons": comparisons,
        "interpretation": (
            "GO on the mechanism question if FIFO/structural gates pass (single "
            "actor_id per impl, all completed, schema 1) AND the cooperative hpx "
            "lane shows materially lower sleep overshoot at the longer service "
            "times (the experiment-15 primitive advantage surviving in a real "
            "FIFO lane). This is a mechanism probe, NOT a corpus-comparable "
            "serving-control result and NOT a general HPX-scheduler claim."),
    }


def _print_cell(c):
    print(f"  {c['lane_impl']:3s} s{int(c['service_ms']):<2d} "
          f"thr={c['throughput_req_s']} "
          f"svc_p50={c['service_ms_p50']} svc_p99={c['service_ms_p99']} "
          f"overshoot_p50%={c['pct_overshoot_p50']} "
          f"tot_p50={c['total_ms_p50']} "
          f"lanes={c['lanes_seen']} completed={c['completed_min']}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="tiny subset (service_ms {0,5}, 1 repeat, 20 requests); "
                         "no aggregate.json written")
    args = ap.parse_args(argv)

    if not os.path.exists(NATIVE_BIN):
        print("[exp16] SKIP: native binary not built (build hpx_impl/build first)")
        return 0

    service_set, repeats, requests = SERVICE_MS, REPEATS, REQUESTS
    if args.quick:
        service_set, repeats, requests = (0.0, 5.0), 1, 20

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"lane_mechanism_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    n_runs = len(LANE_IMPLS) * len(service_set) * repeats
    print(f"[exp16] raw -> {raw_dir}")
    print(f"[exp16] lane_impls={LANE_IMPLS} work_mode={WORK_MODE} "
          f"hpx_threads={HPX_THREADS} service_ms={service_set} "
          f"num_lanes=1 concurrency=1 repeats={repeats} requests={requests} "
          f"-> {n_runs} runs")

    cells, all_fails = [], []
    for impl in LANE_IMPLS:
        for sm in service_set:
            c = _cell(impl, sm, raw_dir, repeats, requests, all_fails)
            cells.append(c)
            _print_cell(c)

    go_no_go = _assess_go_no_go(cells)
    print(f"[exp16] go/no-go overshoot prediction met: "
          f"{go_no_go['overshoot_prediction_met']}")
    for cmp in go_no_go["comparisons"]:
        print(f"    s{int(cmp['service_ms'])}: std overshoot {cmp['std_pct_overshoot']}% "
              f"vs hpx {cmp['hpx_pct_overshoot']}% "
              f"(rel improve {cmp['rel_improvement']}, hpx_lower={cmp['hpx_lower']})")

    aggregate = {
        "experiment": "hpx_lane_mechanism_probe",
        "opt_in_incomparable": True,
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("OPT-IN lane-MECHANISM probe: native std lane (rayhpx::ServiceLane, "
                 "blocking sleep -- the stable Ray-actor-like anchor) vs HPX "
                 "cooperative lane (rayhpx::HpxLane, hpx::thread + cooperative "
                 "hpx::this_thread::sleep_for). Both preserve actor-like FIFO "
                 "(single consumer, submission order); only the consumer thread "
                 "type, queue suspension primitive, and parked-sleep timer differ. "
                 "NOT comparable to benchmark 06/10 or the corpus (those fix the "
                 "lane mechanism; this varies it); hpx-lane rows carry boundary "
                 "'hpx-intra-locality-hpxlane' and an '_hpxlane' workload tag. "
                 "HpxLane is NOT a replacement for ServiceLane and this is NOT a "
                 "general HPX-scheduler result. sleep-only (cooperative timing does "
                 "not apply to spin); JSONL schema stays version 1 (no analyzer "
                 "change). Cells are medians across repeats; raw per-run JSONL is "
                 "scratch under results/ (gitignored); magnitudes are "
                 "machine-specific."),
        "matrix": {"lane_impls": list(LANE_IMPLS), "work_mode": WORK_MODE,
                   "hpx_threads": HPX_THREADS, "service_ms": list(service_set),
                   "num_lanes": 1, "concurrency": 1, "retire_mode": "one_by_one",
                   "repeats": repeats, "requests": requests,
                   "warmup_requests": WARMUP},
        "go_no_go": go_no_go,
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp16] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp16] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp16] GATES FAILED ({len(all_fails)}):")
        for where, fails in all_fails:
            print(f"  {where}: {fails}")
        return 1
    print(f"[exp16] all structural gates passed; {len(cells)} cells, {n_runs} runs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
