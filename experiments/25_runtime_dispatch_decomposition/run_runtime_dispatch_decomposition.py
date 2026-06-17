#!/usr/bin/env python3
"""Experiment 25 runner: rayx.runtime dispatch decomposition (native probe).

Drives the standalone native binary ``rayx_runtime_dispatch_probe``
(hpx_impl/rayx_runtime_dispatch_probe.cpp), which decomposes the per-operation
dispatch path of the experimental ``rayx.runtime`` ``RuntimeLane`` and estimates
the cost of the nested ``hpx::async(exec_, task).get()`` inside the lane worker.
The probe includes the production runtime headers READ-ONLY and UNMODIFIED (no
pybind, no _rayx); the inline-dispatch lane it also runs is a MEASUREMENT FORK
ONLY -- not production RuntimeLane, not a maintained alternate implementation.

Modes (same registered-op bodies; only dispatch differs):
  inline_body / async_get          primitive estimate of the nested-async hop
  lane_nested_* / lane_inline_*    in-lane estimate (production RuntimeLane vs
                                   the probe-local inline fork), _seq one-at-a-
                                   time and _batch submit-all-then-drain

Ops: square(7), busy_sum(256), park_ms(0) -- tiny bodies only; real parked
durations are exp24's axis and are deliberately NOT measured here.

GATES are structural only (every op completes with the expected value; lane
modes FIFO inversions == 0 and the right id prefix; every (hpx_threads, mode,
op) cell present). ALL timing is OBSERVATION-ONLY and machine-specific: no
performance verdict, no speedup claim, no Ray comparison, no "HPX beats Ray"
claim. The derived deltas are APPROXIMATE, not exact subtractions -- they still
include closure-copy cost, scheduling jitter, and clock granularity.

The HPX runtime is a process resource, so each hpx_threads value is one probe
invocation (--hpx:threads=N). Tracked evidence: the curated aggregate.json
beside this script plus the markdown report. The probe emits JSON on stdout;
nothing is written under results/.

Usage (repo root; probe built via `cmake --build hpx_impl/build`):
    python experiments/25_runtime_dispatch_decomposition/run_runtime_dispatch_decomposition.py
    python experiments/25_runtime_dispatch_decomposition/run_runtime_dispatch_decomposition.py --quick
    # overrides: --probe-bin PATH --hpx-threads "1,2" --iters N --warmup N
"""
import argparse
import json
import os
import subprocess
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
DEFAULT_BIN = os.path.join(_REPO_ROOT, "hpx_impl", "build",
                           "rayx_runtime_dispatch_probe")

HPX_THREADS_FULL = (1, 2)
HPX_THREADS_QUICK = (1, 2)
OPS = ("square", "busy_sum", "park_ms")
MODES = ("inline_body", "async_get", "lane_nested_seq", "lane_inline_seq",
         "lane_nested_batch", "lane_inline_batch")


def _run_probe(bin_path, hpx_threads, quick, iters, warmup):
    cmd = [bin_path, f"--hpx:threads={hpx_threads}"]
    if quick:
        cmd.append("--quick")
    if iters is not None:
        cmd += ["--iters", str(iters)]
    if warmup is not None:
        cmd += ["--warmup", str(warmup)]
    proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"[exp25] probe failed (ht={hpx_threads}, "
                         f"exit={proc.returncode})")
    # The probe prints exactly one JSON object as its only stdout output.
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _gate(runs):
    """Firm structural gates only; timing is never inspected here."""
    fails = []
    for ht, r in runs.items():
        if not r["all_structural_gates_passed"]:
            fails.append(f"ht{ht}: probe-side structural gates failed")
        seen = {(c["mode"], c["op"]) for c in r["cells"]}
        want = {(m, o) for m in MODES for o in OPS}
        if seen != want:
            fails.append(f"ht{ht}: cell set mismatch (missing "
                         f"{sorted(want - seen)})")
        for c in r["cells"]:
            tag = f"ht{ht}/{c['mode']}/{c['op']}"
            if not c["all_completed"]:
                fails.append(f"{tag}: not all operations completed")
            if not c["values_ok"]:
                fails.append(f"{tag}: wrong operation value")
            if c["fifo_inversions"] != 0:
                fails.append(f"{tag}: FIFO inversions "
                             f"({c['fifo_inversions']})")
            if not c["lane_prefix_ok"]:
                fails.append(f"{tag}: lane id prefix mismatch")
    return fails


def _deltas(runs):
    """Derived nested-async-hop estimates per (hpx_threads, op). APPROXIMATE,
    not exact subtractions; observation-only, never gated."""
    out = []
    for ht, r in sorted(runs.items()):
        p50 = {(c["mode"], c["op"]): c["p50_ns"] for c in r["cells"]}
        for op in OPS:
            out.append({
                "hpx_threads": ht, "op": op,
                "primitive_async_minus_inline_p50_ns": round(
                    p50[("async_get", op)] - p50[("inline_body", op)], 1),
                "lane_seq_nested_minus_inline_p50_ns": round(
                    p50[("lane_nested_seq", op)]
                    - p50[("lane_inline_seq", op)], 1),
                "lane_batch_nested_minus_inline_p50_ns": round(
                    p50[("lane_nested_batch", op)]
                    - p50[("lane_inline_batch", op)], 1),
                "lane_seq_nested_p50_ns": p50[("lane_nested_seq", op)],
                "body_p50_ns": p50[("inline_body", op)],
            })
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--probe-bin", default=DEFAULT_BIN,
                    help="path to the rayx_runtime_dispatch_probe binary")
    ap.add_argument("--hpx-threads", dest="hpx_threads",
                    help='override swept HPX worker counts, e.g. "1,2"')
    ap.add_argument("--iters", type=int, help="per-cell iterations override")
    ap.add_argument("--warmup", type=int, help="warmup iterations override")
    ap.add_argument("--quick", action="store_true",
                    help="probe --quick (2000 iters, 30 batches); no "
                         "aggregate.json written")
    args = ap.parse_args(argv)

    if not os.path.exists(args.probe_bin):
        raise SystemExit(
            f"[exp25] probe binary not found: {args.probe_bin}\n"
            f"        build it: cmake --build hpx_impl/build\n"
            f"        (or pass --probe-bin PATH)")

    if args.hpx_threads:
        hpx_threads_list = tuple(int(x) for x in args.hpx_threads.split(","))
    else:
        hpx_threads_list = HPX_THREADS_QUICK if args.quick else HPX_THREADS_FULL

    print(f"[exp25] probe={os.path.relpath(args.probe_bin, _REPO_ROOT)} "
          f"hpx_threads={hpx_threads_list} quick={args.quick} (structural "
          f"gates only; deltas are approximate observations, never gated)")

    runs = {}
    for ht in hpx_threads_list:
        runs[ht] = _run_probe(args.probe_bin, ht, args.quick, args.iters,
                              args.warmup)
        g = "ok" if runs[ht]["all_structural_gates_passed"] else "FAIL"
        print(f"  ht={ht} cells={len(runs[ht]['cells'])} probe-gates={g}")

    fails = _gate(runs)
    deltas = _deltas(runs)

    if not fails:
        print("[exp25] structural gates passed: every cell completed with the "
              "expected value, FIFO held, prefixes correct")
    else:
        print("[exp25] STRUCTURAL GATE FAILURES:")
        for f in fails:
            print(f"  - {f}")

    for d in deltas:
        print(f"  [obs] ht={d['hpx_threads']} {d['op']:9s} nested-hop ~ "
              f"primitive {d['primitive_async_minus_inline_p50_ns']} ns | "
              f"lane seq {d['lane_seq_nested_minus_inline_p50_ns']} ns | "
              f"lane batch {d['lane_batch_nested_minus_inline_p50_ns']} ns "
              f"(lane_nested_seq p50 {d['lane_seq_nested_p50_ns']} ns, "
              f"body {d['body_p50_ns']} ns)")

    aggregate = {
        "experiment": "runtime_dispatch_decomposition",
        "schema": "rayx-runtime-dispatch-decomposition-1",
        "schema_note": "experiment-local; NOT the v1 benchmark JSONL schema",
        "boundary": "rayx-runtime-native-probe",
        "goal": "decompose the rayx.runtime RuntimeLane per-op dispatch path "
                "and estimate the cost of the nested hpx::async(exec_, "
                "task).get() inside the lane worker, via a primitive "
                "inline-vs-async pair and a production-vs-inline-fork lane "
                "pair. Mechanism/structure evidence; every delta is an "
                "approximate observation, never a gate.",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "probe": "hpx_impl/rayx_runtime_dispatch_probe.cpp "
                 "(rayx_runtime_dispatch_probe binary; production runtime "
                 "headers included read-only and unmodified; the inline lane "
                 "is a measurement fork only, not production RuntimeLane)",
        "matrix": {
            "hpx_threads": list(hpx_threads_list),
            "num_lanes": 1,
            "modes": list(MODES),
            "ops": list(OPS),
            "op_args": {"square": 7, "busy_sum": 256, "park_ms": 0},
            "iters": runs[hpx_threads_list[0]]["iters"],
            "warmup": runs[hpx_threads_list[0]]["warmup"],
            "batch_size": runs[hpx_threads_list[0]]["batch_size"],
            "batches": runs[hpx_threads_list[0]]["batches"],
        },
        "firm_gates": {
            "G1": "every operation completes (status=completed, has_value)",
            "G2": "every completed value equals the closed-form expectation",
            "G3": "lane modes: FIFO end_ns inversions == 0",
            "G4": "lane modes: rt-hpx- (production) / rt-prbi- (fork) prefix",
            "G5": "every (hpx_threads, mode, op) cell present in every run",
        },
        "note": "Structural gates only; ALL timing (per-cell percentiles and "
                "every derived delta) is OBSERVATION-ONLY and machine-specific "
                "-- no performance verdict, no speedup claim, no 'HPX beats "
                "Ray' claim, no Ray comparison. The deltas are approximate, "
                "NOT exact subtractions (closure-copy cost, scheduling jitter, "
                "clock granularity remain inside them). The inline lane is a "
                "probe-local measurement fork, never a production backend. "
                "Tiny op bodies at the no-op floor: nothing here describes a "
                "serving workload (corpus service is ms-scale), real I/O, or "
                "inference. No production runtime / analyzer / JSONL / CI "
                "change.",
        "all_structural_gates_passed": not fails,
        "gate_failures": fails,
        "runs": {str(ht): r for ht, r in sorted(runs.items())},
        "derived_deltas_non_gating": deltas,
    }

    if args.quick:
        print("[exp25] --quick: aggregate.json NOT written (smoke only)")
    else:
        agg_path = os.path.join(_HERE, "aggregate.json")
        with open(agg_path, "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp25] curated aggregate -> {agg_path}")

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
