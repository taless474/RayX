#!/usr/bin/env python3
"""Experiment 23 runner: uncontended Python-boundary cost of the HpxLane
adapter hop (lane_impl="hpx") vs the no-hop ServiceLane path (lane_impl="std").

The rayx Engine runs on the external Python thread. For lane_impl="std" the
RayxLaneAdapter<ServiceLane> forwards lane-state calls DIRECTLY (std primitives
are safe off an HPX thread); for lane_impl="hpx" the RayxLaneAdapter<HpxLane>
must hop EVERY lane-state call onto an HPX worker via hpx::run_as_hpx_thread
(hpx::mutex / hpx::thread may be touched only from an HPX thread). This
experiment characterizes, UNCONTENDED, how much per-call latency that hop adds.

INTERPRETATION (deliberately softened): the std-vs-hpx per-call delta is NOT a
perfect subtraction that "isolates the hop." It is the closest observable
approximation of the hop-dominated boundary cost: it still includes Python /
pybind11 call overhead, HPX scheduling/dispatch overhead, and machine-specific
jitter. At num_lanes=1, lane_stats() is the CLEANEST available public-API isolator
(same mutex+copy work on both backends, the only structural difference being the
single hop -- no lane-worker dispatch, no future, no service). submit_get and
submit_batch additionally include lane-worker dispatch and future retrieval, so
they are reported as end-to-end no-op op cost, NOT a hop measurement.

This is NOT a serving-throughput benchmark, NOT a speedup claim, NOT an
"HPX beats Ray" claim, and NOT an "HpxLane is faster/slower" verdict. std has no
hop only because it uses std primitives; the delta is a structural cost of the
chosen off-HPX-thread seam design, not a backend-quality judgement. All timing
and all derived deltas are OBSERVATION-ONLY and are NEVER gated.

Scope: rayx-only, uncontended no-op (service_ms=0, work_mode="sleep") paths only.
work_mode="spin" is deliberately NOT used here. Separate from exp21 (contract
parity) and exp22 (under-load divergence). No analyzer / benchmark-JSONL schema /
driver / CI / public Future-ownership change; no HPX internals exposed. Not Ray
Serve, not a Ray object store, not real model inference.

Because the HPX runtime is a PROCESS resource (one hpx::start per process with a
fixed worker count), each (backend, hpx_threads) pair runs in its OWN subprocess.
Engine construction/shutdown is OUTSIDE every timed loop, and warmup iterations
are discarded. Raw per-call latency arrays (if kept) are experiment-local scratch
under results/ (gitignored), NOT the v1 benchmark schema. Tracked evidence: the
curated aggregate.json beside this script (summary percentiles only) plus the
markdown report.

Usage (repo root, venv active, _rayx built):
    python experiments/23_rayx_hpxlane_adapter_hop_cost/run_rayx_hpxlane_adapter_hop_cost.py
    python experiments/23_rayx_hpxlane_adapter_hop_cost/run_rayx_hpxlane_adapter_hop_cost.py --quick
    # overrides: --iters N --warmup W --repeats R --hpx-threads "1,4" --ops "lane_stats,submit_get" --batch-size K
    # internal: --worker --backend {std,hpx} --hpx-threads N
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
if _RAYX_SRC not in sys.path:
    sys.path.insert(0, _RAYX_SRC)

BACKENDS = ("std", "hpx")
EXPECTED_PREFIX = {"std": "act-hpx-", "hpx": "act-hpxl-"}
ALL_OPS = ("lane_stats", "submit_get", "submit_batch")

HPX_THREADS_FULL = (1, 4)
HPX_THREADS_QUICK = (1,)
ITERS_FULL = 20000
WARMUP_FULL = 1000
ITERS_QUICK = 2000
WARMUP_QUICK = 200
# submit_batch does K enqueues + a K-drain per iter, so it uses its own (smaller)
# iteration count to keep the no-op request volume bounded.
BATCH_ITERS_FULL = 2000
BATCH_ITERS_QUICK = 200
BATCH_SIZE = 64

OP_DESCS = {
    "lane_stats": "e.lane_stats() at num_lanes=1 -- cleanest available public-API "
                  "isolator of the hop-dominated boundary cost (no worker "
                  "dispatch, no future, no service)",
    "submit_get": "f=e.submit(service_ms=0); f.result() -- end-to-end no-op op "
                  "cost (submit-path + lane-worker dispatch + future retrieval), "
                  "NOT a pure hop measurement",
    "submit_batch": "fs=e.submit_batch(service_ms=0, count=K); e.get(fs) -- "
                    "one-hop-amortized bulk path; per-request figure is derived",
}


def _pct(sorted_xs, q):
    """Linear-interpolated percentile (q in [0,100]) over a pre-sorted list."""
    if not sorted_xs:
        return None
    if len(sorted_xs) == 1:
        return sorted_xs[0]
    k = (len(sorted_xs) - 1) * (q / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_xs) - 1)
    if f == c:
        return round(sorted_xs[f], 4)
    return round(sorted_xs[f] + (sorted_xs[c] - sorted_xs[f]) * (k - f), 4)


def _summary_us(samples_us):
    """Observation-only latency summary (microseconds) over raw per-call samples."""
    xs = sorted(samples_us)
    n = len(xs)
    total_s = sum(xs) / 1e6  # samples are in us; sum/1e6 -> seconds
    return {
        "n": n,
        "min_us": round(xs[0], 4) if xs else None,
        "p50_us": _pct(xs, 50),
        "p90_us": _pct(xs, 90),
        "p99_us": _pct(xs, 99),
        "max_us": round(xs[-1], 4) if xs else None,
        "mean_us": round(statistics.fmean(xs), 4) if xs else None,
        "iters_per_s": round(n / total_s, 1) if total_s > 0 else None,
    }


def _load(path):
    with open(path) as fh:
        return json.load(fh)


# --------------------------------------------------------------------------
# Per-operation measurement. Engine built OUTSIDE the timed loop; warmup
# discarded; completion/shape checks kept OUT of the measured interval.
# --------------------------------------------------------------------------
def _measure_lane_stats(Engine, backend, hpx_threads, iters, warmup, prefix):
    samples = []
    shape_ok = True
    prefix_ok = True
    with Engine(num_lanes=1, hpx_threads=hpx_threads, lane_impl=backend) as e:
        probe = e.lane_stats()
        if not (isinstance(probe, list) and len(probe) == 1
                and {"actor_id", "queue_depth", "active"} <= set(probe[0])):
            shape_ok = False
        if not str(probe[0]["actor_id"]).startswith(prefix):
            prefix_ok = False
        for _ in range(warmup):
            e.lane_stats()
        for _ in range(iters):
            t0 = time.perf_counter_ns()
            e.lane_stats()
            t1 = time.perf_counter_ns()
            samples.append((t1 - t0) / 1000.0)
    return samples, {"n_recorded": len(samples), "shape_ok": shape_ok,
                     "prefix_ok": prefix_ok, "completed": None,
                     "completed_expected": None}


def _measure_submit_get(Engine, backend, hpx_threads, iters, warmup, prefix):
    samples = []
    completed = 0
    prefix_ok = True
    with Engine(num_lanes=1, hpx_threads=hpx_threads, lane_impl=backend) as e:
        for _ in range(warmup):
            e.submit(service_ms=0.0).result()
        for _ in range(iters):
            t0 = time.perf_counter_ns()
            f = e.submit(service_ms=0.0)
            r = f.result()
            t1 = time.perf_counter_ns()
            samples.append((t1 - t0) / 1000.0)  # measured window ends here
            if r["status"] == "completed":
                completed += 1
            if not str(r["actor_id"]).startswith(prefix):
                prefix_ok = False
    return samples, {"n_recorded": len(samples), "shape_ok": True,
                     "prefix_ok": prefix_ok, "completed": completed,
                     "completed_expected": iters}


def _measure_submit_batch(Engine, backend, hpx_threads, iters, warmup, prefix, K):
    samples = []
    completed_iters = 0
    prefix_ok = True
    with Engine(num_lanes=1, hpx_threads=hpx_threads, lane_impl=backend) as e:
        for _ in range(warmup):
            fs = e.submit_batch(service_ms=0.0, count=K)
            e.get(fs)
        for _ in range(iters):
            t0 = time.perf_counter_ns()
            fs = e.submit_batch(service_ms=0.0, count=K)
            rows = e.get(fs)
            t1 = time.perf_counter_ns()
            samples.append((t1 - t0) / 1000.0)  # measured window ends here
            if len(rows) == K and all(r["status"] == "completed" for r in rows):
                completed_iters += 1
            if rows and not str(rows[0]["actor_id"]).startswith(prefix):
                prefix_ok = False
    return samples, {"n_recorded": len(samples), "shape_ok": True,
                     "prefix_ok": prefix_ok, "completed": completed_iters,
                     "completed_expected": iters, "batch_size": K}


def _worker(args):
    from rayx import Engine

    backend = args.backend
    ht = args.hpx_threads
    prefix = EXPECTED_PREFIX[backend]
    ops = args.ops
    iters = ITERS_QUICK if args.quick else ITERS_FULL
    warmup = WARMUP_QUICK if args.quick else WARMUP_FULL
    batch_iters = BATCH_ITERS_QUICK if args.quick else BATCH_ITERS_FULL
    if args.iters is not None:
        iters = args.iters
        batch_iters = args.iters
    if args.warmup is not None:
        warmup = args.warmup
    repeats = args.repeats if args.repeats is not None else 1
    K = args.batch_size

    op_out = {}
    for op in ops:
        pooled = []
        meta = None
        for _ in range(repeats):
            if op == "lane_stats":
                s, m = _measure_lane_stats(Engine, backend, ht, iters, warmup,
                                           prefix)
            elif op == "submit_get":
                s, m = _measure_submit_get(Engine, backend, ht, iters, warmup,
                                           prefix)
            elif op == "submit_batch":
                s, m = _measure_submit_batch(Engine, backend, ht, batch_iters,
                                             warmup, prefix, K)
            else:
                raise ValueError(f"unknown op {op!r}")
            pooled += s
            # Accumulate gate facts across repeats (completion is additive; the
            # boolean shape/prefix facts AND together).
            if meta is None:
                meta = dict(m)
            else:
                meta["n_recorded"] += m["n_recorded"]
                meta["shape_ok"] = meta["shape_ok"] and m["shape_ok"]
                meta["prefix_ok"] = meta["prefix_ok"] and m["prefix_ok"]
                if m["completed"] is not None:
                    meta["completed"] += m["completed"]
                    meta["completed_expected"] += m["completed_expected"]
        summary = _summary_us(pooled)
        op_iters = batch_iters if op == "submit_batch" else iters
        entry = {
            "iters_per_repeat": op_iters,
            "repeats": repeats, "warmup_per_repeat": warmup,
            "n_recorded": meta["n_recorded"],
            "n_expected": op_iters * repeats,
            "shape_ok": meta["shape_ok"], "prefix_ok": meta["prefix_ok"],
            "completed": meta["completed"],
            "completed_expected": meta["completed_expected"],
            "latency": summary,
        }
        if op == "submit_batch":
            entry["batch_size"] = K
            p50 = summary["p50_us"]
            entry["per_request_p50_us"] = round(p50 / K, 4) if p50 is not None \
                else None
        op_out[op] = entry

    out = {
        "backend": backend, "expected_prefix": prefix, "hpx_threads": ht,
        "quick": bool(args.quick), "ops": list(ops), "ran_ok": True,
        "results": op_out,
    }
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2)
    return 0


# --------------------------------------------------------------------------
# Orchestrator: spawn one subprocess per (backend, hpx_threads); gate G1-G5.
# --------------------------------------------------------------------------
def _gate(workers, hpx_threads_list, ops):
    """Firm structural gates only (G1-G5). workers: {(backend,ht): summary}."""
    fails = []
    for (b, ht), s in workers.items():
        tagw = f"{b}/ht{ht}"
        if not s.get("ran_ok"):
            fails.append(f"{tagw}: worker did not report ran_ok")
        if s["expected_prefix"] != EXPECTED_PREFIX[b]:
            fails.append(f"{tagw}: expected_prefix {s['expected_prefix']!r}")
        if list(s["ops"]) != list(ops):
            fails.append(f"{tagw}: op set {s['ops']} != {list(ops)}")
        for op in ops:
            r = s["results"].get(op)
            if r is None:
                fails.append(f"{tagw}/{op}: missing result")
                continue
            tag = f"{tagw}/{op}"
            if r["n_recorded"] != r["n_expected"]:
                fails.append(f"{tag}: G3 sample count "
                             f"{r['n_recorded']} != {r['n_expected']}")
            if not r["shape_ok"]:
                fails.append(f"{tag}: G1 shape check failed")
            if not r["prefix_ok"]:
                fails.append(f"{tag}: G2 actor_id prefix mismatch")
            if r["completed_expected"] is not None \
                    and r["completed"] != r["completed_expected"]:
                fails.append(f"{tag}: G1 completion "
                             f"{r['completed']} != {r['completed_expected']}")
    # G5: both backends ran the same op set + sample counts per (ht, op).
    for ht in hpx_threads_list:
        std, hpx = workers[("std", ht)], workers[("hpx", ht)]
        if list(std["ops"]) != list(hpx["ops"]):
            fails.append(f"ht{ht}: backends ran different op sets")
            continue
        for op in ops:
            ns, nh = (std["results"][op]["n_recorded"],
                      hpx["results"][op]["n_recorded"])
            if ns != nh:
                fails.append(f"ht{ht}/{op}: G5 sample-count parity {ns} != {nh}")
    return fails


def _observations(workers, hpx_threads_list, ops):
    """Observation-only latency summaries + derived std-vs-hpx deltas. NONE of
    this is gated; the deltas are the closest observable approximation of the
    hop-dominated boundary cost, not a perfect subtraction (they still include
    Python/pybind overhead, HPX scheduling overhead, and machine jitter)."""
    per_op = []
    for (b, ht), s in sorted(workers.items()):
        for op in ops:
            r = s["results"][op]
            row = {"backend": b, "hpx_threads": ht, "op": op,
                   "latency_us": r["latency"]}
            if op == "submit_batch":
                row["batch_size"] = r.get("batch_size")
                row["per_request_p50_us"] = r.get("per_request_p50_us")
            per_op.append(row)

    deltas = []
    for ht in hpx_threads_list:
        for op in ops:
            sl = workers[("std", ht)]["results"][op]["latency"]
            hl = workers[("hpx", ht)]["results"][op]["latency"]

            def _d(key):
                a, b2 = hl.get(key), sl.get(key)
                return round(a - b2, 4) if (a is not None and b2 is not None) \
                    else None
            deltas.append({
                "hpx_threads": ht, "op": op,
                "std_p50_us": sl["p50_us"], "hpx_p50_us": hl["p50_us"],
                "delta_p50_us": _d("p50_us"), "delta_p90_us": _d("p90_us"),
                "delta_p99_us": _d("p99_us"),
                "_label": "observation-only; closest observable approximation of "
                          "the hop-dominated boundary cost (includes Python/pybind "
                          "+ HPX scheduling overhead + machine jitter); NOT gated, "
                          "NOT a speedup or faster/slower verdict",
            })
    return {"per_op": per_op, "derived_deltas_non_gating": deltas}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run one (backend, hpx_threads) battery")
    ap.add_argument("--backend", choices=BACKENDS)
    ap.add_argument("--hpx-threads", dest="hpx_threads")
    ap.add_argument("--iters", type=int)
    ap.add_argument("--warmup", type=int)
    ap.add_argument("--repeats", type=int)
    ap.add_argument("--batch-size", dest="batch_size", type=int, default=BATCH_SIZE)
    ap.add_argument("--ops", help='comma list subset of '
                                  f'{",".join(ALL_OPS)} (default: all)')
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="smaller sample (iters 2000 / batch 200, hpx_threads 1); "
                         "no aggregate.json written")
    args = ap.parse_args(argv)

    if args.ops:
        ops = tuple(x.strip() for x in args.ops.split(",") if x.strip())
        bad = [o for o in ops if o not in ALL_OPS]
        if bad:
            raise SystemExit(f"[exp23] unknown ops: {bad}")
    else:
        ops = ALL_OPS
    args.ops = ops

    if args.worker:
        args.hpx_threads = int(args.hpx_threads)
        return _worker(args)

    if args.hpx_threads:
        hpx_threads_list = tuple(int(x) for x in args.hpx_threads.split(","))
    else:
        hpx_threads_list = HPX_THREADS_QUICK if args.quick else HPX_THREADS_FULL

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results",
                           f"rayx_hpxlane_adapter_hop_cost_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp23] raw -> {raw_dir}")
    print(f"[exp23] backends={BACKENDS} hpx_threads={hpx_threads_list} ops={ops} "
          f"quick={args.quick} (structural gates only; timing OBSERVATION-only, "
          f"never gated)")

    workers = {}
    for ht in hpx_threads_list:
        for backend in BACKENDS:
            out = os.path.join(raw_dir, f"{backend}_ht{ht}.json")
            cmd = [sys.executable, os.path.abspath(__file__), "--worker",
                   "--backend", backend, "--hpx-threads", str(ht),
                   "--ops", ",".join(ops), "--out", out]
            if args.quick:
                cmd.append("--quick")
            if args.iters is not None:
                cmd += ["--iters", str(args.iters)]
            if args.warmup is not None:
                cmd += ["--warmup", str(args.warmup)]
            if args.repeats is not None:
                cmd += ["--repeats", str(args.repeats)]
            cmd += ["--batch-size", str(args.batch_size)]
            proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True,
                                  text=True)
            if proc.returncode != 0:
                sys.stderr.write(proc.stderr)
                raise SystemExit(f"[exp23] worker {backend}/ht{ht} failed")
            s = workers[(backend, ht)] = _load(out)
            ls = s["results"].get("lane_stats", {}).get("latency", {})
            print(f"  {backend:3s} ht={ht} ops={len(s['ops'])} "
                  f"lane_stats p50={ls.get('p50_us')}us prefix={s['expected_prefix']}")

    fails = _gate(workers, hpx_threads_list, ops)
    obs = _observations(workers, hpx_threads_list, ops)

    aggregate = {
        "experiment": "rayx_hpxlane_adapter_hop_cost",
        "schema": "rayx-adapter-hop-cost-1",
        "schema_note": "experiment-local; NOT the v1 benchmark JSONL schema",
        "boundary": "hpx-python-frontend",
        "goal": "uncontended per-call Python-boundary cost of the "
                "RayxLaneAdapter<HpxLane> hpx::run_as_hpx_thread hop "
                "(lane_impl='hpx') vs the no-hop ServiceLane path "
                "(lane_impl='std'). Observation only; the delta is the closest "
                "observable approximation of the hop-dominated boundary cost, NOT "
                "a perfect subtraction.",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "backends": list(BACKENDS),
        "expected_prefix": EXPECTED_PREFIX,
        "config": {
            "hpx_threads": list(hpx_threads_list), "num_lanes": 1,
            "ops": list(ops), "op_descriptions": {o: OP_DESCS[o] for o in ops},
            "iters_full": ITERS_FULL, "warmup_full": WARMUP_FULL,
            "iters_quick": ITERS_QUICK, "warmup_quick": WARMUP_QUICK,
            "batch_iters_full": BATCH_ITERS_FULL,
            "batch_iters_quick": BATCH_ITERS_QUICK,
            "batch_size": args.batch_size, "repeats": args.repeats or 1,
            "service_ms": 0.0, "work_mode": "sleep (no-op); spin NOT used",
            "subprocess_axis": "one process per (backend, hpx_threads) because "
                               "the HPX runtime is process-fixed",
            "timing_method": "time.perf_counter_ns around each call; Engine "
                             "construction/shutdown OUTSIDE the timed loop; warmup "
                             "discarded",
        },
        "firm_gates": {
            "G1": "operations complete (lane_stats shape; submit_get/submit_batch "
                  "all rows status=='completed')",
            "G2": "actor_id prefix matches backend (act-hpx- / act-hpxl-)",
            "G3": "expected sample count recorded per op (n_recorded == n_expected)",
            "G4": "no exceptions (worker reports ran_ok; a crash fails the run)",
            "G5": "both backends ran the same op set and sample counts",
        },
        "note": "Uncontended boundary-crossing / adapter-hop cost OBSERVATION "
                "only. NOT a serving-throughput benchmark, NOT a speedup claim, "
                "NOT an 'HPX beats Ray' claim, NOT an 'HpxLane faster/slower' "
                "verdict. The std-vs-hpx delta is the closest observable "
                "approximation of the hop-dominated boundary cost, not a perfect "
                "subtraction -- it still includes Python/pybind11 call overhead, "
                "HPX scheduling/dispatch overhead, and machine-specific jitter; "
                "lane_stats() at num_lanes=1 is the cleanest available public-API "
                "isolator, while submit_get/submit_batch are end-to-end no-op op "
                "cost (they add lane-worker dispatch + future retrieval). std has "
                "no hop only because it uses std primitives; the delta is a "
                "structural cost of the chosen off-HPX-thread seam, not a "
                "backend-quality judgement. Uncontended only -- under load the hop "
                "competes for the worker pool (see exp22). rayx-only, no-op "
                "sleep-path; work_mode='spin' NOT used. Separate from exp21 "
                "(parity) and exp22 (load divergence). No analyzer / "
                "benchmark-JSONL-schema / driver / CI / Future-ownership change; "
                "no HPX internals exposed. Not Ray Serve, not a Ray object store, "
                "not real inference. Raw per-call arrays are experiment-local "
                "scratch under results/ (gitignored); this file keeps summary "
                "percentiles only.",
        "all_structural_gates_passed": not fails,
        "gate_failures": fails,
        "observations_non_gating": obs,
    }

    if not fails:
        print("[exp23] structural gates passed (G1-G5): all operations completed, "
              "prefixes matched, sample counts as expected; timing reported as "
              "observation only")
    else:
        print("[exp23] STRUCTURAL GATE FAILURES:")
        for f in fails:
            print(f"  - {f}")

    # Compact observation readout (NON-gating).
    for d in obs["derived_deltas_non_gating"]:
        print(f"  [obs] ht={d['hpx_threads']} {d['op']:12s} "
              f"std p50={d['std_p50_us']}us hpx p50={d['hpx_p50_us']}us "
              f"delta p50={d['delta_p50_us']}us (observation, non-gating)")

    if args.quick:
        print("[exp23] --quick: aggregate.json NOT written (smoke only)")
    else:
        agg_path = os.path.join(_HERE, "aggregate.json")
        with open(agg_path, "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp23] curated aggregate -> {agg_path}")

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
