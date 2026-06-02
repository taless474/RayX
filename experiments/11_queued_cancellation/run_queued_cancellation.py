#!/usr/bin/env python3
"""Experiment 11 runner: queued-only cancellation under backlog.

Characterizes the queued-only cancellation primitive (engine.cancel(future) ->
bool, future.cancelled() -> bool, status="cancelled") added to the rayx frontend.
The benchmark driver deliberately never cancels, so this is an EXPERIMENT-LOCAL
runner that drives the facade directly.

Backlog model (per cell): submit all N requests WITHOUT retiring (so a deep FIFO
queue builds on each round-robin lane), apply a cancellation policy, then drain
as-completed (Engine.wait + per-sweep recv_ns), reconstructing rows in submission
order. Cancelled (still-queued) requests resolve immediately with
status="cancelled" and skip service; everything else services normally.

Policies:
  * none          -- baseline; no cancellation.
  * tail_half     -- immediately cancel idx [N/2, N). Those are deep in the queue
                     (idx >= N/2 >> num_lanes), so within the ~ms it takes to
                     submit N requests the lanes cannot have reached them: every
                     such cancel is structurally guaranteed to succeed.
  * alt           -- immediately cancel odd idx. Most are queued (cancel True),
                     but the first num_lanes requests start immediately, so an
                     early odd target may already be running (cancel False). Only
                     the universal invariant + success>=1 are gated (the exact
                     split is scheduling-dependent and is reported, not gated).
  * delayed_front -- wait 2*service_ms, THEN cancel idx [0, N/2). After that wait
                     each lane has serviced its front request(s), so cancelling
                     the front yields some False (already started -> completes)
                     AND some True (still queued -> cancelled): the queued-vs-
                     running boundary, shown honestly.

NO new RayX API, NO driver change, NO result-row / benchmark-JSONL schema change.
The per-run JSONL written here is an EXPERIMENT-LOCAL scratch format (under
results/, gitignored), not the v1 benchmark schema. Tracked evidence: the curated
aggregate.json beside this script + the markdown report.

One subprocess per cell (the rayx Engine owns one HPX runtime per process).

Usage (repo root, venv active, _rayx built):
    python experiments/11_queued_cancellation/run_queued_cancellation.py
    python experiments/11_queued_cancellation/run_queued_cancellation.py --quick
    # internal: --worker runs a single cell (used via subprocess)
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
_BENCH = os.path.join(_REPO_ROOT, "bench")
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
for p in (_BENCH, _RAYX_SRC):
    if p not in sys.path:
        sys.path.insert(0, p)

import analyze_jsonl  # noqa: E402  (reuse the canonical summarizer + percentiles)

WORK_MODES = ("sleep", "spin")
LANES = (1, 4, 8)
HPX_THREADS = 4
SERVICE_MS = 20.0       # long enough for a stable backlog; keeps runtime sane
REQUESTS = 64
REPEATS = 3
POLICIES = ("none", "tail_half", "alt", "delayed_front")
# delayed_front waits this long before cancelling the front half, so each lane
# has serviced >=1 front request (2*service_ms covers the ~25% sleep overshoot
# too) while the rest of the front half is still queued. For that policy to be
# well-formed (some front-half requests still queued after the wait), the front
# half must exceed the work a lane can start in the delay: N/2 > num_lanes *
# (DELAY_FACTOR + 1). Holds for N=64 at lanes<=8 (32 > 24) and N=40 at lanes<=4.
DELAY_FACTOR = 2.0


def _policy_targets(policy, n):
    """Indices to ATTEMPT cancel for a policy (a set), and whether it is delayed."""
    if policy == "none":
        return set(), False
    if policy == "tail_half":
        return set(range(n // 2, n)), False
    if policy == "alt":
        return {i for i in range(n) if i % 2 == 1}, False
    if policy == "delayed_front":
        return set(range(0, n // 2)), True
    raise ValueError(f"unknown policy {policy!r}")


# --------------------------------------------------------------------------
# Worker: run ONE cell in its own process and write experiment-local JSONL.
# --------------------------------------------------------------------------
def _worker(args):
    from rayx import Engine

    n = args.requests
    targets, delayed = _policy_targets(args.policy, n)
    delay_s = (DELAY_FACTOR * args.service_ms) / 1000.0

    with Engine(num_lanes=args.num_lanes, hpx_threads=args.hpx_threads) as engine:
        # Warmup: a few quick requests drained so the measured path is warm.
        for f in [engine.submit(service_ms=1.0, work_mode=args.work_mode)
                  for _ in range(max(2 * args.num_lanes, 4))]:
            f.result()

        # Submit the whole batch WITHOUT retiring -> a deep FIFO backlog per lane.
        futs = [engine.submit(service_ms=args.service_ms,
                              work_mode=args.work_mode, label=f"r{i}")
                for i in range(n)]

        if delayed:
            time.sleep(delay_s)

        # Apply the cancellation policy. cancel_result[i] in {True, False, None}.
        cancel_result = [None] * n
        for i in sorted(targets):
            cancel_result[i] = engine.cancel(futs[i])

        # Drain as-completed; reconstruct input order. Cancelled futures are
        # ready immediately; the rest complete as serviced.
        idx_of = {id(f): i for i, f in enumerate(futs)}
        rows = [None] * n
        inflight = list(futs)
        while inflight:
            ready, inflight = engine.wait(inflight, num_returns=1)
            recv_ns = time.perf_counter_ns()
            for f in ready:
                r = f.result(recv_ns=recv_ns)
                i = idx_of[id(f)]
                rows[i] = {
                    "idx": i,
                    "label": f"r{i}",
                    "label_echoed": r["label"],   # to verify preservation
                    "policy": args.policy,
                    "cancel_attempted": cancel_result[i] is not None,
                    "cancel_result": cancel_result[i],
                    "cancelled_flag": f.cancelled(),  # facade view, post-retire
                    "status": r["status"],
                    "actor_id": r["actor_id"],
                    "submit_ns": r["submit_ns"],
                    "total_ms": r["total_ms"],
                    "queue_wait_ms": r["queue_wait_ms"],
                    "service_ms_observed": r["service_ms_observed"],
                    "work_mode": args.work_mode,
                    "lanes": args.num_lanes,
                }

    with open(args.out, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return 0


# --------------------------------------------------------------------------
# Orchestrator helpers
# --------------------------------------------------------------------------
def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def _load(path):
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _schema1_projection(rows):
    """Project experiment rows to the v1 per-request schema fields the analyzer
    reads, so we can exercise the additive `cancelled` bucket end to end."""
    out = []
    for r in rows:
        out.append({
            "schema_version": "1", "run_id": "exp11", "backend": "synthetic",
            "boundary": "hpx-python-frontend", "workload": "exp11",
            "status": r["status"], "submit_ns": r["submit_ns"],
            "total_ms": r["total_ms"], "queue_wait_ms": r["queue_wait_ms"],
            "service_ms_observed": r["service_ms_observed"],
        })
    return out


def _gate(rows, policy, num_lanes, n, service_ms):
    """Structural, mostly timing-robust contract gates. Returns failure strings."""
    fails = []
    if len(rows) != n:
        fails.append(f"rows {len(rows)} != {n}")
    if any(r["status"] not in ("completed", "cancelled") for r in rows):
        fails.append("unexpected status (not completed/cancelled)")

    # (1) Universal invariant: cancel_result True IFF status == cancelled.
    for r in rows:
        won = (r["cancel_result"] is True)
        is_canc = (r["status"] == "cancelled")
        if won != is_canc:
            fails.append(f"invariant broken at idx {r['idx']}: "
                         f"cancel_result={r['cancel_result']} status={r['status']}")
            break
        # facade Future.cancelled() must agree with the row status too.
        if r["cancelled_flag"] != is_canc:
            fails.append(f"cancelled_flag disagrees at idx {r['idx']}")
            break

    # (2) Cancelled rows: ~zero observed service + label preserved.
    canc = [r for r in rows if r["status"] == "cancelled"]
    svc_cap = max(2.0, 0.2 * service_ms)
    for r in canc:
        if r["service_ms_observed"] > svc_cap:
            fails.append(f"cancelled idx {r['idx']} spent service "
                         f"{r['service_ms_observed']} (> {svc_cap})")
            break
    # (label preservation checked for ALL rows below)

    # (3) Completed rows ran real service.
    comp = [r for r in rows if r["status"] == "completed"]
    for r in comp:
        if r["service_ms_observed"] < 0.5 * service_ms:
            fails.append(f"completed idx {r['idx']} service too small "
                         f"{r['service_ms_observed']} (< {0.5 * service_ms})")
            break

    # Label preservation on every row (cancelled and completed alike).
    if any(r["label_echoed"] != r["label"] for r in rows):
        fails.append("label not preserved on some row")

    # (4) Policy-specific structural expectations.
    attempts = sum(1 for r in rows if r["cancel_attempted"])
    success = sum(1 for r in rows if r["cancel_result"] is True)
    false_ = sum(1 for r in rows if r["cancel_result"] is False)
    if policy == "none":
        if attempts or success or false_:
            fails.append(f"none policy attempted/cancelled work "
                         f"(att={attempts} ok={success} no={false_})")
        if len(comp) != n:
            fails.append(f"none completed {len(comp)} != {n}")
    elif policy == "tail_half":
        exp_att = n - n // 2
        if attempts != exp_att:
            fails.append(f"tail_half attempts {attempts} != {exp_att}")
        if false_ != 0:
            fails.append(f"tail_half had {false_} False cancels (expected 0: "
                         "deep-queue targets cannot start in time)")
        if success != attempts:
            fails.append(f"tail_half success {success} != attempts {attempts}")
    elif policy == "alt":
        # Exact split is scheduling-dependent (first num_lanes start at once);
        # gate only the honest invariant + that queued cancels do succeed.
        if success < 1:
            fails.append("alt had no successful cancels")
    elif policy == "delayed_front":
        # After the delay, the front of each lane has started -> some False; the
        # rest of the front half is still queued -> some True.
        if false_ < 1:
            fails.append("delayed_front had no False cancels (running work "
                         "should not be cancelable)")
        if success < 1:
            fails.append("delayed_front had no successful cancels")
        if success + false_ != attempts:
            fails.append("delayed_front success+false != attempts")

    # (5) Round-robin lane balance over ALL requests (cancelled rows carry the
    # actor_id of the lane they were assigned, so balance holds for the whole
    # submission, not just serviced work).
    from collections import Counter
    lc = Counter(r["actor_id"] for r in rows)
    if len(lc) != num_lanes:
        fails.append(f"lanes_seen {len(lc)} != {num_lanes}")
    if lc and max(lc.values()) - min(lc.values()) > 1:
        fails.append(f"lane imbalance {min(lc.values())}-{max(lc.values())}")

    # (6) Analyzer additive `cancelled` bucket works (schema-1 projection).
    summary = analyze_jsonl.summarize(_schema1_projection(rows))
    if summary["schema_version"] != "1":
        fails.append(f"analyzer schema_version {summary['schema_version']!r}")
    if summary["completed"] != len(comp):
        fails.append(f"analyzer completed {summary['completed']} != {len(comp)}")
    if summary["cancelled"] != len(canc):
        fails.append(f"analyzer cancelled {summary['cancelled']} != {len(canc)}")
    if summary["failed"] != 0:
        fails.append(f"analyzer failed {summary['failed']} != 0")

    return fails


def _cell(work_mode, lanes, policy, raw_dir, repeats, requests, service_ms,
          all_fails):
    att, succ, fls, comp_n, canc_n = [], [], [], [], []
    wall, csvc, dsvc = [], [], []   # wall span; cancelled svc p50; done svc p50
    lanes_seen, lane_min, lane_max = [], [], []
    for rep in range(repeats):
        fname = f"{work_mode}_l{lanes}_{policy}_r{rep}.jsonl"
        out = os.path.join(raw_dir, fname)
        subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--worker",
             "--work-mode", work_mode, "--num-lanes", str(lanes),
             "--hpx-threads", str(HPX_THREADS), "--requests", str(requests),
             "--service-ms", str(service_ms), "--policy", policy, "--out", out],
            check=True, cwd=_REPO_ROOT, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE)
        rows = _load(out)
        fails = _gate(rows, policy, lanes, requests, service_ms)
        if fails:
            all_fails.append((fname, fails))

        att.append(sum(1 for r in rows if r["cancel_attempted"]))
        succ.append(sum(1 for r in rows if r["cancel_result"] is True))
        fls.append(sum(1 for r in rows if r["cancel_result"] is False))
        comp = [r for r in rows if r["status"] == "completed"]
        canc = [r for r in rows if r["status"] == "cancelled"]
        comp_n.append(len(comp))
        canc_n.append(len(canc))
        span_start = min(r["submit_ns"] for r in rows)
        span_end = max(r["submit_ns"] + r["total_ms"] * 1e6 for r in rows)
        wall.append((span_end - span_start) / 1e6)  # ms
        csvc.append(analyze_jsonl._pcts(
            [r["service_ms_observed"] for r in canc])["p50"] if canc else None)
        dsvc.append(analyze_jsonl._pcts(
            [r["service_ms_observed"] for r in comp])["p50"] if comp else None)
        from collections import Counter
        lc = Counter(r["actor_id"] for r in rows)
        lanes_seen.append(len(lc))
        lane_min.append(min(lc.values()))
        lane_max.append(max(lc.values()))

    return {
        "work_mode": work_mode, "lanes": lanes, "policy": policy,
        "service_ms": service_ms, "requests": requests, "repeats": repeats,
        "cancel_attempts": int(statistics.median(att)),
        "cancel_success": int(statistics.median(succ)),
        "cancel_false": int(statistics.median(fls)),
        "completed": int(statistics.median(comp_n)),
        "cancelled": int(statistics.median(canc_n)),
        "wall_ms": _med(wall),
        "cancelled_service_ms_p50": _med(csvc),
        "completed_service_ms_p50": _med(dsvc),
        "lanes_seen": int(statistics.median(lanes_seen)),
        "lane_min_count": int(statistics.median(lane_min)),
        "lane_max_count": int(statistics.median(lane_max)),
        "label_preserved": True,  # gated per run; cell-level summary flag
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="internal: run a single cell and write its JSONL")
    ap.add_argument("--work-mode")
    ap.add_argument("--num-lanes", type=int)
    ap.add_argument("--hpx-threads", type=int, default=HPX_THREADS)
    ap.add_argument("--requests", type=int, default=REQUESTS)
    ap.add_argument("--service-ms", type=float, default=SERVICE_MS)
    ap.add_argument("--policy")
    ap.add_argument("--out")
    ap.add_argument("--quick", action="store_true",
                    help="tiny matrix (sleep+spin, lanes {1,4}, all policies, 1 "
                         "repeat, 40 requests); no aggregate.json")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker(args)

    lanes_set, repeats, requests = LANES, REPEATS, REQUESTS
    if args.quick:
        lanes_set, repeats, requests = (1, 4), 1, 40

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    raw_dir = os.path.join(_REPO_ROOT, "results", f"queued_cancellation_{stamp}")
    os.makedirs(raw_dir, exist_ok=True)
    print(f"[exp11] raw -> {raw_dir}")
    print(f"[exp11] work_mode={WORK_MODES} lanes={lanes_set} policies={POLICIES} "
          f"service_ms={SERVICE_MS} requests={requests} repeats={repeats}")

    cells, all_fails = [], []
    for work_mode in WORK_MODES:
        for lanes in lanes_set:
            for policy in POLICIES:
                c = _cell(work_mode, lanes, policy, raw_dir, repeats, requests,
                          SERVICE_MS, all_fails)
                cells.append(c)
                print(f"  {work_mode:5s} L{lanes:<2d} {policy:14s} "
                      f"att={c['cancel_attempts']:>2d} ok={c['cancel_success']:>2d} "
                      f"no={c['cancel_false']:>2d} done={c['completed']:>2d} "
                      f"canc={c['cancelled']:>2d} wall={c['wall_ms']:>8.1f}ms "
                      f"canc_svc={c['cancelled_service_ms_p50']} "
                      f"done_svc={c['completed_service_ms_p50']}")

    aggregate = {
        "experiment": "queued_cancellation",
        "boundary": "hpx-python-frontend",
        "submit": "facade Engine.submit (single-request, cancelable); full "
                  "backlog submitted without retiring, then policy-cancelled",
        "retire": "as-completed Engine.wait, per-sweep recv_ns, input order",
        "machine": "macOS laptop, 10 cores (4 P + 6 E), single locality",
        "note": ("Queued-ONLY cancellation: a request is cancelable only before "
                 "its lane starts it. Running work is never interrupted. Raw "
                 "per-run JSONL is an experiment-local scratch format under "
                 "results/ (gitignored), NOT the v1 benchmark schema."),
        "policies": {
            "none": "no cancellation (baseline)",
            "tail_half": "immediately cancel idx [N/2,N) (deep queue -> all succeed)",
            "alt": "immediately cancel odd idx (mostly queued; first-per-lane may run)",
            "delayed_front": (f"wait {DELAY_FACTOR}x service_ms, then cancel idx "
                              "[0,N/2) (front partly serviced -> some False)"),
        },
        "matrix": {"work_mode": list(WORK_MODES), "num_lanes": list(lanes_set),
                   "policies": list(POLICIES), "service_ms": SERVICE_MS,
                   "requests": requests, "hpx_threads": HPX_THREADS,
                   "repeats": repeats},
        "cells": cells,
    }
    if not args.quick:
        with open(os.path.join(_HERE, "aggregate.json"), "w") as fh:
            json.dump(aggregate, fh, indent=2)
        print(f"[exp11] wrote {os.path.join(_HERE, 'aggregate.json')}")
    else:
        print("[exp11] --quick: aggregate.json NOT written (smoke only)")

    if all_fails:
        print(f"[exp11] GATES FAILED ({len(all_fails)}):")
        for fname, fails in all_fails:
            print(f"  {fname}: {fails}")
        return 1
    print(f"[exp11] all gates passed; {len(cells)} cells")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
