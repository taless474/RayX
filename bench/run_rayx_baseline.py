"""Driver for the standalone RayX Engine baseline.

Drives N synthetic requests through a single in-process ``rayx.Engine`` (no Ray)
and writes one JSONL row per request, v1-schema-compatible with
bench/run_ray_baseline.py and bench/run_ray_hosting_rayx.py so all three legs
share one analyzer path (bench/analyze_jsonl.py).

Boundary measured: rayx-local (Python client + C++ Engine + HPX serving lane,
all in ONE process). The Engine computes submit/total/queue/service timing from
a single monotonic clock, so total_ms is exact and queue_wait_ms is exact (not
cross-process-approximated). This is the "local HPX/RayX serving only" leg used
for boundary decomposition against the Ray legs -- NOT a performance claim.

Example:
  python bench/run_rayx_baseline.py --service-ms 1 --concurrency 4 \\
      --requests 200 --warmup-requests 20 --out results/rayx_local_sleep1.jsonl
"""

import argparse
import json
import os
import sys
import time
import uuid

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
_BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (_REPO_ROOT, _BENCH_DIR, _RAYX_SRC):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import rayx  # noqa: E402

from service_sequence import service_for as _service_for  # noqa: E402

SCHEMA_VERSION = "1"
BACKEND = "rayx"
BOUNDARY = "rayx-local"


def _gen_run_id():
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"rayxlocal-{stamp}-{uuid.uuid4().hex[:6]}"


def _fmt_ms(service_ms):
    if float(service_ms).is_integer():
        return str(int(service_ms))
    return str(service_ms)


def _default_workload(service_ms, suffix):
    if service_ms == 0:
        return f"noop_{suffix}"
    return f"sleep_{_fmt_ms(service_ms)}ms_{suffix}"


def _make_record(args, run_id, workload, req_id, row, service_ms_requested):
    # The Engine row already carries exact same-process timing.
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "backend": BACKEND,
        "boundary": BOUNDARY,
        "workload": workload,
        "request_id": req_id,
        "actor_id": row["actor_id"],
        "worker_id": None,
        "status": row["status"],
        "submit_ns": row["submit_ns"],
        "start_ns": row["start_ns"],
        "end_ns": row["end_ns"],
        "total_ms": row["total_ms"],
        "queue_wait_ms": row["queue_wait_ms"],
        "service_ms_observed": row["service_ms_observed"],
        "service_ms_requested": service_ms_requested,
        "chunks": args.chunks,
        "chunk_delay_ms": args.chunk_delay_ms,
        "work_mode": args.work_mode,
        "retire_mode": args.retire_mode,
        "error": row["error"],
    }


def _run_windowed(engine, args, indices, run_id, workload, record):
    """Hold --concurrency requests in flight, retire one per as-completed sweep."""
    records = []
    inflight = {}  # Future -> (req_id, svc)
    n = len(indices)
    next_i = 0

    def submit_one(idx):
        req_id = f"req-{idx:06d}"
        svc = _service_for(idx, args)
        fut = engine.submit(service_ms=svc, work_mode=args.work_mode,
                            chunks=args.chunks, chunk_delay_ms=args.chunk_delay_ms)
        inflight[fut] = (req_id, svc)

    while next_i < n and len(inflight) < args.concurrency:
        submit_one(indices[next_i])
        next_i += 1
    while inflight:
        ready, _pending = engine.wait(list(inflight.keys()), num_returns=1)
        for fut in ready:
            req_id, svc = inflight.pop(fut)
            row = fut.result()
            if record:
                records.append(_make_record(args, run_id, workload, req_id,
                                            row, svc))
            if next_i < n:
                submit_one(indices[next_i])
                next_i += 1
    return records


def _run_submit_all(engine, args, indices, run_id, workload, record):
    """Submit every request, then retire all with one Engine.get(list)."""
    futs = []
    meta = []
    for idx in indices:
        req_id = f"req-{idx:06d}"
        svc = _service_for(idx, args)
        futs.append(engine.submit(service_ms=svc, work_mode=args.work_mode,
                                  chunks=args.chunks,
                                  chunk_delay_ms=args.chunk_delay_ms))
        meta.append((req_id, svc))
    rows = engine.get(futs)
    records = []
    if record:
        for (req_id, svc), row in zip(meta, rows):
            records.append(_make_record(args, run_id, workload, req_id, row, svc))
    return records


def _dispatch(engine, args, indices, run_id, workload, record):
    if args.retire_mode == "one_by_one":
        return _run_windowed(engine, args, indices, run_id, workload, record)
    if args.retire_mode == "submit_all_get_all":
        return _run_submit_all(engine, args, indices, run_id, workload, record)
    raise ValueError(f"unknown retire_mode: {args.retire_mode!r}")


def run(args):
    suffix = "submit_all" if args.retire_mode == "submit_all_get_all" \
        else f"c{args.concurrency}"
    if args.workload:
        workload = args.workload
    elif args.service_pattern == "bimodal":
        p = int(round(args.service_p_high * 100))
        workload = (f"bimodal_lo{_fmt_ms(args.service_low)}"
                    f"_hi{_fmt_ms(args.service_high)}_p{p}_{suffix}")
    else:
        workload = _default_workload(args.service_ms, suffix)
    run_id = _gen_run_id()

    engine = rayx.Engine(num_lanes=args.num_lanes, hpx_threads=args.hpx_threads,
                         lane_impl=args.lane_impl,
                         max_queue_depth_per_lane=args.max_queue_depth_per_lane)
    try:
        if args.warmup_requests > 0:
            _dispatch(engine, args, list(range(args.warmup_requests)),
                      run_id, workload, record=False)
        wall_start = time.perf_counter_ns()
        records = _dispatch(engine, args, list(range(args.requests)),
                            run_id, workload, record=True)
        wall_end = time.perf_counter_ns()
    finally:
        engine.shutdown()

    records.sort(key=lambda r: r["request_id"])

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    wall_s = (wall_end - wall_start) / 1e9
    completed = sum(1 for r in records if r["status"] == "completed")
    print(
        f"[run_rayx_baseline] run_id={run_id} workload={workload} "
        f"boundary={BOUNDARY} retire_mode={args.retire_mode} "
        f"warmup={args.warmup_requests} lane_impl={args.lane_impl}"
    )
    print(
        f"[run_rayx_baseline] requests={len(records)} completed={completed} "
        f"wall_s={wall_s:.3f} out={args.out}"
    )


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Standalone RayX Engine baseline driver.")
    p.add_argument("--work-mode", default="sleep", choices=["sleep"])
    p.add_argument("--service-ms", type=float, default=0)
    p.add_argument("--chunks", type=int, default=1)
    p.add_argument("--chunk-delay-ms", type=float, default=0.0)
    p.add_argument("--service-pattern", default="fixed",
                   choices=["fixed", "bimodal"])
    p.add_argument("--service-low", type=float, default=1.0)
    p.add_argument("--service-high", type=float, default=20.0)
    p.add_argument("--service-p-high", type=float, default=0.1)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--requests", type=int, default=500)
    p.add_argument("--warmup-requests", type=int, default=0)
    p.add_argument("--retire-mode", default="one_by_one",
                   choices=["one_by_one", "submit_all_get_all"])
    p.add_argument("--num-lanes", type=int, default=1)
    p.add_argument("--hpx-threads", type=int, default=1)
    p.add_argument("--lane-impl", default="std", choices=["std", "hpx"])
    p.add_argument("--max-queue-depth-per-lane", type=int, default=None)
    p.add_argument("--workload", default=None)
    p.add_argument("--out", required=True, help="Output JSONL path.")
    args = p.parse_args(argv)
    if args.num_lanes < 1:
        p.error("--num-lanes must be >= 1")
    if args.hpx_threads < 1:
        p.error("--hpx-threads must be >= 1")
    if args.chunks < 1:
        p.error("--chunks must be >= 1")
    if args.chunk_delay_ms < 0:
        p.error("--chunk-delay-ms must be >= 0")
    if args.max_queue_depth_per_lane is not None and \
            args.max_queue_depth_per_lane < 1:
        p.error("--max-queue-depth-per-lane must be >= 1 when set")
    if args.service_pattern == "bimodal":
        if args.service_low < 0 or args.service_high < 0:
            p.error("--service-low and --service-high must be >= 0")
        if not 0.0 <= args.service_p_high <= 1.0:
            p.error("--service-p-high must be in [0, 1]")
    return args


if __name__ == "__main__":
    run(parse_args())
