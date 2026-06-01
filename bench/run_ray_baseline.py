"""Driver for the Ray actor baseline.

Drives N synthetic requests through a single Ray actor, then writes one JSONL
row per request matching the per-request schema in docs/experiment_plan.md
(plus an optional backward-compatible `retire_mode` metadata field).

Boundary measured: ray-actor-process (Python + Ray runtime + process + IPC +
serialization). total_ms is measured client-side and is authoritative for
round-trip latency. queue_wait_ms is approximated as total_ms - service_ms
to avoid comparing client and actor clocks across processes.

Retire modes (how the client retires in-flight requests):
  * one_by_one        : ray.wait(num_returns=1), retire one, submit one.
  * batch_wait        : ray.wait(num_returns=k), retire up to k, refill.
  * submit_all_get_all: submit all refs, then a single ray.get(refs).
                        Ignores --concurrency; per-request total_ms is shaped
                        by queue position / bulk completion, so throughput is
                        the meaningful metric for this mode, not latency.

Example:
  python bench/run_ray_baseline.py --service-ms 0 --concurrency 8 \\
      --requests 200 --warmup-requests 20 --retire-mode batch_wait \\
      --wait-batch 8 --out results/noop_batch_wait.jsonl
"""

import argparse
import json
import os
import sys
import time
import uuid

# Quiet a benign Ray FutureWarning about accelerator env vars when num_gpus=0.
os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

# Make the repo root importable so "ray_impl" resolves regardless of CWD, and
# the bench/ dir importable so the shared service-sequence helper resolves.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
_BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
if _BENCH_DIR not in sys.path:
    sys.path.insert(0, _BENCH_DIR)

import ray  # noqa: E402

from ray_impl.ray_actor_baseline import (  # noqa: E402
    BACKEND,
    BOUNDARY,
    SCHEMA_VERSION,
    SyntheticServer,
    make_request,
)
# Canonical deterministic service sequence, shared with run_hpx_python_baseline
# and mirrored by the native C++ service_for (keeps the cross-engine comparison
# fair). See bench/service_sequence.py.
from service_sequence import service_for as _service_for  # noqa: E402


def _gen_run_id():
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"ray-{stamp}-{uuid.uuid4().hex[:6]}"


def _fmt_ms(service_ms):
    # Render whole numbers without a trailing ".0" for clean workload names.
    if float(service_ms).is_integer():
        return str(int(service_ms))
    return str(service_ms)


def _default_workload(service_ms, suffix):
    if service_ms == 0:
        return f"noop_{suffix}"
    return f"sleep_{_fmt_ms(service_ms)}ms_{suffix}"


def _make_record(args, run_id, workload, retire_mode, req_id, submit_ns,
                 result, recv_ns, service_ms_requested):
    total_ms = (recv_ns - submit_ns) / 1e6
    service_ms_observed = result["service_ms_observed"]
    queue_wait_ms = max(total_ms - service_ms_observed, 0.0)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "backend": BACKEND,
        "boundary": BOUNDARY,
        "workload": workload,
        "request_id": req_id,
        "actor_id": result["actor_id"],
        "worker_id": None,
        "status": result["status"],
        "submit_ns": submit_ns,
        "start_ns": result["start_ns"],
        "end_ns": result["end_ns"],
        "total_ms": total_ms,
        "queue_wait_ms": queue_wait_ms,
        "service_ms_observed": service_ms_observed,
        "service_ms_requested": service_ms_requested,
        "chunks": 1,
        "chunk_delay_ms": 0,
        "work_mode": args.work_mode,
        "retire_mode": retire_mode,
        "error": result["error"],
    }


def _submit(actors, rr, args, idx, inflight):
    """Submit request `idx` to the next actor in round-robin order.

    `rr` is a single-element list used as a mutable counter so submission order
    maps deterministically to actors[k % N]. With one actor this routes every
    request to actors[0], reproducing the original single-actor behavior.
    """
    req_id = f"req-{idx:06d}"
    svc = _service_for(idx, args)
    req = make_request(req_id, svc, work_mode=args.work_mode)
    actor = actors[rr[0] % len(actors)]
    rr[0] += 1
    submit_ns = time.perf_counter_ns()
    ref = actor.serve.remote(req)
    inflight[ref] = (req_id, submit_ns, svc)


def _run_windowed(actors, args, indices, k, run_id, workload, retire_mode,
                  record):
    """Windowed retirement: hold --concurrency in flight, retire k per wait.

    k == 1 reproduces the original one_by_one behavior exactly.
    """
    records = []
    inflight = {}
    rr = [0]
    n = len(indices)
    next_i = 0
    while next_i < n and len(inflight) < args.concurrency:
        _submit(actors, rr, args, indices[next_i], inflight)
        next_i += 1
    while inflight:
        num = min(k, len(inflight))
        done, _ = ray.wait(list(inflight.keys()), num_returns=num)
        results = ray.get(done)
        recv_ns = time.perf_counter_ns()
        for ref, result in zip(done, results):
            req_id, submit_ns, svc = inflight.pop(ref)
            if record:
                records.append(_make_record(
                    args, run_id, workload, retire_mode, req_id, submit_ns,
                    result, recv_ns, svc))
        for _ in range(len(done)):
            if next_i < n:
                _submit(actors, rr, args, indices[next_i], inflight)
                next_i += 1
    return records


def _run_submit_all(actors, args, indices, run_id, workload, retire_mode,
                    record):
    """Submit every request, then retire all with a single ray.get(refs)."""
    refs = []
    meta = {}
    rr = [0]
    for idx in indices:
        inflight = {}
        _submit(actors, rr, args, idx, inflight)
        (ref, (req_id, submit_ns, svc)), = inflight.items()
        refs.append(ref)
        meta[ref] = (req_id, submit_ns, svc)
    results = ray.get(refs)
    recv_ns = time.perf_counter_ns()
    records = []
    if record:
        for ref, result in zip(refs, results):
            req_id, submit_ns, svc = meta[ref]
            records.append(_make_record(
                args, run_id, workload, retire_mode, req_id, submit_ns,
                result, recv_ns, svc))
    return records


def _dispatch(actors, args, indices, run_id, workload, record):
    mode = args.retire_mode
    if mode == "one_by_one":
        return _run_windowed(actors, args, indices, 1, run_id, workload, mode,
                             record)
    if mode == "batch_wait":
        return _run_windowed(actors, args, indices, max(1, args.wait_batch),
                             run_id, workload, mode, record)
    if mode == "submit_all_get_all":
        return _run_submit_all(actors, args, indices, run_id, workload, mode,
                              record)
    raise ValueError(f"unknown retire_mode: {mode!r}")


def run(args):
    # submit_all_get_all does not enforce an in-flight window, so label it by
    # mode rather than a concurrency value.
    suffix = "submit_all" if args.retire_mode == "submit_all_get_all" \
        else f"c{args.concurrency}"
    # Encode actor count in the workload name when scaling past one actor, so
    # multi-actor runs stay distinguishable. N==1 keeps the original names.
    if args.num_actors > 1:
        suffix = f"{suffix}_a{args.num_actors}"
    if args.workload:
        workload = args.workload
    elif args.service_pattern == "bimodal":
        p = int(round(args.service_p_high * 100))
        workload = (f"bimodal_lo{_fmt_ms(args.service_low)}"
                    f"_hi{_fmt_ms(args.service_high)}_p{p}_{suffix}")
    else:
        workload = _default_workload(args.service_ms, suffix)
    run_id = _gen_run_id()

    ray.init(
        num_cpus=args.num_cpus,
        include_dashboard=False,
        ignore_reinit_error=True,
        logging_level="error",
    )
    try:
        actors = [SyntheticServer.remote() for _ in range(args.num_actors)]
        actor_ids = ray.get([a.get_actor_id.remote() for a in actors])

        # Warmup: run requests through the same path, discard their records
        # and exclude them from wall time.
        if args.warmup_requests > 0:
            _dispatch(actors, args, list(range(args.warmup_requests)),
                      run_id, workload, record=False)

        wall_start = time.perf_counter_ns()
        records = _dispatch(actors, args, list(range(args.requests)),
                            run_id, workload, record=True)
        wall_end = time.perf_counter_ns()
    finally:
        ray.shutdown()

    records.sort(key=lambda r: r["request_id"])

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    wall_s = (wall_end - wall_start) / 1e9
    completed = sum(1 for r in records if r["status"] == "completed")
    print(
        f"[run_ray_baseline] run_id={run_id} workload={workload} "
        f"boundary={BOUNDARY} retire_mode={args.retire_mode} "
        f"warmup={args.warmup_requests} num_actors={args.num_actors} "
        f"actor_ids={actor_ids}"
    )
    print(
        f"[run_ray_baseline] requests={len(records)} completed={completed} "
        f"wall_s={wall_s:.3f} out={args.out}"
    )


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Ray actor baseline driver.")
    p.add_argument("--work-mode", default="sleep", choices=["sleep"],
                   help="Synthetic backend work mode (only 'sleep' for now).")
    p.add_argument("--service-ms", type=float, default=0,
                   help="Service time per request in ms (0 == no-op). Used by "
                        "--service-pattern fixed.")
    p.add_argument("--service-pattern", default="fixed",
                   choices=["fixed", "bimodal"],
                   help="Per-request service-time model. 'fixed' uses "
                        "--service-ms for every request. 'bimodal' draws "
                        "service_high with probability --service-p-high else "
                        "service_low, deterministically per request index "
                        "(--seed); identical sequence across Ray/HPX/rayx.")
    p.add_argument("--service-low", type=float, default=1.0,
                   help="bimodal: low service time in ms. Default 1.0.")
    p.add_argument("--service-high", type=float, default=20.0,
                   help="bimodal: high service time in ms. Default 20.0.")
    p.add_argument("--service-p-high", type=float, default=0.1,
                   help="bimodal: probability of the high service time. "
                        "Default 0.1.")
    p.add_argument("--seed", type=int, default=0,
                   help="bimodal: seed for the deterministic per-index draw. "
                        "Default 0.")
    p.add_argument("--concurrency", type=int, default=8,
                   help="Target in-flight requests (ignored by "
                        "submit_all_get_all).")
    p.add_argument("--num-actors", type=int, default=1,
                   help="Number of Ray actors (serving lanes); requests are "
                        "round-robined across them. Default 1.")
    p.add_argument("--requests", type=int, default=500,
                   help="Total number of requests to issue (measured).")
    p.add_argument("--warmup-requests", type=int, default=0,
                   help="Requests run before measurement; not written to "
                        "JSONL and excluded from wall time.")
    p.add_argument("--retire-mode", default="one_by_one",
                   choices=["one_by_one", "batch_wait", "submit_all_get_all"],
                   help="How the client retires in-flight requests.")
    p.add_argument("--wait-batch", type=int, default=8,
                   help="num_returns for ray.wait in batch_wait mode.")
    p.add_argument("--num-cpus", type=int, default=4,
                   help="num_cpus passed to ray.init for determinism.")
    p.add_argument("--workload", default=None,
                   help="Workload name (auto-derived if omitted).")
    p.add_argument("--out", required=True, help="Output JSONL path.")
    args = p.parse_args(argv)
    if args.num_actors < 1:
        p.error("--num-actors must be >= 1")
    if args.service_pattern == "bimodal":
        if args.service_low < 0 or args.service_high < 0:
            p.error("--service-low and --service-high must be >= 0")
        if not 0.0 <= args.service_p_high <= 1.0:
            p.error("--service-p-high must be in [0, 1]")
    return args


if __name__ == "__main__":
    run(parse_args())
