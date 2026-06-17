"""Driver for the "Ray actor hosting RayX" composition prototype.

A single long-lived Ray actor constructs exactly ONE HPX-backed ``rayx.Engine``
in ``__init__`` and serves synthetic requests through it locally. Ray owns only
the outer actor placement/lifecycle; RayX owns the fine-grained local serving.
The composition is deliberately one-directional and narrow:

  * The actor submits to its local Engine and retires the RayX future INSIDE the
    actor process. Only a plain JSON-serializable row crosses the Ray boundary.
  * No RayX Future ever crosses the Ray boundary; there is no ``ray.get``
    wrapper over a RayX future, no object-store emulation, and no Ray
    compatibility layer. RayX does not grow any Ray semantics here.

Why one Engine per actor: the HPX runtime is a process-global singleton (see
python/src/rayx/_rayx.cpp: a single ``process_runtime_active()`` guard). A Ray
actor is its own worker process, so "one RayX Engine per long-lived Ray actor"
is the only viable shape. ``RayxHostActor.try_second_engine`` demonstrates that
a second Engine in the same process is rejected.

Boundary measured: ray-actor-rayx-engine (Ray client + Ray runtime + process +
IPC + serialization, THEN a local RayX/HPX serving lane inside the actor).
total_ms is measured client-side and is authoritative for round-trip latency;
queue_wait_ms is approximated as total_ms - service_ms_observed to avoid
comparing client and actor-process clocks. The row schema is v1-compatible with
bench/run_ray_baseline.py so the JSONL flows through bench/analyze_jsonl.py and
is directly comparable to the pure Ray actor baseline and standalone RayX.

This is a composition feasibility prototype, NOT a performance claim: hosting
RayX inside a Ray actor adds the Ray actor-call boundary ON TOP of RayX's local
serving, so it is expected to be slower than standalone RayX, not faster.

Example:
  python bench/run_ray_hosting_rayx.py --service-ms 0 --concurrency 8 \\
      --requests 200 --warmup-requests 20 --out results/rayx_hosted_noop.jsonl
"""

import argparse
import json
import os
import sys
import time
import uuid

# Quiet a benign Ray FutureWarning about accelerator env vars when num_gpus=0.
os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

# Make the repo root importable so "bench" helpers resolve regardless of CWD,
# and compute the absolute rayx source dir to hand to the actor (the Ray worker
# is a separate process; it needs python/src on sys.path to import rayx). Local
# single-node Ray only: the worker shares this machine's filesystem.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
_BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (_REPO_ROOT, _BENCH_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ray  # noqa: E402

# Canonical deterministic service sequence, shared with run_ray_baseline and
# run_hpx_python_baseline (and mirrored by the native service_for) so the
# cross-engine comparison is fair. See bench/service_sequence.py.
from service_sequence import service_for as _service_for  # noqa: E402

# v1 schema constants. backend/boundary label this leg distinctly from the pure
# Ray actor baseline (backend="synthetic", boundary="ray-actor-process") while
# keeping the SAME row fields, so all three legs share one analyzer path.
SCHEMA_VERSION = "1"
BACKEND = "rayx"
BOUNDARY = "ray-actor-rayx-engine"


@ray.remote
class RayxHostActor:
    """A long-lived Ray actor hosting exactly one HPX-backed rayx.Engine.

    Constructed once; serves requests through its local Engine; retires each
    RayX future internally and returns only plain dicts. The Engine is shut
    down via the explicit shutdown() method (graceful drain) before the actor
    is released.
    """

    def __init__(self, rayx_src, num_lanes, hpx_threads, lane_impl,
                 max_queue_depth_per_lane):
        # The Ray worker is a separate process: put rayx on its path, then
        # import and construct the ONE Engine this actor will host.
        if rayx_src not in sys.path:
            sys.path.insert(0, rayx_src)
        import rayx  # noqa: F401  (imported here: this runs in the Ray worker)

        self._rayx = rayx
        self.ray_actor_id = "ray-" + uuid.uuid4().hex[:8]
        self.engine = rayx.Engine(
            num_lanes=num_lanes,
            hpx_threads=hpx_threads,
            lane_impl=lane_impl,
            max_queue_depth_per_lane=max_queue_depth_per_lane,
        )

    def ids(self):
        """Return (ray_actor_id, rayx_lane_actor_id).

        The rayx lane id (act-hpx-... / act-hpxl-...) is the inner serving
        identity recorded per row; the Ray actor id is the outer placement
        identity. Both are surfaced so a row can be traced to both layers.
        """
        lane = self.engine.lane_stats()
        lane_id = lane[0]["actor_id"] if lane else None
        return self.ray_actor_id, lane_id

    def serve(self, request):
        """Service one request on the local Engine; return a plain row.

        Submits to the hosted Engine, retires the RayX future HERE (inside the
        actor), and returns only the JSON-serializable inner-timing fields the
        driver needs. No RayX Future crosses the Ray boundary.
        """
        fut = self.engine.submit(
            service_ms=request["service_ms"],
            work_mode=request.get("work_mode", "sleep"),
            chunks=request.get("chunks", 1),
            chunk_delay_ms=request.get("chunk_delay_ms", 0.0),
        )
        row = fut.result()  # retire INSIDE the actor; row is a plain dict
        return {
            "actor_id": row["actor_id"],          # inner rayx serving-lane id
            "start_ns": row["start_ns"],          # actor-process clock
            "end_ns": row["end_ns"],              # actor-process clock
            "service_ms_observed": row["service_ms_observed"],
            "status": row["status"],
            "error": row["error"],
            "chunks_completed": row["chunks_completed"],
        }

    def try_second_engine(self):
        """Attempt a SECOND rayx.Engine in this process; report the outcome.

        The HPX runtime is a process singleton, so this must be rejected while
        the hosted Engine is active. Returns {"rejected": bool, "error": str}.
        On the (unexpected) success path the stray Engine is shut down so the
        actor is left with exactly its one hosted Engine.
        """
        try:
            stray = self._rayx.Engine(num_lanes=1, hpx_threads=1)
        except Exception as exc:  # noqa: BLE001 - we report any rejection
            return {"rejected": True, "error": repr(exc)}
        stray.shutdown()
        return {"rejected": False, "error": None}

    def shutdown(self):
        """Graceful-drain the hosted Engine. Idempotent-safe for one call."""
        if self.engine is not None:
            self.engine.shutdown()
            self.engine = None
        return True


def _gen_run_id():
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"rayxhost-{stamp}-{uuid.uuid4().hex[:6]}"


def _fmt_ms(service_ms):
    if float(service_ms).is_integer():
        return str(int(service_ms))
    return str(service_ms)


def _default_workload(service_ms, suffix):
    if service_ms == 0:
        return f"noop_{suffix}"
    return f"sleep_{_fmt_ms(service_ms)}ms_{suffix}"


def _make_record(args, run_id, workload, ray_actor_id, req_id, submit_ns,
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
        # actor_id = inner rayx serving lane; worker_id = outer Ray actor handle.
        "actor_id": result["actor_id"],
        "worker_id": ray_actor_id,
        "status": result["status"],
        "submit_ns": submit_ns,
        "start_ns": result["start_ns"],
        "end_ns": result["end_ns"],
        "total_ms": total_ms,
        "queue_wait_ms": queue_wait_ms,
        "service_ms_observed": service_ms_observed,
        "service_ms_requested": service_ms_requested,
        "chunks": args.chunks,
        "chunk_delay_ms": args.chunk_delay_ms,
        "work_mode": args.work_mode,
        "retire_mode": args.retire_mode,
        "error": result["error"],
    }


def _submit(actor, args, idx, inflight):
    req_id = f"req-{idx:06d}"
    svc = _service_for(idx, args)
    req = {
        "request_id": req_id,
        "service_ms": svc,
        "work_mode": args.work_mode,
        "chunks": args.chunks,
        "chunk_delay_ms": args.chunk_delay_ms,
    }
    submit_ns = time.perf_counter_ns()
    ref = actor.serve.remote(req)
    inflight[ref] = (req_id, submit_ns, svc)


def _run_windowed(actor, args, indices, run_id, workload, ray_actor_id, record):
    """Hold --concurrency requests in flight, retire one per ray.wait."""
    records = []
    inflight = {}
    n = len(indices)
    next_i = 0
    while next_i < n and len(inflight) < args.concurrency:
        _submit(actor, args, indices[next_i], inflight)
        next_i += 1
    while inflight:
        done, _ = ray.wait(list(inflight.keys()), num_returns=1)
        results = ray.get(done)
        recv_ns = time.perf_counter_ns()
        for ref, result in zip(done, results):
            req_id, submit_ns, svc = inflight.pop(ref)
            if record:
                records.append(_make_record(
                    args, run_id, workload, ray_actor_id, req_id, submit_ns,
                    result, recv_ns, svc))
        for _ in range(len(done)):
            if next_i < n:
                _submit(actor, args, indices[next_i], inflight)
                next_i += 1
    return records


def _run_submit_all(actor, args, indices, run_id, workload, ray_actor_id,
                    record):
    """Submit every request, then retire all with a single ray.get(refs)."""
    refs = []
    meta = {}
    for idx in indices:
        one = {}
        _submit(actor, args, idx, one)
        (ref, (req_id, submit_ns, svc)), = one.items()
        refs.append(ref)
        meta[ref] = (req_id, submit_ns, svc)
    results = ray.get(refs)
    recv_ns = time.perf_counter_ns()
    records = []
    if record:
        for ref, result in zip(refs, results):
            req_id, submit_ns, svc = meta[ref]
            records.append(_make_record(
                args, run_id, workload, ray_actor_id, req_id, submit_ns,
                result, recv_ns, svc))
    return records


def _dispatch(actor, args, indices, run_id, workload, ray_actor_id, record):
    if args.retire_mode == "one_by_one":
        return _run_windowed(actor, args, indices, run_id, workload,
                             ray_actor_id, record)
    if args.retire_mode == "submit_all_get_all":
        return _run_submit_all(actor, args, indices, run_id, workload,
                              ray_actor_id, record)
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

    ray.init(
        num_cpus=args.num_cpus,
        include_dashboard=False,
        ignore_reinit_error=True,
        logging_level="error",
    )
    try:
        actor = RayxHostActor.remote(
            _RAYX_SRC, args.num_lanes, args.hpx_threads, args.lane_impl,
            args.max_queue_depth_per_lane,
        )
        ray_actor_id, lane_id = ray.get(actor.ids.remote())

        if args.warmup_requests > 0:
            _dispatch(actor, args, list(range(args.warmup_requests)),
                      run_id, workload, ray_actor_id, record=False)

        wall_start = time.perf_counter_ns()
        records = _dispatch(actor, args, list(range(args.requests)),
                            run_id, workload, ray_actor_id, record=True)
        wall_end = time.perf_counter_ns()

        ray.get(actor.shutdown.remote())  # graceful-drain the hosted Engine
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
        f"[run_ray_hosting_rayx] run_id={run_id} workload={workload} "
        f"boundary={BOUNDARY} retire_mode={args.retire_mode} "
        f"warmup={args.warmup_requests} ray_actor_id={ray_actor_id} "
        f"rayx_lane_id={lane_id}"
    )
    print(
        f"[run_ray_hosting_rayx] requests={len(records)} completed={completed} "
        f"wall_s={wall_s:.3f} out={args.out}"
    )


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Ray actor hosting a RayX Engine (composition prototype).")
    p.add_argument("--work-mode", default="sleep", choices=["sleep"],
                   help="Synthetic work mode (only 'sleep' for this slice).")
    p.add_argument("--service-ms", type=float, default=0,
                   help="Service time per request in ms (0 == no-op). Used by "
                        "--service-pattern fixed.")
    p.add_argument("--chunks", type=int, default=1,
                   help="Chunked synthetic service: split TOTAL active "
                        "service_ms into N equal active sleeps (default 1).")
    p.add_argument("--chunk-delay-ms", type=float, default=0.0,
                   help="Parked inter-chunk gap in ms (default 0).")
    p.add_argument("--service-pattern", default="fixed",
                   choices=["fixed", "bimodal"],
                   help="Per-request service-time model (shared sequence with "
                        "the Ray/HPX/rayx drivers).")
    p.add_argument("--service-low", type=float, default=1.0,
                   help="bimodal: low service time in ms. Default 1.0.")
    p.add_argument("--service-high", type=float, default=20.0,
                   help="bimodal: high service time in ms. Default 20.0.")
    p.add_argument("--service-p-high", type=float, default=0.1,
                   help="bimodal: probability of the high service time.")
    p.add_argument("--seed", type=int, default=0,
                   help="bimodal: seed for the deterministic per-index draw.")
    p.add_argument("--concurrency", type=int, default=8,
                   help="Target in-flight requests (one_by_one retire mode).")
    p.add_argument("--requests", type=int, default=500,
                   help="Total number of requests to issue (measured).")
    p.add_argument("--warmup-requests", type=int, default=0,
                   help="Requests run before measurement; not written to JSONL "
                        "and excluded from wall time.")
    p.add_argument("--retire-mode", default="one_by_one",
                   choices=["one_by_one", "submit_all_get_all"],
                   help="How the client retires in-flight requests.")
    p.add_argument("--num-cpus", type=int, default=4,
                   help="num_cpus passed to ray.init for determinism.")
    # Hosted Engine knobs. hpx_threads should be sized against the actor's CPU
    # budget (one Ray actor = one process = one HPX runtime of hpx_threads
    # workers); see the experiments note on the oversubscription caveat.
    p.add_argument("--num-lanes", type=int, default=1,
                   help="Lanes in the hosted rayx.Engine. Default 1.")
    p.add_argument("--hpx-threads", type=int, default=1,
                   help="HPX worker threads in the hosted Engine. Default 1.")
    p.add_argument("--lane-impl", default="std", choices=["std", "hpx"],
                   help="Hosted Engine lane backend ('std' default anchor).")
    p.add_argument("--max-queue-depth-per-lane", type=int, default=None,
                   help="Per-lane bounded admission for the hosted Engine "
                        "(default None == unbounded).")
    p.add_argument("--workload", default=None,
                   help="Workload name (auto-derived if omitted).")
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
