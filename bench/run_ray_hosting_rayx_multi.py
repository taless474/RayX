"""Multi-actor oversubscription characterization for Ray-hosted rayx.Engine.

The systems question this answers: when N Ray actors each host one rayx.Engine
with `hpx_threads=T`, how should Ray's per-actor `num_cpus` reservation be sized,
and what happens when it is mismatched? Ray's scheduler accounts only for the
`num_cpus` you RESERVE per actor -- it cannot see how many HPX worker threads an
Engine spawns -- so two distinct mismatches exist:

  * UNDER-RESERVE (num_cpus_per_actor < hpx_threads): Ray places the actors (its
    accounting is satisfied) but the process can run more threads than the CPU
    budget Ray handed out. Whether that becomes real core contention depends on
    how many CPU-bound lanes are *actively* running -- ~num_lanes per Engine for
    the std/ServiceLane backend, bounded by hpx_threads for the hpx backend --
    NOT on the HPX worker-pool size alone; idle HPX workers do not contend.
    hpx_threads is the worker-pool size; it bounds active CPU demand for the hpx
    backend, but for the std/ServiceLane backend active CPU-bound demand is driven
    by concurrently-active lanes and can exceed hpx_threads when
    num_lanes > hpx_threads. The binding
    condition is (Sum of active CPU-bound lanes) > physical cores;
    `work_mode="spin"` is the diagnostic that makes it visible.
  * OVER-RESERVE (num_actors * num_cpus_per_actor > num_cpus): Ray refuses to
    place all actors -> they sit PENDING. Guarded here by --actor-ready-timeout
    so it reports cleanly instead of hanging.

This is a RESOURCE-BUDGET characterization, not a Ray benchmark, not a serving
recommendation, and not an "HPX beats Ray" claim. The N-actors-round-robin shape
is a Ray-pattern resource diagnostic, NOT HPX-native programming guidance (an
idiomatic HPX design would coordinate cores in ONE runtime via resource
partitioning / executors rather than N independent per-process runtimes -- see
the experiment note). `work_mode="spin"` is a synthetic CPU-bound DIAGNOSTIC (it
contends for cores); `work_mode="sleep"` parks and will NOT reveal core
oversubscription. All magnitudes are machine-specific. See
experiments/29_ray_hosting_rayx_multi_oversubscription/.

Separate from the single-actor driver (bench/run_ray_hosting_rayx.py) on purpose:
it reuses that driver's `RayxHostActor`, v1 `_make_record`, and schema constants
unchanged, and adds only the multi-actor round-robin + per-actor reservation +
placement guard here.

Example:
  python bench/run_ray_hosting_rayx_multi.py --num-actors 2 --hpx-threads 2 \\
      --num-cpus-per-actor 1 --num-cpus 2 --work-mode spin --service-ms 5 \\
      --requests 40 --warmup-requests 8 --concurrency 4 --out results/oversub.jsonl
"""

import argparse
import json
import os
import sys
import time

os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (_REPO_ROOT, _BENCH_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ray  # noqa: E402

# Reuse the single-actor driver's actor, schema constants, row builder, and the
# rayx src path -- this driver changes nothing about how an actor hosts an Engine.
from run_ray_hosting_rayx import (  # noqa: E402
    RayxHostActor,
    _RAYX_SRC,
    _make_record,
    _fmt_ms,
    SCHEMA_VERSION,  # noqa: F401  (re-exported context; _make_record uses them)
    BACKEND,
    BOUNDARY,
)
from service_sequence import service_for as _service_for  # noqa: E402


def _gen_run_id():
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"rayxhostmulti-{stamp}-{os.getpid():x}"


def _workload(args):
    if args.workload:
        return args.workload
    base = "noop" if args.service_ms == 0 else f"{args.work_mode}_{_fmt_ms(args.service_ms)}ms"
    return (f"{base}_a{args.num_actors}_t{args.hpx_threads}"
            f"_cpa{args.num_cpus_per_actor}_c{args.num_cpus}")


def _submit(actors, actor_ids, rr, args, idx, inflight):
    """Submit request `idx` to the next actor in round-robin order."""
    req_id = f"req-{idx:06d}"
    svc = _service_for(idx, args)
    req = {
        "request_id": req_id,
        "service_ms": svc,
        "work_mode": args.work_mode,
        "chunks": args.chunks,
        "chunk_delay_ms": args.chunk_delay_ms,
    }
    k = rr[0] % len(actors)
    rr[0] += 1
    submit_ns = time.perf_counter_ns()
    ref = actors[k].serve.remote(req)
    inflight[ref] = (req_id, submit_ns, svc, actor_ids[k])


def _run_windowed(actors, actor_ids, args, indices, run_id, workload, record):
    """Hold --concurrency requests in flight across actors, retire one per wait."""
    records = []
    inflight = {}
    rr = [0]
    n = len(indices)
    next_i = 0
    while next_i < n and len(inflight) < args.concurrency:
        _submit(actors, actor_ids, rr, args, indices[next_i], inflight)
        next_i += 1
    while inflight:
        done, _ = ray.wait(list(inflight.keys()), num_returns=1)
        results = ray.get(done)
        recv_ns = time.perf_counter_ns()
        for ref, result in zip(done, results):
            req_id, submit_ns, svc, ray_actor_id = inflight.pop(ref)
            if record:
                records.append(_make_record(
                    args, run_id, workload, ray_actor_id, req_id, submit_ns,
                    result, recv_ns, svc))
        for _ in range(len(done)):
            if next_i < n:
                _submit(actors, actor_ids, rr, args, indices[next_i], inflight)
                next_i += 1
    return records


def _run_submit_all(actors, actor_ids, args, indices, run_id, workload, record):
    """Submit every request round-robin, then retire all with one ray.get."""
    refs = []
    meta = {}
    rr = [0]
    for idx in indices:
        one = {}
        _submit(actors, actor_ids, rr, args, idx, one)
        (ref, tup), = one.items()
        refs.append(ref)
        meta[ref] = tup
    results = ray.get(refs)
    recv_ns = time.perf_counter_ns()
    records = []
    if record:
        for ref, result in zip(refs, results):
            req_id, submit_ns, svc, ray_actor_id = meta[ref]
            records.append(_make_record(
                args, run_id, workload, ray_actor_id, req_id, submit_ns,
                result, recv_ns, svc))
    return records


def _dispatch(actors, actor_ids, args, indices, run_id, workload, record):
    if args.retire_mode == "one_by_one":
        return _run_windowed(actors, actor_ids, args, indices, run_id, workload,
                             record)
    if args.retire_mode == "submit_all_get_all":
        return _run_submit_all(actors, actor_ids, args, indices, run_id, workload,
                              record)
    raise ValueError(f"unknown retire_mode: {args.retire_mode!r}")


def run(args):
    workload = _workload(args)
    run_id = _gen_run_id()
    total_reserved = args.num_actors * args.num_cpus_per_actor
    total_hpx_threads = args.num_actors * args.hpx_threads
    active_lanes_total = args.num_actors * args.num_lanes
    cores = os.cpu_count() or 0
    placement_refusal_expected = total_reserved > args.num_cpus
    # Accounting flag: per-actor HPX pool exceeds its Ray reservation (the
    # under-reserve condition). It is NOT a contention claim -- idle workers do
    # not contend.
    hpx_pool_gt_reserved = args.hpx_threads > args.num_cpus_per_actor
    # The operationally binding condition for CPU-bound (spin) work: concurrently
    # active lanes (~num_lanes per Engine for std) exceeding physical cores. Ray
    # num_cpus is logical accounting, not a core cap, so this can hold even with
    # aligned reservations.
    active_lanes_gt_cores = cores > 0 and active_lanes_total > cores

    ray.init(num_cpus=args.num_cpus, include_dashboard=False,
             ignore_reinit_error=True, logging_level="error")
    try:
        # Each actor reserves num_cpus_per_actor and hosts ONE Engine of
        # hpx_threads workers. Reservation is the only Ray-visible budget; the
        # hpx_threads spawned inside are invisible to Ray's scheduler.
        actors = [
            RayxHostActor.options(num_cpus=args.num_cpus_per_actor).remote(
                _RAYX_SRC, args.num_lanes, args.hpx_threads, args.lane_impl, None)
            for _ in range(args.num_actors)
        ]

        # Placement guard: if total_reserved > num_cpus, Ray cannot place all
        # actors and ids() never runs -> report starvation instead of hanging.
        t0 = time.perf_counter_ns()
        try:
            id_pairs = ray.get([a.ids.remote() for a in actors],
                               timeout=args.actor_ready_timeout)
        except ray.exceptions.GetTimeoutError:
            print(
                f"[run_ray_hosting_rayx_multi] PLACEMENT-STARVATION: "
                f"{args.num_actors} actors x num_cpus_per_actor="
                f"{args.num_cpus_per_actor} = {total_reserved} reserved CPUs > "
                f"num_cpus={args.num_cpus}; Ray left actor(s) PENDING past "
                f"--actor-ready-timeout={args.actor_ready_timeout}s. No JSONL "
                f"written. (This is the over-reserve refusal case, separate from "
                f"under-reserve latency.)"
            )
            return
        ready_wall_ms = (time.perf_counter_ns() - t0) / 1e6
        actor_ids = [p[0] for p in id_pairs]
        lane_ids = [p[1] for p in id_pairs]

        if args.warmup_requests > 0:
            _dispatch(actors, actor_ids, args, list(range(args.warmup_requests)),
                      run_id, workload, record=False)

        wall_start = time.perf_counter_ns()
        records = _dispatch(actors, actor_ids, args, list(range(args.requests)),
                            run_id, workload, record=True)
        wall_end = time.perf_counter_ns()

        # Explicit graceful-drain of every hosted Engine before release.
        shutdown_ok = all(ray.get([a.shutdown.remote() for a in actors]))
    finally:
        ray.shutdown()

    records.sort(key=lambda r: r["request_id"])
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    wall_s = (wall_end - wall_start) / 1e9
    completed = sum(1 for r in records if r["status"] == "completed")
    # Per-actor completed counts (by outer Ray actor id) to expose any starved
    # actor; analyze_jsonl.py reports the aggregate, this is the fairness view.
    per_actor = {aid: 0 for aid in actor_ids}
    for r in records:
        if r["status"] == "completed":
            per_actor[r["worker_id"]] = per_actor.get(r["worker_id"], 0) + 1

    print(
        f"[run_ray_hosting_rayx_multi] run_id={run_id} workload={workload} "
        f"boundary={BOUNDARY}"
    )
    print(
        f"[run_ray_hosting_rayx_multi] CONFIG num_actors={args.num_actors} "
        f"hpx_threads={args.hpx_threads} num_cpus_per_actor="
        f"{args.num_cpus_per_actor} num_cpus={args.num_cpus} "
        f"work_mode={args.work_mode} reserved_cpus={total_reserved} "
        f"hpx_threads_total={total_hpx_threads} "
        f"active_lanes_total={active_lanes_total} cores={cores} "
        f"hpx_pool_gt_reserved={hpx_pool_gt_reserved} "
        f"active_lanes_gt_cores={active_lanes_gt_cores} "
        f"placement_refusal_expected={placement_refusal_expected}"
    )
    print(
        f"[run_ray_hosting_rayx_multi] requests={len(records)} "
        f"completed={completed} wall_s={wall_s:.3f} ready_wall_ms="
        f"{ready_wall_ms:.1f} shutdown_ok={shutdown_ok} out={args.out}"
    )
    print(
        f"[run_ray_hosting_rayx_multi] per_actor_completed="
        f"{ {aid: per_actor[aid] for aid in actor_ids} } "
        f"lane_ids={lane_ids}"
    )


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Multi-actor oversubscription characterization for "
                    "Ray-hosted rayx.Engine (resource-budget diagnostic).")
    p.add_argument("--num-actors", type=int, default=2,
                   help="Number of Ray actors, each hosting one rayx.Engine.")
    p.add_argument("--hpx-threads", type=int, default=1,
                   help="HPX worker threads per hosted Engine (T).")
    p.add_argument("--num-cpus-per-actor", type=int, default=None,
                   help="Ray num_cpus reserved per actor (R). Default = "
                        "max(hpx_threads, num_lanes). For std/ServiceLane lanes "
                        "num_lanes is the active CPU-bound demand driver (each "
                        "lane is a std::thread); for hpx/HpxLane hpx_threads is "
                        "the active worker-pool bound. max(hpx_threads, "
                        "num_lanes) is a conservative default for this diagnostic "
                        "driver, NOT a universal production rule.")
    p.add_argument("--num-cpus", type=int, default=None,
                   help="Total ray.init num_cpus (C). Default = "
                        "num_actors * num_cpus_per_actor (aligned).")
    p.add_argument("--num-lanes", type=int, default=1,
                   help="Lanes per hosted Engine. Default 1.")
    p.add_argument("--work-mode", default="spin", choices=["spin", "sleep"],
                   help="spin (default) is a CPU-bound DIAGNOSTIC that reveals "
                        "core oversubscription; sleep parks and will NOT.")
    p.add_argument("--service-ms", type=float, default=5,
                   help="Service time per request in ms (fixed pattern).")
    p.add_argument("--service-pattern", default="fixed", choices=["fixed"],
                   help="Per-request service model (fixed only for this driver).")
    p.add_argument("--seed", type=int, default=0,
                   help="Unused for fixed pattern; kept for service_for parity.")
    p.add_argument("--requests", type=int, default=200,
                   help="TOTAL requests across all actors (round-robined).")
    p.add_argument("--concurrency", type=int, default=8,
                   help="Target in-flight requests (one_by_one retire).")
    p.add_argument("--warmup-requests", type=int, default=20,
                   help="Requests run before measurement; not written.")
    p.add_argument("--retire-mode", default="one_by_one",
                   choices=["one_by_one", "submit_all_get_all"])
    p.add_argument("--lane-impl", default="std", choices=["std", "hpx"])
    p.add_argument("--actor-ready-timeout", type=float, default=20.0,
                   help="Seconds to wait for all actors to become ready before "
                        "reporting a Ray placement-starvation (over-reserve) "
                        "case instead of hanging.")
    p.add_argument("--workload", default=None,
                   help="Workload name (auto-derived if omitted).")
    p.add_argument("--out", required=True, help="Output JSONL path.")
    # Fixed echoes consumed by the reused _make_record (this driver is unchunked).
    args = p.parse_args(argv)
    args.chunks = 1
    args.chunk_delay_ms = 0.0
    # Default reservation: max(hpx_threads, num_lanes). This is safe for the
    # default std/ServiceLane backend, where active CPU-bound demand is driven by
    # num_lanes (each lane is a std::thread) and can exceed hpx_threads; the max
    # also covers the hpx backend's worker pool. Conservative diagnostic default,
    # not a production rule. num_cpus then defaults to the aligned total.
    if args.num_cpus_per_actor is None:
        args.num_cpus_per_actor = max(args.hpx_threads, args.num_lanes)
    if args.num_cpus is None:
        args.num_cpus = args.num_actors * args.num_cpus_per_actor
    if args.num_actors < 1:
        p.error("--num-actors must be >= 1")
    if args.hpx_threads < 1:
        p.error("--hpx-threads must be >= 1")
    if args.num_cpus_per_actor < 1:
        p.error("--num-cpus-per-actor must be >= 1")
    if args.num_cpus < 1:
        p.error("--num-cpus must be >= 1")
    if args.num_lanes < 1:
        p.error("--num-lanes must be >= 1")
    if args.actor_ready_timeout <= 0:
        p.error("--actor-ready-timeout must be > 0")
    return args


if __name__ == "__main__":
    run(parse_args())
