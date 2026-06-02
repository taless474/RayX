"""Driver for the rayx Python frontend over HPX.

Drives N synthetic requests through the rayx Engine (one HPX runtime, N
serialized service lanes), then writes one JSONL row per request matching the
per-request schema in docs/experiment_plan.md (plus the backward-compatible
`retire_mode` metadata field).

Boundary measured: hpx-python-frontend. Python submits native work to in-process
HPX service lanes via the pybind11 `_rayx` extension. There is NO process
boundary, IPC, or serialization (unlike Ray), but there IS a pybind11/GIL
crossing on every submit and result (unlike the native HPX executable, whose
client loop is pure C++). This is the "middle" boundary between:
  * ray-actor-process   (Python + process + IPC + serialization)
  * hpx-intra-locality  (pure in-process C++)

Timing: submit_ns / recv_ns are measured in Python (time.perf_counter_ns),
so total_ms includes the pybind11/GIL crossing in both directions and is the
client-authoritative latency. start_ns / end_ns / service_ms_observed come from
the C++ lane (steady_clock). Because the Python and C++ clocks differ,
queue_wait_ms is APPROXIMATE here (= total_ms - service_ms_observed), like Ray.

Dispatch (--api): engine/actor use windowed one_by_one (hold --concurrency in
flight, retire the oldest, submit one), matching the native HPX executable's
one_by_one path and recorded as retire_mode="one_by_one". batch uses
Engine.submit_batch -- one Python->C++ crossing enqueues ALL requests, so
--concurrency is inert and total_ms is queue-shaped; recorded as
retire_mode="batch".

Service pattern (--service-pattern): 'fixed' uses --service-ms for every
request; 'bimodal' draws --service-high with probability --service-p-high else
--service-low, deterministically per request index (--seed). Each row records
its own service_ms_requested (schema unchanged). bimodal runs on both the
one_by_one paths and the bulk batch path: for --api batch/actor_batch the
generated per-request sequence is passed as a list to the true bulk-varied
submit_batch(service_ms=[...]) (one crossing), not a Python loop over submit().

Example:
  python bench/run_hpx_python_baseline.py --service-ms 5 --concurrency 8 \\
      --num-lanes 2 --hpx-threads 4 --requests 200 --warmup-requests 20 \\
      --out results/hpx_python_sleep5_c8_l2.jsonl
"""

import argparse
import collections
import json
import os
import sys
import threading
import time
import uuid

# Make the repo-local rayx package importable without requiring PYTHONPATH, and
# the bench/ dir importable so the shared service-sequence helper resolves.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_RAYX_SRC = os.path.join(_REPO_ROOT, "python", "src")
if _RAYX_SRC not in sys.path:
    sys.path.insert(0, _RAYX_SRC)
_BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
if _BENCH_DIR not in sys.path:
    sys.path.insert(0, _BENCH_DIR)

# Canonical deterministic service sequence, shared with run_ray_baseline and
# mirrored by the native C++ service_for. See bench/service_sequence.py.
from service_sequence import service_for as _service_for  # noqa: E402

try:
    from rayx import Engine, SyntheticActor  # noqa: E402
except ImportError as exc:  # pragma: no cover - clearer message on missing build
    raise SystemExit(
        "could not import rayx; build the extension first:\n"
        "  cmake -S python -B python/build -G Ninja -DCMAKE_BUILD_TYPE=Release "
        "-DPYBIND11_FINDPYTHON=ON "
        "-DCMAKE_PREFIX_PATH=\"<hpx-install>;$(.venv/bin/python -m pybind11 "
        "--cmakedir)\" -DPython_EXECUTABLE=\"$(pwd)/.venv/bin/python\"\n"
        "  cmake --build python/build\n"
        f"(import error: {exc})"
    )

SCHEMA_VERSION = "1"
BACKEND = "synthetic"
BOUNDARY = "hpx-python-frontend"


def _gen_run_id():
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"hpxpy-{stamp}-{uuid.uuid4().hex[:6]}"


def _fmt_ms(service_ms):
    # Render whole numbers without a trailing ".0" for clean workload names.
    if float(service_ms).is_integer():
        return str(int(service_ms))
    return str(service_ms)


def _default_workload(args):
    suffix = f"c{args.concurrency}"
    if args.num_lanes > 1:
        suffix = f"{suffix}_l{args.num_lanes}"
    # CPU-bound spin and blocking sleep share the workload-naming shape; only the
    # prefix differs ("noop" is the shared service_ms==0 degenerate case).
    prefix = "spin" if args.work_mode == "spin" else "sleep"
    # Variable (bimodal) service: encode the pattern, e.g. bimodal_lo1_hi20_p10_c8.
    # one_by_one keeps the c{concurrency} suffix; batch makes --concurrency inert,
    # so (like fixed batch) it drops c{concurrency} and tags the bulk dispatch.
    if args.service_pattern == "bimodal":
        p = int(round(args.service_p_high * 100))
        base = (f"bimodal_lo{_fmt_ms(args.service_low)}"
                f"_hi{_fmt_ms(args.service_high)}_p{p}")
        if args.api in ("batch", "actor_batch"):
            name = f"{base}_batch"
            if args.num_lanes > 1:
                name = f"{name}_l{args.num_lanes}"
            return name
        return f"{base}_{suffix}"
    # Fixed service. Batch submits all requests in one call: --concurrency is
    # inert, so the name drops the c{concurrency} token and tags the bulk
    # dispatch instead.
    if args.api in ("batch", "actor_batch"):
        base = ("noop" if args.service_ms == 0
                else f"{prefix}_{_fmt_ms(args.service_ms)}ms")
        name = f"{base}_batch"
        if args.num_lanes > 1:
            name = f"{name}_l{args.num_lanes}"
        return name
    if args.service_ms == 0:
        return f"noop_{suffix}"
    return f"{prefix}_{_fmt_ms(args.service_ms)}ms_{suffix}"


def _make_record(args, run_id, workload, req_id, row, retire_mode,
                 service_ms_requested):
    # `row` is the dict returned by rayx Future.result(): it already carries the
    # Python-measured submit_ns/total_ms/queue_wait_ms and the C++-measured
    # start_ns/end_ns/service_ms_observed. We only add run-level fields here.
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
        "retire_mode": retire_mode,
        "error": row["error"],
    }


def _run_one_by_one(submit_fn, args, indices, run_id, workload, retire_mode,
                    record, window=None):
    """Hold `window` requests in flight, retire the oldest (FIFO), submit one.

    `submit_fn` is the API-agnostic submit callable (Engine.submit or
    SyntheticActor.remote); both take (service_ms, work_mode) and return a
    Future. Mirrors the native HPX executable's dispatch_one_by_one and Ray's
    one_by_one path. `window` defaults to --concurrency (single-client path);
    the multi-client dispatcher passes a per-thread slice of the window. req_id
    and service_ms are derived from the global index in `indices`, so they stay
    correct regardless of how indices are partitioned across client threads.
    """
    if window is None:
        window = args.concurrency
    records = []
    inflight = collections.deque()  # (request_id, Future, service_ms_requested)
    n = len(indices)
    next_i = 0

    def submit(idx):
        req_id = f"req-{idx:06d}"
        svc = _service_for(idx, args)
        fut = submit_fn(service_ms=svc, work_mode=args.work_mode,
                        chunks=args.chunks, chunk_delay_ms=args.chunk_delay_ms)
        inflight.append((req_id, fut, svc))

    while next_i < n and len(inflight) < window:
        submit(indices[next_i])
        next_i += 1
    while inflight:
        req_id, fut, svc = inflight.popleft()
        row = fut.result()
        if record:
            records.append(_make_record(args, run_id, workload, req_id, row,
                                         retire_mode, svc))
        if next_i < n:
            submit(indices[next_i])
            next_i += 1
    return records


def _run_one_by_one_threaded(submit_fn, args, indices, run_id, workload,
                             retire_mode, record, client_threads):
    """Run `client_threads` concurrent one_by_one loops sharing one Engine.

    Total in-flight is held at --concurrency: each thread gets a window of
    concurrency // client_threads, with the remainder handed deterministically
    to the first threads. Global request indices are partitioned into contiguous
    disjoint blocks, so request IDs stay unique and service_ms_requested stays a
    pure function of the global index (identical multiset/sequence to the
    single-thread run; only submission interleaving and lane routing differ).

    Thread-safety (no C++ change): Engine.submit() never releases the GIL, so
    concurrent submits — and the engine's round-robin counter — are GIL-
    serialized; each thread owns its own in-flight deque and its own futures
    (Future.result() releases the GIL only around its own blocking wait); per-
    thread record lists are merged only after all threads join.
    """
    n = len(indices)
    t = min(client_threads, max(1, n))
    base, rem = divmod(args.concurrency, t)
    windows = [max(1, base + (1 if k < rem else 0)) for k in range(t)]
    blocks = [indices[(n * k) // t:(n * (k + 1)) // t] for k in range(t)]

    results = [None] * t
    errors = [None] * t

    def worker(k):
        try:
            results[k] = _run_one_by_one(submit_fn, args, blocks[k], run_id,
                                         workload, retire_mode, record,
                                         window=windows[k])
        except Exception as exc:  # surface in the main thread; never swallow
            errors[k] = exc

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(t)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    for exc in errors:
        if exc is not None:
            raise exc
    out = []
    for r in results:
        if r:
            out.extend(r)
    return out


def _run_batch(batch_fn, args, n, run_id, workload, retire_mode, record):
    """Bulk dispatch: submit all n requests in ONE batch crossing.

    `batch_fn` is the API-agnostic batch callable (Engine.submit_batch or
    SyntheticActor.remote_batch). A single Python->C++ call enqueues all n
    requests (same round-robin lane routing as Engine.submit), so --concurrency
    is inert here -- there is no in-flight window. Every returned Future shares
    one Python submit_ns, so total_ms is queue-shaped (request k includes
    draining the k requests ahead of it on its lane); throughput is the
    meaningful batch metric, not the total_ms percentiles. Futures are drained in
    submission order.

    Service pattern: 'fixed' passes one scalar service_ms for the whole batch
    (unchanged). 'bimodal' builds the per-request service sequence with the SAME
    deterministic service_for used by the one_by_one path and passes the list to
    batch_fn(service_ms=[...]) -- a TRUE single bulk-varied crossing (the C++
    submit_batch_varied path), NOT a Python loop over scalar submit(). Each row
    records its own service_ms_requested (per-row field; schema unchanged).
    """
    records = []
    if args.service_pattern == "bimodal":
        svc_list = [_service_for(i, args) for i in range(n)]
        futures = batch_fn(service_ms=svc_list, work_mode=args.work_mode)
    else:
        svc_list = None
        futures = batch_fn(service_ms=args.service_ms, count=n,
                           work_mode=args.work_mode)
    for idx, fut in enumerate(futures):
        req_id = f"req-{idx:06d}"
        row = fut.result()
        if record:
            svc_req = svc_list[idx] if svc_list is not None else args.service_ms
            records.append(_make_record(args, run_id, workload, req_id, row,
                                         retire_mode, svc_req))
    return records


def _run_batch_wait(engine, args, indices, run_id, workload, retire_mode,
                    record):
    """Windowed as-completed retire, mirroring native dispatch_batch_wait.

    Holds --concurrency requests in flight; blocks (in C++/HPX, GIL released)
    until at least one future is ready via ``Engine.wait(..., num_returns=1)``,
    captures ONE shared recv_ns for the sweep, retires up to --wait-batch ready
    futures with that recv_ns (matching native's per-sweep recv_ns), then
    refills one per retired. Not-ready and un-retired-ready futures stay in
    flight. This is the engine-only, single-client analog of the native
    `batch_wait` path; submit_batch is intentionally NOT used (it removes the
    fixed window). req_id and service_ms are pure functions of the global index.
    """
    window = args.concurrency
    k = args.wait_batch
    records = []
    inflight = []  # list of (request_id, Future, service_ms_requested)
    n = len(indices)
    next_i = 0

    def submit(idx):
        req_id = f"req-{idx:06d}"
        svc = _service_for(idx, args)
        fut = engine.submit(service_ms=svc, work_mode=args.work_mode,
                            chunks=args.chunks, chunk_delay_ms=args.chunk_delay_ms)
        inflight.append((req_id, fut, svc))

    while next_i < n and len(inflight) < window:
        submit(indices[next_i])
        next_i += 1

    while inflight:
        futs = [fut for (_, fut, _) in inflight]
        # Block until >=1 is ready (wait_any). Returns all currently-ready.
        ready, _not_ready = engine.wait(futs, num_returns=1)
        recv_ns = time.perf_counter_ns()
        # Retire up to k ready futures with ONE shared recv_ns (native parity).
        by_id = {id(fut): (rid, fut, svc) for (rid, fut, svc) in inflight}
        retire = [by_id[id(f)] for f in ready[:k]]
        for req_id, fut, svc in retire:
            row = fut.result(recv_ns=recv_ns)
            if record:
                records.append(_make_record(args, run_id, workload, req_id, row,
                                             retire_mode, svc))
        retired_ids = {id(fut) for (_, fut, _) in retire}
        inflight = [e for e in inflight if id(e[1]) not in retired_ids]
        # Refill: one per retired, keeping the window full.
        for _ in range(len(retire)):
            if next_i < n:
                submit(indices[next_i])
                next_i += 1
    return records


def run(args):
    workload = args.workload or _default_workload(args)
    run_id = _gen_run_id()

    # engine/actor run the identical windowed one_by_one path (SyntheticActor is
    # a thin facade whose .remote forwards to Engine.submit); batch/actor_batch
    # are a distinct bulk dispatch (one batch crossing for all requests).
    # retire_mode records the dispatch shape honestly: "batch" vs "one_by_one".
    batch_api = args.api in ("batch", "actor_batch")
    retire_mode = "batch" if batch_api else args.retire_mode

    # actor/actor_batch use the SyntheticActor facade (whose .remote/.remote_batch
    # forward to Engine.submit/.submit_batch); engine/batch use Engine directly.
    if args.api in ("actor", "actor_batch"):
        client = SyntheticActor(num_lanes=args.num_lanes,
                                hpx_threads=args.hpx_threads)
    else:
        client = Engine(num_lanes=args.num_lanes, hpx_threads=args.hpx_threads)

    with client:
        if batch_api:
            batch_fn = (client.submit_batch if args.api == "batch"
                        else client.remote_batch)
            # Warmup: a discarded batch so the warmed path matches the measured
            # one. --concurrency is inert for batch.
            if args.warmup_requests > 0:
                _run_batch(batch_fn, args, args.warmup_requests, run_id,
                           workload, retire_mode, record=False)
            wall_start = time.perf_counter_ns()
            records = _run_batch(batch_fn, args, args.requests, run_id, workload,
                                 retire_mode, record=True)
            wall_end = time.perf_counter_ns()
        else:
            submit_fn = client.submit if args.api == "engine" else client.remote

            def dispatch(indices, record):
                if retire_mode == "batch_wait":
                    # Engine-only, single-client as-completed retire (validated
                    # in parse_args). `client` is the Engine.
                    return _run_batch_wait(client, args, indices, run_id,
                                           workload, retire_mode, record)
                if args.client_threads == 1:
                    return _run_one_by_one(submit_fn, args, indices, run_id,
                                           workload, retire_mode, record,
                                           window=args.concurrency)
                return _run_one_by_one_threaded(submit_fn, args, indices, run_id,
                                                workload, retire_mode, record,
                                                args.client_threads)

            # Warmup: same path, discarded, excluded from wall time.
            if args.warmup_requests > 0:
                dispatch(list(range(args.warmup_requests)), record=False)
            wall_start = time.perf_counter_ns()
            records = dispatch(list(range(args.requests)), record=True)
            wall_end = time.perf_counter_ns()

    records.sort(key=lambda r: r["request_id"])

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    wall_s = (wall_end - wall_start) / 1e9
    completed = sum(1 for r in records if r["status"] == "completed")
    lane_ids = sorted({r["actor_id"] for r in records})
    print(
        f"[run_hpx_python_baseline] run_id={run_id} workload={workload} "
        f"boundary={BOUNDARY} api={args.api} retire_mode={retire_mode} "
        f"warmup={args.warmup_requests} num_lanes={args.num_lanes} "
        f"client_threads={args.client_threads} lane_ids={lane_ids}"
    )
    print(
        f"[run_hpx_python_baseline] requests={len(records)} "
        f"completed={completed} wall_s={wall_s:.3f} out={args.out}"
    )
    if batch_api:
        print(
            f"[run_hpx_python_baseline] NOTE api={args.api}: all requests share one "
            "Python submit timestamp, so total_ms is bulk/queue-shaped "
            "(includes queue position). Compare throughput_req_s across modes, "
            "NOT total_ms percentiles; --concurrency is inert for batch."
        )


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="rayx Python-frontend baseline driver.")
    p.add_argument("--work-mode", default="sleep", choices=["sleep", "spin"],
                   help="Synthetic backend work mode. 'sleep' parks the lane "
                        "(std::this_thread::sleep_for); 'spin' busy-waits the "
                        "lane on-core until service_ms elapses (CPU-bound, no "
                        "sleep/wakeup overshoot). Both go through the shared "
                        "C++ ServiceLane.")
    p.add_argument("--service-ms", type=float, default=0,
                   help="Service time per request in ms (0 == no-op). Used by "
                        "--service-pattern fixed.")
    p.add_argument("--chunks", type=int, default=1,
                   help="Chunked synthetic service: split each request's TOTAL "
                        "active service_ms into N equal active steps (default 1, "
                        "unchunked). Single-submit modes only (--api engine/actor "
                        "with one_by_one/batch_wait); rejected for --api "
                        "batch/actor_batch. Populates the JSONL 'chunks' field; "
                        "schema stays 1.")
    p.add_argument("--chunk-delay-ms", type=float, default=0.0,
                   help="Chunked synthetic service: parked inter-chunk gap in ms "
                        "(blocking sleep, both work modes) between the chunks-1 "
                        "boundaries (default 0). With >0, service_ms_observed is "
                        "lifecycle/lane-occupancy time (active + parked gaps), "
                        "not active-only.")
    p.add_argument("--service-pattern", default="fixed",
                   choices=["fixed", "bimodal"],
                   help="Per-request service-time model. 'fixed' uses "
                        "--service-ms for every request. 'bimodal' draws "
                        "service_high with probability --service-p-high else "
                        "service_low, deterministically per request index (see "
                        "--seed); the same seed yields an identical sequence "
                        "across Ray/HPX/rayx (shared bench/service_sequence.py). "
                        "bimodal works with one_by_one AND --api batch/actor_batch "
                        "(the sequence is passed as a list to the true bulk-varied "
                        "submit_batch); batch bimodal needs low/high > 0.")
    p.add_argument("--service-low", type=float, default=1.0,
                   help="bimodal: low service time in ms. Default 1.0.")
    p.add_argument("--service-high", type=float, default=20.0,
                   help="bimodal: high service time in ms. Default 20.0.")
    p.add_argument("--service-p-high", type=float, default=0.1,
                   help="bimodal: probability of the high service time. "
                        "Default 0.1.")
    p.add_argument("--seed", type=int, default=0,
                   help="bimodal: seed for the deterministic per-index draw. "
                        "Same seed reproduces the same sequence. Default 0.")
    p.add_argument("--concurrency", type=int, default=8,
                   help="Target in-flight requests.")
    p.add_argument("--requests", type=int, default=500,
                   help="Total number of requests to issue (measured).")
    p.add_argument("--warmup-requests", type=int, default=0,
                   help="Requests run before measurement; not written to "
                        "JSONL and excluded from wall time.")
    p.add_argument("--num-lanes", type=int, default=1,
                   help="Number of HPX service lanes (round-robin). Default 1.")
    p.add_argument("--hpx-threads", type=int, default=4,
                   help="HPX worker threads (--hpx:threads). Default 4.")
    p.add_argument("--client-threads", type=int, default=1,
                   help="Number of Python client threads driving the engine "
                        "(one_by_one only). Default 1 (single-thread, unchanged "
                        "path). With N>1, --concurrency is split across N "
                        "threads sharing one Engine; total in-flight stays "
                        "--concurrency. Tests whether the client loop is the "
                        "throughput bottleneck.")
    p.add_argument("--retire-mode", default="one_by_one",
                   choices=["one_by_one", "batch_wait"],
                   help="Client retire mode. 'one_by_one': FIFO windowed retire "
                        "(hold --concurrency in flight, retire oldest, submit "
                        "one). 'batch_wait': as-completed retire (hold "
                        "--concurrency in flight, block until >=1 ready via "
                        "Engine.wait, retire up to --wait-batch ready with one "
                        "shared recv_ns, refill) -- the Python-frontend analog "
                        "of the native batch_wait path: windowed as-completed "
                        "retirement, NOT the bulk one-crossing dispatch of "
                        "--api batch/submit_batch. batch_wait is engine "
                        "single-client only.")
    p.add_argument("--wait-batch", type=int, default=8,
                   help="batch_wait: max ready futures retired per wait sweep "
                        "(native --wait-batch parity). Default 8.")
    p.add_argument("--api", default="engine",
                   choices=["engine", "actor", "batch", "actor_batch"],
                   help="Python-facing API. Windowed one_by_one paths: 'engine' "
                        "(Engine.submit) or 'actor' (SyntheticActor.remote). "
                        "Bulk dispatch paths (all requests in one crossing; "
                        "--concurrency inert; total_ms queue-shaped, compare "
                        "throughput): 'batch' (Engine.submit_batch) or "
                        "'actor_batch' (SyntheticActor.remote_batch). actor/"
                        "actor_batch are facade equivalents of engine/batch.")
    p.add_argument("--workload", default=None,
                   help="Workload name (auto-derived if omitted).")
    p.add_argument("--out", required=True, help="Output JSONL path.")
    args = p.parse_args(argv)
    if args.service_ms < 0:
        p.error("--service-ms must be >= 0")
    if args.concurrency < 1:
        p.error("--concurrency must be >= 1")
    if args.num_lanes < 1:
        p.error("--num-lanes must be >= 1")
    if args.hpx_threads < 1:
        p.error("--hpx-threads must be >= 1")
    if args.client_threads < 1:
        p.error("--client-threads must be >= 1")
    if args.wait_batch < 1:
        p.error("--wait-batch must be >= 1")
    if args.chunks < 1:
        p.error("--chunks must be >= 1")
    if args.chunk_delay_ms < 0:
        p.error("--chunk-delay-ms must be >= 0")
    if args.api in ("batch", "actor_batch") and (
            args.chunks > 1 or args.chunk_delay_ms > 0):
        p.error("chunked service (--chunks > 1 / --chunk-delay-ms > 0) is not "
                "supported with --api batch/actor_batch (batch submit is "
                "unchunked, matching the facade); use --api engine or actor.")
    if args.client_threads > 1 and args.api in ("batch", "actor_batch"):
        p.error("--client-threads > 1 is not supported with --api "
                "batch/actor_batch (batch is a single bulk crossing); use "
                "--api engine or actor.")
    if args.retire_mode == "batch_wait":
        if args.api != "engine":
            p.error("--retire-mode batch_wait is supported only with --api "
                    "engine (it uses Engine.wait directly).")
        if args.client_threads > 1:
            p.error("--retire-mode batch_wait is single-client only; use "
                    "--client-threads 1.")
    if args.service_pattern == "bimodal":
        if args.service_low < 0 or args.service_high < 0:
            p.error("--service-low and --service-high must be >= 0")
        if not 0.0 <= args.service_p_high <= 1.0:
            p.error("--service-p-high must be in [0, 1]")
        # bimodal now runs on batch too (the per-request sequence is passed as a
        # list to the true bulk-varied submit_batch). The varied batch form
        # requires each duration to be strictly positive, so batch bimodal cannot
        # use a zero service component -- the no-op stays the scalar
        # service_ms=0 form; use --api engine/actor for a zero component.
        if args.api in ("batch", "actor_batch") and (
                args.service_low <= 0 or args.service_high <= 0):
            p.error("--service-pattern bimodal with --api batch/actor_batch "
                    "requires --service-low > 0 and --service-high > 0 (the "
                    "varied batch form rejects zero/negative service times); "
                    "use --api engine/actor for a zero component.")
    return args


if __name__ == "__main__":
    run(parse_args())
