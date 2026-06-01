# rayx Python API: `Engine`, `SyntheticActor`, and the Equivalence Result

Documentation for the `rayx` Python frontend's API surface and the check that
the Ray-like actor façade adds no measurable overhead. Documentation only; it
does not change benchmark code, the analyzer, the C++ extension, or the metrics
contract in `docs/experiment_plan.md`. Companion to
`benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md`.

## 1. Purpose

`SyntheticActor` exists to test whether HPX-backed native work can be exposed
through a small **Ray-like actor façade** — an actor handle whose call returns a
future — **without** claiming to replace Ray actors. It is an **ergonomic
layer** over the lower-level `Engine`; the underlying execution (in-process HPX
service lanes running native C++ synthetic work) is unchanged.

## 2. API

Lower-level engine API:

```python
from rayx import Engine

with Engine(num_lanes=2, hpx_threads=4) as engine:
    f = engine.submit(service_ms=5)
    row = f.result()
```

Ray-like actor façade:

```python
from rayx import SyntheticActor

with SyntheticActor(num_lanes=2, hpx_threads=4) as actor:
    f = actor.remote(service_ms=5)
    row = f.result()
```

Bulk submit (one Python→C++ crossing for all requests) is available on both:

```python
with Engine(num_lanes=2, hpx_threads=4) as engine:
    futures = engine.submit_batch(service_ms=5, count=100)
    rows = [f.result() for f in futures]

with SyntheticActor(num_lanes=2, hpx_threads=4) as actor:
    futures = actor.remote_batch(service_ms=5, count=100)
    rows = [f.result() for f in futures]
```

Windowed **as-completed** retirement (block until ready, retire the ready ones,
keep the rest in flight) is available via `Engine.wait` / `SyntheticActor.wait`:

```python
with Engine(num_lanes=16, hpx_threads=4) as engine:
    inflight = [engine.submit(service_ms=5) for _ in range(32)]
    while inflight:
        ready, inflight = engine.wait(inflight, num_returns=1)  # blocks in HPX
        recv_ns = time.perf_counter_ns()                        # one per sweep
        rows = [f.result(recv_ns=recv_ns) for f in ready]
```

An ergonomic generator wraps that wait loop — `Engine.as_completed` /
`SyntheticActor.as_completed`:

```python
with Engine(num_lanes=16, hpx_threads=4) as engine:
    inflight = [engine.submit(service_ms=5) for _ in range(32)]
    for f in engine.as_completed(inflight):  # blocks in HPX between sweeps
        row = f.result()                      # caller retires each future
```

A small runnable tour of these calls (context manager, `submit`, `wait`,
`as_completed`, once-only `result`, graceful-drain shutdown) is in
`examples/rayx_basic.py` — runnable after the `_rayx` extension is built.

The complete small API surface is: `Engine.submit(...)`,
`Engine.submit_batch(...)`, `Engine.wait(...)`, `Engine.as_completed(...)`,
`SyntheticActor.remote(...)`, `SyntheticActor.remote_batch(...)`,
`SyntheticActor.wait(...)`, `SyntheticActor.as_completed(...)`, plus
`Future.ready()` / `Future.result(...)`.

* **`Future.ready() -> bool`** — non-blocking readiness check (raises if the
  future was already retired). A building block / test hook; **do not** spin on
  it in a Python loop in benchmarks (that holds the GIL and reintroduces a
  Python/GIL artifact) — use `Engine.wait` to block in HPX instead.
* **`Future.result(recv_ns=None)`** — default behavior unchanged (captures its
  own receive timestamp). Retiring **consumes** the Future: `result()` may be
  called only once; a second call raises `RuntimeError` (and `ready()` likewise
  raises after retire) rather than surfacing a raw HPX error. The optional
  `recv_ns` (a `time.perf_counter_ns()`
  value) is for batch / as-completed retire paths: pass one shared `recv_ns` for
  every future retired in a single `Engine.wait` sweep, matching native
  `batch_wait`'s per-sweep receive timestamp. Only correct for an already-ready
  future. Returns a per-request timing dict (`actor_id`, `submit_ns`,
  `start_ns`, `end_ns`, `total_ms`, `queue_wait_ms`, `service_ms_observed`,
  `status`, `error`).
* **`repr(future)`** — debug-friendly and non-blocking: shows `pending` /
  `ready` for a live future and `retired` after a successful `result()`, plus
  `submit_ns` (e.g. `<rayx.Future ready submit_ns=123>`). It never blocks or
  consumes the future.
* **`Engine.wait(futures, num_returns=1) -> (ready, not_ready)`** — a Ray-like
  wait primitive (`ray.wait`). It **blocks in C++/HPX with the GIL released**
  (`hpx::wait_some`, not a Python busy-poll) until at least `num_returns` futures
  are ready, then returns a partition of the **original** `Future` objects — so
  each keeps its Python-side `submit_ns`. `num_returns=1` is wait-any behavior.
  Each `Future` may appear at most once in the input list; a duplicate raises
  `ValueError`. `SyntheticActor.wait(...)` forwards to it.
* **`Engine.as_completed(futures)`** — an ergonomic **generator** wrapping
  `Engine.wait`. It copies the inputs into an internal in-flight list and
  repeatedly calls `wait(inflight, num_returns=1)`, yielding each sweep's ready
  Futures and continuing with the not-ready ones until all are exhausted. The
  block therefore happens **inside C++/HPX with the GIL released** (`Engine.wait`
  → `hpx::wait_some`), **not** a Python busy-poll. It yields the **original**
  `Future` objects (each keeping its `submit_ns`), exactly once each; the
  **caller** calls `.result()` on each yielded future. `SyntheticActor.as_completed(...)`
  forwards to it. This is a convenience wrapper, **not** the benchmark
  `batch_wait` retire path: it does not share one `recv_ns` across a ready
  sweep, so drivers that need per-sweep shared-`recv_ns` fairness keep their
  explicit `wait` loop (see `docs/reference/rayx_submit_batch.md` §2a).

Both classes own one HPX runtime, so only one active `Engine` or
`SyntheticActor` is allowed per process; both are context managers.

### Lifecycle / shutdown

`Engine.shutdown()` (also context-manager exit, `__del__`, and
`SyntheticActor.shutdown()`, which forwards to it) is a **graceful drain**: it
**blocks** until every queued and in-flight request submitted before shutdown
completes and its `Future` is fulfilled, then stops the HPX runtime. It does
**not** cancel or drop work, so shutdown latency can scale with the outstanding
queued service time.

Consequently, **Futures submitted before shutdown stay valid afterward** and are
ready: `future.ready()` returns `True` and `future.result()` retires the row as
usual, even after the owning engine has been shut down. New work after shutdown
still raises (`Engine is shut down`): `submit`, `submit_batch`, `wait`, and
`as_completed` (which goes through `wait`).

## 3. What it is

* `Engine` is the lower-level API (`submit(service_ms, work_mode)`,
  `submit_batch(service_ms, count, work_mode)`).
* `SyntheticActor` is a thin Ray-like façade over `Engine`.
* `SyntheticActor.remote(...)` forwards directly to `Engine.submit(...)`.
* `SyntheticActor.remote_batch(...)` forwards directly to
  `Engine.submit_batch(...)`, with the same bulk semantics: one Python→C++
  crossing enqueues all requests, every returned future shares one Python-side
  `submit_ns`, so `total_ms` is queue/bulk-drain shaped and throughput (not the
  latency percentiles) is the meaningful batch signal. See
  `docs/reference/rayx_submit_batch.md`.
* Both use the same boundary: `hpx-python-frontend`.
* Both submit **native C++ synthetic work** to the HPX service lanes (the same
  lanes the native HPX baseline uses).

## 4. What it is not

* Not a general Ray actor (single fixed synthetic operation, not arbitrary
  named methods). `remote_batch()` is likewise **not** a general Ray actor batch
  API — it bulk-dispatches the one fixed synthetic operation, not arbitrary
  Python functions.
* Not arbitrary remote Python function execution (native C++ work only).
* No object store.
* No distributed scheduler.
* No fault tolerance / autoscaling.
* No Ray Serve equivalent.
* No named actor registry yet.

## 5. Equivalence result

Reference output directory:

```text
results/rayx_api_equivalence_20260529T212558Z/
```

* 20 per-request JSONL files + 20 aggregate summary JSON files
  (2 cells × 2 APIs × 5 repeats).
* 20/20 benchmark runs passed.
* 20/20 analyzer summaries passed.
* Cells:
  * no-op (`service_ms=0`), concurrency=1, 1 lane, requests=1000.
  * sleep5 (`service_ms=5`), concurrency=8, 2 lanes, requests=200.
* The two API modes differ **only by output filename** (`engine_…` /
  `actor_…`); schema, boundary, and workload names are identical.

`SyntheticActor` matched `Engine` **within run-to-run noise** on every metric in
both cells — all median differences ≤1.1%, with the two APIs' min–max repeat
ranges overlapping (the gaps are smaller than the repeat-to-repeat spread).

The batch façade was checked the same way: a small `remote_batch()` vs
`submit_batch()` run (no-op count=1000 1-lane, sleep5 count=200 2-lanes, 5
repeats each, measuring submission and full-drain wall-time). End-to-end
wall-time matched within noise — median `actor/engine` total-time ratios 0.96
(no-op) and 1.005 (sleep5). The only larger relative gap was no-op submission
wall-time (~0.07 ms, a few µs of extra Python method indirection through the
façade), negligible against any real workload. `remote_batch()` adds no new
execution path over `submit_batch()`.

## 6. Key numbers (medians across 5 repeats)

| metric | Engine | SyntheticActor |
|---|---|---|
| no-op c1 1-lane, total_ms_p50 | 0.007792 ms | 0.007875 ms |
| sleep5 c8 2-lane, total_ms_p50 | 24.91 ms | 24.87 ms |

Throughput and `queue_wait_ms` were also effectively identical: no-op throughput
≈95.5k (Engine) vs ≈96.2k (Actor) req/s; sleep5 throughput 324.78 vs 324.40
req/s; sleep5 `queue_wait_ms_p50` 18.76 vs 18.67 ms. The only larger relative
gap was no-op `total_ms_p99` (+16.5%), but it is tiny in absolute terms
(~0.016 vs ~0.018 ms), a tail metric on the noisy no-op hot loop, with
overlapping ranges — not a real effect.

## 7. Conclusion

`SyntheticActor` adds Ray-like `.remote()` ergonomics without adding a new
execution path or measurable overhead. It is a façade over `Engine.submit()`,
not a general Ray actor replacement. This supports the project direction:
expose HPX-backed native work through a small Python API while keeping the scope
honest.

## 8. Caveats / next directions

Caveats:

* No-op is client-loop-sensitive (single Python thread, one_by_one); its numbers
  (especially p99) reflect client design and are noisy.
* Native synthetic C++ work only — not arbitrary Python functions.
* Not a general actor system (single fixed synthetic operation).
* Medians are the signal; p99/tails softer than medians.

Possible future directions:

* `actor.serve.remote(...)` method-style façade (closer to Ray's
  `actor.method.remote()`).
* Variable service time.
* Cancellation / streaming serving-control workloads.
* A real native backend behind the lane (once the synthetic contract is stable).
