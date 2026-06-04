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

Optional Ray-like **method-style** sugar mirrors `worker.method.remote(...)` for
the **one** fixed synthetic operation, `serve` (forwards to `remote` /
`remote_batch`, returns the same `Future` / `list[Future]`):

```python
with SyntheticActor(num_lanes=2, hpx_threads=4) as actor:
    f = actor.serve.remote(service_ms=5)               # -> Future
    futures = actor.serve.remote_batch(service_ms=5, count=100)  # -> list[Future]
    # actor.anything_else.remote(...) raises AttributeError: there is only `serve`.
```

Bulk submit (one Python→C++ crossing for all requests) is available on both,
in a **scalar** form (one `service_ms` for `count` requests) and a **varied**
form (a list/tuple of per-request service times, one request per element):

```python
with Engine(num_lanes=2, hpx_threads=4) as engine:
    futures = engine.submit_batch(service_ms=5, count=100)          # scalar
    varied  = engine.submit_batch(service_ms=[1, 5, 1, 10])         # varied
    rows = [f.result() for f in futures]

with SyntheticActor(num_lanes=2, hpx_threads=4) as actor:
    futures = actor.remote_batch(service_ms=5, count=100)           # scalar
    varied  = actor.serve.remote_batch(service_ms=[1, 5, 1, 10])    # varied
    rows = [f.result() for f in futures]
```

The varied form models a heterogeneous / skewed synthetic service workload in a
single bulk submission; `count` is inferred from the list length (and, if also
passed, must equal it). Single-request `submit(...)` / `remote(...)` stay
scalar-only.

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
`Engine.get(...)`, `Engine.cancel(...)`, `SyntheticActor.remote(...)`,
`SyntheticActor.remote_batch(...)`, `SyntheticActor.wait(...)`,
`SyntheticActor.as_completed(...)`, `SyntheticActor.get(...)`,
`SyntheticActor.cancel(...)`, `Engine.lane_stats()` /
`SyntheticActor.lane_stats()`,
`SyntheticActor.serve.remote(...)` / `SyntheticActor.serve.remote_batch(...)`,
plus `Future.ready()` / `Future.cancelled()` / `Future.result(...)`.

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
  `status`, `error`, `label`, `chunks`, `chunk_delay_ms`, `chunks_completed`).
* **`label` (optional client-side request annotation)** — the single-request
  submit paths (`Engine.submit(..., label=...)`, `SyntheticActor.remote(...,
  label=...)`, `actor.serve.remote(..., label=...)`) accept an optional
  `label: str | None` (a non-`str`, non-`None` value raises `TypeError`).
  `Future.result()` echoes it back as `row["label"]` — the supplied string, or
  `None` when omitted — so a retired row can be mapped back to a user-level
  request id. It is **client-side metadata only**: it never crosses into C++,
  never influences execution, is not a payload and is not stored, and is **not**
  part of the benchmark JSONL (the driver assigns its own `request_id`; see
  `docs/experiment_plan.md`). The **batch** paths (`submit_batch` /
  `remote_batch` / `serve.remote_batch`) do **not** accept `label` in v1.
* **`work_mode` (synthetic service-time shape, `"sleep"` | `"spin"`)** — every
  submit path (`Engine.submit` / `submit_batch`, `SyntheticActor.remote` /
  `remote_batch`, `actor.serve.remote` / `serve.remote_batch`) takes
  `work_mode`, defaulting to `"sleep"`. `"sleep"` models **parked/waiting**
  service time (the lane blocks in `sleep_for` for `service_ms`); `"spin"` models
  **CPU-bound active** service time (the lane stays busy on-core for `service_ms`
  in a no-yield loop, no sleep). Both are **synthetic service-time shapes, not
  payload/function execution** — `service_ms` sets the target duration in either
  mode, and the effect appears in `service_ms_observed`. It is validated at the
  Python boundary: an unrecognized mode raises `ValueError` and a non-`str`
  raises `TypeError` (so a typo fails fast instead of returning a
  `status="failed"` row). `work_mode` **crosses into C++** (it selects the lane's
  service path) but is **not** echoed in the result row — the row schema is
  **unchanged**. (The CPU-bound regime is explored in
  `experiments/05_spin_work_mode_knee_sweep/`.)
* **Variable service time (batch only)** — the batch paths (`submit_batch` /
  `remote_batch` / `serve.remote_batch`) accept `service_ms` as either a scalar
  (one duration for `count` requests, unchanged) **or** a `list`/`tuple` of
  per-request service times (one request per element, **input order
  preserved**). The varied form is dispatched to the native `submit_batch_varied`
  path — **one** Python→C++ crossing, **not** a Python loop over `submit` — so
  the bulk property holds (all returned futures share one `submit_ns`). Elements
  are validated at the Python boundary: the list must be **non-empty** with
  **finite, strictly-positive** real numbers (no `bool`, `NaN`, `inf`, zero, or
  negative) — a non-numeric/`bool` element raises `TypeError`, the others raise
  `ValueError`; the zero no-op is only the scalar `service_ms=0` form. If `count`
  is also passed with a list it must equal `len(service_ms)`. Single-request
  `submit(...)` / `remote(...)` stay **scalar-only**, and the result-row schema
  is **unchanged** (each row already reports its own `service_ms_observed`).
* **Chunked synthetic service (single-request only)** — the single-request paths
  (`Engine.submit`, `SyntheticActor.remote`, `actor.serve.remote`) accept
  `chunks` (an `int` >= 1, default `1`) and `chunk_delay_ms` (finite >= 0,
  default `0`). `chunks` splits the **total** active `service_ms` into that many
  equal active steps of `service_ms / chunks` (using `work_mode`);
  `chunk_delay_ms` is a **parked** gap (blocking sleep, both modes) between
  consecutive chunks (`chunks-1` gaps), modelling token-like cadence. This is
  **synthetic timing only** — **not** real token streaming, not payload
  execution, no per-chunk rows/events: the request still returns **one**
  `Future` and **one** final row, which **echoes `chunks` / `chunk_delay_ms`**.
  Because the lane is occupied for the whole lifecycle, with `chunk_delay_ms > 0`
  the row's `service_ms_observed` is **lifecycle/lane-occupancy time** (active
  service **plus** the parked gaps), **not** active-only service. `chunks=1,
  chunk_delay_ms=0` reproduces the unchunked single-step path exactly. Inputs are
  validated at the Python boundary (`chunks` int >= 1 non-`bool`;
  `chunk_delay_ms` finite >= 0 non-`bool`). The **batch** paths (`submit_batch` /
  `remote_batch` / `serve.remote_batch`) are **unchunked** in v1 and **reject**
  `chunks` / `chunk_delay_ms` (unexpected keyword → `TypeError`). Cancellation
  applies in both modes (a *queued* chunked request cancels whole; a *running*
  chunked request stops at its next chunk boundary, `1 <= chunks_completed <
  chunks`; an active chunk / parked gap is never interrupted mid-flight). See
  `docs/reference/rayx_frontend_design.md` §7–§8.
* **`chunks_completed` (result-row field)** — lane-determined count of active
  chunks that **actually ran**: `== chunks` on a normal finish, `0` on a queued
  cancel, `1 <= chunks_completed < chunks` on a running (chunk-boundary) cancel.
  Unlike `chunks` / `chunk_delay_ms` (echoed from the submit-side copy), the
  client cannot know where an early stop landed, so it comes from the C++
  `Result`. Facade-row only — the benchmark JSONL schema is unchanged.
* **`repr(future)`** — debug-friendly and non-blocking: shows `pending` /
  `ready` for a live future and `retired` after a successful `result()`, plus
  `submit_ns` (e.g. `<rayx.Future ready submit_ns=123>`). It never blocks or
  consumes the future.
* **`Engine.wait(futures, num_returns=1, timeout=None) -> (ready, not_ready)`** —
  a Ray-like wait primitive (`ray.wait`). It partitions the **original** `Future`
  objects (each keeps its Python-side `submit_ns`) and is **non-consuming**:
  readiness/retirement coordination, *not* result consumption — you still call
  `result()` / `get()` once to retire a ready future. Each `Future` may appear at
  most once; a duplicate raises `ValueError`. `timeout` is in **seconds** (Ray/
  Python convention) and selects the mode:
  * **`timeout=None`** (default) — **blocks in C++/HPX with the GIL released**
    (`hpx::wait_some`, not a Python busy-poll) until at least `num_returns`
    futures are ready. `num_returns=1` is wait-any behavior.
  * **`timeout=0`** — a **non-blocking poll**: returns `(ready_now, pending_now)`
    immediately by probing each future's non-consuming readiness. `ready` holds
    **all** currently-ready futures (a true readiness partition — it may hold
    fewer or more than `num_returns`), while `num_returns` is still range-checked
    (`1 ≤ num_returns ≤ len`). A **cancelled** future is ready only once its row
    exists: a *queued* cancel is ready immediately, but a *running* chunk-boundary
    cancel stays **pending** until the lane reaches the boundary and retires the
    cancelled row.
  * **`timeout > 0`** (finite) — raises `NotImplementedError`. A bounded
    finite-timeout wait needs a non-consuming *timed multi-future* wait, which HPX
    v1.11.0 does not provide; it is intentionally deferred (see
    `docs/reference/rayx_frontend_design.md` §9). Use `timeout=0` to poll or
    `timeout=None` to block.

  A negative / `NaN` / `inf` / `bool` / non-numeric `timeout` raises `TypeError`
  or `ValueError` before any future is inspected. `SyntheticActor.wait(...)`
  forwards `timeout` unchanged. There is deliberately **no** `Future.done()`
  alias — `ready()` is the single non-blocking readiness predicate, and on a
  retired future the honest answer is "raise", not a quiet boolean.
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
  explicit `wait` loop (see `docs/reference/rayx_submit_batch.md` §2a, and
  `docs/reference/rayx_frontend_design.md` for the full design rationale behind
  Future ownership, `wait`/`as_completed`, and the `hpx::wait_some` choice).
* **`Engine.get(futures, recv_ns=None)`** — ergonomic retire/collect sugar over
  `Future.result`. A single `Future` returns one row dict; a list returns a list
  of row dicts **in input order**. It is **not** Ray's `ray.get`: RayX has no
  object store and no computed user value, so `get` returns RayX **measurement
  rows** (the per-request timing dict), not a function result. Like `result`, it
  **consumes** each Future once — a second `get(...)` / `result()` on the same
  Future raises (the once-only guard). The optional `recv_ns` (a
  `time.perf_counter_ns()` value) is forwarded to each `result(recv_ns=...)` and,
  as there, is only correct for an already-ready Future. It is a convenience
  helper, **not** the benchmark `batch_wait` retire path: it does not coordinate
  one shared `recv_ns` across a wait sweep, so drivers that need per-sweep
  shared-`recv_ns` fairness keep their explicit `wait` loop.
  `SyntheticActor.get(...)` forwards to it.
* **`Engine.cancel(future) -> bool`** (and `SyntheticActor.cancel(...)`, which
  forwards to it) — **honest two-mode** cancellation: a *queued skip* or a
  *running stop at a chunk boundary*. Returns `True` when this call **settles** a
  cancellation: the request was still **queued** (lane skips it, **no service
  time spent**, `status="cancelled"`, `chunks_completed == 0`), **or** it is a
  **running chunked** request with a chunk boundary still ahead (the lane stops
  at that next boundary — `True` means *guaranteed-to-stop, not ready-now* — and
  fulfills `status="cancelled"` with `1 <= chunks_completed < chunks`). Returns
  `False` when nothing can be settled: already **completed**, stop **already
  requested**, a started **single-chunk** (`chunks=1`) request, or already on its
  **final** chunk. Active work is **never** interrupted (no check inside
  `sleep_for` / `spin_for`, none inside a parked gap — only *between* chunks). It
  **raises** for an already-retired Future, after engine `shutdown()`, and for a
  **non-cancelable** Future — only single-request `submit` / `remote` /
  `serve.remote` futures are cancelable; **batch-submitted futures are not** (no
  batch-cancel in this slice). `cancel()` is **not** a retire: a cancelled
  Future still becomes ready and must still be consumed once via `result()` /
  `get()`, which returns a `status="cancelled"` row that **preserves the
  `label`** — so cancelled futures flow through `wait` / `as_completed` / `get`
  like any other. This is **not** Ray task/object cancellation (no task graph,
  no object-store value, no interrupt of an in-progress active chunk); see §4 and
  `docs/reference/rayx_frontend_design.md` §7 for the state machine and race
  semantics.
* **`Future.cancelled() -> bool`** — non-blocking, non-consuming: `True` once a
  cancellation is **settled** — a queued cancel **or** a requested running
  stop-at-boundary — which can occur *before* the cancelled row is ready; else
  `False` (including for a request that completed normally and for a
  non-cancelable batch future). Unlike `ready()` / `result()`, it does **not**
  raise after retire and does **not** consume the Future.
* **`Engine.lane_stats() -> list[dict]`** (and `SyntheticActor.lane_stats()`,
  which forwards to it) — an **observability snapshot for debugging**. Returns
  one dict per service lane, **in stable lane order** (the order `submit`
  round-robins over): `{"actor_id": str, "queue_depth": int, "active": bool}`.
  `queue_depth` is the count of requests **queued but not yet started** on that
  lane (the in-service request is not counted); `active` is `True` once the lane
  has **popped** a request and is inside its service lifecycle (until that row is
  fulfilled, or a queued-cancel skip clears it), `False` when idle. It briefly
  takes each lane's mutex and is **non-consuming** — it touches no `Future` and
  does not change `submit` / `wait` / `cancel` / `result` semantics. It is a
  **snapshot**: the values can change the instant after it returns (a concurrent
  pop/fulfilment races it), so treat it as a debugging/observability view, **not**
  a coordination primitive. It is **not** Ray scheduler state, **not** placement
  control (lane choice stays internal round-robin — `actor_id` reports the
  serving lane, it is not a handle you submit to), and **not** part of the
  benchmark JSONL / analyzer schema. It **raises** `RuntimeError` after engine
  `shutdown()` (the lanes are destroyed), consistent with the other coordination
  APIs. See `docs/reference/rayx_frontend_design.md` §11.
* **Bounded admission — `Engine(max_queue_depth_per_lane=None)` +
  `QueueFullError`** (also `SyntheticActor(max_queue_depth_per_lane=...)`, which
  forwards it) — an optional, **local per-lane admission-by-rejection** cap. The
  default `None` is **unbounded** and preserves the original behavior exactly (no
  admission check runs). A positive `int` `N` bounds each lane to at most `N`
  **queued-but-not-started** requests; the **active in-service request is not
  counted**. When the round-robin **target lane** is already at the cap,
  `Engine.submit` / `SyntheticActor.remote` / `actor.serve.remote` raise
  `QueueFullError` (a `RuntimeError` **subclass**, so `except RuntimeError` still
  catches it, while a load-shedding caller can catch it specifically). The
  rejection is raised **before** any `Future` is created — a rejected request has
  **no Future, no result row, and no JSONL row** — and the round-robin rotation
  **still advances** (a rejected call consumes its lane turn, so one full lane
  never stalls submissions to the other lanes). The cap is **per-lane queue
  depth, not a global backlog**, and a full lane is **never** skipped to a
  different lane — rejection targets only the one lane the rotation picked. A
  **cancelled but not-yet-popped** queued request **still counts** against the
  cap until the lane pops/skips it (cancel settles the future; it does not
  dequeue). Admission is checked **atomically with the enqueue, under the lane
  mutex** (one check-and-push), so there is no read-then-act (TOCTOU) window —
  `lane_stats()` is observability only and is **not** used as the gate. The
  **batch** paths (`submit_batch` / `remote_batch` / `serve.remote_batch`) are
  **not supported under a cap**: rather than silently bypass the per-lane limit
  they **refuse loudly** with a plain `RuntimeError` (not `QueueFullError` — no
  lane is "full"; the op is simply unsupported when a cap is set). Constructor
  validation: `None` and a positive `int` are accepted; `0` / negative raise
  `ValueError`, and `bool` / `float` / `str` raise `TypeError`. This is **not**
  Ray Serve backpressure, **not** distributed flow control, **not** a scheduler,
  and **not** blocking backpressure (the call returns immediately by raising; it
  never blocks waiting for space). See
  `docs/reference/rayx_frontend_design.md` §12.
* **Lane backend — `Engine(lane_impl="std")`** (also
  `SyntheticActor(lane_impl="std")`, which forwards it) — selects the lane
  **mechanism** behind every lane. The default `"std"` is the `std::thread`
  `ServiceLane`, the project's **stable comparison anchor**; `"hpx"` opts into the
  cooperative HPX-thread `HpxLane`. **Both honor the identical lane contract** —
  FIFO ordering, `actor_id`, `lane_stats()` `queue_depth` / `active`, bounded
  admission (`max_queue_depth_per_lane` + `QueueFullError`), queued cancellation,
  running cancellation at chunk boundaries, and `wait` / `get` / `as_completed`
  behavior — and the **result-row / JSONL schema is unchanged**. The only visible
  difference is the lane `actor_id` **prefix**: `act-hpx-` for `std` /
  `ServiceLane`, `act-hpxl-` for `hpx` / `HpxLane`. No HPX internals are exposed
  to Python by either choice. Validation: a non-`str` raises `TypeError`, an
  unknown string raises `ValueError` (before the HPX runtime starts). This is
  **not** the task/dataflow mechanism probe from experiment 20, **not** Ray Serve,
  and **not** an "HPX beats Ray" claim — it is an opt-in lane mechanism behind the
  same Ray-like API. See `docs/reference/rayx_frontend_design.md` §13.

  ```python
  with Engine(num_lanes=2, hpx_threads=4, lane_impl="hpx") as engine:
      row = engine.submit(service_ms=5).result()
      # row["actor_id"] starts with "act-hpxl-"
  ```
* **`SyntheticActor.serve.remote(service_ms, work_mode)` /
  `SyntheticActor.serve.remote_batch(service_ms, count, work_mode)`** — optional
  Ray-like **method-style** sugar. `actor.serve.remote(...)` mirrors Ray's
  `worker.method.remote(...)` shape, forwarding to `actor.remote(...)` /
  `actor.remote_batch(...)` and returning the **same** `Future` / `list[Future]`
  (so `wait` / `as_completed` / `get` / `result` and graceful-drain shutdown all
  behave identically; consume-once is inherited). It is **one fixed synthetic
  operation** named `serve`, **not** Ray's general actor-method dispatch — RayX
  has no arbitrary actor methods, so any other dotted access (`actor.predict`,
  `actor.foo`) raises `AttributeError`. `actor.remote(...)` remains the primary
  API; `serve` is sugar only, and the benchmark driver uses `remote`/`submit`,
  not this façade.

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
still raises (`Engine is shut down`): `submit`, `submit_batch`, `cancel`,
`wait`, and `as_completed` (which goes through `wait`).

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
  Python functions. The `actor.serve.remote(...)` method-style sugar exposes that
  one operation in Ray's `.method.remote` shape; it does **not** add method
  dispatch — `serve` is the only method, and any other dotted access raises
  `AttributeError`.
* Not arbitrary remote Python function execution (native C++ work only).
* **Not real token streaming / not Ray streaming.** `chunks` / `chunk_delay_ms`
  model a multi-step synthetic service lifecycle as **timing only** — there are
  no tokens, no payloads, no per-chunk events/callbacks, and no streamed values;
  one request still returns one `Future` and one row (see §2 and
  `docs/reference/rayx_frontend_design.md` §8).
* **Not Ray task/object cancellation.** `Engine.cancel(future)` either skips a
  **queued** synthetic request or stops a **running chunked** one at its next
  chunk boundary; it does not cancel a task graph, drop an object-store value, or
  interrupt an **in-progress active chunk / parked gap** (those always run to the
  boundary — see §2 and `docs/reference/rayx_frontend_design.md` §7). It is not
  real token-stream cancellation either.
* **Not Ray Serve / not backpressure or distributed flow control.** The optional
  `max_queue_depth_per_lane` cap (§2) is **local per-lane admission by
  rejection**: when a lane's queued-but-not-started depth is at the cap, the
  submit raises `QueueFullError` immediately. It does not block, buffer, shed
  across lanes, or coordinate flow across processes, and it is not a scheduler.
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

## 8. Mapping Ray actor-pool code to RayX

A common Ray idiom is an explicit pool of actor workers with client-side
round-robin placement:

```python
workers = [Worker.remote() for _ in range(4)]
refs = [workers[i % 4].work.remote(i) for i in range(20)]
results = ray.get(refs)
```

RayX expresses the same request-routing **shape** with one `Engine` over `N`
internal service lanes — not `N` independent actors. There is no pool object;
`Engine(num_lanes=N)` already round-robins submissions across the lanes, and
each retired row reports the lane that served it:

```python
with Engine(num_lanes=4, hpx_threads=4) as engine:
    futures = [
        engine.submit(service_ms=5, label=f"req-{i}")
        for i in range(20)
    ]
    rows = engine.get(futures)

for row in rows:
    print(row["label"], row["actor_id"])
```

### What maps cleanly

* **Per-request Futures.** Each `engine.submit(...)` returns one `Future`,
  like each `worker.work.remote(i)` returns one `ObjectRef`.
* **Round-robin over `N` lanes.** `Engine` distributes submissions across its
  `num_lanes` lanes internally — the same distribution Ray's `workers[i % 4]`
  expresses by hand, without the client doing the modulo.
* **`Engine.wait` / `Engine.as_completed`.** The as-they-complete retire shapes
  map to `ray.wait(...)` and the as-completed idiom (see §2).
* **`Engine.get`.** Retiring a list of Futures in input order maps to
  `ray.get(refs)` (with the difference noted below).
* **`label` for user request identity.** The optional client-side `label` echoes
  back as `row["label"]`, so a retired row maps to a user-level request id — the
  role `i` plays in the Ray loop.
* **`actor_id` for lane identity.** Each lane has a stable `actor_id`, reported
  on every row, so `row["actor_id"]` tells you which lane served the request
  after the fact — analogous to knowing which worker handled a `ref`.

### What does not map

* **Ray's `N` handles are `N` independent actor workers (processes).** RayX
  `num_lanes=N` is `N` internal HPX service lanes inside **one** `Engine` and one
  HPX runtime; only one active `Engine` (or `SyntheticActor`) exists per process.
* **No user-controlled placement.** There is no `workers[i]` equivalent: lane
  choice is internal round-robin, not client-selected. `actor_id` reports the
  serving lane after the fact; it is not a handle you submit to.
* **No arbitrary Python actor methods.** RayX runs one fixed native C++
  synthetic operation, not user-defined methods like `Worker.work` (see §4).
* **`get` returns measurement rows and consumes once.** `Engine.get` returns
  RayX per-request timing rows, not object-store values, and retires each Future
  exactly once — unlike Ray's idempotent `ray.get` over a value store (see §2).

For the conceptual actor ↔ HPX-component mapping behind this, see
`docs/ray_hpx_mapping.md`; for the full API contract, see §2 above.

## 9. Caveats / next directions

Caveats:

* No-op is client-loop-sensitive (single Python thread, one_by_one); its numbers
  (especially p99) reflect client design and are noisy.
* Native synthetic C++ work only — not arbitrary Python functions.
* Not a general actor system (single fixed synthetic operation).
* Medians are the signal; p99/tails softer than medians.

Possible future directions:

* Chunked synthetic service (§2) and **chunk-boundary running cancellation** now
  exist: `Engine.cancel` settles both a queued skip and a stop-at-next-boundary
  for a running chunked request (`1 <= chunks_completed < chunks`), without
  interrupting an in-progress active chunk or parked gap — see
  `docs/reference/rayx_frontend_design.md` §7. A deeper streaming model (per-chunk
  events / partial-value delivery) remains out of scope.
* A real native backend behind the lane (once the synthetic contract is stable).
