# rayx Frontend Design Note

Design rationale for the `rayx` Python frontend: the API model, the Future
ownership contract, the wait / as-completed design, and the HPX primitive
choices behind them. This is a **stable design note**, not a benchmark write-up;
it captures the decisions made in the frontend hardening slice while they are
fresh. Documentation only — it does not change benchmark code, the analyzer, the
C++ extension, or the metrics contract in `docs/experiment_plan.md`.

Companions: `docs/reference/rayx_actor_api.md` (API surface + façade equivalence
result) and `docs/reference/rayx_submit_batch.md` (bulk submit + first
throughput benchmark).

## 1. What `rayx` is

`rayx` is a **thin Python frontend over the HPX synthetic service lanes**. The
Python layer (`python/src/rayx/__init__.py`) wraps a compiled pybind11/HPX
extension (`_rayx`) that owns the HPX runtime and `N` serialized service lanes
(by default `rayhpx::ServiceLane`; an opt-in cooperative `HpxLane` backend is
selectable via `lane_impl`, see §13); submissions are round-robined across the
lanes.

It is a **synthetic serving-control frontend**, not a Ray replacement and not
real model inference. The lanes run native C++ synthetic work (blocking sleep or
on-core spin), the same work the native HPX baseline runs. There is no object
store, scheduler, fault tolerance, autoscaling, distributed placement,
supervision, or named-actor registry.

The Python API exists for one reason: to expose the **HPX-native control path**
through a small, recognizable developer-facing surface, so the same in-process
HPX lane mechanism can be measured across a Python boundary
(`hpx-python-frontend`) and compared against both the Ray actor-process path and
the pure-C++ native path. The Python layer adds only client-side timing and
lifecycle; it adds no new execution path over the underlying `Engine`.

## 2. API model

The public surface is intentionally small:

* **`Engine`** — process-singleton, HPX-backed, owns `num_lanes` serialized
  lanes. Context manager. Only one active `Engine` (or `SyntheticActor`) per
  process; a second before `shutdown()` raises.
* **`Future`** — handle for one in-flight request; carries the Python-side
  `submit_ns` captured at submission.
* **`SyntheticActor`** — a thin Ray-flavored façade over one `Engine`. It
  exposes the recognizable Ray idiom (an actor handle whose call returns a
  future) and forwards directly to `Engine`; it adds no measurable overhead and
  no new execution path.
* **`submit(service_ms, work_mode, label=None)`** — one request → one `Future`
  (`SyntheticActor.remote(...)`). Optional client-side `label` echoed by
  `result()` (see below).
* **`submit_batch(service_ms, count, work_mode)`** — one Python→C++ crossing
  enqueues `count` requests → list of `Future`
  (`SyntheticActor.remote_batch(...)`). Bulk semantics: all returned futures
  share one `submit_ns`, so per-request `total_ms` is queue-shaped and
  throughput is the meaningful signal.
* **`wait(futures, num_returns=1)`** — block until ≥ `num_returns` are ready;
  return a `(ready, not_ready)` partition of the **same** `Future` objects.
* **`as_completed(futures)`** — ergonomic generator over `wait` that yields the
  original `Future` objects as they become ready.
* **`Future.result(recv_ns=None)`** — retire one `Future`, returning its
  measured row.
* **`get(futures, recv_ns=None)`** — ergonomic retire/collect sugar over
  `Future.result`: retire one `Future` (→ row) or a list (→ rows in input order)
  (`SyntheticActor.get(...)`).
* **`shutdown()`** — graceful drain, then stop the HPX runtime (also context
  exit / `__del__`).

`Future.ready()` is a non-blocking readiness probe — a building block / test
hook, **not** something to spin on in a Python loop (that holds the GIL and
reintroduces a Python/GIL artifact); use `wait` to block in HPX instead.

`get` is deliberately **method-based** on `Engine` (and forwarded by
`SyntheticActor`), **not** a module-level `rayx.get`: a global `get` would imply
a global default engine and reintroduce the lifecycle confusion the explicit
process-singleton `Engine` model avoids. It also differs from Ray's `ray.get` on
purpose — it returns RayX **measurement rows** (there is no object store and no
computed value) and it **consumes** each Future once (`ray.get` is idempotent;
`get` is not).

`SyntheticActor.serve.remote(...)` / `serve.remote_batch(...)` is **optional
Ray-like sugar**: a read-only `serve` property returns a tiny stateless handle
(`_SyntheticServeMethod`) that forwards to `actor.remote` / `actor.remote_batch`,
giving Ray users the recognizable `actor.method.remote(...)` shape for the one
fixed synthetic operation. The honesty guardrail is structural, not just
documentary: `serve` is the **single** named method, so any other dotted access
(`actor.predict`, `actor.foo`) raises `AttributeError` — RayX is **not** a
general actor system with arbitrary method dispatch. `actor.remote(...)` remains
primary, and the benchmark driver uses `remote`/`submit`, never the façade.

The optional **`label`** on the single-request submit paths is a deliberately
**minimal client-side request annotation**, not a `metadata` dict and not a
payload. It is captured in Python and stored on the `Future` (exactly like
`submit_ns`), validated to `str | None` (else `TypeError`), and echoed by
`result()` as `row["label"]` (`None` when omitted). It **does not cross into
C++**, does not influence the synthetic work, and does not touch the JSONL
schema or analyzer (the benchmark driver assigns its own `request_id`). Its only
purpose is examples/debugging — mapping a retired facade row back to a
user-level id. The richer `metadata: dict` and batch-label forms were
**rejected** for v1: a dict reads like a stored payload (straight at the
no-object-store guardrail), and a single batch label cannot distinguish the
requests it would tag.

The **`work_mode`** parameter selects the synthetic service-time *shape* the
lane runs: `"sleep"` (default) parks the lane in a blocking `sleep_for`
(waiting/parked service time), while `"spin"` keeps it busy on-core in a
no-yield loop until the requested `service_ms` elapses (CPU-bound active service
time). Both run the **same native lane** and are **synthetic service-time
shapes, not payload execution** — `service_ms` sets the target duration either
way. The two modes exist so the same coordination/lane mechanism can be measured
under parked vs CPU-bound service (the sleep-timer artifact vs the
hardware/core-boundary regime; see `experiments/05_spin_work_mode_knee_sweep/`).
`work_mode` is **validated at the Python boundary** — `ValueError` for an
unrecognized mode, `TypeError` for a non-`str` — so a typo fails fast rather than
surfacing as a native `status="failed"` row, mirroring the `label` validation
and the single-enforcement-point pattern (every facade submit path funnels
through `Engine.submit` / `submit_batch`). Two deliberate asymmetries with
`label`: unlike `label`, `work_mode` **crosses into C++** (it picks the lane's
service path); and unlike `label`, it is **not echoed in the result row** — it
is an execution input whose effect is already visible in `service_ms_observed`,
so the row schema is **unchanged** and the benchmark JSONL (driver-owned) is
unaffected.

**Variable service time** is confined to the **batch paths**: `submit_batch`
(and the `remote_batch` / `serve.remote_batch` forwards) accept `service_ms` as
either a scalar (one duration for `count` requests, unchanged) or a `list`/`tuple`
of per-request durations (one request per element, input order preserved, `count`
inferred). The design constraint that made this honest is that it preserves the
**bulk contract**: the varied form is dispatched to a dedicated native path
(`_Engine.submit_batch_varied`, a C++ loop over a `std::vector<double>`) — **not**
a Python loop over `submit()` — so it is still **one** Python→C++ crossing and
every returned future still shares **one** Python `submit_ns`. A Python-loop
implementation was rejected for exactly this reason: it would silently break the
single-crossing / shared-`submit_ns` property that defines `submit_batch`, giving
the same method two incompatible timing models. Single-request `submit` /
`remote` stay **scalar-only** (one request has one duration; per-call variation
already covers the windowed/`one_by_one` case — it is how the benchmark driver's
bimodal pattern works). Inputs are validated at the Python boundary (non-empty;
finite, strictly-positive, non-`bool` reals; `count == len` if both given) so bad
workloads fail fast before the crossing. `service_ms` is purely **synthetic
duration control**, never a payload or work item — the list is durations, not
tasks — and the result-row / JSONL schema is **unchanged** (each row already
reports its own `service_ms_observed`).

## 3. Future ownership model

A `Future` is **single-use**:

* **`result()` consumes / retires the future.** In C++, `EngineFuture::result()`
  calls `fut_.get()`, which moves the value out and leaves the `hpx::future`
  invalid.
* **It may be called once.** A second `result()` is guarded *before* re-entering
  C++ and raises a clean RayX `RuntimeError` ("already retired via result()"),
  instead of surfacing a raw HPX `no_state` error.
* **`ready()` after retire raises.** The same validity guard rejects a readiness
  probe on a consumed future, so a retired future fails loudly rather than
  silently misreporting.
* **`repr(future)` is non-consuming and debug-friendly.** It shows
  `pending` / `ready` for a live future (a single non-blocking `ready()` probe)
  and `retired` after a successful `result()`. A Python-only `_retired` flag
  lets `__repr__` report the retired state **without** touching the moved-out
  `_cf` (which would raise). `repr` never blocks and never consumes.

**Why no `Future.done()`:** it was intentionally deferred. `ready()` already
covers the non-blocking "is the result available?" question, and on a retired
future the honest answer is "raise" (the future is consumed), not a quietly
returned boolean. Adding a second, overlapping predicate with subtly different
post-retire semantics would widen the surface and invite confusion for no new
capability. The contract stays: `ready()` to probe, `wait` to block, `result()`
once to retire.

## 4. Wait / as-completed design

`Engine.wait` is the Ray-like `wait` primitive (`ray.wait(num_returns=k)`), and
its design choices matter for both correctness and fairness:

* **It blocks in C++/HPX via `hpx::wait_some`** — not a Python busy-poll.
  `wait_some(num_returns, futures)` spans `num_returns == 1` (wait-any) and
  `num_returns > 1` uniformly.
* **The GIL is released only around the blocking HPX wait.** Inside the
  `gil_scoped_release` scope no Python objects are touched (the temp vector holds
  C++ futures); the ready-list construction afterward runs with the GIL
  reacquired. The blocking `wait_some` runs on the external Python caller's OS
  thread, not an HPX worker thread, so it does not starve the HPX scheduler.
* **`wait` does not consume futures.** It returns a partition of the **same**
  `Future` objects, so each keeps its Python-side `submit_ns`. `hpx::wait_some`
  guarantees all input futures remain valid after it returns (it acquires shared
  state, never reads the value).
* **Duplicate futures are rejected before any C++ `take()`.** The C++ layer
  builds a `seen` set and raises on a repeated `EngineFuture*` *before* moving
  any underlying future out — otherwise the same `hpx::future` would be moved
  twice and corrupted. An already-retired or non-`_Future` entry is likewise
  rejected up front.
* **RAII restores moved futures.** Because `hpx::future` is move-only and the
  `std::vector` overload is used, each underlying future is moved into a temp
  vector and back into the **same** `_Future` object. A `Restore` guard,
  installed before the take loop, moves them back on **every** exit path (normal
  return or an exception out of `wait_some`), so a Python `_Future` is never left
  moved-from.
* **`as_completed` is a Python generator over `wait`.** It copies the inputs into
  an internal in-flight list and repeatedly calls `wait(inflight, num_returns=1)`,
  so the block still happens inside HPX with the GIL released.
* **`as_completed` yields Futures, not rows.** It yields the original `Future`
  objects (each preserving `submit_ns`), exactly once each; the **caller** calls
  `.result()` on each. It does not retire on the caller's behalf.
* **`wait` takes a `timeout` (seconds): block, poll, or reject.** `wait(futures,
  num_returns=1, timeout=None)` keeps the blocking behavior above. `timeout=0` is
  a **non-blocking poll** — it returns `(ready_now, pending_now)` immediately by
  probing each future's existing non-consuming `ready()` (no `wait_some`, no
  block, no C++ rebuild), with the **same** structural guards as the blocking path
  (engine-running, `num_returns` range, duplicate, retired, wrong-type, empty). It
  is a true readiness partition: `ready` holds **all** currently-ready futures,
  so `num_returns` is range-checked but does not cap `ready`. A finite **positive**
  `timeout` raises `NotImplementedError` (§9). This keeps `wait` the single
  readiness/retirement coordinator across both "block until k ready" and "what is
  ready right now?" without a second predicate. (Relatedly, there is **no**
  `Future.done()` — see §3: `ready()` already answers the per-future question, and
  on a retired future the honest answer is to raise.)

## 5. Why the benchmark `batch_wait` is different

There are three easily-conflated things; keep them distinct:

* **`submit_batch`** is *bulk submission* — one crossing enqueues all requests,
  `--concurrency` is inert, latency is queue-shaped.
* **`Engine.as_completed`** is the *ergonomic API* for draining a set of futures
  as they complete.
* **The benchmark `--retire-mode batch_wait`** (in
  `bench/run_hpx_python_baseline.py`, `_run_batch_wait`) is a *windowed
  as-completed retire loop* that deliberately keeps an **explicit** `Engine.wait`
  loop rather than calling `as_completed`.

The reason for the explicit loop is **native parity**: `batch_wait` captures
**one shared `recv_ns` per ready sweep** and retires up to `--wait-batch` ready
futures with that single timestamp, matching the native `dispatch_batch_wait`
path (one batch `recv_ns` per sweep). `as_completed` yields futures one at a time
and does not share a sweep-level `recv_ns`, so it is the convenience wrapper, not
the fair benchmark retire path. Drivers that need per-sweep shared-`recv_ns`
fairness therefore keep the explicit `wait` loop. `result(recv_ns=...)` exists
precisely to feed that shared sweep timestamp in, and is only correct for an
already-ready future.

So: **do not conflate `submit_batch` (bulk submit), `as_completed` (ergonomic
drain), and benchmark `batch_wait` (windowed as-completed with shared per-sweep
`recv_ns`).** See `docs/reference/rayx_submit_batch.md` §2a and
`experiments/07_rayx_as_completed/rayx_as_completed.md`.

## 6. Shutdown contract

`shutdown()` is a **graceful drain**:

* Submitted work **completes** — queued and in-flight requests are drained, never
  cancelled or dropped. (The C++ side joins the lane threads, then stops HPX.)
  Shutdown latency can therefore scale with the outstanding queued service time.
* **Futures submitted before shutdown remain retirable afterward**: `ready()`
  returns `True` and `result()` retires the row as usual, even after the owning
  engine has been shut down.
* **New work after shutdown raises** (`Engine is shut down`): `submit`,
  `submit_batch`, `cancel`, `wait`, and `as_completed` (which goes through
  `wait`).
* **Shutdown itself never cancels or drops** queued/in-flight work — it drains.
  (Explicit `cancel()` is a separate verb, §7; a request already cancelled before
  shutdown still drains as `cancelled`.)

Context-manager exit and `SyntheticActor.shutdown()` both route to this same
drain; `__del__` drains best-effort and warns if the engine was never shut down
explicitly.

## 7. Cancellation contract (queued skip + chunk-boundary running stop)

`Engine.cancel(future) -> bool` (and `SyntheticActor.cancel(...)`, which forwards
to it) is **honest two-mode cancellation** — a queued skip, or an early stop at a
chunk boundary of a *running chunked* request. It is **not** interruption of
active work:

> A request can be cancelled if it is still **queued** (skip it entirely), or if
> it is a **running chunked** request with a chunk boundary still ahead (stop at
> the next boundary). An in-progress active chunk and an in-progress parked
> inter-chunk gap are **never** interrupted.

* **Returns `True`** when this call **settles** a cancellation:
  * **Queued → Cancelled**: the lane skips the request, **no service time is
    spent**, and the promise is fulfilled (by `cancel()` itself) with a
    `status="cancelled"` row whose `chunks_completed == 0`.
  * **Running (cancellable) → StopRequested**: the request has started a chunked
    service with at least one chunk boundary still ahead; the **lane** will stop
    at that next boundary and fulfill the `status="cancelled"` row there. `True`
    here means **guaranteed-to-stop, not ready-now** — the cancelled row appears
    only once the lane reaches the boundary, with `1 <= chunks_completed < chunks`.
* **Returns `False`** when no cancellation can be settled: the request already
  **completed**, a stop was **already requested**, it is a started **single-chunk**
  (`chunks=1`) request (no boundary exists), or it is already running its
  **final** chunk (the lane has committed to completion). Active work is **never
  interrupted**: there is deliberately **no cancellation check inside `sleep_for`
  / `spin_for`**, and none inside an in-progress parked gap — only *between*
  chunks.
* **Raises** for an already-retired Future (consumed via `result`/`get`), after
  engine `shutdown()` (consistent with every other engine operation), and for a
  **non-cancelable** Future. Only single-request `submit` / `remote` /
  `serve.remote` futures are cancelable; **batch-submitted futures are not**
  (there is no batch-cancel in this slice, and adding a token to every batch
  request would tax the bulk hot path for a feature scoped to single submits).

`cancel()` is **not a retire.** A cancelled Future still becomes ready and must
still be consumed exactly once via `result()` / `get()`, returning a
`status="cancelled"` measurement row that **preserves the submit-time `label`**.
So a cancelled future flows through `wait` / `as_completed` / `get` like any
other ready future. `future.cancelled()` reports the **settled** view: it can
become `True` as soon as a queued cancel or a running stop is requested, *before*
the row is ready (use `ready()` / `result()` for readiness/retirement).

**`chunks_completed` (lane-determined).** The result row carries how many active
chunks **actually ran**: `== chunks` on a normal finish, `0` on a queued cancel,
and `1 <= chunks_completed < chunks` on a running (chunk-boundary) cancel. Unlike
`chunks` / `chunk_delay_ms` (echoed from the Python copy), the client cannot know
where an early stop landed, so this field is carried on the **C++ `Result`**. It
is a **facade row field only** — the benchmark JSONL schema is unchanged (drivers
do not chunk, so it would be a constant `chunks`).

**State machine** (all transitions under the token's mutex, single-arbiter):

```
Queued        --cancel()-------------------> Cancelled      (cancel() fulfills now; chunks_completed = 0)
Queued        --begin_service(chunks)------> Running        (cancellable iff chunks > 1)
Running(cxl)  --cancel()-------------------> StopRequested  (cancel() True; LANE fulfills at the boundary)
StopRequested --lane reaches next boundary-> Cancelled      (lane fulfills; 1 <= chunks_completed < chunks)
Running       --lane finishes all chunks---> Completed       (lane fulfills; chunks_completed == chunks)
Running(final)--cancel()-------------------> (no transition; returns False)
```

`cancellable_` (`cxl` above) is armed by `begin_service(chunks)` **only when
`chunks > 1`** (a boundary exists), and the lane clears it — in the **same
critical section** that checks for a stop — right before committing to the
**final** active chunk. So a cancel that arrives after the lane has passed the
last boundary deterministically loses (`False`), and the promise is still
fulfilled exactly once (for a running stop the **lane** fulfills at the boundary;
`cancel()` does **not** fulfill).

**Race resolution.** All phase writes happen under a **per-request lock inside a
self-contained `CancelToken`**, not the lane mutex:

* `CancelToken::cancel()` (client side), `begin_service()` and
  `stop_at_boundary()` / `mark_completed()` (lane side) are the **only** writers
  of the token's phase, all under the token's mutex — **single-arbiter**.
  Whichever acquires the lock first at each decision point wins; exactly one of
  {serviced, queued-cancelled, boundary-cancelled} happens, and the promise is
  fulfilled exactly once (a queued cancel fulfills outside the lock so
  `set_value` never runs under it; a running stop is fulfilled by the lane).
* The token is **fully self-contained** — it owns its mutex, phase, and a copy
  of the request's promise, and holds **no pointer back into the lane**. So
  cancellation is safe even for a future whose owning lane/engine has already
  been shut down and destroyed: the token outlives the lane, and a stale cancel
  simply observes a terminal/non-cancellable phase and returns `False`. (Because
  the HPX runtime is a per-process singleton, cross-engine use is not a normal
  case; the self-contained token makes it memory-safe regardless.)
* Items **without** a token (the native baseline path) always service — the
  lane's hot path carries no cancellation state for opted-out requests, keeping
  the shared `service_lane.hpp` execution byte-identical for serviced work.

**Interaction with shutdown.** `shutdown()` still drains: queued-but-already-
cancelled items resolve as `cancelled` (the lane pops and skips them), a
running stop is honored at its boundary as the lane drains, and everything else
drains as serviced. Cancel **after** shutdown raises, like other new work.

**Why no public `Future.cancel()`.** Cancellation is engine/actor-mediated on
purpose: a bare `future.cancel()` reads like Ray task/object cancellation
(stop a task graph / drop a stored value). RayX does neither — it cancels one
synthetic request on an in-process lane (queued skip or chunk-boundary early
stop). Routing through `Engine.cancel(future)` keeps that scope visible.

**What running-cancel deliberately is NOT.** It is **not** mid-chunk
interruption (active work and parked gaps run to their boundary), **not** real
token-stream cancellation, **not** Ray task/object-store cancellation, and emits
**no** per-chunk events. It is an early stop *between* synthetic service steps —
the smallest honest cancellation the chunked model (§8) admits.

## 8. Chunked / streaming synthetic service

`Engine.submit(service_ms, chunks=1, chunk_delay_ms=0, work_mode)` (and the
`remote` / `serve.remote` forwards) model a **multi-step serving lifecycle** —
token-like repeated service steps — as **synthetic timing only**. It is **not**
real token streaming, not payload execution, and emits **no** per-chunk
rows/events/callbacks.

The model (smallest honest form):

* **`service_ms` stays the TOTAL active service time.** `chunks` (an `int` >= 1)
  splits it into that many **equal active steps** of `service_ms / chunks`, each
  consumed via `work_mode` (sleep parks, spin stays on-core — exactly as a single
  step). `chunks=1` is the unchunked default.
* **`chunk_delay_ms` is a PARKED inter-chunk gap** (a blocking sleep in **both**
  modes), inserted between consecutive chunks (`chunks-1` gaps). It models
  client-visible cadence/spacing, i.e. *waiting*, not active work.
* **One request → one `Future` → one final row.** A chunked request is still a
  single future, ready when its whole lifecycle completes, and flows through
  `wait` / `as_completed` / `get` / `result` like any other.
* **The lane is occupied for the whole chunked lifecycle** (active steps + parked
  gaps), so FIFO serialization is preserved — a chunked request holds the serving
  slot across its cadence, exactly like a single long request of the same span.

**`service_ms_observed` is lifecycle/lane-occupancy time.** Because `start_ns` /
`end_ns` bracket the entire lifecycle, `service_ms_observed = active service +
the chunks-1 parked gaps`. With `chunk_delay_ms = 0` it equals the unchunked
`≈ service_ms` (spin: exact); with `chunk_delay_ms > 0` it is **not** active-only
time. The row **echoes `chunks` and `chunk_delay_ms`** (facade metadata, parallel
to `label`), so the active vs delay split is recoverable (active `≈ service_ms`,
delay `≈ (chunks-1) × chunk_delay_ms`; note the parked gap carries the same
sleep-timer overshoot as `work_mode="sleep"`). **No `active_ms` field** and
**no JSONL schema-version change**: the benchmark JSONL already reserved `chunks`
/ `chunk_delay_ms`. Chunked service has since graduated into the **benchmark
drivers** too (all three expose `--chunks` / `--chunk-delay-ms` for single-submit
modes, populating those reserved fields; batch submit stays unchunked), so the
field is no longer facade-only — but the schema stays version `1`. The
`chunks_completed` result-row field (chunk-boundary running cancellation, §7)
remains **facade-only** and is not emitted into the benchmark JSONL.

**Scope — single-request only, by design:** `submit_batch` / `remote_batch` /
`serve.remote_batch` are **unchunked** and reject `chunks` / `chunk_delay_ms`
(unexpected keyword → `TypeError`). This is a **deliberate design boundary, not a
pending feature**: chunking is a single-request **lifecycle / serving-control /
cancellation** probe, while batch is a bulk **submission / throughput** probe.
Mixing them would blur the reading — a chunked-batch `total_ms` would combine bulk
queue drain with chunk-lifecycle lane occupancy — and batch futures are
**non-cancelable**, so the chunk-boundary running stop (§7) that gives chunks
their main serving-control purpose could never apply. Per-request service-time
**heterogeneity** is already served by varied batch (`submit_batch(service_ms=
[...])`, one crossing; §2). Validated at the Python boundary (`chunks`
int >= 1, non-`bool`; `chunk_delay_ms` finite >= 0, non-`bool`), the same
single-enforcement-point pattern as `work_mode`. **Cancellation** applies in both
modes (§7): a *queued* chunked request cancels (skips the whole lifecycle,
`status="cancelled"`, `chunks_completed == 0`, still echoing `chunks` /
`chunk_delay_ms`); a *running* chunked request stops at its next chunk boundary
(`1 <= chunks_completed < chunks`). The active chunks and parked gaps are never
interrupted mid-flight — the boundary is the only checkpoint.

## 9. HPX-native rationale

`Engine.wait` uses **`hpx::wait_some`** deliberately, over the nearer-looking
alternatives:

* **`hpx::wait_any`** only covers `k == 1`. `wait_some(k, …)` covers both
  wait-any (`k == 1`) and "wait for `k` of them" with one code path.
* **`hpx::when_any` / `when_some`** return a `future<…_result>` — a heavier
  composer that allocates a continuation and **moves the inputs into its
  result**. That is the wrong shape for a simple blocking partition: we want to
  block, then report which inputs are ready, while leaving every input future
  intact and owned by its Python `_Future`.
* **`shared_future`** would let multiple handles observe one result, but `rayx`'s
  ownership model is deliberately single-use (`result()` consumes; §3). Shared
  futures would add reference-counted multi-observer semantics the API does not
  want and would blur the once-only retire contract.

`wait_some` is the one combinator that is both **non-consuming** (all inputs
stay valid afterward) and **uniform across `num_returns`**, which is exactly the
"block, then partition the same futures" primitive `wait` needs. (Implementation
detail kept minimal here: the move-out-and-RAII-restore is only because
`hpx::future` is move-only and the inputs live in separate pybind objects, not
contiguously.)

### Decision: `timeout` endpoints shipped; finite-positive timeout deferred (HPX v1.11.0)

`Engine.wait` takes a `timeout` (seconds), but only the two **endpoints** are
supported on HPX v1.11.0; a finite **positive** timeout is deferred.

* **`timeout=None` (block)** and **`timeout=0` (non-blocking poll)** are both
  shipped. The poll needs no timed primitive at all — it just probes each future's
  existing non-consuming `ready()` and partitions, so it is a pure-Python facade
  addition (no C++ change) and reuses the blocking path's structural guards (§4).
* **`timeout > 0` (finite)** raises `NotImplementedError`. A bounded finite
  timeout (the `ray.wait` timeout analog — bound only the wait call, no cancel, no
  dropped work, timed-out futures still valid and drainable) needs a **non-consuming
  timed multi-future wait**, which HPX v1.11.0 does not provide:
  * HPX v1.11.0 has **no `hpx::wait_some_for` / `wait_some_until`** and no timed
    overload of `wait_some` (verified by header inspection and a compile probe).
  * Per-future **`future.wait_for` / `wait_until`** is non-consuming but is **not
    enough**: it cannot wake on *whichever* future in a set becomes ready first, so
    it cannot express the wait-any / wait-some semantics `wait` needs (a per-future
    timed loop can block to the full timeout even after the target count is
    already satisfiable).
  * **`when_some`** and other composer-style alternatives **move/own the inputs**
    and, on timeout, would leave them stranded in a still-pending composite future
    — incompatible with the ownership / RAII-restore model above (the same reason
    `wait` chose `wait_some` over `when_*`).
  * **Busy-polling is intentionally avoided** (it reintroduces the GIL/poll
    artifact `wait` exists to remove).

The only correct path for the finite case would be **bespoke synchronization** (a
continuation-fed latch, effectively re-implementing `when_some` non-consumingly) —
real new concurrency machinery, not a small slice. The finite-timeout path may be
revisited if a future HPX adds a suitable timed `wait_some` primitive, or if a
workload with genuine stragglers justifies that bespoke synchronization; until
then `timeout=0` (poll) plus `timeout=None` (block) cover the readiness/coordination
need without it.

## 10. Validation / contract coverage

The frontend contract is locked by shape-only smokes, not timing thresholds:

* **`bench/smoke_rayx.py`** exercises the full public façade over a built `_rayx`
  and asserts the *shape* of what comes back: import, `hpx_smoke`, `Engine`
  submit / multi / `submit_batch`, `Future.ready` / `Engine.wait` /
  as-completed loop, `as_completed`, the double-`result` and `ready`-after-retire
  guards, `__repr__` tokens, the `wait` negative contracts (empty / out-of-range
  `num_returns` / wrong type / retired / duplicate / post-shutdown), the
  post-shutdown drained-future retire contract, **cancellation** — *queued*
  (queued → `True` + `status="cancelled"` + `chunks_completed == 0` + label
  preserved; already-serviced → `False`; flow through `wait` / `as_completed` /
  `get`; actor forwarding; retired / post-shutdown / batch-future cancel raise;
  unrelated futures unaffected) and *running chunk-boundary* (started chunked w/
  boundary → `True` + `cancelled()` settles + `status="cancelled"` +
  `1 <= chunks_completed < chunks` + label + consume-once; started `chunks=1` →
  `False`, completes; flows via `get`) — **chunked synthetic service** (default
  `1`/`0` unchanged; chunked single request → one row echoing `chunks` /
  `chunk_delay_ms` + label; delay structural; actor/serve forward; queued cancel
  preserves `chunks` / delay; bad input and batch chunk kwargs rejected),
  **bounded wait** (`timeout=0` non-blocking poll → ready/pending partition,
  non-consuming, cancelled-queued ready, running-cancel pending-until-boundary,
  `num_returns` + duplicate/type/retired/empty guards; `timeout=None` unchanged;
  finite `> 0` → `NotImplementedError`; negative / `NaN` / `inf` / `bool` /
  non-numeric rejected; actor forwards), **`lane_stats()` observability** (one
  well-typed `{actor_id, queue_depth, active}` row per lane; fresh engine idle;
  a long occupier + queued work → some lane `active` with total `queue_depth > 0`;
  idle again after draining; actor forwards; post-shutdown raises), **bounded
  admission** (`max_queue_depth_per_lane`: ctor validation — `None` / positive
  `int` accepted, `0` / negative → `ValueError`, `bool` / `float` / `str` →
  `TypeError`; default `None` accepts a deep backlog unchanged; a full target
  lane → `QueueFullError` raised before any Future, leaving `queue_depth`
  unchanged; per-lane not global — a full lane does not block a non-full one;
  draining a slot reopens admission; a queued cancel does **not** free a slot
  until the lane pops it; `submit_batch` refuses under a cap with a plain
  `RuntimeError` while uncapped batch is unchanged; actor forwards; the
  post-shutdown `RuntimeError` takes precedence over `QueueFullError`), and the
  `SyntheticActor` façade.
* **`bench/smoke_local.py`** is the local smoke / golden / contract aggregator.
  It runs the gates available on the host and skips unavailable optional tiers
  (no built `_rayx`, no native binary, no Ray). It is not a benchmark matrix.
* **Native CI** (`Native RayX smoke`) builds HPX and `_rayx`, then runs
  `bench/smoke_rayx.py` plus the driver **retire-mode** smoke — a tiny no-op cell
  through both `--retire-mode one_by_one` and `--retire-mode batch_wait`, each
  parsed by the analyzer — so both retire paths stay runnable end to end. See
  `docs/experiment_plan.md` §"rayx contract + retire-mode gates".

## 11. Observability: `lane_stats()`

`Engine.lane_stats()` (and `SyntheticActor.lane_stats()`, which forwards to it)
is a small **debugging/observability** surface — a point-in-time view of the
service lanes — deliberately scoped so it cannot be mistaken for a control or
scheduling API.

* **What it returns.** One dict per `ServiceLane`, in **stable lane order** (the
  same order `submit` round-robins over): `{"actor_id": str, "queue_depth": int,
  "active": bool}`. `queue_depth` is **queued-but-not-started** (the in-service
  request is not counted); `active` is `True` from the moment the lane **pops** a
  request until that request's row is fulfilled (a queued-cancel skip also clears
  it), `False` when the lane is idle.
* **How it is implemented (and why it is cheap and honest).** Each lane carries
  one `std::atomic<bool> active_`, set under the lane mutex right after `pop` and
  cleared after `set_value` / a queued-cancel skip — **per request, off the inner
  sleep/spin (and chunk) loop**, so it adds nothing to the service hot path.
  `stats()` briefly takes the lane mutex to read `queue_.size()` and loads
  `active_`. `service_lane.hpp` gains only this flag + a `stats()` snapshot; the
  submit/service/cancel paths are otherwise unchanged.
* **Snapshot, can race.** The reading is a snapshot: a concurrent pop or
  fulfilment can change either field the instant after `lane_stats()` returns. It
  is **non-consuming** — it touches no promise/future and is safe to call at any
  time — but it is **not** a synchronization primitive; never gate correctness on
  it. (Use `wait` / `as_completed` / `ready` for readiness/coordination.)
* **What it is not.** Not Ray scheduler state; not placement control (lane choice
  stays internal round-robin — `actor_id` reports the serving lane after the
  fact, it is not a handle you submit to); not part of the benchmark JSONL /
  analyzer schema (it is a separate Python call, not a result-row field). It
  **raises** `RuntimeError` after `shutdown()` (the lanes are destroyed),
  consistent with the other new-work / coordination APIs.
* **Scope note.** `lane_stats()` reports the lanes of whichever backend the
  Engine was built with (`lane_impl`, §13): `ServiceLane` by default, or the
  opt-in cooperative `HpxLane` under `lane_impl="hpx"`. The snapshot shape is
  identical for both; the `actor_id` prefix (`act-hpx-` vs `act-hpxl-`) shows
  which. It does **not** cover the experiment-20 task/dataflow mechanism probe,
  which is a native-only experiment and not a rayx lane backend.

## 12. Bounded admission: `max_queue_depth_per_lane` + `QueueFullError`

`Engine(max_queue_depth_per_lane=None)` (forwarded by
`SyntheticActor(max_queue_depth_per_lane=...)`) adds an **optional, local,
per-lane admission cap**. It is deliberately the smallest honest mechanism that
protects a lane's backlog — admission **by rejection** — and is scoped so it
cannot be mistaken for Ray Serve, a scheduler, or distributed flow control.

* **What it is.** A per-lane bound on **queued-but-not-started** depth. The
  default `None` is **unbounded** and preserves the original behavior exactly
  (no admission check runs — the uncapped `ServiceLane::submit` path is
  byte-for-byte unchanged). A positive `int` `N` admits at most `N` queued
  requests per lane; the **active in-service request is not counted** (it has
  already been popped off `queue_`). When the round-robin **target lane** is at
  the cap, `Engine.submit` / `SyntheticActor.remote` / `actor.serve.remote`
  raise `QueueFullError`.
* **`QueueFullError`.** Python-owned, defined in `python/src/rayx/__init__.py`
  as a subclass of `RuntimeError` (no pybind exception registration). So
  `except RuntimeError` still catches it, while a load-shedding caller can catch
  `QueueFullError` specifically — and distinguish it from the plain
  `RuntimeError` raised after `shutdown()`.
* **Rejection is side-effect-free and Future-free.** The rejection is decided
  **before** anything is created: on a full lane there is **no promise, no
  Future, no cancel token, no queue entry, and no notify** — and therefore **no
  result row and no JSONL row**. The C++ `Engine::submit` returns Python `None`
  (a clean sentinel, not a stub Future) and the façade raises `QueueFullError`
  before constructing a `rayx.Future`.
* **Atomic check-and-enqueue (no TOCTOU).** Admission uses an additive
  `ServiceLane::try_submit(req, max_queue_depth, ...)` that performs the depth
  check **and** the queue push under **one** acquisition of the lane mutex, so
  the depth admitted against is exactly the depth observed. `lane_stats()` (§11)
  releases the lock before returning, so it is **observability only** and is
  **not** used as the gate — using it would reintroduce a read-then-act window.
  The uncapped path keeps calling `submit(...)` unchanged; `try_submit` is a
  separate capped-only method, so the native baseline and the uncapped rayx path
  carry none of the cap logic.
* **Per-lane, not global; no skip-to-another-lane.** The cap is **per-lane queue
  depth**, never a global backlog count, and a full lane is **never** skipped to
  a different lane: a submit targets exactly the one lane the round-robin picked,
  and rejects there if it is full. The round-robin counter `rr_` advances on
  **every** submit call — admitted **or rejected** — so call index `i` always
  maps to lane `i % num_lanes` and one saturated lane never shifts the rotation
  or stalls submissions to the other lanes. (`rr_` is Engine state, not a
  result-row field.)
* **Cancel does not free a slot until the lane pops it.** A queued cancel
  settles the future immediately but **leaves the item in `queue_`** (it does not
  dequeue), so a cancelled-but-not-yet-popped request **still counts** against
  the cap until the lane reaches it and pops/skips it. This is consistent with
  `lane_stats()` `queue_depth`, which likewise counts the still-present cancelled
  item.
* **Batch refuses loudly under a cap.** The batch paths (`submit_batch` /
  `remote_batch` / `serve.remote_batch`) are **not supported when a cap is set**.
  Rather than silently bypass the per-lane limit (the batch path is the
  multi-lane, non-cancelable bulk/throughput probe), they **refuse loudly** with
  a plain `RuntimeError` — **not** `QueueFullError`, because no lane is "full";
  the operation is simply unsupported under a cap. v1 deliberately does **not**
  implement all-or-nothing batch preflight admission; use `submit()` under a cap,
  or build the Engine without `max_queue_depth_per_lane` for batches.
* **Validation.** `None` and a positive `int` are accepted. `0` and negatives
  raise `ValueError`; `bool` (an `int` subclass), `float`, `str`, and other types
  raise `TypeError`. `None` maps to the C++ sentinel `-1` (unbounded); the
  positive int is passed through. Constructor rejections raise inside `__init__`
  **before** the HPX runtime starts, so a bad cap leaves no active engine.
* **Precedence.** The running-engine guard fires first: after `shutdown()`,
  `submit` raises the "Engine is shut down" `RuntimeError`, **not**
  `QueueFullError`.
* **What it is not.** Not Ray Serve and not Ray Serve backpressure; not
  distributed flow control; not a scheduler or placement control (lane choice
  stays internal round-robin); and **not blocking backpressure** — the call
  returns immediately by raising, it never blocks waiting for space. It is a
  local, in-process, per-lane admission-by-rejection cap on synthetic service
  requests, nothing more.

## 13. Lane backend selection: `lane_impl` (`"std"` / `"hpx"`)

`Engine(lane_impl="std")` (forwarded by `SyntheticActor(lane_impl="std")`)
selects the **lane mechanism** behind every lane, beneath the same Ray-like API.
It is an opt-in mechanism choice, not a behavior change: both backends implement
the **identical RayX lane contract** and leave the result-row / JSONL schema
untouched. For the consolidated evidence arc across exp16/20/21/22/23, see
`docs/reference/hpxlane_backend_arc.md`.

* **`"std"` (default) — `rayhpx::ServiceLane`.** The `std::thread` lane that has
  been the project's **stable comparison anchor** throughout. It remains the
  default, so every existing measurement, example, and benchmark keeps its
  current behavior; selecting `"std"` explicitly is behavior-equivalent to the
  default.
* **`"hpx"` (opt-in) — `rayhpx::HpxLane`.** The cooperative **HPX-thread** lane:
  its worker runs as an `hpx::thread`, its queue is guarded by `hpx::mutex` /
  `hpx::condition_variable_any`, and the inter-chunk parked gaps use the
  cooperative HPX timer. This is the same cooperative-lane mechanism first
  explored as a native-only probe in experiment 16, now reachable as an opt-in
  rayx backend.
* **Same contract, both backends.** FIFO per-lane ordering; stable `actor_id`;
  `lane_stats()` `queue_depth` (queued-but-not-started) and `active` (in-service)
  (§11); bounded admission (`max_queue_depth_per_lane` + `QueueFullError`, §12,
  via a cancel-token-aware `try_submit`); queued cancellation (skip before
  service); running cancellation at a **chunk boundary** (strictly-partial run,
  §7); and `wait` / `get` / `as_completed` retirement. Both reuse the **one**
  shared `Request` / `Result` / `CancelToken` and synthetic-service semantics, so
  cancellation and timing fields mean the same thing on either lane.
* **Visible only through the `actor_id` prefix.** `std` / `ServiceLane` lanes
  report `act-hpx-…`; `hpx` / `HpxLane` lanes report `act-hpxl-…`. That prefix is
  the only observable difference — result rows are otherwise schema-identical, so
  the choice is invisible to the analyzer and JSONL.
* **The backend seam (rayx-local, no HPX leak).** The Engine holds
  `std::vector<std::unique_ptr<RayxLaneIface>>`; a `RayxLaneAdapter<Lane>` wires
  each backend to that interface. The `ServiceLane` adapter forwards calls
  **directly** (behavior-equivalent to the previous Engine-owns-`ServiceLane`
  code — the interface adds structure, not semantics). The `HpxLane` adapter
  **hops** each lane-state operation — construction, destruction, `submit`,
  `try_submit`, `submit_bulk`, `stats` — onto an HPX thread via
  `hpx::run_as_hpx_thread`, because `hpx::mutex` / `hpx::thread` may be touched
  only from an HPX worker (the Engine itself runs on the external Python thread).
  Cancellation needs no hop: `CancelToken` uses its own `std::mutex` +
  `hpx::promise`, so `Future.cancel()` stays on its existing path for both
  backends. `RayxLaneIface` is **separate** from the native benchmark `LaneIface`
  in `hpx_synthetic_baseline.cpp`, which is left untouched. **No HPX types cross
  into Python.**
* **Validation.** `lane_impl` must be a `str`; a non-`str` raises `TypeError`, an
  unrecognized string raises `ValueError`. Like the other constructor knobs, the
  rejection happens inside `__init__` **before** the HPX runtime starts.
* **Read-only accessor.** The validated string is stored on the `Engine`
  (`self._lane_impl`) and exposed by `Engine.lane_impl()` (forwarded by
  `SyntheticActor.lane_impl()`), returning `"std"` or `"hpx"`. It is a method for
  consistency with `num_lanes()`, and is pure constructor-echo **introspection** —
  it lets user code/logging confirm the backend *before* any submit, rather than
  inferring it from a row's `actor_id` prefix. It returns a plain Python `str`
  (never an HPX type), is **not** scheduler state, placement control, or part of
  the result-row / JSONL schema, and — unlike `num_lanes()` — does not cross into
  C++, so it remains valid after `shutdown()`.
* **What it is not.** **Not** the `hpx::async` task / `hpx::dataflow` mechanism
  probe from experiment 20: those pools are deliberately **not** drop-in rayx
  lane backends (a different execution model, kept as native mechanism
  experiments). Not Ray Serve, not a Ray object store, not real model inference,
  and **not** a general "HPX beats Ray" claim — `lane_impl` only swaps the
  in-process synthetic lane mechanism behind a fixed, narrow API. Whether `"hpx"`
  is faster or slower than `"std"` is an empirical, workload-dependent question
  this seam does not assert.
* **Evidence.** Contract parity between the two backends is verified in
  `experiments/21_rayx_hpxlane_backend_parity/`. Where they **structurally
  diverge under load** is characterized in
  `experiments/22_rayx_hpxlane_load_divergence/`: under parked **sleep** service
  both backends overlap (cooperative parking yields the HPX worker, so `HpxLane`
  overlaps even at `hpx_threads=1`), while under non-yielding **spin** —  a
  synthetic CPU-bound diagnostic mode — `ServiceLane` parallelism follows the
  OS/core count whereas `HpxLane` concurrency is bounded by the `hpx_threads`
  worker pool. That is a scheduling-**mechanism** difference recorded as
  observation only; exp22 gates structural facts and makes **no** speedup or
  "HPX beats Ray" claim. The **uncontended** per-call cost of the `HpxLane`
  adapter's `run_as_hpx_thread` hop (vs the no-hop `ServiceLane` path) is
  characterized in `experiments/23_rayx_hpxlane_adapter_hop_cost/` — a
  single-digit-to-tens-of-µs boundary cost (best approximated by `lane_stats()`),
  observation-only and not a faster/slower verdict.
