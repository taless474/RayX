# RayX Phase 1: Registered Native Operation API

**Status: design / modeling only.** This document refines **Phase 1** of the RayX
runtime problem model into a spec precise enough that a later implementation has
no ambiguity. It is **not** shipped API, **not** implementation, and **not** a
delivery commitment. It lives under `docs/design/` (exploratory) deliberately
apart from `docs/reference/` (stable contracts for shipped APIs). Nothing here is
implemented. All Python names below are **provisional design names**, not a
shipped surface.

> **Supersession note (Phase 1 shipped).** Phase 1 has since shipped as an
> *experimental* prototype in commit `9143d2d` — see
> [rayx_runtime_phase1_summary.md](rayx_runtime_phase1_summary.md). The "nothing
> here is implemented" / "provisional design names, not a shipped surface" framing
> is now **historical**: the design below was built essentially as specified, and
> the provisional names (`Runtime`, `RuntimeFuture`, `OperationResult`, the error
> classes, `submit_operation` / `get` / `wait` / `as_completed` / `lane_stats`)
> became the **actual shipped `rayx.runtime` surface**. This doc is preserved as
> **provenance** (the precise contract that was implemented), not rewritten.
> `docs/design/` stays **exploratory**; shipping Phase 1 does **not** promote this
> doc into a stable `docs/reference/` contract, and the runtime stays experimental.

This doc makes **no** Ray-replacement claim, **no** "HPX beats Ray" claim, and
**no** performance-superiority claim. The HPX mechanisms it chooses are chosen for
*fit*, not asserted as superior.

## 1. Scope banner

* **Local only.** No distributed localities, no remote dispatch.
* **Registered native operations, not HPX actions.** A Phase-1 operation is a
  local entry in a fixed C++ function registry, dispatched with `hpx::async` on
  the existing serving-control lane/executor. It is **not** an `HPX_PLAIN_ACTION`.
  The word *action* is reserved in this corpus for **HPX remote actions**, which
  belong to a later distributed phase (Phase 5), and is used below only when
  explicitly contrasting against that remote machinery.
* **No HPX actions/components, no ObjectRef, no object store, no arbitrary
  Python, no Python callables.** (See §14.)
* **No module-level `rayx.get` / `rayx.wait`, no global runtime.** All runtime API
  hangs off an explicit object (D5).
* **No changes to the current harness `Future -> row` semantics.** The shipped
  `Engine` / `SyntheticActor` / `Future` world is frozen for the purposes of this
  work.
* **No changes to the v1 benchmark JSONL / analyzer schema.** (D4.)

## 2. Relationship to existing docs

This doc refines **Phase 1 only** of the two Phase-0 design documents; read it
alongside them:

* [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) — the settled
  goal, locked decisions D1–D7, the 12-issue problem map, the phase order, and
  the out-of-scope list. Phase 1 there = "Registered native operation behind the
  existing lane" (issues 1, 6, 11; minimal 3).
* [rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md) —
  the HPX-native *how*: which HPX mechanism is the primary tool per phase and
  which Ray-shaped instinct to avoid. Phase 1 maps to principles **P1** (registry
  + `hpx::async` on an executor), **P2** (actions/components are remote
  machinery — not introduced locally), **P7** (cooperative cancellation), **P8**
  (errors via future exception propagation), and **P9** (rows stay separate from
  values; v1 schema frozen).

This doc adds nothing to Phases 2–5; it does not relitigate the locked decisions,
only translates the Phase-1 row of the problem model's mechanism table into a
named, testable shape. Where this doc and the Phase-0 docs appear to differ, the
Phase-0 docs and the project guardrails win.

## 3. Phase 1 goal

Run **one named, pre-registered native operation** behind the existing
serving-control lane, and return a **user value** plus the **existing measurement
row** as **two separate things**.

Concretely, the single load-bearing change Phase 1 models: the lane's service
step — today either a blocking `sleep` or an on-core `spin` for `service_ms` —
gains a third shape, "**execute registered operation `op_id(args)`**", which
captures a native return value and fills a measurement row carrying the **same
core timing/identity/status fields** as today — the harness-facade echoes
(`label` / `chunks` / `chunk_delay_ms` / `chunks_completed`) do **not** apply to
operations (see §9, §12). Phase 1 adds *only* this value-producing dispatch path.
Everything the shipped lane already provides is **reused unchanged**:

* FIFO ordering per lane,
* queued + cooperative (checkpoint-boundary) cancellation,
* bounded admission / `QueueFullError`,
* `lane_stats()` (`queue_depth` / `active`),
* a stable per-lane `actor_id`.

These lane *semantics* are reused via a separate, HPX-native `RuntimeLane`
(§11) — Phase 1 does **not** mutate the shipped `ServiceLane` / `HpxLane`, and the
runtime does **not** expose the harness's `lane_impl="std"` / `"hpx"` backend
selector (§6): the runtime lane is HPX-native by construction.

Phase 1 does **not** add an `ObjectRef`, an object store, dependency composition,
serialization, or any distributed surface. The value is returned **directly**
through a retire path (D3); no reference type is introduced.

## 4. Python API sketch (provisional)

The runtime lives in a **separate subnamespace**, `rayx.runtime`, never as a
top-level `rayx.Runtime`. This keeps the two frozen worlds — the harness
`Future -> measurement-row` world and the runtime `value + row` world — visibly
apart (D1, D7) and leaves `rayx.__all__` unchanged.

```python
from rayx.runtime import Runtime

with Runtime(num_lanes=2, hpx_threads=4) as rt:
    f = rt.submit_operation("square", 2)
    res = f.result()
    assert res.value == 4
    assert res.row["status"] == "completed"
```

Note there is **no** `lane_impl` argument: the Phase 1 runtime lane is HPX-native
by construction (see §6, §11), so there is no `"std"` / `"hpx"` backend selector
to choose.

Provisional design names (all subject to change before any implementation):

* **`Runtime`** — the explicit runtime object (§6). Constructor knobs are
  `num_lanes`, `hpx_threads`, and `max_queue_depth_per_lane` — **not** the
  harness `Engine`'s `lane_impl` selector (§6). Process-singleton, mutually
  exclusive with `Engine` in Phase 1.
* **`submit_operation(op_id: str, *args) -> RuntimeFuture`** — submit one
  registered operation by id with typed positional args; captures the Python-side
  `submit_ns` before the single Python->C++ crossing, exactly as `Engine.submit`
  does, then returns a handle.
* **`RuntimeFuture`** — a **distinct** handle type from the harness `Future` (it
  does **not** subclass it). Same **consume-once** contract: `.result()` once,
  second call raises; `.ready()` / `.cancelled()` are non-consuming; `__repr__` is
  non-consuming and never probes a retired handle.
* **`OperationResult`** — the retired payload returned by `RuntimeFuture.result()`,
  carrying `.value` and `.row` as **separate attributes on one object** (§9). Not
  a two-step "value then row" retrieval. Unlike `RuntimeFuture`, an
  `OperationResult` is **not** consume-once: `.value` and `.row` are idempotently
  re-readable.

The runtime also forwards the lane-coordination surface to the same underlying
lane contract, operating on `RuntimeFuture`s: `wait`, `as_completed`, `get`,
`cancel`, `num_lanes`, `lane_stats`, `shutdown`, and context-manager use. It does
**not** expose `lane_impl()` — there is no backend selector in the Phase 1 runtime
(§6). Runtime `get` returns `OperationResult`(s), not rows-only — that is the
runtime's deliberate distinction from the harness `Engine.get` (which returns
measurement rows).

There is **no** module-level `rayx.runtime.get` / `rayx.runtime.wait`, **no**
global default runtime, and **no** `@rayx.remote`-style decorator (D5, D10, §14).

## 5. Naming decisions

* **`operation`, not `action`.** An HPX action (`HPX_PLAIN_ACTION`) is
  AGAS-registered, parcel-marshalled remote-dispatch machinery that belongs to
  the distributed phase. Calling a *local* registry entry an "action" would
  collide with that meaning and invite the exact "future-proof with actions
  locally" inversion the principles doc (P2) warns against. "operation" is already
  the established term across both Phase-0 docs ("registered native operation").
* **`OperationResult`, not `ActionResult`.** Same reason — "action" is reserved
  for the remote phase. The retired value+row payload is an *operation* result.
* **`submit_operation`, not `run_operation`.** `submit_*` echoes the harness
  `submit` and makes "this returns a handle, it does not block" obvious; `run_*`
  reads as synchronous execution.
* **Why not `kernel` / `handler` / `action`:**
  * `kernel` implies a compute/numeric/GPU primitive — wrong altitude for a
    serving-control registry entry.
  * `handler` implies request-handling / serving semantics, which blurs into the
    lane (the lane *is* the serving-control story); the registry entry is the
    *work*, not the request dispatcher.
  * `action` is reserved for HPX remote actions (above).

## 6. Runtime object semantics

* **Explicit runtime object.** All Phase-1 runtime API hangs off a `Runtime`
  instance (D5). There is no implicit/global runtime and no module-level
  get/wait.
* **Process-singleton.** Like the harness `Engine`, a `Runtime` owns one HPX
  runtime, so only one active `Runtime` is allowed per process; constructing a
  second before `shutdown()` raises. Usable as a context manager (`__enter__` /
  `__exit__` graceful-drain shutdown), mirroring `Engine`.
* **Mutually exclusive with the current `Engine`.** Because HPX permits a single
  process runtime and the harness `Engine` already claims it as a process-
  singleton, a `Runtime` and an `Engine` cannot both be live in the same process
  in Phase 1: constructing one while the other is active raises, reusing the same
  single-runtime guard. Letting `Engine` and `Runtime` **coexist** over one shared
  runtime bootstrap is a later, more invasive refactor (see §15), explicitly out
  of Phase-1 scope.
* **Constructor knobs:** `num_lanes`, `hpx_threads`, and
  `max_queue_depth_per_lane` (default `None` = unbounded), validated with the same
  rules the harness already enforces (e.g. `max_queue_depth_per_lane` is `None` or
  a positive int). The Phase 1 `Runtime` does **not** mirror the harness `Engine`'s
  `lane_impl` knob — there is no `lane_impl="std"` / `"hpx"` selector.
* **The Phase 1 runtime lane is HPX-native by construction — there is no backend
  selector.** Unlike the harness `Engine` (which offers `lane_impl="std"`
  std::thread `ServiceLane` vs `lane_impl="hpx"` cooperative `HpxLane` as a
  *comparison* seam), the runtime exists *to be* HPX-native: it uses an HPX-thread
  FIFO lane and dispatches operation bodies via `hpx::async` on a chosen executor
  (§11). There is therefore nothing to select between, so:
  * Phase 1 exposes **no** `lane_impl` constructor argument, and
  * `Runtime.lane_impl()` is **not** part of Phase 1.
  A std::thread runtime lane would have no clean HPX-native dispatch story and
  would merely duplicate the harness's existing std-vs-hpx serialization
  comparison; it is **deferred** unless later evidence justifies a std runtime
  anchor. (The harness keeps its own `lane_impl` selector and tests, unchanged and
  separate.)

## 7. Operation registry

* **C++-side fixed built-ins only in Phase 1.** The registry is a native table
  (e.g. `std::unordered_map<std::string, OperationEntry>`) populated at extension
  init. An `OperationEntry` holds a typed native dispatcher plus arg-type metadata
  for boundary validation. There is **no Python-side registration** in Phase 1.
* **Phase 1 ships three locked built-ins:**
  * **`square(int) -> int`** — the milestone operation. A native functor
    `int square(int)`; submitting `("square", 2)` yields the value `4` plus the
    existing row.
  * **`add(int, int) -> int`** — included (not optional) to exercise
    multi-argument typed dispatch and **arg-arity** validation, which `square`
    alone cannot cover.
  * **`busy_sum(int) -> int`** — a **checkpointed native iterative** operation: it
    accumulates over `n` steps in native code and polls the cancel token at safe
    per-chunk checkpoints. This is **real native work returning a real value**
    (a bounded accumulator / checksum), **not** a synthetic sleep/service mode —
    it must never be implemented as `sleep(ms)`. Its purpose is twofold: it takes
    wall-time proportional to `n`, so it can **hold a lane** long enough to make
    **queued** cancel deterministic, and its checkpoints make **cooperative
    running** cancel deterministic (see §10/§10a, §13). **Slice 2a implementation
    (locked):**
    * **Value:** `busy_sum(n) = (Σ_{i=0}^{n-1} i) mod 2³¹`, accumulated iteratively
      with per-step masking (`acc = (acc + i) & 0x7FFFFFFF`). This is overflow-safe
      (`acc` stays `< 2³¹`) and deterministic; it equals the closed form
      `(n*(n-1)/2) mod 2³¹`, which the smoke uses to verify the value.
    * **`n >= 0` is validated at the Python boundary** (`ValueError` otherwise),
      matching the fail-fast-at-the-boundary discipline.
    * **Shared `STRIDE`.** A single `STRIDE` constant (in `runtime_ops.hpp`) drives
      *both* the checkpoint count `ceil(n / STRIDE)` (used to arm running-cancel,
      §10a) and the loop's chunk size, so the engine's count and the loop agree by
      construction.
    * **Loop mirrors the harness chunk loop** (`ServiceLane::service`): for each
      chunk `c` of `STRIDE` steps, if `c > 0` call `stop(next_is_final = c ==
      n_chunks - 1)` *before* the chunk's work; the final boundary clears
      cancellability before the last segment. On an honored stop it returns a
      `cancelled` outcome with no value.
* **Added after Phase 1 — `fanout_sum(int n, int parts) -> int`** (first
  internal-composition op). Value equals `busy_sum(n)` (`(n*(n-1)/2) mod 2³¹`),
  independent of `parts`; the body splits `[0, n)` into `parts` contiguous ranges,
  launches each masked partial with `hpx::async`, and combines with
  `hpx::when_all(...).get()` + a masked fold (P1 launch-all). Validation:
  `n >= 0`, `1 <= parts <= 1024` (`FANOUT_PARTS_MAX`); `parts > n` is allowed (empty
  trailing ranges contribute 0). Because it uses HPX in its body, its entry lives in
  a new HPX-side header `runtime_ops_hpx.hpp` (the HPX-free `runtime_ops.hpp` keeps
  only pure helpers — `FANOUT_PARTS_MAX`, `masked_range_sum`, `fanout_sum_checkpoints`),
  and `_rayx.cpp` merges `op_arities()` with `hpx_op_arities()` for the Python table.
  `checkpoint_count == 1`, so it is **queued-cancelable only** (an active `cancel()`
  returns `False`). See
  [rayx_runtime_internal_composition_note.md](rayx_runtime_internal_composition_note.md).
* **No Python-side registration, no Python callables.** There is **no** code path
  that accepts a Python callable, lambda, pickled closure, or source string into
  the registry. Only a `str` op id and scalar args cross the boundary; the
  dispatcher resolves a **pre-compiled native** functor by id. This is structural,
  not a runtime check — it is the same guarantee the harness gives today (the lane
  runs only native synthetic work, never arbitrary Python). The Python name
  `register_operation` is **reserved** for a later phase's shape
  (`rt.register_operation(...)` on the explicit runtime, never a global
  `@rayx.remote`) and is **not** an entry point in Phase 1.
* **Unknown op id rejected at the Python boundary.** A `submit_operation` with an
  unregistered id raises `ValueError` **before** the C++ crossing and **before**
  any `RuntimeFuture` is created — mirroring how `work_mode` is validated at the
  boundary and how bounded admission raises before building a `Future`.
* **Wrong arg count / type rejected at the Python boundary.** Arg arity and types
  are validated against the registry's metadata before the crossing; a mismatch
  raises `TypeError` (wrong type / non-supported type) or `TypeError` /
  `ValueError` (wrong arity), so a bad call fails fast at the boundary instead of
  returning a `status="failed"` row. Phase 1's closed arg type set is **`int`
  first**; `float` / `bytes` are noted as later (P6) additions, not Phase-1
  surface.

## 8. C++ / pybind shape

* **Same `_rayx` extension, not a separate `_rayx_runtime.so`.** The runtime
  reuses the existing extension's HPX-runtime bootstrap. A second compiled
  extension would duplicate/conflict that single-process runtime bootstrap; the
  single-runtime constraint (§6) is the deciding factor.
* **Additive runtime bindings/classes only.** New bound types (e.g. an internal
  `_RuntimeEngine` / `_RuntimeFuture`, or a `_rayx.runtime` pybind submodule),
  surfaced in Python under `rayx.runtime`. They are **additive**.
* **Do not mutate the frozen `_Engine` / `_Future` / row bindings.** The shipped
  harness binding surface and the C++ `Result` struct that feeds the v1 row are
  not edited. The runtime path captures its value **alongside** the existing
  `Result`, never by adding a field to it.
* **The registry dispatches a fixed native functor.** Lookup by op id returns the
  native dispatcher; it runs in the lane's service slot via `hpx::async` on the
  lane/executor (§11), and its result is delivered through an `hpx::future`.
* **Operations receive an HPX-free `StopCheckpoint` predicate (Slice 2a).** The op
  signature is `OpOutcome(const args&, const StopCheckpoint& stop)` where
  `StopCheckpoint = std::function<bool(bool next_is_final)>`. A checkpointed op
  (`busy_sum`) calls `stop(...)` at its chunk boundaries; non-checkpointed ops
  (`square` / `add` / `boom`) ignore it. The predicate is **bound by the lane**,
  not the op: the lane wires `stop` to the cancel token's `stop_at_boundary` **and
  performs the cooperative `hpx::this_thread::yield()`** there. This keeps
  `runtime_ops.hpp` (registry + `busy_sum`) **HPX-free** — no HPX header, no
  `yield`, no token type leaks into the registry — while the cooperative-scheduling
  yield (§11b) and the cancel poll live together in the HPX-aware lane.
* **Value captured separately from the row.** The native return value is captured
  into the `OperationResult`'s `value`; the row carries the core measurement-row
  fields (§9), filled with the same timing computation as today. The two never
  share a dict (§9, P9).

## 9. Result model

* **`RuntimeFuture.result()` is consume-once; `OperationResult` is not.** Retiring
  consumes the *handle*: `RuntimeFuture.result()` may be called only once, and a
  second call raises (same guard as the harness `Future`). `.ready()` raises after
  retire; `.cancelled()` stays valid (non-consuming) before and after retire. The
  returned `OperationResult`, by contrast, is a plain retired record — **not**
  consume-once: `.value` and `.row` are idempotently re-readable any number of
  times.
* **`result()` always returns an `OperationResult` — for completed, failed, and
  cancelled outcomes alike.** It does **not** raise on a failed or cancelled
  operation *outcome*; the row is always produced so it is always inspectable.
  This mirrors the frozen harness, where `Future.result()` returns a
  `status="cancelled"` / `status="failed"` row rather than raising. `result()`
  raises **only** for **structural misuse** — a second `result()` on the same
  handle (consume-twice), use after `shutdown()`, or a wrong handle/type — never
  for the operation's own failed/cancelled status.
* **`OperationResult.value` is the user value** (e.g. `4` for `square(2)`).
* **`OperationResult.row` carries the core measurement-row fields** — exactly
  `actor_id`, `submit_ns`, `start_ns`, `end_ns`, `total_ms`, `queue_wait_ms`,
  `service_ms_observed`, `status`, `error` — each with the **same name and timing
  semantics** as the harness `Future.result()` row. The runtime row deliberately
  **omits the harness-facade echoes** (`label`, `chunks`, `chunk_delay_ms`,
  `chunks_completed`): those are synthetic-service / client-annotation concepts
  that do **not** apply to a registered operation, and faking them would
  misrepresent operation execution. The runtime row is therefore a **strict
  subset, with identical semantics**, of the harness row's keys. `row["status"]`
  uses the existing vocabulary **only**: `"completed"` / `"failed"` /
  `"cancelled"`. No new status string is introduced. (`"ok"` is a
  smoke/health-check value used by `hpx_smoke`, **not** a row status; runtime rows
  never use it.) `.row` is **always safe to inspect** for every outcome. The
  frozen v1 benchmark JSONL/analyzer schema is unaffected because runtime rows
  never enter JSONL (§12).
* **Never merge value into row; no `value` key in row.** Value and row are kept
  strictly separate (D1, P9). The row must not gain a `value` key, and the value
  must not be inferable from the row.
* **On failure, `.value` raises (not `result()`).** An operation throwing in C++
  surfaces through its `hpx::future` (P8); the runtime maps it to an
  `OperationResult` whose `row["status"] == "failed"` with `row["error"]`
  populated. `result()` still returns that `OperationResult`; reading `.value`
  raises `OperationFailedError` (idempotently, every read), rather than returning a
  bogus value. Reading `.row` is always safe.
* **On cancel, `.value` raises (not `result()`).** A cancelled request retires with
  `row["status"] == "cancelled"` (queued cancel: nothing ran; cooperative running
  cancel: stopped at a checkpoint). `result()` still returns the `OperationResult`;
  reading `.value` raises `OperationCancelledError`; reading `.row` is safe.
  Cancellation does not produce a value.

### 9a. Error classes

A small, closed hierarchy, all in the `rayx.runtime` namespace:

* **`RuntimeOperationError(RuntimeError)`** — base for value-read failures.
  Subclassing `RuntimeError` mirrors the harness `QueueFullError(RuntimeError)`
  precedent, so existing `except RuntimeError` still catches the family and a
  caller can catch the whole group at once.
* **`OperationFailedError(RuntimeOperationError)`** — raised by `.value` when the
  operation's `row["status"] == "failed"`; carries the `row["error"]` message.
* **`OperationCancelledError(RuntimeOperationError)`** — raised by `.value` when
  `row["status"] == "cancelled"`. Distinct from the failed case so callers can
  treat "I cancelled this" differently from "it threw".

These typed errors are raised **only** on a `.value` read of a non-completed
result. **Boundary validation keeps the builtin error types**, matching every
existing `_validate_*` helper: an **unknown operation id** raises `ValueError`,
and a **wrong arg type / arity** raises `TypeError` (or `ValueError` for arity) —
**not** a `RuntimeOperationError`. This slice deliberately introduces **no** wider
`RayXError` umbrella base (the harness has none today; an umbrella would be a
separate cross-cutting refactor touching the harness too).

### 9b. `rt.get([...])` over multiple futures

* **Collect, do not fail-fast.** `rt.get(futures)` returns a `list[OperationResult]`
  in **input order**, retiring each `RuntimeFuture` exactly once. It does **not**
  raise on a failed/cancelled operation *outcome*; per-element `.value` raises the
  typed error only when that element is accessed.
* **Why collect over fail-fast:** it preserves *both* invariants better.
  (1) *Consume-once* — a fail-fast raise on the first failed result would leave the
  remaining futures un-retired (leaking lane/promise state) or half-retired;
  collecting guarantees every input future is retired exactly once.
  (2) *Row-observability* — a batch where one element failed still surfaces rows
  for all the others, instead of discarding them. This also matches the harness
  `Engine.get` (input-order list, no fail-fast).
* **Structural misuse still raises**, exactly as single `result()` — e.g. a future
  already retired, or use after `shutdown()`. An all-or-nothing check is the
  caller's to make (`all(r.row["status"] == "completed" for r in results)`); no
  all-or-nothing mode is baked into Phase 1.

### 9c. Collection-API concurrency (accepted non-guarantees)

The collection APIs are designed for a **single driver** that owns the runtime and
coordinates its own `RuntimeFuture`s. Two limitations are accepted by design, and
they match the harness exactly (they are not specific to the runtime):

* **Per-future access is not thread-safe.** `Runtime.wait()` is non-consuming and
  safe for normal single-driver use. Its blocking path briefly moves the underlying
  HPX future out of the `RuntimeFuture` during the GIL-released `hpx::wait_some`, so
  you must **not** call `ready()` / `result()` on the *same* `RuntimeFuture` from
  another Python thread while it is inside `wait()`. `RuntimeFuture.cancel()`
  **remains safe** under this race — it operates on the independent runtime cancel
  token (§10a), not on the future.
* **Concurrent shutdown during a blocked wait is unsupported.** Calling
  `shutdown()` from another thread while a `wait()` / `result()` is blocked is not
  supported (it could stop the HPX runtime out from under the blocked wait). Normal
  usage is a single driver controlling the runtime lifetime — the same limitation
  the harness `Engine` carries.

These are coordination-usage notes only; they do not change the API contract, the
result/row model (§9, §9b), or the cancellation semantics (§10).

## 10. Cancellation

Phase 1 reuses the shipped `CancelToken` state machine wholesale (P7); it adds no
new cancellation mechanism.

* **Queued cancel.** A request not yet started is skipped by its lane; it retires
  with `status="cancelled"` and no value. To make this **deterministic**, a
  long-running `busy_sum(n)` is submitted first to hold the lane, so a request
  queued behind it is still queued when cancelled (the instantaneous `square` /
  `add` cannot reliably hold a lane).
* **Cooperative running cancel.** A native operation that polls the `CancelToken`
  (a `should_stop()` predicate) at safe checkpoint/chunk boundaries will stop at
  the next boundary; it retires `status="cancelled"`. An in-progress
  non-checkpoint segment is **never** interrupted. Phase 1 ships `busy_sum` as the
  checkpointed built-in precisely so cooperative running cancel is **deterministic
  and tested in Phase 1** (cancel a `busy_sum` mid-loop; it stops at the next
  per-iteration checkpoint). `square` / `add` are effectively instantaneous, so
  only the **queued** cancel path is observable for them.
* **Operation author contract.** To be cancellable mid-run, a native operation
  must accept the `CancelToken` (or `should_stop()` predicate) and poll it at safe
  checkpoints. Operations that do neither are cancellable only while queued.
* **No forced kill of non-cooperative native code.** HPX cannot safely abort a
  thread that never reaches a suspension/interruption point; this is a stated
  limitation (and the *reason* operations are registered + cooperative rather than
  arbitrary Python), not a gap to fill.
* **Cancellation does not delete values or handles.** There is no value handle to
  delete in Phase 1; cancel only settles the request's outcome.

### 10a. Runtime cancellation mechanics (Slice 2a)

* **Runtime-local token mirror, typed on `RuntimeResult`.** The runtime cannot
  literally reuse `rayhpx::CancelToken` — that class fulfills an
  `hpx::promise<rayhpx::Result>`, and the runtime's result type is
  `rayx_runtime::RuntimeResult`. Slice 2a adds a **faithful mirror**
  (`rayx_runtime::RuntimeCancelToken`, in `runtime_cancel.hpp`) of the proven
  state machine — `Queued → Running → {StopRequested → Cancelled | Completed}`,
  plus the queued `Queued → Cancelled` — typed on `RuntimeResult`. It uses a
  `std::mutex` (no atomics / lock-free), keeps every critical section to a
  nanosecond phase flip, **sets the promise outside the lock**, and holds **no
  pointer back to the lane**. Because it is a `std::mutex` + a copy of the promise,
  `RuntimeFuture.cancel()` runs on the external Python thread **with no HPX hop**
  (exactly as the harness `CancelToken` does behind `HpxLane`).
* **Running-cancellability is armed by checkpoint *count*, not a flag.**
  `begin_service(checkpoint_count)` sets `cancellable_ = (checkpoint_count > 1)`.
  The count is computed from the op's args at submit time (see §7): `square` /
  `add` / `boom` are `1`; `busy_sum(n)` is `ceil(n / STRIDE)`. A
  **`checkpoint_count == 1`** operation (every instantaneous op, and a short
  `busy_sum(n ≤ STRIDE)`) is therefore **queued-cancelable only** — a `cancel()`
  that arrives once it is already running returns `False`. This is what makes the
  state machine race-free: a checkpointed op clears `cancellable_` at its final
  boundary *before* its last segment, so a late cancel deterministically loses
  rather than returning `True` while the op completes. (Using a plain
  `checkpointed` bool would let a short checkpointed op return `cancel() == True`
  and then retire `completed` — the exact race this count-based arming prevents.)
* **Shutdown cancels outstanding runtime work before draining.** Unlike the
  harness, which **drains queued work to completion** on teardown, the runtime
  **cancels** its outstanding operations first: `RuntimeEngine.shutdown()` calls a
  lane-level `cancel_pending()` (cancel every queued token and the tracked
  in-flight token) and *then* `stop_and_join()`. Queued cancelled items skip; a
  running `busy_sum` exits at its next checkpoint; every promise is still fulfilled
  (as `cancelled` or `completed`), so no future breaks. This is an **intentional
  difference from the harness**, justified because registered operations can be
  long-running: it bounds teardown latency to **one checkpoint stride** instead of
  the full remaining operation. The cancel/drain/join runs inside one
  `run_as_hpx_thread` hop with the **GIL released** (the drain may block on a
  checkpoint); lanes are cleared **before** `stop_process_hpx()`, and
  `process_runtime_active()` is cleared **last**.

### 10b. Bounded admission (Slice 2b)

* **`Runtime(max_queue_depth_per_lane=None)`.** `None` (default) = unbounded;
  a positive int = a **per-lane** cap on queued-but-not-started operations. Validated
  at the Python boundary (`None` or positive int; `0`/negative -> `ValueError`;
  `bool`/`float`/`str` -> `TypeError`), passed to C++ as a `-1` sentinel for `None`.
* **Per-lane, never global.** The cap applies to each `RuntimeLane`'s own queue
  independently; filling one lane does not reject another. Round-robin still advances
  on a rejected submit (`rr_` is engine state), so a full lane does not shift the
  rotation for later calls -- matching the harness `Engine` admission behavior.
* **Cap counts queued items only.** It is checked against `queue_.size()` (the
  queued-but-not-started count). The in-service (popped) item is **not** counted. A
  **cancelled-but-not-yet-popped** queued item **is** still in `queue_`, so it still
  counts toward the cap **until the worker pops/skips it** -- `cancel()` settles the
  outcome but does not remove the item from the queue or free its admission slot.
* **TOCTOU-free admission.** `RuntimeLane::try_submit` checks `queue_.size() < cap`
  and pushes the item under **one** `hpx::mutex` acquisition, so the depth admitted
  against is exactly the depth observed. `lane_stats()` (which releases the lock
  before returning) is **not** the gate. Mirrors `rayhpx::HpxLane::try_submit`.
* **A rejected submit is a side-effect-free no-op.** On reject the lane creates **no
  promise, no future, no cancel token, no queue entry, and issues no notify**; the
  C++ path returns `None` and `Runtime.submit_operation` raises
  `rayx.runtime.QueueFullError`. **No `RuntimeFuture` is created**, and the lane's
  `queue_depth` is unchanged.
* **`QueueFullError(RuntimeError)` lives in `rayx.runtime`.** It is a **distinct**
  class from the harness top-level `rayx.QueueFullError` (not imported or aliased
  across the boundary), exported via `rayx.runtime.__all__`; subclassing
  `RuntimeError` matches the harness precedent so `except RuntimeError` still catches
  it. (Bounded admission is the **only** new `__all__` surface in Slice 2b.)

## 11. Lanes / scheduling / HPX mechanism

* **A parallel, HPX-native `RuntimeLane` — not the harness lanes.** Phase 1 does
  **not** mutate `ServiceLane` / `HpxLane` / `Request` / `Result` or the native
  baseline. It adds a separate `RuntimeLane` that **reuses the lane semantics**
  (FIFO service slot, queued + cooperative cancellation, bounded admission,
  `lane_stats`) while its service slot runs a registered operation rather than
  synthetic sleep/spin.
* **FIFO** per lane, **round-robin** lane selection, **bounded admission /
  `QueueFullError`**, and **`lane_stats()`** (`queue_depth` / `active`) carry the
  same meaning as the harness.
* **HPX-native by construction — no `lane_impl` selector.** The `RuntimeLane` is an
  **HPX-thread** lane (a single HPX-thread worker over an HPX-synchronized FIFO
  queue), not a std::thread lane. There is no `lane_impl="std"` / `"hpx"` choice
  for the runtime (§6): the runtime *is* the HPX-native path, so there is nothing
  to select. The harness keeps its own `lane_impl` comparison seam, separate and
  unchanged.
* **Dispatch operation bodies via `hpx::async` on a chosen executor** (P1, P4):
  inside the serialized service slot the worker invokes the operation through
  `hpx::async(executor, ...)` and cooperatively `.get()`s the resulting
  `hpx::future`. Because the worker is an **HPX thread**, that `.get()` is a
  **cooperative HPX suspension** (the scheduler runs other work while the worker is
  parked), **not** a std::thread block — and FIFO holds because the single worker
  retires one operation before popping the next. The `executor` argument is the
  **P4 placement seam** (default pool in Phase 1; named pools such as `control` /
  `work` are a later, illustrative axis — see principles P4).
* **Distinct `actor_id` prefix: `rt-hpx-`.** The runtime's `actor_id` is
  `rt-hpx-` + 8 lowercase hex chars (e.g. `rt-hpx-9f3a1c07`), generated by a
  runtime-local helper — it does **not** reuse the harness prefixes `act-hpx-`
  (ServiceLane) / `act-hpxl-` (HpxLane). Because `rt-` is a different namespace
  from `act-`, runtime ids are never confusable with harness lane ids. There is no
  `rt-hpxl-` variant: the runtime has a single HPX-native lane type, not the
  harness's std/hpx split. **The `rt-hpx-` prefix is stable from Slice 0 into
  Slice 1**: in Slice 0 it is a single `Runtime`-level id; in Slice 1 it becomes
  the per-`RuntimeLane` id (the serving lane's id flows into the row, mirroring the
  harness), with the same prefix — so any test asserting the prefix survives the
  transition. This prefix is also the structural "HPX-native path" signal (§13).
* **No client placement.** Lane/pool choice stays internal (round-robin default),
  statically resource-partitioned at construction (`hpx_threads` -> pool worker
  count). There is **no** client-selected lane/locality.
* **No scheduler clone.** Phase 1 builds no dynamic per-task placement scheduler
  and exposes no affinity/resource-spec/placement-group surface (P4).

### 11a. Why a worker+queue lane, not a continuation chain (Slice 1 decision)

An HPX-native serialized lane could instead be a **per-lane continuation chain** —
a `tail` future extended per submit (`tail = hpx::async(exec, [tail]{ tail.get();
run(); })`), with no worker thread and no condition variable; serialization falls
out of the dataflow dependency. **Slice 1 deliberately does not use this.** The
chain serializes correctly, but it has no *owned queue* to observe or control, which
is exactly what a serving-control lane needs:

* **Observability.** `lane_stats()` `queue_depth` / `active` come from the worker's
  own deque under one `hpx::mutex` (a coherent snapshot). A chain has no queue;
  depth would be a side counter that drifts from real scheduler state.
* **Slice 2 admission.** Bounded admission is a check-and-push under the lane mutex
  against the real queue size; a chain would admit against a notional counter.
* **Slice 2 queued cancellation.** A queued item sits in the lane's deque and can be
  skipped *before* it starts; a chained continuation is already handed to the
  scheduler and cannot be cleanly skipped or have its slot reclaimed.
* **Semantic boundary.** RayX lanes model **serving-control** (queue, service slot,
  drain, admission, cancel). A continuation chain models a **task-graph dependency
  edge**. Adopting it would blur serving-control into task-graph semantics — the
  exact conflation this project keeps separate — and re-open the dataflow-edge shape
  experiment 20 already found is *not* a drop-in lane backend.

The chain's only advantage (the link's own future is the caller's future, so no
explicit promise) is minor. Slice 1 uses an explicit `hpx::promise<RuntimeResult>`
— the local, in-process promise, the same type the harness `HpxLane` uses — bridged
from the enqueue-time future to worker-side fulfillment.

**Teardown is explicit and HPX-thread-safe.** A `RuntimeLane` destructor never locks
the `hpx::mutex` or joins the worker `hpx::thread` (it may run on a non-HPX thread).
Instead `RuntimeLane::stop_and_join()` (set `stop_` under the mutex → `notify_all`
→ cooperative `join`) is called for every lane inside **one** `run_as_hpx_thread`
hop, which then clears the lanes — **before** `hpx::finalize` / `hpx::stop` — and
`process_runtime_active()` is cleared **last**. The worker drains its queued items
(servicing each, fulfilling each promise) before exiting, so no pending
`RuntimeFuture` is left with a broken promise. Partial construction failure tears
down the already-built lanes the same way, stops HPX, clears the guard, and
rethrows.

### 11b. Operation author contract (cooperative scheduling)

Because operation bodies run on HPX worker threads, registered operations **must be
cooperative**:

* No non-cooperative blocking on an HPX worker — any wait must yield the worker.
* No `std::this_thread::sleep_*`; use `hpx::this_thread::sleep_for` if a wait is ever
  needed.
* No long `std::mutex` (or other OS-primitive) stalls; brief uncontended locks only.
* The later `busy_sum` (Slice 2) must be **cooperative CPU work** (bounded compute,
  periodically yielding), never a sleep.

A non-cooperative block pins an OS worker for its whole duration; this is the
liveness hazard when `num_lanes > hpx_threads`, where pinning one worker can starve
other lanes. The cooperative contract is what lets N lane workers and their async
tasks multiplex `hpx_threads` cores.

### 11c. HPX-native alternatives considered and deliberately not adopted (Phase 1)

Several shapes are *more idiomatic HPX* than what Phase 1 ships, and were considered.
They are recorded here as **deliberate non-choices**, not oversights. None of the
notes below is a performance claim — each is a design / semantics trade-off, and any
efficiency comparison would need its own measured slice.

* **Inline execution vs `hpx::async(exec_, task).get()`.** The lane worker is already
  an `hpx::thread`, so it could run the op body **inline** (`RuntimeResult r =
  task(stop);`) instead of dispatching it through `hpx::async(exec_, task).get()`.
  For tiny ops (`square` / `add`) inline would avoid a task spawn + a future + a
  suspension and would be cheaper. Phase 1 **keeps the executor dispatch** on
  purpose: it preserves an explicit **executor / pool placement seam** (the op body
  can later be steered onto a named work pool, distinct from the lane-control
  thread), and it keeps the **operation body conceptually separate** from
  lane-control work (queue, service slot, admission, cancel). This matches the
  accepted Phase 1 lane design and leaves room for later pool / resource-partitioning
  experiments. An **inline-dispatch variant could be evaluated later as a separate,
  measured slice**; it is not folded into the current runtime work, precisely because
  it would reverse the placement-seam decision rather than just tidy code.

* **Executor choice.** The lane's current executor is, in Phase 1, **mostly that
  seam** rather than real placement isolation — a single serialized slot does not
  need a parallel executor's bulk-spawn behavior. **Be honest about what the seam is
  not yet:** with the default executor, the `hpx::async(exec_, task).get()` hop is
  **not real resource isolation** — the op body and the lane-control work run on the
  **same** pool, so the hop currently buys an option (a place to later steer the body
  onto a distinct pool), not isolation. Crucially, that option is **not free per
  `Runtime()`**: HPX **resource partitioning (named pools) is configured at HPX
  initialization** via the resource partitioner, **not** at `Runtime()` construction
  time — and the runtime **shares the one process HPX bootstrap** (possibly booted by
  the harness `Engine`; see §6, §15). So distinct `control` / `work` pools only
  become available if the **shared HPX bootstrap declares them at init**; the seam
  cannot conjure isolation on its own. If/when resource partitioning becomes real
  (named control vs work pools declared at bootstrap), a **pool-specific executor**
  may replace the current default. Phase 1 does not change this yet; the
  inline-dispatch-vs-async-dispatch question is a **future measured mechanism slice
  (not a cleanup)** — see the inline bullet above — and this note motivates **no**
  source change on its own and makes **no** performance claim.

* **`async_rw_mutex` / continuation-chain lane.** A more HPX-native way to serialize
  a lane is a **continuation chain** (e.g. extending a tail future per submit, or
  serializing via `hpx::experimental::async_rw_mutex`): no worker thread, no
  condition variable — serialization falls out of the dataflow dependency.
  **To be precise: HPX async primitives can serialize access** — `async_rw_mutex`
  in particular is a *serialized exclusive-access* primitive (each write-access
  continuation runs after the previous one completes), which is genuinely lane-like,
  not merely a fine-grained task-graph edge. So the reason RayX uses an explicit
  worker+queue lane in Phase 1 is **not** that async primitives are incapable of
  serialization; it is the lane's **serving-control properties**, which an
  owned, inspectable queue provides and a bare continuation/`async_rw_mutex` chain
  does not:
  * **bounded admission against the real queue size** (check-and-push under the
    lane mutex), not against a notional side counter;
  * **`lane_stats()` `queue_depth` / `active` observability** as a coherent snapshot
    of the owned deque;
  * **queued-cancel *before start*** — skipping an item that is still sitting in the
    queue, which a chain (already handed to the scheduler) cannot cleanly do;
  * **FIFO lane semantics visible to the runtime** (a real queue position), which is
    the serving-control contract the project keeps separate from task-graph
    dependency edges.
  A pure continuation chain has no owned queue, so it cannot offer these; that is the
  serving-control gap, distinct from the (real) fact that it serializes. The full
  rationale is in §11a; `async_rw_mutex` narrows the gap (it is a serialization
  primitive, not just an edge) but still lacks the owned, inspectable queue. An
  `async_rw_mutex`- or sender-based lane that *adds* an explicit admission/observable
  queue in front of the serialization primitive remains a possible later mechanism
  variant (see the senders note in the principles doc), evaluated separately, not a
  Phase-1 change.

* **`hpx::stop_token` / `hpx::stop_source` for cancellation.** HPX offers a
  first-class `std::stop_token`-style cancellation primitive, and it is the idiomatic
  choice for cooperative stop. The runtime instead uses a custom
  `RuntimeCancelToken` (§10a) because RayX cancellation carries semantics
  `stop_token` does not express: a **“who won” boolean** (`cancel()` returns `true`
  iff *this* call settled a queued or running stop), a **queued cancel that fulfills
  a `status="cancelled"` row immediately** (no service run), a **running cancel that
  must coordinate with checkpoint-count state** (armed only when there is a remaining
  boundary), and a **value/row result contract** that is RayX-specific. A
  `stop_token`-based rewrite would have to re-encode all of that around a primitive
  that has no race-winner return; the custom token is a deliberate fit to the
  contract, not a reinvention to avoid. **Recorded tradeoff:** the custom
  `RuntimeCancelToken` buys RayX-specific *who-won* cancellation and immediate
  `status="cancelled"` row fulfillment, **but** it forgoes integration with HPX's
  structured cancellation — so if a future **sender/receiver-based** design (see the
  senders note in the principles doc) wanted cancellation to flow through
  `hpx::stop_token` / `get_stop_token`, the custom token would become a **migration
  cost**. This is a noted forward cost of the Phase-1 fit, not a reason to redesign
  cancellation now.

**Guardrails for this note.** These alternatives change *mechanism*, not the project
framing: nothing here is a Ray-replacement claim, an “HPX beats Ray” claim, or any
performance claim. None of them introduces an object store / `ObjectRef`, HPX
actions / components, distributed localities, or arbitrary Python — all of which
remain Phase 1 non-goals (§14).

## 12. Measurement

* **The row carries the core measurement-row fields with unchanged timing
  semantics** (a strict subset of the harness row's keys; see §9). The runtime
  fills them from the same monotonic timing the lane already uses (Python
  `submit_ns` / `total_ms`; C++ steady_clock `start_ns` / `end_ns` /
  `service_ms_observed`).
* **`service_ms_observed` is work-shape-agnostic lane service-lifecycle time** —
  *not* a schema change and *not* a semantic shift. The field has never meant
  "pure CPU time": in the shipped harness it is lane occupancy / observed service,
  and with `chunk_delay_ms > 0` it already includes parked inter-chunk gaps. Its
  meaning is "how long the lane was occupied servicing this request," independent
  of the *work shape*:
  * for **synthetic harness work**, that is observed synthetic-service occupancy
    (sleep/spin, plus any parked gaps);
  * for **runtime operations**, that is the operation's execution occupancy on the
    lane.
  The field, its name, and its semantics are unchanged; only the work being
  serviced differs.
* **No runtime JSONL emission in Phase 1.** The Phase-1 runtime is an in-process
  value API, not a benchmark driver: it does not write JSONL and does not touch
  the analyzer. This is the concrete mitigation for any `service_ms_observed`
  conflation worry — because runtime rows are never emitted to JSONL, an
  operation's execution occupancy can never land in the **v1 benchmark analyzer
  corpus** alongside synthetic-service occupancy, so the two are never aggregated
  together.
* **No v1 schema / analyzer change** (D4, P9). The value lives only on
  `OperationResult.value`, never in a row field.
* **No runtime-only observability record needed yet.** Runtime-only fields
  (executor/pool id, locality, op id, movement timing) are a later-phase concern;
  in Phase 1 the op id is caller-known and there is no movement/locality to
  record. A separate runtime record is intentionally **absent** to keep the slice
  minimal, and reserved for later phases.

## 13. Tests / validation plan

A **new, opt-in** runtime smoke (e.g. `bench/smoke_rayx_runtime.py`, separate from
the frozen `bench/smoke_rayx.py`), plus the existing checks:

* **`py_compile`** on the new runtime Python module(s) and the new smoke.
* **`square(2) -> 4`**: `submit_operation("square", 2).result()` gives
  `.value == 4` and a well-formed row with `status == "completed"`, monotonic
  `start_ns <= end_ns`, non-negative `service_ms_observed` / `total_ms`.
* **`add(2, 3) -> 5`**: correct value plus a well-formed `completed` row.
* **`add` arg-arity errors**: `submit_operation("add", 2)` and
  `submit_operation("add", 2, 3, 4)` raise at the boundary (`TypeError` /
  `ValueError`), before any `RuntimeFuture` is created.
* **`busy_sum(n)` correct value**: returns the expected bounded accumulator for a
  small `n`, with a `completed` row.
* **Unknown operation rejected**: `submit_operation("nope", 2)` raises
  `ValueError` at the boundary (no `RuntimeFuture` created).
* **Unsupported type rejected**: e.g. `submit_operation("square", "x")` or
  `2.5` raises `TypeError` (closed `int`-first type set).
* **Value + row separation**: `.value` and `.row` are distinct attributes.
* **Exact runtime row key set**: assert `set(res.row)` equals exactly
  `{"actor_id", "submit_ns", "start_ns", "end_ns", "total_ms", "queue_wait_ms",
  "service_ms_observed", "status", "error"}` — exact equality, so an accidental
  field addition or omission fails the test.
* **No `value` key in row**: assert `"value" not in res.row` (the value lives only
  on `OperationResult.value`).
* **No harness-facade echo fields**: assert none of `label`, `chunks`,
  `chunk_delay_ms`, `chunks_completed` appear in `res.row`.
* **Runtime `actor_id` prefix**: assert `res.row["actor_id"].startswith("rt-hpx-")`,
  `not res.row["actor_id"].startswith("act-")`, and that the suffix after
  `rt-hpx-` is 8 lowercase hex chars. This is the structural HPX-native-path
  signal (never a timing/performance check).
* **`.row` readable for every outcome**: `.row` is inspectable for completed,
  failed, and cancelled results (`result()` never raises on outcome).
* **`.value` raises for failed/cancelled**: a failed result's `.value` raises
  `OperationFailedError`; a cancelled result's `.value` raises
  `OperationCancelledError`; both catchable as `RuntimeOperationError` /
  `RuntimeError`.
* **`OperationResult` re-readable (not consume-once)**: `.value` and `.row` can be
  read repeatedly; `.value` raises idempotently on a non-completed result.
* **Failure path**: an operation that raises retires `status == "failed"` with a
  populated `error`.
* **Queued cancellation (deterministic)**: a request queued behind a long
  `busy_sum(n)` and cancelled before it starts retires `status == "cancelled"`;
  `.row` readable, `.value` raises.
* **Cooperative running cancellation (deterministic)**: a `busy_sum` cancelled
  mid-loop stops at a per-iteration checkpoint and retires `status == "cancelled"`;
  `.row` readable, `.value` raises.
* **`rt.get([...])` collects, no fail-fast**: over a mixed
  completed/failed/cancelled batch, `get` returns a `list[OperationResult]` in
  input order (same length); each future retired exactly once; per-element
  `.value` raises only on access for non-completed elements; a second
  `get`/`result` on any element raises (structural misuse).
* **Current `bench/smoke_rayx.py` unchanged and still passes**: the harness smoke
  is not edited and continues to pass.
* **v1 schema / golden unchanged**: a golden/contract check that the v1 row schema
  and analyzer behavior are unchanged; `rayx.__all__` unchanged (runtime lives
  under `rayx.runtime`).
* **Process-singleton guard**: constructing a second `Runtime`, or a `Runtime`
  while an `Engine` is live (and vice versa), raises.
* **No `lane_impl` on the runtime**: there is **no** `Runtime.lane_impl()` to test
  and no `lane_impl=` constructor argument; a test asserts `Runtime` has no
  `lane_impl` attribute (and that passing `lane_impl=...` raises `TypeError`),
  confirming the backend selector is absent.
* **HPX-native path (structural)**: the runtime's `rt-hpx-` `actor_id` prefix
  (asserted above) is the visible structural signal that the runtime is on the
  HPX-native path and never confusable with a harness lane id. Structural check
  only, never a timing/performance check.
* **Harness `lane_impl` tests stay separate and unchanged**: the existing
  `bench/smoke_rayx.py` `lane_impl="std"` / `"hpx"` coverage is **not** moved,
  merged, or altered; it remains the harness's own seam.

Per the project's CI rules, none of this enters the normal CI matrix as a
benchmark or performance gate: CI stays at `py_compile` + artifact hygiene + the
deterministic schema-golden contract; no HPX build, no benchmark matrix, no
machine-sensitive performance check is added.

### 13a. Implementation staging (Slices 0–2)

Phase 1 implements in three slices. **Crucially, no slice ships a std-only /
non-HPX-native runtime as evidence** — operation dispatch is HPX-native (via
`hpx::async`) from the very first slice, so the first runtime that runs an
operation is already the designed mechanism, never std-thread scaffolding
mislabeled as the HPX-native runtime.

* **Slice 0 — value-channel scaffolding, already HPX-native.** Registry
  (`square`, `add`) + the `value`/`row` channel + `RuntimeFuture` /
  `OperationResult` + the error classes (§9a) + the shared single-runtime guard
  with `Engine`. Operation dispatch is already via `hpx::async(...).get()` (P1) —
  there is **no** intervening std::thread dispatch path. The FIFO lane is **not**
  introduced yet (one operation at a time, retired immediately), so this slice
  proves value/row separation, consume-once, failure mapping, and `Engine`↔
  `Runtime` mutual exclusion, without the lane concurrency.
* **Slice 1 — HPX-native `RuntimeLane`.** Introduce the HPX-thread FIFO lane
  (§11): an HPX-thread worker over an HPX-synchronized queue whose serialized
  service slot dispatches each operation via `hpx::async(executor, ...).get()`.
  Adds `num_lanes`, round-robin selection, per-lane `rt-hpx-` ids, FIFO ordering,
  and `lane_stats()`. Uses a worker+queue lane (not a continuation chain) and the
  explicit HPX-thread teardown of §11a. The runtime lane is HPX-only; there is no
  std runtime lane and no `lane_impl` selector.
* **Slice 2 — cooperative cancellation, admission, collection APIs.** Split into
  three independently-reviewable sub-slices (build/review order; the final shape is
  unchanged):
  * **Slice 2a — cancellation core.** The runtime-local cooperative token
    (`runtime_cancel.hpp`, `std::mutex` + a copy of the `hpx::promise<RuntimeResult>`,
    no HPX hop on `cancel()`; §10a), the `busy_sum` checkpointed built-in (§7),
    **checkpoint-count-armed** queued **and** cooperative running cancellation,
    `RuntimeFuture.cancel()` / `cancelled()`, `.value` raising
    `OperationCancelledError`, and **cancel-on-shutdown** (§10a). `lane_stats`
    `active` / `queue_depth` become testable under load via `busy_sum`.
  * **Slice 2b — bounded admission.** `Runtime(max_queue_depth_per_lane=...)` +
    per-lane `try_submit` + `QueueFullError` (no global cap, rejected submit is a
    side-effect-free no-op).
  * **Slice 2c — collection APIs (implemented).** `Runtime.get` (collect, input
    order, no fail-fast; §9b) / `Runtime.wait` (non-consuming; `timeout=None` blocks
    via `hpx::wait_some`, `timeout=0` polls, finite `timeout > 0` raises
    `NotImplementedError`; strict `num_returns` and entry-type validation shared by
    both modes) / `Runtime.as_completed` (yields the input `RuntimeFuture` handles,
    each once), mirroring the harness semantics without touching harness code. The
    accepted concurrency non-guarantees are recorded in §9c.

This staging is a build/review order, not a change to the Phase 1 contract: the
final shape is exactly §1–§14.

## 14. Explicit non-goals (Phase 1)

* Arbitrary Python execution.
* Python callbacks / callables / pickled closures into the registry.
* `ObjectRef` / any return-value reference handle.
* Object store (of any kind).
* HPX actions / components / GIDs.
* Distributed localities / remote dispatch.
* Module-level `rayx.get` / `rayx.wait` (or `rayx.runtime.get` / `.wait`).
* A global / implicit default runtime.
* `@rayx.remote` (or any decorator) on arbitrary callables.
* Ray Serve / Ray Train / Ray Tune / Ray Data behavior.
* Real model inference / model backends.
* Any "Ray replacement", "HPX beats Ray", or performance-superiority claim.

## 15. Risks / open decisions

* **Single HPX runtime per process.** The harness `Engine` already claims the
  process runtime as a singleton; the `Runtime` must share that bootstrap and be
  mutually exclusive with `Engine` in Phase 1 (§6). The clean coexistence of both
  over one shared bootstrap is deferred.
* **Registry as an arbitrary-execution backdoor.** The registry must remain a
  fixed native table; the standing risk is a later "ergonomics" change sliding
  toward accepting a callable. Mitigation: Phase 1 keeps registration C++-side
  with no Python callable path, and this constraint is written into the design.
* **Value typing across pybind.** Phase 1 keeps `int` only to avoid float/bytes
  codec scope; the closed set and where P6 extends it must be documented at the
  boundary.
* **Future coexistence of `Engine` and `Runtime` over one bootstrap.** A later
  refactor to share a single HPX runtime bootstrap between the harness and the
  runtime is plausible but out of Phase-1 scope; its cost/benefit is unevaluated.

The following were open in earlier drafts and are now **locked** (no longer open):
the failure/cancel result behavior and `RuntimeFuture` vs `OperationResult`
consume-once semantics (§9); the error-class hierarchy and names (§9a); the
`rt.get([...])` collect-not-fail-fast behavior (§9b); the `service_ms_observed`
framing as work-shape-agnostic lane service-lifecycle time, not a semantic shift
(§12); and the locked built-in set including the checkpointed `busy_sum` (§7).

## See also

* [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) — the runtime
  problem model (goal, locked decisions, 12-issue map, phase order) this doc
  refines at Phase 1.
* [rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md) —
  the HPX-native design principles (P1–P9) this doc applies to Phase 1.
* [../reference/rayx_actor_api.md](../reference/rayx_actor_api.md) — the current
  shipped harness API (Engine / SyntheticActor) and the Ray actor-pool mapping.
* [../reference/rayx_frontend_design.md](../reference/rayx_frontend_design.md) —
  the harness design rationale: Future ownership, why no module-level `get`, and
  the `lane_impl` seam.
