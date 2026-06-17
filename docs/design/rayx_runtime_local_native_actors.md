# rayx.runtime local native actors design note

## Status

**Status: implemented (experimental MVP).** The local native actor MVP designed in
this note has now shipped experimentally in the `rayx.runtime` prototype (Steps
B→C2). The note began as the Slice A design artifact (the **next serious runtime
axis after the closed value/type model**) and is preserved as the design rationale;
what it specified is now built. It still lives under `docs/design/` (exploratory)
deliberately apart from `docs/reference/` (stable shipped contracts): shipping the
MVP does **not** promote it to a stable reference contract, and the runtime stays
experimental.

**Implemented MVP (through C2):**

* `Runtime.create_actor("counter", initial) -> ActorHandle` and
  `ActorHandle.call("add" | "get" | "reset" | "busy_get", *args) -> RuntimeFuture`
  (`busy_get` added in C5);
* the native `CounterActor` (`int64` state; `add` / `get` / `reset`, plus the
  read-only checkpointed `busy_get`) over a dedicated per-actor HPX-native FIFO
  `RuntimeLane`;
* `rt-act-<16 hex>` actor ids (real 64-bit entropy), stamped on the result row;
* Python-boundary validation (`validate_actor_create` / `validate_actor_call`):
  unknown type/method → `ValueError`, wrong arity → `ValueError`, wrong type →
  `TypeError`, `int64` out of range → `ValueError`, all before any native crossing;
* full reuse of `RuntimeFuture` / `OperationResult` and the `get` / `wait` /
  `as_completed` collection APIs (actor and op futures interoperate);
* the **exact 9-field row** preserved (no `value` key, no `actor_type` / `method`
  field);
* `ActorHandle.stats()` — a per-actor **read-only observability snapshot**
  (`{actor_id, queue_depth, active}`, the actor analogue of one
  `Runtime.lane_stats()` element; added after C5 — see the per-actor
  observability section below).

**Still out (unchanged limitations):** local-only; a fixed native actor
type/method registry (no arbitrary Python methods, no dynamic registration); no
`.remote()`; no Python object state; no `ObjectRef` / object store; no `kill()`
and no GC/refcount/destructor lifecycle (explicit **local** release now exists —
`Runtime.release_actor(actor)`, see the lifecycle contract below; it is not
`ray.kill` and carries no distributed-ownership semantics); no actor lanes in
`Runtime.lane_stats()` (it stays **op-lanes-only**;
per-actor observability is only the per-handle `ActorHandle.stats()` snapshot, with
no all-actors enumeration and no counters); no running-cancel of state
*mutation* (`add` / `get` / `reset` are atomic, queued-cancel only; the C5
`busy_get` is running-cancellable but **read-only**, so it never mutates state); and
**no performance claim**.

It depends on, and does not relitigate, the shipped Phase 1 runtime
([rayx_runtime_phase1_summary.md](rayx_runtime_phase1_summary.md)) and the closed
value/type model ([rayx_runtime_value_model.md](rayx_runtime_value_model.md): the
typed-signature substrate + `std::variant<int64,double>` value channel +
`scale_double`). It works within the HPX-native lens of
[rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md)
(P1, P2, P7, P9, P10, P11, and the futures-vs-senders mechanism-currency note) and
sits at item 4 (local stateful native actors, Phase 2) of the problem model's
near-term axis ordering
([rayx_runtime_problem_model.md](rayx_runtime_problem_model.md), issue 5).

This note makes **no** Ray-replacement claim, **no** "HPX beats Ray" claim, and
**no** performance-superiority claim. The HPX mechanisms it weighs are weighed for
*fit* and *demonstration value*, never asserted as faster.

## Motivation

Phase 1 proved that **registered native operations run over HPX-native FIFO
`RuntimeLane`s** with value/row separation, cooperative cancellation, bounded
admission, and the `get` / `wait` / `as_completed` collection APIs. The value model
then escaped `int64`-only with a small closed typed arg/result set. Both are
*stateless*: every `submit_operation` is an independent function call.

The next capability is **long-lived, addressable, stateful entities with a fixed
set of registered methods** — the problem model's issue 5. This is the HPX-native,
narrow analogue of a Ray actor: state plus *registered native methods* (by id),
serialized FIFO per actor, returning a value plus the existing measurement row. It
demonstrates that RayX's serving-control lane can carry **native state** without
arbitrary Python, an object store, or HPX components — and it does so by reusing
the machinery already shipped, not by inventing a new runtime.

## Relationship to current runtime

This direction is **additive within the existing runtime contract**. It keeps,
unchanged:

* `Runtime` as the single explicit, process-singleton runtime (D5); mutually
  exclusive with the harness `Engine`.
* The HPX-native FIFO `RuntimeLane` mechanism, its `RuntimeCancelToken`, bounded
  admission (`try_submit` / `QueueFullError`), and `lane_stats()`.
* `RuntimeFuture` and `OperationResult` with **`.value` and `.row` strictly
  separate** (P9) — actor method calls return the *same* `RuntimeFuture`, so
  `get` / `wait` / `as_completed` work on them with no new collection code.
* The closed value channel (`OpArgs` / `OpValue` = `variant<int64,double>`) for
  method arguments and results.
* The **exact 9-field runtime row**: `actor_id`, `submit_ns`, `start_ns`,
  `end_ns`, `total_ms`, `queue_wait_ms`, `service_ms_observed`, `status`, `error`
  — no new field, no `value` key, no harness-facade echoes.

The single new C++ concept is a **fixed actor registry** (actor type → init
signature + factory + registered method table) plus per-actor state ownership; the
single new Python surface is `Runtime.create_actor(...)` and a small `ActorHandle`
object whose `.call(...)` returns the existing `RuntimeFuture`.

## Non-goals

This note is **local, single-locality, native-state** only. It deliberately does
**not** provide, and does not imply:

* local only; single-locality only
* native C++ state only — **no Python object state**, **no pickle**
* **fixed registered actor types** and **registered native methods** only — **no
  arbitrary Python methods**, no dynamic method registration
* no `ObjectRef`, no object store, no Ray task-result semantics
* no HPX actions / components / localities; no AGAS; no GIDs
* no distributed implementation of any kind
* no Ray-compatible clone; no `.remote()`; no global `rayx.get` / `rayx.wait`
* no real model inference
* **no performance claim**
* **no benchmark JSONL / analyzer schema change**; **value + row separation
  preserved**; the **exact 9-field runtime row preserved**

Keeping method signatures typed/by-value (from the closed type set) means a *later*
lift to HPX components is mechanical (P2) — but actions/components are introduced
only when crossing localities actually requires them, which this note does not.

## Minimal Python API

```python
with Runtime() as rt:
    counter = rt.create_actor("counter", 0)   # actor type id + init args
    f1 = counter.call("add", 5)               # method id + args -> RuntimeFuture
    f2 = counter.call("get")                  # -> RuntimeFuture
    results = rt.get([f1, f2])                # list[OperationResult], input order
    # results[0].value == 5, results[1].value == 5
```

Clarifications:

* **`create_actor` lives on `Runtime`** — the explicit, single runtime (D5). There
  is no separate `ActorRuntime`, no global default, no `@rayx.remote`.
* **The actor handle is a Python object** (`ActorHandle`) holding the actor's
  `actor_id` string and a back-reference to the `Runtime`.
* **`ActorHandle.call(method_id, *args)` returns the existing `RuntimeFuture`** —
  not a new handle type. The method is dispatched onto the actor's dedicated lane.
* **Actor method futures work with existing `Runtime.get`, `Runtime.wait`, and
  `Runtime.as_completed`** unchanged (they are ordinary `RuntimeFuture`s).
* **Do not use `.remote()`.** We want Ray-*familiar* control ergonomics, not a Ray
  clone: `.call("method", *args)` reads as an explicit dispatch over a *fixed*
  registered method set, and avoids implying Ray's `.remote()` future / object-store
  semantics. Attribute-style `actor.add(...)` is also rejected — it would imply
  arbitrary method names rather than a closed registered set.

## Native actor state model

* **Fixed actor types with native C++ struct state.** A small abstract base gives
  the registry a uniform handle while each type keeps strongly-typed native fields:

  ```cpp
  struct ActorState {
      virtual ~ActorState() = default;
      virtual const char* type_id() const = 0;   // defensive identity check
  };
  struct CounterState : ActorState {
      std::int64_t v = 0;
      const char* type_id() const override { return "counter"; }
  };
  ```

* **State holds native fields, not `OpValue` blobs.** `CounterState::v` is a real
  `std::int64_t`. A generic `OpValue` state bag is deliberately avoided — that would
  drift toward an object store. The value channel (`OpValue`) is for method
  args/results; **state is per-type native data**.
* **Ownership:** the engine owns, per actor, a `{ dedicated RuntimeLane,
  std::shared_ptr<ActorState> }` entry keyed by `actor_id`. Each method's lane-task
  closure captures the `shared_ptr<ActorState>`, so the state outlives every
  in-flight method body (including during shutdown drain).
* **No Python state, ever.** No Python object is stored as actor state and no Python
  callback runs on an HPX worker (P11): actor method bodies are pre-compiled native
  code, so the HPX workers that run them never acquire the GIL.

## Lane and scheduling model

* **One dedicated `RuntimeLane` per actor.** The lane *is* the actor's
  serialization domain and FIFO mailbox; the lane's `actor_id` *is* the actor id
  (see [Row/result model](#rowresult-model) for the `rt-act-` prefix). This reuses
  the shipped `RuntimeLane` (`python/src/rayx/runtime_lane.hpp`): its
  **queue / worker / cancellation / admission behavior is reused unchanged; only
  id generation gains an additive prefix parameter** (see the prefix note below).
  The lane already runs an injected `OpTask` closure and knows nothing about the
  registry, values, or state, so a stateful method is simply a closure that captures
  `shared_ptr<ActorState>`.
* **Why per-actor and not multiplexed onto the shared op-lane pool:** plain
  operations round-robin across a fixed pool of shared lanes; an actor instead needs
  a *private* FIFO lane so its methods serialize against *each other* (and its state)
  for the actor's whole lifetime. So the engine owns both: the existing round-robin
  op-lane pool **and** a map `actor_id → actor lane`.
* **Cooperative-suspension correctness is inherited.** The lane worker uses
  `hpx::mutex` + `hpx::condition_variable_any` (the idle wait *suspends* the HPX
  thread, freeing the worker) and dispatches each body via
  `hpx::async(exec_, task).get()` (a cooperative HPX suspension, not an OS-thread
  block). Reusing the lane's worker / cancellation / admission logic (only id
  generation gains a prefix parameter) means actors inherit this correct behavior and
  cannot reintroduce the experiment-16 bug class (a `std::condition_variable` on an
  HPX worker starving the scheduler). This is a *reason* to reuse, not just
  convenience — see [HPX alternatives considered](#hpx-alternatives-considered).

## HPX alternatives considered

Three HPX mechanisms can serialize stateful method access. They are weighed here so
"reuse the lane" reads as a decision, not a default.

1. **Worker + owned queue per actor, using the existing `RuntimeLane`.** A dedicated
   single-`hpx::thread` FIFO worker with an owned, inspectable queue.
2. **Continuation chain over the state**, e.g.
   `state_chain = state_chain.then(run_method)`. Serializes by future-chaining — no
   dedicated worker, no condition variable, no queue, no parked thread per actor.
3. **`hpx::execution::experimental::async_rw_mutex<State>` / sender-style serialized
   access.** The modern HPX primitive purpose-built for serialized read/write access
   to a value: it hands out senders for rw-access, auto-serializing writers and even
   allowing **concurrent readers**, in the P2300-aligned sender/receiver framework.

**Decision: use the existing `RuntimeLane` for the first actor slice.**

Justification:

* It preserves the **serving-control surface RayX already cares about**, all of
  which require an *owned, inspectable queue* that options 2 and 3 do not have:
  * **bounded admission** (`try_submit` / `QueueFullError`),
  * **queue depth / active observability** (`lane_stats`),
  * **queued-cancel-before-start** (skip a still-queued method),
  * **FIFO method semantics** per actor.
* It stays on the **stable futures / `hpx::async` mechanism path** the lane,
  cancellation, and collection APIs are already built on.
* It **avoids rewriting around the experimental sender/receiver APIs now** — the P1
  mechanism-currency note explicitly says Phase 1 should not be rewritten around
  senders yet.
* It **avoids reintroducing the old HPX bug class** (blocking an OS worker with
  `std::mutex` / `std::condition_variable` patterns on an HPX thread); the shipped
  lane already uses the correct `hpx::` primitives.

Tradeoffs to record honestly:

* **one suspended HPX lightweight thread per actor** (cheap — a parked user-level
  thread with a small stack, not an OS thread — but not free);
* **FIFO-serialized reads**: a `get()` cannot run concurrently with another method
  even though it safely could; there is no concurrent-reader optimization;
* **many tiny, mostly-idle actors may later justify an `async_rw_mutex` /
  sender-based design** (no parked thread per actor, concurrent readers);
* **that future mechanism axis is deferred** — recorded here as a direction only,
  exactly parallel to the senders note for the composition op. It is not part of
  this slice and carries no performance claim.

## Dynamic lane lifecycle contract

`RuntimeLane` construction spawns an `hpx::thread`, and its stop/join locks
`hpx::mutex` and joins that thread — both of which are valid **only on an HPX
thread, while HPX is up**. Because actors are created at arbitrary times from the
external Python thread, this slice has an explicit lifecycle contract:

* **Actor lane construction must happen on an HPX thread** — via
  `hpx::run_as_hpx_thread(...)`, mirroring how `RuntimeEngine` already builds its
  fixed lane pool. `create_actor` must hop.
* **Actor cleanup must also happen on an HPX thread while HPX is still alive.**
* **Actor teardown order is fixed:**
  1. **cancel** queued / in-flight actor methods (`cancel_pending()`),
  2. **stop and join** the actor lane (`stop_and_join()`),
  3. **drop** the actor state (`shared_ptr<ActorState>` released only *after* the
     worker has joined, so no in-flight body can touch freed state).
* **Do not release actors after `stop_process_hpx()`.** Once HPX is stopped, no
  lane lock / join is valid.
* **Actors live until explicitly released or the runtime shuts down.** The
  first slice had runtime-lifetime actors only; `Runtime.release_actor(actor)`
  has since been added as the explicit **local** release: `shutdown()`'s actor
  teardown scoped to one record, following exactly steps 1→2→3 above inside one
  GIL-released `hpx::run_as_hpx_thread` hop while HPX is up. Synchronous and
  **bounded by one checkpoint stride** — queued method calls cancel, an
  in-flight checkpointed method stops at its next boundary, an in-flight
  instantaneous method completes, **every** outstanding future still resolves
  (and stays retirable afterwards), and the record (lane +
  `shared_ptr<ActorState>`) is erased only after the worker joins. Strict
  afterwards: `call` / `stats` / a second release on the handle raise
  `RuntimeError`; rows already produced keep their stamped `rt-act-` id. Not
  `ray.kill`, no distributed ownership, no handle refcounting, no GC/destructor
  lifecycle. Remaining actors are still torn down by `Runtime.shutdown()`
  (unchanged, inside the same hop that cancels + drains the op lanes).
* **Partial construction failure must not leave a joinable `hpx::thread` behind.**
  If `create_actor` fails after the lane's worker exists, it must stop/join that
  lane (same HPX-thread hop) before unwinding — a joinable `hpx::thread` destroyed
  during unwind terminates the process. This mirrors the engine ctor's existing
  partial-construction cleanup.

**`create_actor` ordering (MVP).** The MVP orders construction so the cheap,
HPX-free, fail-fast work happens before any worker thread exists:

```text
validate init args (Python boundary)
  -> build native state via the factory (synchronous, HPX-free, on the Python thread)
  -> construct the actor lane on an HPX thread (the run_as_hpx_thread hop)
  -> insert the actor entry into the actor map
```

* Init args are validated at the **Python boundary** (unknown actor type / wrong
  arity / wrong type / `int64` range → `ValueError` / `TypeError`) before any state
  or lane is built.
* The **factory builds native state synchronously before the actor lane starts**, so
  a factory failure (relevant for future actor types; `CounterState` cannot fail)
  raises before any worker `hpx::thread` exists — **no actor lane is ever left alive
  on a factory failure**, and there is nothing to join.
* **`init` is not a queued actor method** in the MVP — there is no `"init"` method id
  on the lane; initialization is the factory call at `create_actor` time.
* If a *future* design ever reverses this (constructs the lane before the factory
  completes), it **must** use the same partial-construction cleanup path: stop/join
  the lane (HPX-thread hop) before unwinding.

**Actor-map concurrency (single-driver, first slice).** The `actor_id → {lane,
state}` map follows the **same single-driver assumption as the current runtime**:
concurrent `create_actor` / `call` / `shutdown` from multiple Python threads is **not
guaranteed** in the first slice (the map is read/written without an internal lock,
exactly as the runtime already documents for `wait` / shutdown concurrency). This
note does **not** imply a thread-safe actor map unless a later implementation adds
explicit locking.

## Actor registry model

A new **HPX-free** registry header (mirroring the HPX-free discipline of
`runtime_ops.hpp`; `CounterActor` needs no HPX) describes the fixed actor types:

```cpp
using MethodFn = std::function<
    OpOutcome(ActorState& st, const OpArgs& args, const StopCheckpoint& stop)>;

struct MethodEntry {
    int arity;
    std::vector<OpType> arg_types;          // typed signature (per value model)
    OpType result_type;
    std::function<int(const OpArgs&)> checkpoint_count;   // one_checkpoint for MVP
    MethodFn fn;                            // downcasts ActorState defensively
};
struct ActorTypeEntry {
    std::vector<OpType> init_arg_types;
    std::function<std::unique_ptr<ActorState>(const OpArgs&)> factory;
    std::unordered_map<std::string, MethodEntry> methods;
};
const std::unordered_map<std::string, ActorTypeEntry>& actor_registry();  // {"counter": ...}
```

* A method body does a defensive `type_id()` check then `static_cast` to its
  concrete state — the same defensive-backstop posture as `as_int64` / `as_double`
  for args. A wrong tag maps to a `status="failed"` row, never a crash.
* **No new lane header** (`runtime_actor_lane.hpp` is *not* added): state ownership,
  the per-method task-closure builder (`make_method_task`, analogous to
  `make_op_task`), the `actor_id → {lane, state}` map, and a Python-boundary
  `runtime_actor_table()` (typed metadata, analogous to `runtime_op_table()`) live
  in `_rayx.cpp`, where `make_op_task` and the registry merges already live.
* The registry stays a **fixed native set** — no Python callable, no dynamic
  registration (D2). This is the standing registry-as-arbitrary-execution-backdoor
  guardrail, unchanged.

## CounterActor MVP

```text
counter

init(int64 initial)

add(int64 delta) -> int64     # mutate state, return the NEW value
get()            -> int64     # read current state
reset(int64 value) -> int64   # overwrite state, return the new value
busy_get(int64 work_n) -> int64   # READ-ONLY checkpointed on-core work, then current value
```

Why `CounterActor`:

* `int64` is the primary channel and is closed-form cross-checkable; `add` / `get`
  / `reset` proves **state persistence across calls**, **per-actor FIFO ordering**
  (a sequence of `add`s observed in order with monotone returns), **instance
  independence** (two counters do not interfere), a **read** method, and **two
  distinct typed mutators**. That is the real state + typed-value machinery, not a
  toy.
* It is exactly the problem model's issue-5 MVP ("state:int, registered methods,
  FIFO"). `reset` is a cheap second mutator that takes an arg.
* `AccumulatorDouble` (a `double` counter) would re-prove the already-shipped double
  channel rather than the *new* state axis; it is an easy follow-up once the state
  model is proven, not the first type.

`add` / `get` / `reset` are **instantaneous, single-mutation** (see cancellation
below). `busy_get(work_n)` is the one **checkpointed, READ-ONLY** method (C5): it
performs synthetic on-core diagnostic/calibration work — the actor-method analog of
the op-level `busy_sum`, reusing `BUSY_SUM_STRIDE` / `busy_sum_checkpoints` / the
masked chunk loop / the lane-bound `StopCheckpoint` and its cooperative yield — and
then returns the **current counter value unchanged**. With `work_n > BUSY_SUM_STRIDE`
its `checkpoint_count > 1`, so it is **running-cancellable through the actor dispatch
path**. It exists to exercise that armed checkpoint/cancel path and to occupy the
service slot for in-flight-shutdown coverage; it is purely synthetic, mutates
nothing, and carries **no performance claim**. `work_n >= 0` is validated at the
Python boundary (mirroring the op-level `busy_sum` guard).

## Type/value model interaction

* **Method args/results and init args are `OpArgs` / `OpValue`** (the closed
  `variant<int64,double>` channel), reused unchanged — so the existing Python-thread
  marshaller, the typed-table boundary validation, and the `std::visit` result
  conversion in `RuntimeFuture.result()` all apply as-is.
* **Actor state holds native C++ fields, not `OpValue`** (see state model).
* **Bytes is not required and not added.** `CounterActor` is pure `int64`; nothing
  in the actor model needs a heap payload type. Bytes stays gated.
* **Typed validation metadata:** per actor type, an `init` signature (`arg_types`)
  and per method `{arg_types, result_type}`, exposed via `runtime_actor_table()`,
  read once at import into an `_ACTOR_TABLE`, and validated at the Python boundary
  before the crossing (unknown actor type / unknown method / wrong arity / wrong
  type / `int64` range → `ValueError` / `TypeError`, before any `RuntimeFuture` or
  lane work).

## Cancellation and admission semantics

* **First actor methods are instantaneous (`checkpoint_count == 1`) → queued-cancel
  only.** A queued method cancel skips it before service and **leaves state
  untouched** (the body never runs). Once a method is **active**, `cancel()` returns
  `False` — there is no boundary to stop at. This is the same honest posture as
  `scale_double` / `fanout_sum`-P1.
* **No running-cancel of checkpointed state *mutation*.** A checkpointed mutator that
  running-cancels mid-body could leave **partial state**, which needs transactional
  semantics, so no such method is shipped. `busy_get` (C5) is checkpointed and
  running-cancellable but **read-only** — it never writes `c.v`, so a running cancel
  returns `status="cancelled"` with no value and leaves state untouched, sidestepping
  the partial-mutation question entirely. A checkpointed *mutator* remains a
  deliberately deferred, separately-designed concern (no `busy_add`).
* **Admission reuses `Runtime` `max_queue_depth_per_lane`, applied per actor lane**
  for the first slice: a full actor lane makes `counter.call(...)` raise the
  existing `QueueFullError` (no future / token / promise created). No separate
  per-actor cap argument yet.
* **Deterministic actor `QueueFull` is now tested via `stats()` gating.**
  (Supersedes the earlier C5 posture, kept here as provenance: a deterministic
  actor-path `QueueFull` test was originally omitted rather than made flaky because
  it would have required either an actor-lane observability surface or a hold/pause
  primitive, neither of which existed.) The bounded-admission *mechanism* was
  already proven deterministically at the operation level
  (`test_bounded_admission_queue_full`), and `call_actor_method` reaches the
  identical `RuntimeLane::try_submit(..., max_queue_depth, ...)` path.
  `ActorHandle.stats()` now provides exactly the missing observability surface:
  `test_actor_queue_full_deterministic` occupies the service slot with a large
  checkpointed `busy_get`, gates on `stats()["active"]` (no sleeps), fills the
  queue to the cap, and asserts the next call raises `QueueFullError`. The C5
  `busy_get` cancellation coverage remains **invariant-scoped** by design (its race
  loop deliberately does not gate on `stats()`); the stats-gated
  `test_actor_running_cancel_deterministic` and
  `test_actor_shutdown_fulfills_all_futures` add the deterministic running-cancel
  and provably-in-flight-at-shutdown coverage on top.
* **Exactly-once fulfillment reuses the existing lane / `RuntimeCancelToken`
  machinery** — no new fulfillment path is introduced.

## Per-actor observability: `ActorHandle.stats()`

`ActorHandle.stats()` is the one per-actor observability surface — **read-only
observability only**, nothing more:

* It returns one `{"actor_id", "queue_depth", "active"}` dict for this actor's
  dedicated lane — the actor analogue of a single `Runtime.lane_stats()` element,
  with the same three fields and the same semantics: `queue_depth` counts
  **queued-but-not-started** method calls (the in-service call has been popped and
  is **not** counted), and `active` is whether one method has been popped into the
  service slot and has not yet fulfilled.
* It is a **non-consuming, point-in-time, racy snapshot**, exactly like
  `lane_stats()`: it touches no future and changes no call / cancel / admission
  semantics, and the values can change the instant it returns. It exists for
  debugging and deterministic test-gating only — it is **not** scheduler state,
  **not** placement control, and **not** a synchronization primitive.
* Natively it is `_RuntimeEngine.actor_stats(actor_id)`: an actor-map lookup, then
  the lane's `stats()` read **on an HPX thread** (the same
  `hpx::run_as_hpx_thread` hop the other lane-mutex paths use, because
  `RuntimeLane::stats()` takes the lane's `hpx::mutex`), with the result dict built
  back under the GIL. An unknown `actor_id` raises `ValueError` (only reachable via
  the raw bypass today — actors live until shutdown); after runtime shutdown the
  call raises `RuntimeError`, consistent with `call()`.
* **Deliberately not added:** no counters or cumulative totals, no `actor_type`
  field, no `Runtime.actor_stats()` all-actors enumeration, no actor lanes in
  `Runtime.lane_stats()` (it stays op-lanes-only, same length / order / `rt-hpx-`
  prefix as before), no JSONL / row-schema change, and no performance claim built
  on it.

## Row/result model

* A method call returns one `RuntimeFuture` → one `OperationResult`: one `.value`
  (the method's typed result) and **one exact 9-field `.row`**. Internal actor
  mechanics are invisible to the result shape.
* **The row schema is unchanged.** `actor_id` is the actor's dedicated-lane id,
  which already identifies the instance; **no `actor_type` / `method` field is added
  to the row** (that would be a schema change — D4 / P9). Method/type identity is the
  caller's knowledge. If actor-type / method observability is ever wanted, it goes
  in a *separate* runtime record, never the v1-shaped row.
* **`actor_id` uses the `rt-act-` prefix** (vs plain ops' `rt-hpx-`), so actor
  method rows are visibly distinguishable. This is a local opaque handle; it is
  forward-compatible with the eventual (much-later, design-only) component/AGAS phase
  where identity would become an `hpx::id_type` exposed only as an opaque RayX
  handle.
* **Prefix mechanism (implementation detail; shipped in Step A / C1a).**
  The lane self-generates its id in its constructor via
  `make_runtime_actor_id(prefix)` (the prefix was originally a hard-coded
  `rt-hpx-`), and
  `RuntimeLane::stats()` reports that same id — so the lane's own id flows into
  *both* the row (`make_op_task` stamps `lane.actor_id()`) and `lane_stats`. To make
  actors carry `rt-act-` **consistently across the row and actor `lane_stats`**, the
  prefix is made an **additive parameter** on the runtime actor-id generation path:
  `make_runtime_actor_id(prefix)` and an optional prefix argument on the
  `RuntimeLane` constructor (or equivalent), **default `rt-hpx-`** for normal
  operation lanes, **`rt-act-`** for actor lanes. The queue / worker / cancellation /
  admission logic is untouched; only id generation gains the parameter. We do **not**
  use a row-only stamp (passing a separate `rt-act-` string into the method task
  while leaving the lane's id `rt-hpx-`), because that would make lane stats report
  `rt-hpx-` for an actor whose rows say `rt-act-` — an inconsistency that would have
  surfaced exactly when per-actor observability landed (and indeed
  `ActorHandle.stats()` now reports the same `rt-act-` id the rows carry).
* `row["service_ms_observed"]` measures the **method's lane-occupancy lifecycle**
  (work-shape-agnostic), exactly as for operations.

### State lifetime and race-freedom invariant

Actor state is mutated without a state lock or atomics, and that is race-free **only
because of a specific, load-bearing invariant** that this note pins down:

* each actor has **one dedicated `RuntimeLane`**;
* the lane **retires exactly one method body at a time** (the worker does
  `hpx::async(exec_, task).get()` to completion before popping the next item);
* **method N completes before method N+1 is launched**, so there is a full
  happens-before chain (N's write → future-ready/release → worker `.get()`/acquire →
  worker launches N+1 → N+1 reads) — method N's write is visible to method N+1's
  read;
* **there is no pipelining of actor method bodies**;
* the actor **state is dropped only after the lane worker joins** (teardown step 3).

Notes:

* **The executor identity is not the key guarantee.** Bodies may land on different
  executor OS threads; that is irrelevant precisely because the worker fully
  serializes them, so at most one body touches the state at any instant.
* **The load-bearing invariant is strict one-at-a-time retirement on the actor
  lane.** If the lane is ever "optimized" to pipeline bodies (launch N+1 before N's
  `.get()` returns), this actor-state correctness proof must be revisited and the
  state would need explicit synchronization.

## Testing strategy

Mirrors the Phase-1 runtime unit / integration / smoke split; **no timing or
performance assertion** anywhere. (Shipped in C2; the tests described below now
exist. The shipped method-call validator is named `validate_actor_call`.)

* **Unit (`tests/unit/`, import-light, no `_rayx`):** new actor-create / method
  validation (`validate_actor_create` / `validate_actor_call` in `_validate.py`,
  tested in a new `tests/unit/test_actor_validate.py`): unknown actor type →
  `ValueError`; wrong init arity/type → `ValueError` / `TypeError`; `int64` range on
  init; unknown method → `ValueError`; wrong method arity/type; actor-table fixture
  shape.
* **Integration (`tests/integration/`, requires built `_rayx`):** create actor;
  `add` / `get` / `reset` values; **state persists across calls**; **per-actor FIFO
  ordering**; **two same-type actors are independent** (create two `counter`
  actors, mutate both differently, verify each retains its own value — proving
  per-*instance* state, not just per-*type* state); method futures flow through
  `get` / `wait`
  / `as_completed` (mixed with plain-op futures); failure row on a defensive
  wrong-state / unknown-method bypass (`status="failed"`, not a crash); **queued
  cancel** → `status="cancelled"`, `.value` raises `OperationCancelledError`, state
  unchanged, active `cancel()` → `False`; exact **9-field row**, `"value" not in
  row`, `actor_id` starts `rt-act-`; shutdown cleanup (no dangling future; second
  `create_actor` / `call` after shutdown raises); `ActorHandle.stats()` snapshot
  shape + non-consumption, the stats-gated deterministic `QueueFull` / running
  cancel / shutdown-fulfillment tests, post-shutdown `stats()` raise, and
  `lane_stats()` excluding actor lanes.
* **Smoke (`bench/smoke_rayx_runtime.py`):** a small `CounterActor` workflow (create,
  `add`×k, `get`, `reset`, `get`) + 9-field-row + `rt-act-` prefix + queued-only
  cancel posture + an idle `stats()` snapshot check (shape, non-consumption,
  op-lanes-only `lane_stats`, post-shutdown raise).
* **Harness untouched:** `bench/smoke_rayx.py` and the v1 schema golden unchanged;
  `rayx.__all__` unchanged.

## Implementation slices

**Status: implemented through C2** — the Slice A note, then the HPX-free actor
registry header, 16-hex/64-bit actor ids, the native engine plumbing
(`create_actor` / `call_actor_method` / `runtime_actor_table()`), and the public
Python API + validation + tests + smoke. The original slice plan below is preserved
as provenance (the shipped split was B → C1a → C1b → C2).

1. **Slice A — this docs-only design note.** (No source, no tests.)
2. **Slice B — native actor registry + Python `ActorHandle` + `create_actor` /
   `call` skeleton + `CounterActor` happy path.** New HPX-free actor registry header;
   `make_method_task` + actor map + `create_actor` / `call_actor_method` +
   `runtime_actor_table()` in `_rayx.cpp` (reusing `RuntimeLane`, with the
   HPX-thread-hop lifecycle); Python `ActorHandle` + `Runtime.create_actor`;
   collection-API compatibility (free). Build + a minimal create/add/get smoke.
3. **Slice C — validation + cancellation / admission / stats / shutdown + full
   coverage.** Python-boundary actor validation; per-actor queued cancel; per-actor
   bounded admission; actor `lane_stats`; runtime-shutdown teardown of actor lanes;
   full unit + integration + smoke + example (`examples/rayx_runtime_basic.py` actor
   tour); docs reconciliation (value model + problem-model status notes; this note's
   status). Slices B and C may merge into one coherent reviewable slice if small;
   tests/docs land with the behavior they cover.

## Risks and guardrails

* **Registry-as-arbitrary-execution backdoor (D2).** Actor types/methods are a
  **fixed native set**; nothing admits a Python callable or dynamic registration.
* **Schema/analyzer frozen (D4 / P9).** No row field, no `value` key, no child rows,
  no JSONL, no analyzer change. The exact 9-field row and value/row separation are
  preserved.
* **State is native, not an `OpValue` / object-store bag.** No Python state, no
  pickle, no `ObjectRef`, no object store.
* **Atomic methods only in the first slice**, so queued-only cancellation can never
  leave partial state; running-cancel of state mutation is deferred.
* **Local only (P2).** No HPX actions / components / GIDs / AGAS / localities; no
  distributed implementation. Signatures stay serialization-friendly so a later lift
  is mechanical, but no remote machinery is introduced.
* **Dynamic lane lifecycle discipline** (HPX-thread hop for create/cleanup, teardown
  order, no release after `stop_process_hpx`, no joinable thread on a failure unwind)
  — see the lifecycle contract; this is the primary implementation hazard.
* **Strict one-at-a-time retirement is load-bearing** for state race-freedom — see
  the invariant; do not pipeline actor method bodies without revisiting the proof.
* **Micro-method overhead caveat (P10).** `CounterActor` methods are micro-methods:
  their cost includes the Python→C++ crossing, lane queueing, and the existing
  `hpx::async(...).get()` lane-body hop. **Do not treat `CounterActor` timing as an
  actor-throughput benchmark or any performance claim** — a micro-method measures the
  boundary, not work.
* **No re-entrancy.** Registered actor method bodies must **not** re-enter the
  runtime or block on another lane: no body may call back into `Runtime`, submit
  more runtime work, or wait on another actor/op (with `hpx_threads=1` that could
  deadlock the single worker, and it is the gated composition fork regardless).
  **Cross-actor dependencies belong to a future composition/DAG design**, not the
  first actor slice.
* **No performance claim** anywhere in the actors, their tests, or their write-up.

## See also

* [rayx_runtime_phase1_summary.md](rayx_runtime_phase1_summary.md) — the shipped
  Phase 1 runtime this builds on.
* [rayx_runtime_value_model.md](rayx_runtime_value_model.md) — the closed typed
  value model actor methods reuse.
* [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) — the phase map;
  this is issue 5 / item 4 (local stateful native actors), **not** remote actors
  (issue 8) and **not** the public composition fork.
* [rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md) —
  P1 (registry + `hpx::async`), P2 (actions/components are remote machinery), P7
  (cooperative cancellation), P9 (rows separate from values), P10 (operation
  granularity), P11 (GIL-free workers), and the futures-vs-senders mechanism note.
* [rayx_runtime_internal_composition_note.md](rayx_runtime_internal_composition_note.md)
  — the internal-composition op direction; distinct from this actor axis.
