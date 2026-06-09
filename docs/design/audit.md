# RayX HPX/Runtime-Systems Audit — through Step B

**Status: accepted audit (docs-only, exploratory).** This is a point-in-time
HPX/runtime-systems review of the `rayx.runtime` prototype as of the staged
value-model stack plus the unstaged Step A (id-prefix) and Step B (HPX-free actor
registry header) changes. It lives under `docs/design/` (exploratory) deliberately
apart from `docs/reference/` (stable shipped contracts). It records findings and
recommendations; it is not itself a shipped contract and makes no performance or
"HPX beats Ray" claim. The decisions it triggers are tracked separately (see the
C0 decision memo and the local-native-actors design note).

Scope reviewed: `readme.md`; the runtime design notes (problem model, HPX design
principles, phase-1 summary, value model, internal-composition note,
local-native-actors note, phase-1 registered-operation API); the native sources
(`_rayx.cpp`, `runtime_ops.hpp`, `runtime_ops_hpx.hpp`, `runtime_actor_ops.hpp`,
`runtime_lane.hpp`, `runtime_cancel.hpp`); the Python layer (`runtime/__init__.py`,
`runtime/_validate.py`); and the smokes/tests/examples.

## 1. Executive verdict

This is a genuinely well-engineered project with an unusually honest design
discipline, and the current direction is coherent and defensible. The core systems
decision — *registered native operations over HPX-native FIFO lanes, GIL-free,
single-locality, with Ray-shaped control ergonomics* — is the right call for what
HPX is actually good at, and the codebase backs the docs: no concurrency
correctness bug was found in the shipped runtime, no double-fulfillment path, no
joinable-thread-destroyed hazard, and a careful GIL boundary.

The cancellation/admission/lifetime model is the strongest part of the code. The
HPX usage is sound and idiomatic for the *futures* generation of HPX (not senders,
deliberately and correctly). The value model (V1→V3) was the right next axis and is
appropriately narrow. The actor design note is excellent — it has already found and
pinned down the two hazards that will actually bite in actor implementation (the
`hpx::thread` lifecycle/partial-construction discipline, and the strict
one-at-a-time-retirement invariant for lock-free state). Step A and B are correct,
minimal, and verifiably inert.

The most important concrete finding:

> **`make_runtime_actor_id` produces only 32 bits of effective entropy.** It emits
> 8 hex chars, and it seeds a fresh `std::mt19937` from a single 32-bit
> `std::random_device()` draw per call — so even widening the printed width would
> leave effective entropy at the 32-bit seed. This is fine for a handful of op
> lanes but becomes an **observability correctness issue** once actors multiply:
> actor rows are keyed solely by `actor_id`, and a 32-bit space is birthday-fragile
> (~50% collision near ~77k ids). Two actors (or an actor and an op lane) colliding
> makes rows ambiguous about *which* instance produced them. Not a blocker, but it
> should be fixed before actor identity is load-bearing.

The biggest *project-level* risk is not technical — it is **ceremony overhead**:
the docs-to-code ratio and the slice granularity have become heavy enough that they
are starting to cost more than they protect (§12).

## 2. What we built

RayX is a Ray-vs-HPX comparison harness that grew a second, separate limb: an
experimental HPX-native runtime. Two cleanly-partitioned worlds share exactly one
thing — the process-singleton HPX bootstrap guard — and nothing else.

The original façade/harness work established the comparison anchor: a Python
frontend over `std::thread` `ServiceLane`s (default) or opt-in `HpxLane`s, behind a
`RayxLaneIface`/`RayxLaneAdapter<Lane>` seam. The adapter's central insight is
encoded in `kHpxHop`: `ServiceLane` uses `std::mutex`/`std::thread` and is safe to
touch from the external Python thread directly, whereas `HpxLane` uses
`hpx::mutex`/`hpx::thread` and so *every* lane-state operation must hop through
`hpx::run_as_hpx_thread`. That is the single most important HPX-correctness idea in
the repo, and it recurs everywhere.

The `rayx.runtime` Phase 1 work established the systems thesis: a *fixed C++
operation registry* (`square`/`add`/`boom`/`busy_sum`) dispatched over
`RuntimeLane`s — single-`hpx::thread` FIFO workers using `hpx::mutex` +
`hpx::condition_variable_any` for the idle wait and `hpx::async(exec_, task).get()`
for cooperative dispatch. It separated the **value** from the **row**:
`RuntimeResult` carries both; Python `OperationResult` exposes `.value` (raises on
failed/cancelled) vs `.row` (the exact 9 fields, no `value` key). It reuses one
`RuntimeCancelToken` for queued + checkpoint-boundary cancellation, adds bounded
per-lane admission (`try_submit`/`QueueFullError`), and provides
`get`/`wait`/`as_completed` via `hpx::wait_some`.

The staged value-model stack (V1→V3) added the typed substrate: registry entries
gained `arg_types`/`result_type`; the Python boundary validates per-arg types +
int64 range + strict-finite double; the value channel became `OpValue =
std::variant<std::int64_t,double>`; `scale_double` was added as the first `double`
op; and `fanout_sum` was added as the first *internally-composed* op (HPX
`async`-per-part + `when_all`) in a separate HPX-side registry header so
`runtime_ops.hpp` stays HPX-free. Conversion (`std::visit` → `py::cast`) happens in
`RuntimeFuture::result()` on the Python thread with the GIL held.

Step A made `make_runtime_actor_id` and the `RuntimeLane` constructor take an
*additive, default-`"rt-hpx-"`* prefix, so a future actor lane can mint `rt-act-`
ids consistently across both the row and `lane_stats()`. Step B added the HPX-free
actor registry layer (`ActorState`/`CounterState`, `MethodFn`/`MethodEntry`/
`ActorTypeEntry`, `actor_registry()`, `as_counter()`, and the `counter` type) plus
a bare `#include`. No wiring.

How actors fit: they are problem-model issue 5 — long-lived addressable native
state with a fixed registered method set, FIFO per actor. The design reuses
everything: one `RuntimeLane` per actor (the lane *is* the serialization domain and
mailbox), method bodies return the same `OpOutcome`→`RuntimeResult`→`hpx::future`,
so `get`/`wait`/`as_completed` and cancellation work with zero new collection code.

## 3. HPX mechanism analysis

* **`hpx::start`/`hpx::stop` + process-singleton guard.** `process_runtime_active()`
  is a single `std::atomic<bool>` flipped by `compare_exchange_strong` in both
  engine ctors — exactly what makes them mutually exclusive. Library-mode lifecycle
  (no `wrap_main`/`main()`), GIL released around start/stop, lanes joined before
  `hpx::stop`, guard cleared last. **Sound.**
* **`hpx::run_as_hpx_thread`.** Correct, consistent bridge for HPX-thread-only state
  (lane construction, submit, stats, stop/join). Synchronous semantics are what is
  wanted. **Sound.**
* **`RuntimeLane` as a primitive.** Good primitive; the note's defense of it (vs a
  continuation chain or `async_rw_mutex`) is correct *for RayX's requirements*: the
  lane needs an owned, inspectable queue for bounded admission, `lane_stats`, and
  queued-cancel-before-start — which neither `then`-chains nor `async_rw_mutex`
  provide. The worker model (idle wait suspends the HPX thread; pop-one; dispatch
  via cooperative `.get()`; retire-before-pop-next) avoids the experiment-16 bug
  class.
* **Worker + queue + token model.** The nested `hpx::async(exec_, task).get()` is a
  cooperative suspension because the worker is itself an `hpx::thread`; the
  `hpx::this_thread::yield()` in the `StopCheckpoint` keeps a CPU-bound op from
  starving siblings. Dispatching each body via a nested async (rather than inline)
  costs a task hop per op but buys a uniform suspension point and the P4 pool seam
  (`exec_`); for micro-ops the hop is overhead, honestly flagged under P10.
* **`hpx::when_all` (`fanout_sum`).** Idiomatic; division-based range split avoids
  overflow; sum-mod associativity makes it a self-checking oracle
  (`fanout_sum(n,parts) == busy_sum(n)`). The doc correctly warns it is a
  demonstration, not numeric best practice (real work → `hpx::transform_reduce`).
* **`hpx::wait_some` for `wait()`.** Well-justified: spans `num_returns==1` and `>1`
  uniformly, non-consuming; the move-out/RAII-`Restore`/move-back and the
  pre-`take()` duplicate check are correct and necessary.
* **Futures vs senders.** Deliberately futures + `hpx::async` — the stable path the
  lane/cancel/collection APIs are built on. Rewriting around senders now would be
  churn for no Phase-1 benefit; recorded honestly as a future axis. **Agree.**
* **Why no actions/components/localities.** Correct (P2): remote machinery, no local
  benefit; by-value signatures keep a later lift mechanical.
* **Blocking / starvation.** No path blocks an OS worker with a `std::` primitive on
  an HPX thread. `RuntimeCancelToken` deliberately uses `std::mutex` (not
  `hpx::mutex`) with `set_value` always outside the lock so it can run hop-free from
  the external Python thread without pinning a worker. **This is the subtle thing
  most people get wrong, and it is right here.**
* **One subtlety:** `submit_operation` performs the enqueue hop while holding the
  GIL. Because the hopped lambda is pure C++ (lock/push/notify) touching no Python,
  the worker never needs the GIL → no deadlock; it briefly serializes other Python
  threads across a microsecond hop. Keep the actor `call` path the same: marshal on
  the Python thread before the hop; keep the hop enqueue-only.

## 4. Correctness / lifetime analysis

* **Double promise fulfillment — impossible.** The single arbiter is the
  `RuntimeCancelToken` phase machine under `m_`. `begin_service` returns false iff
  `Cancelled`, and a queued `cancel()` is the only path that both sets `Cancelled`
  and fulfills. Queued-cancel-wins → cancel fulfills once, worker skips.
  Worker-wins → cancel can only reach `StopRequested` or return false; the worker's
  single `set_value` fulfills. The cancelled-but-still-queued item is correctly
  skipped on pop.
* **Future hang — no path.** `run()`'s drain-then-exit services/skips every queued
  item before returning; `cancel_pending()` pre-cancels so drain is prompt; every
  promise-creating path reaches exactly one `set_value` (including the defensive
  `catch(...)`).
* **Shutdown race — only under explicitly-unsupported concurrent use.** Shutdown
  runs cancel+join+clear in one GIL-released hop before `stop_process_hpx`; the
  single-driver assumption is documented. The actor map inherits the same
  assumption.
* **Joinable `hpx::thread` destroyed — handled.** `~RuntimeLane` is inert; the real
  stop/join is `stop_and_join()` under the hop, driven by `shutdown()` (and
  `~RuntimeEngine`). The ctor spawns `worker_` last; the engine ctor's
  partial-construction `catch` stop/joins built lanes on an HPX thread before
  rethrowing. This is the single most dangerous HPX footgun, and it is correct. The
  discipline must extend to `create_actor` (build state first; if anything after the
  lane exists throws, stop/join the lane on the hop before unwinding).
* **Actor state freed under a running body — not under the planned design.** The
  method closure must capture `std::shared_ptr<ActorState>` by value; state is
  dropped only after the worker joins. The load-bearing invariant — strict
  one-at-a-time retirement, so method N's write happens-before N+1's read — is
  correctly identified. Implementation risk: `make_method_task` must capture the
  `shared_ptr` by value, not a reference into the actor map.
* **Exception through a bad HPX boundary — no path.** Op bodies map their own
  throws to failed rows; the worker has a defensive `catch(...)` that still
  fulfills; `run_as_hpx_thread` propagates lambda exceptions to the (external,
  GIL-holding) caller for pybind translation. One cosmetic corner (a cancellable
  op's dispatch throwing while a concurrent cancel set `StopRequested` → `cancel()`
  returns true but the row is `failed` not `cancelled`) is unreachable for the
  built-ins; not worth fixing.
* **`actor_id` entropy (the one real finding).** 32 bits effective (8 hex; mt19937
  seeded from a single 32-bit `random_device` draw). Birthday-fragile in the
  thousands once actors are keyed by `actor_id` with no `actor_type`/`method` field
  (by D4). Recommend widening the *entropy source* (not just the printed width)
  before actor identity is load-bearing.

## 5. Python / GIL boundary analysis

* **HPX workers are GIL-free** by construction (D2/P11) — op bodies are pure native
  code; no Python callable runs on a worker. Python↔HPX contact is the submit
  crossing (GIL held, Python thread) and the blocking waits (GIL released around
  exactly the blocking call).
* **Python objects kept off workers.** Arg marshalling (`py::isinstance`/`cast` →
  `OpArgs`) on the Python thread before the hop; result conversion (`std::visit` →
  `py::cast`) in `result()` GIL-held after `fut_.get()`. No Python object is ever
  built on a worker.
* **pybind safety.** Move-only futures cast with `return_value_policy::move`; reject
  path returns `py::none()`; `valid()` guards convert raw HPX `no_state` into clean
  RayX errors on double-retire.
* **Boundary validation placement.** Correct two-layer split: import-light
  `_validate.py` at the public boundary (unit-testable without `_rayx`) + native
  defensive re-checks for the raw bypass. Error types coherent (`TypeError` for
  wrong type incl. explicit `bool` rejection; `ValueError` for range/arity/domain;
  `QueueFullError`; lazy `OperationFailedError`/`OperationCancelledError`).
* **`RuntimeFuture` reuse for actor methods — right, strongly.** A method returns
  the same `hpx::future<RuntimeResult>`, so collection/cancel work unchanged with no
  new code. Type-indistinguishability from op futures is fine; the `rt-act-` prefix
  distinguishes provenance.
* **`ActorHandle.call("method", *args)` — right shape.** Rejecting `.remote()` and
  attribute-style in favor of explicit `call` over a fixed registered set is the
  honest API. Keep `create_actor` on the explicit `Runtime` (D5).

## 6. Value model analysis

The value model was the right next step — `int64`-only was the binding constraint on
everything real, and it is lower-risk and more foundational than distribution.
Folding V2 into V3 (skip the single-alternative variant; introduce the variant with
its first real second alternative `double` + `scale_double`) is good judgment. The
"neither V1 nor V3 is an HPX mechanism step" observation is correct: the HPX
machinery only default-constructs/moves `RuntimeResult` and never reads `.value`, so
the variant change is transparent. The set is appropriately narrow (int64 + double,
strict finite, explicit int64 range check, conversion on the Python thread).
Actors should come before bytes (bytes is highest-risk and unneeded by
`CounterActor`); the existing ordering is right. Minor latent item: no compile-time
tie between the `OpType` enum, `op_type_name`, and `_TYPE_VALIDATORS` — it fails
closed at runtime, but a `static_assert`/self-check test would catch a future type
addition earlier. No debt that should block actors.

## 7. Internal composition analysis

Internal HPX composition under one public future/row makes sense; `fanout_sum` is
the right demonstration (fan out across cores, present one future + one 9-field row,
no child rows/futures, no HPX type leak). `when_all` usage is idiomatic and
self-checking. Queued-only cancellation for `fanout_sum` is honest — a launch-all
design has no honest running-cancel boundary once parts are in flight, and the code
says so. Pursue actors next, not more composition: composition has made its point
with one op; actors open a genuinely new axis (native state).

## 8. Local actor analysis

* **Makes HPX sense.** "The lane *is* the actor's serialization domain and FIFO
  mailbox" is the correct HPX-native framing; reusing `RuntimeLane` wholesale (only
  the id prefix changes) means actors inherit the proven worker, the
  cancellation/admission/stats surface, and the lifecycle discipline.
* **One `RuntimeLane` per actor is right for the first slice.** It is the only
  option among lane / `then`-chain / `async_rw_mutex` that gives the owned,
  inspectable queue the serving-control surface requires. The honest tradeoffs (one
  parked `hpx::thread` per actor; FIFO-serialized reads) are recorded accurately.
* **`async_rw_mutex<State>` / senders — later, if ever, and only on evidence.** It
  is the purpose-built HPX primitive for serialized read/write and would win on the
  two tradeoffs above, but it has no owned queue, so it cannot give bounded
  admission / queue_depth / queued-cancel — the differentiating surface. Revisit
  only if a many-idle-actors workload is demonstrated.
* **Step-B foundation is sound.** HPX-free; native state only (`CounterState::v` is
  a real `int64_t`, not an `OpValue` bag); defensive `as_counter` mirroring
  `as_int64`; `one_checkpoint` for all methods (queued-cancel-only → no partial-state
  corruption); reuse of the shared op types. The `#include` is bare and inert.
* **Real risks before implementation:** (1) `make_method_task` capture contract
  (capture `shared_ptr<ActorState>` and `OpArgs` by value); (2) `create_actor`
  partial-construction cleanup; (3) actor-map single-driver race (document, do not
  imply thread-safety); (4) `actor_id` entropy; (5) the re-entrancy ban (a method
  body must never call back into the runtime or wait on another lane).

## 9. Alternatives considered

* HPX actions/components from the start — **worse** (remote machinery, no local
  benefit); by-value signatures keep the later lift mechanical.
* Senders/receivers from the start — **worse now**, plausibly better later;
  correctly deferred.
* `async_rw_mutex<State>` for actors — **worse now** (loses the owned-queue
  serving-control surface), better later for a specific workload.
* One global queue — **worse** (destroys per-lane FIFO/admission/stats and, for
  actors, per-instance state serialization).
* Method bodies inline vs inner `hpx::async(...).get()` — **either is defensible**;
  the current inner-async is fine (uniform suspension + pool seam; the micro-method
  hop is disclaimed as non-benchmark). Not worth changing.
* Arbitrary Python callables — **much worse**, correctly excluded (reimports the GIL
  onto workers, unkillable cancellation, object-store-of-pickles). The most
  important "no" in the project.
* Ray-compatible `.remote()` façade — **worse** (implies object-store/future
  semantics RayX does not have).
* Object-store-like values — **worse**, HPX's weakest ground; correctly
  evidence-gated / possibly-never.
* Benchmarks-only vs building the runtime — **building it was right**; the runtime
  is where the interesting systems thesis lives, and the strict separation prevents
  contamination.
* Bytes or composition before actors — **worse than actors-next**.

## 10. Recommendations

**Must fix before actor engine implementation:** none in shipped code or Steps A/B.
The two would-be blockers are not yet written (they are implementation plumbing) and
belong in the implementation plan: pin `make_method_task`'s by-value capture
contract, and specify `create_actor`'s partial-construction cleanup.

**Should fix soon:** widen `actor_id` entropy (source, not just width) before actor
identity is load-bearing; add an `OpType`↔name↔validator consistency check.

**Good to defer:** running-cancel of checkpointed state mutation; `async_rw_mutex`/
sender actors; a second actor type; bytes; the P4 named-pool seam; a per-actor
admission cap argument; inlining method bodies.

**Do not do:** `ObjectRef`/object store; arbitrary Python callables; `.remote()`;
dynamic method registration; HPX actions/components locally; any
`value`/`actor_type`/`method` field in the v1 row; a per-actor running-kill;
presenting `CounterActor` timing as a benchmark.

## 11. What we did well

The `kHpxHop` adapter dichotomy; the `CancelToken` using `std::mutex` with
`set_value` outside the lock; checkpoint-count-armed cancellability (race-free
arming); value/row separation enforced in code; the HPX-free / HPX-side registry
split; import-light `_validate.py` with native backstops; the `wait()` RAII restore
+ pre-`take()` duplicate check; the actor design note doing the hard thinking before
code; and the pervasive honesty discipline (queued-only cancel stated as a
limitation, micro-op timing disclaimed, "no performance claim" everywhere).

## 12. What we overdid

* **Docs-to-code ratio has inverted.** Several docs re-state the same guardrails
  (no object store / no arbitrary Python / no perf claim / GIL-free / value-row
  separation) in full, in parallel. A guardrail change now has to be made in many
  places. Consider a single canonical guardrails anchor that the others *link*.
* **Slice granularity is very fine.** A header-only inert step (Step B) is real
  ceremony for a runtime no-op. Resist splitting further than necessary.
* **Internal diagnostic scaffolding** (`_submit_batch_cost_probe`,
  `_set_bulk_enqueue`) is well-marked but is shipped surface area.
* **The cancellation state machine is at the edge of "too clever" for the runtime's
  one checkpointed op** — justified by harness-token reuse fidelity, so keep it, but
  do not grow it.

None of these are correctness problems; they are velocity problems.

## 13. Clean next path

Proceed to actor work, but lock two decisions first (the C0 memo): the
`make_method_task` by-value capture contract, and the `actor_id` entropy question.
Then implement the native actor engine plumbing (records + map + `make_method_task`
+ `create_actor` with the HPX-hop lifecycle + `call_actor_method` + shutdown
extension + `runtime_actor_table()` + native pybind), with no Python `ActorHandle`
and no tests in that first plumbing slice; then add the Python `ActorHandle`,
boundary validation, and the full unit/integration/smoke coverage in a following
slice. Do not add more scaffolding steps; front-load the two correctness decisions.

## See also

* [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md)
* [rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md)
* [rayx_runtime_local_native_actors.md](rayx_runtime_local_native_actors.md)
* [rayx_runtime_value_model.md](rayx_runtime_value_model.md)
* [rayx_runtime_internal_composition_note.md](rayx_runtime_internal_composition_note.md)
