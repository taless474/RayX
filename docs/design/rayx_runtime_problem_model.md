# RayX Runtime Problem Model

**Status: design / modeling only.** This document defines *what would need to be
designed* to grow RayX toward an HPX-native serving-control runtime. It is **not**
a delivery roadmap, not a commitment, and not a description of shipped behavior.
It lives under `docs/design/` (exploratory) deliberately apart from
`docs/reference/` (stable contracts for shipped APIs). Nothing here is
implemented.

The HPX-native *how* for this model — which HPX mechanism is the primary design
tool at each phase, and which Ray-shaped instinct to avoid — lives in the
companion doc
[rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md).
Read that alongside this one.

**Terminology.** A RayX **"registered native operation"** (the term used below) is
**not** an HPX action. An HPX action is remote-dispatch machinery (AGAS-
registered, parcel-marshalled) and belongs to the **distributed phase** (Phase 5).
Locally (Phases 1–2) an operation is just a native entry in a function registry
dispatched with `hpx::async`. RayX operation ≠ HPX action until the remote phase.

## Settled goal (and non-goal)

The goal is **an HPX-native serving-control runtime that Ray users find
familiar** — not full Ray-compatible Python ergonomics.

* **In:** familiar control *shapes* (per-request handles, as-completed / wait
  collection, cancellation, admission, actor-style state) over local — and later
  distributed — HPX-backed lanes running **real native work**.
* **Out (target-level, not just "later"):** arbitrary remote Python execution,
  Ray's object store, and full Ray compatibility. These are not deferred Ray
  features we are ramping toward; they are deliberately **not the target**.

Consequence: **registered native operations are the design choice, not a stepping
stone to arbitrary Python.** A registered-operation model is the HPX-native
answer. It is *better suited than arbitrary remote Python for this project's
goals* precisely because it diverges from Ray's arbitrary-execution model: it
keeps the boundary narrow,
keeps cancellation tractable (cooperative operations can stop at checkpoints;
arbitrary Python cannot be force-killed safely on HPX), and avoids an
object-store-of-pickles. We do not present it as a path to Ray's developer
experience, because that is not where this is going.

## Three layers (keep nameable at all times)

1. **Current RayX harness — shipped, stable.** The synthetic comparison harness:
   `Engine` / `SyntheticActor`, consume-once `Future` retiring to a measurement
   row, `get` / `wait` / `as_completed`, queued + chunk-boundary cancellation,
   bounded admission / `QueueFullError`, `lane_stats()`, `lane_impl="std"/"hpx"`.
   See [../reference/rayx_actor_api.md](../reference/rayx_actor_api.md) and
   [../reference/rayx_frontend_design.md](../reference/rayx_frontend_design.md).
   **This layer is frozen for the purposes of the runtime work** (see "Locked
   decisions").
2. **Future RayX runtime prototype — exploratory, this document.** A new,
   explicitly-experimental layer that runs real native operations behind the same
   serving-control lanes. It lives in a separate namespace and never edits the
   harness layer's types or schema.
3. **Actual Ray compatibility — explicitly not a goal.** We do not aim to be a
   drop-in Ray, to match Ray's API, or to replace Ray.

The conceptual Ray↔HPX background for all three is
[../ray_hpx_mapping.md](../ray_hpx_mapping.md) ("Three stories to keep
separate").

## Locked design decisions

These are constraints on any runtime-prototype work, not preferences:

* **D1 — Two frozen worlds, never overloaded.** The harness `Future →
  measurement-row` world stays exactly as-is. Any user-value semantics live in a
  **separate type and namespace**. No single `get()` ever means two things on one
  type.
* **D2 — Registered native operations, not arbitrary Python.** A task runs a
  *named, pre-registered* native operation with typed inputs (dispatched locally
  via `hpx::async`; not an HPX action until the remote phase). Arbitrary pickled
  closures / arbitrary remote Python are out at the target level.
* **D3 — Value-return does not require ObjectRef.** A Phase-1 task returns a user
  value **directly** through the existing retire path *plus* the existing row. For
  *intra-locality dependencies*, the HPX-native mechanism is **future composition**
  (`hpx::future::then` / `when_all` / `dataflow`), not a reference/handle. An
  `ObjectRef`/object store is a *later, evidence-gated* question — justified only
  if reusable / shared / addressable values are proven necessary **beyond a future
  graph** (especially cross-locality or large shared data) — not a Phase-1 type
  (see "Considered and rejected").
* **D4 — Frozen v1 measurement schema.** The benchmark JSONL / analyzer schema is
  not edited by runtime work. Runtime-only observability (locality, actor id,
  object movement) goes in a *separate* record type, never inside the v1 row.
* **D5 — Explicit engine/runtime only.** No global default engine, no module-level
  `rayx.get` / `rayx.wait`. The runtime API hangs off an explicit object, same as
  the harness. (Rationale carried over from
  [../reference/rayx_frontend_design.md](../reference/rayx_frontend_design.md) §2.)
* **D6 — Distributed implies a failure model.** Any multi-locality phase ships its
  minimal error/failure model in the *same* phase; distributed work without a
  death story is unsafe and therefore not a valid phase on its own.
* **D7 — Naming discipline.** The harness keeps the name RayX and stays stable;
  runtime work is the "RayX runtime prototype" in an experimental subnamespace,
  with docs stating plainly that current RayX is **not** Ray-compatible.

## Problem map (12 issues)

Each issue: problem it solves · what Ray provides · what RayX provides now · what
RayX would need to design · possible HPX mechanism · minimal prototype scope ·
explicitly out of scope · risks / hard parts · how we would test it.

### 1. Execution model
* **Solves:** running user-meaningful work, not only fixed synthetic service.
* **Ray:** arbitrary `@ray.remote` tasks returning ObjectRefs.
* **RayX now:** one fixed synthetic op (`sleep`/`spin` for `service_ms`), row only.
* **Design:** `Task = (registered_operation_id, typed_args) → (value, row)`, run on
  the existing serving-control lane. Arbitrary Python is **not** the target (D2).
* **HPX:** a function registry + `hpx::async` over an executor dispatches a
  registered C++ functor (local first); **not** an HPX action/component locally.
* **MVP:** one registered native operation (e.g. `square(int) → int`) behind a
  lane, producing a value and the existing row.
* **Granularity:** the `square(int)` MVP is a **correctness/design** example only,
  not performance evidence — operations must be coarse enough to amortize the
  Python→C++ crossing + HPX scheduling overhead before any timing is interpreted
  (see principles **P10**).
* **Out of scope:** arbitrary closures, dynamic codegen, multi-language tasks.
* **Risks:** the registry becoming a backdoor to arbitrary execution; value/row
  confusion (handled by D1).
* **Test:** correct value + well-formed row; unknown operation id rejected at the
  Python boundary (mirrors `work_mode` validation).

### 2. Dependencies, ObjectRef, and object store
* **Solves:** expressing "B depends on A" and referring to / passing computed
  values without inlining them.
* **Ray:** distributed object store + ObjectRef, zero-copy, spilling, lineage.
* **RayX now:** none.
* **Design:** **HPX future composition is the primary dependency mechanism**, not
  ObjectRef. Intra-locality dependencies compose `hpx::future` directly
  (`then` / `when_all` / `dataflow`), so no addressable intermediate value is
  needed. ObjectRef is **demoted** and not Phase 1 (D3): if ever pursued it is a
  *return-value handle* separate from `Future` (D1), local-only first, with
  idempotent reads — **never** a `get()` overload — and only when reusable /
  shared / addressable values are proven necessary **beyond a future graph**
  (especially cross-locality or large shared data). A general object store is an
  open question, possibly answered "no" (see "Considered and rejected").
* **HPX:** `hpx::future` composition (`then` / `when_all` / `dataflow`) for
  dependencies; only if an addressable store is ever justified, a value table with
  AGAS-style addressing for the remote case.
* **MVP (only if reached):** no ObjectRef by default; if an addressable handle is
  ever justified, a local handle for one supported type with explicit release /
  refcount.
* **Out of scope:** spilling, zero-copy, cross-locality store, lineage, pinning;
  exposing future composition as default public API (that is the story-3 → story-2
  fork — see issue 6 and the companion principles doc).
* **Risks:** the object store is where HPX is *weakest* and the most-excluded
  feature; pursuing it early steers into RayX's least-credible comparison ground.
* **Test (only if reached):** composed dependency completes in correct order with
  no ObjectRef type present; if a handle is ever added — idempotent reads; release
  frees; refcount underflow rejected.

### 3. Serialization and data movement
* **Solves:** moving args/results into/out of operations (and later across localities).
* **Ray:** Arrow / pickle5, zero-copy numpy, plasma.
* **RayX now:** none (scalars/strings only, no payloads as data).
* **Design:** a **small closed type set** with explicit codecs; everything else
  rejected at the boundary. In-process copy first.
* **HPX:** `hpx::serialization` for cross-locality; direct copy/move in-process.
* **MVP:** `int`, `float`, `bytes` by value, in-process copy only.
* **Out of scope (now):** zero-copy, numpy/Arrow, arbitrary Python objects, custom
  C++ buffers, cross-locality serialization (until the distributed phase).
* **Risks:** the type set creeping toward "arbitrary objects" (re-creating an
  object store of pickles).
* **Test:** round-trip each supported type; unsupported type → `TypeError`; size
  guard rejects oversized payloads.

### 4. Work placement (not a scheduler)
* **Solves:** deciding *where* a task runs.
* **Ray:** scheduler with resources, placement groups, data/locality-aware placement.
* **RayX now:** internal round-robin across local lanes; per-lane admission.
* **Design:** placement is **executor / thread-pool selection** (statically
  resource-partitioned at runtime construction) plus, when remote, **AGAS locality
  resolution** — **not** a custom dynamic scheduler. Keep **internal round-robin**
  as default; no client-selected placement. Bounded admission stays **per-lane**; a
  global cap, if ever, is a separate additive concept.
* **HPX:** executors bound to named pools from the resource partitioner choose
  where local work runs; AGAS resolves a global id to its locality for remote work
  (move work to data).
* **MVP:** unchanged round-robin; document that placement is not user-controlled.
* **Out of scope:** placement groups, resource accounting, affinity/data-aware
  scheduling, client-selected lane/locality, any custom per-task scheduler.
* **Risks:** "fake Ray placement / fake Ray scheduler" — exposing a handle users
  think controls placement when it does not, or reinventing scheduling HPX already
  provides via executors/pools + AGAS.
* **Test:** distribution stays round-robin; admission still per-lane; no API admits
  client-chosen placement or a custom scheduler.

### 5. Actors and state
* **Solves:** long-lived, addressable, stateful entities with methods.
* **Ray:** stateful actors, arbitrary methods, FIFO per actor, remote placement,
  restart.
* **RayX now:** `SyntheticActor` = façade over one synthetic op; structurally
  rejects other methods.
* **Design:** **native registered-method actors:** state + a fixed set of
  *registered* methods (by id), serialized **FIFO per actor** (the lane already
  gives this). Arbitrary Python methods are not the target (D2).
* **HPX:** a **lane-owned native state object** locally (FIFO-served by the
  existing lane); methods are registered operations dispatched FIFO. An HPX
  **component** (with a global id + client handle) is **not** introduced locally —
  it belongs to the remote phase, only when an actor must be remotely addressable.
* **MVP:** local actor with `state:int`, two registered methods (`incr`, `read`),
  FIFO, returning value + row.
* **Out of scope:** arbitrary Python methods, remote actors (issue 8), actor
  restart/supervision, named-actor registry.
* **Risks:** method registry becoming arbitrary execution; state lifetime vs handle
  lifetime confusion.
* **Test:** FIFO ordering; state visible across calls; unknown method id rejected.

### 6. Future / ownership semantics
* **Solves:** coherent handle semantics across the two layers.
* **Ray:** ObjectRef = reusable, idempotent `get`, refcounted.
* **RayX now:** `Future` = consume-once measurement handle; `repr` non-consuming.
* **Design:** **keep `Future` exactly as-is** for harness rows (frozen). Phase 1
  returns a value through the existing retire path (D3) — no new handle type yet.
  Dependencies (if ever exposed) are expressed by **HPX future composition**, not
  a reference type; exposing that composition is the **story-3 → story-2 fork**
  (serving-control vs HPX-native task-graph) and is *not* automatic public API. If
  a reference type is ever added (issue 2), it is *distinct* from `Future` with
  *idempotent* reads, never a merge.
* **HPX:** the runtime handle mirrors `hpx::future` consume-once semantics
  (move-once), like the harness `Future`; `then` / `when_all` / `dataflow` would be
  a separate, explicitly-labeled composition surface only if the fork is taken.
* **MVP:** no new type in Phase 1; a doc table contrasting consume-once vs (future)
  idempotent reads.
* **Out of scope:** making `Future` idempotent; auto-converting between handle
  types; exposing future composition as default public API.
* **Risks:** user confusion — mitigated by D1 (distinct names/namespace/docs); and
  story-3 → story-2 drift if composition is exposed without an explicit decision.
* **Test:** `Future.result()` still raises on 2nd call; Phase-1 value retrieval is
  well-defined and does not change `Future` semantics.

### 7. Cancellation and backpressure
* **Solves:** stopping work and shedding load with real tasks/actors.
* **Ray:** best-effort task cancel, actor kill, resource/queue backpressure.
* **RayX now:** queued-skip + chunk-boundary running cancel; local per-lane
  admission-by-rejection.
* **Design:** **cooperative cancellation** for native operations (queued skip +
  checkpoint stop), reusing the existing `CancelToken`. No forced thread kill.
  Admission stays **local per-lane rejection**; "distributed backpressure" is a
  later, separate concept. Cancellation does **not** delete values/handles.
* **HPX:** cooperative cancellation via `CancelToken`; HPX has no safe forced kill
  of a running thread — this is *why* operations must be cooperative (D2).
* **MVP:** queued task cancel; cooperative checkpoint cancel for a chunked native
  operation.
* **Out of scope:** forced interruption of non-cooperative work; cancel-deletes-
  value semantics; global/distributed backpressure.
* **Risks:** users expecting Ray's `kill`; coupling cancel to value lifetime.
* **Test:** queued cancel → cancelled row, no value; cooperative running cancel
  stops at a checkpoint; existing values/handles unaffected by cancel.

### 8. Remote / distributed HPX localities
* **Solves:** running work (and locating results) across processes/nodes.
* **Ray:** cluster of nodes, GCS, transparent remote refs.
* **RayX now:** single process / single HPX runtime; nothing distributed exposed.
* **Design:** **explicit, static bootstrap first** (user names a fixed locality
  set); a remote task is a registered operation dispatched to a chosen locality
  (here it is backed by an **HPX action**); results/handles are located by **AGAS /
  global-id resolution** — RayX does **not** invent a separate results locator
  (AGAS is the directory). Python sees **opaque RayX handles that wrap an
  `hpx::id_type` internally**; no HPX GID or HPX type crosses into Python.
  **Ships with its failure model in the same phase (D6).**
* **HPX:** HPX localities + parcelport + AGAS; HPX actions for remote operation
  dispatch; components + clients for remote stateful actors; `hpx::async` to a
  remote locality.
* **MVP:** **2 localities, static config**, one remote registered operation, result
  resolved via AGAS with one explicit step, plus error-on-unreachable.
* **Out of scope:** elastic add/remove, autodiscovery, transparent global
  namespace, cross-locality zero-copy; exposing HPX GIDs/types to Python.
* **Risks:** large surface; serialization (issue 3) is a hard prerequisite;
  partial-failure semantics are genuinely hard.
* **Test:** 2-locality smoke — remote result correct; wrong/unreachable locality
  errors cleanly (see issue 9).

### 9. Fault tolerance and lifecycle
* **Solves:** behavior under errors / death.
* **Ray:** worker/actor restart, retries, lineage reconstruction, object loss
  handling.
* **RayX now:** none — synthetic work does not throw; graceful drain only.
* **Design:** **error-as-value MVP:** a native operation that throws yields a
  *failed* row (`status="failed"`, captured message), not a crash; actor/locality
  death surfaces as an **explicit error** on the handle. No auto-restart, no
  lineage. This MVP is **part of the distributed phase**, not a sequel (D6).
* **HPX:** exceptions propagate through `hpx::future`; locality loss surfaces as
  transport/parcel errors.
* **MVP:** operation exception → failed row; reading a failed result raises a clear
  RayX error; (distributed) locality-down → handle error.
* **Out of scope:** retries, lineage/reconstruction, actor restart, supervision,
  object spill/recovery.
* **Risks:** users assuming Ray-grade durability; distributed partial failure.
* **Test:** raising operation → failed row; failed-result read raises typed error;
  locality-down → handle error.

### 10. Python API surface
* **Solves:** Ray-familiar *shape* without misleading *semantics*.
* **Ray:** `@ray.remote`, `ray.get`, `ray.put`, `ray.wait`, global init.
* **RayX now:** explicit `Engine` / `SyntheticActor`; no `@rayx.remote`, no
  module-level `get`/`wait`, no global engine.
* **Design:** **explicit runtime stays mandatory (D5).** Any registration sugar is
  `runtime.register_operation(...)`-style on an *explicit* runtime — never a global
  `@rayx.remote`, never module-level `get`/`wait`. Ray-like *shape* (a handle +
  `wait`/value) is fine; Ray-like *globals* are not.
* **HPX:** n/a (API layer).
* **MVP:** all runtime API hangs off an explicit `Runtime` object in a separate
  namespace.
* **Out of scope:** global default engine, module-level get/wait, `@rayx.remote`
  on arbitrary callables.
* **Risks:** Ray muscle-memory; users importing it expecting Ray.
* **Test:** no module-level get/wait exists; runtime API requires an explicit
  object; registration never auto-runs.

### 11. Measurement and observability
* **Solves:** keeping RayX's measurement strength while adding value semantics.
* **Ray:** dashboards/timeline; values primary, timing secondary.
* **RayX now:** authoritative per-request rows; fixed JSONL schema.
* **Design:** **rows stay separate from values (D1, D4).** A runtime task exposes a
  value *and* the existing row shape; the v1 JSONL/analyzer schema is **frozen**;
  new runtime observability (locality, actor id, object movement) goes in a
  *separate* record, never edited into the v1 row.
* **HPX:** steady_clock service timing as today; runtime adds movement timestamps
  in its own layer only.
* **MVP:** runtime task yields value + the *existing* row unchanged.
* **Out of scope:** changing the v1 schema/analyzer; merging value+row into one
  dict.
* **Risks:** accidental schema drift (the project's explicit fear) — mitigated by
  D4 + a golden check.
* **Test:** golden check that the v1 row schema is unchanged; runtime records
  validated separately.

### 12. Scope and naming
* **Solves:** preventing "RayX is already Ray" misreadings.
* **Ray:** n/a.
* **RayX now:** documented as a synthetic harness, not Ray.
* **Design (D7):** harness keeps the name RayX and stays stable; runtime work is the
  "RayX runtime prototype" in an experimental subnamespace; docs state plainly that
  current RayX is **not** Ray-compatible.
* **HPX:** n/a.
* **MVP:** this doc + the namespace decision; no harness behavior change.
* **Out of scope:** renaming/breaking the harness; any Ray-compatible claim.
* **Risks:** scope bleed from prototype into the stable harness.
* **Test:** CI/docs assert the harness API + v1 schema are unchanged when prototype
  work lands.

## Phase order

Phases describe *what each step would need to define*, not commitments.

* **Phase 0 — Problem model.** This document: the settled goal, locked decisions,
  problem map, out-of-scope list. No code.
* **Phase 1 — Registered native operation behind the existing lane.** A named
  native operation (function registry + `hpx::async`; **not** an HPX action) runs
  on a serving-control lane and returns a **user value plus the existing
  measurement row**. **No new `ObjectRef` type** (D3). Reuses the lane, FIFO,
  cancellation, and admission already shipped. (Issues 1, 6, 11; minimal 3.)
* **Phase 2 — Local stateful native actors.** Actor = lane-owned state + registered
  FIFO methods, returning value + row. **No HPX component yet.** (Issue 5.)
* **Phase 3 — Optional dependency-composition fork (serving-control stays
  primary).** Intra-locality dependencies, *if ever exposed*, use **HPX future
  composition** (`then` / `when_all` / `dataflow`) — **not** ObjectRef. Exposing a
  composition surface is the deliberate **story-3 → story-2 fork**, not automatic
  public API; the default is no user-visible composition. **No `ObjectRef` by
  default.** (Issues 2, 6.)
* **Phase 4 — Closed-type serialization; a real object store only if evidence-
  gated.** A small typed arg/result serialization set for the remote case; an
  addressable store only if reusable / shared / addressable values are proven
  necessary beyond a future graph (especially cross-locality or large shared
  data) — possibly never. (Issues 2, 3.)
* **Phase 5 — Distributed HPX locality MVP with failure model included.** Static
  2-locality bootstrap; a remote registered operation backed by an **HPX action**;
  remote stateful actors as **HPX components + clients**; **AGAS / global-id
  resolution** (no separate locator); opaque RayX handles wrapping GIDs; **and**
  the error-as-value / locality-down model via `hpx::future` exceptions in the
  *same* phase (D6). (Issues 8, 9, 3-remote, 4-placement.)

## Smallest first milestone

A single design artifact (this document) plus one **worked paper example** — not
code:

> Register native operation `square(int) → int`. Submitting it on a lane returns a
> value `4` through the existing retire path, alongside the existing measurement
> row. The harness `Future`/measurement-row world is untouched; no `ObjectRef`
> exists.

Milestone deliverables: the locked decisions D1–D7, the registered-operation
stance, the value-without-ObjectRef rule, the frozen v1 schema rule, and the
naming/namespace decision. Design only.

## Dependency gates

**Before any ObjectRef / return-value handle (after Phase 3):**
1. A value-producing execution model exists (Phase 1).
2. **HPX future composition has been shown insufficient** for the dependency need
   — i.e. a *demonstrated* need for reusable / shared / addressable values
   **beyond a future graph** (not hypothetical), else the handle is unjustified.
3. The type-separation rule (D1) is honored: distinct type, idempotent reads.
4. The frozen-schema rule (D4): the handle must not edit the v1 row.

**Before any remote actors (within Phase 5):**
1. Local stateful actors with registered FIFO methods (Phase 2).
2. A working local value path (Phase 1) and, if used, a local handle (Phase 3).
3. Cross-locality serialization for the supported type set (issue 3).
4. Locality bootstrap + addressing/routing (issues 8, 4).
5. A failure model for unreachable locality / dead actor (issue 9) — shipped in the
   same phase (D6).

## Considered and rejected (provenance)

Recorded so these are not re-proposed cold:

* **ObjectRef-first / object-store-early — rejected for Phase 1.** Reasons: (a) the
  object store is the subsystem where HPX is *weakest*
  ([../ray_hpx_mapping.md](../ray_hpx_mapping.md), "Where the analogy breaks"), so
  prototyping it early steers into RayX's least-credible comparison ground; (b) it
  is the most explicitly-excluded feature in the project guardrails; (c) a value
  can be returned **directly** without any reference type (D3), so ObjectRef is not
  required to make tasks useful; (d) RayX's demonstrated strength is
  *serving-control* (lanes, FIFO, cancellation, admission, the `std`/`hpx`
  scheduling-mechanism divergence), not a data plane; (e) the HPX-native way to
  express intra-locality dependencies is **future composition** (`then` /
  `when_all` / `dataflow`), which removes most of the motivation for an
  addressable reference in the first place. ObjectRef is therefore demoted to an
  *evidence-gated* question **after** the Phase-3 composition fork (justified only
  beyond a future graph), and a general object store to a possibly-never Phase 4
  question.
* **Registered operations as a ramp to arbitrary Python — rejected.** Registered
  native operations are the *target design*, not a transitional step (D2).
  Arbitrary remote Python is not a goal at any phase.
* **Value-return implies a new handle type — rejected.** Phase 1 returns the value
  through the existing retire path; introducing a handle type in Phase 1 was
  over-scoping (D3).
* **Distributed before a failure model — rejected.** Multi-locality without a death
  story is unsafe; the failure model ships in the same phase (D6).

## Out of scope (target-level, not just deferred)

* Arbitrary remote Python execution.
* Pickled closures / arbitrary serialized callables.
* Global `rayx.get` / `rayx.wait`.
* A global default engine.
* Ray Serve / Ray Train / Ray Tune / Ray Data.
* An object store of arbitrary Python objects.
* Full Ray compatibility.
* Any "Ray replacement" claim.
* Any "HPX beats Ray" or performance-superiority claim.

Real model inference also remains out of scope; native operations are real work,
but the project does not introduce model backends.

## See also

* [rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md) —
  the HPX-native design principles for this model: which HPX mechanism is the
  primary tool per phase and which Ray-shaped instinct to avoid.
* [../reference/rayx_actor_api.md](../reference/rayx_actor_api.md) — current
  harness API (Engine / SyntheticActor) and the Ray actor-pool mapping (§8).
* [../reference/rayx_frontend_design.md](../reference/rayx_frontend_design.md) —
  harness design rationale: Future ownership, why no module-level `get`, the
  `lane_impl` seam (§13).
* [../ray_hpx_mapping.md](../ray_hpx_mapping.md) — Ray↔HPX conceptual mapping and
  the three stories to keep separate.
* [../reference/hpxlane_backend_arc.md](../reference/hpxlane_backend_arc.md) —
  the `HpxLane` backend evidence arc (exp16 → exp23).
