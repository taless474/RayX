# RayX Runtime: HPX-Native Design Principles

**Status: design / modeling only.** This is the HPX-native lens for the runtime
problem model ([rayx_runtime_problem_model.md](rayx_runtime_problem_model.md)).
It is exploratory, contains **no implementation**, is **not** a delivery
commitment, and is **not** a Ray-compatibility claim. It lives under
`docs/design/` (exploratory) deliberately apart from `docs/reference/` (stable
contracts for shipped APIs). Nothing here is shipped.

Its purpose: ensure the runtime is designed with HPX's own mechanisms as the
primary tools, rather than Ray-shaped concepts mapped onto HPX. Where a Ray
instinct and an HPX mechanism diverge, this document records which to prefer and
why.

## Why this companion exists

The problem model defines *what* a serving-control runtime would need. This doc
constrains *how*, in HPX terms. The recurring failure mode it guards against is
reaching for a Ray-shaped subsystem (an object store, a placement scheduler, a
results locator) when HPX already provides the capability through a different,
leaner mechanism (future composition, resource-partitioned pools, AGAS).

## HPX vocabulary vs RayX vocabulary

Several words mean different things in Ray, in HPX, and in the RayX runtime
prototype. Keeping them distinct prevents the design from drifting into a fake
Ray clone or from misusing HPX terms.

* **RayX "registered native operation" is not an HPX action (in Phase 1).** A
  Phase-1 operation is a local entry in a function registry, dispatched with
  `hpx::async` on an executor. It is *not* an `HPX_PLAIN_ACTION`.
* **HPX actions are remote-dispatch machinery.** An HPX action is an AGAS-
  registered, parcel-marshalled type for invoking a function on a (possibly
  remote) locality. Actions earn their place only at the **distributed phase**;
  introducing them locally pays AGAS/parcel cost for no local benefit.
* **HPX components belong to remote/stateful component phases, not local Phase
  1/2.** A component is a server-side stateful object with a global id and a
  client handle, addressable across localities. A *local* stateful actor needs
  none of that — the serving-control lane already gives single-consumer FIFO
  state. Components appear only when an actor must be **remotely addressable**.
* **AGAS is the directory/addressing mechanism.** HPX's Active Global Address
  Space resolves a global id to the locality that holds it. The runtime must
  **not** invent a Ray-style results locator or registry that competes with
  AGAS; resolving a handle *is* an AGAS lookup.
* **Python sees opaque RayX handles, never HPX GIDs or HPX types.** When the
  runtime goes remote, a handle wraps an `hpx::id_type` internally but is exposed
  at the Python boundary as an opaque RayX identifier. No HPX type crosses into
  Python — the same guardrail the harness already honors.

For the Ray↔HPX conceptual background and the "three stories to keep separate,"
see [../ray_hpx_mapping.md](../ray_hpx_mapping.md). The serving-control lane is
**story 3** (a synthetic actor-like anchor); HPX-native task/dataflow programming
is **story 2**. This distinction drives Principle 3 below.

## Principles

### P1 — Local dispatch is a function registry + `hpx::async` on an executor
The smallest HPX-native execution path is a registry mapping an operation id to a
native callable, dispatched via `hpx::async` on a chosen executor, with the
result delivered through an `hpx::future`. This is the dispatch floor HPX is built
for (cf. the `hpx::async` floor in
`../../experiments/15_hpx_native_lane_feasibility/`). No action/component
machinery is required locally.

### P2 — Actions and components are remote machinery
Do not model local operations as HPX actions, or local actors as HPX components,
"to future-proof." That inverts HPX's own layering. Keep operation/method
*signatures* serialization-friendly (typed, by-value, from the closed type set)
so a later lift to actions/components is mechanical — but introduce the action/
component types only when crossing localities actually requires them. (Evidence
that local task/dataflow pools are *not* drop-in lane backends:
`../../experiments/20_hpx_task_dataflow_probe/`.)

### P3 — Dependencies are HPX future composition, not ObjectRef-first
The HPX-native way to express "B depends on A" is to compose futures —
`hpx::future::then`, `hpx::when_all` / `when_some`, `hpx::dataflow` — not to
materialize an intermediate addressable value. For intra-locality dependencies,
**future composition is the preferred alternative to an ObjectRef/object store.**

This has two consequences:

* **The primary path stays serving-control (story 3).** `hpx::future::then`,
  `when_all`, and `dataflow` may be used **internally** to implement a dependency
  between operations; what is gated is **exposing** them as public API. A public
  composition surface would shift RayX toward HPX-native task-graph programming
  (story 2) — a deliberate **design fork reserved for an explicit Phase-3
  decision, never an accidental API leak**. The default is: no user-visible
  dependency-composition surface.
* **ObjectRef stays delayed and evidence-gated.** Because composition handles
  intra-locality dependencies, an ObjectRef/object store is justified only if a
  concrete need is proven for **reusable / shared / addressable values beyond a
  future graph** — especially **cross-locality or large shared data**. Until
  then, there is no ObjectRef.

### P4 — Placement is executors / pools / resource partitioning, not Ray placement groups
"Where work runs" is decided by selecting an HPX executor bound to a named thread
pool created by the resource partitioner at startup — a **static** partitioning,
chosen declaratively at runtime construction. This is the HPX-native alternative
to Ray placement groups / resource specs / affinity scheduling. The runtime must
**not** build a dynamic per-task placement scheduler or expose client-selected
placement; lane/pool choice stays internal (round-robin by default), mirroring
the harness. (`hpx_threads` maps to the worker count of a pool; the std-vs-hpx
concurrency behavior is the executor/pool story characterized in
`../../experiments/22_rayx_hpxlane_load_divergence/`.)

**Future design axis (pseudocode / illustrative configuration only — not an API
proposal and not a committed signature).** Runtime construction may eventually
declare named HPX pools — e.g. a `control` pool and a `work` pool — created once
by the resource partitioner at startup. The following is illustrative pseudocode,
not a proposed `Runtime` signature:

```python
# Pseudocode — illustrative configuration shape only, not a committed API.
Runtime(
    pools={
        "control": 1,
        "work": 4,
    }
)
```

It illustrates only *static, declarative* pool partitioning. Even with named
pools, **lane/pool choice stays internal** (round-robin by default) and is
**never client-selected per request** — declaring pools is configuration, not
Ray-style placement control.

### P5 — Move work to data via AGAS when remote
When the runtime is distributed, prefer invoking an operation on the locality
that already holds the relevant component/state (resolved through AGAS) over
shipping state to a caller. Moving work to data minimizes serialization and
matches HPX's addressing model. There is no central scheduler fetching data to a
worker.

### P6 — Serialize only a closed typed arg/result set
Cross-locality movement serializes operation **arguments and results** drawn from
a small, explicit, closed type set (e.g. `int` / `float` / `bytes` first), via
`hpx::serialization`. Everything outside the set is rejected at the boundary.
This is *not* a general object store and *not* serialization of arbitrary Python
objects; it is narrow argument/result marshalling.

### P7 — Cancellation is cooperative tokens / checkpoints; no forced kill
The HPX-native cancellation model for native operations is a `CancelToken`
carried into the operation, polled at safe checkpoint/chunk boundaries — the same
mechanism the harness already uses. A cancelled or exceptional future propagates
to any continuation. Operation authors must accept the token (or a
`should_stop()` predicate) and honor it cooperatively. **Forced kill of non-cooperative native
code is impossible** (HPX cannot safely abort a thread that never reaches a
suspension/interruption point); this is a stated limitation, not a gap to fill.

### P8 — Errors propagate through HPX futures and map to typed RayX runtime errors
A native operation that throws surfaces its exception through its `hpx::future`
(`get()` rethrows); the runtime maps that to a *failed* result plus a row with
`status="failed"`. For the distributed phase, locality loss surfaces as an HPX
error on the future. The runtime relies on future exception propagation rather
than inventing a parallel error channel. Failure handling is **part of the
remote phase, not a later add-on** (a distributed step without a death story is
unsafe).

### P9 — Measurement rows stay separate from values; the v1 schema stays frozen
The runtime preserves RayX's row-based measurement strength: a runtime operation
yields a user **value** and the existing measurement **row** as *separate*
fields, never merged into one dict. Runtime-only observability — executor/pool
id, locality, operation id, composition/movement timing — lives in a **separate
runtime record**, never edited into the frozen v1 benchmark JSONL / analyzer
schema. HPX performance counters may inform the runtime layer but must not couple
to the v1 schema.

### P10 — Operation granularity: size to amortize crossing + scheduling overhead
Registered native operations should be **coarse enough that runtime/scheduling
overhead is not the dominant cost**. Each operation pays a Python→C++ crossing and
HPX scheduling overhead, so a micro-operation measures the boundary, not the work.
Micro-operations like `square(int)` are **correctness/design examples only** —
valid for correctness tests and design/paper illustration, but **never**
performance evidence. **Performance interpretation requires either coarser
operations or explicit overhead accounting** that separates crossing/scheduling
cost from operation cost. (This restates at the operation level the harness's
existing micro-timing caveat; cf. P9 and the adapter-hop cost evidence in
`../../experiments/23_rayx_hpxlane_adapter_hop_cost/`.)

## Per-phase HPX mechanism table

Phases describe *what each step would need to design*, not commitments. The
primary path is the serving-control runtime; Phase 3 is an explicit fork.

| Phase | What | Primary HPX mechanism | Explicitly NOT |
|---|---|---|---|
| **1** | Local registered native operation on the serving-control lane, returning value + existing row | Function registry + `hpx::async` on an executor; result via `hpx::future` | HPX actions; HPX components; ObjectRef; arbitrary Python |
| **2** | Local stateful actor (registered FIFO methods) | Lane-owned native state object, FIFO-served by the existing lane | HPX component / GID (not yet); arbitrary Python methods |
| **3** | *Optional* dependency-composition fork (not default public API) | `hpx::future::then` / `when_all` / `dataflow`; `shared_future` for fan-out | ObjectRef by default; an object store; drifting story-3 → story-2 without an explicit decision |
| **4** | Closed-type serialization; a real store only if evidence-gated | `hpx::serialization` over the closed type set | A general object store of arbitrary Python objects; zero-copy/plasma |
| **5** | Distributed MVP **with** failure model in the same phase | HPX actions; components + clients; AGAS / `hpx::id_type`; localities; `hpx::future` exception propagation; move-work-to-data | A custom scheduler; a Ray-style results locator (AGAS is the directory); client-selected placement; retries / lineage; exposing HPX types to Python |

## Guardrails (carried from the problem model)

* No source/API/runtime implementation; this is design/modeling only.
* No Ray-replacement claim; no "HPX beats Ray" / performance-superiority claim.
  HPX mechanisms are chosen for **fit**, not asserted as superior.
* Arbitrary remote Python execution is **not a target** at any phase.
* No ObjectRef / object store in Phase 1.
* The current RayX harness and the frozen v1 measurement schema are preserved.

## See also

* [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) — the runtime
  problem model these principles constrain (goal, locked decisions, problem map,
  phase order, out-of-scope list).
* [../ray_hpx_mapping.md](../ray_hpx_mapping.md) — Ray↔HPX conceptual mapping and
  the three stories to keep separate (serving-control is story 3).
* [../reference/rayx_frontend_design.md](../reference/rayx_frontend_design.md) —
  current harness design: Future ownership, the `lane_impl` seam, the
  `hpx::wait_some` choice.
* [../reference/hpxlane_backend_arc.md](../reference/hpxlane_backend_arc.md) — the
  `HpxLane` backend evidence arc (exp16 → exp23).
