# RayX runtime: in-substrate dependency reference across op boundaries (exploratory note)

> **Status: exploratory `docs/design/` note.** Not promoted reference. Not an
> implementation decision, not an API spec, not a benchmark, not fabric/endpoint work.
> **"Do not build / not as an in-process API" is an explicitly allowed outcome** of this
> note. It pre-registers a *question* and its constraints; it commits to nothing.

## Question

What would an **"in-substrate dependency reference across RayX Runtime op boundaries"**
mean — a handle that lets one Runtime op's result feed another op *without* round-tripping a
closed value through Python — and does such a reference belong in RayX at all?

By "in-substrate" we mean a dependency edge that stays inside the runtime (resolved below
the Python boundary) rather than being materialized back into a Python value and re-supplied
as an argument to the next op.

## 1. Motivation from evidence

This note is motivated by the exp45–48 in-process characterization arc — specifically the
edge-residence finding of exp48 — and by nothing downstream of it.

* **exp45 / exp46 (boundary-placement / count arc):** for a fixed fan-in and a fixed
  diamond DAG, a single coarse Runtime op crosses the Python/Runtime boundary O(1) times,
  while an equivalent Python-mediated decomposition crosses it O(N) times. The crossing
  count is a function of *where the op boundary is drawn*, not of the runtime.
* **exp47 (lane-boundary faithfulness / liveness):** nested HPX async work launched inside
  one Runtime op schedules, joins, and returns correctly through the lane boundary, and the
  HPX interior stays *live* below that boundary (in-flight overlap was observed and reported
  separately, never as a performance claim).
* **exp48 (edge-residence mechanism inventory, vs real Ray):** for the same fixed diamond at
  matched granularity, a Ray ObjectRef and an HPX `future` / `shared_future` are **both
  in-substrate dependency handles**, with different semantics and scopes. RayX **already
  uses HPX in-substrate references inside `diamond_fanin`** (the A→{B,C} fork is carried by a
  `shared_future` + `.then`; the B,C→D join by futures moved into `hpx::dataflow`). But
  RayX's fixed Python Runtime op boundary **exposes no such reference across op
  boundaries**, so a fine-grained decomposition expressed as separate Runtime ops
  necessarily **round-trips closed values through Python**.

The recurring fact across the arc is therefore conceptual, not empirical: the in-substrate
dependency reference is something RayX uses *internally* but does not *expose*. This note
examines what exposing it would mean. **It does not advance the future distributed-fabric
direction** (see §8): it produces no transport, endpoint, parcelport, AGAS, or multi-node
evidence and proposes no such work.

## 2. Current contract recap

The thing any answer must respect — the existing `rayx.runtime` contract (see
`rayx_runtime_value_model.md` and `rayx_runtime_internal_composition_note.md`):

* **Fixed value-in/value-out Runtime ops.** A registered native op takes closed arguments
  and returns a closed value; dispatch is via a `RuntimeFuture` whose `.result()`
  materializes that value on the Python thread.
* **Closed value model: `int64` / `double` only.** No `bytes`, no heap payload, no Python
  object channel.
* **No arbitrary Python execution** — only fixed registered native operations and fixed
  registered native actor methods.
* **No object store**, no `ObjectRef`, no `rayx.get` / `rayx.wait` module surface.
* **No cross-op in-substrate reference.** Each op's result returns to Python as a closed
  value; an edge between two *separately submitted* ops is carried by a Python-materialized
  value, by construction.
* **Fine decomposition round-trips through Python by design** — this is a property of the
  narrow contract, not a defect, and not a statement about Python orchestration.

Inside a single coarse op the runtime is free to compose HPX-natively (futures /
`shared_future` / `.then` / `hpx::dataflow`); that interior composition is exactly the
in-substrate reference, already present, just unexposed.

## 3. Concept comparison

| Concept | Backing / representation | Scope | Serializable | Consumers | Lifetime / ownership | Materialize to Python | Closed type model | Cancellation interaction | Lane / scheduling | Crosses actor/lane boundary |
|---|---|---|---|---|---|---|---|---|---|---|
| **Ray ObjectRef** | object-store entry or inlined value, by ref | location-transparent (cluster) | yes | many | ref-counted, distributed GC | via `ray.get` | arbitrary serializable objects | producer/task lifecycle | Ray scheduler / workers | yes (across workers/nodes) |
| **HPX `future<T>`** | shared state to a single result | in-process | no | one (move-only) | moved; consumed once | n/a (C++) | any C++ `T` | cooperative; cancellation is separate | HPX threads / executors | no (in-process handle) |
| **HPX `shared_future<T>`** | shared state, multi-hold | in-process | no | many (fan-out) | shared, ref-counted in-process | n/a (C++) | any C++ `T` | cooperative; separate | HPX threads / executors | no |
| **RayX `RuntimeFuture`** | Python handle to one pending op | Python ↔ one runtime | no | one (Python-side) | tied to the submitted op | **yes** — that *is* its job | closed int64/double | queued + chunk-boundary running-cancel | one `RuntimeLane` | no (Python-side handle) |
| **Current fixed op contract** | none (no cross-op reference) | n/a | n/a | n/a | n/a | always (value returns to Python) | closed int64/double | per-op cancel only | per-op lane dispatch | n/a |
| **Hypothetical RayX in-substrate ref** | *open* — would have to be a runtime-side handle to a pending/closed op result | *open* — in-process-only at most | **must be: no** | *open* (single vs fan-out) | *open* | *open* (probably optional) | **must stay** int64/double | **must compose** with existing cancel | **must respect** lane FIFO/admission | **open — default no** |

Read across the "hypothetical" row: most cells are *open design choices*, but two are
**fixed by the existing contract** — it must not be serializable, and it must keep the
closed int64/double type model. Ray ObjectRef appears here strictly as a **comparison
anchor** describing Ray's legitimate distributed mechanism, not as a target or a criticism.

## 4. Semantics such a reference would need (and its tension with current guarantees)

For each dimension: what an in-substrate reference would require, and the tension it creates
with a guarantee RayX currently keeps.

* **Identity.** A way to name "the result of op X" before it is materialized.
  *Tension:* the current model has no runtime-side name for a result — results exist only as
  the closed value a `RuntimeFuture` hands to Python. Introducing an identity is the first
  new concept and the first new lifetime to manage.
* **Lifetime.** When the referenced result becomes available and when its backing is
  released. *Tension:* today an op's result lifetime ends at `.result()`; a reference would
  extend a result's lifetime *inside the runtime* until all dependents consume it — new
  bookkeeping the lane does not have.
* **Ownership.** Who owns the held result and is responsible for releasing it.
  *Tension:* RayX has no in-runtime owner for a pending inter-op value; adding one risks a
  GC-shaped subsystem the closed value model deliberately avoids.
* **Single-consumer vs fan-out.** Whether a reference can feed one dependent or many (the
  `future` vs `shared_future` distinction). *Tension:* fan-out is exactly why `diamond_fanin`
  needs `shared_future` *internally*; exposing fan-out across op boundaries multiplies the
  lifetime/ownership questions above.
* **In-process-only vs location-transparent.** Whether the reference is meaningful only
  within one process or could name a result elsewhere. *Tension:* location transparency is
  ObjectRef/AGAS territory and squarely in the **gated** future distributed-fabric
  direction; an honest in-process answer must cap scope at in-process-only.
* **Closed type model.** A reference still names a closed `int64`/`double`.
  *Tension:* none if held to the closed model; **hard violation** if a reference becomes a
  way to pass arbitrary/opaque payloads (that would be an object store by another name).
* **Error propagation.** What a dependent op sees when its producer op failed.
  *Tension:* today failure surfaces as a `status="failed"` row at `.result()`; a reference
  would need a defined "poisoned reference" semantics so a failed producer deterministically
  fails or cancels dependents — new error-flow the per-op model does not specify.
* **Cancellation.** What cancelling a producer means for a held reference, and vice versa.
  *Tension:* RayX has queued + chunk-boundary running cancellation per op; a cross-op
  reference would have to compose with that (cancel-producer ⇒ cancel/triple-state
  dependents) without weakening the existing guarantees.
* **Lane / scheduling interaction.** Which lane runs a dependent that consumes a reference,
  and whether a reference pins or reorders lane work. *Tension:* lanes are FIFO,
  `actor_id`-stable, with bounded admission; a reference that let one lane wait on another's
  pending result could create cross-lane coupling or deadlock surfaces the FIFO model
  currently rules out.
* **Admission / backpressure.** Whether an unmaterialized reference counts against
  `max_queue_depth` / `max_inflight_per_lane`. *Tension:* bounded admission assumes work is
  counted at submit; held references represent latent work whose accounting is undefined —
  ignoring them could silently bypass backpressure.
* **Actor / lane boundary crossing.** Whether a reference may be consumed by an op on a
  different lane or a different native actor. *Tension:* this is where in-process coupling
  becomes most dangerous (cross-lane wait graphs); the conservative default is **no**.
* **Materialization to Python.** Whether a reference can still be turned into a Python value
  on demand. *Tension:* if it can, the simplest honest design is "a reference is an
  optimization that *collapses* to the existing value path when materialized" — which keeps
  the closed model but raises the question of whether the reference earns its added concept
  count at all.

## 5. What it must not become

Fixed constraints, not criticisms:

* **Not an ObjectRef clone** — no serialization, no location transparency, no distributed
  ref-counting.
* **Not an object store** — no opaque/heap payloads, no global value namespace, no spill.
* **Not arbitrary Python execution** — references feed only fixed registered native ops.
* **Not a general distributed dataflow graph engine** — no user-defined DAG submission.
* **Not a locality / AGAS / parcelport handle** — that is the gated future distributed-fabric
  direction.
* **Not performance work** — no speedup/throughput/latency goal or claim attaches here.
* **Not fabric work. Not endpoint work.**
* **Not a claim that Python orchestration is bad** — the round-trip is a property of a narrow
  contract, not a flaw to fix.
* **Not criticism of Ray ObjectRefs** — ObjectRef is Ray's legitimate distributed mechanism
  and serves only as a comparison anchor here.

## 6. Verdict-space

All five are legitimate outcomes; the note keeps them honest and states a leaning.

1. **Do not build; keep RayX fixed value-in/value-out.** The round-trip is an accepted
   property of the narrow contract; the added identity/lifetime/ownership/cancellation/
   admission concepts (§4) are real cost for unclear in-process benefit.
2. **Internal-only; keep HPX in-substrate references only inside coarse ops.** Status quo:
   `diamond_fanin`-style composition already keeps edges in-substrate where it matters,
   exposed to Python as a single closed value. Expose nothing across op boundaries.
3. **Possible in-process-only reference later, for controlled fixed ops.** A future *design*
   candidate — in-process-only, closed-value, never serialized, single-consumer first — to
   be considered only if concrete API pressure appears. A candidate to study, not a decision.
4. **Defer any exposed reference** until the future distributed-fabric direction is
   explicitly opened (a reference may be more naturally introduced there, if at all).
5. **Reject ObjectRef/object-store-like semantics** as out of scope for RayX, independent of
   1–4.

**Leaning: outcomes 1 + 2 (the low-risk status quo), with 3 explicitly kept open as a
study-later candidate and 5 affirmed.** Rationale: RayX already keeps edges in-substrate
where it is valuable (inside coarse ops); exposing a cross-op reference adds a meaningful
concept count (§4) and new failure/coupling surfaces against the FIFO/admission/cancellation
guarantees, for a benefit that — in-process, single node — is not currently demonstrated to
exist. The honest move is to keep the status quo, retain the *vocabulary* (§7/§8), and
revisit only on the triggers in §10. This is a leaning, not a closure: 3 and 4 remain
available without contradiction.

## 7. Open design choices left intentionally unresolved

If outcome 3 or 4 is ever pursued, these stay open (and are *not* decided here): single vs
fan-out first; whether references ever cross lane/actor boundaries (default no); whether
references are always materializable to Python (leaning yes, as a collapse to the existing
value path); how held references are accounted against admission; the exact poisoned-
reference error semantics. Recording them as open is the point — this note does not pick.

## 8. Relationship to the future distributed-fabric direction

* **Gated, not advanced.** This note neither opens nor designs that direction.
* **No transport evidence. No endpoint evidence. No parcelport/AGAS/multi-node evidence.**
  None has been produced anywhere in the arc, and none is produced here.
* The note may define **vocabulary** ("in-substrate dependency reference", "in-process-only
  vs location-transparent", "poisoned reference") that could be reused if that direction is
  later opened — but it **must not, and does not, propose fabric implementation**. A
  location-transparent reference is explicitly out of scope for an in-process answer.

## 9. Non-claims

No speedup / throughput / latency / performance claim; no HPX faster than Ray; no RayX
replaces Ray; no RayX makes Ray faster; no "Ray is bad"; no ObjectRef/object-store
criticism; no claim RayX should add an object store; no arbitrary Python execution; no real
inference; no endpoint/fabric/parcelport/AGAS/multi-node claim; no transport conclusion; no
scheduler-control / placement-control / arbitrary-parallelism claim; no "future
distributed-fabric direction" work proposed or advanced; no wall-clock/performance reasoning.
(The project uses "future distributed-fabric direction" throughout; no other track label.)

## 10. Open questions / what would change the verdict

The leaning (status quo) should be revisited only if one or more of these appears:

* **Concrete API pressure:** a real, fixed, in-process workload where the Python round-trip
  of an intermediate closed value is a *correctness or expressiveness* limitation (not a
  performance one) that a coarse op cannot already absorb.
* **A composition that a single coarse op genuinely cannot express** while the closed value
  model and fixed-op registry are preserved — i.e., the internal-only outcome stops being
  sufficient.
* **The future distributed-fabric direction is explicitly opened** with its own evidence
  (and the boundary-vs-transport question is actually addressed), at which point a reference
  may belong there rather than in-process.
* **A worked design** for the §4 tensions (lifetime/ownership/admission/cancellation/error)
  that adds no GC-shaped subsystem and no object-store-shaped surface — i.e., a reference
  that demonstrably costs little against the existing guarantees.

Absent these, the status quo (outcomes 1 + 2) stands.

## 11. Cross-references

* `rayx_runtime_value_model.md` — the closed int64/double value channel any reference must
  respect.
* `rayx_runtime_internal_composition_note.md` — how edges are kept in-substrate *inside* a
  coarse op (the existing, unexposed in-substrate reference).
* `rayx_runtime_hpx_design_principles.md` — HPX-native design discipline this note follows.
* exp46 write-up: `experiments/46_diamond_join_dag/diamond_join_dag.md` (fixed diamond DAG).
* exp47 write-up: `experiments/47_native_overlap_observation/native_overlap_observation.md`
  (lane-boundary faithfulness / liveness).
* exp48 write-up:
  `experiments/48_ray_boundary_mechanism_inventory/ray_boundary_mechanism_inventory.md`
  (edge-residence mechanism inventory that motivates this note).
