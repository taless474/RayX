# RayX as a Ray-hosted HPX-native execution island (discussion-prep note)

> **Status: exploratory `docs/design/` discussion-prep note** for a technical conversation
> with HPX maintainers / upstream reviewers. Not promoted reference. Not an implementation
> decision. Not an API spec. Not a benchmark. Not fabric/endpoint work. No performance claim.
> **The Ray-hosting seam is NOT yet characterized**, and nothing here claims it is solved or
> validated. The evidence cited is in-process, single-node, structural, counts-only.

## Thesis

RayX is best framed as an **HPX-native execution island intended to be hosted by a Ray
actor**: Ray stays outside as the orchestration / placement / actor-lifecycle / process
system, and HPX lives inside the actor providing fine-grained native async composition
(futures, `shared_future`, continuations, `hpx::dataflow`, cooperative scheduling) below one
coarse Python/Ray boundary. The exp45–48 arc **characterizes the island interior** — what a
coarse HPX-native op does below the Runtime boundary and how its dependency edges reside. It
does **not** characterize the actual **Ray-hosting lifecycle seam** (an HPX runtime
co-resident inside a Ray worker process). That seam is the next open gap, and it is the
reason for this discussion. RayX is positioned as a separation of concerns — Ray outside, HPX
inside — **not** as a Ray replacement.

## 1. What is demonstrated (the island interior)

All of the following is **in-process, single-node, structural, counts-only** evidence. No
timing, no performance comparison, no winner claim.

* **exp45 / exp46 — boundary-placement / count arc.** For a fixed fan-in and a fixed diamond
  DAG, a single coarse native Runtime op crosses the Python/Runtime boundary O(1) times,
  while an equivalent Python-mediated decomposition crosses it O(N) times. The crossing count
  tracks **where the op boundary is drawn**, not the runtime.
* **exp47 — lane-boundary faithfulness / liveness.** Nested HPX async work launched inside
  one Runtime op schedules, joins, and returns the correct closed value through the lane
  boundary, and the HPX interior remains **live** below that boundary (in-flight overlap was
  observed and reported separately, never as a performance claim).
* **exp48 — edge-residence mechanism inventory vs real Ray.** For the same fixed diamond at
  matched granularity, a Ray ObjectRef and an HPX `future` / `shared_future` are **both
  in-substrate dependency handles**, with different semantics and scopes (this is a
  comparison anchor describing Ray's legitimate mechanism, not a critique).
* **RayX already uses HPX `future` / `shared_future` internally** inside `diamond_fanin`: the
  A→{B,C} fork is carried by a `shared_future` + `.then` continuations; the B,C→D join by
  plain futures moved into `hpx::dataflow`.
* **RayX exposes only closed values across Runtime op boundaries.** A dependency edge between
  two separately submitted ops is carried by a Python-materialized closed value, by design.
* **Conservative in-substrate-reference conclusion.** The companion design note
  (`rayx_runtime_in_substrate_reference_note.md`) leans to keeping HPX dependency references
  **internal to coarse ops** for now, and to not exposing a Python-visible cross-op reference
  unless concrete design pressure appears.

Net: the island *interior* — HPX-faithful composition behind one coarse Python/Runtime
boundary — is characterized. The hosting *seam* is not.

## 2. What is not demonstrated yet (the hosting seam)

Stated as open questions, not failures. None of these has been produced by the exp45–48 arc,
which deliberately kept HPX and Ray in **separate processes** (never co-init'd).

* HPX runtime **co-resident inside a Ray actor process**, including which **embedding API**
  fits: hosting inside a foreign process likely wants **`hpx::start` / `hpx::stop`** (or the
  runtime **suspend / resume** path), not only `hpx::init` (which assumes HPX owns `main`).
* HPX **init / teardown** inside the Ray worker lifecycle.
* **Process-scoped runtime, not per-object.** HPX runtime state is **process-global /
  locality-scoped** — there is one HPX runtime per process, not one per object/actor. So the
  open question is the *mapping*: should the hosting model pin one Ray actor to a **dedicated
  Ray worker process** that owns the single HPX runtime for its lifetime, vs another pattern?
* **Single-locality / no-networking startup.** For a single-process actor-hosting demo, HPX
  should — if possible — start in a **single-locality, networking-disabled** configuration
  (distributed startup services / parcelport off), so it neither binds ports nor expects a
  distributed bootstrap. This reinforces the no-fabric / no-transport posture rather than
  weakening it.
* **Thread / resource partitioning** between the Ray worker and the HPX runtime: how HPX
  (`--hpx:threads`, affinity / binding, resource partitioner / executor choices) should size
  against the Ray actor's CPU accounting (`num_cpus`) **without oversubscription** or affinity
  conflicts.
* **GIL / Ray boundary** behavior at the Python edge: the boundary likely needs explicit GIL
  handling — **release the GIL** around blocking boundary calls (e.g. waiting on the coarse
  Runtime result) so HPX worker threads can progress, and **re-acquire** it if any HPX task
  ever calls back into Python. RayX currently performs **no arbitrary Python execution**, and
  keeping that constraint avoids most GIL-callback complexity.
* **Fork-safety and signal handling.** HPX runtime state should not be initialized **before a
  worker-process fork** (HPX state does not survive `fork()`); the open question is whether
  HPX must be initialized **only after the Ray worker process exists**. HPX **signal-handler**
  configuration (e.g. not installing HPX's handlers where Ray owns them) is a caveat /
  mitigation to confirm, not a solved detail.
* **Senders / receivers vs `future::then` / `hpx::dataflow`** as the forward-looking HPX
  composition mechanism inside the island (see §3 for the question).
* No **fabric / endpoint / parcelport / AGAS / multi-node / transport** evidence (the future
  distributed-fabric direction remains gated).
* No **performance / speedup** behavior (out of scope by design).

## 3. Questions for HPX maintainers / upstream reviewers

1. What is the recommended **HPX runtime lifecycle pattern inside a foreign host process**
   like a Ray worker — specifically, would you embed via **`hpx::start` / `hpx::stop`** (or
   **suspend / resume** to park the runtime between actor calls) rather than `hpx::init`?
2. Given HPX's **one-runtime-per-process / locality** model (runtime state is process-global,
   not per object), should the hosting model **pin one Ray actor to a dedicated Ray worker
   process** that owns the single HPX runtime for its lifetime, or is another mapping
   preferable?
3. For a single-process actor-hosting demo, should HPX be started in a **single-locality,
   networking-disabled** configuration (distributed startup services / **parcelport off**) to
   keep it strictly in-process?
4. How should **HPX threads / resources be sized** relative to the Ray actor's CPU accounting
   (`num_cpus`)? Specifically `--hpx:threads`, **affinity / binding**, **oversubscription**
   risk, and **resource partitioner / executor** choices when HPX shares a process with a Ray
   worker.
5. Are there HPX caveats around **init / teardown, fork-safety** (HPX state not surviving a
   pre-init worker fork), **signal-handler** configuration, or **process-global runtime
   state** inside a hosted actor — and is "initialize HPX only after the worker process
   exists" the right rule?
6. At the **GIL / Ray boundary**, is the expected pattern to **release the GIL** around
   blocking boundary waits (e.g. on the coarse Runtime result) and **re-acquire** it only if
   an HPX task calls back into Python? (RayX keeps **no arbitrary Python execution**, which we
   intend to preserve.)
7. Should in-actor composition use **`future::then` / `hpx::dataflow`** for now (what the
   current experiments use), or should the forward-looking design prefer **HPX senders /
   receivers** (`hpx::execution::experimental` / P2300-style composition)?
8. Where would you draw the **seam** between a Ray task / ObjectRef graph and a coarse
   HPX-native subgraph inside one actor?

## 4. Possible next structural demo (question only)

Posed as a question, not an implementation decision:

> Can a single Ray actor host a RayX/HPX Runtime — start it once, run fixed registered native
> coarse ops that compose HPX-natively below one Python/Ray boundary, return closed
> `int64` / `double` values, and tear down cleanly — with the HPX runtime lifecycle and the
> Ray actor lifecycle coexisting correctly in one process?

* **Load-bearing = lifecycle / coexistence faithfulness** (single HPX init per actor, ops run
  and return correct closed values, clean shutdown, no double-init or leak).
* **Not** speed. **Not** performance. **Not** distribution. **Not** fabric.

## 5. Non-claims

No speedup / throughput / latency / performance; no Ray-comparison-as-performance; no HPX
faster than Ray; no RayX replaces Ray; no RayX makes Ray faster; no "Ray is bad"; no
ObjectRef / object-store criticism; no claim RayX should add an object store; no arbitrary
Python execution; no real inference; no endpoint / fabric / parcelport / AGAS / multi-node
claim; no transport conclusion; no scheduler-control / placement-control /
arbitrary-parallelism claim; no "Python orchestration is bad"; no claim the hosting is solved
or validated; no future distributed-fabric direction work proposed or advanced. (The project
uses "future distributed-fabric direction" throughout; no other track label.)

## 6. Risks and mitigations

* **Hosting outruns evidence** → this note explicitly separates the *demonstrated island
  interior* (exp45–48) from the *uncharacterized hosting seam*; the seam is named as the open
  gap, not as a result.
* **HPX / Ray runtime-coexistence reality** (HPX wanting to own threads / a process-global
  runtime inside a Ray worker) → surfaced directly as the §3 questions on lifecycle, thread
  pool sizing, GIL, and init / teardown.
* **Performance creep from "native C++ island"** → all language kept structural / counts-only;
  the §5 non-claims forbid any speed reading.
* **Fabric pull-forward** → the future distributed-fabric direction is stated as **gated**;
  no transport / endpoint / parcelport / AGAS / multi-node work is proposed here.
* **Dated mechanism optics** → senders / receivers is raised as a forward-looking question
  (§3.7), without committing away from `future::then` / `hpx::dataflow`.
* **Seam-as-solved** → the coarse-op-vs-Ray-task boundary is framed as **provisional**, a
  judgment to be informed by §3.8, not a characterized rule.
* **ObjectRef framing drift** → Ray ObjectRef is used strictly as a **comparison anchor**
  describing Ray's legitimate mechanism, never as criticism.

## 7. Cross-references

* `rayx_runtime_in_substrate_reference_note.md` — the conservative in-substrate-reference
  conclusion this note cites.
* `rayx_runtime_value_model.md` — the closed `int64` / `double` value channel.
* `rayx_runtime_internal_composition_note.md` — how edges are kept in-substrate *inside* a
  coarse op.
* `rayx_runtime_hpx_design_principles.md` — HPX-native design discipline this note follows.
* exp46 write-up: `experiments/46_diamond_join_dag/diamond_join_dag.md` (fixed diamond DAG).
* exp47 write-up: `experiments/47_native_overlap_observation/native_overlap_observation.md`
  (lane-boundary faithfulness / liveness).
* exp48 write-up:
  `experiments/48_ray_boundary_mechanism_inventory/ray_boundary_mechanism_inventory.md`
  (edge-residence mechanism inventory vs real Ray).
