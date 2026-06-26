# Direct HPX-to-HPX across Ray actors (Level-4 design-gate note)

> **Status: exploratory `docs/design/` design-gate note** for the **future
> distributed-fabric direction**. Not promoted reference. Not an implementation decision.
> Not an API spec. Not a benchmark. **Level 4 — direct HPX-to-HPX communication across Ray
> actors — is GATED and NOT demonstrated.** This note produces **no endpoint / fabric /
> parcelport / AGAS / locality / transport result**; those terms appear only as the gated
> mechanism under design discussion. The evidence cited (exp27–48) is **in-process,
> single-node, structural, counts-only**.

## Thesis

Basic Ray hosting of RayX/HPX is **already characterized** (exp27 / exp28 / exp30), and the
HPX/RayX **island interior** is characterized (exp45–48). So the open question is **not**
"can Ray host RayX?" — it is whether **HPX inside one Ray-hosted actor can communicate
directly with HPX inside another Ray-hosted actor**. That is Level 4 of the hosting ladder,
the intended **future distributed-fabric direction**, and it is **gated, not demonstrated**.
Throughout, **Ray remains the outer control / bootstrap / lifecycle plane**; whether HPX can
be an inner **data plane across actors** is exactly what is being gated here. This note is
deliberately **value-proposition-before-mechanism**: it asks what direct HPX-to-HPX would
*buy* over Ray-mediated coordination before it reasons about any mechanism.

## 1. Where we are

In-process, single-node, structural, counts-only — **no L4 evidence exists**:

* **L1 basic hosting — characterized.** A real Python `@ray.remote` actor hosts a RayX/HPX
  runtime in its own worker process: exp27 (`rayx.Engine`), exp28 (`rayx.runtime.Runtime` +
  `square`), exp30 (`Runtime` + native `CounterActor`). Established: **one HPX runtime per
  process** (a second is rejected by the process guard), **repeated-call reuse**, **clean
  shutdown / teardown**, and **multi-actor isolation** (each actor its own process, its own
  runtime, distinct inner lane ids).
* **L2 hosted coarse composition — small connector (not yet done).** exp27–30 ran *trivial*
  ops; hosting a *composed* coarse op (`diamond_fanin`) inside a host actor is the small
  missing connector between basic hosting and the exp45–48 interior.
* **Island interior — characterized (exp45–48):** boundary placement / counts, lane-boundary
  faithfulness / liveness, and edge-residence (HPX `future` / `shared_future` kept in-substrate
  *inside* a coarse op). These say nothing about communication *across* actors.

## 2. The four-level ladder

* **L1 — basic hosting.** Ray actor hosts RayX/HPX. **Done** (exp27/28/30).
* **L2 — hosted coarse composition.** Ray actor runs a composed HPX-native coarse op
  (`diamond_fanin`). **Small connector.**
* **L3 — Ray-mediated actor coordination.** Two actors each host RayX/HPX; they coordinate
  **only through normal Ray actor calls / ObjectRefs**, carrying **closed values**. **Safe
  control / baseline; no HPX object crosses the actor/process boundary.** Ungated.
* **L4 — direct HPX-to-HPX across Ray actors.** HPX inside actor A communicates directly with
  HPX inside actor B. **Intended future distributed-fabric direction; gated, not
  demonstrated.**

## 3. Bright-line test (L3 vs strong/weak L4)

The level is not binary — there is a **strong** and a **weak/gray** form of L4, and the
distinction is load-bearing:

* **L3:** only **closed, Python/Ray-serializable values** cross the actor/process boundary,
  carried by a normal Ray mechanism (actor call / ObjectRef). Every HPX object stays inside
  one process.
* **Strong L4:** **HPX's own distributed machinery crosses the actor/process boundary** —
  e.g. the **parcelport**, an **AGAS-addressed action**, an **HPX locality identity**, or an
  **HPX component reference**. This is the full HPX-distributed-semantics case.
* **Weak / gray L4:** **only HPX serialization touches the payload**, while a **self-managed
  socket or other external channel** carries the bytes between independent runtimes. Here HPX
  is used as a serialization library, not as a distributed runtime — and this **must not be
  oversold as full HPX distributed semantics**.

The deciding question: *what actually carries the bytes across the boundary?* Ray → L3; HPX's
own transport / AGAS-addressed action → **strong L4**; a self-managed/external channel with
only HPX serialization on the payload → **weak/gray L4**. Do **not** use vague "HPX awareness"
language unless it is defined as one of these; knowing about another actor via Ray metadata is
L3, not L4.

**Worked anchor (the existing `rayx.endpoint` seam, §9): it is not even weak L4.** The seam's
AF_UNIX channel carries **HPX-free fixed frames** (PING / `CALL_OP` of closed values), not an
HPX-serialized payload — so nothing HPX crosses the wire. It sits at L3-equivalent over a
self-managed local channel, and the federated L4 probe is precisely the step of making that
wire carry HPX serialization into a fixed HPX handler (→ weak/gray L4).

## 4. Value proposition before mechanism

Before any mechanism is designed, L4 must answer: **what does direct HPX-to-HPX buy over L3
Ray-mediated coordination?** Stated as hypotheses / open questions only — **no speed or
performance value is claimed**, and single-node shows nothing measurable:

* **Native dependency flow across actor boundaries** — could a dependency edge stay
  HPX-native end-to-end instead of being materialized to a closed value at each actor edge?
* **Less reliance on Python/Ray in the *data* path** while Ray remains the **control** plane —
  a division-of-labor hypothesis, not a cost claim.
* **HPX-native composition spanning actors** — extending the in-substrate composition story
  (exp45–48) across processes, as a design question.
* **Ray as control/bootstrap plane, HPX as data plane** — testing whether that split is even
  coherent.

If the only honest current value is **architectural interest**, the note says so. The
mechanism (§7) is not designed until this proposition is argued.

## 5. The architecture fork (center of the note)

Two fundamentally different topologies. **Federated (B) is recommended as the first probe**
unless transparent global addressing is truly required.

### A. Shared HPX distributed runtime
* Ray actors become **HPX localities** of **one shared HPX runtime**.
* Uses **AGAS / parcelport / actions / components**; gives **global addressing** — the full
  **strong-L4** HPX-distributed-semantics case.
* **Risks:** high coupling with Ray's restart/failure model; the **AGAS root / locality-0 is a
  single point of failure** whose loss baseline HPX may not recover from cleanly;
  **bootstrap ordering** — the AGAS root must exist *before* connecting localities, but Ray
  actor startup order is dynamic, so the bootstrap is an *additional* coupling beyond the
  SPOF; depends on the maturity of **`runtime_mode::connect`** (a locality dynamically joining
  a running AGAS root started independently). (All open questions, not results.)

### B. Federated independent HPX runtimes
* Each Ray actor owns its **own independent HPX runtime**. **No shared AGAS / no global
  address space.**
* **Honest mechanism caveat.** Standard HPX inter-locality machinery (**parcelport,
  AGAS-addressed actions, futures-across-localities, distributed channels**) generally assumes
  **one shared distributed runtime with AGAS**. With independent runtimes there may be **no
  off-the-shelf HPX remote-action / remote-object semantics** between the actors. The likely
  **concrete** federated mechanism is therefore:
  * `hpx::serialization` over a **self-managed or Ray-bootstrapped socket / channel**;
  * **fixed registered handlers** on the receiving side;
  * **closed int64 / double** payloads, consistent with RayX's fixed-op discipline.
* **Semantic cost.** Federated buys **lifecycle compatibility** (each runtime lives and dies
  on its own; no global-state coupling; no AGAS single point of failure), but likely **gives
  up transparent HPX global addressing, AGAS-resolved actions, and remote-object semantics** —
  i.e. it tends to land in **weak/gray L4** (§3), not strong L4.

**The value-vs-lifecycle paradox (the decision the gate must resolve).** The two options trade
off in opposite directions:
* **Shared (A)** gives the **stronger HPX-native distributed semantics** but is
  **lifecycle-hostile under Ray** (AGAS-root/locality-0 SPOF, locality loss, dynamic actor
  restart, bootstrap ordering, coupling).
* **Federated (B)** is **Ray-lifecycle-compatible**, but the **HPX delta over L3 may be thin**
  for closed int64/double values if it reduces to HPX serialization over a custom transport —
  i.e. the lifecycle-friendly option may buy little over Ray-mediated coordination, while the
  high-value option is the fault-hostile one.
* So the design gate must ask: **does Level 4's value proposition actually require shared HPX
  global addressing (A), or is a narrower federated message channel (B) still meaningful** for
  the closed-value, fixed-op workload? This is unresolved and is a primary question for the
  HPX experts (§8).

**Decision rule (softened).** *Federated (B) is the likely **first probe** only if the goal is
Ray-lifecycle compatibility **and** the loss of HPX global addressing is acceptable. The
shared runtime (A) remains the **stronger HPX-distributed-semantics** path but has the
**hardest lifecycle / fault-model gate**. Neither is asserted as the answer; the fork is the
question.*

## 6. Decisive risks

Each is an **open question**, not a result:

* **Continuous progress.** HPX cross-actor communication **cannot be suspended between Python
  calls** — parcel / message progress needs **live HPX threads even when the Ray actor is idle
  in Python**. This **rules out suspend / resume for L4** (unlike the in-process hosting case).
* **Ray lifecycle vs HPX distributed-runtime lifecycle.** Independent, dynamic, restartable
  actors vs a runtime that expects a stable set of participants.
* **AGAS-root / locality-0 single point of failure** (shared model) — its loss collides
  directly with Ray's per-actor restart value.
* **Actor restart / failure behavior** — what a peer observes when an actor (runtime /
  locality) dies or restarts.
* **Thread / CPU progress budget** inside Ray workers — who guarantees HPX progress threads
  get CPU.
* **GIL boundary** — whether HPX progress threads run independently of the GIL-held Python
  edge.
* **Transport choice** — **TCP-class likely realistic**; **MPI likely incompatible** with
  dynamically launched Ray actors (no single launcher / fixed world); **LCI / libfabric**
  open.
* **Action / serialization surface** — a **fixed registered HPX action / handler** model over
  **serializable closed int64 / double** values, consistent with RayX's fixed-op discipline
  (no arbitrary payloads).
* **Deserialization / trust surface.** Deserializing and dispatching a message can become an
  **execution surface** (deserializing an action invokes it). The **fixed registered
  actions / handlers are therefore also a containment boundary**: **arbitrary payloads and
  arbitrary Python callbacks remain out of scope**, which reinforces the closed int64 / double
  and fixed-registered-op/action discipline. (Open question: what is the right registration /
  validation model — not a result.)
* **Per-actor runtime weight.** In the federated model, **N Ray actors means N independent HPX
  runtimes**, each potentially carrying scheduler threads, local runtime state, progress
  threads, and a resource budget. This is a **lifecycle / resource cost to characterize, not a
  performance claim**.

## 7. Smallest feasibility probes

Described here as design targets — **not built, gated, single-node, closed-value, no
performance / multi-node claim.**

### Federated (likely first probe — see §5 caveats)
* Single node; **two Ray actors**, each running an **independent HPX runtime**.
* **Ray bootstraps endpoint discovery** between them.
* Actor A's HPX work produces a **closed int64**.
* An **HPX-serialized message** over a self-managed / Ray-bootstrapped channel carries it to
  actor B's HPX runtime.
* B runs a **fixed registered handler / action / op** and returns a **closed int64**.
* **No shared AGAS. No multi-node claim. No performance claim.**

Isolates the **weak/gray-L4** form (§3): HPX serialization on the payload over a self-managed
channel, with the **least** runtime coupling. It does **not** by itself establish strong-L4
HPX-distributed semantics, and its delta over L3 for a closed int64 should be weighed against
the value-vs-lifecycle paradox (§5).

### Shared-runtime alternative (heavier, higher risk)
* Single node; two Ray actors **join one shared HPX runtime as localities**.
* Ray exchanges **endpoints / AGAS-root info**.
* **One HPX action A→B** returns a **closed int64**.
* **Marked heavier and higher risk** because of AGAS / root / lifecycle coupling and the
  single point of failure.

## 8. Questions for HPX maintainers / upstream reviewers

1. For **independently launched, independently restartable** Ray actors, do you recommend a
   **shared HPX runtime / localities** or **federated independent runtimes exchanging
   messages**? Where is the crossover?
2. Is a **federated HPX-native message channel** a sane first step, or does HPX's model push
   toward **shared localities / AGAS**?
3. **Is there any supported HPX path for two independently-started HPX runtimes to exchange
   messages *without* a shared AGAS** — or would a federated channel really be
   **`hpx::serialization` over a custom / Ray-bootstrapped transport with fixed handlers**
   (i.e. weak/gray L4, not strong L4)?
4. Given the **value-vs-lifecycle paradox** (§5): does Level 4's value proposition **require**
   the shared-runtime / global-addressing path (A), or is the narrower federated channel (B)
   still meaningful for closed int64 / double, fixed-op payloads?
5. What keeps **parcel / message progress alive** inside a Ray actor while Python is idle?
6. What **thread / CPU budget** does **continuous progress** need?
7. In the shared model, **what happens when a locality dies**?
8. Is **AGAS-root / locality-0 loss recoverable** in baseline HPX?
9. Does **HPX resilience** research change this, or is it **not turnkey** for Ray-actor
   restart?
10. Which **transport** is realistic inside Ray workers — **TCP, LCI / libfabric, other**?
    (Treat **MPI as likely out** for dynamically launched actors.)
11. What **fixed action / serialization model** suits RayX's closed **int64 / double**
    discipline?
12. Is **`runtime_mode::connect`** mature enough for **dynamic locality join**?
13. Does **Ray-as-bootstrap / HPX-as-data-plane** make sense as the division of labor?

## 9. Reconciliation with the existing `rayx.endpoint` seam (the federated branch is NOT greenfield)

A README-archaeology pass found that the **federated branch (B) is a continuation of the
existing HPX-free `rayx.endpoint` seam**, not a from-first-principles design. Per the README
documentation map (the `rayx.endpoint` seam + A1 local transport section; this note relies on
that summary and does **not** open or import the endpoint notes/code), the seam **already
prototyped** most of the federated scaffolding:

* **endpoint identity** (minted `rtb-ep-<hex>` ids) and a **process-local registry**;
* **Ray/Python-carried bootstrap metadata** (i.e. "Ray as bootstrap" already exists);
* an **AF_UNIX A1 local transport** (opt-in, process-local listener, fixed PING frame,
  one-shot dial-per-call);
* a **same-process registry dispatch path** and a **cross-process one-shot AF_UNIX path**;
* an **endpoint→Runtime bridge** carrying a **fixed closed `CALL_OP` frame** into a hosted
  `Runtime` (reaching `square` / `add` / composed `fanout_sum`);
* a **shared-HPX-owner lifecycle** (Variant 2): the **`Runtime` owns HPX**, the **`Endpoint`
  is HPX-free**, order-independent teardown.

So the **federated Level-4 probe is the small, grounded delta** of making that
endpoint-style local channel **carry an HPX-serialized payload into a fixed HPX
handler / action-like receiver**, instead of the seam's **HPX-free** fixed PING / `CALL_OP`
frame. It is not new transport, identity, bootstrap, bridge, or lifecycle work — those exist.

**Branch lineage:**
* **Federated (B):** *direct continuation* of the endpoint seam — likely a small delta if it
  reuses the existing endpoint-style identity / bootstrap / AF_UNIX channel and adds HPX
  serialization + a fixed HPX handler.
* **Shared runtime (A):** *genuinely new* relative to the endpoint seam — the seam **explicitly
  excluded** parcelport / AGAS / locality / multi-node / fabric.

**Preserve the seam's standing non-claims (do not contradict them):** the endpoint seam is
**HPX-free**; the **`Runtime` owns HPX**; **endpoint/IPC evidence does not prove fabric** and
does **not** advance the future distributed-fabric direction; **no parcelport, no AGAS, no
multi-node, no HPX socket serving, no HPX async socket I/O, no persistent-transport/channel
claim, no public endpoint-call API** beyond the existing fixed ping; and **exp42 / exp43 are
observation-only, OS-local, non-transferable, and do not isolate transport cost**. This note
**does not import** any endpoint-note claim beyond this README-archaeology summary; deeper
reconciliation with the endpoint notes/code remains a separate reviewed step.

### What remains genuinely new after the prior endpoint work

The scaffolding (identity / bootstrap / AF_UNIX channel / bridge / shared-HPX-owner lifecycle)
is **not** new. What is genuinely new and still open:

* **The HPX-crossing step itself** — weak or strong L4 (§3); the seam is **HPX-free by design**
  and never crossed HPX.
* **Carrying HPX serialization (weak L4) or HPX distributed machinery (strong L4)** across the
  actor/process boundary.
* **The federated-vs-shared fork** and the **value-vs-lifecycle paradox** (§5).
* **The continuous-progress risk** (§6) — a live locality must progress while the actor is idle.
* **The AGAS-root / locality-0 single point of failure** in the shared model (§5, §6).
* **Direct HPX-to-HPX semantics are not demonstrated** — by the seam or anywhere.

## 10. Non-claims

* **L4 is not demonstrated.**
* No **endpoint / fabric / parcelport / AGAS / locality / multi-node / transport** result.
* No speedup / throughput / latency / performance.
* No HPX faster than Ray.
* No RayX replaces Ray.
* No RayX makes Ray faster.
* No ObjectRef / object-store criticism.
* No arbitrary Python execution.
* No real inference.
* No scheduler-control / placement-control / arbitrary-parallelism.
* No claim that direct HPX-to-HPX is better than Ray-mediated coordination.
* No claim that RayX is already a distributed runtime.
* The **future distributed-fabric direction remains gated**.

(Parcelport / AGAS / locality / endpoint / transport appear in this note only as the **gated
mechanism under design discussion**, never as built or measured.)

## 11. Open questions / what would ungate L4

Before any L4 code is attempted, in order:

1. **Value proposition over L3** articulated (§4) — what HPX-to-HPX buys that Ray-mediated
   coordination does not.
2. **Federated-vs-shared decision** made or narrowed (§5).
3. **Continuous-progress model** understood (§6) — how an embedded locality progresses while
   the actor is idle in Python.
4. **Fault / restart model** understood (§6) — what a peer sees when an actor dies/restarts.
5. **Transport / bootstrap model** selected (§6, §8).
6. **HPX maintainer / upstream-reviewer vetting** of the above.
7. Only then, a **tiny single-node feasibility probe** (§7), starting federated.

Until these hold, Level 4 stays gated and no code is written.

## 12. Cross-references

* `docs/design/rayx_ray_hosted_execution_island_discussion.md` — the hosting framing and the
  four-level ladder this note extends.
* `docs/design/rayx_runtime_in_substrate_reference_note.md` — in-substrate references kept
  internal to coarse ops (the in-process analog of the cross-actor question).
* `docs/design/rayx_runtime_hpx_design_principles.md` — HPX-native design discipline.
* L1 basic-hosting write-ups: `experiments/27_ray_hosting_rayx_engine/ray_hosting_rayx_engine.md`,
  `experiments/28_ray_hosting_rayx_runtime/ray_hosting_rayx_runtime.md`,
  `experiments/30_ray_hosting_rayx_runtime_counter/ray_hosting_rayx_runtime_counter.md`.
* Island-interior write-ups:
  `experiments/45_boundary_placement_comparison/boundary_placement_comparison.md`,
  `experiments/46_diamond_join_dag/diamond_join_dag.md`,
  `experiments/47_native_overlap_observation/native_overlap_observation.md`,
  `experiments/48_ray_boundary_mechanism_inventory/ray_boundary_mechanism_inventory.md`.
* Prior `rayx.endpoint` lineage the federated branch continues (§9) — referenced by path only;
  content **not** imported beyond the README-archaeology summary:
  * `docs/design/endpoint_runtime_seam.md` — canonical endpoint seam (identity / A1 AF_UNIX
    transport / shared-HPX-owner / endpoint→Runtime bridge).
  * `experiments/42_endpoint_bridge_boundary_cost/bridge_boundary_cost.md` — exp42, the
    cross-process endpoint→Runtime bridge path (observation-only).
  * `experiments/43_endpoint_transport_ping_floor/endpoint_ping_floor.md` — exp43, the
    Runtime-less HPX-free AF_UNIX endpoint ping floor (observation-only).
* `docs/ray_hpx_mapping.md` — the "Future optional path" roadmap framing for the Ray/HPX
  direction (the original source the hosting line realizes).
