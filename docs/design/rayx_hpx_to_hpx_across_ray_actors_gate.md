# HPX-mediated communication across Ray actors (Level-4 design-gate note)

> **Status: exploratory `docs/design/` design-gate note** for the **future
> distributed-fabric direction**. Not promoted reference, not an implementation decision,
> not an API spec, not a benchmark. **Level 4 — HPX-mediated communication across Ray
> actors, whether by HPX's own transport (strong) or by HPX serialization over a custom
> channel (weak/gray, §3) — is GATED and NOT demonstrated.** This note produces no
> endpoint / fabric / parcelport / AGAS / locality / transport result; those terms appear
> only as the gated mechanism under design discussion. The in-process evidence cited
> (exp27–48) is in-process, single-node, structural, counts-only. The standalone
> connect-mode evidence cited (exp49–52, 57–65) runs HPX in standalone processes, never
> inside Ray actor workers; it informs specific gate questions (§1a) but does not close
> the gate.

## Thesis

Basic Ray hosting of RayX/HPX is already characterized (exp27/28/30), and the RayX/HPX
island interior is characterized (exp45–48). The open question is no longer "can Ray host
RayX?" — it is whether HPX inside one Ray-hosted actor can communicate with HPX inside
another Ray-hosted actor. That is Level 4 of the hosting ladder and the intended future
distributed-fabric direction. Ray stays the outer control / bootstrap / lifecycle plane
throughout; the question is whether HPX can be an inner data plane across actors. The
note is deliberately value-proposition-before-mechanism: first ask what HPX-mediated
communication would buy over Ray-mediated coordination, then reason about mechanism.

## Terminology: admission and connection vocabulary

The connect-mode evidence below is only readable if five things stay distinct:

* **Static multi-locality startup** — every locality is present at startup (a fixed
  `--hpx:localities`-style world); membership is decided before any work runs. exp49
  Phase 1 attempted it and produced an undiagnosed bare-process launch failure; the
  project then standardized on connect mode because it better matches independently
  launched workers. No distributed HPX-island probe after exp49 Phase 1 uses it, and
  no experiment shows static startup to be generally broken.
* **Late connect-mode locality admission** — a locality joins a *running* AGAS root later
  via `runtime_mode::connect`. The distributed HPX-island probes from exp49 Phase 2
  onward all use it — but in an orchestrated **assemble-then-measure** pattern:
  connectors launched at orchestration start, roots polling for the joiner or failing
  closed on an expected count before dispatch.
* **Demand-ordered admission** — the root is operational and making local HPX progress
  *first*; a connector is created only *after* an external demand event and discovered
  without a predetermined connector count. Demonstrated narrowly by exp65, on loopback
  and across two real nodes (§1a).
* **Lazy parcelport TCP connection establishment** — *when* the transport socket to a
  joined locality is actually opened (admission-time vs first-parcel). Not observed by
  any experiment; explicitly open.
* **Elastic membership under in-flight work** — join/leave overlapping in-flight remote
  work, concurrent connector churn. Not demonstrated.

Whether "late and on demand" connect-mode use means locality admission (the second and
third items) or lazy transport establishment (the fourth) is an open maintainer question
(§8) — hence the split vocabulary.

## 1. Where we are

In-process, single-node, structural, counts-only — **no L4 evidence exists**:

* **L1 basic hosting — characterized.** A real Python `@ray.remote` actor hosts a RayX/HPX
  runtime in its own worker process: exp27 (`rayx.Engine`), exp28 (`rayx.runtime.Runtime` +
  `square`), exp30 (`Runtime` + native `CounterActor`). Established: one HPX runtime per
  process (a second is rejected by the process guard), repeated-call reuse, clean
  shutdown / teardown, and multi-actor isolation (each actor its own process, its own
  runtime, distinct inner lane ids).
* **L2 hosted coarse composition — small connector (not yet done).** exp27–30 ran trivial
  ops; hosting a composed coarse op (`diamond_fanin`) inside a host actor is the small
  missing connector between basic hosting and the exp45–48 interior.
* **Island interior — characterized (exp45–48):** boundary placement / counts, lane-boundary
  faithfulness / liveness, and edge-residence (HPX `future` / `shared_future` kept
  in-substrate *inside* a coarse op). These say nothing about communication *across*
  actors.

### 1a. Standalone connect-mode evidence (exp49–65)

A standalone connect-mode arc has since produced evidence that informs specific gate
questions. All of it runs HPX in standalone processes (plain-Python, Slurm, or
Ray-as-launcher orchestration), never inside Ray actor worker processes:

* **Late connect-mode admission, not static startup.** The distributed HPX-island probes
  from exp49 Phase 2 onward all used `runtime_mode::connect` late locality admission
  rather than fixed static multi-locality startup — but always assemble-then-measure
  (see Terminology).
* **Demand-ordered admission demonstrated, narrowly (exp65 — loopback and two-node).**
  The root starts alone; local HPX progress occurs before any connector exists; an
  external demand event launches one connector; the root discovers it by membership
  set-difference without a predetermined connector count; one bounded oracle-verified
  remote action succeeds; the connector leaves gracefully; the root continues local work
  and finalizes cleanly (3/3 both arms; the no-demand control root finalizes cleanly
  with zero connectors). Two slices: a single-node loopback slice (macOS, HPX 1.11,
  plain Python controller) and a Rostam cross-node slice (Slurm job 170014:
  root/controller on medusa00, connector created only after the demand event on
  medusa01, TCP parcelport over `10.42.5.x`, verified remote action on locality 1,
  graceful leave, root continued and finalized; 3/3 both arms, all structural/placement
  gates passed). The safe claim stays narrow: demand-ordered connect-mode admission is
  demonstrated on loopback and across two real nodes, within a boot-time
  `--hpx:expect-connecting-localities` willingness. (The loopback slice's side
  observation that a single full-bound `future::wait_for` on the dispatched action
  returned only at its full bound is scoped to loopback/macOS only; the cross-node
  slice used sliced waits and does not reproduce that wait construction.)
* **Fault / lifecycle boundary (exp50/51 — single-node loopback / macOS / HPX 1.11 only).**
  After ungraceful **non-root** locality loss, stale membership allowed continued service
  to a fresh connector but blocked collective shutdown. No supported public
  stale-locality eviction path was found in the inspected HPX 1.11 build and headers;
  whether another supported or intended mechanism exists is an upstream question (§8).
  Whole-island external restart was the recovery boundary. AGAS-root loss remains
  untested.
* **Connector lifetime needed a user-space protocol (exp63).** A heartbeat /
  root-completion-sentinel protocol was required to prevent connector departure during
  root dispatch. Whether HPX should offer a clearer disconnect/quiesce contract is a
  maintainer discussion item, not a proven required HPX design.
* **Scoped progress/readiness finding (exp64 Phase A→A4 — HPX 1.11 / TCP parcelport /
  Rostam).** Native `when_all` / `dataflow` continuations entered and completed promptly,
  but the suspended timed waiter resumed only at the timeout
  (`waiter_resume_at_timeout`) — unchanged by root/background-thread adjustments,
  disabled idle backoff, and TCP parcel-pool sizes 2 (observed default), 4, and 8, while
  polling/yield controls stayed prompt. Consequence: the polling gather baseline was not
  retired and no native payload-size ladder was started. A scoped per-build observation —
  not a general HPX defect claim and not a performance claim.

**Still explicitly open, none of it closed by the above:** HPX embedded inside separate
Ray actor worker processes (this gate); the shared-vs-federated runtime decision (§5);
elastic admission/removal while work is in flight; concurrent connector churn; lazy
establishment of TCP parcelport connections; reproduction of exp65 beyond two nodes;
failure recovery after ungraceful connector loss.

## 2. The four-level ladder

* **L1 — basic hosting.** Ray actor hosts RayX/HPX. **Done** (exp27/28/30).
* **L2 — hosted coarse composition.** Ray actor runs a composed HPX-native coarse op
  (`diamond_fanin`). **Small connector.**
* **L3 — Ray-mediated actor coordination.** Two actors each host RayX/HPX; they coordinate
  only through normal Ray actor calls / ObjectRefs, carrying closed values. Safe
  control / baseline; no HPX object crosses the actor/process boundary. Ungated.
* **L4 — HPX-mediated communication across Ray actors**, in either the strong or the
  weak/gray form (§3). Intended future distributed-fabric direction; gated.

## 3. Bright-line test (L3 vs strong/weak L4)

L4 is not binary — it has a strong and a weak/gray form, and the distinction is
load-bearing:

* **L3:** only closed, Python/Ray-serializable values cross the actor/process boundary,
  carried by a normal Ray mechanism (actor call / ObjectRef). Every HPX object stays
  inside one process.
* **Strong L4:** HPX's own distributed machinery crosses the actor/process boundary — the
  parcelport, an AGAS-addressed action, an HPX locality identity, or an HPX component
  reference. This is the full HPX-distributed-semantics case.
* **Weak / gray L4:** HPX serialization produces and consumes the bytes, but a
  self-managed socket or other external channel carries them between independent
  runtimes. On the receiving side, a fixed registered receiver deserializes the payload
  and submits the work to its local HPX runtime. HPX is used as a serialization library
  here, not as a distributed runtime: the channel provides no HPX distributed-runtime
  semantics, and HPX never owns the transport. This must not be oversold as full HPX
  distributed semantics.

The deciding question: what actually carries the bytes across the boundary? Ray → L3.
HPX's own transport or an AGAS-addressed action → strong L4. A self-managed or external
channel with HPX serialization only on the payload → weak/gray L4. Avoid vague "HPX
awareness" language unless it resolves to one of these; knowing about another actor via
Ray metadata is L3, not L4.

**Worked anchor — the existing `rayx.endpoint` seam (§9) is not even weak L4.** Its
AF_UNIX channel carries HPX-free fixed frames (PING / `CALL_OP` of closed values), so
nothing HPX-serialized crosses the wire; it sits at L3-equivalent over a self-managed
local channel. The federated L4 probe is precisely the step of making that wire carry an
HPX-serialized payload that a fixed receiver deserializes and hands to its local HPX
runtime (→ weak/gray L4).

## 4. Value proposition before mechanism

Before any mechanism is designed, L4 must answer: what does HPX-mediated communication
buy over L3 Ray-mediated coordination? These are hypotheses and open questions — no speed
or performance value is claimed, and single-node shows nothing measurable:

* **Native dependency flow across actor boundaries** — could a dependency edge stay
  HPX-native end-to-end instead of being materialized to a closed value at each actor
  edge?
* **Less reliance on Python/Ray in the data path** while Ray remains the control plane —
  a division-of-labor hypothesis, not a cost claim.
* **HPX-native composition spanning actors** — extending the in-substrate composition
  story (exp45–48) across processes, as a design question.
* **Ray as control/bootstrap plane, HPX as data plane** — is that split even coherent?

If the only honest current value is architectural interest, say so. The mechanism (§7) is
not designed until this proposition is argued.

## 5. The architecture fork

Two fundamentally different topologies. Federated (B) is the likely first probe if the
goal is Ray-lifecycle compatibility and the loss of HPX global addressing is acceptable;
shared (A) remains the stronger HPX-distributed-semantics path with the hardest
lifecycle / fault-model gate. Neither is asserted as the answer — the fork is the
question.

### A. Shared HPX distributed runtime

* Ray actors become **HPX localities** of one shared HPX runtime.
* Uses AGAS / parcelport / actions / components; gives global addressing — the full
  **strong-L4** HPX-distributed-semantics case.
* **Risks (open questions, not results):** high coupling with Ray's restart/failure
  model. The AGAS root / locality-0 is a single point of failure whose loss baseline HPX
  may not recover from cleanly. Bootstrap ordering adds coupling beyond the SPOF: the
  AGAS root must exist before connecting localities, but Ray actor startup order is
  dynamic. And the model depends on the maturity of `runtime_mode::connect` (a locality
  dynamically joining a running AGAS root started independently) — now substantially
  informed by the standalone exp49/50/51/65 arc (§1a: graceful join/leave, the
  ungraceful-loss / stale-membership boundary, demand-ordered admission on loopback and
  across two nodes), but only outside Ray workers, so not fully closed.

### B. Federated independent HPX runtimes

* Each Ray actor owns its own independent HPX runtime. No shared AGAS, no global address
  space.
* **Honest mechanism caveat.** Standard HPX inter-locality machinery (parcelport,
  AGAS-addressed actions, futures-across-localities, distributed channels) generally
  assumes one shared distributed runtime with AGAS. With independent runtimes there may
  be no off-the-shelf HPX remote-action / remote-object semantics between the actors. The
  likely concrete federated mechanism is therefore:
  * `hpx::serialization` for the payload bytes;
  * a self-managed or Ray-bootstrapped socket / channel carrying those bytes;
  * a fixed registered receiver deserializing them and submitting the work to the local
    HPX runtime;
  * closed int64 / double payloads, consistent with RayX's fixed-op discipline.
* **Semantic cost.** Federated buys lifecycle compatibility — each runtime lives and dies
  on its own, no global-state coupling, no AGAS single point of failure — but likely
  gives up transparent HPX global addressing, AGAS-resolved actions, and remote-object
  semantics. It tends to land in weak/gray L4 (§3), not strong L4.

**The value-vs-lifecycle paradox.** The two options trade off in opposite directions.
Shared (A) gives the stronger HPX-native distributed semantics but is lifecycle-hostile
under Ray: AGAS-root SPOF, locality loss, dynamic actor restart, bootstrap ordering.
Federated (B) is Ray-lifecycle-compatible, but its HPX delta over L3 may be thin for
closed int64/double values if it reduces to HPX serialization over a custom transport.
The lifecycle-friendly option may buy little over Ray-mediated coordination, while the
high-value option is the fault-hostile one. Whether L4's value proposition actually
requires shared HPX global addressing (A), or a narrower federated channel (B) is still
meaningful for the closed-value, fixed-op workload, is unresolved — and a primary
question for the HPX experts (§8).

## 6. Decisive risks

Each is an open question, not a result:

* **Continuous progress.** HPX cross-actor communication cannot be suspended between
  Python calls — parcel / message progress needs live HPX threads even when the Ray actor
  is idle in Python. That rules out suspend / resume for L4, unlike the in-process
  hosting case. Related, scoped input: the exp64 `waiter_resume_at_timeout` observation
  (§1a); suspended timed-wait readiness is a maintainer question (§8).
* **Ray lifecycle vs HPX distributed-runtime lifecycle.** Independent, dynamic,
  restartable actors vs a runtime that expects a stable set of participants.
* **AGAS-root / locality-0 single point of failure** (shared model) — its loss collides
  directly with Ray's per-actor restart value. AGAS-root loss remains untested; exp50/51
  probed only non-root loss (§1a).
* **Actor restart / failure behavior** — what a peer observes when an actor (runtime /
  locality) dies or restarts. The scoped exp50/51 boundary (§1a): stale membership after
  ungraceful non-root loss allowed continued service but blocked collective shutdown.
* **Thread / CPU progress budget** inside Ray workers — who guarantees HPX progress
  threads get CPU.
* **GIL boundary** — whether HPX progress threads run independently of the GIL-held
  Python edge.
* **Transport choice** — TCP-class likely realistic; MPI likely incompatible with
  dynamically launched Ray actors (no single launcher / fixed world); LCI / libfabric
  open.
* **Action / serialization surface** — a fixed registered HPX action / handler model over
  serializable closed int64 / double values, consistent with RayX's fixed-op discipline
  (no arbitrary payloads).
* **Deserialization / trust surface.** Deserializing and dispatching a message can become
  an execution surface (deserializing an action invokes it). The fixed registered
  receivers are therefore also a containment boundary: arbitrary payloads and arbitrary
  Python callbacks stay out of scope, reinforcing the closed int64 / double and
  fixed-registered-op discipline. The right registration / validation model is open.
* **Per-actor runtime weight.** In the federated model, N Ray actors means N independent
  HPX runtimes, each potentially carrying scheduler threads, local runtime state,
  progress threads, and a resource budget — a lifecycle / resource cost to characterize,
  not a performance claim.

## 7. Smallest feasibility probes

Design targets only — not built, gated, single-node, closed-value, no performance or
multi-node claim.

### Federated (likely first probe — see §5 caveats)

* Single node; two Ray actors, each running an independent HPX runtime.
* Ray bootstraps endpoint discovery between them.
* Actor A's HPX work produces a closed int64.
* HPX serialization encodes it; a self-managed / Ray-bootstrapped channel carries the
  bytes to actor B.
* A fixed registered receiver in B deserializes the payload and submits the fixed op to
  B's local HPX runtime, which returns a closed int64.
* No shared AGAS. No multi-node claim. No performance claim.

This isolates the weak/gray-L4 form (§3) with the least runtime coupling: HPX
serialization on the payload, an external channel for the bytes, local HPX execution on
each side. It does not by itself establish strong-L4 HPX-distributed semantics, and its
delta over L3 for a closed int64 should be weighed against the value-vs-lifecycle paradox
(§5).

### Shared-runtime alternative (heavier, higher risk)

* Single node; two Ray actors join one shared HPX runtime as localities.
* Ray exchanges endpoints / AGAS-root info.
* One HPX action A→B returns a closed int64.
* Heavier and higher risk because of AGAS / root / lifecycle coupling and the single
  point of failure.

## 8. Questions for HPX maintainers / upstream reviewers

1. For **independently launched, independently restartable** Ray actors, do you recommend a
   **shared HPX runtime / localities** or **federated independent runtimes exchanging
   messages**? Where is the crossover?
2. Is a **federated HPX-native message channel** a sane first step, or does HPX's model push
   toward **shared localities / AGAS**?
3. **Is there any supported HPX path for two independently-started HPX runtimes to exchange
   messages *without* a shared AGAS** — or would a federated channel really be
   **`hpx::serialization` over a custom / Ray-bootstrapped transport with fixed receivers**
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
12. Is **`runtime_mode::connect`** mature enough for **dynamic locality join**? (Now
    substantially informed by the standalone arc, §1a — graceful join/leave, and
    demand-ordered admission reproduced cross-node on two real nodes — but untested
    inside Ray workers, under churn, and beyond two nodes for exp65's ordering.)
13. Does **Ray-as-bootstrap / HPX-as-data-plane** make sense as the division of labor?
14. Does "**late and on demand**" connect-mode use mean **late locality admission**
    (demand-ordered admission is narrowly demonstrated by exp65, on loopback and
    reproduced across two nodes) or **lazy parcelport TCP connection establishment**
    (not observed by any experiment)?
15. Should departing connectors have a **clearer disconnect/quiesce contract**? exp63 needed
    a user-space heartbeat / root-completion protocol to prevent connector departure during
    root dispatch — a discussion item, not a proven required HPX design.
16. Is there a supported **stale-locality eviction** path after ungraceful non-root loss?
    We found no supported public path in the inspected HPX 1.11 build and headers (exp50/51,
    single-node loopback / macOS; stale membership served a fresh connector but blocked
    collective shutdown; AGAS-root loss untested) — does a supported or intended mechanism
    exist elsewhere?
17. What is the intended **wake behavior of a suspended timed waiter** on a composed future?
    exp64 observed `waiter_resume_at_timeout` (continuations prompt, waiter woken only at
    its bound) on HPX 1.11 / TCP parcelport; the result was insensitive to every lever
    tested (root/background threads, idle backoff, parcel-pool size) — a scoped
    observation offered for review, not a defect claim.

## 9. Lineage: the federated branch continues the `rayx.endpoint` seam

The federated branch (B) is not greenfield. Per the README documentation map (this note
relies on that summary and does not import the endpoint notes/code), the existing HPX-free
`rayx.endpoint` seam already prototyped most of the federated scaffolding: endpoint
identity (minted `rtb-ep-<hex>` ids) with a process-local registry; Ray/Python-carried
bootstrap metadata; an opt-in AF_UNIX A1 local transport (fixed PING frame, one-shot
dial-per-call); a same-process registry dispatch path and a cross-process one-shot AF_UNIX
path; an endpoint→Runtime bridge carrying a fixed closed `CALL_OP` frame into a hosted
`Runtime`; and a shared-HPX-owner lifecycle (the `Runtime` owns HPX, the `Endpoint` is
HPX-free, order-independent teardown).

The federated L4 probe would change what the wire carries: an HPX-serialized payload
instead of the seam's HPX-free fixed frames, deserialized by a fixed receiver and
submitted to the receiving process's local HPX runtime. The channel stays self-managed and
Ray-bootstrapped; HPX never owns the transport. That makes the probe a narrower and
better-grounded delta than a greenfield transport design — identity, bootstrap, channel,
bridge, and lifecycle already exist — but the HPX-crossing step itself is genuinely new,
and it brings new serialization, dispatch, lifecycle, validation, and error-handling
questions with it.

Still genuinely new after the endpoint work:

* the HPX-crossing step itself, weak or strong (§3) — the seam is HPX-free by design and
  never crossed HPX;
* the federated-vs-shared fork and the value-vs-lifecycle paradox (§5);
* the continuous-progress risk (§6);
* the AGAS-root / locality-0 single point of failure in the shared model (§5, §6);
* the shared-runtime branch (A) in its entirety — the seam explicitly excluded
  parcelport / AGAS / locality / multi-node / fabric.

The seam's standing non-claims hold unchanged: it is HPX-free; the `Runtime` owns HPX; it
proves no fabric, parcelport, AGAS, locality, or distributed-HPX semantics; and exp42/43
remain observation-only and OS-local (see `docs/design/endpoint_runtime_seam.md` and the
exp42/43 write-ups in §12). Deeper reconciliation with the endpoint notes/code remains a
separate reviewed step.

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
* No claim that HPX-mediated communication is better than Ray-mediated coordination.
* No claim that RayX is already a distributed runtime.
* The **future distributed-fabric direction remains gated**.

(Parcelport / AGAS / locality / endpoint / transport appear in this note only as the **gated
mechanism under design discussion**, never as built or measured.)

## 11. Open questions / what would ungate L4

Before any L4 code is attempted, in order (status annotations reflect exp49–65, §1a):

1. **Value proposition over L3** articulated (§4) — what HPX-mediated communication buys
   that Ray-mediated coordination does not. **Open.**
2. **Federated-vs-shared decision** made or narrowed (§5). **Open.**
3. **Continuous-progress model** understood (§6) — how an embedded locality progresses while
   the actor is idle in Python. **Open**; the scoped exp64 `waiter_resume_at_timeout`
   observation is an input to the maintainer discussion, not an answer.
4. **Fault / restart model** understood (§6) — what a peer sees when an actor dies/restarts.
   **Open**; exp50/51 give a scoped non-root-loss boundary only, and AGAS-root loss remains
   untested.
5. **Transport / bootstrap model** selected (§6, §8). **Open**; connect-mode **admission** is
   now **substantially informed** (late admission across the distributed HPX-island probes
   from exp49 Phase 2 onward; demand-ordered admission in exp65, on loopback and reproduced
   cross-node on two real nodes) but **not fully closed** — lazy parcelport TCP connection
   establishment, connector churn, and reproduction of exp65 beyond two nodes are all
   unobserved.
6. **HPX maintainer / upstream-reviewer vetting** of the above. **Actively initiated** —
   Hartmut's invitation opened the upstream discussion — but **not completed**.
7. Only then, a **tiny single-node feasibility probe** (§7), starting federated. **Gated**;
   HPX-inside-Ray-worker execution stays gated and no L4 code is written.

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
* Standalone connect-mode / lifecycle / readiness arc cited in §1a (standalone processes,
  never Ray actor workers):
  `experiments/49_strong_l4_hpx_distributed_spike/strong_l4_hpx_distributed_spike.md`,
  `experiments/50_strong_l4_connect_failure/strong_l4_connect_failure.md`,
  `experiments/51_strong_l4_stale_locality_shutdown/strong_l4_stale_locality_shutdown.md`,
  `experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md`,
  `experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md` (Slice 5, Phase A→A4),
  `experiments/65_demand_admission/demand_triggered_admission.md`.
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
