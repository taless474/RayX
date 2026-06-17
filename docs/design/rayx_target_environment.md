# RayX Target Environment: Local Native Actor Runtime with Python Control

**Status: design / positioning (exploratory).** This document defines the first
narrow environment where RayX could serve as a narrow alternative to a *small
subset* of Ray usage, and how RayX should expand from there without becoming a
fake Ray clone. It follows
the accepted strategy built on the internal HPX/runtime audit notes (local
provenance, not tracked) and the implemented actor MVP
([rayx_runtime_local_native_actors.md](rayx_runtime_local_native_actors.md)).
It lives under `docs/design/` deliberately: it is direction, not a shipped
contract, and it makes no performance or "HPX beats Ray" claim.

## 1. Executive positioning

RayX targets **one machine, Python in control, C++ owning the state and doing
the work**: many fast calls into long-lived stateful native code, with futures,
strict per-actor FIFO, guaranteed-if-accepted cooperative cancellation, explicit
bounded admission, orderly shutdown, and inspectable queues. **RayX is not a
general Ray replacement** — there is no distribution, no object store, no
arbitrary remote Python, and no Ray ecosystem. RayX offers a **narrow
alternative to one small subset of Ray usage**: the case where Ray actors are
used as an awkward way to serialize calls into native state on a single
machine — a case that pays Ray's process boundary, serialization, and
distributed control plane for a problem that needs none of them.

## 2. Target environment

**Who the user is.** An engineer fluent in both Python and C++ who owns a
stateful native library and drives it from Python scripts or notebooks on one
many-core machine. Python is the control language; C++ is where the state and
the work live.

**What the workload is.** Many short-to-medium calls (microseconds to tens of
milliseconds) into long-lived native objects; fan-out across instances;
"give me results as they finish"; the occasional need to abandon queued or
long-running work; and a need to shed load explicitly under bursts rather than
queue unboundedly.

**Why they might use Ray today.** `@ray.remote class` is the most convenient
off-the-shelf way to get serialized stateful access plus method futures,
`ray.wait`, and an actor-pool shape — even when nothing is distributed.

**Why Ray is too broad/heavy here.** Each actor is a process; every call
crosses an IPC + serialization boundary (millisecond-scale per call, per this
repo's own measured baselines); values pass through a distributed object store;
and the user inherits a distributed control plane, fault-tolerance machinery,
and a large dependency they never needed. None of that is wrong — it is the
price of distribution, paid where there is no distribution.

**Why RayX fits better.** RayX's lanes and actors are in-process and native:
microsecond-scale dispatch, no pickle, no actor processes, GIL-free native
method bodies, and semantics that are *stronger in this niche and stated
honestly* — deterministic per-actor FIFO, a cancellation `True` that is a
guarantee tied to a checkpoint contract (not best-effort), and admission that
rejects explicitly (`QueueFullError`) instead of queueing silently.

## 3. Concrete examples

* A **C++ simulator or RL environment** stepped from Python: each env instance
  is an actor; `step`-like registered methods run natively; Python orchestrates
  sweeps and collects results as they complete.
* A **quant/risk/pricing engine** exposed to research Python: each instrument
  book or curve is a stateful actor; thousands of small revaluation calls per
  session, where per-call overhead dominates under Ray.
* A **native tokenizer / cache / preprocessing component**: long-lived native
  state (vocab, cache, index) behind serialized typed method calls.
* A **robotics / control simulation bench**: a hardware-in-the-loop model
  stepped at a fixed cadence, with cancellation and bounded admission as
  safety/sanity rails.
* Generally: **many small native actors on one machine**, where the actor
  exists to serialize access to state, not to place work on a cluster.

## 4. What RayX provides — today vs. next

Everything below is implemented today in the experimental `rayx.runtime`
prototype, except the explicitly marked **next** block at the end. "Today"
still means experimental: this whole environment is direction, not shipped
support.

* **In-process native dispatch** — Python → pybind11 → C++ engine → HPX-native
  lane; no process boundary, no serialization of values beyond a closed typed
  channel.
* **Fixed registered C++ operations and actor methods** — a compiled-in
  registry with typed signatures, validated at the Python boundary before any
  native crossing.
* **Per-actor FIFO** — each actor owns a dedicated lane (one queue, one
  worker); methods run one at a time in call order, which is what makes native
  state safe without locks.
* **Cooperative cancellation** — queued work is skipped entirely; running
  checkpointed work stops at its next checkpoint; `cancel() == True` guarantees
  the result row retires as `cancelled`. Never force-kill.
* **Bounded admission / backpressure** — an atomic per-lane cap; a full lane
  rejects at submit time with `QueueFullError`, creating no future and no row.
* **Shutdown/drain** — teardown cancels outstanding work, drains, and joins;
  every future ever handed out resolves (completed, failed, or cancelled).
* **Futures and results** — consume-once futures with `get` / `wait` /
  `as_completed`; results separate the user **value** from the measurement
  **row**.
* **Observability (operation lanes)** — per-lane `{actor_id, queue_depth,
  active}` snapshots exist for operation lanes today.

**Next (accepted direction):** per-actor observability — the
same snapshot for actor lanes (internal audit notes §9), because it is what
unlocks deterministic actor tests (queue-full, running-cancel, teardown
assertions); since shipped as `ActorHandle.stats()` — followed by actor
lifecycle (since shipped as `Runtime.release_actor`: explicit, local, bounded
cancel-then-drain release of one actor) and the extension SDK (§7, §8).

## 5. What RayX explicitly does not provide

* No object store and no `ObjectRef` — values are returned, never stored and
  referenced.
* No arbitrary Python remote functions — only pre-compiled registered native
  operations run.
* No Python actor state — actor state is native C++ data, never a Python
  object or pickle.
* No Ray Serve, no Ray Train, no inference stack.
* No distributed / multi-node claim — everything is one process on one
  machine.
* No autoscaling, no fault tolerance, no actor restart, no placement.
* No `.remote()` compatibility shim and no attribute-style method dispatch —
  the call surface is an explicit `call("method", *args)` over a closed
  registry.

These are absences by design, not roadmap gaps: they are exactly the machinery
whose cost this niche should not pay.

## 6. Why not a Ray compatibility layer?

A `ray`-importable shim would promise semantics RayX deliberately lacks.
`ray.get` means "fetch a value from a distributed object store"; RayX's `get`
retires a local future into a value-plus-row pair. `.remote()` implies an open
Python method set and cluster scheduling; RayX dispatches a fixed native
registry locally. Ray's `ray.cancel` is best-effort; RayX's cancel is a
checkpoint contract. Compatibility layers are judged by what silently breaks,
and everything interesting about RayX is precisely where it differs — so a shim
would mislead exactly the users it attracted, and would re-import the "Ray
replacement" framing this project refuses. The better artifact is a **migration
mapping**: Ray idiom → RayX idiom, with the semantic difference stated per row
(seeded today by [../ray_hpx_mapping.md](../ray_hpx_mapping.md) and the
actor-pool example). Keep API names distinct so the differences stay visible at
the call site.

## 7. Expansion roadmap

* **Stage 0 — finish current hardening.** Land C5 (`busy_get`); actor lane
  observability plus the deterministic actor tests it unlocks; the hygiene
  sweep (including the `Engine` constructor exception-safety fix); one
  cooperative parked-op demo so the runtime demonstrates HPX's overlap value
  itself. This is the accepted roadmap from the internal audit notes (§9).
* **Stage 1 — actor lifecycle.** `release_actor` (since shipped as
  `Runtime.release_actor`: stop/join the lane, resolve outstanding futures,
  free state — explicit and local only); still open: a many-actors stress test
  and a per-actor admission cap knob — what makes "an actor per
  simulator/instrument" a sane pattern instead of a thread leak.
* **Stage 2 — native extension SDK.** Let a downstream C++ project register
  its own typed ops/actor types against the existing contract (`OpArgs`,
  `OpOutcome`, `StopCheckpoint`, typed signatures, checkpoint counts) and build
  its own extension module. Registration stays **compile-time** — no `dlopen`,
  no runtime plugins — so "fixed registered native methods" remains literally
  true; only *whose build* fixes them changes. Ships with a conformance test
  suite and one real example extension. This is the stage where RayX becomes
  usable by someone who is not its author.
* **Stage 3 — optional, evidence-gated, much later.** If a real Stage-2 user
  needs more than one machine's cores: first multi-process on one node
  (independent runtimes supervised from Python — honestly process isolation,
  *not* HPX distribution), and only after that, HPX multi-locality as a design
  study. **Distribution is explicitly not the near-term direction**; it stays
  design-only until demanded by evidence.

## 8. Minimum credible product shape

The smallest shape an outsider would call usable rather than a demo:

* `Runtime` (process-singleton, context-managed lifecycle).
* Fixed registered native operations with the closed typed value model
  (int64 / finite double; `bytes` deferred and gated).
* Fixed registered native actor types over dedicated per-actor lanes.
* `ActorHandle.call(method_id, *args)` as the sole method-dispatch surface.
* Consume-once futures with `get` / `wait` / `as_completed` and value/row
  separation.
* Queue/admission semantics: atomic per-lane bounded admission;
  `QueueFullError` on reject; no future and no row created.
* Cancellation semantics: queued skip always; running stop at declared
  checkpoints; `cancel() == True ⟺` cancelled row.
* **Actor observability** (per-actor queue depth / active snapshot).
* **Release/lifecycle** (`release_actor`; bounded teardown).
* Eventually: the **SDK plus conformance tests** that make the op/actor
  contract executable for downstream registries.

## 9. Risks

* **The niche may be too narrow.** Users might "just write pybind11." The
  counter-argument is the audited kernel — exactly-once futures, race-free
  cancellation, atomic admission, bounded teardown — which is the part everyone
  gets wrong by hand. A narrow honest benchmark and one real example are the
  cheap way to test whether that argument lands.
* **The SDK is a trust boundary.** A downstream op can lie about checkpoints,
  block an HPX worker, or misuse state. Mitigation: compile-time registration
  only, a written obligations contract, and a conformance suite — stated
  plainly rather than pretending to sandbox.
* **HPX setup/build friction.** HPX is a from-source build and a real adoption
  barrier; mitigate with pinned versions and vendoring guidance, and never hide
  the cost.
* **The bytes/buffer value model is the slippery slope.** `bytes` → buffers →
  references is how an accidental object store happens. Any extension of the
  value channel stays bounded, copied, and gated on a real downstream need.
* **Scope creep toward a "worse Ray."** Every convenience that erodes the
  closed registry, the local-only scope, or the honest cancellation contract
  converts RayX from credible-in-its-niche into an inferior imitation. The
  do-not-pursue list (no object store, no arbitrary Python, no `.remote()`, no
  distribution-first work, no unqualified benchmarks) is the guardrail.

## 10. README-ready positioning paragraph

> **RayX** is an experimental single-machine runtime that lets Python drive
> *stateful native C++ actors* over HPX-scheduled service lanes — with
> in-process native dispatch, strict per-actor FIFO, guaranteed-if-accepted
> cooperative cancellation, explicit bounded admission, and inspectable queues.
> It is **not** Ray and not a Ray replacement: there is no distribution, no
> object store, no arbitrary remote Python, and no performance claim beyond its
> narrow, measured niche. RayX targets the case Ray was never shaped for —
> many fast calls into long-lived native state on one machine — where paying a
> process boundary, serialization, and a distributed control plane buys
> nothing. If your workload is distributed, Python-native, or needs Ray's
> ecosystem, use Ray.

## See also

* The internal HPX/runtime audit notes (local provenance, not tracked) — the
  independent review whose roadmap this follows.
* [rayx_runtime_local_native_actors.md](rayx_runtime_local_native_actors.md) —
  the implemented actor MVP this environment builds on.
* [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) — the runtime
  problem model and locked decisions.
* [../ray_hpx_mapping.md](../ray_hpx_mapping.md) — the Ray ↔ HPX conceptual
  mapping (seed of the migration guide).
