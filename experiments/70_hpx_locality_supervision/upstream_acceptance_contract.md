# HPX locality-lifecycle acceptance contract

Written for HPX maintainers evaluating **issue #7390**. Not a roadmap and not an evidence dump:
it shows how four completed *external* reference implementations become runnable acceptance tests
for a future HPX-native lifecycle API, so that if such an API is added an unchanged harness is
already waiting to validate it.

## Problem

A dynamically connected locality needs its peers, or an external supervisor, to tell four states
apart:

* **alive but idle** — connected, doing nothing, still obliged to accept work;
* **explicitly completed** — no further work will be sent to it;
* **gracefully departed** — it left through a validated disconnect;
* **unexpectedly lost / suspected lost** — it is gone, or has stopped answering.

None of these is available today as a classified HPX runtime fact. Membership can be polled, but
polling cannot separate "idle" from "gone", and we measured membership still containing a
departed locality at the observation point. We implemented all four externally, as gated tests,
to find the boundary: the external backends demonstrate **executable semantics** — state
machines, event vocabulary and acceptance gates that are real and passing — but cannot supply an
**authoritative runtime fact**. An outside observer only ever reports bounded suspicion.

## Executable external references

Each reference verifies a deterministic bit-exact distributed workload *first*, then exercises
the lifecycle transition, and gates every claim.
**Slice 0 — why fixed connector lifetimes fail.** A connector with a fixed serving window
departs; the root then idles longer than that window and validly dispatches again; the dispatch
fails. An external completion/liveness workaround removes the guess and the same late dispatch
succeeds. Two loopback processes, HPX-only — no Ray, no Python. Standalone public reproducer.
→ [`upstream_reproducer/README.md`](upstream_reproducer/README.md) ·
[`run_case.sh`](upstream_reproducer/run_case.sh)

**Slice 2A — explicit completion.** Completion is published only after the final result is
verified; connectors stay available across an idle interval longer than the former fixed serving
window; later valid work succeeds; duplicate publication is rejected; a post-completion
application dispatch is fenced before it could reach HPX; connectors leave cleanly only after
observing completion.
→ [`slice2_explicit_completion/README.md`](slice2_explicit_completion/README.md) ·
[`run_slice2.py`](slice2_explicit_completion/run_slice2.py)

**Slice 3A — connector departure vs loss.** One lifecycle event schema drives both arms. Graceful
departure is classified separately from unexpected loss, and the classifier cannot access
injection metadata — its evidence mapping is restricted to a fixed allow-list and raises on
anything else. Membership behaviour after the loss is recorded, never assumed.
→ [`slice3_connector_loss_event/README.md`](slice3_connector_loss_event/README.md) ·
[`run_slice3.py`](slice3_connector_loss_event/run_slice3.py)

**Slice 4A — root completion vs suspected root loss.** Explicit root completion is classified
separately from bounded suspected root loss. The classifier cannot access root-kill metadata. The
suspicion bound is measured in monotonic time, never wall clock. All actor-side calls after root
loss are bounded, and the poisoned island is discarded rather than repaired.
→ [`slice4_root_loss_event/README.md`](slice4_root_loss_event/README.md) ·
[`run_slice4.py`](slice4_root_loss_event/run_slice4.py)

## Native completion acceptance contract

Slice 2B should run the **existing Slice 2A state machine and gates unchanged**, replacing only
the completion publication/observation backend. Runtime facts required:

* a root can state that **no further work will be sent** to a connected locality;
* all intended connectors can **observe** that completion;
* observation is **epoch/locality scoped**, so a stale notification cannot be mistaken for a
  current one;
* completion is **distinct** from voluntary departure and from unexpected loss;
* **no fixed connector lifetime** is required to make the guarantee hold.

Explicitly *not* requested: HPX need not enforce the supervisor's post-completion application
fence. That is harness policy, and Slice 2A does not claim HPX blocks parcels after completion.
It becomes a runtime concern only if maintainers choose to offer a stronger contract.

## Native connector-lifecycle acceptance contract

Slice 3B should run the **existing Slice 3A classifier and gates unchanged**, replacing external
Ray/process evidence with an HPX-native connector lifecycle event. Required classifications:

```text
graceful_departure
unexpected_loss or suspected_loss
```

Required properties:

* identifies the **target locality**;
* scoped to the **current runtime epoch**;
* graceful departure is **not conflated** with process or transport loss;
* **bounded delivery semantics** are documented;
* **duplicate and stale event behaviour** is defined.

The discriminator Slice 3A found externally is worth preserving natively: a gracefully departed
connector's *host* stays answerable, whereas a lost connector takes its host with it. A native
event should state that directly rather than leave it inferred.

## Native root-lifecycle acceptance contract

Slice 4B should run the **existing Slice 4A state machine unchanged**, replacing the filesystem
liveness witness and supervisor inference with HPX-native root lifecycle facts. Required facts:

* **explicit root completion**;
* **root loss or suspected-loss notification**;
* an **idle-but-healthy root is never classified as lost**;
* if certainty is impossible, **documented timeout/suspicion semantics** instead;
* **bounded application-facing observation**.

## Key observed behavior

One accepted finding is directly relevant to the design:

> On the tested HPX build and two-node topology, actor-hosted HPX calls timed out after
> unexpected root loss rather than failing promptly. Bounded external observations were required
> to prevent the supervisor from becoming stranded.

This is **scoped to the tested build and topology**, not generalized to other builds, networks or
failure modes. We raise it because a fail-fast error — or a documented blocking bound — on calls
whose root locality is gone would make the difference between "slow" and "gone" a runtime fact
rather than each caller's own timeout guess.

## Backend substitution model

```text
        existing state machine and acceptance gates
                         |
                lifecycle backend interface
                  /                    \
        external reference         future HPX-native
        filesystem/Ray/process      runtime facts/events
```

* The **A slices** validate the semantics and harnesses; they are complete and hardware-verified.
* The **B slices** must replace **only the backend**.
* A renamed external backend is **not** HPX-native, and we will not present one as such.
* The **acceptance gates stay unchanged** across substitution — that is what makes them a usable
  conformance test rather than a description. Each A slice already ships a substitute-backend
  selftest proving a different backend passes the identical gates.

## Recovery boundary

**HPX exposes lifecycle facts. RayX/Ray owns** placement, failure policy, and whole-island
replacement. HPX is **not** being asked to repair AGAS, reconstruct application state, continue a
partial island, or restart supervisor actors.

Slice 1 is mentioned for one reason only: it shows lifecycle facts are *sufficient* for an
external supervisor to classify the island as failed and replace it whole. No runtime-side
recovery is needed for the model to work.

→ [`slice1_actor_hosted_island_restart/README.md`](slice1_actor_hosted_island_restart/README.md)

## Upstream API gaps

Gate-by-gate mapping from each A-slice acceptance gate to the runtime fact its B equivalent needs:
[`native_backend_gap_matrix.md`](native_backend_gap_matrix.md).
```text
2B blocked: missing native explicit completion
3B blocked: missing native connector departure/loss event
4B blocked: missing native root completion/loss or liveness event
```

All three are:

> upstream-blocked, not harness-blocked

## Suggested contribution sequence

We are offering implementation and regression-test work, not a fixed design — deliberately narrow,
and without prescribing final HPX API names:
1. **Agree on event ownership and API shape** — runtime, AGAS, or opt-in component, and who may
   publish each event.
2. **Implement explicit completion first**, if maintainers agree it is the smallest useful
   capability; it is the only item needing no failure semantics at all.
3. **Adapt the two-process reproducer or the Slice 2 harness into upstream regression tests**, in
   whatever form the test suite prefers.
4. **Add classified connector departure/loss.**
5. **Add root-loss / liveness behaviour**, including the timeout semantics above.
6. **Run the unchanged acceptance contracts** against each step.

Related: [`README.md`](README.md) (technical overview) ·
[`evidence_index.md`](evidence_index.md) (per-slice evidence records) · HPX **#7384**, the
separate stale-dispatch error-reporting defect found while reducing Slice 0 — already fixed
upstream, and not a lifecycle change.
