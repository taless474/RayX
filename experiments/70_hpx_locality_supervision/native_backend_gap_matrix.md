# exp70 — native-backend gap matrix

Status of the B-slices as of 2026-07-19. Each A-slice is complete and hardware-verified with an
**external** backend. Every B-slice is **blocked**, because none of them can be built honestly
without an upstream HPX API that does not currently exist.

```text
Slice 2B requires:
    HPX-native explicit-completion publication and observation

Slice 3B requires:
    HPX-native connector graceful-departure/loss notification

Slice 4B requires:
    HPX-native root completion/loss or runtime liveness notification
```

Every B slice is:

```text
upstream-blocked, not harness-blocked
```

The acceptance harnesses exist, pass, and are hardware-verified. What is missing is the upstream
runtime fact each one would consume. A renamed external backend is **not** a native backend.

## Block status

| Slice | A-status | B-status | Block code |
|---|---|---|---|
| 2 | complete (job 173796, medusa11/12) | blocked | `blocked_missing_hpx_native_completion` |
| 3 | complete (job 173797, medusa06/07) | blocked | `blocked_missing_hpx_native_event` |
| 4 | complete (job 173798, medusa00/01) | blocked | `blocked_missing_hpx_native_root_event` |

**What "blocked" forbids.** A B-slice may not be satisfied by Ray actor death, OS process
liveness, filesystem witnesses, or supervisor inference — nor by moving an existing external
observation behind a differently named class. Those are exactly what the A-slices already do,
and renaming them would manufacture a native result that does not exist.

**What "blocked" preserves.** Each A-slice keeps a backend-neutral acceptance surface, so a
genuine HPX backend can later be substituted and must feed the **same** state machine, gates and
event vocabulary **without changing their semantics**. Each A-slice already selftests a
substitute backend passing the identical gates, so the substitution path is exercised today.

| Slice | Acceptance surface retained | Substitution already selftested |
|---|---|---|
| 2A | `publish_complete(epoch, connectors, payload)` / `observe_completion(epoch, connector, bound)` | yes — minimal substitute backend passes all gates |
| 3A | `publish_event(epoch, connector, kind, digest, classification)` / `observe_events(epoch, observer, bound)` | yes — `SubstituteBackend` passes all gates |
| 4A | `observe_root_event(epoch, root_identity, bound)` / `publish_completion(epoch, root_identity, payload)` | yes — `SubstituteRootObserver` drives the identical contract |

## Gate → required upstream API fact

Each row is an A-slice acceptance gate that currently passes on external evidence, paired with
the upstream fact its B equivalent would need. "Upstream fact" means a documented HPX API
guarantee, not an inference a supervisor can make from outside.

### Slice 2B — native explicit completion

| A-gate (external, passing) | Upstream API fact required for 2B |
|---|---|
| `completion.publish_only_after_final_verification` | a runtime call that publishes "no further work will be sent" as a first-class island event, orderable after application results |
| `completion.duplicate_publish_rejected` | runtime-side idempotence for that publication, or a documented single-publication guarantee |
| `epoch_scope.stale_marker_from_prior_epoch_rejected` | an epoch/generation identifier carried by the runtime event itself |
| `observation.{a,b}_epoch_match`, `_bounded` | a bounded, per-locality observation API that returns the completion event for a named epoch |
| `observation.all_connectors_acknowledged` | per-locality acknowledgment visible to the publisher |
| `post_completion_fence.post_completion_dispatch_never_reached_hpx` | *(optional)* runtime refusal of application parcels after completion. Slice 2A explicitly does **not** claim HPX blocks them today |

### Slice 3B — native connector departure/loss event

| A-gate (external, passing) | Upstream API fact required for 3B |
|---|---|
| `<arm>_classification.classification_matches_arm` | a runtime-emitted departure event that is itself **classified** graceful vs lost |
| `<arm>_evidence_blindness.*` | classification decided inside the runtime, so no supervisor-side evidence assembly is needed at all |
| `<arm>_event_contract.exactly_one_terminal_accepted` | exactly-once terminal event per departing locality |
| `<arm>_event_contract.no_terminal_for_non_departed` | runtime guarantee that no terminal event is emitted for a live locality |
| `<arm>_observation.epoch_match` | epoch/generation identity on the departure event |
| `<arm>_publication_bound.publication_within_bound` | a documented bound between departure and notification |
| `loss` post-departure membership (`membership_stale`, observational) | AGAS membership repair, or a documented statement that membership is not repaired |

### Slice 4B — native root completion/loss event or runtime liveness surface

| A-gate (external, passing) | Upstream API fact required for 4B |
|---|---|
| `normal_root_event.event_matches_arm` (`explicit_completion`) | a runtime root-completion event distinguishable from loss |
| `loss_root_event.event_matches_arm` (`suspected_root_loss`) | a runtime root-loss notification, or a liveness surface with a documented detection bound |
| `loss_monotonic.silence_reached_bound` | a runtime-published expected refresh/liveness interval, so the bound is not a supervisor guess |
| `loss_monotonic.pre_bound_probe_did_not_declare_loss` | a documented false-positive boundary for that surface |
| `<arm>_epoch_scope.*` | epoch identity on root events; today `root.done` carries none |
| `loss_actor_observations.{a,b}_category` = `call_timeout` | **the sharpest gap**: after root death, actor-hosted HPX calls block rather than fail fast. 4B needs either a fail-fast error on calls whose root is gone, or a documented blocking contract with a bound |
| `loss_disposal.disposal_mode_is_poisoned_island` | a runtime statement about whether an island can survive root loss at all (exp51/53 say it cannot today) |

## Certainty ceiling

Slice 4A's verdict is **bounded suspicion**, not detection. No external backend can raise that to
certainty: silence is indistinguishable from a stalled-but-live root without a runtime liveness
guarantee. This ceiling is a property of the external approach, not of the instrument, and it is
the single strongest argument for the upstream feature request.

## Related

* [`upstream_acceptance_contract.md`](upstream_acceptance_contract.md) — the maintainer-facing
  companion to HPX issue #7390: how each A-slice becomes an unchanged acceptance test for a
  future native lifecycle API. This matrix is the gate-level detail behind it.
* [`README.md`](README.md) — exp70 technical overview.
* [`evidence_index.md`](evidence_index.md) — per-slice evidence records.
* [`slice2_explicit_completion/README.md`](slice2_explicit_completion/README.md),
  [`slice3_connector_loss_event/README.md`](slice3_connector_loss_event/README.md),
  [`slice4_root_loss_event/README.md`](slice4_root_loss_event/README.md) — the executable
  external reference tests.
* [`slice1_actor_hosted_island_restart/README.md`](slice1_actor_hosted_island_restart/README.md)
  — whole-island replacement, the recovery boundary these events feed.
* [`upstream_reproducer/README.md`](upstream_reproducer/README.md) — Slice 0, the public defect
  reproducer (HPX issue #7384; fix and regression-test PR approved upstream).
