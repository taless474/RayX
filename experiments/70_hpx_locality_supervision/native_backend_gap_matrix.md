# exp70 — native-backend gap matrix

Status of the B-slices as of 2026-07-19: each A-slice complete and hardware-verified with an
**external** backend; every B-slice **blocked**, because none of them could be built honestly
without an upstream HPX API that did not exist yet.

**2026-08-18 update.** Upstream landed `components/supervision_dispatch` +
`hpx::force_disconnect` (HPX issues #7390 / #7441, merged PR #7447) since the assessment below
was written. This is documented additively, not by deleting the original analysis — see
"2026-08-18 upstream update" below each affected slice's block-status row for the current
reading. In short: **3B has a native harness now implemented** against the real upstream API
(`slice3_connector_loss_event/native/`, `run_slice3b.py`), pending a Rostam build+run for
hardware evidence; **4B's blocking reasons narrowed but did not close**; **2B's prior assessment
(unblocked, not yet implemented) is unchanged** by this update.

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

| Slice | A-status | B-status (2026-07-19) | B-status (2026-08-18) | Block code |
|---|---|---|---|---|
| 2 | complete (job 173796, medusa11/12) | blocked | unblocked in principle (unchanged since 07-30; no native harness implemented yet) | `blocked_missing_hpx_native_completion` |
| 3 | complete (job 173797, medusa06/07) | blocked | **native harness implemented, run on hardware (job 185466-185474, medusa00): blocked at `discover_and_join()`**, not a gate PASS | `blocked_missing_hpx_native_event` → superseded, see below |
| 4 | complete (job 173798, medusa00/01) | blocked | narrowed (detection plausible via symmetric fencing) but still blocked (recovery structurally excluded) | `blocked_missing_hpx_native_root_event` (unchanged) |

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

**2026-08-18 update — native harness implemented, gate-by-gate:**

* `classification_matches_arm` / `evidence_blindness.*` — **closed by `failure_detection_loop()`**
  (`components/supervision_dispatch/src/dispatch_api.cpp`), a background sweep started
  automatically by `hpx::supervision::init()`. It calls `await_terminal()` per joined peer,
  re-confirms genuine silence via `query_state()`'s `event_sequence_number` (`probe_peer_silence`/
  `stalled_after_grace`, three consecutive silent windows), then publishes `event::failed`
  **against its own local shadow of that peer** — no application code decides this. exp70's
  `native/root_supervised.cpp` never calls `publish_event(..., event::failed, ...)` anywhere
  (grep it; `g_app_failed_publish_count` can only read 0), so classification is structurally
  runtime-only.
* `exactly_one_terminal_accepted` / `no_terminal_for_non_departed` — covered by
  `hpx::supervision::event::completed`/`failed` terminal latching
  (`publish_result::already_terminal` on any later terminal publish for the same target/epoch;
  `is_valid_transition()` forbids a terminal event for a target still in `started`/`running`).
* `<arm>_publication_bound` — the sweep's own `poll_timeout` (the `default_discovery_timeout`
  upstream default is 60 s; exp70 overrides it via the **public, documented**
  `hpx::supervision::testing::set_failure_detection_poll_timeout_for_testing()` test hook to keep
  the harness bounded) plus up to three `stalled_after_grace` grace windows.
* `epoch_match` — closed by `hpx::supervision::discovered_peer::join_epoch` and
  `check_admission(locality, epoch)`'s epoch-scoped latch read.
* `loss` post-departure membership / AGAS repair — **closed, but only for late-connecting
  (`is_connecting==true`) localities and only by an EXPLICIT root call**: `hpx::force_disconnect`
  (merged PR #7447) performs `runtime_support::remove_locality()` (bounded 5 s target-shutdown
  notify, `remove_from_connection_cache_action` broadcast to every other known locality,
  primary-namespace AGAS deregistration, console cache prune). exp70 observes this from **two**
  vantage points: the calling root (`hpx::agas::resolve()` failing, membership shrinking, a plain
  post-force_disconnect dispatch failing) and, as a bonus check beyond force_disconnect's own
  upstream test coverage, the **surviving connector A independently** re-probing B's old locality
  id after force_disconnect (`connector_ext.cpp::probe_locality`) to test the cluster-wide
  cache-purge broadcast, not just root's own view.

**What remains genuinely open even with the native harness:** (1) fencing and `force_disconnect`
are two independently-callable mechanisms — nothing upstream triggers the second automatically
from the first, so exp70's root calls it explicitly, only after observing fencing, and the
write-up must not imply otherwise; (2) the stronger, templated
`hpx::supervision::dispatch_work<Action>()`/`fenced_action<>` path is recorded only as a
**non-gating diagnostic** in `root_supervised.cpp`, because whether HPX guarantees a
class-template-instantiated fenced action is registered in a *separately compiled* binary that
never itself calls `dispatch_work()` was not independently verified against upstream (the only
worked example, `late_component_launcher.cpp`/`late_component_worker.cpp`, never exercises that
direction either — the worker binary never calls `dispatch_work`); the harness's actual gating
oracle is the untemplated, registration-risk-free `check_admission()` local latch read instead;
(3) hardware verification (Rostam, `-DHPX_WITH_SUPERVISION=ON`) **has now run** (job
185466-185474, medusa00, 2026-08-18) and is **blocked before reaching any of the 16 gates**:
`hpx::supervision::discover_and_join()` reproducibly found zero peers in both directions
(root-discovers-connector and connector-discovers-root/peer), across five consecutive attempts
including a 30s bounded retry, despite each connector's own registry symbol name self-resolving
successfully and root correctly enumerating the expected remote-locality count. This looks like a
cross-locality AGAS symbol-visibility gap specific to this component's discovery mechanism in a
topology of independently-launched OS processes, not a harness bug — see
`slice3_connector_loss_event/README.md`'s "Hardware run result" section for the full diagnostic
trail. `classification_matches_arm`/`evidence_blindness.*` etc. above describe what the
mechanism *would* close if discovery worked; none of it was exercised on hardware.

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

**2026-08-18 update — narrowed, not closed.** Per Phase 4 of the exp70 continuation ("do NOT try
to solve root loss"), this is source-level analysis only; no new native Slice 4B binary was
built. Three specific questions, answered directly from the current upstream source:

1. **"Does a connector get an equivalent native root-loss classification?"** — plausibly yes,
   newly. `failure_detection_loop()` is symmetric: it runs on *any* locality that called
   `hpx::supervision::init()`, sweeping whichever peers *that* locality itself joined via
   `discover_and_join()`. `components/supervision_dispatch/examples/late_component_worker.cpp`
   already demonstrates exactly this direction — a worker joins root, then loops on
   `check_admission(root_peer.locality, root_peer.join_epoch)` to detect "root locality fenced."
   This did not exist in the 07-19 assessment and narrows `loss_root_event`/`loss_monotonic`.
2. **"Is `hpx::force_disconnect` usable against the root/console?"** — **no, by construction.**
   `libs/full/init_runtime/tests/unit/force_disconnect.cpp`'s own
   `test_console_cannot_disconnect_itself` asserts this fails with `hpx::error::bad_parameter`,
   and `force_disconnect`'s own doc comment restricts callers to "the console locality only,"
   which is precisely the locality that is dead in the root-loss scenario. There is no
   proxy/self-recovery path. This is the **structural** reason 4B stays blocked, not an oversight.
3. **"Do post-root-loss operations fail in a bounded way, or still block?"** — supervision
   fencing only protects a call that explicitly checks `check_admission()` (or goes through
   `dispatch_work()`) **before** dispatching; it does nothing for an ordinary, already-in-flight,
   or naively-issued plain HPX call to a dead locality, which is exactly what Slice 4A's hardware
   evidence measured (`call_timeout` on every post-loss actor probe). That finding is therefore
   **unchanged**: the sharpest gap (`loss_actor_observations.{a,b}_category = call_timeout`)
   requires the *application* to adopt a check-admission-first pattern; it is not automatic.

Net: `loss_root_event`/`loss_monotonic` narrowed (detection looks buildable); the AGAS-repair
half of `loss_disposal` and the blocking-call half of `loss_actor_observations` remain closed
exactly as before. Do not read point 1 as "4B is now implementable" — recovery (force_disconnect)
is still impossible for the one locality (root/console) that would need it, so a genuinely
useful 4B still has no path to the same recovery story 3B now has.

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
* [`slice3_connector_loss_event/native/`](slice3_connector_loss_event/native/) — the Slice 3B
  native harness (`root_supervised.cpp`, `connector_ext.cpp`, `CMakeLists.txt`) and
  [`slice3_connector_loss_event/run_slice3b.py`](slice3_connector_loss_event/run_slice3b.py) —
  the executable native reference test, pending hardware verification.
* [`slice1_actor_hosted_island_restart/README.md`](slice1_actor_hosted_island_restart/README.md)
  — whole-island replacement, the recovery boundary these events feed.
* [`upstream_reproducer/README.md`](upstream_reproducer/README.md) — Slice 0, the public defect
  reproducer (HPX issue #7384; fix and regression-test PR approved upstream).
