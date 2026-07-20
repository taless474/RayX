# exp70 Slice 4A — explicit root completion vs unexpected root loss

**Status:** complete — local phase and two-node hardware phase both green. Mechanism /
application-contract evidence only. Not a performance experiment, not a Ray comparison, not
production code, not a shipped RayX API.

## 1. Question

Can connected actor-hosted HPX localities distinguish **explicit root completion** from
**unexpected loss** of the separately supervised work-free root, using the current external
lifecycle backend?

Slice 2A closed explicit connector-side completion; Slice 3A closed classified connector
departure/loss. Slice 4A moves the same two-item contract to the root — the one process the
island cannot replace in place.

## 2. Architectural boundary

```text
node A   controller / Ray head / separately supervised WORK-FREE HPX root / actor A
node B   actor B
```

The work-free root stays a separately supervised **process**, never a Ray actor.

The root witness is exp68's `root.alive`, which the root rewrites on a ~200 ms loop. It is an
**external, periodically refreshed root-liveness witness**. It is **not** an HPX-native
heartbeat, **not** HPX failure detection, and **not** authoritative proof of failure. Every loss
verdict here is **bounded suspicion derived by the supervisor**.

## 3. Backend-neutral event surface

```text
RootLifecycleObserver.observe_root_event(epoch, root_identity, bound)
                     .publish_completion(epoch, root_identity, payload)

events: explicit_completion | suspected_root_loss | observation_error | observation_timeout
```

Implementations: `ExternalRootLifecycleObserver` (live), `SyntheticRootLifecycleObserver`
(selftests), and a substitute observer that drives the identical contract — the Slice 4B
substitution surface.

`pending` is a polling sentinel, explicitly **not** an event class.

### Epoch scoping

exp68's root consumes a bare mechanical `root.done` trigger carrying no epoch. Slice 4A
therefore publishes its own epoch-scoped `root.completion` witness, which is what the classifier
reads; `root.done` is written afterwards only to drive exp68's finalize path. A prior-epoch
`root.completion` or a never-advancing `root.alive` can never satisfy the current epoch.

## 4. Classifier allow-list and blindness proof

The classifier sees exactly nine observable fields and nothing else:

```text
epoch_id · completion_witness_present · completion_witness_epoch_match · expected_refresh_s
observed_silence_s · classification_bound_s · root_pid_alive · witness_read_error
observation_deadline_exceeded
```

Blindness is **structural**: `_assert_root_blind` raises `RootBlindnessViolation` on any field
outside the allow-list; `collect_root_evidence` has a fixed positional signature; and the root
kill record lives at `markers.json arms.loss.injection_record_private`, which the observer never
reads. Selftests prove refusal for `signal`, `injected`, `controller_intent`, `victim`,
`root_was_killed`, and `arm`.

**Documented policy:** a dead root pid does **not** shortcut the bound. `root_pid_alive` is
recorded as corroboration only; suspicion requires `observed_silence_s >= classification_bound_s`.
The boundary is inclusive at exactly the bound, and that is selftested at
`bound - 0.001`, `bound`, and `bound + 0.001`.

**Monotonic time:** the witness token is `(st_mtime_ns, st_size, st_ino)`. A *changed* token
stamps `last_advance_monotonic = time.monotonic()`. Filesystem mtime only shows *that* the
witness advanced; it never determines the interval.

## 5. Results

Selftests: **115/115**, covering all 27 required cases — completion, graceful departure after
completion, sub-bound silence, beyond-bound silence, the pid-death policy, prior-epoch
`root.done` / `root.alive` rejection, stale-mtime rejection, blindness, duplicate rejection,
wrong-root and wrong-epoch rejection, all five actor-observation categories, one actor timing
out without blocking the other, publication-before-verification rejection, no dispatch after
suspected loss, no graceful markers on the poisoned arm, `root.final` required only for the
normal arm, orphan detection, PID-reuse discrimination, cleanup after intermediate failure,
observer substitution, clean off-cluster skip, and no Slurm submission from selftest or local
mode.

### Local phase

Two passing runs, `20260719T164253Z` and `20260719T164403Z`, each **30 gate groups / 193 checks,
zero failed gates**, `explicit_completion` and `suspected_root_loss` correctly assigned.

### Two-node hardware phase

Slurm job **173798** on **medusa00 + medusa01** (partition `medusa`, `-N 2 --exclusive`, elapsed
00:02:34). Two live runs, **`20260719T184648Z`** and **`20260719T184749Z`**, both `overall=pass`
with **31 gate groups / 205 checks and zero failed gates**. Disjoint port blocks per invocation
(8011–8023 / 8041–8053, Ray heads 6529 / 6549) and per arm; a fresh root process, actor set and
Ray cluster in every arm. One classifier fingerprint `774ebdc6147bad94220f…` across all four
arms.

**Normal-control arm →`explicit_completion`.** Evidence: epoch-matched completion witness
present. Completion was published strictly after final-result verification
(`completion_after_result_verification = true`); a pre-verification attempt was rejected, and a
duplicate publication was rejected. Both connectors observed `explicit_completion` for the exact
epoch; both left through the validated stop sequence; the root reached
`final_membership = 1`, wrote `root.final`, and exited `finalized_clean`.

**Unexpected-root-loss arm →`suspected_root_loss`.** All ten root-injection preconditions passed
before the kill. Evidence: no epoch-matched completion, monotonic silence reaching the bound.

| Run | bound | observed silence | classification elapsed | pre-bound probe |
|---|---|---|---|---|
| `20260719T184648Z` | 5.0 s | 5.0131 s | 5.0131 s | `observation_timeout` at 2.008 s, silence 0.000 s |
| `20260719T184749Z` | 5.0 s | 5.0142 s | 5.0143 s | `observation_timeout` at 2.008 s, silence 0.100 s |

The pre-bound probe is the negative gate: a **healthy, refreshing root observed for 2 s produced
no suspicion at all**, only an observation timeout.

Placement and hosting, run `20260719T184648Z`:

| Arm | Role | PID | HPX locality | Hostname |
|---|---|---|---|---|
| normal | root | 354876 | 0 | medusa00 |
| normal | actor A | 352691 | 1 | medusa00.rostam.cct.lsu.edu |
| normal | actor B | 1784275 | 2 | medusa01.rostam.cct.lsu.edu |
| loss | root | 354959 | 0 | medusa00 |
| loss | actor A | 352690 | 1 | medusa00.rostam.cct.lsu.edu |
| loss | actor B | 1784635 | 2 | medusa01.rostam.cct.lsu.edu |

**Actor-side post-loss observations:** both actors returned `call_timeout` in every local and
hardware run (bound 15 s). This is the substantive finding of the loss arm — after the root
dies, actor-hosted HPX calls **block** rather than fail fast, and only the bound prevents the
controller from being stranded. It is recorded as evidence, not required to take this form, and
it is **not generalized beyond the tested HPX build and topology**.

**Poisoned-island cleanup:** no `root.done`, no completion witness, `root.final` absent as
expected, root process gone, both actors removed and both worker PIDs confirmed gone. Post-run
orphan sweep: `none` on both nodes.

### Copy-back and hash verification

Two-stage protocol, correcting the Slice 2/3 approach:

1. **In-job manifest** (`hardware_evidence_manifest_173798.json`) covers only closed, stable
   artifacts, and declares `post_job_hash_required: ["exp70s4xn_173798.out"]`. Self-hash matches
   its sidecar; **37/37 closed artifacts exact**.
2. **Post-job manifest** (`post_job_hash_manifest_173798.json`), generated on the analysis host
   after `sbatch --wait` returned, hashes the completed Slurm log; cross-checked independently
   against the remote file.

**Combined final verification: 38 artifacts, 38 verified, 0 mismatched, 0 missing — exact.** No
prefix-equivalence reasoning is used anywhere in Slice 4's validation protocol. Details in
`copyback_verification.txt`; curated summary in `slice4_curated_evidence.json`.

> Slice 2A and Slice 3A remain hash-verified by the earlier prefix method: their in-job manifests
> hashed the Slurm log while the job was still appending to it, and the difference was verified
> as an exact byte-prefix with matching final remote and local hashes. Their raw manifests are
> left untouched; Slice 4A's two-stage protocol is the one to reuse going forward.

## 6. Supported claim

> In a two-node actor-hosted HPX island, the external root-lifecycle backend distinguished
> explicit root completion from bounded suspicion after unexpected loss of the separately
> supervised work-free root, while the supervisor discarded the poisoned island.

## 7. Non-claims

* No HPX-native root-loss notification.
* No HPX-native heartbeat.
* No authoritative failure certainty — the loss verdict is bounded suspicion.
* No transparent recovery.
* No automatic AGAS repair.
* No partial-island continuation.
* No performance claim.

## Files

* `run_slice4.py` — instrument: selftest, local live phase, cross-node phase, curation.
* `exp70_slice4_crossnode.sbatch` — two-node Rostam launcher (never rebuilds exp68).
* `slice4_curated_evidence.json` — curated local + hardware evidence with hashes.
* `_exp70_slice4_runs/` — ignored raw run area (see `.gitignore`).
