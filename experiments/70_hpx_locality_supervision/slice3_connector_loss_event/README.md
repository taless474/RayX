# exp70 Slice 3A — classified connector loss/departure events for an actor-hosted HPX island

**Status:** complete — local phase and two-node hardware phase both green. Mechanism /
application-contract evidence only. Not a performance experiment, not a Ray comparison, not
production code, not a shipped RayX API.

## 1. Question

Can a connector's departure from the actor-hosted HPX island be surfaced as an explicit,
**classified** lifecycle event — graceful departure vs unexpected loss — decided only from
observable evidence, with **no HPX-native failure detection**?

Slice 2A (`../slice2_explicit_completion/`) closed the first half of the two-item lifecycle
contract: explicit normal completion. Slice 3A closes the second half: a classified
departure/loss event. The surface stays backend-neutral so a future HPX-native backend
(Slice 3B) could implement it without changing the state machine, the classifier, or the gates.

## 2. Contract (backend-neutral)

Two-method surface: `publish_event(epoch, connector, kind, evidence_digest, classification)` /
`observe_events(epoch, observer, bound)`.

1. Each connector that joins produces a `JOINED` lifecycle event.
2. A departing connector produces **exactly one** terminal event.
3. The terminal event carries a classification: `graceful_departure` or `unexpected_loss`.
4. The classification is computed **injection-blind** from observable evidence only.
5. Publication happens within a bounded interval after the departure is observed.
6. The surviving connector observes the terminal event for the **exact island epoch**; events
   from prior epochs are rejected and can never satisfy an observation.
7. No terminal event is published for a connector that has not departed.
8. Teardown: the graceful arm finalizes through the root; the **loss arm treats the island as
   poisoned** and performs external whole-island teardown with no graceful HPX attempt
   (exp51 / exp53 / Slice 1 lineage).
9. No process remains (experiment-scoped owned sweep with PID-reuse discrimination).

## 3. State machine (per arm)

```text
STARTING -> READY -> WORK_VERIFIED -> DEPARTED -> EVIDENCE_COLLECTED -> EVENT_PUBLISHED
-> EVENT_OBSERVED -> TORN_DOWN -> FINALIZED
```

Invalid transitions raise deterministically and are recorded. Application dispatch is allowed
only in `READY`.

## 4. The injection-blind classifier

This is the point of the slice. `classify_departure(evidence)` sees a **fixed allow-list** of
six observable fields and nothing else:

```text
connector · pid_gone · clean_disconnect_recorded · stop_rc · actor_call_raised · actor_error_type
```

Blindness is **structural, not conventional**: `_assert_blind` raises `BlindnessViolation` if
the evidence mapping carries any key outside the allow-list, `collect_departure_evidence` has a
fixed positional signature with no parameter through which intent could be smuggled, and the
injection record is stored on a separate branch of the marker tree (`injection_record_private`)
that the collector never reads. Selftests prove the classifier refuses evidence carrying
`injected`, `arm`, `signal`, or `controller_intent`.

Decision rule — the event describes the **HPX connector leaving**, not the hosting process
dying, and that distinction is the discriminator:

| Label | Rule |
|---|---|
| `graceful_departure` | a validated disconnect/stop handshake was recorded **and** no post-departure call to the host failed |
| `unexpected_loss` | the hosting PID is gone **and** no clean disconnect was recorded **and** a post-departure call failed |
| `indeterminate` | anything else — recorded verbatim, never silently coerced |

The two positive labels are complementary on both `clean_disconnect_recorded` and
`actor_call_raised`, so they cannot fire together. `root_leave_observed` is deliberately **not**
an input: exp68's work-free root reports a graceful leave only when membership returns to 1,
i.e. after *all* connectors have gone, so it is unavailable when a single connector departs. It
is recorded per arm as corroboration, outside the evidence.

## 5. Two arms, one classifier

Each live run executes both arms, each a **fresh island epoch** (fresh boot directory, disjoint
port block, fresh actor and process identities; Slice 1 epoch-isolation discipline):

* **graceful** — connector B leaves through the validated disconnect/stop sequence. Its hosting
  Ray actor is deliberately left alive; its continued answerability is the graceful signal.
* **loss** — connector B is lost to a controller-initiated `SIGKILL` of the **cross-checked**
  actor worker PID (exp53 / Slice 1 lineage). No stop is ever dispatched to it.

Both arms feed the same collector and the same classifier, whose source fingerprint is recorded
and checked stable across the run (`classifier_discriminates.single_classifier_used`).

Connector B is the departing connector in both arms, and is **remote by construction** in the
cross-node phase. Workload `cross_both` (V=64, split=32, k=6, seed=1) is verified bit-exactly
against the imported exp68 oracle in **both** arms before any departure.

## 6. Results

Selftests: **96/96** — classifier blindness and refusal cases, ambiguity never coerced, state
machine, event-contract idempotence/eligibility, epoch scoping, both backends, backend
substitution, owned-process sweep with PID-reuse discrimination, arm isolation, cross-node
placement rejection cases, and off-cluster preflight discipline.

### Local phase

Two passing local runs, `20260719T145940Z` and `20260719T150004Z`, each **27 gate groups / 164
checks, zero failed gates**, `graceful_departure` and `unexpected_loss` correctly assigned.
Curated at `_exp70_slice3_runs/curated_local_evidence/`.

### Two-node hardware phase

Slurm job **173797** on **medusa06 + medusa07** (partition `medusa`, `-N 2 --exclusive`, elapsed
00:01:58). Topology: node A (medusa06, 10.42.5.36) hosts the batch step, controller, Ray head,
work-free HPX root and actor A; node B (medusa07, 10.42.5.37) hosts actor B. Two live runs,
**`20260719T170156Z`** and **`20260719T170216Z`**, both `overall=pass` with **28 gate groups /
176 checks and zero failed gates**. Each run used a disjoint port block (7951–7963 / 7981–7993,
Ray head 6489 / 6509), and within each run the two arms used disjoint blocks again.

Both runs, both arms — one classifier fingerprint `9a5d1ec8edd0116612d1cc4f…` across all four:

| Arm | Evidence | Classification | Publication latency | Survivor observation |
|---|---|---|---|---|
| graceful | `clean_disconnect_recorded=true`, `stop_rc=0`, `actor_call_raised=false`, `pid_gone=false` | `graceful_departure` | 3.236 s / 3.242 s | observed, epoch-matched, 0.3 ms |
| loss | `clean_disconnect_recorded=false`, `actor_call_raised=true` (`ActorDiedError`), `pid_gone=true` | `unexpected_loss` | 0.083 s / 0.068 s | observed, epoch-matched, 0.3 ms |

Placement and hosting, run `20260719T170156Z`:

| Arm | Connector | PID | HPX locality | Hostname |
|---|---|---|---|---|
| graceful | A | 3361344 | 1 | medusa06.rostam.cct.lsu.edu |
| graceful | B | 2510488 | 2 | medusa07.rostam.cct.lsu.edu |
| loss | A | 3361337 | 1 | medusa06.rostam.cct.lsu.edu |
| loss | B | 2511124 | 2 | medusa07.rostam.cct.lsu.edu |

Teardown behaved differently per arm exactly as the contract requires: the graceful arm's root
reached `final_membership=1`, recorded `leave_observed=true` and exited `finalized_clean`; the
loss arm used `external_whole_island` teardown with `graceful_attempted=false`. Post-run orphan
sweep reported `none` on both nodes in both runs.

**Observational only:** after the loss, the survivor's bounded membership probe reported
`membership_stale` in every local and hardware run — consistent with Slice 1. This is recorded
and categorized but is **not a gate**; a shrinking membership would be a finding, not a failure.

No timing or performance claim is made from these runs. The publication-latency figures are
contract-bound checks (bound 15 s), not measurements of anything.

### Copy-back and hash verification

Copied to `_exp70_slice3_runs/rostam_copyback_20260719T220347Z/`. Manifest self-hash matches its
sidecar. **35 of 36 listed artifacts hash-verified exactly, 0 missing.** The 36th is the Slurm
job log, which the in-job manifest step necessarily hashes before the job finishes writing it;
verified as an exact byte-prefix (426 appended bytes are the manifest step's own output), with
identical final remote and local hashes. Details in `copyback_verification.txt`; curated summary
in `slice3_curated_evidence.json`.

## 7. Supported claim

> In a two-node actor-hosted HPX island, a departing connector produced exactly one terminal
> lifecycle event whose graceful-vs-lost classification was computed injection-blind from
> observable evidence, was published within a bounded interval, and was observed by the
> surviving connector for the exact island epoch.

## 8. Non-claims

The result does not demonstrate: HPX-native loss detection; an HPX-native heartbeat; HPX-native
eviction or membership repair; recovery or failover; runtime-level enforcement; any performance
improvement. **The classification is computed by the supervisor from observable evidence; HPX is
not claimed to report it.** The post-departure membership observation is not evidence of
detection.

## Files

* `run_slice3.py` — instrument: selftest, local live phase, cross-node phase, curation.
* `exp70_slice3_crossnode.sbatch` — two-node Rostam launcher (never rebuilds exp68).
* `slice3_curated_evidence.json` — curated local + hardware evidence with hashes.
* `_exp70_slice3_runs/` — ignored raw run area (see `.gitignore`).
