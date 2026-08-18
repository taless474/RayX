# exp70 Slice 3 — connector loss/departure events for an actor-hosted HPX island

This directory holds two related but separate slices:

* **Slice 3A** (below) — complete, hardware-verified: an **external**, application-computed,
  injection-blind classifier for graceful-vs-lost connector departure, built when no upstream
  HPX API classified this natively.
* **Slice 3B** (`native/`, `run_slice3b.py`) — added 2026-08-18, after upstream HPX landed
  `components/supervision_dispatch` + `hpx::force_disconnect` (issues #7390/#7441, merged PR
  #7447): a **native** validation of the same silent-crash scenario against the real upstream
  API. See "Slice 3B" below. Slice 3A's evidence, claim, and non-claims are **unchanged** — 3B
  does not supersede them, it tests whether the runtime gap 3A described has since been closed.

## Slice 3A — classified connector loss/departure events for an actor-hosted HPX island

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

## Slice 3B — native validation against upstream supervision_dispatch + force_disconnect

**Status (2026-08-18): native harness implemented, selftested, and run on Rostam hardware
(medusa00, job 185466–185474). Build/topology/registration validated; the run is BLOCKED at
`discover_and_join()` before any of the 16 gates could be exercised — see "Hardware run result"
below.** Added
after hkaiser's status comment on HPX issue #7390 reported new upstream work (issue #7441,
merged PR #7447 "Adding hpx::force_disconnect") built on top of the `components/supervision_dispatch`
component landed since Slice 3A's 07-19 assessment. See
[`native_backend_gap_matrix.md`](../native_backend_gap_matrix.md) for the gate-by-gate mapping
and exactly what remains open even with this harness.

### Question

Can current upstream HPX autonomously classify and fence the silent crash of a late-connected
connector locality, after which the surviving root can use `hpx::force_disconnect` to clean up
that failed locality and permit a replacement connector to join successfully?

### Topology (Slice 3's, reused unmodified)

One standalone, separately supervised, work-free root locality
(`native/root_supervised.cpp`) plus Ray-actor-hosted connect-mode connectors
(`native/connector_ext.cpp`, the exp66/67/68 in-Ray-actor hosting mechanism, unchanged). Single
scenario, not two arms — Slice 3A already owns the graceful-vs-loss comparison; 3B tests only the
silent-crash-then-recover path:

```text
root starts -> connector A and B join late, call hpx::supervision::init() ->
root discover_and_join()s both -> useful plain HPX work verified on A and B ->
B is SIGKILLed (no disconnect(), no graceful shutdown, no app-authored event::failed) ->
root polls check_admission()/query_state() until rejected_fenced is observed ->
root explicitly calls hpx::force_disconnect(B) -> effect verified (resolve fails, membership
shrinks, a plain post-force_disconnect dispatch fails, and connector A independently confirms
B's old locality is unreachable) -> replacement connector C joins -> C's locality/epoch proven
distinct from B's -> B's old (locality, epoch) pair still rejected, never confused with C
```

### Three responsibilities kept observably distinct

1. **HPX classifies the crash, not the application.** `root_supervised.cpp` never calls
   `hpx::supervision::publish_event(..., event::failed, ...)` anywhere — classification is
   entirely `components/supervision_dispatch`'s own `failure_detection_loop()` background sweep,
   started automatically by `hpx::supervision::init()`.
2. **supervision_dispatch fences the failed incarnation**, observed via
   `hpx::supervision::check_admission()` — a pure local latch read with no cross-binary
   action-registration risk. The templated `dispatch_work<Action>()`/`fenced_action<>` path is
   also attempted, but recorded only as a **non-gating diagnostic**: whether HPX guarantees a
   fenced action instantiated only in the root binary is registered in a separately-compiled
   connector binary that never itself calls `dispatch_work()` was not independently verified
   against upstream (see the file comment in `root_supervised.cpp`).
3. **Root explicitly invokes `hpx::force_disconnect`**, only after step 2 is observed — never
   automatically. Upstream does not wire fencing to `force_disconnect`; this experiment does not
   imply otherwise.

### Gates (16 independent fields, `run_slice3b.py:GATE_FIELDS` / `eval_gates()`)

```text
connector_late_join_proven · pre_crash_work_ok · hard_crash_used ·
graceful_disconnect_used_is_false · application_failed_event_publish_count_is_zero ·
runtime_failure_classification_observed · failed_epoch_or_incarnation_identified ·
stale_incarnation_fenced · fenced_outcome_is_specific · force_disconnect_invoked ·
force_disconnect_completed · force_disconnect_effect_observed · replacement_joined ·
replacement_incarnation_distinct · replacement_work_ok ·
stale_incarnation_not_confused_with_replacement
```

None are collapsed into one boolean; the selftest proves each gate is independently wired by
flipping one underlying signal at a time and checking exactly that gate (and only that gate's
downstream `failure_class`) breaks.

### Build requirement (explicit, not assumed)

`native/CMakeLists.txt` requires `-DHPX_WITH_SUPERVISION=ON` (which also enables
`HPX_HAVE_FORCE_DISCONNECT`) and refuses to configure without it — this is a hard requirement,
not a skip, because the whole point of the slice is exercising that capability. The local macOS
HPX build used elsewhere in this repo predates the supervision merge and does not have it; a
hardware run needs a fresh HPX checkout/build on Rostam with the flag enabled first. Both
`root.started` and `root_result.json` also record `hpx_have_supervision`/
`hpx_have_force_disconnect` as observed at compile time, so a result file is self-describing
about which capability it actually exercised.

### Results

**Selftest: 12/12 checks, 0 failures** (pure logic/schema; no Ray, no HPX) — a synthetic
all-pass marker set rolls up correctly, six independent signal flips each break exactly their
own gate and the overall pass, a fully-graceful departure independently fails both
`hard_crash_used` and `graceful_disconnect_used_is_false`, a missing-classification case names
the specific `classification_not_observed` failure class rather than a generic one, and
`GATE_FIELDS` stays in lockstep with `eval_gates()`.

### Hardware run result (2026-08-18, Rostam job 185466–185474, medusa00)

**Verdict: BLOCKED at discovery, not PASS.** HPX (pinned commit `7b88345b6c72dfe1dabce1c0398e021f5ca55a4f`)
was rebuilt from scratch with `-DHPX_WITH_SUPERVISION=ON -DHPX_WITH_FORCE_DISCONNECT=ON` into an
isolated install (`/work/bitayekrang/apps/hpx-supervision-install`, untouched canonical
`hpx-master-20bc3d4b` install). The native harness built and linked against it (`ldd` confirmed
both binaries load `libhpx_supervision_dispatch.so.2` from the new install). Root started;
connectors A and B each hosted an HPX connect-mode locality in-process inside a Ray actor,
reached the expected locality ids (1, 2), and each independently completed
`hpx::supervision::init()` successfully (`ok=true`, correct epoch).

The run consistently failed at the next step: `hpx::supervision::discover_and_join()` found
**zero peers**, from **both directions**, across five consecutive attempts (jobs 185469, 185470,
185471, 185472, 185474) including one with a 30s bounded retry loop. Diagnostics isolated this
precisely:

* `hpx::find_remote_localities()` correctly reported 2 remote localities from root's side.
* Each connector's own `supervision_dispatch` registry name (`"/" + locality_id +
  "/supervision_dispatch/registry"`) self-resolved successfully via a raw
  `hpx::agas::resolve_name()` call from its own locality (`self_resolve_ok=true` for both).
* The SAME raw `resolve_name()` call for those exact names, issued from root (a different
  locality), returned "not found" with **no error code** — a clean miss, not a timeout.
* Connector A independently running `discover_and_join()` itself (the
  connector-discovers-root/peer direction, mirroring upstream's own
  `late_component_worker.cpp` example) **also** found zero peers.

This rules out a client-side race, a naming-convention mismatch (confirmed identical in
`registry::register_name()` and `discover_peers()`), and direction-specificity. It is consistent
with a cross-locality AGAS symbol-visibility gap specific to
`components/supervision_dispatch`'s discovery mechanism in a topology where distinct localities
run in **independently-launched OS processes** (one standalone root binary + Ray-actor-hosted,
separately-compiled connector processes) — a topology upstream's own example family
(`late_component_launcher`/`late_component_worker`) is *built* for but is not wired into
`add_hpx_example_test()` (i.e., not exercised by upstream's own CI), unlike the same-binary
`LOCALITIES 2` tests (`plain_worker`, `component_worker`) that likely do pass. Per the debugging
protocol for this run, no workaround or weaker discovery oracle was substituted; none of the 16
Slice 3B gates were exercised because the run never got past this point. Raw artifacts, all
seven run directories, and eight Rostam job logs are preserved under `_exp70_slice3b_runs/`
(gitignored, kept locally) for independent review.

### Rostam build/run (not executed this session)

```bash
cd /work/bitayekrang/RayX
module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
# 1. HPX must be (re)built with supervision enabled -- this repo's existing Rostam HPX build
#    predates the merge, same as the local macOS one.
cmake -S <hpx-src> -B <hpx-build> -DHPX_WITH_SUPERVISION=ON -DHPX_WITH_DISTRIBUTED_RUNTIME=ON \
      -DHPX_WITH_NETWORKING=ON -DHPX_WITH_PARCELPORT_TCP=ON -DCMAKE_BUILD_TYPE=Release
cmake --build <hpx-build> -j"$(nproc)" --target install
# 2. configure/build exp70 Slice 3B against that install
cmake -S experiments/70_hpx_locality_supervision/slice3_connector_loss_event/native \
      -B experiments/70_hpx_locality_supervision/slice3_connector_loss_event/native/build \
      -G Ninja -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
      -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)"
cmake --build experiments/70_hpx_locality_supervision/slice3_connector_loss_event/native/build
# 3. run
source /work/bitayekrang/venvs/rayx-a2b/bin/activate
python3 experiments/70_hpx_locality_supervision/slice3_connector_loss_event/run_slice3b.py \
        --selftest
python3 experiments/70_hpx_locality_supervision/slice3_connector_loss_event/run_slice3b.py \
        --phase local
```

### Non-claims (Slice 3B specifically, in addition to Slice 3A's own)

* Not autonomous/automatic recovery — `force_disconnect` is an explicit, deliberate root-side
  call made only after fencing is observed, never triggered by HPX itself.
* Not a claim that fencing and `force_disconnect` are wired together upstream.
* Not console/root-loss evidence (see Slice 4B, which remains blocked for a structural reason:
  `force_disconnect` cannot target the console).
* Does not supersede Slice 3A's external, application-contract evidence.
* No performance, Ray-vs-HPX, or general-fabric claim. Not production API.

## Files

* `run_slice3.py` — Slice 3A instrument: selftest, local live phase, cross-node phase, curation.
* `exp70_slice3_crossnode.sbatch` — two-node Rostam launcher for Slice 3A (never rebuilds exp68).
* `slice3_curated_evidence.json` — curated Slice 3A local + hardware evidence with hashes.
* `_exp70_slice3_runs/` — ignored raw Slice 3A run area (see `.gitignore`).
* `native/root_supervised.cpp`, `native/connector_ext.cpp`, `native/CMakeLists.txt` — Slice 3B's
  native harness against upstream `hpx::supervision`/`hpx::force_disconnect`.
* `run_slice3b.py` — Slice 3B instrument: selftest (12/12) and local live phase (requires an HPX
  build with `HPX_WITH_SUPERVISION=ON`; see the Rostam commands above).
