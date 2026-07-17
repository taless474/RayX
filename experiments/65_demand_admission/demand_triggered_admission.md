# exp65 — demand-triggered connect-mode admission (loopback + cross-node slices)

## Executive summary

**Problem.** Every prior distributed experiment (exp49–64) used connect-mode late
admission in an orchestrated assemble-then-measure pattern: connectors were launched at
orchestration start and roots waited for them before doing anything. For a Ray-supervised
island design, the interesting ordering is the opposite: a root that is already alive and
doing verified work, admitting a locality only **when demand arises**.

**Answer.** Demand-ordered admission works on this build: the root starts alone, completes
verified local HPX work with zero connectors in existence, admits one connector only after
an external demand event, discovers it by membership set-difference (no predetermined
count), serves one bounded oracle-verified remote action, observes a graceful leave, keeps
working, and finalizes cleanly. A no-demand control proves the same root finalizes cleanly
having never seen a connector. Both arms passed **3/3 repetitions** on the single-node
loopback slice (macOS, AppleClang, HPX 1.11 networking build) and **3/3** on the Rostam
two-node cross-node slice (medusa00 root → medusa01 connector, TCP `10.42.5.x`).

**Limitations.** Structural mechanism evidence only; Ray-free; one connector and one
demand event; no membership change while remote work is in flight; graceful leave only; the
root still pre-declares willingness via `--hpx:expect-connecting-localities`; all recorded
durations are observational and never gate inputs. Job IDs and artifacts are listed in the
cross-node section below.

## Question

Can the connect-mode HPX island run **demand-ordered admission** on this build: the root
starts **alone** (no `--hpx:localities`, no expected connector count anywhere in code or
argv), proves **local** HPX progress while zero connectors exist, keeps doing local liveness
work through a deliberate zero-connector dwell, and only **after** the controlling layer's
demand event is one connect-mode locality launched, discovered by membership
**set-difference**, served **one** bounded oracle-verifiable remote action, and released —
after which the root proves it is **still operational** and finalizes cleanly? A no-demand
control arm proves the same root finalizes cleanly with **zero connectors ever joining**.

## Why this is new relative to exp49–64

The evidence audit behind this experiment found that every prior distributed experiment
(exp49 Phase 2 → exp64) used connect-mode **late admission**, but always in an
**orchestrated assemble-then-measure** pattern: connectors were launched at orchestration
start, and roots either polled for the joiner immediately (exp49/57/58) or failed closed on
`await_remotes(expected_count)` before any dispatch (exp62–64). No experiment had proven the
ordering *root operational → local work → demand event → admission*. exp65 proves exactly
that ordering, and nothing more.

## Non-claims / claim fence

No Ray and no Ray actors (the controller is a plain-Python orchestrator). No
HPX-inside-Ray-worker result (gate-doc Level 4 stays gated). Not Ray autoscaling. No
elasticity-under-load result: one connector, one demand event, no membership change while
remote work is in flight. No concurrent membership churn. No failure-recovery result
(graceful leave only; the exp50/51 ungraceful-loss findings are untouched). No
lazy-TCP-socket-establishment result: this experiment says nothing about when parcelport
sockets open (that is a separate, not-yet-built diagnostic). No performance comparison, no
production / public API, no `rayx.runtime` change. The cross-node slice licenses exactly a
**two-node** reproduction (medusa00/medusa01); there is no general multi-node or scale
claim. **All recorded durations are observational only and never participate in pass/fail
gates.**

**Honest structural caveat.** The root still boots with
`--hpx:expect-connecting-localities` — exp49 proved connect mode refuses late joiners
without it. Demand-driven admission therefore operates **within a pre-declared willingness**
to accept connectors: exp65 removes the predetermined *count* and the assemble-before-measure
*gate* (`static_locality_count_used=false`, `expected_connector_count_used=false`), not the
boolean *intent* flag (`admission_intent_flag_required=true`).

## Design

One standalone binary (`demand_admission_spike`, isolated CMake, **not** wired into
`_rayx`), two roles; one plain-Python runner (`run_exp65_demand_admission.py`), two arms,
each repeated 3×. Lineage is deliberate reuse of proven patterns:

* oracle action `(x ^ 0x52415958) + (executing_locality << 1)`, console root +
  `hpx::start`/`runtime_mode::connect` connector, `hpx::post([]{hpx::disconnect();});
  hpx::stop();` teardown — exp49;
* bounded dispatch classification (`returned | timed_out | threw`; a timed-out future is
  never `.get()`-ed), set-difference discovery against a pre-demand membership baseline —
  exp50;
* `root.alive` heartbeat + `root.done` completion sentinel; the connector `serve-timeout`
  is a **deadman for root silence only**, never the normal release path — exp63.

### Lifecycle and event ordering (demand arm)

```
root:       INIT → root.ready{membership=[0]} → local phase (K=5 oracle calls, all local)
            → local_phase.done{membership=[0]} → available-wait (200 ms ticks: heartbeat
              root.alive + one local liveness oracle call per tick; NO count gate)
controller:      … VERIFY_ALONE → DWELL (3 s, connector process does not exist) →
                 write demand.issued → Popen connector
connector:                          connector.spawned → hpx::start(connect) →
                                    connector.joined{locality_id}
root:       set-difference vs baseline [0] detects new id → admission_detected →
            heartbeat → ONE bounded remote dispatch (100 ms wait_for slices) → served →
            root.done (completion sentinel)
connector:  sees root.done → post(disconnect)+stop → connector.disconnected{clean,
            shutdown_reason=root_completion_signal}
root:       id-specific departure wait → leave_observed → one more local oracle call →
            final_local.done → root_result.json → hpx::finalize()
```

Load-bearing ordering proof (same-host wall clocks plus process existence): the connector
process **is not created** until `demand.issued` is on disk; gates check
`local_phase.done < demand.issued < connector.spawned < admission_detected`, and that at
least one liveness tick precedes the demand event.

The no-demand arm is the identical root; the controller never launches a connector and
writes `no_demand.finish` after the dwell; the root must exit its wait loop on that marker,
run the final local oracle call, and finalize cleanly with `max_localities_ever == 1`.

### Instrumentation

Every marker carries role, pid, `wall_ms`, `steady_ms`. Root records membership snapshots
(ids) at startup, after the pre-demand local phase (the set-difference baseline), at
admission, and after leave; local/liveness/final oracle inputs, outputs, expected values and
executing locality ids; full argv (audited for `--hpx:localities`); dispatch classification
with `detected_before_bound` / `wait_slices_used`; and the structural fields
`remote_work_started_before_admission=false` (by construction: the only remote dispatch site
runs strictly after set-difference admission), `admission_detected`, `max_localities_ever`.
The connector records process-start attestation (written **before** `hpx::start`), joined
locality id, and `shutdown_reason ∈ {root_completion_signal, serve_timeout_expired, error,
unknown}`. The runner records the demand event, Popen timing,
`connector_existed_before_demand`, exp50-style exit-path classification, a
`pgrep`-based no-orphans check, and the aggregate structural flags.

## Result — local loopback slice (this machine; 3 reps per arm; job-free local run)

Demand arm — all 12 gates pass in every rep:

| check (gate) | rep 1 | rep 2 | rep 3 |
|---|---|---|---|
| root started alone (`membership=[0]`) | pass | pass | pass |
| local progress before demand (5/5 local oracle + liveness tick < demand) | pass | pass | pass |
| connector absent before demand | pass | pass | pass |
| demand precedes creation and admission | pass | pass | pass |
| discovery without count gate (set-difference, baseline `[0]` → new id 1) | pass | pass | pass |
| remote oracle on connector (`returned`, match, `executed_on=1≠0`) | pass | pass | pass |
| connector left gracefully (`root_completion_signal`, clean, rc 0) | pass | pass | pass |
| root operational after leave (leave observed + final local oracle ok) | pass | pass | pass |
| clean exits (root `finalized_clean`, no SIGKILL) | pass | pass | pass |
| no orphans | pass | pass | pass |
| argv audit: no `--hpx:localities` | pass | pass | pass |
| no expected-count startup/dispatch gate | pass | pass | pass |

Observational durations (never gate inputs): demand→spawn ≈ 31–32 ms, demand→admission ≈
189–202 ms, admission→served ≈ 100 ms (first 100 ms wait slice), liveness ticks before
admission = 17 per rep.

No-demand control arm — all 7 gates pass in every rep: root alone, 5/5 local oracle, 17
liveness ticks, `max_localities_ever = 1`, `admission_detected = false`,
`no_demand_finish_seen = true`, root `finalized_clean` rc 0, no orphans, argv audit clean.

Aggregate: `demand_admission_aggregate.json` (`overall = "pass"`, structural flags
`static_locality_count_used=false`, `expected_connector_count_used=false`,
`admission_intent_flag_required=true`; per-rep `connector_existed_before_demand=false`,
`remote_work_started_before_admission=false`).

### Observation-only side finding: timed-wait deadline wake

During validation, a **single** `future::wait_for(bound)` on the dispatched action future
consistently returned only at its **full** bound with the future already ready
(admission→served equaled the bound exactly: 15000 ms at bound 15 s, 2000 ms at a 2 s
control probe; oracle matched in both). The dispatch wait was therefore implemented as
100 ms `wait_for` slices (still strictly bounded; a timed-out future is still never
`.get()`-ed), after which the action completed within the **first** slice in every rep.
This is an observation about timed-future-wait wake behavior on this build/configuration
only — not a latency measurement, not a general HPX claim, and not a gate input. It is
consistent with exp50's choice to record only detection booleans rather than
wall-time-to-detection.

## Cross-node slice (Rostam, medusa00 → medusa01, Slurm job 170014)

The demand-ordered admission result reproduces across two real nodes over the TCP
parcelport. Same binary, oracle, event ordering, gate logic, and heartbeat /
completion-sentinel lifetime; the additions are only Slurm/cross-node orchestration
(`--phase rostam-cross-node` in the same runner) plus placement / subnet / completeness
gates. The loopback slice above is unchanged and remains the baseline evidence.

**Shape.** Controller + root on `medusa00`; the connector is `srun`-launched on
`medusa01` (`--overlap --cpu-bind=none`, the proven exp63/64 connector-launch shape)
**strictly after** the controller records `demand.issued`. Explicit numeric endpoints pin
the transport to the `10.42.5.` subnet: root `--hpx:hpx=10.42.5.30:<p>` (also the AGAS
endpoint), connector `--hpx:hpx=10.42.5.31:<p'>` with `--hpx:agas=10.42.5.30:<p>`. A
process cannot bind an address that is not local to its host, so endpoint binding plus
per-marker hostname/pid attestation (every marker now records `host`) is the placement
proof. Fresh port pairs per rep; `--hpx:ignore-batch-env` island-wide; still no
`--hpx:localities` and no expected-count gate (same argv audit).

**Same-clock gate design.** The two orderings that would otherwise compare wall clocks
across nodes are gated on the root-node clock only: `demand.issued` ≤ connector `srun`
Popen < admission (controller and root share medusa00, and the connector process cannot
exist before its `srun` Popen). The connector's own `spawned.wall_ms` is recorded as
attestation but never compared against a root-node clock. This mattered: the observational
clock-skew probe estimated medusa01's wall clock ≈ 1.3 s behind medusa00's, so cross-node
wall-clock ordering gates would have been unsound.

**Result (job 170014, `medusa[00-01]`, COMPLETED, exit 0:0).** Demand arm: **all 19 gates
pass in 3/3 reps** — the twelve loopback gates (with the two same-clock replacements) plus
`root_on_root_node`, `connector_on_connector_node`, `cross_node_placement`,
`subnet_pinned_both_endpoints`, `slurm_allocation_recorded`, `remote_orphan_check_ran`,
and `evidence_fields_complete`. No-demand control: **all 12 gates pass in 3/3 reps**
(`max_localities_ever = 1`, `admission_detected = false`, root finalizes cleanly alone on
medusa00; remote orphan check clean on medusa01). Per-rep placement evidence: root
attested `medusa00.rostam.cct.lsu.edu` (pids 2345393 / 2345452 / 2345500), connector
attested `medusa01.rostam.cct.lsu.edu` (pids 1269244 / 1269458 / 1269664), joined
locality id 1, oracle `returned` + match + `executed_on = 1 ≠ 0`, connector leave
`root_completion_signal` (clean, rc 0), no orphans on either node (remote `pgrep` via
`srun`).

Observational durations only (never gate inputs): demand→srun-Popen 0–1 ms,
demand→admission ≈ 197–804 ms, admission→served ≈ 101–208 ms (first or second 100 ms
wait slice).

**Artifacts.** Curated: `demand_admission_crossnode_aggregate.json` (job 170014; separate
from the loopback aggregate, which is untouched). Raw, under the gitignored
`_exp65_runs/`: `crossnode_170014/{demand_r1..3,nodemand_r1..3}/` marker/log trees,
`crossnode_170014.log`, `exp65_crossnode.sbatch`, and
`demand_admission_crossnode_aggregate_170014.json`.

**Cross-node claim fence.** This slice licenses exactly: demand-ordered connect-mode
admission reproduced on **two** real nodes (medusa00/medusa01) over the TCP parcelport on
`10.42.5.x` (HPX 1.11, gcc 15.1.0, Rostam). It is still not HPX inside Ray actors, not
Ray autoscaling, not elasticity under in-flight work, not concurrent churn, not failure
recovery, and not lazy TCP socket-establishment evidence; it carries no performance,
latency, throughput, ratio, speedup, winner, or general HPX claim. The root still
pre-declares willingness via `--hpx:expect-connecting-localities`.

## What this establishes / does not establish

Establishes, on the loopback slice (this build, single node) **and reproduced on the
two-node cross-node slice** (medusa00/medusa01, TCP `10.42.5.x`): connect-mode admission
is **demand-orderable** — root life and local HPX progress provably precede connector
existence; the admission is caused by a controlling-layer event; discovery needs no
predetermined count; the released connector leaves gracefully and the root keeps working
and finalizes cleanly; and a zero-connector `expect-connecting-localities` root finalizes
cleanly on its own.

Does **not** establish: anything about Ray actors or HPX inside Ray workers; elastic
membership (join/leave overlapping in-flight remote work, concurrent admissions,
re-admission under load); when parcelport TCP sockets are actually opened (admission-time
vs first-parcel — the separate planned diagnostic); multi-node behavior beyond the
two-node cross-node slice; fault tolerance; performance.

## Roadmap

* **Experiment interpretation:** all structural gates passed 3×/3× in both arms on the
  loopback slice, and the cross-node slice reproduced 3×/3× both arms on real nodes
  (19-gate demand arm, 12-gate control) with hard placement/subnet proof; the result
  supports the hypothesis that the proven connect-mode mechanism (exp49–63) does not
  *require* the orchestrated assemble-then-measure pattern those experiments used — the same
  island supports demand-ordered admission within a pre-declared `expect-connecting-
  localities` willingness, on loopback and across two nodes. Ambiguous/remaining: whether
  admission stays clean when demanded *while remote work is already in flight*, and
  everything transport-level. Not to be claimed: any Ray-model equivalence, elasticity, or
  on-demand *transport* behavior.
* **Roadmap impact: Roadmap strengthened.** The demand-ordered admission fact needed for
  the upstream "late and on demand" discussion now holds on real hardware over the TCP
  parcelport, not only on loopback, without touching `rayx.runtime` or the gate document.
* **Updated roadmap:**
  * *In-process HPX-inside-Ray-actors direction:* unchanged by this experiment.
  * *Future distributed-fabric direction:* connect-mode evidence now covers graceful
    lifecycle (exp49), ungraceful-loss boundary (exp50/51), supervised bootstrap
    (exp52/57), and demand-ordered admission (exp65, loopback + two-node cross-node).
    Elastic membership under in-flight work remains the next un-demonstrated lifecycle
    regime.
  * *Same-axis Python-boundary comparison direction:* unchanged; exp64's payload ladder
    remains the headline there.
* **Next recommended step:** the transport-level diagnostic slice — distinct root/connector
  ports, a deliberate dwell between `admission_detected` and first dispatch, and per-process
  established-socket sampling (`lsof -nP -iTCP`) at admission / post-dispatch / post-response
  checkpoints, to determine whether the TCP parcelport connection to the connector's parcel
  endpoint exists at admission or only after the first parcel — observation-only, per-build,
  explicitly separate from the admission-semantics result above, and now runnable in both
  the loopback and cross-node shapes.
