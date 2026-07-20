# exp70 Slice 2A — explicit-completion contract for an actor-hosted HPX island (external backend)

**Status:** complete — local phase and two-node hardware phase both green. Mechanism /
application-contract evidence only. Not a performance experiment, not a Ray comparison, not
production code, not a shipped RayX API.

## 1. Question

Can the actor-hosted HPX island avoid connector lifetime guesses by making **"no further work
will be sent"** an explicit, testable lifecycle event?

Slice 0 (`../upstream_reproducer/`) reduced the lifetime-guess failure to two processes: a
connector with a fixed 3 s serve window departs, and a later valid dispatch to the departed
locality fails. Slice 2A elevates Slice 0's external-lifecycle answer — an explicit completion
witness written only after the final verified result — to the exp66–68 actor-hosted
shared-runtime island, behind a **backend-neutral contract** that a future HPX-native mechanism
(Slice 2B) could implement without changing the state machine or acceptance gates.

## 2. Contract (backend-neutral)

1. The island begins accepting work.
2. An idle interval does **not** authorize connector departure.
3. Valid HPX work can arrive after the idle interval and is verified bit-exactly.
4. Completion is published **exactly once**, only after the final verified result.
5. After publication no application work is accepted (controller-level fence, checked before
   any dispatch could reach HPX).
6. Every connector observes completion for the exact island epoch; stale markers from prior
   epochs are rejected and can never satisfy an observation.
7. Every connector leaves via the validated disconnect/stop sequence, only after its own
   observation acknowledgment.
8. The work-free root observes membership return to itself and finalizes cleanly.
9. No process remains (experiment-scoped owned sweep with PID-reuse discrimination).

No failure detection is required or implied in this slice.

## 3. State machine

```text
STARTING -> READY -> WORK_1_VERIFIED -> IDLE -> WORK_2_VERIFIED -> COMPLETION_PUBLISHED
-> CONNECTORS_LEAVING -> ROOT_ALONE -> FINALIZED
```

Invalid transitions raise deterministically and are recorded. Application dispatch is allowed
only in `READY` (work 1) and `IDLE` (work 2); the `dispatch_guard` consulted before every
dispatch is the post-completion fence (a rejected guard proves the attempt never reached HPX).

## 4. Backends

The two-method surface (`publish_complete(epoch, connectors, payload)` /
`observe_completion(epoch, connector, bound)`) is duck-typed and selftested for
substitutability:

* `SyntheticCompletionBackend` — in-memory, selftests only.
* `ExternalWitnessCompletionBackend` — this slice's live backend: an epoch-scoped
  `island.complete` JSON marker in the island boot directory, published atomically
  (write + rename), observed by bounded **monotonic** polling. Distinct from exp68's
  mechanical `root.done` root-finalize trigger, which is still used afterwards to finalize
  the work-free root.
* A minimal substitute backend passes the identical state machine and gates in the selftests
  (the Slice 2B substitution surface).

Observation is **controller-mediated**: the exp68 actor surface is fixed (never modified
here), so one observer record is produced per connector in the controller, and each
connector's graceful leave is dispatched only after its own acknowledgment. A native backend
would relocate publication/observation into the runtime; the per-connector acknowledgment
gate stays.

## 5. Topology and workloads

Work-free `exp68_peer` root (locality 0) + two Ray actors hosting HPX connect-mode localities
in-process (exp68 build artifacts referenced in place; no exp68 code modified or copied). Two
**distinguishable** exp68 matrix cases, both coordinator directions, bit-exact vs the imported
oracle: work 1 `cross_both` (V=64, split=32, k=6, seed=1) before the idle interval, work 2
`both_contrib` (V=100, split=50, k=10, seed=1) after it — a stale work-1 result cannot satisfy
the work-2 oracle gate.

Two-node hardware topology:

```text
node A (medusa11, 10.42.5.41)   batch step / controller / Ray head / work-free HPX root / actor A
node B (medusa12, 10.42.5.42)   actor B
```

Idle semantics, not tuning: the idle interval (6 s) exceeds Slice 0's former fixed 3 s
serve window to demonstrate that idleness does not end the island's obligation to accept work.
In the actor-hosted topology connectors cannot self-depart; the idle gates prove continued
availability (health, membership 3, unchanged localities) and that no stop was dispatched
during the interval.

## 6. Results

Selftests: **73/73** (state transitions, publication guards, duplicate/stale rejection,
fence, per-connector acknowledgment, departure ordering, root-alone and finalize gates,
owned-process sweep with PID-reuse discrimination, backend substitution, off-cluster
preflight discipline, and 8 cross-node placement/preflight checks).

### Local phase

Three passing local runs, each with zero failed gates and the full 9-state history. The
latest, `20260719T135320Z`, was re-verified against the current `run_slice2.py` after the
cross-node phase was added (14 gate groups, idle 6.01 s > 3.0 s, both connectors epoch-matched,
root `finalized_clean`, no owned process remaining). The two earlier runs
(`20260718T225645Z`, `20260718T225705Z`) predate the cross-node addition and are curated at
`_exp70_slice2_runs/curated_local_evidence/`.

### Two-node hardware phase

Slurm job **173796** on **medusa11 + medusa12** (partition `medusa`, `-N 2 --exclusive`,
elapsed 00:00:43), launched by `exp70_slice2_crossnode.sbatch`. Two live runs,
**`20260719T164029Z`** and **`20260719T164049Z`**, both `overall=pass` with **15 gate groups /
109 individual checks all true and zero failed gates**. Each run used a fresh run id, boot
directory, actor set, Ray cluster, process identities, and a **disjoint port block**
(7911/7912/7913 with Ray head 6479; 7931/7932/7933 with Ray head 6499).

Proven per run:

| # | Property | Evidence |
|---|---|---|
| 1 | Work-free root on node A | `root_argv` bound to `10.42.5.41:7911` (resp. `:7931`); `no_application_on_root` |
| 2 | Actor A hard-placed on node A | `placement.actor_a_on_nodeA`, `strategy_hard_node_affinity`, host `medusa11…` |
| 3 | Actor B hard-placed on node B | `placement.actor_b_on_nodeB`, `actors_on_distinct_ray_nodes`, host `medusa12…` |
| 4 | Both actors host HPX in-process | `inprocess.{a,b}_pid_identity`, `{a,b}_no_hpx_children` |
| 5 | Shared-island membership | `startup.membership_reached_3`, localities 0/1/2, distinct connector localities |
| 6 | Workload 1 bit-exact, both directions | `work1` 22/22 incl. `float32_bits_exact`, `global_token_ids_exact`, `coordinator_symmetry` |
| 7 | Idle exceeds the former window | `idle_elapsed_s = 6.0001` > 3.0 (`idle_exceeds_former_window`) |
| 8 | Connectors available after idle | `membership_still_3_after_idle`, `localities_unchanged_after_idle`, pre/post health equal |
| 9 | Workload 2 distinct and bit-exact | `work2` 25/25 incl. `work2_case_differs_from_work1`, `work2_oracle_differs_from_work1` |
| 10 | Completion only after work 2 | `completion.publish_only_after_final_verification`, `work2_dispatched_from_idle_state` |
| 11 | Duplicate publication rejected | `completion.duplicate_publish_attempted` + `duplicate_publish_rejected` |
| 12 | Post-completion dispatch fenced | `post_completion_fence.post_completion_dispatch_never_reached_hpx` |
| 13 | Epoch-matched observation | `observation.{a,b}_epoch_match`, `_bounded`, `all_connectors_acknowledged`; `epoch_scope.stale_marker_from_prior_epoch_rejected` |
| 14 | Graceful departure | `departure.{a,b}_graceful_stop`, `observation_precedes_departure`, `no_departure_before_publication_*` |
| 15 | Root sees membership return | `root_alone.root_observed_leave`, `root_final_present` |
| 16 | Root finalizes cleanly | `finalize.root_finalized_clean`, `actor_pids_gone` |
| 17 | No owned process remains | `final.owned_processes_gone`, `owned_records_cover_island`, `no_rundir_scoped_processes`; job-level sweep `none` on both nodes |
| 18 | Phase ordering | `ordering.state_history_complete_and_ordered`, `no_rejected_transitions` |

Recorded peer evidence for both workloads (run `20260719T164029Z`):

| Actor | PID | HPX locality | Hostname | Ray node id | HPX endpoint |
|---|---|---|---|---|---|
| A | 132633 | 1 | medusa11.rostam.cct.lsu.edu | `d556be943a64e0bc…` | `10.42.5.41:7912` (AGAS `10.42.5.41:7911`) |
| B | 1679263 | 2 | medusa12.rostam.cct.lsu.edu | `29a93a6d420e21d1…` | `10.42.5.42:7913` (AGAS `10.42.5.41:7911`) |

Run `20260719T164049Z` repeats this with PIDs 136080 / 1680822, fresh actor and Ray node ids,
and endpoints on `:7932` / `:7933` (AGAS `10.42.5.41:7931`).

No timing or performance claim is made from these runs.

### Copy-back and hash verification

Copied to `_exp70_slice2_runs/rostam_copyback_20260719T214109Z/` (both raw run directories,
aggregates, markers, root/Ray/Slurm logs, environment record, orphan sweep, `sacct`, source
and dependency hashes, hardware manifest and its `.sha256`).

Manifest self-hash matches its sidecar. **25 of 26 listed artifacts hash-verified exactly, 0
missing.** The 26th entry is the Slurm job log, which the in-job manifest step necessarily
hashes before the job finishes writing it; this was verified as an exact byte-prefix (the 349
appended bytes are the manifest step's own stdout plus the final `done` line), and the final
remote and local hashes are identical. Details in `copyback_verification.txt` beside the
artifacts. Curated summary: `slice2_curated_evidence.json`.

Job **173795** (same node pair, both runs also `pass`) is retained for provenance at
`_exp70_slice2_runs/rostam_copyback_20260719T213725Z/`; it is superseded because its two runs
shared one port block.

## 7. Supported claim

> In a two-node actor-hosted HPX island, both connectors remained available across an idle
> interval longer than the former fixed serving window, accepted later valid distributed work,
> and departed cleanly only after explicit completion was published through the external
> backend.

## 8. Non-claims

The result does not demonstrate: HPX-native completion; an HPX-native heartbeat; HPX-native
failure detection; runtime-level enforcement against arbitrary parcels after completion;
recovery; any performance improvement. The post-completion fence is an application contract
gate, not a claim that current HPX blocks parcels after completion.

## Files

* `run_slice2.py` — instrument: selftest, local live phase, cross-node phase, curation.
* `exp70_slice2_crossnode.sbatch` — two-node Rostam launcher (never rebuilds exp68).
* `slice2_curated_evidence.json` — curated local + hardware evidence with hashes.
* `_exp70_slice2_runs/` — ignored raw run area (see `.gitignore`).
