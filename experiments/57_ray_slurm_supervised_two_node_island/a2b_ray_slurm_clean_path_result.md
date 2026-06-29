# exp57 Slice A2b — Ray/Slurm-supervised clean two-node HPX island (result)

**Status:** recorded result. `overall=pass`. A2b is the **first Ray/Slurm-supervised clean two-node
HPX island** in exp57. Mechanism/bootstrap evidence only — no performance, fault-tolerance, or
production claim. Failure/restart, poison detection, and detector timing remain deferred to exp58.

This note records the validated clean-path run so it is easy to explain later. It is a recording note;
it retracts nothing and changes no code.

---

## 1. Verdict

- `overall=pass`.
- A2b validated as the first Ray/Slurm-supervised clean two-node HPX island in exp57.
- Run: Rostam, two-node `medusa` allocation, nodes `medusa00` / `medusa01`, subnet `10.42.5.`,
  AGAS port `8000`, HPX port `8001`, pre-probe timeout `60000` ms.
- `mechanism_success_on_disk=true`, `failure_restart_used=false`.

## 2. Architecture (role separation)

- **Ray** = bootstrap / supervision / control plane **only**. It issues the `srun` commands and collects
  outcomes; it carries no HPX action/data and stores no HPX payload (no object store).
- **Slurm** = node placement / `srun` step launch.
- **HPX** = the execution / data path (the closed-int64 remote action and the AGAS join/leave).
- **TCP parcelport only.**
- **Closed-int64 action only.**

## 3. Launch details

- Ray is **lazy-imported inside `--phase run`** (never at module top); Ray 2.55.1, `ray_init_ok=true`.
- Supervisor shape: **one `_SrunRunner` Ray actor per role**; root and connector `srun` issued from
  inside those Ray actors (`launched_from_ray_actor=true` for both).
- Root and connector issued **near-concurrently**, back-to-back, with no marker wait between them:
  - `root_srun_issue_ms=1`
  - `connector_srun_issue_ms=1`
  - `srun_issue_gap_ms=0`
- The connector was **not gated on `root.ready`** (`connector_gated_on_root_ready=false`;
  `shared_fs_readiness_gate_on_connector_launch=false`). Connector readiness came from its own
  connector-side AGAS TCP pre-probe (see §5).
- This is a fresh launch path; it deliberately does **not** template `_launch_settle_diag`
  (`run_phase_must_not_template_settle_diag=true`). All marker waits use the revalidating readers
  (`read_json_eventually` / `exists_eventually`); `marker_waits_revalidating=true`,
  `plain_exists_critical_waits=false`.

## 4. Environment / loader gate

- Pre-Ray child-env preservation validated: child env anchored to the pre-Ray baseline, all load-bearing
  keys present and matching the baseline, `load_bearing_missing=[]`.
- `ray_mutation_defense_validated=true` — the `srun` children were launched from inside a Ray actor with
  the explicit pre-Ray-anchored child env, so any Ray mutation of the actor env cannot reach the HPX
  child.
- Both compute nodes used the expected GCC 15 libstdc++:
  `/opt/apps/gcc/15.1.0/lib64/libstdc++.so.6.0.34` (derived from `g++ -print-file-name=libstdc++.so`
  under the same child env; both nodes' `ldd` matched the expected file and realpath'd dir, and the
  resolved path is not a system copy).
- `ldd_both_use_expected_gcc_libstdcxx=true`.

## 5. AGAS TCP pre-probe

- `agas_preprobe_active=true`.
- Target: `10.42.5.30:8000` (root AGAS endpoint).
- `agas_preprobe_ok=true`, `agas_preprobe_timeout=false`, `agas_preprobe_ms=450`.
- `connect.preprobe_ok` present.
- **Meaning:** `agas_preprobe_ok` means the **TCP endpoint accepted a connection** — it is **not** an
  AGAS semantic readiness proof by itself. It is the connector-side readiness signal that replaces
  shared-FS `root.ready` gating; AGAS-level admission is proven separately by the HPX mechanism in §6.

## 6. HPX mechanism

- Root observed two localities (`reached_two=true`, `localities_seen=2`).
- Closed-int64 oracle matched: `result == oracle == 1380014433` (`oracle_match=true`, in-binary
  `match=true`).
- `proved_remote_by_oracle=true`.
- `remote_locality_id_differs=true` (root locality `0`, action executed on locality `1`).
- Root-side `observed_connector_leave=true` (the authoritative AGAS / cross-node clean-leave signal).
- `connect.disconnected1.clean=true` (teardown `post(disconnect)+stop`, `rc=0`, `served=true`). Note:
  `connect.disconnect_initiated.json` is an initiation-only marker; connector-local disconnect
  completion is not claimed — the root-side observed leave is authoritative.
- Root finalized cleanly: `root_finalize_done.json` present, `init_rc=0`, `root_rc=0`, `connector_rc=0`.
- `no_orphans=true` (both nodes clean, empty pid lists).
- `mechanism_success_on_disk=true`.
- `failure_restart_used=false`.

## 7. Artifact preservation

- Latest: `run_aggregate.json`.
- Per-run: `_two_node_runs/<bootdir>/run_aggregate.json`
  (this run: `_two_node_runs/exp57_runscaffold_0oyg1cr7/run_aggregate.json`, byte-identical to latest).
- Index: `_two_node_runs/run_aggregate_index.jsonl` (one `pass` row for this run).
- Raw `_two_node_runs/` run artifacts (stdout/stderr, markers) stay ignored; the curated
  `run_aggregate.json` carries the full verdict.

## 8. Claim hygiene

- No performance / speedup / throughput claim.
- No network / fabric performance claim.
- No HPX fault-tolerance claim.
- No Ray failure-recovery claim.
- No public API / production claim.
- No duration here is an HPX/AGAS settle. The in-binary `settle_ms=300` and the various bounded
  observed-leave / finalize / served durations are structural readiness/wait durations only; do not
  reuse them as performance, fabric, or restart/detector timing.
- Ray is only the bootstrap/supervision/control plane; HPX carries the execution/data path.
- Failure/restart remains deferred to exp58.

## 9. Structural caveat (do not chase here)

- The in-binary, connector-side wait for `served1.ok` visibility was about **~30 s** this run
  (`connector_served_ms≈30708` on the connector clock). The root proved the remote action early
  (`root_action_result_ms≈745` on the root clock), so this gap is the connector-process wait for the
  shared-FS `served1.ok` marker to become visible — it lives **inside `two_node_island_spike.cpp`**, not
  in the A2b Python orchestration (which uses the revalidating readers).
- Treat it as a **structural observation only** — not a performance, HPX, AGAS, network, or fabric claim,
  and not an HPX/AGAS settle.
- It resembles the Slice 0 shared-FS marker-visibility pattern (negative-dentry / attribute caching),
  but that is a hypothesis, not a measured attribution for this in-binary wait.
- Do **not** chase or modify it in this slice. Any future investigation would be a separately scoped
  C++ / diagnostic change (read-only diagnostic or an exp58-adjacent C++ edit), explicitly out of scope
  here.
