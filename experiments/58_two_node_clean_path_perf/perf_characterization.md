# exp58 — Two-Node Clean-Path Performance Characterization (Slice 0: plan + schema)

**Status:** Slice 0 only — plan and schema. No spike, no runner, no CMake, no build, no run.

This document is the design + schema authority for exp58. It copies the
exp56/57 closed-int64 two-node HPX mechanism (the `dist_probe(int64) -> int64`
`HPX_PLAIN_ACTION` with the unchanged three-way remote proof: closed result,
`remote_locality_id != root_locality_id`, and oracle match) and replaces the
*measurement and schema layer* with high-resolution monotonic timing. The
prior `*_ms` `steady_clock` deltas (process-local since `main()`) remain
Class-A diagnostics only and are excluded from every Class-B metric.

---

## 1. Experiment framing

* **exp58 = clean-path performance characterization.** Clean path only.
* **exp59 = failure / restart** (deferred; out of scope here).
* exp58 introduces **no** failure injection, restart, or detector.

Two metric classes, reported **separately**:

* **Class A** — structural / mechanism facts (island forms, three-way remote
  proof holds, clean leave, no orphans). Unchanged from exp56/57.
* **Class B** — timing characterization (high-resolution only), in two modes:
  depth-1 serialized RTT floor and pipelined throughput.

---

## 2. Defaults (initial)

| Param | Value | Meaning |
|---|---|---|
| `K` | 1000 | measured steady-state actions per island (depth-1 floor) |
| `W` | 100 | warmup actions, dropped from steady-state stats |
| `R` | 5 | island repetitions |
| pipeline depths | `[1, 8, 32, 128]` | depth-1 floor (=1) + pipeline depths (8/32/128) |
| secondary `busy_sum` workload | **deferred** | depth-1 closed-int64 only for now |

Baselines in scope: **Ray-free HPX two-node** and **Ray-supervised HPX two-node
clean path**. **Ray actor baseline deferred.**

Rationale (fair internal comparison):

> The Ray-free baseline is the fair internal comparison because it uses the same
> spike/workload and removes only the Ray supervisor/control plane. Steady-state
> Class-B should be mostly Ray-independent; if it is not, that is supervisor
> interference to investigate, not a general Ray/HPX speed claim.

---

## 3. Timing requirements

* **No millisecond Class-B timing.** Millisecond `*_ms` fields are Class-A
  diagnostics only.
* High-resolution monotonic timing only: `std::chrono::steady_clock`
  nanoseconds **or** `hpx::chrono::high_resolution_timer`.
* Per-action durations recorded in **ns (or µs), never ms**.
* Also capture aggregate loop time over the whole K-loop and derive an
  aggregate mean, as a cross-check against the per-action mean (exposes
  timestamping overhead).

Timing schema block:

```
clock_type                     # e.g. "std::chrono::steady_clock" | "hpx::chrono::high_resolution_timer"
clock_resolution_ns            # measured/known clock tick resolution
per_action_duration_ns_raw     # array of per-action ns deltas (depth-1 steady-state)
aggregate_loop_duration_ns     # whole K-loop wall time in ns
aggregate_mean_action_ns       # aggregate_loop_duration_ns / K
timestamp_overhead_ns          # measured or estimated; null if not measured
```

`timestamp_overhead_ns` is a **hygiene check only** — at µs-scale remote RTT it
is negligible. It must not be prioritized over the scheduler-backoff,
coalescing, and TCP controls below; those are the substantive corrections.

---

## 4. Depth-1 framing

Metric name: **`remote_action_rtt_floor_depth1`**
Allowed alias: **`closed_int64_remote_action_overhead_floor_qd1`**

Serialized `hpx::async<dist_probe_action>(remote, x).get()` loop, exactly one
action in flight at a time, K measured iterations after prewarm + warmup.

> Depth-1 `hpx::async(...).get()` measures a serialized remote-action RTT floor
> at queue depth 1, including HPX future suspension/resume on the root HPX
> thread. It is not representative of saturated HPX execution and must not be
> called general per-action cost.

Additional caveat (scheduler):

> QD1 serialized `.get()` may include HPX worker **idle-backoff wakeup latency**:
> at depth 1 the root worker has no other HPX thread to run while the parcel is
> in flight, so the scheduler can enter idle backoff and must wake before the
> result continuation reschedules `hpx_main`. QD1 must not be interpreted as
> pure network RTT. See the `scheduler_tuning` block (§ Scheduler tuning).

Derived depth-1 stats (all ns, after prewarm + warmup drop): count, mean,
p50 / p90 / p99, min / max. Report **per-island** before pooled (see
§ Statistics).

---

## 5. Pipelined mode

Metric name: **`remote_action_pipeline`**. Issue N actions without per-action
waiting; drain together. Queue depths `[8, 32, 128]`.

Schema (one record per depth):

```
queue_depth
pipeline_actions
pipeline_total_duration_ns
pipeline_actions_per_sec
pipeline_amortized_action_time_ns  # = pipeline_total_duration_ns / N (NOT a latency)
pipeline_correct_count
pipeline_remote_proof_first_last   # three-way remote proof on first AND last future
hpx_wait_primitive                 # "hpx::wait_all" | "hpx::when_all(...).get()"
pipeline_wait_all_includes_allocation_overhead  # true if when_all vector/continuation in timed region
```

**Metric relabeling (was `pipeline_mean_action_ns`):** the per-action figure is
now `pipeline_amortized_action_time_ns`.

> Pipeline makespan divided by N is an **amortized throughput inverse, not
> per-action latency**. It includes issue-loop cost and is tail-gated by the
> slowest future. It must not be placed next to the QD1 RTT floor as the same
> type of metric.

**Wait primitive (point 10):** prefer `hpx::wait_all(futures)` if it avoids the
extra continuation/vector allocation of `hpx::when_all(...).get()`. If
`when_all` is used, set `pipeline_wait_all_includes_allocation_overhead=true`
and record that the allocation/continuation cost is inside the timed region.

**Coalescing / parcel-pool caveat:** `pipeline_actions_per_sec` at these depths
is shaped by HPX TCP parcelport coalescing and parcel-pool scheduling — see the
`parcelport_config` block (§ Parcelport config). Interpret as path
characterization, not pure action-dispatch throughput.

---

## 6. First-action / connection decomposition

No silent mixing of first-parcel / TCP-connect / AGAS-cache effects into
steady-state.

```
prewarm_action_duration_ns
first_action_duration_ns
connection_establishment_component_labeled=true
first_action_includes_tcp_connection_and_agas_resolution_possible=true
```

Design / ordering:

```
prewarm action (1 deliberate, one-shot)
  -> W warmup actions (dropped from stats)
    -> K measured steady-state actions
```

Prewarm and first action are labeled **one-shot**, never folded into
steady-state. Steady-state begins only after prewarm + warmup.

---

## 7. Runtime / build pinning

```
spike_build_type               # Release | RelWithDebInfo | Debug
perf_build_tier                # derived (see gate below)
hpx_build_type                 # if discoverable
compiler_id
compiler_version
hpx_version
allocator                      # if discoverable (system/tcmalloc/jemalloc)
hpx_threads
hpx_bind
parcel_interface
agas_port
hpx_port
tcp_parcelport_enabled
parcelport_tcp_config          # if discoverable
nodeA
nodeB
nodeA_ip
nodeB_ip
selected_subnet
```

**Build gate / tiering:**

| `spike_build_type` | `perf_build_tier` | `perf_valid` |
|---|---|---|
| `Release` | `"primary"` | true |
| `RelWithDebInfo` | `"secondary_relwithdebinfo"` | true (flagged secondary/debuggable) |
| `Debug` | (n/a) | **false** |

* `Release` is the primary valid perf tier.
* `RelWithDebInfo` is allowed but flagged as a secondary, debuggable perf tier.
* **Never mix `Release` and `RelWithDebInfo` numbers in the same aggregate
  summary.** `perf_aggregate.json` is single-tier per file.
* `Debug` is invalid for perf → `perf_valid=false` (fails the perf-valid gate).

---

## 8. Interface / subnet pinning

Perf run pins and records the selected interface/subnet (per allocation;
e.g. `eno16` → `10.42.5.x`, `ibp94s0` → `10.42.6.x`) from `--prefer-subnet`,
with explicit advertised IPs:

```
selected_subnet
parcel_interface
root_advertised_ip
connector_advertised_ip
```

**Endpoint binding vs advertising (point 9).** Recording advertised IPs is not
enough: a multi-homed node can listen or return parcels on a different NIC than
the one advertised unless the parcel server address is pinned on **both**
localities. When possible, verify the actual bound endpoints:

```
endpoints_bound_subnet_verified
root_bound_ip
connector_bound_ip
return_path_interface_verified
```

If binding cannot be verified, record the limitation rather than asserting it.

No network / fabric performance claim is attached to interface choice.

---

## 9. HPX-thread context

Timing loop must run on an **HPX thread**, preferably inside `hpx_main`, so
future suspend/resume is included in the measured deltas.

```
timing_loop_context="hpx_thread"
future_get_resume_included=true
```

If the timing loop runs on a `std::thread`, the run is **marked invalid** for
the intended metric.

---

## 10. Remote id caching

The spike calls `find_all_localities()` **once**, caches the remote
`hpx::id_type`, and reuses it for all prewarm / warmup / steady-state /
pipeline actions. No per-iteration AGAS lookup.

```
remote_id_cached=true
per_iteration_agas_lookup=false
```

---

## 11. Marker / NFS exclusion

The exp57 NFS negative-dentry / directory-attribute-cache marker artifact must
never enter Class-B. No shared-FS marker wait inside the timed steady-state
loop; all critical marker waits stay revalidating (`read_json_eventually` /
`os.listdir`-revalidated) and outside the timed region.

```
steady_state_contains_shared_fs_marker_wait=false
plain_exists_critical_waits=false
marker_waits_revalidating=true
nfs_marker_artifact_excluded_from_perf=true
```

---

## 12. Scheduler tuning (idle backoff)

At depth 1 the root worker has no other HPX thread to run while a parcel is in
flight, so the scheduler can enter idle backoff and must wake before the result
continuation reschedules `hpx_main`. Uncontrolled, this inflates the QD1 floor
with scheduler wake-from-idle latency that is **not** network RTT.

```
scheduler_tuning:
  hpx_max_idle_backoff_time
  hpx_max_idle_loop_count
  idle_backoff_controlled
  idle_backoff_mode                          # "disabled" | "recorded_only" | "unknown"
  scheduler_idle_backoff_may_affect_qd1      # true | false
```

Plan requirement: for the **primary QD1 RTT-floor run**, prefer disabling idle
backoff if supported, e.g.:

```
--hpx:ini=hpx.max_idle_backoff_time=0
```

If not disabled, the run must record the settings and label QD1 as **including
scheduler wake-from-idle latency** (`idle_backoff_mode="recorded_only"`,
`scheduler_idle_backoff_may_affect_qd1=true`).

> QD1 serialized `.get()` may include HPX worker idle-backoff wakeup latency. It
> must not be interpreted as pure network RTT.

---

## 13. Parcelport config (coalescing, Nagle, parcel pool)

`pipeline_actions_per_sec` and depth-128 throughput are shaped by HPX TCP
parcelport policy, not action dispatch alone. Record it.

**Coalescing (point 2):**

```
parcelport_config:
  parcel_coalescing_enabled
  parcel_coalescing_interval
  parcel_coalescing_buffer_size
  parcel_coalescing_control_run               # true if a coalescing-disabled control was run
  parcel_coalescing_mode                      # "disabled" | "enabled_recorded" | "unknown"
```

Plan requirement: pipeline depths `[8, 32, 128]` must record coalescing
settings. If feasible, include a control run with coalescing disabled. If not
feasible, label pipeline throughput as including HPX TCP parcelport coalescing
policy.

> `pipeline_actions_per_sec` may measure parcel batching/coalescing behavior as
> much as action dispatch throughput, so it must be interpreted as path
> characterization, not pure action cost.

**TCP_NODELAY / Nagle (point 3):**

```
  tcp_nodelay_expected
  tcp_nodelay_verified
  tcp_nodelay_note
```

Plan requirement: record/verify whether HPX TCP parcelport sockets have
TCP_NODELAY / `no_delay` enabled if discoverable. If not directly verifiable,
set `tcp_nodelay_verified=false` and note that Nagle/delayed-ACK artifacts are a
possible confound for serialized small-parcel QD1 RTT.

**Parcel-pool threads (point 5):**

```
  parcel_pool_threads
  parcel_pool_affinity
  parcel_pool_threading_config
  parcel_pool_contends_with_worker_cores
```

Plan requirement: record parcelport thread-pool configuration if discoverable.
Depth-128 throughput must not be interpreted without this context;
`hpx_threads` alone does not pin the parcel path.

---

## 14. Loopback / same-node two-locality control

Inter-node TCP alone cannot separate HPX parcel-stack/scheduler software cost
from the network leg. A same-node two-locality TCP loopback run decomposes them.

```
loopback_control_available
loopback_control_run
loopback_control_purpose
```

Plan requirement: the loopback control is **recommended** but **not required**
for the first Slice 1 implementation unless explicitly approved; the schema must
reserve these fields regardless.

> Inter-node TCP results alone cannot separate HPX parcel-stack/scheduler cost
> from the network leg. No network/fabric performance claim is allowed.

---

## 15. Statistics: per-island first, pooled only as supplementary

R=5 islands are 5 fresh processes / TCP connections / possibly different
scheduler/cpufreq states. Pooling K×R before percentiles can hide an anomalous
island.

```
per_island_stats          # per island: QD1 p50/p90/p99/min/max (ns)
across_island_stats       # median + spread of the per-island summaries
pooled_stats              # K×R pooled, supplementary only
pooled_stats_allowed      # false for primary claims unless paired with per-island
node_pair_stable_across_R
```

Plan requirement for R=5: report per-island p50/p90/p99/min/max for QD1, then
across-island median/spread of those summaries, and only then optionally pooled
K×R as supplementary. A pooled K×R p99 must never stand alone as a primary
claim.

---

## 16. Environment confounds (DVFS, connector idle)

**CPU frequency governor / DVFS (point 8):**

```
cpu_governor_nodeA
cpu_governor_nodeB
cpu_frequency_policy_recorded
turbo_or_dvfs_note
```

Plan requirement: record CPU governor/frequency policy if accessible. Prefer a
performance governor if available, but do not require changing system policy. If
the governor is unknown, mark it as a confound.

**Connector locality idle state (point 11):**

```
connector_locality_idle_except_actions
connector_background_work_note
```

Plan requirement: the connector must be otherwise idle during the steady-state
loop, because the floor also includes the connector's scheduling latency. Any
background work is recorded as a confound.

---

## 17. Baselines

Keep:

* Ray-free HPX two-node baseline;
* Ray-supervised HPX two-node clean path (same spike, supervisor added).

Defer:

* Ray actor baseline;
* `busy_sum` / secondary workload.

---

## 18. Artifact layout (intended)

Created in this slice (Slice 0):

* `perf_characterization.md` — this plan/schema (tracked).
* `.gitignore` — artifact hygiene (tracked).

Created in Slice 1 (Ray-free baseline infrastructure):

* `two_node_perf_spike.cpp` — copy-in-spirit closed-int64 two-node spike with
  high-resolution timing; root runs depth-1 QD1 floor + pipelined modes.
* `CMakeLists.txt` — build for the spike; injects build-identity, warns on
  non-Release perf tiers.
* `run_exp58_perf.py` — **Ray-free** runner (`--phase rayfree-baseline`); no Ray
  import, no failure/restart.

Extended in Slice 3 (Ray-supervised path, same `run_exp58_perf.py`):

* `--phase ray-supervised` — SAME spike / workload / flags / gates / timing
  schema as the Ray-free path; Ray is the **control plane only** (one
  `_SrunRunner` Ray actor per role issues the root/connector `srun`
  near-concurrently; the connector is **not** gated on `root.ready` and relies
  on its AGAS TCP pre-probe). Ray is imported **lazily inside this phase only**;
  records `ray_imported=true`, `ray_init_ok`, `ray_version`,
  `ray_supervisor_used=true`, `failure_restart_used=false`,
  `ray_in_action_data_path=false`. HPX owns the action/data path.
* HPX-version provenance fix (shared by both phases): `check_config` parses the
  real version from the HPX version line of `--hpx:version`
  (`hpx_version_parsed`, plus `hpx_allocator`, `hpx_build_type_reported`,
  `hpx_git`, and `hpx_version_capture_note`); never fabricated, falls back to the
  banner with a note.

Curated PASS aggregates (top level, trackable; written **only** by a cluster
pass via the overwrite guard — see § 21):

* `perf_aggregate_rayfree.json` — Ray-free baseline (`--phase rayfree-baseline`).
* `perf_aggregate_ray_supervised.json` — Ray-supervised (`--phase
  ray-supervised`); separate file so the two sit side by side for diffing.

The generic `perf_aggregate.json` is **deprecated and no longer written** (the
on-disk copy is a clobbered local skip; see § 21). Per-run
`_perf_runs/<bootdir>/run_aggregate.json` stays authoritative.

Raw / ignored artifacts (per `.gitignore`):

* skip/fail/local/redirected aggregates
  (`perf_aggregate_*_skip.json`, `*_fail.json`, `perf_aggregate_local_skip.json`,
  `perf_aggregate_*_redirected_*.json`, deprecated `perf_aggregate.json`).

* `_perf_runs/` — per-run scratch outputs.
* `perf_index.jsonl` — raw run index.
* `build/`, raw logs / HPX parcel logs.

---

## 19. Slice order

* **Slice 0** — plan + schema only *(this document)*.
* **Slice 1** — copied spike with high-resolution timing (depth-1 + pipelined
  modes), **Ray-free runner only**.
* **Slice 2** — Ray-free Rostam baseline run.
* **Slice 3** — Ray-supervised runner using the **same spike**.
* **Slice 4** — replicated (`R=5`) summary / write-up.
* **exp59 (later)** — failure / restart.

---

## 20. Claim hygiene (fences)

* No general Ray-vs-RayX claim.
* No "HPX beats Ray" claim.
* No production / API claim.
* No HPX fault-tolerance claim.
* No Ray failure-recovery claim.
* No network / fabric performance claim.
* TCP parcelport only.
* Closed-int64 only.
* Rostam / allocation-specific characterization only.
* No single-run speedup claim.
* Class A and Class B reported separately.
* Depth-1 numbers are an RTT floor at queue depth 1, **not** general per-action
  cost.

HPX-internals fences (review hardening):

* QD1 RTT floor may include HPX scheduler idle-backoff wake latency.
* Pipeline throughput may include parcel coalescing policy and parcel-pool
  scheduling behavior.
* Pipeline amortized time (`pipeline_amortized_action_time_ns`) is not
  per-action latency.
* Inter-node result is not decomposed into software vs network unless the
  loopback control is run.
* CPU governor / DVFS may affect low-latency timing unless recorded or
  controlled.

---

## 21. Artifact-integrity policy (overwrite guard)

**Why this exists.** Twice a top-level curated aggregate was overwritten by a
local/skip run. Specifically, the Slice 2 Ray-free top-level convenience
aggregate (`perf_aggregate.json`) was **clobbered by a local Mac no-SLURM skip**
(it now reads `overall:"skip"`, `Platform: Mac OS`). No evidence was lost — the
**authoritative Ray-free Slice 2 record survived** in
`_perf_runs/exp58_perf_r0_5xjgdx0z/run_aggregate.json` and in the first
`_perf_runs/perf_index.jsonl` line — but the curated top-level copy was
destroyed. The lost top-level aggregate is **not reconstructed/fabricated**;
Slice 4 re-captures the Ray-free R=5 baseline under the same allocation anyway.

**Guard (`safe_write_aggregate`, atomic temp+fsync+rename).** Top-level
aggregate writes now follow this policy and can no longer clobber a curated
pass:

* PASS aggregates are **phase-specific**: `perf_aggregate_rayfree.json`,
  `perf_aggregate_ray_supervised.json` — the two phases cannot overwrite each
  other.
* A **skip/fail** result never lands on a pass path; it is redirected to a
  `_skip` / `_fail` sibling (ignored).
* A **pass-over-pass** write is refused unless it is the **same phase** and
  `--allow-overwrite-pass` is given; otherwise it is redirected to an ignored
  `_redirected_<runid>` sibling so the existing curated pass is preserved.
* A **local / unbuilt-binary** skip prints to stdout only and writes **no**
  aggregate file.
* Pass → skip/fail is **never** a silent downgrade.
* Each written payload records `artifact_write_policy`,
  `top_level_overwrite_guard_active=true`, `top_level_aggregate_path`,
  `redirected_from_path`, `overwrite_refused`.
* `_perf_runs/<bootdir>/run_aggregate.json` remains authoritative and is
  untouched by this layer.

**For analysis, read these files** (the generic `perf_aggregate.json` is
deprecated):

* `perf_aggregate_rayfree.json`
* `perf_aggregate_ray_supervised.json`
* `_perf_runs/perf_index.jsonl`
* `_perf_runs/<bootdir>/run_aggregate.json` (authoritative per run)
