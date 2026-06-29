# exp58 — Two-Node HPX TCP Clean-Path Performance Characterization (R=5, Rostam)

**Scope.** Clean-path performance characterization of a two-node HPX TCP island, run two
ways under the *same* spike/workload: a **Ray-free** baseline and a **Ray-supervised** clean
path. Ray supervises only launch/control; HPX carries the action/data path. This is an
instrument/characterization result, **not** a benchmark verdict.

**Fences (read first).** TCP parcelport only. Closed-`int64` action only. Depth-1 numbers are a
**serialized RTT floor at queue depth 1** (`remote_action_rtt_floor_depth1`), *not* general
per-action cost and *not* pure network RTT. Pipeline numbers are path characterization, *not*
latency. Rostam/this-allocation-specific. No Ray-vs-RayX speed claim, no "HPX beats Ray", no
network/fabric performance claim, no production/API claim, no HPX fault-tolerance, no Ray
failure-recovery. Failure/restart (exp60) is out of scope here.

---

## 1. What was run

- Spike: `two_node_perf_spike.cpp` (copy-in-spirit of the exp56/57 two-node HPX TCP mechanism:
  closed-`int64` `dist_probe` `HPX_PLAIN_ACTION`, three-way remote proof, clean connector leave
  observed by root, root finalize; **no failure/restart**). The root runs the Class-B timing on a
  cached remote `hpx::id_type`.
- Runner: `run_exp58_perf.py`, two phases over the *same* binary/workload/flags:
  - `--phase rayfree-baseline` — root via direct `srun`, connector launched after a revalidating
    `root.ready` wait.
  - `--phase ray-supervised` — one `_SrunRunner` Ray actor per role, both `srun` issued
    near-concurrently (back-to-back `.remote()`), connector **not** gated on `root.ready`
    (relies on its AGAS TCP pre-probe).
- Parameters (both phases): `K=1000` measured steady-state actions, `W=100` warmup (dropped),
  pipeline depths `[8, 32, 128]`, `R=5` islands, `--hpx:threads=4`, idle backoff disabled.
- Allocation: Rostam `medusa00` / `medusa01`, interface `eno16`, subnet `10.42.5.x`
  (`10.42.5.30` / `10.42.5.31`), Release build, GCC 15.1.0, HPX **1.11.0** (allocator `system`,
  git `c9b81b401f`).

**Result: PASS.** 10/10 valid islands (5 Ray-free + 5 Ray-supervised); every correctness gate
green on every island; `failure_restart_used=false` throughout.

---

## 2. Main finding

exp58 completed **R=5 Ray-free and R=5 Ray-supervised** clean-path characterization on Rostam with
**10/10 valid islands** and **all correctness gates green** (reached-two, oracle match, remote
locality differs, depth-1 correct = K, pipeline correct = depths, first/last proofs, observed
connector leave, clean disconnect/finalize, no orphans, no shared-FS marker wait in Class-B,
timing on an HPX thread, valid Release tier).

**Interpretation.** The Ray-free and Ray-supervised **Class-B steady-state measurements overlap
within across-island jitter**. This supports the architecture expectation that **steady-state HPX
action execution is mostly Ray-independent**, because Ray is not in the HPX action/data path: it
only launches and supervises the processes, while HPX carries the closed-`int64` action over the
TCP parcelport.

---

## 3. Architecture interpretation

Roles are cleanly separated and the artifacts confirm the separation:

- **Ray = control plane / launch supervision.** One `_SrunRunner` Ray actor per role issues `srun`
  and reports process status only (`launched_from_ray_actor=true`,
  `ray_in_action_data_path=false`).
- **Slurm = placement / `srun`.** The actors shell out to `srun --export=ALL` with a
  pre-Ray-anchored child env (`ray_mutation_defense_validated=true`).
- **HPX = action/data path.** The closed-`int64` `dist_probe` action and its result travel over the
  HPX **TCP parcelport** between localities; the remote `hpx::id_type` is resolved once and cached.
- **Ray object store is not used for HPX action results**
  (`ray_object_store_used_for_action_results=false`).
- **TCP parcelport only**; **closed-`int64` only**.

Because Ray never touches the action/data path, the steady-state floor is governed by
HPX/TCP/scheduler, which is exactly what the overlapping Class-B bands show.

---

## 4. QD1 result interpretation (`remote_action_rtt_floor_depth1`)

Across-island **median [min–max spread]** of the per-island QD1 percentiles (ns):

| metric | Ray-free | Ray-supervised |
|---|---|---|
| p50 | **≈ 120.3 µs** (120,270) [112,829–127,641] | **≈ 115.8 µs** (115,831) [115,108–119,431] |
| p90 | ≈ 156.2 µs (156,150) [149,926–160,603] | ≈ 153.8 µs (153,779) [147,430–156,090] |
| p99 | **≈ 188.0 µs** (187,957) [187,641–194,729] | **≈ 185.7 µs** (185,662) [178,906–197,230] |
| min | ~79.6–83.5 µs | ~79.1–82.8 µs |

**Interpretation.**
- This is the **serialized QD1 RTT floor** — one `hpx::async<dist_probe_action>(remote, x).get()`
  in flight at a time, including HPX future suspend/resume on the root HPX thread. It is **not**
  general per-action cost and **not** pure network RTT.
- The phase-to-phase **median p50 gap (~4.5 µs)** is **smaller than the across-island jitter**,
  especially the Ray-free p50 spread (~15 µs, 112.8–127.6 µs). The Ray-free low island (r4, p50
  112.8 µs) sits *below* the entire Ray-supervised p50 band, and the Ray-free high island (r0, p50
  127.6 µs) sits *above* it. p90 and p99 bands likewise overlap.
- **Therefore this is not a speedup or a regression.** The two phases land in the same band; the
  difference is run-to-run island variance, not a separable Ray-vs-HPX effect.

---

## 5. Pipeline interpretation

Across-island median `pipeline_actions_per_sec` (amortized = makespan/N), per depth:

| depth | Ray-free (median) | Ray-supervised (median) |
|---|---|---|
| 8 | ≈ 25k/s | ≈ 26k/s |
| 32 | ≈ 54k/s | ≈ 54k/s |
| 128 | ≈ 93k/s | ≈ 98k/s |

(per-island spreads overlap at every depth; all pipeline correct counts full, first/last proofs
`true`, `hpx_wait_primitive=hpx::wait_all`, `pipeline_wait_all_includes_allocation_overhead=false`.)

**Interpretation.**
- `pipeline_amortized_action_time_ns` is **makespan/N, not a latency** — it is tail-gated and
  includes the issue loop.
- Pipeline results are **path characterization**, not a dispatch ceiling.
- HPX TCP **parcel coalescing is enabled** and **parcel-pool scheduling (2 threads)** is included —
  both are disclosed in `parcelport_config`, so the depth-32/128 throughput partly reflects parcel
  batching, not pure dispatch concurrency.
- **No dispatch-ceiling claim and no network/fabric claim.** Ray-free vs Ray-supervised medians sit
  within each other's across-island spread at all three depths.

---

## 6. Validity controls (why this is defensible)

- **High-resolution timing**: `std::chrono::steady_clock` nanoseconds; per-action raw arrays in ns;
  timestamp overhead ~20 ns.
- **Aggregate-vs-per-action mean agreement**: `aggregate_mean_action_ns` matches the per-action
  mean to **~34–41 ns** on every island, both phases — two independent timing paths agree.
- **Scheduler confound controlled**: idle backoff **disabled** and recorded
  (`idle_backoff_mode="disabled"`, `scheduler_idle_backoff_may_affect_qd1=false`) on all islands.
- **DVFS confound controlled/recorded**: CPU governors **performance / performance** on both nodes.
- **Build provenance**: Release / `perf_build_tier=primary` / `perf_valid=true`; GCC 15.1.0;
  HPX 1.11.0 (parsed via the provenance fix; allocator `system`, build `release`, git
  `c9b81b401f`); GCC-15 libstdc++ ldd gate passed on both nodes
  (`ldd_both_use_expected_gcc_libstdcxx=true`).
- **Comparability**: identical node pair `medusa00`/`medusa01`, subnet `10.42.5.x`, interface
  `eno16`, ports, K/W/depths, threads, idle-backoff flag for both phases; `node_pair_stable_across_R`.
- **No shared-FS marker wait inside Class-B**: `steady_state_contains_shared_fs_marker_wait=false`,
  `plain_exists_critical_waits=false`, `nfs_marker_artifact_excluded_from_perf=true` on all islands;
  `served1.ok` written only after the timed regions.
- **Raw evidence preserved**: 1000-element `per_action_duration_ns_raw` in every per-run artifact;
  all root/connector `stderr` 0 bytes (clean).

---

## 7. Artifact-integrity note

- **Hardened aggregate writer active** (`top_level_overwrite_guard_active=true`), atomic
  temp+fsync+rename writes.
- **Phase-specific top-level aggregates** (curated, trackable):
  - `perf_aggregate_rayfree.json`
  - `perf_aggregate_ray_supervised.json`
- The generic **`perf_aggregate.json` is deprecated and ignored** (the on-disk copy is a stale local
  skip; see `perf_characterization.md` § 21).
- **Authoritative raw evidence** is the per-run `_perf_runs/<bootdir>/run_aggregate.json` (full spike
  artifact incl. raw arrays); `_perf_runs/perf_index.jsonl` is the run index.
- **No skip/fail/local result overwrote a pass aggregate** this slice
  (`redirected_from_path=null`, `overwrite_refused=false` on both R=5 aggregates).

---

## 8. Open confounds / backlog (disclosed limitations, not blockers)

- **TCP_NODELAY** expected (`tcp_nodelay_expected=true`) but **not verified**
  (`tcp_nodelay_verified=false`) — Nagle/delayed-ACK remains a possible QD1 confound.
- **Parcel coalescing enabled**, no coalescing-disabled control run; coalescing interval/buffer not
  exposed (`parcel_coalescing_mode="unknown"`).
- **Parcel-pool affinity/contention unknown** (`parcel_pool_threads="2"`, affinity/contention
  fields `unknown`).
- **Live endpoint binding / return-path verification not proven** (`endpoints_bound_subnet_verified`
  and `return_path_interface_verified` are `null`; binding is inferred from advertise-match +
  bidirectional reachability).
- **No loopback decomposition control yet** (same-node two-locality TCP run to separate HPX
  parcel-stack/scheduler cost from the inter-node leg); fields reserved, not run.

---

## 9. Claim hygiene

**Allowed:**
- exp58 provides **Rostam-specific R=5 clean-path characterization** of a two-node HPX TCP island.
- Ray-supervised and Ray-free HPX **steady-state action measurements overlap within across-island
  jitter**.
- The result **supports the Ray-as-control-plane / HPX-as-action-path separation** (Ray not in the
  action/data path).

**Forbidden:**
- No Ray-vs-RayX speedup claim.
- No "HPX beats Ray" claim.
- No broad benchmark claim.
- No network/fabric performance claim.
- No production/API claim.
- No HPX fault-tolerance claim.
- No Ray failure-recovery claim.

---

## Roadmap (4-part)

### 1. Interpretation
The instrument is sound (ns timing, aggregate/per-action means agree to ~tens of ns, idle backoff
disabled, performance governor, hardened/atomic artifact writes, 10/10 valid islands). The
substantive reading is **negative-by-design and that is the point**: with Ray restricted to
launch/control, the Class-B steady-state QD1 floor and the pipeline throughput are
**statistically indistinguishable** between the Ray-free and Ray-supervised phases at R=5 — the
median gaps are inside the across-island spread. This **supports** the hypothesis that HPX
steady-state action execution is Ray-independent because Ray never enters the action/data path. It
does **not** establish any speed relationship between Ray and HPX, and the QD1 number is a
QD1 RTT floor, not a general per-action cost.

### 2. Roadmap impact
**Roadmap strengthened.** exp57 proved the supervised clean two-node mechanism (Ray bootstraps /
supervises a clean HPX island; HPX carries the action/data path). exp58 shows that the **measurement
instrument and the steady-state action path remain stable** when Ray supervises launch/control, with
Ray **not** entering the action/data path. This strengthens the future distributed-fabric direction,
but it **remains claim-gated and narrow**: single allocation, loopback-free two-node TCP, closed-
`int64`, no performance/fault-tolerance/multi-node/production/general-fabric claim.

### 3. Updated roadmap positioning
- **In-process HPX-inside-Ray-actors direction:** unchanged by exp58; remains the local-scheduling /
  nonblocking-lane / native-composition / Python-boundary / serving-shaped track.
- **Future distributed-fabric direction (where exp58 sits):** exp49 (Ray-free connect-mode lifecycle)
  → exp50–51 (ungraceful loss / stale-locality shutdown / whole-island restart boundary) → exp52
  (Ray bootstraps clean island) → exp53–55 (supervised restart / poison detection — separate
  failure track) → exp56 (two-node TCP parcelport) → exp57 (Ray/Slurm-supervised clean two-node
  island, mechanism) → **exp58 (clean-path R=5 characterization: instrument + steady-state action
  path stable under Ray supervision, Ray out of the action/data path)**. Still single-node-pair,
  loopback TCP-free two-node, closed-`int64`; no performance/fabric claim is licensed.

### 4. One concrete next step
**exp59 — Ray actor baseline vs HPX action path** (then **exp60 — whole-island failure/restart**).
With the clean two-node mechanism (exp57) and its R=5 characterization (exp58) in hand, the next step
is exp59: characterize the native Ray actor call path for the same closed-`int64` micro-workload and
place it beside this exp58 HPX action path (path characterization only, disclosed measurement planes,
no broad Ray-vs-RayX/HPX claim). Whole-island failure/restart moves to **exp60**, on this same
validated spike/allocation shape, keeping the same fences (TCP parcelport, closed-`int64`,
Rostam-specific, no fault-tolerance/recovery claim beyond the mechanism observed).
