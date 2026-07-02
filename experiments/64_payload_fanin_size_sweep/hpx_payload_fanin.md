# exp64 -- payload-carrying fanin size sweep (poll-mode gather baseline)

**Status:** Slice 4 complete — matched payload ladder **band**, R=5 / measured=30. Five clean
fresh-allocation islands were aggregated into a gated band that earns `evidence_grade="matched_band_r5"`
with **within-arm** p50/p90 payload-size distributions per arm. `same_axis_comparison=True` is a
**structural-correlation flag only**, `distributional_evidence=True` is **within-arm only**, and the
stronger `distributional_payload_ladder` grade stays **blocked** (HPX serialization runtime path not
observed). Still **no cross-arm timing arithmetic**, no ratio/speedup/difference/winner; every fence
locked False. HPX remains the `root_flat_gather_poll` poll-gather baseline (not exp63 native
composition). (Slice 3 established the structural R=1 manifest; see the sections below.)

## What this experiment is

exp64 extends the exp62 same-axis distributed direction from **scalar** fanout/fanin to a **payload
size** axis that exp62 explicitly did not cover. Each remote leaf returns `S` opaque **payload bytes**
(plus its closed-int64 scalar value and locality witness) back across the **Python caller boundary**.
The payload is a deterministic synthetic byte pattern with a closed digest oracle.

It is **experiment-only**: not the shipped `rayx.runtime` API, not an object store, not arbitrary
Python execution, not real inference, not a production runtime.

## Explicit poll-mode baseline (durable framing)

Every exp64 HPX result is labeled as the **proven poll-mode gather baseline**:

- HPX composition mode: `root_flat_gather_poll`.
- Kind: **naive all-to-root gather** over a bounded `is_ready` poll. The root receives `O(N*S)` bytes.
- This is the **reliable known-good baseline, NOT "the HPX answer"** and **NOT** idiomatic/native
  passive `when_all` composition.

The native/passive `when_all` + collective/tree reduction direction stays in the **exp63** diagnostic
arc. exp64 does **not** gate on exp63 resolving; it deliberately uses the baseline that already works
cross-node.

**Future HPX-native work (not exp64 evidence):** tree-of-partials, `hpx::collectives` tree reduction
(folding `O(S)` at the root), and MPI / LCI / InfiniBand transport variants.

## Payload vehicle and transport metadata (recorded, not assumed)

- C++ payload representation (Slice 1+): `hpx::serialization::serialize_buffer<char>` -- **not** a naive
  `std::vector<char>` transport-facing type (which would benchmark the serializer, not the wire).
- `serialize_buffer` init mode: recorded at Slice 1.
- Parcelport / transport: TCP on `eno16` / `10.42.5.x` (the exp62/exp63 subnet).
- Zero-copy / array optimization, coalescing, and any size thresholds: **recorded at runtime**, not
  assumed. On TCP the zero-copy/RMA benefit is limited; that is a property to record, not to hide.

## Timing boundary (durable)

To measure **response payload size at the Python caller boundary**:

- The single timed blocking call returns the payload **bytes** (+ scalar values + witnesses) to Python.
- Python folds and checks the scalar oracle and the payload digest **after** timing, **outside** the
  RTT window, **identically for both arms** (HPX and Ray).
- The digest is **not** folded inside HPX/Ray -- otherwise the timed result would no longer carry the
  response payload size across the boundary.
- The post-timing digest-check cost may be recorded separately, but is **not** part of the boundary RTT.

## Closed oracles (canonical; a future C++ leaf must match these exactly)

- Scalar: `leaf_value(x, i) = (x ^ 0x52415958) + (i << 1)` (mod 2^64), matching exp62/exp63.
- Composite (order-independent): `composite_oracle(x, n) = sum_i leaf_value(x, i)` (mod 2^64).
- Payload byte: `payload_byte(x, i, k) = low 8 bits of (uint64(leaf_value(x,i)) + k)` -- a per-leaf
  sawtooth of period 256.
- Payload digest: `payload_digest(x, n, S) = sum over all leaves and bytes of payload_byte` (mod 2^64).
  Computed in `O(n * min(S, 256))` via the full-period identity and cross-checked against the naive
  byte-by-byte reference in the selftest.

## Size ladder

`[0, 64, 1024, 16384, 262144]`

- `0`  -- **poll + RTT + fixed-machinery floor**, zero payload (first-class control).
- `64`, `1024` -- small payload.
- `16384` -- serialization / transport starts to matter.
- `262144` -- stress point where **HPX and/or Ray transport regime changes** may appear.

## Ray transport caveat

Ray may change transport regime at larger returned-object sizes (likely object-store / plasma
behavior). **Do not infer an HPX-only cause from any observed kink** unless Ray transport metadata
supports it. Both systems can change regime with size, at different thresholds; the write-up must name
both.

## Fences (locked False for the exp64 arc unless a future experiment explicitly permits)

`speedup_computed`, `ratio_reported`, `arms_differenced`, `placement_bands_differenced`,
`same_axis_comparison`. Slice 0 has no runtime and no comparison, so `same_axis_comparison=false`.

## Slice plan

- **Slice 0 (done):** pure Python oracle + corrected design record + selftest. Runs anywhere.
- **Slice 1 (this):** native leaf action carrying a `serialize_buffer<char>` payload over
  `root_flat_gather_poll`; HPX-only 3-node all-remote mechanism smoke; ext + connector built from the
  identical serialized `payload_leaf_record` (registration discipline: `payload_action.hpp` included in
  exactly one TU per binary). Smoke sizes S=0 then S=262144.
- **Slice 2 (done):** Ray matched payload smoke -- the Ray arm at the same Python caller boundary
  (coordinator control-plane only, N leaves hard-pinned 4/4 across two remote workers, payload bytes
  returned, Python-side fold after timing). Smoke sizes S=0 and S=262144. Mechanism smoke only; no
  comparison; `same_axis_comparison` stays False.
- **Slice 3 (done):** matched payload ladder `[0, 64, 1024, 16384, 262144]` -- both arms in one
  allocation (HPX then Ray, Ray driver step `--cpu-bind=none`); a gated manifest that stays
  no-ratio/no-speedup/no-winner and flips `same_axis_comparison` True only if all correlation gates pass.
  Ran clean on job 159384 (medusa[11-13]); see [Slice 3 below](#slice-3--matched-payload-ladder-structural-r1-manifest).
- **Slice 4 (done):** matched payload ladder **band** -- R=5 fresh-allocation islands, measured=30,
  aggregated by a pure band that earns `matched_band_r5` with **within-arm** p50/p90 distributions,
  still no ratio/speedup/winner and no cross-arm arithmetic. Ran clean on band `band20260702_174335`
  (jobs 159385/159386/159388/159389/159390); see
  [Slice 4 below](#slice-4--matched-payload-ladder-band-r5--measured30).

## Slice 1 mechanism (implemented)

- `exp64_payload_leaf(x, i, S)` returns `payload_leaf_record{int64 value; uint32 locality;
  serialize_buffer<char> payload}` filled with `payload_byte(x,i,k)`; registered as
  `exp64_payload_leaf_action`.
- The root dispatches N leaves round-robin to >=2 cached REMOTE localities, gathers with the bounded
  `is_ready` poll, and returns each leaf's `(value, locality, S bytes)` to Python.
- The runner TIMES the single call, then folds + checks the scalar and payload digests AFTER timing
  (`digest_check_ms` recorded separately). Per-call gates: N dispatched, `leaves_local=0`,
  `leaves_remote=N`, every remote locality covered, witness count N, scalar oracle, payload byte
  length, payload digest, no dispatch timeout. Structural gates add connector joined/served/graceful,
  cpuset-not-collapsed, TCP_NODELAY verified, and the fence/boundary labels.

## Slice 2 mechanism — Ray matched smoke (implemented)

- The Ray arm mirrors exp62 Slice 4b: head/coordinator on node A (`num_cpus=0`, ZERO leaves) + two
  remote workers; N=8 leaves hard-pinned round-robin across the two remote Ray worker node ids (4/4,
  all leaves remote, `NodeAffinitySchedulingStrategy(soft=False)`). One blocking
  `ray.get(coordinator.remote(x, n, payload_bytes))` per timed iteration returns the RAW payload bytes
  to Python; Python folds/checks the scalar oracle + payload digest AFTER timing, OUTSIDE the RTT window
  — the coordinator never folds the digest in the timed call.
- On job 159228 (medusa[11-13], head/coordinator medusa11, workers medusa12/medusa13, Ray 2.55.1, N=8,
  prewarm=3, measured=5), both S=0 and S=262144 passed 5/5 with every payload gate True
  (coordinator-on-head / `num_cpus=0` / zero leaves, `leaves_local=0`, `leaves_remote=8`, 4/4, scalar
  oracle, payload byte length, post-timing digest, no dispatch timeout, no orphans). Ray-on-Slurm note:
  the driver step must run `--cpu-bind=none` or the nested Ray head GCS starves during startup. See
  [`ray_matched_smoke.md`](ray_matched_smoke.md) for the full gate table, the operational note, and
  claim discipline. Mechanism-smoke evidence only; not the ladder; no ratio/speedup/winner;
  `same_axis_comparison=false`.

## Slice 3 — matched payload ladder (structural R=1 manifest)

Slice 3 runs the full payload ladder on **both** arms inside **one** allocation and pairs the per-size
artifacts with a pure manifest. It is a **structural matched-ladder pass, not distributional evidence**:
`evidence_grade="structural_r1"`, and `same_axis_comparison=True` is earned **only** as a
structural-correlation flag through the manifest when every gate passes. **No cross-arm timing
arithmetic was computed** and **no ratio, speedup, difference, or winner is implied.** The two arms use
**intentionally different runtime paths** — HPX is the `root_flat_gather_poll` polled payload-gather
baseline over the TCP parcelport; Ray is a coordinator plus Ray object transport. The honest cross-arm
anchor is not timing but the **closed oracle / expected digest matching at every payload size**.

### Run shape (job 159384)

- One fresh **3-node exclusive** medusa allocation: **medusa11, medusa12, medusa13**, subnet
  `10.42.5.x`. First compute node / driver = **medusa11**.
- Order: **HPX phase first**, then **Ray phase** (driver step `--cpu-bind=none`), then the **manifest
  phase** after both arms.
- Build on medusa11: `cmake --build build` succeeded; `payload_connector` and
  `payload_ext.cpython-312-x86_64-linux-gnu.so` built. **C++ source unchanged from Slice 1/2** — Slice 3
  changed only the Python / manifest logic.
- Ladder `[0, 64, 1024, 16384, 262144]`. Settings: `N=8`, `prewarm=3`, `measured=5`, `n-remote=2`;
  HPX `dispatch-timeout-s=8.0`, `serve-timeout=600`; Ray `dispatch-timeout-s=30`.

### HPX arm — per size (within-arm observation only; **not** compared to Ray)

All five sizes pass; all leaves remote over localities `[1, 2]`; 4/4 distribution; clean connector
teardown. RTT columns are within-arm observations, **never** a cross-arm operand.

| S (bytes) | calls | mean ms | min ms | max ms | pass | expected_digest |
|--:|--:|--:|--:|--:|:-:|--:|
| 0 | 5 | 0.302 | 0.249 | 0.356 | pass | 0 |
| 64 | 5 | 0.310 | 0.256 | 0.340 | pass | 68352 |
| 1024 | 5 | 0.349 | 0.294 | 0.385 | pass | 1044480 |
| 16384 | 5 | 1.509 | 1.439 | 1.576 | pass | 16711680 |
| 262144 | 5 | 19.759 | 18.725 | 21.000 | pass | 267386880 |

### Ray arm — per size (within-arm observation only; **not** compared to HPX)

All five sizes pass; all leaves remote across two worker node ids; 4/4 distribution; `no_orphans=True`;
Ray 2.55.1. Expected digests match the HPX/Python oracle at every size.

| S (bytes) | calls | mean ms | min ms | max ms | pass | expected_digest |
|--:|--:|--:|--:|--:|:-:|--:|
| 0 | 5 | 7.697 | 4.372 | 19.938 | pass | 0 |
| 64 | 5 | 4.153 | 4.005 | 4.330 | pass | 68352 |
| 1024 | 5 | 4.273 | 4.184 | 4.351 | pass | 1044480 |
| 16384 | 5 | 6.915 | 6.394 | 7.817 | pass | 16711680 |
| 262144 | 5 | 60.373 | 54.779 | 73.474 | pass | 267386880 |

> The two tables sit side by side for provenance only. Per the fences, the manifest computes no
> difference, ratio, speedup, or winner between them, and none is implied here. The arms took different
> runtime paths at different transport thresholds; the only legitimate cross-arm statement is that the
> **closed digest matches** (HPX == Ray == Python oracle) at S = 0 / 64 / 1024 / 16384 / 262144.

### Manifest result (`exp64_payload_ladder_159384_manifest.json`)

- `evidence_grade="structural_r1"`, `same_axis_comparison=True` (structural-correlation flag only).
- `overall_manifest_pass=True`, `validator_ok=True`, `validator_problems=[]`.
- `no_cross_arm_timing_computed=True`.
- Fences: `arms_differenced=False`, `ratio_reported=False`, `speedup_computed=False`,
  `placement_bands_differenced=False`, `distributional_evidence=False`,
  `percentiles_evidence_ready=False`.
- **26 / 26 correlation gates passed.** Including the HPX/runtime-review-added gates:
  `single_slurm_job_identity`, `node_set_matched`, `transport_family_hpx_tcp_on_subnet`,
  `transport_family_ray_on_subnet`, `hpx_residue_clear_before_ray`, `ray_no_orphan_proof`,
  `hpx_phase_affinity_recorded`, `ray_phase_affinity_recorded`, `prewarm_excluded_from_timed_both_arms`,
  `both_arms_all_remote_all_sizes`, `both_arms_balanced_distribution_all_sizes`,
  `expected_digest_matched_every_size`, `all_fences_false`, `no_forbidden_keys`.

### Recorded provenance (recorded, not equalized, not differenced)

- **HPX residue clear before the Ray phase:** `hpx_teardown_clean=True`, `connector_lifecycle_ok=True`.
- **Ray no-orphan after teardown:** `no_orphan_proof=True`.
- **Affinity asymmetry — recorded, not equalized or differenced** (exactly the asymmetry the HPX/runtime
  review flagged before implementation): HPX root effective CPU binding `[0,2,4,6,8,10,12,14]` (the
  `-c 8` balanced-bind root step) vs Ray driver effective CPU binding `[0..39]` under `--cpu-bind=none`.
  This is captured as provenance precisely so it cannot silently bias a (non-existent) cross-arm compare.
- **HPX transport/composition:** `root_flat_gather_poll`; TCP parcelport; `tcp_nodelay=True`;
  `serialize_buffer<char>`; `zero_copy_optimization` and `coalescing` recorded as `not_observed`
  (honest: not exposed by the current harness on TCP).
- **Ray transport/placement:** `ray_object_transport`; `object_return_path` and `plasma_engagement`
  recorded as `not_observed` (inline-vs-plasma not exposed by the public Ray API here); `resource_map`
  captured; head/coordinator `num_cpus=0`; hard `NodeAffinitySchedulingStrategy(soft=False)`.
- **Both arms:** `boundary="python_caller_monotonic_ns_around_blocking_call"`, `clock="monotonic_ns"`,
  `selected_subnet=10.42.5.`.

### Artifacts

Copyback directory (confirmed gitignored):
`experiments/64_payload_fanin_size_sweep/_exp64_runs/payload_ladder_copyback_159384/`

- 10 per-size arm JSONs (`exp64_payload_smoke_159384_S{0,64,1024,16384,262144}_{hpx,ray}.json`),
- `exp64_payload_ladder_159384_manifest.json`,
- 3 phase logs (`hpx_ladder_159384.log`, `ray_ladder_159384.log`, `manifest_159384.log`).

### Interpretation

- Slice 3 validates the **matched payload-ladder machinery** across both arms at **R=1**: both arms
  cover the full ladder, and the manifest **structurally correlates** them across every payload size with
  all 26 gates green.
- This **unblocks** a later **R≥5 / measured≥30** matched payload band if distributional evidence is
  wanted; the machinery, gates, and provenance are now in place to build one honestly.
- It **does not** establish performance, percentiles, superiority, or a winner, and it computes no
  cross-arm timing. Within-arm RTTs are observations only.
- It **does not** convert HPX's payload arm into exp63 native composition; the HPX arm remains the
  **polled payload gather baseline** (`root_flat_gather_poll`), deliberately separate from the exp63
  native-passive/collective arc.
- It **does not** claim real inference or model output; the payload is a synthetic closed-digest byte
  pattern.

### Roadmap impact

- **Roadmap strengthened.** exp64 now has **both arms** and a **gated structural ladder manifest** across
  all planned payload sizes, at a clean structural checkpoint.
- **Next recommended step:** either design a later exp64 **R≥5 / measured≥30** matched payload band (with
  `evidence_grade` promoted only when the band is actually distributional), or pause exp64 at this
  structural checkpoint. Do **not** update README / `docs/evidence_index.md` yet unless explicitly asked.

## Slice 4 — matched payload ladder band (R=5 / measured=30)

Slice 4 upgrades Slice 3 from **one** structural ladder to a clean **R=5 matched band**. Five independent
fresh-allocation islands each ran the full ladder on both arms; a pure aggregate paired them into a band
that earns `evidence_grade="matched_band_r5"`. `same_axis_comparison=True` remains a **structural-correlation
flag only**; `distributional_evidence=True` is **within-arm only** (each runtime's own payload-size RTT
distribution); `percentiles_evidence_ready=True` applies to **p50/p90 only** (`p99_evidence_ready=False` at
measured=30). The stronger `distributional_payload_ladder_ready=False`, blocked by
`hpx_serialization_runtime_path_not_observed` and `hpx_poll_gather_baseline`. **No ratio, speedup,
difference, or winner is computed or implied.** The two arms are **intentionally different runtime paths** --
HPX is `root_flat_gather_poll` poll-gather over the TCP parcelport (still **not** exp63 native composition);
Ray is a coordinator plus Ray object transport. The honest cross-arm anchor stays the closed oracle / digest
correctness, **not** timing.

### Run shape (band `band20260702_174335`)

- **R=5 fresh exclusive allocations**, jobs 159385 / 159386 / 159388 / 159389 / 159390; all on
  medusa11/medusa12/medusa13 (IPs .41/.42/.43 on `10.42.5.x`), driver = medusa11. Every island was a fresh
  allocation, but the scheduler reused the **same three physical nodes** each time -- so this band shows
  **repeatability under fresh allocations with low placement diversity**, not broad cluster-wide placement
  variance.
- Order per island: HPX phase first, Ray phase second (`--cpu-bind=none`), then the per-island manifest.
  Pure band aggregate after all five islands.
- Build: island 1 built 4/4 (`payload_connector` + `payload_ext.cpython-312-x86_64-linux-gnu.so`); islands
  2--5 reported `ninja: no work to do`. **C++ unchanged from Slice 1--3** -- Slice 4 changed only the Python
  band/aggregate logic.
- Settings: ladder `[0,64,1024,16384,262144]`, `N=8`, `n-remote=2`, `prewarm=5`, `measured=30`; HPX
  `serve-timeout=600`, `dispatch-timeout-s=8.0`; Ray `dispatch-timeout-s=30`.

### HPX per island (all pass)

All 5 islands pass; all 5 sizes pass in every island; 30 calls per size; all leaves remote over localities
`[1, 2]`; 4/4 distribution; clean connector teardown. Within-arm mean-RTT sanity only: S=0 ≈ 0.29--0.33 ms,
S=262144 ≈ 19.3--19.6 ms. **Do not compare to Ray.**

### Ray per island (all pass)

All 5 islands pass; all 5 sizes pass in every island; 30 calls per size; all leaves remote across two worker
node ids; 4/4 distribution; `no_orphans=True`; Ray 2.55.1. Within-arm mean-RTT sanity only: S=0 ≈ 4.0--4.5 ms,
S=262144 ≈ 55.2--55.8 ms. **Do not compare to HPX.**

### Per-island manifests (all 5)

`overall_manifest_pass=True`, `same_axis_comparison=True`, `validator_ok=True`, `evidence_grade=structural_r1`,
`measured_ge_required_both_arms=True`, **27/27 gates**, no gates failed.

### Band aggregate (`exp64_payload_band_band20260702_174335_aggregate.json`)

- `overall_band_pass=True`, `evidence_grade=matched_band_r5`, `validator_ok=True`, `validator_problems=[]`.
- `same_axis_comparison=True` (structural-correlation flag only); `distributional_evidence=True` (within-arm
  only); `percentiles_evidence_ready=True` (p50/p90); `p99_evidence_ready=False`.
- `distributional_payload_ladder_ready=False`, blocked by
  `["hpx_serialization_runtime_path_not_observed", "hpx_poll_gather_baseline"]`.
- `no_cross_arm_timing_computed=True`.
- **12 / 12 band gates pass:** `islands_present_ge_required`, `band_id_present`, `all_islands_manifest_pass`,
  `all_islands_same_axis`, `all_islands_validator_ok`, `all_islands_clean_quality`, `all_islands_full_ladder`,
  `all_islands_measured_ge_required`, `structural_params_consistent_across_islands`,
  `island_independence_declared`, `no_forbidden_keys`, `no_cross_arm_arithmetic`.
- **Fences all False:** `arms_differenced`, `ratio_reported`, `speedup_computed`, `placement_bands_differenced`,
  `islands_cherry_picked`.
- **Islands quality:** 5/5 `clean`, no reasons, **none cherry-picked**.

### Within-arm p50/p90 (observation only -- keyed per arm; the two tables are never differenced)

Across-island median of the per-island p50/p90, per arm. These are **within-arm observations only** -- the
band computes no ratio, difference, speedup, or winner between the arms, and none is implied. The arms take
intentionally different runtime paths at different transport thresholds; the only legitimate cross-arm
statement is the closed-digest correctness anchor.

HPX (poll-gather baseline; root deserializes/gathers O(N·S) on its pinned cores):

| S (bytes) | p50 median (ms) | p90 median (ms) |
|--:|--:|--:|
| 0 | 0.299 | 0.335 |
| 64 | 0.290 | 0.317 |
| 1024 | 0.326 | 0.388 |
| 16384 | 1.453 | 1.516 |
| 262144 | 19.518 | 20.552 |

Ray (coordinator + Ray object transport):

| S (bytes) | p50 median (ms) | p90 median (ms) |
|--:|--:|--:|
| 0 | 4.225 | 4.821 |
| 64 | 4.191 | 4.580 |
| 1024 | 4.392 | 4.667 |
| 16384 | 6.850 | 7.153 |
| 262144 | 55.378 | 57.370 |

### Variability / modality flags (coarse; not formal distributional tests)

- HPX: `any_high_variability=False` at every size; `any_multimodal_suspected=True` only at **S=16384**.
- Ray: `any_high_variability=True` only at **S=64**; `any_multimodal_suspected=False` at every size.

These are coarse within-arm flags (a CV threshold and a tail-ratio proxy), not statistical modality tests, and
carry no cross-arm meaning.

### Connector anomaly witness (all 5 islands)

`connector_lifecycle_ok=True`, `connector_shutdown_reason=served_signal`, `serve_timeout_expired_any=False`,
`connector_stayed_alive_until_root_done=True`, `late_parcel_after_shutdown_detected=not_observed`,
`heartbeat_anomaly_detected=not_observed`. Interpretation: the longer `measured=30` serve window held clean --
the exp63 connector-lifetime fault class did **not** recur.

### HPX provenance (recorded, not equalized, not differenced)

- **Poll:** `root_flat_gather_poll`, `bounded_is_ready_poll_sleep_for`, interval 50 µs,
  `hpx_not_exp63_native_composition=true`.
- **Runtime:** bind `balanced`, connector bind `none`, root cpuset `[0,2,4,6,8,10,12,14]`,
  `parcel_pool_size=2`, `message_handlers=1`, `hpx_threads="unknown"` (config key absent -- recorded honestly,
  not fabricated).
- **NUMA/NIC (real hardware, all islands):** `selected_iface=eno16`, `nic_numa_node=0`,
  `root_core_numa_nodes=[0]`, `numa_nic_colocated=True`.
- **Serialization:** `serialize_buffer<char>`; config-level `zero_copy_optimization=1`, `array_optimization=1`,
  `message_handlers=1`, `max_message_size=1000000000`; **`zero_copy_runtime_path_taken=not_observed`**. Config
  flags are observed, but the per-call zero-copy path **taken** is not -- this is exactly what **blocks** the
  stronger `distributional_payload_ladder` grade.

### Ray provenance

`transport_family=ray_object_transport`, `object_return_path=not_observed`, `plasma_engagement=not_observed`,
head/coordinator `num_cpus=0`, `no_orphan_proof=True`; `resource_map` captured (CPU 16.0, `object_store_memory`
present, three worker node keys on `10.42.5.x`).

### Artifacts

Copyback directory (confirmed gitignored):
`experiments/64_payload_fanin_size_sweep/_exp64_runs/payload_band_copyback_band20260702_174335/`

- 5 island directories, each with 10 arm JSONs + the island manifest + build/hpx/ray/manifest logs + a meta
  file;
- the band aggregate `exp64_payload_band_band20260702_174335_aggregate.json` + `band_aggregate_*.log`.

### Interpretation

**What Slice 4 establishes:**

- The matched payload ladder band runs **clean across 5 fresh allocations**.
- Both arms pass **all payload sizes in every island** (30 calls/size).
- The structural same-axis manifest **repeats across R=5**.
- **Within-arm** p50/p90 payload-size distributions are now available per arm.

**What Slice 4 does not establish:**

- no HPX-vs-Ray performance comparison; no ratio; no speedup; no winner;
- no p99 evidence; no full `distributional_payload_ladder`;
- no HPX native-composition payload path (HPX stays the poll-gather baseline);
- no real inference / model-output payload claim.

**Important caveat:** although there were 5 fresh allocations, the scheduler **reused the same physical nodes**
each time. So the band shows **repeatability under fresh allocations with low placement diversity**, not broad
cluster-wide placement variance. Across-island spread here is dominated by run-to-run jitter, not placement.

### Roadmap impact

- **Roadmap strengthened.** exp64 now has a coherent arc: Slice 1 HPX payload smoke → Slice 2 Ray matched
  smoke → Slice 3 structural R=1 manifest → Slice 4 R=5 matched band aggregate.
- **Next recommended step:** a future stronger slice could **observe the HPX runtime serialization
  (zero-copy) path** -- the one gate blocking `distributional_payload_ladder` -- or move the HPX payload arm
  from the poll-gather baseline to an exp63-style native-composition payload path. Do **not** update README /
  `docs/evidence_index.md` yet unless explicitly asked.

## Files

- `shared_payload.hpp` -- pure oracle (leaf_value / composite_oracle / payload_byte); mirrors the
  Python oracle exactly.
- `payload_action.hpp` -- `payload_leaf_record` (with `serialize_buffer<char>` payload) + the
  `exp64_payload_leaf_action` HPX plain action (ONE TU per binary).
- `payload_ext.cpp` -- pybind `payload_ext`: embedded HPX root, `fanout_fanin_payload_remote`
  (root_flat_gather_poll, returns payload bytes), config provenance.
- `payload_connector.cpp` -- standalone connect-mode remote locality registering the same action.
- `CMakeLists.txt` -- builds `payload_ext` + `payload_connector`.
- `run_exp64_payload.py` -- oracles, design record, gates, and phase dispatch (`selftest` pure;
  `hpx-payload-remote-smoke` Slice 1 + `ray-payload-remote-smoke` Slice 2 hardware; the pure
  `payload-ladder-manifest` Slice 3 pairing/validator over `--job`; the pure `payload-band-aggregate`
  Slice 4 R-island within-arm band over `--band-id`; all skip cleanly off-cluster).
- `selftest_slice0.py` -- pure oracle + design-label + fence + gate + off-cluster-skip checks.
- `.gitignore` -- keeps `_exp64_runs/` raw outputs / build products untracked.
