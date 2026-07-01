# exp62 — Same-axis Python-boundary distributed fanout/fanin: Ray vs experiment-only HPX/RayX

## Why exp62 exists

exp61 established the first **fair same-axis** measurement: a single QD1 closed-`int64`
remote call, timed for both runtimes at the **same Python caller boundary**
(`perf_counter_ns` around one blocking call). That removed the measurement-plane
mismatch of exp58/exp59 (HPX timed from C++, Ray from Python), but it exercised only a
**single scalar remote call** per timed iteration.

exp62 extends that same-axis methodology from one scalar call to a **semantic
distributed workload**: one outer blocking Python call

```
fanout_fanin(x, N) -> int64
```

that internally dispatches **N leaf actions to a remote locality** and **reduces** them
to a single closed-`int64` value. The workload is still QD1 at the outer Python caller
boundary (one blocking call per timed iteration), but each call now does real distributed
fanout and a fan-in reduction, so the result is no longer attributable to a single-RTT
artifact.

The oracle is **placement-independent** so it can never be used to "prove" placement:

```
leaf(x, i)      = (x ^ 0x52415958) + (i << 1)
composite(x, N) = sum over i in [0, N) of leaf(x, i)   (int64, mod 2^64, order-independent)
```

Distribution is proven **separately** by a per-leaf locality witness, hard placement
gates, attested transport, and node/locality ids — never by the oracle alone.

Both arms are measured at the **same Python caller boundary**, with matched K/W/prewarm/
clock, the same `inner_fanout_n`, the same `fanout_placement=all-remote`, the same node
pair, and the same selected subnet. The HPX side is an **experiment-only** pybind binding
(`ext.fanout_fanin_remote(x, N)`) under `experiments/`; it is **not** shipped
`rayx.runtime` API and does not give the public RayX API distributed actions.

## Slice history

* **Slice 0 — pure-Python scaffold.** Oracle, per-leaf witness tally, all-remote and
  round-robin placement assignment, fail-closed gates, manifest/band scaffolding, and the
  hard no-ratio/no-speedup fences. Pure Python only (no C++, Ray, HPX, or Rostam); clean
  off-cluster skip behavior.
* **Slice 1 — local HPX mechanism smoke.** Added `shared_fanout.hpp` and `fanout_ext.cpp`
  (plus `CMakeLists.txt`). Single-locality, in-process `hpx::when_all` fanout/reduce. A
  single-locality run can **never** set `same_axis_comparison=true`; oracle parity is
  checked through `build_arm_artifact` / `composite_oracle_correct`.
* **Slice 2 — HPX-only one-remote dry-run.** Added `fanout_action.hpp` and
  `fanout_connector.cpp`. A root locality admits one connector locality over the TCP
  parcelport and dispatches all-remote leaves to it. Proven on Rostam as a mechanism
  dry-run (`localities_distinct`, `leaves_remote=N`, `composite_oracle_correct`). Caveat
  recorded at the time: the connector's effective cpuset collapsed to `[0]` under the
  dry-run resource shape, so Slice 2 proved **mechanism only**, not resource-shape-clean
  timing — which Slice 3 fixes.
* **Slice 3 — matched cross-node R=5 band.** The first trackable, gate-fenced same-axis
  band correlating matched HPX and Ray islands, at **one remote locality**. Superseded by
  Slice 4b as the strongest exp62 same-axis evidence, but preserved below as the
  one-remote-locality predecessor.
* **Slice 4a — HPX-only ≥2-remote-locality mechanism dry-run.** Extends the fanout from
  one remote locality to **two** distinct remote HPX localities on a 3-node allocation
  (root + two connectors). HPX-only mechanism evidence: it does **not** run a Ray arm and
  **does not** set `same_axis_comparison`. Documented below; preserved as mechanism-only.
* **Pre-Slice-4b HPX-native composition spike — informative negative.** `when_all_then_reduce`
  was mathematically correct but stalled to the composed-future timeout cross-node (job
  `158814`); the native modes are kept gated-off and `root_flat_gather_poll` remains the
  proven cross-node composition. Documented below.
* **Slice 4b — matched multi-remote R=5 band.** The first matched same-axis band at **≥2
  remote localities/nodes** (4/4 split), correlating HPX (proven `root_flat_gather_poll`)
  and Ray (coordinator + round-robin leaves). **This is the current strongest exp62
  same-axis distributed evidence and supersedes Slice 3.** Documented below.

## Slice 3 setup

* Job `158809`, one allocation on **medusa00, medusa01** (`--exclusive`).
* Node pair: **medusa00 (root / Ray head) → medusa01 (connector / Ray worker)**.
* Selected subnet `10.42.5.`.
* `inner_fanout_n = 8`, `fanout_placement = all-remote`, **one remote locality**.
* `node_placement = cross_node`.
* `K = 1000`, `W = 100`, `prewarm = 1`.
* `clock = perf_counter_ns`, `measurement_boundary = python_caller_boundary`.
* HPX phase first (root in-process on medusa00, connector launched on medusa01), Ray phase
  second only after HPX is fully down, then five pair manifests and one combined band
  aggregate.

HPX arm resource shape: root 4 HPX threads on medusa00; connector launched with
`--cpus-per-task=8 --cpu-bind=mask_cpu:0x5555 --hpx:threads=8 --hpx:bind=none`. The mask
`0x5555` corresponds to CPUs `{0, 2, 4, 6, 8, 10, 12, 14}` — the 8-core set Slurm
deterministically allocates for `--cpus-per-task=8` on medusa01 (2x20 cores,
ThreadsPerCore=1); a contiguous `0xFF` (CPUs 0-7) is **not** a subset of that step
allocation and is rejected by Slurm. The attested connector cpuset was
`[0, 2, 4, 6, 8, 10, 12, 14]` (8 distinct CPUs, not collapsed) on all five islands.

Ray arm resource shape: head started with `--num-cpus 0` (node A is coordinator/
control-plane only); the coordinator task is submitted with `num_cpus=0` and hard-pinned
to node A (`NodeAffinitySchedulingStrategy(node_id=nid_a, soft=False)`); the N leaf tasks
are hard-pinned to node B (`soft=False`) on a worker with `--num-cpus 8`. One caller-
boundary submission per timed iteration: `ray.get(coordinator.remote(x, N))`.

## Artifacts

Trackable (curated, in the tracked experiment directory):

* `exp62_fanout_band-158809_i1_manifest.json`
* `exp62_fanout_band-158809_i2_manifest.json`
* `exp62_fanout_band-158809_i3_manifest.json`
* `exp62_fanout_band-158809_i4_manifest.json`
* `exp62_fanout_band-158809_i5_manifest.json`
* `exp62_fanout_band_158809_aggregate.json`

Gitignored (raw / provenance, kept locally under `_exp62_runs/band_copyback_158809/`, not
tracked): the ten raw island artifacts `exp62_fanout_band-158809_i{1..5}_{hpx,ray}.json`,
the per-island connector bootstrap dirs (`attest_connect.json` plus the
`joined1`/`served1.ok`/`disconnected1` lifecycle markers), the Ray head/worker logs, and
the batch output log. Build outputs (`build/`, the `fanout_ext` `.so`, `fanout_connector`)
also stay gitignored.

## Gate summary

* All five pair manifests passed (`overall=pass`); all 19 per-manifest correlation gates
  True on every island.
* The combined band aggregate passed: all 13 aggregate gates True, R=5 for both arms.
* Both arms oracle-correct (composite `11040115504`) with matched configs across all
  islands; both arms `leaves_local=0`, `leaves_remote=8`, `witness_leaf_count=8`.
* HPX per island: `hpx_tcp_nodelay_verified=true`, `parcelport_transport=tcp`,
  `connector_cpuset_not_collapsed=true` (8 CPUs), `threads_cover_fanout=true`,
  `no_dispatch_timeout=true`, `timed_out_leaf_count=0`,
  `composition_primitive=hpx::async+is_ready_poll`.
* Ray per island: `hard_placement=true` / `soft=false`, `single_submission=true`,
  `coordinator_single_submission=true`, `leaves_on_target_node=true`,
  `coordinator_on_driver_node=true`, `ray_head_num_cpus=0`, `ray_coordinator_num_cpus=0`,
  `ray_no_dispatch_timeout=true` (bounded `ray.get`, 30 s budget).
* `cross_island_agreement=true` (job / node pair / subnet / K / W / prewarm / clock /
  boundary / N / placement agree across all ten islands).
* Teardown `no_orphans=true`; a post-run scan found no real `raylet`/`gcs_server`/
  `plasma`/`fanout_connector` processes on either node.

**`same_axis_comparison=true`** is earned **purely structurally** — it means every gate
passed across both arms and all islands. It carries no ratio, speedup, ranking, or winner
semantics.

Hard fences (locked false in the aggregate and in every manifest):

* `speedup_computed=false`
* `ratio_reported=false`
* `arms_differenced=false`
* `placement_bands_differenced=false`

## Per-arm band

Each arm is measured at the **same Python caller boundary** (`perf_counter_ns` around one
blocking call), QD1 outer, K=1000 / W=100 / prewarm=1, R=5, all-remote, cross-node. Bands
are across-island medians of per-island percentiles, reported **separately** (the arms are
never differenced, ratioed, or ranked):

| arm (same Python caller boundary) | call primitive | p50 | p90 | p99 | mean |
|---|---|---|---|---|---|
| Ray actor/task path | `ray.get(coordinator.remote(x, N))` | 3640.9 µs | 3895.6 µs | 6407.0 µs | 3718.6 µs |
| experiment-only Python→HPX action path | `ext.fanout_fanin_remote(x, N)` | 345.4 µs | 401.7 µs | 466.2 µs | 359.0 µs |

> For this specific QD1 closed-int64 distributed fanout/fanin microbenchmark, N=8,
> all-remote, one remote locality, cross-node medusa00→medusa01, measured at the same
> Python caller boundary, the experiment-only HPX action path shows a lower RTT band than
> the Ray actor/task path. This is a structurally valid same-axis juxtaposition, not a
> ratio, speedup, or winner claim, and not shipped `rayx.runtime` distributed API.

## Hard caveats

* **One remote locality only.** All N=8 leaves go to a single remote locality — this is
  fanout in count, not fanout across multiple localities. exp62 Slice 3 does not establish
  multi-locality distributed fanout.
* **Experiment-only pybind path.** `ext.fanout_fanin_remote` is an experiment-only binding
  under `experiments/`. It is **not** shipped `rayx.runtime` API and does not give the
  public RayX API distributed actions.
* **Closed-`int64` QD1-outer micro-workload.** Synthetic closed-value work, one blocking
  call per timed iteration. Not real serving, not real inference, no QD>1 / pipeline
  regime.
* **Mechanisms differ.** The HPX arm is a registered C++ action fanned out over the TCP
  parcelport and reduced in C++; the Ray arm is task scheduling plus object-store
  transport with a Python-driver coordinator. Because the execution models differ, the
  bands are reported separately and **no ratio, speedup, or winner** is computed or
  implied.
* Magnitudes are Rostam-allocation-specific and TCP-parcelport-specific.

## Mid-run fixes (provenance)

Two fixes were applied and locally validated before the accepted run; both are confined to
the experiment-only exp62 code (no shipped `rayx.runtime` change):

* **Ray coordinator `num_cpus=0` + bounded `ray.get`.** The head advertises 0 CPUs (node A
  is control-plane only). The coordinator is hard-pinned to node A, so it is submitted with
  `num_cpus=0`; otherwise Ray treats it as a 1-CPU task and, with `soft=False`, it stays
  PENDING forever. The coordinator `ray.get` is bounded by `ray_dispatch_timeout_s` so a
  placement failure fails closed (records an artifact) instead of hanging to the SLURM time
  limit. Leaf tasks remain hard-pinned to node B.
* **HPX watchdog: `when_all().wait_for()` → bounded fine `is_ready` poll.** The initial
  Slice 3 watchdog gathered the leaf futures with `hpx::when_all` and waited on the
  aggregate future with `future::wait_for(dispatch_timeout_s)`. On the cross-node TCP
  parcelport path that aggregate future did not observe leaf completion promptly and blocked
  the **full timeout** every call even though all leaves had completed correctly (observed
  as ~30 s per call). It was reverted to a bounded fine-grained `is_ready` poll over the
  individual leaf futures (50 µs sleep/yield between checks): on success all futures are
  ready and reduced normally; on timeout unready futures are never `get()`'d, only ready
  leaves are collected, and `timed_out_leaf_count` makes the Python gates fail closed. The
  per-call latency went from ~30 s to ~340 µs after the fix. An orphan-check false positive
  (the teardown `pgrep` matched its own `srun` wrapper) was also corrected to filter the
  detector's own command lines.

## Slice 4a — HPX-only ≥2-remote-locality mechanism

Slice 3's headline caveat is **one remote locality**: all N leaves land on a single remote
locality, so it is fanout in *count*, not fanout *across* localities. Slice 4a removes that
specific limitation as an **HPX-only mechanism dry-run** — it proves the ≥2-remote-locality
fanout/fanin path works end-to-end on real hardware. It is **not** a Ray comparison and
**does not** supersede Slice 3 as the strongest same-axis matched evidence.

### Setup

* Job `158813`, one `--exclusive` allocation on **medusa00, medusa01, medusa02**
  (3 nodes, `--cpus-per-task=8`).
* Root: **medusa00** (in-process root locality; runs **zero** leaves).
* Connectors (all-remote round-robin):
  * **medusa01 / 10.42.5.31 / locality 1**
  * **medusa02 / 10.42.5.32 / locality 2**
* Selected subnet `10.42.5.`.
* `node_placement = cross_node_multi_remote`, `placement_mode = all-remote`,
  `n_remote = 2`.
* `composition_primitive = root_flat_gather_reduce` (interim), reduce
  `root_fold_sum_int64`, watchdog `bounded_is_ready_poll_50us`.
* `clock = perf_counter_ns`, `measurement_boundary = python_caller_boundary`,
  `dry_run = true`.

Command:

```
python run_exp62_fanout.py --phase hpx-multi-remote-smoke \
  --n 8 --k 20 --w 5 --n-remote 2 --prefer-subnet 10.42.5. --connector-threads 8
```

Connector resource shape: each connector was launched with `--cpus-per-task=8`; the
attested effective cpuset was `[0, 2, 4, 6, 8, 10, 12, 14]` on **both** medusa01 and
medusa02 (8 distinct CPUs, not collapsed, not `[0]`, threads cover the assigned fanout).
The root cpuset was CPUs `0-23`.

### Gate summary

`overall = pass`, `placement_class = distributed`; all **17** gates True:
`leaves_dispatched_eq_n`, `witness_leaf_count_eq_n`, `distribution_ok`,
`threads_cover_fanout`, `composite_oracle_correct`, `no_dispatch_timeout`,
`prewarm_correct`, `no_error`, `same_boundary`, `remote_join_ok`, `localities_distinct`,
`connector_cpuset_not_collapsed`, `n_remote_localities_ge_2`,
`leaves_per_remote_locality_covers_all`, `all_connector_cpuset_not_collapsed`,
`all_connector_nodelay_true`, `all_connector_lifecycle_ok`.

Structural result:

* `n_remote_localities = 2`, `remote_locality_ids = [1, 2]`.
* `leaves_per_remote_locality = {1: 4, 2: 4}` (every remote locality received ≥1 leaf).
* `leaves_local = 0` (root ran zero leaves), `leaves_remote = 8`, `witness_leaf_count = 8`.
* Composite oracle correct (`measured_value = 11040115504`, matching the Slice 3 composite).
* `no_dispatch_timeout = true`, `timed_out_leaf_count = 0`.
* Both connectors: `transport = tcp`, `tcp_nodelay_verified = true`, `joined = true`,
  `served = true`, `graceful_disconnect = true`.
* Hard fences locked false: `same_axis_comparison = false`, `speedup_computed = false`,
  `ratio_reported = false`, `arms_differenced = false`, `placement_bands_differenced = false`.

### Artifacts and connector lifecycle evidence

Raw run + provenance were copied back to the **gitignored** `_exp62_runs/slice4a_copyback/`
(matches the `exp62_fanout_*_hpx.json` and `_exp62_runs/` ignore patterns — like the Slice 2
one-remote dry-run, Slice 4a has no separately-tracked curated artifact; it is captured in
this write-up and the evidence index):

* `exp62_fanout_hpx-multi-remote-158813-51f6d8723350_hpx.json` (single-run smoke artifact).
* Per-connector bootstrap dirs
  `hpx_multi_remote_smoke_158813_51f6d8723350_c{1,2}_*/`, each with the lifecycle markers:
  `connect.preprobe_ok`, `connect.joined1`, `served1.ok`, `attest_connect.json`,
  `connect.disconnected1`.

### Cleanup / orphan-freedom basis

The allocation was released cleanly (`salloc` relinquished job `158813`) and both connectors
reported `graceful_disconnect = true`. A post-hoc node-level `pgrep` was **not** possible
because medusa compute nodes are not SSH-reachable after the allocation ends; the
orphan-freedom basis is therefore the **graceful-disconnect lifecycle witnesses plus clean
Slurm teardown**, not a post-run process scan.

### Interpretation and caveats

Slice 4a proves the ≥2-remote-locality distribution *mechanism*: a root on medusa00 fans
8 closed-`int64` leaves across two distinct remote HPX localities (4 each), reduces them in
C++ over the TCP parcelport with verified NODELAY and attested non-collapsed 8-CPU cpusets,
with full join/serve/graceful-disconnect lifecycle and zero dispatch timeouts. Keep these
caveats explicit:

* **HPX-only mechanism evidence.** No Ray arm; `same_axis_comparison = false`. Slice 4a is
  **not** a Ray-vs-HPX comparison and **does not supersede Slice 3**.
* **Not real LLM inference, not payload-size evidence.** Closed-`int64` synthetic work,
  QD1 outer, small dry-run K.
* **Not shipped distributed `rayx.runtime`.** `ext.fanout_fanin_remote` remains an
  experiment-only pybind path under `experiments/`.
* **`root_flat_gather_reduce` is an interim stepping stone.** HPX collectives / tree-style
  reduction remain the target for later LLM-shaped reduction.
* **`bounded_is_ready_poll_50us` remains known experiment workaround / design debt** before
  payload / collective work.

## Pre-Slice-4b HPX-native composition spike (informative negative)

Before building the Slice 4b Ray matched band, an HPX-expert review flagged that the interim HPX
fan-in — a root flat-gather of the leaf futures with a bounded `is_ready` poll watchdog
(`root_flat_gather_poll`) — is not HPX-native and should not be frozen into the comparison. A spike
added selectable composition modes to the experiment-only `fanout_ext` and tried to replace the poll
with a true future continuation:

* `root_flat_gather_poll` — the interim, **proven** cross-node path (Slice 4a job `158813`).
* `when_all_then_reduce` — HPX-native: `hpx::when_all(...).then(reduce)`, bounded by a single
  `future::wait_for` on the composed future (no success-path polling).
* `dataflow_reduce` — the same via `hpx::dataflow`.

The native modes run the closure on an HPX thread and record honest composition provenance
(`composition_primitive`, `watchdog`, `polled_in_success_path`, `hpx_native_composition`,
`composition_ran_on_hpx_thread`) plus a fail-closed `composition_provenance_consistent` gate. Local
single-locality `hpx-local-smoke` with `when_all_then_reduce` passes off-cluster (correct oracle,
`hpx_native_composition=true`, `polled_in_success_path=false`).

**Cross-node result — informative negative (Rostam job `158814`,** `hpx-multi-remote-smoke`,
`when_all_then_reduce`, medusa00 root, medusa01/medusa02 connectors, N=8/K=20/W=5). The composition
was **mathematically correct** (composite oracle `11040115504`, leaves split 4/4 across both remote
localities, `witness_leaf_count=8`, `leaves_local=0`, `timed_out_leaf_count=0`) and the composition
provenance recorded correctly (`composition_primitive=when_all_then_reduce`,
`watchdog=composed_future_wait_for`, `polled_in_success_path=false`, `hpx_native_composition=true`,
`composition_ran_on_hpx_thread=true`, `composition_provenance_consistent=true`). **But the composed
future stalled to `dispatch_timeout_s` (~30 s per call)**: the single recorded timed call measured
~30.0 s (= the timeout), `no_dispatch_timeout=false`, and the run then failed with
`RuntimeError: Operation not permitted` and abnormal connector disconnect (`overall=fail`). The
`when_all(...).then(...)` + `future::wait_for` path **reproduced the same cross-node readiness/progress
issue** that motivated the original poll workaround, rather than fixing it. The root cause is not
pinned down beyond a **cross-node readiness/progress issue in this setup** (a passive wait is not
woken promptly by parcelport completion here; the poll path makes progress because the yielding
thread lets completion handlers run).

Consequences:

* `root_flat_gather_poll` **remains the proven cross-node composition** and is the basis for Slice 4b
  and any ≥2-locality matched band. It stays the default.
* The native modes are **kept in-tree, gated off** (default `root_flat_gather_poll`), flagged
  `native_mode_experimental=true` / `cross_node_composition_validated=false` in provenance, and any
  cross-node runner emits a loud warning when a native mode is selected. They are retained for future
  debugging, not for Slice 4b.
* **HPX collectives / tree-style reduction** (e.g. `hpx::collectives::reduce`, which drives its own
  cross-locality progress) remain the target for a **later** HPX-native reduction spike; the passive
  `when_all().then()` continuation is not that mechanism.

## Slice 4b — matched multi-remote R=5 band (current strongest exp62 same-axis evidence)

Slice 4b is the first **matched same-axis band at ≥2 remote localities/nodes**. It removes
Slice 3's one-remote-locality limitation on **both** arms simultaneously: the HPX arm fans out
across two remote HPX localities on the proven `root_flat_gather_poll` composition, and the Ray
arm fans out across two remote Ray worker nodes from a coordinator that runs zero leaves. Both
arms are measured at the **same Python caller boundary** with matched K/W/prewarm/clock and a
matched 3-node topology. **This supersedes Slice 3 as the strongest exp62 same-axis distributed
evidence.**

### Setup

* Job `158817`, one `--exclusive` allocation on **medusa00, medusa01, medusa02**
  (`--cpus-per-task=8`).
* Root / Ray head / coordinator: **medusa00** (runs **zero** leaves in both arms).
* Remote connectors / Ray workers: **medusa01** and **medusa02**.
* Selected subnet `10.42.5.`; `node_placement = cross_node_multi_remote`;
  `fanout_placement = all-remote`; `n_remote = 2`.
* `N = 8` (split **4/4** across the two remotes), `K = 1000`, `W = 100`, `prewarm = 1`,
  `R = 5`.
* `clock = perf_counter_ns`, `measurement_boundary = python_caller_boundary`.
* HPX composition: proven `root_flat_gather_poll` (`composition_primitive=root_flat_gather_reduce`,
  watchdog `bounded_is_ready_poll_50us`, `hpx_native_composition=false`). The native-continuation
  spike is **not** used (see the informative negative above).

### Commands summary

Both arm drivers are launched **on medusa00 via `srun --overlap -N1 -n1 --nodelist=medusa00`**
(the Ray driver must run on a cluster node, per the exp59 lesson; launching from the bare
`salloc`/login shell fails to reach GCS). All five HPX islands run first (each its own process,
since HPX embeds in-process), fully down before the Ray arm; the Ray arm then runs its five
islands from a single cluster session; then the pair manifests and the combined aggregate:

```
# per island i = 1..5, on medusa00:
python run_exp62_fanout.py --phase hpx-multi-remote-fanout --band-id mrband_158817 \
  --island-index $i --n 8 --band-k 1000 --band-w 100 --prewarm 1 --n-remote 2 \
  --prefer-subnet 10.42.5. --connector-threads 8 --composition-mode root_flat_gather_poll
# then, after all HPX islands are down, on medusa00 (one cluster, 5 islands):
python run_exp62_fanout.py --phase ray-multi-remote-fanout --band-id mrband_158817 \
  --islands 5 --n 8 --band-k 1000 --band-w 100 --prewarm 1 --n-remote 2 \
  --prefer-subnet 10.42.5. --worker-cpus 8
python run_exp62_fanout.py --phase pair-manifest-multi-remote --band-id mrband_158817 --islands 5 --n 8
python run_exp62_fanout.py --phase band-aggregate-multi-remote --band-id mrband_158817 --islands 5 --n 8
```

(`--islands` is a count in this runner, not a list.)

### Artifacts

Trackable (curated, in the tracked experiment directory):

* `exp62_fanout_mrband_158817_aggregate.json`
* `exp62_fanout_mrband_158817_i{1..5}_manifest.json`

Gitignored (raw / provenance, kept locally under
`_exp62_runs/mrband_full_copyback_158817/`): the ten raw island artifacts
`exp62_fanout_mrband_158817_i{1..5}_{hpx,ray}.json`, the ten per-connector HPX bootstrap dirs
(`attest_connect.json` + `connect.joined1`/`served1.ok`/`connect.disconnected1` lifecycle
markers), and the Ray head + two worker logs. Build outputs stay gitignored.

### Gates

* All five HPX arms passed; all five Ray arms passed; all five pair manifests passed
  (failed gates: none); the combined aggregate passed.
* Aggregate: `overall=pass`, `same_axis_comparison=true`, `R=5`, `cross_island_agreement=true`,
  `node_set=[medusa00, medusa01, medusa02]`.
* Both arms oracle-correct (composite `11040115504`) with matched configs; both arms
  `leaves_local=0`, `leaves_remote=8`, `witness_leaf_count=8`, and both cover **both** remote
  localities/nodes (4/4); both arms `no_dispatch_timeout`, `timed_out_leaf_count=0`.
* HPX per island: `composition_primitive=root_flat_gather_reduce`,
  `watchdog=bounded_is_ready_poll_50us`, `hpx_native_composition=false`,
  `composition_provenance_consistent=true`; all ten connectors (medusa01 + medusa02 × 5)
  joined → served → graceful-disconnect, `tcp_nodelay_verified=true`, cpuset
  `[0,2,4,6,8,10,12,14]` (8, not collapsed).
* Ray per island: head on medusa00 `num_cpus=0`; coordinator hard-pinned to medusa00
  `num_cpus=0` running **zero** leaves; leaves hard-pinned (`soft=false`) round-robin across
  the two remote node ids; bounded `ray.get`; teardown `no_orphans=true`.

Hard fences (locked false in the aggregate and in every manifest):

* `speedup_computed=false`
* `ratio_reported=false`
* `arms_differenced=false`
* `placement_bands_differenced=false`

`same_axis_comparison=true` is earned **purely structurally** (every gate passed across both
arms and all islands). It carries no ratio, speedup, ranking, or winner semantics.

### Per-arm band

Bands are across-island medians of per-island percentiles, reported **separately** (the arms
are never differenced, ratioed, or ranked):

| arm (same Python caller boundary) | call primitive | p50 | p90 | p99 | mean |
|---|---|---|---|---|---|
| Ray coordinator/task path | `ray.get(coordinator.remote(x, N))` | 3717.4 µs | 3874.4 µs | 7012.5 µs | 3805.4 µs |
| experiment-only Python→HPX action path | `ext.fanout_fanin_remote(x, N)` (poll) | 249.5 µs | 270.7 µs | 320.2 µs | 251.5 µs |

> For this synthetic closed-`int64` N=8 fanout/fanin workload, measured at the same Python
> caller boundary with matched 3-node topology (medusa00 → medusa01/medusa02, all-remote,
> 4/4 split), the experiment-only Python→HPX path and the Ray coordinator path produced the
> separate per-arm RTT bands above. This is a structurally valid same-axis juxtaposition, not a
> ratio, speedup, or winner claim, and not shipped `rayx.runtime` distributed API.

### Interpretation and caveats

Slice 4b is the strongest exp62 same-axis distributed evidence: a matched R=5 band that exercises
real distributed fanout/fanin across **two** remote localities/nodes on both arms, so no reading
is attributable to a single-remote or single-call artifact. Keep these caveats explicit:

* **Same-axis means same Python caller boundary + matched topology, not same internals.** The HPX
  arm is a C++ action fanned over the TCP parcelport and reduced in C++ with the interim poll
  watchdog; the Ray arm is task scheduling + object-store transport with a Python-driver
  coordinator. The execution models differ, so bands are reported separately and **no ratio,
  speedup, or winner** is computed or implied.
* **`root_flat_gather_poll` is a proven interim composition, not a final HPX-native collective.**
  It is the Rostam-validated cross-node path; HPX collectives / tree-style reduction remain the
  target for a later HPX-native reduction (the passive `when_all().then()` spike was an informative
  negative).
* **Synthetic closed-`int64` QD1-outer micro-workload.** One blocking call per timed iteration.
  Not real serving, not real inference, no payload-size evidence, no QD>1 / pipeline regime.
* **Experiment-only pybind path.** `ext.fanout_fanin_remote` is not shipped `rayx.runtime` API and
  does not give the public RayX API distributed actions.
* Magnitudes are Rostam-allocation-specific and TCP-parcelport-specific.

## Roadmap impact

* **Roadmap strengthened.** exp62 **Slice 4b** is now the **strongest current same-axis
  distributed evidence**: a matched R=5 band at ≥2 remote localities/nodes on both arms. It
  supersedes Slice 3 (one remote locality), which remains the one-remote predecessor. exp61
  remains the scalar QD1 same-axis predecessor; exp58/59/60 remain precursor / within-runtime
  decomposition evidence on different measurement planes.
* **Slice 4a preserved as HPX-only mechanism evidence** (job `158813`); the native-continuation
  composition spike (job `158814`) is preserved as an **informative negative**, with
  `root_flat_gather_poll` the proven cross-node composition.
* **Next experiment:** move beyond the closed-`int64` micro-workload toward **payload-carrying
  leaves and an HPX-native collective/tree reduction** (`hpx::collectives::reduce`), so the HPX
  arm can retire the interim root gather + poll. This stays an experiment-only same-axis probe.
* This does **not** imply or motivate runtime/API work: exp62 must not mutate shipped
  `rayx.runtime`, and the same-axis distributed evidence must not be presented as a public
  RayX distributed API.
