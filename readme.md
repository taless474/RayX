# RayX

<p align="center">
  <img src="docs/figures/logo.png" alt="RayX logo" width="280">
</p>

**RayX** is an experimental **Ray-hosted HPX-native runtime**. It asks a narrow
question: can a long-lived Ray actor host a narrow HPX-native runtime while
preserving useful HPX execution semantics — futures, async work, cooperative
suspension, cancellation, admission/backpressure, and local native actor state —
across the Python/Ray boundary?

* **Ray** owns the outer distributed boundary: actor placement, process
  lifecycle, and Python-ecosystem integration.
* **RayX/HPX** owns the local native runtime *inside* one Ray actor: fixed
  registered native operations, local native actors, HPX futures/continuations,
  runtime lanes, cancellation, admission/backpressure, and shutdown.
* The **benchmark/experiment harness** is synthetic evidence infrastructure used
  to isolate and validate adapter/runtime mechanisms — not real model inference,
  not Ray Serve, and not a Ray replacement.

**Naming:** **RayX** = this repo / project (the Ray-hosted HPX-native runtime
exploration plus its synthetic evidence harness); **`rayx`** = the Python package
(the thin `Engine` frontend over HPX service lanes, and the experimental
`rayx.runtime` of fixed registered native operations and local native actors).

## What this project is

* An experimental **Ray-hosted HPX-native runtime**: a long-lived Ray actor
  hosts one in-process RayX/HPX runtime, Ray owns placement/lifecycle, and only
  plain values/results cross the Ray boundary.
* `rayx.runtime`, the experimental native runtime — fixed registered native
  operations and local native actors over HPX-native FIFO `RuntimeLane`s, with
  HPX futures, cooperative cancellation, bounded admission, and shutdown.
* A **synthetic evidence harness** over a shared workload contract and v1 metrics
  schema — the `rayx` `Engine` frontend, a Ray actor baseline (public Ray APIs),
  and an HPX-native C++ baseline — used to isolate adapter/runtime mechanisms.
* Exploratory, narrow side-seams: a local **`rayx.endpoint`** AF_UNIX seam and
  experiment-only **HPX connect-mode / Ray-orchestrated HPX-island** probes. These
  are standalone mechanism experiments — not shipped `rayx.runtime` API, not a
  fabric, and not performance evidence.

## What this project is not

* Not a Ray replacement, and not a fork of Ray.
* Not modifying Ray Core internals; not Ray Serve / Ray Train / Ray object store.
* The `Engine` / `SyntheticActor` harness runs synthetic C++ work only, and the
  experimental `rayx.runtime` prototype runs only fixed registered native
  operations and fixed registered native actor methods — neither runs arbitrary
  Python functions remotely, and neither has an object store / `ObjectRef`, Ray
  task semantics, or real model inference.
* Not benchmarking real model inference.

## Current status

Ray actor baseline, HPX-native synthetic baseline, and the `rayx` Python frontend
all exist and run. The stable comparison anchor across the whole arc is the
actor-like, single-`std::thread` `ServiceLane` (one serialized FIFO lane), shared
by the HPX-native baseline and `rayx`; the public `rayx` API and the v1 benchmark
JSONL schema (still `1`) are unchanged across the additions below.

Evidence spans the synthetic serving-control benchmarks, the `rayx.runtime` native
operations and local native actors, Ray-hosting composition, in-process HPX
composition, an HPX-island lifecycle policy, and a distributed same-axis /
payload-ladder evidence arc (experiments 61–64). The full chronological
**“what we learned”** index lives in
[docs/evidence_index.md](docs/evidence_index.md); the headline summary is below.

## Current evidence snapshot (experiments 61–64)

Experiments 61, 62, and 64 use a single Python caller boundary on Rostam
(medusa nodes, subnet `10.42.5.`), while exp63 is the HPX-native
composition/progress diagnostic that explains and hardens the HPX side.
Every result reports the two arms **separately**: **no ratio, no speedup, no cross-arm difference, no
winner**. Throughout, the HPX side is an **experiment-only Python→HPX action path** (a pybind binding),
**not** the shipped `rayx.runtime` API and **not** distributed RayX; the closed-`int64` oracle / payload
digest is the only cross-arm correctness anchor. For the measured exp62 and exp64 HPX arms, the HPX composition is
`root_flat_gather_poll`, a proven interim gather baseline, not a final
HPX-native collective.

| Experiment | Question | Setup | Status | Safe to claim | Not claimed |
|---|---|---|---|---|---|
| **exp61** | Scalar remote-call RTT at one Python boundary | Ray actor RPC vs experiment-only Python→HPX scalar action; medusa00→01; R=5; closed-`int64` | **Same-axis**; all gates passed | Per-arm RTT bands for this QD1 closed-`int64` call at the same boundary | Any ratio/speedup/winner; production API; broad benchmark |
| **exp62** | Distributed fanout/fanin RTT at one Python boundary | N=8 **all-remote**, 4/4 hard-pinned; medusa00→01/02; R=5; K=1000/W=100 | **Same-axis**; strongest distributed scalar evidence | Per-arm RTT bands for this closed-`int64` N=8 fanout/fanin | Ratio/speedup/winner; production distributed API; final HPX collective |
| **exp63** | Is HPX-native cross-node composition viable? | Native `when_all`/`dataflow` reduce + depth-2 star-of-partials; connector-lifetime hardening | **Mechanism validated** (20/20 per mode) | Native composition works once connector lifetime is correct; root-of-partials works cross-node | No Ray comparison; no performance numbers; no `hpx::collectives` |
| **exp64** | How does response-payload size behave, per runtime? | Payload ladder `[0..256 KB]`; HPX poll-gather vs Ray coordinator; R=5 band, measured=30 | **`matched_band_r5`**; within-arm only | Each arm's own within-arm p50/p90 payload-size curve; structural repeatability | Cross-arm comparison; ratio/speedup/winner; p99; `distributional_payload_ladder` |

### exp61 — scalar same-boundary remote call

The first **same-axis** comparison of a QD1 closed-`int64` micro-call: both arms timed with the same
monotonic clock around one blocking Python call, in an **R=5** matched band on **medusa00 → medusa01**
(subnet `10.42.5.`, K=1000 / W=100). All Slice-4 gates passed (`same_axis_comparison=true`); the two
arms are reported **separately** with `speedup_computed=false`, `ratio_reported=false`,
`arms_differenced=false`.

Cross-node, R=5 (per-arm bands, µs):

| arm (same Python boundary) | p50 | p90 | p99 | mean |
|---|---|---|---|---|
| Ray actor path | ~518.3 | ~850.7 | ~1125.7 | ~584.7 |
| experiment-only Python→HPX action path | ~184.8 | ~257.5 | ~322.6 | ~188.7 |

A **same-node placement control** (single node medusa00, R=5, all gates passed) is a control, not a
ranking: Ray p50 ~519.1 µs, experiment-only Python→HPX p50 ~93.0 µs. This is one closed-`int64`
micro-call on one node pair — **not** a broad benchmark and **not** a production-runtime claim. Detail:
[experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md](experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md).

### exp62 — distributed fanout/fanin (strongest same-axis distributed evidence)

One outer blocking Python call `fanout_fanin(x, N) -> int64` dispatches **N=8** leaf actions
**all-remote**, hard-pinned, and reduces them — both arms at the same Python boundary. The HPX arm uses
`root_flat_gather_poll`; the Ray arm's coordinator (`num_cpus=0`) runs zero leaves and hard-pins leaves
across the remotes. All gates passed; arms reported separately (`speedup_computed=false`,
`ratio_reported=false`, `arms_differenced=false`, `placement_bands_differenced=false`).

Slice 3 — one-remote matched cross-node, R=5 (medusa00→01, N=8, K=1000, W=100, prewarm=1; per-arm µs):

| arm (same Python boundary) | p50 | p90 | p99 | mean |
|---|---|---|---|---|
| Ray coordinator path | ~3640.9 | ~3895.6 | ~6407.0 | ~3718.6 |
| experiment-only Python→HPX `root_flat_gather_poll` | ~345.4 | ~401.7 | ~466.2 | ~359.0 |

Slice 4b — matched ≥2-remote, R=5 (medusa00 root/head/coordinator, medusa01/02 remotes, N=8, K=1000,
W=100, prewarm=1; per-arm µs) — the **headline** same-axis distributed scalar evidence:

| arm (same Python boundary) | p50 | p90 | p99 | mean |
|---|---|---|---|---|
| Ray coordinator, hard-pinned leaves | ~3717.4 | ~3874.4 | ~7012.5 | ~3805.4 |
| experiment-only Python→HPX `root_flat_gather_poll` | ~249.5 | ~270.7 | ~320.2 | ~251.5 |

Closed-`int64` correctness, all leaves remote, placement hard-gated. Still an **experiment-only** HPX
path, **not** production RayX runtime, and `root_flat_gather_poll` is an interim composition, not a final
collective. Detail:
[experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md](experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md).

### exp63 — native HPX composition / progress diagnosis (mechanism evidence, not a Ray comparison)

exp63 resolved the earlier native-composition concern. It is **mechanism evidence only** — no Ray
comparison, no performance numbers, no HPX collectives claim.

- The earlier native-composition failure traced to **connector lifetime**, not an intrinsic HPX
  native-progress failure. The connector-side fault was `HPX(invalid_status): thread pool is not
  running` during parcel scheduling.
- Serve-timeout sweep:

  | serve-timeout | outcome |
  |---|---|
  | 90 s | fault at call 7 |
  | 150 s | fault at call 14 |
  | 300 s | pass |
  | 600 s | pass |

- A hardened **heartbeat / root-completion lifetime** fixed it.
- **Slice 2a** validated native cross-node composition: `when_all_then_reduce` **pass, 20/20**;
  `dataflow_reduce` **pass, 20/20**; `root_flat_gather_poll` is a mechanics control only, **not**
  native-validated.
- **Slice 2b** validated depth-2 star / root-of-partials: `dataflow_reduce` **20/20**,
  `when_all_then_reduce` **20/20**; topology `depth2_star_of_partials_contiguous_blocks`, partials
  `[4, 4]` across the two remote localities.

In plain terms: the HPX-native path is viable once connector lifetime is correct, and root-of-partials
composition works cross-node. This claims **no** Ray performance and **no** HPX `collectives`. Detail:
[experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md](experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md).

### exp64 — payload fanin size sweep (within-arm payload evidence)

exp64 measures **response-payload size** at the same Python boundary, in four slices: **Slice 1** HPX
payload smoke, **Slice 2** Ray matched smoke, **Slice 3** structural R=1 matched ladder, **Slice 4** an
R=5 matched band. Each leaf returns `S` opaque payload bytes plus its closed scalar; Python folds and
checks the payload digest **after** timing, outside the RTT window, identically for both arms.

The **Slice 4 band** (`band20260702_174335`; jobs 159385/159386/159388/159389/159390; R=5 **fresh
exclusive** allocations — the scheduler reused medusa11/12/13 for all islands, so this shows
repeatability under **low placement diversity**, not broad cluster-wide variance) ran the full ladder
`[0,64,1024,16384,262144]` (N=8, prewarm=5, measured=30, HPX phase then Ray phase). All per-island
manifests passed and the band aggregate passed:

- `overall_band_pass=True`, `evidence_grade=matched_band_r5`
- `same_axis_comparison=True` — **structural flag only**
- `distributional_evidence=True` — **within-arm only**; `percentiles_evidence_ready=True` — **p50/p90
  only**; `p99_evidence_ready=False`
- `distributional_payload_ladder_ready=False`, blocked by
  `hpx_serialization_runtime_path_not_observed` and `hpx_poll_gather_baseline`
- `no_cross_arm_timing_computed=True`; no ratios, no speedups, no winner

The per-arm tables below are **within-arm observations only** (across-island median of the per-island
p50/p90). **Do not compare the two tables**, do not compute ratios, and do not read a winner — the arms
take intentionally different runtime paths and the only cross-arm anchor is the closed digest.

HPX (`root_flat_gather_poll` poll-gather payload baseline):

| S (bytes) | p50 (ms) | p90 (ms) |
|---|---|---|
| 0 | 0.299 | 0.335 |
| 64 | 0.290 | 0.317 |
| 1024 | 0.326 | 0.388 |
| 16384 | 1.453 | 1.516 |
| 262144 | 19.518 | 20.552 |

Ray (coordinator + Ray object transport):

| S (bytes) | p50 (ms) | p90 (ms) |
|---|---|---|
| 0 | 4.225 | 4.821 |
| 64 | 4.191 | 4.580 |
| 1024 | 4.392 | 4.667 |
| 16384 | 6.850 | 7.153 |
| 262144 | 55.378 | 57.370 |

HPX here remains the `root_flat_gather_poll` poll-gather baseline — **not** the exp63 native-composition
payload path yet. The HPX serialization **runtime** path is **not observed** (config-level flags are, the
per-call zero-copy path taken is not), which is exactly what blocks a stronger distributional
payload-ladder grade; the Ray object/plasma return path is **not observed** either. Detail:
[experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md](experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md).

## Quickstart / smoke run

All commands assume the repo root as the working directory. Each driver writes a
per-request JSONL file; `bench/analyze_jsonl.py` rolls it up into an aggregate
summary under `results/`.

**Ray baseline** (Ray only, no HPX toolchain needed):

```bash
pip install -r requirements.txt   # ray
python bench/run_ray_baseline.py --service-ms 0 --concurrency 1 \
    --requests 1000 --out results/ray_noop_c1.jsonl
python bench/analyze_jsonl.py results/ray_noop_c1.jsonl
```

**HPX / rayx smoke** — *not* a one-line setup. The `rayx` frontend and the
HPX-native baseline require HPX v1.11.0 built/installed from source, then the
local pybind11 `_rayx` extension built against that prefix. See
[docs/hpx_build_notes.md](docs/hpx_build_notes.md) for the full build. Once HPX is
installed:

```bash
pip install -r requirements-python.txt   # pybind11

cmake -S python -B python/build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DPYBIND11_FINDPYTHON=ON \
    -DCMAKE_PREFIX_PATH="/path/to/hpx-install;$(python -m pybind11 --cmakedir)"
cmake --build python/build

python bench/run_hpx_python_baseline.py --service-ms 0 --concurrency 1 \
    --requests 1000 --num-lanes 1 --hpx-threads 4 \
    --out results/rayx_noop_c1.jsonl
python bench/analyze_jsonl.py results/rayx_noop_c1.jsonl
```

Runnable API tours live under [examples/](examples/) (`rayx_basic.py`,
`rayx_actor_pool.py`, `rayx_lane_impl.py`, `rayx_bounded_admission.py`,
`rayx_runtime_basic.py`). To run the local smoke/golden gates in one step, use the
stdlib-only aggregator `python bench/smoke_local.py` (it skips any unavailable
tier — no built `_rayx`, no native binary, no Ray). The full `rayx` API and the
`rayx.runtime` surface are documented in the reference docs (see the
documentation map).

## Separate in-process direction (HPX inside one Ray actor)

Distinct from the distributed same-axis / payload arc above, the **in-process**
direction (HPX running inside one long-lived Ray actor) has **three validated,
bounded properties**, all observation-only and machine-specific:

1. **Control-plane dispatch floor** — in the synthetic no-op benchmark, `rayx`
   stays near the HPX-native control-plane floor rather than collapsing toward
   Ray's actor-process boundary (benchmark 06).
2. **Ray-hosted native scaling inside one actor** — inside one long-lived Ray
   actor, native HPX-backed Async CPU work scales cleanly through **W=16** on
   homogeneous Linux while a pure-Python in-process CPU loop stays flat (exp32 on
   Rostam).
3. **Adapter design preserves (or hides) HPX cooperative behavior** — a *blocking*
   per-lane retirement can hide HPX cooperative suspension under a synthetic
   parked+compute mix, while a *non-blocking* op-lane with sufficient in-flight
   admission restores it; the exp35→exp38 arc localized this to per-lane
   head-of-line and an undersized in-flight cap, not a retirement-path failure.

Together these validate an **intra-process runtime mechanism** and show that
**adapter design matters** inside the Ray boundary. They are **not** Ray cluster
scaling, **not** “RayX makes Ray faster”, **not** “HPX beats Ray”, and **not**
sizing/capacity guidance. Per-experiment detail and numbers:
[docs/evidence_index.md](docs/evidence_index.md).

## Documentation map

Framing/reference docs live under `docs/` (`docs/reference/` for API notes); result
notes live under `benchmarks/` and `experiments/<name>/`. The three code/evidence
trees are distinct: `bench/` holds runnable harness code (drivers, smokes,
analyzers), `benchmarks/` holds formal benchmark write-ups, and `experiments/`
holds investigative write-ups / curated evidence packages.

### Start here

* [docs/rayx_for_beginners.md](docs/rayx_for_beginners.md) — a from-zero tour: what RayX is and is not, the lane/actor mental model, and how the pieces fit.
* [docs/project_proposal.md](docs/project_proposal.md) — motivation, hypothesis, scope, phases.
* [docs/ray_hpx_mapping.md](docs/ray_hpx_mapping.md) — Ray ↔ HPX conceptual mapping and project direction.
* [docs/experiment_plan.md](docs/experiment_plan.md) — measurement contract, schemas, workload matrix.
* [docs/hpx_build_notes.md](docs/hpx_build_notes.md) — HPX build/install notes.

### Core rayx API and design

* [docs/reference/rayx_actor_api.md](docs/reference/rayx_actor_api.md) — `rayx` Python API (`Engine` / `SyntheticActor`), façade equivalence, and how Ray actor-pool code maps to `Engine(num_lanes=N)`.
* [docs/reference/rayx_frontend_design.md](docs/reference/rayx_frontend_design.md) — frontend design rationale: Future ownership, `wait`/`as_completed`, shutdown drain, cancellation, the `lane_impl` backend seam.
* [docs/reference/rayx_submit_batch.md](docs/reference/rayx_submit_batch.md) — the `submit_batch()` bulk-submit path.
* [docs/reference/chunked_service_synthesis.md](docs/reference/chunked_service_synthesis.md) — chunked-service cross-reading (benchmark 09 + experiment 14).
* [docs/reference/hpxlane_backend_arc.md](docs/reference/hpxlane_backend_arc.md) — the opt-in `HpxLane` backend reading guide.
* `rayx.runtime` design notes live under [docs/design/](docs/design/) (problem model, HPX design principles, registered-operation API, value model, local native actors, the endpoint seam); runnable tour: [examples/rayx_runtime_basic.py](examples/rayx_runtime_basic.py).

### Evidence

* [docs/evidence_index.md](docs/evidence_index.md) — the chronological **“what we learned”** index for every benchmark and experiment arc: main benchmark arc (01–10), frontend/serving-control + `HpxLane` (01–23), `rayx.runtime` / local actors (24–26) and the endpoint seam (42–43), Ray-hosting composition (27–30), runtime/adapter (31–38), in-process HPX composition (39–44), HPX-island lifecycle / Ray-orchestrated bootstrap (49–52), the two-node precursors (58–60), and the distributed same-axis / payload-ladder arc (61 scalar, 62 distributed fanout/fanin, 63 native HPX composition, 64 payload ladder).
* [experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md](experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md) — exp61: the scalar same-axis Python-boundary write-up (experiment-only).
* [experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md](experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md) — exp62: the same-axis Python-boundary **distributed fanout/fanin** write-up (experiment-only; not shipped `rayx.runtime` API).
* [experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md](experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md) — exp63: HPX-native composition / progress diagnosis (mechanism evidence only; no Ray comparison, no `hpx::collectives`).
* [experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md](experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md) — exp64: payload fanin size sweep through the Slice 4 `matched_band_r5` within-arm band (experiment-only).
* Source write-ups live beside the code under [benchmarks/](benchmarks/) and [experiments/](experiments/).

### Project rules

* [CLAUDE.md](CLAUDE.md) — working rules and project guardrails.
