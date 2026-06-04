# RayX

<p align="center">
  <img src="docs/figures/logo.png" alt="RayX logo" width="280">
</p>

**RayX** is a standalone comparison harness. It asks a narrow question: can HPX
serve as a low-overhead native execution substrate, exposed through a thin
Python frontend, while preserving HPX's native control-path advantage for
C++/HPC-style ML serving-control workloads? It measures control-plane overhead
and workload sensitivity. It does not run real model inference, it is not a Ray
replacement.

**Naming:**

* **RayX** = this repo / project (the Ray-vs-HPX comparison harness).
* **`rayx`** = the Python package: a thin Python frontend over HPX service lanes.
* **Ray-vs-HPX benchmarks** are the technical framing throughout.

## What this project is

* A standalone Ray-vs-HPX comparison harness over a shared synthetic workload
  contract and metrics schema.
* A Ray actor baseline using public Ray APIs.
* An HPX-native synthetic baseline (C++).
* `rayx`, a thin Python frontend over HPX service lanes, benchmarked against
  both the Ray actor-process path and the HPX-native path.

## What this project is not

* Not a Ray replacement, and not a fork of Ray.
* Not modifying Ray Core internals; not Ray Serve / Ray Train / Ray object store.
* `rayx` runs native synthetic C++ work only — it does not run arbitrary Python
  functions remotely.
* Not benchmarking real model inference yet.

## Current status

* Ray actor baseline, HPX-native synthetic baseline, and the `rayx` Python
  frontend all exist and run. The stable comparison anchor across the whole arc
  is the actor-like, single-`std::thread` `ServiceLane` (one serialized FIFO
  lane, blocking sleep), shared by the HPX-native baseline and `rayx`.
* The full comparison arc has been executed end-to-end and validated against the
  v1 metrics schema (each run's JSONL passes the analyzer rollup): single-lane,
  service × concurrency matrix, multi-lane scaling, variable (bimodal) service,
  lane sweeps, a multi-client-thread driver (Python and native C++), and a
  CPU-bound `work_mode=spin` **diagnostic/calibration** shape — a synthetic
  on-core service mode for probing the hardware/core-boundary regime, **not** a
  serving design and **not** an HPX runtime mode — with a saturation-knee sweep.
* On top of that arc, the `rayx` frontend has grown serving-control ergonomics: a
  Ray-flavored API surface (`get`, actor `serve.remote`, single-request labels),
  true varied bulk batch, chunked synthetic service, queued + chunk-boundary
  running cancellation, and a non-blocking readiness poll (`wait(timeout=0)`)
  (experiments 08–13 in the documentation map; see the at-a-glance section next).
  `work_mode=spin`, varied batch, and **chunked synthetic service**
  (`--chunks` / `--chunk-delay-ms`) are first-class benchmark-driver workloads
  across Ray, HPX-native, and rayx; cancellation and `chunks_completed` stay
  facade-only. `rayx` batch submission also now uses an internal **per-lane bulk
  enqueue** (one lock+notify per lane), trimming no-op / tiny multi-lane batch
  enqueue overhead. None of these change the public `rayx` API or the v1
  benchmark JSONL schema (still `1`).
* Experiments 15–16 are opt-in lane-**mechanism** probes, explicitly
  **incomparable** to the benchmark corpus (which holds the lane mechanism
  fixed): an isolated HPX primitive probe (sleep overshoot, no-op dispatch) and a
  native-only opt-in HPX cooperative-lane probe (`--lane-impl hpx`). They justify
  *exploring* HPX-lane mechanics; the `ServiceLane` anchor is unchanged and is
  **not** being replaced.
* On top of that, the cooperative HPX-thread lane now ships as an **opt-in `rayx`
  backend**: `Engine(lane_impl="hpx")` selects `HpxLane` (default `"std"` keeps the
  `ServiceLane` anchor) behind the *same* RayX lane contract — only the `actor_id`
  prefix differs (`act-hpx-` vs `act-hpxl-`), no HPX internals are exposed to
  Python, and the v1 JSONL schema is unchanged. Its evidence arc is exp16 (native
  feasibility) → exp20 (task/dataflow pools are **not** drop-in lane backends) →
  exp21 (contract parity) → exp22 (load-divergence mechanism) → exp23 (adapter-hop
  cost); the exp22/exp23 timings are **observation-only and machine-specific**, not
  performance claims and not an "HpxLane faster/slower" verdict. See
  [docs/reference/hpxlane_backend_arc.md](docs/reference/hpxlane_backend_arc.md).
* The `rayx` frontend has also gained two serving-control / observability
  additions to the public API (a method and a constructor option) that leave
  `ServiceLane` semantics, the benchmark drivers, and the v1 JSONL schema (still
  `1`) unchanged: `Engine.lane_stats()`, a non-consuming per-lane
  `{actor_id, queue_depth, active}` snapshot for debugging (observability only —
  not scheduler state, placement control, or schema), and **bounded admission**
  via `Engine(max_queue_depth_per_lane=N)`, a local per-lane admission-by-rejection
  cap that raises `QueueFullError` when the round-robin target lane's
  queued-but-not-started depth is full (**not** Ray Serve backpressure, distributed
  flow control, or blocking backpressure; experiments 17–19 in the documentation
  map).
* Headline result below; per-experiment result notes in the documentation map.

## The `rayx` frontend at a glance

`rayx` is a thin Python frontend over HPX service lanes — a Ray-flavored surface
for **synthetic** scheduling/serving experiments. It is **not** Ray: no object
store, no arbitrary actor methods, no real task-payload execution, and no Ray
performance claim. `service_ms` is duration control, never a real workload.

* **API** — `Engine(num_lanes=N, lane_impl="std")` (round-robin serialized lanes;
  default `"std"` is the `ServiceLane` anchor, opt-in `"hpx"` selects the
  cooperative `HpxLane` — same contract, also via `SyntheticActor(...,
  lane_impl="hpx")`) or the `SyntheticActor` actor-style façade (`remote` /
  `serve.remote`), with `submit`,
  `submit_batch` (one bulk Python→C++ crossing, internal per-lane bulk enqueue),
  `wait` / `as_completed` (`ray.wait`-style, incl. a non-blocking
  `wait(timeout=0)` readiness poll), `get` (retire → RayX **measurement rows**,
  *not* `ray.get` values), `cancel`, and graceful-drain `shutdown`. Single-request
  submits take an optional client-side `label`.
* **Synthetic workload controls** — `work_mode="sleep"` (parked) or `"spin"`
  (CPU-bound on-core; a synthetic diagnostic/calibration shape, not a serving
  design); a scalar `service_ms`; **varied batch** service via
  `submit_batch(service_ms=[...])` (lists are batch-only; single `submit` stays
  scalar); and **chunked service** via `chunks` / `chunk_delay_ms` — one future /
  one row, synthetic cadence, **not** real token streaming.
* **Cancellation** — `cancel(future)` skips a *queued* request entirely
  (`chunks_completed=0`), or stops a *running chunked* request at its **next chunk
  boundary** (`1 ≤ chunks_completed < chunks`; "guaranteed stop, not ready-now").
  An in-progress active chunk or parked gap is never interrupted; batch futures
  are non-cancelable. The benchmark JSONL schema stays version `1`.
* **Observability & admission** — `lane_stats()` returns a non-consuming per-lane
  `{actor_id, queue_depth, active}` snapshot (debugging only; **not** scheduler
  state, placement control, a synchronization primitive, or part of the JSONL
  schema). `Engine(max_queue_depth_per_lane=N)` (default `None` = unbounded,
  unchanged) enables **bounded admission**: a local per-lane cap on
  queued-but-not-started work that raises `QueueFullError` when the round-robin
  target lane is full — the active in-service request is *extra* (not counted),
  and a rejected request gets no Future and no row. It is **not** Ray Serve
  backpressure, distributed flow control, or blocking backpressure; `submit_batch`
  refuses under a cap.

Full semantics live in the reference docs:
[rayx_actor_api.md](docs/reference/rayx_actor_api.md),
[rayx_frontend_design.md](docs/reference/rayx_frontend_design.md),
[rayx_submit_batch.md](docs/reference/rayx_submit_batch.md).

## Quickstart / smoke run

All commands assume the repo root as the working directory. Each driver writes a
per-request JSONL file; `bench/analyze_jsonl.py` rolls it up into an aggregate
summary.

**Ray baseline** (Ray only, no HPX toolchain needed):

```bash
pip install -r requirements.txt   # ray
python bench/run_ray_baseline.py --service-ms 0 --concurrency 1 \
    --requests 1000 --out results/ray_noop_c1.jsonl
python bench/analyze_jsonl.py results/ray_noop_c1.jsonl
```

**HPX / rayx smoke** — this is *not* a one-line setup. The rayx Python frontend
(and the HPX-native baseline) require HPX v1.11.0 built and installed from
source, then the local pybind11 `_rayx` extension built against that install
prefix. See
[docs/hpx_build_notes.md](docs/hpx_build_notes.md) for the full HPX build. Once
HPX is installed:

```bash
pip install -r requirements-python.txt   # pybind11

# build the _rayx extension into python/src/rayx (no install step needed).
# -DPYBIND11_FINDPYTHON=ON is required so pybind11 binds the active Python (run
# this from your activated env) instead of a system one; the pybind11 cmakedir
# must be on CMAKE_PREFIX_PATH so find_package(pybind11) resolves.
cmake -S python -B python/build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DPYBIND11_FINDPYTHON=ON \
    -DCMAKE_PREFIX_PATH="/path/to/hpx-install;$(python -m pybind11 --cmakedir)"
cmake --build python/build

# rayx smoke (the driver adds python/src to sys.path automatically)
python bench/run_hpx_python_baseline.py --service-ms 0 --concurrency 1 \
    --requests 1000 --num-lanes 1 --hpx-threads 4 \
    --out results/rayx_noop_c1.jsonl
python bench/analyze_jsonl.py results/rayx_noop_c1.jsonl
```

Per-request JSONL and an aggregate summary are written under `results/`.

For a minimal walkthrough of the `rayx` Python API itself (`Engine` context
manager, `submit`, `wait`, `as_completed`, once-only `result`, graceful-drain
shutdown), see [examples/rayx_basic.py](examples/rayx_basic.py) — runnable once
`_rayx` is built. For the common Ray **actor-pool** shape expressed with the
`SyntheticActor` façade (`serve.remote`, round-robin lanes, `wait(num_returns=1)`
as-completed, input-order `get`, and a capped pool that sheds via
`QueueFullError`), see [examples/rayx_actor_pool.py](examples/rayx_actor_pool.py)
— it mirrors the control shape of a Ray actor pool but is not Ray, and the
lane-pool shape is a Ray-comparison convenience rather than HPX best-practice
guidance (conceptual mapping in
[docs/reference/rayx_actor_api.md](docs/reference/rayx_actor_api.md) §8; the
Ray / HPX-native / harness distinction in
[docs/ray_hpx_mapping.md](docs/ray_hpx_mapping.md)).

To run the available local smoke/golden gates in one step, use the stdlib-only
aggregator [bench/smoke_local.py](bench/smoke_local.py): `python
bench/smoke_local.py` runs the checks that apply on your machine and skips any
unavailable optional tier (no built `_rayx`, no native binary, no Ray). It is a
validation helper, not a benchmark.

The native HPX baseline also accepts an opt-in `--diag` flag that writes a
separate `<out>.diag.json` (schema `diag-1`) decomposing per-request latency
into push / pickup / service / completion phases plus queue depth and per-lane
utilization. It is off by default and does not change the normal JSONL output.

## Headline result

From benchmark 06 (three-way, single lane, `retire_mode=one_by_one`, medians of
5 repeats). These are control-plane / dispatch numbers on a **synthetic**
backend, across **three different boundaries** — not real inference, and not an
identical-boundary comparison.

| no-op (`service_ms=0`, 1 lane) | Ray | HPX-native | rayx |
|---|---|---|---|
| throughput (req/s) | 313 | 201,725 | 97,671 |
| queue-wait p50 (ms) | 3.18 | 0.003 | 0.008 |

* At the no-op dispatch floor the HPX paths show far lower per-call control
  overhead than Ray's actor-process boundary, and rayx stays near the HPX-native
  floor rather than collapsing toward Ray.
* **Caveats:** no-op is **client-loop-sensitive** (single client thread,
  one-by-one), and rayx runs ~2–4× below HPX-native on this ultra-hot loop. Once
  each request does real work the gap closes — at `service_ms=20` all three
  converge to ~42–45 req/s. This is **not** a claim that HPX beats Ray in
  general.

Source: [benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md](benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md).

## Documentation map

Framing/reference docs live under `docs/` (with `docs/reference/` for the API
notes); result notes live under `benchmarks/` and `experiments/<name>/`. New
here? Start with the docs below, then open the **Detailed evidence index** at the
end for the full per-run notes — every benchmark/experiment link is preserved
there.

### Start here

* [docs/project_proposal.md](docs/project_proposal.md) — motivation, hypothesis, scope, phases.
* [docs/ray_hpx_mapping.md](docs/ray_hpx_mapping.md) — Ray ↔ HPX conceptual mapping and project direction.
* [docs/experiment_plan.md](docs/experiment_plan.md) — measurement contract, schemas, workload matrix.
* [docs/hpx_build_notes.md](docs/hpx_build_notes.md) — HPX build/install notes (source build, prefix, CMake invocation).
* [docs/reference/hpxlane_backend_arc.md](docs/reference/hpxlane_backend_arc.md) — consolidated reading guide for the `HpxLane` backend evidence arc (exp16 native feasibility → exp20 task/dataflow-not-a-backend → exp21 contract parity → exp22 load divergence → exp23 adapter-hop cost); an evidence index that links out to the reports, not new evidence.

### Core rayx API and design

* [docs/reference/rayx_actor_api.md](docs/reference/rayx_actor_api.md) — rayx Python API (Engine / SyntheticActor), façade equivalence result, and how common Ray actor-pool code maps to `Engine(num_lanes=N)` plus `label` / `actor_id`.
* [docs/reference/rayx_submit_batch.md](docs/reference/rayx_submit_batch.md) — rayx `submit_batch()` bulk-submit path and first throughput benchmark (engine/actor/batch).
* [docs/reference/rayx_frontend_design.md](docs/reference/rayx_frontend_design.md) — rayx frontend design rationale: Future ownership, `wait`/`as_completed`, why benchmark `batch_wait` differs, shutdown drain, queued + chunk-boundary running cancellation, the `lane_impl` backend seam (§13), and the `hpx::wait_some` choice.
* [docs/reference/chunked_service_synthesis.md](docs/reference/chunked_service_synthesis.md) — cross-reading of benchmark 09 (sleep, cross-driver) and experiment 14 (spin, HPX-native + rayx): the shared chunked model, why sleep "preservation" is overshoot-approximate while spin is exact, and what readers should not conclude (no "Ray faster", no "rayx faster than HPX-native", L8 is lane×core not chunking).

### Main benchmark arc (benchmarks 01–10)

The baseline Ray / HPX / rayx comparison over the shared synthetic workload and v1
metrics schema. Grouped:

* **Ray vs HPX control-overhead & scaling baselines** — single actor/lane, the service × concurrency matrix, throughput scaling, and the no-op retire-mode artifact: [01](benchmarks/01_ray_single_actor_baseline/ray_single_actor_baseline.md), [02](benchmarks/02_ray_hpx_single_lane_comparison/ray_hpx_single_lane_comparison.md), [03](benchmarks/03_ray_hpx_matrix_comparison/ray_hpx_matrix_comparison.md), [04](benchmarks/04_ray_hpx_scaling_comparison/ray_hpx_scaling_comparison.md), [05](benchmarks/05_ray_hpx_retire_mode_noop/ray_hpx_retire_mode_noop.md).
* **rayx Python frontend vs Ray / HPX-native** — preserves most of HPX's low control-plane overhead: [06](benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md).
* **Variable (bimodal) service** — lane scaling cuts queueing/tail; single-lane engines rank by control overhead: [07](benchmarks/07_rayx_variable_service/rayx_variable_service.md), [08](benchmarks/08_ray_hpx_rayx_variable_service/ray_hpx_rayx_variable_service.md).
* **Chunked synthetic service (cross-driver)** — [09](benchmarks/09_chunked_service_cross_driver/chunked_service_cross_driver.md).
* **Bulk-enqueue throughput path** — narrow no-op/tiny-batch enqueue win, no API/schema change: [10](benchmarks/10_rayx_bulk_enqueue/rayx_bulk_enqueue.md).

### Frontend / serving-control experiments (experiments 01–19)

rayx serving-control behavior, plus the measurement-artifact analysis underneath
it. Grouped by theme:

* **Measurement-artifact & retire-loop analysis** — sleep-fidelity overshoot, the closed-loop FIFO-retire ceiling, multi-client drivers, and the diag decomposition: [01](experiments/01_sleep_overshoot/sleep_overshoot_note.md), [02](experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md), [03](experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md), [04](experiments/04_hpx_native_multiclient/hpx_native_multiclient.md), [06](experiments/06_diag_fifo_ceiling_analysis/diag_fifo_ceiling_analysis.md).
* **as_completed / retire-loop fix** — [07](experiments/07_rayx_as_completed/rayx_as_completed.md).
* **spin vs sleep diagnostics** (`work_mode="spin"` is a synthetic CPU-bound diagnostic/calibration mode, not a serving design) — the spin knee, spin-vs-sleep coordination, and the core-boundary sweep: [05](experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md), [08](experiments/08_spin_vs_sleep_coordination/spin_vs_sleep_coordination.md), [09](experiments/09_spin_core_boundary_sweep/spin_core_boundary_sweep.md).
* **Varied batch / lane placement** — [10](experiments/10_varied_batch_service_time/varied_batch_service_time.md).
* **Cancellation & chunking** — queued cancel, v1 chunked service, chunk-boundary running stop, spin chunked: [11](experiments/11_queued_cancellation/queued_cancellation.md), [12](experiments/12_chunked_service/chunked_service.md), [13](experiments/13_chunk_boundary_cancellation/chunk_boundary_cancellation.md), [14](experiments/14_spin_chunked_service/spin_chunked_service.md).
* **lane_stats & bounded admission** — observability snapshot and per-lane admission-by-rejection (not Ray Serve backpressure): [17](experiments/17_lane_stats_observability/lane_stats_observability.md), [18](experiments/18_bounded_admission_burst/bounded_admission_burst.md), [19](experiments/19_bounded_admission_offered_load/bounded_admission_offered_load.md).

### HpxLane backend evidence arc

The opt-in cooperative `HpxLane` backend (`lane_impl="hpx"`; default `"std"` keeps
the `ServiceLane` anchor). Read
[docs/reference/hpxlane_backend_arc.md](docs/reference/hpxlane_backend_arc.md) for
the consolidated narrative; the underlying reports are native primitive
feasibility [15](experiments/15_hpx_native_lane_feasibility/hpx_native_lane_feasibility.md)
and the cooperative-lane mechanism probe [16](experiments/16_hpx_lane_mechanism_probe/hpx_lane_mechanism_probe.md);
task/dataflow pools are **not** a drop-in backend [20](experiments/20_hpx_task_dataflow_probe/hpx_task_dataflow_probe.md);
rayx-backend contract parity [21](experiments/21_rayx_hpxlane_backend_parity/rayx_hpxlane_backend_parity.md);
load-divergence mechanism [22](experiments/22_rayx_hpxlane_load_divergence/rayx_hpxlane_load_divergence.md)
and uncontended adapter-hop cost [23](experiments/23_rayx_hpxlane_adapter_hop_cost/rayx_hpxlane_adapter_hop_cost.md).
The exp22/exp23 timings are observation-only and machine-specific — no
performance and no "HPX faster/slower than ServiceLane" claim.

### Detailed evidence index

Full per-run notes, with provenance preserved. Expand a section for the complete
"what we learned" bullets and links.

<details>
<summary>Benchmarks 01–10 — full notes</summary>

* [benchmarks/01_ray_single_actor_baseline/ray_single_actor_baseline.md](benchmarks/01_ray_single_actor_baseline/ray_single_actor_baseline.md) — learned one Ray actor is a single serialized server: concurrency deepens its queue rather than parallelizing, so tiny requests are actor/process-overhead-bound and 20 ms requests cap at ~43 req/s.
* [benchmarks/02_ray_hpx_single_lane_comparison/ray_hpx_single_lane_comparison.md](benchmarks/02_ray_hpx_single_lane_comparison/ray_hpx_single_lane_comparison.md) — learned HPX's intra-locality C++ control path is far cheaper than Ray's actor-process boundary for tiny/no-op work, and the two converge as service time grows (different boundaries; not a general "HPX is faster" claim).
* [benchmarks/03_ray_hpx_matrix_comparison/ray_hpx_matrix_comparison.md](benchmarks/03_ray_hpx_matrix_comparison/ray_hpx_matrix_comparison.md) — learned that overhead-vs-service crossover holds across the whole service_ms × concurrency matrix: HPX has much lower overhead at tiny service, both converge once the serialized lane dominates.
* [benchmarks/04_ray_hpx_scaling_comparison/ray_hpx_scaling_comparison.md](benchmarks/04_ray_hpx_scaling_comparison/ray_hpx_scaling_comparison.md) — learned that with nonzero per-request synthetic service both scale throughput (HPX near-ideal at 5/20 ms, Ray near-ideal at 20 ms but sublinear at 5 ms), while apparent no-op negative scaling is the single-client retire loop, not a runtime limit.
* [benchmarks/05_ray_hpx_retire_mode_noop/ray_hpx_retire_mode_noop.md](benchmarks/05_ray_hpx_retire_mode_noop/ray_hpx_retire_mode_noop.md) — learned the no-op multi-lane regression is a client-loop / cross-thread coordination artifact, not an HPX lane-scaling limit; batch retirement reduces but does not eliminate it.
* [benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md](benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md) — learned the rayx Python frontend preserves most of HPX's low control-plane overhead and stays far below Ray's actor-process cost for native work, without being a Ray replacement.
* [benchmarks/07_rayx_variable_service/rayx_variable_service.md](benchmarks/07_rayx_variable_service/rayx_variable_service.md) — learned that under bimodal load more lanes sharply cut rayx queueing and absolute tail latency (1→4 lanes: total p99 78→28 ms, ~2.08× throughput bounded by concurrency); compare absolute p99, not the p99/p50 ratio.
* [benchmarks/08_ray_hpx_rayx_variable_service/ray_hpx_rayx_variable_service.md](benchmarks/08_ray_hpx_rayx_variable_service/ray_hpx_rayx_variable_service.md) — learned that under one shared deterministic sequence single-lane engines rank by control overhead (rayx ≈ HPX-native > Ray) and converge on the 20 ms service tail, while Ray's fixed per-request overhead keeps its absolute tail higher even as lanes scale.
* [benchmarks/09_chunked_service_cross_driver/chunked_service_cross_driver.md](benchmarks/09_chunked_service_cross_driver/chunked_service_cross_driver.md) — learned, now that `--chunks` / `--chunk-delay-ms` are first-class on all three drivers, that chunked sleep service preserves total active work at `delay=0` (observed stays ~flat across chunks 1/4/8, rising only with accumulated per-sleep overshoot) and adds lane-occupancy ≈ `(chunks-1)×chunk_delay_ms` at `delay>0`; the only backend split is sleep fidelity (Ray ~5% overshoot vs HPX-native/rayx ~23–25%, on both active chunks and parked gaps), rayx tracks HPX-native rather than collapsing toward Ray, and chunking adds lifecycle/cadence structure rather than changing the no-op control-plane story — sleep-mode only, machine-specific magnitudes, not token streaming.
* [benchmarks/10_rayx_bulk_enqueue/rayx_bulk_enqueue.md](benchmarks/10_rayx_bulk_enqueue/rayx_bulk_enqueue.md) — learned that rayx batch throughput was limited by per-request lane enqueue overhead at multi-lane no-op scale, and that per-lane bulk enqueue (one lock+notify per lane) removes that overhead without changing the public API or JSONL schema; the win is narrow to no-op/tiny-batch enqueue (it vanishes once per-request service dominates), not a general workload-speedup claim and not an HPX scheduler result.

</details>

<details>
<summary>Experiments 01–23 — full notes</summary>

* [experiments/01_sleep_overshoot/sleep_overshoot_note.md](experiments/01_sleep_overshoot/sleep_overshoot_note.md) — learned HPX/rayx `sleep_for` carries a stable ~25% proportional service overshoot vs Ray's ~5%, a backend sleep-fidelity gap (not control cost) that must be read separately when comparing cross-engine service/total.
* [experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md](experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md) — learned the bimodal lane/actor sweep (1–16) plateaus near ~1390 req/s; the original "coordination ceiling" reading is refined/superseded — it is a closed-loop FIFO-retire / client-driver ceiling (see the top banner in that note).
* [experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md](experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md) — learned multiple client threads lift the ~1390 ceiling to ~1740 but saturate quickly; its original GIL-bound reading is superseded by the native C++ driver below.
* [experiments/04_hpx_native_multiclient/hpx_native_multiclient.md](experiments/04_hpx_native_multiclient/hpx_native_multiclient.md) — learned pure-C++ client threads match rayx within ±3%, ruling out Python/GIL as the high-lane bottleneck (later sharpened by the `--diag` decomposition in experiment 06).
* [experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md](experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md) — learned the CPU-bound `work_mode=spin` regime removes the sleep-timer artifact and shows the high-lane saturation knee is a hardware/core-boundary / oversubscription effect, not the HPX worker count.
* [experiments/06_diag_fifo_ceiling_analysis/diag_fifo_ceiling_analysis.md](experiments/06_diag_fifo_ceiling_analysis/diag_fifo_ceiling_analysis.md) — confirmed with preserved diag-1 outputs that the bimodal ceiling is a FIFO-retire / client-driver effect: FIFO `one_by_one` ≈1391 req/s, `batch_wait` ≈2544 req/s, `submit_all_get_all` ≈2971 req/s (context-only); the penalty sits in the client completion/retire phase while lanes stay under-utilized and balanced.
* [experiments/07_rayx_as_completed/rayx_as_completed.md](experiments/07_rayx_as_completed/rayx_as_completed.md) — learned RayX `Engine.wait` reproduces the native as-completed FIFO-retire fix through Python, lifting L16/c32 bimodal throughput from ~1366 to ~2529 req/s (+85%) without a separate Python/GIL ceiling.
* [experiments/08_spin_vs_sleep_coordination/spin_vs_sleep_coordination.md](experiments/08_spin_vs_sleep_coordination/spin_vs_sleep_coordination.md) — learned, contrasting `sleep` vs `spin` at the rayx frontend, that `spin` removes the ~25% sleep overshoot (service exactly 1.0/5.0 ms) and that the high-lane flattening is a CPU/core-boundary effect — parked `sleep` scales linearly to 8 lanes while CPU-bound `spin` only flattens (and its p99 inflates) at L8 — not Python/facade or HPX coordination overhead; lanes stay perfectly balanced by `actor_id`.
* [experiments/09_spin_core_boundary_sweep/spin_core_boundary_sweep.md](experiments/09_spin_core_boundary_sweep/spin_core_boundary_sweep.md) — learned, sweeping spin lanes around the knee while varying `hpx_threads`, that the knee moves *earlier* and the throughput ceiling *lower* as HPX worker threads increase (svc=1 onset L10→L8→L6 for threads 2→4→8) — the oversubscription signature of a fixed core budget, not HPX-worker starvation — while parked `sleep` stays flat and thread-insensitive and lanes stay balanced; sharpens experiment 08's core-boundary reading.
* [experiments/10_varied_batch_service_time/varied_batch_service_time.md](experiments/10_varied_batch_service_time/varied_batch_service_time.md) — learned, using the new true bulk-varied `submit_batch(service_ms=[...])`, that a request's latency in a shared-`submit_ns` batch is set by round-robin lane placement + FIFO queue position: the same 1 ms request swings ~20× between short-first and short-last submission (convoy), and patterns whose long-index period aligns with the lane count segregate heavy work onto a lane subset, halving throughput despite equal per-lane request counts — count-balanced routing is not work-balanced; `spin`/`sleep` change magnitudes, not structure.
* [experiments/11_queued_cancellation/queued_cancellation.md](experiments/11_queued_cancellation/queued_cancellation.md) — learned that queued-only `engine.cancel(future)` honestly skips service for not-yet-started requests (`status="cancelled"`, ~0 service, labels preserved, `True` iff cancelled), halving drain when survivors are balanced; a fixed-delay front cancel exposes the queued-vs-running boundary (more lanes drain the front faster → fewer cancels succeed, those complete), running work is never interrupted, and surviving work can still lane-segregate (cf. experiment 10) so count-balanced cancellation is not work-balanced.
* [experiments/12_chunked_service/chunked_service.md](experiments/12_chunked_service/chunked_service.md) — learned that v1 chunked synthetic service (`submit(service_ms, chunks, chunk_delay_ms)`) splits total active service without changing it (spin: exactly 8.000 ms for chunks 1/2/4/8 at delay 0) and that a parked inter-chunk delay adds lane-occupancy ≈ `(chunks-1)×chunk_delay_ms` (so `service_ms_observed` is lifecycle/lane-occupancy time, not active-only, when delay>0), while each request still returns exactly one future/one row echoing chunks/delay — synthetic timing only, not token streaming (the chunk boundaries it creates are what experiment 13 cancels at).
* [experiments/13_chunk_boundary_cancellation/chunk_boundary_cancellation.md](experiments/13_chunk_boundary_cancellation/chunk_boundary_cancellation.md) — learned that running cancellation stops a *started* chunked request at its next chunk boundary (`1 ≤ chunks_completed < chunks`), never interrupting an in-progress active chunk or parked gap, while a queued cancel still skips the whole lifecycle (`chunks_completed = 0`) and the final chunk is a hard commit point (`cancel()` → `False`, request completes); `cancel()==True` holds iff the row is `status="cancelled"` across queued, running, and a deliberately-racy late case, where sleep's parked overshoot reliably stops at the boundary but spin's exact timing more often lands on the final chunk and completes — synthetic timing only, not token-stream or Ray task cancellation.
* [experiments/14_spin_chunked_service/spin_chunked_service.md](experiments/14_spin_chunked_service/spin_chunked_service.md) — learned, characterizing chunked service under `work_mode="spin"` for HPX-native and rayx only (no Ray, both pinned to 4 HPX threads), that spin preserves total active service *exactly* — 8.000 ms for chunks 1/2/4/8 across lanes 1/4/8 and both engines (vs sleep's ~4–5 ms accumulated overshoot in benchmark 09) — while a parked `chunk_delay_ms` still adds lifecycle ≈ `(chunks-1)×delay` carrying ~25% sleep overshoot on the gaps only; rayx tracks HPX-native on service fidelity (same C++ lane) with a small ~10% throughput edge at the L8 saturation point, and L8 shows the milder spin core-boundary/oversubscription effect from experiments 08/09 (per-lane efficiency ~0.98→0.81–0.95) as a lane×core effect independent of chunk count — synthetic timing only, not token streaming, not inference, not a Ray comparison, machine-specific magnitudes.
* [experiments/15_hpx_native_lane_feasibility/hpx_native_lane_feasibility.md](experiments/15_hpx_native_lane_feasibility/hpx_native_lane_feasibility.md) — learned, from an isolated lane-primitive probe (not comparable to the main corpus; no JSONL schema, retire modes, Ray, or rayx), that the cooperative `hpx::this_thread::sleep_for` has different, tighter overshoot than the blocking `std::this_thread::sleep_for` the lane uses (~+5% vs ~+25% at 20 ms), and that plain `hpx::async` no-op dispatch lands in the same order of magnitude as the `ServiceLane` reference path (both a few million no-op ops/s) — enough to justify *considering* a future opt-in HPX-native lane prototype, not replacing the current `ServiceLane` anchor; synthetic primitive timing only, machine-specific magnitudes.
* [experiments/16_hpx_lane_mechanism_probe/hpx_lane_mechanism_probe.md](experiments/16_hpx_lane_mechanism_probe/hpx_lane_mechanism_probe.md) — learned, from an opt-in lane-*mechanism* probe (native-only `--lane-impl std|hpx`, explicitly incomparable to the corpus — it varies the lane mechanism the corpus holds fixed; hpx rows tagged `boundary=hpx-intra-locality-hpxlane` / `_hpxlane`), that an HPX cooperative lane (`HpxLane`: `hpx::thread` consumer + cooperative `hpx::this_thread::sleep_for`) preserves actor-like FIFO (single `actor_id`, all completed, schema 1, 40/40 runs) while showing strictly lower sleep overshoot than the blocking `ServiceLane` at every sleep service time (1 ms 26→14%, 5 ms 17→13%, 20 ms 13→5%) — so the experiment-15 cooperative-timer advantage *survives in a real FIFO lane* (a qualified GO on the mechanism question), though the magnitude is duration-dependent and the in-lane std profile is not the flat ~25% of the isolated primitive; `HpxLane` is an opt-in axis, not a `ServiceLane` replacement and not a general HPX-scheduler result, synthetic sleep-only timing, machine-specific magnitudes.
* [experiments/17_lane_stats_observability/lane_stats_observability.md](experiments/17_lane_stats_observability/lane_stats_observability.md) — learned, using the observability snapshot `Engine.lane_stats()` (rayx-only, sleep, `num_lanes ∈ {2,4}`, burst `N=lanes*8` submitted without retiring), that a backlog shows up live as a round-robin split (per-lane `queue_depth` spread 0, `num_active==lanes`, `total_queue==N-lanes` with the in-service request uncounted) that drains FIFO monotonically to idle; and that a tail `cancel()` does **not** drop `queue_depth` (cancel doesn't dequeue — the first all-active `total_queue` is still `N-lanes` with or without it), instead showing up as a faster drain / skipped service (~0 cancelled service, a late `total_queue` cliff when the lane pops-and-skips the cancelled tail, ~halved wall) — so `lane_stats()` is a racy, live, observability-only view (gates assert only direction-of-change and the idle endpoint), not scheduler state, placement control, a synchronization primitive, or any JSONL/analyzer/schema change; synthetic sleep-only timing, machine-specific magnitudes.
* [experiments/18_bounded_admission_burst/bounded_admission_burst.md](experiments/18_bounded_admission_burst/bounded_admission_burst.md) — learned, contrasting an unbounded vs a capped `Engine` (`max_queue_depth_per_lane=None` vs `3`) under a bursty overflow (rayx-only, sleep, `num_lanes=4`, `service_ms=40`, burst `N=lanes*8=32`, 3 repeats), that bounded admission is **local per-lane admission by rejection**: the unbounded engine admits all 32 and the per-lane backlog grows deep (peak `queue_depth=7`, `queue_wait` p99 ≈ 310 ms), while the capped engine pins per-lane `queue_depth` at exactly the cap (3, never exceeded in any sample), admits `lanes*(cap+1)=16` (one active + `cap` queued per lane — the active request is *extra*) and sheds the other 16 as caller-visible `QueueFullError` (no Future, no row), giving admitted work a bounded tail (`queue_wait` p99 ≈ 134 ms, ~half the drain span) — so the cap trades shed load for bounded backlog/tail, and is **not** Ray Serve backpressure, distributed flow control, blocking backpressure, a scheduler, or a global cap, with no API / `ServiceLane` / driver / analyzer / JSONL-schema change; structural gates are the result, magnitudes machine-specific.
* [experiments/19_bounded_admission_offered_load/bounded_admission_offered_load.md](experiments/19_bounded_admission_offered_load/bounded_admission_offered_load.md) — learned, the **sustained-flow** companion to experiment 18: under a *continuous* producer (rayx-only, sleep, `num_lanes=4`, `service_ms=40`, fixed inter-arrival, 800 ms, 3 repeats) at ~50 req/s (below the ~90 req/s lane capacity) and ~200 req/s (≈2× over), that bounded admission only *needs* to act under sustained overload — below capacity both modes stay at `queue_depth` 0–1 and the cap sheds nothing, but over capacity the **unbounded** engine's per-lane backlog **grows for the whole window** (`total_queue` 0→87, `queue_depth` reaching 22 and still climbing), while the **capped** engine **pins** per-lane `queue_depth` at the cap (3, never exceeded) so backlog **plateaus** (`total_queue` ≈ 10–12) and sheds the overflow (admitted 85, rejected 76 of 161) as caller-visible `QueueFullError` (`rows == admitted == attempted − rejected`); so the cap trades shed work for bounded backlog/admitted-tail and is **not** Ray Serve backpressure, distributed flow control, blocking backpressure, or a global cap, with no API / `ServiceLane` / driver / analyzer / JSONL-schema change — the grow-vs-plateau trajectory is the durable structural result, while the latency magnitudes are reported-only and dominated by the closed-loop retire-at-end driver pattern (below-capacity `queue_wait` p50 ≈ 357 ms at `queue_depth=1` is the holding artifact, not lane queueing).
* [experiments/20_hpx_task_dataflow_probe/hpx_task_dataflow_probe.md](experiments/20_hpx_task_dataflow_probe/hpx_task_dataflow_probe.md) — learned, from a native-only opt-in mechanism probe (own schema `hpx-task-dataflow-probe-1`, **not** the benchmark corpus, **not** a rayx feature) serving identical synthetic sleep work through serialized lanes (`ServiceLane`/`HpxLane`, unmodified) vs scheduler-placed HPX pools (`hpx::async` / `hpx::dataflow` / `hpx::async(...).then`), that exactly **two** lane contracts are **universal** (one result row per request, per-request `hpx::future` ownership — every mechanism keeps them) while the **six lane-specific** ones split by mechanism: lanes preserve all (FIFO `inversions=0`, real `actor_id`), but the pools **relax** identity (`pool_id` + `lane_identity="n/a"`, ~4 workers) and FIFO (thousands of `end_ns` inversions) and leave `queue_depth`/`active`/per-lane-cap/lane-targeted-cancel **n/a** with no lane queue to measure or cap (not faked); the pools' high sleep "throughput" (~34k/s at `service_ms=5` vs the lane's serialized ~160–178/s) is **cooperative parked-wait overlap (unbounded concurrency), not faster serving** — and at the `service_ms=0` no-op floor the serialized lane is actually *faster* (~2.6M/s vs ~1.2–1.9M/s) — so the one-at-a-time serialization **is** the actor contract, and the evidence-backed decision is that the near-term "HPX-native inside" win is the contract-preserving cooperative `HpxLane` (a clean drop-in behind the API) whereas a task/dataflow pool dissolves the per-lane contracts and is a separate future axis with its own non-lane model, not a drop-in backend; continuations compose **below** the caller-visible future (no Python exposure), and this is **not** Ray Serve, object store, arbitrary remote Python, real inference, or an HPX-beats-Ray claim — synthetic sleep timing only, magnitudes machine-specific and reported, not gated.
* [experiments/21_rayx_hpxlane_backend_parity/rayx_hpxlane_backend_parity.md](experiments/21_rayx_hpxlane_backend_parity/rayx_hpxlane_backend_parity.md) — rayx-only parity battery comparing `Engine(lane_impl="std")` / `ServiceLane` against `Engine(lane_impl="hpx")` / `HpxLane` across completion, `wait` / `as_completed`, chunking, cancellation, bounded admission, `lane_stats()`, and actor_id prefixes; semantics/parity evidence only, with timing recorded as non-gating observation.
* [experiments/22_rayx_hpxlane_load_divergence/rayx_hpxlane_load_divergence.md](experiments/22_rayx_hpxlane_load_divergence/rayx_hpxlane_load_divergence.md) — learned, the load follow-on to exp21's parity result (rayx-only, own schema `rayx-hpxlane-load-divergence-1`, one subprocess per `(backend, hpx_threads)` since the HPX runtime is process-fixed): with the contract held under load (firm structural gates G1–G5 — completion, per-lane FIFO `inversions=0`, `lane_stats()` sanity, `actor_id` prefix, cross-backend completion parity), the two backends **structurally diverge along how concurrency is bounded** — under parked **sleep** service both overlap (`overlap_ratio` ≈ `num_lanes`, and `HpxLane` overlaps all lanes even at `hpx_threads=1` because cooperative parking yields the worker), while under non-yielding **spin** (a synthetic CPU-bound diagnostic mode) `ServiceLane` overlap follows the OS/core count (~`num_lanes`, independent of `hpx_threads`) whereas `HpxLane` is **bounded near `hpx_threads`** (spin `overlap_ratio` ≈ 1.0 / 2.0 / 3.83 at `hpx_threads` 1 / 2 / 4, `num_lanes=8`); this is a scheduling-**mechanism** difference recorded as **observation only** (`drain_wall_ms`, `overlap_ratio`, sampled `max_active_lanes`), never gated — `lane_stats().active` is not a perfect proof of true worker-level concurrency (under spin saturation the stats hop is starved, so `HpxLane` `max_active_lanes` reads low, a measurement artifact, not zero concurrency) — and it is **not** the exp16 native single-lane probe, **not** the exp20 task/dataflow probe, not Ray Serve / object store / real inference, and **not** an HPX-beats-Ray claim; magnitudes machine-specific and reported.
* [experiments/23_rayx_hpxlane_adapter_hop_cost/rayx_hpxlane_adapter_hop_cost.md](experiments/23_rayx_hpxlane_adapter_hop_cost/rayx_hpxlane_adapter_hop_cost.md) — learned, on an idle worker pool (rayx-only, schema `rayx-adapter-hop-cost-1`, `num_lanes=1`, no-op sleep path, `spin` not used), that the `RayxLaneAdapter<HpxLane>` `run_as_hpx_thread` hop adds a single-digit-to-tens-of-µs per-call cost over the no-hop `ServiceLane` path — best approximated by `lane_stats()` (`std` p50 ≈ 0.2 µs vs `hpx` ≈ 2 µs at `hpx_threads=1`, ≈ 5.5 µs at 4; the cost **rises** with pool size) — orders smaller than the corpus's ms-scale synthetic service, so a hop-reduction source slice looks weakly justified for serving-shaped workloads; firm gates are structural only (operations complete, prefixes, sample counts, same op set), all timing and the std-vs-hpx delta are **observation-only** and the delta is the closest approximation of the hop-dominated boundary cost, not a perfect subtraction (the `submit_get` end-to-end delta even flips sign by pool size, so it is dispatch-dominated, not a hop measure); no speedup, no faster/slower verdict, separate from exp21/exp22.

</details>

* [CLAUDE.md](CLAUDE.md) — working rules and project guardrails.

