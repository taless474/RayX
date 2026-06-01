# RayX

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
* Not benchmarking real model inference.

## Current status

* Ray actor baseline, HPX-native synthetic baseline, and the `rayx` Python
  frontend all exist and run.
* The full comparison arc has been executed end-to-end and validated against the
  v1 metrics schema (each run's JSONL passes the analyzer rollup): single-lane,
  service × concurrency matrix, multi-lane scaling, variable (bimodal) service,
  lane sweeps, a multi-client-thread driver (Python and native C++), and a
  CPU-bound `work_mode=spin` mode with a saturation-knee sweep.
* Headline result below; per-experiment result notes in the documentation map.

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
`_rayx` is built.

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
notes); result notes live under `benchmarks/` and `experiments/<name>/`.

* [docs/project_proposal.md](docs/project_proposal.md) — motivation, hypothesis, scope, phases.
* [docs/ray_hpx_mapping.md](docs/ray_hpx_mapping.md) — Ray ↔ HPX conceptual mapping and project direction.
* [docs/experiment_plan.md](docs/experiment_plan.md) — measurement contract, schemas, workload matrix.
* [docs/hpx_build_notes.md](docs/hpx_build_notes.md) — HPX build/install notes (source build, prefix, CMake invocation).
* [benchmarks/01_ray_single_actor_baseline/ray_single_actor_baseline.md](benchmarks/01_ray_single_actor_baseline/ray_single_actor_baseline.md) — learned one Ray actor is a single serialized server: concurrency deepens its queue rather than parallelizing, so tiny requests are actor/process-overhead-bound and 20 ms requests cap at ~43 req/s.
* [benchmarks/02_ray_hpx_single_lane_comparison/ray_hpx_single_lane_comparison.md](benchmarks/02_ray_hpx_single_lane_comparison/ray_hpx_single_lane_comparison.md) — learned HPX's intra-locality C++ control path is far cheaper than Ray's actor-process boundary for tiny/no-op work, and the two converge as service time grows (different boundaries; not a general "HPX is faster" claim).
* [benchmarks/03_ray_hpx_matrix_comparison/ray_hpx_matrix_comparison.md](benchmarks/03_ray_hpx_matrix_comparison/ray_hpx_matrix_comparison.md) — learned that overhead-vs-service crossover holds across the whole service_ms × concurrency matrix: HPX dominates at tiny service, both converge once the serialized lane dominates.
* [benchmarks/04_ray_hpx_scaling_comparison/ray_hpx_scaling_comparison.md](benchmarks/04_ray_hpx_scaling_comparison/ray_hpx_scaling_comparison.md) — learned that with real per-request service both scale throughput (HPX near-ideal at 5/20 ms, Ray near-ideal at 20 ms but sublinear at 5 ms), while apparent no-op negative scaling is the single-client retire loop, not a runtime limit.
* [benchmarks/05_ray_hpx_retire_mode_noop/ray_hpx_retire_mode_noop.md](benchmarks/05_ray_hpx_retire_mode_noop/ray_hpx_retire_mode_noop.md) — learned the no-op multi-lane regression is a client-loop / cross-thread coordination artifact, not an HPX lane-scaling limit; batch retirement reduces but does not eliminate it.
* [benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md](benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md) — learned the rayx Python frontend preserves most of HPX's low control-plane overhead and stays far below Ray's actor-process cost for native work, without being a Ray replacement.
* [docs/reference/rayx_actor_api.md](docs/reference/rayx_actor_api.md) — rayx Python API (Engine / SyntheticActor) and the façade equivalence result.
* [docs/reference/rayx_submit_batch.md](docs/reference/rayx_submit_batch.md) — rayx `submit_batch()` bulk-submit path and first throughput benchmark (engine/actor/batch).
* [benchmarks/07_rayx_variable_service/rayx_variable_service.md](benchmarks/07_rayx_variable_service/rayx_variable_service.md) — learned that under bimodal load more lanes sharply cut rayx queueing and absolute tail latency (1→4 lanes: total p99 78→28 ms, ~2.08× throughput bounded by concurrency); compare absolute p99, not the p99/p50 ratio.
* [benchmarks/08_ray_hpx_rayx_variable_service/ray_hpx_rayx_variable_service.md](benchmarks/08_ray_hpx_rayx_variable_service/ray_hpx_rayx_variable_service.md) — learned that under one shared deterministic sequence single-lane engines rank by control overhead (rayx ≈ HPX-native > Ray) and converge on the 20 ms service tail, while Ray's fixed per-request overhead keeps its absolute tail higher even as lanes scale.
* [experiments/01_sleep_overshoot/sleep_overshoot_note.md](experiments/01_sleep_overshoot/sleep_overshoot_note.md) — learned HPX/rayx `sleep_for` carries a stable ~25% proportional service overshoot vs Ray's ~5%, a backend sleep-fidelity gap (not control cost) that must be read separately when comparing cross-engine service/total.
* [experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md](experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md) — learned the bimodal lane/actor sweep (1–16) plateaus near ~1390 req/s; the original "coordination ceiling" reading is refined/superseded — it is a closed-loop FIFO-retire / client-driver ceiling (see the top banner in that note).
* [experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md](experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md) — learned multiple client threads lift the ~1390 ceiling to ~1740 but saturate quickly; its original GIL-bound reading is superseded by the native C++ driver below.
* [experiments/04_hpx_native_multiclient/hpx_native_multiclient.md](experiments/04_hpx_native_multiclient/hpx_native_multiclient.md) — learned pure-C++ client threads match rayx within ±3%, ruling out Python/GIL as the high-lane bottleneck (later sharpened by the `--diag` decomposition in experiment 06).
* [experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md](experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md) — learned the CPU-bound `work_mode=spin` regime removes the sleep-timer artifact and shows the high-lane saturation knee is a hardware/core-boundary / oversubscription effect, not the HPX worker count.
* [experiments/06_diag_fifo_ceiling_analysis/diag_fifo_ceiling_analysis.md](experiments/06_diag_fifo_ceiling_analysis/diag_fifo_ceiling_analysis.md) — confirmed with preserved diag-1 outputs that the bimodal ceiling is a FIFO-retire / client-driver effect: FIFO `one_by_one` ≈1391 req/s, `batch_wait` ≈2544 req/s, `submit_all_get_all` ≈2971 req/s (context-only); the penalty sits in the client completion/retire phase while lanes stay under-utilized and balanced.
* [experiments/07_rayx_as_completed/rayx_as_completed.md](experiments/07_rayx_as_completed/rayx_as_completed.md) — learned RayX `Engine.wait` reproduces the native as-completed FIFO-retire fix through Python, lifting L16/c32 bimodal throughput from ~1366 to ~2529 req/s (+85%) without a separate Python/GIL ceiling.
* [CLAUDE.md](CLAUDE.md) — working rules and project guardrails.

