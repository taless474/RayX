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
composition, an HPX-island lifecycle policy, and a two-node path characterization.
The full chronological **“what we learned”** index lives in
[docs/evidence_index.md](docs/evidence_index.md); the headline summary is below.

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

## Current evidence summary

RayX has **three validated, bounded properties**, all observation-only and
machine-specific:

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

### Two-node path characterization (experiments 58–60)

A two-node Rostam (medusa00/medusa01, eno16, `10.42.5.`) characterization of the
**QD1 closed-`int64` micro-call path**. The two sides are on **strictly different
measurement planes** and are **not the same axis**:

* **Ray plane** — Python/`ray.get`-observed actor RTT.
* **HPX plane (exp58/exp60)** — caller-observed C++ `hpx::async(...).get()` RTT.

| evidence | plane | reps | p50 | p99 |
|---|---|---|---|---|
| **exp58** HPX inter-node TCP | caller-observed C++ `hpx::async().get()` | R=5 | ~115.8 µs | ~185.7 µs |
| **exp59** Ray two-node actor path | Python/`ray.get`-observed | R=5 | ~742 µs | ~1190 µs |
| exp59 Ray same-host control | Python/`ray.get`-observed | R=1 | ~609 µs | ~733 µs |
| **exp60** HPX same-node TCP control | caller-observed C++ `hpx::async().get()` | R=5 | ~76.6 µs | ~101.8 µs |

**Within-runtime decompositions** (each stays inside its own runtime, never
crossed): Ray's same-host ~609 µs of cross-node ~742 µs gives a ~133 µs cross-node
increment (R=1 same-host caveat); HPX's same-node ~76.6 µs of inter-node ~115.8 µs
gives a ~39 µs wire increment (exp60 is **within-HPX decomposition only**; kernel
loopback ≠ zero cost). **Shared reading:** in both runtimes the QD1 floor is
dominated by **local stack, not the physical inter-node hop**.

exp59 placement is proven by hard `NodeAffinity(soft=False)` + resolved Ray
`node_id` + FQDN-normalized hostname; oracle correctness proves the intended actor
executed and returned the expected closed-`int64`, **not** physical placement by
itself. **No speedup, no ratio, no “HPX beats Ray”, and no same-axis Ray-vs-HPX
comparison.** Detail and provenance: [docs/evidence_index.md](docs/evidence_index.md).

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

* [docs/evidence_index.md](docs/evidence_index.md) — the chronological **“what we learned”** index for every benchmark and experiment arc: main benchmark arc (01–10), frontend/serving-control + `HpxLane` (01–23), `rayx.runtime` / local actors (24–26) and the endpoint seam (42–43), Ray-hosting composition (27–30), runtime/adapter (31–38), in-process HPX composition (39–44), HPX-island lifecycle / Ray-orchestrated bootstrap (49–52), and the two-node path characterization (58–60).
* Source write-ups live beside the code under [benchmarks/](benchmarks/) and [experiments/](experiments/).

### Project rules

* [CLAUDE.md](CLAUDE.md) — working rules and project guardrails.
