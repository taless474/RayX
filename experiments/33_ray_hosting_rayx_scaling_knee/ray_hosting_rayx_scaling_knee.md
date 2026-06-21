# Intra-actor RayX/HPX CPU scaling knee (granularity-sensitivity envelope, observation-only)

> **Observation-only result on one homogeneous many-core Linux node.** exp33 has
> now been run on an appropriate homogeneous many-core Linux node (Rostam; see
> *Results*). It maps a **scaling envelope / granularity-sensitivity surface, not a
> single universal knee**: the knee moves with operation granularity. The result is
> **observation-only** and **machine-specific**. It is **not** "RayX beats Ray",
> **not** "RayX makes Ray faster", **not** "HPX beats Ray", **not** "RayX replaces
> Ray", **not** Ray cluster scaling, and **not** a benchmark / sizing / capacity
> claim. It **does not reinterpret exp32**; it adds the deferred knee follow-up.
> Any output on an Apple-silicon laptop remains **smoke-only, not evidence**.

## Question

Inside **one long-lived Ray actor** (one process, one Ray boundary held
constant), **where** does intra-process RayX/HPX native Async CPU scaling
(`busy_sum`) stop being efficient as the worker count `W` grows — and **how does
that knee move with operation granularity** (the per-op work size `n`, which also
sets the derived `checkpoint_count = ceil(n / BUSY_SUM_STRIDE)`)?

This is the deferred **saturation-knee** follow-up to exp32. exp32 established (on
homogeneous many-core Linux) that intra-actor RayX/HPX `busy_sum` scales while an
in-process Python CPU loop stays GIL-bound, and it deliberately **capped `W` at 4**
on the Apple-silicon laptop, leaving the `W>4` knee to "a homogeneous many-core
box". exp33 is that box's tool. exp33 asks only the *knee* question; it asserts
nothing new about exp32's `W≤4` SUPPORT reading.

## Methodology

* **Same Ray-hosted shape as exp32.** One long-lived Ray actor hosts one
  `rayx.runtime.Runtime(num_lanes=W, hpx_threads=W)`. `ActorHandle` /
  `RuntimeFuture` / `OperationResult` are created and retired **inside** the actor;
  only plain scalars/containers cross the Ray boundary.
* **Load op:** RayX native **Async** `busy_sum` (the Async DispatchPolicy is what
  lets ops overlap across the HPX worker pool; an Inline op would not scale).
* **Strong scaling of a fixed batch, per (granularity, leg) normalized.** For each
  granularity `n`, a batch is `K` `busy_sum(n)` ops with `K` **held fixed** across
  the `W` sweep. `speedup(W) = T(W=1)/T(W)`, `efficiency = speedup / W`.
* **`K ≥ max(W)` for every cell** — we use `K = 2·max(W)` so a batch can always
  keep every worker occupied. (exp32's earlier `K=4` capped occupancy once `W>4`;
  exp33 does not repeat that.)
* **Granularity sweep (≥ 2 sizes).** A smaller and a larger `n`. Because
  `checkpoint_count = ceil(n / BUSY_SUM_STRIDE)` is **derived** from `n`, the
  granularity sweep doubles as a coarse checkpoint-count sweep, reported per cell.
* **Checkpoint stride is fixed, not swept.** `BUSY_SUM_STRIDE` is a compile-time
  `constexpr` in `runtime_ops.hpp`, **not** a runtime op parameter. We deliberately
  do **not** modify C++ to make it one, so the available lever is `n` (which moves
  `checkpoint_count`).
* **Knee detection (observational).** Per granularity, the knee is the **first
  `W>1` whose efficiency falls below a conservative threshold (`0.70`)**; otherwise
  "no clear knee in tested range" with the minimum efficiency reported. This is a
  **reading, not a pass/fail gate**.
* **Optional small Python leg (`--with-py`).** A pure-Python serial GIL reference,
  run **only at the smallest granularity with a capped `K`**. It is a flatness
  reference, not the focus — exp32 already made the GIL point.
* **Structural gates only** (`agg_ok`, `futures_completed`, `lane_ids_ok`,
  `plain_types_ok`, `clean_shutdown`). **Timing/efficiency is never a pass/fail
  gate.** Exit 0 = gates passed (or cleanly skipped); exit 1 = a structural gate
  failed.
* **Two timing points:** in-actor wall (engine-clean) and end-to-end wall
  (includes the Ray boundary, not a pure engine metric).

### Reported per cell

`W`, `K`, granularity `n` and its derived `checkpoint_count`, in-actor median ms,
spread, speedup vs `W=1`, efficiency (`speedup/W`), end-to-end median ms, and the
structural gates. Plus a per-granularity knee reading.

## Required machine type

A trusted knee reading requires a **homogeneous many-core Linux node**:

* a single Linux workstation / bare-metal server (not a cluster, not a laptop);
* **homogeneous cores** — all cores the same type (avoid ARM big.LITTLE and Intel
  P-core/E-core hybrids);
* ideally Xeon / EPYC / Threadripper / older homogeneous Core / Ryzen;
* enough cores to reach the sweep: `W=16` wants ~16 **physical** cores, and the
  `W=32` cell is only added when `os.cpu_count()` (logical, incl. SMT) `≥ 32`.
  Because the cap is on *logical* CPUs, a high-`W` cell can land on SMT siblings
  on a smaller box — prefer SMT/HT off, and check `lscpu` "Thread(s) per core";
* not thermally throttled; not a noisy shared VM.

`--full` does **not** drop high-`W` cells (reaching/exceeding the core count is how
the knee becomes visible); instead it **warns** when a `W` exceeds the logical CPU
count, so oversubscription rolloff is not misread as a true scaling knee.

## Commands

```
python -m py_compile experiments/33_ray_hosting_rayx_scaling_knee/run_ray_hosting_rayx_scaling_knee.py

# cheap local structural validation (Mac/laptop-safe; SMOKE-ONLY, not evidence)
python experiments/33_ray_hosting_rayx_scaling_knee/run_ray_hosting_rayx_scaling_knee.py --smoke

# homogeneous many-core Linux knee sweep (run 2-3 times; send full output)
python experiments/33_ray_hosting_rayx_scaling_knee/run_ray_hosting_rayx_scaling_knee.py --full

# add the small optional pure-Python GIL reference to either mode
python experiments/33_ray_hosting_rayx_scaling_knee/run_ray_hosting_rayx_scaling_knee.py --full --with-py
```

* `--smoke` (default): `W ∈ {1,2,4}`, `K=8`, small granularities, few reps. Runs
  in well under a minute. Exercises every code path (granularity sweep, knee
  detection, gates) but is **smoke-only**.
* `--full`: `W ∈ {1,2,4,8,16}` (plus `32` when `cpu_count ≥ 32`), `K = 2·max(W)`,
  small + large granularity, more reps. Intended for homogeneous many-core Linux.

Requires Ray and the built `_rayx`; the runner skips cleanly (exit 0) if either is
unavailable. Every mode prints a compact `machine-info` block. No JSONL/corpus
artifact is written. **Do not run `--full` on this Mac/laptop as evidence.**

## Interpretation guardrails

* The supported claim, **only on appropriate hardware and only if the data
  supports it**: "intra-process RayX/HPX native Async CPU scaling stays efficient
  up to `W=<knee>` for this op granularity on this machine, and rolls off beyond
  it."
* **Forbidden:** "RayX makes Ray faster", "HPX beats Ray", "RayX replaces Ray",
  Ray cluster scaling, any benchmark / sizing / capacity guidance, and any raw
  Python-vs-RayX wall-time speedup claim.
* The knee is an **observation**, not a verdict; timing/efficiency is never a
  pass/fail gate.
* The scaling bound is `min(num_lanes, hpx_threads, physical cores)` (cf. exp22 /
  exp31 / exp32); a knee near the physical-core count is expected, and an
  oversubscribed-`W` rolloff is not a true scaling limit.
* End-to-end numbers include the Ray actor boundary and are not a pure engine
  metric.
* exp33 **does not reinterpret exp32**; it adds the deferred knee tool only.

## Results (Rostam, homogeneous many-core Linux)

**Provenance.**

* Commit: `2e14a068fc47081ee9cd8a84f787c152ee543b18`.
* Machine: homogeneous Linux node, **Intel Xeon Gold 6148 @ 2.40 GHz**, **40
  physical cores**, **2 sockets × 20 cores**, **Thread(s) per core = 1** (SMT off),
  **performance** governor, **exclusive Slurm allocation**; Linux 5.14, Python
  3.12.3, Ray 2.55.1. **No explicit affinity/pinning evidence** — so no per-socket
  placement claim is made and no socket/NUMA cause is attributed.
* exp33 `--full` settings: `W ∈ {1,2,4,8,16,32}`, `K = 64` (`= 2·max(W)`),
  `reps=5`, `warmup=1`, `with_py=False`, `knee_efficiency = 0.70`, granularities
  `n = 2,000,000` (derived `checkpoint_count = 245`) and `n = 20,000,000` (derived
  `checkpoint_count = 2442`).
* Three independent runs (`full_run1`, `full_run2`, `full_run3`); **all three
  STRUCTURAL GATES: PASS**. Numbers below are curated from the raw Rostam run logs
  (kept outside the repo); a concise curated `aggregate.json` sits beside this note.

**Curated cross-run table** (efficiency `= speedup/W`; spread is intra-run
dispersion at that cell; in-actor / engine-clean view):

| granularity `n` | W   | efficiency (run1 / run2 / run3) | speedup (approx)   | spread (run1 / run2 / run3)   | knee reading           |
| --------------- | --- | ------------------------------- | ------------------ | ----------------------------- | ---------------------- |
| 2,000,000       | 16  | 0.89 / 0.88 / 0.88              | ≈14.06–14.17       | 0.7% / 1.0% / 0.7%            | —                      |
| 2,000,000       | 32  | 0.61 / 0.68 / 0.61              | ≈19.40–21.75       | **1587.8% / 8.2% / 1791.8%**  | **knee at W=32** (×3)  |
| 20,000,000      | 16  | 0.97 / 0.96 / 0.97              | ≈15.44–15.52       | 0.3% / 0.5% / 0.7%            | —                      |
| 20,000,000      | 32  | 0.92 / 0.90 / 0.92              | ≈28.95–29.36       | 27.5% / 35.3% / 3.5%          | **no clear knee** (×3) |

**What the three runs show.**

* **Granularity sensitivity reproduces across all three runs.** At fine
  granularity (`n=2,000,000`) the knee sits at **W=32** in every run; at coarse
  granularity (`n=20,000,000`) there is **no clear knee** through W≤32 in every
  run. The knee **moves with operation granularity** — exp33 maps a *scaling
  envelope*, not one universal knee.
* **Fine-grain W=32 is stable-degraded with intermittent extreme dispersion.** The
  *degradation* is stable (efficiency 0.61 / 0.68 / 0.61, sub-`0.70` in all three),
  echoing the exp32 W=32 stable-degraded/noisy regime; the *extreme spread* is
  **intermittent** (1587.8% / 8.2% / 1791.8% — run2 is nearly tight while the
  degradation persists). (Note: `K=64` here vs exp32's `K=32`, so the efficiency
  numbers are the same *kind* of regime, not directly comparable values.)
* **Coarse granularity restores W=32 efficiency.** Per run, `n=2M → n=20M` at W=32
  lifts efficiency 0.61→0.92, 0.68→0.90, 0.61→0.92, and the dispersion is **greatly
  reduced relative to the fine-grain extreme** — coarse W=32 spread is 27.5% /
  35.3% / 3.5%, i.e. **no extreme spread like the fine-grain W=32 cells**, though
  not uniformly tiny. The fine-grain rolloff is **granularity-sensitive**,
  consistent with per-op / scheduling overhead becoming significant at fine
  granularity and high `W` (the lever actually swept: `n` → `checkpoint_count`).
* W=16 is clean-stable at both granularities (efficiency ≈0.88–0.89 fine,
  ≈0.96–0.97 coarse; low spread), consistent with exp32's clean-through-W=16
  reading.

**Final allowed claim (observation-only, machine-specific).** On this homogeneous
40-core (2×20, 1 thread/core, performance governor, exclusive Slurm allocation, no
affinity set) Xeon node, inside one long-lived Ray actor, intra-process RayX/HPX
native Async CPU scaling (`busy_sum`) is **granularity-sensitive**: at fine
granularity (`n=2,000,000`) it stays efficient through W=16 and rolls off by W=32
(knee at W=32, with high and intermittent intra-run dispersion); at coarse
granularity (`n=20,000,000`) it stays efficient across the full tested range
(W=16 and W=32) with no clear knee through W≤32 and no extreme dispersion. The
knee **moves with operation granularity** rather than sitting at a fixed `W`. These
are end-to-end-inclusive measurements reported on the in-actor (engine-clean) view;
**no socket/NUMA cause is attributed without affinity evidence**, and this is **not**
a Ray comparison, performance, sizing, or capacity claim.

**Diagnostics remain conditional — not a required next task.** The fine-grain W=32
rolloff is reproducible *and already explained* by the granularity lever (coarsening
`n` removes both the degradation and the extreme dispersion). Diagnostics
(e.g. affinity-controlled / `numactl` runs to probe a socket/NUMA cause) are
justified **only** if a real (non-synthetic) workload needs efficient fine-grain
work at W>16 on this class of hardware, **or** if later evidence leaves a
decision-relevant unexplained rolloff. Until then exp33 stands as an
observation-only granularity-sensitivity result.

## Current status (tool)

`--smoke` validates the structure on a laptop (gates pass, both knee branches
exercised) but is explicitly **smoke-only, not evidence** — on Apple silicon the
`W>4` region is confounded by P/E-core asymmetry, SMT, and thermal behavior. The
trusted knee reading above comes from `--full` on the homogeneous many-core Linux
node (Rostam) documented in *Results*.
