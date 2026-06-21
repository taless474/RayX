# Intra-actor RayX/HPX serving-shaped latency-mix probe (observation-only, slices 1+2)

> **HPX-honest caveat (read first).** exp34 is an **observation-only,
> machine-specific** probe of the **current Ray-hosted Runtime/lane adapter**. The
> current `ServiceLane` / `RuntimeLane` adapter serves each lane as a **FIFO
> queue**, so any head-of-line behavior observed here is a property of **this
> adapter**, **not** a general HPX scheduling property — real HPX latency control
> under a heterogeneous mix is a **priority + cooperative-yield** story at fixed
> worker count, and **HPX priorities are explicitly out of scope** here. So exp34
> **cannot evaluate HPX-native priority scheduling**; it can only observe the
> current adapter under a synthetic serving-shaped mix, with **fixed-W behavior
> primary** and any W set **secondary** (not an exp32-style scaling result). exp34
> has **two paths**: a default **S/C FIFO-adapter baseline** (slice 1,
> `BASELINE/STOP` only — SUPPORT unreachable by construction) and an opt-in
> **`--with-parked` cooperative-overlap path** (slice 2, SUPPORT-capable). The
> parked class **Wp** is **synthetic cooperative overlap, not real I/O**, carries
> **no** latency-SLO claim, and parked readings **never assert wall-clock**
> (`overlap_ratio` is approximate/structural and never a gate). **W=32 / NUMA /
> binding / `lane_impl="hpx"` are out of scope.** This is **not** "RayX makes Ray
> faster", **not** "HPX beats Ray", **not** "RayX replaces Ray", **not** Ray cluster
> scaling, **not** Ray Serve, and **not** real inference.

## Research question

Inside **one long-lived Ray actor** (one process, one `Runtime`, one Ray boundary
held constant), at a **fixed clean worker count `W`**, does a **synthetic
serving-shaped mix** of small-but-real CPU work (class **S**) and heavier CPU work
(class **C**) show a **fixed-W scheduling/interference effect** that is **not
merely exp32 homogeneous CPU strong-scaling**?

Fixed-W is the headline because, in an HPX-native view, latency under a
heterogeneous mix is a **scheduling/yield** question at fixed worker count, not a
matter of adding OS workers. The optional W set (`--full`) is **secondary** and
must not be read as an exp32-style scaling curve.

## Workload (two paths)

* **One Ray actor, one `Runtime(num_lanes=W, hpx_threads=W)`**, `lane_impl="std"`
  (the `ServiceLane`/`RuntimeLane` FIFO anchor). `RuntimeFuture` /
  `OperationResult` are created and **retired inside** the actor; only plain
  Python rows/aggregates cross the Ray boundary.
* **Class S — short, small-but-real CPU:** Async `busy_sum(n_s)`.
* **Class C — heavier CPU:** Async `busy_sum(n_c)`.
* S/C are **Async `busy_sum`** so they actually dispatch onto the HPX worker pool.
  We deliberately **do not** use the instantaneous Inline `square`/`add` as the S
  latency signal — a sub-microsecond op would measure lane + Python-boundary
  overhead, not HPX.
* **Class Wp — synthetic parked wait (opt-in `--with-parked`, slice 2):** Async
  `park_ms(ms)`. `park_ms` **cooperatively suspends the HPX task** (frees the
  worker) and parks in 10 ms chunks; it **echoes `ms` back** as its value. Wp is
  **synthetic cooperative overlap, not real I/O**, not inference, not a latency-SLO
  result. It is the **only** SUPPORT-capable mechanism in exp34, because
  cooperative parking lets parked waits overlap compute — a property with no
  CPython analog and not reducible to exp32 homogeneous CPU strong-scaling.
* **Deterministic mix:** exact per-class counts, seeded shuffle, so the aggregate
  count is deterministic. Baseline: `n_c = round(N·F_C)`, `F_C=0.25`, `n_s = N −
  n_c`. Parked: `n_c = round(N·0.20)`, `n_wp = round(N·0.30)`, `n_s = N − n_c −
  n_wp`. Per-class counts are reported in every config line.
* **Closed-loop load:** the actor keeps ~`outstanding` requests in flight,
  submitting a replacement as each retires, so **offered load** is a real,
  controlled factor (see *Load-level factor*).

## Why this is more serving-shaped than exp32/33 (but still synthetic)

exp32/33 ran a **homogeneous** batch of identical `busy_sum` ops — pure
strong-scaling, headline = throughput/efficiency. A serving control plane handles
a **heterogeneous mix** (many cheap requests, a few expensive ones) where the
metric that matters is **per-class latency under load**, not aggregate throughput.
exp34 moves toward that shape — request classes + per-class latency + a closed-loop
load level — while staying strictly **synthetic and controlled**: fixed registered
native ops only, fixed seed, fixed mix/counts, no real model, no Ray cluster, no
Ray Serve. It is a more serving-shaped *question* layered on the same Ray-hosted
`Runtime` composition, not a new system.

## Metrics and timing caveats

All metrics are **observation-only**; **timing is never a pass/fail gate**.

* **Throughput:** overall and **per-class** (requests / in-actor second).
* **Per-class total-latency p50 / p90** from `OperationResult.total_ms`.
* **Per-class p99 only when the pooled per-class sample count `>= 100`**
  (`P99_MIN_SAMPLES`); otherwise printed as **`NA`**. Per-class **sample counts**
  are always printed, so a missing p99 is self-explaining.
* **`queue_wait_ms` / `service_ms_observed`: approximate context only.**
  `queue_wait_ms` crosses the **Python submit clock** and the **native service
  clock** (different domains) and is **clamped at 0**, so it is **not** used for
  per-class p99 queue/service attribution. **Precise tail attribution would
  require native enqueue/dequeue timing in C++, which is out of scope for exp34.**
* **In-actor wall and end-to-end wall reported separately;** end-to-end includes
  the **Ray actor boundary** and is **not** a pure engine metric.
* **Parked-overlap context (`--with-parked` only; approximate/structural, never a
  gate, never a wall-clock claim):** `Wp` count, `park_ms`,
  `total_parked_demand_ms = Wp_count · park_ms · reps`, `in_actor_wall_ms`, and
  `overlap_ratio = total_parked_demand_ms / in_actor_wall_ms`. `overlap_ratio > 1`
  means parked demand was **absorbed/overlapped** rather than paid serially (a
  cooperative-parking signal). It is **not** a wall-clock performance claim and is
  read **alongside** the S/C table (it does not, by itself, prove park-vs-compute
  overlap). `queue_wait_ms` p99 decomposition and cross-clock timing remain **not**
  load-bearing.

## Load-level factor (primary)

Offered load relative to the per-`W` knee, via the closed-loop `outstanding`
window:

* **under-knee:** `outstanding = max(1, W//2)` — lanes underutilized, little
  queueing.
* **near-knee:** `outstanding = W` — roughly matched.
* **over-knee:** `outstanding = min(4W, N)` — more in flight than workers; queueing.

## Structural gates (the only pass/fail)

* `agg_ok` — exact per-class submitted/completed counts match, and each class's
  value-sum matches the closed form (`busy_sum(n)` → `busy_sum_value(n)`;
  `park_ms(ms)` → `ms`, the echoed value).
* `futures_completed` — every future reaches `completed` (per call,
  `n_completed == n_total`).
* `plain_types_ok` — only plain Python-safe rows/aggregates cross the Ray
  boundary.
* `lane_ids_ok` — `W` lanes, each id `rt-hpx-…`.
* `clean_shutdown` — `Runtime.shutdown()` and Ray actor cleanup succeed.

Exit `0` = gates passed **or** cleanly skipped (Ray or `rayx.runtime`
unavailable); exit `1` = a structural gate failed.

## Interpretation criteria

exp34 has **two paths** with distinct labels.

### Slice 1 — S/C baseline (default)

A **baseline / harness slice** that exercises **no** HPX-native overlap or
priority/yield mechanism, so SUPPORT is **unreachable by construction**.

* **`BASELINE/STOP` (acceptable and useful)** — a clean full S/C run. A **narrow**
  verdict: **not** final exp34 evidence and **not** a claim that the adapter has no
  serving behavior. Any S-vs-C head-of-line interference is a **FIFO adapter
  property**, not HPX-native scheduling. If comfortable in `W<=16`, it does **not**
  motivate W=32/NUMA/binding; the next lever is the `--with-parked` path (below).
* **INCONCLUSIVE** — dominated by the **Ray boundary / measurement** (end-to-end ≫
  in-actor), **insufficient samples** for the reported tail, or inseparable from
  raw CPU scaling. (Smoke is always INCONCLUSIVE/smoke-only by design.)

### Slice 2 — `--with-parked` cooperative-overlap (opt-in, SUPPORT-capable)

* **SUPPORT** (narrow) — at fixed `W<=16`, the synthetic parked-wait mix shows a
  **cooperative-overlap signal** (median `overlap_ratio ≥ 2.0`: parked demand
  absorbed rather than paid serially, while S/C metrics stay in the table). The
  exact allowed claim: *"Under this synthetic parked-wait mix, the current
  Ray-hosted Runtime/lane adapter shows a fixed-W cooperative-overlap signal."*
  This is a **synthetic** cooperative-overlap property (`park_ms` suspends the HPX
  task and frees the worker), **not** reducible to exp32 homogeneous CPU
  strong-scaling. It does **not** claim real I/O, HPX priority scheduling, Ray
  Serve, Ray cluster scaling, latency-SLO/capacity guidance, "HPX beats Ray", or
  "RayX makes Ray faster".
* **STOP** — parked waits show **no** useful fixed-W overlap signal (median
  `overlap_ratio < 2.0`) or the run stays adapter/boundary dominated. No
  W=32/NUMA/binding is motivated.
* **INCONCLUSIVE** — **insufficient samples** for tails, **Ray boundary** /
  Python-retire-GIL dominated, **cross-clock timing ambiguity** dominates, or
  overlap cannot be separated from raw CPU scaling. (Smoke is always
  INCONCLUSIVE/smoke-only.)

`overlap_ratio` is **approximate/structural** and **never** a pass/fail gate; only
the structural gates decide exit status.

### FOLLOW-UP

If even the `--with-parked` path STOPs and the missing lever looks like
**priority/yield rather than W**, the next motivated experiment is a **fixed-W
priority/yield mechanism slice** — **not** W=32 / NUMA / binding.

## Allowed / forbidden claims

**Allowed:** "Under this synthetic serving-shaped mix, the current Ray-hosted
Runtime/lane adapter [does/does not] show a fixed-W scheduling/interference
effect"; (with `--with-parked`) "Under this synthetic parked-wait mix, the current
Ray-hosted Runtime/lane adapter shows a fixed-W cooperative-overlap signal";
"observation-only and machine-specific"; "this does not evaluate HPX priority
scheduling because priorities are out of scope."

**Forbidden:** RayX makes Ray faster; HPX beats Ray; RayX replaces Ray; Ray
cluster scaling; Ray Serve behavior; real inference; latency-SLO / capacity /
sizing guidance; p99 queue/service attribution from cross-clock `queue_wait_ms`;
NUMA/socket attribution without binding evidence; treating FIFO-lane HOL blocking
as an HPX-native scheduling property.

## Scope (source-complete exp34)

* `lane_impl="std"` only — **no `lane_impl="hpx"`**.
* **No W=32** (all W ≤ 16 by construction).
* **No HPX priorities**; no pools / resource partitioning / APEX / numactl /
  binding / counters / lane-affinity machinery.
* The **only** optional mechanism is the synthetic `--with-parked` Wp class
  (`park_ms`) — synthetic cooperative overlap, **not** real I/O.
* No real inference, no Ray Serve.
* No JSONL/corpus artifact; **no `aggregate.json`** until real runs exist.

## Commands

```
python -m py_compile experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py

# laptop-safe structural smoke (SMOKE-ONLY, not evidence)
python experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py --smoke
python experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py --smoke --with-parked

# homogeneous many-core Linux observation (W in {4,8,16}, fine/coarse, 3 load levels)
python experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py --full
python experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py --full --with-parked
```

* `--smoke` (default): `W=4` (capped to `cpu_count`), fine granularity only,
  `N=60`, `reps=2`. Exercises every code path (closed-loop window, per-class
  metrics, p99-`NA` path, gates) in well under a minute. **Smoke-only, not
  evidence.** `--smoke --with-parked` adds the Wp class (`park_ms=5`) so the
  parked-overlap context + SUPPORT-capable reading path are exercised structurally
  (p99 still `NA` at smoke sample counts).
* `--full`: `W ∈ {4,8,16}` (capped), fine + coarse granularity, all three load
  levels, `N=200`, `reps=3` (pooled per-class samples reach the p99 floor).
  Intended for a homogeneous many-core Linux node; any output on a Mac/laptop is
  **smoke-only**. `--full --with-parked` is the SUPPORT-capable observation.

Requires Ray and the built `_rayx`; the runner **skips cleanly (exit 0)** if
either is unavailable. Every mode prints a compact `machine-info` block.

## Results (Rostam, homogeneous 40-core Linux)

> **Observation-only / machine-specific.** Curated from the Rostam run logs (kept
> outside the repo under the ignored `results/`); a compact
> `aggregate_rostam_40core.json` sits beside this note. **All structural gates
> PASS** in every run. Mac/laptop output remains **smoke-only, not evidence**.

**Node / environment.** Intel **Xeon Gold 6148 @ 2.40 GHz**, `CPU(s)=40`, **2
sockets × 20 cores**, **Thread(s) per core = 1**; Linux 5.14, Python 3.12.3, Ray
2.55.1; commit `661efb0`. No affinity/pinning was set, so **no per-socket/NUMA
placement is claimed**.

**Smoke (both paths).** `--smoke` (S/C) and `--smoke --with-parked` both
**STRUCTURAL GATES: PASS**, reading **INCONCLUSIVE (smoke-only)** — low samples,
per-class p99 `NA` by design. Structural only, **not evidence**.

**Full S/C baseline (run1) — `BASELINE/STOP`, gates PASS.** The FIFO-adapter
head-of-line interference is real and **largest at small `W` / coarse
granularity**, shrinking as `W` grows (more lanes/workers absorb the over-load):

| cell | S p90 under→over | ×inflation |
| --- | --- | --- |
| W=4/coarse | 1.56 → 51.59 ms | ×33.1 |
| W=4/fine | 0.21 → 5.22 ms | ×24.9 |
| W=8/coarse | 4.12 → 32.97 ms | ×8.0 |
| W=8/fine | 0.37 → 3.31 ms | ×8.9 |
| W=16/coarse | 5.33 → 32.89 ms | ×6.2 |
| W=16/fine | 0.53 → 2.60 ms | ×4.9 |

This is a **FIFO adapter property, not HPX-native scheduling** — `BASELINE/STOP`
correctly makes no SUPPORT claim.

**Full parked (`--with-parked`, 3 runs) — `SUPPORT` (3/3), gates PASS.** Median
`overlap_ratio` per run = **2.32 / 2.10 / 2.32** (`≥ 2.0` in all three). The
fine-granularity over-load cells are the load-bearing, tightly-reproducible
signal (`parked_demand_ms = 900` in every cell):

| cell | overlap_ratio (run1 / run2 / run3) | wall_ms (run1 / run2 / run3) |
| --- | --- | --- |
| W=4/fine/over | 2.61 / 2.60 / 2.60 | 345.5 / 345.5 / 345.5 |
| **W=8/fine/over** | **4.48 / 4.45 / 4.47** | 201.1 / 202.4 / 201.4 |
| **W=16/fine/over** | **7.61 / 7.65 / 7.62** | 118.2 / 117.6 / 118.1 |

The signal is **granularity-sensitive**: strong at fine granularity and rising
with `W` (more workers freed by cooperative parking → more parked overlap), but
**weaker at coarse granularity** where the heavy compute dominates the wall and
overlap headroom is small (coarse over-load: W=4 `≈0.96`, W=8 `≈1.88`, W=16
`≈2.95`). `overlap_ratio > 1` means parked demand was **absorbed/overlapped**
rather than paid serially.

**Final allowed claim (observation-only, machine-specific).** On this homogeneous
40-core Xeon node, **under a synthetic parked-wait mix, the current Ray-hosted
Runtime/lane adapter shows a fixed-W cooperative-overlap signal** (reproducible
3/3; fine over-load `overlap_ratio` ≈4.45–4.48 at W=8 and ≈7.61–7.65 at W=16).
This is a **synthetic** cooperative-overlap property — `park_ms` cooperatively
suspends the HPX task and frees the worker — **not** reducible to exp32
homogeneous CPU strong-scaling. `overlap_ratio` is **approximate/structural and
never a gate**; the S/C baseline stays `BASELINE/STOP` with FIFO-adapter (not
HPX-scheduling) interference. **Not** real I/O, **not** inference, **not** Ray
Serve, **not** Ray cluster scaling, **not** HPX priority scheduling, **not** a
latency-SLO / capacity / sizing claim, **not** "HPX beats Ray", **not** "RayX
makes Ray faster", **not** "RayX replaces Ray"; **no** socket/NUMA attribution
without binding evidence.

## Future opt-ins (not in source-complete exp34)

* **`lane_impl="hpx"` (later mechanism comparison):** run the same mix on the
  cooperative HPX-thread lane under the same RayX contract, as a mechanism
  comparison against the `std` anchor — not a replacement of the anchor.
* **Priority/yield probe (only if exp34 STOPs there):** a fixed-W priority/yield
  scheduling experiment, motivated **only** if even the `--with-parked` path STOPs
  and the missing lever looks like scheduling priority rather than worker count.
