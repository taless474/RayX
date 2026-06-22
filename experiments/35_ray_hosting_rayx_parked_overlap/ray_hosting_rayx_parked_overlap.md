# Ray-hosted RayX cooperative-suspension preservation under parked load (adapter non-regression, observation-only)

> **HPX-honest caveat (read first).** HPX cooperative park-vs-compute suspension is
> **true by construction** for `hpx::this_thread::sleep_for` — a suspended HPX task
> vacates its OS worker, so concurrent compute proceeds. **exp35 does not claim to
> discover overlap.** It is an **adapter non-regression** probe: does the
> Ray-hosted RayX stack — one `Runtime` over FIFO `RuntimeLane`s,
> `DispatchPolicy=Async`, driven by a CPython closed loop inside one Ray actor —
> **preserve** that HPX property at fixed `W`, and how much do **shared-FIFO
> admission** and the **CPython driver** erode it? `lane_impl="std"` only; **no
> W=32**, no NUMA/binding, no priorities/pools/counters. `park_ms` is **synthetic
> cooperative parked wait, not real I/O**. This is **not** Ray Serve, **not** Ray
> cluster scaling, **not** HPX priority scheduling, **not** a latency-SLO /
> capacity / sizing claim, **not** "RayX makes Ray faster" / "HPX beats Ray" /
> "RayX replaces Ray", and carries **no** socket/NUMA attribution.

## Question

> Does the Ray-hosted RayX `Runtime` / FIFO `RuntimeLane` / `DispatchPolicy=Async`
> / CPython closed-loop stack **preserve** HPX cooperative park-vs-compute
> suspension at fixed `W`, and how much do **(a)** shared-FIFO admission and
> **(b)** the CPython driver erode it?

This hardens exp34: exp34 saw a fixed-W cooperative-overlap *signal* (pooled
`overlap_ratio`) but could not separate **park-vs-compute** (parked waits free the
worker for concurrent native compute) from **park-vs-park** (parks overlapping each
other). exp35's discriminator is **compute-class retention**.

## Three arms (per cell)

* **Arm C — compute-only:** `K_C` × `busy_sum(n_c)`, closed-loop window `O_C`.
* **Arm Wp — parked-only reference:** `K_Wp` × `park_ms(ms)`, window `O_Wp = O_C`
  (park-vs-park baseline; gives the `wall_Wp` reference for max-vs-sum).
* **Arm T — matched:** the **same** `K_C` compute **plus** `K_Wp` parked, with a
  **class-aware** closed loop holding compute concurrency at `O_C` and adding
  parked concurrency `O_Wp` *on top* (parked additive, not displacing). The
  compute-completion wall is captured when the last `busy_sum` retires.

One Ray actor hosts one `Runtime(num_lanes=W, hpx_threads=W)`; `RuntimeFuture` /
`OperationResult` are created and **retired inside** the actor; only plain Python
rows/aggregates cross the Ray boundary.

## Workload matrix

| factor | smoke | full |
| --- | --- | --- |
| W (`num_lanes=hpx_threads`) | {4} | **{4, 8, 16}** (no W=32) |
| granularity | fine `n_c=2,000,000` | fine (headline) + coarse `n_c=20,000,000` (retention-only) |
| load level | over | near, over |
| `K_C` | 16 | 60 (reps=3 → pooled C ≥ 180 ⇒ p99 eligible) |
| `park_ms` | 5 | 5 |
| parked density | 1× | **0.5×, 1×, 2×** |
| reps / warmup | 2 / 1 | 3 / 1, **3 independent full runs** |

**Parked-demand calibration (so the wall indicator is valid).** Per cell, Arm C is
run first to measure `wall_C`; the **1× `K_Wp`** is set so parked-only `wall_Wp` is
roughly comparable to `wall_C` (`K_Wp ≈ wall_C · O / park_ms`, clamped to
`[W, K_WP_CAP=400]`). The density sweep is `0.5×/1×/2×` of that. The **max-vs-sum**
indicator is computed **only** when realized `wall_Wp / wall_C ∈ [⅓, 3]`; otherwise
`NA`. **Coarse cells are retention-only when walls are not comparable — coarse is
NOT a max-vs-sum negative control.**

## Metrics

All are **reading criteria, never structural gates.**

* `wall_C`, `wall_Wp`, `wall_CWp` (compute-completion), `wall_CWp_total`.
* **C throughput retention** = `thr_C(T)/thr_C(C)` (≈1 = intact) — **primary
  discriminator**, from the compute-completion wall.
* **C p50/p90/p99 retention** = `pXX_C(T)/pXX_C(C)` (p99 only when pooled C ≥ 100,
  else `NA`).
* **`added_wall`** = `wall_CWp − wall_C`; **`added_wall_fraction`** =
  `added_wall / parked_demand`.
* **max-vs-sum position** = `((wall_C+wall_Wp) − wall_CWp) / ((wall_C+wall_Wp) −
  max(wall_C,wall_Wp))` — **only where walls comparable**, else `NA`; **never a
  gate**. **Uncapped** (not `[0,1]`) because `wall_CWp` is the compute-completion
  sub-wall (parked work keeps running after compute retires): `1.0` = matched
  compute completes at `max(wall_C,wall_Wp)`; `0` = completes at the sum
  (serialized); **`>1` = `wall_CWp < max(wall_C,wall_Wp)`** (compute finishes before
  the slower solo arm — strong preservation); **`<0`** = worse than serial
  (admission tax). Higher = more preserved.
* **`lane_stats()` structural trace** (during Arm T; racy, context-only, never a
  gate): `backlog_seen`, `active_fraction`, mean/max `queue_depth`. Its job is the
  **CPython-driver control** — show HPX was *backlogged and busy* so the walls
  measure HPX, not a starving Python driver.

## Structural gates (the only pass/fail)

`agg_ok` (exact per-class counts + value correctness: `busy_sum`→closed form,
`park_ms`→echoed `ms`), `futures_completed`, `plain_types_ok`, `lane_ids_ok`
(`W` lanes, `rt-hpx-…`), `clean_shutdown`. Exit `0` = gates passed or cleanly
skipped (Ray/`rayx.runtime` unavailable); exit `1` = a structural gate failed.
**Timing / retention / max-vs-sum / lane_stats never gate.**

## Interpretation (reading, observation-only)

* **SUPPORT** (requires 3/3 full runs): at fine granularity, across the density
  sweep, C throughput retention ≥ ~0.9 **and** C p90 retention ≤ ~1.2 (compute
  intact), **and** `lane_stats` shows HPX was backlogged/active (driver not
  starving), **and** (where comparable) max-vs-sum ≥ ~0.7. ⇒ *the adapter
  **preserves** HPX cooperative suspension* — **despite shared FIFO lanes**, not
  pure worker-pool overlap, and not a discovery.
* **STOP**: C throughput retention ≤ ~0.7 when parks are added, and/or (where
  comparable) max-vs-sum ≤ ~0.3 (serialized). ⇒ the adapter path **does not**
  preserve cooperative suspension — a decision-relevant negative worth recording.
* **INCONCLUSIVE**: `lane_stats` shows HPX **starved** (driver-governed → wall
  indicators invalid); samples too sparse for tail retention; walls never
  comparable while retention sits in the noise band; or cross-run spread too large
  for a 3/3 read. (Smoke is always INCONCLUSIVE/smoke-only by design.)

## What SUPPORT would claim

> *"Under a synthetic parked-wait mix, the Ray-hosted RayX adapter **preserves**
> HPX cooperative park-vs-compute suspension at fixed W on Rostam: compute
> throughput and latency are **retained** under added synthetic parked load —
> robust across a 0.5×/1×/2× density sweep — **despite shared FIFO lanes**, with
> `lane_stats` confirming HPX stayed backlogged."*

## What SUPPORT still would NOT claim

Not a *discovery* of overlap (HPX's by construction); not **pure** worker-pool
overlap (retention folds in FIFO admission); still synthetic (`park_ms` ≠ real I/O
/ inference), `lane_impl="std"`, **W≤16**, Rostam-specific. No Ray Serve, cluster
scaling, HPX priority scheduling, latency-SLO / capacity-sizing, socket/NUMA, or
"RayX makes Ray faster" / "HPX beats Ray" / "RayX replaces Ray".

## Caveats (must stay visible)

1. HPX cooperative suspension is **true by construction**; exp35 tests **adapter
   preservation**, not discovery.
2. **Shared FIFO lanes** are a confound: `RuntimeLane` is FIFO per lane and Python
   cannot pin a class to a lane without an API change (per-class lanes would be
   resource partitioning — out of scope). A compute op can queue behind a parked op
   before dispatch, so **C retention is an adapter-level result** (cooperative
   suspension **+** FIFO admission). (`park_ms` is Async, so the lane consumer
   dispatches it non-blocking via `hpx::async`; the admission coupling is
   dispatch-latency-scale, not `park_ms`-scale, but it is not zero and grows with
   parked density.)
3. The **CPython submit/retire loop** may govern the measurement if HPX is not
   backlogged; the `lane_stats` trace is the in-scope control, and a starved trace
   forces INCONCLUSIVE for wall indicators.
4. `lane_stats` / `ActorHandle.stats` snapshots are **racy, approximate,
   context-only, never a gate.**
5. **max-vs-sum is valid only when `wall_C` and `wall_Wp` are comparable**
   (`∈[⅓,3]`); coarse cells are retention-only, **not** a negative control.
6. `park_ms` is synthetic cooperative parked wait — **not real I/O, not
   inference**. Not Ray Serve / cluster scaling / HPX priority scheduling /
   latency-SLO / capacity-sizing.

## Commands

```
python -m py_compile experiments/35_ray_hosting_rayx_parked_overlap/run_ray_hosting_rayx_parked_overlap.py

# laptop-safe structural smoke (SMOKE-ONLY, not evidence)
python experiments/35_ray_hosting_rayx_parked_overlap/run_ray_hosting_rayx_parked_overlap.py --smoke

# homogeneous many-core Linux observation (3 independent runs)
python experiments/35_ray_hosting_rayx_parked_overlap/run_ray_hosting_rayx_parked_overlap.py --full
```

Requires Ray and the built `_rayx`; the runner **skips cleanly (exit 0)** if either
is unavailable. Every mode prints a compact `machine-info` block.

## Results

**One full Rostam run recorded — `STRUCTURAL GATES: PASS`, `READING: STOP` (valid,
adapter-level, observation-only).** Measurement stopped after this single `--full`
run: STOP was already valid, and the goal is adapter non-regression, **not** chasing
SUPPORT (per the Interpretation section, a clean STOP is decision-relevant on its
own; SUPPORT is what requires 3/3).

**Node (`medusa04`).** Intel Xeon Gold 6148, 40 CPUs, 2 sockets × 20 cores,
1 thread/core — homogeneous many-core Linux.

**Structural gates — all PASS.** Real Rostam smoke **before and after** the
reporting patch and the `--full` run all reached `STRUCTURAL GATES: PASS` (`agg_ok`,
`futures_completed`, `plain_types_ok`, `lane_ids_ok`, `clean_shutdown`). The
reporting patch was **text-only** (max-vs-sum description + STOP wording); smoke
gates and the `INCONCLUSIVE (smoke-only)` reading were unchanged across it, and no
threshold, criterion, or runner logic changed.

**Full-run reading — STOP (observation-only, machine-specific):**

* **Valid, not driver-starved.** `lane_stats` showed HPX **backlogged and active**
  in every cell (`backlog=True`, `active_fraction ≈ 0.93–1.00`, `qd_max` up to ~96),
  so the walls measure HPX, not a starving CPython driver; the driver-starvation
  INCONCLUSIVE guard was nowhere near tripping.
* **Fine-grain compute-class retention is not broadly preserved** under added parked
  load: fine `thr_retention` fell as low as ~0.26 with C p90 retention ~4×, well
  inside the STOP band. STOP fired on **both** `thr_retention ≤ 0.70` and (where
  comparable) `max-vs-sum ≤ 0.30`.
* **Preservation is load-shape / admission-sensitive, not uniform.** Coarse cells
  (`n_c=20,000,000`) degraded only modestly (retention ~0.75–1.00, p90 ~1.3–1.4),
  and **W=16 fine/over preserved cleanly** (retention ~1.0, p90 ~1.0, `max-vs-sum`
  ~2.2–2.55). The STOP is concentrated in fine-grain low/medium-W cells.
* **Not full serialization.** `added_wall_fraction` stayed small (~0.02–0.32) and
  several comparable cells had `max-vs-sum > 1` (matched compute completed before the
  slower solo arm), so **much of the parked demand was still overlapped**. The honest
  reading is **compute-class retention eroded under added parked load** — consistent
  with shared-FIFO admission and CPython closed-loop load-shape sensitivity — **not**
  proof that parks were fully serialized against compute.

**Scope (unchanged).** Single run, `lane_impl="std"`, **W≤16**, Rostam-specific,
observation-only. No curated `aggregate_rostam_40core.json` is added for a single
STOP run. Not Ray Serve, cluster scaling, HPX priority scheduling,
latency-SLO/capacity-sizing, socket/NUMA attribution, or "RayX makes Ray faster" /
"HPX beats Ray".

## Future opt-ins (not in this slice)

* **`lane_impl="hpx"` second leg** — only if exp35 (std) SUPPORTs: run the same
  arms on the cooperative HPX-thread lane as a **robustness cross-check** (the
  Async bodies already run on the `hpx::async` worker pool, so the lane is
  admission, not the overlap mechanism), never an "hpx faster/slower" verdict.
* **Priority/yield probe** — only if a real (non-synthetic) workload ever shows the
  missing lever is scheduling priority rather than cooperative suspension.
