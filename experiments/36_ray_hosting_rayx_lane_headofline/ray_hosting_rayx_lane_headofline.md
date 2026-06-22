# Ray-hosted RayX lane-count head-of-line diagnostic (no-new-API, observation-only)

> **HPX-honest caveat (read first).** HPX cooperative park-vs-compute suspension is
> **true by construction** for `hpx::this_thread::sleep_for` — a suspended HPX task
> vacates its OS worker. exp36 is **not** about discovering that. It is a
> **mechanism diagnostic** built on exp35's STOP and the Step-0 code inspection:
> does increasing `num_lanes` at **fixed** `hpx_threads` dilute **per-lane FIFO
> head-of-line (HOL)** and recover fine-grain compute retention? It uses **existing
> `Runtime` knobs only** (no routing primitive, no API change). `park_ms` is
> **synthetic cooperative parked wait, not real I/O**. This is **not** Ray Serve,
> **not** Ray cluster scaling, **not** HPX priority scheduling, **not** a latency-SLO
> / capacity / sizing claim, **not** "RayX makes Ray faster" / "HPX beats Ray", and
> carries **no** socket/NUMA attribution.

## Mechanism settled by Step-0 code inspection

(`python/src/rayx/runtime_lane.hpp`, `runtime_ops_hpx.hpp`, `_rayx.cpp`)

* `park_ms` uses `DispatchPolicy::Async`; the body's `hpx::this_thread::sleep_for`
  runs inside `hpx::async(exec_, …)`, so the **HPX OS worker is freed** during the
  park (cooperative suspension, true by construction).
* But the `RuntimeLane` consumer is a **single serial `hpx::thread`** that does
  `hpx::async(exec_, body).get()` and **"one op retires before the next is popped"**
  (`runtime_lane.hpp:270`). The `.get()` cooperatively suspends the consumer (frees
  the worker) **but the lane does not advance** until the op — including the full
  `park_ms` — completes.
* `submit_operation` **round-robins** ops across `num_lanes` lanes
  (`_rayx.cpp:1166–1169`).

⇒ A compute op round-robined **behind a park on the same lane waits ~`park_ms`.**
exp35's fine-grain erosion is best explained by this **per-lane FIFO head-of-line
occupancy**, not worker blocking (the worker is freed) and not failed cooperative
suspension. The serial `ServiceLane` terminology / `lane_impl="std"` does **not**
apply here: `RuntimeLane` consumers are always `hpx::thread`s and there is no
`Runtime.lane_impl`.

## Question

> At **fixed** `hpx_threads` (worker-pool capacity), does raising `num_lanes` (more
> serial FIFO admission slots) **dilute per-lane HOL** and **recover** fine-grain
> compute retention?

## Why this isolates the variable

* **Compute parallelism is hard-capped at `hpx_threads`** (the `parallel_executor`
  pool), independent of `num_lanes`. So more lanes **cannot buy compute capacity** —
  retention cannot exceed ~1.0 for capacity reasons, which defends against a **false
  SUPPORT**: a recovery means HOL was removed, not workers added.
* **Park count and `park_ms` are held constant across the lane sweep**, so the same
  timers fire in every cell — only their distribution across lanes changes. The sweep
  separates "lanes" from "timer/scheduler churn" **by construction**. (Spreading
  parks over more lanes can even *raise* the instantaneous wake-up rate, biasing
  *against* the HOL hypothesis, so a recovery is conservative evidence for HOL.)
* **`num_lanes` decoupled from `hpx_threads`** (exp35 pinned them equal). Ray
  `num_cpus = hpx_threads`, **not** `num_lanes` — more admission slots must not buy
  more CPUs.

## Three arms (per cell, exp35 structure)

* **Arm C — compute-only:** `K_C` × `busy_sum(n_c)`, window `O`.
* **Arm Wp — parked-only:** `K_Wp` × `park_ms(ms)`, window `O`.
* **Arm T — matched:** the **same** `K_C` compute **plus** `K_Wp` parked, class-aware
  closed loop holding compute concurrency at `O` and adding parked on top.

One Ray actor hosts one `Runtime(num_lanes=NL, hpx_threads=HT)`; `RuntimeFuture` /
`OperationResult` are created and **retired inside** the actor; only plain rows cross
the Ray boundary. **One actor per `(HT, NL)` cell** (a fresh process): `num_lanes` is
a constructor parameter and the HPX runtime is process-global, so a new process per
`NL` is the only safe way to vary it.

## Workload matrix

| factor | smoke | full |
| --- | --- | --- |
| ladders (`hpx_threads → num_lanes`) | `4 → {4,8}` | **`4 → {4,8,16}`**, `8 → {8,16}` (no W=32) |
| granularity | fine `n_c=2,000,000` | fine `n_c=2,000,000` only |
| load level | over | near, over |
| `K_C` | 16 | 60 (reps=3 → pooled C ≥ 180 ⇒ p99 eligible) |
| `park_ms` | 5 | 5 |
| reps / warmup | 2 / 1 | 3 / 1 |

**Window held by `hpx_threads`, not `num_lanes`:** `O = outstanding_for(level, HT)`
(near→`HT`, over→`4·HT`). The same offered concurrency is spread over more admission
slots as `NL` rises. **`K_Wp` is calibrated once per `(HT, level)` from the baseline
(`NL == HT`) `wall_C`** (`K_Wp ≈ wall_C·O/park_ms`, clamped `[HT, 400]`) and **held**
across the `NL` sweep, so the lane sweep varies **only** `num_lanes`.

## Metrics (reading criteria, never structural gates)

* `wall_C`, `wall_Wp`, `wall_CWp` (compute-completion), `wall_CWp_total`.
* **C throughput retention** = `thr_C(T)/thr_C(C)` (≈1 = intact) — primary, from the
  compute-completion wall.
* **C p50/p90/p99 retention** (p99 only when pooled C ≥ 100, else `NA`).
* **`qd_mean` (C and T arms), `qd_max` (T)** — the HOL corroboration signal.
* **`lane_stats()` structural trace** (matched arm; racy, context-only): `backlog_seen`,
  `active_fraction` — the CPython-driver control.

## Structural gates (the only pass/fail)

`agg_ok`, `futures_completed`, `plain_types_ok`, `lane_ids_ok` (`NL` lanes,
`rt-hpx-…`), `clean_shutdown`. Exit `0` = gates passed or cleanly skipped
(Ray/`rayx.runtime` unavailable); exit `1` = a gate failed. **Timing / retention /
qd / lane_stats never gate.**

## Controls (must pass before any reading)

1. **Positive control** — the baseline cell (`NL == HT`) must reproduce erosion
   (`thr_retention ≤ 0.70`); else the ladder is **INCONCLUSIVE** (no disease to cure).
2. **Driver / lane control** — the matched arm must show backlog/active via
   `lane_stats`; a starved trace ⇒ **INCONCLUSIVE** (walls measure the driver).
3. **Compute-baseline flatness** — compute-only `wall_C` must stay within **±25%**
   across `num_lanes` at fixed `hpx_threads`. The `wall_C` curve is **printed**, not
   just pass/fail; if it shifts materially the ladder is **INCONCLUSIVE** (adding
   lanes may be changing compute capacity / driver behavior).
4. **HOL corroboration** — `qd_mean`/`qd_max` vs `num_lanes` is printed for the
   matched arms. The expected HOL signature is **queue depth falls as `num_lanes`
   rises** *and* retention improves as queue depth falls. **If retention improves but
   queue depth does not fall, the HOL story is reported as incomplete.**

## Interpretation (graded reading, observation-only)

Read the **trend across `num_lanes`**, not just the top endpoint. Thresholds are the
**same as exp35** (`0.90` / `1.20` / `0.70`); only how they are combined differs.

* **FULL SUPPORT** — baseline eroded, controls pass, the **top** `num_lanes`
  recovers (`thr_retention ≥ 0.90` **and** p90 ret ≤ 1.20), **and** queue depth fell
  with more lanes. ⇒ per-lane FIFO HOL **is** the exp35 eroder; diluting it at fixed
  worker capacity restores compute.
* **PARTIAL SUPPORT** — baseline eroded, controls pass, retention **rises clearly /
  monotonically** with `num_lanes` but the top cell does **not** fully reach the
  SUPPORT band (e.g. `0.26 → 0.55 → 0.85`). ⇒ lane count is a **major contributor**,
  but adding lanes did not fully restore compute retention under this cell — a
  residual (worker-pool / scheduler / driver) remains.
* **STOP** — baseline eroded, controls pass, retention stays **flat/bad** across the
  sweep (especially top cell still `≤ 0.70`). ⇒ increasing admission slots did **not**
  recover compute; per-lane HOL is not sufficient/dominant on this evidence — the
  cause lies elsewhere (worker-pool / scheduler / timer churn or the CPython driver).
* **INCONCLUSIVE** — baseline did not erode; driver-starved; compute baseline not
  flat; or noisy / non-monotone / mid-band without a clear trend. (Smoke is always
  INCONCLUSIVE / smoke-only by design.)

## Caveats (must stay visible)

1. exp36 **confirms or refutes the lane-HOL mechanism within the current serial-lane
   contract** (consumer does `hpx::async(…).get()`, one op retires before the next is
   popped). It does **not** evaluate the deeper alternative of a **non-blocking
   `RuntimeLane` consumer** that dispatches and tracks completion via continuations
   (`.then`) instead of blocking the lane — that is a separate design axis and an
   API/contract change, out of scope here.
2. **More lanes is a diagnostic lever, not automatically the best production design**
   — each lane is a consumer `hpx::thread`, and at `num_lanes ≫ cores` the
   consumer-thread overhead would itself dominate. The `≤ 16 ≪ 40 cores` cap keeps
   that overhead mild so the mechanism reads cleanly.
3. **HPX cooperative suspension remains true by construction**; exp36 probes a
   consequence of the serial-lane contract, not the suspension primitive.
4. The **CPython submit/retire loop** may govern the measurement if HPX is not
   backlogged; the `lane_stats` trace is the in-scope control.
5. `lane_stats` snapshots are **racy, approximate, context-only, never a gate.**
6. `park_ms` is synthetic cooperative parked wait — **not real I/O, not inference**.
   Not Ray Serve / cluster scaling / HPX priority scheduling / NUMA / latency-SLO /
   capacity / general performance claim.

## Commands

```
python -m py_compile experiments/36_ray_hosting_rayx_lane_headofline/run_ray_hosting_rayx_lane_headofline.py

# laptop-safe structural smoke (SMOKE-ONLY, not evidence)
python experiments/36_ray_hosting_rayx_lane_headofline/run_ray_hosting_rayx_lane_headofline.py --smoke

# homogeneous many-core Linux observation
python experiments/36_ray_hosting_rayx_lane_headofline/run_ray_hosting_rayx_lane_headofline.py --full
```

Requires Ray and the built `_rayx`; the runner **skips cleanly (exit 0)** if either
is unavailable. Every mode prints a compact `machine-info` block.

## Results

**One full Rostam run recorded — `STRUCTURAL GATES: PASS`, lane-HOL mechanism
SUPPORTED (FULL at light load, PARTIAL under heavier load).** Measurement stopped
after this single `--full` run: the mechanism reading is already decisive, and the
goal is to confirm/refute the exp35 head-of-line mechanism, not to chase a capacity
number.

**Node (`medusa04`).** Intel Xeon Gold 6148, 40 CPUs, 2 sockets × 20 cores,
1 thread/core — homogeneous many-core Linux.

**Structural gates — all PASS.** Real Rostam smoke (`STRUCTURAL GATES: PASS`,
`INCONCLUSIVE (smoke-only)`) and the `--full` run both passed all five gates
(`agg_ok`, `futures_completed`, `plain_types_ok`, `lane_ids_ok`, `clean_shutdown`).

**Controls (all four held this run):** every baseline `nl==ht` cell reproduced
erosion (positive control); `lane_stats` showed HPX backlogged/active in every
matched arm (`active_frac` ≈ 0.95–1.00 — not driver-starved); compute-only `wall_C`
stayed flat across `num_lanes` at fixed `hpx_threads` (≈22 ms at HT=4, ≈12 ms at
HT=8 — so recovery is **not** added compute capacity); and `qd_mean_T` **fell as
`num_lanes` rose** in every ladder (HOL corroboration).

**Per-ladder graded reading (compute `thr_retention` vs `num_lanes`):**

| ladder | retention by `num_lanes` | top p90 ret | `qd_mean_T` | verdict |
| --- | --- | --- | --- | --- |
| HT=4 near | 0.47 → 0.68 → **0.98** (nl 4/8/16) | 1.03 | 1.1 → 0.8 → 0.2 | **FULL SUPPORT** |
| HT=4 over | 0.26 → 0.43 → **0.74** (nl 4/8/16) | 1.29 | 22.8 → 17.9 → 11.7 | **PARTIAL SUPPORT** |
| HT=8 near | 0.47 → **0.70** (nl 8/16) | 2.45 | 3.2 → 1.8 | **PARTIAL SUPPORT** |
| HT=8 over | 0.37 → **0.56** (nl 8/16) | 2.40 | 29.4 → 27.2 | **PARTIAL SUPPORT** |

**What this confirms (observation-only, machine-specific):**

* exp36 **confirms the exp35 mechanism**: per-lane FIFO **head-of-line occupancy is
  a major eroder** of fine-grain compute retention. `park_ms` frees the HPX worker,
  but the serial `RuntimeLane` consumer waits on `hpx::async(…).get()`, so the lane
  is **held until the park completes**; round-robin puts compute behind parks.
* **Increasing `num_lanes` at fixed `hpx_threads` recovers (FULL) or partially
  recovers (PARTIAL) compute retention**, with `qd_mean_T` falling in lockstep. At
  fixed worker capacity this means the eroder is **not** HPX failing to cooperatively
  park and **not** simply a shortage of HPX workers — it is **lane-level admission**.
* **Under heavier load (`over`, and HT=8), lane count helps but does not fully cure
  the tail** (top retention 0.56–0.74, p90 ret 1.29–2.45): a **residual
  worker-pool / scheduler / CPython-driver effect remains** once HOL is diluted. The
  HT=4 light-load ladder is the clean FULL recovery (0.98, p90 1.03).

**What this does NOT claim.** **More lanes is a diagnostic lever, not automatically
the production design** (each lane is a consumer `hpx::thread`; at `num_lanes ≫ cores`
the consumer overhead would dominate). exp36 confirms the mechanism **within the
current serial-lane (`.get()`) contract** and does **not** evaluate the deeper
alternative of a **non-blocking `RuntimeLane` consumer using continuations** instead
of `.get()`. HPX cooperative suspension remains **true by construction**. Single
run, fine granularity, W≤16, Rostam-specific; no curated `aggregate` JSON for a
single mechanism run. **Not** Ray Serve, Ray cluster scaling, HPX priority
scheduling, real I/O, inference, NUMA, latency-SLO / capacity / sizing, or
"RayX makes Ray faster" / "HPX beats Ray".
