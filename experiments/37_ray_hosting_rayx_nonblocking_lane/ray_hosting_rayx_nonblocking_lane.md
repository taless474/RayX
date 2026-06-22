# Ray-hosted RayX serial vs non-blocking op-lane comparison (mechanism probe, observation-only)

> **HPX-honest caveat (read first).** HPX cooperative park-vs-compute suspension is
> **true by construction**. exp37 is **not** about that. It compares the **default
> serial-blocking** op-lane against the **experimental non-blocking** op-lane under the
> synthetic parked+compute mix that eroded fine-grain compute retention in exp35/exp36,
> at **fixed `num_lanes == hpx_threads`**. The non-blocking lane's **retirement path is
> a first-class confound**, so recovery is read **above an explicit overhead floor**
> (`nb(max_inflight=1)`). `park_ms` is **synthetic cooperative parked wait, not real
> I/O**. Non-blocking is **experimental and operation-lane-only** (actor lanes are
> always serial). This is **not** Ray Serve, cluster scaling, HPX priority scheduling,
> NUMA, a latency-SLO / capacity / sizing / performance claim, **not** "RayX makes Ray
> faster" / "HPX beats Ray", and **not** a recommendation to make non-blocking the
> default.

## Mechanism (settled by Step-0 + exp36)

* **Serial lane (default):** the `RuntimeLane` consumer (one `hpx::thread`) dispatches
  an Async op via `hpx::async(exec_, body).get()` and **does not pop the next op until
  that op — including a full `park_ms` — completes**. A compute op round-robined behind
  a park on the same lane waits ~`park_ms`: **per-lane head-of-line**.
* **exp36** confirmed head-of-line by **diluting** it across more independent FIFO
  lanes (raising `num_lanes` at fixed `hpx_threads` recovered retention).
* **Non-blocking lane (experimental):** the consumer dispatches Async ops via
  `hpx::async(...).then(continuation)` **without** the inline `.get()`, so it pops and
  dispatches more work while a parked op is suspended, bounded by
  `max_inflight_per_lane`. It tries to **remove** per-lane head-of-line rather than
  dilute it.

## Question

> At **fixed `num_lanes == hpx_threads`** (the exp35/exp36 eroded config), does
> switching op-lanes from serial-blocking to non-blocking **recover** fine-grain
> compute retention under the parked+compute mix — and **how much of the result is the
> non-blocking retirement-path overhead** rather than head-of-line removal?

## The non-blocking retirement path is a first-class confound

The non-blocking path is **not free**. Relative to the serial `.get()` retirement it
adds, per Async op:

* a `.then(...)` continuation;
* **per-lane mutex contention** in that continuation;
* a `cv_.notify_all()` wakeup (consumer wakeup noise);
* a **completion-worker → consumer-worker handoff** (vs the serial in-place resume);
* in-flight accounting (`inflight_` ++/--, token bookkeeping).

So a non-blocking result mixes **head-of-line removal** with **retirement-path
overhead**. exp37 isolates the overhead with `nb(max_inflight=1)` — one Async op per
lane at a time (same concurrency as serial), but retired through the continuation/cv
path instead of `.get()`. **`nb(1)` is the overhead floor; recovery is read above it.**

## Three arms (per cell, exp35/exp36 structure)

* **Arm C — compute-only:** `K_C` × `busy_sum(n_c)`, window `O`.
* **Arm Wp — parked-only:** `K_Wp` × `park_ms(ms)`, window `O`.
* **Arm T — matched:** the **same** `K_C` compute **plus** `K_Wp` parked, class-aware
  closed loop holding compute concurrency at `O` and adding parked on top.

One Ray actor hosts one `Runtime(num_lanes=HT, hpx_threads=HT, …)`; `RuntimeFuture` /
`OperationResult` are created and **retired inside** the actor, **by readiness**
(`wait` / `as_completed`), never by submission order — non-blocking lanes complete
**order-agnostically**. One actor per `(HT, mode, max_inflight)` cell (a fresh process:
mode/`max_inflight` are constructor-fixed and the HPX runtime is process-global).

## Workload matrix

| factor | smoke | full |
| --- | --- | --- |
| `hpx_threads` (`== num_lanes`) | {4} | **{4, 8}** (no W=32) |
| op-lane modes | serial; nb(`mi`∈{1,4}) | **serial; nb(`mi`∈{1,2,4})** |
| granularity | fine `n_c=2,000,000` | fine `n_c=2,000,000` only |
| load level | over | near, over |
| `K_C` | 16 | 60 (reps=3 → pooled C ≥ 180 ⇒ p99 eligible) |
| `park_ms` | 5 | 5 |
| reps / warmup | 2 / 1 | 3 / 1 |

Per `(HT, level)`: one **serial** anchor + a **non-blocking `max_inflight` sweep**, all
at `num_lanes == HT` — the only intended lever is op-lane dispatch behaviour (+
`max_inflight` within non-blocking). `K_Wp` is calibrated once from the **serial**
baseline `wall_C` and held across all modes.

**Relation to exp36 (analogy, not equivalence).** With `num_lanes == HT` fixed, total
in-flight capacity is `HT × max_inflight`. This is an **in-flight-capacity analogy** to
exp36's `num_lanes` sweep, **not** a strict equivalence: exp36 **diluted** head-of-line
across `N` **independent FIFOs** (it remained per-lane), whereas exp37 tries to
**remove** per-lane head-of-line up to the in-flight cap. exp36 is used only as a
**cross-experiment sanity check** (removal should do at least as well as dilution at
comparable in-flight capacity).

## Metrics (reading criteria, never structural gates)

* `wall_C`, `wall_Wp`, `wall_CWp` (compute-completion), `wall_CWp_total`.
* **C throughput retention** = `thr_C(T)/thr_C(C)` (≈1 = intact) — primary.
* **C p50/p90/p99 retention** (p99 only when pooled C ≥ 100, else `NA`).
* **`nb1_minus_serial`** = `retention(nb-mi1) − retention(serial)` — the
  retirement-path **overhead floor**.
* **`qd_mean` (C and T), `qd_max` (T)** — head-of-line corroboration (should fall as
  `max_inflight` rises).
* **`lane_stats()` trace** (matched arm; racy, context-only): `backlog_seen`,
  `active_fraction` — the CPython-driver control. (`active` means slightly different
  things across modes — single service slot vs in-flight > 0 — so `queue_depth` is the
  apples-to-apples head-of-line signal.)

## Structural gates (the only pass/fail)

`agg_ok`, `futures_completed` (every future terminal — no broken futures),
`plain_types_ok`, `lane_ids_ok` (`HT` lanes, `rt-hpx-…`), `clean_shutdown`. Exit `0` =
gates passed or cleanly skipped (Ray/`rayx.runtime` unavailable); exit `1` = a gate
failed. The "existing contract not violated" requirement is covered by the green
`tests/integration/test_runtime_contract.py` and
`tests/integration/test_runtime_nonblocking_lane.py` (test tier), not re-litigated here.
**Timing / retention / `nb1_minus_serial` / qd / lane_stats never gate.**

## Controls (must pass before any reading)

1. **Positive control** — the serial anchor must reproduce erosion
   (`thr_retention ≤ 0.70`); else the ladder is **INCONCLUSIVE** (no head-of-line to
   remove).
2. **Driver / lane control** — matched arms show backlog/active via `lane_stats`; a
   starved trace ⇒ **INCONCLUSIVE**.
3. **Compute-baseline flatness** — compute-only `wall_C` within **±25%** across the
   modes/`max_inflight` at fixed `hpx_threads`; else **INCONCLUSIVE** (the mode/cap may
   be changing compute capacity / driver behaviour). The curve is printed.
4. **Overhead floor (`nb(1)` vs serial)** — if `nb(1)` is more than **0.10** *below*
   the serial anchor, the continuation/cv retirement path is biasing the floor and the
   comparison is read as **INCONCLUSIVE (biased by retirement overhead)**. `nb(1)` well
   *above* serial (while serial eroded) is also a confound (the mode helping without
   added in-flight concurrency) and is flagged the same way.
5. **Head-of-line corroboration** — `qd_mean`/`qd_max` vs `max_inflight` is printed; the
   expected signature is **queue depth falls as `max_inflight` rises** *and* retention
   improves as it falls. If retention improves but queue depth does not fall, the
   head-of-line story is reported **incomplete**.

## Expected shape

* **serial** eroded (`≤ 0.70`);
* **nb(1)** ≈ serial, **possibly slightly worse** (retirement-path overhead floor);
* **nb(2)** the **main recovery step** (one in-flight slot past the park unblocks
  compute on the same lane);
* **nb(4)** mostly **plateau / knee**.

A **smooth ramp** (rather than a step at `nb(2)`) is informative and is discussed as
evidence of a **residual limiter beyond single-slot head-of-line** (e.g. the
retirement-path overhead itself, or worker-pool/scheduler/driver effects).

## Interpretation (graded reading, observation-only; bands as exp35/exp36)

Recovery is read **above the `nb(1)` overhead floor**, across the `max_inflight` sweep.

* **FULL SUPPORT** — serial eroded, controls pass (incl. `nb(1)`≈serial), the top
  `max_inflight` recovers (`thr_retention ≥ 0.90` **and** p90 ret ≤ 1.20), a clear rise
  above the floor, **and** queue depth fell. ⇒ at fixed lanes/workers, the non-blocking
  op-lane **removes per-lane head-of-line** within the op-lane admission contract.
* **PARTIAL SUPPORT** — serial eroded, controls pass, non-blocking retention **rises
  clearly/monotonically** with `max_inflight` but the top cell does **not** fully reach
  the SUPPORT band. ⇒ non-blocking dispatch is a **major** lever; a **residual** remains
  — retirement-path overhead (continuation / per-lane mutex / notify / handoff) or
  another limiter beyond single-slot head-of-line.
* **STOP** — serial eroded, controls pass, non-blocking retention stays **flat/bad**
  across the sweep. ⇒ the **current non-blocking prototype did not recover retention**.
  This does **not** refute the head-of-line mechanism (Step-0 + exp36 already support
  it) — it indicates **retirement-path overhead / continuation churn / handoff cost
  dominated**, or another residual limiter became dominant.
* **INCONCLUSIVE** — serial anchor did not erode; `nb(1)` biased vs serial;
  driver-starved; compute baseline not flat; or noisy / non-monotone / mid-band. (Smoke
  is always INCONCLUSIVE / smoke-only.)

## Caveats (must stay visible)

1. The non-blocking **retirement path** (continuation / per-lane mutex contention /
   `notify_all` wakeup / completion→consumer-worker handoff / in-flight accounting) is a
   first-class confound; `nb(1)` is its overhead floor and recovery is read above it.
2. exp37 confirms or refutes head-of-line **removal** within the **op-lane admission
   contract** and the unchanged **RuntimeFuture / OperationResult result contract**;
   only per-lane completion order is relaxed (**order-agnostic completion**).
3. **More in-flight is a diagnostic lever, not automatically the production design**;
   non-blocking is **experimental and operation-lane-only**, and `max_inflight` is a
   bounded knob, not sizing guidance.
4. Actor lanes are always serial; their safety is covered by the prototype integration
   tests, not this experiment.
5. The CPython submit/retire loop may govern the measurement if HPX is not backlogged;
   `lane_stats` is the in-scope control. `lane_stats` snapshots are racy, context-only,
   never a gate.
6. `park_ms` is synthetic cooperative parked wait — **not real I/O, not inference**. Not
   Ray Serve / cluster scaling / HPX priority scheduling / NUMA / latency-SLO /
   capacity / performance claim, and not "make non-blocking the default".

## Commands

```
python -m py_compile experiments/37_ray_hosting_rayx_nonblocking_lane/run_ray_hosting_rayx_nonblocking_lane.py

# laptop-safe structural smoke (SMOKE-ONLY, not evidence)
python experiments/37_ray_hosting_rayx_nonblocking_lane/run_ray_hosting_rayx_nonblocking_lane.py --smoke

# homogeneous many-core Linux observation
python experiments/37_ray_hosting_rayx_nonblocking_lane/run_ray_hosting_rayx_nonblocking_lane.py --full
```

Requires Ray and the built `_rayx`; the runner **skips cleanly (exit 0)** if either is
unavailable. Every mode prints a compact `machine-info` block.

## Results

**One full Rostam run recorded — `STRUCTURAL GATES: PASS`; near-load FULL SUPPORT,
over-load PARTIAL SUPPORT (no STOP).** Measurement stopped after this single `--full`
run: the mechanism reading is decisive, and the goal is to confirm/refute head-of-line
removal within the op-lane admission contract, not to chase a number.

**Node (`medusa06`).** Intel Xeon Gold 6148, 40 CPUs, 2 sockets × 20 cores,
1 thread/core; Ray 2.55.1; `.venv/bin/python`.

**Structural gates — all PASS.** Real Rostam smoke (`STRUCTURAL GATES: PASS`,
`INCONCLUSIVE (smoke-only)`) and the `--full` run both passed all five gates
(`agg_ok`, `futures_completed`, `plain_types_ok`, `lane_ids_ok`, `clean_shutdown`).

**Controls (all held this run):** every serial anchor reproduced erosion (positive
control); `lane_stats` showed HPX backlogged/active in the matched arms (driver not
starved); compute-only `wall_C` stayed flat across modes at fixed `hpx_threads`
(recovery is **not** added compute capacity); `nb(max_inflight=1)` sat at the serial
anchor (`nb1_minus_serial ≈ 0.00–0.08`) — the retirement-path **overhead floor**
behaved as intended; and `qd_mean_T` **fell as `max_inflight` rose** in every ladder
(head-of-line corroboration).

**Per-ladder graded reading (compute `thr_retention` by mode; recovery read above the
`nb(1)` floor):**

| ladder (`HT=num_lanes`) | serial → nb-mi1 → nb-mi2 → nb-mi4 | `nb1−serial` | `qd_mean_T` | shape | verdict |
| --- | --- | --- | --- | --- | --- |
| HT=4 near | 0.47 → 0.47 → **0.91** → **0.99** | ≈0.00 | 1.2 → 0.0 | step at nb-mi2 | **FULL SUPPORT** |
| HT=8 near | 0.47 → 0.47 → **1.00** → **1.01** | ≈0.00 | 3.2 → 0.0 | step at nb-mi2 | **FULL SUPPORT** |
| HT=4 over | 0.19 → 0.26 → 0.45 → **0.77** | 0.08 | 21.1 → 11.1 | ramp | **PARTIAL SUPPORT** |
| HT=8 over | 0.37 → 0.36 → 0.55 → **0.81** | ≈0.00 | 30.9 → 15.0 | ramp | **PARTIAL SUPPORT** |

**What this confirms (observation-only, Rostam-specific, synthetic):**

* **Near load — FULL SUPPORT.** At fixed `num_lanes == hpx_threads` (no added lanes or
  workers), switching op-lanes to non-blocking **removes** the per-lane head-of-line
  that the serial `.get()` consumer imposed: compute retention recovers to ~0.91–1.01,
  with the expected **step at `nb-mi2`** (one in-flight slot past the park unblocks
  compute on the same lane) and `qd_mean_T` collapsing to ~0. `nb(1)` ≈ serial confirms
  the recovery is the added in-flight concurrency, not the mode itself.
* **Over load — PARTIAL SUPPORT.** Non-blocking dispatch is a **major** lever (0.19→0.77
  at HT=4, 0.37→0.81 at HT=8, `qd_mean_T` roughly halving), but recovery is a **ramp,
  not a step**, and the top `max_inflight` does not fully reach the SUPPORT band. A
  **residual limiter remains** under over-load — most plausibly the non-blocking
  **retirement-path overhead** (continuation / per-lane mutex contention /
  `notify_all` / completion→consumer-worker handoff) and/or driver/scheduler pressure
  at high offered concurrency. The ramp (rather than a single-slot step) is itself the
  evidence that something beyond single-slot head-of-line is binding here.
* **No STOP.** Head-of-line removal is real in all four ladders; the open question
  under over-load is the residual, not whether removal happens.

**What this does NOT claim.** **More in-flight is a diagnostic lever, not a production
design**; non-blocking stays **experimental and operation-lane-only** (actor lanes
serial; their safety is covered by `tests/integration/test_runtime_nonblocking_lane.py`,
not this experiment), and `max_inflight` is a bounded knob, not sizing guidance. exp36
is an in-flight-capacity **analogy** (dilution), not an equivalence (removal). Single
run, fine granularity, `num_lanes == hpx_threads ∈ {4,8}`, Rostam-specific; no curated
`aggregate` JSON for a single mechanism run. **Not** Ray Serve, Ray cluster scaling,
HPX priority scheduling, real I/O, inference, NUMA, latency-SLO / capacity / sizing /
performance, "RayX makes Ray faster" / "HPX beats Ray", or a recommendation to make
non-blocking the default. The over-load residual is a candidate for a later
retirement-path probe (e.g. `notify_one` vs `notify_all`, lock-hold reduction), **not**
revisited here.
