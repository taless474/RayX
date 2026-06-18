# Intra-actor native CPU scaling inside one Ray actor (observation-only, quick)

> **Observation-only / machine-specific.** This measures an *in-process
> execution-engine scaling property*, not a Ray-vs-RayX performance verdict. It
> is **not** "RayX makes Ray faster", **not** "HPX beats Ray", **not** "RayX
> replaces Ray", and **not** a Ray backend / Serve / object-store / inference
> claim. Numbers are from one machine, one run, and are not a benchmark.

## Question

Inside **one long-lived Ray actor** (one process, one Ray boundary held
constant), can RayX/HPX scale native CPU-bound Async work (`busy_sum`) across HPX
workers, while an **equivalent pure-Python in-process CPU loop** stays GIL-bound
(~1×)?

This is the CPU-scaling reframing of the Ray-hosting arc: exp27/28/30 showed a
Ray actor can *host* a RayX `Engine` / `Runtime` cleanly. exp32 asks the one
honest performance-flavored question that has a real mechanism behind it — does
the **choice of in-process engine** change how CPU work scales *within* a single
Ray actor.

## Method — strong scaling of a fixed batch, normalized per leg

* A batch is `K` `busy_sum(n)` ops. We vary `W = num_lanes = hpx_threads ∈
  {1,2,4}` and measure the wall to complete all `K`. With `W` workers the `K`
  ops run in `ceil(K/W)` waves, so ideal `T(W) ≈ (K/W)·op`.
* **Per-leg normalized speedup** `= T_leg(W=1) / T_leg(W)`. Dividing each leg by
  its **own** single-worker time **cancels the absolute C++-vs-CPython per-op
  cost (M1)**, leaving only how the engine **scales (M2)**. We compare the two
  speedup *curves*, never raw Python-vs-RayX wall time.
* The Python leg runs the **iterative masked loop** (`acc = (acc + i) &
  0x7FFFFFFF`) — genuine CPU work, not the closed form — so it is a real CPU
  workload computing the same value `busy_sum` defines.

Two measurement points per batch: **in-actor wall** (around the engine loop
only — the engine-clean metric) and **end-to-end wall** (around the single Ray
actor call — includes the boundary). One Ray actor method call per measured
batch; warmup 1, reps 3.

### Baselines and boundaries

* **P leg (structural GIL baseline):** a Ray actor running the CPU work as a
  **serial** in-process Python loop. `W` is nominal — we deliberately do **not**
  thread Python (threaded pure-Python CPU would still be ~1× under the GIL), so
  the flat curve is a structural CPython property, not a tuned strawman.
* **C leg (candidate):** a Ray actor hosting one
  `Runtime(num_lanes=W, hpx_threads=W)` running `W` concurrent `busy_sum` per
  batch. `ActorHandle` / `RuntimeFuture` / `OperationResult` are created and
  retired **inside** the actor; only plain scalars/containers cross the Ray
  boundary.
* **Ray's idiomatic CPU-scaling answer is more actors/processes** — acknowledged
  here, and deliberately **not** run as a head-to-head leg. The architectural
  contrast is "scale within one process (HPX) vs across processes (Ray)".

## Results (this machine, one run; observation-only)

`cpu_count=10` (Apple silicon, P/E heterogeneous), `K=4`, `n=5_000_000`,
`checkpoint_count=611`, warmup 1, reps 3.

| leg | W | in-actor ms (med) | spread | speedup | eff | end-to-end ms (med) |
|---|---|---|---|---|---|---|
| PY | 1 | 535.8 | 0.7% | 1.00 | 1.00 | 537.1 |
| PY | 2 | 535.7 | 0.4% | 1.00 | 0.50 | 536.9 |
| PY | 4 | 535.2 | 0.3% | 1.00 | 0.25 | 536.6 |
| RAYX | 1 | 10.1 | 9.1% | 1.00 | 1.00 | 11.0 |
| RAYX | 2 | 5.1 | 7.6% | 1.99 | 0.99 | 6.3 |
| RAYX | 4 | 3.0 | 9.7% | 3.43 | 0.86 | 5.6 |

Structural gates all passed: `agg_ok`, `futures_completed`, `lane_ids_ok`,
`plain_types_ok`, `clean_shutdown`. (M1 context only, **not** a speedup claim:
the native/CPython per-op factor was ~52.8× at `W=1` — this is the disclosed
language-cost axis, removed from the claim by per-leg normalization.)

## Reading

**SUPPORT (this run/machine).** RayX in-actor speedup rises with `W`
(1.00 → 1.99 → 3.43, efficiency ≥ 0.86 through `W=4`) while the Python leg stays
~1.00 at every `W`. Under the per-leg normalization (which removes the absolute
C++-vs-Python factor), this isolates the mechanism:

> Inside one long-lived Ray actor, RayX/HPX shows **intra-process native CPU
> scaling** for Async native work, while pure-Python CPU work in one process
> remains **GIL-bound**.

Nothing stronger is claimed. Specifically: this is *not* "RayX makes Ray
faster", not "HPX beats Ray", not "RayX replaces Ray", and the raw
Python-vs-RayX wall times are not a speedup claim.

### On the Ray boundary

End-to-end numbers include the Ray actor boundary and should not be read as a
pure engine metric. In **this** run the boundary added only ~1 ms (RAYX `W=1`:
10.1 ms in-actor vs 11.0 ms end-to-end), so at these op sizes the boundary did
**not** dominate — the per-batch native work is large relative to one Ray call.
This does not contradict exp27 (where the per-call work was smaller and the
boundary dominated); it just reflects that exp32 deliberately sized the batch so
the *engine* is the measured thing. The boundary cost is fixed per call, so it
would dominate again for much smaller/finer batches.

## Decoupling panel (opt-in `--decouple`, observation-only)

A separate RAYX-only panel probes the **runtime scaling bound** — effective
parallelism ≈ `min(num_lanes, hpx_threads, cores)` — by decoupling lanes from
workers. Four cells at `K=8` (≥ the largest lane/worker count, so a batch can
occupy every worker), each holding **effective parallelism ≤ 4** (only four busy
bodies ever run at once → stays in the P-core region; no `W>4` knee). `busy_sum`
only; no Python leg (the bound is a runtime property, not a language
comparison). `speedup/1x` is vs the `baseline_1x` cell; `ratio/4x` is vs the
coupled `4×` reference (≈1.0 means "behaves like 4 effective workers").

Representative run (this machine; observation-only, machine-specific):

| cell | lanes | hpx_threads | in-actor ms (med) | spread | speedup/1x | ratio/4x |
|---|---|---|---|---|---|---|
| baseline_1x | 1 | 1 | 19.0 | 2.1% | 1.00 | 3.36 |
| coupled_4x | 4 | 4 | 5.7 | 16.2% | 3.36 | 1.00 |
| worker_bound | 8 | 4 | 5.9 | 3.0% | 3.25 | 1.04 |
| lane_bound | 4 | 8 | 9.0 | 26.2% | 2.12 | 1.59 |

**Panel verdict: INCONCLUSIVE (this run/machine).** The **worker-bound**
direction is clean: `lanes=8, hpx_threads=4` tracks the 4-effective reference
(`ratio/4x ≈ 1.04`) — adding lanes beyond the 4 workers does **not** help, as the
bound predicts. The **lane-bound** direction did **not** track cleanly:
`lanes=4, hpx_threads=8` came out ~1.6× *slower* than the reference and noisy
(26% spread). The likely cause is over-provisioning workers past lanes —
`hpx_threads=8` spawns eight HPX OS workers while only four lanes feed work, so
the four idle workers add polling/scheduling overhead and the active bodies are
more exposed to P/E heterogeneity. So on this laptop the panel **observes the
worker-bound half of `min(num_lanes, hpx_threads)` but cannot cleanly confirm the
lane-bound half**; a homogeneous many-core box would be needed to read it
without the idle-worker/heterogeneity noise. This panel is a runtime-bound
observation only — **it does not modify the exp32 quick SUPPORT** above, and is
not a benchmark or sizing claim.

## Confounds and caveats

* **M1 (native vs CPython per-op cost):** real (~50×) but removed from the claim
  by per-leg normalization; reported as context only.
* **GIL is structural:** the Python flat curve is a CPython property in one
  process, not a tuning failure; threaded pure-Python CPU would also be ~1×.
* **Ray scales via more processes** — the honest alternative, not measured here.
* **DispatchPolicy:** only **Async** ops (`busy_sum`) run on the worker pool and
  overlap; **Inline** ops (`square`, counter `add/get/reset`) run on the lane
  consumer and would **not** show this scaling — so the CPU op must be `busy_sum`.
* **busy_sum checkpoint yields** (`STRIDE=8192`) and **per-lane consumer
  threads** add scheduling overhead that caps efficiency below ideal (the `W=4`
  efficiency of 0.86 is consistent with this, plus core heterogeneity).
* **Apple-silicon P/E cores, SMT, thermal:** scaling is non-linear and saturates;
  `W` is capped at 4 in quick mode to stay near the performance cores. The
  speedup magnitudes are qualitative, not a clean linear-scaling benchmark.
* **Scaling bound:** parallel speedup is bounded by
  `min(num_lanes, hpx_threads, physical cores)` (cf. exp22 / exp31).

## Reproduction

```
python -m py_compile experiments/32_ray_hosting_rayx_cpu_scaling/run_ray_hosting_rayx_cpu_scaling.py
python experiments/32_ray_hosting_rayx_cpu_scaling/run_ray_hosting_rayx_cpu_scaling.py --quick
python experiments/32_ray_hosting_rayx_cpu_scaling/run_ray_hosting_rayx_cpu_scaling.py --decouple
python experiments/32_ray_hosting_rayx_cpu_scaling/run_ray_hosting_rayx_cpu_scaling.py --full
```

Requires Ray and the built `_rayx`; the runner skips cleanly (exit 0) if either
is unavailable. Exit 0 = gates passed (or skipped); exit 1 = a structural gate
failed. No JSONL/corpus artifact is written. Every mode prints a compact
`machine-info` block (platform, Python, CPU count, uname/arch, Ray version, plus
an `lscpu` summary on Linux) so future cross-machine runs are self-describing.

## Full mode (`--full`) — prepared tool, not yet trusted evidence

`--full` now **exists** as a prepared tool for **future homogeneous many-core
Linux validation**. It walks the wider `W ∈ {1,2,4,8,16,32}` sweep at `K=32`,
running the **same** workload shape (RayX Async `busy_sum` + the pure-Python CPU
loop) and the **same** per-leg normalization as quick. `K=32` is required because
**`K` must be ≥ `max(W)`** — otherwise the batch has fewer ops than workers and
cannot occupy all of them, capping observed occupancy (exactly quick mode's old
`K=4` mistake). If the detected CPU count is below a default `W`, those high-`W`
cells are dropped and a warning is printed. It keeps **only the structural gates**
(`agg_ok`, `futures_completed`, `lane_ids_ok`, `plain_types_ok`,
`clean_shutdown`); timing is **never** a pass/fail gate, and a noisy or
non-monotone run is reported as an observational `NOISY` / `INCONCLUSIVE`, not
`FAIL`.

**Full mode has not yet produced a trusted result.** It is meaningful only on a
homogeneous many-core box; on this Apple-silicon laptop the `W>4` cells are
confounded by P/E heterogeneity, SMT, and thermal behavior (the decoupling
panel's noisy lane-bound cell already shows over-provisioning workers strays into
that regime). Any full-mode output produced here is therefore **smoke-only, not
evidence**, and the runner prints that warning at the top of every full run.

**The current validated exp32 evidence remains unchanged:** `--quick` is
**SUPPORT** (RayX in-actor speedup rises with `W`, Python flat), and `--decouple`
is **INCONCLUSIVE overall with the worker-bound half clean** (`lanes=8,
hpx_threads=4` tracks the 4-effective reference; the lane-bound half stays noisy
on this laptop). `--full` does not change either reading until it is run on
appropriate homogeneous hardware.

Absolute numbers will differ per machine and run; the **shape** (RayX speedup
rising with `W`, Python flat) and the per-leg-normalized framing are the portable
parts.
