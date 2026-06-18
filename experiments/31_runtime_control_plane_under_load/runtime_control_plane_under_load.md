# Runtime control-plane-under-load probe (observation-only, decision gate)

> **Observation-only / machine-specific.** This is a **decision gate**, not a
> conclusion about HPX performance. It measures whether `rayx.runtime`
> control-plane operations stay responsive while the same HPX runtime is
> saturated with CPU-bound Async work — to decide whether HPX thread priorities
> or named pools are worth exploring next. No Ray comparison, no throughput /
> capacity / sizing claim, no "HPX is fast/slow" verdict. All numbers below are
> from one machine, one run, and are not a benchmark.

## Question

The `rayx.runtime` control plane and data plane share **one default HPX thread
pool**: the lane consumers (`RuntimeLane` workers), the control hops, and the op
bodies all live there. When every HPX worker is busy running CPU-bound Async
`busy_sum`, do the control operations that cross `hpx::run_as_hpx_thread` stay
**bounded**, or do they **balloon** because they cannot get a worker slot?

This replaces the earlier generic "busy_sum/busy_get overlap" framing (which
would only have re-measured "CPU work needs cores"). The control-plane question
is the one that actually decides whether HPX priorities / named pools are
motivated.

## Two classes of control op (by exposure to pool saturation)

* **HPX-hop-exposed** — route through `hpx::run_as_hpx_thread`, so they need a
  worker slot on the saturated default pool to make progress:
  * `Runtime.submit_operation` (enqueue / admission)
  * `Runtime.lane_stats`
  * `Runtime.create_actor`
  * `Runtime.release_actor`
* **Hop-free negative control** — signalled from the Python thread via the atomic
  cancel token with **no** `run_as_hpx_thread` hop (`runtime_cancel.hpp`):
  * `RuntimeFuture.cancel`

If the hop ops inflate under load while the hop-free `cancel` stays flat, the
inflation is **HPX scheduling latency** (not Python / GIL / pybind), because
`cancel` is subject to the same Python/GIL path but not to the HPX pool.

## Method

`run_runtime_control_plane_under_load.py`, public `rayx.runtime` API only:

1. Build one `Runtime(num_lanes, hpx_threads)`; verify a small **completed**
   `busy_sum` value `(n(n-1)/2) mod 2^31` and `rt-hpx-` lane ids (structural
   gates).
2. Measure each control op's Python-call wall latency on the **idle** runtime
   (warmup first, then repetitions; p50/p90/p99).
3. **Saturate** the data plane: submit one long, oversized Async `busy_sum` per
   load lane; poll `lane_stats()` until every load lane is `active` (so all HPX
   workers have a runnable CPU body) *before* timing anything.
4. Re-measure the same control ops under saturation, in the **same process /
   runtime** (same pool — only the load differs, so idle vs saturated is a clean
   comparison).
5. Teardown: running-cancel the load (cancellable at chunk boundaries), retire
   every future (no broken promises), shut down.

Each cell runs in its own **pristine subprocess** (the HPX runtime is a
process-global singleton; one active `Runtime` per process). The parent stays
HPX-free and only aggregates. The runner writes **no JSONL / corpus artifact** —
only a compact printed summary.

### Timing discipline and confounds

* Warmup iterations are untimed; load is fully established before any saturated
  timing; control ops are timed as single synchronous calls (no submit-stagger
  skew); monotonic `perf_counter`.
* `busy_sum` bodies are **native C++ and do not hold the GIL during the loop**,
  so the Python thread issuing control calls is not GIL-blocked by the load —
  HPX scheduling is the isolated variable.
* **Checkpoint-yield confound:** `busy_sum` yields cooperatively every
  `STRIDE=8192` iterations. That yield cadence sets how often a queued control
  hop can be scheduled, so results are specific to this fixed stride; a tighter
  (non-yielding) body would be expected to starve control more. `checkpoint_count`
  of the load op is recorded per cell.
* **Ratio caveat:** idle baselines are microsecond-scale, so a large p50 *ratio*
  over a few-µs baseline is scheduling jitter, not a balloon. A control plane is
  judged by **absolute** saturated latency; the reading treats a meaningful
  absolute p99 as the real trigger and the ratio as supporting color.

## Sweep

* `--quick`: 1 cell (`hpx_threads=2`, `load_lanes=4` — 2× oversubscription),
  reps 25.
* `--full`: `hpx_threads ∈ {1,2,4}` (capped to cores) × load multiplier
  `{1,2,4}`, reps 80, work_n 4e9 (oversized; running-cancelled at teardown).

## Results (this machine, one run — `cpu_count=10`; observation-only)

Structural gates passed on **every** cell (quick + all 9 full cells):
`busy_sum_value_ok`, `lane_ids_valid`, `controls_valid`, `actor_prefix_ok`,
`load_retired_clean`, `saturation_established`, `cell_completed` — all true. A
cell that never saturated the data plane fails `saturation_established`, since
its saturated timings would not be a valid idle-vs-load comparison.

Representative full-mode cells (p50, ms; ratio = saturated/idle p50):

| hpx_threads | load_lanes | op | idle p50 | sat p50 | ratio | sat p99 |
|---|---|---|---|---|---|---|
| 1 | 1 | release_actor | 0.0028 | 0.0128 | 4.50× | 0.0154 |
| 1 | 4 | lane_stats | 0.0033 | 0.0233 | 7.08× | 0.0270 |
| 1 | 4 | release_actor | 0.0030 | 0.0549 | 18.57× | 0.0673 |
| 1 | 4 | cancel (hop-free) | 0.0001 | 0.0004 | 3.00× | 0.0028 |
| 2 | 8 | release_actor | 0.0091 | 0.0532 | 5.83× | 0.0644 |
| 4 | 16 | release_actor | 0.0137 | 0.0497 | 3.62× | 0.1014 |
| 4 | 16 | cancel (hop-free) | 0.0005 | 0.0010 | 1.92× | 0.0084 |

* **The HPX scheduling effect is real and monotone:** hop-op latency rises with
  the runnable-thread:worker ratio (worst at `hpx_threads=1, load=4`, where
  `release_actor` shows ~18× and `lane_stats` ~7×). `release_actor` is the most
  exposed (it must run a lane worker to observe stop and join under load).
* **But in absolute terms the control plane stayed bounded:** the worst HPX-hop
  saturated p99 across all 9 cells was **0.10 ms (~101 µs)** — about **50× under
  the 5 ms meaningful-latency gate**.
* **The hop-free `cancel` stayed flat** (sub-10 µs p99 everywhere), confirming the
  inflation that exists is HPX scheduling, not Python/GIL.

## Reading and decision

**STOP — named pools / thread priorities are NOT motivated by this evidence.**

The mechanism the chapter hypothesized (control hops competing with CPU bodies
on one pool) is visible, but its magnitude is trivial on this machine: even at
heavy oversubscription the control plane answers in tens of microseconds. This
is the expected outcome when the data-plane bodies yield cooperatively at
checkpoints, keeping control hops schedulable. Building HPX priority scheduling
or resource partitioning on top of a sub-150 µs control plane would be a
solution without a measured problem.

This is a **decision gate**, not a performance claim. The decision rule was set
in advance:

* hop ops bounded + cancel flat → **STOP** (this run). *(reached)*
* hop ops reach a meaningful absolute latency + cancel flat → next slice would
  test **HPX control-hop priority** before named pools.
* named pools / resource partitioning stays deferred either way (and would in any
  case need a many-core target and an opt-in, bootstrap-isolated design).

## When to revisit

Revive only if a concrete signal contradicts this: a future workload where
`submit`/`lane_stats`/`create`/`release` is *observed* to reach
serving-relevant latency (e.g. milliseconds) under load — for instance a
non-yielding data-plane body, a many-core box with far higher oversubscription,
or a Ray-hosted runtime workload reporting control sluggishness. Until then, the
runtime stays at its coherent resting state.

## Reproduction

```
python -m py_compile experiments/31_runtime_control_plane_under_load/run_runtime_control_plane_under_load.py
python experiments/31_runtime_control_plane_under_load/run_runtime_control_plane_under_load.py --quick
python experiments/31_runtime_control_plane_under_load/run_runtime_control_plane_under_load.py --full
```

Requires the built `_rayx` extension; the runner skips cleanly (exit 0) if it is
unavailable. Exit 0 = structural gates passed (or skipped); exit 1 = a structural
gate failed. No Ray dependency. Latency numbers will differ per machine and run;
the structural gates and the hop-exposed-vs-hop-free *contrast* are the portable
part, not the absolute values.
