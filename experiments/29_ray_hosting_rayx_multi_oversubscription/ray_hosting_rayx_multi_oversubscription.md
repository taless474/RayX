# Multi-actor oversubscription for Ray-hosted rayx.Engine (resource-budget, observation-only)

Follow-on to experiment 27 (`27_ray_hosting_rayx_engine/`), answering its open
lifecycle question: when **N Ray actors each host one `rayx.Engine`** with
`hpx_threads=T`, how should Ray's per-actor `num_cpus` reservation be sized, and
what happens when it is mismatched? Driver:
`bench/run_ray_hosting_rayx_multi.py` (reuses exp27's `RayxHostActor`, v1
`_make_record`, and `boundary="ray-actor-rayx-engine"` unchanged; adds only the
multi-actor round-robin, per-actor `num_cpus` reservation, and a placement guard).

**Strict framing.** Resource-budget characterization only — **not** a Ray
benchmark, **not** "HPX beats Ray", **not** a general performance result.
`work_mode="spin"` is a synthetic **CPU-bound diagnostic** (it contends for
cores); `work_mode="sleep"` parks rather than contends and will **not** reveal
core oversubscription. All magnitudes below are **machine-specific** (this host:
**10 physical cores**), **single-run with modest sample counts** (so the p99
figures are noisy, not stable estimates), and never gated. The multi-actor
round-robin shape is a **Ray-pattern mapping / resource diagnostic, not
HPX-native programming guidance** (see "HPX-native direction" below). The
finding was measured on the **default `lane_impl="std"` / `ServiceLane`**
backend; the `hpx` backend differs (see Guidance).

## Two distinct mismatches

Ray's scheduler accounts only for the `num_cpus` you **reserve** per actor; it
cannot see how many HPX worker threads (or `ServiceLane` threads) an Engine
actually runs. So there are two separate failure modes:

* **Under-reserve** (`num_cpus_per_actor < hpx_threads`): Ray places the actors
  (accounting satisfied) but the process runs more threads than the reserved
  budget. Ray permits it silently.
* **Over-reserve** (`num_actors × num_cpus_per_actor > num_cpus`): Ray **refuses
  to place** all actors — they sit `PENDING`.

## Finding 1 — the `hpx_threads` vs `num_cpus` mismatch did **not** bite (num_lanes=1)

The originally-framed knob, run as specified:

| Cell | actors | T | R (cpus/actor) | C | active spins | completed | p50 / p90 / p99 ms |
|---|---|---|---|---|---|---|---|
| Aligned | 2 | 1 | 1 | 2 | 2 | 40 | 14.80 / 15.30 / 16.49 |
| Under-reserved | 2 | 2 | 1 | 2 | 2 | 40 | 15.14 / 16.13 / 17.67 |

Near-identical — under-reserving `hpx_threads` relative to the reservation did
**not** inflate latency. Why: with `num_lanes=1` a `ServiceLane` Engine spins
**one** request at a time, so the extra HPX worker is idle and there is no real
core contention. `hpx_threads` is the HPX worker-pool size, **not** the active
core demand for the default `std`/`ServiceLane` backend; `num_lanes` is what sets
concurrently-active serialized requests per Engine. (The driver's
`hpx_pool_gt_reserved=True` flag here means only "the per-actor HPX pool exceeds
its Ray reservation," an accounting fact — **not** observed contention. The real
signal is the separate `active_lanes_gt_cores` flag.)

## Finding 2 — the binding resource is concurrent **active lanes** vs **physical cores**

Scaling concurrently-active spinning lanes (`num_actors × num_lanes` for `std`
lanes) across the 10-core boundary, with Ray reservations kept **aligned** in
both cells:

| Cell | actors × lanes | active spins | completed | p50 / p90 / p99 ms | svc p50 / p99 ms | thr req/s |
|---|---|---|---|---|---|---|
| Below cores | 4 × 2 | 8 (≤ 10) | 160 | 15.06 / 17.11 / 18.62 | 5.00 / 5.24 | 519 |
| Above cores | 8 × 2 | 16 (> 10) | 320 | 17.95 / 23.40 / 28.04 | 5.00 / 6.61 | 859 |

Once active spins (16) exceed physical cores (10), the tail inflates clearly
(p99 18.6 → 28.0 ms, ≈ +50%) and the **observed service time** itself inflates at
the tail (spin svc p99 5.24 → 6.61 ms) — the fixed 5 ms spin loop is timeshared
across too few cores. Aggregate throughput still **rises** (519 → 859 req/s,
more actors doing work in parallel) — but the two cells **change `N`**, so
throughput is **not a controlled comparison** and should not be read as a
speedup; the **durable signal is the tail / service-time inflation** once active
spin lanes exceed cores. Both cells had aligned reservations (`R=1, C=num_actors`),
so **Ray's `num_cpus` did not prevent the contention** — it is logical
accounting, not a CPU cap or core pin.

## Finding 3 — over-reserve placement refusal exits cleanly

`--num-actors 2 --hpx-threads 2 --num-cpus-per-actor 2 --num-cpus 2`
(`2 × 2 = 4 reserved > 2`) with `--actor-ready-timeout 5`:

```
[run_ray_hosting_rayx_multi] PLACEMENT-STARVATION: 2 actors x num_cpus_per_actor=2
 = 4 reserved CPUs > num_cpus=2; Ray left actor(s) PENDING past
 --actor-ready-timeout=5.0s. No JSONL written.
```

The guard reports cleanly and exits 0 (no hang, no JSONL), then `ray.shutdown()`.
Distinct from Finding 2: this is Ray refusing placement at the **reservation**
level, separate from any latency interpretation.

## Guidance (machine-specific, observation-only) — and it is **backend-specific**

The active-CPU-demand limiter depends on the lane backend; this experiment
measured only the default **`lane_impl="std"` / `ServiceLane`**.

* **`std` / `ServiceLane` (measured here).** Each lane is a **serialized
  `std::thread` lane**, so active CPU-bound demand is driven by **concurrently
  active lanes** (`≈ num_lanes` per Engine). This demand **can exceed
  `hpx_threads`** when `num_lanes > hpx_threads` — the HPX worker pool sits
  mostly idle for std lanes, so **`hpx_threads` is *not* the active-demand bound**
  here. Budget `num_cpus_per_actor` for the expected **active lanes**; if you
  want Ray's accounting to cover *both* the lane `std::thread`s and the (largely
  idle) HPX workers, **`max(num_lanes, hpx_threads)`** is the conservative
  process-thread budget.
* **`hpx` / `HpxLane`.** Lanes run as `hpx::thread`s on the HPX worker pool, so
  active CPU-bound execution is **bounded by `hpx_threads`** rather than by
  `num_lanes` — the limiter is approximately **`min(num_lanes, hpx_threads)`**.
  This is consistent with **experiment 22**, where spin overlap under `HpxLane`
  roughly follows `hpx_threads` (`overlap_ratio` ≈ 1.0 / 2.0 / 3.83 at
  `hpx_threads` 1 / 2 / 4, `num_lanes=8`); same observation-only,
  machine-specific caveat applies. So for `hpx`, `hpx_threads` is closer to the
  active execution bound.
* **The operationally binding constraint is total concurrent active CPU-bound
  lanes ≤ physical cores** (for `std`, `Σ num_actors × num_lanes ≤ cores`).
  Exceeding it inflates tail latency and observed service time even when Ray's
  per-actor reservations look satisfied — because `num_cpus` is logical
  accounting, not a core cap.
* **Do not over-reserve past total logical CPUs** (`Σ num_cpus_per_actor ≤
  num_cpus`), or Ray leaves actors `PENDING`.
* `sleep`-bound work does not contend for cores, so this sizing matters for
  CPU-bound serving, not parked-wait serving.

## HPX-native direction (framing, not a proposal)

The deeper lesson is about HPX's resource model. Ray-hosting runs **N
independent HPX runtimes in N Ray worker processes**, each started with its own
`--hpx:threads` and each implicitly assuming it owns the whole machine. Those
runtimes **do not coordinate core ownership** with one another, and Ray's
`num_cpus` is only **logical accounting** — not HPX resource partitioning and not
core pinning. That fragmentation is exactly why the oversubscription appears and
why `num_cpus` cannot prevent it.

A more idiomatic HPX-native design would prefer **one coordinated HPX runtime**
managing all cores via **resource partitioning** (`hpx::resource::partitioner`),
**explicit thread pools**, and **executors** — so the scheduler arbitrates cores
globally instead of N schedulers each over-claiming them. Ray-hosting
**deliberately trades that global HPX coordination away** because Ray owns the
outer placement/lifecycle; the price is that **operators must size per-actor
budgets honestly** (above). This is framing for the long-term direction, not a
design proposal in this slice, and the multi-actor round-robin driver remains a
**Ray-pattern resource diagnostic, not HPX-native programming guidance**.

## Reproduce

```
# Finding 1 (specified matrix)
python bench/run_ray_hosting_rayx_multi.py --num-actors 2 --hpx-threads 1 \
  --num-cpus-per-actor 1 --num-cpus 2 --work-mode spin --service-ms 5 \
  --requests 40 --warmup-requests 8 --concurrency 4 --out results/multi_aligned.jsonl
python bench/run_ray_hosting_rayx_multi.py --num-actors 2 --hpx-threads 2 \
  --num-cpus-per-actor 1 --num-cpus 2 --work-mode spin --service-ms 5 \
  --requests 40 --warmup-requests 8 --concurrency 4 --out results/multi_oversub.jsonl

# Finding 2 (active-lanes vs cores — adjust to your core count)
python bench/run_ray_hosting_rayx_multi.py --num-actors 4 --num-lanes 2 \
  --num-cpus-per-actor 1 --num-cpus 4 --work-mode spin --service-ms 5 \
  --requests 160 --warmup-requests 16 --concurrency 8 --out results/multi_below.jsonl
python bench/run_ray_hosting_rayx_multi.py --num-actors 8 --num-lanes 2 \
  --num-cpus-per-actor 1 --num-cpus 8 --work-mode spin --service-ms 5 \
  --requests 320 --warmup-requests 32 --concurrency 16 --out results/multi_above.jsonl

# Finding 3 (placement refusal — clean, no hang)
python bench/run_ray_hosting_rayx_multi.py --num-actors 2 --hpx-threads 2 \
  --num-cpus-per-actor 2 --num-cpus 2 --work-mode spin --service-ms 5 \
  --requests 20 --warmup-requests 0 --concurrency 4 --actor-ready-timeout 5 \
  --out results/multi_pending.jsonl

# Analyzer is reused UNCHANGED on every JSONL above:
python bench/analyze_jsonl.py results/multi_below.jsonl
```

Rows are v1 (`backend="rayx"`, `boundary="ray-actor-rayx-engine"`,
`worker_id`=outer Ray actor id, `actor_id`=inner rayx lane id); the driver's
stdout adds a CONFIG line and per-actor completed counts (the fairness view —
the analyzer reports the aggregate). The work was evenly balanced across actors
in every measured cell. JSONL stays under the git-ignored `results/`.

## Scope note

This slice covers the **Engine** (spin diagnostic) host only. It changes no
architecture and no analyzer. The `rayx.runtime.Runtime` host (experiment 28)
remains smoke-only with no JSONL; CounterActor hosting and any measured runtime
decomposition are deliberate later slices, not included here.
