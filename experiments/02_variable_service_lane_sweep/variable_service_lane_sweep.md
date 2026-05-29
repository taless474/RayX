# Variable-Service Lane/Actor Saturation Sweep

Cross-engine scaling sweep under the deterministic bimodal variable-service
workload. Documentation only; no code, analyzer, or schema changes. Follows
`benchmarks/08_ray_hpx_rayx_variable_service/ray_hpx_rayx_variable_service.md` (the cross-engine comparison that
introduced the shared sequence) and `experiments/01_sleep_overshoot/sleep_overshoot_note.md` (the
service-fidelity diagnostic whose caveats this sweep applies).

> **Update (2026-05-31) — high-lane ceiling interpretation refined.** §5 reads
> the high-lane plateau (all three engines converging near ~1370–1390 req/s) as
> a "client/coordination limit ... the single-threaded one_by_one client ...
> cannot retire results fast enough." A later native sleep-mode diagnostic (the
> `--diag` phase decomposition in `hpx_impl/hpx_synthetic_baseline.cpp`) pinned
> the mechanism precisely: it is primarily a **closed-loop FIFO-retire /
> client-driver artifact**, not HPX/service-lane coordination or machine
> scheduling. At the ceiling cell (16 lanes, concurrency 32, bimodal 1 ms/20 ms
> `p_high=0.1` seed 0, ct1, `--hpx:threads=4`), strict FIFO `one_by_one`
> retirement stalls short completed requests behind older in-flight 20 ms
> requests: throughput **1384 req/s**, `completion_ms` p50 **21.860 ms**,
> `total_ms` p50 **23.830 ms**, mean lane utilization **0.28**. Switching to
> `batch_wait` / as-completed retirement at the **same** concurrency window and
> **identical** lane service behavior (per-lane `busy_ms` unchanged) collapses
> the stall: throughput **2558 req/s (+85%)**, `completion_ms` p50 **0.003 ms**,
> `total_ms` p50 **1.326 ms**, mean lane utilization **0.52**. So the ~22 ms
> `completion` bucket was ready-but-unretired FIFO wait, **not** HPX
> future-completion overhead (which is microsecond-scale). A window-removed
> `submit_all_get_all` reaches ~**2985 req/s** but is **not** apples-to-apples
> (the in-flight window is removed). This removes FIFO head-of-line blocking
> **for this closed-loop bimodal sleep workload** — not a general claim that
> `batch_wait` is always faster. This sleep-mode retire ceiling is **distinct
> from** the `work_mode=spin` core-boundary saturation
> (`experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md`),
> which is a hardware/scheduling effect, not retire discipline. The §5
> "client/coordination limit" reading is **sharpened, not reversed**; the
> measurements below are preserved as provenance.

## 1. Purpose

Find where Ray, native HPX, and rayx **saturate** as serving lanes/actors
increase under bimodal variable service time. The prior cross-engine slice
showed 4 lanes reduced queueing and tail far more for HPX/rayx than Ray; this
sweep widens the lane count (1→16) and adds a second concurrency level to locate
the point of diminishing returns and the coordination ceiling.

## 2. Output location

```text
results/variable_service_lane_sweep_20260529T225359Z/
```

* 90 JSONL + 90 summary JSON + `aggregate.json`.
* 90/90 runs passed; 90/90 analyzer summaries passed.
* Pre-flight smokes passed: Ray 16 actors with `--num-cpus=16` (no hang, 16
  distinct actor IDs), HPX native 16 lanes, rayx 16 lanes. Machine: 10 cores
  (4 P + 6 E).

## 3. Experiment shape

* Engines: Ray (`ray-actor-process`), native HPX (`hpx-intra-locality`), rayx
  (`hpx-python-frontend`, `--api engine`).
* Service pattern: bimodal, low 1 ms / high 20 ms, `p_high=0.1`, `seed=0`.
* Lanes/actors: 1, 2, 4, 8, 16.
* Concurrency: 16 and 32.
* Repeats: 3. Requests: 1000. Warmup: 20. `retire_mode`: one_by_one. No batch.
* HPX native `--hpx:threads=4` and rayx `--hpx-threads=4`, fixed across all lane
  counts. Ray `--num-cpus = num_actors`.

## 4. Results

Median of 3 repeats. `spd` = throughput speedup vs 1 lane, `eff` = `spd / L`,
`p99cut` = `total_ms_p99` reduction vs 1 lane. Latencies in ms, throughput in
req/s.

**concurrency = 32** (lanes fed with ≥2 in-flight each, so lanes are the
bottleneck):

| eng | L | thru | spd | eff | tot_p50 | tot_p99 | svc_p99 | qw_p50 | qw_p99 |
|-----|--:|-----:|----:|----:|--------:|--------:|--------:|-------:|-------:|
| ray  | 1 | 164.4 | 1.00 | 1.00 | 191.77 | 261.10 | 21.03 | 188.96 | 248.98 |
| ray  | 2 | 307.2 | 1.87 | 0.93 |  87.70 | 240.33 | 21.02 |  86.26 | 238.28 |
| ray  | 4 | 598.6 | 3.64 | 0.91 |  38.76 | 149.62 | 21.01 |  36.04 | 147.33 |
| ray  | 8 | 1039.6 | 6.32 | 0.79 | 23.06 |  98.04 | 21.01 |  19.05 |  94.86 |
| ray  | 16 | 1373.0 | 8.35 | 0.52 | 18.41 | 91.24 | 21.01 |  16.50 |  90.10 |
| hpx  | 1 | 304.8 | 1.00 | 1.00 | 104.90 | 192.27 | 25.01 | 103.28 | 171.84 |
| hpx  | 2 | 535.8 | 1.76 | 0.88 |  61.85 | 107.43 | 25.01 |  38.04 | 104.12 |
| hpx  | 4 | 858.6 | 2.82 | 0.70 |  32.46 |  68.95 | 25.01 |   8.73 |  53.96 |
| hpx  | 8 | 1103.6 | 3.62 | 0.45 | 25.63 |  51.77 | 25.02 |   0.04 |  44.05 |
| hpx  | 16 | 1390.2 | 4.56 | 0.29 | 23.89 | 49.62 | 25.02 |   0.02 |  23.80 |
| rayx | 1 | 301.7 | 1.00 | 1.00 | 106.45 | 193.75 | 25.02 | 103.70 | 177.71 |
| rayx | 2 | 527.6 | 1.75 | 0.87 |  63.48 | 112.26 | 25.02 |  61.47 | 109.78 |
| rayx | 4 | 841.5 | 2.79 | 0.70 |  33.36 |  71.33 | 25.02 |  30.83 |  70.06 |
| rayx | 8 | 1102.1 | 3.65 | 0.46 | 26.06 |  51.29 | 25.02 |  24.03 |  50.03 |
| rayx | 16 | 1374.9 | 4.56 | 0.28 | 23.74 | 49.20 | 25.01 |  21.95 |  47.96 |

**concurrency 16 vs 32 at 16 lanes** (throughput, req/s):

| engine | conc 16 | conc 32 |
|--------|--------:|--------:|
| ray  | 1272 | 1373 |
| hpx  |  990 | 1390 |
| rayx |  985 | 1375 |

At 16 lanes, concurrency 16 supplies exactly 1 in-flight request per lane (no
queue), starving the lanes; concurrency 32 recovers ~40% throughput for
HPX/rayx. Full per-cell numbers are in `aggregate.json`.

## 5. Interpretation

* **Throughput rises for all engines up to 16 lanes; none fully plateaus** — but
  efficiency falls, marking diminishing returns.
* **HPX/rayx start much higher than Ray at 1 lane** (~302 vs 164 req/s, ~1.85×):
  Ray's per-request actor-process overhead dominates at a single lane.
* **Ray scales more linearly and reaches throughput parity by 16 lanes**
  (~1373 vs HPX 1390 / rayx 1375): adding actors parallelizes Ray's per-request
  overhead, so its speedup (8.35×) outpaces HPX/rayx (4.56×) from a lower base.
* **HPX/rayx have lower tail latency throughout.** At 16 lanes / concurrency 32:
  Ray `total_ms_p99` ≈ 91 ms vs HPX/rayx ≈ 49–50 ms. Ray's p99 is almost all
  (approximate) queue_wait — its runtime/IPC cost is added per request and does
  not shrink with lanes the way pure lane queueing does.
* **rayx tracks native HPX on the real metrics** — throughput within ~2% and
  `total_ms` p50/p99 nearly identical at every point.
* **The large queue_wait gap between HPX and rayx at high lanes is a definition
  artifact.** At 16 lanes native HPX reports `qw_p50` ≈ 0.02 ms (exact
  `start − submit`, excludes client-retire backlog) while rayx reports ≈ 22 ms
  (approximate `total − service`, includes it); their near-identical `total_ms`
  confirms identical underlying behavior. Compare total latency and throughput,
  not queue_wait, across these two.
* **Concurrency 16 starved 16-lane HPX/rayx; concurrency 32 was necessary** to
  feed the lanes and measure lane-limited (not concurrency-limited) behavior.
* **More than 4 lanes still helps, with diminishing returns.** Throughput keeps
  rising 4→8→16 and tails keep dropping, but the marginal gain shrinks.
* **HPX/rayx efficiency falls after ~8 lanes** (0.70 at 4 → 0.45 at 8 → ~0.29 at
  16), suggesting a client/coordination limit rather than lane-service capacity:
  the single-threaded one_by_one client plus the fixed 4 HPX worker threads
  cannot retire results fast enough, and all three engines converge near a
  shared ~1370–1390 req/s ceiling (far below the theoretical service ceiling).
* **Ray benefits more from 8→16 lanes** (1040→1373) but retains the worse p99
  tail.
* Per-lane work imbalance grows with lane count (at 16 lanes a lane holds 62–63
  requests but high-request counts span 2..12), identical across engines (shared
  sequence + round-robin routing) — an additional diminishing-returns factor.

## 6. Sleep-overshoot check

Service fidelity was re-checked at every lane count. HPX/rayx `service_ms_p99`
stayed ≈ 25 ms for 20 ms requests (~+25%); Ray stayed ≈ 21 ms (~+5%). **The
overshoot did not grow at 8/16 lanes** — the stability finding extends from the
earlier ≤4-lane diagnostic to 16 lanes. See `experiments/01_sleep_overshoot/sleep_overshoot_note.md` for
the measurement and the cross-engine reading rules (treat HPX/rayx service and
service-dominated total as carrying a stable ~25% sleep-fidelity inflation vs
Ray; emphasize throughput and queue_wait — with the queue_wait boundary caveat —
for control-plane claims).

## 7. Conclusion

Under bimodal variable service, HPX native and rayx provide higher throughput at
low lane counts and lower tail latency throughout the sweep. Ray catches up in
throughput by 16 actors because actor-level parallelism spreads Ray's
per-request overhead, but its p99 tail remains substantially higher. rayx tracks
native HPX closely, supporting the claim that the thin Python frontend preserves
HPX's native execution behavior for native C++ work. This is not a Ray
replacement claim.

## 8. Caveats

* Synthetic blocking sleep only — measures dispatch/coordination/queueing, not
  real compute.
* Local macOS laptop; 10 cores (4 P + 6 E); single machine.
* The three boundaries (`ray-actor-process`, `hpx-intra-locality`,
  `hpx-python-frontend`) genuinely differ; interpret by boundary.
* `queue_wait_ms` is exact only for native HPX; approximate for Ray and rayx
  (and at high lane counts produces large but artifactual gaps — use
  total/throughput).
* rayx runs native C++ work only, not arbitrary Python execution.
* Not Ray Serve; not a distributed-Ray replacement.
* p99/tails are softer than medians (service overshoot was stable, but latency
  tails are noisier).
* Lane threads sleep rather than spin, so 16 lanes on 10 cores did not saturate
  cores; CPU-bound (`spin`) behavior would differ.

## 9. Suggested next directions

* A multi-client-thread driver to push past the ~1390 req/s coordination ceiling
  and locate the true lane-service limit.
* A `work_mode=spin` CPU-bound axis (sleep and spin schedule differently on both
  runtimes).
* Method-style `actor.serve.remote(...)` API polish (closer to Ray's idiom).
* A real native backend behind the lane, later, once the synthetic contract has
  served its purpose.
