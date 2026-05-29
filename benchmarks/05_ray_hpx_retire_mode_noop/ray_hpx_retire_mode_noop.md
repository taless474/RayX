# Ray vs HPX Retire-Mode / No-Op Client-Bottleneck Slice

Results note for the client-retire-mode refinement slice. Documentation only; it
records what was run and what the numbers say. It does not change benchmark
code, the analyzer, or the metrics contract in `docs/experiment_plan.md`. It
follows up a specific artifact from `benchmarks/04_ray_hpx_scaling_comparison/ray_hpx_scaling_comparison.md`.

## 1. Purpose

The multi-lane scaling experiment
(`benchmarks/04_ray_hpx_scaling_comparison/ray_hpx_scaling_comparison.md`) showed HPX no-op throughput **regressed**
as lane count increased (≈1.16M → 0.40M req/s from 1 → 4 lanes), while sleep
workloads scaled near-ideally. The suspected cause was the **single-client
`one_by_one` retire loop** becoming the benchmark bottleneck for ultra-cheap
no-op requests, not an HPX runtime limit.

This slice tests that hypothesis by comparing two client retire modes on the
no-op workload:

* `one_by_one` — retire the oldest in-flight request, submit one (the prior
  default).
* `batch_wait` — block until at least one request is ready, retire up to
  `--wait-batch` ready requests with a single batch timestamp, then refill.

If `batch_wait` changes the no-op curve, the regression is (at least partly) a
client-driver artifact.

## 2. Implementation summary

* HPX baseline (`hpx_impl/hpx_synthetic_baseline.cpp`) gained:
  * `--retire-mode {one_by_one,batch_wait,submit_all_get_all}`
  * `--wait-batch` (default 8)
* `one_by_one` remains the **default** and is behavior-preserving with the
  pre-refactor driver.
* `batch_wait` uses a blocking `hpx::wait_any` (no busy-spin) followed by an
  `is_ready()` sweep of up to `--wait-batch` futures, with a single batch
  `recv_ns`, then round-robin refill.
* `submit_all_get_all` was implemented and smoke-tested (submit all, then
  `hpx::wait_all`) but **not** included in the main experiment — its latency
  percentiles are bulk-completion shaped.
* Ray already had comparable retire modes (`one_by_one`, `batch_wait`,
  `submit_all_get_all`) and `--wait-batch`; the **Ray driver was not changed**.
* JSONL schema stayed version `1` (the `retire_mode` field already existed; the
  baseline now emits the actual value instead of a hardcoded constant).
* Analyzer (`bench/analyze_jsonl.py`) **unchanged**; it parsed all outputs.

## 3. Output location

```text
results/retire_mode_noop_ray_hpx_20260529T180835Z/
```

* 60 per-request JSONL files (2 engines × 3 lane counts × 2 retire modes × 5
  repeats).
* 60 aggregate summary JSON files.
* 60/60 benchmark runs passed.
* 60/60 analyzer summaries passed.

## 4. Experiment shape

* Engines: Ray, HPX.
* `service_ms`: 0 only.
* Actors/lanes: 1, 2, 4.
* `concurrency`: 8.
* Retire modes: `one_by_one`, `batch_wait` (`--wait-batch 8`).
* `repeats`: 5.
* `requests`: 1000 (raised from 200 to reduce no-op noise).
* `warmup_requests`: 20.
* Ray: boundary `ray-actor-process`, `--num-cpus 4`.
* HPX: boundary `hpx-intra-locality`, `--hpx:threads=4`.

## 5. Results (no-op, requests=1000, concurrency=8)

`tput` = throughput req/s `median (min–max)`; `t50` = total_ms p50
`median (min–max)`; `t99` = total_ms p99 median; `×` = throughput scaling vs
that mode's 1-lane median.

### Ray (`ray-actor-process`)

| mode | lanes | tput (min–max) | × | t50 (min–max) | t99 |
|---|---|---|---|---|---|
| one_by_one | 1 | 355.4 (327–365) | 1.00 | 22.197 (21.72–22.68) | 27.51 |
| one_by_one | 2 | 726.7 (719–728) | 2.04 | 11.038 (10.95–11.13) | 12.03 |
| one_by_one | 4 | 1,218.1 (1,173–1,235) | 3.43 | 6.524 (6.45–6.62) | 9.06 |
| batch_wait | 1 | 305.6 (300–324) | 1.00 | 24.516 (22.62–26.30) | 30.71 |
| batch_wait | 2 | 629.8 (592–669) | 2.06 | 12.336 (11.75–13.24) | 14.01 |
| batch_wait | 4 | 992.4 (810–1,062) | 3.25 | 7.454 (7.03–8.07) | 9.03 |

### HPX (`hpx-intra-locality`)

| mode | lanes | tput (min–max) | × | t50 (min–max) | t99 |
|---|---|---|---|---|---|
| one_by_one | 1 | 1,302,719 (867k–1.42M) | 1.00 | 0.0047 (0.0045–0.0084) | 0.0164 |
| one_by_one | 2 | 793,257 (772k–840k) | 0.61 | 0.0099 (0.0090–0.0102) | 0.0184 |
| one_by_one | 4 | 398,751 (391k–427k) | 0.31 | 0.0215 (0.0189–0.0221) | 0.0255 |
| batch_wait | 1 | 893,655 (734k–1.20M) | 1.00 | 0.0064 (0.0050–0.0090) | 0.0211 |
| batch_wait | 2 | 765,307 (729k–867k) | 0.86 | 0.0072 (0.0067–0.0077) | 0.0231 |
| batch_wait | 4 | 417,667 (414k–434k) | 0.47 | 0.0125 (0.0119–0.0127) | 0.0248 |

### Throughput scaling factor (relative to 1 lane, per mode)

| engine | mode | ×(2)/×(1) | ×(4)/×(1) |
|---|---|---|---|
| Ray | one_by_one | 2.04 | 3.43 |
| Ray | batch_wait | 2.06 | 3.25 |
| HPX | one_by_one | 0.61 | 0.31 |
| HPX | batch_wait | 0.86 | 0.47 |

## 6. Interpretation

* **`batch_wait` partially reduces the HPX no-op regression.** The 1→2-lane
  decline shrinks from −39% (one_by_one, 1.30M→793k) to −14% (batch_wait,
  894k→765k), and the 4-lane scaling factor improves from **0.31× to 0.47×**.
* **It does not become positive scaling.** Neither mode exceeds 1.0×: HPX no-op
  throughput still does not rise with lane count. The factor improves partly
  because batch_wait's 1-lane baseline is *lower* (894k vs 1.30M) — batching adds
  `wait_any`+sweep overhead with nothing to amortize at a single lane. In
  absolute terms the modes land close at 2 lanes (765k vs 793k) and 4 lanes
  (418k vs 399k).
* **Ray scaling is mostly unchanged by `batch_wait`** (≈2.0× at 2 actors, ≈3.3–
  3.4× at 4 under both modes; batch_wait marginally lowers absolute throughput).
  Ray's no-op never regressed — its ~3 ms per-request boundary lets actors
  overlap during round trips regardless of retire mode.
* **Therefore the HPX no-op issue is not a service-lane scaling limit.** A
  client-side change shifted the no-op curve without touching the serving
  lanes — the signature of a client-driver bottleneck. The residual flatness is
  the single submitting thread plus cross-thread promise/condvar coordination,
  which batching only partly amortizes.

## 7. Conclusion

HPX no-op multi-lane regression is not evidence that HPX lanes cannot scale. The
sleep workload already showed near-ideal HPX scaling at 5 ms and 20 ms. For
no-op, there is no meaningful service work to parallelize, so the benchmark
becomes dominated by the single client thread and cross-thread coordination.
Batch retirement reduces the artifact but does not eliminate it.

## 8. Caveats

* No-op microbenchmarks are very client-design sensitive — these numbers measure
  the benchmark's client loop as much as either runtime.
* Batch retirement changes latency semantics: batched requests share one
  `recv_ns`, so `total_ms` includes ready-but-not-yet-retired time (applied
  identically on Ray and HPX, but batched latencies are not directly comparable
  to one_by_one latencies).
* Boundaries still differ (`ray-actor-process` vs `hpx-intra-locality`); no
  general "HPX is faster than Ray" claim.
* Service workloads (5/20 ms) were **not** rerun; they already scaled
  near-ideally in the prior slice.
* Medians are stronger than p99/tails; HPX no-op is high-variance even at 1000
  requests (one_by_one L1 ranged 867k–1.42M).
* `submit_all_get_all` was excluded from the main experiment due to its
  bulk-completion latency semantics.

## 9. Suggested next direction

* Stop investing in no-op multi-lane scaling unless we specifically want to
  design a dedicated multi-threaded client — the no-op path is now understood as
  client-bound, and further tuning would measure the harness, not the runtimes.
* Move toward a more realistic serving-control question:
  * a **Python frontend over HPX**, or
  * a variable-service-time / streaming / cancellation serving-control workload.
* Recommended next major direction: a **Python frontend over HPX**, because
  Ray's real advantage is Python ecosystem integration — testing whether HPX can
  be driven from Python is the more decision-relevant comparison than further
  synthetic micro-tuning.
