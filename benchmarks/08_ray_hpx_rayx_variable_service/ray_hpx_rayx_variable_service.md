# Ray vs HPX-native vs rayx: Variable Service Time

Cross-engine comparison under a shared deterministic bimodal service-time
sequence. Documentation only; it does not change the analyzer, the rayx C++
extension, `hpx_impl/service_lane.hpp`, or the JSONL schema. Companion to
`benchmarks/07_rayx_variable_service/rayx_variable_service.md` (the rayx-only slice that introduced the
workload) and `benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md`.

## 1. Purpose

`benchmarks/07_rayx_variable_service/rayx_variable_service.md` showed bimodal load exposing queueing/tail/lane
behavior within rayx. This slice ports the **identical** deterministic workload
to the Ray actor driver and the native HPX executable, so all three engines
service the **same per-request service-time sequence**, and compares them at
1 and 4 lanes/actors.

Still synthetic blocking sleep, local laptop only, one_by_one dispatch. For
rayx this is native C++ work through the Python frontend, not arbitrary Python
execution, and none of this is a Ray replacement.

## 2. Implementation summary

* Added `--service-pattern {fixed,bimodal}` + `--service-low/-high/-p-high`,
  `--seed` to `bench/run_ray_baseline.py` and
  `hpx_impl/hpx_synthetic_baseline.cpp` (native exe rebuilt). Matches the rayx
  driver's CLI.
* `fixed` behavior is unchanged in both.
* The bimodal service time is a **pure function of `(seed, request_index)`** via
  a splitmix64-style integer hash, implemented identically in Python and C++.
  `(double)z / 2^64` equals Python's `z / 2**64` exactly (division by a power of
  two is exact), so the `draw < p_high` bucket matches across languages.
* JSONL schema stays version `"1"`; `service_ms_requested` is the per-row
  scalar, populated with each request's actual value (recomputed from the
  request index in the native exe to avoid touching the shared `Result`). No new
  fields.
* Workload naming: `bimodal_lo1_hi20_p10_c8` (+ each driver's existing
  lane/actor suffix: `_l{n}` for HPX/rayx, `_a{n}` for Ray).

## 3. Output location

```text
results/variable_service_cross_engine_20260529T222519Z/
```

* 30 JSONL + 30 summary JSON + `aggregate.json` (3 engines × {1,4} lanes ×
  5 repeats).
* 30/30 runs passed.
* 30/30 analyzer summaries passed.

## 4. Experiment shape

* Engines: Ray (`ray-actor-process`), native HPX (`hpx-intra-locality`), rayx
  (`hpx-python-frontend`, `--api engine`).
* Pattern: bimodal, low 1 ms / high 20 ms, `p_high=0.1`, `seed=0`.
* Concurrency 8; `retire_mode one_by_one`; lanes/actors 1 and 4; requests 1000;
  warmup 20; repeats 5.

## 5. Sequence verification

For seed=0 the ordered-by-`request_id` `service_ms_requested` sequence is
**byte-identical across Ray, native HPX, and rayx** — verified at 200 requests
(smoke) and again at 1000 requests in the benchmark. High fraction ≈ 9–10%.
Because routing is round-robin on the same index, the per-lane breakdown at 4
lanes is also identical across engines (representative repeat):

```text
counts     = [250, 250, 250, 250]
high count = [16, 20, 26, 30]
sum_req_ms = [554, 630, 744, 820]
```

So any cross-engine difference below is attributable to the engine/boundary, not
to a different workload.

## 6. Results

Median across 5 repeats (ms; `thru` = req/s). `tot`/`svc`/`qw` =
`total_ms` / `service_ms_observed` / `queue_wait_ms`.

| engine | lanes | thru | tot_p50 | tot_p90 | tot_p99 | svc_p50 | svc_p99 | qw_p50 | qw_p99 | p99/p50 |
|--------|------:|-----:|--------:|--------:|--------:|--------:|--------:|-------:|-------:|--------:|
| ray  | 1 | 167.0 | 49.33 | 71.18 | 90.45 | 1.13 | 21.02 | 36.30 | 86.81 | 1.82 |
| ray  | 4 | 540.0 |  8.93 | 32.99 | 57.49 | 1.13 | 21.02 |  5.89 | 45.46 | 6.41 |
| hpx  | 1 | 272.6 | 30.38 | 56.44 | 79.65 | 1.26 | 25.01 | 16.62 | 75.24 | 2.60 |
| hpx  | 4 | 591.3 |  6.68 | 26.28 | 30.67 | 1.40 | 25.01 |  1.26 | 25.01 | 5.06 |
| rayx | 1 | 300.5 | 29.68 | 55.10 | 78.52 | 1.27 | 25.03 |  8.95 | 76.89 | 2.65 |
| rayx | 4 | 632.1 |  2.58 | 25.46 | 27.27 | 1.26 | 25.02 |  1.31 | 25.14 | 10.63 |

Absolute `total_ms_p99` reduction, 1 → 4 lanes (median):

| engine | 1 lane | 4 lanes | factor |
|--------|-------:|--------:|-------:|
| ray  | 90.45 | 57.49 | 1.57× |
| hpx  | 79.65 | 30.67 | 2.60× |
| rayx | 78.52 | 27.27 | 2.88× |

## 7. Interpretation

* **More lanes reduce queueing and tail latency in every engine, but unevenly.**
  Going 1→4 cut `queue_wait_ms_p50` and `total_ms_p99` across the board. The
  native HPX and rayx tails shrink ~2.6–2.9×; Ray's only ~1.6×.
* **Ray's per-request overhead floor limits its lane scaling.** At 4 lanes Ray's
  `total_ms_p99` stays ~57 ms vs ~28–31 ms for HPX/rayx, and its
  `queue_wait_ms_p99` stays ~45 ms vs ~25 ms. Adding actors parallelizes the
  service *and* Ray's per-call cost (so Ray's throughput scales the most, 167→540,
  3.2×), but the fixed actor-process/IPC/serialization cost is added to every
  request and does not shrink with lanes the way pure queueing does — so its
  absolute tail stays higher.
* **At 1 lane the systems are service-/queue-bound and rank by control
  overhead.** Throughput: Ray 167 vs HPX 273 vs rayx 300 req/s. The single-lane
  ceiling is ~1/mean-service ≈ 290 req/s; HPX and rayx sit near it, Ray well
  below because ~2–3 ms of actor-process overhead is added per request (matching
  the earlier no-op ~3.18 ms Ray floor).
* **Native HPX and rayx track each other closely.** They share the exact same
  C++ service-lane code (`service_lane.hpp`); the remaining gap (e.g. 1-lane
  throughput 273 vs 300) is run-to-run scheduling/overshoot noise plus the
  pybind/GIL crossing, not a different execution path. Do not read rayx as
  "faster than native" — they are the same lane.
* **Where variable service dominates, the engines converge on service.** The
  20 ms requests set the tail for all three; once a request is being serviced,
  the 20 ms sleep dwarfs control overhead, so `service_ms` percentiles are
  similar in magnitude across engines (differences there are sleep overshoot,
  see below — not control cost).
* **Service overshoot vs control overhead are different axes.** `service_ms_p99`
  is ~21 ms for Ray (Python `time.sleep`, ~5% overshoot on 20 ms) but ~25 ms for
  HPX/rayx (`std::this_thread::sleep_for` under HPX worker contention, ~25%
  overshoot). That gap is **backend sleep fidelity**, not control-plane cost —
  and it actually flatters Ray's service numbers. Conversely Ray's higher
  `total_ms`/`queue_wait` is control overhead, not service. Read the two axes
  separately.
* **The rising `p99/p50` ratio with more lanes is a denominator effect.** It
  grows (e.g. rayx 2.65→10.63) only because the median collapses as the queue
  drains; absolute p99 *drops*. Compare absolute p99, not the ratio.

## 8. Caveats

* Synthetic blocking sleep only — measures dispatch/serialization/queueing, not
  real inference.
* **Sleep overshoot differs across engines** (Python `time.sleep` ~5% vs HPX
  `sleep_for` ~25% on 20 ms here); compare `service_ms_observed`, never nominal.
  This was characterized separately — it is a stable, proportional, lane/
  concurrency-invariant sleep-primitive artifact, not control overhead. See
  `experiments/01_sleep_overshoot/sleep_overshoot_note.md` for the measurement and the cross-engine
  reading rules.
* `queue_wait_ms` is **exact only for native HPX** (`hpx-intra-locality`,
  single steady clock); it is **approximate** (`total_ms − service_ms_observed`)
  for Ray and rayx, and for Ray it bundles IPC/serialization, not just queueing.
* The three boundaries (`ray-actor-process`, `hpx-intra-locality`,
  `hpx-python-frontend`) genuinely differ; a shared workload does not make them
  identical-boundary. Interpret by boundary, not as a single verdict.
* Local laptop, single machine, 1000 requests, 5 repeats; bimodal tail placement
  is stochastic per seed.
* Medians are the signal; p99/tails are noisier.
* Not arbitrary Python execution (rayx runs native C++ work); not a Ray
  replacement.

## 9. Suggested next directions

* Wider lane sweep (1/2/4/8/16) to find where each engine's scaling saturates.
* Push the tail harder (higher `p_high`, larger `service_high`, or a third
  mode) to separate queueing from overhead more sharply.
* A busy-spin `work_mode` (CPU-bound) as a separate axis — sleep and spin
  schedule differently on both runtimes.
* Reduce HPX sleep overshoot (investigate lane-thread vs HPX-worker contention)
  so service fidelity matches Ray's, isolating control overhead more cleanly.
* Multi-node / real backend remain explicitly out of scope.
