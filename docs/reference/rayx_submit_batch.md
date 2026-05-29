# rayx `submit_batch()`: Bulk Submit and First Benchmark

Documentation for the `rayx` Python frontend's bulk-submit path and the first
small benchmark comparing it against the windowed `engine`/`actor` paths.
Documentation only; it does not change benchmark code, the analyzer, the C++
extension, or the metrics contract in `docs/experiment_plan.md`. Companion to
`benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md` and `docs/reference/rayx_actor_api.md`.

## 1. Purpose

`Engine.submit_batch()` was added to reduce **Python/FFI per-call overhead** for
hot loops in the `rayx` frontend. Calling `Engine.submit()` in a tight loop
crosses the pybind11/GIL boundary once per request; for ultra-fine-grained or
no-op work that crossing dominates. `submit_batch()` crosses into C++ **once**
and enqueues many requests there.

This is still **native synthetic C++ work only** (blocking-sleep service lanes),
not arbitrary Python remote execution.

## 2. Implementation summary

* API: `Engine.submit_batch(service_ms=0.0, count=1, work_mode="sleep")`.
* One Python→C++ crossing enqueues `count` requests.
* Same round-robin lane routing as `Engine.submit()`.
* Returns one `Future` per request.
* All returned futures share **one** Python-side submit timestamp.
* `SyntheticActor.remote_batch(service_ms, count, work_mode)` now exists as an
  ergonomic façade forwarding to `Engine.submit_batch()` (same bulk semantics);
  a small equivalence check found end-to-end wall-time matches within noise
  (median actor/engine total-time ratios 0.96 no-op, 1.005 sleep5). See
  `docs/reference/rayx_actor_api.md`.
* JSONL schema remains version `"1"`.
* Batch outputs are tagged `retire_mode="batch"`. Windowed paths are tagged
  `retire_mode="one_by_one"` (FIFO) or `retire_mode="batch_wait"` (as-completed,
  see §2a). See `docs/experiment_plan.md` §4 for the documented semantics.

## 2a. `submit_batch` vs windowed as-completed (`Engine.wait`)

These are **different things** and should not be confused:

* **`submit_batch` is bulk submission.** One Python→C++ crossing enqueues all
  requests at once, so there is **no fixed concurrency window** (`--concurrency`
  is inert) and all futures share one `submit_ns`; latency is bulk/queue-shaped
  and throughput is the only meaningful metric.
* **`Engine.wait` / `--retire-mode batch_wait` is windowed as-completed
  retirement.** It **keeps the fixed `--concurrency` window** and only changes
  the *retire discipline*: block until ≥1 future is ready (`Engine.wait`,
  num_returns=1, blocking in HPX with the GIL released), retire up to
  `--wait-batch` ready futures with one shared `recv_ns`, then refill. This — not
  `submit_batch` — is the fair Python-frontend analog of the native `batch_wait`
  path, because it preserves the same in-flight window. See
  [experiments/07_rayx_as_completed/rayx_as_completed.md](../../experiments/07_rayx_as_completed/rayx_as_completed.md).

Driver (`bench/run_hpx_python_baseline.py`, `--api engine`) retire modes:

* `--retire-mode one_by_one` — FIFO windowed retire (default).
* `--retire-mode batch_wait --wait-batch N` — as-completed windowed retire
  (engine, single-client); retires up to `N` ready futures per wait sweep.

## 3. Output location

```text
results/rayx_submit_batch_20260529T214957Z/
```

* 75 JSONL + 75 summary JSON + `aggregate.json`.
* 75/75 runs passed.
* 75/75 analyzer summaries passed.

## 4. Experiment shape

* APIs: `engine`, `actor`, `batch`.
* `service_ms`: `0`, `1`, `5`.
* `repeats`: `5`.
* `num_lanes`: `1`.
* `engine`/`actor`: concurrency `1` and `8`.
* `batch`: no concurrency dimension; all measured requests submitted in one
  batch (`--concurrency` is inert for batch).
* `requests`: `1000` for no-op, `200` for 1 ms and 5 ms.
* `warmup_requests`: `20`.

## 5. Results

Median across 5 repeats, `num_lanes=1`. `tot`/`svc`/`qw` are `total_ms`,
`service_ms_observed`, `queue_wait_ms`.

| api | service_ms | mode | throughput_req_s | total_ms_p50 | total_ms_p99 | service_ms_p50 | queue_wait_ms_p50 |
|-----|-----------:|------|-----------------:|-------------:|-------------:|---------------:|------------------:|
| engine | 0 | c1    |  93772.3 |   0.0080 |    0.0183 | 0.0000 |   0.0080 |
| actor  | 0 | c1    |  97105.8 |   0.0078 |    0.0172 | 0.0000 |   0.0078 |
| engine | 0 | c8    | 255948.2 |   0.0225 |    0.0650 | 0.0000 |   0.0225 |
| actor  | 0 | c8    | 221631.2 |   0.0344 |    0.0698 | 0.0000 |   0.0344 |
| **batch** | 0 | batch | **324293.6** |   2.2416 |    3.0723 | 0.0000 |   2.2416 |
| engine | 1 | c1    |    774.9 |   1.2895 |    1.3086 | 1.2663 |   0.0229 |
| actor  | 1 | c1    |    776.5 |   1.2873 |    1.3100 | 1.2658 |   0.0223 |
| engine | 1 | c8    |    789.8 |  10.1332 |   10.2374 | 1.2659 |   8.8727 |
| actor  | 1 | c8    |    789.7 |  10.1357 |   10.1916 | 1.2653 |   8.8720 |
| **batch** | 1 | batch | **790.0** | 127.1738 |  250.6233 | 1.2657 | 125.9087 |
| engine | 5 | c1    |    162.4 |   6.2957 |    6.3617 | 6.2661 |   0.0356 |
| actor  | 5 | c1    |    162.7 |   6.2960 |    6.3598 | 6.2656 |   0.0362 |
| engine | 5 | c8    |    163.7 |  48.9532 |   50.2359 | 6.2670 |  42.8124 |
| actor  | 5 | c8    |    163.4 |  48.9100 |   50.2173 | 6.2671 |  42.9196 |
| **batch** | 5 | batch | **164.2** | 617.1470 | 1206.7929 | 6.2669 | 610.8791 |

Throughput-only view (median `req/s`), the meaningful batch metric:

| service_ms | engine c1 | actor c1 | engine c8 | actor c8 | **batch** |
|-----------:|----------:|---------:|----------:|---------:|----------:|
| 0 (no-op) | 93,772 | 97,106 | 255,948 | 221,631 | **324,294** |
| 1 | 775 | 777 | 790 | 790 | **790** |
| 5 | 162 | 163 | 164 | 163 | **164** |

## 6. Interpretation

* **No-op: batch clearly improves throughput.** batch ≈ 324k req/s vs engine c8
  ≈ 256k vs engine c1 ≈ 94k. With zero service time the per-request pybind/GIL
  crossing dominates, so folding many submits into one crossing is the win.
* **1 ms: batch is roughly tied with windowed c8** (≈ 790 req/s). The single
  service lane is already at its service ceiling (~1000 / 1.27 ms ≈ 787 req/s),
  so removing submission overhead buys little.
* **5 ms: no meaningful difference** (~163 req/s across all modes). Service time
  dominates.
* **`actor` remains equivalent to `engine`** within noise in every cell.
* **Batch latency is bulk/queue-shaped** and should not be compared directly
  with windowed `one_by_one` latency. The batch `total_ms_p50` values are
  queue-position medians (e.g. 1 ms → ~127 ms ≈ 100th request × 1.27 ms; 5 ms →
  ~617 ms ≈ 100th × 6.27 ms), not per-request service latency.

## 7. Honest conclusion

`submit_batch()` helps when the bottleneck is Python/FFI submission overhead,
especially no-op/hot-loop workloads. Once each request has real service time,
the single HPX service lane becomes the bottleneck and batching adds little.
Batch mode is a throughput tool, not a steady-state latency mode.

## 8. Caveats

* Batch is **not** apples-to-apples latency vs windowed `one_by_one`.
* No-op is client-loop-sensitive; it measures the Python/FFI control path, not
  useful work.
* Native synthetic C++ work only.
* Not arbitrary Python remote execution.
* Not a Ray replacement.
* `retire_mode="batch"` is a **new schema value** (schema version still `"1"`).
* Medians are stronger than p99/tails here; treat tail figures as indicative.

## 9. Suggested next directions

* `SyntheticActor.remote_batch()` façade, if we want API symmetry.
* Variable service-time workload (instead of fixed sleep).
* Streaming / cancellation workload.
* A real native backend.
