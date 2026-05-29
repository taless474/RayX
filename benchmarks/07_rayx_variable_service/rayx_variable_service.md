# rayx Variable Service Time: Bimodal Queueing Behavior

Documentation for the rayx Python driver's deterministic variable-service-time
workload and the first small benchmark using it. Documentation only; it does
not change the analyzer, the C++ extension, `hpx_impl/service_lane.hpp`, the Ray
driver, the native HPX executable, or the metrics contract beyond the
`service_pattern` note in `docs/experiment_plan.md` §3. Companion to
`docs/reference/rayx_submit_batch.md` and `docs/reference/rayx_actor_api.md`.

## 1. Purpose

Fixed `service_ms` is good for isolating control-path overhead, but real serving
workloads have **variable request durations**. A bimodal mix (mostly short, a
few long) exposes what fixed sleep cannot: queueing build-up, tail latency, lane
imbalance, and whether adding lanes actually helps under non-uniform work.

This is still **native synthetic C++ sleep work only**, local laptop only,
driven through the rayx Python frontend — not model compute, not arbitrary
Python remote execution, not a Ray replacement.

## 2. Implementation summary

* New CLI on `bench/run_hpx_python_baseline.py`: `--service-pattern
  {fixed,bimodal}` (default `fixed`), `--service-low` (1.0), `--service-high`
  (20.0), `--service-p-high` (0.1), `--seed` (0).
* `fixed` is unchanged: every request uses `--service-ms`.
* `bimodal`: each request's service time is a **pure function of
  `(seed, request_index)`** via a portable splitmix64-style integer hash; if the
  draw in `[0,1)` is `< p_high` use `service_high`, else `service_low`. No
  stateful RNG.
* Same seed reproduces the same sequence; different seed differs. The hash is
  plain integer arithmetic, so the sequence is engine-independent (lets Ray and
  native HPX reproduce it later). It is decorrelated from round-robin lane
  assignment (`lane = idx % num_lanes`).
* JSONL schema stays version `"1"`. `service_ms_requested` remains a scalar, now
  populated with each request's actual requested value. No new fields.
* `bimodal` is `one_by_one`-only (engine/actor); combining it with `--api
  batch`/`actor_batch` is rejected (the batch API takes one `service_ms` for the
  whole batch).

## 3. Output location

```text
results/rayx_variable_service_20260529T221301Z/
```

* 10 JSONL + 10 summary JSON + `aggregate.json`.
* 10/10 runs passed.
* 10/10 analyzer summaries passed.

## 4. Experiment shape

* Engine/API: rayx `--api engine` (`one_by_one`).
* Pattern: bimodal, low 1 ms / high 20 ms, `p_high=0.1`, `seed=0`.
* Concurrency: 8.
* Lanes: **1** and **4**.
* Requests: 1000; warmup: 20; repeats: 5.
* = 2 cells × 5 repeats = 10 runs.

## 5. Results

Median across 5 repeats (ms unless noted). `tot`/`svc`/`qw` =
`total_ms`, `service_ms_observed`, `queue_wait_ms`.

| lanes | thru_req_s | tot_p50 | tot_p90 | tot_p99 | svc_p50 | svc_p90 | svc_p99 | qw_p50 | qw_p90 | qw_p99 |
|------:|-----------:|--------:|--------:|--------:|--------:|--------:|--------:|-------:|-------:|-------:|
| 1 | 304.8 | 29.583 | 53.939 | 78.198 | 1.266 | 1.291 | 25.024 | 8.932 | 51.280 | 74.953 |
| 4 | 633.8 |  2.572 | 25.115 | 28.439 | 1.261 | 1.283 | 25.022 | 1.316 | 23.793 | 25.103 |

Tail amplification (median across repeats):

| lanes | total p99/p50 | queue_wait p99/p50 |
|------:|--------------:|-------------------:|
| 1 | 2.66 | 8.39 |
| 4 | 11.09 | 19.09 |

Per-lane distribution:

| config | per-lane request count [min..max] | per-lane high-req count [min..max] | per-lane summed requested ms [min..max] |
|--------|----------------------------------:|-----------------------------------:|----------------------------------------:|
| 1 lane | 1000 .. 1000 | 92 .. 92 | 2748 .. 2748 |
| 4 lanes | 250 .. 250 | 16 .. 30 | 554 .. 820 |

Representative 4-lane run (repeat 1):

```text
lane0: n=250 high=30 sum_req_ms=820
lane1: n=250 high=26 sum_req_ms=744
lane2: n=250 high=20 sum_req_ms=630
lane3: n=250 high=16 sum_req_ms=554
```

## 6. Interpretation

* **4 lanes sharply reduce queueing and absolute latency.** Going 1→4 lanes cut
  `queue_wait_ms_p50` 8.93→1.32 ms, `queue_wait_ms_p99` 75.0→25.1 ms,
  `total_ms_p50` 29.6→2.57 ms, and `total_ms_p99` 78.2→28.4 ms, while throughput
  rose 304.8→633.8 req/s (~2.08×). Under bimodal load on a single lane, the
  occasional 20 ms request blocks the queue and everything behind it waits —
  visible in the 1-lane `total_ms_p50` (29.6 ms) sitting far above the 1 ms
  service median.
* **Why throughput is ~2.08×, not 4×.** `--concurrency 8` caps in-flight work,
  so 4 lanes carry ~2 requests each rather than saturating; the gain is bounded
  by concurrency and mean service (~2.75 ms/request here), not lane count alone.
* **The rising tail-amplification *ratio* at 4 lanes is a denominator effect,
  not worse tails.** `total p99/p50` goes 2.66→11.09, but that is because the
  *median* collapsed (queue mostly drained), not because the tail grew — the
  absolute `total_ms_p99` actually dropped 78.2→28.4 ms. With 4 lanes a typical
  request barely queues (p50 ≈ 2.6 ms ≈ one short service), while the unavoidable
  20 ms requests still set the p99, so the ratio looks large. Read absolute p99,
  not the ratio, when comparing lane counts.
* **Round-robin balances count exactly but not work.** All lanes got exactly 250
  requests, but the long (20 ms) requests landed unevenly: high-request counts
  ranged 16..30 and summed requested service 554..820 ms across the four lanes
  (~1.5× spread). Which submission slots are "high" is random relative to
  `idx % 4`, so per-lane *work* imbalance persists at this scale; it would even
  out with many more requests (law of large numbers).
* **This measures queueing/tail behavior, not model compute.** The backend only
  sleeps; results reflect dispatch + serialization + lane occupancy, not CPU,
  memory, or kernel effects.

## 7. Caveats

* Synthetic blocking `sleep` only; exposes queueing, not real inference cost.
* **Sleep overshoot:** observed `service_ms_p99` ≈ 25 ms for 20 ms-requested
  work (~25% overshoot under thread contention). Compare `service_ms_observed`,
  not the nominal value; overshoot differs by engine and load.
* `queue_wait_ms` here is approximate (`total_ms − service_ms_observed`, Python
  and C++ clocks differ), like Ray — not the exact same-process value the native
  HPX `hpx-intra-locality` path reports.
* Single-engine (rayx) only; no cross-engine claim. Boundaries still differ.
* Bimodal tail placement is stochastic per seed; medians over 5 repeats are the
  signal, single-run p99 is noisy.
* Local laptop only; native C++ work through rayx, not arbitrary Python.

## 8. Next directions

* Cross-engine variable service (Slice B): add the identical splitmix64 hash to
  the Ray driver and the native HPX executable (rebuild), verify the three emit
  identical per-index sequences, then compare under bimodal load.
* Larger lane sweep (1/2/4/8) and a higher `p_high` or larger `service_high` to
  push the tail harder.
* Lognormal or multi-modal service distributions.
* Variable service combined with batch dispatch (needs a per-request batch API
  in the C++ extension; out of current scope).
