# Ray vs HPX Multi-Lane Scaling Comparison

Results note for the first multi-actor Ray vs multi-lane HPX scaling
experiment. Documentation only; it records what was run and what the numbers
say. It does not change benchmark code, the analyzer, or the metrics contract
in `docs/experiment_plan.md`. It builds on the single-lane and matrix notes
(`benchmarks/02_ray_hpx_single_lane_comparison/ray_hpx_single_lane_comparison.md`,
`benchmarks/03_ray_hpx_matrix_comparison/ray_hpx_matrix_comparison.md`) by adding a lane-count axis.

## 1. Purpose

* First scaling experiment: move beyond one serialized lane and test whether
  throughput rises as independent serving lanes are added.
* Ray: N independent actors, each serializing its own requests.
* HPX: N independent service lanes, each serializing its own requests.
* Routing: deterministic round-robin by submission order (request *k* → lane
  `k % N`).
* Synthetic backend only: no-op (`service_ms=0`) and blocking `sleep`.

## 2. Output location

```text
results/scale_ray_hpx_20260529T173252Z/
```

* 90 per-request JSONL files (2 engines × 3 lane counts × 3 service times × 5
  repeats).
* 90 aggregate summary JSON files (one analyzer summary per JSONL).
* 90/90 benchmark runs passed.
* 90/90 analyzer summaries passed.

## 3. Experiment shape

* Engines: Ray, HPX.
* Lanes/actors: 1, 2, 4.
* `service_ms`: 0, 5, 20.
* `concurrency`: 8.
* `repeats`: 5.
* `requests`: 200.
* `warmup_requests`: 20.
* `retire_mode`: one_by_one.
* Ray: boundary `ray-actor-process`, `--num-cpus 4`.
* HPX: boundary `hpx-intra-locality`, `--hpx:threads=4`.

## 4. Implementation notes

* Ray driver (`bench/run_ray_baseline.py`) gained `--num-actors` (default 1);
  it creates N actors and round-robins submissions across them.
* HPX baseline (`hpx_impl/hpx_synthetic_baseline.cpp`) gained `--num-lanes`
  (default 1); it creates N independent serialized lanes (each owns a
  `std::thread`) and round-robins submissions across them.
* Requests are routed round-robin by submission order; with 200 requests this
  splits evenly (200 / 100 / 50 per lane at 1 / 2 / 4 lanes). Even split was
  verified in a 2-lane smoke (10/10 of 20 requests on both engines).
* JSONL schema stayed version `1`; no fields added or changed.
* `actor_id` identifies the Ray actor / HPX lane that served each row; lane
  count is recoverable as the number of distinct `actor_id`s.
* The analyzer (`bench/analyze_jsonl.py`) did **not** change — one summary per
  file is the aggregate throughput/latency across all lanes, which is the
  headline scaling metric.
* Default `--num-actors 1` / `--num-lanes 1` reproduce the prior single-lane
  behavior exactly.

## 5. Results

Median / min–max across the 5 repeats per cell. Latency in ms, throughput in
req/s. `tput` and `t50` show `median (min–max)`; `t99`, `qw50` (queue_wait p50),
`svc50` (service p50) show medians. `×` is throughput scaling vs 1 lane.

### service_ms = 0 (no-op)

| eng | lanes | tput (min–max) | × | t50 (min–max) | t99 | qw50 | svc50 |
|---|---|---|---|---|---|---|---|
| Ray | 1 | 353.0 (337–363) | 1.00 | 22.819 (22.01–23.45) | 26.68 | 22.818 | 0.0004 |
| Ray | 2 | 651.6 (645–764) | 1.85 | 12.086 (10.29–12.45) | 13.87 | 12.086 | 0.0005 |
| Ray | 4 | 861.4 (759–987) | 2.44 | 8.850 (7.84–9.84) | 15.24 | 8.849 | 0.0008 |
| HPX | 1 | 1,162,223 (1.01M–1.25M) | 1.00 | 0.0054 (0.0048–0.0065) | 0.0137 | 0.0031 | 0.0000 |
| HPX | 2 | 819,112 (733k–898k) | 0.70 | 0.0096 (0.0081–0.0105) | 0.0169 | 0.0047 | 0.0000 |
| HPX | 4 | 400,433 (369k–566k) | 0.34 | 0.0206 (0.0137–0.0229) | 0.0249 | 0.0057 | 0.0000 |

### service_ms = 5

| eng | lanes | tput (min–max) | × | t50 (min–max) | t99 | qw50 | svc50 |
|---|---|---|---|---|---|---|---|
| Ray | 1 | 155.2 (150–157) | 1.00 | 51.062 (50.60–52.33) | 54.20 | 45.442 | 5.635 |
| Ray | 2 | 232.2 (212–235) | 1.50 | 34.312 (32.97–36.15) | 71.75 | 28.833 | 5.637 |
| Ray | 4 | 408.8 (341–446) | 2.63 | 18.183 (17.47–18.66) | 29.58 | 12.608 | 5.638 |
| HPX | 1 | 164.2 (162–165) | 1.00 | 48.848 (48.51–48.90) | 50.15 | 42.681 | 6.260 |
| HPX | 2 | 328.5 (324–331) | 2.00 | 24.275 (24.05–24.69) | 25.08 | 17.790 | 6.258 |
| HPX | 4 | 654.0 (652–663) | 3.98 | 12.449 (12.07–12.49) | 12.55 | 5.466 | 6.258 |

### service_ms = 20

| eng | lanes | tput (min–max) | × | t50 (min–max) | t99 | qw50 | svc50 |
|---|---|---|---|---|---|---|---|
| Ray | 1 | 45.31 (45.0–45.8) | 1.00 | 176.443 (175.0–177.6) | 178.77 | 155.570 | 21.015 |
| Ray | 2 | 90.59 (90.3–90.9) | 2.00 | 87.996 (87.3–88.6) | 90.89 | 67.056 | 21.015 |
| Ray | 4 | 180.84 (172–182) | 3.99 | 43.534 (43.3–43.8) | 55.39 | 22.682 | 21.009 |
| HPX | 1 | 42.85 (42.3–43.1) | 1.00 | 186.191 (185.0–189.4) | 197.75 | 162.988 | 23.574 |
| HPX | 2 | 85.24 (84.8–86.0) | 1.99 | 93.267 (93.1–93.9) | 99.13 | 69.542 | 23.537 |
| HPX | 4 | 169.80 (168–171) | 3.96 | 47.329 (47.1–48.1) | 50.06 | 21.963 | 23.688 |

### Throughput scaling factor (relative to 1 lane)

| engine | service_ms | tput(2)/tput(1) | tput(4)/tput(1) |
|---|---|---|---|
| Ray | 0 | 1.85 | 2.44 |
| Ray | 5 | 1.50 | 2.63 |
| Ray | 20 | 2.00 | 3.99 |
| HPX | 0 | 0.70 | 0.34 |
| HPX | 5 | 2.00 | 3.98 |
| HPX | 20 | 1.99 | 3.96 |

## 6. Interpretation

* **No-op (`service_ms=0`)** — Ray scales **sublinearly** (1.85× at 2, 2.44× at
  4): its ~3 ms per-request boundary is large enough that multiple actors work
  in parallel during those round trips, partly hiding the single-threaded client
  retire loop. HPX **regresses** (1.16M → 0.40M req/s; 0.70×, 0.34×) because the
  HPX path is so cheap there is no ms-scale work to overlap — the single client
  thread doing `future.get()` + submit one-by-one is essentially the whole cost,
  and adding lanes only adds cross-thread wakeups and cache-line contention.
  This HPX no-op regression is a **benchmark-driver bottleneck**, not a runtime
  scaling limit.

* **5 ms** — HPX scales **near-ideally** (2.00× at 2, 3.98× at 4). Ray scales
  but **sublinearly** (1.50×, 2.63×): at the higher completion rate (~409 req/s
  at 4 lanes) the single client retire loop plus the per-request boundary become
  visible again, and 4 actors exactly saturate `--num-cpus 4` alongside the
  driver.

* **20 ms** — **both** scale near-ideally (Ray 2.00× / 3.99×; HPX 1.99× /
  3.96×). Service time dominates, the completion rate is low enough that neither
  the client loop nor CPU saturation binds, and the boundary overhead is a small
  fraction of total latency. Absolute throughput converges (Ray 181 vs HPX 170
  req/s at 4 lanes).

* **Latency** — `total_ms` and `queue_wait_ms` drop as requests spread across
  more actors/lanes (e.g. Ray sleep20 total p50 176 → 88 → 43.5 ms; HPX sleep5
  total p50 48.8 → 24.3 → 12.4 ms), while `service_ms` stays flat — the expected
  behavior of adding independent serialized servers. The one exception is HPX
  no-op, where latency rises because throughput regressed.

## 7. Conclusion

For sleep workloads with real per-request service time, independent Ray actors
and HPX lanes both increase throughput as expected. HPX scales nearly ideally at
5 ms and 20 ms; Ray scales nearly ideally at 20 ms but is sublinear at 5 ms. For
no-op workloads, the HPX path is so cheap that the single client one-by-one
retire loop becomes the benchmark bottleneck, causing apparent negative scaling.
This does not establish an HPX runtime scaling limit; it identifies the next
driver limitation to fix if no-op scaling is the target.

## 8. Caveats

* Different boundaries: Ray `ray-actor-process` vs HPX `hpx-intra-locality` —
  not an identical-boundary comparison; no general "HPX is faster than Ray"
  claim.
* Synthetic blocking `sleep` only — models I/O-bound / GPU-offloaded serving,
  not compute-bound work or real inference.
* Local only; single locality; no distributed HPX.
* `--num-cpus 4` gates Ray actors (1 CPU/actor by default), so 4 actors exactly
  fill it and contend with the driver — a likely contributor to Ray's
  sub-ideal 4-actor sleep5 result.
* `--hpx:threads=4` does **not** gate the HPX service lanes: lanes are plain
  `std::thread`s with blocking sleep, independent of the HPX worker pool. For
  sleep workloads sleeping threads release their core, which is why HPX scales
  near-ideally to 4 lanes.
* The one-by-one single-client retire loop caps no-op scaling and makes HPX
  no-op regress with more lanes (a driver artifact).
* p99/tails are softer than medians (e.g. Ray sleep5 L2 single-repeat p99 spiked
  to ~71.7 ms vs ~54 ms at L1).
* Per-lane sample counts are thin at 4 lanes (~50 requests per lane).

## 9. Suggested next experiments

* A batched or multi-threaded client retire mode to probe HPX no-op scaling
  without the single-client-loop bottleneck.
* concurrency=16 scaling.
* lanes=8.
* Later: a real model backend, once the synthetic contract is stable.
* Later: streaming and cancellation.
