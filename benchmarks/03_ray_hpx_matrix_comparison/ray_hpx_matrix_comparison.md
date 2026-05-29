# Ray vs HPX Synthetic Matrix Comparison

Results note for the widened same-session Ray-vs-HPX synthetic matrix.
Documentation only; it records what was run and what the numbers say. It does
not change benchmark code, the analyzer, or the metrics contract in
`docs/experiment_plan.md`. It builds on the first single-lane comparison in
`benchmarks/02_ray_hpx_single_lane_comparison/ray_hpx_single_lane_comparison.md` by widening the service-time and
concurrency axes.

## 1. Purpose

* Widen the single-lane Ray-vs-HPX comparison across a full service-time ×
  concurrency grid, in one machine/session.
* One serialized Ray actor vs one serialized HPX service lane.
* Synthetic backend only: no-op (`service_ms=0`) and blocking `sleep`.
* 5 repeats per cell; compare medians, treat tails as soft.

## 2. Output location

```text
results/compare_ray_hpx_matrix_20260529T171149Z/
```

* 160 per-request JSONL files (2 engines × 4 service times × 4 concurrency
  levels × 5 repeats).
* 160 aggregate summary JSON files (one analyzer summary per JSONL).
* 160/160 benchmark runs passed.
* 160/160 analyzer summaries passed.

## 3. Matrix shape

* Engines: Ray, HPX.
* `service_ms`: 0, 1, 5, 20.
* `concurrency`: 1, 4, 8, 16.
* `requests`: 200.
* `warmup_requests`: 20.
* `repeats`: 5.
* `retire_mode`: one_by_one.
* Ray: boundary `ray-actor-process`, `--num-cpus 4`.
* HPX: boundary `hpx-intra-locality`, `--hpx:threads=4`.

## 4. Boundary framing

The two engines do **not** cross the same boundary, so this is **not** an
identical-boundary comparison.

* Ray (`ray-actor-process`): each actor call crosses Python, the Ray runtime,
  the process boundary, IPC, and serialization.
* HPX (`hpx-intra-locality`): same process, in-process futures, no
  serialization, no IPC, no process boundary; C++ on the HPX local runtime.

A gap between the two may reflect this boundary difference rather than any
difference in scheduling or control-plane design. There is **no** general claim
that HPX is faster than Ray.

## 5. Results

Median / min–max across the 5 repeats per cell. Latency in ms, throughput in
req/s. Headline metrics (`tput`, `t50`) show `median (min–max)`; secondary
metrics (`t99`, `qw50` = queue_wait p50, `svc50` = service p50) show medians.

### service_ms = 0

| conc | Ray tput (min–max) | HPX tput (min–max) | Ray t50 (min–max) | HPX t50 (min–max) | Ray t99 | HPX t99 | Ray qw50 | HPX qw50 | Ray svc50 | HPX svc50 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 287.4 (272.5–291.0) | 186,698 (160,691–207,227) | 3.403 (3.392–3.707) | 0.0046 (0.0045–0.0049) | 4.249 | 0.0103 | 3.403 | 0.0032 | 0.0004 | 0.0000 |
| 4 | 306.6 (291.7–324.2) | 1,062,654 (748,131–1,110,340) | 12.900 (12.153–13.650) | 0.0023 (0.0022–0.0054) | 15.176 | 0.0094 | 12.899 | 0.0013 | 0.0004 | 0.0000 |
| 8 | 305.0 (294.7–320.4) | 1,045,522 (989,893–1,221,993) | 26.427 (24.766–26.936) | 0.0061 (0.0044–0.0072) | 30.297 | 0.0159 | 26.427 | 0.0028 | 0.0005 | 0.0000 |
| 16 | 306.9 (291.0–321.4) | 1,137,708 (995,847–1,196,709) | 49.996 (48.388–55.572) | 0.0127 (0.0114–0.0152) | 59.449 | 0.0238 | 49.995 | 0.0092 | 0.0005 | 0.0000 |

### service_ms = 1

| conc | Ray tput (min–max) | HPX tput (min–max) | Ray t50 (min–max) | HPX t50 (min–max) | Ray t99 | HPX t99 | Ray qw50 | HPX qw50 | Ray svc50 | HPX svc50 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 216.3 (212.7–216.7) | 793.5 (791.1–795.3) | 4.628 (4.547–4.703) | 1.269 (1.268–1.271) | 5.480 | 1.294 | 3.494 | 0.0030 | 1.133 | 1.263 |
| 4 | 221.5 (219.9–224.3) | 795.6 (794.2–796.5) | 18.023 (17.768–18.254) | 5.065 (5.054–5.068) | 20.141 | 5.093 | 16.879 | 3.799 | 1.135 | 1.264 |
| 8 | 221.3 (215.8–228.2) | 793.9 (792.5–794.6) | 35.847 (34.864–37.113) | 10.120 (10.104–10.126) | 39.574 | 10.186 | 34.709 | 8.857 | 1.134 | 1.263 |
| 16 | 224.5 (220.9–227.4) | 792.3 (748.0–797.2) | 71.521 (70.211–72.530) | 20.176 (20.055–20.246) | 79.498 | 20.336 | 70.384 | 18.919 | 1.134 | 1.264 |

### service_ms = 5

| conc | Ray tput (min–max) | HPX tput (min–max) | Ray t50 (min–max) | HPX t50 (min–max) | Ray t99 | HPX t99 | Ray qw50 | HPX qw50 | Ray svc50 | HPX svc50 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 129.3 (128.6–131.1) | 169.3 (167.6–171.4) | 7.605 (7.463–7.703) | 5.863 (5.731–5.945) | 8.866 | 6.293 | 1.998 | 0.0025 | 5.639 | 5.858 |
| 4 | 138.6 (136.7–140.6) | 168.5 (167.9–169.5) | 28.745 (28.401–29.044) | 23.655 (23.556–23.806) | 32.336 | 24.655 | 23.115 | 17.740 | 5.639 | 5.876 |
| 8 | 139.3 (136.9–141.0) | 168.3 (168.0–169.3) | 57.602 (56.704–58.371) | 47.488 (47.116–47.573) | 61.301 | 49.001 | 51.956 | 41.549 | 5.638 | 5.897 |
| 16 | 140.5 (139.7–143.0) | 169.4 (167.6–169.5) | 113.735 (111.448–114.761) | 94.319 (94.224–95.295) | 119.618 | 96.674 | 108.099 | 88.413 | 5.638 | 5.856 |

### service_ms = 20

| conc | Ray tput (min–max) | HPX tput (min–max) | Ray t50 (min–max) | HPX t50 (min–max) | Ray t99 | HPX t99 | Ray qw50 | HPX qw50 | Ray svc50 | HPX svc50 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 40.6 (39.9–40.7) | 43.4 (43.3–43.6) | 23.897 (23.747–24.191) | 22.529 (22.466–22.635) | 28.670 | 25.072 | 2.901 | 0.0061 | 21.023 | 22.512 |
| 4 | 44.0 (43.5–44.1) | 43.3 (43.0–43.4) | 90.444 (90.140–90.665) | 92.578 (92.045–93.068) | 99.164 | 99.250 | 69.491 | 69.276 | 21.027 | 22.601 |
| 8 | 44.1 (43.9–44.1) | 43.3 (43.1–43.4) | 181.049 (180.876–182.251) | 184.489 (183.477–185.962) | 191.100 | 191.821 | 160.287 | 161.619 | 21.028 | 22.680 |
| 16 | 43.9 (43.8–44.1) | 43.3 (43.1–43.4) | 363.890 (361.884–365.490) | 368.889 (366.595–371.149) | 375.787 | 382.151 | 342.907 | 345.665 | 21.029 | 22.684 |

## 6. Interpretation

* **service_ms=0** — Ray actor-process overhead dominates. The Ray lane
  completes ~290–307 req/s regardless of concurrency, set entirely by
  per-request boundary cost (~3.3 ms of non-service time; observed service ≈ 0).
  HPX is microsecond-scale (~0.18–1.1M req/s).

* **service_ms=1** — Ray's boundary still dominates: its lane runs ~220 req/s
  while HPX runs ~793 req/s (≈3.6×). HPX mostly tracks service time — its lane
  time is just the ~1.26 ms service, whereas Ray pays service **plus** the
  boundary.

* **service_ms=5** — transition zone. HPX still leads (~169 vs ~140 req/s,
  ≈1.2×), but the gap is far smaller than in the no-op/`sleep1` regime because
  service time is now a meaningful fraction of total latency.

* **service_ms=20** — service-dominated. Throughput collapses to the
  single-lane ceiling on both (~43–44 req/s) and `total_ms`/`queue_wait` track
  each other within ~1–2%. HPX can be slightly **slower** here (e.g. c8 total
  p50 184.5 vs 181.0 ms) purely because its blocking `sleep_for` overshoots the
  requested service time a bit more (observed ~22.6 ms vs Ray's ~21.0 ms) —
  backend sleep granularity, not control-plane cost.

* **concurrency** — for one serialized lane, throughput stays mostly **flat**
  across concurrency (1→16) at every service level; adding in-flight requests
  does not raise completion rate. Instead `total_ms` and `queue_wait_ms` grow
  roughly **linearly** with in-flight depth (single-server queueing:
  `total_ms ≈ concurrency / throughput`). Both engines show the same shape,
  confirming the single-lane serialization model holds on each side.

## 7. Conclusion

This matrix shows that HPX's intra-locality C++ control path is far cheaper for
tiny requests, while Ray's actor-process boundary is visible when service time
is small. As service time grows, the single serialized service lane dominates
and the runtimes converge. The result supports HPX as a low-overhead native
serving-control substrate for fine-grained local control paths, not a blanket
claim that HPX is generally faster than Ray.

## 8. Caveats

* Different boundaries: `ray-actor-process` vs `hpx-intra-locality` — not an
  identical-boundary comparison.
* `queue_wait_ms` is defined differently per engine: exact same-process
  submit→start for HPX; approximate (`total_ms − service_ms_observed`, all
  non-service time) for Ray across the process boundary. `total_ms` is the
  authoritative client-side latency on both.
* Medians are stronger than p99/tails; single-repeat tails at 200 requests are
  noisy and should not drive conclusions.
* Synthetic `sleep` backend only — models I/O-bound / GPU-offloaded serving, not
  compute-bound work.
* One actor / one serialized lane only.
* No distributed HPX (single locality, intra-process).
* No Ray Serve.
* No real model inference.
* No streaming / cancellation.

## 9. Suggested next experiments

* Multi-actor Ray vs multi-lane HPX scaling (move past one serialized lane).
* Later: a real model backend, once the synthetic contract is stable.
* Later: streaming and cancellation.
