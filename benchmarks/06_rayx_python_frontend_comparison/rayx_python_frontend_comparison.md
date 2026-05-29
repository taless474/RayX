# Three-Way Comparison: Ray vs HPX Native vs rayx Python Frontend

Results note for the first three-way comparison of execution paths.
Documentation only; it records what was run and what the numbers say. It does
not change benchmark code, the analyzer, or the metrics contract in
`docs/experiment_plan.md`. It builds on the single-lane, matrix, scaling, and
retire-mode notes, and on the `rayx` frontend slices.

## 1. Purpose

First same-session comparison of all three execution paths:

* **Ray actor-process** — `bench/run_ray_baseline.py`.
* **HPX native C++ intra-locality** — `hpx_impl/build/hpx_synthetic_baseline`.
* **rayx Python frontend over HPX** — `bench/run_hpx_python_baseline.py`.

Motivation: Ray's real advantage is Python usability. This experiment tests
whether HPX can expose a Python-facing API (`rayx`) **without losing its native
low-overhead advantage** — i.e., does the Python frontend sit near the HPX
native floor, or collapse toward Ray's actor-process overhead?

## 2. Output location

```text
results/three_way_python_frontend_20260529T210054Z/
```

* 120 per-request JSONL files (3 engines × 4 service times × 2 concurrency × 5
  repeats).
* 120 aggregate summary JSON files.
* 120/120 benchmark runs passed.
* 120/120 analyzer summaries passed.

## 3. Experiment shape

* Engines: Ray, HPX native, rayx Python frontend.
* `service_ms`: 0, 1, 5, 20.
* `concurrency`: 1, 8.
* Lanes/actors: 1.
* `repeats`: 5.
* `warmup_requests`: 20.
* `retire_mode`: one_by_one.
* Request counts: `service_ms=0` → 1000; `service_ms ∈ {1,5,20}` → 200 (same
  count across engines within each `service_ms` cell).
* Ray: `--num-cpus 4`, `--num-actors 1`. HPX native: `--hpx:threads=4`,
  `--num-lanes 1`. rayx: `--hpx-threads 4`, `--num-lanes 1`.

## 4. Boundary framing

Three **different** boundaries — this is **not** an identical-boundary
comparison; gaps partly reflect the boundary, not scheduler quality.

* Ray: `ray-actor-process` — Python + Ray runtime + process + IPC +
  serialization.
* HPX native: `hpx-intra-locality` — pure in-process C++, no FFI.
* rayx: `hpx-python-frontend` — Python submits **native C++ work** to in-process
  HPX lanes via pybind11; no process/IPC/serialization, but a pybind11/GIL
  crossing per submit and result.

rayx submits native C++ synthetic work from Python; it does **not** run
arbitrary Python functions on the lane.

## 5. Results (medians across 5 repeats; ms, throughput in req/s)

`nat` = HPX native; `rayx` = rayx Python frontend.

### service_ms = 0 (no-op, requests=1000)

| metric | Ray c1 | nat c1 | rayx c1 | Ray c8 | nat c8 | rayx c8 |
|---|---|---|---|---|---|---|
| throughput_req_s | 313.2 | 201,725 | 97,671 | 319.6 | 1,083,864 | 265,062 |
| total_ms_p50 | 3.181 | 0.00458 | 0.00788 | 25.01 | 0.00667 | 0.0216 |
| total_ms_p99 | 3.847 | 0.0103 | 0.0188 | 30.07 | 0.0183 | 0.0592 |
| service_ms_p50 | 0.00038 | 0.0 | 0.0 | 0.00042 | 0.0 | 0.0 |
| queue_wait_ms_p50 | 3.180 | 0.00313 | 0.00787 | 25.01 | 0.00388 | 0.0216 |

### service_ms = 1 (requests=200)

| metric | Ray c1 | nat c1 | rayx c1 | Ray c8 | nat c8 | rayx c8 |
|---|---|---|---|---|---|---|
| throughput_req_s | 225.5 | 791.7 | 776.3 | 239.5 | 793.7 | 791.2 |
| total_ms_p50 | 4.410 | 1.270 | 1.288 | 33.57 | 10.13 | 10.13 |
| total_ms_p99 | 5.014 | 1.298 | 1.312 | 36.31 | 10.18 | 10.18 |
| service_ms_p50 | 1.132 | 1.264 | 1.266 | 1.133 | 1.264 | 1.266 |
| queue_wait_ms_p50 | 3.281 | 0.00317 | 0.0223 | 32.43 | 8.863 | 8.871 |

### service_ms = 5 (requests=200)

| metric | Ray c1 | nat c1 | rayx c1 | Ray c8 | nat c8 | rayx c8 |
|---|---|---|---|---|---|---|
| throughput_req_s | 135.2 | 169.2 | 161.7 | 147.8 | 168.6 | 162.7 |
| total_ms_p50 | 7.297 | 5.866 | 6.292 | 53.83 | 47.38 | 49.21 |
| total_ms_p99 | 8.492 | 6.314 | 6.356 | 58.95 | 48.90 | 50.25 |
| service_ms_p50 | 5.639 | 5.859 | 6.265 | 5.639 | 5.892 | 6.266 |
| queue_wait_ms_p50 | 1.712 | 0.00261 | 0.0306 | 48.19 | 41.49 | 43.04 |

### service_ms = 20 (requests=200)

| metric | Ray c1 | nat c1 | rayx c1 | Ray c8 | nat c8 | rayx c8 |
|---|---|---|---|---|---|---|
| throughput_req_s | 42.69 | 43.55 | 42.00 | 44.93 | 43.23 | 41.92 |
| total_ms_p50 | 23.16 | 22.50 | 24.38 | 177.87 | 184.75 | 190.90 |
| total_ms_p99 | 26.95 | 25.08 | 25.12 | 185.89 | 196.84 | 200.17 |
| service_ms_p50 | 21.02 | 22.46 | 24.32 | 21.02 | 22.73 | 24.96 |
| queue_wait_ms_p50 | 2.222 | 0.00706 | 0.0591 | 156.92 | 161.63 | 166.70 |

### Where rayx lands between the HPX-native floor and Ray

Two lenses: total latency vs the control-plane component (`queue_wait`,
non-service time).

| cell | metric | native (floor) | rayx | Ray | rayx position |
|---|---|---|---|---|---|
| s0 c1 | total_ms_p50 | 0.0046 | 0.0079 | 3.18 | ~0.1% toward Ray |
| s1 c1 | total_ms_p50 | 1.270 | 1.288 | 4.41 | ~0.6% toward Ray |
| s0 c1 | queue_wait_p50 | 0.0031 | 0.0079 | 3.18 | ~0.1% toward Ray |
| s1 c1 | queue_wait_p50 | 0.0032 | 0.0223 | 3.28 | ~0.6% toward Ray |

For no-op and sleep1, rayx sits essentially on the HPX-native floor (~0.1–0.6%
of the way toward Ray), not near Ray. On throughput, rayx is ~300× Ray at no-op
c1 (98k vs 313 req/s) and ~830× at c8 (265k vs 320), while ~2–4× below native.

## 6. Interpretation

* **rayx preserves HPX's native control-plane advantage.** The cleanest lens is
  `queue_wait_ms` (non-service overhead): at c1, rayx is tens of microseconds
  (0.008 / 0.022 / 0.031 / 0.059 ms across s0/s1/s5/s20) — at the native floor
  and ~55–400× below Ray's millisecond-scale control overhead (1.7–3.3 ms).
* **For no-op and sleep1, rayx stays near the native floor, not near Ray** — it
  does not collapse toward the actor-process boundary.
* **rayx is much slower than native HPX only on the ultra-hot no-op loop**,
  where pybind11/GIL/client-loop per-call overhead caps throughput at ~2–4×
  below native (still ~300–800× above Ray).
* **For real service time, rayx tracks native throughput closely** (sleep1:
  ~776–791 vs native ~792–794; ~3.3× Ray). The per-call cost is amortized once
  each request does real work.
* **At sleep20 all three converge** — the control-plane boundary is a negligible
  fraction of a 20 ms request; throughput approaches the single-lane ceiling
  (~42–45 req/s).
* **`total_ms` is confounded at sleep5/sleep20 by service sleep overshoot.**
  `service_ms_observed` differs by engine (Ray ~21.0/5.64/1.13 vs native
  ~22.5/5.86/1.26 vs rayx ~24.3/6.27/1.27): the HPX lanes' blocking
  `std::this_thread::sleep_for` overshoots more than Ray's `time.sleep`, and
  rayx overshoots ~0.4 ms more than native. So rayx's higher `total_ms` at
  sleep5 (and exceeding Ray at sleep20 c1) is mostly **backend sleep
  granularity, not control-plane cost** — `queue_wait` (near zero for rayx) is
  the better control-overhead lens.

## 7. Conclusion

The rayx Python frontend preserves most of HPX's low control-plane overhead for
native C++ work. It gives a Python-facing API without paying Ray's
actor-process / IPC / serialization cost. This supports the project direction:
HPX should not be framed as a wholesale Ray replacement, but as a low-overhead
native execution substrate that can be exposed through a thin Python frontend
for fine-grained native workloads.

## 8. Caveats

* Different boundaries: `ray-actor-process` vs `hpx-intra-locality` vs
  `hpx-python-frontend` — not an identical-boundary comparison.
* rayx is **not** a Ray replacement: no object store, distributed scheduler,
  fault tolerance, autoscaling, or Ray Serve ecosystem.
* rayx submits **native C++ work, not arbitrary Python functions** — a real
  Python workload would add the cost of executing that Python on the lane.
* `queue_wait_ms` differs by boundary: exact for HPX native (one steady_clock);
  approximate for Ray and rayx (client vs server clock domains). `total_ms` is
  the authoritative cross-engine latency.
* Service sleep overshoot differs by engine (HPX `sleep_for` > Ray
  `time.sleep`), confounding `total_ms` at larger `service_ms`.
* No-op is client-loop-sensitive (single client thread, one_by_one); no-op
  throughput reflects client design as much as the runtime.
* Medians are stronger than p99/tails; 200-request service cells are modest
  samples.

## 9. Suggested next directions

* Design a small Ray-like actor API over rayx (named handles, method calls)
  rather than a single `submit(service_ms)`.
* Add a batch-submit path to reduce Python per-call overhead on hot loops.
* Add variable service time / streaming / cancellation serving-control
  workloads.
* Later: test a real native backend behind the lane (once the synthetic
  contract is stable), per the project's staged plan.
