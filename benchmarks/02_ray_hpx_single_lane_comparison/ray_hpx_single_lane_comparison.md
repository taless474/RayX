# Ray vs HPX Single-Lane Comparison (first same-session run)

Results note for the first same-session Ray-vs-HPX single-lane synthetic
comparison. Documentation only; it records what was run and what the numbers
say. It does not change benchmark code, the analyzer, or the metrics contract
in `docs/experiment_plan.md`.

## 1. Purpose

* First same-session comparison of the Ray actor single-lane baseline against
  the HPX intra-locality single-lane baseline, on the same machine/session.
* Synthetic backend only (`work_mode = sleep`), one serialized service lane on
  each side.
* 5 repeats per cell; compare medians, treat tails as soft.
* Same four selected cells used in the Ray variance experiment:
  * `noop_c1`   — service_ms=0, concurrency=1
  * `noop_c8`   — service_ms=0, concurrency=8
  * `sleep5_c8` — service_ms=5, concurrency=8
  * `sleep20_c8`— service_ms=20, concurrency=8
* Fixed per run: `requests=200`, `warmup_requests=20`,
  `retire_mode=one_by_one`, Ray `--num-cpus 4`, HPX `--hpx:threads=4`.

## 2. Output location

```text
results/compare_ray_hpx_single_lane_20260529T165700Z/
```

* 40 per-request JSONL files (2 engines × 4 cells × 5 repeats).
* 40 aggregate summary JSON files (one analyzer summary per JSONL).
* 40/40 benchmark runs passed.
* 40/40 analyzer summaries passed.

## 3. Boundary framing

The two engines do **not** cross the same boundary, so this is **not** an
identical-boundary comparison.

* Ray boundary: `ray-actor-process` — each actor call crosses Python, the Ray
  runtime, the process boundary, IPC, and serialization.
* HPX boundary: `hpx-intra-locality` — same process, in-process futures, no
  serialization, no IPC, no process boundary; C++ on the HPX local runtime.

A gap between the two may reflect this boundary difference rather than any
difference in scheduling or control-plane design. Every comparison below should
be read with that in mind.

## 4. Aggregate results

Median / min / max across the 5 repeats per cell. Latency in ms, throughput in
req/s. `service_ms_p50` ranges are omitted where the spread is negligible.

### noop_c1 (service_ms=0, concurrency=1)

| metric | Ray median | Ray min–max | HPX median | HPX min–max |
|---|---|---|---|---|
| throughput_req_s | 283.1 | 275.4–286.1 | 184,914 | 161,323–207,254 |
| total_ms_p50 | 3.581 | 3.445–3.678 | 0.0045 | 0.0044–0.0048 |
| total_ms_p99 | 4.300 | 4.019–4.549 | 0.0105 | 0.0096–0.0175 |
| service_ms_p50 | 0.0004 | — | 0.0000 | — |
| queue_wait_ms_p50 | 3.581 | 3.444–3.678 | 0.0031 | 0.0029–0.0032 |

### noop_c8 (service_ms=0, concurrency=8)

| metric | Ray median | Ray min–max | HPX median | HPX min–max |
|---|---|---|---|---|
| throughput_req_s | 307.1 | 289.0–322.3 | 1,048,718 | 990,305–1,080,351 |
| total_ms_p50 | 25.82 | 24.41–27.39 | 0.0071 | 0.0067–0.0073 |
| total_ms_p99 | 31.57 | 27.73–34.58 | 0.0158 | 0.0132–0.0164 |
| service_ms_p50 | 0.0004 | — | 0.0000 | — |
| queue_wait_ms_p50 | 25.82 | 24.41–27.39 | 0.0041 | 0.0040–0.0046 |

### sleep5_c8 (service_ms=5, concurrency=8)

| metric | Ray median | Ray min–max | HPX median | HPX min–max |
|---|---|---|---|---|
| throughput_req_s | 141.0 | 139.8–142.1 | 170.2 | 166.8–171.2 |
| total_ms_p50 | 56.57 | 56.23–57.48 | 47.06 | 46.54–47.12 |
| total_ms_p99 | 61.07 | 60.35–71.17 | 48.44 | 48.28–66.65 |
| service_ms_p50 | 5.638 | 5.638–5.639 | 5.797 | 5.735–5.821 |
| queue_wait_ms_p50 | 50.98 | 50.69–51.91 | 41.17 | 40.71–41.19 |

### sleep20_c8 (service_ms=20, concurrency=8)

| metric | Ray median | Ray min–max | HPX median | HPX min–max |
|---|---|---|---|---|
| throughput_req_s | 43.93 | 43.66–43.99 | 43.22 | 43.19–43.47 |
| total_ms_p50 | 182.05 | 181.20–183.42 | 184.14 | 183.76–184.87 |
| total_ms_p99 | 192.42 | 191.90–193.15 | 196.34 | 190.93–200.08 |
| service_ms_p50 | 21.03 | 21.03–21.03 | 22.55 | 22.39–22.78 |
| queue_wait_ms_p50 | 161.20 | 160.26–162.47 | 161.50 | 160.79–161.85 |

## 5. Interpretation

* **noop_c1** — HPX is microsecond-scale (total p50 ≈ 4.5 µs) while Ray is
  millisecond-scale (≈ 3.58 ms). Ray's entire round trip is non-service time:
  observed service is ~0, so `queue_wait_ms` ≈ `total_ms`. This is boundary
  cost (process/IPC/serialization vs an in-process hop), not a general claim
  about scheduler quality.

* **noop_c8** — The single Ray actor serializes, so adding 8 in-flight requests
  deepens the queue without raising completion rate: throughput stays ~307 req/s
  while per-request total rises to ~25.8 ms. HPX stays very low (total p50 ≈ 7
  µs, throughput ~1.05M req/s) because the lane is intra-process and absorbs the
  pipelined window cheaply.

* **sleep5_c8** — Transition zone: service time (~5.6–5.8 ms observed on both)
  is now comparable to dispatch overhead. HPX leads modestly (total p50 47.1 vs
  56.6 ms; throughput 170 vs 141 req/s), but the gap is far smaller than in the
  no-op cells. Observed service times nearly match, confirming both run the same
  single-lane serialization shape.

* **sleep20_c8** — Service-dominated: the single lane's service rate sets the
  ceiling, and the runtimes converge (throughput 43.9 vs 43.2 req/s; total p50
  182.1 vs 184.1 ms; queue_wait p50 ≈ 161 ms on both). HPX is marginally slower
  here because its blocking `sleep_for` overshoots a bit more (observed service
  ~22.5 ms vs Ray's 21.0 ms). At this service size Ray's boundary overhead is a
  negligible fraction of total latency.

## 6. Conclusion

HPX's intra-locality C++ control path is far cheaper for tiny/no-op requests.
Ray's actor-process boundary cost is visible when service time is tiny. As
service time grows, the single serialized service lane dominates and the
runtimes converge. This does not prove HPX is generally faster than Ray; it
shows where HPX's native same-process control path can matter.

## 7. Caveats

* Different boundaries: `ray-actor-process` vs `hpx-intra-locality` — not an
  identical-boundary comparison.
* `queue_wait_ms` is defined differently per engine: exact same-process
  submit→start for HPX; approximate (`total_ms − service_ms_observed`, all
  non-service time) for Ray across the process boundary. `total_ms` is the
  authoritative client-side latency on both.
* p99/tails are softer than the medians (e.g. single-repeat sleep5 p99 spikes to
  ~67–71 ms that do not appear in the medians); read medians as the signal.
* Synthetic `sleep` backend only — models I/O-bound / GPU-offloaded serving, not
  compute-bound work.
* One serialized lane / one actor only.
* No distributed HPX (single locality, intra-process).
* No Ray Serve.
* No real model inference.

## 8. Suggested next experiments

* Widen the matrix to `sleep1` and concurrency `c4` / `c16` for both engines.
* Multi-actor Ray scaling (more than one actor).
* Multi-lane HPX scaling (more than one service lane).
* Later: a real model backend, once the synthetic contract is stable.
