# Native C++ Multi-Client Driver

Documentation for the native HPX multi-client experiment: does driving the same
HPX service lanes from several **native C++ client threads** (no Python, no
pybind, no GIL) raise the high-lane throughput ceiling beyond what the rayx
Python multi-client driver reached? Documentation only; no code, analyzer, or
schema changes. Companion to `experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md`,
`experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md`, and `experiments/01_sleep_overshoot/sleep_overshoot_note.md`.

> **Update (2026-05-31) — remaining-ceiling interpretation refined.** This note
> concludes (§6, §7, §9) that, with Python/GIL ruled out, the remaining
> high-lane ceiling is **"shared HPX/service-lane coordination and/or laptop
> scheduling."** A later native sleep-mode diagnostic (the `--diag` phase
> decomposition in `hpx_impl/hpx_synthetic_baseline.cpp`) supersedes that
> attribution: the ceiling is primarily a **closed-loop FIFO-retire /
> client-driver artifact**, not HPX/service-lane coordination and not machine
> scheduling. The decomposition shows the dominant non-service time at the
> ceiling sits in a `completion_ms` bucket that is **ready-but-unretired FIFO
> wait** — short requests that finished service but are held behind older
> in-flight 20 ms requests under strict FIFO `one_by_one` retirement — and
> **not** HPX future-completion overhead (which is microsecond-scale). At the
> ceiling cell (16 lanes, concurrency 32, bimodal 1 ms/20 ms `p_high=0.1` seed 0,
> ct1, `--hpx:threads=4`): FIFO `one_by_one` gives throughput **1384 req/s**,
> `completion_ms` p50 **21.860 ms**, `total_ms` p50 **23.830 ms**, mean lane
> utilization **0.28**; switching to `batch_wait` / as-completed retirement at
> the **same** concurrency window and **identical** lane service behavior gives
> **2558 req/s (+85%)**, `completion_ms` p50 **0.003 ms**, `total_ms` p50
> **1.326 ms**, mean lane utilization **0.52** (idle lane capacity is recovered
> once the client stops stalling on the 20 ms head). A window-removed
> `submit_all_get_all` reaches ~**2985 req/s** but is **not** apples-to-apples
> (the in-flight window is removed). This removes FIFO head-of-line blocking
> **for this closed-loop bimodal sleep workload**; it is not a general claim
> that `batch_wait` is always faster, and it does not say HPX coordination or
> Python/GIL is the bottleneck. This sleep-mode retire ceiling is **distinct
> from** the `work_mode=spin` core-boundary saturation
> (`experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md`),
> which is a hardware/scheduling effect. The measurements and the
> "not Python/GIL" finding below stand and are preserved as provenance; only the
> *positive* attribution of the remaining ceiling is refined. Phase B
> (2026-05-31) reran rayx's own FIFO path on the A2-exact bimodal setup and it
> tracked native FIFO within noise (e.g. L16 ct1 rayx ~1370 vs native ~1384
> req/s), confirming the same FIFO-retire pattern holds through the Python
> frontend with no separate Python/GIL ceiling.

## 1. Purpose

The rayx multi-client experiment (`experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md`) showed that
driving one shared `Engine` from several Python client threads moved the
high-lane ceiling upward — at 16 lanes, **~1351 → ~1740 req/s** — but the gains
saturated quickly, consistent with a GIL-bound submit/bookkeeping path.

This experiment tests **what the remaining ceiling actually is**: Python/pybind/
GIL overhead in the client path, or native HPX/service-lane coordination (and
laptop scheduling). The native C++ multi-client driver removes Python, pybind,
and the GIL entirely from the client path, so if the GIL were the limiter the
native driver should break past the rayx ceiling.

## 2. Implementation summary

* `hpx_impl/hpx_synthetic_baseline.cpp` gained `--client-threads` (default 1).
* `--client-threads 1` preserves the existing single-client path unchanged.
* `--client-threads > 1` spawns N native C++ client threads (`std::thread`),
  each running its own windowed `one_by_one` loop with its own in-flight deque
  and futures. Supported only with `retire_mode one_by_one`.
* All client threads share the **same** vector of `ServiceLane`s. The
  `--concurrency` window is split across the N threads (remainder to the first
  threads), so total in-flight stays `--concurrency`.
* Global request indices are partitioned into contiguous disjoint blocks, so
  **request IDs remain unique** and **`service_ms_requested` remains
  deterministic by the global request index** (identical multiset/sequence to
  the single-client run; only submission interleaving and lane routing differ).
* A shared `std::atomic` round-robin counter routes requests to lanes — the
  lock-free analog of the rayx Engine's single GIL-serialized round-robin
  counter.
* `ServiceLane::submit()` was already mutex-protected, so concurrent submits to
  the same lane are safe and **`hpx_impl/service_lane.hpp` did not change**.
* JSONL schema stayed **version 1**; **no new fields** (`client_threads` appears
  in the stdout summary only, not in rows). **No analyzer change. No new
  executable.**

## 3. Output location

```text
results/hpx_native_multiclient_20260530T011043Z/
```

* 18 JSONL + 18 summary JSON + `aggregate.json` + `run.log`.
* 18/18 runs passed.
* 18/18 analyzer summaries passed.
* Integrity: every run `completed == 1000`, `rows == 1000`, unique request IDs
  `== 1000`.

## 4. Experiment shape

* HPX native only (`boundary = hpx-intra-locality`), `retire_mode one_by_one`.
* Bimodal variable service: low 1 ms, high 20 ms, `p_high=0.1`, `seed=0`.
* Lanes: 8 and 16. Concurrency: 32. Client threads: 1, 2, 4.
* Requests: 1000. Warmup: 20. Repeats: 3. (2 × 3 × 3 = 18 runs.)
* `--hpx:threads=4`, fixed across all runs (matches the earlier comparable
  HPX/rayx runs; avoids mixing HPX worker-count changes with client-thread
  changes).

## 5. Results

### Native aggregate (mean of 3 repeats)

Latencies in ms, throughput in req/s.

| lanes | client_threads | throughput | tot_p50 | tot_p90 | tot_p99 | svc_p50 | svc_p99 | qw_p50 | qw_p99 |
|------:|---------------:|-----------:|--------:|--------:|--------:|--------:|--------:|-------:|-------:|
| 8  | 1 | 1121.3 | 25.50 | 45.98 | 51.31 | 1.263 | 25.02 | 0.035 | 43.74 |
| 8  | 2 | 1316.5 | 24.73 | 41.46 | 49.71 | 1.263 | 25.02 | 0.835 | 39.71 |
| 8  | 4 | 1347.8 | 24.22 | 45.26 | 70.67 | 1.263 | 25.02 | 0.867 | 47.84 |
| 16 | 1 | 1390.1 | 23.83 | 42.16 | 47.94 | 1.262 | 25.02 | 0.021 | 23.83 |
| 16 | 2 | 1634.7 | 23.07 | 25.77 | 46.29 | 1.261 | 25.02 | 0.016 | 24.50 |
| 16 | 4 | 1771.5 | 21.87 | 26.29 | 48.39 | 1.261 | 25.02 | 0.016 | 25.06 |

(Per-repeat throughput spread is ≈ ±3–5%, e.g. 16-lane ct4 = [1788, 1675, 1851].)

### Side-by-side: native vs rayx multi-client

Throughput in req/s. rayx values are the recorded medians from
`experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md`; native values are the means above. Delta =
(native − rayx) / rayx.

| cell      | rayx | native | delta |
|-----------|-----:|-------:|------:|
| L8  ct1   | 1111 | 1121   | +0.9% |
| L8  ct2   | 1287 | 1317   | +2.3% |
| L8  ct4   | 1337 | 1348   | +0.8% |
| L16 ct1   | 1351 | 1390   | +2.9% |
| L16 ct2   | 1663 | 1635   | −1.7% |
| L16 ct4   | 1740 | 1772   | +1.8% |

## 6. Interpretation

* **Native multi-client does not go meaningfully beyond rayx multi-client.**
  Native tracks rayx within **±3%** at every cell — inside run-to-run noise.
* **Removing Python/pybind/GIL buys essentially nothing on throughput** for this
  workload. A pure-C++ client with no GIL hits the same ceiling rayx did.
* **Therefore the remaining ceiling is not Python/GIL.** The likely limit is
  **HPX/service-lane coordination and/or laptop scheduling** (blocking-sleep
  lanes, OS scheduler across lanes and cores).
* **Native client threads help modestly, the same shape as rayx:** 8 lanes
  **1121 → 1348 req/s** (+20%), 16 lanes **1390 → 1772 req/s** (+27%), saturating
  by 2–4 threads. So the single-submitter feeding bottleneck the extra client
  threads recover **exists even in native C++** — it is not the GIL.
* **rayx is already running at the native HPX ceiling** for this workload.
* **Native single-client matches the prior native HPX baseline.** Native ct1 at
  16 lanes = 1390 req/s, total p99 ≈ 48 ms, reproducing the
  concurrency-32 / 16-lane figure from `experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md`
  (~1390 req/s, p99 ~50 ms) within noise — a clean control.
* Service overshoot is unaffected — `svc_p99` stays ~25 ms (~+25%) at every
  thread count (see `experiments/01_sleep_overshoot/sleep_overshoot_note.md`).

## 7. Conclusion

The native C++ multi-client driver shows that rayx is not leaving meaningful
throughput on the table in the Python/pybind/GIL layer for this workload. Native
C++ and rayx track within run-to-run noise. The high-lane ceiling appears to be a
shared HPX/service-lane coordination or laptop scheduling ceiling, not a Python
frontend ceiling. This strengthens the rayx result: the thin Python frontend
preserves native HPX behavior for this synthetic native workload.

## 8. Caveats

* Synthetic blocking sleep only.
* Native C++ work only, not arbitrary Python execution.
* Local macOS laptop, single locality; results are machine-specific.
* HPX/rayx sleep overshoot still applies (`svc_p99` ≈ 25 ms for the 20 ms tail).
* Multi-client lane routing interleaves and is nondeterministic (shared atomic
  counter); per-lane balance is no longer the clean `idx % lanes` mapping —
  compare throughput/total, not per-lane. The service sequence by global index
  stays deterministic.
* No Ray or rayx rerun in this slice; comparison uses the existing recorded rayx
  multi-client results on the same workload/concurrency/`--hpx:threads`.
* p99/tails are softer than medians; read throughput and p50 as the firmer
  signals.
* Not a Ray replacement claim.

## 9. Suggested next directions

* Investigate the HPX/service-lane coordination ceiling directly (where the
  ~1772 req/s limit comes from: lane handoff, sleep wakeups, or scheduler).
* Add a `work_mode=spin` CPU-bound axis, free of the sleep-fidelity artifact, as
  a separate scheduling regime.
* Later: test a real native backend behind the lane.
* Deprioritize the GIL-release submit path — native C++ did not outperform rayx
  here, so removing the GIL from `submit` is unlikely to raise this ceiling.
