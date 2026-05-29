# Sleep-Overshoot Diagnostic Note

A short diagnostic note on synthetic-sleep service-time fidelity across Ray,
native HPX, and rayx. Documentation only; no code, analyzer, or schema changes.
Companion to `benchmarks/08_ray_hpx_rayx_variable_service/ray_hpx_rayx_variable_service.md`.

## 1. Purpose

Under both fixed and variable (bimodal) sleep workloads, HPX native and rayx
reported higher `service_ms_observed` than Ray (e.g. ~25 ms vs ~21 ms for a
20 ms request). This diagnostic was run to determine whether that gap was a
**measurement artifact**, **lane/concurrency contention**, or **sleep-primitive
wakeup behavior** — before scaling to a wider lane sweep where it could confound
cross-engine comparison.

## 2. Output location

```text
results/sleep_overshoot_diagnostic_20260529T223610Z/
```

* 108 JSONL + 108 summary JSON + `aggregate.json`
  (3 engines × service_ms {1,5,20} × lanes {1,4} × concurrency {1,8} × 3 repeats).
* 108/108 runs passed.
* 108/108 analyzer summaries passed.

## 3. Measurement check

The service brackets are tight around the sleep body in every path, on monotonic
clocks:

* **Ray** (`ray_impl/ray_actor_baseline.py::serve`): `perf_counter_ns()` →
  `time.sleep(service_ms/1000)` → `perf_counter_ns()`.
* **HPX native & rayx** (shared `hpx_impl/service_lane.hpp::service`): `now_ns()`
  (`steady_clock`) → `std::this_thread::sleep_for(...)` → `now_ns()`.

The brackets are equivalent and immediately around the sleep call, so the gap is
**not a measurement artifact** — it reflects how long each sleep primitive
actually blocks.

## 4. Key result

Median of 3 repeats, `service_ms_observed` overshoot (observed − requested):

* **Ray:** ~5% at 20 ms (and ~13–15% at 1–5 ms); the absolute ms overshoot is
  small and the percentage shrinks with duration.
* **HPX / rayx:** ~25% **proportional** overshoot at p99 across all durations
  (1 ms → ~28%, 5 ms → ~25%, 20 ms → ~25%; only a tiny additive floor at 1 ms).
* **HPX native and rayx match** within noise (20 ms p99 ≈ 25.03 vs 25.04 ms) —
  expected, since they share the same `service_lane.hpp` (`sleep_for`). The
  pybind/GIL crossing adds nothing to service time.
* **Overshoot does not materially worsen with lanes or concurrency:** p99
  overshoot is essentially flat at 1 vs 4 lanes and at concurrency 1 vs 8
  (deltas ≤ ~0.013 ms).
* Under contention (4 lanes / concurrency 8) the HPX/rayx **p50 shifts toward the
  p99 ceiling** (e.g. 20 ms p50 overshoot 2.4 ms → 3.6 ms), but the **p99 ceiling
  itself stays stable** at ~25%.

## 5. Interpretation

* This is **sleep-primitive / wakeup fidelity, not control-plane overhead**.
  `std::this_thread::sleep_for` (libc++) wakes less tightly than Python
  `time.sleep` here; the proportional ~25% tail is consistent with macOS timer
  coalescing applying a duration-proportional tolerance (hypothesis, not proven).
* Likely **macOS/laptop-specific**.
* It affects cross-engine `service_ms_observed` and the service-dominated portion
  of `total_ms`.
* It **does not invalidate within-engine lane-scaling results**, because it is
  stable across lanes and concurrency — adding lanes does not change per-request
  service fidelity.

## 6. How to read future results

* For **cross-engine service/total** comparisons, treat HPX/rayx as carrying a
  stable ~25% sleep-fidelity inflation versus Ray's lower (~5% at 20 ms)
  overshoot.
* Cross-engine service/total gaps up to **roughly 20%** may be sleep-primitive
  behavior, not control overhead.
* For **control-plane** claims, emphasize **throughput** and **queue_wait**
  (while still noting that `queue_wait` is exact only for native HPX and
  approximate for Ray/rayx — see `docs/experiment_plan.md` §9).
* For **within-engine lane scaling**, proceed as-is — the overshoot is stable.

## 7. Caveats

* macOS/laptop-specific; will not transfer to other OSes/core counts.
* Synthetic sleep only; sleep is not real model compute.
* p99 is softer than medians in general, though it was stable here.
* A busy-spin `work_mode` or a real backend would remove or change this artifact.

## 8. Suggested next step

* Proceed with the wider lane sweep, carrying the sleep-overshoot caveat into
  interpretation.
* Optional future: add a `work_mode=spin` CPU-bound axis to remove sleep-fidelity
  effects and probe scheduling differently.
