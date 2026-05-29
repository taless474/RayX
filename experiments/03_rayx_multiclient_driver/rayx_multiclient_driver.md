# rayx Multi-Client-Thread Driver

Documentation for the rayx multi-client-thread experiment: does driving one
`Engine` from several Python client threads raise the throughput ceiling seen in
the lane sweep? Documentation only; no code, analyzer, or schema changes.
Companion to `experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md` and
`experiments/01_sleep_overshoot/sleep_overshoot_note.md`.

> **Update (2026-05-30) — interpretation superseded.** This note reads the
> high-lane throughput ceiling as **Python/GIL-bound** (§6 Interpretation, §7
> Conclusion, §9 next levers). That reading was later tested directly by the
> native C++ multi-client driver (`experiments/04_hpx_native_multiclient/hpx_native_multiclient.md`), which
> removes Python, pybind, and the GIL from the client path. Native C++ matched
> `rayx` within run-to-run noise (~±3%) at every lane and client-thread count, so
> removing the GIL bought essentially nothing on throughput. The remaining
> high-lane ceiling is therefore better attributed to shared HPX/service-lane
> coordination and/or machine scheduling, **not** Python/GIL. The measurements
> and original interpretation below are preserved as provenance; read the
> GIL-bound conclusion as **superseded**.
>
> **Further refinement (2026-05-31).** The "shared HPX/service-lane coordination
> and/or machine scheduling" attribution this banner forwarded to is itself now
> sharpened: a later native sleep-mode `--diag` decomposition shows the
> remaining bimodal high-lane ceiling is primarily a **closed-loop FIFO-retire /
> client-driver artifact** — short completed requests held behind older
> in-flight 20 ms requests under strict FIFO `one_by_one` retirement — **not**
> HPX coordination and **not** Python/GIL. At the ceiling cell (16 lanes,
> concurrency 32, ct1) switching from FIFO `one_by_one` to `batch_wait` /
> as-completed retirement, at the same window and identical lane service, lifts
> throughput **1384 → 2558 req/s (+85%)** and collapses the `completion_ms` p50
> from **21.860 → 0.003 ms**. See
> `experiments/04_hpx_native_multiclient/hpx_native_multiclient.md` (top banner)
> for the full numbers and caveats.
>
> **Phase B confirmation (2026-05-31).** rayx's own FIFO path was rerun on the
> A2-exact bimodal setup (engine API, `one_by_one`, low 1 ms / high 20 ms
> `p_high=0.1` seed 0, concurrency 32, requests 1000, warmup 20,
> `--hpx-threads 4`, 3 repeats) and tracks native FIFO at every cell:
> L16 ct1 **rayx 1370 vs native ~1384 req/s (~0.99×)**, L8 ct1 **1100 vs 1128
> (~0.97×)**, L8 ct4 **1280 vs 1348 (~0.95×)**; L16 ct4 **rayx 1874 vs native
> ~1772–1827 req/s**, i.e. within the high-lane oversubscription variance band
> (not a real frontend speedup). So rayx reproduces the same FIFO-retire behavior
> through the Python frontend and **Python/GIL adds no structural high-lane
> ceiling** on top of the FIFO-retire artifact. (Compare throughput and
> `total_ms`; rayx `queue_wait_ms` is approximate/cross-clock.) A rayx
> `wait_any` / as-completed retire path was **not** built — it is optional /
> low-priority, only needed to demonstrate the +85% lift through the frontend,
> and not required for this conclusion.

## 1. Purpose

The wider lane/actor sweep (`experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md`) showed rayx
and native HPX throughput converging near **~1390 req/s** at high lane counts,
far below the theoretical service capacity — suspected to be a
client/coordination ceiling. This experiment tests whether the **single-threaded
Python `one_by_one` client loop** was part of that ceiling, by driving one shared
`Engine` from multiple Python client threads.

## 2. Output location

```text
results/rayx_multiclient_20260529T231946Z/
```

* 18 JSONL + 18 summary JSON + `aggregate.json`.
* 18/18 runs passed.
* 18/18 analyzer summaries passed.

## 3. Implementation summary

* `bench/run_hpx_python_baseline.py` gained `--client-threads` (default 1).
* `--client-threads 1` preserves the existing single-thread path unchanged.
* Multi-client mode shares **one** `Engine`; the `--concurrency` window is split
  across N threads (remainder to the first threads), so total in-flight stays
  `--concurrency`.
* Global request indices are partitioned into contiguous disjoint blocks, so
  **request IDs remain unique** and **`service_ms_requested` remains
  deterministic by the global request index** (identical multiset/sequence to
  the single-thread run; only submission interleaving and lane routing differ).
* JSONL schema stayed **version 1**; **no new fields** (no `client_thread_id`).
* **No C++ extension change**, no rebuild, **no analyzer change**.
* Thread-safety relies on the existing design: `Engine.submit()` never releases
  the GIL (so concurrent submits and the round-robin counter are GIL-serialized),
  each thread owns its own in-flight futures, and `Future.result()` releases the
  GIL only around its own blocking wait.

## 4. Experiment shape

* rayx only, `--api engine`, `retire_mode one_by_one`, no batch.
* Bimodal variable service: low 1 ms, high 20 ms, `p_high=0.1`, `seed=0`.
* Lanes: 8 and 16. Concurrency: 32. Client threads: 1, 2, 4.
* Requests: 1000. Warmup: 20. Repeats: 3. (2 × 3 × 3 = 18 runs.)

## 5. Results

Median of 3 repeats. `thru_an` = analyzer `throughput_req_s`; `wall_thru` =
`requests / driver wall_s`. Latencies in ms, throughput in req/s.

| lanes | client_threads | thru_an | wall_thru | tot_p50 | tot_p90 | tot_p99 | svc_p50 | svc_p99 | qw_p50 | qw_p99 |
|------:|---------------:|--------:|----------:|--------:|--------:|--------:|--------:|--------:|-------:|-------:|
| 8  | 1 | 1111.1 | 1111.1 | 25.75 | 47.12 | 50.65 | 1.26 | 25.02 | 23.91 | 47.78 |
| 8  | 2 | 1286.6 | 1287.0 | 24.91 | 45.88 | 71.43 | 1.26 | 25.02 | 23.05 | 53.57 |
| 8  | 4 | 1336.7 | 1336.9 | 23.89 | 47.85 | 62.73 | 1.26 | 25.02 | 21.89 | 57.23 |
| 16 | 1 | 1351.3 | 1351.4 | 24.32 | 45.43 | 50.04 | 1.25 | 25.02 | 22.59 | 48.85 |
| 16 | 2 | 1662.5 | 1661.1 | 23.46 | 26.14 | 46.28 | 1.25 | 25.02 | 21.16 | 44.98 |
| 16 | 4 | 1740.4 | 1739.1 | 22.12 | 26.28 | 48.62 | 1.26 | 25.02 | 16.92 | 46.45 |

Throughput vs client_threads (analyzer median): **8 lanes** 1111 → 1287 (1.16×) →
1337 (1.20×); **16 lanes** 1351 → 1663 (1.23×) → 1740 (1.29×). The 16-lane,
1-thread cell (1351) reproduces the prior sweep's 16-lane / concurrency-32 number
(~1375–1390) within noise — a clean control.

## 6. Interpretation

* **Throughput improves with client threads** — ~1111 → ~1337 req/s at 8 lanes
  (+20%) and ~1351 → ~1740 req/s at 16 lanes (+29%).
* **The previous ~1390 ceiling moves upward** (to ~1740 at 16 lanes), so the
  single-threaded client loop **was a partial bottleneck**.
* **Gains saturate quickly** — most of the benefit lands by 2 threads, with 2→4
  adding only ~4–5%.
* This is consistent with the **Python GIL / per-request bookkeeping**:
  * `Engine.submit()` holds the GIL, so submits serialize across threads.
  * `Future.result()` releases the GIL only while waiting, so only the blocking
    wait overlaps across threads; per-request record building stays GIL-held.
* So multi-client threads **help, but do not fully bypass** the Python/GIL
  ceiling.
* Service overshoot is unaffected — `service_ms_p99` stays ~25 ms (~+25%) at
  every thread count (see `experiments/01_sleep_overshoot/sleep_overshoot_note.md`). Latency also improved
  slightly with threads (16-lane `queue_wait_p50` 22.6 → 16.9 ms). `total_ms_p99`
  is noisy at 3 repeats; read medians.

## 7. Conclusion

Multi-client Python threads partially raise the rayx throughput ceiling, showing
that the earlier high-lane ceiling was partly a client-driver bottleneck.
However, throughput does not scale linearly with client threads because
submit-side and per-request bookkeeping remain GIL-bound. The next performance
levers are `submit_batch`, a native C++ driver, or safely releasing the GIL in
`submit` after protecting the round-robin state.

## 8. Caveats

* rayx only (no Ray / native-HPX multi-client comparison here).
* Synthetic blocking sleep only.
* Local macOS laptop, 10 cores (4 P + 6 E).
* Python GIL limits submit-side and per-request bookkeeping scaling.
* Multi-client changes lane routing and submission interleaving (per-lane balance
  is no longer the clean `idx % lanes` mapping — compare throughput/total, not
  per-lane).
* Native C++ work only, not arbitrary Python execution.
* Not a Ray replacement.
* p99/tails are softer than medians.

## 9. Suggested next directions

* Use `submit_batch()` for hot-loop throughput (one Python→C++ crossing for many
  requests).
* Consider a native C++ driver to remove Python/GIL entirely.
* Consider modifying `_rayx.cpp` so `submit()` can release the GIL safely —
  requires making the round-robin counter atomic or mutex-protected first.
* Later: a `work_mode=spin` CPU-bound axis.
* Later: a real native backend behind the lane.
