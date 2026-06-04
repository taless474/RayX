# RayX HpxLane Adapter-Hop Cost (uncontended boundary-crossing observation)

After exp21 (contract parity) and exp22 (under-load divergence), one question
remained open: **what does the `HpxLane` adapter's per-call hop actually cost,
uncontended?** The rayx `Engine` runs on the external Python thread. For
`lane_impl="std"` the `RayxLaneAdapter<ServiceLane>` forwards lane-state calls
**directly**; for `lane_impl="hpx"` the `RayxLaneAdapter<HpxLane>` must hop
**every** lane-state call onto an HPX worker via `hpx::run_as_hpx_thread`
(`hpx::mutex` / `hpx::thread` may be touched only from an HPX thread). This
experiment measures, on an idle worker pool, how much per-call latency that hop
adds relative to the no-hop path.

**What this is not.** Not a serving-throughput benchmark, not a speedup claim,
not an "HPX beats Ray" claim, and **not** an "HpxLane is faster/slower" verdict.
`std` has no hop only because it uses std primitives; the std-vs-hpx delta is a
structural cost of the chosen off-HPX-thread seam design, not a backend-quality
judgement. All timing and all derived deltas are **observation-only** and are
**never gated**. Companion reference: `docs/reference/rayx_frontend_design.md` §13
(the `lane_impl` backend seam). Separate from exp21 (parity) and exp22 (load
divergence).

## 1. How to read the delta (softened interpretation)

The std-vs-hpx per-call delta is **not** a perfect subtraction that "isolates the
hop." It is the **closest observable approximation of the hop-dominated boundary
cost**: it still includes Python / pybind11 call overhead, HPX scheduling /
dispatch overhead, and machine-specific jitter.

* At `num_lanes=1`, **`lane_stats()` is the cleanest available public-API
  isolator**: both backends do the same mutex + copy work, and the only
  structural difference is the single `run_as_hpx_thread` hop — no lane-worker
  dispatch, no future, no service. Its delta is the best available proxy for the
  per-call hop-dominated cost (still not a pure hop).
* **`submit_get`** and **`submit_batch`** additionally include lane-worker
  dispatch and future retrieval, so they are reported as **end-to-end no-op op
  cost**, not a hop measurement. As the data shows (§4), their deltas are
  dispatch-dominated and can even change sign.

## 2. Method

* `num_lanes=1`, `service_ms=0`, `work_mode="sleep"` (a 0 ms no-op). **`spin` is
  not used** — this experiment is the uncontended no-op/sleep path only.
* Each operation is timed with `time.perf_counter_ns` in a tight loop; the
  `Engine` is constructed and shut down **outside** the timed loop, and the first
  `warmup` iterations are discarded (the first hop pays one-time pool warmup; the
  first submit pays lane-worker spin-up). Completion / prefix checks are kept
  **outside** the measured interval.
* Operations:
  * `lane_stats` — `e.lane_stats()`.
  * `submit_get` — `f = e.submit(service_ms=0); f.result()`.
  * `submit_batch` — `fs = e.submit_batch(service_ms=0, count=K); e.get(fs)`
    (one hop amortized over `K` enqueues for `hpx`; `K = 64`).
* Because the HPX runtime is a **process resource** (one `hpx::start` per process,
  fixed worker count), each `(backend, hpx_threads)` pair runs in its **own
  subprocess**.

## 3. Matrix

| Axis | Full | Quick |
|---|---|---|
| backend | std, hpx | std, hpx |
| `hpx_threads` (subprocess axis) | 1, 4 | 1 |
| `num_lanes` | 1 | 1 |
| `iters` (lane_stats / submit_get) | 20000 | 2000 |
| `iters` (submit_batch) | 2000 | 200 |
| `warmup` (discarded) | 1000 | 200 |
| `batch_size` K | 64 | 64 |

## 4. Results (full run, this machine; observation-only)

All firm structural gates passed (`all_structural_gates_passed: true`,
`gate_failures: []`). Latency percentiles in **µs**; the curated `aggregate.json`
keeps the summary percentiles, raw per-call arrays stay under `results/`.

**`lane_stats()` — the cleanest hop-dominated approximation (`num_lanes=1`):**

| `hpx_threads` | std p50 | hpx p50 | delta p50 | hpx p99 |
|---|---|---|---|---|
| 1 | 0.21 | 1.96 | **+1.75** | 5.0 |
| 4 | 0.21 | 5.50 | **+5.29** | 13.1 |

The no-hop `std` path is ~0.2 µs (a direct mutex + list build); the `hpx` path
adds a single-digit-µs hop-dominated cost. **Notably, the cost rises with
`hpx_threads`** (≈1.75 µs at 1 worker → ≈5.3 µs at 4) — contrary to a prior guess
that it would be pool-size-insensitive. The likely driver is HPX
scheduling/wakeup onto a larger idle pool; this is an observation, machine- and
runtime-version-specific, not a modeled result.

**`submit_get` — end-to-end no-op op cost (dispatch-dominated, sign-variable):**

| `hpx_threads` | std p50 | hpx p50 | delta p50 |
|---|---|---|---|
| 1 | 6.38 | 3.54 | **−2.83** |
| 4 | 6.46 | 11.83 | **+5.38** |

The delta **changes sign** across pool size — at 1 worker the `hpx` no-op op is
actually lower than `std`, at 4 workers higher. This is exactly why `submit_get`
is reported as end-to-end op cost, **not** a hop measurement: lane-worker dispatch
(cooperative `hpx::thread` wakeup vs `std::thread` condition-variable wakeup)
dominates and can go either way.

**`submit_batch` — single hop amortized over K=64 (per-request derived):**

| `hpx_threads` | std p50 (iter) | hpx p50 (iter) | hpx per-request p50 |
|---|---|---|---|
| 1 | 66.5 | 75.9 | 1.19 |
| 4 | 67.7 | 159.4 | 2.49 |

Amortized over a 64-request bulk submission, the per-request figure is ~1–2.5 µs
for `hpx`; the batch-level delta also grows with `hpx_threads` (≈9 µs at 1 → ≈92 µs
at 4).

## 5. Interpretation (evidence toward a decision, not the decision)

* The **uncontended hop-dominated boundary cost is single-digit-to-tens of µs per
  call** (best approximated by `lane_stats()` at `num_lanes=1`), and it **grows
  with the HPX worker-pool size**.
* That magnitude is **orders smaller than the synthetic service times** used
  across the RayX corpus (typically ms-scale, e.g. `service_ms` of 4–200). So for
  serving-shaped (parked, ms-scale) workloads the per-call hop is a small fraction
  of a request's lifecycle.
* This is **evidence toward**, not a decision about, a future source-touching seam
  optimization: a hop-reduction slice looks **weakly justified for serving-shaped
  workloads**, and would matter mainly for very-high-rate tiny/no-op control
  operations or large lane counts (where `lane_stats()` hops once per lane). The
  decision itself is out of scope here.
* The `submit_get` sign flip is a useful caution: end-to-end no-op deltas are
  dispatch-dominated and must **not** be read as a hop cost or a faster/slower
  verdict.

## 6. Firm gates vs observation-only

| Firm structural gates (pass/fail) | Observation-only (never gated) |
|---|---|
| G1 operations complete (lane_stats shape; submit rows `completed`) | p50 / p90 / p99 / min / max / mean (µs) per op |
| G2 `actor_id` prefix matches backend | derived std-vs-hpx deltas (p50/p90/p99) |
| G3 expected sample count recorded (`n_recorded == n_expected`) | `submit_batch` per-request amortized figure |
| G4 no exceptions (`ran_ok`) | `iters_per_s` (context) |
| G5 both backends ran the same op set + sample counts | the pool-size-sensitivity pattern |

## 7. Caveats

* **Observation-only, machine-specific.** µs-scale timing is jitter-prone;
  percentiles (not just mean) are reported, and `p99`/`max` are especially
  jitter-sensitive. No std-vs-hpx timing comparison is gated.
* **Closest approximation, not a pure hop.** Even `lane_stats()` includes
  Python/pybind overhead and HPX scheduling overhead; it is the cleanest available
  public-API isolator, not a perfect subtraction.
* **Uncontended only.** This measures an idle worker pool. Under load the hop
  competes for HPX workers (see exp22); these figures are a floor, not the loaded
  cost.
* **No verdict.** Neither backend is "faster"; the delta is a structural cost of
  the off-HPX-thread seam, reported as evidence, not a quality judgement.
* **Scope.** rayx-only, no-op sleep path; `spin` not used. Separate from exp21
  (parity) and exp22 (load divergence). Not Ray Serve, not a Ray object store, not
  real model inference, not an HPX-beats-Ray claim. No analyzer / benchmark-JSONL
  schema / driver / CI / public `Future`-ownership change; no HPX internals
  exposed. Raw per-call arrays are experiment-local scratch under `results/`
  (gitignored); `aggregate.json` keeps summary percentiles only.

## 8. Reproduction

```bash
# quick smoke (smaller sample; no aggregate.json written)
python experiments/23_rayx_hpxlane_adapter_hop_cost/run_rayx_hpxlane_adapter_hop_cost.py --quick

# full run (writes the curated aggregate.json beside this report)
python experiments/23_rayx_hpxlane_adapter_hop_cost/run_rayx_hpxlane_adapter_hop_cost.py

# optional overrides
#   --iters N  --warmup W  --repeats R  --hpx-threads "1,4"  --ops "lane_stats,submit_get"  --batch-size K
```

Requires the `_rayx` extension built (`cmake --build python/build`).
