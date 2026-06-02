# True Bulk-Varied Synthetic Service Time

Characterizes the new varied batch path — `engine.submit_batch(service_ms=[...])`
— where a **single true batch crossing** mixes short and long synthetic
durations. The question: how do lane assignment, completion latency, and tails
behave when the durations inside one batch are heterogeneous? Experiment/report
slice only: no new RayX API, no driver/analyzer change, no result-row or v1
benchmark-JSONL schema change.

Companions: `docs/reference/rayx_actor_api.md` / `rayx_frontend_design.md` (the
varied batch contract), `experiments/08_spin_vs_sleep_coordination/` (sleep
overshoot vs spin fidelity), `experiments/09_spin_core_boundary_sweep/` (core
boundary).

## 1. Setup

* **Submit:** one true `submit_batch` crossing — varied `service_ms=[...]` (or
  scalar for the uniform baseline). The facade stamps every returned future with
  **one shared `submit_ns`**; the runner gates on this (proving a single crossing,
  not a Python loop over `submit()`).
* **Retire:** as-completed (`Engine.wait`, per-sweep `recv_ns`), rows
  reconstructed in input order, so `total_ms` reflects true completion latency
  (isolating lane-queue effects from client retire order).
* **Matrix:** `work_mode ∈ {sleep, spin}` × `num_lanes ∈ {4, 8}` × patterns ×
  **3 repeats**, `batch_size=128`, `hpx_threads=4`. Patterns: `uniform5`
  (scalar, all 5 ms), `alt_1_10` (`[1,10,1,10,…]`), `block_sl_1_10` (first half
  1 ms, second half 10 ms), `block_ls_1_10` (first half 10 ms, second half 1 ms),
  `rare_long_1_20` (every 10th request 20 ms, rest 1 ms). → **60 runs**.
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all 60 passed):** one shared `submit_ns`; `completed == 128`;
  per-lane FIFO order (sorting a lane's rows by C++ `end_ns` yields increasing
  submission index — a structural, timing-robust order check); round-robin lane
  balance (`lanes_seen == num_lanes`, per-lane count spread ≤ 1); short/long
  `service_ms_observed` separation (`long_p50 ≥ 2×short_p50`).
* **Reproduce:** `python experiments/10_varied_batch_service_time/run_varied_batch_service_time.py`
  (raw JSONL → `results/`, gitignored; `--quick` for a tiny smoke).

## 2. Measured facts (medians; spin shown — sleep mirrors it, inflated ~25% by
the sleep-timer overshoot of experiment 08)

### 2a. Throughput and batch completion (`spin`)

| pattern | L4 thr (req/s) | L4 complete (ms) | L8 thr | L8 complete |
|---|---|---|---|---|
| uniform5       | 799  | 160 | 1531 | 84 |
| block_sl_1_10  | 727  | 176 | 1357 | 94 |
| block_ls_1_10  | 727  | 176 | 1433 | 89 |
| rare_long_1_20 | 775  | 165 | 1390 | 92 |
| **alt_1_10**   | **400** | **320** | **798** | **160** |

### 2b. Short- vs long-request total latency, `total_ms_p50` (`spin`, L4)

| pattern | short `t_p50` (ms) | long `t_p50` (ms) |
|---|---|---|
| block_sl_1_10  | **8.6**   | 101.1 |
| block_ls_1_10  | **168.6** | 85.1 |
| alt_1_10       | 16.6  | 165.1 |
| rare_long_1_20 | 25.1  | 92.1 |

The **same 1 ms short request** has `t_p50` ≈ **8.6 ms** when shorts are submitted
first (`block_sl`) but ≈ **168.6 ms** when submitted after the longs
(`block_ls`) — a ~20× swing driven purely by input **order**.

### 2c. Lane assignment (`actor_id`, L4, 128 requests)

Round-robin balances request **count** (every cell: 32/lane at L4, 16/lane at
L8, spread ≤ 1) but not necessarily **work**:

| pattern | long requests per lane (4 lanes) | long-work spread |
|---|---|---|
| block_sl / block_ls | 16, 16, 16, 16 | **even** |
| alt_1_10            | 32, 0, 32, 0   | **segregated** (longs on 2 lanes) |
| rare_long_1_20      | 7, 0, 6, 0     | **segregated** (longs on 2 lanes) |

Because lane = `idx % num_lanes`, when the pattern's long-index **period**
correlates with `num_lanes` (period 2 in `alt`, period 10 in `rare_long`, both
even — like 4 and 8), the long requests land on a **subset** of lanes.

## 3. Interpretation

1. **Does true varied batch preserve the bulk contract?** **Yes.** Every cell
   gated clean: all 128 rows share **one** `submit_ns` (a single Python→C++
   crossing, not a loop), per-lane completion is strict FIFO by submission index,
   and all requests complete. The varied path is a faithful bulk crossing.

2. **Do short requests suffer behind long ones depending on input pattern?**
   **Yes — strongly.** With a shared batch `submit_ns`, a request's latency
   includes draining everything ahead of it **on its round-robin lane**. So the
   identical 1 ms short request costs ~8.6 ms (`block_sl`, shorts first) vs
   ~168.6 ms (`block_ls`, shorts behind 16 longs/lane) at L4 spin — head-of-line /
   convoy behavior set by submission order.

3. **Does round-robin lane assignment explain the latency/tail behavior?**
   **Yes, completely.** Two mechanisms, both from `idx % num_lanes` + FIFO-per-lane:
   (a) **convoy** — submission order = queue position on each lane, so order
   dictates short-request latency (Q2); (b) **work segregation** — when the long
   indices' period aligns with the lane count, longs pile onto a subset of lanes
   (`alt`: 2 of 4 lanes do all the 10 ms work), so the batch finishes only when
   the heavy lanes drain. That halves `alt` throughput (~400 vs ~727 req/s at L4)
   even though every lane has the same request **count**. `p99 ≈ batch completion`
   throughout: the tail is the last/heaviest lane, not a pathology.

4. **Does `spin` differ from `sleep` for varied batches?** The **structure** is
   identical (same convoy and segregation). `spin` gives exact service (no ~25%
   sleep overshoot, experiment 08) and therefore higher throughput and cleaner
   numbers (e.g. `block_ls` short `t_p50` ~168.6 ms spin vs ~203.6 ms sleep, the
   sleep figure inflated by the overshoot of the longs ahead). No new mode-specific
   effect beyond experiment 08's overshoot and a little experiment-09 L8
   oversubscription noise.

5. **What this says about RayX synthetic workloads — and what not to claim.**
   * **Claim:** the varied batch is a faithful single-crossing heterogeneous-
     duration probe; within a shared-`submit_ns` batch, per-request latency is
     governed by round-robin lane placement plus FIFO queue position, so input
     **order** and the **pattern-period-vs-lane-count** alignment dominate
     short-request latency and batch throughput. Count-balanced routing is **not**
     work-balanced.
   * **Do not claim:** anything about real model inference (durations are
     synthetic timing control, not payloads or tasks); any Ray comparison (none
     run here); any OS-scheduler mechanism; or portability of absolute numbers
     (one local 10-core machine). The list is durations, never work items.

## 4. Scoped takeaways

* Varied batch behaves exactly as the bulk contract promises: one crossing,
  shared `submit_ns`, FIFO-per-lane, balanced round-robin counts.
* Short-request latency in a varied batch is an **order** phenomenon (convoy),
  swinging ~20× between short-first and short-last submission.
* Throughput is a **work-balance** phenomenon: patterns whose long-index period
  aligns with the lane count segregate heavy work onto a lane subset and lose up
  to ~½ throughput, despite equal per-lane request counts.
* `sleep` vs `spin` change the magnitudes (overshoot), not the structure.

## 5. Caveats / non-claims

* Single macOS laptop, 10 cores (4 P + 6 E); magnitudes are machine-specific.
* Synthetic CPU/parked durations, not model inference; `service_ms` is timing
  control, never a payload/work item.
* rayx frontend only (`Engine.submit_batch`); no Ray comparison.
* `total_ms` is queue-shaped (shared batch `submit_ns`); it is the intended batch
  latency shape, and `p99 ≈ batch completion` is structural.
* Spin L8 carries mild oversubscription noise (experiment 09); medians are the
  firm signal.
* No OS-scheduler mechanism claim.
* Raw per-request JSONL is an experiment-local scratch format (not the v1
  benchmark schema) under `results/` (gitignored); the curated `aggregate.json`
  beside this note is the tracked evidence.
