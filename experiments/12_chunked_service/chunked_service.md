# Chunked Synthetic Service

Validates and characterizes the v1 **chunked synthetic-service** primitive on the
rayx frontend: `engine.submit(service_ms, chunks, chunk_delay_ms, work_mode)`
(and the `remote` / `serve.remote` forwards). A request services in `chunks`
equal **active** steps — total active `= service_ms`, split `chunks` ways — with
`chunks-1` **parked** inter-chunk gaps of `chunk_delay_ms`. The question:

> Does chunking split active service without changing total active work, and does
> the inter-chunk delay add lane-occupancy ≈ `(chunks-1) × chunk_delay_ms`, while
> a request still returns exactly **one** row?

This is **synthetic timing only** — **not** real token streaming, not payload
execution, no per-chunk rows/events/callbacks (experiment 12 itself never
cancels; the chunk boundaries it creates are where experiment 13 later adds
running cancellation). Experiment/report slice only: no result-row / v1
benchmark-JSONL schema change
(the benchmark JSONL already reserved `chunks` / `chunk_delay_ms`; drivers still
emit `1` / `0`). Companion: `docs/reference/rayx_frontend_design.md` §8.

## 1. Setup

* **Submit:** the benchmark driver stays unchunked, so this uses an
  experiment-local runner driving the facade directly. Each cell submits `N`
  single chunked requests (a backlog), drains as-completed (`Engine.wait`,
  per-sweep `recv_ns`), and records **one row per request**.
* **Signal:** `service_ms_observed = (end_ns - start_ns)/1e6` (C++ steady clock)
  is the per-request **lifecycle span** — active service **plus** the parked
  inter-chunk gaps — and is **queue-position-independent** (lane-side), so it is
  the clean chunk signal. `total_ms` is queue-shaped under the backlog and is
  reported only for context.
* **Matrix:** `work_mode ∈ {sleep, spin}` × `num_lanes ∈ {1, 4, 8}` ×
  `chunks ∈ {1, 2, 4, 8}` × `chunk_delay_ms ∈ {0, 2}`, `service_ms = 8` (total
  active), `hpx_threads = 4`, `N = 48`, **3 repeats** → **144 runs / 48 cells**.
  Medians across repeats.
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all 48 cells passed):**
  1. One submitted request → **exactly one** final row (`len == N`; no per-chunk
     rows/events).
  2. Each row **echoes** `chunks` / `chunk_delay_ms` matching the request.
  3. All rows `completed` (no cancellation here).
  4. Label preserved on every row.
  5. **spin, delay=0:** `service_ms_observed` is chunk-invariant — splitting the
     same total active service into more chunks does not change it (band check).
  6. **delay>0:** lifecycle grows by approximately `(chunks-1) × chunk_delay_ms`
     vs the same cell at delay=0 (loose band, overshoot-aware); for `chunks=1`
     the delta is ~0 (no gaps).
  7. Round-robin lane balance over all requests (spread ≤ 1).
* **Reproduce:** `python experiments/12_chunked_service/run_chunked_service.py`
  (raw JSONL → `results/`, gitignored; `--quick` for a tiny smoke).

## 2. Measured facts (medians)

### 2a. `service_ms_observed` p50 — `spin` (lane-independent; L1 shown)

| chunks | delay=0 | delay=2 | delta |
|---|---|---|---|
| 1 | **8.000** | 8.000  | 0.00 |
| 2 | **8.000** | 10.52  | +2.52 |
| 4 | **8.000** | 15.55  | +7.55 |
| 8 | **8.000** | 25.60  | +17.60 |

At delay=0 the observed active service is **exactly 8.000 ms for every chunk
count** — chunking splits the work without changing total active service. The
delay column rises by ≈ `(chunks-1) × 2 ms × ~1.25`; the `~1.25` is the
sleep-timer overshoot the **parked** inter-chunk gap carries (the gap is a
blocking sleep in both modes).

### 2b. `service_ms_observed` p50 — `sleep` (L1)

| chunks | delay=0 | delay=2 |
|---|---|---|
| 1 | 10.02 | 10.02 |
| 2 | 10.03 | 12.54 |
| 4 | 10.06 | 17.60 |
| 8 | 10.12 | 27.70 |

Sleep mirrors spin's structure with the ~25% sleep overshoot folded in: active is
~10 ms (8 ms × ~1.25) and barely rises with chunk count (overshoot is roughly
proportional, so splitting the sleep adds little), and the delay column again
grows by ≈ `(chunks-1) × 2 ms × ~1.25`.

### 2c. Lane independence and balance

`service_ms_observed` p50 is the same across `num_lanes ∈ {1,4,8}` (e.g. spin
delay=0 = 8.000 at L1/L4/L8) — it is lane-side lifecycle, not affected by lane
count or queue position. Round-robin balance is exact in every cell
(`N/lanes`: L1=48, L4=12, L8=6 per lane). `total_ms` is queue-shaped under the
backlog (e.g. spin L1 chunks=1 `tot_p50` ≈ 196 ms ≈ the 24th request × 8 ms) and
is not the chunk signal.

## 3. Interpretation

1. **One request → one row?** Yes (gate 1, all 48 cells). Chunking is a
   multi-step lifecycle *within one* request, not multiple outputs: there are no
   per-chunk rows or events, and the single row echoes `chunks` / `chunk_delay_ms`.

2. **Does `chunks` split active service without changing total active?** Yes. At
   delay=0, spin `service_ms_observed` is **exactly 8.000 ms for chunks 1/2/4/8**
   (2a); sleep is ~10 ms and rises only marginally with chunk count (per-sleep
   overshoot accumulating, 2b). Splitting `service_ms` `chunks` ways preserves
   total active work.

3. **Does `chunk_delay_ms` add lane-occupancy ≈ `(chunks-1) × delay`?** Yes. The
   delay column grows monotonically with chunk count by ≈ `(chunks-1) × delay`
   (2a/2b), inflated by the parked-gap sleep overshoot. So with `chunk_delay_ms >
   0`, `service_ms_observed` is **lifecycle/lane-occupancy time** (active +
   parked gaps), **not** active-only — the row's echoed `chunks` / `chunk_delay_ms`
   let an analyst recover the split (active ≈ `service_ms`, delay ≈
   `(chunks-1) × chunk_delay_ms × overshoot`).

4. **`sleep` vs `spin`?** Identical structure. `spin` gives exact active service
   (8.000) and an exact-modulo-overshoot delay; `sleep` folds the ~25% sleep
   overshoot into both active and the parked gaps. No new mode-specific effect.

5. **What it is / is not.** Chunked service models a multi-step serving lifecycle
   (token-like cadence) as **timing only**. It returns one future and one row,
   holds the lane for its whole lifecycle (FIFO preserved), and is single-request
   only (batch is unchunked).

6. **What came next.** Running-cancellation. The chunk boundaries this slice
   created are exactly the checkpoints a running cancel needs: **experiment 13**
   wires a between-chunks stop, so cancelling a *started* chunked request halts it
   at its next boundary (`1 ≤ chunks_completed < chunks`) without interrupting an
   in-progress active chunk or parked gap. (In experiment 12 itself there is no
   cancellation — every request completes.)

## 4. Scoped takeaways

* `chunks` splits total active service cleanly (spin: exactly, every chunk count);
  `chunk_delay_ms` adds parked lane-occupancy ≈ `(chunks-1) × delay`.
* `service_ms_observed` is lifecycle/lane-occupancy time when `chunk_delay_ms > 0`
  — not active-only — and is lane-independent; the echoed `chunks` / `chunk_delay_ms`
  make the decomposition recoverable.
* One request → one row → one future throughout; lanes stay perfectly balanced.
* `sleep` and `spin` differ only by the sleep-timer overshoot, not in structure.

## 5. Caveats / non-claims

* **Not real token streaming**, **not Ray streaming**, **no callbacks/events**,
  **no per-chunk rows**, **no cancellation in this experiment** (running
  cancellation at these boundaries is experiment 13) — synthetic timing only;
  `service_ms` is duration control, never a payload or token.
* Single macOS laptop, 10 cores (4 P + 6 E); timing magnitudes (including the
  ~25% parked/sleep overshoot) are machine-specific. The structural gates (one
  row per request, chunk echo, chunk-invariance at delay=0, the loose
  `(chunks-1)×delay` band, lane balance) are the firm signals.
* Single-request facade only; **batch is unchunked** and rejects `chunks` /
  `chunk_delay_ms`. No Ray comparison.
* Benchmark JSONL schema version stays `1`; benchmark drivers still emit
  `chunks=1` / `chunk_delay_ms=0` (chunking is a facade feature). Raw per-run
  JSONL here is an experiment-local scratch format under `results/` (gitignored),
  **not** the v1 benchmark schema; the curated `aggregate.json` beside this note
  is the tracked evidence.
