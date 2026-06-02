# Queued-Only Cancellation Under Backlog

Characterizes the queued-only cancellation primitive added to the rayx frontend
— `engine.cancel(future) -> bool` / `actor.cancel(future) -> bool`,
`future.cancelled() -> bool`, and `status="cancelled"`. The question:

> Under backlog, can RayX cancel queued synthetic requests **before service
> starts**, and does cancellation reduce drained work **without pretending to
> interrupt running work**?

Experiment/report slice only: **no** new RayX API, **no** driver change, **no**
result-row / v1 benchmark-JSONL schema change. The per-run JSONL written here is
an experiment-local scratch format. Companion: `docs/reference/rayx_frontend_design.md`
§7 (the cancellation contract) and `experiments/10_varied_batch_service_time/`
(round-robin work segregation, which reappears here).

## 1. Setup

* **Submit:** the benchmark driver deliberately never cancels, so this uses an
  experiment-local runner driving the facade directly. Each cell submits all
  `N` requests via `Engine.submit` (single-request, cancelable) **without
  retiring**, so a deep FIFO backlog builds on each round-robin lane.
* **Cancel policy** (applied after submission):
  * `none` — baseline, no cancellation.
  * `tail_half` — immediately cancel idx `[N/2, N)`. Those are deep in the queue
    (`idx ≥ N/2 ≫ num_lanes`), so within the ~ms it takes to submit `N` requests
    the lanes cannot have reached them: **structurally all-True**.
  * `alt` — immediately cancel odd idx. Most are queued (True), but the first
    `num_lanes` requests start at once, so an early odd target may already be
    running (False).
  * `delayed_front` — wait `2 × service_ms`, **then** cancel idx `[0, N/2)`. By
    then each lane has serviced its front request(s), so the front yields some
    **False** (already started → completes) and some **True** (still queued →
    cancelled).
* **Retire:** as-completed (`Engine.wait`, per-sweep `recv_ns`), rows
  reconstructed in submission order.
* **Matrix:** `work_mode ∈ {sleep, spin}` × `num_lanes ∈ {1, 4, 8}` × the 4
  policies, `service_ms = 20`, `hpx_threads = 4`, `N = 64`, **3 repeats** →
  **72 runs / 24 cells**. Medians across repeats.
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all 24 cells passed):**
  1. **Invariant:** `cancel()` returned `True` **iff** the row is
     `status="cancelled"` (and `Future.cancelled()` agrees) — every request.
  2. Cancelled rows: `service_ms_observed ≈ 0` (service skipped) and **label
     preserved**.
  3. Completed rows ran real service (`≥ 0.5 × service_ms`).
  4. `tail_half`: every attempt succeeded, **0 False**. `delayed_front`:
     **≥1 False and ≥1 True** (the boundary). `alt`: ≥1 success. `none`: zero
     cancels, all completed.
  5. Round-robin lane balance over **all** requests (cancelled rows carry their
     assigned lane's `actor_id`), spread ≤ 1.
  6. Analyzer additive `cancelled` bucket matches counts (schema-1 projection).
* **Reproduce:** `python experiments/11_queued_cancellation/run_queued_cancellation.py`
  (raw JSONL → `results/`, gitignored; `--quick` for a tiny smoke).

## 2. Measured facts (medians; `spin` shown — `sleep` mirrors it, service
inflated ~25% by the sleep-timer overshoot of experiment 08)

### 2a. Cancellation outcomes (`spin`, attempts / success / False)

| policy | L1 ok/no | L4 ok/no | L8 ok/no | cancelled | completed |
|---|---|---|---|---|---|
| none          | 0 / 0   | 0 / 0    | 0 / 0    | 0  | 64 |
| tail_half     | 32 / 0  | 32 / 0   | 32 / 0   | 32 | 32 |
| alt           | 32 / 0  | 30 / 2   | 29 / 3   | ~30 | ~34 |
| **delayed_front** | **29 / 3** | **20 / 12** | **8 / 24** | varies | varies |

`tail_half` cancels all 32 deep-queue targets at every lane count. `delayed_front`
is the boundary probe: with a **fixed** `2 × service_ms` wait, **more lanes drain
more of the front before the cancel arrives**, so successful cancels fall
(29 → 20 → 8) and `False` rises (3 → 12 → 24). Cancelling already-running work
fails — by design.

### 2b. Drain wall time (ms; `spin`), cancellation vs the `none` baseline

| policy | L1 | L4 | L8 |
|---|---|---|---|
| none          | 1280 | 320 | 161 |
| tail_half     | **640** | **160** | **80** |
| alt           | 640 | 320 | 160 |
| delayed_front | 700 | 220 | 140 |

`tail_half` **halves** drain at every lane count: the 32 cancelled requests spend
**no** service, and the 32 survivors (idx `[0,32)`) round-robin evenly. `alt`
cancels ~half too, yet its wall barely improves at **even** lane counts — see 2d.

### 2c. Service split and label preservation

* **Cancelled rows:** `service_ms_observed` p50 = **0.0** in every cancelling
  cell (`sleep` and `spin` alike) — service was skipped, no time spent.
* **Completed rows:** p50 = **20.0** (`spin`, exact) / **~25.0** (`sleep`,
  overshoot) — normal service.
* **Labels preserved:** every row's echoed `label` matched its submit-time label
  (cancelled and completed alike) — gated on all 72 runs.

### 2d. Lane distribution (why `alt` drain doesn't fall)

Round-robin balances request **count** across lanes for every policy (spread ≤ 1,
including cancelled rows). But `alt` cancels **odd indices**, and with
`lane = idx % num_lanes` at an **even** lane count the even (surviving) indices
land only on **even lanes**: at L4, lanes 0 and 2 each run all 16 of their
requests (16 × 20 ms = 320 ms) while lanes 1 and 3 sit idle. So `alt`'s surviving
work is **lane-segregated** exactly as in experiment 10 (long-index period vs
lane-count alignment) — count-balanced cancellation is **not** work-balanced.
`tail_half` avoids this because its survivors `[0,32)` cover all lane residues.

## 3. Interpretation

1. **Does queued cancellation skip service for not-yet-started requests?**
   **Yes.** Every `cancel()==True` request is `status="cancelled"` with
   `service_ms_observed ≈ 0` (2c), and the invariant `True ⇔ cancelled` held on
   all 64 requests in all 72 runs (gate 1). The lane skips the request entirely.

2. **Does cancellation reduce drained work under backlog?** **Yes, when the
   survivors are balanced.** `tail_half` halves drain wall at every lane count
   (2b) because the cancelled half spends no service. The reduction tracks
   *which* requests survive, not just how many: `alt` cancels just as many but
   barely cuts wall at even lane counts because its survivors segregate onto
   half the lanes (2d) — a routing effect (experiment 10), not a cancellation
   failure.

3. **Does delayed cancellation show the queued-vs-running boundary honestly?**
   **Yes.** With a fixed wait, successful cancels fall and `False` rises as lanes
   drain the front faster (2a: L1 29/3 → L8 8/24). Every `False` request
   **completed normally** (gate 1 invariant) — running work was never
   interrupted. This is the honest boundary: you can cancel what a lane has not
   started, and only that.

4. **Does behavior differ between `sleep` and `spin`?** Not structurally. The
   invariant, the skipped-service result, and the policy outcomes are identical;
   `spin` gives exact service (20.0) and `sleep` inflates completed service ~25%
   (the experiment-08 overshoot) and starts marginally fewer requests in a fixed
   delay. Cancellation itself is mode-agnostic — a cancelled request never
   reaches either service path.

5. **What should users understand vs Ray cancellation?** RayX cancellation
   cancels **one queued synthetic request before its lane starts it**. It is
   **not** Ray task/object cancellation: there is no task graph, no object-store
   value to drop, and **running work is not interruptible**. `cancel()` is not a
   retire — a cancelled future still becomes ready and is consumed once via
   `result()`/`get()`, returning a `status="cancelled"` row (with its label).

6. **What came after this experiment.** Cancelling **running** work. At the time
   of experiment 11 the synthetic service was atomic (one `sleep_for` / one
   `spin_for`, no interruption point), so `delayed_front`'s `False` cancels are
   the honest evidence of that queued-vs-running boundary. The natural
   cancellation points it needed arrived as **chunked service** (experiment 12);
   **experiment 13** then cancels a *running* chunked request at its next chunk
   boundary (`1 ≤ chunks_completed < chunks`), still never interrupting an
   in-progress active chunk or parked gap. This experiment itself remains
   queued-only — its running work is uninterruptible.

## 4. Scoped takeaways

* Queued-only cancellation works exactly as contracted: `True ⇔ status="cancelled"`,
  skipped service, labels preserved, and cancelled futures retire through
  `wait`/`as_completed`/`get` like any other.
* Under backlog, cancelling not-yet-started requests **removes their service
  entirely** and can halve drain — but the *benefit* depends on which survive
  (count-balanced cancellation is not work-balanced; cf. experiment 10).
* Delayed cancellation honestly exposes the queued-vs-running boundary: more
  in-flight progress → fewer cancellable → more `False`; those requests complete.
* Running work is **never** interrupted; mid-flight cancellation awaits a future
  streaming/chunked-service axis.

## 5. Caveats / non-claims

* Single macOS laptop, 10 cores (4 P + 6 E); wall magnitudes are
  machine-specific. The structural gates (the invariant, skipped service, label
  preservation, lane balance) are the firm signals; exact `delayed_front`
  success/False counts are scheduling-dependent and **reported, not gated**.
* Synthetic durations only — `service_ms` is timing control, never a payload or
  work item. Cancellation cancels a queued *request*, not a task.
* **Not** Ray task/object cancellation; **no** object-store semantics; running
  work is **not** interruptible; **no** OS-scheduler mechanism is claimed (lane
  progress during the delay is observed, not attributed to a scheduler model).
* Single-request facade only (`Engine.submit` / `actor.remote`); **batch
  futures are not cancelable** and none are cancelled here. No Ray comparison.
* Raw per-run JSONL is an experiment-local scratch format under `results/`
  (gitignored), **not** the v1 benchmark schema; the curated `aggregate.json`
  beside this note is the tracked evidence.
