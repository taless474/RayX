# Lane Backlog Observability via `lane_stats()`

Uses the rayx observability snapshot `Engine.lane_stats()` — one
`{actor_id, queue_depth, active}` row per `ServiceLane` — to watch live lane
backlog and active state under offered load. The question:

> Under a burst that overfills the lanes, does `lane_stats()` honestly show the
> backlog distributed **round-robin** and draining **FIFO** to idle, and what
> does it show for queued requests that are **cancelled before they start**?

Observability/experiment slice only: **no** new RayX API, **no** `ServiceLane`
semantics change, **no** result-row / v1 benchmark-JSONL schema change, **no**
analyzer change, and **nothing** touching the experiment-16 `HpxLane` mechanism
probe. `lane_stats()` is a **snapshot** and is **non-consuming** (it touches no
`Future`), so the lanes drain on their own while we sample; we retire the futures
only afterwards to confirm per-request outcomes. Companion:
`docs/reference/rayx_frontend_design.md` §11 (the `lane_stats()` contract),
`experiments/11_queued_cancellation/` (the queued-cancel boundary reused here),
and `experiments/10_varied_batch_service_time/` (round-robin routing).

## 1. Setup

* **Instrument:** `Engine.lane_stats()` — a per-lane snapshot of
  `queue_depth` (**queued but not yet started**; the in-service request is not
  counted) and `active` (**`True` once the lane has popped a request** and is in
  its service lifecycle, until that row is fulfilled). It briefly takes each
  lane's mutex, is non-consuming, and can race — treat each call as a live photo.
* **Submit:** the benchmark driver never inspects lanes, so this uses an
  experiment-local runner driving the facade directly. Each cell warms up, then
  **burst-submits `N = lanes × 8` sleep requests via `Engine.submit` without
  retiring**, so a deep FIFO backlog builds on each round-robin lane (one in
  service per lane, the rest queued).
* **Sample:** after the burst, call `lane_stats()` on a ~1 ms cadence until every
  lane is idle, recording `(t_rel_ms, per-lane queue_depth/active, total_queue,
  num_active)`. The cadence is just a sampling interval — **no gate is a timing
  threshold**. Then retire all futures (input order) to read per-request status.
* **Scenarios:**
  * `drain` — submit the burst and watch it drain.
  * `cancel_tail` — immediately cancel the tail half (idx `[N/2, N)`). Those sit
    deep in each lane's queue (position ≥ 4 in an 8-deep lane), so the lanes
    cannot have started them in the ~ms it takes to submit `N`: **every such
    cancel is a queued cancel** (`True`).
* **Matrix:** `work_mode = sleep` × `num_lanes ∈ {2, 4}` × `scenario ∈ {drain,
  cancel_tail}`, `service_ms = 40`, `hpx_threads = 4`, `N = lanes × 8`, **3
  repeats** → 12 runs / 4 cells. Medians across repeats; the representative
  trajectory/snapshots in `aggregate.json` are from repeat 0.
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all 4 cells passed; structural, timing-robust):**
  1. **Round-robin backlog** — at the first all-active snapshot: `num_active ==
     lanes`, per-lane `queue_depth` spread ≤ 1, `total_queue == N − lanes`.
  2. **Monotonic drain** — the recorded `total_queue` sequence is non-increasing
     (no work is ever re-queued; lanes only pop).
  3. **Reaches idle** — the final snapshot has `num_active == 0`, `total_queue == 0`.
  4. **Outcomes** — `drain`: `completed == N`, `cancelled == 0`. `cancel_tail`:
     `cancelled == N/2` (each ~0 service), `completed == N/2` (real service), and
     every tail cancel was queued (`True`).
  5. **Round-robin over all requests** — per-lane assigned-request counts (from
     row `actor_id`, cancelled rows included) span ≤ 1 across exactly `lanes` lanes.
* **Reproduce:**
  `python experiments/17_lane_stats_observability/run_lane_stats_observability.py`
  (raw per-run JSON → `results/`, gitignored; `--quick` for a tiny smoke).

## 2. Measured facts (medians; representative trajectory from repeat 0)

### 2a. First all-active snapshot — round-robin backlog

| scenario | lanes | N | `num_active` | per-lane `queue_depth` | `total_queue` (N−lanes) | spread |
|---|---|---|---|---|---|---|
| drain        | 2 | 16 | 2 | `[7, 7]`         | 14 / 14 | 0 |
| drain        | 4 | 32 | 4 | `[7, 7, 7, 7]`   | 28 / 28 | 0 |
| cancel_tail  | 2 | 16 | 2 | `[7, 7]`         | 14 / 14 | 0 |
| cancel_tail  | 4 | 32 | 4 | `[7, 7, 7, 7]`   | 28 / 28 | 0 |

Every lane is `active` and carries an **identical** `queue_depth` (spread 0): the
burst is split round-robin, one request in service per lane and the rest queued.
`total_queue` is exactly `N − lanes` (the `lanes` in-service requests are not
counted). **The two `cancel_tail` rows are identical to their `drain` rows** —
cancelling the tail did **not** change `total_queue` (see §2c).

### 2b. Drain trajectory and outcomes

| scenario | lanes | drain span (ms) | completed | cancelled | completed svc p50 (ms) | cancelled svc p50 (ms) |
|---|---|---|---|---|---|---|
| drain        | 2 | 346.9 | 16 | 0  | 43.98 | — |
| drain        | 4 | 356.6 | 32 | 0  | 43.93 | — |
| cancel_tail  | 2 | 172.4 | 8  | 8  | 41.21 | 0.0 |
| cancel_tail  | 4 | 176.3 | 16 | 16 | 42.86 | 0.0 |

`drain` empties FIFO: `total_queue` steps `14 → 12 → 10 → … → 0` (L2) and
`28 → 24 → 20 → … → 0` (L4) while `num_active` stays at `lanes`, then both drop to
0 together. Each lane services its 8 requests at ~44 ms sleep (the
experiment-08 ~10 % overshoot on a 40 ms sleep), so the span is ~8 × 44 ≈ 350 ms
at **both** lane counts — more lanes do more concurrent work, not faster per
request. Completed service p50 ≈ 44 ms; nothing was cancelled.

`cancel_tail` drains in **~half** the wall (172 / 176 ms) and ends with
`completed == cancelled == N/2`: the front half ran real ~41 ms service while the
cancelled tail spent **0 ms** (`cancelled_service_ms_p50 = 0.0`).

### 2c. The `cancel_tail` subtlety — `queue_depth` still counts cancelled items

`cancel()` does **not** remove an item from the lane deque; it arms a queued
cancel that the lane honors when it later **pops** the request and **skips**
service. So `queue_depth` keeps counting a cancelled-but-not-yet-popped request.
The data shows this directly:

* **No immediate drop.** The first all-active `total_queue` is `N − lanes`
  (14 / 28) **with or without** the tail cancel (§2a). Cancelling 8 (L2) / 16
  (L4) queued requests did not lower `queue_depth` at all.
* **A late cliff, not a step.** The `cancel_tail` trajectory steps down only as
  the **front half** services (L2: `14 → 12 → … → 8` over ~140 ms), then
  **collapses `8 → 0` in a single sample** as the lanes reach the cancelled tail
  and pop-and-skip all of it near-instantly. (L4 is the same shape:
  `28 → 24 → … → 16`, then `16 → 0`.) The cancelled requests occupied
  `queue_depth` the whole time; they left it only on pop.

So cancellation's visible signature in `lane_stats()` is a **faster drain /
skipped service later**, not a `queue_depth` drop at `cancel()` time. This is the
honest reading of an observability snapshot, and exactly why `lane_stats()` is
not a queue-control surface.

## 3. Interpretation (answering the experiment's six questions)

1. **`queue_depth` means queued-but-not-started.** Every first all-active
   snapshot reports `queue_depth = per-lane count − 1` (`[7,7]` / `[7,7,7,7]` for
   8 requests/lane): the one in-service request per lane is excluded, the other 7
   are queued. `total_queue == N − lanes` confirms it (gate 1).
2. **`active` means popped/in-service.** At the first snapshot exactly `lanes`
   lanes are `active` — one popped request each — and at idle every lane is
   `active = False` with `queue_depth = 0` (gate 3). `active` tracks the
   popped-to-fulfilled lifecycle, nothing else.
3. **Backlog distribution follows round-robin.** Per-lane `queue_depth` spread is
   **0** in all cells, and per-lane assigned-request counts (cancelled rows
   included) also span ≤ 1 across exactly `lanes` lanes (gate 5): the burst is
   split evenly by the internal round-robin, visible live in the snapshot.
4. **Stats are snapshots and can race.** Consecutive ~1 ms samples differ as
   lanes pop (the trajectories in §2b are nothing but that change over time), and
   a lane shown `active` can fulfil its request the instant after the read. We
   never gate on a single value being stable; gate 2 only asserts the **direction**
   of change (non-increasing), which holds precisely because no work is
   re-queued. The snapshot is a live photo, not a latched count.
5. **Observability only — not scheduler state or synchronization.** The
   `cancel_tail` result (§2c) is the clearest proof: `queue_depth` reflects what
   is *in the deque*, not what is *logically pending*, so it cannot be used to
   drive or gate behavior. `lane_stats()` reads lane state; it does not choose
   lanes (placement stays internal round-robin — `actor_id` reports the serving
   lane, it is not a handle you submit to), does not wait, and is not a Ray
   scheduler view. Use `wait` / `as_completed` / `ready` for coordination.
6. **No JSONL / analyzer / schema change.** `lane_stats()` is a separate Python
   call returning a list of dicts; it is **not** a per-request result-row field
   and touches neither the v1 benchmark JSONL nor the analyzer. The raw per-run
   JSON this runner writes is an experiment-local scratch format under `results/`
   (gitignored), distinct from the v1 schema.

## 4. Scoped takeaways

* `lane_stats()` honestly visualizes a round-robin, FIFO-draining backlog:
  even per-lane `queue_depth` at the burst, `num_active == lanes` while busy, a
  monotonic drain, and a clean return to idle.
* `queue_depth` is a deque occupancy count, not a logical-pending count:
  cancelled-but-unpopped requests still count, and cancellation shows up as a
  **later, faster drain** (a `total_queue` cliff when the lane skips the tail),
  not an immediate drop. Treat the snapshot as observability, never control.
* The snapshot is live and racy by construction; only direction-of-change and
  endpoint (idle) are stable enough to assert — and those are exactly what the
  gates use.

## 5. Caveats / non-claims

* Single macOS laptop, 10 cores (4 P + 6 E); drain-span magnitudes are
  machine-specific. The firm signals are the structural gates (round-robin
  backlog, monotonic drain, idle endpoint, cancel outcomes); the wall numbers are
  reported, not gated.
* Synthetic `sleep` timing only — `service_ms` is lane-occupancy control, never a
  payload or work item, and the ~44 ms observed on a 40 ms sleep is the
  experiment-08 blocking-sleep overshoot, not real work.
* `lane_stats()` is **observability only**: not Ray scheduler state, not placement
  control, not a synchronization primitive, and not part of the v1 JSONL /
  analyzer schema. Never gate correctness on it.
* This experiment measured the default rayx `ServiceLane` backend
  (`lane_impl="std"`). `lane_stats()` itself is backend-agnostic and reports
  whichever backend the Engine was built with; the opt-in cooperative `HpxLane`
  backend (`lane_impl="hpx"`) is exercised later in exp21/22/23, not here. No Ray
  comparison and no `HpxLane` backend in this experiment.
* Raw per-run JSON is an experiment-local scratch format under `results/`
  (gitignored), **not** the v1 benchmark schema; the curated `aggregate.json`
  beside this note is the tracked evidence.
