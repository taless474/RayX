# Chunk-Boundary Running Cancellation

Validates and characterizes **running (chunk-boundary) cancellation** layered on
top of the existing queued cancellation. `Engine.cancel(future)` now settles two
outcomes on a single-request chunked future:

* **Queued** → the lane skips the request entirely (`chunks_completed == 0`).
* **Running** → a *started* chunked request with a chunk boundary still ahead
  stops at the **next boundary** (`1 <= chunks_completed < chunks`).

The question:

> Does cancelling a *running* chunked request stop it at a chunk boundary —
> running a strictly-partial set of chunks, never interrupting an in-progress
> active chunk or parked gap — while preserving the "one request → one row" and
> the `cancel()==True` ⟺ `status=="cancelled"` contracts?

Running-cancel for a running request means **"guaranteed to stop at the next
boundary," not "ready now."** It is **synthetic timing only** — **not** real
token-stream cancellation, **not** Ray task/object cancellation, **no** per-chunk
events. Experiment/report slice only: the facade result row gains the
lane-determined `chunks_completed`; the **v1 benchmark JSONL is unchanged**
(version `1`, unchunked, never cancelled). Companion:
`docs/reference/rayx_frontend_design.md` §7.

## 1. Setup

* **Submit / cancel:** the benchmark driver never chunks or cancels, so this uses
  an experiment-local runner driving the facade directly. Each cell runs **four
  policies** sequentially in one engine (one HPX runtime per process), drained
  as-completed (`Engine.wait`, per-sweep `recv_ns`), **one row per request**.
* **Signal:** the structural row fields — `status`, lane-determined
  `chunks_completed`, the boolean `cancel()` return, and the preserved `label` —
  are the firm signal. Wall-clock magnitudes are not gated (they are
  host/jitter-dependent); cancel *timing* is used only to place the cancel within
  the chunk sequence.
* **Policies:**
  1. **baseline** — no cancel; every request completes (`chunks_completed ==
     chunks`).
  2. **queued** — submit a long backlog, cancel the **tail half** immediately;
     each request is long (40 ms), so the tail is still queued → `True`,
     `chunks_completed == 0`, never runs.
  3. **running** — waves of `num_lanes` requests; cancel ~**25 %** into the active
     span (started, widest margin to the final-chunk boundary) → `True`, a
     strictly-partial run.
  4. **late** — waves; cancel ~**90 %** of the *nominal* time-to-final-boundary.
     **Inherently racy** (this is the point): `True` (stop at the final boundary,
     `chunks_completed == chunks-1`) **or** `False` (already on the final chunk →
     completes). Both are valid; the invariant must hold either way.
* **Matrix:** `work_mode ∈ {sleep, spin}` × `num_lanes ∈ {1, 4}` ×
  `chunks ∈ {2, 4, 8}` × `chunk_delay_ms ∈ {0, 2}`, `service_ms = 40` (total
  active), `hpx_threads = 4`, backlog `N = 24`, `4` cancel waves → **24 cells**.
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all 24 cells passed):**
  1. One submitted request → **one** final row; no row has `chunks_completed >
     chunks`; `label` preserved.
  2. **`cancel()==True` ⟺ final row `status=="cancelled"`** (the core invariant,
     over every attempted cancel).
  3. `completed` ⟹ `chunks_completed == chunks`; `cancelled` ⟹
     `0 <= chunks_completed < chunks`.
  4. **queued**: every tail cancel `True` with `chunks_completed == 0`.
  5. **running**: every cancel that settled `True` is a strictly-partial run
     (`1 <= chunks_completed < chunks`).
  6. **baseline**: all completed, round-robin lane balance (spread ≤ 1).
  7. **Run-level**: running-cancel demonstrably fires somewhere (a broken feature
     would settle `True` nowhere). Per-cell firing is *characterized*, not gated —
     a single narrow boundary under spin contention can race past (see §3).
* **Reproduce:**
  `python experiments/13_chunk_boundary_cancellation/run_chunk_boundary_cancellation.py`
  (raw JSONL → `results/`, gitignored; `--quick` for a tiny smoke). The
  deterministic proof that running-cancel works is the structural check in
  `bench/smoke_rayx.py` (`check_running_cancel`); this experiment characterizes it
  across the matrix.

## 2. Measured facts

### 2a. Queued cancel — uniform full skip

Every cell: all **12** tail-half cancels returned `True`, retired
`status="cancelled"` with **`chunks_completed == 0`** (the `(cancelled,
cancel_true) = (12, 12)` pair is identical across all 24 cells). A queued cancel
skips the **whole** lifecycle regardless of `chunks` / delay / mode / lanes.

### 2b. Running cancel — strictly-partial stop, `chunks_completed` median

`chunks_completed` for running cancels (`1 <= cc < chunks` in every cell), median
across the wave samples:

| chunks | sleep d0 | sleep d2 | spin d0 | spin d2 |
|---|---|---|---|---|
| 2 | 1.0 | 1.0 | 1.0 | 1.0 |
| 4 | 1.5–2.0 | 1.0 | 2.0 | 2.0 |
| 8 | 2.5–3.0 | 2.0 | 3.0 | 2.0 |

Running-cancel **fired reliably** (settled `True`) in every cell. The stop lands
**early** — a cancel at ~25 % of active stops after ~`chunks/4` chunks, bounded
strictly below `chunks`. With `chunk_delay_ms = 2` the median is a touch lower:
the parked gaps stretch wall-clock, so the same ~25 %-active cancel arrives after
fewer **active** chunks. The range never reaches `chunks` (no `cc == chunks` for
any cancelled row).

### 2c. Late cancel — the boundary race (sleep vs spin)

Cancel attempts settling `True` vs `False` at ~90 % of the nominal final
boundary, summed over all cells per mode:

| mode | late `True` (stopped at boundary) | late `False` (completed) |
|---|---|---|
| sleep | **120** | 0 |
| spin  | **44** | 76 |

This is the headline characterization. In **sleep** mode the ~25 % parked/sleep
overshoot puts the lane *behind* the nominal schedule, so a ~90 %-nominal cancel
still arrives **before** the (overshot) final boundary → it stops there
(`chunks_completed == chunks-1`). In **spin** mode service time is *exact*, so a
~90 %-nominal cancel frequently lands **on or past** the final-chunk boundary —
where the lane has already cleared cancellability — so `cancel()` returns `False`
and the request **completes**. Either way the invariant holds:
`cancel()==True` exactly when the row is `cancelled`.

### 2d. Work saved

Summed over the running policy, active work **not** run because of the early stop
(one full request = 1.0): **sleep ≈ 75.1**, **spin ≈ 69.0** request-equivalents
of active service skipped. Cancellation actually elides synthetic work — it is
not a no-op relabel.

## 3. Interpretation

1. **Does a running chunked request stop at a chunk boundary?** Yes — running
   cancel settled `True` in every cell with `1 <= chunks_completed < chunks`
   (2b). The stop is **between** chunks: an in-progress active chunk and an
   in-progress parked gap are never interrupted (there is no cancel check inside
   `sleep_for` / `spin_for`).

2. **Is the `cancel()` ⟺ `cancelled` contract preserved?** Yes, over all four
   policies and the racy late case (gate 2). `cancel()==True` settles a
   cancellation; the row is `cancelled` iff so. For a *running* `True` it means
   "will stop at the next boundary," not "ready now."

3. **Queued vs running vs late.** Queued is a clean full skip (`cc==0`, uniform,
   2a). Running is a clean strictly-partial stop (2b). Late exposes the
   **final-chunk boundary race** (2c): once the lane commits to its last chunk,
   cancellability is cleared and the request completes — `cancel()` honestly
   returns `False`.

4. **sleep vs spin.** The structural outcomes are identical; the **timing** of the
   late race differs by the sleep/parked overshoot (2c): sleep lags the schedule
   and reliably stops at the boundary, spin is exact and more often completes.
   This is a host-timing property, not a different cancellation semantics — and it
   matches the earlier sleep-overshoot vs spin-fidelity finding.

5. **Why one cell needed an earlier cancel.** The hardest corner is `spin, L4,
   chunks=2, delay=0`: a single boundary at ~50 % of active, under spin
   contention, where main-thread sleep jitter can overshoot the lone boundary.
   Cancelling at ~25 % active (max margin) makes running-cancel fire reliably
   there too; whether any *given* cell fires is characterized, not gated (the
   smoke test is the deterministic proof).

## 4. Scoped takeaways

* Running cancel stops a started chunked request at its **next chunk boundary**,
  running `1 <= chunks_completed < chunks` and skipping the rest — never
  interrupting an active chunk or parked gap.
* Queued cancel remains a full skip (`chunks_completed == 0`); the final chunk is
  a hard commit point (`cancel()` → `False`, request completes).
* `cancel()==True` ⟺ `status=="cancelled"` across queued, running, and the racy
  late case; `chunks_completed` cleanly distinguishes queued (0), running
  (partial), and completed (full).
* sleep and spin differ only in the *timing* of the final-boundary race (sleep
  stops, spin often completes), not in semantics.

## 5. Caveats / non-claims

* **Not real token-stream cancellation, not Ray task/object cancellation, no
  per-chunk events, no mid-chunk interruption** — an early stop *between*
  synthetic service steps only. `service_ms` is duration control, never a payload
  or token.
* Single macOS laptop, 10 cores (4 P + 6 E); all wall-clock magnitudes (and the
  late-policy True/False split) are **machine- and jitter-specific**. The firm
  signals are the structural gates (one row per request, the `cancel()` ⟺
  `cancelled` invariant, `chunks_completed` ranges, queued `cc==0`, running
  partial). The `late` policy is **deliberately racy**.
* Single-request facade only; **batch futures are non-cancelable** (no token, no
  batch-cancel) and unchunked. No Ray comparison.
* The facade result row carries `chunks_completed`; the **benchmark JSONL schema
  stays version `1`** (drivers do not chunk or cancel). Raw per-run JSONL here is
  an experiment-local scratch format under `results/` (gitignored), **not** the v1
  benchmark schema; the curated `aggregate.json` beside this note is the tracked
  evidence.
