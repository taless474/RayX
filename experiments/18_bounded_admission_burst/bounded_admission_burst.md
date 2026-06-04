# Bounded Backlog Under Bursty Arrivals via `max_queue_depth_per_lane`

Uses the rayx admission-control feature `max_queue_depth_per_lane` together with
the observability snapshot `Engine.lane_stats()` to show, structurally, the
backlog tradeoff under a burst that overfills the lanes. The question:

> When a burst offers far more work than the lanes can hold, what does an
> **unbounded** engine do versus a **capped** one — to the per-lane backlog, to
> the offered work, and to the tail queueing of the work that *is* admitted?

The two behaviours, stated up front and then measured:

* **Unbounded** (`max_queue_depth_per_lane=None`) — admits **all** offered work;
  the per-lane backlog grows deep and the late-admitted tail waits behind the
  whole queue.
* **Capped** (`max_queue_depth_per_lane=3`) — **rejects overflow early** with a
  caller-visible `QueueFullError`; per-lane `queue_depth` stays bounded by the
  cap, so admitted work has a bounded backlog/tail.

`max_queue_depth_per_lane` is **local, per-lane admission by rejection**. It is
**not** Ray Serve backpressure, **not** distributed flow control, and **not**
blocking backpressure (the submit returns by *raising*; it never blocks waiting
for space). The cap counts **queued-but-not-started** work only — the **active
in-service request on each lane is extra** (already popped off the queue) — so a
lane's live footprint is at most `cap + 1` (one active + `cap` queued). Rejected
requests are **caller-visible exceptions, not result rows** (no Future, no row).

Experiment slice only: **no** new RayX API, **no** `ServiceLane` semantics change,
**no** benchmark-driver / analyzer change, **no** result-row / v1 benchmark-JSONL
schema change, and **nothing** touching the experiment-16 `HpxLane` mechanism
probe or CI. Companions: `docs/reference/rayx_frontend_design.md` §12 (the
admission-control contract) and §11 (the `lane_stats()` contract);
`experiments/17_lane_stats_observability/` (the backlog instrument reused here).

## 1. Setup

* **Instruments:**
  * `Engine(max_queue_depth_per_lane=cap)` — bounded admission. The check and the
    enqueue happen under one lane-mutex acquisition (no TOCTOU window); a full
    target lane raises `QueueFullError` **before** any Future exists.
  * `Engine.lane_stats()` — a per-lane snapshot of `queue_depth`
    (**queued-but-not-started**; the in-service request is not counted) and
    `active`. Non-consuming, can race — treat each call as a live photo.
* **Submit:** an experiment-local runner driving the facade directly. Each cell
  warms up, then **burst-submits `N = lanes × 8` sleep requests via
  `Engine.submit` as fast as possible, without retiring**. In `capped` mode each
  `submit` is wrapped in `try/except QueueFullError`: admitted requests collect a
  Future, rejected ones are counted (no Future).
* **Sample:** after the burst, call `lane_stats()` on a ~1 ms cadence until every
  lane is idle, recording per-lane `queue_depth`/`active` and the running **peak**
  per-lane `queue_depth`. `service_ms = 40` ≫ the microseconds the burst takes, so
  for the first ~40 ms nothing completes and the backlog sits at its peak — the
  first samples capture it. The cadence is just a sampling interval — **no gate is
  a timing threshold**. Then retire the admitted futures (input order) to read
  per-request status / service / latency.
* **Modes / matrix:** `mode ∈ {unbounded, capped}`, `num_lanes = 4`,
  `work_mode = sleep`, `service_ms = 40`, `cap = 3` (capped) / `None` (unbounded),
  `N = lanes × 8 = 32`, `hpx_threads = 4`, **3 repeats** → 6 runs / 2 cells.
  Medians across repeats; the representative snapshots/trajectory in
  `aggregate.json` are from repeat 0. (`--quick`: both modes × 1 repeat, `N = 24`
  — still > the `lanes × (cap+1) = 16` capacity, so the cap still overflows — and
  writes no `aggregate.json`.)
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (both cells passed; structural, timing-robust):**
  1. **Unbounded admits all** — `admitted == N`, `rejected == 0`.
  2. **Capped admitted band** — `lanes·cap ≤ admitted ≤ lanes·(cap+1)`,
     `rejected > 0`, `admitted + rejected == N`. The band is the robust form of
     "≈ `lanes·(cap+1)`": the **floor** `lanes·cap` holds even if no lane drains
     during the burst, the **ceiling** `lanes·(cap+1)` is the one-pop-per-lane
     case. The ceiling is widened by any *detected* mid-burst drain so a draining
     lane never flakes.
  3. **Capped bounds backlog** — peak per-lane `queue_depth ≤ cap`, every sample.
  4. **Unbounded grows backlog** — peak per-lane `queue_depth > cap` at least once.
  5. **Admitted complete** — every admitted (retired) row `status == "completed"`.
  6. **Reject ⇒ no row** — `rows == admitted == N − rejected` (rejected requests
     produced no Future and no row).
  7. **Reaches idle** — final `lane_stats()` sample: `num_active == 0`,
     `total_queue == 0`.
  8. **Round-robin balance** — admitted-row `actor_id` counts span ≤ 1 across
     exactly `lanes` lanes.
* **Reproduce:**
  `python experiments/18_bounded_admission_burst/run_bounded_admission_burst.py`
  (raw per-run JSON → `results/`, gitignored; `--quick` for a tiny smoke).

## 2. Measured facts (medians across 3 repeats; representative snapshots from repeat 0)

| | **unbounded** | **capped (cap = 3)** |
|---|---|---|
| admitted | **32** | **16** |
| rejected (`QueueFullError`) | **0** | **16** |
| admitted band (expected) | `[32, 32]` | `[12, 16]` |
| peak per-lane `queue_depth` | **7** (> cap) | **3** (= cap, never exceeded) |
| first all-active snapshot | `queue_depth=[7,7,7,7]`, `total_queue=28` | `queue_depth=[3,3,3,3]`, `total_queue=12` |
| admitted rows completed | 32 / 32 | 16 / 16 |
| `rows == admitted == N − rejected` | ✓ (32 = 32 − 0) | ✓ (16 = 32 − 16) |
| reaches idle | ✓ `[0,0,0,0]` | ✓ `[0,0,0,0]` |
| lanes seen / lane-count spread | 4 / 0 | 4 / 0 |
| admitted drain span | ~349.6 ms | ~174.0 ms |
| completed `service_ms` p50 | 42.66 ms | 42.67 ms |
| completed `queue_wait_ms` p50 / p99 | 307.1 / 309.7 ms | 132.2 / 133.8 ms |
| completed `total_ms` p50 / p99 | 349.7 / 349.7 ms | 174.0 / 174.1 ms |

Latency/throughput magnitudes are **machine-specific and reported, not gated** —
the structural gates above are the result.

### 2a. The structural contrast

* **Backlog.** The first all-active snapshot is the picture: unbounded holds
  `[7,7,7,7]` queued (28 in flight behind 4 active), capped holds exactly
  `[3,3,3,3]` (12 behind 4 active). The cap turns "depth grows with offered load"
  into "depth is pinned at `cap`". Across **every** sample of **every** capped
  repeat, no lane was ever observed above `queue_depth = 3`; unbounded reached 7.
* **Shed load.** Capped admitted 16 and rejected 16 of the 32 offered. Admitted
  landed at the top of the `[12, 16]` band — `lanes·(cap+1) = 16` — i.e. each of
  the 4 lanes admitted one active + `cap = 3` queued before rejecting the rest.
  Unbounded admitted all 32 and rejected none.
* **Tail of admitted work.** The work that *was* admitted paid a bounded queue
  wait under the cap: `queue_wait_ms` p99 ≈ 134 ms (capped) vs ≈ 310 ms
  (unbounded), and the admitted backlog drained in ≈ 174 ms vs ≈ 350 ms. The
  per-request **service** time is identical (~42.7 ms both modes) — only the
  *queueing* differs, because the cap bounds how much work can sit ahead of any
  admitted request (`cap × service_ms ≈ 3 × 40 = 120 ms`, consistent with the
  observed ~132 ms p50 including the active request ahead).
* **Routing unchanged.** Both modes stayed perfectly round-robin balanced
  (`queue_depth` spread 0 at the peak; admitted-row lane-count spread 0 across all
  4 lanes). The cap is the *only* difference; lane choice and FIFO ordering are
  unchanged.

## 3. Interpretation

* **The cap converts unbounded backlog growth into early, caller-visible
  shedding.** Under the same offered burst, unbounded lets the per-lane queue grow
  to whatever the arrivals demand (here 7 deep); capped pins it at `cap` and
  raises `QueueFullError` for the overflow. This is the structural tradeoff the
  experiment set out to show: **bounded backlog/tail in exchange for shed load.**
* **The active request is genuinely extra.** Each capped lane's live footprint at
  the peak was `cap + 1 = 4` (one `active=True` + `queue_depth=3`), and total
  admitted was `lanes·(cap+1) = 16` — direct evidence that the cap bounds only the
  **queued-but-not-started** count and the in-service request sits on top of it.
* **Rejections are not rows.** `rows == admitted == N − rejected` held in both
  modes (16 = 32 − 16 capped; 32 = 32 − 0 unbounded). A rejected request produced
  no Future to retire and contributed no measurement row — load-shedding is
  visible to the *caller* (as an exception it can catch), not as a silent or
  late-failing row.
* **The tail saving is the point of the cap, not a free lunch.** Capped's lower
  `queue_wait`/`total_ms`/drain-span apply to the **admitted** half only; the
  other half was shed. The honest framing is *bounded tail for admitted work, at
  the cost of refusing excess*, not "capped is faster".

## 4. Scoped takeaways

* `max_queue_depth_per_lane` is **local, per-lane admission by rejection**: a full
  target lane raises `QueueFullError` immediately and the round-robin rotation
  still advances (one full lane does not stall the others).
* The cap **bounds queued-but-not-started depth** at `cap`; the active in-service
  request is extra, so the per-lane live footprint is `cap + 1`.
* Capped mode **trades shed load for a bounded backlog and bounded tail queueing**
  of admitted work; unbounded admits everything and the late-queued tail grows
  with the burst.
* The result is **structural** — admitted/rejected accounting, the
  `queue_depth ≤ cap` invariant, the all-completed/idle endpoints, and round-robin
  balance — and held across all repeats. Latency magnitudes are illustrative.

## 5. Caveats / non-claims

* **Not Ray Serve, not backpressure, not flow control.** This is not Ray Serve
  backpressure, not distributed flow control, and not blocking backpressure: the
  submit never blocks waiting for space — it returns by raising `QueueFullError`.
  It is not a scheduler and not placement control (lane choice stays internal
  round-robin).
* **Cap is per-lane, not a global backlog cap.** The bound is each lane's
  `queue_depth`; there is no global admitted-count limit. With `lanes = 4` and
  `cap = 3` the per-lane bound yields a `lanes·(cap+1) = 16` live capacity, but
  that is a *consequence* of the per-lane cap, not a configured global.
* **Cancelled queued items still count until popped** (see experiment 17 and
  `docs/reference/rayx_frontend_design.md` §12) — a queued cancel settles the
  Future but does not dequeue, so it keeps occupying a cap slot until the lane
  pops/skips it. **Cancellation is not exercised in this experiment**; it is noted
  only so the cap's accounting is not misread.
* **Machine-specific magnitudes.** All `*_ms` numbers (queue wait, total, drain
  span, ~42.7 ms sleep service carrying the usual sleep-timer overshoot) are
  laptop-specific and depend on lanes vs cores and host load. The structural gates
  are the durable result.
* **Synthetic sleep service only.** `work_mode="sleep"` is parked synthetic
  service time, not real inference or payload execution; this is rayx-only, single
  locality, in-process — no Ray comparison and no native-baseline comparison here.
* **Raw outputs are scratch.** Per-run JSON under `results/` is an
  experiment-local scratch format (gitignored), not the v1 benchmark JSONL schema;
  the curated `aggregate.json` beside this report is the tracked evidence.
