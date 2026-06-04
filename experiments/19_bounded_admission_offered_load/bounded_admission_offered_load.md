# Bounded Backlog Under Sustained Offered Load via `max_queue_depth_per_lane`

The **sustained-flow** companion to [experiment 18](../18_bounded_admission_burst/bounded_admission_burst.md).
Experiment 18 asked the question with one **single burst** (submit `N` as fast as
possible, then watch it drain). This experiment asks the same question under a
**continuous producer** that keeps offering work at a fixed rate for a fixed
duration:

> Under a continuous submitter at a fixed offered rate, does
> `max_queue_depth_per_lane` keep per-lane backlog bounded by shedding overload,
> while the **unbounded** engine accumulates queue when offered load exceeds the
> lanes' service rate?

The two behaviours, stated up front and then measured under a sustained
**over-capacity** producer:

* **Unbounded** (`max_queue_depth_per_lane=None`) — admits **all** offered work;
  the per-lane backlog **grows for the whole window** (here to `queue_depth = 22`
  per lane, `total_queue = 87`) and keeps climbing as long as the producer runs.
* **Capped** (`max_queue_depth_per_lane=3`) — **pins** per-lane `queue_depth` at
  the cap (never above `3` in any sample) and **sheds the overflow** with a
  caller-visible `QueueFullError`; the backlog plateaus instead of growing.

`max_queue_depth_per_lane` is **local, per-lane admission by rejection**. It is
**not** Ray Serve backpressure, **not** distributed flow control, and **not**
blocking backpressure (the submit returns by *raising*; it never blocks waiting
for space). The cap counts **queued-but-not-started** work only — the **active
in-service request on each lane is extra** (already popped off the queue) — so a
lane's live footprint is at most `cap + 1`. The cap is **per-lane, not a global
backlog cap**. Rejected requests are **caller-visible exceptions, not result
rows** (no Future, no row).

Experiment slice only: **no** new RayX API, **no** `ServiceLane` semantics change,
**no** Python-API / benchmark-driver / analyzer change, **no** result-row / v1
benchmark-JSONL schema change, and **nothing** touching the experiment-16
`HpxLane` mechanism probe or CI. Reuses the admission-control contract
(`docs/reference/rayx_frontend_design.md` §12), the `lane_stats()` contract
(§11), and the instruments from experiments 17 (the backlog snapshot) and 18 (the
burst version of this same tradeoff).

## 1. Setup

* **Instruments:**
  * `Engine(max_queue_depth_per_lane=cap)` — bounded admission. The check and the
    enqueue happen under one lane-mutex acquisition (no TOCTOU window); a full
    target lane raises `QueueFullError` **before** any Future exists.
  * `Engine.lane_stats()` — a per-lane snapshot of `queue_depth`
    (**queued-but-not-started**; the in-service request is not counted) and
    `active`. Non-consuming, can race — treat each call as a live photo.
* **Producer (the sustained part):** an experiment-local runner driving the
  facade directly. Each cell warms up and drains, then runs a **duration-based
  producer** that **attempts one `Engine.submit` per fixed inter-arrival
  interval**, on an absolute schedule (`next_t += interval`, so the offered rate
  does not drift with per-iteration cost). It does **not** retire futures while
  producing — so the live per-lane backlog is exactly *arrivals − serviced*. In
  `capped` mode each `submit` is wrapped in `try/except QueueFullError`: admitted
  requests collect a Future, rejected ones are counted (no Future). Once per
  attempt it samples `lane_stats()`, tracking the running **peak** per-lane
  `queue_depth` and **peak** `total_queue`. After the producer stops it **drains
  (retires) the admitted futures** in input order, then confirms the lanes reach
  idle.
* **Offered rates** (relative to the lanes' service rate). With `num_lanes = 4`
  and a sleep `service_ms = 40` that lands near ~45 ms observed (the sleep
  overshoot), aggregate service capacity is roughly `num_lanes / observed_service
  ≈ 4 / 0.045 ≈ 90 req/s`:
  * **below capacity** — one submit every **20 ms ≈ 50 req/s** (utilisation ≈ 0.6).
  * **over capacity** — one submit every **5 ms ≈ 200 req/s** (≈ 2× capacity).
  The realized attempt rates landed on target: **51 req/s** and **201 req/s** (the
  runner reports `realized_attempt_rate_per_s`; no interval adjustment was needed).
* **Modes / matrix:** `num_lanes = 4`, `work_mode = sleep`, `service_ms = 40`,
  `cap = 3` (capped) / `None` (unbounded), `hpx_threads = 4`, duration **800 ms**,
  **3 repeats** → four cells:
  `below_capacity_{unbounded,capped}`, `over_capacity_{unbounded,capped}`.
  Medians across repeats; representative snapshots/trajectory in `aggregate.json`
  are from repeat 0. (`--quick`: 300 ms duration × 1 repeat, no `aggregate.json`.)
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all four cells passed; structural, timing-robust, load/bound-aware):**
  1. **Accounting** — `admitted + rejected == attempted`; unbounded rejects `0`.
  2. **Reject ⇒ no row** — `rows == admitted == attempted − rejected`.
  3. **Admitted complete** — every admitted (retired) row `status == "completed"`.
  4. **Reaches idle** — final `lane_stats()` sample: `num_active == 0`,
     `total_queue == 0`.
  5. **Lane balance** — completed rows span exactly `lanes` lanes and the
     least-loaded lane handled ≥ half the busiest (`min·2 ≥ max`).
  6. **Capped bounds backlog** — (capped modes) peak per-lane `queue_depth ≤ cap`,
     every sample.
  7. **Below-capacity capped near-zero shed** — `rejected ≤ ⌈0.05 · attempted⌉`
     (offered load is under the service rate, so almost nothing sheds).
  8. **Over-capacity capped sheds** — `rejected > 0`.
  9. **Over-capacity unbounded grows backlog** — peak per-lane `queue_depth > cap`
     at least once (the unbounded queue overruns the cap under sustained overload).
  The offered rate itself is **not** gated (the host's sleep granularity can
  stretch the interval); the gates assert direction-of-change, and the realized
  rate is reported.
* **Reproduce:**
  `python experiments/19_bounded_admission_offered_load/run_bounded_admission_offered_load.py`
  (raw per-run JSON → `results/`, gitignored; `--quick` for a tiny smoke).

## 2. Measured facts (medians across 3 repeats; representative trajectory from repeat 0)

| | below unbounded | below capped | **over unbounded** | **over capped (cap=3)** |
|---|---|---|---|---|
| offered interval / realized rate | 20 ms / **51/s** | 20 ms / **51/s** | 5 ms / **201/s** | 5 ms / **201/s** |
| attempted | 41 | 41 | 161 | 161 |
| admitted | 41 | 41 | **161** | **85** |
| rejected (`QueueFullError`) | 0 | 0 | **0** | **76** |
| peak per-lane `queue_depth` | 1 | 1 | **22** (> cap) | **3** (= cap, never exceeded) |
| peak `total_queue` | 1 | 1 | **87** | **12** |
| backlog trend over the window | flat ≈ 0–1 | flat ≈ 0–1 | **grows 0 → 87** | **plateaus ≈ 10–12** |
| admitted rows completed | 41 / 41 | 41 / 41 | 161 / 161 | 85 / 85 |
| `rows == admitted == att − rej` | ✓ | ✓ | ✓ (161 = 161 − 0) | ✓ (85 = 161 − 76) |
| reaches idle | ✓ `[0,0,0,0]` | ✓ `[0,0,0,0]` | ✓ `[0,0,0,0]` | ✓ `[0,0,0,0]` |
| lanes seen / per-lane count (min–max) | 4 / 10–11 | 4 / 10–11 | 4 / 40–41 | 4 / 21–22 |
| completed `service_ms` p50 | 45.0 | 44.4 | 44.8 | 44.8 |
| completed `queue_wait_ms` p50 / p99 † | 356 / 759 | 358 / 760 | 623 / 932 | 427 / 756 |
| completed `total_ms` p50 / p99 † | 399 / 804 | 400 / 804 | 665 / 977 | 470 / 801 |

† **The `*_ms` columns are reported, not gated, and are dominated by the
closed-loop retire-at-end pattern** — see §3 (latency). The durable result is the
structural backlog above, not these magnitudes.

### 2a. The backlog trajectory (the centrepiece)

The live `lane_stats()` trajectory over the producer window is where the sustained
contrast shows up — the burst version (experiment 18) could only show a single
peak-then-drain, whereas here the **shape over time** is the result:

```
over-capacity UNBOUNDED — total_queue grows for the whole 800 ms window:
  t(ms):    0    76   145   221   291   366   436   511   584   656   726   801
  total_q:  0     8    14    23    31    38    47    55    63    71    78    87
  max_qd:   0     2     4     6     8    10    12    14    16    18    20    22

over-capacity CAPPED (cap=3) — total_queue plateaus, max_qd pinned at the cap:
  t(ms):    0    76   146   221   291   366   436   511   581   656   726   801
  total_q:  0     8    10    11    11    11    11    11    10    12    11    10
  max_qd:   0     2     3     3     3     3     3     3     3     3     3     3
```

Under the **same** offered rate (201 req/s, ≈ 2× capacity), unbounded backlog
rises without bound for as long as the producer runs (linear in window length:
arrivals − served accumulates), while capped backlog climbs once to the cap and
then **stays there**, shedding everything that would have grown it further. Across
**every** sample of **every** capped repeat, no lane was ever observed above
`queue_depth = 3`; unbounded reached 22 — and would have kept climbing with a
longer window. The below-capacity cells (both modes) never saturate: `total_queue`
hovers at 0–1 the whole time because arrivals (51/s) are comfortably under the
service rate, so the cap has nothing to shed.

## 3. Interpretation

* **Sustained overload separates the two regimes; a burst cannot.** A burst offers
  a fixed `N` once, so even unbounded backlog is bounded by `N`. A *continuous*
  over-capacity producer offers unbounded work over time, so the unbounded queue's
  defining property — it grows with the **window**, not with a fixed `N` — only
  appears here: `total_queue` rose 0 → 87 and was still climbing at 800 ms. The cap
  converts that open-ended growth into a **plateau at the cap** plus a stream of
  caller-visible rejections.
* **The cap pins per-lane depth; the active request is genuinely extra.** Capped
  `queue_depth` sat at the cap (`[3,2,2,3]`-style snapshots, `total_queue ≈ 10–12`
  across 4 lanes) with all four lanes `active=True` — i.e. each lane held one
  in-service request **plus** at most `cap = 3` queued, a live footprint of
  `cap + 1 = 4`. The per-lane bound, not a global one, is what holds.
* **Shedding is caller-visible and is not a row.** Capped admitted 85 and rejected
  76 of the 161 offered; `rows == admitted == attempted − rejected` held (85 = 161
  − 76). A rejected request produced no Future to retire and contributed no
  measurement row — load-shedding is visible to the *caller* (as a
  `QueueFullError` it can catch and react to), not as a silent or late-failing row.
  Admitted count (~85) tracks what the lanes could actually service in the window
  plus the bounded backlog drained afterward — the cap let throughput, not queue
  depth, absorb the overload.
* **Latency magnitudes here are closed-loop / retire-at-end artifacts, not clean
  per-request latency.** Because the producer **holds** futures and retires them
  only after it stops, `total_ms` (and hence `queue_wait_ms = total_ms − service`)
  includes the time a request sat *retired-late*, not just its lane-queue wait. The
  tell is the **below-capacity** cells: `peak queue_depth = 1` (essentially no lane
  queueing), yet `queue_wait_ms` p50 ≈ 357 ms — that 357 ms is almost entirely the
  retire-at-end holding (a request submitted early waits in the client until the
  ~800 ms producer finishes), **not** queueing on a lane. So the across-mode
  latency gap under overload (over-unbounded `queue_wait` p99 ≈ 932 ms vs
  over-capped ≈ 756 ms) is **directional** — the unbounded lane queues are
  genuinely deeper, on top of the shared holding floor — but the magnitudes are a
  driver pattern (cf. the FIFO-retire / client-driver readings in experiments 02,
  06, 07), not a steady-state serving latency. **The durable result is the
  structural backlog (peak `queue_depth`, the growth-vs-plateau trajectory), not
  these `*_ms` numbers.**
* **Routing stayed balanced.** Round-robin advances on **every** attempt (admitted
  or rejected), so completed-row `actor_id` counts stayed near-even in all cells
  (10–11 below, 40–41 over-unbounded, 21–22 over-capped across 4 lanes). The cap
  changes *whether* a turn is admitted, not *which* lane the turn targets.

## 4. Scoped takeaways

* Under **sustained over-capacity** offered load, `max_queue_depth_per_lane`
  **bounds per-lane backlog** (pinned at `cap`, a plateau) by **shedding overload**
  as caller-visible `QueueFullError`; the unbounded engine instead **accumulates
  queue for the whole window** (here to `queue_depth = 22`, `total_queue = 87`,
  still climbing).
* **Below capacity**, the cap is inert: arrivals are under the service rate, the
  backlog stays at 0–1, and capped sheds essentially nothing (0 rejections) — the
  cap costs nothing when it is not needed.
* Capped mode **trades rejected work for bounded backlog and bounded
  admitted-tail queueing** — it is **not** "faster"; the saving applies to admitted
  work and the other work was refused.
* The result is **structural** — admitted/rejected accounting, the
  `queue_depth ≤ cap` invariant, the grow-vs-plateau trajectory, all-completed and
  idle endpoints, and round-robin balance — and held across all repeats. Latency
  magnitudes are illustrative and contaminated by the retire-at-end driver pattern.

## 5. Caveats / non-claims

* **Not Ray Serve, not backpressure, not flow control.** This is not Ray Serve
  backpressure, not distributed flow control, and not blocking backpressure: the
  submit never blocks waiting for space — it returns by raising `QueueFullError`.
  It is not a scheduler and not placement control (lane choice stays internal
  round-robin).
* **Cap is per-lane, not a global backlog cap.** The bound is each lane's
  `queue_depth`; there is no global admitted-count limit. The `total_queue ≈ 12`
  plateau is a *consequence* of four lanes each capped at `cap = 3`, not a
  configured global.
* **Latency / throughput are reported, not gated, and are retire-at-end shaped.**
  As §3 explains, the `*_ms` numbers (and the `~85` admitted count) are
  machine-specific and dominated by the closed-loop producer holding futures until
  it stops; they are **not** steady-state per-request serving latency or a
  saturated-throughput measurement. The structural gates are the durable result.
* **Offered rate is nominal.** The 50 / 200 req/s targets depend on the host's
  `time.sleep` granularity; the runner reports the realized rate (here 51 / 201
  req/s) and the gates assert direction-of-change, not a rate.
* **Synthetic sleep service only.** `work_mode="sleep"` is parked synthetic service
  time, not real inference or payload execution; this is rayx-only, single
  locality, in-process — no Ray comparison and no native-baseline comparison here.
* **Raw outputs are scratch.** Per-run JSON under `results/` is an
  experiment-local scratch format (gitignored), not the v1 benchmark JSONL schema;
  the curated `aggregate.json` beside this report is the tracked evidence.
