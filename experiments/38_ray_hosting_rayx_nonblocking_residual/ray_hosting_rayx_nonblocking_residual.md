# exp38 — Ray-hosted RayX non-blocking lane: over-load residual attribution

**Status: COMPLETE (one full Rostam run). Diagnostic-only. The result section
below records the curated outcome; §1–§12 are the design as run.**

**Scope:** observation-only, synthetic, Rostam-specific. `max_inflight` is a
diagnostic lever, not sizing/capacity guidance. The non-blocking lane stays
experimental and op-lane-only (actor lanes remain serial). No Ray-vs-HPX,
performance, capacity, latency-SLO, cluster-scaling, Ray Serve, real-I/O,
inference, "HPX beats Ray", or "RayX makes Ray faster" claim. The exp38 verdict
is **attribution of the exp37 over-load residual**, not feature SUPPORT/STOP.

This note follows the exp35→exp37 arc:

* exp35: adapter-level compute-retention erosion under a synthetic parked+compute
  mix (HPX cooperative parking itself was not the problem).
* exp36: mechanism localized to per-lane FIFO head-of-line (HOL) blocking; raising
  `num_lanes` at fixed `hpx_threads` recovered retention by diluting HOL.
* exp37: HOL removal by the experimental non-blocking op-lane at fixed
  `num_lanes == hpx_threads`; near-load FULL SUPPORT, over-load PARTIAL SUPPORT.

exp38 diagnoses the exp37 **over-load PARTIAL residual**. It does not re-prove HOL.

---

## Result (full Rostam run, 2026-06-22)

**Node:** medusa01 — Intel Xeon Gold 6148, 40 CPUs, 2 sockets × 20 cores,
1 thread/core, Ray 2.55.1, `.venv/bin/python` (Python 3.12). Single full run.
Log: `logs/exp38_full_20260622_144143.log`.

**Structural gates:** smoke **PASS**; full **PASS**
(`agg_ok, futures_completed, plain_types_ok, lane_ids_ok, clean_shutdown`).

**Attribution: H-CAP in every Probe A ladder.** The exp37 over-load PARTIAL is
attributed **primarily to an undersized admission cap (`max_inflight=4`), not to
retirement-path cost.** Raising `max_inflight` drained the lane queue and restored
compute-throughput retention to SUPPORT under both HT=4 and HT=8 over-load:

| ladder | serial | nb-mi4 | nb-mi8 | nb-mi16 | nb-mi32 | qd_mean_T (base→top) | verdict |
|---|---|---|---|---|---|---|---|
| HT=4 near | 0.47 | 1.00 | 1.00 | 0.99 | 0.99 | 0.0 → 0.0 | H-CAP (near FULL control holds) |
| HT=4 over | 0.26 | 0.70 | 0.98 | 0.99 | 0.98 | 10.7 → 0.1 | **H-CAP** |
| HT=8 near | 0.47 | 0.99 | 1.01 | 1.01 | 1.00 | 0.0 → 0.0 | H-CAP (near FULL control holds) |
| HT=8 over | 0.37 | 0.78 | 0.99 | 0.99 | 0.99 | 14.5 → 0.0 | **H-CAP** |

The qd→0 partition is clean: at over-load the lane queue drains monotonically as
`max_inflight` rises and retention reaches SUPPORT (≥0.90) by `nb-mi8`, with the
guarded `nb-mi32` cell flat (`d_retention ≈ 0`, **no churn/knee**) — so HT=8
remained decisive and the HT=4 fallback was not needed. The near-load FULL control
held (qd already ~0, retention ~1.0 from `nb-mi4`).

**Decisive mechanism:** non-blocking op-lanes **remove per-lane head-of-line when
the in-flight cap is large enough for the offered synthetic mix.** exp37's
`max_inflight=4` was below the over-load offered concurrency (O = 4·HT = 16 at
HT=4, 32 at HT=8), so compute still queued behind parked ops; extending the cap
removed it.

**What is NOT motivated by this result:**

* **No retirement-path optimization** (continuation / per-lane mutex / handoff /
  in-flight accounting). The residual was the cap, not retirement cost.
* **No Probe C counters.** The counter-free qd→0 partition resolved the attribution.
* `notify_all` / mutex / in-flight bookkeeping are **not** indicated for change.

`max_inflight=8/16/32` recovering retention is a **diagnostic lever only — NOT
sizing or capacity guidance** for any real workload.

### Probe B: not needed for attribution (corroborating only, and partly unusable)

Probe B printed an **`H-RETIRE`** line, but it is **not decisive and is not curated
as evidence of retirement-path overhead**, for two independent reasons:

1. **Probe A did not land in DRAINED-BUT-PLATEAU** — it landed **H-CAP**. Per the
   design, Probe B is decisive only when Probe A shows a drained-but-plateau
   residual; here there is no such residual to split, so Probe B is corroborating
   context at most.
2. **The Probe B fine `nb-mi32` C-only baseline is anomalous.** Its `wall_C` read
   **33.7 ms** versus ~11–12 ms for every other fine HT=8 C-only arm (including the
   Probe A HT=8 over serial/nb cells and the Probe B fine serial anchor at
   11.7 ms). That inflated baseline produced a nonsensical `thr_retention = 2.86`
   (> 1) and `residual = −1.96`, which is what the `H-RETIRE` line keyed on. A
   broken C-only reference makes that branch **unsuitable as an attribution
   verdict.** Additionally the **coarse serial anchor did not erode** (0.85 > the
   0.70 erosion control), so the coarse positive-erosion control was absent
   (compute dominates at coarse — the exp35 effect), further weakening the split.

**Do not present exp38 as evidence for retirement-path overhead.** The supported
exp38 claim is exactly: under the synthetic parked+compute mix at fixed
`num_lanes == hpx_threads` on this node, the exp37 over-load PARTIAL is attributable
to an undersized in-flight cap; the non-blocking op-lane removes per-lane
head-of-line once the cap is large enough.

**Scope (unchanged):** observation-only; synthetic `park_ms` cooperative wait (not
real I/O, not inference); Rostam-specific, single run; non-blocking experimental and
operation-lane-only (actor lanes stay serial); `max_inflight` a diagnostic lever,
not sizing/capacity guidance; **not** Ray Serve, **not** cluster scaling, **not** a
latency/SLO/capacity/performance claim, **not** "HPX beats Ray", **not** "RayX makes
Ray faster", and **not** a recommendation to make non-blocking the default. No W=32
scaling curve; no NUMA/binding; no priorities/pools/counters.

---

## 1. Lead observation: H-cap is the first suspect

exp37's own data says where to look first. At `nb-mi4` under over-load,
`qd_mean_T` stayed **nonzero** — 11.1 (HT=4) and 15.0 (HT=8) — while near-load
drained to 0.0:

| cell | serial | nb-mi1 | nb-mi2 | nb-mi4 | qd_mean_T | reading |
|---|---|---|---|---|---|---|
| HT=4 near | 0.47 | 0.47 | 0.91 | 0.99 | 1.2 → 0.0 | FULL |
| HT=4 over | 0.19 | 0.26 | 0.45 | 0.77 | 21.1 → 11.1 | PARTIAL |
| HT=8 near | 0.47 | 0.47 | 1.00 | 1.01 | 3.2 → 0.0 | FULL |
| HT=8 over | 0.37 | 0.36 | 0.55 | 0.81 | 30.9 → 15.0 | PARTIAL |

A non-empty lane queue at `nb-mi4`, with the consumer pinned at the in-flight cap
and ops backing up, means the **cap was binding**. So the leading candidate for
the over-load PARTIAL is an **undersized admission cap** below the over-load
offered concurrency, not (yet) intrinsic retirement-path cost. Retirement and
scheduler costs are secondary, revealed only if draining the lane queue fails to
restore retention.

---

## 2. Hypothesis set (H-cap, H-retire, H-sat, H-driver)

All retention readings are **normalized against the C-only (compute-only)
baseline at the same `hpx_threads` (HT)**. This normalization is load-bearing for
H-sat below: "HT workers are finite" is *already priced into the C-only
baseline*, so finite HT alone cannot explain a normalized retention loss. H-sat
must therefore mean *mixed-workload* pressure **beyond** that baseline, not bare
core count.

| Hyp | Claim | Counter-free signature |
|---|---|---|
| **H-cap** (primary) | The over-load PARTIAL is admission-capacity: `max_inflight` is below over-load park+compute concurrency, so compute still queues behind parked ops in the lane. | As `max_inflight` rises: `qd_mean_T` → ~0 **and** retention climbs to SUPPORT (≥0.90). |
| **H-retire** | Retirement-path cost dominates: `.then(...)` continuation, per-lane mutex serialization, lock-acquire wait under contention, completion-worker→consumer-worker handoff, in-flight accounting. | `qd` → ~0, retention plateaus **below** SUPPORT, and the residual **shrinks** when compute is coarsened (per-op retirement rate drops ~10× at fixed parked load). |
| **H-sat** (scheduler-worker saturation) | **Mixed-workload** scheduler/worker pressure *beyond* the C-only baseline: extra concurrently-suspended `park_ms` tasks, their timers, in-flight continuations, retirements, and HPX scheduler-queue depth interfere with compute progress. Backlog migrates from the *visible* lane queue (`qd`) into the *invisible* HPX scheduler task/timer queue as `max_inflight` rises. | `qd` → ~0, retention plateaus **below** SUPPORT, and the residual does **not** shrink much when compute is coarsened. Mixed pressure exceeds C-only at the same HT. |
| **H-driver** | CPython submit/retire cadence: every submit hops `run_as_hpx_thread` from the single Python driver thread; at over-load submit rate that hop serializes. | Residual persists independent of cap and of compute-retirement rate; `lane_stats` shows backlog/active inconsistent with a lane-queue or worker bound; a submit-rate ceiling is visible with a no-op / Inline op. |

**H-sat vs H-driver vs H-retire are deliberately not fully separable
counter-free.** Probe B distinguishes H-retire (granularity-sensitive) from
{H-sat, H-driver} (granularity-insensitive). Separating H-sat from H-driver, and
pinpointing the H-retire mechanism, is what the deferred Probe C counters (§6)
are for — and only if A+B leave it ambiguous.

**Explicitly not claimed:** that finite HT alone explains the residual. Retention
is normalized against C-only at the same HT, so a normalized loss is always
*mixed-workload* interference, never bare core count.

---

## 3. Pre-run compute-demand / saturation sanity calc (reading aid, not a gate)

Compute this **on paper from exp37 before running exp38**, per HT cell. It tells
us which regime we are in and whether qd→0 can mechanically reach SUPPORT.

Inputs to pull from exp37:

* `wall_C` — C-only (compute-only) wall time at this HT.
* `K_C` — compute op count (= 60).
* `hpx_threads` (HT) and `num_lanes` (= HT).
* approximate `busy_sum` service time `s_C` and per-worker throughput `1/s_C`,
  derived from `wall_C`, `K_C`, and HT.
* offered compute window `O` for the over-load level (cited as `O = 4·HT`).

Derive and record:

1. **C-only worker demand** ≈ `K_C · s_C / wall_C`. If this already ≈ HT, the
   C-only baseline is itself worker-bound — and since retention normalizes
   against it, that bound is *removed* from the residual (do not re-attribute it).
2. **Mixed offered compute demand** at over-load vs HT: is the compute fraction of
   `O` above or below HT? Parks (`park_ms`, cooperative `sleep_for`) suspend and
   free their worker, so they cost ≈0 worker-occupancy; only the compute fraction
   competes for HT workers.
3. **Backlog-migration prediction:** as `max_inflight` rises with `O` fixed, does
   the model predict backlog moving from the lane queue (`qd`, visible) into the
   HPX scheduler/timer queue (invisible to `lane_stats`)? If yes, expect the H-sat
   signature (qd→0 with retention plateau) even with zero retirement-path cost.

If the calc shows mixed compute demand comfortably **below** HT, H-sat is largely
out of play and the residual is more likely H-retire / H-driver. If it shows
demand **near/above** HT once parks and continuations are added, H-sat is live and
must be read explicitly. This calc is a **reading aid only** — never a structural
gate.

---

## 4. The decisive partition (counter-free) and the HT=8 truncation fix

The qd→0 test under a `max_inflight` extension cleanly separates H-cap from
{H-retire, H-sat, H-driver}, with Probe B then splitting H-retire from
{H-sat, H-driver}:

| outcome as `max_inflight` rises | `qd_mean_T` | retention | attribution |
|---|---|---|---|
| recovers to ≥0.90 | → ~0 in step | ↑ to SUPPORT | **H-cap** (undersized cap; HOL removed with enough slots) |
| plateaus < ~0.90 | → ~0 | stalls | **H-retire / H-sat / H-driver** (queue drained, retirement/scheduler/driver cost) — Probe B then splits these |
| stays backed up | stays > 0 | stalls | **still cap-bound** or consumer/retire throughput bound |

This needs zero C++ — only the existing `max_inflight_per_lane` knob.

**Truncation problem.** The worst exp37 residual is **HT=8 over**, where offered
concurrency `O=32`. If `max_inflight` tops out at 16, the cap stays *below* `O`,
so `qd` may not drain **by construction** — leaving the partition inconclusive in
exactly the cell that matters most.

**Resolution (preferred): include `max_inflight=32` for HT=8 as an optional,
guarded decisive cell.** Cooperatively-suspended `park_ms` tasks are not OS
threads — 256 suspended timers (8 lanes × 32) is cheap; the real concurrency cost
is *active compute*, bounded by HT regardless of `max_inflight`. So extending the
cap is HPX-safe. Guard it:

* Run mi=32 at HT=8 **last**, after the mi≤16 cells are green.
* Keep the blast radius bounded and **report explicitly** if mi=32 shows churn /
  a non-monotone knee / instability.

**Secondary resolution (if mi=32 at HT=8 is unstable):** explicitly designate
**HT=4 over as the decisive H-cap cell** (its sweep reaches mi=32 = 2·O, enough to
drive qd→0 if the cap is the cause) and demote **HT=8 to corroborating only**.

Either way the design states up front which cell carries the H-cap verdict, so a
"stays backed up" reading is never confused with "the sweep never reached the cap
the model needs."

---

## 5. Probes (staged; counter-free first)

**Probe A (primary, counter-free).** Extend the `max_inflight` sweep under
over-load (keep near as the FULL-SUPPORT control). Read the §4 qd→0 partition.
Uses only existing `Runtime` kwargs (`max_inflight_per_lane`, `num_lanes`, `n_c`).
**No C++, no counters.**

**Probe B (secondary, counter-free).** At the best `max_inflight` and over-load,
vary compute granularity (fine `n_c=2,000,000` → coarse `n_c=20,000,000`) to drop
the compute-retirement rate ~10× at fixed parked load.

* Residual (gap to SUPPORT at qd≈0) **shrinks** with coarser compute ⇒ per-op
  retirement cost (**H-retire**).
* Residual **persists** ⇒ scheduler/worker or driver (**H-sat / H-driver**) — use
  the §3 calc and `lane_stats` context to lean between them.
* Re-calibrate `K_Wp` per granularity and re-check the serial positive-erosion
  control (coarse compute may not erode — exp35).

**Probe C (deferred, conditional — only if A+B are ambiguous).** Minimal
`--diag`-gated C++ counters, off by default, on a separate diag snapshot (not
`lane_stats`, not the row schema). **Out of scope for the first slice;** listed in
§6 for priority only.

**Answer to "diagnostic-only and counter-free first?": yes.** Do Probe A (+B)
before adding any counters, and before touching `notify_all` / mutex / in-flight
accounting. The qd→0 test already partitions the residual with existing knobs.

---

## 6. Probe C counter priority (deferred; out of scope for the first slice)

If and only if A+B cannot separate H-retire from {H-sat, H-driver}, add
`--diag`-gated experimental per-lane counters, off by default, on a separate diag
snapshot — **not** `lane_stats`, **not** the v1 row schema. Priority order:

1. **`cap_wait_count` / `cap_wait_ns`** — consumer time blocked on
   `inflight_ < max_inflight_`. The clean, direct **H-cap** signal (better than
   `qd`; it measures the stall at its source).
2. **continuation lock-acquire wait** — time spent *waiting to acquire* the
   per-lane mutex in the retirement continuation, **not only** critical-section
   hold time. Hold time under-counts contention; acquire-wait is the H-retire
   scaling cost.
3. **`dispatched` / `retired`** — retirement throughput vs dispatch.
4. **`inflight_high_water`** — confirms the cap actually pinned at `max_inflight`
   (cap-bound confirmation).
5. **`toks_scan_len_sum`** (low priority) — total O(inflight) in-flight-token erase
   scan length. At mi≤32 this is a ~32-element scan under an already-held lock;
   almost certainly too small to motivate an O(1) rewrite **on its own**.

All counters would be cumulative-per-lane debug numbers behind the experimental
flag; default builds and the v1 schema stay untouched. **This first slice adds
none of them.**

---

## 7. notify_all framing (corrected)

Do **not** treat `cv_.notify_all()` as a likely thundering-herd cost. There is
usually exactly **one** consumer waiting on the lane condition variable for the
admission predicate, plus — only during teardown — one `stop_and_join` waiter on
`inflight_ == 0`. notify_all wakes at most two threads and usually one; switching
to `notify_one` will not move the over-load curve. The more plausible retirement
cost is structural:

* continuations running on the completing executor workers (launch::sync, inline),
* serialization of those continuations on the **single per-lane mutex**,
* lock-acquisition wait under contention,
* completion-worker → consumer-worker handoff,
* in-flight accounting.

exp38 does **not** optimize notify_all, the mutex, or in-flight bookkeeping — it
only attributes the residual.

---

## 8. Cell matrix (small, targeted — not a sweep)

**Probe A.** `hpx_threads = num_lanes ∈ {4, 8}`; level **over** (primary) +
**near** (FULL-SUPPORT control); fine `n_c=2,000,000`; serial anchor +
`nb(max_inflight ∈ {4, 8, 16})`; **add mi=32 at HT=4** and **mi=32 at HT=8 as the
optional guarded decisive cell** (§4: run last, report churn/knee). `K_C=60`,
`park_ms=5`, `reps=3`.

**Probe B.** HT=8 over only (the worst exp37 residual), best `max_inflight` from
A, `n_c ∈ {2,000,000, 20,000,000}`, serial + nb, `reps=3`.

No other axes. No new offered-load shapes, no W=32 as a scaling curve, no
NUMA/binding.

---

## 9. Gates and readings

**Structural gates (only pass/fail), identical to exp37:** `agg_ok`,
`futures_completed`, `plain_types_ok`, `lane_ids_ok`, `clean_shutdown`.

**Readings (never gate):** `thr_retention`; p50/p90/p99 retention (**reps=3 →
treat the tail as directional only**; anchor the verdict on the qd→0 +
`thr_retention` crux); `qd_mean_T` / `qd_max_T` trajectory (the crux);
`nb1_minus_serial` overhead floor; `lane_stats` backlog/active; the §3 pre-run
saturation calc as a reading aid. Probe C, if it ever runs, adds the diag
counters as readings only.

**Controls (reuse exp37):** serial positive-erosion control; driver / `lane_stats`
control; **compute-baseline (C-only) flatness — promoted to the primary H-sat
discriminator** (if mixed over-load retention converges toward the C-only
worker-bound throughput as `max_inflight` rises, attribution is saturation, not
retirement); `nb(1)` overhead floor; HOL / qd corroboration; near-load FULL
control must hold.

---

## 10. Verdict criteria (attribution only — not feature SUPPORT/STOP)

* **H-cap** — over-load retention climbs to **≥0.90 with qd→0** as `max_inflight`
  rises. ⇒ exp37's PARTIAL was an undersized cap; non-blocking removes HOL given
  enough in-flight slots; **no retirement optimization motivated yet.**
* **H-retire** — `qd→0` but retention **plateaus below ~0.90**, and the residual
  **shrinks** when compute is coarsened (Probe B). ⇒ motivates a future
  retirement-path look (lock hold/acquire, handoff, accounting) and, only then,
  Probe C counters to pinpoint.
* **H-sat / scheduler saturation** — `qd→0` but retention **plateaus below ~0.90**,
  and the residual does **not** shrink much with coarser compute; mixed
  scheduler/worker pressure beyond the C-only baseline is the likely cause
  (corroborated by the §3 calc showing backlog migrating into the invisible HPX
  scheduler/timer queue). ⇒ this is an honest mixed-load ceiling, **not** a
  retirement-path optimization target.
* **Still cap-bound** — `qd` stays **>0** and retention stalls; the cap is still
  binding or the sweep did not reach the cap the §3 calc requires.
* **INCONCLUSIVE** — a control fails (serial didn't erode, driver-starved, C-only
  baseline not flat, `nb(1)` biased, near-load not FULL), or `qd` and retention
  move inconsistently, or the trend is non-monotone without a clean qd→0 crossing.

---

## 11. Risks

* **High `max_inflight` self-induced churn.** Many concurrent parks/continuations
  can add the very scheduler pressure being probed → non-monotone retention.
  Mitigation: read `qd` alongside — a retention dip at high mi with qd≈0 is the
  H-retire / H-sat signature, not a failure. Run the guarded HT=8 mi=32 cell last
  and report churn/knee explicitly.
* **Bounded blast radius.** Parked tasks are cooperatively suspended, not OS
  threads (256 suspended timers at HT=8 mi=32 is cheap); the active-worker bound
  stays HT. Still run mi=32 cells last and fall back to HT=4-decisive if
  instability appears.
* **Probe B confound.** Coarsening compute changes the park/compute time ratio and
  may stop the serial anchor eroding (exp35). Keep the per-cell positive-erosion
  control and re-calibrate `K_Wp`.
* **H-sat vs H-driver may not separate counter-free.** State this honestly; gate
  Probe C behind A+B ambiguity.
* **Probe C / "no counters" rule.** If counters are ever needed they must stay
  `--diag`-gated / experimental / off-by-default, never on `lane_stats` or the
  row. This first slice adds none.
* **Scope.** Observation-only, synthetic, Rostam-specific; `max_inflight` is a
  diagnostic lever, not sizing/capacity guidance; non-blocking stays experimental
  and op-lane-only.
* **Python stays thin.** Probe A + B use only existing `Runtime` kwargs
  (`max_inflight_per_lane`, `num_lanes`, `n_c`); zero C++ until (and unless) Probe
  C.

---

## 12. Recommendation / first implementation plan

Implement **Probe A + Probe B only**, counter-free, after this design note is
accepted:

* no C++ changes,
* no counters,
* no notify_all / mutex / in-flight-bookkeeping optimization,
* no broad benchmark sweep,
* no README update until actual results exist.

Do the §3 saturation calc first (free, on paper), then let the qd→0 test plus
Probe B's granularity split decide among H-cap / H-retire / H-sat / H-driver — and
whether any retirement-path work or Probe C counters are warranted at all.
