# Spin-vs-Sleep Coordination at the rayx Frontend

A focused rayx-Python-frontend contrast of the two synthetic service modes —
`work_mode="sleep"` (parked/waiting service time) vs `work_mode="spin"`
(CPU-bound active service time) — across a lane sweep, to classify what the
high-lane flattening actually is. Experiment/reporting slice only: no new RayX
API, no driver change, no result-row or JSONL schema change. The lane
distribution is read from the existing per-row `actor_id`.

Companion to `experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md`
(native + rayx knee sweep) and `experiments/01_sleep_overshoot/sleep_overshoot_note.md`
(the sleep timer artifact). This note isolates the effect at the
`hpx-python-frontend` boundary and adds the `actor_id` lane-distribution check
that 05 did not report.

## 1. Setup

* Driver: `bench/run_hpx_python_baseline.py --api engine` (rayx `Engine.submit`,
  `retire_mode=one_by_one`). Summaries: `bench/analyze_jsonl.py`. Orchestration +
  curated aggregate: `run_spin_vs_sleep.py` (this directory).
* Matrix: `work_mode ∈ {sleep, spin}` × `num_lanes ∈ {1, 2, 4, 8}` ×
  `service_ms ∈ {1, 5}`, `concurrency=16` (≥ lanes, so every lane is offered
  load), `hpx_threads=4`, `requests=500`, `warmup=50`, **3 repeats** → **48
  runs**. Medians across repeats; min/max for throughput.
* Machine: macOS laptop, 10 cores (4 P + 6 E), single locality.
* Gates (all 48 passed): `completed == 500`, unique request ids, and — for spin —
  `|service_ms_p50 − service_ms| ≤ 5%`.
* Reproduce: `python experiments/08_spin_vs_sleep_coordination/run_spin_vs_sleep.py`
  (writes raw JSONL under `results/` and regenerates the curated `aggregate.json`;
  `--quick` runs a tiny smoke matrix without writing the aggregate).

## 2. Measured facts (medians)

| mode | svc | L | thr req/s | per-lane eff | svc_p50 | total_p50 | total_p99 |
|---|---|---|---|---|---|---|---|
| sleep | 1 | 1 | 789.2 | 0.789 | 1.2660 | 20.288 | 20.424 |
| sleep | 1 | 2 | 1583.8 | 0.792 | 1.2629 | 10.117 | 10.173 |
| sleep | 1 | 4 | 3180.5 | 0.795 | 1.2632 | 5.042 | 5.125 |
| sleep | 1 | 8 | 6309.1 | 0.789 | 1.2615 | 2.517 | 2.609 |
| sleep | 5 | 1 | 162.6 | 0.813 | 6.2672 | 98.328 | 100.476 |
| sleep | 5 | 2 | 324.6 | 0.811 | 6.2665 | 49.368 | 50.217 |
| sleep | 5 | 4 | 650.0 | 0.812 | 6.2647 | 24.676 | 25.165 |
| sleep | 5 | 8 | 1302.9 | 0.814 | 6.2596 | 12.453 | 12.682 |
| spin | 1 | 1 | 997.7 | 0.998 | 1.0000 | 16.033 | 16.049 |
| spin | 1 | 2 | 1995.3 | 0.998 | 1.0000 | 8.015 | 8.050 |
| spin | 1 | 4 | 3985.1 | 0.996 | 1.0000 | 4.008 | 4.065 |
| spin | 1 | 8 | 7351.1 | **0.919** | 1.0000 | 2.004 | **3.128** |
| spin | 5 | 1 | 199.9 | 0.999 | 5.0000 | 80.045 | 80.069 |
| spin | 5 | 2 | 399.7 | 0.999 | 5.0000 | 40.024 | 40.079 |
| spin | 5 | 4 | 799.3 | 0.999 | 5.0000 | 20.010 | 20.071 |
| spin | 5 | 8 | 1580.1 | **0.988** | 5.0000 | 10.002 | **13.993** |

`per-lane eff` = `throughput ÷ (lanes × 1000/service_ms)` (1.0 == ideal).

**Lane distribution (`actor_id`):** exactly even in every cell — round-robin
gives `500/L` per lane: L1 = [500], L2 = [250, 250], L4 = [125×4], L8 = [62 or
63]×8. No hot/cold lane, no starvation, identical for sleep and spin.

## 3. Interpretation (answers to the six questions)

Facts above; interpretation below, kept deliberately scoped.

1. **Does `spin` reduce the sleep overshoot?** **Yes, fully.** Sleep `svc_p50`
   sits at ~1.26 ms (svc=1) / ~6.26 ms (svc=5) — a stable ≈26% / ≈25% overshoot.
   Spin `svc_p50` is **exactly 1.0000 / 5.0000**. The CPU-bound loop tracks the
   target on the same `steady_clock` the metrics use, so the sleep/wakeup timer
   artifact (experiment 01) disappears.

2. **Does `spin` expose CPU saturation earlier than `sleep`?** **Yes.** Sleep
   per-lane efficiency is **flat at ~0.79–0.81 from L1 through L8** with no
   degradation — parked lanes don't occupy cores, so 8 of them don't oversubscribe
   the 10-core box. Spin holds **~0.99 through L4** then **drops at L8** (0.919 at
   svc=1, 0.988 at svc=5), and L8 is the **only** place `total_ms_p99` separates
   from p50 (2.00→3.13, 10.00→14.0). So the core-boundary cost shows up under
   active spinning and is invisible under parked sleep.

3. **Do lanes distribute evenly by `actor_id`?** **Yes, exactly** — `500/L` per
   lane in every cell, sleep and spin alike. The flattening is **not** lane
   imbalance.

4. **Does throughput scale with `num_lanes`?** Both scale up; neither regresses.
   **Sleep scales linearly to L8** at a constant ~0.8 efficiency (the offset is
   the overshoot tax, not a coordination loss). **Spin scales near-linearly to
   L4** then **flattens mildly at L8** (the only sub-linear point). The flattening
   is svc-dependent: worse at svc=1 (higher request rate / more churn, 0.919)
   than svc=5 (0.988).

5. **Is the flattening Python/facade, HPX coordination, sleep overshoot, CPU
   saturation, or laptop noise?** Most consistent with **CPU saturation at the
   core boundary under active spinning**, and **inconsistent** with the others:
   * *Not Python/facade/GIL:* the facade path is identical for both modes, yet
     sleep scales linearly to L8 while only spin flattens. A facade/GIL ceiling
     would cap **both** modes the same way. (Corroborates experiments 04 and 06.)
   * *Not HPX coordination overhead:* that would grow with lanes in **both** modes;
     sleep shows no per-lane efficiency loss through L8.
   * *Not the sleep overshoot:* that is a **constant proportional offset**
     (~0.8 efficiency), present at every lane count, not a lane-dependent knee —
     and it is absent under spin, which still flattens at L8.
   * *CPU saturation:* the spin-only L8 efficiency drop **and** the spin-only L8
     p99 inflation line up with the on-core budget (8 spinning lane threads + 4
     HPX workers + 1 client thread on 10 cores).
   * *Laptop scheduling:* contributes to **magnitude** (svc=1/L8 more affected,
     and these numbers are machine-specific), but the **sign** — spin L8 below
     ideal while sleep L8 stays linear — is consistent across both service times.

   Net: this is the **hardware/core-boundary** regime of experiment 05, now
   cleanly isolated at the rayx frontend by the sleep-vs-spin contrast. We
   classify the boundary; we do not assert the exact OS scheduler mechanism.

6. **Next most useful experiment.** A **finer spin lane sweep across the
   core boundary** at the rayx frontend — `num_lanes ∈ {6, 8, 10, 12}` at
   `hpx_threads=4`, svc 1 and 5 — to map the shape of the knee (onset, slope)
   rather than only catching its onset at L8. (A CPU-bound Ray reference would be
   a separate, explicitly caveated comparison; out of scope here.)

## 4. Scoped takeaways

* `spin` removes the sleep-fidelity artifact (observed service exactly 1.0 / 5.0
  ms) and exposes a CPU-saturation onset at L8 that parked `sleep` hides.
* Up to the core budget the lane mechanism scales near-ideally (spin eff ~0.99
  to L4) with perfectly balanced round-robin routing.
* The high-lane flattening is a CPU/core-boundary effect, **not** Python/facade
  overhead and **not** HPX coordination cost.
* `sleep` and `spin` are complementary probes: `sleep` for parked-lane
  queueing/timer behavior, `spin` for CPU saturation. Use the one that matches
  the question.

## 5. Caveats / non-claims

* Single macOS laptop, 10 cores, single locality; on-core spin is more
  scheduling-sensitive than parked sleep, and these magnitudes are
  machine-specific.
* Spin is synthetic CPU burn, not model inference; runs were kept short.
* rayx frontend only (`--api engine`, one_by_one). No Ray here; a CPU-bound Ray
  reference would be a separate comparison.
* p99/tails are softer than medians; throughput and p50 are the firmer signals.
* The knee is observed at **onset** (L8 at threads=4); it is not mapped past the
  core boundary (see §3.6).
* No mechanism claim about the OS scheduler beyond the boundary classification.
* Raw per-request JSONL is not tracked (kept under `results/`, gitignored); the
  curated `aggregate.json` beside this note is the tracked evidence.
