# Spin Core-Boundary Sweep: Does the Knee Move with HPX Threads?

Follow-up to `experiments/08_spin_vs_sleep_coordination/spin_vs_sleep_coordination.md`.
Experiment 08 found that CPU-bound `work_mode="spin"` flattens around 8 lanes
while parked `sleep` keeps scaling, and read that as CPU/core-boundary saturation
rather than Python/facade or HPX-coordination overhead. This experiment tests
that reading directly: sweep `num_lanes` **around** the knee and vary
`hpx_threads`. If the knee is a core-budget effect, adding HPX worker threads
(which compete for the same cores) should move it **earlier**; if it were
HPX-worker starvation, more workers should move it **later**.

Experiment/reporting slice only: no RayX API, facade, driver, analyzer, C++,
result-row, or JSONL-schema change. Lane distribution is read from the existing
per-row `actor_id`.

## 1. Setup

* Driver `bench/run_hpx_python_baseline.py --api engine` (rayx `Engine.submit`,
  `one_by_one`); summaries `bench/analyze_jsonl.py`; orchestration
  `run_spin_core_boundary_sweep.py` (this dir).
* **Spin sweep:** `num_lanes ∈ {4,6,8,10,12,16}` × `hpx_threads ∈ {2,4,8}` ×
  `service_ms ∈ {1,5}` = 36 cells. **Sleep controls (svc=5):** `(L,threads) ∈
  {(4,4),(8,4),(12,4),(8,2),(8,8)}` = 5 cells. `concurrency=16` fixed,
  `requests=500`, `warmup=50`, **3 repeats** → **123 runs**.
* Machine: macOS laptop, **10 cores (4 P + 6 E)**, single locality.
* Architecture note: each lane is a dedicated `std::thread` that runs the
  spin loop itself; the `hpx_threads` worker pool is **separate** and handles the
  submit/promise/future plumbing. So while spinning, the runnable on-core threads
  are the `num_lanes` spinning lanes **plus** active HPX workers **plus** the
  client thread.
* Gates (all 123 passed): `completed==500`, unique ids, spin
  `|service_ms_p50−service_ms| ≤ 5%`.
* Caveat carried throughout: with `concurrency=16` fixed, `offered_depth =
  16/lanes` falls to ~1 by L16, so **absolute** high-lane efficiency also
  includes client-refill underutilization. The knee-vs-threads comparison is made
  **across thread counts at the same `(lanes, concurrency)`**, where that
  confound is identical and cancels.
* Reproduce: `python experiments/09_spin_core_boundary_sweep/run_spin_core_boundary_sweep.py`
  (writes raw JSONL under `results/` and regenerates the curated `aggregate.json`;
  `--quick` runs a tiny smoke matrix without writing the aggregate).

## 2. Measured facts

### 2a. Spin per-lane efficiency (median; 1.0 = ideal `lanes × 1000/service_ms`)

`service_ms = 1`:

| lanes \ hpx_threads | 2 | 4 | 8 |
|---|---|---|---|
| 4  | 0.996 | 0.996 | 0.977 |
| 6  | 0.990 | 0.987 | 0.887 |
| 8  | 0.983 | 0.928 | 0.705 |
| 10 | 0.906 | 0.797 | 0.572 |
| 12 | 0.757 | 0.673 | 0.472 |
| 16 | 0.569 | 0.504 | 0.398 |

`service_ms = 5`:

| lanes \ hpx_threads | 2 | 4 | 8 |
|---|---|---|---|
| 4  | 0.999 | 0.999 | 0.999 |
| 6  | 0.992 | 0.991 | 0.988 |
| 8  | 0.991 | 0.991 | 0.945 |
| 10 | 0.977 | 0.953 | 0.861 |
| 12 | 0.840 | 0.801 | 0.726 |
| 16 | 0.597 | 0.593 | 0.530 |

**Knee onset** (first lane count with efficiency < 0.95):

| | hpx_threads 2 | 4 | 8 |
|---|---|---|---|
| svc=1 | L10 | L8 | L6 |
| svc=5 | L12 | L12 | L8 |

### 2b. Spin throughput ceiling (peak median req/s over the lane sweep)

| | hpx_threads 2 | 4 | 8 |
|---|---|---|---|
| svc=1 | 9099 | 8074 | 6362 |
| svc=5 | 2015 | 1923 | 1742 |

Throughput **saturates** (adding lanes past the knee buys ~no more req/s, only
lower per-lane efficiency), and the ceiling is **lower with more HPX threads**.

### 2c. Sleep controls (svc=5)

| cell | throughput | per-lane eff | svc_p50 |
|---|---|---|---|
| t4 L4  | 652.1  | 0.815 | 6.264 |
| t4 L8  | 1292.7 | 0.808 | 6.263 |
| t4 L12 | 1936.6 | 0.807 | 6.232 |
| t2 L8  | 1298.7 | 0.812 | 6.260 |
| t8 L8  | 1288.7 | 0.805 | 6.260 |

Sleep efficiency is **flat ~0.81 across L4→L12** and **flat across t2/t4/t8 at
L8** — no knee, no thread sensitivity. (The ~0.81 offset is the ~25% sleep
overshoot, a constant tax, not a knee; see experiments 01/08.)

### 2d. Lane balance and tails

* **`actor_id` distribution:** exactly even in every cell — `500/lanes ±1`
  (L4=125, L6=83–84, L8=62–63, L10=50, L12=41–42, L16=31–32), independent of
  threads and mode.
* **Tails:** `total_ms_p99` inflates with both lanes and threads under spin
  (e.g. svc=1 L4: t2/t4 ≈4.06 ms vs t8 ≈6.04 ms), tracking the same contention.
  Spin `service_ms_p50` stays exactly 1.0000 / 5.0000 everywhere.

## 3. Interpretation (six questions)

Facts above; interpretation below, kept scoped.

1. **Does the spin knee move as `hpx_threads` changes?** **Yes — earlier with
   more threads.** At fixed service time the knee-onset lane count falls as
   threads rise (svc=1: L10→L8→L6 for t2→t4→t8; svc=5: L12→L12→L8), and the
   throughput ceiling falls monotonically (svc=1: 9099→8074→6362). Because this
   is compared at the same `(lanes, concurrency)`, it is **not** an
   offered-depth artifact.

2. **Knee vs lanes, threads, or service time?** It tracks **total on-core thread
   demand** — `num_lanes` (spinning) **and** `hpx_threads` (competing workers)
   together — relative to the physical core budget; each added worker thread
   pulls the knee in by ~1–2 lanes. **Service time is a secondary modulator:**
   low `service_ms` (higher request rate → busier HPX workers/client) knees
   **earlier** than high `service_ms` at the same thread count (svc=1 t4 knee L8
   vs svc=5 t4 knee L12). It is **not** governed by lanes alone.

3. **Does `sleep` avoid the knee at the control points?** **Yes.** Parked sleep
   holds ~0.81 efficiency flat through L12 and is **thread-insensitive** at L8
   (0.805–0.812 across t2/t4/t8). Parked lanes don't occupy cores, so neither
   more lanes nor more HPX workers saturate in this range.

4. **Are lanes still evenly balanced by `actor_id`?** **Yes, exactly** —
   `500/lanes ±1` in every cell. The knee is not lane imbalance or routing.

5. **Does this strengthen or weaken the experiment-08 reading?** **Strengthens
   it, and rules out the main alternative.** If the flattening were
   HPX-worker/coordination starvation, **more** workers would push the knee
   **out**; instead more workers pull it **in** and **lower** the ceiling —
   the signature of oversubscribing a fixed core budget, not of insufficient
   workers. Sleep showing no knee and no thread sensitivity separates active CPU
   saturation from generic RayX/HPX coordination overhead. Balanced routing rules
   out imbalance. All three lines converge on the experiment-08 (and 05)
   core-boundary reading.

6. **What to claim / not claim.**
   * **Claim:** For CPU-bound spin at the rayx frontend, per-lane efficiency
     degrades and throughput hits a ceiling once on-core thread demand
     (spinning lanes + HPX workers) approaches/exceeds the physical core count;
     the knee moves **earlier** and the ceiling **lower** as `hpx_threads`
     increases; service time modulates the onset via request-rate/churn; parked
     `sleep` shows neither effect in the same range; lanes stay perfectly
     balanced. This is **hardware/core-boundary / oversubscription** behavior.
   * **Do not claim:** any exact OS-scheduler or P/E-core mechanism (we classify
     the boundary, not the mechanism); that `lanes+threads = 10` exactly (the
     observed onset region is ~12–16 total, loosened by partly-idle HPX workers
     and E-core capacity); portability of absolute numbers (machine-specific,
     4P+6E asymmetry, macOS QoS scheduling); anything about Ray (not measured
     here); and any clean reading of the **absolute** L16 efficiency, which also
     reflects `offered_depth ≈ 1` — only the thread-shift, measured at fixed
     `(lanes, concurrency)`, is the clean signal.

## 4. Scoped takeaways

* The experiment-08 knee is a **core-boundary / oversubscription** effect: it
  moves earlier with more HPX worker threads and the throughput ceiling drops —
  the opposite of an HPX-worker-starvation ceiling.
* For this CPU-bound synthetic workload, **fewer** HPX threads gives the higher
  peak throughput (less oversubscription against the lane threads).
* Below the core budget, lanes scale near-ideally (eff ~0.99) with perfectly
  balanced round-robin; parked `sleep` is unaffected by the same lane/thread
  range.
* Practical reading: match `num_lanes + hpx_threads` to the core budget for
  CPU-bound work; raising `hpx_threads` does not buy spin throughput here.

## 5. Caveats / non-claims

* Single macOS laptop, 10 cores (4 P + 6 E), single locality; on-core spin is
  scheduling-sensitive and these magnitudes are machine-specific.
* Spin is synthetic CPU burn, not model inference; runs kept short.
* rayx frontend only (`--api engine`, one_by_one); no Ray comparison here.
* `concurrency=16` fixed: high-lane absolute efficiency conflates core saturation
  with low offered depth; the knee-vs-threads shift (fixed `(lanes, concurrency)`)
  is the robust result.
* p99/tails softer than medians; throughput and p50 are the firmer signals.
* No OS-scheduler mechanism claim beyond the boundary classification.
* Raw per-request JSONL is not tracked (under `results/`, gitignored); the
  curated `aggregate.json` beside this note is the tracked evidence.
