# RayX Lane-Backend Load Divergence: `lane_impl="std"` vs `lane_impl="hpx"`

Exp21 proved the two rayx lane backends are **contract-equivalent**. This
experiment asks the follow-on question parity left open:

> Given identical semantics, **where** do `lane_impl="std"` (the std::thread
> `ServiceLane` anchor) and `lane_impl="hpx"` (the cooperative HPX-thread
> `HpxLane`) **structurally diverge under load** — and why is that divergence a
> **scheduling-mechanism** fact rather than a speedup?

Mechanism / structure evidence only. **No** new RayX API, **no** result-row / v1
benchmark-JSONL schema change, **no** analyzer / driver / CI change, **no** change
to the public `Future` ownership model, and **no** HPX internals exposed.
Companion reference: `docs/reference/rayx_frontend_design.md` §13 (the `lane_impl`
backend seam). This is the opt-in serialized rayx `HpxLane` backend, **not** the
exp16 native single-lane mechanism probe and **not** the exp20
`hpx::async` / `hpx::dataflow` task-pool probe.

## 1. The two mechanisms

| | `lane_impl="std"` / `ServiceLane` | `lane_impl="hpx"` / `HpxLane` |
|---|---|---|
| lane worker | one `std::thread` per lane | one `hpx::thread` per lane |
| true parallelism set by | OS / core count | the `hpx_threads` HPX worker pool |
| relation to `hpx_threads` | **independent** | **bounded by it** |
| parked sleep | blocking `std::this_thread::sleep_for` | cooperative `hpx::this_thread::sleep_for` (**yields** the HPX worker) |
| spin (busy on-core) | spins on its own OS thread | spins on an HPX worker (**does not yield**) |

`work_mode="spin"` is used here as a synthetic **CPU-bound diagnostic /
calibration mode** to expose the scheduling bound. It is **not** the serving
design; the serving model is parked (sleep) synthetic service. Sleep-mode,
spin-mode, and the cooperative HPX lane should not be conflated.

## 2. What this experiment does and does not test

**Does test — firm structural gates (machine-independent, hold for both backends):**

* **G1 completion** — `rows == offered`, exactly one row per request (no loss
  under load).
* **G2 per-lane FIFO** — within each lane, `end_ns` is monotonic in submit order
  (`inversions == 0`); a serialized FIFO lane services one request at a time.
* **G3 lane_stats sanity** — `active` + `queue_depth` observed under a controlled
  **sleep** load, idle after drain (run on sleep so the snapshot is not subject to
  spin worker-pool starvation).
* **G4 actor_id prefix** — `act-hpx-` (std) / `act-hpxl-` (hpx) on every row.
* **G5 cross-backend completion parity** — same `offered` / `completed` per
  matching `(hpx_threads, num_lanes, workload)` cell.

**Does not gate — observation only (mechanism evidence, never a gate):**

* `drain_wall_ms`, `overlap_ratio`, `max_active_lanes` (sampled from
  `lane_stats()`), `max_total_queue`, and the inferred convergence / divergence
  pattern. **No** speedup claim and **no** "HPX beats Ray" claim is made.
* `lane_stats().active` is valid RayX observability but is **not** a perfect proof
  of true HPX worker-level concurrency: under spin the stats call itself needs
  HPX-worker access and can be delayed by saturated workers (see §5). That is
  exactly why the spin bound is **reported, not gated**.

Out of scope: the exp16 native single-lane probe; the exp20 task-pool mechanisms;
anything outside the synthetic harness (not Ray Serve, not a Ray object store, not
real model inference).

## 3. Setup

* Because the HPX runtime is a **process resource** (one `hpx::start` per process
  with a fixed worker count), each `(backend, hpx_threads)` pair runs in its **own
  subprocess**. Within a subprocess, `num_lanes` and `workload` vary via
  short-lived `Engine` contexts, as `bench/smoke_rayx.py` does.
* Each cell submits a burst of `REQS_PER_LANE = 2` requests per lane
  (`offered = 2 × num_lanes`), round-robined across lanes (so every lane gets a
  real per-lane FIFO sequence), then samples `lane_stats()` while the burst is in
  flight and drains via `get()`.
* `overlap_ratio = (offered × service_ms) / drain_wall_ms` — total requested
  service divided by wall time, an **observational** proxy for effective
  concurrent lanes: ≈ `num_lanes` when lanes overlap, ≈ `hpx_threads` when a
  non-yielding (spin) `HpxLane` is bound by the worker pool.

## 4. Matrix

| Axis | Full | Quick |
|---|---|---|
| backend | std, hpx | std, hpx |
| `hpx_threads` (subprocess axis) | 1, 2, 4 | 1, 2 |
| `num_lanes` | 2, 4, 8 | 2, 4 |
| `workload` | sleep, spin | sleep, spin |
| repeats | 3 | 1 |
| sleep `service_ms` | 120 | 80 |
| spin `service_ms` | 40 | 30 |

## 5. Results

All firm structural gates passed (`all_structural_gates_passed: true`,
`gate_failures: []`; G3 passed for all six `(backend, hpx_threads)` workers).
Curated evidence is in `aggregate.json` beside this report. Headline numbers below
are the full run on this machine (median over 3 repeats), at `num_lanes = 8`.

**Sleep → convergent (both overlap parked waits).** `overlap_ratio` ≈ `num_lanes`
for **both** backends, and for `HpxLane` it is **independent of `hpx_threads`** —
cooperative parking yields the worker, so even `hpx_threads = 1` overlaps all 8
lanes:

| backend | `hpx_threads` | sleep `overlap_ratio` (L=8) | drain wall |
|---|---|---|---|
| std | 1 / 2 / 4 | 7.68 / 7.68 / 7.70 | ~250 ms |
| hpx | 1 / 2 / 4 | 7.93 / 7.93 / 7.93 | ~242 ms |

**Spin → divergent (the scheduling bound).** `ServiceLane` overlaps ≈ `num_lanes`
regardless of `hpx_threads` (its `std::thread`s are scheduled across cores);
`HpxLane`'s overlap is **bounded near `hpx_threads`** (non-yielding work occupies
the HPX worker pool):

| backend | `hpx_threads` | spin `overlap_ratio` (L=8) | drain wall |
|---|---|---|---|
| std | 1 / 2 / 4 | 7.93 / 7.72 / 7.59 | ~80–84 ms |
| hpx | 1 / 2 / 4 | **1.00 / 2.00 / 3.83** | 640 / 320 / 167 ms |

The drain wall makes the mechanism explicit: `hpx`, `hpx_threads=1`, `num_lanes=8`,
spin drains 16 requests in ~640 ms (≈ 16 × 40 ms, **serialized** on one worker),
while `std` drains the same 16 in ~80 ms (≈ 2 rounds of 8 across cores). At
`hpx_threads=4` the `HpxLane` spin overlap rises to ~3.83 — tracking the worker
pool, not the lane count.

**`max_active_lanes` is the fragile, secondary observation.** Sampled from
`lane_stats()`, it reads the true count for `std` (e.g. spin L=8 → `8`) but reads
~`0` for `hpx` under spin: the stats snapshot hops onto an HPX worker, and with the
pool saturated by non-yielding spin loops that hop is starved, so the sampler
rarely catches a lane mid-service. **This is a measurement artifact of observing a
saturated cooperative pool, not zero concurrency** — `overlap_ratio` (from drain
wall) is the robust evidence, and it is precisely why this signal is observation,
never a gate.

## 6. Interpretation

Within this synthetic harness, the backends are contract-equivalent (exp21) but
diverge under load along one structural axis — **how concurrency is bounded**:

* **Sleep (the serving model): convergent.** Parked time is not "held." Both
  backends overlap parked waits — `ServiceLane` via independent OS threads,
  `HpxLane` via cooperative yield (which overlaps all lanes even at
  `hpx_threads=1`). The choice is contract-invisible here.
* **Spin (a CPU-bound diagnostic): divergent.** Non-yielding work exposes that
  `ServiceLane`'s parallelism is set by the OS/core count and is independent of
  `hpx_threads`, while `HpxLane`'s true concurrency is bounded by the
  `hpx_threads` worker pool.

This is a **mechanism** statement, not a performance ranking: neither backend is
"faster." Which bound is appropriate is a workload-and-deployment question
(`HpxLane` ties lane concurrency to an explicit, configurable HPX worker budget;
`ServiceLane` leans on the OS scheduler), and it explains why `work_mode="spin"`
is kept as a calibration knob rather than the serving design.

## 7. Caveats

* **Mechanism / structure, not performance.** Every timing-derived figure
  (`drain_wall_ms`, `overlap_ratio`, `max_active_lanes`) is an observation on one
  machine; no speedup or "HPX beats Ray" claim is made or implied.
* **`max_active_lanes` under spin is unreliable for `hpx`** (stats-hop starvation,
  §5); it is a secondary signal, not gated.
* **`work_mode="spin"` is a synthetic CPU-bound calibration mode**, not the serving
  design; sleep-mode and spin-mode results are not interchangeable.
* **Scope.** Opt-in serialized rayx `HpxLane` backend (`lane_impl="hpx"`); **not**
  the exp16 native single-lane probe, **not** the exp20 task/dataflow probe, and
  not a statement about HPX vs Ray, Ray Serve, object stores, or real inference.
* Raw per-worker JSON is experiment-local scratch under `results/` (gitignored)
  and is **not** the v1 benchmark JSONL schema; the tracked evidence is
  `aggregate.json` + this report.

## 8. Reproduction

```bash
# quick smoke (smaller matrix; no aggregate.json written)
python experiments/22_rayx_hpxlane_load_divergence/run_rayx_hpxlane_load_divergence.py --quick

# full run (writes the curated aggregate.json beside this report)
python experiments/22_rayx_hpxlane_load_divergence/run_rayx_hpxlane_load_divergence.py

# optional overrides
#   --hpx-threads "1,2,4"  --num-lanes "2,4,8"  --workload {sleep,spin,both}  --repeats N
```

Requires the `_rayx` extension built (`cmake --build python/build`). The RayX
contract smoke (`python bench/smoke_rayx.py`) covers the same `lane_impl`
selection and hpx-backend semantics as a fast laptop check.
