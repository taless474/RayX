# Chunked Synthetic Service — Cross-Driver (Ray / HPX-native / rayx)

A bounded three-way comparison of **chunked synthetic service** now that
`--chunks` / `--chunk-delay-ms` are first-class benchmark-driver flags on all
three engines. Each request's **total active** `service_ms` is split into `chunks`
equal active steps with `chunks-1` **parked** `chunk_delay_ms` inter-chunk gaps.
The question:

> Across Ray, HPX-native, and rayx, does chunking preserve total active service
> (at `delay=0`) and add lane-occupancy ≈ `(chunks-1)×delay` (at `delay>0`), and
> does it change the earlier control-plane story or just add lifecycle structure?

**Synthetic timing only** — **not** real token streaming, **not** model
inference, **not** Ray Serve, no object-store/task semantics, **no** per-chunk
events: one request → one row. No new API, **no JSONL schema change** (still `1`),
no analyzer change. Companion: `experiments/12_chunked_service/` (rayx-local
chunk characterization) and `docs/experiment_plan.md` §3 status note.

## 1. Setup

* **Drivers (existing, unchanged APIs):** `bench/run_ray_baseline.py`
  (`ray-actor-process`), `hpx_impl/build/hpx_synthetic_baseline`
  (`hpx-intra-locality`), `bench/run_hpx_python_baseline.py --api engine`
  (`hpx-python-frontend`), rolled up by `bench/analyze_jsonl.py` (schema `"1"`).
* **Runner:** `run_chunked_cross_driver.py` (this package) drives the matrix, one
  driver subprocess per cell, and writes the curated `aggregate.json`. Raw per-run
  JSONL is scratch under `results/` (gitignored). `--quick` runs a tiny smoke.
* **Matrix (reduced first cut):** engines {Ray, HPX-native, rayx} × lanes/actors
  {1, 4} × `service_ms = 20` (total active) × chunks {1, 4, 8} ×
  `chunk_delay_ms` {0, 2} × **2 repeats** × **100 requests** (+10 warmup),
  `retire_mode = one_by_one`, `work_mode = sleep` (all engines; **Ray is
  sleep-only**). **72 runs / 36 cells**; medians across repeats. Concurrency =
  lanes (one in-flight per lane).
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all passed):** all rows `completed`; every row echoes the requested
  `chunks` / `chunk_delay_ms`; `schema_version == "1"` (rows + analyzer); analyzer
  summarizes every run; **`delay=0`** keeps observed active service ~flat across
  chunks (overshoot-aware band, `max < 1.8×min` per engine/lanes); **`delay>0`**
  adds ≈ `(chunks-1)×delay` lifecycle (loose band, `chunks=1` ⇒ ~0). Timing bands
  are deliberately **loose** (sleep magnitudes are host-specific).
* **Reproduce:**
  `python benchmarks/09_chunked_service_cross_driver/run_chunked_cross_driver.py`
  (`--quick` for the smoke subset). Curated evidence: `aggregate.json` + this note.

## 2. Measured facts (medians, ms / req·s⁻¹)

### 2a. `service_ms_observed` p50 at `delay=0` — active preservation + overshoot (L1)

| chunks | Ray | HPX-native | rayx |
|---|---|---|---|
| 1 | **21.02** | **24.60** | 25.01† |
| 4 | 22.55 | 28.52 | 29.12 |
| 8 | 22.59 | 29.11 | 29.32 |

†The full-sweep cell read `28.41` (run-ordering noise — rayx ran last in the
sequential loop under residual load); a clean targeted rerun (200 req × 3 repeats,
no co-running engines) gives rayx **25.01** vs native **24.67** — i.e. rayx ≈
HPX-native (same C++ lane). The table shows the rechecked value; `aggregate.json`
preserves the raw single-sweep number with this provenance.

At `delay=0` the **total active work is preserved** (~20 ms target, no blow-up
with chunk count). Observed service rises only mildly with `chunks` — the
**per-sleep overshoot accumulates** (N small `sleep_for`s overshoot a bit more
than one large one): Ray +1.6 ms (1→8), HPX/rayx +4–5 ms. This is a sleep-fidelity
artifact, not extra work (spin would be exact — cf. experiment 12).

### 2b. Delay effect: `service_ms_observed` p50 delta `(delay=2) − (delay=0)` (L1)

| chunks | gaps | nominal `(chunks-1)×2` | Ray Δ | HPX Δ | rayx Δ |
|---|---|---|---|---|---|
| 1 | 0 | 0 | ~0.0 | +0.8 | ~0.0 |
| 4 | 3 | 6 | +6.8 | +8.7 | +8.8 |
| 8 | 7 | 14 | +15.8 | +21.0 | +21.0 |

`chunk_delay_ms > 0` adds parked lane-occupancy ≈ `(chunks-1)×delay`, inflated by
the same blocking-sleep overshoot the gaps carry: **Ray ~1.1×** the nominal,
**HPX/rayx ~1.5×**. `chunks=1` adds ~0 (no gaps). So with `delay>0`,
`service_ms_observed` is **lifecycle/lane-occupancy time** (active + parked gaps),
not active-only.

### 2c. Throughput (req/s), L1 → L4, `delay=0`

| | Ray L1 / L4 | HPX L1 / L4 | rayx L1 / L4 |
|---|---|---|---|
| chunks=1 | 43.2 / 173.4 | 42.2 / 142.8 | 37.1 / 144.5 |
| chunks=8 | 40.4 / 161.1 | 33.7 / 130.8 | 34.1 / 135.7 |

Throughput scales ~3.4–4× from 1→4 lanes for every engine. It falls with chunk
count + delay (longer lifecycle ⇒ fewer req/s): the longest cell, Ray L1 c8 d2,
is 24.2 req/s (≈38 ms lifecycle). Ray's slightly higher throughput here tracks its
**shorter** lifecycle (less sleep overshoot), **not** lower control overhead.

### 2d. Sleep fidelity / overshoot (c1 d0, 20 ms target)

Ray **~5%** overshoot (21.0), HPX-native **~23%** (24.6), rayx **~25%** (25.0,
rechecked). Ray's sleep is the most faithful; HPX's blocking `sleep_for` overshoots
more — exactly the experiment-01 backend sleep-fidelity gap, now seen on both the
active chunks **and** the parked gaps.

## 3. Interpretation

1. **Does `delay=0` preserve total active service across engines?** Yes — observed
   active stays ~flat across chunks (2a), no blow-up; the mild rise is per-sleep
   overshoot accumulation, a fidelity artifact, not added work. (Spin would be
   exact; this is the sleep axis.)
2. **Does `delay>0` add the expected lane-occupancy?** Yes — ≈ `(chunks-1)×delay`,
   monotone in chunk count (2b), inflated by the gap's blocking-sleep overshoot
   (Ray ~1.1×, HPX/rayx ~1.5×).
3. **Ray vs HPX/rayx sleep fidelity?** Ray ~5% vs HPX-native/rayx ~23–25% overshoot
   (2d), on both active chunks and parked gaps — the documented backend gap, not a
   control-plane effect.
4. **Does rayx stay close to HPX-native?** Yes — rayx tracks native within ~1 ms /
   a few % (2a/2c; the lone c1 outlier was run-ordering noise, confirmed by the
   recheck). rayx **does not collapse toward Ray** — it inherits the native lane's
   sleep behavior because it *is* the same C++ service lane.
5. **Does chunking change the control-plane story, or add lifecycle structure?**
   It **adds lifecycle structure.** This benchmark is service-dominated (20 ms+),
   so dispatch overhead is a small fraction and all three cluster; chunking extends
   **lane-occupancy/cadence** (parked gaps), it does not alter dispatch cost. The
   control-plane story (Ray's actor-process boundary vs HPX intra-locality) lives
   at the **no-op floor** (benchmark 06), and is untouched here.
6. **What readers should not conclude:** see §4.

## 4. Non-claims / caveats

* **Not real token streaming, not model inference, not Ray Serve, no
  object-store/task semantics, no per-chunk events** — synthetic timing only; one
  request → one row. `service_ms` is duration control, never a payload or token.
* **Not a universal performance claim.** Ray's lower *overshoot* here is **sleep
  fidelity**, not lower control cost — at the no-op dispatch floor the HPX paths
  have far lower per-call overhead (benchmark 06). Do not read 2c/2d as "Ray is
  faster."
* **Sleep mode only.** Spin chunking (rayx/HPX-only, exact active service) is a
  deliberate separate axis (experiment 12); not mixed in here.
* **Machine-specific magnitudes.** Single laptop (4 P + 6 E); all ms/throughput
  numbers (and the ~5% vs ~25% overshoot) are host-specific. The firm signals are
  the **structural** ones: active preservation at `delay=0`, the `(chunks-1)×delay`
  add at `delay>0`, correct `chunks`/`chunk_delay_ms` echo, schema `1`, and rayx
  tracking HPX-native.
* **Reduced first matrix** (2 repeats, 100 requests). Cells carry a few-ms
  run-to-run noise (the rayx c1 d0 outlier, §2a, is the clearest case and was
  rechecked). Tighter numbers would need more repeats/requests; the conclusions
  rest on the structural bands, which hold.
