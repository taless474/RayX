# Spin Chunked Synthetic Service — HPX-native + rayx (no Ray)

A bounded two-engine characterization of **chunked synthetic service under
`work_mode="spin"`** — active, CPU-bound service — for the two HPX-backed engines
**only**: HPX-native and the rayx Python frontend. Benchmark 09 compared
Ray / HPX-native / rayx in **sleep** mode; this follow-up drops Ray, switches to
**spin**, and asks the active-CPU question that sleep cannot answer:

> When chunked service is CPU-bound (`spin`, no sleep-timer overshoot), does
> splitting the same total active service into more chunks preserve it (at
> `delay=0`), does a parked `chunk_delay_ms` still add lane-occupancy time, does
> rayx track HPX-native, and where do chunk/lane/core pressure start to bite?

Each request's **total active** `service_ms` is split into `chunks` equal active
**spin** steps with `chunks-1` **parked** `chunk_delay_ms` inter-chunk gaps.
**Synthetic timing only** — **not** real token streaming, **not** model inference,
**not** Ray Serve, **not a Ray comparison**, no object-store/task semantics, **no**
per-chunk events: one request → one row. No new API, **no JSONL schema change**
(still `1`), no analyzer change. Companions: `experiments/12_chunked_service/`
(rayx-local sleep+spin chunk characterization), `benchmarks/09_chunked_service_cross_driver/`
(sleep cross-driver incl. Ray), and `experiments/08_*` / `09_*` (the spin
core-boundary / oversubscription findings this report reads L8 against).

## 1. Setup

* **Drivers (existing, unchanged APIs):** `hpx_impl/build/hpx_synthetic_baseline`
  (`hpx-intra-locality`) and `bench/run_hpx_python_baseline.py --api engine`
  (`hpx-python-frontend`), both `--work-mode spin`, rolled up by
  `bench/analyze_jsonl.py` (schema `"1"`). The native binary is pinned with
  `--hpx:threads=4` so **both engines run 4 HPX worker threads** — without that
  the native default thread count would confound the high-lane spin throughput
  comparison (service fidelity is identical either way; see §2d).
* **Runner:** `run_spin_chunked_service.py` (this package) drives the matrix, one
  driver subprocess per cell, computes lane (`actor_id`) distribution from the
  existing rows, and writes the curated `aggregate.json`. Raw per-run JSONL is
  scratch under `results/` (gitignored). `--quick` runs a tiny smoke (no
  aggregate written).
* **Matrix:** engines {HPX-native, rayx} × `work_mode = spin` × lanes {1, 4, 8} ×
  `hpx_threads = 4` × `service_ms = 20`→ **`service_ms = 8`** (total active) ×
  chunks {1, 2, 4, 8} × `chunk_delay_ms` {0, 2} × **2 repeats** × **100 requests**
  (+10 warmup), `retire_mode = one_by_one`. **96 runs / 48 cells**; medians across
  repeats. Concurrency = lanes (one in-flight per lane).
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all passed):** every run — rows == requests, unique `request_id`, all
  `completed`, `schema_version == "1"`, rows echo requested `chunks` /
  `chunk_delay_ms`, `work_mode == "spin"`, round-robin lane balance
  (`lanes_seen == lanes`, max−min ≤ 1), no negative service. Cross-cell —
  **`delay=0`** spin service stays near the 8 ms target **and** chunk-invariant
  (per engine/lanes, `max < 1.4×min`); **`delay>0`** lifecycle delta vs the same
  `delay=0` cell ≈ `(chunks-1)×delay` (loose 0.5×–2.5× band; `chunks=1` ⇒ ~0).
  Bands are deliberately **loose** (magnitudes are host-specific); no exact-timing
  asserts.
* **Reproduce:**
  `python experiments/14_spin_chunked_service/run_spin_chunked_service.py`
  (`--quick` for the smoke subset). Curated evidence: `aggregate.json` + this note.

## 2. Measured facts (medians, ms / req·s⁻¹)

### 2a. `service_ms_observed` p50 at `delay=0` — spin active preservation

| chunks | HPX-native L1 / L4 / L8 | rayx L1 / L4 / L8 |
|---|---|---|
| 1 | 8.000 / 8.000 / 8.000 | 8.000 / 8.000 / 8.000 |
| 2 | 8.000 / 8.000 / 8.000 | 8.000 / 8.000 / 8.000 |
| 4 | 8.000 / 8.000 / 8.000 | 8.000 / 8.000 / 8.000 |
| 8 | 8.000 / 8.000 / 8.000 | 8.000 / 8.000 / 8.001 |

Spin **preserves total active service exactly** — `8.000 ms` regardless of chunk
count, lane count, or engine (max deviation `0.0006 ms`). Unlike sleep (benchmark
09, where active service rose ~4–5 ms from chunks 1→8 as per-sleep overshoot
accumulated), the spin step is wall-clock-exact, so splitting it N ways adds **no**
measurable active time. This is the clean control that the sleep axis cannot give.

### 2b. Delay effect: `service_ms_observed` p50 at `delay=2` (lifecycle, L1)

| chunks | gaps | nominal `8+(chunks-1)×2` | HPX-native | rayx | Δ vs delay=0 |
|---|---|---|---|---|---|
| 1 | 0 | 8 | 8.000 | 8.000 | ~0.0 |
| 2 | 1 | 10 | 10.51 | 10.52 | +2.5 |
| 4 | 3 | 14 | 15.53 | 15.54 | +7.5 |
| 8 | 7 | 22 | 25.56 | 25.60 | +17.6 |

`chunk_delay_ms > 0` adds parked lane-occupancy ≈ `(chunks-1)×delay`, **inflated by
the parked gap's own sleep overshoot**: each 2 ms gap lands at ~2.5 ms (≈25%
overshoot), so the delta runs ~1.25× nominal. `chunks=1` adds ~0 (no gaps). The
split is the inverse of 2a: the **active** part stays exact (spin), the **parked**
part carries the sleep-timer overshoot. So with `delay>0`, `service_ms_observed`
is **lifecycle / lane-occupancy time** (active + parked gaps), not active-only —
identical across both engines and all lane counts.

### 2c. Throughput (req/s) and per-lane efficiency, `delay=0`

| | HPX-native thr / eff | rayx thr / eff |
|---|---|---|
| L1 c1 | 124.9 / 0.999 | 124.7 / 0.998 |
| L4 c1 | 491.9 / 0.984 | 487.0 / 0.974 |
| L8 c1 | 850.5 / 0.851 | 953.9 / 0.954 |
| L8 c8 | 810.3 / 0.810 | 918.8 / 0.919 |

Throughput scales ~4× from 1→4 lanes (efficiency ~0.98) for both engines. At
**L8** per-lane efficiency drops to ~0.81–0.95 and `total_ms_p50` rises (native
8.0→8.7, rayx 8.0→8.02): the spin work no longer fits cleanly. chunk count barely
moves the median (≤1% at L8); only the tail jitters (§2d).

### 2d. rayx vs HPX-native, and chunk-boundary tail

* **Service fidelity:** identical — both `8.000` at `delay=0` and the same
  `8/10.5/15.5/25.6` lifecycle at `delay=2` (§2a/2b). rayx **is** the same C++
  service lane, so the active/parked split matches the native binary exactly.
* **Throughput:** within a few % at L1/L4; at L8 rayx runs ~10% **higher** than
  native (eff 0.92–0.95 vs 0.81–0.85, both at 4 HPX threads). This is a small
  client-loop / scheduling difference at saturation, **not** a service-lane
  difference, and it is the opposite direction from sleep-mode dispatch cost — do
  not read it as a control-plane ranking.
* **Chunk-boundary tail:** at `delay=0` the median is flat in chunk count, but the
  p99 jitters mildly with more chunks (native L1: c1 p99 `8.000` → c8 p99 `9.51`;
  rayx stays ~`8.003`). More boundaries = more scheduling points = a little more
  tail, no median cost.

## 3. Interpretation

1. **Does spin chunking preserve active service at `delay=0`?** **Yes, exactly** —
   `8.000 ms` for chunks 1/2/4/8 across all lanes and both engines (2a). Spin
   removes the sleep overshoot that made benchmark 09's "preservation" only
   approximate; here it is exact, confirming chunking splits active work without
   adding any.
2. **Does parked `chunk_delay_ms` add lifecycle even under spin?** **Yes** —
   ≈ `(chunks-1)×delay`, monotone in chunk count (2b), each gap carrying ~25%
   sleep overshoot. The active part stays exact while the parked part behaves like
   sleep — a clean decomposition of `service_ms_observed`.
3. **Does rayx track HPX-native?** **Yes** — identical service fidelity (same C++
   lane); throughput within a few % except a small ~10% rayx edge at the L8
   saturation point, which is a client/scheduling effect, not a lane difference
   (2d).
4. **Does chunk count add measurable overhead?** **Negligible at the median** —
   active service and throughput are flat in chunk count at `delay=0`; the only
   cost is mild p99 tail jitter from extra boundaries (2c/2d). (At `delay>0` the
   change is the intended parked time, not overhead.)
5. **Does L8 show the experiment 08/09 core-boundary effect under spin?**
   **Yes, in milder form.** Per-lane efficiency falls from ~0.98 (L1/L4) to
   ~0.81–0.95 (L8) and `total_ms` p50/p99 inflate (2c) — the CPU/core-budget
   oversubscription signature from experiments 08/09 (4 P-cores, spin lanes
   contending), now with `hpx_threads` fixed at 4 and on a wall-clock-bounded spin,
   so the knee is gentler than 08/09's. The effect is **lane×core** pressure, not
   chunking: it is present at chunks=1 and barely changes with chunk count.
6. **What readers should not conclude:** see §4.

## 4. Non-claims / caveats

* **Not real token streaming, not model inference, not Ray Serve, not a Ray
  comparison, no object-store/task semantics, no per-chunk events** — synthetic
  timing only; one request → one row. `service_ms` is duration control, never a
  payload or token. Ray is deliberately absent from this slice.
* **Spin mode only.** This isolates the active-CPU axis; the sleep cross-engine
  comparison (incl. Ray, with its sleep-fidelity advantage) is benchmark 09, not
  here. Do not mix the two.
* **L8 efficiency loss is lane×core pressure, not chunking or a frontend cost.**
  It appears at chunks=1 and is the experiment 08/09 core-boundary effect; the
  rayx-vs-native throughput ordering at L8 is a saturation-point client/scheduling
  artifact, not a control-plane ranking.
* **Both engines pinned to 4 HPX worker threads.** The cross-engine numbers are
  only apples-to-apples because of `--hpx:threads=4` on the native binary; the
  native default thread count gives a different (confounded) high-lane throughput.
* **Machine-specific magnitudes.** Single laptop (4 P + 6 E); all ms/throughput
  numbers and the L8 efficiency drop are host-specific. The firm signals are the
  **structural** ones: exact active preservation at `delay=0`, the
  `(chunks-1)×delay` parked add at `delay>0`, correct `chunks`/`chunk_delay_ms`
  echo, schema `1`, exact round-robin lane balance, and rayx matching HPX-native
  on service fidelity.
* **Bounded matrix** (2 repeats, 100 requests). Cells carry a few-tenths-ms /
  few-% run-to-run noise; conclusions rest on the structural bands, which hold.
