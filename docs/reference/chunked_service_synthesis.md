# Chunked Synthetic Service — Sleep vs Spin Synthesis

A short cross-reading of the two chunked-service result packages:

* [`benchmarks/09_chunked_service_cross_driver/`](../../benchmarks/09_chunked_service_cross_driver/chunked_service_cross_driver.md)
  — **sleep** mode, all three drivers (Ray / HPX-native / rayx).
* [`experiments/14_spin_chunked_service/`](../../experiments/14_spin_chunked_service/spin_chunked_service.md)
  — **spin** mode, HPX-native + rayx only (no Ray).

It adds **no** new data, API, schema, or run — it only connects the two
existing curated `aggregate.json`/report packages so the combined story is
readable in one place. Companions: `experiments/12_chunked_service/` (rayx-local
sleep+spin characterization) and `docs/experiment_plan.md` §3 status note.

**Synthetic timing only** — **not** real token streaming, **not** model
inference, **not** Ray Serve, no object-store/task semantics, **no** per-chunk
events: one request → one row, JSONL schema still `1`.

## The shared model both packages measure

A request's **total active** `service_ms` is split into `chunks` equal active
steps separated by `chunks-1` **parked** `chunk_delay_ms` inter-chunk gaps. So:

* `service_ms` is **total active service** — it does not grow with `chunks`.
* `chunks` splits that active service into multiple steps; it does not add work.
* `chunk_delay_ms` inserts parked inter-chunk lifecycle time.
* `service_ms_observed` is therefore **active-only when `delay=0`** and
  **lifecycle / lane-occupancy time (active + parked gaps) when `delay>0`**.

The two packages probe this same model on the two `work_mode` axes that the
project keeps deliberately separate.

## 1. What benchmark 09 establishes (sleep, cross-driver)

With `--chunks` / `--chunk-delay-ms` now first-class on **all three** drivers,
benchmark 09 shows — in **sleep** mode, lanes {1, 4}, `service_ms=20` — that:

* All three drivers echo the requested `chunks` / `chunk_delay_ms` and keep
  schema `1`; the analyzer is unchanged.
* At `delay=0`, **total active work is preserved** — observed service stays
  roughly flat across chunks 1/4/8 (no blow-up with chunk count). The mild rise
  that *is* present is **per-sleep overshoot accumulating** (Ray +~1.6 ms, HPX /
  rayx +~4–5 ms from chunks 1→8), a sleep-fidelity artifact, not added work.
* At `delay>0`, parked lane-occupancy ≈ `(chunks-1)×delay` is added, monotone in
  chunk count, inflated by the same blocking-sleep overshoot the gaps carry
  (Ray ~1.1×, HPX / rayx ~1.5× of nominal).
* The only cross-engine split is **sleep fidelity**: Ray ~5% overshoot vs
  HPX-native / rayx ~23–25% (experiment 01), seen on both active chunks and
  parked gaps. This is a backend property, **not** a control-plane effect.
* **rayx tracks HPX-native** (same C++ service lane) rather than collapsing
  toward Ray.

So benchmark 09 fixes the cross-driver baseline: chunking adds lifecycle/cadence
structure, all three drivers agree on shape, and the only divergence is the
documented sleep-timer gap.

## 2. What experiment 14 adds (spin, HPX-native + rayx)

Experiment 14 keeps the same chunked model but switches to **spin**
(active, CPU-bound service), drops Ray, and pins both HPX engines to 4 HPX
worker threads (`--hpx:threads=4`) so the high-lane throughput comparison is
apples-to-apples. It answers the active-CPU question sleep cannot:

* At `delay=0`, spin preserves total active service **exactly** — `8.000 ms` for
  chunks 1/2/4/8 across lanes 1/4/8 and both engines (max deviation `0.0006 ms`).
  Splitting the active work N ways adds **no** measurable active time.
* At `delay>0`, parked `chunk_delay_ms` **still** adds lifecycle ≈
  `(chunks-1)×delay` — but now the decomposition is clean: the **active** part
  stays exact (spin) while the **parked** gaps carry the ~25% sleep overshoot
  (each 2 ms gap lands ~2.5 ms).
* rayx matches HPX-native on service fidelity (same lane); the only difference is
  a small ~10% rayx **throughput edge at the L8 saturation point**, a
  client/scheduling artifact, not a lane difference — and the *opposite*
  direction from sleep-mode dispatch cost, so it is not a control-plane ranking.
* L8 shows a **milder** form of the experiment 08/09 core-boundary /
  oversubscription effect (per-lane efficiency ~0.98 at L1/L4 → ~0.81–0.95 at
  L8). It appears at `chunks=1` and barely moves with chunk count, so it is a
  **lane×core** effect, not a chunking or frontend cost.

## 3. Why sleep mode needs careful interpretation

Sleep "preservation" is only **approximate**. The blocking `sleep_for` overshoots
its target, and that overshoot **accumulates** with chunk count (N small sleeps
overshoot a bit more than one large one) and **also** rides on each parked gap.
So in sleep mode `service_ms_observed` mixes the quantity of interest (active
work, parked occupancy) with backend **timer fidelity**. Concretely:

* The chunk-count rise at `delay=0` is overshoot, not work.
* Ray's lower numbers are its **better sleep fidelity** (~5% vs ~25%), **not**
  lower control cost — at the no-op dispatch floor (benchmark 06) the HPX paths
  have far lower per-call overhead.

Sleep mode is the right axis for the cross-engine comparison that **includes
Ray** (Ray is sleep-only here), but its magnitudes must be read with the
sleep-fidelity gap in mind.

## 4. What spin mode isolates

Spin is wall-clock-exact, so it removes the timer-overshoot confound entirely and
isolates the **active-CPU** axis. This gives the clean control sleep cannot:

* `delay=0` preservation becomes **exact** (`8.000 ms`), proving chunking splits
  active work without adding any.
* `service_ms_observed` cleanly **decomposes**: exact active (spin) + parked gaps
  that behave like sleep. The `(chunks-1)×delay` lifecycle add at `delay>0` is
  then unambiguously the parked time, not measurement noise.
* It exposes the L8 lane×core pressure on a wall-clock-bounded workload, so the
  knee reads directly against experiments 08/09.

Spin drops Ray on purpose: spin is not in scope for the Ray baseline in this
context, and the two modes must not be conflated.

## 5. Does chunking change the control-plane story, or add lifecycle structure?

It **adds lifecycle structure.** Both packages are service-dominated (20 ms /
8 ms total active), so dispatch overhead is a small fraction and all engines
cluster. Chunking extends **lane-occupancy / cadence** (more steps, optional
parked gaps); it does not alter dispatch cost. The control-plane story — Ray's
actor-process boundary vs HPX intra-locality, and rayx staying near the
HPX-native floor — lives at the **no-op dispatch floor** (benchmark 06) and is
untouched here. The only chunk-count cost either package finds is mild **p99 tail
jitter** from extra scheduling points, with no median cost.

## 6. Implications for future chunked / serving-control benchmarks

* **Report active and parked separately.** `service_ms_observed` is active-only
  at `delay=0` and lifecycle (active + parked) at `delay>0`; do not present the
  `delay>0` number as service time.
* **Pick the mode for the question.** Use **spin** to test active preservation
  (exact, no timer confound); use **sleep** when the comparison must include Ray
  (sleep-only) — and then read magnitudes through the sleep-fidelity gap.
* **Keep them unconflated.** Sleep-mode and spin-mode results are different axes
  and should not be merged into one table.
* **Read L8 against experiments 08/09.** High-lane efficiency loss is the
  core-boundary / oversubscription signature of a fixed core budget, not chunking
  or a frontend cost. Pin `hpx_threads` for apples-to-apples cross-engine
  throughput.
* **Hold the invariants.** One row per request, schema `1`, analyzer unchanged,
  `chunks` / `chunk_delay_ms` echoed; `chunks_completed` stays facade-only.

## 7. What readers should not conclude

* **Not** real token streaming, model inference, Ray Serve, or object-store/task
  semantics — synthetic timing only; `service_ms` is duration control, never a
  payload or token.
* **Not** "Ray is faster." Ray's lower *overshoot* in benchmark 09 is sleep
  fidelity, not lower control cost (benchmark 06 shows the opposite at the
  dispatch floor). This is **not** a general "HPX beats Ray" claim.
* **Not** "rayx is faster than HPX-native." rayx **is** the same C++ service
  lane; the L8 spin throughput edge is a saturation-point client/scheduling
  artifact, not a lane or control-plane ranking.
* **Not** a chunking or frontend cost at L8. The high-lane efficiency drop is
  **lane×core** pressure (experiments 08/09), present already at `chunks=1`.
* **Machine-specific magnitudes.** Single laptop (4 P + 6 E); all ms / throughput
  numbers and the overshoot percentages are host-specific. The firm signals are
  the **structural** ones: active preservation at `delay=0` (exact under spin,
  overshoot-approximate under sleep), the `(chunks-1)×delay` parked add at
  `delay>0`, correct `chunks` / `chunk_delay_ms` echo, schema `1`, and rayx
  tracking HPX-native.
