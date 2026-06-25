# exp40 — independent-chain overlap: RayX lane-level (Arm A) vs HPX-native intra-op (Arm B)

**Type:** concurrency / overlap slice. Single actor, single node, synthetic CPU work,
closed `int64`. Follows exp39 (which measured *boundary avoidance* in a linear
*dependent* chain with no parallelism). exp40 asks the complementary question: **given
`K` independent native chains, can HPX overlap them inside one RayX runtime?**

**This is not an "HPX wins" result.** Arm A is the **RayX lane-level mapping /
baseline / control** (Ray-shaped). Arm B is the **load-bearing HPX-native arm**. The
primary signal is a **fixed-core overlap ratio**, not a thread-count speedup curve. The
accepted Rostam diagnostic probe (see `run_overlap_probe.py`) already showed both
mechanisms present with clean controls; this is the narrow full matrix that
characterizes the gap to ideal. **Do not claim ideal scaling** — the probe already
saturated below ideal in both arms.

## What it measures

One chain kernel, shared by both arms and by exp39: `stage(x,q) = (x +
masked_range_sum(0,q)) & BUSY_SUM_MASK`, applied `S` times from a seed (the same
`chain_stage` that backs `chain_sum_loop`). `K` chains are made **independent** by a
distinct per-chain seed `seed_j = SEED + j`.

| Arm | What runs | What it isolates |
|---|---|---|
| **A** `chain_sum_loop ×K` | `K` independent `chain_sum_loop(seed_j, S, q)` submissions, gathered in Python (`as_completed` and bulk `wait(num_returns=K)`) | **RayX lane-level overlap** + **`K` outer `hpx::async` hops** + **`K` Python/pybind/`RuntimeFuture` retirements** (baseline / control, Ray-shaped mapping) |
| **B** `chain_fanout` | one `chain_fanout(SEED, K, S, q)` op: `K` children via bare `hpx::async` on the default HPX pool, joined with `hpx::when_all`, folded under the mask | **HPX-native intra-op overlap** — **one** outer op retirement + **`K` internal HPX children**, no inter-child Python retirement (load-bearing HPX arm) |

`chain_fanout` mirrors the existing `fanout_sum` launch-all + `when_all` pattern and
stays inside the fixed registered native operation model. Equal-work invariant (the
credibility gate): `chain_fanout(SEED,K,S,q) == (Σ_j chain_sum_loop(SEED+j,S,q)) &
BUSY_SUM_MASK`, so Arm A folded == Arm B folded == a Python reference fold.

## Primary matrix (fixed cores)

`hpx_threads = 8` fixed. `S = 64`, `reps = 200`, `warmup = 30`.

- `K ∈ {1, 2, 4, 8, 16}`, `q ∈ {256, 4096, 16384, 65536}`.
- **Arm A:** `num_lanes = min(K, hpx_threads)` (so lanes are not the cap until `K`
  exceeds workers — i.e. `num_lanes = K` for `K ≤ 8`, `num_lanes = 8` for `K = 16`).
- **Arm B:** `num_lanes = 1`.

Each cell measures **both** `T1` (count==1) and `TK` (count==K) in the **same** runtime,
so the overlap ratio is within-runtime.

Metrics per cell: `T1` p50; `TK` p50/p90/p99; `overlap_ratio = K·T1/TK`;
`normalized_wall = TK/T1`; `throughput = K/TK`; `efficiency = overlap_ratio /
min(K, hpx_threads)`; `TK_sum_service_p50` (`service_ms_observed` decomposition — Arm A:
sum of `K` rows; Arm B: the single op).

## Controls (kept in the report — show the overlap is not an artifact)

- **Arm A serial-lane control:** `K=8, hpx_threads=8, num_lanes=1, S=64, q=16384`
  (`--armA-num-lanes 1`). Must stay ≈ serial (overlap ≈ 1).
- **Arm B serial-worker control:** `K=8, hpx_threads=1, num_lanes=1, S=64, q=16384`
  (the `hpx_threads=1` point of the secondary sweep). Must stay ≈ serial (overlap ≈ 1).

## Secondary thread-count characterization (NOT the headline)

`K=8`, `S=64`, `q=16384`, `hpx_threads ∈ {1, 2, 4, 8}`. **Arm A:**
`num_lanes = min(K, hpx_threads)` (stated choice). **Arm B:** `num_lanes = 1`.

**Caveat (load-bearing):** binding is HPX default and placement is unknown, so this
thread-count sweep is confounded by worker-to-core placement (and frequency scaling).
It is reported as a secondary, clearly-caveated characterization — the fixed-core
primary metric is the headline precisely because it holds `hpx_threads` constant.

## Binding / affinity

RayX passes **only** `--hpx:threads=N` (`_rayx.cpp` `start_process_hpx`); HPX binding is
the **default** and the policy is **unreported**, so **placement is unknown**. **No
NUMA/socket attribution** is made anywhere in exp40. An experiment-only
`RAYX_RUNTIME_HPX_BIND` path (to pin the primary line intra-socket on Rostam) is **not**
added in this slice — it is a separate proposal; until then the result stands as
"default binding, placement unknown."

## Structural gates (pass/fail; driver aborts the run on any failure)

- Deterministic `T1`/`TK` values across all reps.
- Arm A: exactly `K` futures, `K` distinct seeds (`SEED+j`), no missing/duplicate.
- Equal-work: each arm's folded value (`T1` and `TK`) equals the independent Python
  reference fold of `K` `chain_sum_loop` chains → Arm A folded == Arm B folded.
- `get`/`wait`/`as_completed` contracts unchanged (exercised by the Arm A gathers).
- Op table includes `chain_fanout` (checked before any runtime is built).

Structural gates are the only pass/fail; **all timing is observational**. Contract-level
coverage lives in `tests/integration/test_runtime_chain_fanout.py` and
`tests/unit/test_chain_fanout_validate.py`.

## Interpretation rules (fixed a priori, before the numbers)

- If **Arm B** shows overlap across `q`/`K`: HPX can overlap independent native work
  inside one RayX runtime *under this synthetic CPU workload* → **strengthens Track A**.
- If overlap **weakens at fine grain** (small `q`): read as **grain-size / scheduler
  overhead**, not failure.
- If overlap **saturates below ideal**: **characterize the ceiling**; do **not** claim
  linear scaling.
- If results are **noisy / non-monotone**: mark **inconclusive** and recommend a
  bind-controlled follow-up.

## Explicit non-claims

- no Ray-mediated baseline (Arm A is RayX-internal lane routing, **not** Ray),
- no multi-node evidence,
- no Ray transport evidence,
- no HPX fabric / parcelport / AGAS evidence,
- no Python-callback evidence,
- no tokenizer / vLLM / SGLang / Ray Serve / real inference evidence,
- no no-GIL evidence,
- no general "HPX beats Ray" claim.

## How to run

Laptop smoke (structural gates only; the laptop build may closed-form the masked kernel,
so do **not** read laptop timing as evidence — it is non-authoritative):

```bash
PYTHONPATH=python/src python \
  experiments/40_native_independent_overlap/run_independent_overlap.py --smoke
```

Full sweep (Rostam, authoritative) — primary + Arm A serial-lane control + secondary
thread sweep, all appended to one JSONL:

```bash
PROBE=experiments/40_native_independent_overlap/run_independent_overlap.py
OUT=results/exp40.jsonl
# Primary: fixed hpx_threads=8, K x q grid, both arms.
PYTHONPATH=python/src .venv/bin/python $PROBE --hpx-threads 8 \
  --ks 1,2,4,8,16 --quanta 256,4096,16384,65536 --arms A,B \
  --armA-num-lanes minKT --reps 200 --warmup 30 --label primary --out $OUT
# Arm A serial-lane control (num_lanes=1).
PYTHONPATH=python/src .venv/bin/python $PROBE --hpx-threads 8 \
  --ks 8 --quanta 16384 --arms A --armA-num-lanes 1 --reps 200 --warmup 30 \
  --label controlA_serial_lane --out $OUT
# Secondary thread-count sweep (caveated): one process per hpx_threads.
for T in 1 2 4 8; do \
  PYTHONPATH=python/src .venv/bin/python $PROBE --hpx-threads $T \
    --ks 8 --quanta 16384 --arms A,B --armA-num-lanes minKT \
    --reps 200 --warmup 30 --label secondary --out $OUT ; \
done
```

The `hpx_threads=1` secondary Arm B cell is the Arm B serial-worker control; one HPX
lifecycle per `hpx_threads` value (separate process) keeps each `hpx::start` clean.

## Results (Rostam, authoritative)

Rostam (Intel Xeon Gold 6148, 40 physical cores, exclusive Slurm, governor
`performance`), `S=64`, 200 reps, 30 warmup, seed 12345. Curated p50 grid and derived
metrics live in [`aggregate.json`](aggregate.json); raw rows are `results/exp40.jsonl`
(ignored). All figures are client-side per-batch wall p50 — **observation-only and
machine-specific**, not a winner. Binding is HPX default and **placement is unknown**,
so **no NUMA/socket claim** is made anywhere below.

### Structural gates — PASS (74/74 cells)

All cells completed; values deterministic across reps; Arm A used `K` distinct seeds
and gathered exactly `K` futures; each arm's folded `TK` value equals the independent
Python reference fold of `K` `chain_sum_loop` chains; **Arm A == Arm B equal-work** for
every `(K,S,q)` (0 `TK_value != ref_value`).

### Arm B is the load-bearing HPX-native result; Arm A is the RayX lane-level baseline

**Arm B (one `chain_fanout` op, `num_lanes=1`) demonstrates HPX-native intra-op overlap
of independent native work inside one RayX runtime.** The `K` children overlap on the
worker pool from a *single* submitted op — and the op's own `service_ms` itself drops
(q=16384, K=8: 1.58 ms wall of service for 6.3 ms of child work), so the overlap happens
**natively inside the op**, not in the Python layer.

Primary overlap ratio (`K·T1/TK`) / efficiency (`overlap/min(K,8)`) at `hpx_threads=8`:

| K | Arm B q=16384 | Arm B q=65536 | Arm A/ac q=16384 | Arm A/bulk q=16384 |
|---|---|---|---|---|
| 1 | 1.00 / 1.00 | 1.00 / 1.00 | 1.00 / 1.00 | 1.00 / 1.00 |
| 2 | 2.00 / 1.00 | 2.00 / 1.00 | 1.96 / 0.98 | 1.89 / 0.95 |
| 4 | 3.97 / 0.99 | 3.99 / 1.00 | 2.05 / 0.51 | 2.03 / 0.51 |
| 8 | **4.15 / 0.52** | 5.57 / 0.70 | 2.64 / 0.33 | 2.66 / 0.33 |
| 16 | 8.21 / 1.03 | 8.06 / 1.01 | 3.21 / 0.40 | 3.15 / 0.39 |

- **Arm B is stronger than Arm A across the entire primary matrix** (every `(K,q)`
  cell, by a wide margin), with tighter tails (Arm B K=16/q=16384: p50 1.653, p90 1.665,
  p99 1.683 ms; Arm A widens at high K). Arm A pays `K` outer `hpx::async` hops + `K`
  Python/pybind/`RuntimeFuture` retirements — by construction it is the Ray-shaped
  baseline/control, not the HPX result.
- **Overlap is near-ideal up to about 4 effective workers:** Arm B is within a few
  percent of ideal for `K ≤ 4` at every work-dominated grain, and the secondary sweep
  scales cleanly `T=1→2→4` (efficiency ≈ 1.0).
- **Overlap saturates below ideal at 8 workers.** At `K=8` (== worker count) Arm B
  efficiency falls to ~0.5–0.74 (deepest at q=16384). The secondary sweep
  (`K=8, q=16384`) pins it: Arm B overlap is **4.20 at T=4 (eff 1.05)** but only **4.28
  at T=8 (eff 0.54)** — wall is unchanged (1.639 → 1.634 ms), so **workers 5–8 buy
  nothing**; effective parallelism ceilings near ~4 for this op/workload. `K=16` recovers
  to overlap ≈ 8 (the worker count) because more children pack the pool into ~2 waves.

### Characterized limitations (not failures)

The **K=8 efficiency dip** and the **T=4→T=8 stall** are *characterized limitations of
the current op/workload/runtime configuration, not failures*: overlap is real and clean
in the K≤4 / T≤4 regime, and the controls hold. We do **not** claim linear scaling to 8
workers. Because binding is HPX default and **placement is unknown**, the ceiling could
be placement- or scheduler/op-structure-bound — this slice does **not** attribute it,
and makes **no NUMA/socket claim**.

### Controls (overlap is not an artifact)

- **Arm A serial-lane control** (`K=8, hpx_threads=8, num_lanes=1`): overlap ≈ 1.06,
  normalized wall ≈ 7.5 → serial, as required.
- **Arm B serial-worker control** (`K=8, hpx_threads=1`): overlap ≈ 1.07, normalized
  wall ≈ 7.47 → serial, as required.

### Gather variant (Arm A)

`as_completed` vs bulk `wait(num_returns=K)` is **negligible at work-dominated grains**
(~0.1 ms, mixed sign); only at the finest grain (q=256, K=8) does `as_completed` show a
higher ratio (3.21 vs 1.85). Neither gather is the dominant cap — the overlap ceiling is
structural, not a gather artifact.

## Roadmap impact

**Category: Roadmap strengthened (Track A).**

exp40 strengthens **Track A** (in-process HPX inside Ray actors): Arm B demonstrates
real HPX-native intra-op overlap of independent native work inside one RayX runtime
under this synthetic CPU workload, with clean controls and Arm B stronger than the
RayX lane-level baseline (Arm A) throughout. This adds the overlap leg to exp39's
boundary-avoidance leg. The sub-ideal ceiling (effective ~4×; K=8 dip; T=4→T=8 stall)
is a characterized limitation, not a refutation.

The **distributed HPX fabric direction remains gated**: exp40 is single-process,
single-node and contains **no** HPX parcelport, AGAS, locality-to-locality
communication, or remote action — it gives **no distributed evidence** and does not
unblock the distributed endpoint/fabric direction.

Explicit non-claims:

* no Ray-mediated baseline (Arm A is RayX-internal lane routing, not Ray),
* no multi-node evidence,
* no Ray transport evidence,
* no HPX fabric / parcelport / AGAS evidence,
* no Python-callback evidence,
* no tokenizer / vLLM / SGLang / Ray Serve / real inference evidence,
* no no-GIL evidence,
* no general "HPX beats Ray" claim.

## Recommendation

Track A performance work is **paused** after the exp39/exp40 closeout; exp40 is not the
start of an optimization push. The overlap result stands as recorded: "near-ideal to ~4
effective workers, saturating below ideal at 8; default binding, placement unknown."

A bind-controlled follow-up is **deferred / optional (only if asked)**, not the next
slice: it would pin the workers intra-socket on Rostam (e.g. an experiment-only
`--hpx:bind` path) at `K ∈ {8,16}`, `q ∈ {16384,65536}`, `hpx_threads ∈ {4,8}` and
compare against the default-binding numbers here, to attribute the ~4× ceiling /
`T=4→T=8` stall to placement vs scheduler/op-structure. Optimizing that ceiling is **not
the current priority**, and no stronger scaling claim is made until such a follow-up is
run.
