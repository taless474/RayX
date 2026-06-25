# exp39 — native loop vs `hpx::future::then` chain vs mediated boundary

**Type:** latency / **decomposition** slice. Single actor, single node, synthetic
CPU work, closed `int64` values. No Python callbacks, no Ray-mediated baseline, no
HPX fabric / parcelport / AGAS, no tokenizer / vLLM / SGLang / Ray Serve.

**This is not an "HPX scheduling wins" result.** A linear *dependent* chain has no
parallelism, so it is the least-flattering shape for HPX. exp39 deliberately measures
*where the cost goes*, not who wins. The concurrency / overlap story — where HPX can
start earning the word "runtime" rather than "boundary avoidance" — is the separate
**exp40**.

## What it measures

A dependent chain of `S` stages from a seed, where one stage is
`stage(x, q) = (x + masked_range_sum(0, q)) & BUSY_SUM_MASK` — `q` units of the same
masked on-core work as `busy_sum`/`fanout_sum`, folded into the running value. The
single `chain_stage` kernel (in the HPX-free `runtime_ops.hpp`) is shared by all
three variants, so they cannot drift and the three-way equal-work invariant holds by
construction.

| Variant | What runs | What it isolates |
|---|---|---|
| `chain_sum_loop` | one submitted op; plain C++ `for` loop over `S` stages on the lane worker | **in-process native floor** (no per-stage continuation) |
| `chain_sum_then` | one submitted op; `S` stages as an `hpx::future::then` chain, each stage pinned to `hpx::launch::async` | **scheduled `hpx::future::then` shared-state + continuation cost** |
| `mediated_samelane` | Python left-fold of `S` calls to `chain_sum_loop(_, 1, q)` into the same runtime | **repeated Python / pybind / RuntimeFuture / lane-admission boundary cost** |

`mediated_samelane` reuses `chain_sum_loop` with `steps=1` as the one-stage unit — so
the per-stage native work is byte-identical to the floor, and the only difference is
that each stage crosses the Python boundary and is submitted/retired separately.

## Decomposition (the point of the slice)

Reported at **p50 client-side wall time**, not as a winner:

- `mediated − loop` ≈ **repeated boundary overhead** — `(S−1)` extra submit/retire
  cycles (pybind marshal + GIL cycle + `RuntimeFuture` alloc + lane admission +
  dispatch hop).
- `then − loop` ≈ **scheduled `hpx::future::then` continuation / shared-state
  overhead** — the price HPX pays to express the same dependent chain natively.
- `mediated − then` ≈ **net benefit of staying native while still paying HPX
  continuation cost**.

**Revised claim (what exp39 can support, if the evidence is stable):** *for dependent
native work, in-process native composition avoids repeated
Python/pybind/RuntimeFuture boundary cost; the `future::then` variant measures the HPX
continuation overhead paid to express the chain natively.* Nothing more.

## Methodology

- **Headline metric is client-side per-chain wall time** (`time.perf_counter_ns`)
  around the full chain, for all three variants. The per-step boundary cost lives
  **outside** the native JSONL row by construction, so the row alone would measure the
  wrong thing. Runtime rows (`service_ms_observed`) are kept for decomposition only.
- **`chain_sum_then` launch policy is pinned to `hpx::launch::async`** and is a
  **deliberately visible / pessimistic** measurement of *scheduled* `future::then`
  cost — **not** the theoretical lower bound of HPX composition. `hpx::launch::sync`
  (inlines the continuation, collapsing `then → loop`), sender/receiver composition,
  and the plain loop are different points in the design space.
- **Warmup discarded** (HPX pool / first-touch one-time costs).
- **Python GC disabled inside the measured timing loop** (re-enabled immediately
  after) so a collection cannot land in a wall sample.
- **p50 / p90 / p99** reported per `(variant, S, q)` cell.
- **`--hpx:threads` and lane count reported**; thread binding/affinity is the HPX
  default (RayX sets only `--hpx:threads`, never `--hpx:bind`) and is reported as
  such.
- **Anti-elision is runtime-based** (no mandatory disassembly): cost scales
  monotonically with `q`; returned values depend on `seed`, `S`, `q`; the three-way
  equal-work invariant holds. The driver aborts the run on any equal-work or
  determinism mismatch.
- **Large-`q` convergence is a soft read, not pass/fail:** as `q` grows, relative
  differences should shrink because stage work dominates the fixed
  boundary/continuation overheads. If the absolute or relative gap grows, or the
  ordering becomes unstable, the result is measuring additional effects (scheduler
  contention, allocator pressure, affinity, cache behavior) and the writeup must say
  so — the experiment does **not** fail just because fixed overhead stays visible.

## How to run

Laptop smoke (driver + structural gates only):

```bash
PYTHONPATH=python/src python \
  experiments/39_native_continuation_vs_mediated_chain/run_native_continuation_vs_mediated_chain.py --smoke
```

Full sweep (Rostam, authoritative):

```bash
PYTHONPATH=python/src .venv/bin/python \
  experiments/39_native_continuation_vs_mediated_chain/run_native_continuation_vs_mediated_chain.py \
  --steps 1,2,4,8,16,32,64 --quanta 0,16,64,256,1024,4096,16384,65536,100000 \
  --reps 200 --warmup 30 --hpx-threads 1 --num-lanes 1 --out results/exp39.jsonl
```

The large-`q` end of the sweep must push `q` high enough that per-stage work
dominates the ~µs-scale per-step boundary cost; otherwise convergence will not be
visible (see the local-smoke caveat below). `q` is capped at `100000`
(`CHAIN_QUANTUM_MAX`, a boundary guardrail in `runtime/_validate.py` mirrored in
`runtime_ops.hpp`): a larger `q` such as `262144` is correctly rejected at the Python
boundary, so the top of the grid is `100000`, not an arbitrary power of two. The cap
is working as designed — no code change is needed to run the authoritative sweep.

## Rostam results (authoritative)

Rostam (Intel Xeon Gold 6148, 40 physical cores, exclusive Slurm, governor
`performance`), `--hpx-threads 1 --num-lanes 1`, 200 reps, 30 warmup, seed 12345,
`q` grid `0…100000`. Structural gates PASS at every cell (three-way equal-work held;
values deterministic across all reps). Curated p50 grid and derived metrics live in
[`aggregate.json`](aggregate.json); raw rows are `results/exp39.jsonl` (ignored). All
figures are client-side per-chain wall p50 in ms — **observation-only and
machine-specific**, not a winner.

### Fixed per-stage overhead (q=0, no stage work)

`q=0` isolates the fixed overheads. `mediated − loop` is essentially linear in
`(S−1)` — one extra Python/pybind/`RuntimeFuture`/lane boundary crossing per stage at
**≈ 20.2 µs each**:

| S | `mediated − loop` (ms) | per crossing (µs) | `then − loop` (ms) | per `then` stage (µs) |
|---:|---:|---:|---:|---:|
| 2  | 0.0204 | 20.4 | 0.0001 | 0.1 |
| 4  | 0.0609 | 20.3 | 0.0073 | 2.4 |
| 8  | 0.1409 | 20.1 | 0.0121 | 1.7 |
| 16 | 0.3042 | 20.3 | 0.0205 | 1.4 |
| 32 | 0.6308 | 20.3 | 0.0328 | 1.1 |
| 64 | 1.2733 | 20.2 | 0.0641 | 1.0 |

The decomposition is the point:

- **`mediated − loop` = repeated boundary overhead.** It scales strongly with `S`
  (0.02 → 1.27 ms from `S=2` to `S=64` at `q=0`) because each extra stage adds a full
  submit/retire cycle across the Python boundary.
- **`then − loop` = scheduled `hpx::future::then` continuation / shared-state cost.**
  It is *visible* but *much smaller* than the mediated boundary cost — at `S=64, q=0`,
  `then − loop ≈ 0.0641 ms` versus `mediated − loop ≈ 1.2733 ms` (≈ 20× smaller per
  stage). This is the price HPX pays to express the dependent chain *natively*, under
  the deliberately pessimistic `hpx::launch::async` policy (not the HPX floor).
- **`mediated − then` = net benefit of staying native while still paying continuation
  cost.** At `S=64, q=0` it is ≈ 1.209 ms.

**Supported claim:** for dependent synthetic native work inside one RayX runtime,
in-process native composition avoids the repeated
Python/pybind/`RuntimeFuture`/lane-boundary cost; the `future::then` variant measures
the HPX continuation/shared-state overhead paid to express the chain natively. Nothing
broader — no Ray-mediated baseline, no overlap/parallelism story (that is exp40).

### Large-`q` read (soft, not pass/fail)

As `q` grows, stage work should dominate the fixed overheads and the relative spread
of the three p50s should shrink. It does — at `S=64` the relative spread falls from
≈ 63 at `q=0` to ≈ 0.50 at `q=100000` — **but it does not fully collapse**, and the
`mediated − loop` gap actually *grows* at the top of the grid (≈ 1.27 ms fixed value
at `q=0` → ≈ 2.11 ms at `q=100000, S=64`).

This is **not** a failure. Two honest readings, kept observational and
machine-specific:

1. Fixed mediated boundary overhead remains visible for long chains even when per-stage
   work is large.
2. At large `q` the gap is no longer pure fixed overhead — there are likely additional
   scheduler / allocator / cache / affinity effects that this slice does not attribute
   (no `--hpx:bind`, no affinity evidence). The writeup does not claim to isolate them.

## Roadmap impact

**Category:** Roadmap strengthened.

exp39 strengthens Track A: the in-process HPX-inside-RayX actor direction. It
quantifies the cost of repeatedly crossing the Python/pybind/RuntimeFuture/lane
boundary for dependent synthetic native work, and shows that scheduled
`hpx::future::then` continuation/shared-state overhead is visible but much smaller in
this experiment.

This does not change the later Python-callback direction, because exp39 did not test
Python callbacks. It also does not unblock Track B, the future HPX fabric direction,
because exp39 is single-process, single-node, and contains no HPX parcelport, AGAS,
locality-to-locality communication, or remote action.

The natural next experiment remains exp40: concurrency/overlap with multiple
independent native chains. exp39 measured boundary avoidance in a linear dependent
chain with no parallelism; exp40 should test whether HPX can also provide a
runtime/overlap benefit when independent work is available.

Explicit non-claims:

* no Ray-mediated baseline,
* no multi-node evidence,
* no HPX fabric / parcelport / AGAS evidence,
* no Python-callback evidence,
* no tokenizer / detokenization / vLLM / SGLang evidence,
* no no-GIL evidence,
* no general “HPX beats Ray” claim.

## Validation status

1. **Implemented:** `chain_sum_loop` (`runtime_ops.hpp`), `chain_sum_then`
   (`runtime_ops_hpx.hpp`), boundary validation (`runtime/_validate.py`), unit +
   integration contract tests, and this driver/writeup.
2. **Validated locally (laptop, darwin):** import-light unit tests, the full
   integration suite (incl. the three-way equal-work gate, determinism, queued-only
   cancellation, `get`/`wait`/`as_completed`), the runtime smoke, and the driver in
   `--smoke` mode (structural gates PASS).
3. **Validated on Rostam (authoritative):** the full sweep above. `pytest
   tests/integration/` (116 passed) and `bench/smoke_rayx_runtime.py` (all pass) on the
   same build; structural gates PASS; curated evidence in `aggregate.json`. Laptop
   numbers remain observation-only and were not used for the curated tables.
