# rayx.runtime Many-Actors Stress / Idle-Footprint Observation Note

The Stage-1 evidence note that `Runtime.release_actor` unblocked: exercise the
**create → use → release → shutdown → reinit** lifecycle for many local native
actors over the **public `rayx.runtime` API only**, prove the contract holds
structurally at every scale tested, and **observe** (never gate) what actors
cost on this machine.

Observation-only, machine/allocator-specific. **No** performance verdict,
**no** capacity or production-serving claim, **no** Ray comparison, **no**
"HPX beats Ray", **not** a benchmark verdict. No runtime source, `RuntimeLane`,
analyzer, JSONL, or CI change; no per-actor cap knob and no `async_rw_mutex`
work — those remain open design questions (§7), not decisions.

## 1. Why this follows `release_actor`

Until the release slice, every actor held its dedicated lane (`hpx::thread` +
queue + native state) for the runtime's lifetime, so the create-use-release
pattern the target-environment doc treats as the point ("an actor per
simulator/instrument") could not even be exercised. This note is the evidence
the doc's Stage-1 left open: does the pattern hold structurally at N actors,
and what does an idle actor actually cost?

## 2. What is structurally proven vs only observed

**Proven (firm gates G1–G7, every cell, both repeats):** exactly N actors
created with unique `rt-act-<16 hex>` ids; every `add(1)` returns 1 with all
futures retiring `completed`; every actor's `stats()` idle after retire;
`Runtime.lane_stats()` stays op-lanes-only with N actors live; on the release
path `release_actor` returns `None` for every actor and `call`/`stats` raise
afterwards on every handle; `shutdown()` is clean on **both** paths (after
release-all, and with N live actors); a fresh `Runtime` then creates and uses
an actor (guard released); and both recycling-cycle waves create/release
cleanly.

**Observed (never gated):** every elapsed-ms and RSS figure in §5–§6.

## 3. Footprint measurement, honestly scoped

Current RSS is best-effort `ps -o rss= -p <pid>` (KiB on macOS and Linux; null
fields if unavailable, gates unaffected); `ru_maxrss` is reported as the
**peak only** — it is a high-water mark that never decreases, so it cannot
show after-release drops (bytes on macOS vs KiB on Linux, normalized). Each
cell runs in its **own subprocess** so every baseline is pristine (no warm
HPX/malloc pools inherited from a previous cell). Values are rounded to
0.1 MB.

Process-level RSS is **allocator-mediated**: HPX pools thread stacks and
descriptors, and malloc may retain freed memory. Therefore **a non-drop after
release is not proof of a leak, and a drop would not be proof of guaranteed
reclamation**. macOS memory compression additionally makes RSS an undercount
with wobble. All memory numbers are observations of one machine.

## 4. Pre-registered expected shapes

Stated before reading the results, so observations are not misread as defects:

1. **After-release RSS ≈ after-create RSS** — expected, because HPX recycles
   terminated-thread stacks/descriptors into pools and malloc retains pages.
2. **Second-wave plateau** in the recycling cycle suggests reuse/pooling
   (healthy); **repeated growth across waves** would be the concerning signal.
3. **Per-actor committed footprint ≪ reserved stack size** — stacks are lazily
   committed; an idle worker that only reached its cv wait touched few pages.
4. **Release-all elapsed may exceed shutdown-only elapsed** — release pays one
   `run_as_hpx_thread` hop + join per actor; shutdown amortizes teardown
   through one runtime-level hop.
5. **`hpx_threads` should mostly affect the baseline**, not per-actor cost —
   actor cost lives in suspended HPX threads, not OS workers.

## 5. Results (this machine; medians over 2 repeats)

All structural gates passed (`all_structural_gates_passed: true`,
`gate_failures: []`, 32 matrix cells + the cycle cell). Curated evidence in
`aggregate.json`. Representative slice (`hpx_threads=1`, release path):

| N | calls | RSS after runtime start | after create | after release | per-actor create | per-actor release |
|---|---|---|---|---|---|---|
| 1 | 1 | 28.0 MB | 28.1 MB | 28.1 MB | 51 µs | 39 µs |
| 16 | 1 | 28.0 MB | 29.4 MB | 29.5 MB | 13 µs | 11 µs |
| 64 | 1 | 28.0 MB | 31.0 MB | 31.0 MB | 10 µs | 9 µs |
| 256 | 0 | 28.1 MB | 34.1 MB | 34.1 MB | 7 µs | 8 µs |
| 256 | 1 | 28.1 MB | 35.5 MB | 35.5 MB | 7 µs | 9 µs |

Observations (all matching the pre-registered shapes):

* **The HPX runtime itself is the fixed cost, actors are marginal.** Python
  baseline ~25.6 MB → ~28.0 MB after `Runtime()` (~2.4 MB one-time at
  `hpx_threads=1`; ~28.2 MB at 2 — the `hpx_threads` effect is baseline-only,
  per shape 5). The `rss_mb_after_runtime_start` checkpoint makes this
  separation direct rather than inferred.
* **Idle per-actor committed footprint reads as ~23 KiB (never called) /
  ~30 KiB (one retired call)** at N=256 — far below any reserved-stack
  arithmetic, consistent with lazily-committed stacks (shape 3); the ~6 KiB
  delta between the two is the stack the one service actually touched.
* **Creation and release are µs-scale per actor and roughly linear in N**
  (256 creates ≈ 1.8 ms total; 256 releases ≈ 2.2 ms total). The N=1 figures
  (~50 µs) are dominated by one-off warmup; the per-actor cost settles at
  ~7–11 µs by N=16.
* **After-release RSS equals after-create RSS exactly** (shape 1) — pooled,
  not leaked, which the cycle cell then distinguishes (§6).
* **Release-all (≈2.2 ms at N=256) vs shutdown-only (sub-ms to ~2 ms)** —
  consistent with shape 4's direction, though shutdown timing is noisy at
  this scale; neither figure is a verdict.

## 6. Recycling-cycle observation (N=64, calls=1, `hpx_threads=1`)

| checkpoint | RSS |
|---|---|
| before cycle (post runtime start) | 28.0 MB |
| after wave-1 create | 31.1 MB |
| after wave-1 release | 31.1 MB |
| after wave-2 create | **31.1 MB** |
| after wave-2 release | 31.1 MB |

The second wave of 64 actors added **0.0 MB**: wave-2 ran entirely out of the
pools wave-1's release returned its stacks/descriptors to. This is the plateau
of pre-registered shape 2 — **reuse/pooling, not growth** — and it converts
the §4 "non-drop is not a leak" caveat from an unfalsifiable disclaimer into a
falsified-leak observation at this N. (Still an observation on one machine,
not a guarantee.)

## 7. What remains open (questions, not decisions)

* **Per-actor admission cap knob** — still the open Stage-1 ergonomics item;
  nothing here decides its shape.
* **`async_rw_mutex` actor alternative** — the design doc's weighed
  alternative would trade the owned-queue serving contracts (exp20's lesson)
  for near-zero idle footprint. This note prices what that trade would buy on
  this machine: roughly **23–30 KiB and ~10 µs of lifecycle time per actor**
  at N≤256 — i.e., at the target-environment scales measured here, the
  lane-per-actor design's idle cost is small, and no architecture change is
  motivated by this evidence. A future case for it would need a different
  workload regime, not these numbers.

## 8. Caveats

* Machine/allocator-specific (macOS laptop, 10 cores, single locality);
  best-effort process-level RSS with macOS-compression undercount; 0.1 MB
  rounding; timing medians over 2 repeats with run-to-run noise (shutdown
  especially).
* N ≤ 256 by default (512 is opt-in via `--actor-counts`); nothing here
  claims behavior at thousands of actors, under concurrent load, or in
  production serving.
* No Ray comparison of any kind; synthetic `counter` actors, not real
  workloads.

## 9. Reproduction

```bash
# quick smoke (counts 1,16; ht 1; cycle N=16; no aggregate.json written)
python experiments/26_runtime_many_actors_footprint/run_runtime_many_actors_footprint.py --quick

# full run (writes the curated aggregate.json beside this report)
python experiments/26_runtime_many_actors_footprint/run_runtime_many_actors_footprint.py

# optional overrides (512 actors is opt-in only):
#   --actor-counts "1,16,64,256,512"  --hpx-threads-list "1,2"  --repeats N
```

Requires the `_rayx` extension built (`cmake --build python/build`). The actor
contract tests (`pytest tests/integration/test_actor_contract.py`) and the
runtime smoke (`python bench/smoke_rayx_runtime.py`) cover the underlying
release semantics this note exercises at scale.
