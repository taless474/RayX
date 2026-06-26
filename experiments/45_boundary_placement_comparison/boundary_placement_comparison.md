# exp45 — fixed-granularity boundary-placement comparison

**Status:** closed as a boundary-placement / orchestration-**count** result. Deterministic
and machine-independent. **Not** a performance result.

## What this shows

The **same** closed fan-in/reduction workload — `L` independent leaves, each a single
`chain_stage`, folded under `BUSY_SUM_MASK` — computed two ways that return the
**bit-identical** closed `int64`, differing only in *where the boundary is placed*:

| | Path A — native / coarse | Path B — Python-mediated |
|---|---|---|
| how | `barrier_fanin(seed, L, quantum)` | `L × chain_sum_loop(seed+j, 1, quantum)` + Python masked fold |
| Runtime submissions | **1** | **L** |
| Runtime retirements | **1** | **L** |
| **Python/Runtime boundary crossings** | **2** (O(1)) | **2L** (O(L)) |
| reduction | native (`when_all` → scheduled `.then`) | Python masked fold (**L−1** ops, **0** Runtime crossings) |

`chain_sum_loop(seed+j, 1, quantum)` applies `chain_stage` exactly once → it **is** a
`barrier_fanin` leaf. So Path B is the *same logical computation* relocated to the Python
side of the boundary, one leaf per crossing pair. **No new op, no arbitrary Python
execution, no object store.**

## Value equivalence (the substance)

For every `L`, triangulated against a pure-Python oracle that mirrors native masking
exactly (per-step mask in `masked_range_sum`, uint64 wrap in `chain_stage`):

```
result_a (barrier_fanin)  ==  result_b (chain_sum_loop leaves + Python fold)  ==  oracle
```

This is the load-bearing content: the coarse op is a **faithful boundary relocation** of
the fine-grained pattern, not a different or cheaper computation.

## Result (this machine, count-only — the load-bearing run)

`seed=3`, `quantum=64`, `hpx_threads=2`, `overall_structural_pass = true`:

| L | result (= oracle) | eq | A sub/ret/xing | B sub/ret/xing | B fold-ops |
|---:|---:|:--:|:--:|:--:|---:|
| 1 | 2019 | OK | 1 / 1 / **2** | 1 / 1 / **2** | 0 |
| 2 | 4039 | OK | 1 / 1 / **2** | 2 / 2 / **4** | 1 |
| 4 | 8082 | OK | 1 / 1 / **2** | 4 / 4 / **8** | 3 |
| 8 | 16180 | OK | 1 / 1 / **2** | 8 / 8 / **16** | 7 |
| 16 | 32424 | OK | 1 / 1 / **2** | 16 / 16 / **32** | 15 |
| 32 | 65104 | OK | 1 / 1 / **2** | 32 / 32 / **64** | 31 |

Counts are **observed** (incremented at each submit/retire) and then gated against the
exact formulas — Path A `1/1/2` independent of `L`; Path B `L/L/2L` with `L−1` Python fold
ops. The Python fold ops cross **no** Runtime boundary and are reported only for
transparency (nothing is hidden: Path B does real Python reduction work, it just costs
zero Runtime-boundary transits — which is the honest property under study).

### What is *not* counted

We count **Python/Runtime boundary crossings**, not internal HPX tasks. Path A still
performs `L` internal `hpx::async` leaves below the boundary — they cost **zero** Python
crossings. So the comparison is not "Path A does less work"; it does the *same* leaf work,
natively, behind one crossing pair.

## Timing (opt-in `--timing`, default OFF, observation-only, tautological)

Timing is **not** the result and is off by default. When enabled it is **tautological**:
fewer pybind transits cost less pybind time. It is **not** a speedup / throughput /
latency / HPX / Ray / scheduling claim.

A concrete reason it would *mislead* if led with: on this machine the `--timing` mode shows
Path A's wall-time **flat at ~2.27 ms regardless of L** and *slower* than Path B
(~13–100 µs). That is **not** a boundary-placement signal — it is an artifact of
`barrier_fanin`'s internal cooperative-watchdog poll granularity (~2 ms), wholly unrelated
to boundary crossings. Reading wall-time here would invert the boundary-count story.
This is exactly why the deterministic **count** is the load-bearing metric and timing is
fenced as observation-only.

## Allowed claim

> exp45 compares two boundary placements for the same closed fan-in/reduction workload: one
> coarse Runtime operation (`barrier_fanin`) versus Python-mediated orchestration of fixed
> Runtime operations (`chain_sum_loop` per leaf plus a Python masked fold). It shows the two
> are value-equivalent while the coarse native path uses one Runtime submission/retirement,
> O(1) Python/Runtime crossings, and the Python-mediated path uses L submissions/retirements,
> O(L) crossings. This is boundary-placement/orchestration-count evidence only.

## Required non-claims

No speedup · no throughput · no latency/performance claim (timing is opt-in,
observation-only, tautological) · no HPX faster than Ray · no Ray comparison · no real
inference · no endpoint/fabric claim · no parcelport · no AGAS · no multi-node · no
`ObjectRef`/object-store semantics · no arbitrary Python execution · no parallelism/
scheduling claim (the `barrier_fanin` gate is incidental here; its witness is checked only
for structural validity) · **no claim that Python orchestration is "bad"** — it is a
legitimate, flexible pattern; exp45 only *counts* its Runtime-boundary transits for this
workload.

## Files

* `run_boundary_placement_comparison.py` — the comparison (Path A / Path B, pure oracle,
  counters, optional timing).
* `boundary_placement_comparison.md` — this write-up.
* `aggregate.json` — generated; deterministic counts + equivalence gates.

## Run

```
python -m py_compile experiments/45_boundary_placement_comparison/run_boundary_placement_comparison.py
PYTHONPATH=python/src python experiments/45_boundary_placement_comparison/run_boundary_placement_comparison.py --smoke
PYTHONPATH=python/src python experiments/45_boundary_placement_comparison/run_boundary_placement_comparison.py
# optional, observation-only (tautological), not a performance run:
PYTHONPATH=python/src python experiments/45_boundary_placement_comparison/run_boundary_placement_comparison.py --timing
```

## Interpretation and roadmap impact

**Experiment interpretation.** Structurally everything passed: the coarse `barrier_fanin`
path and the Python-mediated `chain_sum_loop`-per-leaf path return the bit-identical closed
`int64` (triangulated against a pure oracle) at every `L`, and the boundary-crossing counts
match the exact formulas (Path A O(1) = 2; Path B O(L) = 2L, with `L−1` zero-crossing Python
fold ops). The result is a **quantified, equivalence-proven boundary relocation**, not a
discovery and not a performance result — and the `--timing` aside (Path A slower in
wall-time, dominated by an unrelated watchdog-poll artifact) confirms why the deterministic
count, not timing, is the honest metric.

**Roadmap impact: `Roadmap strengthened`.** This turns the boundary-reduction thesis
(exp39 → exp40 → exp44) into a concrete counted artifact and an equivalence proof: the same
fan-in/reduction can live at either boundary granularity, and moving it behind one coarse
Runtime op collapses O(L) Python/Runtime crossings to O(1) while preserving the closed
result. It is also the explicit precondition for a *fair* later comparison.

**Updated roadmap.**

* *In-process HPX inside Ray actors:* strengthened — the in-process composition arc now has
  a deterministic boundary-placement quantification on top of the exp44 keystone.
* *Distributed-fabric direction:* unchanged and still gated/fenced. exp45 is in-process and
  count-only — no endpoint, no fabric, no transport, no Ray.

**Next recommended step.** A **fair, fixed-granularity Ray comparison** (separate slice, if
pursued): a Ray-mediated equivalent that crosses the Ray boundary per leaf versus one coarse
op, counting **boundary crossings** (not wall-clock) at the *same* dependency granularity
exp45 fixed here. Keep it count-first and honest — no speedup, no "HPX beats Ray", no
fabric. Do not start it without explicit approval; exp45 only makes it well-defined.
