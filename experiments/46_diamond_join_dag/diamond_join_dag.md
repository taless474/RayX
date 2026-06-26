# exp46 — diamond/join DAG boundary-placement comparison

**Status:** closed as a boundary-placement / orchestration-**count** result for **one
fixed non-linear (diamond/join) dependency DAG**. Deterministic and machine-independent.
**Not** a performance result, **not** a discovery — the counts below are analytically
predictable; the run confirms them through the real Runtime/lane boundary.

## What this shows

The **same** closed workload — a fixed diamond DAG `A → {B, C} → D`, where the sink `D`
depends on **both** arms (the cross-edge join) — computed two ways that return the
**bit-identical** closed `int64`, differing only in *where the boundary is placed*. Each
node is a single `chain_stage`:

```
A = chain_stage(seed,            quantum)
B = chain_stage(A + 1,           quantum)        # depends on A
C = chain_stage(A + 2,           quantum)        # depends on A
D = chain_stage((B + C) & MASK,  quantum)        # depends on B AND C  (cross-edge join)
result = D                                       # (B + C) commutative → order-free
```

| | Path A — native / coarse | Path B — Python-mediated (fair) |
|---|---|---|
| how | `diamond_fanin(seed, quantum)` | 4 × `chain_sum_loop(x, 1, quantum)` nodes |
| Runtime submissions | **1** | **4** (= node count N) |
| Runtime retirements | **1** | **4** |
| **Python/Runtime boundary crossings** | **2** | **8** (= 2N) |
| **intermediate node values materialized in Python** | **0** | **3** (= N−1: A, B, C) |
| cross-edge join | native (`hpx::dataflow`) | Python holds B and C, then submits D |

`chain_sum_loop(x, 1, quantum)` applies `chain_stage` exactly once → it **is** a diamond
node. Path B is the *same logical computation* relocated to the Python side of the
boundary. **No new op, no arbitrary Python execution, no object store.**

Path B is written in its **fair form**: B and C depend only on A, so both are submitted
before either is retired (they are not artificially serialized). The forced round-trips
are a property of the **value-in / value-out op interface with no `ObjectRef`/object
store** — Python must hold each parent value (`A`, then `B` and `C`) to feed its children
— **not** a deficiency of Python orchestration.

## The counts are analytically predictable

The boundary counts here are **derivable on paper before running**, directly from the
fixed 4-node diamond and the value-only op interface: the coarse path is `1` / `1` / `2`
with `0` intermediate materializations; the Python-mediated path is `4` / `4` / `8` with
`3`. Nothing here could "fail" except an implementation bug.

The experiment is therefore a **faithfulness / liveness confirmation**, not a finding.
Running it establishes two things and no more: (1) the native `diamond_fanin` op, executed
through the real Runtime submission/retirement and lane path, returns the **bit-identical**
closed `int64` as the pure-Python oracle for every `(seed, quantum)`; and (2) the
**observed** submit/retire/materialization counters (incremented at runtime, not asserted)
match the analytic formulas.

## Value equivalence (the substance)

For every `(seed, quantum)`, triangulated against a pure-Python oracle that mirrors native
masking exactly (per-step mask in `masked_range_sum`, uint64 wrap in `chain_stage`):

```
result_a (diamond_fanin)  ==  result_b (chain_sum_loop nodes + Python joins)  ==  oracle
```

The integration test additionally triangulates the native coarse op against the **native**
Path-B decomposition (`chain_sum_loop` per node), so equivalence holds native-vs-native and
native-vs-oracle.

## `hpx::dataflow` is representational, not load-bearing for the count

The `2`-vs-`8` crossing difference is **not caused by `hpx::dataflow`**. A plain sequential
native C++ diamond body — compute `A`, then `B` and `C`, then `D`, with no futures at all —
would produce the **same value and the same boundary counts**, because the saving comes from
placing the whole computation behind **one** coarse op, not from any HPX composition
primitive.

`hpx::shared_future` (forking the two arms off `A`), the `.then` continuations, and
`hpx::dataflow` (the join) are present for one reason: to **express and resolve the
diamond's cross-edge join below the Python/Runtime boundary in HPX-native form**. That is
the honest increment over exp45, whose fan-in leaves were order-free and individually
batchable — here the join `D = f(B, C)` genuinely cannot be batched away under a value-only
interface, so the Python-mediated path is obliged to materialize `A`, `B`, and `C`. This
experiment does **not** and **cannot** show that `dataflow` does anything a sequential body
wouldn't (it runs canonically at `--hpx-threads=1`); it makes **no overlap, concurrency,
parallelism, or scheduling claim**.

## Result (this machine, count-only — the load-bearing run)

`hpx_threads=1`, `seeds={1,3,7}`, `quanta={16,64}`, `overall_structural_pass = true`:

| seed | quantum | result (= oracle) | eq | A sub/ret/xing/interm | B sub/ret/xing/interm |
|---:|---:|---:|:--:|:--:|:--:|
| 1 | 16 | 605 | OK | 1 / 1 / **2** / **0** | 4 / 4 / **8** / **3** |
| 1 | 64 | 10085 | OK | 1 / 1 / **2** / **0** | 4 / 4 / **8** / **3** |
| 3 | 16 | 609 | OK | 1 / 1 / **2** / **0** | 4 / 4 / **8** / **3** |
| 3 | 64 | 10089 | OK | 1 / 1 / **2** / **0** | 4 / 4 / **8** / **3** |
| 7 | 16 | 617 | OK | 1 / 1 / **2** / **0** | 4 / 4 / **8** / **3** |
| 7 | 64 | 10097 | OK | 1 / 1 / **2** / **0** | 4 / 4 / **8** / **3** |

The value varies and matches the oracle every time while the counts stay pinned —
demonstrating the counts are structural and value-independent.

## Timing (opt-in `--timing`, default OFF, observation-only, tautological)

Timing is **not** the result and is off by default. When enabled it is **tautological**:
fewer pybind transits cost less pybind time. It is **not** a speedup / throughput /
latency / HPX / Ray / scheduling claim and must not be cited as one.

## Allowed claim

> exp46 compares two boundary placements for one fixed non-linear (diamond/join)
> dependency DAG: a single coarse Runtime op (`diamond_fanin`) that resolves the cross-edge
> join natively below the Python/Runtime boundary via HPX `shared_future` continuations +
> `hpx::dataflow`, versus a Python-mediated decomposition into fixed value-in/value-out
> Runtime ops (`chain_sum_loop` per node) that must materialize each intermediate node value
> in Python to satisfy data dependencies. Both return the bit-identical closed `int64`
> (triangulated against a pure-Python oracle). The coarse path is 1/1/2 with 0 intermediate
> materializations; the Python-mediated path is 4/4/8 with 3. The counts are analytically
> predictable; the run is a faithfulness/liveness confirmation, not a discovery. This is
> boundary-placement / orchestration-count evidence for one fixed diamond DAG only.

## Required non-claims

No speedup · no throughput · no latency/performance claim (timing is opt-in,
observation-only, tautological) · no HPX faster than Ray · no Ray comparison · no real
inference · no endpoint/fabric claim · no parcelport · no AGAS · no multi-node ·
no `ObjectRef`/object-store semantics · no arbitrary Python execution · no
parallelism/overlap/scheduling claim (canonical `--hpx-threads=1`) · **not** a proof about
arbitrary DAGs (one fixed diamond) · **no claim that Python orchestration is "bad"** — it
is a legitimate, flexible pattern; exp46 only *counts* its Runtime-boundary transits for
this workload.

## Files

* `run_diamond_join_dag.py` — the comparison (Path A / Path B fair form, pure oracle,
  counters, optional timing).
* `diamond_join_dag.md` — this write-up.
* `aggregate.json` — generated; deterministic counts + equivalence gates.

## Run

```
python -m py_compile experiments/46_diamond_join_dag/run_diamond_join_dag.py
PYTHONPATH=python/src python experiments/46_diamond_join_dag/run_diamond_join_dag.py --smoke
PYTHONPATH=python/src python experiments/46_diamond_join_dag/run_diamond_join_dag.py
# optional, observation-only (tautological), not a performance run:
PYTHONPATH=python/src python experiments/46_diamond_join_dag/run_diamond_join_dag.py --timing
```

## Interpretation and roadmap impact

**Experiment interpretation.** *What passed structurally:* value-equivalence
`result_A == result_B == oracle` for all `(seed, quantum)`; observed counters equal to the
analytic constants `1/1/2` (0 intermediates) and `4/4/8` (3 intermediates), invariant across
the value sweep. The cross-edge join is resolved natively below the boundary on Path A and
forces 3 Python-side intermediate materializations on Path B. *What it supports:* it extends
the in-process boundary-placement story from linear chains (exp39), associative fan-in
(exp45), and barrier rendezvous (exp44) to a **non-linear DAG with a non-batchable
cross-edge** — the first case where the dependency structure itself, not just the op count,
is what the boundary placement relocates. *What must not be claimed:* no speedup, throughput,
latency; no Ray comparison; no endpoint/fabric, parcelport, AGAS, multi-node; no
`ObjectRef`/object-store; no arbitrary Python execution; no real inference; no
parallelism/overlap/scheduling claim; **not** a result about arbitrary DAGs (one fixed
diamond only); and **no** claim that Python orchestration is bad — the 3 intermediate
materializations are a property of the deliberate value-only op interface (no object store),
not a defect of orchestration.

**Roadmap impact: `Roadmap strengthened`.** exp46 is the **terminal count-only slice** of the
local boundary-placement arc. Together, exp39 → exp40 → exp44 → exp45 → exp46 now cover linear
chain, independent fan-out, barrier rendezvous, associative fan-in count, and non-linear
cross-edge join — the in-process "where is the Python/Runtime boundary placed" story is
consolidated and does not need another, larger count-only DAG. Producing one would add no new
knowledge.

**Updated roadmap (directions separated).**

* *In-process HPX inside Ray actors:* consolidated on the count axis. The open question is no
  longer "how many crossings" but whether the native interior **does** something a sequential
  body can't — i.e., concurrency/overlap.
* *Future distributed-fabric direction:* unchanged and still gated. exp46 is in-process and
  count-only; no endpoint, transport, fabric, or Ray.

**Next recommended step (exactly one, and not part of exp46).** Pivot off counting to one of:

1. a **carefully framed in-process overlap/concurrency observation** at `--hpx-threads>1` —
   the question `dataflow` actually exists to answer — with explicit, conservative framing
   that distinguishes observed overlap from any throughput/speedup claim and stays clear of
   the `max_simultaneously_suspended_leaves`-style overreach; **or**
2. a **fair fixed-granularity Ray boundary-mechanism comparison** — count (not wall-clock)
   boundary crossings for the same diamond at the same dependency granularity across the Ray
   boundary versus one coarse op, to begin separating Ray's boundary/orchestration cost from
   transport.

Choose only one as the next slice; exp46 only makes them well-defined. Neither is started
here, and the distributed-fabric direction stays gated until the project has evidence about
whether Ray's relevant cost is boundary/orchestration versus transport.
