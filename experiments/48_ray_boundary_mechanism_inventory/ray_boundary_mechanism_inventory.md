# exp48 — Ray boundary-mechanism inventory (fixed diamond DAG)

**Type:** structural / count. **Not** a performance result, **not** a winner claim. Counts-only, single-node, `hpx_threads=1`, **timing omitted entirely**.

## What this experiment is

exp48 inventories — at **fixed decomposition granularity**, across two substrates (RayX
fixed-op runtime vs **real Ray**) — the **driver/orchestration-observable** mechanism each
uses to carry the same dependency edges of one fixed diamond DAG, i.e. **where each
cross-node dependency edge lives**. It reuses the exp46 diamond and oracle unchanged:

```
A = chain_stage(seed,            quantum)
B = chain_stage(A + 1,           quantum)
C = chain_stage(A + 2,           quantum)
D = chain_stage((B + C) & MASK,  quantum)        # depends on B AND C
```

closed `int64`, `MASK = 0x7FFFFFFF`, 4 nodes, 4 dependency edges (A→B, A→C, B→D, C→D). The
per-node kernel is matched **by value** (RayX native `chain_stage` ≡ `chain_sum_loop(x,1,q)`;
Ray uses a Python `_stage`), gated by `value == oracle`.

## The four paths

* **`rayx_coarse`** — one `diamond_fanin(seed, quantum)` Runtime op. The four edges are
  resolved **in-op by HPX composition, and not by one uniform primitive**: A fans out to B
  and C, so **A→B and A→C** are carried by an `hpx::shared_future` (shared *because* A has
  two consumers) + `.then` continuations → `hpx_shared_future_fork = 2`; **B→D and C→D** are
  plain `hpx::futures` **moved into `hpx::dataflow`** → `hpx_future_into_dataflow = 2`.
* **`rayx_fine`** — exp46 fair decomposition into four `chain_sum_loop(x, 1, quantum)` ops (B
  and C submitted before either retires). Each cross-node edge **round-trips through the
  Python/Runtime boundary as a closed `int64`** → `python_materialized_int64 = 4`.
* **`ray_coarse`** — one Ray remote task wrapping the whole diamond; the four edges are
  **task-local Python values inside one task** → `ray_task_local_python_value = 4` (only the
  final result is an ObjectRef).
* **`ray_fine`** — four Ray remote tasks; ObjectRefs passed naturally, **one** final
  `ray.get` (no premature `ray.get`). The four edges are carried as **Ray ObjectRefs** →
  `ray_objectref = 4`.

**Excluded path:** a "Ray fine with per-node `ray.get`" variant — a strawman (artificial
serialization) that would read as Ray criticism — deliberately omitted.

## Lens note (vs exp46)

exp46 called `hpx::dataflow` *representational* for **boundary counts** (it isn't what made
the op one crossing — any native body would be). exp48 uses a **different lens**: it
inventories the **in-op edge carriers**, for which `shared_future`/`.then`/`dataflow` are the
actual mechanisms. The two framings answer different questions and **do not contradict**.

## Count scope (read this before the table)

Every count is **driver/orchestration-observable only**. For `ray_fine`,
`intermediate_driver_materializations = 0` means **zero driver materializations** — Ray may
perform in-cluster serialization / inlining / object handling that is **intentionally not
driver-counted**. The table must **not** be read as "Ray has no materialization work." For
tiny `int64` payloads a Ray ObjectRef value **may be inlined rather than stored in plasma**;
exp48 **does not assert plasma/object-store transport** and, being single-node, gives **no
transport evidence**.

## Mechanism table (counts-only, driver-observable)

| path | subm | refs (kind) | edge residence (×4) | interm. **driver** mat. | final **driver** mat. (kind) | py-orch events | boundary_kind / driver crossings |
|---|---|---|---|---|---|---|---|
| `rayx_coarse` | 1 | 1 RuntimeFuture | `hpx_shared_future_fork=2`, `hpx_future_into_dataflow=2` | 0 | 1 (`RuntimeFuture.result()`) | 2 | python_runtime / 2 |
| `rayx_fine` | 4 | 4 RuntimeFuture | `python_materialized_int64=4` | 3 | 1 (`.result()` of D) | 8 | python_runtime / 8 |
| `ray_coarse` | 1 | 1 ObjectRef | `ray_task_local_python_value=4` | 0 | 1 (`ray.get`) | 2 | python_driver_cluster / 2 |
| `ray_fine` | 4 | 4 ObjectRef | `ray_objectref=4` | 0 *(driver)* | 1 (`ray.get` of D) | 5 | python_driver_cluster / 5 |

The two `boundary_kind`s are **not the same physical boundary** (in-process value
marshalling vs driver↔cluster process/serialization); crossing numbers are **not
cross-substrate comparable**. Only the within-substrate coarse-vs-fine shape and the
**edge-residence vector** carry cross-substrate meaning.

## Coarse control: what it does and does not equalize

Coarse-vs-coarse equalizes **only the driver/submission boundary shape** (1 submit / 1
materialize). It does **not** imply internal-execution equivalence: `rayx_coarse` internally
runs an HPX futures/continuations/`dataflow` DAG; `ray_coarse` uses task-local Python
values. The control exists to show the fine-row count differences track **where edges live**,
not a substrate-quality verdict.

## Why there is no "HPX-native fine row"

The native HPX fine-grained futures graph already exists **inside `diamond_fanin`**. Exposing
an in-substrate reference across the Python Runtime op boundary is exactly the **future
distributed-fabric direction** question and remains gated — so exp48 deliberately adds no
`rayx_fine_hpx_native` row.

## Observed (this machine; `aggregate.json` beside this note)

`overall_structural_pass = true`. Ray available (v2.55.1); all four paths executed
(`paths_skipped = []`); `value_failures = []`, `count_failures = []`. Every executed row
returns the bit-identical closed `int64` against the oracle, and every row's measured
driver counts equal the declared mechanism formula. (When Ray is unavailable the two Ray
paths are recorded as `paths_skipped` with `ray_available=false` and a reason — **skipped,
not failed** — and the structural pass is computed over the executed RayX rows.)

## Allowed claim

For one fixed diamond DAG, at matched decomposition granularity, RayX (fixed-op runtime)
and real Ray carry the same cross-node dependency edges through **different mechanisms** — a
driver/orchestration-observable structural inventory, single-node, counts-only, **not** a
performance comparison or winner claim. `rayx_coarse` keeps all four edges in-op (A→B, A→C
via `hpx::shared_future` + `.then`; B→D, C→D as futures into `hpx::dataflow`); `rayx_fine`
round-trips each edge through the Python/Runtime boundary as a closed `int64`; `ray_fine`
carries edges as **Ray ObjectRefs**, whose concrete value movement for tiny `int64` payloads
is **implementation-dependent and not asserted** (single-node; no transport evidence);
`ray_coarse` keeps edges as task-local Python values in one task.

**Sharper, HPX-faithful point:** a Ray **ObjectRef** and an HPX **future/`shared_future`** are
both **in-substrate dependency handles** (with different semantics and scopes). RayX
**already uses** HPX in-substrate references inside `diamond_fanin`; its current **fixed-op
Python boundary does not expose** such a reference across op boundaries, so fine-grain RayX
decomposition round-trips closed values through Python. At matched coarse granularity both
substrates reduce to a one-submission / one-materialization **driver/boundary** shape
(internal execution still differs), so the fine-row differences are about **where dependency
edges live**, not a substrate-quality verdict.

## Required non-claims

No speedup / throughput / latency / performance; no HPX faster than Ray; no RayX replaces
Ray; no RayX makes Ray faster; no "Ray is bad"; **no ObjectRef/object-store criticism**; **no
assertion of plasma/object-store transport** for the `int64` payload; no "Python orchestration
is bad"; no real inference; no Ray Serve/Train; no endpoint/fabric; no parcelport/AGAS/
multi-node; **no claim this resolves boundary-vs-transport** (single-node has no transport
evidence); no arbitrary Python execution claim; no scheduler-control / placement-control /
arbitrary-parallelism claim; no overlap/worker-parallelism claim (`hpx_threads=1`); counts
are driver-observable mechanism events, **not costs**, and **not cross-substrate comparable**
across boundary kinds; no implication RayX should add an object store (that is the gated
future distributed-fabric direction); no wall-clock assertions (timing omitted entirely).

---

## Experiment interpretation

* **Passed structurally:** value faithfulness (every row == oracle) and count faithfulness
  (every row's driver counts == declared mechanism formula), across all executed paths.
* **Measured result suggests:** at matched granularity the substrates differ in **where a
  cross-node dependency edge lives** — Ray (fine) keeps it as an in-substrate ObjectRef; HPX
  (inside the coarse op) keeps it as an in-substrate `shared_future`/`future`; the current
  RayX fixed-op fine decomposition round-trips it through Python because that API exposes no
  in-substrate reference across op boundaries.
* **Hypothesis supported/weakened:** strengthens the framing that the in-substrate
  **dependency-reference** is the shared concept across Ray and HPX, and locates RayX's
  fine-grain round-trip as an **API-boundary** property, not an HPX limitation.
* **Remains ambiguous / not shown:** any cost; any transport behavior (single-node, tiny
  payload — ObjectRefs may be inlined); whether exposing an in-substrate reference across the
  RayX op boundary is desirable (gated).
* **Must not be claimed:** anything in the non-claims block — especially perf, transport, or a
  substrate verdict.

## Roadmap impact

**Roadmap strengthened (characterization), future distributed-fabric direction unchanged and
still gated.** exp48 adds a real Ray reference point to the in-process characterization arc
and surfaces the in-substrate-dependency-reference concept that the future distributed-fabric
direction would have to reason about — as an **observation**, not a proposal, and with **no**
transport evidence pulled forward.

## Updated roadmap

* **In-process direction:** local scheduling, nonblocking lanes, native composition
  (exp39/40/46), barrier rendezvous (exp44), boundary-placement/counts (exp45/46), in-flight
  overlap (exp47), and now a **cross-substrate driver-observable mechanism inventory vs real
  Ray for one fixed DAG (exp48)**.
* **Future distributed-fabric direction:** unchanged and still gated (Ray as
  placement/bootstrap/lifecycle; HPX locality-to-locality or lighter inter-actor transport;
  endpoint discovery; remote-action prototype; multi-node comparison). exp48 provides **no**
  transport/endpoint/fabric/parcelport/AGAS/multi-node evidence and must not advance it.

## Next recommended step

If a cost question is ever opened, it must be a **same-substrate, same-granularity,
explicitly observation-only** measurement (never cross-substrate timing of native-C++ vs
Python kernels across process models). Otherwise the characterization arc for a single fixed
DAG is now complete; the next credible move is to pre-register what an **in-substrate
dependency reference** would mean for the RayX boundary **as a design question only**, keeping
the future distributed-fabric direction gated until that design and a fair cost model exist.
