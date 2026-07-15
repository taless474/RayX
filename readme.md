# RayX

<p align="center">
  <img src="docs/figures/logo.png" alt="RayX logo" width="280">
</p>

**RayX** is an experimental **Ray-hosted HPX-native runtime**. It asks a narrow
question: can a long-lived Ray actor host a narrow HPX-native runtime while
preserving useful HPX execution semantics — futures, async work, cooperative
suspension, cancellation, admission/backpressure, and local native actor state —
across the Python/Ray boundary?

* **Ray** owns the outer distributed boundary: actor placement, process
  lifecycle, and Python-ecosystem integration.
* **RayX/HPX** owns the local native runtime *inside* one Ray actor: fixed
  registered native operations, local native actors, HPX futures/continuations,
  runtime lanes, cancellation, admission/backpressure, and shutdown.
* The **benchmark/experiment harness** is synthetic evidence infrastructure used
  to isolate and validate adapter/runtime mechanisms — not real model inference,
  not Ray Serve, and not a Ray replacement.

**Naming:** **RayX** = this repo / project (the Ray-hosted HPX-native runtime
exploration plus its synthetic evidence harness); **`rayx`** = the Python package
(the thin `Engine` frontend over HPX service lanes, and the experimental
`rayx.runtime` of fixed registered native operations and local native actors).

## What this project is

* An experimental **Ray-hosted HPX-native runtime**: a long-lived Ray actor
  hosts one in-process RayX/HPX runtime, Ray owns placement/lifecycle, and only
  plain values/results cross the Ray boundary.
* `rayx.runtime`, the experimental native runtime — fixed registered native
  operations and local native actors over HPX-native FIFO `RuntimeLane`s, with
  HPX futures, cooperative cancellation, bounded admission, and shutdown.
* A **synthetic evidence harness** over a shared workload contract and v1 metrics
  schema — the `rayx` `Engine` frontend, a Ray actor baseline (public Ray APIs),
  and an HPX-native C++ baseline — used to isolate adapter/runtime mechanisms.
* Exploratory, narrow side-seams: a local **`rayx.endpoint`** AF_UNIX seam and
  experiment-only **HPX connect-mode / Ray-orchestrated HPX-island** probes. These
  are standalone mechanism experiments — not shipped `rayx.runtime` API, not a
  fabric, and not performance evidence.

## What this project is not

* Not a Ray replacement, and not a fork of Ray.
* Not modifying Ray Core internals; not Ray Serve / Ray Train / Ray object store.
* The `Engine` / `SyntheticActor` harness runs synthetic C++ work only, and the
  experimental `rayx.runtime` prototype runs only fixed registered native
  operations and fixed registered native actor methods — neither runs arbitrary
  Python functions remotely, and neither has an object store / `ObjectRef`, Ray
  task semantics, or real model inference.
* Not benchmarking real model inference.

## Architecture at a glance

Four distinct paths exist in this repo, and they should not be conflated:

```
Python caller ──┬─ rayx.Engine ───► HPX-backed FIFO service lanes   (synthetic harness)
                └─ rayx.runtime ──► fixed native ops + local native  (experimental,
                                    actors over HPX RuntimeLanes      one process)

Ray actor (long-lived) ── hosts ──► one in-process RayX/HPX runtime (in-process
                                    (Ray owns placement/lifecycle)   direction)

experiments/ only ───────────────► standalone HPX root ⇄ connector  (evidence-only;
                                    localities, connect-mode TCP;    NOT shipped
                                    launched by Python/Slurm/Ray     rayx.runtime API)
```

| Path | What it is | Where it runs |
|---|---|---|
| **`rayx.Engine`** (+ `SyntheticActor`) | Thin Python frontend over local HPX-backed service lanes; the synthetic workload harness | Local process |
| **`rayx.runtime`** | Experimental fixed registered native operations and local native actors | One local process |
| **Ray-hosted HPX runtime direction** | One long-lived Ray actor hosts one local HPX-native runtime; Ray owns placement and process lifecycle | Inside one Ray actor process |
| **Distributed HPX-island experiments** | Standalone experiment-only HPX localities, sometimes launched or supervised by Ray/Slurm | Standalone processes under `experiments/` — **not** the shipped `rayx.runtime` API |

## What exists today

Implemented and runnable now:

* **`rayx.Engine` / `SyntheticActor`** — the Python frontend over serialized FIFO
  service lanes (`ServiceLane` default; opt-in `HpxLane` behind the same
  contract), with `lane_stats()`, bounded admission (`QueueFullError`), queued and
  chunk-boundary cancellation, `get` / `wait` / `as_completed`, and shutdown
  drain. Synthetic workloads only.
* **`rayx.runtime`** (*experimental*) — fixed registered native operations
  (`square`, `add`, `busy_sum`, `fanout_sum`, `park_ms`, and the other registered
  diagnostics) plus the local native `CounterActor`
  (`Runtime.create_actor("counter", ...)` → `ActorHandle.call(...)`). Futures
  with `get` / `wait` / `as_completed`, cooperative cancellation, bounded
  admission, and clean shutdown where supported. Closed `int64`/`double` values
  only — no object store, no arbitrary Python execution.
* **Baselines** — a Ray actor baseline (public Ray APIs only) and an HPX-native
  C++ baseline, both over the shared workload contract and v1 JSONL metrics
  schema.
* **Experiment-only distributed pieces** (*evidence-only*) — standalone HPX
  connect-mode binaries and Python→HPX pybind bindings under `experiments/`.
  These exist to produce evidence and are never shipped `rayx` API.

The `Engine` harness and baselines are synthetic evidence infrastructure;
`rayx.runtime` is experimental; everything distributed is evidence-only.

## Maturity at a glance

| Area | Implemented? | Validated? | Scope |
|---|---|---|---|
| Local `rayx.Engine` service lanes | Yes | Yes — synthetic benchmark arc (benchmarks 01–10, experiments 01–23) | Local process; synthetic workloads only |
| Local `rayx.runtime` fixed native ops | Yes (experimental) | Yes — contract smokes, unit/integration tests (exp24–26, 39–44) | One process; closed `int64`/`double`; no object store |
| Local native actors (`CounterActor`) | Yes (experimental) | Yes — contract coverage (exp24–26, 30) | Local, fixed registered methods only |
| HPX runtime hosted inside one Ray actor | Yes (composition) | Yes — exp27–38 in-process arc | One Ray actor, single node; no cross-actor HPX |
| Ray-orchestrated HPX child-process island | Experiment-only | Bootstrap/supervision validated (exp52, 57) | Standalone child processes, not Ray actor workers |
| **Networking HPX connect-mode locality inside one Ray actor worker** | Experiment-only | Yes — exp66 local (3/3) + Rostam cross-node (3/3) | One actor; in-process by exact PID identity, no HPX child; exp67 extends this to two actors |
| **Two Ray actors sharing one HPX runtime (actor-to-actor HPX)** | Experiment-only | Yes — exp67 local (3/3) + Rostam three-node (3/3) | Two distinct actors on distinct nodes; bidirectional actor-to-actor HPX actions; work-free root; the [across-Ray-actors gate](docs/design/rayx_hpx_to_hpx_across_ray_actors_gate.md) is now closed |
| **Deterministic LLM-shaped distributed workload (vocab-sharded top-k)** | Experiment-only | Yes — exp68 local (3/3) + Rostam three-node (3/3) | Exact token-ID + float32-bit agreement vs an independent oracle; both coordinator directions; synthetic, **not** real inference |
| Distributed same-axis experiment path | Experiment-only | Completed evidence (exp61–64 on Rostam; exp65 on macOS loopback and reproduced across two Rostam nodes) | Per-arm measurements and mechanism probes only |
| Real model inference / serving | No | — | Out of scope |

The short version: local `rayx` and local Ray-hosting have real, validated code
paths; the standalone distributed experiments have completed evidence; and the
Ray-hosted HPX arc has now closed three gates in sequence — **a networking HPX
connect-mode locality runs in-process inside a Ray actor worker (exp66); two Ray
actors share one HPX runtime with verified bidirectional actor-to-actor HPX actions
(exp67); and a deterministic LLM-shaped vocabulary-sharded top-k runs across two
actors with exact token-ID and float32-bit agreement against an independent oracle
(exp68) — each demonstrated locally and across three Rostam nodes**. exp69, a strict
same-axis Ray-mediated vs HPX-mediated performance comparison, is **planned, not yet
evidence**.

## Quickstart

All commands assume the repo root as the working directory. Each benchmark driver
writes a per-request JSONL file; `bench/analyze_jsonl.py` rolls it up into an
aggregate summary under `results/`.

### Easiest path: Ray-only baseline (no HPX toolchain needed)

```bash
pip install -r requirements.txt   # ray
python bench/run_ray_baseline.py --service-ms 0 --concurrency 1 \
    --requests 1000 --out results/ray_noop_c1.jsonl
python bench/analyze_jsonl.py results/ray_noop_c1.jsonl
```

### Local HPX / `rayx` path

*Not* a one-line setup. The `rayx` frontend and the HPX-native baseline require
HPX v1.11.0 built/installed from source, then the local pybind11 `_rayx`
extension built against that prefix. See
[docs/hpx_build_notes.md](docs/hpx_build_notes.md) for the full build. Once HPX is
installed:

```bash
pip install -r requirements-python.txt   # pybind11

cmake -S python -B python/build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DPYBIND11_FINDPYTHON=ON \
    -DCMAKE_PREFIX_PATH="/path/to/hpx-install;$(python -m pybind11 --cmakedir)"
cmake --build python/build

python bench/run_hpx_python_baseline.py --service-ms 0 --concurrency 1 \
    --requests 1000 --num-lanes 1 --hpx-threads 4 \
    --out results/rayx_noop_c1.jsonl
python bench/analyze_jsonl.py results/rayx_noop_c1.jsonl
```

### `rayx.runtime` example

With `_rayx` built (previous step), the smallest native-runtime tour is:

```bash
python examples/rayx_runtime_basic.py
```

Other runnable API tours live under [examples/](examples/) (`rayx_basic.py`,
`rayx_actor_pool.py`, `rayx_lane_impl.py`, `rayx_bounded_admission.py`,
`rayx_runtime_basic.py`). To run the local smoke/golden gates in one step, use the
stdlib-only aggregator `python bench/smoke_local.py` (it skips any unavailable
tier — no built `_rayx`, no native binary, no Ray). The full `rayx` API and the
`rayx.runtime` surface are documented in the reference docs (see the
documentation map).

### Distributed experiments are not part of the quickstart

The distributed probes (exp57–66) need Rostam/Slurm-specific setup (multi-node
exclusive allocations, pinned subnets, experiment-local binaries) or
experiment-local standalone builds. They are evidence experiments under
`experiments/`, not part of the normal quickstart and never part of the shipped
`rayx` API.

## Current status

Ray actor baseline, HPX-native synthetic baseline, and the `rayx` Python frontend
all exist and run. The stable comparison anchor across the whole arc is the
actor-like, single-`std::thread` `ServiceLane` (one serialized FIFO lane), shared
by the HPX-native baseline and `rayx`; the public `rayx` API and the v1 benchmark
JSONL schema (still `1`) are unchanged across the additions below.

Evidence spans the synthetic serving-control benchmarks, the `rayx.runtime` native
operations and local native actors, Ray-hosting composition, in-process HPX
composition, an HPX-island lifecycle policy (now including exp65 demand-triggered
connect-mode admission, demonstrated on loopback and across two Rostam nodes), and
a distributed same-axis / payload-ladder evidence arc
(experiments 61–64, with the exp64 Phase A→A4 native-readiness diagnostic and its
waiter-fix verification complete). The full chronological **“what we learned”**
index lives in
[docs/evidence_index.md](docs/evidence_index.md); the headline summary is below.

exp64 (through its Phase A→A4 readiness diagnostic and the follow-up waiter-fix
verification) and exp65 (both its loopback and Rostam cross-node slices) are
completed evidence. Exp64 isolated a suspended timed-wait wakeup defect on HPX
1.11; the upstream discussion confirmed it as an HPX bug fixed by PR #7367, and
the identical discriminator rerun against HPX master commit `20bc3d4b`
(`20bc3d4bf3068383edcb63be13f22e9ff95842fa`) resumed on readiness across four
runs and two thread configurations. Version policy: new distributed experiments
use that verified fixed build unless a historical-control run explicitly requires
HPX 1.11; older evidence remains scoped to the HPX versions originally recorded;
the repository-wide HPX 1.11 pin is unchanged. The HPX serialization
runtime-path blocker remains open, so the stronger payload-ladder grade is still
not earned.

exp66 is completed evidence for the pivotal in-worker hosting question: **a
networking HPX connect-mode locality can run in-process inside a Ray actor worker,
proven by exact PID identity and the absence of HPX child processes, locally and
across two Rostam nodes.** Ray handled actor placement and lifecycle; HPX actions
carried the distributed operation and its result; a separately supervised,
work-free root locality remained on its own node; and actor health, graceful
teardown, and actor recreation all passed. Both slices ran against the verified
fixed HPX build `20bc3d4b` — the local slice on CPython 3.11.15 (3/3), the Rostam
cross-node slice (Slurm job 170524, medusa00 root/prober, medusa01 Ray actor, TCP
parcelport over `10.42.5.x`) on CPython 3.12.3 (3/3). exp66 is the prerequisite for
exp67, which delivered it (below). exp66's own scope was **one actor only**; **not**
Python 3.14 and **not** free-threaded Python (both deferred); no elasticity and no
individual-locality failure recovery; and no performance claim (the actor-thread
saturation case is a non-gating diagnostic that progressed in every rep, not a
performance or GIL verdict).

exp67 closed the two-actor shared-runtime gate. **Two distinct Ray actor worker
processes each hosted a networking HPX connect-mode locality in-process (exact PID
identity for both, zero HPX child processes) and joined one shared HPX runtime under
a separately supervised, work-free root; the load-bearing proof is bidirectional
actor-to-actor HPX — A→B proved B's PID, locality, and hostname plus the closed
oracle, and B→A proved A's.** Neither actor holds a Ray handle to the other, so the
operation path is HPX, not Ray (proven by construction, not by direct wire
instrumentation). Both slices ran against fixed HPX `20bc3d4b`: the local slice on
CPython 3.11.15 (3/3), and the Rostam three-node slice (accepted job 170744; smoke
job 170743) on CPython 3.12.3 (3/3) — root/controller on medusa00, actor A
hard-placed (`NodeAffinitySchedulingStrategy(soft=False)`) on medusa01, actor B on
medusa11 — three roles on three distinct nodes, endpoints pinned to `10.42.5.x`, both
directions crossing nodes, work-free root, clean lifecycle, actor recreation, and
orphan-clean sweeps on all three nodes. Kept explicit: **not** Python 3.14 or
free-threaded; no elasticity, churn, or individual-locality recovery; **no
performance claim**; operation-over-HPX-not-Ray proven by construction; not a
production API.

**Architecture, stated safely:** *Ray provides placement, lifecycle, and supervision,
while Ray-hosted HPX localities in one shared runtime execute distributed actions,
futures, and composition directly between actors.*

exp68 closed the first deterministic LLM-shaped distributed-workload gate on top of
exp67's two-actor runtime. The workload is a **synthetic** vocabulary-sharded top-k
(next-token candidate-selection shape, **not** real inference): deterministic,
exactly-representable float32 logits on a 1/8 grid; two disjoint, complete contiguous
vocabulary shards; one total order (higher logit first, then lower global token id on
ties). Each actor computes its shard's local top-k; a coordinator fetches the peer's
candidates over an HPX action and merges through an HPX future/continuation; the
global top-k is checked bit-exactly against an independent controller oracle, in both
coordinator directions, across a seven-case matrix. **Two Ray actors on distinct
nodes executed the deterministic vocabulary-sharded top-k operation through HPX
actions/futures in both coordinator directions, with exact token-ID, ordering, and
float32-bit agreement against an independent oracle.** Local (CPython 3.11.15, 3/3)
and Rostam three-node (accepted job 170746; smoke job 170745; CPython 3.12.3, 3/3;
same medusa00/01/11 topology). The cross-node run adds the serialization
discriminator: **204 peer-transferred candidate bit patterns checked (68 per
repetition), zero mismatches** — bit-exact candidate values survived the cross-node
HPX action path (no direct internal serialization instrumentation claimed). Kept
explicit: **not** real LLM inference; no model weights, tokenizer, GPU, or framework;
**not** Python 3.14 or free-threaded; no elasticity, churn, or failure recovery; **no
latency, throughput, ratio, speedup, or winner claim**; not a production API.

exp69 is the next planned step: a **strict same-axis Ray-mediated vs HPX-mediated
performance comparison** of this exact workload — same actors, same placement, same
Python caller boundary, correctness verified before any timing sample counts. It is
**planned only: no results and no ratio/speedup/winner claim exist**, and any future
comparison must stay exact-case, exact-boundary, exact-placement, and version scoped.

## Current evidence snapshot (experiments 61–68)

**Evidence in one paragraph.** The distributed evidence progressed in eight steps:
**exp61** timed one scalar remote call with both arms at the same Python caller
boundary; **exp62** extended that same-axis method to a distributed N=8
fanout/fanin; **exp63** traced an HPX native-composition failure to connector
lifetime and validated native composition once lifetime was hardened; **exp64**
added the response-payload-size axis (within-arm distributions only) plus a
suspended timed-wait readiness diagnostic whose defect was later verified fixed
on an exact HPX master commit; **exp65** showed connect-mode
admission can be demand-ordered rather than assembled up front, on loopback and
across two real nodes; **exp66** moved HPX itself *inside* a Ray actor worker —
a networking connect-mode locality running in-process (proven by exact PID
identity, no HPX child); **exp67** then had two Ray actors share one HPX runtime
with verified bidirectional actor-to-actor HPX actions; and **exp68** ran a
deterministic LLM-shaped vocabulary-sharded top-k across those two actors with exact
token-ID and float32-bit agreement against an independent oracle — exp66–68 each
demonstrated locally and across three Rostam nodes. Together these
inform the future distributed design direction — they are **not** a shipped
distributed RayX API, and none of them licenses a Ray-vs-HPX ratio, speedup, or
winner.

Experiments 61, 62, and 64 use a single Python caller boundary on Rostam
(medusa nodes, subnet `10.42.5.`), while exp63 is the HPX-native
composition/progress diagnostic that explains and hardens the HPX side. exp65 is
a separate connect-mode **admission** mechanism probe with two slices — macOS
loopback and a Rostam two-node cross-node reproduction (medusa00/medusa01, Slurm
job 170014) — lifecycle evidence, not a same-axis timing arm.
Every result reports the two arms **separately**: **no ratio, no speedup, no cross-arm difference, no
winner**. Throughout, the HPX side is an **experiment-only Python→HPX action path** (a pybind binding),
**not** the shipped `rayx.runtime` API and **not** distributed RayX; the closed-`int64` oracle / payload
digest is the only cross-arm correctness anchor. For the measured exp62 and exp64 HPX arms, the HPX composition is
`root_flat_gather_poll`, a proven interim gather baseline, not a final
HPX-native collective.

| Experiment | Question | Setup | Status | Safe to claim | Not claimed |
|---|---|---|---|---|---|
| **exp61** | Scalar remote-call RTT at one Python boundary | Ray actor RPC vs experiment-only Python→HPX scalar action; medusa00→01; R=5; closed-`int64` | **Same-axis**; all gates passed | Per-arm RTT bands for this QD1 closed-`int64` call at the same boundary | Any ratio/speedup/winner; production API; broad benchmark |
| **exp62** | Distributed fanout/fanin RTT at one Python boundary | N=8 **all-remote**, 4/4 hard-pinned; medusa00→01/02; R=5; K=1000/W=100 | **Same-axis**; strongest distributed scalar evidence | Per-arm RTT bands for this closed-`int64` N=8 fanout/fanin | Ratio/speedup/winner; production distributed API; final HPX collective |
| **exp63** | Is HPX-native cross-node composition viable? | Native `when_all`/`dataflow` reduce + depth-2 star-of-partials; connector-lifetime hardening | **Mechanism validated** (20/20 per mode) | Native composition works once connector lifetime is correct; root-of-partials works cross-node | No Ray comparison; no performance numbers; no `hpx::collectives` |
| **exp64** | How does response-payload size behave, per runtime? | Payload ladder `[0..256 KB]`; HPX poll-gather vs Ray coordinator; R=5 band, measured=30 | **`matched_band_r5`**; within-arm only; **Phase A→A4 diagnostic + waiter-fix verification complete** | Each arm's own within-arm p50/p90 payload-size curve; structural repeatability; scoped `waiter_resume_at_timeout` signature on HPX 1.11, verified fixed (`waiter_resumed_on_ready`) on master `20bc3d4b` with PR #7367 | Cross-arm comparison; ratio/speedup/winner; p99; `distributional_payload_ladder`; general HPX claim (fix scoped to the exact tested commit) |
| **exp65** | Can connect-mode admission be demand-ordered? | Root alone → local HPX work → external demand event → one connector; two slices: macOS loopback (HPX 1.11, plain-Python controller) and Rostam cross-node (medusa00→medusa01, TCP `10.42.5.x`, job 170014) | **Mechanism pass**, 3/3 both arms in both slices | Demand-ordered admission on loopback and across two real nodes, within a boot-time `--hpx:expect-connecting-localities` willingness: count-free discovery, verified remote action, graceful leave, clean finalize | HPX inside Ray actors; elasticity during in-flight work; concurrent churn; failure recovery; lazy TCP connection establishment; anything beyond two nodes; performance |
| **exp66** | Can a networking HPX locality run in-process inside a Ray actor worker? | One work-free root + one Ray actor hosting a connect-mode HPX locality in-process (`hpx::start`, no child); local slice (CPython 3.11.15, 3/3) and Rostam cross-node (job 170524, medusa00 root/prober → medusa01 actor, TCP `10.42.5.x`, CPython 3.12.3, 3/3); fixed HPX `20bc3d4b` | **In-worker hosting pass**, 3/3 each; prerequisite for exp67 | In-process hosting by exact PID identity (no HPX child), hard actor placement on the actor node, verified cross-node HPX action + oracle on the actor locality, idle progress, clean graceful lifecycle, actor recreation, no orphans | One actor (exp67 extends to two); Python 3.14; free-threaded Python; elasticity or individual-locality recovery; any performance/GIL verdict (saturation is a non-gating diagnostic) |
| **exp67** | Can two Ray actors share one HPX runtime with actor-to-actor HPX actions? | Work-free root + two Ray actors each hosting a connect-mode HPX locality in-process; bidirectional A↔B HPX actions; local slice (CPython 3.11.15, 3/3) and Rostam **three-node** (root medusa00, actor A medusa01, actor B medusa11; hard `NodeAffinity(soft=False)`; TCP `10.42.5.x`; smoke job 170743, accepted job 170744; CPython 3.12.3, 3/3); fixed HPX `20bc3d4b` | **Two-actor shared-runtime pass**, 3/3 each; across-Ray-actors gate closed | PID identity for both actors (no HPX child), both hard-placed on distinct nodes, A→B proves B's PID/locality/hostname + oracle and B→A proves A's, both directions cross nodes, work-free root, clean lifecycle/recreation/orphan sweeps | Python 3.14; free-threaded Python; elasticity/churn/individual-locality recovery; any performance claim; production API (operation-over-HPX-not-Ray proven by construction, not wire instrumentation) |
| **exp68** | Can two Ray-hosted HPX actors run a deterministic LLM-shaped distributed workload exactly? | Synthetic vocab-sharded top-k (deterministic float32 logits, disjoint shards, exact tie-break); local top-k + A- and B-coordinated HPX-future/continuation merge vs an independent oracle; 7-case matrix; local (CPython 3.11.15, 3/3) and Rostam three-node (medusa00/01/11; smoke job 170745, accepted job 170746; CPython 3.12.3, 3/3); fixed HPX `20bc3d4b` | **Exact-workload pass**, 3/3 each; both coordinator directions | Exact token-ID, ordering, and float32-bit agreement vs the oracle in both directions; **204 cross-node peer-transferred candidate bit patterns (68/rep), zero mismatches**; coordinator symmetry; clean lifecycle | Real LLM inference; model/tokenizer/GPU/framework; Python 3.14 / free-threaded; elasticity/churn/recovery; **latency/throughput/ratio/speedup/winner**; direct serialization instrumentation; production API |

### exp61 — scalar same-boundary remote call

The first **same-axis** comparison of a QD1 closed-`int64` micro-call: both arms timed with the same
monotonic clock around one blocking Python call, in an **R=5** matched band on **medusa00 → medusa01**
(subnet `10.42.5.`, K=1000 / W=100). All Slice-4 gates passed (`same_axis_comparison=true`); the two
arms are reported **separately** with `speedup_computed=false`, `ratio_reported=false`,
`arms_differenced=false`.

Cross-node, R=5 (per-arm bands, µs):

| arm (same Python boundary) | p50 | p90 | p99 | mean |
|---|---|---|---|---|
| Ray actor path | ~518.3 | ~850.7 | ~1125.7 | ~584.7 |
| experiment-only Python→HPX action path | ~184.8 | ~257.5 | ~322.6 | ~188.7 |

A **same-node placement control** (single node medusa00, R=5, all gates passed) is a control, not a
ranking: Ray p50 ~519.1 µs, experiment-only Python→HPX p50 ~93.0 µs. This is one closed-`int64`
micro-call on one node pair — **not** a broad benchmark and **not** a production-runtime claim. Detail:
[experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md](experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md).

### exp62 — distributed fanout/fanin (strongest same-axis distributed evidence)

One outer blocking Python call `fanout_fanin(x, N) -> int64` dispatches **N=8** leaf actions
**all-remote**, hard-pinned, and reduces them — both arms at the same Python boundary. The HPX arm uses
`root_flat_gather_poll`; the Ray arm's coordinator (`num_cpus=0`) runs zero leaves and hard-pins leaves
across the remotes. All gates passed; arms reported separately (`speedup_computed=false`,
`ratio_reported=false`, `arms_differenced=false`, `placement_bands_differenced=false`).

Slice 3 — one-remote matched cross-node, R=5 (medusa00→01, N=8, K=1000, W=100, prewarm=1; per-arm µs):

| arm (same Python boundary) | p50 | p90 | p99 | mean |
|---|---|---|---|---|
| Ray coordinator path | ~3640.9 | ~3895.6 | ~6407.0 | ~3718.6 |
| experiment-only Python→HPX `root_flat_gather_poll` | ~345.4 | ~401.7 | ~466.2 | ~359.0 |

Slice 4b — matched ≥2-remote, R=5 (medusa00 root/head/coordinator, medusa01/02 remotes, N=8, K=1000,
W=100, prewarm=1; per-arm µs) — the **headline** same-axis distributed scalar evidence:

| arm (same Python boundary) | p50 | p90 | p99 | mean |
|---|---|---|---|---|
| Ray coordinator, hard-pinned leaves | ~3717.4 | ~3874.4 | ~7012.5 | ~3805.4 |
| experiment-only Python→HPX `root_flat_gather_poll` | ~249.5 | ~270.7 | ~320.2 | ~251.5 |

Closed-`int64` correctness, all leaves remote, placement hard-gated. Still an **experiment-only** HPX
path, **not** production RayX runtime, and `root_flat_gather_poll` is an interim composition, not a final
collective. Detail:
[experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md](experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md).

### exp63 — native HPX composition / progress diagnosis (mechanism evidence, not a Ray comparison)

exp63 resolved the earlier native-composition concern. It is **mechanism evidence only** — no Ray
comparison, no performance numbers, no HPX collectives claim.

- The earlier native-composition failure traced to **connector lifetime**, not an intrinsic HPX
  native-progress failure. The connector-side fault was `HPX(invalid_status): thread pool is not
  running` during parcel scheduling.
- Serve-timeout sweep:

  | serve-timeout | outcome |
  |---|---|
  | 90 s | fault at call 7 |
  | 150 s | fault at call 14 |
  | 300 s | pass |
  | 600 s | pass |

- A hardened **heartbeat / root-completion lifetime** fixed it.
- **Slice 2a** validated native cross-node composition: `when_all_then_reduce` **pass, 20/20**;
  `dataflow_reduce` **pass, 20/20**; `root_flat_gather_poll` is a mechanics control only, **not**
  native-validated.
- **Slice 2b** validated depth-2 star / root-of-partials: `dataflow_reduce` **20/20**,
  `when_all_then_reduce` **20/20**; topology `depth2_star_of_partials_contiguous_blocks`, partials
  `[4, 4]` across the two remote localities.

In plain terms: the HPX-native path is viable once connector lifetime is correct, and root-of-partials
composition works cross-node. This claims **no** Ray performance and **no** HPX `collectives`. Detail:
[experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md](experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md).

### exp64 — payload fanin size sweep (within-arm payload evidence)

exp64 measures **response-payload size** at the same Python boundary, in four slices: **Slice 1** HPX
payload smoke, **Slice 2** Ray matched smoke, **Slice 3** structural R=1 matched ladder, **Slice 4** an
R=5 matched band. Each leaf returns `S` opaque payload bytes plus its closed scalar; Python folds and
checks the payload digest **after** timing, outside the RTT window, identically for both arms.

The **Slice 4 band** (`band20260702_174335`; jobs 159385/159386/159388/159389/159390; R=5 **fresh
exclusive** allocations — the scheduler reused medusa11/12/13 for all islands, so this shows
repeatability under **low placement diversity**, not broad cluster-wide variance) ran the full ladder
`[0,64,1024,16384,262144]` (N=8, prewarm=5, measured=30, HPX phase then Ray phase). All per-island
manifests passed and the band aggregate passed:

- `overall_band_pass=True`, `evidence_grade=matched_band_r5`
- `same_axis_comparison=True` — **structural flag only**
- `distributional_evidence=True` — **within-arm only**; `percentiles_evidence_ready=True` — **p50/p90
  only**; `p99_evidence_ready=False`
- `distributional_payload_ladder_ready=False`, blocked by
  `hpx_serialization_runtime_path_not_observed` and `hpx_poll_gather_baseline`
- `no_cross_arm_timing_computed=True`; no ratios, no speedups, no winner

The per-arm tables below are **within-arm observations only** (across-island median of the per-island
p50/p90). **Do not compare the two tables**, do not compute ratios, and do not read a winner — the arms
take intentionally different runtime paths and the only cross-arm anchor is the closed digest.

HPX (`root_flat_gather_poll` poll-gather payload baseline):

| S (bytes) | p50 (ms) | p90 (ms) |
|---|---|---|
| 0 | 0.299 | 0.335 |
| 64 | 0.290 | 0.317 |
| 1024 | 0.326 | 0.388 |
| 16384 | 1.453 | 1.516 |
| 262144 | 19.518 | 20.552 |

Ray (coordinator + Ray object transport):

| S (bytes) | p50 (ms) | p90 (ms) |
|---|---|---|
| 0 | 4.225 | 4.821 |
| 64 | 4.191 | 4.580 |
| 1024 | 4.392 | 4.667 |
| 16384 | 6.850 | 7.153 |
| 262144 | 55.378 | 57.370 |

HPX here remains the `root_flat_gather_poll` poll-gather baseline — **not** the exp63 native-composition
payload path yet. The HPX serialization **runtime** path is **not observed** (config-level flags are, the
per-call zero-copy path taken is not), which is exactly what blocks a stronger distributional
payload-ladder grade; the Ray object/plasma return path is **not observed** either.

**Native readiness diagnosis (Phase A→A4 — complete).** An HPX-only diagnostic arc asked whether native
readiness composition could replace the poll baseline for the payload path. Result: native
`when_all`/`dataflow` continuations entered and completed promptly, but the suspended timed waiter resumed
only at the dispatch timeout (`waiter_resume_at_timeout`). The signature was unchanged by
root/background-thread tuning, disabled idle backoff, and TCP parcel-pool sizes 2 (observed default), 4,
and 8, while the polling/yield controls stayed prompt throughout. Consequence at the time: the polling
baseline was **not retired** on HPX 1.11, the native payload-size ladder was **not started**, and
`distributional_payload_ladder` stays blocked. This is a scoped HPX 1.11 / TCP-parcelport / Rostam
progress diagnostic — the timeout-bound values are diagnostic signatures, not latency measurements — not
a performance result and not a general HPX claim.

**Waiter-fix verification (complete).** Exp64 isolated a suspended timed-wait wakeup defect on HPX 1.11.
The upstream discussion confirmed it as an HPX bug, fixed upstream by PR #7367. The identical
discriminator was rerun against HPX master commit `20bc3d4b`
(`20bc3d4bf3068383edcb63be13f22e9ff95842fa`, PR #7367 ancestry proven), and the suspended waiter resumed
on readiness across four runs and two thread configurations (`waiter_resumed_on_ready`), after the HPX
1.11 control first reproduced `waiter_resume_at_timeout`; a separate exp65 loopback re-check corroborates
independently. On that exact fixed build the polling workaround is no longer required for readiness;
polling remains the historical HPX 1.11 evidence path and a diagnostic control. New distributed
experiments use the verified fixed build unless a historical-control run explicitly requires HPX 1.11;
older evidence stays scoped to the versions originally recorded. The HPX serialization **runtime** path
is still not observed, so `distributional_payload_ladder` remains blocked and no native payload ladder
has been run. Curated aggregate:
`experiments/64_payload_fanin_size_sweep/waiter_fix_verification_aggregate.json`. Detail:
[experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md](experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md).

### exp65 — demand-triggered connect-mode admission (mechanism evidence, complete)

exp65 is complete in two slices, passing **3/3 on both arms in each**. In the demand arm the connect-mode
root starts **alone**, performs local HPX work before any connector exists, admits one connector only
**after** an external demand event, discovers it by membership set-difference **without a predetermined
connector count**, executes a verified remote action, observes the connector's graceful leave, continues
local work, and finalizes cleanly; the no-demand control root finalizes cleanly with zero connectors ever
joining.

- **Loopback slice** — single-node macOS loopback, HPX 1.11, plain-Python controller: 3/3 demand and
  3/3 no-demand.
- **Rostam cross-node slice** (Slurm job 170014) — root and controller on medusa00; the connector is
  created only after the demand event, on medusa01; TCP parcelport over `10.42.5.x`; no predetermined
  connector count; set-difference discovery; verified remote action on locality 1; graceful leave; root
  continued and finalized: 3/3 demand and 3/3 no-demand, all structural/placement gates passed.

The safe claim: demand-ordered connect-mode admission is demonstrated on loopback and across two real
nodes, within a boot-time `--hpx:expect-connecting-localities` willingness. Still open: HPX inside Ray
actors, elasticity during in-flight work, concurrent churn, failure recovery, lazy TCP parcelport
connection establishment, anything beyond two nodes, and all performance claims. Detail:
[experiments/65_demand_admission/demand_triggered_admission.md](experiments/65_demand_admission/demand_triggered_admission.md).

### exp66 — HPX networking runtime inside one Ray actor worker (in-worker hosting, complete)

exp66 answers the pivotal question the prior distributed arc could not: can a **networking HPX
connect-mode locality run in-process inside a Ray actor worker** — not a child process it launches? One
Ray actor imports an experiment-only pybind extension and calls `hpx::start` in `runtime_mode::connect`
on background threads of its **own** process; a separately supervised, work-free HPX root runs elsewhere,
and an external HPX prober dispatches the closed-`int64` `pid`/`probe` actions at the actor locality. The
decisive proof is by value: the HPX `pid` action executed **on the actor locality** returns exactly the
Ray actor worker's own PID, and the actor owns **zero** HPX child processes — so the runtime is genuinely
in-process, not a spawned child. Ray carries only bootstrap/lifecycle metadata; the operation and its
result travel the HPX action path.

Interpreter-aware slices: **A** (in-process hosting + full lifecycle) and **B** (HPX progress while the
actor's Python thread is idle) are gating; **C** (actor-thread CPU/GIL saturation) is a non-gating
diagnostic; **D** (Python 3.14 / free-threaded rerun) is deferred and cleanly skipped.

- **Local slice** — CPython 3.11.15 (GIL build), Ray 2.55.1, HPX `20bc3d4b`, macOS loopback: **3/3**. PID
  identity, no HPX child, verified remote action + oracle on the actor locality, idle progress, graceful
  in-process shutdown, actor destruction and recreation, no orphans. Slice C `progressed_under_actor_saturation` ×3.
- **Rostam cross-node slice** (Slurm job **170524**) — CPython 3.12.3 (GIL build), Ray 2.55.1, same fixed
  HPX build, TCP parcelport over `10.42.5.x`: **3/3**. The Ray actor was hard-placed
  (`NodeAffinity(soft=False)`) on **medusa01** while the work-free root and prober stayed on **medusa00**;
  every rep showed the HPX `pid` action returning the actor worker PID across nodes, oracle-correct
  execution on the actor locality, idle progress, graceful leave, root completion, actor recreation on the
  actor node, and clean orphan checks on both nodes. Slice C progressed ×3.

Safe claim: a networking HPX connect-mode locality can run in-process inside a Ray actor worker, proven by
exact PID identity and the absence of HPX child processes, locally and across two Rostam nodes; Ray owned
placement and lifecycle while HPX actions carried the distributed work, with a separately supervised
work-free root. **Not claimed:** two actors sharing one runtime (that is exp67), Python 3.14,
free-threaded Python, elasticity or individual-locality failure recovery, and any performance or GIL
verdict; and the HPX serialization/runtime data path is **not directly observed** — execution is proven by
the closed-`int64` value oracle, not by instrumenting the wire/serialization path. exp66 closes the
one-actor in-process-hosting gate and is the prerequisite for exp67. Detail: the experiment package
[experiments/66_hpx_runtime_inside_ray_actor/](experiments/66_hpx_runtime_inside_ray_actor/) with curated
[hpx_inside_ray_actor_aggregate.json](experiments/66_hpx_runtime_inside_ray_actor/hpx_inside_ray_actor_aggregate.json)
and [hpx_inside_ray_actor_crossnode_aggregate.json](experiments/66_hpx_runtime_inside_ray_actor/hpx_inside_ray_actor_crossnode_aggregate.json).

### exp67 — two Ray actors sharing one HPX runtime (two-actor gate, complete)

exp67 is the load-bearing step exp66 set up: **two distinct Ray actor worker processes, each hosting a
networking HPX connect-mode locality in-process, join one shared HPX runtime under a separately supervised,
work-free root and exchange verified HPX actions in both directions.** For each actor the HPX `pid` action
executed on that actor's locality returns exactly the actor's Ray worker PID, and neither actor owns any HPX
child process. The proof is **bidirectional and actor-to-actor**: A→B proves B's PID, locality, and hostname
plus a closed-`int64` oracle, and B→A proves A's — and because neither actor holds a Ray handle to the other,
the operation path is HPX, not Ray (by construction, not by wire instrumentation).

- **Local slice** — CPython 3.11.15, Ray 2.55.1, HPX `20bc3d4b`, macOS loopback: **3/3**. Distinct PIDs and
  distinct nonzero localities, both childless; bidirectional A↔B actions with exact PID/locality/hostname +
  oracle; work-free root; graceful leave, root finalization, actor recreation, no orphans.
- **Rostam three-node slice** — smoke job **170743** (pass) and accepted job **170744** (**3/3**); CPython
  3.12.3; root/controller on **medusa00**, actor A hard-placed (`NodeAffinitySchedulingStrategy(soft=False)`)
  on **medusa01**, actor B on **medusa11**; **three roles on three distinct nodes**; endpoints pinned to
  `10.42.5.x`; both directions cross nodes; clean lifecycle, actor recreation on the intended nodes, and
  orphan-clean sweeps on all three nodes.

Safe claim: two distinct Ray actor workers, each hosting an HPX connect-mode locality in-process, joined one
shared HPX runtime under a separately supervised work-free root and executed verified HPX actions from one
actor-locality to the other in both directions — locally and across three Rostam nodes. **Not claimed:**
Python 3.14; free-threaded Python; elasticity, churn, or individual-locality recovery; any performance claim;
production API. Curated
[two_ray_actors_shared_hpx_aggregate.json](experiments/67_two_ray_actors_shared_hpx/two_ray_actors_shared_hpx_aggregate.json)
and [two_ray_actors_shared_hpx_crossnode_aggregate.json](experiments/67_two_ray_actors_shared_hpx/two_ray_actors_shared_hpx_crossnode_aggregate.json).

### exp68 — deterministic vocabulary-sharded top-k (first LLM-shaped workload, complete)

exp68 runs a **synthetic, exactly-checkable, LLM-shaped** distributed workload on top of exp67's two-actor
runtime — vocabulary-sharded next-token candidate selection, **not** real inference. Deterministic,
exactly-representable float32 logits (integer grid ÷ 8); two disjoint, complete contiguous vocabulary shards;
one total order (higher logit first, then lower global token id on ties). Each actor computes its shard's
local top-k; a coordinator fetches the peer's candidates over an HPX action and merges through an HPX
future/continuation; the global top-k is checked bit-exactly against an **independent controller oracle**, in
both coordinator directions, over a **seven-case matrix**.

- **Local slice** — CPython 3.11.15, Ray 2.55.1, HPX `20bc3d4b`: **3/3**. Exact local top-k per shard;
  A-coordinated and B-coordinated merges both equal the oracle (token IDs, ordering, and float32 bits);
  HPX action/future/continuation path evidenced; clean lifecycle and actor recreation.
- **Rostam three-node slice** — smoke job **170745** (pass) and accepted job **170746** (**3/3**); CPython
  3.12.3; same medusa00/01/11 topology and hard placement; full seven-case matrix in both directions crossing
  nodes; the serialization discriminator: **204 peer-transferred candidate bit patterns checked (68 per
  repetition), zero mismatches** — bit-exact candidate values survived the cross-node HPX action path.

Safe claim: **two Ray actors on distinct nodes executed a deterministic vocabulary-sharded top-k operation
through HPX actions/futures in both coordinator directions, with exact token-ID, ordering, and float32-bit
agreement against an independent oracle.** **Not claimed:** real LLM inference; model weights, tokenizer, GPU,
or framework integration; Python 3.14 or free-threaded; elasticity, churn, or failure recovery; any latency,
throughput, ratio, speedup, or winner claim; direct internal serialization instrumentation; production API.
Curated
[vocab_sharded_topk_aggregate.json](experiments/68_vocab_sharded_topk/vocab_sharded_topk_aggregate.json)
and [vocab_sharded_topk_crossnode_aggregate.json](experiments/68_vocab_sharded_topk/vocab_sharded_topk_crossnode_aggregate.json).
exp69 (a strict same-axis Ray-mediated vs HPX-mediated **performance** comparison of this exact workload) is
**planned, not yet evidence** — no results and no ratio/speedup/winner claim exist.

## Separate in-process direction (HPX inside one Ray actor)

Distinct from the distributed same-axis / payload arc above, the **in-process**
direction (HPX running inside one long-lived Ray actor) has **three validated,
bounded properties**, all observation-only and machine-specific:

1. **Control-plane dispatch floor** — in the synthetic no-op benchmark, `rayx`
   stays near the HPX-native control-plane floor rather than collapsing toward
   Ray's actor-process boundary (benchmark 06).
2. **Ray-hosted native scaling inside one actor** — inside one long-lived Ray
   actor, native HPX-backed Async CPU work scales cleanly through **W=16** on
   homogeneous Linux while a pure-Python in-process CPU loop stays flat (exp32 on
   Rostam).
3. **Adapter design preserves (or hides) HPX cooperative behavior** — a *blocking*
   per-lane retirement can hide HPX cooperative suspension under a synthetic
   parked+compute mix, while a *non-blocking* op-lane with sufficient in-flight
   admission restores it; the exp35→exp38 arc localized this to per-lane
   head-of-line and an undersized in-flight cap, not a retirement-path failure.

Together these validate an **intra-process runtime mechanism** and show that
**adapter design matters** inside the Ray boundary. They are **not** Ray cluster
scaling, **not** “RayX makes Ray faster”, **not** “HPX beats Ray”, and **not**
sizing/capacity guidance. Per-experiment detail and numbers:
[docs/evidence_index.md](docs/evidence_index.md).

## Documentation map

Framing/reference docs live under `docs/` (`docs/reference/` for API notes); result
notes live under `benchmarks/` and `experiments/<name>/`. The three code/evidence
trees are distinct: `bench/` holds runnable harness code (drivers, smokes,
analyzers), `benchmarks/` holds formal benchmark write-ups, and `experiments/`
holds investigative write-ups / curated evidence packages.

### Start here

* [docs/rayx_for_beginners.md](docs/rayx_for_beginners.md) — a from-zero tour: what RayX is and is not, the lane/actor mental model, and how the pieces fit.
* [docs/project_proposal.md](docs/project_proposal.md) — motivation, hypothesis, scope, phases.
* [docs/ray_hpx_mapping.md](docs/ray_hpx_mapping.md) — Ray ↔ HPX conceptual mapping and project direction.
* [docs/experiment_plan.md](docs/experiment_plan.md) — measurement contract, schemas, workload matrix.
* [docs/hpx_build_notes.md](docs/hpx_build_notes.md) — HPX build/install notes.

### Core rayx API and design

* [docs/reference/rayx_actor_api.md](docs/reference/rayx_actor_api.md) — `rayx` Python API (`Engine` / `SyntheticActor`), façade equivalence, and how Ray actor-pool code maps to `Engine(num_lanes=N)`.
* [docs/reference/rayx_frontend_design.md](docs/reference/rayx_frontend_design.md) — frontend design rationale: Future ownership, `wait`/`as_completed`, shutdown drain, cancellation, the `lane_impl` backend seam.
* [docs/reference/rayx_submit_batch.md](docs/reference/rayx_submit_batch.md) — the `submit_batch()` bulk-submit path.
* [docs/reference/chunked_service_synthesis.md](docs/reference/chunked_service_synthesis.md) — chunked-service cross-reading (benchmark 09 + experiment 14).
* [docs/reference/hpxlane_backend_arc.md](docs/reference/hpxlane_backend_arc.md) — the opt-in `HpxLane` backend reading guide.
* `rayx.runtime` design notes live under [docs/design/](docs/design/) (problem model, HPX design principles, registered-operation API, value model, local native actors, the endpoint seam); runnable tour: [examples/rayx_runtime_basic.py](examples/rayx_runtime_basic.py).

### Evidence

* [docs/evidence_index.md](docs/evidence_index.md) — the chronological **“what we learned”** index for every benchmark and experiment arc: main benchmark arc (01–10), frontend/serving-control + `HpxLane` (01–23), `rayx.runtime` / local actors (24–26) and the endpoint seam (42–43), Ray-hosting composition (27–30), runtime/adapter (31–38), in-process HPX composition (39–44), HPX-island lifecycle / Ray-orchestrated bootstrap (49–52), the two-node precursors (58–60), the distributed same-axis / payload-ladder arc (61 scalar, 62 distributed fanout/fanin, 63 native HPX composition, 64 payload ladder), demand-triggered connect-mode admission (65), HPX-in-one-Ray-actor-worker hosting (66), two-Ray-actors-sharing-one-HPX-runtime (67), and the deterministic vocab-sharded top-k workload (68).
* [experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md](experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md) — exp61: the scalar same-axis Python-boundary write-up (experiment-only).
* [experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md](experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md) — exp62: the same-axis Python-boundary **distributed fanout/fanin** write-up (experiment-only; not shipped `rayx.runtime` API).
* [experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md](experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md) — exp63: HPX-native composition / progress diagnosis (mechanism evidence only; no Ray comparison, no `hpx::collectives`).
* [experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md](experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md) — exp64: payload fanin size sweep through the Slice 4 `matched_band_r5` within-arm band, the Slice 5 Phase A→A4 native-readiness diagnostic (`waiter_resume_at_timeout` on HPX 1.11), and the Slice 5V waiter-fix verification — the identical discriminator on HPX master `20bc3d4b` (PR #7367) resumed on readiness across four runs and two thread configurations, so polling is no longer the required readiness workaround on that exact build (experiment-only).
* [experiments/65_demand_admission/demand_triggered_admission.md](experiments/65_demand_admission/demand_triggered_admission.md) — exp65: demand-triggered connect-mode admission — root starts alone, admits one connector only on an external demand event, count-free discovery, graceful leave, clean finalize; demonstrated on macOS loopback and reproduced across two Rostam nodes (medusa00/medusa01, TCP `10.42.5.x`, job 170014) (mechanism evidence; experiment-only).
* [experiments/66_hpx_runtime_inside_ray_actor/](experiments/66_hpx_runtime_inside_ray_actor/) — exp66: a networking HPX connect-mode locality running **in-process inside one Ray actor worker**, proven by exact PID identity (no HPX child); local slice (CPython 3.11.15, 3/3) and Rostam cross-node slice (job 170524, medusa00 root/prober → medusa01 actor, TCP `10.42.5.x`, CPython 3.12.3, 3/3); fixed HPX `20bc3d4b`; the prerequisite for exp67 (in-worker hosting evidence; experiment-only). Curated [hpx_inside_ray_actor_aggregate.json](experiments/66_hpx_runtime_inside_ray_actor/hpx_inside_ray_actor_aggregate.json) and [hpx_inside_ray_actor_crossnode_aggregate.json](experiments/66_hpx_runtime_inside_ray_actor/hpx_inside_ray_actor_crossnode_aggregate.json).
* [experiments/67_two_ray_actors_shared_hpx/](experiments/67_two_ray_actors_shared_hpx/) — exp67: two Ray actors sharing **one shared HPX runtime**, with verified **bidirectional actor-to-actor** HPX actions (A→B proves B's PID/locality/hostname + oracle, B→A proves A's); local (CPython 3.11.15, 3/3) and Rostam **three-node** slice (root medusa00, actor A medusa01, actor B medusa11; smoke job 170743, accepted job 170744; CPython 3.12.3, 3/3); fixed HPX `20bc3d4b` (two-actor shared-runtime evidence; experiment-only). Curated [two_ray_actors_shared_hpx_aggregate.json](experiments/67_two_ray_actors_shared_hpx/two_ray_actors_shared_hpx_aggregate.json) and [two_ray_actors_shared_hpx_crossnode_aggregate.json](experiments/67_two_ray_actors_shared_hpx/two_ray_actors_shared_hpx_crossnode_aggregate.json).
* [experiments/68_vocab_sharded_topk/](experiments/68_vocab_sharded_topk/) — exp68: a deterministic **LLM-shaped** vocabulary-sharded top-k (synthetic, not inference) across the two actors, with exact token-ID, ordering, and **float32-bit** agreement vs an independent oracle in both coordinator directions; local (CPython 3.11.15, 3/3) and Rostam three-node slice (medusa00/01/11; smoke job 170745, accepted job 170746; CPython 3.12.3, 3/3), **204 cross-node candidate bit patterns, zero mismatches**; fixed HPX `20bc3d4b` (experiment-only). Curated [vocab_sharded_topk_aggregate.json](experiments/68_vocab_sharded_topk/vocab_sharded_topk_aggregate.json) and [vocab_sharded_topk_crossnode_aggregate.json](experiments/68_vocab_sharded_topk/vocab_sharded_topk_crossnode_aggregate.json).
* Source write-ups live beside the code under [benchmarks/](benchmarks/) and [experiments/](experiments/).

### Project rules

* [CLAUDE.md](CLAUDE.md) — working rules and project guardrails.
