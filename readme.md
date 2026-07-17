# RayX

<p align="center">
  <img src="docs/figures/logo.png" alt="RayX logo" width="280">
</p>

**RayX** explores a hybrid distributed-runtime architecture in which **Ray** manages node
placement, actor lifecycle, supervision, and the island-level restart boundary, while **HPX** provides
native C++ actions, futures, and cross-locality composition running **inside Ray-hosted
worker processes**. Instead of running two distributed systems side by side, RayX moves the
integration boundary into the Ray actor process itself: a Ray actor *is* an HPX locality.

RayX is a **research prototype**. Its workloads are synthetic, its topologies are small,
and every claim below is scoped to the experiment that produced it.

**Naming:** **RayX** = this repository (the hybrid-runtime exploration plus its synthetic
evidence harness); **`rayx`** = the Python package (a thin `Engine` frontend over HPX
service lanes, and the experimental `rayx.runtime` of fixed registered native operations
and local native actors).

## The problem

Ray is very good at cluster orchestration: scheduling, actor lifecycle, placement,
fault-domain management, and Python-ecosystem integration. But when an application's inner
loop is a fine-grained or deeply composed **native** distributed operation — many small
cross-node calls, native fan-in/fan-out, continuation-composed reductions — each hop may
pay Python-boundary, serialization, and orchestration costs that the operation itself does
not need.

HPX approaches the same space from the opposite side: a C++ runtime with lightweight
threads, distributed actions, and future-based composition — but without Ray's
actor-management, cluster-supervision, and Python-first operational model.

RayX investigates whether the two can be **combined rather than chosen between**: Ray keeps
the responsibilities it is best at (placement, lifecycle, supervision, restart policy,
Python-facing API), and selected Ray actors internally join a shared HPX runtime that
carries the native distributed operation path. This is not a claim that Ray is deficient —
the entire design depends on Ray doing its half well.

## Architecture

```text
        Ray controller / driver (Python)
                    │  placement, lifecycle, supervision
        ┌───────────┴───────────┐
   Ray actor A             Ray actor B
   ── in-process ──        ── in-process ──
   HPX locality A  ◄─────► HPX locality B
              HPX actions / futures /
              continuations (TCP parcelport)

   Work-free, separately supervised HPX root locality
   (island anchor; runs no application work)
```

**Ray owns:** node placement (hard `NodeAffinity` in the experiments), actor creation and
destruction, process supervision and health, the intended whole-island restart policy, and the
Python-facing API.

**HPX owns:** the in-process native runtime (started with `hpx::start` in connect mode on
background threads of the actor worker — no child process), locality identity, TCP parcel
transport, registered C++ actions, futures and continuations, and native distributed
composition between actor-hosted localities.

In the HPX-native topology demonstrated by exp67 and exp68, the actors hold **no Ray
handles to each other**: the controller holds the Ray actor handles, while actor-to-actor
application work travels through the HPX parcelport. Exp69 later introduced a deliberately
matched Ray-mediated comparison arm alongside this HPX-mediated path.

The repository also contains a separate, earlier **local** direction: `rayx.Engine`
(synthetic FIFO service-lane harness) and the experimental `rayx.runtime` (fixed registered
native operations and local native actors over HPX runtime lanes in one process). These are
narrow by design — no object store, no arbitrary remote Python execution, no Ray
replacement semantics — and are documented in the reference/design docs below.

## What is implemented

The distributed evidence arc, one experiment per gate. Each row links to a durable report
containing the full methodology, gates, accepted job IDs, and artifact hashes.

| Exp | Question | Accepted conclusion | Report |
|---|---|---|---|
| 61 | Can Ray and Python→HPX calls be measured at one identical Python caller boundary? | Yes — matched R=5 cross-node and same-node QD1 bands, per-arm only | [exp61](experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md) |
| 62 | Does the same-axis method extend to distributed fanout/fan-in? | Yes — matched R=5 three-node N=8 all-remote band, per-arm only | [exp62](experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md) |
| 63 | Is HPX-native cross-node composition viable? | Yes — the earlier stall was a connector-lifetime race, not an HPX progress defect; native waits validated after hardening | [exp63](experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md) |
| 64 | How does response-payload size behave per runtime, and can a native passive wait serve it? | Matched R=5 payload ladder band (within-arm distributions); an HPX timed-wait wakeup bug isolated, upstream-confirmed, and verified fixed | [exp64](experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md) |
| 65 | Can an HPX island admit a locality on demand rather than assembling up front? | Yes — demand-ordered admission, loopback and two-node, count-free discovery, clean lifecycle | [exp65](experiments/65_demand_admission/demand_triggered_admission.md) |
| 66 | Can one Ray actor worker host a networking HPX locality **in-process**? | Yes — exact PID identity, zero HPX child processes, clean lifecycle, locally and cross-node | [exp66](experiments/66_hpx_runtime_inside_ray_actor/hpx_runtime_inside_ray_actor.md) |
| 67 | Can two Ray actors join one shared HPX runtime and act on each other? | Yes — bidirectional actor-to-actor HPX actions across three nodes, work-free root, clean lifecycle | [exp67](experiments/67_two_ray_actors_shared_hpx/two_ray_actors_shared_hpx.md) |
| 68 | Can that topology run a useful, exactly checkable distributed operation? | Yes — deterministic vocabulary-sharded top-k, bit-exact against an independent oracle in both directions | [exp68](experiments/68_vocab_sharded_topk/vocab_sharded_topk.md) |
| 69 | How do Ray-mediated and HPX-mediated orchestration of the identical workload compare under matched, verified conditions? | Completed (Slices 0–3): QD1 latency, bounded-concurrency goodput, and a causal resource decomposition resolving the one observed reversal | [exp69](experiments/69_same_axis_topk_perf/same_axis_topk_perf.md) |
| 70 | Locality supervision: what happens on dispatch to a departed locality, and how should lifetime be supervised? | **In progress** — a minimal upstream reproducer for the connector-lifetime gap is built; not accepted evidence | [exp70 reproducer](experiments/70_hpx_locality_supervision/upstream_reproducer/README.md) |

Everything distributed above is **experiment-only** evidence code under `experiments/` —
none of it is shipped `rayx` API, and the local `rayx.runtime` prototype has gained no
distributed actions.

## Key findings

1. **HPX can run networking-capable localities directly inside Ray actor worker
   processes.** Proven by exact PID identity (an HPX action executed on the actor locality
   returns the actor worker's own PID) and empty child-process scans — no child or sidecar
   process model is needed (exp66).
2. **Multiple Ray actors can participate in one shared HPX runtime across nodes**, with
   membership, identity, and lifecycle verified per repetition (exp67).
3. **Ray can retain placement, lifecycle, and supervision while actor-to-actor distributed
   work runs entirely through HPX actions** — by construction, the actors hold no mutual
   Ray handle on the application path (exp67, exp68).
4. **Exact deterministic distributed computation has been demonstrated with independent
   bit-level verification**: token IDs, ordering, and float32 bit patterns of a
   vocabulary-sharded top-k match an independent oracle exactly, in both coordinator
   directions, across nodes (exp68).
5. **Same-boundary measurements show the performance outcome depends on workload and
   resource regime — there is no universal winner.** Payload size, concurrency level, and
   execution-thread supply each changed which arm looked better or whether they converged
   (exp61–64, exp69).
6. **An apparent orchestration-path reversal was actually a resource artifact.** exp69's
   C=4 reversal reproduced under the original two-worker HPX configuration and disappeared
   when HPX worker supply was matched to the concurrency level — classified as a
   thread-supply resource asymmetry with no implementation defect (exp69 Slice 3).
7. **Working with this stack surfaced and verified an upstream HPX fix.** exp64 isolated a
   suspended timed-wait wakeup defect (`future::wait_for` resuming only at its timeout);
   upstream confirmed it as a bug fixed by HPX PR #7367, and the identical discriminator
   verified `waiter_resumed_on_ready` on the fixed commit — the build now used by
   exp66–69.
8. **Lifecycle contracts are load-bearing.** A connector serve-window race masqueraded as
   a native-composition progress failure until an explicit heartbeat/completion lifetime
   protocol was introduced (exp63); demand-ordered late admission works on the tested clean
   path, while graceful disconnect under load, non-root failure eviction, and root-loss
   recovery remain open limitations (exp65, exp70 direction).

## Selected performance evidence

Three scoped examples from exp69 — the only experiment licensed to compare the two
orchestration paths, and only for its exact workload, topology, and gates. Both arms run
inside the **same** Ray-hosted, HPX-resident actor topology (this is not standalone Ray vs
standalone HPX); every timed sample is verified bit-exactly after the timing boundary.
Full context: [exp69 report](experiments/69_same_axis_topk_perf/same_axis_topk_perf.md).

**QD1 latency, payload-oriented case (per-arm distributions).** For the P2 case
(V=200,000, split 100,000, k=10,000 — the larger peer payload), single-in-flight
caller-observed latency over 500 timed samples per rep, R=3, all samples exact: Ray-mediated
arm p50 ≈ 57.5 ms; HPX-mediated arm p50 ≈ 34.9 ms (medians across reps). These are per-arm
distributions; the accepted QD1 aggregates report the arms separately, and P1
(local-compute-dominated) showed the arms nearly indistinguishable — the gap is
case-dependent, not universal.

**Bounded-concurrency P2/C=2 (licensed scoped ratios).** For the identical P2 workload,
A-coordinator, three-node topology, C=2 in-flight requests, `num_cpus=2`, `hpx_threads=2`,
N=1000 verified completions per batch, R=3, caller-observed boundary: HPX-mediated /
Ray-mediated **verified-completion goodput ≈ 1.28** (repetition range 1.27–1.29), and
Ray-mediated / HPX-mediated **p50 latency-under-load ≈ 1.25** (range 1.25–1.26). These are
scoped ratios from exp69's gated review — they license no general speedup claim and no
statement outside this exact workload and configuration.

**Matched-resource causal result (no ratio).** At C=4 with the two-worker HPX
configuration (`cpu2/ht2`), the HPX arm was worker-constrained: its peer-action concurrency
pinned at 2 and its composite dispatch-to-continuation interval rose, and its goodput
distribution sat below the Ray arm's. Raising the band to `cpu4/ht4` raised observed peer
concurrency, dropped the composite interval to at or below the C=2 baseline, and the two
per-arm goodput distributions **converged to the same approximate region**. No ratio or
winner is computed for this slice; the finding is causal (thread supply), not comparative.

## Why this may matter

Design potential, explicitly not proven product outcomes:

- **Keep Ray's ecosystem and operational model while adding native distributed
  composition in selected actors** — applications would not have to leave Ray to get
  native cross-node operation paths.
- **Reduce unnecessary Python/object-boundary crossings** for operations that are natively
  composable (fan-in, reductions, peer exchanges), where the experiments show the boundary
  and orchestration costs are measurable.
- **Explicit fault domains:** Ray supervises an HPX "island" as a unit — placement,
  health, teardown, and whole-island restart — rather than distributed state being
  implicit.
- **A path for C++ framework and runtime developers** to expose native distributed
  operations through Ray actors without adopting a child-process or sidecar model.
- **Runtime interoperability as a research direction** — combining orchestration-first and
  execution-first runtimes rather than forcing an exclusive choice.

## Current limitations

- **Research prototype.** No production packaging, security model, multi-tenancy, or
  autoscaling integration.
- **Synthetic CPU workloads only.** The top-k workload is LLM-*shaped*, not real
  inference: no model weights, no tokenizer, no GPU path.
- **Small fixed topologies.** At most three nodes and two actor-hosted localities plus a
  root; no scale evidence.
- **Clean-path lifecycle.** No transparent fine-grained failure recovery inside an HPX
  island; whole-island restart is the failure policy; HPX root failure is not tolerated
  (exp50/51 established that a poisoned root requires external island restart).
- **Late admission exists, but elasticity does not.** One demand-ordered join on the clean
  path is proven; graceful disconnect under in-flight work, non-root failure eviction, and
  membership churn are open (exp70 is working this area, including an upstream
  reproducer for dispatch-to-departed-locality behavior).
- **Performance findings are workload- and resource-specific.** The licensed ratios cover
  one workload case at one concurrency on one cluster; other cases showed parity,
  case-dependent gaps, or resource-supply artifacts.
- **Free-threaded Python and Python 3.14 are untested** (explicitly deferred slices).

## Roadmap

1. **Locality supervision (exp70, active):** heartbeat/failure detection for actor-hosted
   localities, characterization of dispatch to departed localities, and upstream
   engagement on connector-lifetime behavior.
2. **Root isolation and supervised whole-island restart** as an explicit, tested policy
   rather than an assumption.
3. **Matched-resource comparison discipline** carried into any future band (the exp69
   guard: for C ≥ 4, match worker supply or label the band oversubscribed).
4. **Production-shaped native workloads** beyond closed-value probes, with the same
   exact-verification discipline.
5. **Model-serving integration exploration** (e.g., how an island would sit behind a
   serving layer) — exploratory only.
6. **GPU-aware or accelerator-local native operations** — currently absent.
7. **Packaging and repeatable deployment** of the island bootstrap.
8. **Upstream HPX improvements where generally useful** (the waiter-fix verification and
   the exp70 reproducer are the model).

## Collaboration

RayX welcomes collaboration with industry and research groups working on distributed
runtimes, actor systems, native execution, model serving, and fault-tolerant
orchestration. Useful forms of collaboration include technical review, production-shaped
workload design, engineering mentorship, compute support, internships, and sponsored
student research.

The most valuable next step would be to identify a real Ray or Ray Serve workload whose
distributed native C++ subgraph could execute as an HPX island under Ray supervision, then
evaluate it with the same correctness, lifecycle, and matched-resource discipline used in
the experiments above.

If any of the evidence above is relevant to your work — or looks wrong — issues and
critique are welcome; every accepted claim links to a durable report with its gates,
job IDs, and artifact hashes.

## Quickstart

The local harness runs without any cluster. Each benchmark driver writes per-request
JSONL; `bench/analyze_jsonl.py` rolls it up.

```bash
# Ray-only baseline (no HPX toolchain needed)
pip install -r requirements.txt   # ray
python bench/run_ray_baseline.py --service-ms 0 --concurrency 1 \
    --requests 1000 --out results/ray_noop_c1.jsonl
python bench/analyze_jsonl.py results/ray_noop_c1.jsonl
```

The `rayx` frontend and HPX-native baseline require HPX built from source, then the local
pybind11 `_rayx` extension built against that prefix — see
[docs/hpx_build_notes.md](docs/hpx_build_notes.md). Once built:

```bash
python examples/rayx_runtime_basic.py     # smallest native-runtime tour
python bench/smoke_local.py               # local smoke/golden gates (skips unavailable tiers)
```

Other runnable API tours live under [examples/](examples/). The distributed experiments
are **not** part of the quickstart: they need multi-node Slurm allocations, pinned
subnets, and experiment-local builds, and they are evidence code, not API.

## Reproducibility

- Every experiment's durable report (linked in the table above) records its accepted
  Slurm job IDs, node topology, software commits, curated-aggregate filenames and hashes,
  and raw-artifact inventory.
- [docs/evidence_index.md](docs/evidence_index.md) is the chronological "what we learned"
  index across every benchmark and experiment arc, with operational identifiers.
- Curated aggregates are tracked JSON beside each report; raw runs, logs, and build
  outputs stay gitignored under experiment-local `_exp*_runs/` directories.
- Build instructions: [docs/hpx_build_notes.md](docs/hpx_build_notes.md) (HPX),
  `python/` (the `_rayx` extension), and per-experiment `CMakeLists.txt` files.
- Software stack for the accepted distributed arc: Ray 2.55.1; HPX at the verified
  waiter-fix commit `20bc3d4b…` (exp66–69) or HPX 1.11 where historically recorded;
  CPython 3.11/3.12 GIL builds.

## Status

- **Complete (accepted evidence):** the local benchmark/frontend arc (benchmarks 01–10,
  experiments 01–44 where applicable), the island lifecycle arc (exp49–52, 57–60), and the
  distributed arc exp61–68, plus **exp69 Slices 0–3** (QD1 latency, bounded-concurrency
  goodput, causal resource decomposition).
- **Active:** exp70 — locality supervision and departed-locality behavior, including a
  self-contained upstream reproducer; not yet accepted evidence.
- **Proposed:** the roadmap items above beyond exp70.
- **Maturity:** research prototype throughout. Local `rayx` / `rayx.runtime` code paths
  are real and tested; everything distributed is experiment-only evidence code with
  explicit claim fences.

## What this project is not

- Not a Ray replacement and not a fork of Ray; no Ray internals are modified.
- Not Ray Serve, Ray Train, or a Ray object-store project; no `ObjectRef`, no arbitrary
  remote Python execution.
- Not real model inference — every workload is synthetic and deterministic.
- Not a general claim that HPX outperforms Ray or vice versa; every comparison is scoped,
  gated, and fenced in its experiment report.

## Documentation map

Framing/reference docs live under `docs/` (`docs/reference/` for API notes); result notes
live under `benchmarks/` and `experiments/<name>/`. `bench/` holds runnable harness code;
`benchmarks/` holds formal benchmark write-ups; `experiments/` holds investigative
write-ups and curated evidence packages.

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

* [docs/evidence_index.md](docs/evidence_index.md) — the chronological **"what we learned"** index for every benchmark and experiment arc, including the distributed arc (exp61–69) and its accepted job IDs.
* The durable experiment reports linked in the [What is implemented](#what-is-implemented) table above.
* Source write-ups live beside the code under [benchmarks/](benchmarks/) and [experiments/](experiments/).

### Project rules

* [CLAUDE.md](CLAUDE.md) — working rules and project guardrails.
