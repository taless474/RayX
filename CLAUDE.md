# RayX Working Rules

## Project identity

RayX is a narrow Ray-vs-HPX comparison harness for synthetic ML serving-control workloads.

The project compares:

* Ray actor baselines using public Ray APIs
* HPX-native synthetic baselines
* `rayx`, a thin Python frontend over HPX service lanes
* experiment-only HPX connect-mode and Ray-orchestrated HPX-island probes

RayX is not a Ray replacement, not Ray Serve, not Ray Train, not a Ray object-store project, and not real model inference.

The benchmark harness remains synthetic. The repo also contains an experimental `rayx.runtime` prototype for fixed registered native operations and local native actors only. It is not arbitrary Python remote execution and not an object store.

Experiment-only HPX connect-mode, Ray-orchestrated HPX-island, Ray actor baseline, and Python-boundary comparison probes are standalone mechanism/evidence experiments. They are not shipped `rayx.runtime` API, not endpoint work, not production code, and not evidence that the public RayX API has gained distributed HPX actions.

Do not clone, modify, or vendor Ray internals unless explicitly requested.

## Session startup

At the beginning of each session, read `handoff.md`.

Do not update or rewrite `handoff.md` unless explicitly asked.

Do not update or rewrite `CLAUDE.md` unless explicitly asked.

Do not stage, commit, amend, or push unless explicitly asked.

Do not suggest staging, committing, amending, or pushing unless explicitly asked.

## Technical guardrails

Keep the benchmark harness synthetic and honest.

Keep `rayx.runtime` narrow and honest: fixed registered native operations, fixed registered native actor methods, no arbitrary Python execution, no object store, and no Ray replacement semantics.

Do not imply:

* Ray replacement semantics
* Ray object-store or task-result semantics
* arbitrary remote Python execution
* real model inference
* Ray Serve behavior
* Ray Train behavior
* a general claim that HPX beats Ray
* a general claim that RayX beats Ray
* a general claim that `rayx` is faster than HPX-native
* a production, fault-tolerance, real-serving, or general-fabric claim

When comparing Ray, HPX-native, and `rayx`, distinguish between:

* Python-first ecosystem value
* distributed execution model
* actor/task boundary overhead
* native C++ runtime integration
* fine-grained async execution
* serving-control behavior
* benchmark-driver artifacts
* measurement plane

Keep benchmark claims tied to measured evidence. Separate measured facts from interpretation.

Avoid unqualified “coordination ceiling” wording. When preserving old measurements, mark superseded interpretations clearly instead of deleting useful provenance.

## Current evidence state, post-exp60

Use these as durable interpretation constraints unless new evidence changes them.

### In-process RayX/runtime arc

The in-process HPX-inside-Ray-actors story remains valid as a separate direction from the distributed-fabric evidence arc.

`rayx.runtime` is a separate experimental subpackage for fixed registered native work over HPX-native FIFO `RuntimeLane`s. Registered native operations currently include `square`, `add`, `boom`, `busy_sum`, `fanout_sum`, `scale_double`, `park_ms`, `chain_sum_loop`, `chain_sum_then`, `barrier_fanin`, and `diamond_fanin` when present in the built registry/write-ups. The closed value model is `int64` / `double`.

Runtime operation-lane ids use `rt-hpx-<16 lowercase hex>`.

`chain_sum_loop` and `chain_sum_then` are synthetic in-process native-composition probes for exp39. They carry no real inference, Ray, multi-node, Python-callback, or HPX-fabric claim.

`barrier_fanin` is the exp44 diagnostic barrier-gated fan-in op. It returns a closed `int64` value and writes an opt-in structural witness side channel only. It carries no speedup, throughput, latency, Ray-comparison, endpoint, fabric, parcelport, AGAS, or multi-node claim.

`diamond_fanin` and related exp45–48 probes are in-process Runtime-boundary / in-substrate-reference characterization only. They carry no Ray actor, endpoint, parcelport, AGAS, distributed locality, multi-node, performance, or fabric claim.

`park_ms(ms)` is synthetic cooperative PARKED work using chunked `hpx::this_thread::sleep_for`, capped at 60 s and cancellable at chunk boundaries. It is the parked-wait analog of the CPU-bound `busy_sum` diagnostic. It is not real I/O, not inference, and carries no performance claim. Park-related tests are structural/stats-gated and never assert wall-clock values.

`rayx.runtime` also includes an experimental local native actor MVP: `Runtime.create_actor("counter", initial)` returns an `ActorHandle`, and `ActorHandle.call("add" / "get" / "reset", ...)` dispatches fixed registered native methods on a native `CounterActor`.

Actor method calls return the existing `RuntimeFuture` / `OperationResult` path and work with `get` / `wait` / `as_completed`.

Actor lane ids use `rt-act-<16 lowercase hex>`.

`ActorHandle.stats()` is a non-consuming, point-in-time, racy per-actor debug snapshot. It reports `{actor_id, queue_depth, active}`. The in-service call is not counted in `queue_depth`. It is for debugging/test-gating only. It is not scheduler state, not placement control, not a synchronization primitive, and not an all-actors enumeration API.

`Runtime.lane_stats()` remains op-lanes-only.

`Runtime.barrier_fanin_witness()` is the exp44 debug-only structural witness accessor. It is a guarded snapshot for single-in-flight tests/experiments. Stale/cross-call reads are possible, but torn reads should not be. It is not scheduler state, not placement control, not a synchronization primitive, not a public scheduler API, and `max_simultaneously_suspended_leaves` means coordinated suspension, not parallelism, throughput, or worker-level concurrency.

`rayx.runtime` still has no `ObjectRef`, no object store, no arbitrary Python callables, no HPX actions/components, no distributed locality, no module-level `rayx.get` / `rayx.wait`, no `.remote()` actor API, no `Runtime.lane_impl`, and no performance claim.

Standalone HPX connect-mode binaries and Python-boundary experiment extensions under `experiments/` do not add HPX actions/components/distributed locality to the shipped `rayx.runtime` Python API.

### HpxLane arc

The HpxLane evidence arc is:

* exp16: native single-lane feasibility/timer behavior
* exp20: task/dataflow pools are not drop-in RayX lane backends
* exp21: RayX backend contract parity
* exp22: load-divergence mechanism, observation-only
* exp23: adapter-hop cost, observation-only

See `docs/reference/hpxlane_backend_arc.md` for the consolidated reading guide.

exp22/exp23 timings, including spin divergence, are observation-only and machine-specific. Do not use them as performance claims. Do not state an “HpxLane is faster/slower than ServiceLane” verdict.

`lane_impl` selects the RayX lane backend:

* `"std"` / `ServiceLane` is the default stable comparison anchor.
* `"hpx"` / `HpxLane` is an opt-in cooperative HPX-thread lane behind the same RayX contract.

The shared RayX contract includes FIFO behavior, `actor_id`, `lane_stats()` queue depth/active visibility, bounded admission/`QueueFullError`, queued cancellation, chunk-boundary running cancellation, `get`, `wait`, and `as_completed`.

Backend choice is visible only through the `actor_id` prefix:

* `act-hpx-` for `ServiceLane`
* `act-hpxl-` for `HpxLane`

No HPX internals are exposed to Python and the v1 JSONL schema is unchanged.

### Distributed HPX island arc

exp49–52 established the first mechanism/bootstrap arc:

* exp49 proved Ray-free HPX connect-mode graceful join, remote action, disconnect, and re-admit.
* exp50 characterized ungraceful non-root locality loss: root can still serve a fresh connector by set-difference targeting, but shutdown is poisoned by stale locality state.
* exp51 showed bounded finalize and local-cache cleanup do not rescue the poisoned root; whole-island external restart is the safe recovery boundary.
* exp52 showed Ray can bootstrap the already-validated clean HPX island as launcher/supervisor while HPX carries the action/data path.

That older arc was mechanism/bootstrap evidence only. It did not license performance, fault-tolerance, multi-node, production, or general-fabric claims.

The later two-node arc extends this with hardware placement and performance characterization:

* exp57: Ray/Slurm-supervised clean two-node HPX island.
* exp58: two-node HPX clean-path performance characterization on medusa00/medusa01 over `10.42.5.x`.
* exp59: Ray actor baseline through Slice 5, complete.
* exp60: HPX same-node two-locality TCP control.

### Current two-node evidence

The post-exp52 distributed evidence arc has advanced beyond single-node mechanism probes:

* exp57/58 established Ray/Slurm-supervised two-node HPX clean-path action experiments.
* exp58 measured the HPX two-node action path as caller-observed C++ `hpx::async(...).get()` RTT, not pure network RTT and not a Python-facing measurement.
* exp59 completed the Ray actor baseline through Slice 5. It provides a Ray Python/ray.get-observed actor path and a plane-labeled juxtaposition against exp58, but not a same-axis Ray-vs-HPX comparison.
* exp60 is the HPX same-node two-locality TCP control. It decomposes HPX same-node vs cross-node cost inside HPX only. It replaces the older placeholder that exp60 would be whole-island failure/restart; failure/restart is now a later experiment.

Durable interpretation:

* exp59 Slice 5 is comparison-gating, not a same-axis performance verdict.
* Ray and HPX numbers from exp59/exp58 may be reported side by side only with measurement-plane labels.
* Within-runtime decompositions are allowed:

  * Ray same-host vs Ray cross-node.
  * HPX same-node TCP vs HPX cross-node TCP.
* The safe shared reading is that, for these QD1 closed-int64 paths, most cost is local stack rather than the physical inter-node hop.
* Do not compute or report Ray-vs-HPX speedups or ratios from exp58/59/60.
* Do not say “HPX beats Ray,” “Ray is slower than HPX,” or “RayX makes Ray faster.”
* Do not use pipeline/QD8/QD32/QD128 rows for Ray-vs-HPX comparison.
* Do not say the value oracle alone proves physical placement. Placement proof requires node ids, hostnames, and hard placement gates; the oracle proves intended closed-int64 execution.

Detailed numbers, node ids, run ids, clocks, warmups, ports, hashes, and per-island bands belong in the experiment write-ups, curated aggregates, or handoff, not here.


## Rostam and Slurm workflow rules

Use the Rostam repo path:

```bash
/work/bitayekrang/RayX
```

Do not use `~/RayX` on Rostam.

Typical Rostam environment:

```bash
cd /work/bitayekrang/RayX

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3

source /work/bitayekrang/venvs/rayx-a2b/bin/activate

export SLURM_EXPORT_ENV=ALL
```

After `salloc`, the shell prompt may still show `rostam1`. That is not itself a failure. Always verify Slurm state:

```bash
echo "$SLURM_JOB_ID"
echo "$SLURM_JOB_NODELIST"
scontrol show hostnames "$SLURM_JOB_NODELIST"
```

Do not proceed with hardware claims if `SLURM_JOB_ID` or `SLURM_JOB_NODELIST` is empty.

Network facts:

* `eno16` maps to `10.42.5.x`
* `ibp94s0` maps to `10.42.6.x`
* Use `--prefer-subnet 10.42.5.` for exp58/59/60 parity unless an experiment explicitly changes it.
* medusa00 is `10.42.5.30`
* medusa01 is `10.42.5.31`

For Ray-on-Slurm experiments:

* Use `ray start --block` under `Popen`/`srun` when Slurm would otherwise reap Ray processes.
* Pin the Ray head and worker to the selected nodes.
* Prefer an orchestrator/inner-driver split when the driver must run on a Ray cluster node rather than only a host that can TCP-connect to GCS.
* Treat Ray node id as authoritative for placement.
* Normalize FQDN and short hostnames before comparing hostnames.
* Use hard `NodeAffinitySchedulingStrategy(soft=False)` for placement proofs.
* Do not say the value oracle alone proves physical placement.
* Record Ray node ids, hostnames, selected-subnet IPs, resources, and no-orphan checks.

For copy-back from Rostam:

* Copy artifacts back to the Mac before asking for analysis.
* Prefer artifact-only pulls that do not clobber local source.
* Pull `_ray_runs/`, `_control_runs/`, and curated `*aggregate*.json` files as needed.
* Quote remote globs in zsh, for example:

```bash
rsync -az \
  'rostam:/work/bitayekrang/RayX/experiments/NN_name/*aggregate*.json' \
  experiments/NN_name/
```

Avoid broad source-directory rsync from Rostam back to Mac unless intentionally syncing source changes.

## Implementation rules

Prefer coherent, reviewable slices.

Keep dependencies minimal.

Use public APIs unless explicitly told otherwise.

Keep benchmark and diagnostic code simple and readable.

Do not hide important timing assumptions.

Do not introduce real model backends such as llama.cpp, vLLM, SGLang, TensorRT-LLM, or OpenVINO unless explicitly requested.

Do not add arbitrary payload execution, object-store behavior, Python callable execution, or Ray-like task execution to shipped `rayx.runtime`.

Experiment-only Python-boundary HPX bindings may be added under `experiments/` only when explicitly scoped for measurement. They must be fenced as non-production, non-public-API probes.

Do not create large generated files.

## HPX-native design discipline

When proposing new RayX features, examples, or experiments, always consider whether there is a more HPX-native design before extending a Ray-shaped pattern.

Keep these stories separate:

* Ray-facing mapping: useful for showing how common Ray actor/future control patterns map onto RayX.
* HPX-facing design: should consider HPX futures, `hpx::async`, continuations, `hpx::dataflow`, executors, resource partitioning, cooperative scheduling, and HPX-thread lane mechanisms where appropriate.
* RayX evidence harness design: remains synthetic infrastructure for isolating adapter/runtime mechanisms with explicit, narrow semantics.
* `rayx.runtime` design: registered native operations and local native actors over HPX-native runtime lanes, still narrow and explicitly not arbitrary Python or object-store semantics.
* Distributed HPX-island experiments: standalone mechanism/performance probes, not public RayX API.
* Same-axis Python-boundary experiments: experiment-only measurement probes, not public RayX API.

Do not present a Ray actor-pool pattern as HPX best-practice guidance. If an example mirrors Ray actor-pool code, label it as a Ray-pattern mapping only.

Before adding Ray-shaped API surface or examples, ask:

* Is this needed for the Ray-hosted HPX-native runtime story or its synthetic evidence harness?
* Is there a clearer HPX-native mechanism or experiment?
* Would this blur RayX into a fake Ray clone?
* Does this preserve the honest boundaries: no object store, no arbitrary remote Python execution, no Ray Serve, no real inference?

Prefer HPX-native mechanism probes or reference notes when the goal is to explain HPX design, and Ray-pattern examples only when the goal is to help Ray users understand the mapping.

## Benchmark and metrics discipline

Benchmark outputs should be machine-readable, usually JSONL or curated JSON aggregates.

Each request row should include enough timing information to compute:

* queue wait time
* service time
* total latency
* throughput
* p50/p90/p99 latency
* status
* request id
* backend name
* actor or worker id when applicable

Use monotonic high-resolution timing.

Keep raw timestamps where useful, but also provide derived millisecond fields for readability.

Avoid benchmark schema changes unless explicitly justified. If a schema change is necessary, document the reason and update analyzer/smoke coverage together.

For native diagnostic output:

* Keep `--diag` opt-in.
* Keep normal JSONL schema and analyzer behavior unchanged when diagnostics are off.
* Keep diagnostic summaries compact and separate from normal per-request JSONL.

For deterministic synthetic workloads:

* Keep Ray and `rayx` Python drivers on shared service-sequence helpers where applicable.
* Do not duplicate or silently drift fixed/bimodal service-sequence logic.
* If the deterministic sequence intentionally changes, update the golden smoke and any native golden check together.

For Ray-vs-HPX or Ray-vs-RayX timing:

* Always label the measurement plane.
* QD1 and pipeline/overlap regimes must be separated.
* Do not compare pipeline/QD8/QD32/QD128 numbers across Ray and HPX unless a specific same-regime experiment is explicitly designed.
* Do not compute speedups or ratios unless the experiment is explicitly same-axis and all same-axis gates pass.
* Record clock, clock overhead, warmup, K/W/R, node pair, selected subnet, placement proof basis, transport, and band construction.
* Use per-island primary percentiles plus across-island median/spread unless an experiment explicitly chooses another statistical construction.

## Documentation rules

Use `readme.md` for the project overview, `rayx` frontend at-a-glance, Quickstart, current evidence summary, and documentation map.

Avoid future-looking roadmap, TODO, or “next step” language in `readme.md` unless explicitly requested.

The README documentation map should let a reader understand the project arc. Each benchmark or experiment bullet should include a clear “what we learned” sentence.

Use:

* `docs/project_proposal.md` for longer motivation, hypothesis, scope, and phases.
* `docs/experiment_plan.md` for benchmark shapes, metrics, commands, schemas, and acceptance gates.
* `docs/ray_hpx_mapping.md` for conceptual mapping between Ray and HPX.
* `docs/reference/` for API and design reference material.
* `docs/design/` for exploratory runtime design notes.
* `benchmarks/` for benchmark write-ups.
* `experiments/` for investigative write-ups and curated evidence packages.
* `examples/` for small runnable API examples.

Keep persistent docs stable. Do not put one-time prompts, branch minutiae, current git status, raw run paths, temporary implementation notes, or current slice summaries into long-lived docs.

## Artifact rules

Do not track raw generated artifacts:

* `.jsonl`
* `.summary.json`
* logs, including `experiments/**/logs/` and `benchmarks/**/logs/`
* broad raw `results/` contents
* build outputs
* `_rayx*.so`
* experiment-built `.so` extension outputs
* `.pyc`
* `__pycache__`
* `.o`, `.dylib`, or other build products

Curated aggregate JSON files beside benchmark or experiment reports are allowed.

Small curated diagnostic evidence packages are allowed only when intentionally part of an experiment package.

Raw JSONL and scratch outputs should stay under `results/` or experiment-local ignored run directories and remain ignored.

## CI and validation rules

CI should protect deterministic repository integrity. It should not reproduce benchmark evidence.

Good CI checks include:

* Python syntax checks for scripts and runners
* local markdown link checks
* local HTML image source checks
* artifact hygiene checks
* curated aggregate ignore/trackability checks
* schema/golden contract checks when deterministic and cheap
* pure unit tests that do not require `_rayx` or HPX

Do not add full benchmark or experiment matrices to normal CI.

Do not add machine-sensitive performance checks to normal CI.

Do not require an HPX source build in normal CI unless explicitly requested.

For the runtime test split:

* repo-sanity should run `py_compile` and pure `tests/unit` only. It must not require `_rayx` or HPX.
* the native RayX smoke job may run `bench/smoke_rayx_runtime.py`, `examples/rayx_runtime_basic.py`, and `tests/integration` after `_rayx` is built.
* runtime unit tests include import-light operation and actor validation.
* runtime integration tests include native runtime and local actor contract coverage.
* do not run runtime integration tests or runtime smokes in repo-sanity.
* Ray-hosting and Ray-orchestrated HPX-island smoke checks may live only in a native/Ray/HPX-capable smoke tier and must skip cleanly when Ray, the built `_rayx` extension, or the required standalone HPX experiment binary is unavailable.
* Do not run Ray-hosting performance drivers or observation probes, including exp35/36/37/38 and exp57–61 hardware runners, in normal CI.

Use `bench/smoke_local.py` as the local validation aggregator when appropriate. It should remain a smoke/golden/contract helper, not a benchmark matrix.

## Reporting expectations

Every slice should end with a clear report:

* files changed
* concise diff summary
* commands run
* pass/fail result
* output files, if any
* current known facts
* validation performed
* remaining caveats
* final `git status --short`

For every completed benchmark, diagnostic, or experiment report, include a short interpretation and roadmap-impact section:

* Experiment interpretation: what passed structurally, what the measured result suggests, what hypothesis it supports or weakens, what remains ambiguous, and what should not be claimed.

* Roadmap impact: classify the result as one of `No roadmap change`, `Roadmap strengthened`, `Roadmap narrowed`, `Roadmap changed`, or `Roadmap blocked`, with a short reason.

* Updated roadmap: keep directions separated.

  * In-process HPX-inside-Ray-actors direction: local scheduling, nonblocking lanes, native continuation/composition, Python-boundary characterization, serving-shaped synthetic workloads, and concurrency/overlap.
  * Future distributed-fabric direction: Ray as placement/bootstrap/lifecycle supervision, HPX locality-to-locality action/data-plane probes, whole-island restart policy, Ray-orchestrated HPX bootstrap, and eventual multi-node comparison only when justified.
  * Same-axis Python-boundary comparison direction: Python-observed Ray actor path vs Python-observed experiment-only HPX/RayX action path. This begins with exp61 and must not mutate shipped `rayx.runtime`.

* Next recommended step: end with one concrete technical next step, not a vague list.

Do not let the future distributed-fabric direction pull the in-process direction forward prematurely.

Do not let same-axis experiment-only Python bindings imply public RayX distributed API.

## Style

Be precise and skeptical.

Prefer concrete claims over broad claims.

When discussing Ray and HPX together, distinguish clearly between:

* Python-first ecosystem value
* distributed execution model
* actor/task overhead
* C++ runtime integration
* fine-grained async execution
* serving-control behavior
* benchmark-driver artifacts
* measurement-plane asymmetry

Do not claim HPX beats Ray or that RayX makes Ray generally faster.

Do not describe synthetic service timing as real inference work.
