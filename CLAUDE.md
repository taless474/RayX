# RayX Working Rules

## Project identity

RayX is a narrow Ray-vs-HPX comparison harness for synthetic ML serving-control workloads.

The project compares:

* Ray actor baselines using public Ray APIs
* HPX-native synthetic baselines
* `rayx`, a thin Python frontend over HPX service lanes

RayX is not a Ray replacement, not Ray Serve, not Ray Train, not a Ray object-store project, and not real model inference.

The benchmark harness remains synthetic. The repo also contains an experimental `rayx.runtime` prototype for fixed registered native operations and local native actors only; it is not arbitrary Python remote execution and not an object store.

Do not clone, modify, or vendor Ray internals unless explicitly requested.

## Session startup

At the beginning of each session, read `handoff.md`.

Do not update or rewrite `handoff.md` unless explicitly asked.

Do not stage, commit, amend, or push unless explicitly asked.

Do not suggest staging, committing, amending, or pushing unless explicitly asked.

## Technical guardrails

Keep the benchmark harness synthetic and honest. Keep `rayx.runtime` narrow and honest: fixed registered native operations, fixed registered native actor methods, no arbitrary Python execution, no object store, and no Ray replacement semantics.

Do not imply:

* Ray replacement semantics
* Ray object-store or task-result semantics
* arbitrary remote Python execution
* real model inference
* Ray Serve behavior
* a general claim that HPX beats Ray
* a general claim that `rayx` is faster than HPX-native

When comparing Ray, HPX-native, and `rayx`, distinguish between:

* Python-first ecosystem value
* distributed execution model
* actor/task boundary overhead
* native C++ runtime integration
* fine-grained async execution
* serving-control behavior
* benchmark-driver artifacts

Keep benchmark claims tied to measured evidence. Separate measured facts from interpretation.

Avoid unqualified “coordination ceiling” wording. When preserving old measurements, mark superseded interpretations clearly instead of deleting useful provenance.

## Current interpretation guardrails

Use these as durable interpretation constraints unless new evidence changes them:

* Python/GIL is not the high-lane bottleneck in the current rayx measurements.
* Sleep-mode bimodal high-lane behavior is refined to closed-loop FIFO-retire / client-driver behavior.
* `work_mode="spin"` is a synthetic CPU-bound diagnostic/calibration mode, not an HPX runtime mode and not the serving design.
* Sleep-mode, spin-mode, HPX cooperative-lane, and task/dataflow-pool results should not be conflated.
* The std::thread `ServiceLane` remains the stable actor-like comparison anchor unless an experiment explicitly opts into another lane mechanism.
* Opt-in HPX-thread/cooperative-lane probes are mechanism experiments, not replacements for the main corpus.
* `lane_impl` selects the rayx lane backend: `"std"` / `ServiceLane` is the default stable comparison anchor; `"hpx"` / `HpxLane` is an opt-in cooperative HPX-thread lane behind the *same* RayX contract (FIFO, `actor_id`, `lane_stats()` queue_depth/active, bounded admission/`QueueFullError`, queued + chunk-boundary running cancellation, get/wait/as_completed). Backend choice is visible only via the `actor_id` prefix (`act-hpx-` vs `act-hpxl-`); no HPX internals are exposed to Python and the v1 JSONL schema is unchanged.
* `rayx.runtime` is a separate experimental subpackage for fixed registered native work over HPX-native FIFO `RuntimeLane`s. Registered native operations currently include `square`, `add`, `boom`, `busy_sum`, `fanout_sum`, `scale_double`, and `park_ms`; the closed value model is `int64` / `double`. Runtime operation-lane ids use `rt-hpx-<16 lowercase hex>`.
* `park_ms(ms)` is synthetic cooperative PARKED work (chunked `hpx::this_thread::sleep_for`, capped at 60 s, cancellable at chunk boundaries) — the parked-wait analog of the CPU-bound `busy_sum` diagnostic. It is not real I/O, not inference, and carries no performance claim; park-related tests are structural/stats-gated and never assert wall-clock values.
* `rayx.runtime` also includes an experimental local native actor MVP: `Runtime.create_actor("counter", initial)` returns an `ActorHandle`, and `ActorHandle.call("add" / "get" / "reset", ...)` dispatches fixed registered native methods on a native `CounterActor`. Actor method calls return the existing `RuntimeFuture` / `OperationResult` path and work with `get` / `wait` / `as_completed`. Actor lane ids use `rt-act-<16 lowercase hex>`.
* `Runtime.release_actor(actor)` is explicit local native actor release: it is not `ray.kill`, not refcounting, and not GC lifecycle. Release cancels queued/pending actor work, drains/joins the actor lane so outstanding futures still reach a terminal/retirable state, and after release `ActorHandle.call()` / `stats()` / double release raise. `Runtime.shutdown()` still tears down remaining runtime-owned native state.
* `ActorHandle.stats()` is a non-consuming, point-in-time, racy per-actor debug snapshot (`{actor_id, queue_depth, active}`; the in-service call is not counted in `queue_depth`) for debugging/test-gating only — not scheduler state, not placement control, not a synchronization primitive, and no counters / `actor_type` field / all-actors enumeration. `Runtime.lane_stats()` remains op-lanes-only.
* `rayx.runtime` still has no `ObjectRef`, no object store, no arbitrary Python callables, no HPX actions/components, no distributed locality, no module-level `rayx.get` / `rayx.wait`, no `.remote()` actor API, no `Runtime.lane_impl`, and no performance claim.
* `rayx.runtime` dispatch is explicitly policy-driven: instantaneous serialized operations/methods (`square`, `add`, `boom`, `scale_double`, counter `add` / `get` / `reset`) are `Inline`; parking, checkpointed CPU, internally composed, busy actor, and unclassified/future work (`busy_sum`, `fanout_sum`, `park_ms`, counter `busy_get`) remain `Async`. `DispatchPolicy` is internal registry metadata, not public API and not inferred from `checkpoint_count` (for example, `park_ms` can have one checkpoint and still must remain `Async`).
* Runtime evidence packages stay observation-only unless explicitly framed otherwise: exp24 demonstrates structural parked-overlap behavior for `park_ms`; exp25 prices the nested `hpx::async(...).get()` dispatch shape with a standalone probe; exp26 exercises create/use/release/shutdown/reinit actor lifecycle at scale. Do not turn these into performance, capacity, or production-sizing claims.
* exp31 is a control-plane-under-load decision gate: HPX-hop control latency inflation under saturated Async work is visible but tiny in absolute terms on the measured machine (worst saturated p99 about 0.10 ms, with hop-free `RuntimeFuture.cancel()` flat). The conclusion is STOP: priorities, named pools, and resource partitioning are not motivated by this evidence. Do not revive that machinery without a real workload showing meaningful millisecond-level control sluggishness.
* exp32 is a narrow intra-actor native CPU scaling probe: inside one long-lived Ray actor, RayX/HPX `busy_sum` Async native work scales across HPX workers while pure Python CPU work in the same process stays GIL-bound. The supported claim is per-leg normalized scaling behavior (`PY` flat, `RAYX` rising through W=4 on the measured machine), not raw C++-vs-CPython speed, not “RayX makes Ray faster,” and not “HPX beats Ray.” Its opt-in decoupling panel cleanly observes the worker-bound half and is conservative/INCONCLUSIVE for the lane-bound half on Apple Silicon; wider W>4/knee sweeps are deferred to homogeneous many-core hardware.
* Ray-hosting experiments 27–30 are composition prototypes: a long-lived Ray actor hosts one in-process RayX `Engine` or `Runtime` because the HPX runtime/process guard is process-global. Ray owns outer actor placement/lifecycle; RayX owns local HPX-backed execution; RayX futures are retired inside the Ray actor and only plain rows/results cross the Ray boundary. Exp30 extends the ladder to a Ray-hosted `Runtime` plus local native `CounterActor` with state, release, and shutdown checks. This is not a Ray backend, fallback layer, object-store integration, Ray Serve behavior, or Ray compatibility API.
* In the Ray-hosting arc, exp27 is Engine boundary decomposition (observation-only), exp28 is Runtime fixed-op composition smoke-only (no JSONL/timing/perf), exp29 is a multi-actor resource-budget diagnostic (observation-only), and exp30 is Runtime + stateful local native actor composition smoke-only. For exp29, `lane_impl="std"` / `ServiceLane` active CPU-bound demand is driven by concurrently active lanes (`num_lanes`), not `hpx_threads`, and can exceed `hpx_threads`; `lane_impl="hpx"` / `HpxLane` demand is bounded differently by HPX workers, roughly `min(num_lanes, hpx_threads)` under the exp22 spin observation. Ray `num_cpus` is logical scheduling/accounting, not physical core pinning.
* Multiple Ray-hosted actors mean multiple independent HPX runtimes that do not coordinate core ownership. The more HPX-native long-term direction would be one coordinated HPX runtime with resource partitioning, thread pools, and executors; Ray-hosting deliberately trades that away because Ray owns placement/lifecycle. Do not present the Ray actor-pool / round-robin shape as HPX-native guidance.
* The HpxLane evidence arc is exp16 (native single-lane feasibility/timer behavior) → exp20 (task/dataflow pools are *not* drop-in RayX lane backends) → exp21 (RayX backend contract parity) → exp22 (load-divergence mechanism, observation-only) → exp23 (adapter-hop cost, observation-only). See `docs/reference/hpxlane_backend_arc.md` for the consolidated reading guide.
* exp22/exp23 timings (and the spin divergence) are observation-only and machine-specific: do not use them as performance claims, and do not state an "HpxLane is faster/slower than ServiceLane" verdict.
* Synthetic service timing should not be described as real inference work.

## Repository structure

* `readme.md`: project overview, `rayx` frontend at-a-glance, Quickstart, headline benchmark result, and documentation map.
* `docs/`: stable project framing and reference documentation.
* `docs/reference/`: API and design reference notes.
* `docs/design/`: exploratory runtime design notes; do not treat these as stable shipped reference docs unless explicitly promoted.
* `bench/`: benchmark drivers, analyzers, smoke/contract checks, and shared helpers.
* `ray_impl/`: Ray baseline implementation code.
* `hpx_impl/`: HPX-native baseline implementation code and native diagnostic probes.
* `hpx_impl/rayx_runtime_dispatch_probe.cpp`: standalone exp25 diagnostic target for runtime dispatch decomposition; observation-only, not a benchmark corpus path.
* `python/src/rayx/runtime/`: experimental `rayx.runtime` Python API plus import-light validation/error helpers.
* `python/src/rayx/runtime_ops.hpp`: fixed registered native operation registry and typed value model for `rayx.runtime`.
* `python/src/rayx/runtime_ops_hpx.hpp`: HPX-side registered runtime operations, including internal HPX composition.
* `python/src/rayx/runtime_actor_ops.hpp`: HPX-free native actor registry and `CounterActor` definitions for local native actors.
* `python/src/rayx/runtime_lane.hpp`: HPX-native FIFO `RuntimeLane` implementation.
* `python/src/rayx/runtime_cancel.hpp`: runtime-local cooperative cancellation token.
* `tests/unit/`: pure import-light runtime validation/error tests, including actor validation; must not import `rayx` / `_rayx`.
* `tests/integration/`: native runtime contract tests requiring built `_rayx`, including actor contract tests; should skip cleanly when `_rayx` is unavailable.
* `benchmarks/NN_name/`: chronological benchmark write-ups and curated evidence.
* `experiments/NN_name/`: investigative write-ups and curated evidence packages.
* `examples/`: small runnable API examples.
* `results/`: raw scratch/generated outputs; these should remain ignored.

## Working style

For design, API shape, benchmark design, diagnostics, or new files:

1. Inspect the existing tree first.
2. Give a short plan when the design/API choice is risky or ambiguous.
3. List exact files to create or edit.
4. Explain the intended CLI and output schema when relevant.
5. Stop for approval before editing only for risky API/design/benchmark choices or when explicitly asked.

When the direction is already clear, prefer one coherent, reviewable work chunk that bundles implementation, docs/reference consistency, validation, and reporting.

For small documentation-only updates, editing may proceed if the requested change is clear.

For build, run, and test tasks, proceed without asking when the command is obvious and local-only. Report exact commands and results.

Do not mix unrelated changes in one slice.

## Implementation rules

Prefer coherent, reviewable slices.

Keep dependencies minimal.

Use public APIs unless explicitly told otherwise.

Keep benchmark and diagnostic code simple and readable.

Do not hide important timing assumptions.

Do not introduce real model backends such as llama.cpp, vLLM, SGLang, TensorRT-LLM, or OpenVINO unless explicitly requested.

Do not add arbitrary payload execution, object-store behavior, Python callable execution, or Ray-like task execution unless the project direction is explicitly changed. `rayx.runtime` is limited to fixed registered native operations and fixed registered native actor methods.

Do not create large generated files.

## HPX-native design discipline

When proposing new RayX features, examples, or experiments, always consider whether there is a more HPX-native design before extending a Ray-shaped pattern.

Keep these stories separate:

* Ray-facing mapping: useful for showing how common Ray actor/future control patterns map onto RayX.
* HPX-facing design: should consider HPX futures, `hpx::async`, continuations, `hpx::dataflow`, executors, resource partitioning, cooperative scheduling, and HPX-thread lane mechanisms where appropriate.
* RayX harness design: remains a synthetic comparison harness with explicit, narrow semantics.
* `rayx.runtime` design: registered native operations and local native actors over HPX-native runtime lanes, still narrow and explicitly not arbitrary Python or object-store semantics.

Do not present a Ray actor-pool pattern as HPX best-practice guidance. If an example mirrors Ray actor-pool code, label it as a Ray-pattern mapping only.

Before adding Ray-shaped API surface or examples, ask:

* Is this needed for the Ray-vs-HPX comparison story?
* Is there a clearer HPX-native mechanism or experiment?
* Would this blur RayX into a fake Ray clone?
* Does this preserve the honest boundaries: no object store, no arbitrary remote Python execution, no Ray Serve, no real inference?

Prefer HPX-native mechanism probes or reference notes when the goal is to explain HPX design, and Ray-pattern examples only when the goal is to help Ray users understand the mapping.

## Benchmark and metrics discipline

Benchmark outputs should be machine-readable, usually JSONL.

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

## Documentation rules

Use `readme.md` for the project overview, `rayx` frontend at-a-glance, Quickstart, headline benchmark result, and documentation map.

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
* logs
* broad raw `results/` contents
* build outputs
* `_rayx*.so`
* `.pyc`
* `__pycache__`
* `.o`, `.dylib`, or other build products

Curated `aggregate.json` files beside benchmark or experiment reports are allowed.

Small curated diagnostic evidence packages are allowed only when intentionally part of an experiment package.

Raw JSONL and scratch outputs should stay under `results/` and remain ignored.

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

* repo-sanity should run `py_compile` and pure `tests/unit` only; it must not require `_rayx` or HPX.
* the native RayX smoke job may run `bench/smoke_rayx_runtime.py`, `examples/rayx_runtime_basic.py`, and `tests/integration` after `_rayx` is built.
* runtime unit tests include import-light operation and actor validation.
* runtime integration tests include native runtime and local actor contract coverage.
* do not run runtime integration tests or runtime smokes in repo-sanity.
* Ray-hosting smokes may live only in a native/Ray-capable smoke tier when Ray and built `_rayx` are available and they skip cleanly otherwise; this includes smoke-only Ray-hosted Runtime/CounterActor composition checks.
* Do not run Ray-hosting performance drivers, exp29 resource diagnostics, or exp31/exp32 observation probes in normal CI.
* The exp25 C++ dispatch probe build is an optional native/HPX validation target, not a repo-sanity requirement.

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

When a benchmark, diagnostic, or new CLI behavior is added, include a small smoke command or smoke test that can run quickly on a laptop.

## Style

Be precise and skeptical.

Prefer concrete claims over broad claims.

When comparing Ray and HPX, distinguish clearly between:

* Python-first ecosystem value
* distributed execution model
* actor/task overhead
* C++ runtime integration
* fine-grained async execution
* serving-control behavior
* benchmark-driver artifacts

Do not claim HPX is better than Ray without benchmark evidence.
