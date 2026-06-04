# RayX Working Rules

## Project identity

RayX is a narrow Ray-vs-HPX comparison harness for synthetic ML serving-control workloads.

The project compares:

* Ray actor baselines using public Ray APIs
* HPX-native synthetic baselines
* `rayx`, a thin Python frontend over HPX service lanes

RayX is not a Ray replacement, not Ray Serve, not Ray Train, not a Ray object-store project, and not real model inference.

Do not clone, modify, or vendor Ray internals unless explicitly requested.

## Session startup

At the beginning of each session, read `handoff.md`.

Do not update or rewrite `handoff.md` unless explicitly asked.

Do not stage, commit, amend, or push unless explicitly asked.

Do not suggest staging, committing, amending, or pushing unless explicitly asked.

## Technical guardrails

Keep RayX synthetic and honest.

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
* The HpxLane evidence arc is exp16 (native single-lane feasibility/timer behavior) → exp20 (task/dataflow pools are *not* drop-in RayX lane backends) → exp21 (RayX backend contract parity) → exp22 (load-divergence mechanism, observation-only) → exp23 (adapter-hop cost, observation-only). See `docs/reference/hpxlane_backend_arc.md` for the consolidated reading guide.
* exp22/exp23 timings (and the spin divergence) are observation-only and machine-specific: do not use them as performance claims, and do not state an "HpxLane is faster/slower than ServiceLane" verdict.
* Synthetic service timing should not be described as real inference work.

## Repository structure

* `readme.md`: project overview, `rayx` frontend at-a-glance, Quickstart, headline benchmark result, and documentation map.
* `docs/`: stable project framing and reference documentation.
* `docs/reference/`: API and design reference notes.
* `bench/`: benchmark drivers, analyzers, smoke/contract checks, and shared helpers.
* `ray_impl/`: Ray baseline implementation code.
* `hpx_impl/`: HPX-native baseline implementation code.
* `benchmarks/NN_name/`: chronological benchmark write-ups and curated evidence.
* `experiments/NN_name/`: investigative write-ups and curated evidence.
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

Do not add payload execution, object-store behavior, or arbitrary task execution unless the project direction is explicitly changed.

Do not create large generated files.

## HPX-native design discipline

When proposing new RayX features, examples, or experiments, always consider whether there is a more HPX-native design before extending a Ray-shaped pattern.

Keep these stories separate:

* Ray-facing mapping: useful for showing how common Ray actor/future control patterns map onto RayX.
* HPX-facing design: should consider HPX futures, `hpx::async`, continuations, `hpx::dataflow`, executors, resource partitioning, cooperative scheduling, and HPX-thread lane mechanisms where appropriate.
* RayX harness design: remains a synthetic comparison harness with explicit, narrow semantics.

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

Do not add full benchmark or experiment matrices to normal CI.

Do not add machine-sensitive performance checks to normal CI.

Do not require an HPX source build in normal CI unless explicitly requested.

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
