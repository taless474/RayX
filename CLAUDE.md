# RayX Working Rules

## Project

This repository investigates whether HPX can serve as a C++/HPC-native execution substrate for ML serving-control workloads, comparable to Ray Core's actor/task model.

RayX is a narrow comparison harness. It is not a Ray replacement, not Ray Serve, not real model inference, and not a general claim that HPX is better than Ray.

The repository currently includes:

* Ray actor baseline using public Ray APIs
* HPX-native synthetic baseline
* `rayx` Python frontend over HPX service lanes
* Synthetic serving workloads
* JSONL benchmark output and analyzer
* Chronological benchmark and experiment write-ups
* Curated aggregate/diagnostic artifacts for selected results

Do not clone, modify, or vendor Ray internals unless explicitly requested.

## Read first

At the beginning of each session, read `handoff.md`.

Do not update or rewrite `handoff.md` unless explicitly asked for a handoff.

## Current technical interpretation

Keep these guardrails in mind:

* Python/GIL is not the high-lane bottleneck.
* The sleep-mode bimodal high-lane behavior is refined to closed-loop FIFO-retire / client-driver behavior.
* The spin-mode CPU-bound behavior is hardware/core-boundary / oversubscription behavior.
* Avoid unqualified “coordination ceiling” wording.
* Do not claim `rayx` is generally faster than native HPX.
* Do not claim HPX generally beats Ray.

When preserving old measurements, mark superseded interpretations clearly instead of deleting useful provenance.

## Repository structure

* `readme.md`: short overview, Quickstart, headline result, and documentation map.
* `docs/`: stable project framing and reference documentation.
* `docs/reference/`: API/reference notes.
* `bench/`: benchmark drivers, analyzers, smoke/contract checks, and shared helpers.
* `benchmarks/NN_name/`: chronological benchmark write-ups and curated aggregate artifacts where applicable.
* `experiments/NN_name/`: chronological investigative write-ups and curated artifacts where applicable.
* `examples/`: small runnable API examples.
* `results/`: raw scratch/generated outputs. These should remain ignored.

## Working style

For design, API shape, benchmark design, diagnostics, or new files:

1. Inspect the existing tree first.
2. Give a short plan when the design/API choice is risky or ambiguous.
3. List exact files to create or edit.
4. Explain the intended CLI and output schema when relevant.
5. Stop for approval before editing only for risky API/design/benchmark choices or when explicitly asked.

When the direction is already clear, prefer one coherent, reviewable work chunk that bundles implementation, docs/reference consistency, validation, and reporting. Avoid unnecessary tiny back-and-forth slices.

For small documentation-only updates, editing may proceed if the requested change is clear.

For build, run, and test tasks, proceed without asking when the command is obvious and local-only. Report exact commands and results.

## Implementation rules

Prefer coherent, reviewable slices.

Keep dependencies minimal.

Use public APIs unless explicitly told otherwise.

Keep benchmark and diagnostic code simple and readable.

Do not hide important timing assumptions.

Do not mix unrelated changes in one slice.

Do not introduce real model backends such as llama.cpp, vLLM, SGLang, TensorRT-LLM, or OpenVINO unless explicitly requested.

Do not create large generated files.

## Metrics discipline

Benchmark outputs should be machine-readable, preferably JSONL.

Each request result should include enough timing information to compute:

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

For native diagnostic output:

* Keep `--diag` opt-in.
* Keep normal JSONL schema and analyzer behavior unchanged when diagnostics are off.
* Diagnostic summaries should be compact and separate from normal per-request JSONL.

For deterministic synthetic workloads:

* Keep Ray and `rayx` Python drivers on the shared `bench/service_sequence.py` helper.
* Do not duplicate or silently drift the fixed/bimodal service-sequence logic.
* If the deterministic sequence intentionally changes, update the golden smoke and any native golden check together.

## Documentation rules

Use `readme.md` for the short project overview, Quickstart, headline result, and documentation map.

Avoid future-looking roadmap, TODO, or “next step” language in `readme.md` unless explicitly requested.

The README documentation map should let a reader understand the project arc. Each benchmark or experiment bullet should include one concise “what we learned” sentence.

Use:

* `docs/project_proposal.md` for longer motivation, hypothesis, scope, and phases.
* `docs/experiment_plan.md` for benchmark shapes, metrics, commands, and acceptance gates.
* `docs/ray_hpx_mapping.md` for conceptual mapping between Ray and HPX.
* `docs/reference/` for API/reference material.
* `benchmarks/` for benchmark write-ups.
* `experiments/` for investigative write-ups and curated evidence packages.
* `examples/` for small runnable API examples.

Keep documentation concise and stable. Avoid one-time scratch notes in persistent docs.

## Artifact rules

Do not include raw generated artifacts in commits:

* `.jsonl`
* `.summary.json`
* logs
* build outputs
* `_rayx*.so`
* scratch result directories
* broad raw `results/` contents

Curated `aggregate.json` files beside benchmark or experiment docs are allowed.

Small curated `diag/*.diag.json` evidence packages are allowed only when intentionally part of an experiment package, such as `experiments/06_diag_fifo_ceiling_analysis/`.

Raw JSONL and scratch outputs should stay under `results/` and remain ignored.

## Acceptance mindset

Every slice should end with a clear report:

* files changed
* commands run
* pass/fail result
* output files, if any
* current known facts
* validation performed
* remaining caveats

When a benchmark, diagnostic, or new CLI behavior is added, include a small smoke command or smoke test that can run quickly on a laptop.

Use `bench/smoke_local.py` as the local validation aggregator when appropriate. It should remain a smoke/golden/contract helper, not a benchmark matrix.

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
