# rayx.runtime Phase 1 summary

This is a stable, concise summary of the completed **experimental** `rayx.runtime`
Phase 1 milestone, landed in commit `9143d2d`.

`rayx.runtime` is experimental. It is a separate workstream from the stable
synthetic `Engine` / `SyntheticActor` comparison harness, and it does not change
that harness, its JSONL schema, or its analyzer.

## Current strategic target

The current target is a **single-locality, many-core, GIL-free native runtime with
Ray-familiar control ergonomics**: fixed/typed registered native operations over
HPX-native FIFO lanes, with HPX-native scheduling and composition underneath, and
Ray-*shaped* control (per-request handles, `get` / `wait` / `as_completed`,
cooperative cancellation, bounded admission).

It is **not** a Ray replacement, **not** "distributed Ray with HPX underneath",
**not** an `ObjectRef` / object store, **not** arbitrary Python or dynamic pickled
closures, and **not** Ray-grade fault tolerance / lineage / autoscaling — and it
makes **no** performance or "HPX beats Ray" claim. The credible advantage is
intra-locality (no process / IPC / pickle boundary, GIL-free native workers,
fine-grained M:N scheduling); distribution is design-only and much later. The full
target framing and near-term axis ordering live in
[rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) ("Current strategic
target (post-Phase-1)").

## What was added

An experimental `rayx.runtime` subpackage: a thin Python API over HPX-native FIFO
runtime lanes that executes a small set of **fixed registered native operations**.
The fixed operation registry is:

* `square(int) -> int`
* `add(int, int) -> int`
* `boom() -> failed result`
* `busy_sum(int) -> int`

Added **after** Phase 1 (the first internal-composition op, no new Python surface):

* `fanout_sum(int n, int parts) -> int` — equals `busy_sum(n)` (`(n*(n-1)/2) mod
  2³¹`), but the body fans out across `parts` `hpx::async` sub-tasks and combines
  with `hpx::when_all`. P1 launch-all design: queued-cancelable only. See
  [rayx_runtime_internal_composition_note.md](rayx_runtime_internal_composition_note.md).

The Python surface is:

* `Runtime`
* `RuntimeFuture`
* `OperationResult`
* `RuntimeOperationError`
* `OperationFailedError`
* `OperationCancelledError`
* `QueueFullError`

It supports:

* HPX-native FIFO `RuntimeLane`s
* value + row separation
* cooperative cancellation
* bounded admission
* `lane_stats`
* `get`
* `wait`
* `as_completed`

## Why rayx.runtime exists

The synthetic harness (`Engine` / `SyntheticActor`) is a Ray-vs-HPX comparison
driver. It exercises timing and control-plane behavior using **synthetic work** and
emits per-request **measurement rows**; it does not compute real values.

`rayx.runtime` is a narrowly scoped prototype that explores an actual HPX-native
execution path: it runs **real registered native operations** that return real
values, while preserving the measurement/status row alongside each value. It is a
controlled way to study native runtime integration without turning RayX into a
general task system, and it keeps the same honest boundaries as the harness.

## What it covers beyond the synthetic harness

The synthetic harness covered timing and control-plane behavior over synthetic work,
captured as measurement rows.

`rayx.runtime` covers **real native operation values** while still preserving a
measurement/status row. Concretely:

* `RuntimeFuture.result()` returns an `OperationResult`.
* `OperationResult.value` is the user value (the real result of the native
  operation).
* `OperationResult.row` is the measurement/status row, kept strictly separate from
  the value.
* Per-operation lifecycle controls: cooperative cancellation (queued and running),
  bounded admission with `QueueFullError`, and `lane_stats` for queue/active
  visibility.
* Result-collection APIs: `get`, `wait`, and `as_completed`.

## How it relates to HPX

`rayx.runtime` lives under the `rayx` package and shares the runtime plumbing with
the harness:

* the same `_rayx.so`
* the same one-process HPX runtime guard
* the same mutual-exclusion rule: `Engine` and `Runtime` are mutually exclusive in
  one process

Underneath, the runtime uses HPX-native FIFO lanes and HPX futures, with cooperative
HPX-thread execution. **No HPX types are exposed to Python** — users see only
`Runtime`, `RuntimeFuture`, and `OperationResult`.

## What is intentionally not included

`rayx.runtime` deliberately does **not** provide, and does not imply:

* no `ObjectRef`
* no object store
* no Ray task-result semantics
* no arbitrary Python callables
* no pickled closures
* no HPX actions / components
* no distributed locality
* no module-level `rayx.get` / `rayx.wait`
* no `Runtime.lane_impl`
* no real model inference
* no benchmark JSONL / analyzer changes
* no performance claim

## What tests and CI protect it

* `tests/unit/` — import-light validation/error tests that do not require `_rayx` or
  HPX.
* `tests/integration/` — native contract tests that require a built `_rayx` and skip
  cleanly when it is unavailable.
* `bench/smoke_rayx_runtime.py` — the comprehensive runtime-contract smoke.
* `examples/rayx_runtime_basic.py` — a runnable API/semantics tour (value + row
  separation, fixed operations, failure, cancellation, `lane_stats`, `get`,
  `as_completed`).

CI split:

* repo-sanity runs `py_compile` plus the pure `tests/unit` only (no `_rayx`, no
  HPX).
* the native job builds `_rayx` and runs the runtime smoke, the runtime example, and
  the integration tests.
