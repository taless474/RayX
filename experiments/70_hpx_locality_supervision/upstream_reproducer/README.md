# exp70 upstream reproducer — connector-lifetime gap for dynamically connected HPX localities

**Status:** experiment-only lifecycle mechanism reproducer. Not performance evidence, not a Ray
comparison, not production code, not a shipped RayX API. No ratio / speedup / winner language.

A minimal, self-contained, two-process **loopback** reproducer for the connector-lifetime
problem isolated in exp63
(`experiments/63_hpx_native_collective_reduction/connector_lifetime_hardening.md`), intended to
accompany an upstream HPX issue. It is HPX-only and independently buildable: it does **not**
require Ray, Python, pybind11, Slurm, RayX runtime libraries, or any repository helper outside
this directory. Copying this directory out of the repository does not break it.

## Relationship to exp63

exp63 diagnosed an A1 `when_all_then_reduce` fault ("before_dispatch `std::system_error`,
code 1, *Operation not permitted*") as a **connector serve-window / lifecycle race**: the
connector's lifetime was a fixed wall-clock serve window measured from join, so it stopped its
HPX runtime while the root could still dispatch. A late parcel then reached a stopped thread
pool (`HPX(invalid_status): thread pool is not running` in `scheduled_thread_pool::create_work`
during `parcel::load_schedule` on the connector side). exp63's fix was an **external**
lifecycle protocol beside HPX: a dispatch-driven `root.alive` activity witness, a monotonic
connector deadman that resets when the witness advances, and an explicit `root.done` completion
witness.

This reproducer reduces that to the smallest deterministic two-process form:

* **`late-dispatch-current-behavior`** deterministically constructs the lifecycle gap: the
  connector leaves on its fixed serve window, then the root dispatches again to the departed
  locality and records exactly what HPX does.
* **`external-lifecycle-workaround`** applies the reduced exp63 external protocol to the same
  root, connector, action, startup path, and timing shape, and shows the second dispatch
  succeeding with a clean, explicitly classified shutdown.

## What it intentionally includes (verified exp63/exp49/exp65 semantics)

* Embedded HPX root: `hpx::start` from a plain `main`, late connectors enabled via
  `--hpx:expect-connecting-localities`.
* Connector using `hpx::runtime_mode::connect`, driven from its own non-HPX main thread.
* One minimal registered `HPX_PLAIN_ACTION` (`exp70_probe_action`), registered once per binary
  (the exp63 one-TU-per-binary discipline), with a closed-int64 oracle that encodes the
  executing locality id.
* The verified graceful leave ordering: `hpx::post([]{ hpx::disconnect(); })` then
  `hpx::stop()` on the connector; `hpx::post([]{ hpx::finalize(); })` then `hpx::stop()` on the
  root.
* Dispatch-driven activity witness in the workaround (`root.alive` bumped **before each
  dispatch**, exactly exp63's `_touch_heartbeat` semantics) — deliberately **not** a periodic
  heartbeat thread.
* Monotonic connector deadman: measured on the connector's **own steady clock**, reset only
  when the `root.alive` mtime advances.
* Explicit root completion witness (`root.done`) written **only after** the final remote work
  is verified.
* Bounded dispatch with **no HPX timed waits**: `hpx::post` + untimed `.get()` on the HPX
  runtime, bounded by `std::future::wait_for` on the calling OS thread (the exp69 `post_get`
  pattern; HPX `future::wait_for` at volume crashed a local macOS connect-mode host in exp69).
* Hard wall-clock backstops in both binaries (detached OS watchdog thread, forced exit rc 86)
  plus an overall driver timeout — neither case can hang indefinitely.

## What exp63 machinery it intentionally omits

* The pybind11/Python embedding, Ray, cluster or scheduler-specific launch, and multi-node placement/attestation
  (subnet self-bind, AGAS TCP pre-probe, `TCP_NODELAY`/affinity attestation).
* The N-leaf fan-out, `when_all_then_reduce` / `dataflow_reduce` compositions, partials, and
  the JSONL measurement plane — one scalar action is enough to expose the lifecycle gap.
* The legacy `served1.ok` write path (the connector still checks it for shape fidelity; the
  reproducer root never writes it).

## Build

Point `HPX_DIR` at an HPX install's CMake package directory:

```console
cmake -S . -B build -DHPX_DIR=/path/to/hpx/lib/cmake/HPX
cmake --build build
```

## Run

```console
./run_case.sh late-dispatch-current-behavior
./run_case.sh external-lifecycle-workaround
```

`BUILD_DIR=build-1.11 ./run_case.sh <case>` selects another build tree. The driver picks free
loopback ports, isolates each run in a `mktemp -d` directory (path printed to stderr; kept for
inspection), captures stdout/stderr/PIDs/exit codes per process, enforces an overall timeout
(`TIMEOUT_S`, default 120 s), kills all children on failure or interruption, verifies no
launched process remains alive, prints one final JSON summary line on stdout, and exits 0 only
when the selected case meets its expected result.

## Case 1: `late-dispatch-current-behavior`

Deterministic ordering, proven by marker files rather than sleeps:

1. Connector joins (`connector.joined`); root observes it by membership set-difference
   (`root.observed_join`).
2. Root dispatches action 1 and verifies the oracle (`root.action1`, `ok:true`).
3. The connector's fixed local serve window (default 3 s from join, its own steady clock)
   expires; it writes `connector.stopping`, runs its normal `disconnect`+`stop` path, then
   writes `connector.stopped`.
4. Only after observing `connector.stopped` (`root.observed_stop`) does the root write
   `root.action2_attempt` and dispatch action 2 to the **saved** id of the departed locality.
5. The exact outcome — status, throw site, exception type, `what()`, HPX/`std::error_code`
   codes, elapsed time — is recorded in `root.action2` and surfaced verbatim in the summary
   as `observed_second_dispatch_outcome`.

Expected: `race_constructed:true`, `second_dispatch_attempted:true`,
`second_dispatch_succeeded:false`. The **exact failure mode is recorded, not prescribed**:
`historical_exact_symptom_reproduced` is `true` only when the captured outcome is the exp63
root-side signature (throw at the async call site, `std::system_error`, code 1). A different
error on current HPX master than on HPX 1.11 is a finding, not a reproducer failure — the
gap being demonstrated is that the root has no runtime-level way to know the dispatch was
doomed, whatever the surfaced error is. (Note the deterministic reduction dispatches after the
connector *completed* its stop path and gracefully disconnected; exp63's original in-flight
variant hit the narrower mid-teardown window that produced the connector-side
`invalid_status: thread pool is not running` signal.)

## Case 2: `external-lifecycle-workaround`

Same root, connector, action, startup path, and timing shape, plus the reduced exp63 protocol:

1. Root bumps `root.alive` **before** dispatch 1 (dispatch-driven activity witness), dispatches,
   verifies.
2. Root then stays **idle for 6 s — longer than the case-1 serve window (3 s)** — publishing
   nothing. The connector stays alive because its lifetime is no longer the fixed window: its
   monotonic deadman (default 15 s) fires only on *silence* longer than the deadman bound.
3. Root bumps `root.alive` again and dispatches action 2 — it succeeds and verifies.
4. Root writes the explicit completion witness `root.done` only after action 2 is verified.
5. The connector observes completion, classifies its exit as `root_completion_signal`, and
   performs the verified graceful `disconnect`+`stop`; the root observes both the
   `connector.stopped` marker and the membership shrink.

Expected: both dispatches verified, `normal_completion_observed:true`,
`root_silence_observed:false`, no late-parcel evidence in either stderr, both exit codes 0, no
orphans.

## Why this is a lifecycle gap, not a tuning problem

The fixed serve window is not "too short" — no fixed value is correct, because the connector
cannot know how long the root intends to keep dispatching. Case 2 makes that explicit: the root
is idle *longer than the entire case-1 lifetime* and then validly dispatches again. What the
connector actually needs is **lifecycle information only the root has**: "I may still send you
work" (activity) and "I will never send you work again" (completion). Neither is expressible
through current public HPX connect-mode APIs, so exp63 had to route both through an **external
control plane** — shared-filesystem witness files beside HPX, with mtime visibility and
polling caveats. That external protocol is the workaround being demonstrated, not the proposed
fix.

A runtime-managed lifecycle/supervision contract for dynamically connected localities (the
subject of the planned upstream feature request) would replace the lifecycle role of these
external witnesses with in-band runtime state. **No native HPX supervision API is assumed or
exercised here** — this reproducer uses only current public HPX APIs, and exp70 Slice 0
implements no speculative native cases.

## Summary line fields

`case`, `overall`, `root_rc`, `connector_rc`, `first_dispatch_succeeded`,
`connector_stopped_before_second_dispatch`, `second_dispatch_attempted`,
`second_dispatch_succeeded`, `normal_completion_observed`, `root_silence_observed`,
`race_constructed`, `historical_exact_symptom_reproduced`, `observed_second_dispatch_outcome`
(the raw recorded outcome object), `late_parcel_evidence_seen`, `timed_out_overall`,
`orphan_count`, `hpx_version`, `hpx_git_commit`, `workdir`.
