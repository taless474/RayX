# RayX Lane-Contract Parity: `lane_impl="std"` vs `lane_impl="hpx"`

Verifies that the two rayx lane backends behave the same through the same
Ray-like API. The question:

> Does `rayx.Engine(lane_impl="hpx")` (the opt-in cooperative HPX-thread
> `HpxLane`) honor the **identical RayX lane contract** as the default
> `lane_impl="std"` (the std::thread `ServiceLane` comparison anchor) — same
> completion, retirement, chunking, cancellation, bounded admission, and
> `lane_stats()` semantics — with the **only** observable difference being the
> lane `actor_id` prefix?

Parity / semantics slice only: **no** new RayX API, **no** result-row / v1
benchmark-JSONL schema change, **no** analyzer change, **no** benchmark-driver or
CI change, and **no** change to the public `Future` ownership model. No HPX
internals are exposed. Companion reference:
`docs/reference/rayx_frontend_design.md` §13 (the `lane_impl` backend seam) and
§7 / §11 / §12 (cancellation, `lane_stats()`, bounded admission contracts).

**Scope vs exp22.** This experiment proves **contract parity** — that the two
backends behave identically at the semantic level. It does **not** characterize
where they **diverge under load**: that is a separate, scheduling-mechanism
question (parked sleep vs non-yielding spin, `HpxLane` concurrency bounded by the
HPX worker pool) answered as observation-only evidence in
`experiments/22_rayx_hpxlane_load_divergence/`.

## 1. Purpose

`lane_impl` selects the lane mechanism behind every lane while keeping the API
fixed (see design §13). `"std"` is the stable comparison anchor; `"hpx"` is the
opt-in cooperative HPX-thread lane. This experiment is the parity check that the
swap is **behavior-equivalent at the contract level** — it locks in that turning
on `"hpx"` does not silently change what a request does, only which lane
mechanism services it.

## 2. What this experiment does and does not test

**Does test (semantics / parity):**

* all admitted requests complete unless intentionally cancelled;
* exactly one result row per admitted request;
* `get`, `wait` (non-consuming ready/rest partition), `as_completed`;
* chunked synthetic service (full run, echoes `chunks` / `chunk_delay_ms`);
* queued cancellation → `status="cancelled"`, `chunks_completed == 0`;
* running cancellation at a chunk boundary → `0 < chunks_completed < chunks`;
* bounded admission → `QueueFullError` on a full lane (a no-op on queue depth),
  admitted + occupier work still completes;
* `lane_stats()` shape `{actor_id, queue_depth, active}`, `active` + `queue_depth`
  observed under load, return to idle after drain;
* the lane `actor_id` prefix distinguishes the backends.

**Does not test:**

* **performance.** Timing is recorded as an observation only and is **never a
  gate**. This makes **no** speedup claim and **no** "HPX beats Ray" claim.
* the experiment-20 `hpx::async` / `hpx::dataflow` task-pool mechanisms — those
  pools are deliberately **not** drop-in rayx lane backends and are out of scope.
* anything outside the synthetic harness: not Ray Serve, not a Ray object store,
  not real model inference.

## 3. Backend setup

* **`std` → `rayhpx::ServiceLane`** (`std::thread` lane, default anchor),
  `actor_id` prefix `act-hpx-`.
* **`hpx` → `rayhpx::HpxLane`** (cooperative HPX-thread lane, opt-in),
  `actor_id` prefix `act-hpxl-`.
* Both reach the same shared `Request` / `Result` / `CancelToken` and synthetic
  service semantics; the `HpxLane` adapter hops lane-state operations onto an HPX
  thread (design §13). Boundary: `hpx-python-frontend`.
* Each backend runs in its **own subprocess** (one HPX runtime per process); each
  scenario uses its own short-lived `Engine` context, as `bench/smoke_rayx.py`
  does. The cancellation / bounded-admission scenarios run at `num_lanes=1` (so
  routing is unambiguous); the completion and `lane_stats`-shape scenarios run at
  `num_lanes=4` (full) / `2` (quick).

## 4. Scenario matrix

| Scenario | Config | Parity assertion (identical for both backends) |
|---|---|---|
| `basic` | `num_lanes` 4, N=24 | all complete, one row each, prefix ok, lane balance spread ≤ 1 |
| `wait` | `num_lanes` 4, N=12 | `wait` returns a ready/rest split, is non-consuming, all retire |
| `as_completed` | `num_lanes` 4, N=12 | each future yielded once, all complete |
| `chunked` | `num_lanes` 1 | full run, `chunks`/`chunk_delay_ms` echoed, `chunks_completed==chunks` |
| `queued_cancel` | `num_lanes` 1 | cancel→True, `status="cancelled"`, `chunks_completed==0` |
| `running_cancel` | `num_lanes` 1 | cancel→True, `status="cancelled"`, `0 < chunks_completed < chunks` |
| `bounded_admission` | `num_lanes` 1, cap=2 | full lane → `QueueFullError`, depth unchanged, admitted+occupier complete |
| `lane_stats` | shape @ `num_lanes` 4; load @ 1 | correct shape; active + depth under load; idle after drain |

## 5. Results

Both backends passed **all 8 scenarios**; all parity gates passed
(`all_parity_gates_passed: true`, `gate_failures: []`). Curated evidence is in
`aggregate.json` beside this report. Headline evidence (full run, this machine):

* **Backend prefixes (observed, distinct).** `std` → `act-hpx-…`,
  `hpx` → `act-hpxl-…`. The seam actually switches lanes, and each observed
  sample matches its backend's expected prefix.
* **Completion.** `basic`: 24/24 completed, 24 rows, lane-count spread 0 (both).
  `as_completed`: 12 yielded (both). `wait` non-consuming (all retired after the
  partition).
* **Cancellation (identical outcomes).** queued: `chunks_completed == 0` (both);
  running: `chunks_completed` in `(0, 6)` — observed `2` of `6` on this run for
  both backends (the exact partial count is host-dependent and not gated).
* **Bounded admission (identical).** cap 2 → `QueueFullError` raised; depth before
  and after the rejected submit both `2` (rejection is a no-op); admitted +
  occupier = 3/3 completed (both).
* **`lane_stats` (identical).** keys `{active, actor_id, queue_depth}`;
  `active` observed under load; `queue_depth` observed `3` under load; returned to
  idle after drain (both).
* **Timing (observation only, NON-gating, no claim).** `basic`
  `service_ms_observed` p50 on this run: `std` ≈ 5.0 ms, `hpx` ≈ 4.5 ms (requested
  4 ms). These are single-run, machine-specific observations under a 24-request /
  4-lane burst; they are **not** a benchmark, **not** repeated for significance,
  and support **no** speedup conclusion.

## 6. Interpretation

Within this synthetic harness, `lane_impl="hpx"` is **contract-equivalent** to the
`lane_impl="std"` anchor: every gated semantic — completion, one-row-per-request,
`get`/`wait`/`as_completed`, chunking, queued and running cancellation, bounded
admission, and `lane_stats()` — produced the same outcome on both backends, and
the only observable difference was the `actor_id` prefix. This supports treating
`"hpx"` as a drop-in lane mechanism behind the existing API for the contract
surface RayX exposes. It says nothing about relative performance.

## 7. Caveats

* **Parity, not performance.** The timing figures are observations on one machine
  and one run; no speedup or "HPX beats Ray" claim is made or implied.
* **Single-run gates.** Parity gates are boolean contract checks, not statistical
  thresholds. The `running_cancel` partial count is range-checked `(0, chunks)`,
  not pinned to an exact value (it is host-speed dependent).
* **Scope.** This is the opt-in serialized rayx `HpxLane` backend, not the
  experiment-20 task/dataflow probe, and not a statement about HPX vs Ray, Ray
  Serve, object stores, or real inference.
* Raw per-backend JSON is experiment-local scratch under `results/` (gitignored)
  and is **not** the v1 benchmark JSONL schema; the tracked evidence is
  `aggregate.json` + this report.

## 8. Reproduction

```bash
# quick smoke (smaller matrix; no aggregate.json written)
python experiments/21_rayx_hpxlane_backend_parity/run_rayx_hpxlane_backend_parity.py --quick

# full parity run (writes the curated aggregate.json beside this report)
python experiments/21_rayx_hpxlane_backend_parity/run_rayx_hpxlane_backend_parity.py
```

Requires the `_rayx` extension built (`cmake --build python/build`). The RayX
contract smoke (`python bench/smoke_rayx.py`) covers the same `lane_impl`
selection and hpx-backend semantics as a fast laptop check.
