# rayx.runtime Parked Overlap: the cooperative `park_ms` mechanism

The deferred follow-up to the accepted `park_ms` core slice: an
**observation-only** package showing the HPX cooperative parked-work mechanism
inside `rayx.runtime`.

> This demonstrates the mechanism that parked HPX threads can suspend
> cooperatively, allowing other lanes to make progress with fewer HPX worker
> threads.

Mechanism / structure evidence only. **No** performance verdict, **no** speedup
claim, **no** Ray comparison of any kind, **no** "HPX beats Ray" claim, **no**
real I/O or inference claim. Structural gates first; every timing figure is
machine-specific observation, never gated.

## 1. What `park_ms` is

`park_ms(ms)` is a fixed registered native runtime op: synthetic cooperative
**PARKED** work, the parked-wait analog of the CPU-bound `busy_sum` diagnostic.
Its body parks in 10 ms chunks via `hpx::this_thread::sleep_for` (never
`std::this_thread::sleep_for`), is cancellable at chunk boundaries, is capped at
60 s, and on completion **echoes the requested `ms` back** as its `int64` value.
It is a synthetic diagnostic shape only — not real I/O, not inference, and it
carries no performance claim.

## 2. Why this demonstrates parked / cooperative work

Each `rayx.runtime` `RuntimeLane` worker is one long-lived `hpx::thread`
multiplexed onto the `hpx_threads`-sized HPX worker pool:

* A lane parked inside `park_ms` suspends **cooperatively** and frees its
  OS-level HPX worker, so other lanes can run on that worker. Many parked lanes
  can therefore make progress with few HPX workers — `num_lanes × park_ms` of
  requested parked work can drain in about **one** park duration even at
  `hpx_threads = 1`.
* Contrast `busy_sum`: CPU-bound work does **not** yield, so its concurrent
  progress is bounded by the worker pool. (That is the same contrast exp22
  observed for the harness `HpxLane` under sleep vs spin; no `busy_sum` cells
  are run here — the contrast is mechanism framing, not a measured comparison.)

## 3. What is gated and what is only observed

**Firm structural gates (the result; machine-independent):**

* **G1 retire** — every submitted future retires via `get()`;
  `results == offered` (the run finishing at all is the no-deadlock/no-hang
  evidence).
* **G2 status** — every status is terminal and valid (`completed` / `failed` /
  `cancelled`); this matrix cancels nothing, so `completed == offered`
  (`cancelled == 0`, `failed == 0`).
* **G3 value echo** — every completed value equals the requested `park_ms`.
* **G4 lane ids** — `rt-hpx-` prefix on every row; `lanes_seen == num_lanes`
  (round-robin submission lands exactly one park on each lane).
* **G5 idle drain** — `lane_stats()` returns to `queue_depth == 0` /
  `active == False` after the drain (bounded poll, no sleep).

**Observation-only (mechanism evidence, never a gate):** `makespan_ms`,
`expected_serial_ms = offered × park_ms`, and
`overlap_factor = expected_serial_ms / makespan_ms` (~1 means the parks did not
overlap; ~`num_lanes` means they did). Park-related checks never assert
wall-clock values; no timing figure carries a performance claim.

## 4. Setup and matrix

One `park_ms` request per lane (`offered = num_lanes`), submitted round-robin
and drained via `Runtime.get`. `hpx_threads` varies via short-lived sequential
`Runtime` contexts (each `Runtime` owns one `hpx::start` … `hpx::stop` cycle, as
`bench/smoke_rayx_runtime.py` already exercises), so no subprocess plumbing is
needed. Worst case is bounded by construction at `num_lanes × park_ms`
(32 × 250 ms = 8 s), far below `PARK_MS_MAX`.

| Axis | Full | Quick |
|---|---|---|
| `hpx_threads` | 1, 2 | 1, 2 |
| `num_lanes` | 1, 8, 32 | 1, 8 |
| `park_ms` per request | 250 | 100 |
| requests per lane | 1 | 1 |
| repeats | 3 | 1 |

## 5. Results

All firm structural gates passed (`all_structural_gates_passed: true`,
`gate_failures: []`): every park retired `completed` with the echoed value, one
`rt-hpx-` lane per request, zero cancelled/failed, lanes idle after drain.
Curated evidence is in `aggregate.json` beside this report. Observation-only
makespans (median over 3 repeats, full matrix, this machine):

| `hpx_threads` | `num_lanes` | serial expectation | makespan (p50) | overlap factor (p50) |
|---|---|---|---|---|
| 1 | 1 | 250 ms | ~275 ms | ~0.91 |
| 1 | 8 | 2 000 ms | ~275 ms | ~7.3 |
| 1 | 32 | 8 000 ms | ~276 ms | ~29.0 |
| 2 | 1 | 250 ms | ~275 ms | ~0.91 |
| 2 | 8 | 2 000 ms | ~275 ms | ~7.3 |
| 2 | 32 | 8 000 ms | ~274 ms | ~29.2 |

Two observations (mechanism, not performance):

* **The parked lanes overlap, and the overlap is independent of
  `hpx_threads`.** 32 lanes × 250 ms (8 s of requested parked work) drain in
  ~276 ms even with a single HPX worker — about one park duration, not 32. The
  cooperative `hpx::this_thread::sleep_for` suspension frees the worker, which
  is exactly the mechanism this package exists to show.
* **The per-park overshoot is visible at `num_lanes = 1`** (~275 ms observed
  for a 250 ms park, overlap factor ~0.91): the familiar synthetic sleep
  overshoot of the chunked cooperative timer, reported here only so the
  `overlap_factor` baseline is honest. It is not a finding of this experiment.

## 6. What is intentionally not claimed

* **Not a performance benchmark and no performance verdict.** Makespan and
  overlap factor are machine-specific observations of one laptop; nothing here
  says fast, slow, better, or worse.
* **No Ray comparison.** No Ray cells exist; nothing here compares `park_ms` to
  Ray, and no "HPX beats Ray" claim is made or implied.
* **Not real I/O, not inference, not serving.** `park_ms` is synthetic
  cooperative parked work; overlapping parked waits is a scheduling-mechanism
  fact, not a serving-capacity result.
* **Not a `busy_sum` measurement.** The CPU-bound contrast in §2 is framing;
  this matrix runs no CPU-bound cells.

## 7. Caveats

* Machine-specific (macOS laptop, 10 cores, single locality); synthetic
  diagnostic only; observation-only timing.
* `rayx.runtime` scope only: registered native ops over HPX-native FIFO
  `RuntimeLane`s — **not** the harness `Engine` / `lane_impl` backends
  (exp16/20–23 are a different axis), no analyzer / benchmark-JSONL-schema /
  driver / CI change, no object store, no arbitrary Python.
* The makespans here are dominated by a single park duration plus the chunked
  cooperative timer's overshoot; they say nothing about throughput, dispatch
  cost, or any workload other than parked waiting.

## 8. Reproduction

```bash
# quick smoke (smaller matrix; no aggregate.json written)
python experiments/24_runtime_parked_overlap/run_runtime_parked_overlap.py --quick

# full run (writes the curated aggregate.json beside this report)
python experiments/24_runtime_parked_overlap/run_runtime_parked_overlap.py

# optional overrides
#   --hpx-threads "1,2"  --num-lanes "1,8,32"  --park-ms 250  --repeats N
```

Requires the `_rayx` extension built (`cmake --build python/build`). The runtime
smoke (`python bench/smoke_rayx_runtime.py`) covers `park_ms` contract behavior
(validation, cancellation, echo) as a fast laptop check.
