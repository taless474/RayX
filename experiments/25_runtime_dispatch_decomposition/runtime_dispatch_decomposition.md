# rayx.runtime Dispatch Decomposition: the nested `hpx::async(exec_, task).get()`

The native micro-driver slice that follows exp24: an **observation-only**
decomposition of the `rayx.runtime` `RuntimeLane` per-operation dispatch path,
estimating the cost of the nested

```cpp
hpx::async(exec_, task).get()
```

inside the lane worker (`python/src/rayx/runtime_lane.hpp`, `run()`). The lane
worker is itself an `hpx::thread`, so an inline `task(stop)` call would preserve
the current cooperative semantics (`park_ms` still suspends cooperatively,
checkpoint yields still run, FIFO unchanged); the open architectural question is
what the nested hop costs and whether it is useful or decorative. **This
experiment prices it; it does not decide it.** No performance verdict, no
speedup claim, no Ray comparison, no "HPX beats Ray" claim; all timing is
machine-specific observation, never gated.

## 1. Instrument

A standalone native probe, `hpx_impl/rayx_runtime_dispatch_probe.cpp`
(exp15/exp20 pattern: `HPX::hpx` + `HPX::wrap_main`, JSON on stdout, no pybind,
no `_rayx`). It includes the production runtime headers **read-only and
unmodified** — `runtime_lane.hpp`, `runtime_ops.hpp`, `runtime_ops_hpx.hpp` —
and builds its op tasks with a probe-local mirror of `_rayx.cpp`'s
`make_op_task` (same timing + value/failure mapping, minus pybind).

The probe also defines `InlineDispatchProbeLane`: a **measurement fork only;
not production `RuntimeLane`; not a maintained alternate implementation**. It
copies the production worker loop — `hpx::thread` worker, `hpx::mutex` +
`hpx::condition_variable_any` queue, per-item promise + cancel token,
`begin_service`, the `StopCheckpoint` binding with its cooperative yield, the
same per-item lock pattern — with exactly one difference: the nested
`hpx::async(exec_, task).get()` is replaced by an inline `task(stop)` call. Its
lane ids use a distinct `rt-prbi-` prefix so a probe lane can never be mistaken
for a production `rt-hpx-` lane.

## 2. Modes and ops

| Mode | What runs | What it estimates |
|---|---|---|
| `inline_body` | `task(stop)` directly on an HPX thread | body-only floor |
| `async_get` | `hpx::async(exec, [task,stop]{…}).get()` — the exact production idiom | `async_get − inline_body` ≈ the nested hop (primitive estimate) |
| `lane_nested_seq` / `_batch` | production `RuntimeLane`, unmodified | full per-op lane cost |
| `lane_inline_seq` / `_batch` | the inline-dispatch fork | `lane_nested − lane_inline` ≈ the nested hop (in-lane estimate) |

`_seq` is one-at-a-time submit→get latency; `_batch` submits 64 then drains
(per-op figure = span / 64, worker wakeup amortized). Ops are tiny registered
bodies from the production registries: `square(7)` → 49, `busy_sum(256)` →
32640 (single chunk, no checkpoint reached), `park_ms(0)` → echoes 0 (parks
nothing). Real parked durations are deliberately **not** measured here — exp24
owns parked timing, and timer overshoot would only blur dispatch decomposition.

## 3. Gates vs observations

**Firm structural gates (the result):** every operation completes with the
closed-form expected value; lane modes have zero FIFO `end_ns` inversions and
the correct id prefix (`rt-hpx-` production / `rt-prbi-` fork); every
`(hpx_threads, mode, op)` cell is present. **Observation-only (never gated):**
all percentiles and every derived delta. The deltas are **approximate, not
exact subtractions** — closure-copy cost, scheduling jitter, and clock
granularity remain inside them.

## 4. Matrix

`hpx_threads` ∈ {1, 2} (one probe invocation each; the HPX runtime is a process
resource), `num_lanes = 1` for isolation. Full: 20 000 iters/cell (1 000
warmup), 300 batches of 64 (10 warmup). Quick: 2 000 iters, 30 batches.
Laptop-safe: the full run is a few seconds per invocation.

## 5. Results (this machine; median, full run)

All structural gates passed (`all_structural_gates_passed: true`,
`gate_failures: []`). Curated evidence in `aggregate.json`. The three
independent estimates of the nested-hop cost agree on magnitude:

| `hpx_threads` | op | body p50 | lane (nested) seq p50 | hop ≈ primitive | hop ≈ lane seq | hop ≈ lane batch |
|---|---|---|---|---|---|---|
| 1 | square | 42 ns | 833 ns | 333 ns | 375 ns | 484 ns |
| 1 | busy_sum | 125 ns | 917 ns | 334 ns | 375 ns | 481 ns |
| 1 | park_ms(0) | 42 ns | 833 ns | 333 ns | 375 ns | 472 ns |
| 2 | square | 42 ns | 2 125 ns | 916 ns | 1 083 ns | 998 ns |
| 2 | busy_sum | 125 ns | 2 208 ns | 917 ns | 999 ns | 658 ns |
| 2 | park_ms(0) | 42 ns | 2 083 ns | 875 ns | 958 ns | 1 041 ns |

Three observations (mechanism, not performance):

* **The nested hop reads as roughly 0.3–0.5 µs per op at `hpx_threads = 1` and
  roughly 0.7–1.1 µs at `hpx_threads = 2`**, consistently across the primitive
  pair, the in-lane sequential pair, and the in-lane batch pair. The rise with
  pool size echoes exp23's observation that the `run_as_hpx_thread` boundary
  hop also grew with the worker pool — scheduling across more workers costs
  more, an HPX-scheduling observation, not a verdict.
* **At the no-op floor the hop is a large fraction of per-op lane cost on this
  machine** (~40–50% of the ~0.8–2.2 µs `lane_nested_seq` total). The remainder
  is lane bookkeeping (queue push/pop, cv wakeup, promise + token construction)
  plus the tiny body.
* **For perspective, the corpus serves ms-scale synthetic work**: a ~0.3–1 µs
  per-op component is three orders of magnitude below one 1 ms service slot.
  The fraction is only large because the floor is tiny.

## 6. What this supports (and does not)

This is pricing evidence for a **future, separately-approved** decision on the
nested dispatch, per the exp25 plan:

* The inline fork preserved every structural gate here, and the hop is a
  substantial share of the no-op-floor lane cost — so a removal/inline slice is
  **measurably motivated at the floor** but **immaterial at corpus service
  scales**.
* The nested `exec_` launch remains the lane's placement seam (named
  control/work pools, the P4 axis in `runtime_lane.hpp`); removing it would
  close that seam. Keeping it costs ~0.3–1 µs per op on this machine.
* **No decision is made in this experiment.** Keep / remove / keep-for-named-
  executors all remain open; this package only replaces taste with a number.

Intentionally not claimed: no performance verdict, no "inline is faster" or
"the lane is slow" claim, no Ray comparison, no serving-capacity statement
(tiny bodies at the no-op floor are not a serving workload), no real I/O or
inference, and the fork is probe evidence only — it must never be treated as a
production backend.

## 7. Caveats

* Machine-specific (macOS laptop, 10 cores, single locality); ns-scale numbers
  jitter run to run; observation-only throughout.
* The deltas are approximations: the primitive pair excludes lane bookkeeping
  entirely; the lane pair relies on a fork that copies (rather than shares) the
  worker loop, so compiler/layout differences are inside the delta.
* `hpx_threads = 2` cells time a cross-worker scheduling pattern (the async
  task may run on the other worker); that is part of what is being priced, not
  noise to remove.
* If `RuntimeLane::run()` changes later, the fork in the probe reflects the
  loop as of this experiment and must not be read as current.
* No production runtime, analyzer, JSONL-schema, or CI change; the probe is not
  part of the benchmark corpus.

## 8. Reproduction

```bash
cmake --build hpx_impl/build   # builds rayx_runtime_dispatch_probe

# native smoke (JSON on stdout, human progress on stderr)
./hpx_impl/build/rayx_runtime_dispatch_probe --hpx:threads=1 --quick

# quick runner smoke (no aggregate.json written)
python experiments/25_runtime_dispatch_decomposition/run_runtime_dispatch_decomposition.py --quick

# full run (writes the curated aggregate.json beside this report)
python experiments/25_runtime_dispatch_decomposition/run_runtime_dispatch_decomposition.py

# optional overrides: --probe-bin PATH --hpx-threads "1,2" --iters N --warmup N
```
