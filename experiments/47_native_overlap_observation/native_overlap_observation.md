# exp47 — in-process HPX nested-overlap observation through the Runtime/lane boundary

**Type:** structural / observational. **Not** a performance result.

## What this experiment is

exp47 pivots off the closed boundary-placement / count arc (exp39 → exp40 → exp44 →
exp45 → exp46). Instead of asking *"how many Python/Runtime crossings?"* it asks:

> When two **independent** native arms are launched with bare `hpx::async` **inside one
> coarse Runtime op** and joined with `when_all` below the single Python/Runtime
> boundary, does the RayX lane boundary faithfully preserve their HPX scheduling — and
> can we *observe* whether they were in flight together, distinguishing cooperative
> interleaving from worker-parallel execution?

The probe is **"barrier_fanin without the gate"**: the new op `overlap_probe(seed,
quantum, mode) -> int64` launches two independent bare-`hpx::async` arms on the default
HPX pool and joins with `when_all`. No diamond, no `.then`, no `hpx::dataflow`.
`diamond_fanin` / `barrier_fanin` are untouched.

The closed `int64` is **mode-independent** and equals `chain_fanout(seed, 2, 1, quantum)`:

```
leaf0 = chain_stage(seed,     quantum)
leaf1 = chain_stage(seed + 1, quantum)
result = (leaf0 + leaf1) & BUSY_SUM_MASK
```

`mode` selects only the **arm kernel shape** (never the value):

* **mode 0 — non-yielding:** one flat `chain_stage` sweep, no interior yield.
* **mode 1 — chunked-yielding:** the same masked sum split into `BUSY_SUM_STRIDE` (8192)
  chunks with an `hpx::this_thread::yield()` at each chunk boundary **inside** the
  compute. Value-identical because masked add is mod-2³¹ (associative) — the
  chunk-equals-flat invariant `run_masked_checkpoint_loop` already relies on.

The instrumented `in_flight++ / in_flight--` **brackets the full arm compute** (increment
before the kernel loop, decrement after it), so an observed `max_in_flight ≥ 2` encloses
real chunked work — never a post-compute yield.

## What "in flight" and the classifications mean

"In flight" = an arm has **entered** its bracketed compute and not yet **left** it. It
does **not** assert both arms execute CPU instructions at the same instant (in the
one-worker yielding case they interleave on one worker). The debug-only `OverlapWitness`
samples `hpx::get_worker_thread_num()` at entry and each chunk (HPX threads may migrate
after a suspension point, so a per-arm **set** is kept, not a single sample) and
classifies:

* `serial` — `max_in_flight < 2` (no in-flight overlap observed).
* `cooperative_interleaving` — `max_in_flight ≥ 2`, worker-id evidence consistent with
  **one** worker. Cooperative interleaving, **not** OS-thread parallelism.
* `worker_parallel` — `max_in_flight ≥ 2`, **distinct workers observed** (a candidate;
  never proof of speedup/throughput/latency/parallel execution).
* `inconclusive` — unknown/overflowed worker sets or malformed trace.

## Load-bearing result vs reported observation (kept separate)

* **`overall_structural_pass` (deterministic, load-bearing):** every row returns the
  bit-identical closed `int64` (oracle **and** `chain_fanout`), and the witness
  structural gates hold (`arms_launched == arms_completed == 2`, `clean_exit`,
  `ordering_violations == 0`, per-arm enter/leave ordering, `both_in_flight ==
  (max_in_flight ≥ 2)`, classification in the closed set). This confirms nested HPX async
  work launched inside one Runtime op schedules, joins, and returns correctly through the
  real Runtime/lane boundary. It does **not** depend on any scheduler-sensitive
  observation.
* **`observation_met` / `observation_warnings` (scheduler-sensitive, reported):** whether
  the expected overlap was observed. The single registered expectation is the headline:
  `mode 1 @ hpx_threads = 1 → cooperative_interleaving` with `max_in_flight ≥ 2`. It is
  reported (warned if absent), never folded into `overall_structural_pass`.

## Observed (this machine; `aggregate.json` beside this note)

`overall_structural_pass = true` across all 24 rows (seeds {3,7} × quanta
{9000,16384,49152} × modes {0,1} × hpx_threads {1,2}); `value_failures = []`,
`structural_failures = []`.

| regime | observed classification |
|---|---|
| mode 0, hpx_threads 1 | `serial` (non-yielding kernel cannot be preempted on one worker) |
| mode 1, hpx_threads 1 | `cooperative_interleaving` (max_in_flight 2, one worker) — **headline met** |
| mode 0, hpx_threads 2 | `serial` (arms too short to overlap; honestly reported) |
| mode 1, hpx_threads 2 | mostly `worker_parallel` (distinct workers), one `serial` row |

The `worker_parallel` rows and the lone `serial` at threads 2 are scheduler-sensitive and
**reported only**; `mode 0` overlap is never expected. `observation_met.mode1_threads1 =
true`.

## Allowed claim

For one fixed two-arm independent fork launched with bare `hpx::async` inside a single
coarse Runtime op and joined with `when_all` below the one Python/Runtime boundary
(`overlap_probe`), exp47's load-bearing result is **Runtime/lane-boundary
faithfulness/liveness**: the nested HPX async arms schedule, join, and return the
bit-identical closed `int64` (triangulated against a pure-Python oracle and
`chain_fanout`) through the real boundary — deterministically. Separately, **when** the
debug-only witness observes both arms in flight together (`max_in_flight ≥ 2`), exp47
classifies that overlap from per-chunk `hpx::get_worker_thread_num()` samples as
`cooperative_interleaving` (consistent with one observed worker — not OS-thread
parallelism) or `worker_parallel` (distinct workers observed). The overlap observation is
conditioned on the yielding arm-kernel shape and the host scheduler, is reported
separately, and is **not** asserted to hold on every host/run.

## Required non-claims

No speedup; no throughput; no latency/performance; no HPX faster than Ray; no Ray
comparison; no real inference; no endpoint/fabric; no parcelport; no AGAS; no multi-node;
no ObjectRef/object-store; no arbitrary Python execution; no arbitrary-parallelism /
general-DAG claim; no scheduler control; no placement control; cooperative suspension is
**not** OS-thread parallelism; `worker_parallel` means only "distinct workers observed";
the witness is racy/debug-only, not scheduler state, not a synchronization primitive; no
claim that Python orchestration is "bad"; no wall-clock value is asserted.

---

## Experiment interpretation

* **Passed structurally:** value faithfulness (oracle and `chain_fanout`) and all
  deterministic witness gates, across both modes and both worker counts — nested
  independent HPX async work inside one Runtime op schedules, joins, and retires
  correctly through the lane boundary.
* **Measured result suggests:** the lane boundary does not flatten the HPX interior — a
  yielding interior cooperatively interleaves below one Runtime boundary on a single
  worker, and distinct workers can be observed at `hpx_threads = 2`. The arm-kernel shape
  (yielding) is the load-bearing condition for *observing* interleave.
* **Hypothesis supported/weakened:** strengthens the in-process "HPX inside one coarse
  Runtime boundary" story past counts — the interior keeps independent work *in flight*,
  observably, not just *fewer crossings*.
* **Remains ambiguous:** whether overlap occurs is scheduler/timing-sensitive (the lone
  `serial` row at `mode 1, threads 2`, and `serial` for short non-yielding `mode 0`
  arms). These are reported, not gated.
* **Must not be claimed:** any speedup/throughput/latency, OS-thread parallelism from the
  cooperative case, or that `worker_parallel` proves parallel execution.

## Roadmap impact

**Roadmap strengthened.** The in-process direction now has observational evidence that the
HPX interior keeps independent native work in flight below one Runtime boundary — not only
that boundary crossings are fewer. No new surface or claim was pulled forward; the future
distributed-fabric direction remains gated and untouched.

## Updated roadmap

* **In-process direction:** local scheduling, nonblocking lanes, native
  continuation/composition (exp39/40/46), barrier rendezvous (exp44),
  boundary-placement/counts (exp45/46), and now **observed in-flight overlap /
  cooperative-vs-worker classification below one Runtime boundary (exp47)**. The
  characterization arc (counts → faithfulness → overlap) is now reasonably complete for a
  single coarse op.
* **Future distributed-fabric direction:** unchanged and still gated (Ray as
  placement/bootstrap/lifecycle; HPX locality-to-locality or lighter inter-actor
  transport; endpoint discovery; remote-action prototype; multi-node comparison). exp47
  provides **no** endpoint/fabric/parcelport/AGAS/multi-node evidence and must not be used
  to advance it.

## Next recommended step

Design a **fair fixed-granularity boundary-crossing comparison** (the other branch the
post-exp44 direction named): one coarse Runtime op vs an equivalent fixed-granularity
Python-mediated orchestration, measured under an honest, pre-registered cost model — so
the project gains evidence about whether the relevant cost is boundary/orchestration
versus transport, **before** any distributed-fabric work is considered. Keep it
in-process and structural; do not let it imply fabric.
