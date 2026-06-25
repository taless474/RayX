# exp44 — witnessed barrier-gated fan-in Runtime op

**Status:** keystone integration of exp39/40/41, behind one Python/Runtime boundary
crossing. **Structural gates only — no timing/performance evidence.**

## What this shows

The **RayX Runtime boundary and lane execution model preserve cooperative HPX
scheduling** for a barrier-gated fan-in interior. A single fixed registered op,
`barrier_fanin(seed, leaves, quantum) -> int64`, reached through one Python/pybind
crossing, runs an HPX-native interior and is **load-bearing at `--hpx:threads=1`**.

| piece | exp44 interior |
|---|---|
| independent fan-out | `leaves` children launched with **bare `hpx::async` on the default pool** (so `--hpx:threads` constrains them) |
| mutual rendezvous | each child computes `chain_stage(seed+j, quantum)`, arrives, the **last arriver opens** a per-call `hpx::promise<void>` gate, all cooperatively suspend on `gate.get()`, then release |
| join | `hpx::when_all(leaf_futs)` |
| dependent reduction | `.then(hpx::launch::async, …)` folds the K leaf values under `BUSY_SUM_MASK`, running only after all leaves are released |

The op body runs inside the lane's `hpx::async(exec_, task).get()`, so every interior
`.get()` (each `gate.get()` and the terminal reduction `.get()`) is a **cooperative
suspension of the lane worker**, never an OS-thread block.

### Why exp39/40/41 individually did not show this
- exp39 (boundary cost): value computation; a sequential body reproduces the value.
- exp40 (`chain_fanout`): *independent* leaves that never gate — they serialize harmlessly
  at one worker; value-equality proves op identity, not scheduling.
- exp41 (barrier witness): cooperative gating proven, but **standalone, outside the
  Runtime boundary**.

exp44's novel contribution is the **integration invariant**: `run_as_hpx_thread` enqueue +
the lane's `hpx::async(exec_, task).get()` wrapper + Python marshalling **do not pin a
worker or serialize the barrier-gated interior** at one worker. It is a *negative-result-
resistant integration test*, not a positive HPX-value claim.

## The load-bearing fact (`--hpx:threads=1`)

With one default-pool OS worker, the body launches the leaves and suspends at the join;
the scheduler runs a leaf, which arrives then **cooperatively suspends** at `gate.get()`,
*yielding the single worker* so siblings arrive; the last arriver opens the gate; all
resume, release, and the `.then` reduction runs. A **non-cooperative** interior (OS-level
blocking wait) would pin the only worker on the first leaf → the gate could never open →
**deadlock**, caught by the op's watchdog and ultimately the experiment's external
subprocess timeout.

So a clean completion at one **observed** OS worker, with the structural witness intact,
witnesses cooperative suspend/resume of the gated leaves on a single worker — **not
parallelism**.

## Value oracle (correctness, independent of the gate)

The gate is value-neutral, so:

```
barrier_fanin(seed, leaves, quantum)
  == ( Σ_{j=0}^{leaves-1} chain_stage(seed+j, quantum) ) & BUSY_SUM_MASK
  == chain_fanout(seed, leaves, /*steps=*/1, quantum)
```

Python-checkable with no HPX type. The value is the **correctness** check; the threads=1
witness is the **mechanism** evidence — deliberately independent, so exp44 does not reduce
to "proving op identity."

## Witness schema (debug-only, mutex-guarded, racy=stale-only)

`Runtime.barrier_fanin_witness()` snapshots the **last** `barrier_fanin` execution under a
mutex (no torn read; "racy" means only that a reader may observe a stale/cross-call
value). Tests and this experiment are **single-in-flight** (one `barrier_fanin` at a time,
identified by `seq`). `barrier_fanin` is **the one side-effecting registry op**; every
other registry op stays a pure value function, and this witness touches **nothing** in
`OpOutcome` / `OperationResult` / `lane_stats()` / the v1 JSONL schema.

| field | meaning |
|---|---|
| `seq` | monotonic id of the last recorded execution |
| `observed_os_workers` | `hpx::get_os_thread_count()` at execution |
| `leaves_requested` | the `leaves` arg |
| `arrived_count` / `released_count` | leaves that reached / passed the gate |
| `opener` | `"last_arriver"` \| `"watchdog"` \| `"none"` |
| `reduction_after_all_leaves` | `released == leaves` at reduction entry |
| `ordering_violations` | defensive invariant breaches; expected `0` |
| `clean_exit` | `joined_count == leaves` |
| `watchdog_opened` | `opener == "watchdog"` |
| `joined_count` | leaves joined |
| `max_simultaneously_suspended_leaves` | **observation-only, never a gate:** peak count of leaves suspended at `gate.get()`. **Coordinated suspension, NOT parallel execution / throughput / worker-level concurrency.** At `threads=1` it can equal ~`leaves` because HPX cooperatively suspends the gated tasks on one worker. |

## Failure controls

- **External per-child subprocess timeout** — the true anti-hang guarantee. The runner
  runs each `--hpx:threads` value in a fresh child process under a hard timeout; a genuine
  cooperative-scheduling hang becomes a deterministic FAIL.
- **Internal cooperative watchdog** — defense-in-depth only (generous deadline, ≥ exp41).
  On a healthy run the last arriver opens first and the watchdog never fires; a
  **watchdog-opened success run is a structural FAILURE** (it would have masked a broken
  cooperative path).
- **Shutdown during a batch** — clean teardown, no orphan, no hang (ops are short: the
  gate self-opens at full arrival, so shutdown's cancel+drain is bounded).
- **No injected child faults**, **no mid-flight cancellation control** — the gate
  self-opens at full arrival, so there is no stable parked window; a deterministic cancel
  test would be racy. Fault signatures (skip/throw/launch-fewer) remain in exp41, the
  canonical cooperative-interleaving artifact. `barrier_fanin` is **queued-cancelable
  only** (`one_checkpoint`).

## Result (this machine)

`macOS-26.5-arm64`, full run `--hpx:threads ∈ {1,2,4}`, `leaves ∈ {2,4,8,16}`, seed=3,
quantum=64: **`overall_structural_pass = true`.** Every run passed the structural gates;
`observed_os_workers` matched the requested thread count (1/2/4); at `threads=1` all
load-bearing gates held (`arrived==released==leaves`, `opener==last_arriver`,
`watchdog_opened==false`, `ordering_violations==0`, `reduction_after_all_leaves==true`,
`clean_exit==true`). See `aggregate.json`. Timing is **not** recorded — there is no
performance claim.

## Allowed claim

> exp44 shows the RayX Runtime boundary and lane execution model preserve cooperative HPX
> scheduling for a barrier-gated fan-in interior: a single coarse-grained registered
> operation, reached through one Python/Runtime boundary crossing, launches K
> bare-`hpx::async` leaves that mutually rendezvous on a shared cooperative gate, joins
> them with `when_all`, and reduces them with a scheduled `.then` continuation —
> completing correctly and load-bearing at `--hpx:threads=1` with one observed HPX OS
> worker, with structural witness evidence: `arrived==released==leaves`,
> `opener==last_arriver`, `watchdog_opened==false`, `ordering_violations==0`,
> `reduction_after_all_leaves==true`. A non-cooperative interior would deadlock at one
> worker.

## Required non-claims

No speedup · no throughput · no latency/performance claim · no HPX faster than Ray · no
Ray comparison · no parallelism-required claim · no general DAG scheduler claim · no
general scheduler introspection · no public scheduler API · no arbitrary Python callbacks ·
no `ObjectRef`/object-store semantics · no endpoint/fabric claim · no parcelport · no AGAS ·
no multi-node · no persistent transport. The witness is debug-only/structural;
`max_simultaneously_suspended_leaves` is coordinated suspension, not parallelism.

## Files

* `python/src/rayx/runtime_ops.hpp` — `FANIN_LEAVES_MAX`, watchdog constants, the
  `BarrierFaninWitness` POD + mutex-guarded global slot/record/read.
* `python/src/rayx/runtime_ops_hpx.hpp` — the `barrier_fanin` `OpEntry`.
* `python/src/rayx/_rayx.cpp` — `Runtime.barrier_fanin_witness()` accessor.
* `python/src/rayx/runtime/_validate.py` + `runtime/__init__.py` — `FANIN_LEAVES_MAX`
  mirror, `barrier_fanin` domain validation, facade method.
* `tests/unit/test_barrier_fanin_validate.py`, `tests/integration/test_runtime_barrier_fanin.py`.
* `run_barrier_fanin_witness.py`, this write-up, generated `aggregate.json`.

## Run

```
ninja -C python/build
PYTHONPATH=python/src python -m pytest tests/unit/test_barrier_fanin_validate.py -q
PYTHONPATH=python/src python -m pytest tests/integration/test_runtime_barrier_fanin.py -q
python -m py_compile experiments/44_barrier_fanin_witness/run_barrier_fanin_witness.py
PYTHONPATH=python/src python experiments/44_barrier_fanin_witness/run_barrier_fanin_witness.py --smoke
PYTHONPATH=python/src python experiments/44_barrier_fanin_witness/run_barrier_fanin_witness.py
```

## Interpretation and roadmap impact

**Experiment interpretation.** Structurally everything passed: the value oracle holds
(`barrier_fanin == chain_fanout(seed, leaves, 1, quantum)`), and at one observed HPX OS
worker the barrier-gated interior completed cleanly with the full witness — cooperative
suspend/resume of the gated leaves on a single default-pool worker, behind one Python
boundary crossing. This is the integration invariant exp39/40/41 each lacked: the RayX
Runtime/lane/boundary machinery is **HPX-faithful** — it does not pin a worker or
serialize a barrier-gated dependent interior. It is a *negative-result-resistant*
result (passing means "the boundary didn't break cooperative scheduling"), **not** a
positive performance or HPX-value claim, and the interior is a synthetic barrier/all-reduce
shape, **not** general DAG scheduling.

**Roadmap impact: `Roadmap strengthened`.** The in-process HPX-inside-Ray-actors story now
has its keystone: fine-grained barrier-gated dependent structure can live below the coarse
Python boundary with cooperative HPX scheduling intact, load-bearing at one worker. This is
the first behind-the-boundary result a non-cooperative implementation provably could not
pass (it would deadlock) — the kind of mechanism evidence (not benchmark timing) that is
defensible to HPX reviewers.

**Updated roadmap.**

* *In-process HPX inside Ray actors:* strengthened. exp39 (boundary cost) → exp40 (intra-op
  overlap) → exp41 (cooperative scheduling) are now integrated behind one Runtime boundary
  (exp44). Performance work remains paused; the next natural question is a **fair, fixed-
  granularity comparison** of boundary crossings (a native barrier-gated interior vs a
  Python-mediated rendezvous), which exp44 makes well-defined.
* *Distributed-fabric direction:* unchanged and still gated/fenced. exp44 is in-process
  only — no endpoint, no fabric, no transport claim.

**Next recommended step.** Define a **fixed-granularity boundary-crossing comparison**
(experiment-only): one native `barrier_fanin` (one crossing, K-leaf interior) versus the
equivalent Python-mediated rendezvous that crosses the boundary per leaf, counting
**boundary crossings** (not wall-clock), to quantify the coarse-vs-fine boundary structure
honestly — still no speedup/Ray/fabric claim. Do not promote `barrier_fanin` beyond the
fixed registry, and do not start a Ray comparison or distributed-fabric work yet.
