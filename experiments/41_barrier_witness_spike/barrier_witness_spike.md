# exp41 — Cooperative gate / barrier witness spike

**Status:** experiment-only spike. No Runtime registry op, no endpoint bridge, no
public API, no README evidence bullet, no commits. Structural gates only.

## Question

Can multiple HPX child tasks make *coordinated cooperative progress* through a
shared synchronization point — each suspending mid-execution, the scheduler
running the others, the last arriver opening the gate, all resuming and
completing — **using only confirmed HPX primitives**, with no
`hpx::latch` / `hpx::barrier` / `hpx::counting_semaphore`?

This is a cooperative-scheduling witness, **not** a parallelism test. The
load-bearing signal is the **success case at `--hpx:threads=1`**: with a single
OS worker, a non-cooperative (OS-level blocking) gate wait would pin that worker
and deadlock before all children could arrive. A clean pass at one worker
therefore witnesses cooperative **suspension + resume + scheduler interleaving**.

## What was built

* `hpx_impl/hpx_barrier_witness_spike.cpp` — standalone HPX probe.
* `hpx_impl/CMakeLists.txt` — one `add_executable` target (`HPX::hpx`,
  `HPX::wrap_main`).
* `experiments/41_barrier_witness_spike/run_barrier_witness_spike.py` — wrapper
  that runs the probe per `--hpx:threads` value under a hard subprocess timeout.
* `experiments/41_barrier_witness_spike/aggregate.json` — curated evidence.

No production code was touched.

## Confirmed primitives used

| Primitive | Used | Available |
| --- | --- | --- |
| `hpx::async` | yes (launch children) | yes |
| `hpx::when_all` | yes (join all children) | yes |
| `hpx::promise<void>` | yes (the gate; local promise, as in `runtime_lane.hpp`) | yes |
| `hpx::shared_future<void>` + `.get()` | yes (cooperative gate wait) | yes |
| `hpx::this_thread::sleep_for` | yes (cooperative coordinator poll) | yes |
| `std::atomic<int>` / `std::chrono` | yes | yes |
| `hpx::this_thread::yield()` | **probed only, not relied on** | **yes** (compiled + ran) |
| `hpx::future::wait_for` + `hpx::future_status::ready` | **probed only, not relied on** | **yes** (compiled + ran) |

The witness depends on **neither** `yield` nor `wait_for`: children block on a
plain cooperative `gate.get()`, and the coordinator uses `sleep_for`. Both were
probed once and reported (`yield_available`, `wait_for_available` = `true`) per
the experiment plan. No `hpx::latch` / `hpx::barrier` / `hpx::counting_semaphore`
was used.

## Gate / opener algorithm (exact)

Per round, fresh `hpx::promise<void>` gate, fresh atomics. The gate is opened by
an **idempotent CAS** (`opened` false→true); the CAS winner records the `opener`
and is the sole caller of `set_value()`.

Each child does, **in strict order**:

1. **arrive** — `got = arrived.fetch_add(1) + 1`;
2. **open iff last** — `if (got == participants_expected) open_gate(LAST_ARRIVER)`;
3. **wait** — `gate.get()` (cooperative suspend until opened);
4. **release** — `released.fetch_add(1)`.

Never wait before the open check (that would deadlock the single worker, since
the last arriver would block before opening).

**Opener attribution (approved refinement #1):** the coordinator opens the gate
**only on the deadline/failure path**. On success it exits its poll loop because
`arrived >= N` and does **not** open — so the last arriver is the unambiguous
opener and there is no coordinator-vs-last-arriver race at `--hpx:threads >= 2`.

## Watchdog strategy

The coordinator (main, an HPX thread) polls `arrived` with a cooperative
`sleep_for(poll_ms)` — yielding the worker so children run (critical at
`hpx_threads=1`). If `arrived` reaches `N`, it stops without opening. If the
deadline (`--deadline-ms`, default 500 ms) is hit first, it opens the gate
(`WATCHDOG`) so no child is left orphaned, then joins every launched child with
`hpx::when_all(...).get()`.

**Two-layer anti-hang (approved refinement #2):**

* **In-HPX watchdog** rescues only **logic-failure** rounds — some expected
  participant never arrives, but the children that *did* arrive are cooperatively
  suspended and the worker is free, so the watchdog runs.
* **External subprocess timeout** (in the Python wrapper, default 30 s safety
  bound) is the **only** real guarantee against a true cooperative-scheduling
  **mechanism** failure: at `hpx_threads=1`, if a gate wait ever blocked the
  single OS worker, the in-HPX watchdog — also an HPX thread on that worker —
  would never run. The wrapper converts such a hang into a deterministic
  `FAIL_TIMEOUT`, never an experiment-wide hang. The timeout is a **safety bound
  only**, never timing/performance evidence.

## Forced-failure rounds

| Case | Shape | Expected signature |
| --- | --- | --- |
| `fail_skip` | child 0 returns before arrival | `arrived=N-1`, `released=N-1`, opener=watchdog, errors=0 |
| `fail_throw` | child 0 throws **inside the `hpx::async` callable** | `arrived=N-1`, `released=N-1`, opener=watchdog, errors=1 |
| `launch_fewer` | launch `M = N-2 < N` children | `arrived=M`, `released=M`, opener=watchdog, errors=0 |

The throw is inside the async callable so the exception is captured in the child
future and surfaced through `when_all(...).get()` (counted as `child_errors`),
never escaping on the main thread. All cases must: reach the deadline, have the
coordinator open the gate, join all launched children, report failure
structurally, and exit cleanly.

## Results

Built clean; ran at `--hpx:threads=1` and `2` (`participants=8`,
`deadline_ms=500`). All four rounds matched their expected signatures at both
thread counts; aggregate `overall_pass = true`.

| thread(s) | success | fail_skip | fail_throw | launch_fewer | run |
| --- | --- | --- | --- | --- | --- |
| 1 | arrived 8/8, released 8, opener last_arriver | 7/8, wd | 7/8, wd, err 1 | 6/6 (M<N), wd | **PASS** |
| 2 | arrived 8/8, released 8, opener last_arriver | 7/8, wd | 7/8, wd, err 1 | 6/6 (M<N), wd | **PASS** |

* **Success passed at `--hpx:threads=1`** — the load-bearing witness. ✅
* **Success passed at `--hpx:threads=2`.** ✅
* External timeout path validated separately (`--timeout-s 1` against the
  ~1.5 s run → deterministic `FAIL_TIMEOUT`, exit 1, no hang).
* `yield_available = true`, `wait_for_available = true`.

## Commands

```
ninja -C hpx_impl/build hpx_barrier_witness_spike
python experiments/41_barrier_witness_spike/run_barrier_witness_spike.py \
    --out experiments/41_barrier_witness_spike/aggregate.json
# anti-hang path check (not curated):
python experiments/41_barrier_witness_spike/run_barrier_witness_spike.py \
    --threads 1 --timeout-s 1
```

## Interpretation

**What passed structurally:** multiple HPX tasks arrived at a shared gate,
cooperatively suspended, were interleaved by the scheduler, and all resumed and
completed when the gate opened — at a **single worker**. Forced-failure rounds
were deterministic, watchdog-opened, fully joined, and exited cleanly.

**What it supports:** HPX cooperative suspension / resume / interleaving through a
shared synchronization point is available via plain `hpx::promise<void>` +
`shared_future<void>`, with no specialized sync type.

**What it does NOT claim:** no parallelism (atomic increments cannot distinguish
parallel from interleaved execution), no multi-worker requirement, no speedup, no
throughput / latency / performance. Even `threads=2` success does not prove
parallel release. This is a mechanism witness only.

## Roadmap impact

**Classification: Roadmap strengthened (Track A).** Adds a confirmed cooperative
gate/barrier primitive to the in-process HPX-inside-actors toolbox, on top of the
exp39 native-continuation and exp40 native-fanout/overlap evidence — without
touching the Runtime registry or endpoint bridge, and without any performance
claim. The distributed-fabric direction remains gated.

**Caveats about promotion to a Runtime op later:** a future `barrier`/`gate`
Runtime op would need (a) a registered, closed value model and bounded
`participants`; (b) the anti-hang guarantee re-homed — a Runtime op runs on a lane
worker, so a per-op cooperative deadline plus a non-worker-blocking failure path
must be designed in, since there is no external subprocess timeout around a live
in-process Runtime; (c) cancellation-token integration at the gate wait; and
(d) honest framing as cooperative coordination, never parallelism. Not in scope
here.

## Promotion decision

**Decision: exp41 remains experiment-only. Promotion is deferred.**

* exp41 stays a standalone HPX probe; nothing is added to the Runtime registry
  (`hpx_registry()`) and no endpoint bridge op-code is added.
* **Reason:** the strongest exp41 evidence is the probe's *structural* output —
  `arrived`, `released`, `opener`, `watchdog_opened`, `joined`, `clean_exit`, the
  forced-failure signatures, and the external-timeout behavior. A registered
  Runtime op can return only a **closed `int64` sentinel**, which cannot carry any
  of that. A black-box `int64` result (e.g. `== participants`) is satisfiable by a
  non-cooperative implementation, so it would prove only that an **audited
  cooperative-gate body survives lane dispatch** — strictly *weaker* than the
  standalone artifact, and not a stronger cooperative-interleaving proof.
* **Endpoint bridge promotion is also deferred** (same limitation, plus a bridged
  sentinel adds only socket plumbing — the bridge-v2/v3 reasoning).
* **exp41 is therefore the canonical cooperative-interleaving evidence.**

If promotion is revisited later it must be **design-first** and must resolve, at
minimum:

* in-process hang safety **without** an external subprocess timeout (the in-HPX
  watchdog cannot rescue a true mechanism failure at `hpx_threads=1`);
* cancellation-token / Runtime-and-bridge shutdown behavior at the gate;
* bounded `participants`;
* no overclaiming beyond cooperative coordination (never parallelism,
  multi-worker scheduling, speedup, throughput, latency, or performance).

**Next recommended step:** treat exp39/40/41 as sufficient Track A
cooperative-scheduling evidence and, rather than promote a bare mechanism
demonstrator into the op registry, characterize the Python↔Runtime boundary cost
of an existing composed op (e.g. `chain_fanout`) — a registry/evidence question,
not a new gate-op build.
