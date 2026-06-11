# RayX for Beginners

*A guide for people who know Python and C++, but not Ray, HPX, or pybind11.*

---

## 1. What is RayX?

RayX is a small, experimental research project that explores one question:

> **Can a native C++ runtime (built on HPX) serve as a low-overhead execution backend for Python, for the kind of control-plane work a model-serving system does — queuing, dispatching, cancelling, and bounding requests?**

It contains two things:

1. A **benchmark harness** that compares the control-plane overhead of Ray (a popular Python distributed framework) against HPX (a C++ parallel runtime), using deliberately *synthetic* workloads.
2. An **experimental mini-runtime** (`rayx.runtime`) that runs a small, fixed set of *native C++ operations and actors* behind a Python API, to explore what an honest HPX-backed runtime would actually look like.

Before going further, here is what RayX is **not** — and the project is unusually strict about this:

- **Not Ray, and not a Ray replacement.** It borrows some API *shapes* from Ray (futures, `wait`, actors) because they are good ergonomics, nothing more.
- **Not distributed yet.** Everything runs in one process on one machine.
- **No object store.** There is no `ObjectRef`, no shared memory store, and no "put a value in, pass a reference around." Heavy data is meant to live inside native C++ actors and be operated on there. A bounded copied-bytes value type may be considered later if a real downstream example needs it, but that would still be plain data in/out of a call — not references, not shared storage, and not an object store.
- **No arbitrary Python execution.** You cannot send a Python function to the runtime. Only pre-compiled, registered C++ operations run.
- **No model inference.** No Ray Serve, no Ray Train, no LLMs. "Service time" in the benchmarks is a synthetic sleep or busy-loop, never real work.

---

## 2. The mental model 

Think of the runtime as a **tiny post office**.

- A **lane** is one teller window with one line in front of it. The teller (a single worker thread) serves exactly one customer at a time, strictly in the order they arrived (**FIFO**).
- **Submitting an operation** means joining the line at some window. You immediately get a **ticket** (a *future*) that you can later trade in for the outcome.
- **Bounded admission** means the line has a maximum length. If it's full, you're turned away at the door (`QueueFullError`) — you never get a ticket at all.
- **Queued cancellation** means tearing up your ticket while you're still in line: the teller will skip you entirely.
- **Running cancellation** means your job is already at the window, but it's a long job with natural pause points (**checkpoints**). You can ask it to stop at the next pause point. You can never yank a job out of the teller's hands mid-task.
- An **actor** is a window with a private ledger behind it (native C++ state). All requests touching that ledger go through *that* window, one at a time — which is exactly what makes the state safe without locks.
- **Shutdown** closes the post office in an orderly way: outstanding work is cancelled where possible, every ticket-holder still gets *some* answer (no ticket is ever left worthless), and only then do the tellers go home.

Layered, the system looks like this:

```
your Python code
      │
      ▼
rayx.runtime (pure Python)      ── validation, futures, results
      │
      ▼
_rayx (pybind11 extension)      ── the Python ↔ C++ boundary
      │
      ▼
RuntimeEngine (C++)             ── round-robin over lanes, actor map
      │
      ▼
RuntimeLane × N (C++/HPX)       ── one FIFO queue + one worker each
      │
      ▼
HPX runtime                     ── schedules lightweight threads over OS threads
```

---

## 3. Minimal background

You don't need to know these systems deeply, but you need four ideas.

**What is Ray?** Ray is a Python framework for distributed computing. You decorate a Python function or class, call `.remote()`, and Ray runs it in another *process* (possibly another machine), giving you back a future-like handle. It's enormously convenient, but every call crosses a process boundary: arguments are serialized, sent over IPC, results go into a shared object store. That boundary has a cost, and measuring that cost is where RayX started.

**What is HPX?** HPX is a C++ runtime for parallelism. Its key trick: instead of mapping each task to an OS thread, it runs many lightweight **HPX threads** on a small pool of OS worker threads (an "M:N" scheduler). HPX threads are *cooperative*: when one waits — on a future, a sleep, a lock — it can *yield* its OS worker so another HPX thread runs. HPX also has its own versions of familiar primitives: `hpx::async`, `hpx::future`, `hpx::mutex`, `hpx::thread`. One important rule that shapes a lot of RayX's code: HPX's mutexes and condition variables may only be used *from* HPX threads. Regular OS threads (like the Python interpreter's thread) have to "hop" onto an HPX thread first (`hpx::run_as_hpx_thread`) before touching them.

**What is pybind11?** A C++ library for writing Python extension modules. You write a C++ class, add a few binding declarations, and Python can construct and call it as if it were a Python class. RayX uses it to build the `_rayx` module. The one subtlety that matters here is the **GIL** (Python's Global Interpreter Lock): when C++ code is about to block for a long time (waiting on a future), it must *release* the GIL so other Python threads can run — and it must never touch Python objects while the GIL is released. RayX is careful about this everywhere: no Python object is ever created or touched on an HPX worker thread.

**What is a future?** A handle to a result that doesn't exist yet. You submit work, you get a future immediately, and later you ask the future for the result (blocking until it's ready) or just ask "is it ready?". In RayX, futures are *consume-once*: you retire each future exactly once.

**What is an actor?** In this project: a long-lived object with private state, where all method calls are executed one at a time, in order. The serialization is the point — if only one method ever runs at a time, the state can never be corrupted by concurrent access.

**What is a "lane"?** RayX's own term: one FIFO queue plus one dedicated worker that drains it serially. A lane is the project's unit of serialization — and, in the actor design, a lane *is* an actor's mailbox.

---

## 4. The two worlds inside RayX

The repo contains two clearly separated systems. They share exactly one thing — a guard that says "only one of us may run HPX in this process at a time" — and nothing else.

### World 1: the benchmark harness (`Engine` / `SyntheticActor`)

This is the original project: a fair, narrow comparison of *control-plane overhead* between Ray and HPX. It runs only **synthetic** work — "sleep for 5 ms" or "spin the CPU for 5 ms" — because the point is to measure the cost of the machinery *around* the work (queuing, dispatch, futures, the Python boundary), not the work itself.

Its results are **measurement rows**, not values: each request produces a dict of timestamps and status, which feeds a JSONL metrics pipeline and analyzer. The headline finding (with caveats spelled out in the README): for tiny/no-op requests, the in-process HPX path has dramatically lower per-call overhead than Ray's cross-process actor path, and the Python frontend over HPX keeps most of that advantage. Once requests do ~20 ms of real work, all three converge — the overhead stops mattering. That is the *honest* version of the result; it is not "HPX is faster than Ray."

### World 2: the experimental runtime (`rayx.runtime`)

The harness can only *time* fake work. The natural next question was: what would it look like if the runtime actually *computed something*? `rayx.runtime` is that exploration. It runs **real (but fixed) native C++ operations** that return real **values**, plus local native **actors** with real C++ state — over HPX-native lanes.

Why keep both? Because they answer different questions. The harness answers "what does the boundary cost?" with a frozen, comparable measurement contract. The runtime answers "what are honest semantics for a native-backed runtime?" — values, cancellation, admission, actor state — without contaminating the benchmark world. The two never share result types or schemas, on purpose.

---

## 5. The central design: `RuntimeLane`

If you read one C++ file in this repo, read `python/src/rayx/runtime_lane.hpp` (~320 lines, half of it comments explaining invariants). A `RuntimeLane` is:

```
            submit (from Python, via a hop onto an HPX thread)
               │
               ▼
   ┌───────────────────────────┐
   │  FIFO queue (owned deque) │   guarded by hpx::mutex
   └───────────────────────────┘
               │  pop one at a time
               ▼
        one HPX worker thread
               │  runs the operation, waits for it cooperatively
               ▼
        fulfills that item's future, then pops the next
```

The important design decisions, and why:

- **One owned queue.** The lane *owns* its queue as a plain data structure. It's the load-bearing choice: because the queue is a real, inspectable object under one mutex, the lane can offer (a) an exact `queue_depth` snapshot, (b) **bounded admission** — "check the depth and push, atomically, under one lock" so the cap can never be raced past — and (c) **queued cancellation** — a cancelled item is simply skipped when the worker pops it. Alternative designs (chaining HPX continuations, throwing tasks into a thread pool) have no owned queue, so none of those features can exist honestly. The project actually *tested* this (experiment 20): scheduler pools dissolve FIFO order and lane identity. The lane is the center of gravity because the serving-control features all hang off the owned queue.
- **One HPX worker per lane.** The worker is a single long-lived HPX thread. While the queue is empty it waits on an HPX condition variable — which *suspends the HPX thread and frees the underlying OS worker* for other work. A regular `std::condition_variable` here would pin an OS worker per idle lane.
- **FIFO is the contract, not an accident.** The worker fully finishes one item (fulfills its future) before popping the next. For actors, this one-at-a-time property is what makes lock-free native state safe.
- **Cancellation hooks** (next two sections) are integrated into the pop loop and the operation body, not bolted on.
- **Shutdown drains, never abandons.** On shutdown, the runtime first *cancels* everything outstanding (queued items get skipped; a running checkpointed op stops at its next checkpoint), then the worker drains the rest and exits. Every future that was ever handed out gets fulfilled with *something* — completed, failed, or cancelled. No ticket is ever left worthless.

---

## 6. Registered native operations

The runtime does **not** accept Python functions. Instead there is a fixed, compiled-in registry of operations — a C++ map from a string name to a native function plus a typed signature:

| op | what it does | why it exists |
|---|---|---|
| `square(x)` | `x * x` | simplest possible real value |
| `add(a, b)` | `a + b` | two-argument shape |
| `boom()` | always throws | exercises the failure path (`status="failed"`) |
| `busy_sum(n)` | masked sum of `0..n-1`, in chunks | real CPU work with **cancellation checkpoints** |
| `fanout_sum(n, parts)` | same sum, internally split across HPX tasks | demonstrates HPX composition *inside* one op |
| `scale_double(x, f)` | `x * f` | the first floating-point op |

The **value model is closed**: arguments and results are only `int64` or finite `double`. Python `bool` is rejected (it's an `int` subclass and almost always a mistake), out-of-range ints are rejected, `NaN`/`inf` are rejected — all at the Python boundary, *before* anything crosses into C++, with clear `TypeError`/`ValueError` messages.

Why fixed native ops instead of "just let me pass a lambda"? Three honest reasons:

1. **The GIL.** Running arbitrary Python on the HPX workers would drag the Python interpreter lock into the native runtime, destroying the one structural advantage (GIL-free native execution) the project exists to study.
2. **Cancellation.** You cannot safely interrupt arbitrary code. A *registered* op can be written to cooperate (checkpoints); a random callback cannot be killed honestly.
3. **Scope honesty.** Arbitrary callables plus value passing is the slippery slope toward "a worse Ray." The fixed registry keeps the project what it claims to be.

`fanout_sum` deserves one note: it splits its work across multiple HPX tasks *internally* (`hpx::async` per part, `hpx::when_all` to join), but from Python it is still one operation, one future, one result row. The parallelism is an implementation detail that never leaks into the API. And because modular sums are associative, `fanout_sum(n, parts) == busy_sum(n)` for every `parts` — the op is its own correctness check.

---

## 7. Local native actors

The newest piece. An actor in `rayx.runtime` is:

- **Native state**: a real C++ struct. The only shipped type is `counter`, whose entire state is one `std::int64_t`.
- **A fixed method set**, registered in C++ exactly like ops: `add(delta)`, `get()`, `reset(value)`, and `busy_get(work_n)`.
- **Its own dedicated lane.** Creating an actor creates one `RuntimeLane` just for it. The lane *is* the actor's mailbox and serialization domain: methods run one at a time, in call order, so the state needs no locks at all — FIFO retirement *is* the synchronization.

```python
c = rt.create_actor("counter", 0)   # native state created, dedicated lane spawned
c.call("add", 5).result().value     # 5
c.call("add", 3).result().value     # 8
c.call("get").result().value        # 8
```

Things deliberately **absent**, and why:

- **No `.remote()`, no `c.add(5)` attribute dispatch.** The only call surface is `c.call("add", 5)` with an explicit method name. Attribute-style dispatch would *look* like an open-ended method set (like a real Python object); the explicit string makes the closed registry visible in the API itself. `.remote()` is avoided because it's Ray's vocabulary and would imply Ray's semantics (object store, task scheduling) which don't exist here.
- **No Python state, no pickle.** The counter is an `int64_t` in C++, not a Python object held hostage across a boundary.
- **No per-actor delete yet.** Actors live until the runtime shuts down. Known limitation, on the roadmap.

**What is `busy_get(work_n)`?** It's `busy_sum`-style CPU work routed *through an actor*: it grinds through `work_n` steps of synthetic arithmetic (with cancellation checkpoints), then returns the counter's **current value, unchanged**. It exists for exactly one reason: to prove the running-cancellation machinery works through the actor dispatch path. It is **read-only** on purpose — a *cancellable mutator* (imagine `busy_add` stopping halfway through a multi-step state update) would raise the genuinely hard question of partial state, which needs transactional thinking the project hasn't done. A read-only method sidesteps that entirely: cancel it at any checkpoint and the state is provably untouched. It is diagnostic scaffolding, not a benchmark.

---

## 8. The cancellation model

This is the most carefully engineered part of the codebase, so it deserves a plain-language explanation.

Every cancellable submission carries a small **token** — a five-state machine guarded by a mutex:

```
 Queued ──cancel()──────────▶ Cancelled        (skipped entirely; future fulfilled now)
 Queued ──worker picks it──▶ Running
 Running (has checkpoints) ──cancel()──▶ StopRequested
 StopRequested ──next checkpoint──▶ Cancelled  (op stops; future fulfilled by the lane)
 Running ──finishes──▶ Completed               (terminal; a late cancel returns False)
```

The rules, in order of importance:

1. **Queued cancel always works.** If your item hasn't started, `cancel()` returns `True`, the future is fulfilled immediately with `status="cancelled"`, and the worker skips the item when it reaches it. Zero work was done.
2. **Running cancel only works at checkpoints.** Instant ops (`square`, `add`) have no pause points — once running, `cancel()` returns `False`. Long ops (`busy_sum`, `busy_get`) check the token between every chunk of ~8192 steps; a running cancel makes them stop at the next chunk boundary.
3. **`cancel() == True` is a promise.** If `cancel()` returns `True`, the final result row *will* say `status="cancelled"` — guaranteed, no exceptions. The trickiest race (cancel arriving just as the op commits to its final chunk) is resolved *inside one critical section*: before starting the last chunk, the op atomically turns off its own cancellability, so a late `cancel()` deterministically returns `False` instead of lying. The tests assert exactly this equivalence, repeatedly, under racing conditions.
4. **Cancellation is cooperative, never force-kill.** Nothing is ever interrupted mid-instruction; there's no thread kill anywhere. This isn't squeamishness — force-killing native code with live state is how you corrupt memory. The honest contract is "guaranteed to stop at the next safe point," not "stopped right now."

A cancelled result is still a result: the future resolves, the row reads `status="cancelled"`, and only accessing `.value` raises (`OperationCancelledError`).

---

## 9. The Python API in five minutes

Everything lives in `rayx.runtime`, imported explicitly (it's deliberately *not* re-exported from top-level `rayx`):

```python
from rayx.runtime import Runtime, QueueFullError

# One Runtime per process (it owns the HPX runtime). Context manager = clean shutdown.
with Runtime(num_lanes=2, hpx_threads=4) as rt:

    # --- submit an operation, get a future, retire it ---
    fut = rt.submit_operation("square", 7)
    res = fut.result()          # blocks (GIL released in C++) — consume-once
    print(res.value)            # 49           ← the user value
    print(res.row["status"])    # "completed"  ← the measurement row
    print(res.row["service_ms_observed"])      # timing metadata

    # --- many futures: wait / as_completed (like ray.wait, shape-wise) ---
    futs = [rt.submit_operation("busy_sum", 5_000_000) for _ in range(8)]
    ready, pending = rt.wait(futs, num_returns=1)   # blocks until ≥1 ready
    for f in rt.as_completed(pending):              # yields as they finish
        print(f.result().value)
    for f in ready:
        print(f.result().value)

    # --- a native actor ---
    c = rt.create_actor("counter", 100)
    print(c.call("add", 5).result().value)    # 105
    print(c.call("get").result().value)       # 105

    # --- cancellation ---
    f = rt.submit_operation("busy_sum", 2_000_000_000)
    if f.cancel():                             # True ⇒ row WILL be "cancelled"
        r = f.result()
        assert r.row["status"] == "cancelled"  # guaranteed
        # r.value would raise OperationCancelledError
```

Two design points worth internalizing:

**`OperationResult` separates value from row.** `.value` is what the operation computed; `.row` is a 9-field dict of measurement metadata (timestamps, lane id, status, error). The value is *never* inside the row, by contract and by test. Why? Because the project's whole methodology depends on measurement rows being a stable, value-free schema — and because `.value` can *raise* (on failed/cancelled outcomes) while `.row` must always be safely inspectable. You can always ask "what happened?"; you can only ask "what's the answer?" when there is one.

**Errors arrive at the right moments.** Bad arguments fail *before* anything is submitted (`TypeError`/`ValueError` at the Python boundary — a rejected call provably never touches actor state). A full lane raises `QueueFullError` at submit time, and no future is created. An op that *threw* during execution gives you a normal result whose row says `failed` — only `.value` raises. Nothing is ever half-submitted.

---

## 10. How it's tested

The testing philosophy is worth learning from even if you never run the suite:

- **Two tiers, split by what they need.** `tests/unit/` are *import-light*: they load the validation module directly by file path, never importing the compiled extension — so they run anywhere, including CI machines with no HPX and no C++ toolchain. `tests/integration/` need the built `_rayx` module and skip themselves cleanly when it's absent.
- **No sleeps, no timing assertions.** A test that says "sleep 50 ms, then assert X finished" is a flake factory. Instead, tests gate on *observable state* (polling `lane_stats()` until a lane reports `active`) or rely on determinism *by construction* (a single FIFO lane plus a single submitting thread means the execution order is fixed regardless of scheduling).
- **Races are tested with invariants, not by forcing a winner.** Example: cancel an instantaneous actor call 64 times in a loop. The test *can't* control whether cancel wins the race each time — so it asserts the property that must hold *whoever wins*: `cancel()` returned `True` if and only if the row says `cancelled`, and every completed `add` advanced the counter by exactly one. The race stays random; the invariant stays deterministic.
- **Honest scope is written down.** Some actor tests carry explicit "HONEST SCOPE" notes saying what they do *not* prove. A good example of the discipline: for a while there was no way to observe an actor's lane (no per-actor `queue_depth`/`active` snapshot), so a deterministic "actor lane is full → `QueueFullError`" test was impossible without timing assumptions — and the project chose to *omit* the test and document why, rather than ship a flaky one. That gap has since been closed: `ActorHandle.stats()` now provides the snapshot, and the deterministic tests it unlocks exist (still with written honest-scope notes about the residual margins they rest on).

---

## 11. What the independent audit concluded

A recent independent HPX/runtime-systems audit (internal audit notes, kept as local provenance rather than tracked repo docs) reviewed the whole project. In beginner terms:

- **The architecture is sound and the direction is right.** No better overall design was identified — notably because the team didn't just argue for the lane design, they *experimentally disproved* the alternatives (e.g., showing that HPX task pools destroy the FIFO/identity guarantees the API promises).
- **`RuntimeLane` is correctly the center of everything.** The owned queue is what makes admission, cancellation, and observability honest.
- **The concurrency and lifecycle machinery is correct.** The audit traced the cancellation state machine, the promise-fulfillment paths, and the shutdown ordering and found no double-fulfillment, no abandoned futures, no thread-lifetime hazards, no lock-ordering cycles.
- **The weak spots are second-order:** (1) actor lanes aren't observable, which is now the main limit on test quality; (2) some C++ comments have gone stale relative to the code (in a project where comments *are* the spec, that matters); (3) the documentation has grown so many repeated disclaimers that the signal gets buried.
- **One real (minor) defect:** the *older harness* `Engine` constructor isn't exception-safe partway through construction — a failure there could leave the process unable to create any new engine. The newer `RuntimeEngine` handles the same situation correctly; the fix is to back-port that handling.
- **One sharp observation:** every runtime op today is CPU-bound or instantaneous — nothing ever *parks cooperatively*. So the one thing HPX is structurally best at (many idle lanes sharing few OS threads) isn't yet demonstrated by any runtime workload. The machinery is right; the demo is missing.

---

## 12. Where it's going

The accepted near-term plan, in one breath: make actor lanes *observable* (since landed as `ActorHandle.stats()`, unlocking the deterministic actor tests that were previously impossible), do a hygiene pass (a known constructor defect, stale comments, duplicated code), and add one cooperatively *parking* operation so the runtime can demonstrate HPX's many-lanes-on-few-threads value instead of just asserting it. Beyond that sit possible later steps (actor release, batch submission, a `bytes` value type) and a firm "never" list (object store, Ray compatibility, arbitrary Python actors, distributed claims).

This guide deliberately doesn't repeat the full roadmap — for the current, authoritative version, read [`docs/design/rayx_target_environment.md`](design/rayx_target_environment.md) (the target-environment strategy and its staged expansion, which incorporates the roadmap accepted from the internal audit notes).

---

## 13. Common misunderstandings

- **"RayX makes Ray faster."** No. RayX doesn't touch Ray at all — it *measures* Ray as a baseline, using only Ray's public API.
- **"`rt.get(...)` is like `ray.get(...)`."** Shape, yes; semantics, no. Ray's `get` fetches a value from a distributed object store. RayX's `get` retires a local future and returns a value-plus-measurement-row pair. There is no store.
- **"The headline benchmark shows HPX beats Ray."** It shows that for *near-zero-work requests in one process*, an in-process C++ path has less per-call overhead than a cross-process path — which is almost a tautology, and the docs say so. With 20 ms of real work per request, the difference disappears.
- **"`cancel()` kills the task."** Never. It either skips not-yet-started work, or asks running work to stop at its next checkpoint. `True` means "guaranteed to stop," not "stopped."
- **"`actor_id` means an actor produced it."** Confusingly, every lane — including plain operation lanes — stamps an `actor_id` on its rows (a naming inheritance from the harness's "lane ≈ actor" framing). Real actors are distinguishable by the `rt-act-` prefix vs `rt-hpx-` for op lanes.
- **"I can register my own operation from Python."** No. The registry is compiled C++. Adding an op means editing a header and rebuilding — by design.
- **"`service_ms` / `busy_sum` is real work."** All workloads in this repo are synthetic timing/arithmetic shapes. Nothing infers, trains, or serves anything.
- **"Two `Runtime`s for twice the throughput."** One per process, enforced. HPX is a process-wide resource; the harness `Engine` and the `Runtime` are also mutually exclusive with each other.

---

## 14. What to read next in the repo

In rough order:

1. **`readme.md`** — the project framing, the honest headline result, and the documentation map.
2. **`examples/rayx_runtime_basic.py`** — a runnable tour of everything in §9.
3. **`python/src/rayx/runtime/__init__.py`** — the Python layer; the docstrings are effectively the API reference.
4. **`python/src/rayx/runtime_lane.hpp`** — the core. Read the long header comment first; it explains *why* before *what*.
5. **`python/src/rayx/runtime_cancel.hpp`** — the cancellation state machine; short, and the state diagram in §8 comes straight from it.
6. **`python/src/rayx/runtime_ops.hpp`** and **`runtime_actor_ops.hpp`** — the op and actor registries; see how little it takes to define an op.
7. **`tests/integration/test_actor_contract.py`** — the actor contract, including the invariant-based race tests and the "HONEST SCOPE" notes.
8. **`docs/design/rayx_runtime_problem_model.md`** — what the runtime is trying to become, and the decisions already locked.
9. **`docs/design/rayx_target_environment.md`** — the target-environment strategy and staged roadmap (which absorbed the accepted conclusions of the internal audit notes; those notes themselves are local provenance, not tracked).
10. **`docs/reference/hpxlane_backend_arc.md`** — if you want the harness/benchmark side: how the HPX-cooperative lane was validated as a backend, experiment by experiment.

---

*This guide describes the project as of the C5 slice (`busy_get`, June 2026). The runtime is experimental; semantics may change, and nothing here is a stability promise.*
