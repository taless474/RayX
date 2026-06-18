# Ray actor hosting a rayx.runtime.Runtime + local native counter (composition feasibility, observation-only)

The local-native-actor counterpart of experiment 28
(`28_ray_hosting_rayx_runtime/`). A long-lived **Python `@ray.remote` actor**
constructs exactly one HPX-native `rayx.runtime.Runtime` **and** one
`Runtime.create_actor("counter", initial)` in `__init__`, then dispatches the
fixed registered counter methods (`add` / `get` / `reset`) on it locally. Ray
owns only the outer actor placement/lifecycle; `rayx.runtime.Runtime` owns the
local native counter actor inside the actor process. This note records that the
two **compose cleanly** — nothing more.

**What this is.** A smoke-only feasibility result: one Ray actor hosts one
Runtime plus one local native `counter`, evolves its state
(`initial → add → get → reset → get`), and returns only plain Python
scalars/containers, such as ints, bools, strings, tuples, dicts, or None. State
persistence, two-actor independence, idle release, and clean shutdown are
exercised by `bench/smoke_ray_hosting_rayx_runtime_counter.py`.

**What this is *not*.** Not a performance result — the smoke emits **no JSONL**,
asserts **no timing**, and makes no speed claim. It is **not** Ray Serve, not an
object store, not arbitrary remote Python, not a Ray backend or fallback layer,
and not a Ray compatibility API. The hosted runtime grows **no** Ray semantics
here: the `ActorHandle`, `RuntimeFuture`, and `OperationResult` are all created
and retired **inside** the Ray actor; only plain Python scalars/containers
(such as ints, bools, strings, tuples, dicts, or None) cross the Ray
boundary — **no** `ActorHandle` / `RuntimeFuture` / `OperationResult` ever
crosses, and there is **no** `ray.get` wrapper over a RayX future and **no**
`.remote()` actor API.

## Lifecycle constraint (the load-bearing fact)

The HPX/RayX runtime is a **process-global singleton** (`python/src/rayx/_rayx.cpp`:
a single `process_runtime_active()` guard shared by `rayx.Engine` and
`rayx.runtime.Runtime`). A Ray actor is its own worker process, so the intended —
and only viable — shape is **one Runtime (and its local native actors) per
long-lived Ray actor process**:

* The counter is created once in `__init__` over the hosted Runtime; its
  `ActorHandle` never leaves the actor process.
* `Runtime.release_actor(counter)` is an **explicit, local** release (not
  `ray.kill`, no distributed ownership, no refcounting): after it, a later
  `counter.call(...)` raises inside the actor
  (`RuntimeError: actor has been released`), surfaced across Ray as a plain
  error-marker dict (`{"raised": true, "error": ...}`).
* The Runtime is **graceful-drained** via an explicit `shutdown()` (idempotent);
  a method call after shutdown raises and propagates through Ray.

The in-flight cancel-then-drain release path (queued calls cancelled, a running
checkpointed method stopped at its next boundary) is **not** re-proven across the
Ray boundary — it is already covered by `bench/smoke_rayx_runtime.py` and
`tests/integration`. This slice keeps release **idle/simple**.

## Multi-actor sanity (smoke-level)

The smoke also checks two Ray actors, each owning its own Runtime + counter
(`hpx_threads=1` each; Ray `num_cpus=4` budgets the default `num_cpus=1` actors):
the counters have **distinct** `rt-act-` ids, a mutation on one actor's counter
(`add`) does **not** affect the other's state, and both shut down cleanly. This
confirms the process-singleton holds *per process* (one Runtime per actor
process; several actors are fine) and that local native actor state is isolated
per Ray process. It is a **structural** pass only — no resource budgeting or
oversubscription is characterized here (cf. experiment 29, not repeated).

## Honest comparison scope (and why there is no pure-Ray leg)

This slice is **composition feasibility only**. There is deliberately **no
pure-Ray baseline**: Ray cannot run the same fixed C++ registered counter actor
without reimplementing it as a Python actor, which would be apples-to-oranges.

**Not comparable to the synthetic sleep Engine benchmark.** The counter methods
are deterministic native-method *dispatch*, not timed synthetic *serving*. Their
rows are not the v1 sleep schema and must never be tabled against the sleep legs
of experiment 27 or the Ray/HPX sleep baselines.

## Reproduce

```
# Composition smoke (skips cleanly if ray or _rayx is unavailable):
python bench/smoke_ray_hosting_rayx_runtime_counter.py
```

The actor returns only plain Python scalars/containers, such as ints (counter
values), bools (`release()` / `shutdown()` → `True`), a tuple of strings
(`ids()` → `(ray_actor_id, rt-act- id)`), and small dicts
(`call_after_release()` → `{raised, error}`); the inner `rt-act-` id is the
counter's native serving identity. No JSONL is written and no benchmark matrix
is run.

## Scope note

This slice covers the **local native actor** (`counter`) composition path only,
extending experiment 28's op-submission (`square`) path. Any measured
standalone-vs-hosted dispatch decomposition (which would need a distinct,
non-sleep schema) is a deliberate later slice, not included here.
