# Ray actor hosting a rayx.runtime.Runtime (composition feasibility, observation-only)

The native-op counterpart of experiment 27 (`27_ray_hosting_rayx_engine/`). A
long-lived **Python `@ray.remote` actor** constructs exactly one HPX-native
`rayx.runtime.Runtime` in `__init__` and runs a fixed registered native op
(`square`) through it locally. Ray owns only the outer actor placement/lifecycle;
`rayx.runtime.Runtime` owns local native-op execution inside the actor process.
This note records that the two **compose cleanly** — nothing more.

**What this is.** A smoke-only feasibility result: one Ray actor hosts one
Runtime, runs `square(x) -> x*x` through the fixed registry, and returns plain
JSON-serializable dicts. Lifecycle and the process-singleton constraint are
exercised by `bench/smoke_ray_hosting_rayx_runtime.py`.

**What this is *not*.** Not a performance result — the smoke emits **no JSONL**,
asserts **no timing**, and makes no speed claim. It is **not** Ray Serve, not an
object store, not arbitrary remote Python, not a Ray backend or fallback layer,
and not a Ray compatibility API. `rayx.runtime.Runtime` grows **no** Ray
semantics here: the `RuntimeFuture` is created and retired **inside** the actor;
only a plain dict crosses the Ray boundary — **no** `RuntimeFuture` /
`OperationResult` ever crosses, and there is **no** `ray.get` wrapper over a
RayX future.

## Lifecycle constraint (the load-bearing fact)

The HPX/RayX runtime is a **process-global singleton** (`python/src/rayx/_rayx.cpp`:
a single `process_runtime_active()` guard shared by `rayx.Engine` and
`rayx.runtime.Runtime`). A Ray actor is its own worker process, so the intended —
and only viable — shape is **one Runtime per long-lived Ray actor process**:

* A second `Runtime` in the same process is **rejected** while one is active
  (`RayxRuntimeHostActor.try_second_runtime` →
  `RuntimeError: an Engine or Runtime is already active in this process`).
* A `rayx.Engine` in the same process is **also rejected** — Engine and Runtime
  share the *one* process guard (`RayxRuntimeHostActor.try_second_engine`).
* The Runtime is constructed once in `__init__` and **graceful-drained** via an
  explicit `shutdown()` (idempotent); a `call_square` after shutdown raises and
  propagates through Ray.

## Multi-actor sanity (smoke-level)

The smoke also checks two Ray actors, each owning its own Runtime (`hpx_threads=1`
each; Ray `num_cpus=4` budgets the two default `num_cpus=1` actors): both serve
`square` and shut down cleanly with **distinct** inner `rt-hpx-` op-lane ids. This
confirms the process-singleton holds *per process* (one Runtime per actor process;
several actors are fine). It is a **structural** pass only — oversubscription under
load (≈ N × `hpx_threads` vs `num_cpus`) is **not** characterized here.

## Honest comparison scope (and why there is no pure-Ray leg)

This slice is **composition feasibility only**, with two honest reference points
for any later measured slice:

* **standalone `rayx.runtime.Runtime`** — local native-op dispatch only.
* **Ray actor hosting `rayx.runtime.Runtime`** — the Ray actor boundary *on top
  of* local native-op dispatch.

There is deliberately **no pure-Ray baseline**: Ray cannot run the same fixed C++
registered op (`square`) without changing the workload to a Python
reimplementation, which would be apples-to-oranges. This is therefore at most a
two-leg story, never three.

**Not comparable to the synthetic sleep Engine benchmark.** `square` is a
deterministic native-op *dispatch*, not timed synthetic *serving*. Its rows are
not the v1 sleep schema and must never be tabled against the sleep legs of
experiment 27 or the Ray/HPX sleep baselines.

## Reproduce

```
# Composition smoke (skips cleanly if ray or _rayx is unavailable):
python bench/smoke_ray_hosting_rayx_runtime.py
```

The actor returns only plain dicts (`{op_id, value, status, error, actor_id,
service_ms_observed, start_ns, end_ns}`); `actor_id` is the inner Runtime
`rt-hpx-` op-lane id. No JSONL is written and no benchmark matrix is run.

## Scope note

This slice covers the **op-submission** path (`square`) only. A Ray-hosted
`rayx.runtime.Runtime` driving the registered **CounterActor** (a second actor
axis), and any measured standalone-vs-hosted dispatch decomposition (which would
need a distinct, non-sleep schema), are deliberate later slices, not included
here. The local native counter extension is now realized in experiment 30
(`30_ray_hosting_rayx_runtime_counter/`, smoke-only).
