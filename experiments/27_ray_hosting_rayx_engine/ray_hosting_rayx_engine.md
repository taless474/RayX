# Ray actor hosting a RayX Engine (composition feasibility, observation-only)

The first realization of `docs/ray_hpx_mapping.md`'s "Future optional path — A":
a long-lived **Python `@ray.remote` actor** constructs exactly one HPX-backed
`rayx.Engine` in `__init__` and serves synthetic requests through it locally.
Ray owns only the outer actor placement/lifecycle; RayX owns the fine-grained
local serving. This note records that the two **compose cleanly** — nothing more.

**What this is.** A feasibility result: one Ray actor hosts one RayX Engine,
serves a handful of requests, and emits v1-schema JSONL rows that flow through
the existing `bench/analyze_jsonl.py` path, directly comparable to the pure Ray
actor baseline and standalone RayX.

**What this is *not*.** Not a performance claim and not a speedup. Hosting RayX
inside a Ray actor adds the Ray actor-call boundary **on top of** RayX's local
serving, so it is expected to be **slower** than standalone RayX, not faster.
It is **not** Ray Serve, not an object store, not arbitrary remote Python, not a
Ray compatibility layer, and not "HPX beats Ray". RayX grows **no** Ray
semantics here: the RayX future is created and retired **inside** the actor;
only a plain JSON-serializable row crosses the Ray boundary (no `ray.get`
wrapper over a RayX future).

## Lifecycle constraint (the load-bearing fact)

The HPX runtime is a **process-global singleton** (`python/src/rayx/_rayx.cpp`:
a single `process_runtime_active()` guard shared by `Engine` and
`runtime.Runtime`). A Ray actor is its own worker process, so the intended —
and only viable — shape is **one RayX Engine per long-lived Ray actor process**:

* A second `Engine`/`Runtime` in the same process is **rejected** while one is
  active. The smoke demonstrates this via `RayxHostActor.try_second_engine`
  (`RuntimeError: an Engine or Runtime is already active in this process`).
* The Engine is constructed once in `__init__` and **graceful-drained** via an
  explicit `shutdown()` before the actor is released; serving after shutdown
  raises.

## Open lifecycle questions (not answered here)

Deferred, to be characterized before any multi-actor or performance statement:

* **Oversubscription.** N Ray actors on one node = N processes = N independent
  HPX runtimes. Two-actor *structural* hosting passes at smoke level (see
  "Multi-actor sanity" below); the *timing-under-contention* characterization is
  now done as the follow-on resource-budget experiment
  [`29_ray_hosting_rayx_multi_oversubscription/`](../29_ray_hosting_rayx_multi_oversubscription/ray_hosting_rayx_multi_oversubscription.md)
  (which found the binding resource is concurrently-active CPU-bound lanes vs
  physical cores — backend-specific — not `hpx_threads` alone).
* **Worker reuse / restart.** The "sequential HPX start after stop" path
  (`_rayx.cpp` comment) under Ray worker reuse is asserted by code comment but
  not yet exercised here.

## Reproduce

```
# Composition smoke (skips cleanly if ray or _rayx is unavailable):
python bench/smoke_ray_hosting_rayx.py

# A small comparable run (writes ignored scratch JSONL, then summarize):
python bench/run_ray_hosting_rayx.py --service-ms 1 --concurrency 4 \
    --requests 20 --warmup-requests 5 --out results/rayxhost_smoke.jsonl
python bench/analyze_jsonl.py results/rayxhost_smoke.jsonl
```

The driver (`bench/run_ray_hosting_rayx.py`) shares the deterministic
`bench/service_sequence.py` request sequence with the Ray/HPX/rayx drivers, so
the same `(seed, request_index)` yields the same service times across all legs.
Rows are labelled `backend="rayx"`, `boundary="ray-actor-rayx-engine"`;
`actor_id` is the inner RayX serving lane and `worker_id` is the outer Ray
actor handle. Any latency magnitudes are machine-specific and observation-only.

## Three-leg boundary decomposition (observation-only)

Same 1 ms-sleep workload and shared `service_sequence` through all three legs
(200 requests, concurrency 4, `one_by_one` retire, 20 warmup); one machine, one
run; summarized by `bench/analyze_jsonl.py`. **These magnitudes are
machine-specific and are not gated, not stable benchmark numbers, and not quoted
as a performance result.**

| Leg | Command (abbreviated) | `boundary` | completed | p50 / p90 / p99 total_ms |
|---|---|---|---|---|
| standalone RayX | `run_rayx_baseline.py … --num-lanes 1 --hpx-threads 1` | `rayx-local` | 200 | 5.07 / 5.09 / 5.11 |
| pure Ray actor | `run_ray_baseline.py … --num-cpus 4` | `ray-actor-process` | 200 | 16.77 / 17.74 / 19.23 |
| Ray-hosted RayX | `run_ray_hosting_rayx.py … --num-cpus 4 --num-lanes 1 --hpx-threads 1` | `ray-actor-rayx-engine` | 200 | 17.52 / 18.25 / 18.79 |

(Each command also took `--service-ms 1 --concurrency 4 --requests 200
--warmup-requests 20 --retire-mode one_by_one --out results/…`.)

**Interpretation (strict).**

* **standalone RayX (`rayx-local`)** measures *local HPX/RayX serving only* — no
  Ray boundary. Lowest total_ms.
* **pure Ray (`ray-actor-process`)** measures the *Ray actor-call / process / IPC
  / serialization boundary*; the 1 ms sleep is dwarfed by that boundary.
* **Ray-hosted RayX (`ray-actor-rayx-engine`)** measures the *Ray boundary plus
  local RayX serving*. It is **dominated by the Ray boundary**, with local
  serving adding modestly. It is **not** a clean arithmetic sum of the other two
  legs: under concurrency the inner serving partially overlaps the Ray IPC, so
  the hosted leg lands just above pure Ray, not pure Ray + the local figure.
* This is **composition feasibility / boundary decomposition**, not a speedup and
  **not** "HPX beats Ray". The expected and observed ordering holds — hosting
  RayX inside a Ray actor is slower than standalone RayX, because it adds the Ray
  boundary on top.

## Multi-actor sanity (smoke-level)

`bench/smoke_ray_hosting_rayx.py` also checks two Ray actors, each owning its own
RayX Engine (`hpx_threads=1` each; Ray `num_cpus=4` budgets the two default
`num_cpus=1` actors): both serve and shut down cleanly with **distinct** inner
lane ids. This confirms the process-singleton holds *per process* (one Engine
per actor process; several actors are fine). It is a **structural** pass only —
**oversubscription under load is characterized separately** in experiment 29
([`29_ray_hosting_rayx_multi_oversubscription/`](../29_ray_hosting_rayx_multi_oversubscription/ray_hosting_rayx_multi_oversubscription.md)).

## Scope note

This slice covers the **Engine** (synthetic sleep) host only. The separate
Ray-hosted `rayx.runtime.Runtime` prototype for fixed registered native ops is
its native-op counterpart, written up in
[`28_ray_hosting_rayx_runtime/`](../28_ray_hosting_rayx_runtime/ray_hosting_rayx_runtime.md)
(smoke-only, no timing — deliberately not comparable to this slice's sleep
numbers).
