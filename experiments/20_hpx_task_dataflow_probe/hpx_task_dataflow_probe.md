# HPX Task/Dataflow Mechanism Probe — Which Lane Contracts Survive?

A **native-only, opt-in, contract-relaxing** mechanism probe. It serves the
*same* synthetic sleep work through five dispatch mechanisms and reports, per
mechanism, which serialized-lane **contracts** are preserved, relaxed, or
not-applicable. The question:

> When synthetic work is served by HPX task/future/dataflow mechanisms instead of
> a serialized lane, which RayX lane contracts are lost or must be redefined?

This is **not** a rayx Python feature, **not** a benchmark-corpus entry, and
**not** a serving-lane-vs-serving-lane throughput result for the pool mechanisms
(those are scheduler territory — trivial tasks spread across worker threads, per
experiment 15's framing). The native binary emits a compact experiment-local JSON
(schema `hpx-task-dataflow-probe-1`); it does **not** touch the v1 benchmark JSONL
schema or the analyzer. `ServiceLane` and `HpxLane` are used **unmodified**; no
rayx API change, and no HPX `.then`/`dataflow` exposed to Python.

**Lineage.** experiment 15 (isolated HPX primitives: sleep overshoot + the
`hpx::async` no-op dispatch floor) → experiment 16 (contract-**preserving**
cooperative FIFO `HpxLane`, opt-in, boundary-tagged) → **experiment 20
(contract-relaxing `hpx::async` / `hpx::dataflow` pools)** — the same opt-in,
separately-reported axis.

## 1. Setup

* **Mechanisms** (identical synthetic sleep work; only *dispatch* differs):
  * `service_lane` — `rayhpx::ServiceLane` (std::thread, **blocking** sleep). The
    Ray-actor-like **anchor**; all lane contracts hold.
  * `hpx_lane` — `rayhpx::HpxLane` (hpx::thread, **cooperative** sleep). A
    contract-preserving HPX-thread FIFO lane.
  * `hpx_async` — one `hpx::async` task per request (scheduler-placed **pool**).
  * `hpx_dataflow` — a tiny per-request `prepare → service` `hpx::dataflow`
    dependency (dependency-driven firing).
  * `hpx_async_then` — `hpx::async(...).then(finalize)`: the continuation composes
    **below** the caller-visible future (the reason HPX-native composition stays
    inside the backend, not the Python API).
* **Synthetic work.** Sleep-only (no spin in v1). Lanes service via their own
  internal timer (ServiceLane blocking, HpxLane cooperative); the **pools** run
  the sleep with the **cooperative** `hpx::this_thread::sleep_for` (they execute
  on HPX workers — a blocking std sleep would pin a worker). This is the honest
  native-task service and matches HpxLane's timer.
* **Identity (honest, not faked).** Lanes emit their real `actor_id`
  (`act-hpx-` / `act-hpxl-`). Pools emit a `pool_id` tag + `lane_identity = "n/a"`
  and report `distinct_worker_ids` only as *which HPX worker ran it* — never a
  lane handle. The probe **does not fabricate** a stable per-lane `actor_id`,
  `queue_depth`, `active`, or per-lane cap for a mechanism with no lane queue.
* **Ordering.** FIFO is measured uniformly as **`end_ns` inversions vs submit
  order** (0 = strict FIFO completion). Out-of-order completion is reported as a
  **contract difference, not a bug**.
* **Matrix.** mechanisms × `service_ms ∈ {0,1,5,20}`, `work_mode=sleep`,
  `hpx_threads=4`, `N=200`, **3 repeats** (medians). `--quick`: `N=40`, 1 repeat,
  no `aggregate.json`.
* **Machine.** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Gates (all passed; structural, timing-robust):**
  1. Every mechanism returns exactly `N` well-formed results (0 failed).
  2. Every per-request future fulfilled once and retired cleanly (binary
     `wait_all`/`get` completes, exit 0).
  3. `service_lane` and `hpx_lane` preserve FIFO (`inversions == 0`).
  4. Lanes emit a real prefixed `actor_id` (no `pool_id`); pools emit
     `lane_identity == "n/a"` + a `pool_id` (no faked per-lane `actor_id`).
  5. Contract-coverage table complete (every cell `preserved|relaxed|n/a`).
  6. Service ran for `service_ms > 0` (observed p50 in a loose band).
  7. Probe emits its compact schema, not v1 benchmark JSONL.
  Timing/throughput/overshoot are **reported, never gated**.
* **Reproduce:**
  `cmake --build hpx_impl/build --target hpx_task_dataflow_probe` then
  `python experiments/20_hpx_task_dataflow_probe/run_hpx_task_dataflow_probe.py`
  (raw per-run JSON → `results/`, gitignored; `--quick` for a smoke).

## 2. Contract coverage (the headline)

| Contract | `service_lane` | `hpx_lane` | `hpx_async` / `hpx_dataflow` / `hpx_async_then` |
|---|---|---|---|
| one result per request | preserved | preserved | **preserved** |
| future-ownership compatible | preserved | preserved | **preserved** |
| stable `actor_id` | preserved | preserved | **relaxed** (`pool_id`, no per-lane handle) |
| FIFO lane order | preserved | preserved | **relaxed** (measured inversions ≫ 0) |
| `queue_depth` | preserved | preserved | **n/a** (no per-lane queue) |
| `active` | preserved | preserved | **n/a** (no single in-service-per-lane) |
| per-lane admission cap | preserved | preserved | **n/a** (no lane queue to cap) |
| lane-targeted cancellation | preserved | preserved | **n/a** (no lane to target a queued skip) |

Two **universal** contracts survive every mechanism — *one result row per
request* and *per-request `hpx::future` ownership* (so `wait`/`get` stay
compatible at the C++ level). Everything else is **lane-specific**: born from
serialization, and either relaxed or not-applicable once the scheduler places
work as free tasks.

## 3. Measured facts (medians across 3 repeats)

| mechanism | sm | FIFO | inversions p50 | workers | service p50 (ms) | overshoot p50 | throughput (ops/s) |
|---|---|---|---|---|---|---|---|
| `service_lane` | 0 | ✓ | 0 | — | 0.00 | — | 2,640,264 |
| `service_lane` | 5 | ✓ | 0 | — | 6.27 | 25.3% | **161.7** |
| `service_lane` | 20 | ✓ | 0 | — | 25.02 | 25.1% | **41.4** |
| `hpx_lane` | 0 | ✓ | 0 | — | 0.00 | — | 1,708,175 |
| `hpx_lane` | 5 | ✓ | 0 | — | 5.64 | 12.7% | **177.7** |
| `hpx_lane` | 20 | ✓ | 0 | — | 21.04 | 5.2% | **47.7** |
| `hpx_async` | 0 | ✗ | 1,864 | 4 | 0.00 | — | 1,192,250 |
| `hpx_async` | 5 | ✗ | 8,402 | 4 | 5.64 | 12.9% | **34,635** |
| `hpx_async` | 20 | ✗ | 7,988 | 4 | 20.64 | 3.2% | **9,484** |
| `hpx_dataflow` | 5 | ✗ | 7,386 | 4 | 5.72 | 14.4% | 34,377 |
| `hpx_async_then` | 5 | ✗ | 7,901 | 4 | 5.75 | 15.1% | 34,209 |

(Full per-cell numbers in `aggregate.json`.) Magnitudes are machine-specific and
**reported, not gated** — the contract coverage above is the result.

### 3a. The two readings of "throughput" (read carefully)

The pool throughput at `service_ms > 0` is **not** serving-compute throughput, and
it is **not** "the task backend is faster at serving." It is the consequence of
**cooperative parking**:

* A **serialized lane** services one request at a time. Its throughput is
  ≈ `1 / service_time` (sm=5 → ~160–178/s; sm=20 → ~41–48/s). **Bounded
  concurrency = 1 per lane is the actor contract**, by design.
* A **cooperative-sleep pool** parks each request with `hpx::this_thread::sleep_for`,
  which **yields the worker**, so all `N` parked sleeps overlap and retire in
  roughly one `service_time` of wall (sm=5 → ~34k/s, i.e. ~200 requests in ~6 ms).
  That is **unbounded in-flight concurrency**, not faster compute — and it is a
  property of *parked waits overlapping*, not of doing the work sooner.

So the pool relaxes more than identity and FIFO: it removes the **serialization /
concurrency bound itself**. That is precisely *why* `queue_depth`, `active`,
per-lane cap, and lane-targeted cancellation are **n/a** — there is no
serialization point to queue at, bound, observe, or cancel-before.

### 3b. The no-op dispatch floor

At `service_ms = 0` the ordering flips: the **serialized lane is faster**
(`service_lane` ~2.64M/s, `hpx_lane` ~1.71M/s) than the pools (~1.2–1.9M/s) —
a single FIFO consumer with no per-task scheduling beats spreading trivial tasks
across workers. This matches experiment 15's dispatch-floor reading: the pool's
apparent advantage appears only when work **parks**, not at pure dispatch.

## 4. Answers to the probe's questions

* **Which contracts are universal?** *One result row per request* and *per-request
  future ownership.* Every mechanism — lane or pool — hands back exactly one
  `hpx::future<Result>` per request, fulfilled once. These are the contracts a
  future backend can always keep.
* **Which are lane-specific and lost/relaxed by task/dataflow?** Stable `actor_id`
  (→ `pool_id`), FIFO order (→ scheduler-reordered, thousands of inversions),
  `queue_depth`, `active`, per-lane admission cap, and lane-targeted cancellation
  (all **n/a** — no lane queue exists).
* **Least-misleading identity for a pool backend?** An explicit `pool_id` tag +
  `lane_identity = "n/a"`, with `distinct_worker_ids` reported only as *which
  worker ran it*. Reusing `actor_id` for a pool would be a lie (no stable per-lane
  handle exists). The probe ran across **4 distinct workers** per pool pass.
* **Is task/dataflow dispatch overhead plausible enough to justify a future
  backend seam?** Overhead is **plausible** (no-op floor ~1.2–1.9M/s, same order
  as the lanes), and `hpx_async_then` shows continuations compose cleanly **below**
  the caller-visible future (no Python exposure needed). But the pool **relaxes
  the very contracts the current rayx observability/admission surface is built on**
  (`lane_stats`, `max_queue_depth_per_lane`, FIFO, `actor_id`). So — see §5.

## 5. Decision: where the "HPX-native inside" win actually is

**Go/no-go for a future rayx-side backend seam, from the evidence:**

* **GO (contract-preserving) — the cooperative `HpxLane`.** It is HPX-native
  *inside* (cooperative timer + HPX-thread suspension), keeps **every** lane
  contract (FIFO, `actor_id`, queue/active, expressible cap/cancel), and shows
  lower overshoot than the blocking lane (sm=20: 5.2% vs 25.1%) at the same
  serialized throughput. This is the clean, low-risk "Ray-like outside, HPX-native
  inside" step *behind the existing API* — if/when a rayx-side seam is built.
* **NO-GO as a drop-in (contract-relaxing) — the `hpx::async`/`dataflow` pool.**
  It is honest and low-overhead, but it dissolves the serialization bound and the
  per-lane contracts. Behind the current lane-shaped API it would either **lie**
  about `lane_stats`/admission/FIFO or require **re-adding** a serialization /
  admission layer on top (i.e. rebuilding a lane), which defeats using a raw pool.
  A task/dataflow backend is therefore a **separate, future research axis** with
  its **own** (non-lane) observability/admission model and its own boundary tag —
  **not** a near-term drop-in rayx backend.

The probe's job was to make that call on evidence rather than vibes, and it does:
**the immediate HPX-native-inside win is the cooperative lane; the task/dataflow
pool is a different contract, not a backend swap.**

## 6. Scoped takeaways

* The serialized-lane model carries **two universal** contracts (one-row-per-request,
  future ownership) and **six lane-specific** ones; HPX task/dataflow keeps the
  universal two and relaxes/voids the rest.
* The lane's **one-at-a-time serialization is the actor semantics**, not an
  overhead to optimize away; a cooperative-sleep pool's high "throughput" is
  parked-wait overlap (unbounded concurrency), not faster serving.
* `hpx_async`, `hpx_dataflow`, and `hpx_async_then` behave **identically** at the
  contract level (same relaxation), confirming this is about the
  scheduler-placed-pool *model*, not a specific combinator.
* Continuations (`.then`) compose **below** the caller-visible future — evidence
  that HPX-native composition belongs in the backend, never as Python API.

## 7. Caveats / non-claims

* **Not a rayx feature.** Native-only probe; no rayx Python API, no
  `_rayx.cpp`/`__init__.py` change, no HPX `.then`/`dataflow` exposed to Python.
* **Not a ServiceLane replacement.** `ServiceLane` stays the unmodified
  comparison anchor; `HpxLane` is unmodified.
* **Not comparable to the benchmark corpus.** Own schema
  (`hpx-task-dataflow-probe-1`), own boundary; the pool rows are **scheduler
  territory**, not serving-lane results, and pool throughput must **not** be read
  as "task backend is faster at serving."
* **Not Ray Serve, object store, arbitrary remote Python, or real inference**, and
  **not** a general "HPX beats Ray" claim — synthetic sleep timing only, single
  locality, in-process, machine-specific magnitudes.
* **Out-of-order pool completion is a contract difference, not a bug.**
* **Raw outputs are scratch.** Per-run JSON under `results/` is experiment-local
  (gitignored); the curated `aggregate.json` beside this report is the tracked
  evidence.
