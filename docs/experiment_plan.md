# Experiment Plan

The measurement contract for Ray-vs-HPX serving-control experiments. This doc
is locked before implementation so that future Ray and HPX results are
interpretable and comparisons stay honest. It defines schemas and benchmark
shapes, not implementation.

## 1. Goal

* The first experiments compare a **Ray actor-style serving-control** baseline
  with a **future HPX-native serving-control** runtime.
* The first backend is **synthetic**, not a real model.
* The first goal is **not** to prove HPX is faster. It is to measure
  **control-plane overhead** and **workload sensitivity** — i.e., how much each
  runtime costs per request, and how that cost changes with service time and
  concurrency.

## 2. Boundary Being Measured

The "boundary" is the path a request crosses to reach and return from the work.
It is not the same across runtimes and must be stated in every report.

* **Ray actor baseline** measures public Ray API actor-call overhead, which
  includes Python, the Ray runtime, the process boundary, serialization, and
  IPC. Boundary label: `ray-actor-process`.
* **Future HPX baseline** may first measure **in-process intra-locality**
  futures/actions, with no serialization or IPC. Boundary label:
  `hpx-intra-locality`.
* These boundaries are **not identical**. A difference between them may reflect
  the boundary cost, not the scheduling or control-plane design.
* Every report must state the boundary explicitly (the `boundary` field carries
  it in the data).

## 3. Synthetic Backend Contract

The synthetic backend stands in for an opaque inference engine. It performs no
real computation; it only consumes a controlled amount of time so that
control-plane overhead can be isolated.

Request fields:

* `request_id` — unique id for the request.
* `service_ms` — total time the backend should spend servicing the request.
* `chunks` — number of active service chunks the backend splits the TOTAL active
  `service_ms` into (default `1` = single step). Exposed by the benchmark drivers
  via `--chunks` (single-submit modes); `>1` runs chunked synthetic service — see
  the status note above.
* `chunk_delay_ms` — parked inter-chunk gap in ms inserted between the `chunks-1`
  boundaries (default `0`). Exposed via `--chunk-delay-ms`.
* `work_mode` — how the backend consumes `service_ms`.

Rules for the first implementation:

* First supported `work_mode` is **`sleep`**.
* `sleep` models **I/O-bound or GPU-offloaded** serving, where the control
  plane is waiting for external work rather than burning a CPU core.
* Compute-bound **busy-spin** is a **separate axis** (`work_mode="spin"`, now
  implemented in the drivers) and must **not** be mixed into a single `sleep`
  result set — it changes scheduling behavior on both runtimes. Each run records
  its `work_mode`.
* The first implementation used **no streaming** (`chunks = 1`) and **no
  cancellation**; `chunks`/`chunk_delay_ms` were reserved in the schema. Chunked
  synthetic service has since graduated into the drivers (`--chunks` /
  `--chunk-delay-ms`, single-submit modes) — still **not** streaming (one row per
  request, no per-chunk events) and still no benchmark-driver cancellation.

> **Status note — what has graduated into the drivers vs facade-only.** Since
> this contract was locked, several axes have been implemented. Three are now
> **first-class benchmark-driver workloads** that populate the reserved JSONL
> fields without any schema bump (still `"1"`):
>
> * `work_mode` — `--work-mode sleep | spin` (all three drivers; Ray is
>   `sleep`-only).
> * **varied batch** service — `submit_batch(service_ms=[...])` on the rayx
>   driver (`--api batch` / bimodal), one true bulk crossing.
> * **chunked synthetic service** — `--chunks` / `--chunk-delay-ms` on **all
>   three** drivers (Ray, HPX-native, rayx), single-submit modes only; the total
>   active `service_ms` is split into `chunks` active steps with `chunks-1` parked
>   `chunk_delay_ms` gaps. One row per request, **not** real token streaming, no
>   per-chunk events; batch submit stays unchunked (matching the facade). The
>   chunked *shape* matches across engines; absolute sleep overshoot still differs
>   by backend (Ray ~5% vs HPX ~25%, per experiment 01).
>
> The rest remain **facade-only** (exercised through the `rayx` API and
> experiment-local runners — see `docs/reference/` and `experiments/08`–`13`) and
> do **not** appear in the benchmark JSONL: queued + chunk-boundary running
> **cancellation**, the `chunks_completed` result-row field, and the client-side
> `label`. This document remains the benchmark-driver measurement contract.

### Service-time patterns

How each request's `service_ms` is chosen (a workload-generation concern; the
backend still just sleeps for the per-request value):

* **`fixed`** — every request uses the same `service_ms` (the original
  behavior).
* **`bimodal`** — each request draws a *high* service time with probability
  `p_high`, else a *low* service time, to study queueing and tail latency under
  non-uniform work.

Determinism: the per-request service time is a **pure function of
`(seed, request_index)`** computed by a small portable integer hash
(splitmix64-style), not a stateful RNG. The same `seed` reproduces the same
sequence, and — because the hash is plain integer arithmetic — the sequence is
**engine-independent** (Ray, native HPX, and rayx can produce the identical
sequence). The draw is decorrelated from round-robin lane assignment
(`lane = request_index % num_lanes`), unlike a periodic cycle. `bimodal` is
implemented identically in all three drivers (Ray, native HPX, rayx), verified
to emit byte-identical `service_ms_requested` sequences for the same seed.

## 4. Per-Request JSONL Schema

One JSON object per request, one per line. All `*_ns` fields are monotonic
high-resolution timestamps; all `*_ms` fields are derived for readability.

* `schema_version` — schema version string, e.g. `"1"`.
* `run_id` — id grouping all requests of a single benchmark run.
* `backend` — backend name, e.g. `"synthetic"`.
* `boundary` — boundary label, e.g. `"ray-actor-process"`.
* `workload` — workload name, e.g. `"sleep_5ms_c8"`.
* `request_id` — request id (matches the request).
* `actor_id` or `worker_id` — id of the actor/worker that served the request.
* `status` — `"completed"` | `"failed"`.
* `submit_ns` — monotonic time the request was submitted by the client.
* `start_ns` — monotonic time the backend began servicing the request.
* `end_ns` — monotonic time the backend finished the request.
* `total_ms` — `(end_ns - submit_ns) / 1e6`; full client-observed latency.
* `queue_wait_ms` — `(start_ns - submit_ns) / 1e6`; time spent waiting before
  service began.
* `service_ms_observed` — `(end_ns - start_ns) / 1e6`; measured service time.
* `service_ms_requested` — the `service_ms` asked of the backend, recorded
  per-request. Constant under `service_pattern=fixed`; varies row-to-row under
  `bimodal` (see §3). Still a scalar; no schema change.
* `chunks` — number of active service chunks the request was split into
  (default `1`; set by the driver `--chunks`, single-submit modes).
* `chunk_delay_ms` — parked inter-chunk delay in ms (default `0`; set by
  `--chunk-delay-ms`).
* `work_mode` — `"sleep"` or `"spin"` (the drivers support both; each run records
  which it used).
* `retire_mode` — client dispatch mode (metadata): `"one_by_one"` (windowed:
  hold N in flight, retire oldest, submit one) or `"batch"` (bulk submit; see
  note below).
* `error` — error string if `status == "failed"`, else null/empty.

Note: `start_ns`/`end_ns` are measured at the backend (server side); `submit_ns`
is measured at the client. For the Ray process boundary these may sit in
different clock domains — see §9.

### Batch (bulk) submit semantics (`retire_mode="batch"`)

The rayx Python frontend's `Engine.submit_batch()` enqueues all measured
requests in a single Python→C++ crossing. Its outputs are tagged
`retire_mode="batch"`. Schema stays version `"1"` — only this field's value set
widens. Under batch:

* All futures from one batch share **one** Python-side `submit_ns`.
* `total_ms` is therefore **queue/bulk-drain shaped**: request *k* includes the
  time to drain the *k* requests ahead of it on its lane.
* **Throughput** is the meaningful metric for a batch run.
* Batch `total_ms`/`queue_wait_ms` percentiles are **not** directly comparable
  to windowed `one_by_one` latency percentiles.

## 5. Aggregate Summary Schema

One JSON object emitted by the analyzer per run.

* `schema_version` — schema version string.
* `run_id` — run id (matches the per-request records).
* `backend` — backend name.
* `boundary` — boundary label.
* `workload` — workload name.
* `num_requests` — total requests issued.
* `completed` — count with `status == "completed"`.
* `failed` — count with `status == "failed"`.
* `cancelled` — count with `status == "cancelled"` (additive; `0` for normal
  benchmark runs, which never cancel — non-zero only when the analyzer is run over
  experiment-local rows that used facade cancellation).
* `throughput_req_s` — completed requests per wall-clock second of the run.
* `total_ms_p50`, `total_ms_p90`, `total_ms_p99` — total-latency percentiles.
* `queue_wait_ms_p50`, `queue_wait_ms_p90`, `queue_wait_ms_p99` — queue-wait
  percentiles.
* `service_ms_p50`, `service_ms_p90`, `service_ms_p99` — observed-service
  percentiles.
* `total_ms_min`, `total_ms_max` — min/max total latency.
* `notes` — free-text caveats, including the boundary statement and whether the
  workload was dispatch-dominated or service-dominated.

## 6. Null-Overhead Microbenchmark

* **Required first benchmark.** Run before any sleep workloads.
* **Ray:** a no-op actor method with **no** synthetic service delay
  (`service_ms = 0`, `work_mode = sleep` degenerate / no-op).
* **Future HPX equivalent:** a no-op future/action/component call.
* **Purpose:** establish each runtime's **dispatch / control-plane overhead
  floor** — the minimum cost to send and complete a request with zero work.
* **Report:** p50/p90/p99 round-trip latency and throughput. All later
  service-time numbers are interpreted relative to this floor.

## 7. Initial Workload Matrix

Keep it small. Local laptop only.

* Service times: `noop`, `sleep 1 ms`, `sleep 5 ms`, `sleep 20 ms`.
* Concurrency (in-flight requests): `1`, `4`, `8`, `16`.
* **One actor/worker first.**
* **Local laptop only.**
* **No streaming and no cancellation in the benchmark matrix.** Chunked service
  (`--chunks` / `--chunk-delay-ms`) is available in the drivers but is **not**
  streaming (one row per request, no per-chunk events); cancellation stays a rayx
  facade feature — see the §3 status note.

This is 4 service levels × 4 concurrency levels = 16 points, single worker,
single machine. Expand only after the first smoke run is reviewed.

## 8. Acceptance Criteria for First Ray Baseline

* Ray **no-op** benchmark runs.
* Ray **fixed-sleep** benchmark runs.
* JSONL output **validates against the schema** in §4.
* Analyzer prints an **aggregate summary** matching §5.
* A **smoke command completes quickly** on a laptop.
* The report **clearly states the measured boundary**.

## 9. Fairness and Caveats

* Do **not** compare Ray process-boundary actor calls to HPX in-process futures
  as if they were equivalent. They cross different boundaries (§2).
* Do **not** claim HPX is generally faster than Ray from local synthetic tests.
* Interpret results **by workload size and boundary**, not as a single verdict.
* For each workload, **document whether it is dispatch-dominated** (service time
  near the null-overhead floor) **or service-dominated** (service time well
  above the floor). Overhead only matters where it is a meaningful fraction of
  total latency.
* Clock domains: `submit_ns` (client) and `start_ns`/`end_ns` (server) may come
  from different clocks across the Ray process boundary. Where this is a
  concern, treat `total_ms` (single client clock, submit→result) as the
  authoritative latency and treat the server-side split as approximate.

## 10. Proposed Future CLI Shape

Examples only — no implementation in this slice.

```text
# Ray no-op microbenchmark (null-overhead floor)
ray_bench --backend synthetic --work-mode sleep --service-ms 0 \
          --concurrency 8 --requests 2000 \
          --workload noop_c8 --out results/ray_noop_c8.jsonl

# Ray fixed-sleep benchmark
ray_bench --backend synthetic --work-mode sleep --service-ms 5 \
          --concurrency 8 --requests 2000 \
          --workload sleep_5ms_c8 --out results/ray_sleep5_c8.jsonl

# Analyzer: per-request JSONL -> aggregate summary
analyze results/ray_sleep5_c8.jsonl --out results/ray_sleep5_c8.summary.json
```

## 11. Smoke / Contract Gates

Small, fast checks that lock a contract's *shape* (not its numbers) so a later
change can't silently break it. Run locally, and wired into CI (the
dependency-free checks run in `ci.yml`; the ones needing the built native
binary or rayx extension run in `native-rayx-smoke.yml`).

### `diag` contract gate — `bench/smoke_diag.py`

* **What it guards:** the native HPX baseline's opt-in `--diag` mode.
  `--diag` writes a *separate* `<out>.diag.json` (schema `diag-1`) and must not
  perturb the normal per-request JSONL (schema `"1"`, §4).
* **Asserts (shape only, no numeric values):**
  * diag **off** → exit 0, normal JSONL parses, **no** `.diag.json` is written,
    and no diag-only fields leak into the JSONL;
  * diag **on** → exit 0, normal JSONL still schema `"1"`, and `<out>.diag.json`
    is valid JSON with schema `diag-1` and the expected
    `phases_ms` / `queue_depth_at_enqueue` / `lanes` / `config` fields.
* **Command:**

  ```text
  python bench/smoke_diag.py          # uses hpx_impl/build/hpx_synthetic_baseline
  python bench/smoke_diag.py --bin PATH   # override the binary
  ```

* **Pass condition:** exit code 0, prints `OK: --diag smoke passed`.
* **Prerequisite:** the native baseline must be built first
  (`cmake --build hpx_impl/build`). The script uses only a small synthetic
  workload and a temp directory, so it leaves no result artifacts.

### Service-sequence golden gate — `bench/smoke_service_sequence.py`

* **What it guards:** the deterministic `fixed`/`bimodal` service selection (§3)
  stays identical across engines. `bench/service_sequence.py` is the shared
  Python source of truth, imported by both `run_ray_baseline.py` and
  `run_hpx_python_baseline.py`; the native C++ `service_for` mirrors the same
  splitmix64 logic.
* **Asserts (golden values, no timing):** for the golden cell
  `service-low=1, service-high=20, service-p-high=0.1, seed=0`, the first values
  are `[20, 1, 1, 20, 1, 1, 1, 1]` (high at idx 0 and 3), with the full
  golden table pinned in the script.
* **Coverage:** `bench/smoke_service_sequence.py` is dependency-free (no Ray /
  rayx) and runs in `ci.yml`; `native-rayx-smoke.yml` runs a tiny one-lane,
  concurrency-1 bimodal seed=0 cell on the built native binary and checks the
  emitted `service_ms_requested` JSONL against the same golden values, so the
  C++ mirror cannot drift.
* **Pass condition:** exit code 0, prints `OK: service-sequence golden check
  passed`.

### rayx contract + retire-mode gates — `bench/smoke_rayx.py`, driver smoke

* **What they guard:** the rayx Python frontend's API/behavior shape and the two
  client retire modes. `bench/smoke_rayx.py` locks the `Engine` / `Future` /
  `SyntheticActor` surface, including `Engine.wait`'s contracts (empty list, bad
  `num_returns`, retired Future, duplicate Future, post-shutdown). The driver
  retire-mode smoke runs a tiny no-op cell through both `--retire-mode
  one_by_one` and `--retire-mode batch_wait` and parses each output with the
  analyzer, so both retire paths stay runnable end to end.
* **Coverage:** both run in `native-rayx-smoke.yml` (they need the built `_rayx`
  extension). `bench/smoke_rayx.py` prints `OK: rayx smoke passed`.

### Local aggregator — `bench/smoke_local.py`

* **What it is:** one stdlib-only entry point that runs the gates above in order
  and prints a `PASS`/`FAIL`/`SKIP` line per check. A component that is
  unavailable (no built `_rayx`, no native binary, no Ray) is **skipped**, not
  failed, so the same command works on any machine. It adds no new checks and
  asserts no timing — just a convenience wrapper for local validation; CI still
  invokes the individual gates directly.
* **Command:** `python bench/smoke_local.py` (scratch goes under the gitignored
  `results/_smoke_local/` and is cleaned up on exit).
* **Pass condition:** exit code 0 — every *available* component passed.
