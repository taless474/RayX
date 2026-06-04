# HpxLane Backend — Evidence Arc (reading guide)

A single reading path through the five experiments that build and characterize the
`HpxLane` backend. It is an **evidence index / reading guide, not new evidence**:
each entry links out to the experiment report that holds the detailed methodology,
data, and caveats. Conclusions live in those reports; this page only orders them.

## Purpose and scope

* **`ServiceLane` / `lane_impl="std"`** is the project's **stable comparison
  anchor**: a `std::thread`, single-consumer, one-request-at-a-time FIFO lane.
  It is the default and the baseline every other result is read against.
* **`HpxLane` / `lane_impl="hpx"`** is the **opt-in cooperative HPX-thread
  backend**: same RayX lane contract, but the lane worker is an `hpx::thread`, the
  queue is guarded by `hpx::mutex` / `hpx::condition_variable_any`, and parked
  sleeps use the cooperative HPX timer. It is selected through the public
  `lane_impl` knob (see `rayx_frontend_design.md` §13); no HPX types cross into
  Python.
* This doc indexes the evidence for that backend. It does **not** restate or
  replace any experiment's conclusions.

## The arc at a glance

| Exp | Question | What it showed | What it did NOT show | Scope tag |
|---|---|---|---|---|
| 16 | Does a cooperative HPX-thread lane preserve actor-like FIFO, and how does its timer behave vs the blocking lane? | A native single-lane `HpxLane` keeps FIFO / single `actor_id` and shows lower sleep overshoot than `ServiceLane` at each tested service time | Anything about the rayx backend, multi-lane load, or a general HPX-scheduler result | native single-lane |
| 20 | Are `hpx::async` / `hpx::dataflow` task pools a drop-in RayX lane backend? | Serialized lanes keep all lane contracts; scheduler-placed pools relax identity/FIFO and have no lane queue to measure or cap — the contract-preserving cooperative `HpxLane` is the drop-in, the pools are not | That the pools serve faster (their high sleep "throughput" is parked-wait overlap, not faster serving); any rayx-backend behavior | task/dataflow probe |
| 21 | Does `lane_impl="hpx"` honor the identical RayX lane contract as `lane_impl="std"`? | Full contract parity (completion, get/wait/as_completed, chunking, queued + running cancellation, bounded admission, `lane_stats()`); only the `actor_id` prefix differs | Anything about relative performance or load behavior | RayX backend |
| 22 | Given parity, where do the backends diverge under load? | Mechanism divergence: under parked **sleep** both overlap (`HpxLane` even at `hpx_threads=1`); under non-yielding **spin** `ServiceLane` follows OS/core count while `HpxLane` is bounded near `hpx_threads` | A speedup or faster/slower verdict — the divergence is recorded as observation only | RayX backend / observation-only timing |
| 23 | Uncontended, what does the adapter's `run_as_hpx_thread` hop cost per call? | A single-digit-to-tens-of-µs boundary cost (best approximated by `lane_stats()`), rising with pool size and orders below ms-scale synthetic service | A throughput benchmark, a speedup claim, or a faster/slower verdict; the loaded cost (this is the idle-pool floor) | RayX backend / observation-only timing |

## Per-experiment detail

### exp16 — native single-lane feasibility / timer behavior
* **Question:** can a serialized lane use HPX-native scheduling/timer primitives while preserving actor-like FIFO, and what changes vs the blocking `ServiceLane`?
* **Showed:** the native `HpxLane` preserves FIFO and a single `actor_id`, and the experiment-15 cooperative-timer advantage survives in a real FIFO lane (lower sleep overshoot at each tested duration).
* **Did not show:** any rayx-backend behavior, multi-lane/load behavior, or a general "HPX scheduler wins" result; magnitudes are duration-dependent and machine-specific.
* **Scope:** native single-lane mechanism probe (native binary `--lane-impl`), explicitly incomparable to the benchmark corpus. Report: [experiments/16_hpx_lane_mechanism_probe/hpx_lane_mechanism_probe.md](../../experiments/16_hpx_lane_mechanism_probe/hpx_lane_mechanism_probe.md).

### exp20 — task/dataflow pools are not drop-in lane backends
* **Question:** are scheduler-placed `hpx::async` / `hpx::dataflow` / `…then` pools a drop-in replacement for a serialized RayX lane?
* **Showed:** serialized lanes (`ServiceLane`/`HpxLane`) keep every lane contract; the pools relax identity and FIFO and leave per-lane queue/cap/cancel not-applicable. The evidence-backed decision: the contract-preserving cooperative `HpxLane` is the drop-in "HPX-native inside" path; a task/dataflow pool is a separate, non-lane future axis.
* **Did not show:** that pools serve faster (their high sleep "throughput" is cooperative parked-wait overlap, not faster serving; at the no-op floor the serialized lane is actually faster).
* **Scope:** native-only mechanism probe, own schema, **not** a rayx feature and **not** the benchmark corpus. Report: [experiments/20_hpx_task_dataflow_probe/hpx_task_dataflow_probe.md](../../experiments/20_hpx_task_dataflow_probe/hpx_task_dataflow_probe.md).

### exp21 — RayX backend contract parity
* **Question:** does `Engine(lane_impl="hpx")` honor the identical RayX lane contract as `lane_impl="std"`?
* **Showed:** full parity across completion, `get`/`wait`/`as_completed`, chunked service, queued and chunk-boundary running cancellation, bounded admission/`QueueFullError`, and `lane_stats()`; the only observable difference is the `actor_id` prefix (`act-hpx-` vs `act-hpxl-`).
* **Did not show:** anything about relative performance or under-load behavior (timing recorded as non-gating observation only).
* **Scope:** rayx backend; semantics/parity evidence only. Report: [experiments/21_rayx_hpxlane_backend_parity/rayx_hpxlane_backend_parity.md](../../experiments/21_rayx_hpxlane_backend_parity/rayx_hpxlane_backend_parity.md).

### exp22 — load-divergence mechanism (observation-only)
* **Question:** given parity, where do the two backends structurally diverge under load?
* **Showed:** with the contract held under load (firm structural gates), the backends diverge along *how concurrency is bounded* — under parked **sleep** both overlap (`HpxLane` overlaps all lanes even at `hpx_threads=1` because cooperative parking yields the worker); under non-yielding **spin** `ServiceLane` overlap follows the OS/core count while `HpxLane` is bounded near `hpx_threads`.
* **Did not show:** a speedup or "faster/slower" verdict; all timing/concurrency figures are observation-only and machine-specific, never gated (`lane_stats().active` is not a perfect proof of true worker-level concurrency).
* **Scope:** rayx backend; mechanism/structure evidence with observation-only timing. Report: [experiments/22_rayx_hpxlane_load_divergence/rayx_hpxlane_load_divergence.md](../../experiments/22_rayx_hpxlane_load_divergence/rayx_hpxlane_load_divergence.md).

### exp23 — uncontended adapter-hop cost (observation-only)
* **Question:** uncontended, how much per-call latency does the `RayxLaneAdapter<HpxLane>` `run_as_hpx_thread` hop add vs the no-hop `ServiceLane` path?
* **Showed:** a single-digit-to-tens-of-µs boundary cost (best approximated by `lane_stats()` at `num_lanes=1`), which rises with `hpx_threads` and is orders below the corpus's ms-scale synthetic service.
* **Did not show:** a throughput benchmark or speedup/faster-slower verdict; the std-vs-hpx delta is the closest observable approximation of the hop-dominated cost, not a perfect subtraction (the `submit_get` end-to-end delta even flips sign by pool size, so it is dispatch-dominated). This is the idle-pool floor, not the loaded cost.
* **Scope:** rayx backend; uncontended boundary-cost, observation-only timing. Report: [experiments/23_rayx_hpxlane_adapter_hop_cost/rayx_hpxlane_adapter_hop_cost.md](../../experiments/23_rayx_hpxlane_adapter_hop_cost/rayx_hpxlane_adapter_hop_cost.md).

## Established vs still open

**Established:**
* **Contract parity** — `lane_impl="hpx"` is contract-equivalent to `lane_impl="std"` for the surface RayX exposes (exp21).
* **Load-divergence mechanism (as observation)** — sleep/yield convergence vs spin/non-yielding divergence bounded by the HPX worker pool (exp22).
* **Adapter-hop cost characterization (as observation)** — uncontended per-call hop cost is µs-scale and rises with pool size (exp23).

**Still open:**
* **Whether a source-level hop-reduction slice is worth doing.** exp23 is *evidence toward* this decision, not the decision: the uncontended hop is orders below ms-scale service, so a hop-reduction change looks weakly justified for serving-shaped workloads and would matter mainly for very-high-rate tiny/no-op control operations or large lane counts. Not yet decided.

## Guardrails

* No "HPX beats Ray" claim, and no "`HpxLane` is faster/slower than `ServiceLane`" verdict.
* No Ray replacement claim; not Ray Serve, not a Ray object store, not arbitrary remote Python execution, not real model inference.
* `work_mode="spin"` is only a synthetic **CPU-bound diagnostic/calibration mode**, used to expose the scheduling bound — it is not the serving design.
* The exp22 and exp23 timing figures are **observation-only and machine-specific**; they are never gated and carry no performance claim.
* No analyzer / benchmark-JSONL-schema / benchmark-driver / CI / public `Future`-ownership change is implied by any of these experiments.
* No HPX internals are exposed to Python; backend selection is visible only through the `lane_impl` knob and the `actor_id` prefix.

## See also

* `docs/reference/rayx_frontend_design.md` §13 — the `lane_impl` backend seam (design).
* `docs/ray_hpx_mapping.md` "Three Stories to Keep Separate" — why the native probe, the task/dataflow style, and the serialized-lane harness stay distinct.
