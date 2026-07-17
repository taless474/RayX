# RayX evidence index

A chronological **“what we learned”** index for the RayX benchmark and experiment
arc. Each entry is a short summary plus a link to the full write-up — this is an
index, **not** a raw run log. Claim fences are preserved: nothing here is a
speedup, a ratio, or an “HPX beats Ray” / “RayX makes Ray faster” claim. The
same-axis *measurements* (exp61 scalar QD1; exp62 distributed fanout/fanin — both
arms timed at the same Python caller boundary) still report the two arms as
**separate per-arm bands** and never difference, ratio, or rank them. Magnitudes
are observation-only and machine-specific unless a write-up says otherwise.

**Current headlines (experiments 61–69):** **exp62 Slice 4b** is the headline
same-axis distributed closed-`int64` fanout/fanin evidence; **exp64 Slice 4** is the
headline payload-size evidence (**within-arm only**, `matched_band_r5`); **exp64
Slice 5** is the completed native-readiness diagnostic (`waiter_resume_at_timeout`
on HPX 1.11; scoped, not a general HPX claim) and **exp64 Slice 5V** is its
completed waiter-fix verification — the identical discriminator on HPX master
`20bc3d4b` (containing PR #7367) resumed on readiness (`waiter_resumed_on_ready`),
so polling is no longer the required readiness workaround on that exact build;
**exp63** is HPX-native composition/progress **mechanism** evidence (no Ray
comparison); **exp61** is the scalar predecessor that established the same
Python-boundary measurement plane; **exp65** is demand-ordered connect-mode
**admission** mechanism evidence, demonstrated on macOS loopback and reproduced
across two Rostam nodes (medusa00/medusa01, TCP `10.42.5.x`, Slurm job 170014); and
**exp66** is the first **in-worker hosting** evidence — a networking HPX
connect-mode locality running in-process inside one Ray actor worker (exact PID
identity, no HPX child), locally and across two Rostam nodes (job 170524);
**exp67** closed the **two-actor shared-runtime** gate — two Ray actors on distinct
nodes each hosting an HPX locality in-process, exchanging verified bidirectional
actor-to-actor HPX actions under a work-free root, locally and across three Rostam
nodes (accepted job 170744); and **exp68** closed the first **deterministic
LLM-shaped distributed-workload** gate — a synthetic vocabulary-sharded top-k run
across the two actors with exact token-ID and float32-bit agreement against an
independent oracle in both coordinator directions, locally and across three Rostam
nodes (accepted job 170746, 204 cross-node candidate bit patterns with zero
mismatches). **exp69** — the strict same-axis Ray-mediated vs HPX-mediated
**performance** comparison of the exp68 workload — is **completed accepted evidence
through Slice 3**: QD1 caller-observed latency (accepted job 171408, 21,600 exact
timed samples, 0 invalid), bounded-concurrency verified-completion goodput (accepted
job 171561, 36,000 verified completions, 0 invalid), and a causal resource-band
decomposition (final curated job 172125, 18,000 exact completions) that resolved the
one observed direction reversal as a thread-supply resource asymmetry with no
implementation defect. No ratios, speedups, differences, or winners anywhere in this
index; the only licensed cross-arm ratios are the scoped, gated exp69 review results
recorded in the exp69 report, and the accepted aggregates' ratio fences remain false.

Top-level reading guides that complement this index:

* [docs/reference/hpxlane_backend_arc.md](reference/hpxlane_backend_arc.md) — `HpxLane` backend arc (exp16 → 20 → 21 → 22 → 23).
* [docs/reference/chunked_service_synthesis.md](reference/chunked_service_synthesis.md) — chunked-service cross-reading (benchmark 09 + experiment 14).

---

## Main benchmark arc (benchmarks 01–10)

The baseline Ray / HPX-native / `rayx` comparison over the shared synthetic
workload and v1 metrics schema.

* [benchmarks/01_ray_single_actor_baseline](../benchmarks/01_ray_single_actor_baseline/ray_single_actor_baseline.md) — one Ray actor is a single serialized server: concurrency deepens its queue rather than parallelizing, so tiny requests are actor/process-overhead-bound and 20 ms requests cap at ~43 req/s.
* [benchmarks/02_ray_hpx_single_lane_comparison](../benchmarks/02_ray_hpx_single_lane_comparison/ray_hpx_single_lane_comparison.md) — HPX's intra-locality C++ control path is far cheaper than Ray's actor-process boundary for tiny/no-op work, and the two converge as service time grows (different boundaries; not a general “HPX is faster” claim).
* [benchmarks/03_ray_hpx_matrix_comparison](../benchmarks/03_ray_hpx_matrix_comparison/ray_hpx_matrix_comparison.md) — the overhead-vs-service crossover holds across the whole service_ms × concurrency matrix: HPX has much lower overhead at tiny service, both converge once the serialized lane dominates.
* [benchmarks/04_ray_hpx_scaling_comparison](../benchmarks/04_ray_hpx_scaling_comparison/ray_hpx_scaling_comparison.md) — with nonzero per-request synthetic service both scale throughput (HPX near-ideal at 5/20 ms, Ray near-ideal at 20 ms but sublinear at 5 ms); apparent no-op negative scaling is the single-client retire loop, not a runtime limit.
* [benchmarks/05_ray_hpx_retire_mode_noop](../benchmarks/05_ray_hpx_retire_mode_noop/ray_hpx_retire_mode_noop.md) — the no-op multi-lane regression is a client-loop / cross-thread coordination artifact, not an HPX lane-scaling limit; batch retirement reduces but does not eliminate it.
* [benchmarks/06_rayx_python_frontend_comparison](../benchmarks/06_rayx_python_frontend_comparison/rayx_python_frontend_comparison.md) — the `rayx` Python frontend preserves most of HPX's low control-plane overhead and stays far below Ray's actor-process cost for native work, without being a Ray replacement. (At the no-op floor, medians of 5: Ray ~313 req/s, HPX-native ~201,725, rayx ~97,671; client-loop-sensitive, converges at `service_ms=20`.)
* [benchmarks/07_rayx_variable_service](../benchmarks/07_rayx_variable_service/rayx_variable_service.md) — under bimodal load more lanes sharply cut `rayx` queueing and absolute tail latency (1→4 lanes: total p99 78→28 ms, ~2.08× throughput bounded by concurrency); compare absolute p99, not the p99/p50 ratio.
* [benchmarks/08_ray_hpx_rayx_variable_service](../benchmarks/08_ray_hpx_rayx_variable_service/ray_hpx_rayx_variable_service.md) — under one shared deterministic sequence, single-lane engines rank by control overhead (rayx ≈ HPX-native > Ray) and converge on the 20 ms service tail, while Ray's fixed per-request overhead keeps its absolute tail higher even as lanes scale.
* [benchmarks/09_chunked_service_cross_driver](../benchmarks/09_chunked_service_cross_driver/chunked_service_cross_driver.md) — chunked sleep service preserves total active work at `delay=0` and adds lane-occupancy ≈ `(chunks-1)×chunk_delay_ms` at `delay>0`; the only backend split is sleep fidelity (Ray ~5% overshoot vs HPX-native/rayx ~23–25%), rayx tracks HPX-native; chunking adds lifecycle/cadence, not a new control-plane story — sleep-mode only, not token streaming.
* [benchmarks/10_rayx_bulk_enqueue](../benchmarks/10_rayx_bulk_enqueue/rayx_bulk_enqueue.md) — `rayx` batch throughput was limited by per-request lane enqueue overhead at multi-lane no-op scale; per-lane bulk enqueue (one lock+notify per lane) removes it without changing the public API or JSONL schema; the win is narrow to no-op/tiny-batch enqueue, not a general workload-speedup or HPX-scheduler claim.

---

## Frontend / serving-control experiments (experiments 01–23)

`rayx` serving-control behavior, the measurement-artifact analysis underneath it,
and the opt-in `HpxLane` backend arc.

### Measurement-artifact & retire-loop analysis (01–06)

* [experiments/01_sleep_overshoot](../experiments/01_sleep_overshoot/sleep_overshoot_note.md) — HPX/rayx `sleep_for` carries a stable ~25% proportional service overshoot vs Ray's ~5%, a backend sleep-fidelity gap (not control cost) that must be read separately when comparing cross-engine service/total.
* [experiments/02_variable_service_lane_sweep](../experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md) — the bimodal lane/actor sweep (1–16) plateaus near ~1390 req/s; the original “coordination ceiling” reading is refined/superseded — it is a closed-loop FIFO-retire / client-driver ceiling.
* [experiments/03_rayx_multiclient_driver](../experiments/03_rayx_multiclient_driver/rayx_multiclient_driver.md) — multiple client threads lift the ~1390 ceiling to ~1740 but saturate quickly; the original GIL-bound reading is superseded by the native C++ driver below.
* [experiments/04_hpx_native_multiclient](../experiments/04_hpx_native_multiclient/hpx_native_multiclient.md) — pure-C++ client threads match `rayx` within ±3%, ruling out Python/GIL as the high-lane bottleneck (later sharpened by the `--diag` decomposition in experiment 06).
* [experiments/06_diag_fifo_ceiling_analysis](../experiments/06_diag_fifo_ceiling_analysis/diag_fifo_ceiling_analysis.md) — preserved diag-1 outputs confirm the bimodal ceiling is a FIFO-retire / client-driver effect: FIFO `one_by_one` ≈1391 req/s, `batch_wait` ≈2544, `submit_all_get_all` ≈2971 (context-only); the penalty sits in the client completion/retire phase while lanes stay under-utilized and balanced.
* [experiments/07_rayx_as_completed](../experiments/07_rayx_as_completed/rayx_as_completed.md) — RayX `Engine.wait` reproduces the native as-completed FIFO-retire fix through Python, lifting L16/c32 bimodal throughput from ~1366 to ~2529 req/s (+85%) without a separate Python/GIL ceiling.

### spin vs sleep diagnostics (05, 08, 09)

`work_mode="spin"` is a synthetic CPU-bound diagnostic/calibration mode, not a serving design.

* [experiments/05_spin_work_mode_knee_sweep](../experiments/05_spin_work_mode_knee_sweep/spin_work_mode_knee_sweep.md) — the CPU-bound spin regime removes the sleep-timer artifact and shows the high-lane saturation knee is a hardware/core-boundary / oversubscription effect, not the HPX worker count.
* [experiments/08_spin_vs_sleep_coordination](../experiments/08_spin_vs_sleep_coordination/spin_vs_sleep_coordination.md) — spin removes the ~25% sleep overshoot (service exactly 1.0/5.0 ms); the high-lane flattening is a CPU/core-boundary effect (parked sleep scales linearly to 8 lanes, CPU-bound spin flattens and its p99 inflates at L8), not Python/facade or HPX coordination overhead.
* [experiments/09_spin_core_boundary_sweep](../experiments/09_spin_core_boundary_sweep/spin_core_boundary_sweep.md) — sweeping spin lanes while varying `hpx_threads`, the knee moves earlier and the ceiling lower as workers increase (svc=1 onset L10→L8→L6 for threads 2→4→8) — the oversubscription signature of a fixed core budget, not worker starvation — while parked sleep stays flat.

### Varied batch / cancellation / chunking (10–14)

* [experiments/10_varied_batch_service_time](../experiments/10_varied_batch_service_time/varied_batch_service_time.md) — with true bulk-varied `submit_batch(service_ms=[...])`, a request's latency in a shared-`submit_ns` batch is set by round-robin lane placement + FIFO queue position: the same 1 ms request swings ~20× (convoy), and long-index periods aligned with lane count segregate heavy work, halving throughput despite equal per-lane counts — count-balanced routing is not work-balanced.
* [experiments/11_queued_cancellation](../experiments/11_queued_cancellation/queued_cancellation.md) — queued-only `engine.cancel(future)` honestly skips service for not-yet-started requests (`status="cancelled"`, ~0 service, `True` iff cancelled), halving drain when survivors are balanced; running work is never interrupted, and survivors can still lane-segregate (cf. exp10).
* [experiments/12_chunked_service](../experiments/12_chunked_service/chunked_service.md) — v1 chunked synthetic service splits total active service without changing it (spin: exactly 8.000 ms for chunks 1/2/4/8 at delay 0); a parked inter-chunk delay adds lane-occupancy ≈ `(chunks-1)×chunk_delay_ms`; one future / one row — synthetic timing only, not token streaming.
* [experiments/13_chunk_boundary_cancellation](../experiments/13_chunk_boundary_cancellation/chunk_boundary_cancellation.md) — running cancellation stops a started chunked request at its next chunk boundary (`1 ≤ chunks_completed < chunks`), never interrupting an active chunk or parked gap; a queued cancel skips the whole lifecycle; the final chunk is a hard commit point — synthetic timing only, not token-stream or Ray task cancellation.
* [experiments/14_spin_chunked_service](../experiments/14_spin_chunked_service/spin_chunked_service.md) — under spin (HPX-native + rayx, 4 HPX threads), chunked service preserves total active service exactly (8.000 ms for chunks 1/2/4/8); a parked `chunk_delay_ms` adds lifecycle ≈ `(chunks-1)×delay` with ~25% overshoot on the gaps only; rayx tracks HPX-native with a small ~10% L8 edge; L8 shows the milder spin core-boundary effect as a lane×core effect independent of chunk count.

### lane_stats & bounded admission (17–19)

* [experiments/17_lane_stats_observability](../experiments/17_lane_stats_observability/lane_stats_observability.md) — `Engine.lane_stats()` shows a backlog live as a round-robin split that drains FIFO to idle; a tail `cancel()` doesn't drop `queue_depth` (cancel doesn't dequeue) but shows up as faster drain / skipped service — a racy, live, observability-only view, not scheduler state, placement control, a synchronization primitive, or any schema change.
* [experiments/18_bounded_admission_burst](../experiments/18_bounded_admission_burst/bounded_admission_burst.md) — bounded admission is local per-lane admission by rejection: under a bursty overflow, the unbounded engine grows a deep per-lane backlog (peak `queue_depth=7`, `queue_wait` p99 ≈ 310 ms) while the capped engine pins `queue_depth` at the cap, admits `lanes*(cap+1)` and sheds the rest as `QueueFullError` (no Future/row), giving admitted work a bounded tail — not Ray Serve backpressure, distributed flow control, or a global cap.
* [experiments/19_bounded_admission_offered_load](../experiments/19_bounded_admission_offered_load/bounded_admission_offered_load.md) — the sustained-flow companion to exp18: bounded admission only acts under sustained overload — below capacity both modes stay at `queue_depth` 0–1; over capacity the unbounded engine's backlog grows for the whole window while the capped engine plateaus and sheds the overflow as `QueueFullError`; grow-vs-plateau is the durable structural result, latencies dominated by the closed-loop retire-at-end driver.

### HpxLane backend arc (15, 16, 20, 21, 22, 23)

The opt-in cooperative `HpxLane` backend (`lane_impl="hpx"`; default `"std"` keeps the `ServiceLane` anchor). Consolidated guide: [docs/reference/hpxlane_backend_arc.md](reference/hpxlane_backend_arc.md). exp22/exp23 timings are observation-only and machine-specific — no “HPX faster/slower than ServiceLane” verdict.

* [experiments/15_hpx_native_lane_feasibility](../experiments/15_hpx_native_lane_feasibility/hpx_native_lane_feasibility.md) — an isolated lane-primitive probe (not corpus-comparable): cooperative `hpx::this_thread::sleep_for` has tighter overshoot than the blocking `std::this_thread::sleep_for` (~+5% vs ~+25% at 20 ms), and plain `hpx::async` no-op dispatch lands in the same order of magnitude as `ServiceLane` — enough to justify considering a future opt-in HPX-native lane, not replacing the anchor.
* [experiments/16_hpx_lane_mechanism_probe](../experiments/16_hpx_lane_mechanism_probe/hpx_lane_mechanism_probe.md) — an opt-in lane-mechanism probe (native-only, corpus-incomparable): a cooperative `HpxLane` preserves actor-like FIFO (single `actor_id`, schema 1) while showing strictly lower sleep overshoot than `ServiceLane` at every service time — the exp15 cooperative-timer advantage survives in a real FIFO lane (a qualified GO on the mechanism), magnitude duration-dependent; `HpxLane` is an opt-in axis, not a replacement and not a general HPX-scheduler result.
* [experiments/20_hpx_task_dataflow_probe](../experiments/20_hpx_task_dataflow_probe/hpx_task_dataflow_probe.md) — a native-only mechanism probe (own schema, not corpus, not a rayx feature): lanes keep all eight lane contracts (one row/request, per-request future ownership, FIFO, real `actor_id`), while scheduler-placed HPX pools (`async`/`dataflow`/`then`) relax identity and FIFO and leave queue/cap/cancel `n/a`; the pools' high sleep “throughput” is cooperative parked-wait overlap, not faster serving (and at the no-op floor the serialized lane is faster) — so the near-term “HPX-native inside” win is the contract-preserving `HpxLane`, while task/dataflow pools are a separate future axis, not a drop-in backend.
* [experiments/21_rayx_hpxlane_backend_parity](../experiments/21_rayx_hpxlane_backend_parity/rayx_hpxlane_backend_parity.md) — rayx-only parity battery comparing `ServiceLane` vs `HpxLane` across completion, `wait`/`as_completed`, chunking, cancellation, bounded admission, `lane_stats()`, and `actor_id` prefixes; semantics/parity evidence, timing non-gating.
* [experiments/22_rayx_hpxlane_load_divergence](../experiments/22_rayx_hpxlane_load_divergence/rayx_hpxlane_load_divergence.md) — under load (contract held, gates G1–G5), the two backends diverge in how concurrency is bounded: under parked sleep both overlap (`HpxLane` overlaps all lanes even at `hpx_threads=1`), while under non-yielding spin `ServiceLane` overlap follows OS/core count and `HpxLane` is bounded near `hpx_threads` — a scheduling-mechanism difference recorded as observation only, never gated.
* [experiments/23_rayx_hpxlane_adapter_hop_cost](../experiments/23_rayx_hpxlane_adapter_hop_cost/rayx_hpxlane_adapter_hop_cost.md) — on an idle pool, the `RayxLaneAdapter<HpxLane>` `run_as_hpx_thread` hop adds single-digit-to-tens-of-µs per call over the no-hop `ServiceLane` (best approximated by `lane_stats()`: `std` p50 ≈ 0.2 µs vs `hpx` ≈ 2 µs at `hpx_threads=1`, ≈ 5.5 µs at 4) — orders below ms-scale service; observation-only, no speedup/verdict.

---

## rayx.runtime / local native actor evidence (experiments 24–26)

The experimental `rayx.runtime` subpackage: registered native C++ operations and
fixed registered local native actor methods over HPX-native FIFO `RuntimeLane`s,
value + measurement-row kept separate. Design notes live under `docs/design/`
(see the README documentation map). Not Ray, no object store / `ObjectRef`, no
arbitrary remote Python, no HPX actions / components / distributed locality.

* [experiments/24_runtime_parked_overlap](../experiments/24_runtime_parked_overlap/runtime_parked_overlap.md) — the first `rayx.runtime` package: lanes parked in `park_ms` suspend cooperatively and free their HPX worker, so many parked lanes progress with few workers (32 lanes × 250 ms drains in ~one park duration even at `hpx_threads=1`, overlap factor ~29) — a scheduling-mechanism demonstration only, synthetic parked work, no performance verdict, no Ray comparison.
* [experiments/25_runtime_dispatch_decomposition](../experiments/25_runtime_dispatch_decomposition/runtime_dispatch_decomposition.md) — a standalone probe priced the nested `hpx::async(exec_, task).get()` inside the `RuntimeLane` worker at ~0.3–0.5 µs/op at `hpx_threads=1` (~0.7–1.1 µs at 2), ~40–50% of the no-op-floor per-op lane cost; this motivated the internal `DispatchPolicy` (instantaneous ops inline, parking/checkpointed/composed work stays async) — observation-only, machine-specific, no verdict.
* [experiments/26_runtime_many_actors_footprint](../experiments/26_runtime_many_actors_footprint/runtime_many_actors_footprint.md) — create→use→release→shutdown→reinit for up to 256 local native actors (gates G1–G7 pass): the lifecycle contract holds at scale; the HPX runtime is the fixed cost (~2.4 MB) and actors are marginal (idle ~23–30 KiB each, ~7–11 µs create/release), and a recycle cycle plateaus (pool reuse, not a leak) — best-effort RSS, observation-only, no capacity claim.

### rayx.endpoint seam (experiments 42–43)

An exploratory, HPX-free local endpoint seam: `rayx.endpoint` exchanges metadata for bootstrap and can deliver one fixed typed `int64` ping across two local actor processes over AF_UNIX IPC. Local cross-process plumbing only — not HPX transport/serving, not a fabric, not parcelport / AGAS, not multi-node. Canonical design: [docs/design/endpoint_runtime_seam.md](design/endpoint_runtime_seam.md).

* [experiments/42_endpoint_bridge_boundary_cost](../experiments/42_endpoint_bridge_boundary_cost/bridge_boundary_cost.md) — observation-only characterization of the endpoint→Runtime path: P0 direct submit, P1 same-process in-process bridge dispatch, P2 cross-process AF_UNIX into a child Runtime; `P1−P0` is the in-process bridge-dispatch difference (not socket cost), `P2−P1` bundles AF_UNIX delivery + accept-thread + a second process + a second HPX runtime (does not isolate transport).
* [experiments/43_endpoint_transport_ping_floor](../experiments/43_endpoint_transport_ping_floor/endpoint_ping_floor.md) — a Runtime-less, HPX-free endpoint ping microprobe: EP0 inline, EP1 cross-process one-shot AF_UNIX ping, EPraw a Python AF_UNIX echo control; EP1 is an undifferentiated end-to-end ping floor and EPraw is not a raw OS lower bound, so `EP1−EPraw` is a cross-implementation observation only — not an OS-floor or fabric claim.

---

## Ray-hosting composition prototypes (experiments 27–30)

The smallest honest realizations of `docs/ray_hpx_mapping.md`'s “Future optional
path — A”: a long-lived `@ray.remote` actor hosts one HPX-backed RayX runtime
in-process. **Composition, not a Ray backend** — Ray owns outer placement/lifecycle,
RayX owns local HPX-backed execution, only plain values cross the boundary. Runnable
pieces live under `bench/` (e.g. `bench/run_ray_hosting_rayx.py`,
`bench/smoke_ray_hosting_rayx_runtime.py`).

* [experiments/27_ray_hosting_rayx_engine](../experiments/27_ray_hosting_rayx_engine/ray_hosting_rayx_engine.md) — a Ray actor can host one `rayx.Engine` (synthetic sleep) cleanly: it retires the RayX future internally and returns plain v1 rows; a three-leg decomposition lands standalone RayX < pure Ray ≈ Ray-hosted RayX (the Ray boundary dominates) — observation-only, not a speedup, not “HPX beats Ray”.
* [experiments/28_ray_hosting_rayx_runtime](../experiments/28_ray_hosting_rayx_runtime/ray_hosting_rayx_runtime.md) — a Ray actor can host one `rayx.runtime.Runtime` running a fixed registered native op (`square`) cleanly: smoke-only composition + lifecycle (one Runtime per actor process; a second Runtime/`Engine` is rejected by the shared guard; two actors get distinct `rt-hpx-` lanes), no timing/JSONL/perf comparison.
* [experiments/29_ray_hosting_rayx_multi_oversubscription](../experiments/29_ray_hosting_rayx_multi_oversubscription/ray_hosting_rayx_multi_oversubscription.md) — multi-actor resource budgeting (spin diagnostic, `lane_impl="std"`): the binding resource is concurrently-active CPU-bound lanes vs physical cores, not `hpx_threads`; size `num_cpus_per_actor` to active demand (`max(num_lanes, hpx_threads)` conservative), keep Σ active lanes ≤ cores; over-reserving past logical CPUs leaves actors PENDING. Idiomatic HPX would coordinate cores in one runtime via resource partitioning/executors.
* [experiments/30_ray_hosting_rayx_runtime_counter](../experiments/30_ray_hosting_rayx_runtime_counter/ray_hosting_rayx_runtime_counter.md) — a Ray actor can host one Runtime and one local native `counter` actor cleanly (state `initial → add → get → reset`; two Ray actors hold independent counters with distinct `rt-act-` ids; explicit `release_actor`; clean idempotent shutdown), with the handle/future/result retired inside the actor and only plain Python scalars crossing — no timing/JSONL/perf comparison.

---

## Runtime / adapter experiments (experiments 31–38)

These bound the runtime arc and trace the **adapter mechanism** story inside the
Ray boundary: a control-plane STOP, a narrow CPU-scaling SUPPORT, then the
exp35→exp38 arc isolating how adapter design preserves or hides HPX cooperative
behavior under a synthetic parked+compute mix. All observation-only,
machine-specific; non-blocking stays experimental and op-lane-only.

* [experiments/31_runtime_control_plane_under_load](../experiments/31_runtime_control_plane_under_load/runtime_control_plane_under_load.md) — with the runtime saturated by CPU-bound Async work, control-plane ops crossing `run_as_hpx_thread` stay bounded absolutely (worst saturated p99 ≈ 0.1 ms, ~50× under a 5 ms gate) and `cancel` stays flat — an evidence-backed STOP: HPX thread priorities / named pools / resource partitioning are not motivated by this evidence.
* [experiments/32_ray_hosting_rayx_cpu_scaling](../experiments/32_ray_hosting_rayx_cpu_scaling/ray_hosting_rayx_cpu_scaling.md) — inside one long-lived Ray actor, RayX/HPX scales Async native CPU work (`busy_sum`) across workers while an equivalent pure-Python loop stays GIL-bound at ~1×: a narrow SUPPORT for intra-process native CPU scaling. Rostam-validated (`medusa06`, 40-core Xeon): `--quick` `1.00 → 1.98 → 3.77`, `--decouple` supports `min(num_lanes, hpx_threads, cores)`, `--full` clean through **W=16** (efficiency ≈ 0.89–0.90); **W=32** still improves (~17–18×) but is lower-efficiency / placement-sensitive (≈ 0.53–0.56, bimodal). Not “RayX makes Ray faster”, not “HPX beats Ray”, not Ray cluster scaling, not a sizing claim. Curated: [aggregate_rostam_40core.json](../experiments/32_ray_hosting_rayx_cpu_scaling/aggregate_rostam_40core.json).
* [experiments/33_ray_hosting_rayx_scaling_knee](../experiments/33_ray_hosting_rayx_scaling_knee/ray_hosting_rayx_scaling_knee.md) — intra-actor scaling is granularity-sensitive rather than governed by one universal knee: fine-grain work rolls off at W=32, coarse-grain work has no clear knee through W≤32 and restores W=32 efficiency. Observation-only.
* [experiments/34_ray_hosting_rayx_serving_mix](../experiments/34_ray_hosting_rayx_serving_mix/ray_hosting_rayx_serving_mix.md) — a synthetic serving mix has two readings: the default S/C FIFO-adapter baseline reads BASELINE/STOP (over-load S-p90 inflation is a FIFO-adapter head-of-line property, not HPX scheduling), while the opt-in `--with-parked` cooperative-overlap path reads SUPPORT 3/3 (median `overlap_ratio` ≥ 2.0); `park_ms` is synthetic, not real I/O. Not Ray Serve, not cluster scaling, not a capacity claim. Curated: [aggregate_rostam_40core.json](../experiments/34_ray_hosting_rayx_serving_mix/aggregate_rostam_40core.json).
* [experiments/35_ray_hosting_rayx_parked_overlap](../experiments/35_ray_hosting_rayx_parked_overlap/ray_hosting_rayx_parked_overlap.md) — a stricter three-arm adapter-preservation probe reads a valid STOP: the Ray-hosted adapter does not broadly preserve compute-class retention under added parked load (fine-grain `thr_retention` ~0.26), but it is load-shape/admission-sensitive (coarse and W=16 fine/over preserve cleanly), and `lane_stats` showed HPX backlogged/active — compute-class retention erosion (shared-FIFO admission + CPython closed-loop driver), not park-vs-compute serialization. HPX cooperative suspension itself is true by construction.
* [experiments/36_ray_hosting_rayx_lane_headofline](../experiments/36_ray_hosting_rayx_lane_headofline/ray_hosting_rayx_lane_headofline.md) — a no-new-API diagnostic isolates exp35's eroder: the serial `RuntimeLane` consumer waits on `hpx::async(…).get()`, so a park holds its lane and round-robin queues compute behind it (per-lane FIFO head-of-line). Fixing `hpx_threads` and sweeping `num_lanes` recovers compute retention (FULL SUPPORT at light load, PARTIAL under heavier) — attributable to lane-level admission, not HPX failing to park; more lanes is a diagnostic lever, not the production design.
* [experiments/37_ray_hosting_rayx_nonblocking_lane](../experiments/37_ray_hosting_rayx_nonblocking_lane/ray_hosting_rayx_nonblocking_lane.md) — the experimental non-blocking op-lane (`hpx::async(…).then(…)`, bounded by `max_inflight_per_lane`) removes the per-lane head-of-line that exp36 could only dilute, at fixed `num_lanes == hpx_threads`: FULL SUPPORT at near load (clean step at `nb-mi2`), PARTIAL under over-load (a ramp signalling a residual limiter). Non-blocking stays experimental and op-lane-only.
* [experiments/38_ray_hosting_rayx_nonblocking_residual](../experiments/38_ray_hosting_rayx_nonblocking_residual/ray_hosting_rayx_nonblocking_residual.md) — a counter-free diagnostic attributes the exp37 over-load PARTIAL primarily to an undersized admission cap (`max_inflight=4`), not retirement-path cost: extending the cap drained the lane queue and restored compute retention to SUPPORT (qd→0 = H-CAP). `max_inflight` is a diagnostic lever, not sizing guidance; no retirement-path optimization is motivated.

---

## In-process HPX composition (experiments 39–44)

Boundary reduction plus HPX-faithful native composition behind one coarse
Python/Runtime boundary. Structural / mechanism evidence on a synthetic, local
runtime: no speedup, no throughput, no latency, no Ray comparison, no parallelism,
no fabric claim. The future distributed-fabric direction stays gated.

* [experiments/39_native_continuation_vs_mediated_chain](../experiments/39_native_continuation_vs_mediated_chain/native_continuation_vs_mediated_chain.md) — native loop vs scheduled `future::then` vs Python-mediated chain: for dependent synthetic native work inside one runtime, repeated Python/pybind/RuntimeFuture/lane boundary cost scales strongly with chain length, while scheduled `future::then` overhead is visible but much smaller — supports the in-process native-composition thesis only.
* [experiments/40_native_independent_overlap](../experiments/40_native_independent_overlap/native_independent_overlap.md) — independent-chain overlap, RayX lane-level (control) vs HPX-native intra-op `chain_fanout` (load-bearing): on Rostam (gates PASS 74/74) Arm B overlaps independent native work near-ideal up to ~4 effective workers, saturating below ideal at 8 (a characterized K=8 ceiling) — fixed-core overlap mechanism only, no NUMA/socket claim, not a multi-node or “HPX beats Ray” result.
* [experiments/41_barrier_witness_spike](../experiments/41_barrier_witness_spike/barrier_witness_spike.md) — a standalone native HPX cooperative-interleaving witness (a shared gate; tasks suspend mid-execution, the scheduler runs others, the last arriver opens the gate): load-bearing as a clean pass at `--hpx:threads=1`, with forced-failure signatures and an external subprocess timeout as the anti-hang guard — cooperative interleaving / suspension / resume only, the canonical artifact (deliberately not promoted into a Runtime op).
* [experiments/44_barrier_fanin_witness](../experiments/44_barrier_fanin_witness/barrier_fanin_witness.md) — the witnessed barrier-gated fan-in keystone, integrating exp39/40/41 behind one Runtime boundary: one coarse registered op launches bare `hpx::async` leaves, mutually gates them, joins with `when_all`, reduces with a scheduled continuation, and completes at `--hpx:threads=1` with witness gates passing (a non-cooperative interior would deadlock at one worker) — structural / mechanism evidence, not performance, not a Ray comparison, not parallelism, not fabric; the value oracle is correctness only.

(exp45–48 are in-process Runtime-boundary / in-substrate-reference characterization probes; see their write-ups under `experiments/`.)

---

## HPX island lifecycle / Ray-orchestrated bootstrap (experiments 49–52)

A single-node, loopback-TCP, closed-`int64` mechanism/characterization arc that
establishes the **island lifecycle boundary** before any fabric work, and the
architecture policy: **Ray is the placement / bootstrap / supervision plane** and
**HPX is the execution / data plane inside one HPX island**; an ungraceful HPX
locality death poisons the whole island and the recovery policy is **whole-island
external restart, not in-place repair of stale AGAS state** — external supervision,
not HPX fault tolerance. No fault-tolerance, crash-recovery, multi-node,
general-fabric, production, or performance claim; the future distributed-fabric
direction stays gated.

* [experiments/49_strong_l4_hpx_distributed_spike](../experiments/49_strong_l4_hpx_distributed_spike/strong_l4_hpx_distributed_spike.md) — the Ray-free HPX connect-mode graceful lifecycle passes: a root admits connector #1, serves a registered closed-`int64` HPX action, the connector self-disconnects via `post(disconnect)+stop`, the root re-admits and serves connector #2, and finalizes cleanly. Mechanism/lifecycle feasibility only.
* [experiments/50_strong_l4_connect_failure](../experiments/50_strong_l4_connect_failure/strong_l4_connect_failure.md) — ungraceful connector loss (SIGKILL): the root can still admit and serve a fresh connector by set-difference targeting, but AGAS retains the stale dead locality and the root hangs at collective finalize — usable for a new peer but clean shutdown is poisoned; explicitly not fault tolerance.
* [experiments/51_strong_l4_stale_locality_shutdown](../experiments/51_strong_l4_stale_locality_shutdown/strong_l4_stale_locality_shutdown.md) — the stale-locality shutdown boundary: bounded finalize and local-cache cleanup both return but do not cure the hang (a backtrace localizes a never-signaled `runtime_distributed::wait()`); external whole-island restart yields a clean fresh island — policy, not in-place repair. No public AGAS stale-locality eviction path found.
* [experiments/52_ray_bootstrap_clean_island](../experiments/52_ray_bootstrap_clean_island/ray_bootstrap_clean_island.md) — the first Ray-orchestrated clean bootstrap passes: two Ray actors launch the HPX root/connector child processes, Ray carries only bootstrap metadata, the HPX action travels HPX → HPX over the parcelport (never through Ray), and the island tears down via the exp49 graceful path — Ray launch/bootstrap/supervision plumbing, not a new HPX mechanism. Clean path only; the whole-island-fatal policy is assumed, not exercised.

(exp53–57 continue the supervised-restart / poison-detection / two-node parcelport path that leads into the exp58–60 characterization below; see their write-ups under `experiments/`.)

---

## Two-node path characterization (experiments 58–60)

A two-node Rostam (medusa00/medusa01, eno16, `10.42.5.`) characterization of the
**QD1 closed-`int64` micro-call path**, with **strictly different measurement
planes** held apart: **Ray numbers are Python/`ray.get`-observed actor RTT**;
**HPX numbers (exp58/exp60) are caller-observed C++ `hpx::async(...).get()` RTT**.
These are **not the same measurement axis**. They are **precursor / within-runtime
decomposition** evidence for the same-axis bands (exp61 scalar QD1, exp62
distributed fanout/fanin), **not** same-axis headline evidence.

**Claim fences:** no speedup, no ratio, no “HPX beats Ray”, no “RayX makes Ray
faster”, no same-axis Ray-vs-HPX comparison. Closed-`int64` QD1 micro-workload;
Rostam-allocation-specific; HPX is TCP-parcelport-specific; the future
distributed-fabric direction stays gated.

* [experiments/58_two_node_clean_path_perf](../experiments/58_two_node_clean_path_perf/two_node_clean_path_perf.md) — **exp58**: HPX inter-node action path (R=5), caller-observed C++ `hpx::async(...).get()` RTT over the TCP parcelport, p50 ~115.8 µs, p99 ~185.7 µs (warm-path, idle-backoff disabled, `tcp_nodelay` unverified) — not a user-facing Python call-path number.
* [experiments/59_ray_actor_vs_hpx_action_path](../experiments/59_ray_actor_vs_hpx_action_path/ray_actor_vs_hpx_action_path.md) — **exp59**: Ray actor path through Slice 5. Same-host Ray control p50 ~609 µs (R=1 caveat); two-node placement proof PASS; two-node Ray actor path R=5 p50 ~742 µs, p99 ~1190 µs. Slice 5 is a plane-labeled juxtaposition only — not a same-axis comparison. Placement is proven by hard `NodeAffinity(soft=False)` + resolved Ray `node_id` + FQDN-normalized hostname; oracle correctness proves the intended actor executed and returned the expected closed-`int64`, not physical placement by itself. The juxtaposition + both within-runtime decompositions live in `ray_vs_hpx_plane_labeled_aggregate.json`.
* [experiments/60_hpx_same_node_locality_control](../experiments/60_hpx_same_node_locality_control/run_exp60_control.py) — **exp60**: HPX same-node two-locality TCP control (R=5), two HPX localities co-located on medusa00 over the TCP parcelport (kernel loopback), p50 ~76.6 µs, p99 ~101.8 µs — within-HPX decomposition only; reuses the exp58 binary unmodified.

**Within-runtime decompositions (each stays inside its own runtime, never crossed):**

* **Ray:** same-host ~609 µs of cross-node ~742 µs → cross-node increment ~133 µs (R=1 same-host caveat).
* **HPX:** same-node TCP ~76.6 µs of inter-node TCP ~115.8 µs → wire increment ~39 µs (kernel loopback ≠ zero cost).

**Shared interpretation:** in both runtimes the QD1 floor is dominated by **local
stack, not the physical inter-node hop**.

---

## Same-axis Python-boundary comparison (experiment 61)

The first experiment that moves **both** arms onto the **same measurement axis**.
exp58 timed HPX from **C++** (`hpx::async(...).get()`) and exp59 timed Ray from
**Python** (`ray.get(...)`) — different caller boundaries, hence a plane-labeled
juxtaposition, not a same-axis number. exp61 closes that gap by timing both paths
at the **same Python caller boundary** (`perf_counter_ns` around one blocking
call), using a closed-`int64` QD1 micro-workload and the same oracle family as
exp58. The HPX side is an **experiment-only** pybind binding (`ext.dist_probe_remote(x)`)
under `experiments/`; it is **not** shipped `rayx.runtime` API and does not give the
public RayX API distributed actions.

**Claim fences:** the two arms are summarized **separately** and reported side by
side — **no speedup, no ratio, no arm differencing, no “HPX beats Ray”, no “RayX
makes Ray faster”**, no production / real-inference / Ray-Serve / object-store /
task-semantics / fault-tolerance / scaling claim. Rostam-allocation-specific;
HPX side is experiment-only and TCP-parcelport-specific.

* [experiments/61_python_boundary_same_axis_ray_vs_rayx](../experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md) — **exp61 Slice 4**: R=5 matched same-axis Python-boundary band, job `158724`, medusa00 → medusa01, subnet `10.42.5.`, K=1000 / W=100 / prewarm=1, `clock=perf_counter_ns`, boundary `python_caller_perf_counter_ns_around_blocking_call`. All Slice-4 gates passed (`overall=pass`, no failed gates): five matched islands, both arms `two_node_exercised` and oracle-correct, cross-island agreement on job / node pair / subnet / K / W / prewarm / clock / boundary, clock overhead captured (median 92 ns, count 4096). Resulting flags `same_axis_comparison=true`, `comparison_kind=r5_matched_same_axis_band_no_ratio`, `speedup_computed=false`, `ratio_reported=false`, `arms_differenced=false`. Per-arm RTT bands (across-island median of per-island percentiles), reported separately: **Ray actor path** (`ray.get(actor.dist_probe.remote(x))`) p50 ~518.3 µs, p90 ~850.7 µs, p99 ~1125.7 µs, mean ~584.7 µs; **experiment-only Python→HPX action path** (`ext.dist_probe_remote(x)`) p50 ~184.8 µs, p90 ~257.5 µs, p99 ~322.6 µs, mean ~188.7 µs. Curated `slice4_band_158724_aggregate.json` + the five `slice3_band_158724_i{1..5}_manifest.json` are tracked; raw `slice3_band_158724_i{1..5}_{hpx,ray}.json` are gitignored but kept locally.

**What this licenses:** *for this QD1 closed-`int64` micro-call on medusa00 →
medusa01, measured at the same Python caller boundary in matched R=5 runs, the Ray
actor path and the experiment-only Python→HPX action path produced the per-arm RTT
bands above.* Nothing beyond that — the bands are not differenced, ratioed, or
ranked.

* [experiments/61_python_boundary_same_axis_ray_vs_rayx](../experiments/61_python_boundary_same_axis_ray_vs_rayx/python_boundary_same_axis_ray_vs_rayx.md) — **exp61 Slice 5 (same-node placement control)**: R=5 matched **same-node** band, job `158734`, **single** node `medusa00`, subnet `10.42.5.`, K=1000 / W=100 / prewarm=1, same Python caller boundary. Holds each arm's mechanism constant and varies only co-location: the HPX arm runs **two distinct localities on one host over the loopback TCP parcelport** (`ext.dist_probe_remote(x)`, root locality 0 + connector locality 1, **not** `find_here`), the Ray arm a **genuine actor pinned to the driver's own node** (`NodeAffinity(soft=False)`). Per-locality resources are comparable to the cross-node band: root **4 threads** on cpuset `[0,1,2,3]`, connector **4 threads** on cpuset `[4,5,6,7]` (disjoint, enforced). All gates passed (`overall=pass`): `same_axis_comparison=true`, `comparison_kind=r5_matched_same_node_band_no_ratio`, `speedup_computed=false`, `ratio_reported=false`, `arms_differenced=false`, `placement_bands_differenced=false`; per island `hpx_tcp_nodelay_verified` (getsockopt on 8 live peer sockets), `disjoint_core_binding_verified` (effective root `[0,1,2,3]` / connector `[4,5,6,7]`), `same_node_colocated`, and `ray_actor_on_driver_node` all true; clock overhead median 83 ns. Per-arm same-node RTT bands (across-island median, reported **separately**): **Ray** (`ray.get(actor.dist_probe.remote(x))`) p50 ~519.1 µs, p90 ~790.9 µs, p99 ~1028.6 µs, mean ~559.0 µs; **experiment-only Python→HPX** (`ext.dist_probe_remote(x)`) p50 ~93.0 µs, p90 ~102.3 µs, p99 ~112.9 µs, mean ~94.1 µs. Curated `slice5_samenode_band_158734_aggregate.json` + the five `slice5_sn_sn_band2_158734_i{1..5}_manifest.json` are tracked; raw `slice5_sn_sn_band2_158734_i{1..5}_{hpx,ray}.json` are gitignored. **Audit note:** a first same-node band (job 158732) reported a high quantized HPX tail (p99 ~6975 µs); a skeptical audit traced this to a **resource-shape confound** — the connector was bound with `srun --cpu-bind=map_cpu` (one core per task), so it ran a single worker thread and hit HPX idle-backoff at QD1. Corrected to `--cpu-bind=mask_cpu` + explicit `--hpx:threads` (probe job 158733 confirmed the tail collapsed); job 158734 is the canonical run. 158732 passed every structural gate (min latency ~90 µs), so it is a valid mechanism pass but its timing band is **not** used.

**What Slice 5 controls / does not claim:** it **controls for physical co-location** within each arm and **passed its gates** as a same-node control with comparable per-locality resources. With that corrected shape the experiment-only HPX same-node arm is **tight and consistent** (per-island p99 ~108–127 µs); numbers are observation-only and machine-specific. It is **not** a win, **not** compared to the Ray arm or to the Slice-4 cross-node band by ratio/winner. **exp61 is the scalar QD1 same-axis evidence (Slice 4 cross-node band, Slice 5 same-node control); exp62 (below) extends the same-axis methodology to a distributed fanout/fanin workload and is the current strongest same-axis distributed evidence.**

---

## Same-axis distributed fanout/fanin (experiment 62)

The **current strongest same-axis distributed evidence** is **Slice 4b** (matched
R=5 band across **≥2 remote localities/nodes**), extending exp61 from a single scalar
QD1 remote call to a **semantic distributed workload** measured at the same Python
caller boundary: one outer blocking call `fanout_fanin(x, N) -> int64` that dispatches
**N=8 leaf actions across ≥2 remote localities/nodes** (all-remote: the
root/coordinator runs no leaves) and **reduces** them to one closed-`int64` value.
QD1 at the outer boundary, but each call now does real distributed fanout and a
fan-in reduction across multiple remotes, so no reading is attributable to a
single-call or single-remote artifact. The placement-independent oracle
`leaf(x,i) = (x ^ 0x52415958) + (i<<1)`, composite = int64 sum mod 2^64, proves
intended execution only; distribution is proven **separately** by per-leaf locality
witness, hard placement gates, attested transport, and node/locality ids. The HPX
side is an **experiment-only** pybind binding (`ext.fanout_fanin_remote(x, N)`);
it is **not** shipped `rayx.runtime` API and gives the public RayX API no
distributed actions.

**Claim fences:** the two arms are summarized **separately** and reported side by
side — **no speedup, no ratio, no arm differencing, no placement-band differencing,
no “HPX beats Ray”, no “RayX makes Ray faster”**, no production / real-inference /
Ray-Serve / object-store / fault-tolerance / scaling claim. Slice 4b covers **≥2
remote localities/nodes** (Slice 3 was one remote locality); the HPX arm uses the
proven `root_flat_gather_poll` interim composition (not a final HPX-native collective).
Rostam-allocation-specific; HPX side is experiment-only and TCP-parcelport-specific.

* [experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md](../experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md) — **exp62 Slice 3** (one remote locality; **superseded by Slice 4b** below as the strongest exp62 same-axis evidence): R=5 matched cross-node distributed fanout/fanin band, job `158809`, medusa00 → medusa01, subnet `10.42.5.`, N=8, all-remote, **one remote locality**, K=1000 / W=100 / prewarm=1, `clock=perf_counter_ns`, boundary `python_caller_boundary`. Slice history: Slice 0 pure-Python scaffold, Slice 1 local HPX mechanism smoke, Slice 2 HPX-only one-remote dry-run, Slice 3 the matched band here. All five pair manifests passed (19 correlation gates each) and the combined band aggregate passed (13 gates): both arms oracle-correct (composite `11040115504`), both `leaves_local=0` / `leaves_remote=8` / `witness_leaf_count=8`; HPX per island `hpx_tcp_nodelay_verified` / `parcelport_transport=tcp` / connector cpuset `[0,2,4,6,8,10,12,14]` (8, not collapsed) / `threads_cover_fanout` / `no_dispatch_timeout` / `timed_out_leaf_count=0` (`composition_primitive=hpx::async+is_ready_poll`); Ray per island `hard_placement` / `soft=false` / `single_submission` / `coordinator_single_submission` / `leaves_on_target_node` / `coordinator_on_driver_node` / `ray_head_num_cpus=0` / `ray_coordinator_num_cpus=0` / `ray_no_dispatch_timeout`; `cross_island_agreement=true`; teardown `no_orphans=true`. Resulting flags `same_axis_comparison=true`, `speedup_computed=false`, `ratio_reported=false`, `arms_differenced=false`, `placement_bands_differenced=false`. Per-arm RTT bands (across-island median of per-island percentiles, reported **separately**): **Ray actor/task path** (`ray.get(coordinator.remote(x, N))`) p50 ~3640.9 µs, p90 ~3895.6 µs, p99 ~6407.0 µs, mean ~3718.6 µs; **experiment-only Python→HPX action path** (`ext.fanout_fanin_remote(x, N)`) p50 ~345.4 µs, p90 ~401.7 µs, p99 ~466.2 µs, mean ~359.0 µs. Curated `exp62_fanout_band_158809_aggregate.json` + the five `exp62_fanout_band-158809_i{1..5}_manifest.json` are tracked; raw `exp62_fanout_band-158809_i{1..5}_{hpx,ray}.json` and the `_exp62_runs/` provenance (attest, bootdirs, ray logs, batch log) stay gitignored.

**What this licenses:** *for this specific QD1 closed-`int64` distributed fanout/fanin
microbenchmark, N=8, all-remote, one remote locality, cross-node medusa00→medusa01,
measured at the same Python caller boundary, the experiment-only HPX action path shows
a lower RTT band than the Ray actor/task path. This is a structurally valid same-axis
juxtaposition, not a ratio, speedup, or winner claim, and not shipped `rayx.runtime`
distributed API.* Nothing beyond that — the bands are not differenced, ratioed, or
ranked. This does not motivate runtime/API work.

* **exp62 Slice 4a — HPX-only ≥2-remote-locality fanout/fanin mechanism** (`hpx-multi-remote-smoke`, job `158813`): a **3-node** `--exclusive` allocation (medusa00 root; medusa01/`10.42.5.31`/locality 1 and medusa02/`10.42.5.32`/locality 2 connectors), subnet `10.42.5.`, N=8, all-remote round-robin, `node_placement=cross_node_multi_remote`, `composition_primitive=root_flat_gather_reduce`, watchdog `bounded_is_ready_poll_50us`. `overall=pass`, all 17 gates True: `n_remote_localities=2`, `remote_locality_ids=[1,2]`, `leaves_per_remote_locality={1:4, 2:4}`, `leaves_local=0`, `leaves_remote=8`, `witness_leaf_count=8`, root ran zero leaves, composite oracle correct (`11040115504`), `no_dispatch_timeout` / `timed_out_leaf_count=0`; both connectors `transport=tcp` / `tcp_nodelay_verified` / `joined` / `served` / `graceful_disconnect`, attested cpuset `[0,2,4,6,8,10,12,14]` (8, not collapsed). Raw run + per-connector lifecycle provenance (`connect.preprobe_ok` / `connect.joined1` / `served1.ok` / `attest_connect.json` / `connect.disconnected1`) stay gitignored under `_exp62_runs/slice4a_copyback/`; like the Slice 2 dry-run there is no separately-tracked curated artifact. **This validates the ≥2-remote-locality HPX fanout/fanin mechanism only. It is HPX-only and sets `same_axis_comparison=false`; it is not a Ray comparison.** Cleanup: allocation released cleanly and both connectors reported `graceful_disconnect=true`; orphan-freedom basis is the graceful-disconnect witnesses plus clean Slurm teardown (compute nodes are not SSH-reachable post-allocation for a `pgrep` scan). Slice 4b (below) completed the matched Ray band at this ≥2-locality shape.

* **A pre-Slice-4b HPX-native composition spike was an informative negative** (job `158814`, `when_all_then_reduce`): mathematically correct but the composed future stalled to the dispatch timeout (~30 s) cross-node. The native continuation modes (`when_all_then_reduce` / `dataflow_reduce`) are kept in-tree but **gated off, flagged experimental / cross-node-unvalidated**, with a runtime guard; `root_flat_gather_poll` remains the **proven** cross-node composition. HPX collectives / tree reduction remain the target for a later HPX-native reduction spike.

* [experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md](../experiments/62_distributed_fanout_same_axis/distributed_fanout_same_axis.md) — **exp62 Slice 4b — current strongest exp62 same-axis distributed evidence**: R=5 matched **multi-remote** distributed fanout/fanin band, job `158817`, **medusa00 → medusa01 + medusa02** (root/head/coordinator on medusa00), subnet `10.42.5.`, N=8, all-remote, **two remote localities/nodes** (4/4 split), K=1000 / W=100 / prewarm=1, `clock=perf_counter_ns`, boundary `python_caller_boundary`, `node_placement=cross_node_multi_remote`. HPX composition is the **proven `root_flat_gather_poll`** (`composition_primitive=root_flat_gather_reduce`, watchdog `bounded_is_ready_poll_50us`, `hpx_native_composition=false`); drivers launched on medusa00 via `srun --overlap`. All five HPX arms, all five Ray arms, and all five pair manifests passed; the combined aggregate passed (failed gates: none). Both arms oracle-correct (composite `11040115504`), both `leaves_local=0` / `leaves_remote=8` / `witness_leaf_count=8`, both cover **both** remotes, both `no_dispatch_timeout` / `timed_out_leaf_count=0`. HPX: all ten connectors (medusa01 + medusa02 × 5) `joined`/`served`/`graceful_disconnect`, `tcp_nodelay_verified`, cpuset `[0,2,4,6,8,10,12,14]` (8, not collapsed). Ray: head on medusa00 `num_cpus=0`; coordinator hard-pinned to medusa00 `num_cpus=0` running zero leaves; leaves hard-pinned (`soft=false`) round-robin across the two remote node ids; `no_orphans=true`. Flags `same_axis_comparison=true`, `cross_island_agreement=true`, `node_set=[medusa00, medusa01, medusa02]`, `speedup_computed=false`, `ratio_reported=false`, `arms_differenced=false`, `placement_bands_differenced=false`. Per-arm RTT bands (across-island median of per-island percentiles, reported **separately**): **Ray coordinator/task path** (`ray.get(coordinator.remote(x, N))`) p50 ~3717.4 µs, p90 ~3874.4 µs, p99 ~7012.5 µs, mean ~3805.4 µs; **experiment-only Python→HPX action path** (`ext.fanout_fanin_remote(x, N)`, poll) p50 ~249.5 µs, p90 ~270.7 µs, p99 ~320.2 µs, mean ~251.5 µs. Curated `exp62_fanout_mrband_158817_aggregate.json` + the five `exp62_fanout_mrband_158817_i{1..5}_manifest.json` are tracked; raw `exp62_fanout_mrband_158817_i{1..5}_{hpx,ray}.json` and the `_exp62_runs/` provenance (connector bootdirs, ray logs) stay gitignored.

**What this licenses:** *for this synthetic closed-`int64` N=8 fanout/fanin workload, measured at the same Python caller boundary with matched 3-node topology (medusa00 → medusa01/medusa02, all-remote, 4/4 split), the experiment-only Python→HPX path and the Ray coordinator path produced the separate per-arm RTT bands above.* Nothing beyond that — the bands are not differenced, ratioed, ranked, or called winner/loser; `root_flat_gather_poll` is a proven interim composition (not a final HPX-native collective); this is not shipped `rayx.runtime` distributed API and not a production/serving/inference claim.

---

## HPX-native composition / progress diagnosis (experiment 63)

exp63 resolved the native-composition/progress concern raised by the exp62 Slice-4b
spike (the `when_all_then_reduce` cross-node stall). The earlier failure was traced to
**connector lifetime**, not an intrinsic HPX native-progress failure. **This is
mechanism evidence only:** no Ray comparison, no performance numbers, no HPX
`collectives` claim, and no payload claim.

**Claim fences:** cross-node mechanism/topology validation only; no Ray-vs-HPX timing,
no speedup/ratio/winner, no `hpx::collectives` claim (deferred pending
membership/generation/timeout/poison design), no payload claim.

* **Root cause + connector-lifetime hardening** (job `159061`): the connector-side
  fault was `HPX(invalid_status): thread pool is not running` during parcel scheduling
  (`parcel::load_schedule`) — not a kernel EPERM (strace showed zero EPERM syscalls).
  Serve-timeout sweep: **90 s → fault at call 7**, **150 s → fault at call 14**,
  **300 s → pass**, **600 s → pass**. A hardened **heartbeat / root-completion
  lifetime** fixed it: at serve-timeout=90 the hardened run passed **20/20** with
  shutdown reason `root_completion_signal`, while a no-completion control reproduced the
  call-7 fault with `serve_timeout_expired`.
* [experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md](../experiments/63_hpx_native_collective_reduction/hpx_native_collective_reduction.md) — **exp63 Slice 2a** (job `159167`, root medusa06, connectors medusa07/medusa08, N=8, prewarm=5, K=20, serve-timeout=90 with the hardened lifetime): native cross-node composition validated — `when_all_then_reduce` **pass, 20/20** and `dataflow_reduce` **pass, 20/20** (both `cross_node_composition_validated=true`, `root_completion_signal ×2`, no late parcel); `root_flat_gather_poll` mechanics pass but is a **polled control**, **not** native-validated (`polled_in_success_path=true`).
* **exp63 Slice 2b** (job `159200`, root medusa11, connectors medusa12/medusa13): depth-2 star / root-of-partials fan-in, honest topology `depth2_star_of_partials_contiguous_blocks` — one partial action per remote locality, partials `[4, 4]` across the two remotes, root composes 2 partial futures instead of 8 leaf futures. `dataflow_reduce` **pass, 20/20** and `when_all_then_reduce` **pass, 20/20** (topology valid, local partial oracles correct, validated true); flat controls validate; flat `root_flat_gather_poll` mechanics pass but is not native-validated. It uses hand-rolled action futures, **deliberately not** `hpx::collectives::reduce`.

**What this licenses:** the HPX-native cross-node composition path is **viable once
connector lifetime is correct**, and root-of-partials composition works cross-node.
It claims **no** Ray performance, **no** HPX `collectives`, and **no** payload
behavior; `root_flat_gather_poll` remains a mechanics control, not a native-validated
composition.

---

## Payload fanin size sweep (experiment 64)

exp64 extends the same-axis distributed direction from scalars to a **payload-size**
axis, measured at the same Python caller boundary. Each remote leaf returns `S` opaque
payload bytes (plus its closed scalar); Python folds and checks the payload **digest**
after timing, outside the RTT window, identically for both arms. The payload-size axis
now has HPX smoke (Slice 1), Ray matched smoke (Slice 2), a **structural R=1** matched
ladder (Slice 3), an **R=5** matched band (Slice 4), and a completed **HPX-only
native-readiness diagnostic** (Slice 5, Phase A→A4).

**Claim fences:** the payload distributions are **within-arm only** — **no ratios, no
speedups, no cross-arm differences, no winner**. HPX remains `root_flat_gather_poll`, a
poll-gather payload **baseline** (**not** the exp63 native-composition payload path);
the HPX serialization **runtime** (zero-copy) path is **not observed** (config-level
flags are; the per-call path taken is not); the Ray object/plasma return path is **not
observed**; the closed digest/oracle is the only cross-arm anchor; no real
inference/model payload.

* **exp64 Slice 3 — structural R=1 matched ladder** (job `159384`, medusa[11-13], ladder `[0,64,1024,16384,262144]`, N=8, prewarm=3, measured=5): both arms ran the full ladder in one allocation (HPX phase then Ray phase, Ray driver step `--cpu-bind=none`) and were paired by a pure manifest. `overall_manifest_pass=true`, `same_axis_comparison=true` (**structural-correlation flag only**), `evidence_grade=structural_r1`, all 27 correlation gates passed, all fences False, `no_cross_arm_timing_computed=true`. Machinery validation, **not** distributional evidence.
* [experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md](../experiments/64_payload_fanin_size_sweep/hpx_payload_fanin.md) — **exp64 Slice 4 — headline payload-size band** (band `band20260702_174335`; jobs `159385` / `159386` / `159388` / `159389` / `159390`; R=5 **fresh exclusive** allocations — the scheduler reused medusa11/12/13 for all islands, so this shows repeatability under **low placement diversity**, not broad cluster-wide variance): full ladder `[0,64,1024,16384,262144]`, N=8, prewarm=5, measured=30, HPX phase then Ray phase per island. All 5 per-island manifests passed (27 gates each) and the band aggregate passed: `overall_band_pass=true`, `evidence_grade=matched_band_r5`, `same_axis_comparison=true` (**structural flag only**), `distributional_evidence=true` (**within-arm only**), `percentiles_evidence_ready=true` (**p50/p90 only**), `p99_evidence_ready=false`, `distributional_payload_ladder_ready=false` (blocked by `hpx_serialization_runtime_path_not_observed` and `hpx_poll_gather_baseline`), `no_cross_arm_timing_computed=true`; 12/12 band gates, 5/5 fences False, 5/5 islands `clean`. Connector anomaly witness held clean across all islands (`connector_shutdown_reason=served_signal`, `serve_timeout_expired_any=false`) — the longer measured=30 window did not recur the exp63 fault class; NUMA/NIC provenance captured (eno16, NIC NUMA node 0, root cores on node 0, colocated). The two per-arm tables below are **within-arm observations only** (across-island median of the per-island p50/p90); **do not compare them, do not compute ratios, do not read a winner** — the arms take intentionally different runtime paths and the only cross-arm anchor is the closed digest.

Within-arm p50/p90 (across-island median of per-island percentiles), reported in
**separate** per-arm tables — **do not compare across the two tables**:

HPX (`root_flat_gather_poll` poll-gather baseline):

| S (bytes) | p50 (ms) | p90 (ms) |
|---|---|---|
| 0 | 0.299 | 0.335 |
| 64 | 0.290 | 0.317 |
| 1024 | 0.326 | 0.388 |
| 16384 | 1.453 | 1.516 |
| 262144 | 19.518 | 20.552 |

Ray (coordinator + Ray object transport):

| S (bytes) | p50 (ms) | p90 (ms) |
|---|---|---|
| 0 | 4.225 | 4.821 |
| 64 | 4.191 | 4.580 |
| 1024 | 4.392 | 4.667 |
| 16384 | 6.850 | 7.153 |
| 262144 | 55.378 | 57.370 |

**What this licenses:** each runtime's **own** within-arm p50/p90 payload-size curve and
the structural repeatability of the matched ladder across R=5 islands. Nothing beyond
that — no cross-arm comparison, no ratio/speedup/winner, no p99 evidence, no full
`distributional_payload_ladder`, and no HPX native-composition payload path.

* **exp64 Slice 5 — Phase A→A4 native-readiness diagnosis (complete; HPX-only)** — asked whether HPX-native readiness composition could replace the poll-gather baseline for the payload path. Result: native `when_all`/`dataflow` continuations entered and completed **promptly**, but the **suspended timed waiter resumed only at the dispatch timeout** — classification `waiter_resume_at_timeout` — unchanged by root/background-thread tuning, disabled idle backoff, and TCP parcel-pool sizes 2 (observed default), 4, and 8, while the polling/yield controls stayed prompt throughout. Consequence at the time: the poll baseline was **not retired** on HPX 1.11, the native payload-size ladder was **not started**, and `distributional_payload_ladder` stays blocked. Scope: HPX 1.11, TCP parcelport, Rostam, closed-`int64` S=0 diagnostic; the timeout-bound values are **diagnostic signatures, not latency measurements** — no performance, general-HPX, or Ray-comparison claim. The HPX 1.11 result stands as the historical control; its readiness behavior is superseded on the verified fixed build (Slice 5V below).
* **exp64 Slice 5V — waiter-fix verification (complete; HPX-only)** — the upstream discussion confirmed the Slice 5 signature as an HPX bug, fixed upstream by PR #7367 ("Fixing future::wait_until (and wait_for) to return once future was made ready"). The *identical* discriminator (same runner, sources, modes, shape, gates) was run against two runtime-identity-asserted builds: the pinned control **HPX 1.11.0 (Git `c9b81b401f`)**, which reproduced `waiter_resume_at_timeout` uniformly (job 170136), and **HPX master `20bc3d4bf3068383edcb63be13f22e9ff95842fa`** (PR #7367 merge commit `f5fed9a4be…` proven an ancestor via `git merge-base --is-ancestor`), which classified **`waiter_resumed_on_ready`** in **four runs across two thread configurations** (jobs 170138/170140/170141 at 4 root threads, 170142 at 2), with suspension proven before readiness, continuations completing, and polling/yield controls valid throughout; all 12 structural gates passed. A **formally separate exp65 loopback re-check** corroborates on its own axis: the single full-bound `future::wait_for` returned only at its 15 s bound on HPX 1.11 (3/3) and returned on readiness on the fixed build (3/3) — corroborating evidence, **not** proof of one shared root cause. Consequence: **polling is no longer the required readiness workaround on that exact fixed build** (it remains the historical HPX 1.11 evidence path and a diagnostic control); `hpx_serialization_runtime_path_not_observed` **remains open**, so `distributional_payload_ladder` stays blocked and no payload ladder was started. Curated aggregate: [waiter_fix_verification_aggregate.json](../experiments/64_payload_fanin_size_sweep/waiter_fix_verification_aggregate.json). Scope: the exact tested commit only — no performance, latency, speedup, ratio, winner, or general-HPX claim.

---

## Demand-triggered connect-mode admission (experiment 65)

exp65 extends the island-lifecycle arc (exp49–52) with the one ordering no prior
distributed experiment had proven: **demand-ordered admission**. Every earlier probe
used late connect-mode admission inside an orchestrated assemble-then-measure pattern
(connectors launched at orchestration start); exp65 shows the proven connect-mode
mechanism does not require that pattern — on loopback and across two real nodes.

* [experiments/65_demand_admission](../experiments/65_demand_admission/demand_triggered_admission.md) — demand arm and no-demand control both pass **3/3 in each of two slices**: the root starts **alone**, performs local HPX work before any connector exists, admits one connect-mode connector only after an **external demand event**, discovers it by membership set-difference **without a predetermined connector count**, executes a verified closed-`int64` remote action, observes the connector's graceful leave, continues local work, and finalizes cleanly; the no-demand root finalizes cleanly with zero connectors ever joining. **Loopback slice**: single-node macOS loopback, HPX 1.11, plain Python controller — 3/3 demand and 3/3 no-demand; curated [demand_admission_aggregate.json](../experiments/65_demand_admission/demand_admission_aggregate.json). **Rostam cross-node slice** (Slurm job **170014**): root/controller on **medusa00**, connector created only after the demand event on **medusa01**, TCP parcelport over `10.42.5.x`, no predetermined connector count, set-difference discovery, verified remote action on locality 1, graceful leave, root continued and finalized — 3/3 demand and 3/3 no-demand, **all structural/placement gates passed**; curated [demand_admission_crossnode_aggregate.json](../experiments/65_demand_admission/demand_admission_crossnode_aggregate.json). The root still pre-declares willingness to accept connectors (`--hpx:expect-connecting-localities`), so the safe claim stays narrow: demand-ordered connect-mode admission is demonstrated on loopback and across two real nodes, within that boot-time willingness — **not** HPX inside Ray actors, **not** elasticity during in-flight work, **not** concurrent churn, **not** failure recovery, **not** lazy parcelport TCP connection-establishment evidence, nothing beyond two nodes, and no performance claim (all recorded durations are observational only and never gate inputs). The loopback slice's side observation that a single full-bound `future::wait_for` on the dispatched action returned only at its full bound is scoped to loopback/macOS only; the cross-node slice used sliced waits and does not reproduce that wait construction. A later waiter-fix verification re-check (see the exp64 Slice 5V entry) reproduced that full-bound behavior on a Rostam node under HPX 1.11 (3/3) and observed readiness wakeup on the verified fixed master build (3/3) — **separate corroborating evidence**, not proof of one shared root cause with the exp64 signature.

---

## HPX networking runtime inside one Ray actor worker (experiment 66)

exp66 is the first experiment to run HPX **inside a Ray actor worker process** rather
than in a standalone process. It is the pivotal in-worker hosting test that the exp49–65
standalone connect-mode arc deliberately never attempted: a Ray actor imports an
experiment-only pybind extension and starts a networking HPX locality
(`hpx::start`, `runtime_mode::connect`) on background threads of its **own** process — no
HPX child process. It closes the first in-worker hosting sub-gate of the
[Level-4 design gate](design/rayx_hpx_to_hpx_across_ray_actors_gate.md); it does **not**
close L4 itself (HPX-mediated communication across **two** Ray actors), which needs
exp67.

**Design (interpreter-aware slices):** **A** in-process hosting + full lifecycle
(gating), **B** HPX progress while the actor's Python thread is idle (gating), **C**
actor-thread CPU/GIL saturation (**diagnostic, non-gating**), **D** Python 3.14 /
free-threaded rerun (**deferred**, cleanly skipped — a GIL-build CPython 3.14 exists
locally but has no Ray, and no free-threaded interpreter is present). exp66 passes iff all
Slice A and Slice B gates pass in every rep; Slice C never changes the verdict.

**Claim fences:** one actor only (**not** two actors sharing a runtime); **not** Python
3.14; **not** free-threaded Python; no elasticity or individual-locality failure recovery;
no performance, speedup, ratio, or HPX-vs-Ray winner claim; Slice C is a diagnostic, not an
architectural verdict; timing is never the correctness oracle.

**Durable report:**
[hpx_runtime_inside_ray_actor.md](../experiments/66_hpx_runtime_inside_ray_actor/hpx_runtime_inside_ray_actor.md)
— the standing experiment-local account (process model, gates, same-process proof,
lifecycle, artifacts, hashes).

* [experiments/66_hpx_runtime_inside_ray_actor/](../experiments/66_hpx_runtime_inside_ray_actor/) — **exp66 local slice** (CPython **3.11.15** GIL build, Ray **2.55.1**, HPX **`20bc3d4bf3068383edcb63be13f22e9ff95842fa`**, macOS loopback): **3/3**. A separately supervised work-free root (locality 0) and an external HPX prober run as standalone processes; one Ray actor hosts a connect-mode HPX locality in-process. The HPX `pid` action executed **on the actor locality** returned exactly the Ray actor worker's own PID, and the actor owned **zero** HPX children — in-process hosting proven by value, not a child. Verified remote `probe` action + closed-`int64` oracle on the actor locality; HPX progress while the actor's Python thread was idle; graceful `post(disconnect)+stop` leave (`root_completion_signal`); clean in-process HPX stop; actor destruction and recreation (fresh PID); no orphans. Slice C `progressed_under_actor_saturation` ×3 (HPX executed the action while the actor's Python thread held the GIL — a diagnostic, since the C++ actions never touch the GIL). Curated [hpx_inside_ray_actor_aggregate.json](../experiments/66_hpx_runtime_inside_ray_actor/hpx_inside_ray_actor_aggregate.json); raw `_exp66_runs/` gitignored.
* **exp66 Rostam cross-node slice** (Slurm job **170524**; CPython **3.12.3** GIL build, Ray **2.55.1**, same fixed HPX build, TCP parcelport over `10.42.5.x`): **3/3**. Ray head on **medusa00** (`10.42.5.30`), worker on **medusa01** (`10.42.5.31`); the work-free root and prober ran on medusa00 while the Ray actor was **hard-placed** (`NodeAffinitySchedulingStrategy(soft=False)`, resolved Ray node-id + FQDN-normalized hostname) on **medusa01**. Every rep: the HPX `pid` action executed on the actor locality returned the actor worker PID **across nodes** (e.g. `1342471==1342471`), no HPX child; verified `probe` action + oracle executed on the actor locality (locality 1) over the real TCP parcelport; idle progress passed; graceful leave with `root_completion_signal`; root finalized; actor recreated on medusa01 with a fresh PID; and orphan checks were clean on both nodes. Slice C progressed ×3. Curated [hpx_inside_ray_actor_crossnode_aggregate.json](../experiments/66_hpx_runtime_inside_ray_actor/hpx_inside_ray_actor_crossnode_aggregate.json); raw `_exp66_runs/` gitignored. (A first submission, job 170520, had all reps pass but was invalidated by an orphan-check self-match — the check's own `srun … pgrep -f <pattern>` matched its launcher argv on the controller's node; the checker was fixed to filter its own machinery and the full 3-rep set was rerun as 170524.)

**What this licenses:** *a networking HPX connect-mode locality can run in-process inside a
Ray actor worker, proven by exact PID identity and the absence of HPX child processes,
locally and across two Rostam nodes; Ray owned actor placement and lifecycle while HPX
actions carried the distributed operation and result, with a separately supervised,
work-free root.* Nothing beyond that — one actor only, not two actors sharing a runtime
(exp67); not Python 3.14 or free-threaded; no elasticity or failure recovery; no
performance/GIL verdict; and the HPX serialization/runtime data path is **not directly
observed** (execution is proven by the closed-`int64` value oracle, not by instrumenting
the wire/serialization path). exp66 is the prerequisite for exp67, which delivered the
two-actor shared runtime (below).

---

## Two Ray actors sharing one HPX runtime (experiment 67)

exp67 is the load-bearing step exp66 set up and the exp49–66 arc deliberately never
attempted: **two distinct Ray actor worker processes, each hosting a networking HPX
connect-mode locality in-process, join one shared HPX runtime under a separately
supervised, work-free root and exchange verified HPX actions in both directions.** It
closes the [across-Ray-actors design gate](design/rayx_hpx_to_hpx_across_ray_actors_gate.md).
The load-bearing proof is **bidirectional and actor-to-actor**: A→B proves B's PID,
locality, and hostname plus a closed-`int64` oracle; B→A proves A's. Neither actor holds a
Ray handle to the other, so the operation path is HPX, not Ray — **by construction**, not by
direct wire instrumentation.

**Research question:** can two independently launched Ray actor workers join one shared
distributed HPX runtime as connect-mode localities and execute a verified HPX action from
one actor-locality to the other, in both directions, while a separately supervised
root/AGAS process stays isolated and work-free — locally and across distinct nodes?

**Durable report:**
[two_ray_actors_shared_hpx.md](../experiments/67_two_ray_actors_shared_hpx/two_ray_actors_shared_hpx.md)
— the standing experiment-local account (shared-runtime architecture, bidirectional
mechanism, gates, accepted evidence, artifacts, hashes).

* **Local slice** (CPython **3.11.15** GIL build, Ray **2.55.1**, HPX **`20bc3d4bf3068383edcb63be13f22e9ff95842fa`**, macOS loopback): **3/3**. Two distinct Ray worker PIDs on distinct nonzero HPX localities (1 and 2), both **childless** (no HPX child process); A→B returns B's worker PID, executes on B's locality, and passes the closed oracle, and B→A returns A's worker PID on A's locality; the work-free root is locality 0 (`max_membership=3`, `final_membership=1`, `leave_observed`); graceful `post(disconnect)+stop` leave for both, root finalized clean, both actors destroyed and recreated with fresh PIDs, no orphans. Curated [two_ray_actors_shared_hpx_aggregate.json](../experiments/67_two_ray_actors_shared_hpx/two_ray_actors_shared_hpx_aggregate.json); raw `_exp67_runs/` gitignored.
* **Rostam three-node slice** — smoke job **170743** (pass) and accepted full job **170744** (**3/3**); CPython **3.12.3**, Ray **2.55.1**, same fixed HPX build, TCP parcelport over `10.42.5.x`. **Root/controller on medusa00** (`10.42.5.30`), **actor A on medusa01** (`10.42.5.31`), **actor B on medusa11** (`10.42.5.41`) — **three roles on three distinct nodes** with distinct Ray node-ids; both actors **hard-placed** (`NodeAffinitySchedulingStrategy(soft=False)`, resolved Ray node-id + FQDN-normalized hostname). Every rep: distinct worker PIDs on distinct localities, both childless; A→B (`pid_result==B.pid`, executes on B's locality, hostname medusa11, oracle) and B→A (`pid_result==A.pid`, locality A, hostname medusa01, oracle) — **both directions cross nodes**; root stayed work-free on medusa00 (no application action on locality 0); graceful leave, root finalize, actor recreation on the intended nodes, and orphan-clean sweeps on **all three nodes**. Curated [two_ray_actors_shared_hpx_crossnode_aggregate.json](../experiments/67_two_ray_actors_shared_hpx/two_ray_actors_shared_hpx_crossnode_aggregate.json); raw `_exp67_runs/crossnode_*` gitignored. (Actor B ran on medusa11 rather than medusa02, which was occupied; a distinct third node is equivalent for this mechanism proof and the actual node is recorded.)

**Safe claim:** *two distinct Ray actor workers, each hosting an HPX connect-mode locality
in-process (exact PID identity, no HPX child), joined one shared HPX runtime under a
separately supervised work-free root and executed a verified HPX action from one
actor-locality to the other in both directions (B's PID proven by A→B, A's by B→A) — locally
and across three Rostam nodes, with clean lifecycle and actor recreation.* **Non-claims:**
not Python 3.14; not free-threaded Python; no elasticity, churn, or individual-locality
recovery; **no performance claim**; operation-over-HPX-not-Ray is proven by construction, not
by direct wire instrumentation; not a production API.

---

## Deterministic vocabulary-sharded top-k (experiment 68)

exp68 runs a **synthetic, exactly-checkable, LLM-shaped** distributed workload on top of
exp67's two-actor runtime. It is **not** real inference and carries no model weights,
tokenizer, GPU, or framework.

**Workload:** vocabulary-sharded next-token candidate selection. **Deterministic float32
rule** (identical in the C++ localities and the Python oracle): `h = (uint32(token_id)·2654435761
+ uint32(seed)·40503) mod 2³²`; `grid = (h mod 131) − 65 ∈ [−65,65]`; `logit = float32(grid)/8` —
an exactly-representable 1/8 grid, no rounding. **Shard ownership:** actor A owns `[0, split)`,
actor B owns `[split, V)` — disjoint, union `[0, V)`, every token exactly one owner. **Tie-breaking
contract (one total order everywhere — local top-k, merge, and oracle):** higher logit wins; on
equal logit, the lower global token id wins. Each actor computes its shard's local top-k; a
coordinator fetches the peer's candidates over an HPX action and merges through an HPX
**future/continuation**; the global top-k is checked bit-exactly against an **independent
controller oracle**, in **both** coordinator directions, over a **seven-case matrix** (small
control, k=1, k>1, k smaller than each shard, k crossing both shards, an intentional tie at the
cutoff, one-shard-dominant, both-shards-contributing).

* **Local slice** (CPython **3.11.15** GIL build, Ray **2.55.1**, HPX **`20bc3d4bf3068383edcb63be13f22e9ff95842fa`**): **3/3**. For every case: exact local top-k per shard (token IDs + float32 bits + order) vs an oracle restricted to that shard; **A-coordinated and B-coordinated** merges both equal the full global oracle exactly; HPX action → future → continuation composition evidenced; distinct childless actors on localities 1/2; work-free root; clean lifecycle and actor recreation; no orphans. Curated [vocab_sharded_topk_aggregate.json](../experiments/68_vocab_sharded_topk/vocab_sharded_topk_aggregate.json); raw `_exp68_runs/` gitignored.
* **Rostam three-node slice** — smoke job **170745** (pass) and accepted full job **170746** (**3/3**); CPython **3.12.3**, Ray **2.55.1**, same fixed HPX build, TCP parcelport over `10.42.5.x`; root/controller **medusa00**, actor A **medusa01**, actor B **medusa11**, both hard-placed on distinct nodes. Every rep ran the **full seven-case matrix** in both directions crossing nodes with exact token-ID, ordering, and float32-bit agreement vs the independent oracle; **coordinator symmetry** held (A-coordinated result == B-coordinated result == oracle, ids+bits+order). The cross-node **serialization discriminator**: for every returned candidate — merged results **and** the peer-transferred candidate lists that crossed the parcelport — raw `uint32` bit patterns matched the oracle exactly: **204 peer-transferred candidate bit patterns checked (68 per repetition), zero mismatches**. Root stayed work-free; clean lifecycle, actor recreation on the intended nodes, orphan-clean on all three nodes. Curated [vocab_sharded_topk_crossnode_aggregate.json](../experiments/68_vocab_sharded_topk/vocab_sharded_topk_crossnode_aggregate.json); raw `_exp68_runs/crossnode_*` gitignored. (Actor B ran on medusa11 rather than medusa02, which was occupied; the actual node is recorded.)

**Durable report:**
[vocab_sharded_topk.md](../experiments/68_vocab_sharded_topk/vocab_sharded_topk.md) — the
standing experiment-local account (workload contract, oracle, gates, accepted evidence,
artifacts, hashes).

**Safe claim:** *two Ray actors on distinct nodes executed a deterministic vocabulary-sharded
top-k operation through HPX actions/futures in both coordinator directions, with exact
token-ID, ordering, and float32-bit agreement against an independent oracle — locally and
across three Rostam nodes, including bit-exact preservation of peer-transferred candidates over
the cross-node HPX action path, with clean lifecycle and actor recreation.* **Non-claims:** not
real LLM inference; no model weights, tokenizer, GPU, or framework integration; not Python 3.14
or free-threaded; no elasticity, churn, or failure recovery; **no latency, throughput, ratio,
speedup, or winner claim**; no direct internal serialization instrumentation (only that
bit-exact candidate *values* survived the HPX action path); not a production API.

exp69 (below) is the completed same-axis performance characterization of this exact workload.

---

## Same-axis top-k orchestration performance (experiment 69)

exp69 is the first experiment explicitly permitted to compare Ray-mediated and HPX-mediated
execution-path performance — for the identical exp68 workload, actor topology, placement,
resources, and Python caller boundary, under strict correctness, placement, resource,
sampling, and same-axis gates. Both arms run inside the **same** Ray-hosted, HPX-resident
actor topology (this is not standalone Ray vs standalone HPX); every timed sample is
verified bit-exactly against the independent exp68 oracle strictly after the timing
boundary, and invalid results never contribute timing samples.

**Durable report:**
[same_axis_topk_perf.md](../experiments/69_same_axis_topk_perf/same_axis_topk_perf.md) —
methodology, call paths, per-slice results, causal interpretation, and claim boundaries.

* **Slice 1 — QD1 caller-observed latency** (accepted cross-node job **171408**; root
  medusa00, actor A medusa01, actor B medusa11; local control aggregate also curated):
  six payload cases (P0/P1/P2/P3a/P3b/P3c), direction A at W=50/K=500/R=3 plus a separate
  never-pooled direction-B control — **21,600 timed samples, 0 invalid, 0 timeout**, all
  gates passed. Per-arm latency distributions only in the aggregate; a completed read-only
  review conditionally licensed scoped per-case QD1 discussion (P1 per-arm-only), with the
  repetition-level range as the primary uncertainty. Curated
  [same_axis_topk_qd1_local_aggregate.json](../experiments/69_same_axis_topk_perf/same_axis_topk_qd1_local_aggregate.json)
  and [same_axis_topk_qd1_crossnode_aggregate.json](../experiments/69_same_axis_topk_perf/same_axis_topk_qd1_crossnode_aggregate.json).
* **Slice 2 — bounded-concurrency goodput and latency under load** (smoke job 171560;
  accepted job **171561**, same three-node topology): cases P0/P2/P3b at C∈{2,4},
  `num_cpus=2` / `hpx_threads=2`, N=1000 verified completions per batch, R=3, fixed-count
  closed-loop with a bounded out-of-interval verifier — **36,000 verified completions, 0
  invalid**, no verifier backpressure, all gates passed. A completed read-only review
  licensed **scoped ratios**: P2/C=2 HPX/Ray goodput ≈ **1.28** (rep range 1.27–1.29) and
  Ray/HPX p50 latency-under-load ≈ **1.25** (1.25–1.26); P0/C=2 ≈ 1.69/1.72 with a
  mandatory fixed-overhead caveat; P3b/C=2 direction-unstable (not licensed); all original
  C=4 ratios **not licensed** (resource-sensitive). The accepted aggregate's ratio fences
  remain false; the licensed ratios live in the report as review results. Curated
  [same_axis_topk_throughput_crossnode_aggregate.json](../experiments/69_same_axis_topk_perf/same_axis_topk_throughput_crossnode_aggregate.json).
* **Slice 3 — causal resource decomposition** (smoke 172121; accepted reproduction 172122;
  **final curated accepted job 172125** on medusa01/medusa11/medusa12): P3b-only
  three-band matrix (`C2/cpu2/ht2`, `C4/cpu2/ht2`, `C4/cpu4/ht4`), both arms inside the
  same band — **18,000 exact completions, 0 invalid, 0 timeout**, all island gates passed,
  no orphans. The Slice 2 P3b/C=4 reversal **reproduced** at the oversubscribed control
  band and **disappeared** at the matched band: raising HPX workers to four raised observed
  peer concurrency (2 → 3–4), dropped the composite dispatch-to-continuation interval
  (3.47 → 2.45 ms) and total native coordinate (6.93 → 5.47 ms), and the two per-arm
  goodput distributions converged to the same approximate region. Classification:
  `thread_supply_resource_asymmetry_supported`, `no_implementation_defect_observed`. **No
  cross-arm ratio is computed for Slice 3.** (Job 172122 passed every measurement gate but
  inherited two wrong claim-fence metadata values from the QD1-shaped aggregate builder;
  the fences were corrected and 172125 re-ran the identical matrix — both jobs
  independently reproduce the same result.) Curated
  [same_axis_topk_slice3_crossnode_aggregate.json](../experiments/69_same_axis_topk_perf/same_axis_topk_slice3_crossnode_aggregate.json).

**Safe claim:** *exp69 establishes exact, matched-boundary QD1 latency and
bounded-concurrency performance distributions for the two peer-orchestration paths inside
the same Ray-hosted, HPX-resident topology, with every measured sample verified
bit-exactly; its scoped, gated review ratios cover exactly the licensed cases above; and
its causal slice resolves the one observed reversal as a thread-supply resource asymmetry,
not an implementation defect.* **Non-claims:** not real inference; no standalone
Ray-vs-HPX conclusion; no universal winner or broad speedup; no pooled or Slice 3 ratios;
no claim that `num_cpus` physically caps Ray OS threads; the dispatch-to-continuation
interval is a composite, not pure transport. A comparison-validity guard stands for future
bands: for C ≥ 4, match `num_cpus == hpx_threads == C` or label the band oversubscribed.

**exp70 (status note):** locality supervision — heartbeat/failure detection and
departed-locality dispatch behavior — is **active work, not accepted evidence**. A
self-contained upstream reproducer for the connector-lifetime gap is committed under
[experiments/70_hpx_locality_supervision/upstream_reproducer/](../experiments/70_hpx_locality_supervision/upstream_reproducer/README.md);
its raw run and build directories are untracked.
