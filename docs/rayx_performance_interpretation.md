# RayX Performance Interpretation

## Executive summary

RayX explores a hybrid architecture in which Ray manages placement, actor lifecycle,
supervision, and the outer Python-facing service boundary, while HPX executes selected
distributed C++ operations inside Ray-hosted worker processes.

The performance evidence does **not** support a universal claim that one runtime is faster
than the other. Instead, it supports a narrower and more useful model:

> RayX's performance opportunity grows with the amount of native distributed composition
> placed behind each Python or Ray boundary.

The largest earlier gaps arose primarily when one HPX-native composed operation replaced
many Python/Ray submissions and intermediate materializations. Exp69 intentionally removed
most of that amplification by giving both arms the same outer Ray boundary and differing
only in one inner peer-operation path. Its smaller, workload-dependent effects are therefore
consistent with the earlier evidence rather than contradictory to it.

The practical implication is that RayX should be evaluated on production-shaped workloads
with multiple native stages, several worker localities, native intermediate state, and a
small number of outer Ray calls. A future experiment should be designed to determine
whether the measured benefit grows with composition depth and fanout. If it does not, that
would weigh directly against the architectural performance hypothesis.

## What goodput means

In exp69, **goodput** is the rate of successfully completed requests under the accepted
measurement contract:

```text
goodput = completed requests / measured elapsed time
```

A request contributes to an accepted batch only when the outer Ray call returns and the
result passes the experiment's correctness and mechanism gates.

The Slice 2 and Slice 3 throughput instrument has the following properties:

- The timer begins immediately before the first measured submission.
- The timer ends after the final measured `ray.get` returns.
- Warm-up requests are executed and verified but excluded from the measured batch.
- Verification is performed through a bounded off-thread queue.
- Verifier drain happens after the timing boundary.
- Accepted batches require all expected completions, zero invalid results, no timeout,
  no verifier backpressure, and the required concurrency and mechanism witnesses.
- Any invalid result invalidates the batch rather than being counted as useful throughput.

For that reason, the exp69 reports use the term **verified-completion goodput** rather than
raw throughput.

## What exp69 measured

Both exp69 arms use the same outer caller boundary:

```text
controller
    -> Ray coordinator actor
        -> distributed peer operation
        -> native merge
    -> Ray return to controller
```

The common work includes:

- controller-to-actor Ray invocation;
- coordinator actor scheduling;
- coordinator-local native top-k;
- final native merge;
- Python materialization of the final result;
- the outer Ray return path.

Only the inner peer operation differs:

### Ray-mediated arm

The coordinator issues a nested Ray actor call to the peer, waits for the result, receives
peer candidates as Python-visible objects, converts them for the native merge, and returns
the merged answer through the outer Ray call.

### HPX-mediated arm

The coordinator dispatches a registered HPX action to the peer locality, receives an
HPX-serialized native reply, and consumes it in an HPX continuation before returning the
final answer through the same outer Ray boundary.

This design makes exp69 a strict matched-boundary study of the **incremental inner
orchestration path**. It is not a comparison of standalone Ray against standalone HPX.

## Why the exp69 outcomes vary by workload

### P0: fixed-overhead control

P0 has almost no meaningful native computation and a very small result. Most of the total
time is the common outer Ray boundary plus the inner control-plane hop.

This case exposes a roughly constant inner-hop difference, but the total result is bounded
by the outer Ray floor that both arms share. It is therefore useful as a control, not as a
general application claim.

### P1: compute domination

P1 scans a very large vocabulary while returning a small top-k result. Native computation
dominates the request.

Because both arms execute the same local and peer-side top-k implementation, changing the
peer orchestration mechanism has little opportunity to affect total latency. The two
per-arm distributions are therefore nearly indistinguishable.

### P2: payload and materialization pressure

P2 uses a large candidate set. The Ray-mediated path materializes a large peer result into
Python-visible tuples and then converts it again for the native merge. The HPX-mediated
path keeps the peer reply in a native serialized representation until the final result is
marshalled.

This is the clearest exp69 case in which the inner path matters. It produced the accepted
P2/C=2 scoped goodput and latency-under-load ratios reported in the exp69 durable report.

### P3a and P3b: moderate compute, small payload

These cases contain enough native work to reduce the relative importance of the inner hop,
while the returned candidate set is still small enough that Python materialization does
not dominate.

At QD1, the HPX-mediated path shows a modest difference consistent with a roughly constant
inner-hop cost. Under bounded concurrency, P3b/C=2 was direction-unstable and therefore
not licensed for a comparative ratio.

### P3b/C=4: thread-supply resource asymmetry

The original C=4 band admitted four concurrent requests while the HPX runtime had only two
worker threads. Those workers had to service:

- coordinator-side top-k work;
- incoming peer actions;
- peer-side top-k work;
- reply handling;
- continuation scheduling;
- merge work;
- other HPX runtime tasks.

The observed reversal was reproduced under this resource band. The native top-k stages
themselves did not materially slow down; the additional time accumulated in the composite
dispatch-to-continuation interval and total native coordinate duration.

When the resource band was changed to match concurrency with CPU and HPX worker supply,
the HPX path's queueing fell, observed peer concurrency increased, and the two per-arm
goodput distributions converged to the same approximate region.

The accepted causal classification is:

```text
thread_supply_resource_asymmetry_supported
no_implementation_defect_observed
```

This result is a resource-supply diagnosis, not a winner claim.

### P3c: increasing payload contribution

P3c retains moderate native computation but increases the candidate volume. Its behavior
falls between the small-payload P3 cases and the large-payload P2 case, consistent with
materialization and serialization becoming more important as `k` grows.

## Why earlier experiments showed much larger gaps

The earlier order-of-magnitude observations did not primarily measure an intrinsic
order-of-magnitude transport advantage.

They mostly measured one of two effects:

1. the per-call cost difference between a Python/Ray-mediated control path and a native
   HPX action path at very small payloads; and
2. the elimination or amortization of **many** Python/Ray submissions and intermediate
   materializations behind one native composed operation.

### Exp61: one boundary versus one boundary

Exp61 provided the cleanest scalar same-axis control. Each arm crossed one caller boundary.

Its observed gap is best interpreted as a per-call software-path and control-plane
difference for that exact scalar operation. The cross-arm ratio remained fenced and was
not licensed as a general performance claim.

### Exp62: many Ray submissions versus one composed native operation

Exp62 compared an N=8 distributed fanout/fanin.

The Ray arm issued a coordinator call plus multiple Ray task submissions and returns. The
HPX arm crossed one Python boundary and composed the native fanout and reduction internally.

The large apparent gap therefore combined:

- per-boundary control-plane cost;
- many more Ray/Python submissions;
- object and result materialization;
- loss of composition behind one boundary.

This is the main source of the earlier order-of-magnitude observation.

### Exp64: the gap shrank as payload grew

Exp64 repeated a similar fanout/fanin structure while increasing response payload size.

The apparent difference was largest near zero payload and narrowed as real data movement
became dominant. This is consistent with fixed boundary and orchestration costs mattering
less as serialization and payload transfer consume more of the total time.

### Reconciliation

The direct conclusion is:

> The earlier order-of-magnitude observations primarily reflected the elimination or
> amortization of many Python and Ray boundary crossings, rather than an intrinsic
> order-of-magnitude HPX transport advantage.

Exp69 deliberately removed most of that amplifier:

- both arms share the outer Ray boundary;
- the workload performs real verified native computation;
- only one inner peer hop differs;
- the result is returned through the same actor path.

Its compressed and case-dependent differences are therefore the expected result of a more
strictly matched experiment.

## The RayX performance value model

The current evidence supports the following model:

```text
potential RayX benefit
    grows with:
        native stages per outer Ray call
        distributed fanout
        dependency depth
        intermediate data kept in C++
        avoided Python/Ray submissions
        avoided Python object materialization

    shrinks with:
        compute that is identical in both arms
        unavoidable outer Ray boundary cost
        very small composition depth
        large common data-transfer cost
        under-provisioned HPX worker supply
```

A useful shorthand is:

> Performance value is proportional to composition-per-boundary.

This is a hypothesis with supporting evidence, not a universal law. It should be tested
directly in future experiments.

## Where HPX is likely to help

HPX is most likely to provide measurable value when a Ray-managed request contains a
native distributed subgraph with several of these properties:

- multiple remote stages;
- fanout followed by native fan-in or reduction;
- dependent continuations;
- stateful native shards;
- intermediate values that do not need to become Python objects;
- repeated operations that can be batched behind one outer call;
- small final results relative to the total intermediate work;
- exact native ownership of candidate, partial, or feature buffers.

Examples include:

- sharded retrieval and top-k merge;
- multi-stage ranking;
- distributed feature aggregation;
- native preprocessing or postprocessing graphs;
- tree reductions;
- distributed token-selection or sampling components;
- native side computations behind a Ray Serve deployment.

## Where HPX is unlikely to help much

The architecture is less likely to provide substantial performance value when:

- the request contains only one small peer operation;
- native computation dominates total time equally in both arms;
- the result must already be materialized into large Python objects;
- Ray's object store is the natural shared representation;
- the payload is reused by many Python consumers;
- the workload is already optimized through another native execution layer;
- runtime resources are not matched to admitted concurrency;
- the integration duplicates mature GPU collectives or model-runtime internals.

In particular, RayX should not initially try to replace tensor-parallel or collective
internals already handled by systems such as vLLM and NCCL without a clearly distinct
hypothesis.

## Prioritized performance roadmap

### 1. Multi-stage native composition behind one Ray call

This is the highest-priority direction.

A single outer Ray invocation should execute multiple dependent distributed HPX stages,
retain intermediate state in C++, and return only the final result.

This is the most plausible path to recovering the boundary-amortization effects seen in
exp62 under the stronger correctness and matched-resource discipline established by
exp69.

### 2. Greater fanout and tree reductions

Move from one peer locality to at least four worker localities.

Use a tree or root-of-partials reduction so the experiment measures:

- fanout;
- native partial aggregation;
- continuation depth;
- final merge;
- scaling with shard count.

### 3. Native buffer ownership

Avoid Python tuples, dictionaries, and lists for intermediate candidate sets.

Track:

- bytes retained natively;
- bytes materialized into Python;
- number of conversions;
- number of copies;
- final result size.

P2 suggests that materialization is a meaningful component of the difference.

### 4. Matched-resource controls

Any future accepted concurrent comparison should enforce or explicitly record:

```text
Ray concurrency
actor CPU allocation
HPX worker threads
number of admitted requests
```

For C >= 4, the experiment should either match worker supply or classify the band as
oversubscribed before interpreting it.

### 5. Dispatch-first overlap

Explore whether the coordinator can dispatch peer work before beginning its own local
work, allowing the two native stages to overlap.

This is expected to shape latency rather than create an order-of-magnitude effect, but it
is a useful optimization after the main composition experiment is valid.

### 6. Explicit HPX pools or executors

Separate application compute, reply handling, and continuation work where appropriate.

The goal is robustness against the contention class observed in exp69 Slice 3, not merely
a larger benchmark ratio.

### 7. Batching and streaming interfaces

Evaluate one-call-many-request and long-lived native queue designs.

These move the outer boundary and therefore represent a different operating regime. They
must be measured and fenced separately from exp69.

### 8. Production-shaped serving and retrieval workloads

After the deterministic instrument is validated, replace the synthetic top-k graph with a
production-shaped workload while preserving an exact or independently checkable oracle.

### 9. Ray object-store comparison

Ray's object store may be the better mechanism for large reusable or broadcast objects,
especially when downstream consumers are Python tasks.

A fair roadmap must identify this regime rather than designing only cases favorable to
native parcel movement.

### 10. GPU and accelerator-local operations

This is a future axis. The current repository contains no GPU performance evidence and
should not make claims about it.

## Proposed next decisive experiment

A strong next experiment would be a **one-call composed distributed graph**.

### Research question

> Does the advantage of the HPX-mediated path grow when one outer Ray request contains
> multiple native distributed stages, greater fanout, and native intermediate state?

### Workload

Use a deterministic three-stage sharded top-k or retrieval pipeline:

1. **Shard-local search:** each worker locality computes an exact local top-k over its
   deterministic shard.
2. **Native reduction:** shard results are merged through a tree or root-of-partials
   reduction.
3. **Dependent refinement:** a second native pass uses a seed, threshold, or digest derived
   from the first-stage result.

Only the final IDs and scores return through Ray.

### Topology

- one Ray controller;
- one coordinator actor;
- at least four Ray actors hosting HPX worker localities;
- one separately supervised, work-free HPX root locality;
- hard node placement;
- fixed software stack;
- matched actor CPU and HPX worker supply.

### HPX-mediated arm

One outer Ray call enters the coordinator. The complete multi-stage graph is executed with
HPX actions, futures, and continuations. Intermediate values remain in native C++.

### Ray-mediated arm

Use the same native shard and merge kernels, but orchestrate the stages through Ray actor
calls and normal Ray result handling. Do not weaken the Ray arm with avoidable
implementation choices.

### Primary metrics

- caller-observed end-to-end latency;
- verified-completion goodput;
- native subgraph duration;
- Python/Ray boundary count;
- Ray submissions per request;
- bytes materialized into Python;
- bytes retained natively;
- bytes transferred;
- queueing and active-concurrency witnesses;
- behavior as shard count and graph depth increase.

### Correctness gates

- exact final token or document IDs;
- exact ordering;
- float32 bit agreement where applicable;
- exact stage-dependency witness;
- correct shard coverage;
- correct locality and process witnesses;
- arm-exclusive mechanism counters;
- full completion count;
- zero invalid results;
- zero timeout;
- bounded verifier;
- clean membership and shutdown;
- zero orphans;
- actor recreation where in scope.

### Claim boundaries

Any ratio must remain scoped to:

- the exact deterministic workload;
- topology;
- resource allocation;
- concurrency;
- software stack;
- caller boundary;
- accepted repetitions;
- exact gate set.

No general runtime winner claim is permitted.

## What would validate the hypothesis

The performance hypothesis would gain support if, under matched resources and exact
correctness:

- the HPX-mediated benefit grows as graph depth increases;
- the benefit grows as shard count increases;
- the benefit grows as intermediate Python materialization is removed;
- one outer Ray call replaces several Ray-mediated distributed stages;
- native queueing remains bounded;
- Ray continues to own placement and lifecycle cleanly;
- the architecture remains stable under sustained load.

The strongest result would be a monotonic or at least consistent relationship between
composition-per-boundary and the measured benefit.

## What would falsify the hypothesis

The performance value proposition would be weakened if:

- the gap does not grow with graph depth or shard count;
- native intermediate retention does not reduce end-to-end cost;
- HPX scheduling or parcel overhead grows as fast as the avoided Ray boundaries;
- matched-resource bands remain at parity across production-shaped graphs;
- the integration creates lifecycle or operational complexity without measurable benefit;
- the Ray-mediated arm performs equally well after using the same native kernels and
  reasonable batching;
- the useful regime is too narrow to justify the additional runtime.

A negative result would still leave the lifecycle and runtime-interoperability work as
valid research, but it would argue against a strong performance-centered product claim.

## Implications for collaborators

The most useful collaboration is not an endorsement of a benchmark ratio. It is help
identifying a production-shaped Ray workload in which a distributed native C++ subgraph
could reasonably remain behind one supervised Ray boundary.

Useful forms of collaboration include:

- review of the actor/runtime integration boundary;
- production workload selection;
- matched implementation of the Ray-mediated reference arm;
- lifecycle and fault-domain design;
- benchmark-methodology review;
- engineering mentorship;
- compute resources;
- internships;
- sponsored student research.

## Claim boundaries

The following claims remain unsupported:

- HPX universally outperforms Ray;
- Ray is generally slower;
- any general speedup or winner;
- exp61, exp62, or exp64 per-arm observations as licensed cross-arm ratios;
- extending exp69 P2/C=2 ratios outside their exact configuration;
- treating matched-resource convergence as statistical equivalence;
- treating clean shutdown as transparent failure recovery;
- treating whole-island restart as fully demonstrated in the exp66–69 topology;
- treating exp69 as real model inference;
- production readiness;
- commercial value established by one benchmark;
- involvement or endorsement by any external company.

## Evidence basis

This interpretation is derived from the following durable reports, source, and curated
aggregates:

- `experiments/61_python_boundary_same_axis_ray_vs_rayx/`
  - `python_boundary_same_axis_ray_vs_rayx.md`
  - `slice4_band_158724_aggregate.json`
  - `slice5_samenode_band_158734_aggregate.json`
- `experiments/62_distributed_fanout_same_axis/`
  - `distributed_fanout_same_axis.md`
  - `exp62_fanout_band_158809_aggregate.json`
  - `exp62_fanout_mrband_158817_aggregate.json`
- `experiments/63_hpx_native_collective_reduction/`
  - the main report and three focused follow-up reports
- `experiments/64_payload_fanin_size_sweep/`
  - `hpx_payload_fanin.md`
  - `waiter_fix_verification_aggregate.json`
- `experiments/65_demand_admission/`
  - `demand_triggered_admission.md`
- `experiments/66_hpx_runtime_inside_ray_actor/`
  - `hpx_runtime_inside_ray_actor.md`
- `experiments/67_two_ray_actors_shared_hpx/`
  - `two_ray_actors_shared_hpx.md`
- `experiments/68_vocab_sharded_topk/`
  - `vocab_sharded_topk.md`
- `experiments/69_same_axis_topk_perf/`
  - `same_axis_topk_perf.md`
  - `run_exp69.py`
  - `actor_ext.cpp`
  - all four curated aggregate JSON files
- `readme.md`
- `docs/evidence_index.md`

Operational identifiers, accepted job IDs, source-level witness details, and exact aggregate hashes remain in the experiment-local durable reports and curated evidence.