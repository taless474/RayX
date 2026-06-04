# Ray ↔ HPX Conceptual Mapping

A durable conceptual guide for comparing Ray's actor/task model with HPX's
distributed many-task runtime. This is not an implementation document.

## Conceptual / API Mapping

| Ray concept | HPX equivalent | Meaning |
|---|---|---|
| Task (`f.remote()`) | Action / `hpx::async` | Remote invocation of a function, returning a future |
| Actor (stateful) | Component with a GID | Addressable, stateful distributed object with remotely invokable methods/actions |
| ObjectRef | `hpx::future` | First-class future used to compose dynamic dependency graphs |
| Named actors / registry (GCS) | AGAS (Active Global Address Space) | Global namespace for resolving distributed objects |
| Cluster node | Locality | Unit of distribution |

## Where the Analogy Is Valid

* **Dynamic asynchronous distributed execution.** Both build task graphs at
  runtime rather than running a static SPMD/BSP program. Work is submitted
  asynchronously and resolved through futures.
* **Actor-like stateful execution.** A Ray actor and an HPX component both
  represent a long-lived, addressable, stateful entity whose methods/actions
  are invoked remotely.
* **Dependency graphs through futures.** Ray `ObjectRef`s and HPX
  `hpx::future`s are first-class values that can be passed around and chained,
  letting both express the same producer/consumer dependency structure.

Because of this, the same serving-control patterns (request ownership,
queueing, per-request futures, cancellation handles) can be expressed in
either runtime. This is the legitimate premise of the project.

## Where the Analogy Breaks

* **Execution unit.** Ray schedules *process-level workers* (one task at a time
  per worker). HPX schedules *lightweight user-level threads* cooperatively
  multiplexed over OS threads inside a locality, making microsecond-scale tasks
  viable.
* **Boundaries and cost.** Every Ray actor/task call crosses a
  Python/process/IPC/serialization boundary, even on the same node. HPX
  intra-locality calls are essentially function calls over shared memory with
  no serialization. (Across localities, HPX serializes parcels, like Ray.)
* **Object store / fault tolerance / elasticity.** Ray has a first-class
  distributed object store (zero-copy shared-memory reads, spilling),
  lineage-based reconstruction, actor restart, and elastic add/remove of nodes.
  HPX's equivalents are weaker; it assumes a more fixed locality set.
* **Fine-grained C++ async.** HPX offers tighter integration with native C++
  runtimes and finer-grained asynchronous scheduling than Ray's process-based
  model.
* **Ecosystem orientation.** Ray is Python- and ecosystem-first. HPX is
  C++- and HPC-first.

Net: the win condition for HPX comes from cheap intra-locality execution and
fine-grained scheduling; the cost comes from weaker isolation, object-store,
and fault-tolerance support.

## Three Stories to Keep Separate

These are easy to conflate, so the project keeps them distinct:

1. **Ray actor-pool control shape.** An explicit pool of actor/worker handles,
   client-side round-robin placement, and as-completed / input-order collection
   via `ray.wait` / `ray.get`. This is the Ray idiom that
   `examples/rayx_actor_pool.py` and `docs/reference/rayx_actor_api.md` §8 *map*.
2. **HPX-native task/future/dataflow style.** The idiomatic HPX path: `hpx::async`
   to launch lightweight tasks (the scheduler multiplexes them over a fixed
   worker set), `future::then` continuations, `hpx::when_all` / `when_any` joins,
   and `hpx::dataflow` firing work as input futures resolve — a dependency-driven
   graph, **not** a queue the user drains — with placement/isolation via executors
   and resource partitioning. RayX does **not** expose this as a serving pattern;
   it appears only conceptually here and as the `hpx::async` dispatch-floor number
   in `experiments/15_hpx_native_lane_feasibility/`.
3. **RayX synthetic serialized-lane harness.** The `ServiceLane` / `HpxLane` /
   `rayx` lanes: a single-consumer, one-request-at-a-time, FIFO **actor-like
   anchor** with a stable `actor_id`. It is chosen to make Ray actor-pool control
   paths measurable against an HPX backend — it is **not** a recommendation that
   HPX code be written as a hand-managed pool of FIFO lanes. (`HpxLane` is the one
   place a lane primitive is swapped toward HPX-native cooperative suspension
   while keeping the FIFO contract; it is still story 3, not story 2.)

RayX implements the serialized-lane harness to compare against Ray actor-style
control paths; it is intentionally **not** a showcase of idiomatic HPX
task/dataflow programming.

For how the `HpxLane` backend's evidence is built across experiments (and why the
native single-lane probe, the task/dataflow probe, and the rayx backend stay
distinct), see the reading guide `docs/reference/hpxlane_backend_arc.md`.

## Project Direction

* **Primary path — C: standalone HPX serving-control runtime.** Build an
  HPX-native serving-control layer and compare it against a Ray actor-style
  serving-control baseline. The model backend stays opaque. This is the main
  direction of the project.
* **Future optional path — A: HPX inside a Ray actor.** Running an HPX runtime
  inside a C++ Ray actor to accelerate that actor's own compute is a feasible
  integration path, but it is not the primary path now.
* **Out of scope — B: replacing Ray Core internals.** Replacing Ray's
  scheduler, transport, or object store with HPX is explicitly out of scope.

## Benchmark Implication

* A naive local HPX-vs-Ray comparison may mostly measure
  Python/process/IPC/serialization overhead rather than scheduling or
  control-plane design.
* Therefore a **null-overhead microbenchmark** (no-op task / empty actor call)
  is required to establish the dispatch-cost floor before any workload-level
  numbers are interpretable.
* Every benchmark report must state **which boundary is being measured**
  (intra-locality vs cross-locality vs cross-process/Python).
* Do not claim "HPX is faster than Ray" without specifying the **workload** and
  the **boundary**. The honest form of a result is "HPX is faster on
  *<workload>* at the *<boundary>*, at the cost of *<tradeoff>*."
