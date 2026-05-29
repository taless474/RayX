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
