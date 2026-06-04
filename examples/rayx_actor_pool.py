#!/usr/bin/env python3
"""Ray actor-pool pattern, expressed in rayx (teaching example, NOT Ray).

A very common Ray idiom is a pool of actor workers with client-side round-robin
placement and as-completed collection::

    workers = [Worker.remote() for _ in range(N)]            # N actor processes
    refs = [workers[i % N].serve.remote(x)                   # round-robin submit
            for i, x in enumerate(inputs)]
    while refs:
        ready, refs = ray.wait(refs, num_returns=1)          # next finisher
        result = ray.get(ready[0])                           # user return value

rayx mirrors the **control shape** of that pattern with the existing
``SyntheticActor`` façade -- an actor handle whose ``serve.remote(...)`` returns a
Future, with ``wait`` / ``get`` to collect. It is **not** Ray, and this example
makes only claims the runtime actually supports.

NOT HPX best-practice guidance. This maps a common Ray actor-pool control
*pattern*; it is not a recommendation for how to write HPX code. rayx's serialized
round-robin lane is an actor-like ANCHOR chosen to make the Ray comparison
apples-to-apples -- the HPX-native idiom is fine-grained futures / tasks
(``hpx::async``), continuations (``.then``), ``hpx::when_all`` / ``dataflow``
composition, and executors / resource partitioning, NOT a hand-managed pool of
FIFO lanes. See ``docs/ray_hpx_mapping.md`` ("Three stories to keep separate").

What is the SAME (the shape worth recognizing):
  * a handle whose call returns a future: ``actor.serve.remote(...) -> Future``;
  * round-robin distribution across ``N`` workers (here ``num_lanes=N``);
  * as-completed collection via ``wait(..., num_returns=1)`` (≈ ``ray.wait``);
  * input-order collection via ``get(list_of_futures)`` (≈ ``ray.get(refs)``);
  * a per-request id you choose (``label``) and the worker that served it
    (``actor_id``), both echoed on the retired row.

What is NOT Ray (do not read more into it than this):
  * ``num_lanes=N`` is ``N`` internal HPX **service lanes inside one process /
    one Engine**, NOT ``N`` independent actor processes (only one active
    ``SyntheticActor`` per process);
  * there is ONE fixed **native C++ synthetic** operation -- no ``@ray.remote``,
    no arbitrary remote Python, no user-defined actor methods;
  * lane choice is internal round-robin -- no client-selected placement, no
    resources, no distributed scheduler, no Ray Serve;
  * ``get`` returns a rayx **measurement row** (synthetic timing: ``actor_id`` /
    ``status`` / ``service_ms_observed`` / ``label`` ...), NOT a user function's
    return value -- there is no object store and ``service_ms`` is parked
    synthetic service time, not real inference;
  * rayx ``wait(num_returns=k)`` returns the FULL ready partition (``>= k``),
    not exactly ``k`` like ``ray.wait`` -- so the loop retires every future in
    ``ready`` (see below).

Conceptual mapping in prose: ``docs/reference/rayx_actor_api.md`` §8 ("Mapping
Ray actor-pool code to RayX"). API tour: ``examples/rayx_basic.py``.

Run (from the repo root, after building _rayx):
    python examples/rayx_actor_pool.py
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

from rayx import QueueFullError, SyntheticActor  # noqa: E402

NUM_WORKERS = 4  # -> SyntheticActor(num_lanes=4): round-robin over 4 lanes


def as_completed_pool():
    """The actor-pool + as-completed shape (the headline pattern)."""
    print("-- actor pool, as-completed via wait(num_returns=1) --")
    # A handful of synthetic inputs. In Ray these would be arguments to a remote
    # method; here `service_ms` is the synthetic service time and `label` is the
    # user-chosen request id we get back on the row.
    inputs = [("task-%02d" % i, 2 + (i % 3) * 2) for i in range(10)]  # (label, ms)

    with SyntheticActor(num_lanes=NUM_WORKERS, hpx_threads=NUM_WORKERS) as actor:
        # workers[i % N].serve.remote(x)  ->  actor.serve.remote(...): the actor
        # round-robins each call across its lanes internally (no client modulo).
        pending = [actor.serve.remote(service_ms=ms, label=label)
                   for label, ms in inputs]

        # while refs: ready, refs = ray.wait(refs, num_returns=1); ray.get(...)
        # rayx wait(num_returns=1) BLOCKS in HPX (GIL released) until >= 1 future
        # is ready and returns the FULL ready partition, so we retire EVERY future
        # in `ready` this sweep (not just ready[0]) -- the one real difference from
        # ray.wait's exactly-`num_returns`.
        done = 0
        while pending:
            ready, pending = actor.wait(pending, num_returns=1)
            for row in actor.get(ready):  # consumes each ready Future once
                done += 1
                print(f"  [{done:2d}/{len(inputs)}] label={row['label']} "
                      f"lane={row['actor_id']} status={row['status']} "
                      f"service_ms={row['service_ms_observed']:.3f}")


def input_order_collection():
    """ray.get(refs): retire a whole list of futures in input order."""
    print("\n-- input-order collection via get(list_of_futures) --")
    with SyntheticActor(num_lanes=NUM_WORKERS, hpx_threads=NUM_WORKERS) as actor:
        futs = [actor.serve.remote(service_ms=3, label=f"in-{i:02d}")
                for i in range(8)]
        # get(list) returns rows in the SAME order as `futs` (like ray.get(refs)),
        # regardless of completion order. Each row reports the lane that served it.
        rows = actor.get(futs)
        order = "  ".join(f"{r['label']}@{r['actor_id'][-4:]}" for r in rows)
        print(f"  rows in input order: {order}")


def lane_backlog_snapshot():
    """Optional: a tiny lane_stats() snapshot (observability only)."""
    print("\n-- lane_stats() snapshot (observability only, can race) --")
    with SyntheticActor(num_lanes=NUM_WORKERS, hpx_threads=NUM_WORKERS) as actor:
        # Submit more than the lanes can instantly start, then peek at the backlog.
        # queue_depth = queued-but-not-started; active = lane is mid-service.
        # This is NOT scheduler state / placement control / JSONL schema.
        backlog = [actor.serve.remote(service_ms=8, label=f"bk-{i}")
                   for i in range(NUM_WORKERS * 3)]
        for st in actor.lane_stats():
            print(f"  lane {st['actor_id'][-4:]} "
                  f"queue_depth={st['queue_depth']} active={st['active']}")
        actor.get(backlog)  # drain before the context exits


def capped_pool():
    """Optional: a bounded actor pool that sheds overflow via QueueFullError."""
    print("\n-- capped pool: max_queue_depth_per_lane sheds overflow --")
    # max_queue_depth_per_lane caps each lane's queued-but-not-started depth; a
    # submit to a full lane raises QueueFullError immediately (local per-lane
    # admission by rejection -- NOT Ray Serve backpressure, NOT blocking, NOT a
    # global cap). A rejected request gets no Future and no row.
    with SyntheticActor(num_lanes=2, hpx_threads=2,
                        max_queue_depth_per_lane=3) as actor:
        admitted, rejected = [], 0
        for i in range(20):  # offer far more than 2 lanes * (cap 3 + 1 active)
            try:
                admitted.append(
                    actor.serve.remote(service_ms=10, label=f"cap-{i:02d}"))
            except QueueFullError:
                rejected += 1  # caller-visible shed; no Future to collect
        rows = actor.get(admitted)  # only admitted work has rows
        print(f"  offered=20 admitted={len(rows)} rejected={rejected} "
              f"(rejected requests produced no Future and no row)")


def main():
    as_completed_pool()
    input_order_collection()
    lane_backlog_snapshot()
    capped_pool()
    print("\nall actors shut down (graceful drain); done")


if __name__ == "__main__":
    main()
