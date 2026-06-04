#!/usr/bin/env python3
"""Minimal, runnable tour of the rayx Python API.

This is a developer-facing example, NOT a benchmark and NOT a Ray replacement.
It submits a few native synthetic requests to the HPX-backed service lanes and
shows the two retire styles -- ``Engine.wait`` and ``Engine.as_completed`` --
plus the once-only ``Future.result`` and the graceful-drain shutdown.

It runs the same native C++ synthetic work (blocking-sleep service lane) the
benchmark driver uses; it does not run arbitrary Python functions remotely and
has no object store / scheduler / fault tolerance.

Requires the rayx extension to be BUILT (python/src/rayx/_rayx*.so); like the
smoke, it adds python/src to sys.path automatically. See
docs/reference/rayx_actor_api.md and docs/hpx_build_notes.md.

Run (from the repo root, after building _rayx):
    python examples/rayx_basic.py
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

from rayx import Engine  # noqa: E402


def main():
    # (1) Engine as a context manager: owns one HPX runtime with `num_lanes`
    # serialized service lanes. The context exit shuts it down.
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        print(f"engine with {engine.num_lanes()} lanes")

        # (2) + (3) Submit a few synthetic requests, then retire them with
        # Engine.wait. wait() blocks inside C++/HPX with the GIL released (it is
        # NOT a Python busy-poll) until >= num_returns are ready, returning a
        # (ready, not_ready) partition of the SAME Future objects.
        print("\n-- Engine.wait (windowed as-completed) --")
        # Optional client-side label: pure metadata echoed back in the row as
        # row["label"] (it never crosses C++ and is not a payload), handy for
        # mapping a retired row back to a user-level request id.
        pending = [engine.submit(service_ms=2, label=f"req-{i}") for i in range(4)]
        while pending:
            ready, pending = engine.wait(pending, num_returns=1)
            # (5) Retire the ready Futures with engine.get (list -> rows in
            # input order). get consumes each Future once, like result(); it
            # returns RayX measurement rows, NOT a Ray object-store value.
            for row in engine.get(ready):
                print(f"  wait  -> label={row['label']} lane={row['actor_id']} "
                      f"status={row['status']} "
                      f"service_ms={row['service_ms_observed']:.3f}")

        # (4) + (6) Engine.as_completed is an ergonomic generator over wait. It
        # YIELDS FUTURES (not result rows) as they become ready; the caller
        # calls .result() on each. It is a convenience wrapper, not the
        # benchmark batch_wait retire path.
        print("\n-- Engine.as_completed (yields Futures, caller retires) --")
        inflight = [engine.submit(service_ms=2) for _ in range(4)]
        for fut in engine.as_completed(inflight):
            row = fut.result()  # (5) once-only retire of the yielded Future
            print(f"  ac    -> lane={row['actor_id']} "
                  f"status={row['status']} "
                  f"service_ms={row['service_ms_observed']:.3f}")

        # Engine.wait(..., timeout=0) is a NON-BLOCKING poll: it returns
        # (ready_now, pending_now) right away and does NOT consume the futures
        # (you still call result()/get() once to retire a ready one). timeout is
        # in seconds -- None blocks, 0 polls; a finite >0 raises
        # NotImplementedError (HPX 1.11 has no non-consuming timed multi-wait).
        print("\n-- Engine.wait(timeout=0) (non-blocking readiness poll) --")
        polled = [engine.submit(service_ms=5) for _ in range(4)]
        ready_now, pending_now = engine.wait(polled, timeout=0)  # no block
        print(f"  poll  -> {len(ready_now)} ready, {len(pending_now)} pending "
              "(nothing consumed)")
        # The poll left every future valid; block to drain the same objects.
        for row in engine.get(engine.wait(polled, num_returns=len(polled))[0]):
            print(f"  drain -> lane={row['actor_id']} status={row['status']} "
                  f"service_ms={row['service_ms_observed']:.3f}")

        # (8) work_mode picks the synthetic service-time shape. The default
        # "sleep" parks the lane (blocking sleep); "spin" keeps it CPU-bound
        # (busy on-core, no sleep) for the requested duration. Both are SYNTHETIC
        # service-time shapes, not payload/function execution -- the row shape is
        # identical (work_mode is an input, not echoed in the row); the effect
        # shows up in service_ms_observed.
        print("\n-- work_mode='spin' (CPU-bound synthetic service) --")
        row = engine.submit(service_ms=2, work_mode="spin", label="spin-a").result()
        print(f"  spin  -> label={row['label']} lane={row['actor_id']} "
              f"status={row['status']} "
              f"service_ms={row['service_ms_observed']:.3f}")

        # (9) Variable service time: submit_batch accepts a LIST of per-request
        # service times (one request per element, input order preserved, count
        # inferred) so a single bulk submission can model a skewed/heterogeneous
        # synthetic workload. Single-request submit() stays scalar-only; the row
        # shape is unchanged (each row reports its own service_ms_observed).
        print("\n-- variable service time (submit_batch with a list) --")
        for i, fut in enumerate(
                engine.submit_batch(service_ms=[1, 5, 2], work_mode="spin")):
            row = fut.result()
            print(f"  varied[{i}] -> lane={row['actor_id']} "
                  f"status={row['status']} "
                  f"service_ms={row['service_ms_observed']:.3f}")

        # (10) Chunked synthetic service: a single request services in `chunks`
        # equal active steps (total active = service_ms, split chunks ways), with
        # an optional PARKED gap between chunks. It is synthetic timing only (not
        # real token streaming) and still returns ONE future / ONE row that
        # echoes chunks/chunk_delay_ms. With chunk_delay_ms>0, service_ms_observed
        # is lane-occupancy time (active service + the parked gaps), not active-
        # only. Chunking is single-request only (batch is unchunked).
        print("\n-- chunked service (one request, multi-step lifecycle) --")
        row = engine.submit(service_ms=6, chunks=3, chunk_delay_ms=2,
                            work_mode="spin", label="chk").result()
        print(f"  chunked -> label={row['label']} chunks={row['chunks']} "
              f"chunk_delay_ms={row['chunk_delay_ms']} status={row['status']} "
              f"service_ms={row['service_ms_observed']:.3f} (active~6 + gaps)")

        # (11) lane_stats() is an observability SNAPSHOT for debugging: one
        # {actor_id, queue_depth, active} row per lane, in stable lane order.
        # queue_depth is queued-but-not-started; active means the lane is in a
        # request's service lifecycle. It can race (snapshot), never consumes a
        # future, and is NOT scheduler state / placement control / JSONL schema.
        print("\n-- lane_stats() (observability snapshot) --")
        backlog = [engine.submit(service_ms=5, work_mode="spin") for _ in range(6)]
        for st in engine.lane_stats():
            print(f"  lane {st['actor_id']} queue_depth={st['queue_depth']} "
                  f"active={st['active']}")
        engine.get(backlog)  # drain the snapshot's backlog before shutdown

    # (7) The context exit above called Engine.shutdown(), a graceful drain:
    # it blocks until all queued/in-flight submitted work completes and every
    # Future is fulfilled, then stops the HPX runtime. Shutdown itself never
    # cancels or drops work; Futures submitted before shutdown stay valid after.
    print("\nengine shut down (drained); done")


if __name__ == "__main__":
    main()
