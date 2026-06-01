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
        pending = [engine.submit(service_ms=2) for _ in range(4)]
        while pending:
            ready, pending = engine.wait(pending, num_returns=1)
            for fut in ready:
                # (5) Retire each Future exactly once. result() consumes it; a
                # second result()/ready() on the same Future would raise.
                row = fut.result()
                print(f"  wait  -> lane={row['actor_id']} "
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

    # (7) The context exit above called Engine.shutdown(), a graceful drain:
    # it blocks until all queued/in-flight submitted work completes and every
    # Future is fulfilled, then stops the HPX runtime. Work is never cancelled
    # or dropped; Futures submitted before shutdown stay valid afterward.
    print("\nengine shut down (drained); done")


if __name__ == "__main__":
    main()
