#!/usr/bin/env python3
"""Local bounded admission / ``QueueFullError`` in rayx (teaching example).

This is a developer-facing API example, NOT a benchmark. It prints no timing and
makes no performance / "HPX beats Ray" / Ray-replacement claim. The synthetic
``service_ms`` is parked synthetic service time, NOT real inference.

What it shows: ``Engine(max_queue_depth_per_lane=N)`` turns on **bounded
admission by rejection**. Each lane admits at most ``N`` queued-but-not-started
requests (the one in-service request is not counted); a ``submit`` to a lane
that is already full raises ``QueueFullError`` immediately, *before* any Future
is created -- so a rejected request has **no Future and no result row**.

What it is NOT (read no more into it than this):
  * NOT Ray Serve backpressure and NOT distributed flow control -- this is a
    single in-process Engine over a fixed set of local HPX-backed lanes;
  * NOT blocking backpressure -- ``submit`` returns immediately by raising; it
    never blocks waiting for a free slot;
  * NOT a global cap -- the bound is per-lane (queued-but-not-started depth on
    the round-robin target lane), not a single pool-wide limit;
  * NOT real inference -- ``service_ms`` is synthetic parked service time.

Requires the rayx extension to be BUILT (python/src/rayx/_rayx*.so); like the
other examples it adds python/src to sys.path automatically. See
docs/reference/rayx_frontend_design.md §12 and docs/reference/rayx_actor_api.md.

Run (from the repo root, after building _rayx):
    python examples/rayx_bounded_admission.py
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

from rayx import Engine, QueueFullError  # noqa: E402

NUM_LANES = 2
CAP = 3            # max_queue_depth_per_lane: queued-but-not-started per lane
ATTEMPTED = 20     # offer far more than the lanes can hold


def main():
    print("-- bounded admission: Engine(max_queue_depth_per_lane) sheds overflow --")
    # With NUM_LANES lanes and a per-lane queue cap of CAP, each lane can hold at
    # most one in-service request plus CAP queued ones before further submits to
    # that (round-robin) lane are rejected. Use a long-ish synthetic service time
    # so the lanes stay busy and the queues fill while we submit.
    with Engine(num_lanes=NUM_LANES, hpx_threads=NUM_LANES,
                max_queue_depth_per_lane=CAP) as engine:
        admitted = []   # only admitted submits get a Future
        rejected = 0    # rejected submits get NO Future and NO row
        for i in range(ATTEMPTED):
            try:
                admitted.append(
                    engine.submit(service_ms=10, label=f"req-{i:02d}"))
            except QueueFullError:
                # Local per-lane admission by rejection: the target lane is full.
                # No Future was created, so there is nothing to collect for this i.
                rejected += 1

        # Optional snapshot (observability only, can race): with the queues full,
        # each lane shows active=True and queue_depth at the cap.
        print("  lane_stats() snapshot (active vs queued; observability only):")
        for st in engine.lane_stats():
            print(f"    lane {st['actor_id'][-4:]} "
                  f"queue_depth={st['queue_depth']} active={st['active']}")

        # Drain ONLY the admitted work -- rejected submits produced no Future, so
        # there is nothing to retire for them. get() returns one row per admitted
        # Future, in input order.
        rows = engine.get(admitted)

        # Contract checks (this example doubles as a quick manual check):
        assert ATTEMPTED == len(admitted) + rejected, (
            f"attempted {ATTEMPTED} != admitted {len(admitted)} + "
            f"rejected {rejected}")
        assert rejected > 0, "expected some rejections (cap should be exceeded)"
        assert len(rows) == len(admitted), (
            f"rows {len(rows)} != admitted Futures {len(admitted)}")
        assert all(r["status"] == "completed" for r in rows), (
            "all admitted rows should complete")

        print(f"  attempted={ATTEMPTED} admitted={len(admitted)} "
              f"rejected={rejected}")
        print(f"  collected rows={len(rows)} (one per admitted Future; rejected "
              "submits produced no Future and no row)")
        print("  all admitted rows status=completed")

    # Bounded admission protects each lane locally by rejecting overflow at
    # submit time; it is not Ray Serve backpressure, not distributed flow
    # control, not blocking, and not a global cap. Engine shut down (drained).
    print("\nengine shut down (drained); done")


if __name__ == "__main__":
    main()
