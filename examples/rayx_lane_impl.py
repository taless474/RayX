#!/usr/bin/env python3
"""Minimal tour of the rayx ``lane_impl`` backend selector.

This is a developer-facing API/observability example, NOT a benchmark and NOT a
performance comparison. It does NOT measure or compare timing, makes no
"HpxLane is faster/slower" claim, and makes no "HPX beats Ray" claim. It is also
NOT a Ray replacement / Ray Serve / object store / real inference -- it submits
native synthetic requests to the HPX-backed service lanes, the same synthetic
work the benchmark driver uses.

What it shows: the public ``lane_impl`` selector on ``Engine`` (and the
``SyntheticActor`` facade), and the ONE thing that is observably different
between the two backends -- the lane's ``actor_id`` prefix:

* ``lane_impl="std"`` (default) -> ``ServiceLane`` anchor, ``act-hpx-`` prefix
* ``lane_impl="hpx"``           -> cooperative ``HpxLane``, ``act-hpxl-`` prefix

Both backends honor the identical RayX lane contract (FIFO, actor_id,
queue_depth/active, bounded admission, queued/running cancellation) and return
the identical result-row schema; only the prefix differs. The script ASSERTS
the expected prefixes so it doubles as a quick manual check.

Requires the rayx extension to be BUILT (python/src/rayx/_rayx*.so); like the
other examples it adds python/src to sys.path automatically. See
docs/reference/hpxlane_backend_arc.md, docs/reference/rayx_actor_api.md, and
docs/hpx_build_notes.md.

Run (from the repo root, after building _rayx):
    python examples/rayx_lane_impl.py
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

from rayx import Engine, SyntheticActor  # noqa: E402


def main():
    # (1) Default / std backend: lane_impl="std" selects the std::thread
    # ServiceLane, the stable comparison anchor (this is also the default, so
    # Engine(num_lanes=2) alone would pick it). Submit a few synthetic requests
    # and retire them with the Engine.wait + get partition; print each row's
    # actor_id. ServiceLane lanes carry the act-hpx- prefix.
    print('-- lane_impl="std" (default ServiceLane anchor) --')
    with Engine(num_lanes=2, hpx_threads=2, lane_impl="std") as engine:
        pending = [engine.submit(service_ms=2, label=f"std-{i}") for i in range(4)]
        while pending:
            ready, pending = engine.wait(pending, num_returns=1)
            for row in engine.get(ready):
                aid = row["actor_id"]
                assert aid.startswith("act-hpx-"), aid
                print(f"  std -> label={row['label']} lane={aid} "
                      f"status={row['status']}")

    # (2) Opt-in hpx backend: lane_impl="hpx" selects the cooperative HpxLane.
    # Same submit; retire with the as_completed generator (yields Futures, caller
    # calls result()). Identical contract and row schema -- the only visible
    # difference is the actor_id prefix, which here is act-hpxl-.
    print('\n-- lane_impl="hpx" (opt-in cooperative HpxLane) --')
    with Engine(num_lanes=2, hpx_threads=2, lane_impl="hpx") as engine:
        inflight = [engine.submit(service_ms=2, label=f"hpx-{i}") for i in range(4)]
        for fut in engine.as_completed(inflight):
            row = fut.result()
            aid = row["actor_id"]
            assert aid.startswith("act-hpxl-"), aid
            print(f"  hpx -> label={row['label']} lane={aid} "
                  f"status={row['status']}")

    # (3) The selector forwards through the SyntheticActor facade too. Use the
    # Ray-flavored serve.remote handle and read the lane actor_ids from the
    # observability snapshot lane_stats() -- they carry the same act-hpxl-
    # prefix as the Engine(lane_impl="hpx") path above.
    print('\n-- SyntheticActor(lane_impl="hpx") (facade forwards the selector) --')
    with SyntheticActor(num_lanes=2, hpx_threads=2, lane_impl="hpx") as actor:
        futs = [actor.serve.remote(service_ms=2) for _ in range(2)]
        for st in actor.lane_stats():
            aid = st["actor_id"]
            assert aid.startswith("act-hpxl-"), aid
            print(f"  lane {aid} queue_depth={st['queue_depth']} "
                  f"active={st['active']}")
        actor.get(futs)  # drain before the context-exit shutdown

    # Same RayX lane contract and the same result-row schema in every block
    # above; the only observable difference between lane_impl="std" and "hpx" is
    # the actor_id prefix (act-hpx- vs act-hpxl-). No timing was measured or
    # compared here.
    print("\nprefixes verified (act-hpx- for std, act-hpxl- for hpx); done")


if __name__ == "__main__":
    main()
