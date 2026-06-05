#!/usr/bin/env python3
"""A readable tour of the experimental ``rayx.runtime`` prototype.

This is an API / semantics example, **NOT a benchmark**. It measures nothing,
makes no performance claim, and makes no "HPX beats Ray" claim.

WHY a runtime at all (vs the shipped rayx harness)? The harness (``Engine`` /
``SyntheticActor``) is a synthetic *serving-control* comparison: every request is
fixed synthetic work (a sleep/spin of a chosen duration) and ``result()`` returns a
measurement **row**. It deliberately runs no real computation and produces no user
value. ``rayx.runtime`` is the Phase 1 step past that: it runs **registered native
C++ operations** that produce an actual **value**, kept strictly separate from the
measurement row. So this example can show things the harness could not: a real
operation value, a real failure outcome from native code, and value/row separation
-- while keeping the same FIFO lanes, cancellation, admission, and observability.

WHAT IS THE HPX RELATIONSHIP?

* The Python API is ``rayx.runtime.Runtime`` (import: ``from rayx.runtime import
  Runtime``).
* Underneath, each lane is an HPX-native FIFO ``RuntimeLane``; operations are
  fixed, registered **native C++ operations** (``square`` / ``add`` / ``boom`` /
  ``busy_sum``).
* HPX handles scheduling, futures, and cooperative execution under the hood. The
  Python layer never sees an HPX type -- only ``Runtime`` / ``RuntimeFuture`` /
  ``OperationResult`` and plain Python values/dicts.

WHAT THIS IS NOT: not a Ray replacement, not an object store / ``ObjectRef``, not
arbitrary remote Python execution (you cannot register a Python callable -- only the
fixed native registry runs), and not HPX actions / components / distributed
localities. It is a single-process, single-active runtime, mutually exclusive with
the harness ``Engine``.

Requires the rayx extension to be BUILT (python/src/rayx/_rayx*.so); like the other
examples it adds python/src to sys.path automatically.

Run (from the repo root, after building _rayx):
    python examples/rayx_runtime_basic.py
"""
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

from rayx.runtime import (  # noqa: E402
    Runtime,
    OperationResult,
    OperationFailedError,
    OperationCancelledError,
)


def _wait_until(pred, timeout_s=5.0, where="condition"):
    """Poll a lane_stats predicate instead of sleeping a guessed duration.

    Used so the cancellation demo is deterministic: wait until the long busy_sum is
    actually ``active`` (running) on its lane before cancelling it, rather than
    racing an arbitrary ``time.sleep``.
    """
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        if pred():
            return
        time.sleep(0.001)
    raise TimeoutError(f"timed out waiting for {where}")


def _row_summary(row):
    """A compact, readable view of the 9-field runtime measurement row."""
    return (f"status={row['status']!r} total_ms={row['total_ms']:.3f} "
            f"service_ms={row['service_ms_observed']:.3f} "
            f"lane={row['actor_id']} error={row['error']!r}")


def main():
    # One active Runtime per process; HPX-native lanes underneath. num_lanes=2 here
    # so the collection / lane_stats sections have more than one lane to show.
    with Runtime(num_lanes=2, hpx_threads=2) as rt:

        # (1) VALUE + ROW SEPARATION ----------------------------------------
        # submit_operation returns a RuntimeFuture; .result() retires it once into
        # an OperationResult. The user VALUE and the measurement ROW are two
        # separate things on that object: res.value is the operation result;
        # res.row is the timing/identity dict and never contains the value.
        print("-- (1) value + row separation --")
        res = rt.submit_operation("square", 7).result()
        assert isinstance(res, OperationResult)
        print(f"  square(7).value = {res.value}")
        print(f"  square(7).row   = {{{_row_summary(res.row)}}}")
        print(f"  'value' in row? {'value' in res.row}  "
              "(the value is NOT stored inside the row)")

        # (2) MULTIPLE NATIVE OPERATIONS ------------------------------------
        # The fixed native registry: square / add / boom / busy_sum. These are
        # compiled C++ operations selected by name -- NOT arbitrary Python.
        print("\n-- (2) the fixed native operation registry --")
        print(f"  square(6)        -> {rt.submit_operation('square', 6).result().value}")
        print(f"  add(3, 4)        -> {rt.submit_operation('add', 3, 4).result().value}")
        bs = rt.submit_operation("busy_sum", 50_000).result()
        print(f"  busy_sum(50000)  -> {bs.value}  (real native iterative work)")

        # (3) FAILURE BEHAVIOR ----------------------------------------------
        # 'boom' is a native operation that throws. The failure is mapped to a
        # result, not an exception out of result(): the row is still readable
        # (status='failed'), and only .value raises OperationFailedError.
        print("\n-- (3) failure behavior (boom) --")
        boom = rt.submit_operation("boom").result()
        print(f"  boom().row = {{{_row_summary(boom.row)}}}")
        try:
            _ = boom.value
        except OperationFailedError as e:
            print(f"  boom().value raised OperationFailedError: {e}")

        # (4) CANCELLATION BEHAVIOR -----------------------------------------
        # Submit a long busy_sum, wait (via lane_stats polling, not a guessed
        # sleep) until it is actually running, then cancel it. The result row
        # status becomes 'cancelled' and .value raises OperationCancelledError.
        print("\n-- (4) cooperative cancellation --")
        fut = rt.submit_operation("busy_sum", 2_000_000_000)  # long, checkpointed
        # Round-robin may place it on ANY lane, so poll until SOME lane is active
        # (do not assume lane index 0). This is the lane_stats-driven wait the
        # cancellation tests use instead of a guessed sleep.
        _wait_until(lambda: any(s["active"] for s in rt.lane_stats()), 5.0,
                    "busy_sum active")
        settled = fut.cancel()  # True: this call settled a running cancel
        cancelled = fut.result()
        print(f"  cancel() settled a stop? {settled}")
        print(f"  cancelled.row = {{{_row_summary(cancelled.row)}}}")
        try:
            _ = cancelled.value
        except OperationCancelledError as e:
            print(f"  cancelled.value raised OperationCancelledError: {e}")

        # (5) COLLECTION BEHAVIOR -------------------------------------------
        # get([...]) retires a batch and returns OperationResults in INPUT order
        # (no fail-fast). as_completed([...]) yields the FUTURE handles as they
        # become ready -- the caller still calls .result() on each.
        print("\n-- (5) collection APIs --")
        batch = [rt.submit_operation("square", n) for n in (2, 3, 4, 5)]
        ordered = rt.get(batch)  # input order preserved
        print(f"  get([square(2..5)]) values (input order) = "
              f"{[r.value for r in ordered]}")

        more = [rt.submit_operation("add", n, 10) for n in (1, 2, 3)]
        completed_vals = []
        for f in rt.as_completed(more):   # yields RuntimeFuture handles, not results
            completed_vals.append(f.result().value)
        print(f"  as_completed([add(n,10)]) yielded {len(completed_vals)} futures; "
              f"values = {sorted(completed_vals)}")

        # (6) LANE OBSERVABILITY --------------------------------------------
        # lane_stats() is a debugging snapshot: one dict per lane with the lane's
        # rt-hpx- actor_id, queued-but-not-started depth, and whether it is in a
        # service slot. Runtime lane ids start with 'rt-hpx-' (distinct from the
        # harness 'act-hpx-' / 'act-hpxl-' lane prefixes).
        print("\n-- (6) lane observability (lane_stats) --")
        for st in rt.lane_stats():
            assert st["actor_id"].startswith("rt-hpx-"), st["actor_id"]
            print(f"  lane {st['actor_id']} queue_depth={st['queue_depth']} "
                  f"active={st['active']}")

    # (7) RECAP: the HPX relationship -------------------------------------
    print("\n-- (7) recap --")
    print("  Python API: rayx.runtime.Runtime; underneath, HPX-native FIFO "
          "RuntimeLanes run")
    print("  registered native C++ operations. HPX handles scheduling / futures / "
          "cooperative")
    print("  execution; Python sees no HPX types. Not Ray, not an object store, "
          "not arbitrary")
    print("  remote Python, not HPX actions/distributed. No timing was measured "
          "or compared here.")


if __name__ == "__main__":
    main()
