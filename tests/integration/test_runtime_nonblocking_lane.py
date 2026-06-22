"""Structural safety tests for the EXPERIMENTAL non-blocking operation-lane mode
(``Runtime(experimental_nonblocking_op_lanes=True)``).

Requires the built ``_rayx`` extension + HPX; the whole module skips cleanly via
``importorskip`` if the extension is not built. These are the prototype-safety gates
the design slice must pass BEFORE any exp37 experiment:

  1. long in-flight ``park_ms`` shutdown: shutdown returns (no hang) and every future
     reaches a terminal status with no broken promise;
  2. mixed ``park_ms`` + ``busy_sum`` on ONE non-blocking lane: all futures complete
     with correct values even though completion order may differ from submission;
  3. ``max_inflight_per_lane`` is accepted and the lane stays correct under it;
  4. queued cancel and running cancel still work on a non-blocking lane;
  5. actor lanes stay serial and state-correct even when op lanes are non-blocking.

These assert STRUCTURE/values, never wall-clock timings (the parked/CPU op timings
are machine-specific). ``park_ms`` is synthetic cooperative parked work, not real I/O.
"""
import pytest

rayx_runtime = pytest.importorskip("rayx.runtime")

from rayx.runtime import (  # noqa: E402
    OperationCancelledError,
    Runtime,
)

# A held park: 60 s unless cancelled, so an in-flight park cannot retire on its own
# inside the test window -- shutdown/cancel is what must settle it.
_PARK_BIG_MS = 60_000


def _closed_form(n):
    """busy_sum(n) closed form: (n*(n-1)/2) mod 2^31 (mirrors the native body)."""
    return (n * (n - 1) // 2) % (2 ** 31)


def _any_active(rt):
    return any(s["active"] for s in rt.lane_stats())


# --- 0. opt-in is additive: default Runtime is unchanged ----------------------


def test_default_runtime_is_serial_and_unaffected():
    # The experimental kwarg defaults off; a plain Runtime behaves exactly as before.
    with Runtime(num_lanes=1) as rt:
        res = rt.submit_operation("square", 5).result()
        assert res.value == 25
        assert res.row["actor_id"].startswith("rt-hpx-")


def test_disabled_with_explicit_inflight_is_rejected():
    # max_inflight_per_lane only means something with the mode on (boundary check).
    with pytest.raises(ValueError):
        Runtime(num_lanes=1, max_inflight_per_lane=4)


# --- 1. long in-flight park_ms shutdown: no hang, no broken promise ------------


def test_nonblocking_shutdown_with_inflight_parks_settles_all(wait_until):
    rt = Runtime(num_lanes=1, hpx_threads=2,
                 experimental_nonblocking_op_lanes=True,
                 max_inflight_per_lane=8)
    try:
        # Several held parks dispatched concurrently on ONE non-blocking lane (the
        # serial lane could only hold one in service; here multiple go in flight).
        futs = [rt.submit_operation("park_ms", _PARK_BIG_MS) for _ in range(5)]
        wait_until(lambda: _any_active(rt), where="parks in flight")
    finally:
        rt.shutdown()  # must return promptly (bounded by one chunk), not park 60 s
    # Every future settled terminally; a held park resolves cancelled at shutdown.
    for f in futs:
        res = f.result()
        assert res.row["status"] in ("completed", "cancelled")
        if res.row["status"] == "cancelled":
            with pytest.raises(OperationCancelledError):
                _ = res.value


# --- 2. mixed park + compute on one lane: correct values, order may differ ----


def test_nonblocking_mixed_park_and_busy_sum_all_correct():
    n = 2_000_000
    with Runtime(num_lanes=1, hpx_threads=2,
                 experimental_nonblocking_op_lanes=True,
                 max_inflight_per_lane=8) as rt:
        futs = []
        for _ in range(6):
            futs.append(("Wp", rt.submit_operation("park_ms", 1)))
            futs.append(("C", rt.submit_operation("busy_sum", n)))
        # Retire via as_completed: completion order may differ from submission order
        # on a non-blocking lane, but every future must resolve with the right value.
        results = {}
        for fut in rt.as_completed([f for _, f in futs]):
            results[fut] = fut.result()
        for label, f in futs:
            res = results[f]
            assert res.row["status"] == "completed"
            if label == "C":
                assert res.value == _closed_form(n)
            else:
                assert res.value == 1  # park_ms echoes ms


# --- 3. max_inflight bound is accepted and the lane stays correct -------------


def test_nonblocking_respects_inflight_bound_of_one():
    # max_inflight=1 makes a non-blocking lane behave serially for Async ops; it must
    # still complete everything correctly (the bound is honoured, not bypassed). We
    # cannot count concurrent in-flight ops from Python without a counter (out of
    # scope), so this asserts correctness under the tightest bound.
    n = 1_000_000
    with Runtime(num_lanes=1, hpx_threads=2,
                 experimental_nonblocking_op_lanes=True,
                 max_inflight_per_lane=1) as rt:
        futs = [rt.submit_operation("busy_sum", n) for _ in range(8)]
        for res in rt.get(futs):
            assert res.row["status"] == "completed"
            assert res.value == _closed_form(n)


# --- 4. cancellation still works on a non-blocking lane -----------------------


def test_nonblocking_running_cancel_at_checkpoint(wait_until):
    with Runtime(num_lanes=1, hpx_threads=2,
                 experimental_nonblocking_op_lanes=True,
                 max_inflight_per_lane=4) as rt:
        f = rt.submit_operation("busy_sum", 2_000_000_000)  # long, checkpointed
        wait_until(lambda: _any_active(rt), where="busy_sum in flight")
        assert f.cancel() is True
        res = f.result()
        assert res.row["status"] == "cancelled"
        with pytest.raises(OperationCancelledError):
            _ = res.value


def test_nonblocking_queued_cancel(wait_until):
    # Fill the in-flight slots with held parks, then a further submit sits QUEUED
    # (not yet dispatched) and a queued cancel settles it immediately.
    with Runtime(num_lanes=1, hpx_threads=2,
                 experimental_nonblocking_op_lanes=True,
                 max_inflight_per_lane=1) as rt:
        holder = rt.submit_operation("park_ms", _PARK_BIG_MS)
        wait_until(lambda: _any_active(rt), where="holder in flight")
        queued = rt.submit_operation("park_ms", 1)  # behind the held park (slot full)
        assert queued.cancel() is True
        qres = queued.result()
        assert qres.row["status"] == "cancelled"
        assert holder.cancel() is True  # release the held park so shutdown is prompt
        _ = holder.result()


# --- 5. actor lanes stay serial + state-correct under the op-lane flag --------


def test_actor_lane_serial_and_correct_with_nonblocking_op_lanes():
    # The flag is OPERATION-lane-only; actor lanes must remain serial-blocking so
    # stateful method ordering (counter add/get) stays correct.
    with Runtime(num_lanes=1, hpx_threads=2,
                 experimental_nonblocking_op_lanes=True,
                 max_inflight_per_lane=8) as rt:
        actor = rt.create_actor("counter", 0)
        try:
            assert actor._actor_id.startswith("rt-act-")
            for i in range(1, 11):
                r = actor.call("add", 1).result()
                assert r.value == i  # serial state: strictly increasing, no races
            assert actor.call("get").result().value == 10
        finally:
            rt.release_actor(actor)
