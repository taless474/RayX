"""Integration contract tests for rayx.runtime local native actors (require _rayx).

Uses the PUBLIC ``rayx.runtime`` API (``Runtime.create_actor`` / ``ActorHandle.call``),
NOT the raw ``_RuntimeEngine``. The whole module skips cleanly via importorskip if the
extension is not built. No timing assertions anywhere.
"""
import re

import pytest

# Skips the whole module cleanly if _rayx is not built (ImportError -> skip).
rayx_runtime = pytest.importorskip("rayx.runtime")

from rayx.runtime import (  # noqa: E402
    ActorHandle,
    OperationCancelledError,
    OperationResult,
    QueueFullError,
    Runtime,
    RuntimeFuture,
)
from rayx.runtime._validate import ROW_FIELDS  # noqa: E402  canonical row contract

ACTOR_ID_RE = re.compile(r"rt-act-[0-9a-f]{16}")
FORBIDDEN_ROW_FIELDS = ("value", "label", "chunks", "chunk_delay_ms",
                        "chunks_completed")


# --- create / handle shape --------------------------------------------------


def test_create_returns_actor_handle():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        assert isinstance(c, ActorHandle)
        assert c.actor_type == "counter"
        assert ACTOR_ID_RE.fullmatch(c.actor_id)


# --- state + FIFO -----------------------------------------------------------


def test_add_get_reset_state_persists():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        assert c.call("add", 5).result().value == 5
        assert c.call("add", 3).result().value == 8
        assert c.call("get").result().value == 8
        assert c.call("reset", 2).result().value == 2
        assert c.call("get").result().value == 2


def test_two_counters_independent():
    with Runtime() as rt:
        a = rt.create_actor("counter", 0)
        b = rt.create_actor("counter", 100)
        a.call("add", 1).result()
        b.call("add", 5).result()
        assert a.call("get").result().value == 1
        assert b.call("get").result().value == 105
        assert a.actor_id != b.actor_id


def test_fifo_across_mixed_queued_calls():
    """Strict per-actor FIFO across mutating + read methods.

    Deterministic by construction, NOT by timing: the actor's single RuntimeLane
    worker retires one method before popping the next, and a single Python driver
    enqueues in program order. So the cumulative sequence is fixed regardless of HPX
    scheduling -- submit all five WITHOUT retiring, then collect in input order.
    """
    with Runtime(num_lanes=1) as rt:
        c = rt.create_actor("counter", 0)
        f1 = c.call("add", 1)
        f2 = c.call("add", 2)
        f3 = c.call("reset", 10)
        f4 = c.call("add", 5)
        f5 = c.call("get")
        results = rt.get([f1, f2, f3, f4, f5])
        assert [r.value for r in results] == [1, 3, 10, 15, 15]


# --- collection-API compatibility -------------------------------------------


def test_method_future_with_get():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        res = rt.get(c.call("add", 7))
        assert isinstance(res, OperationResult)
        assert res.value == 7


def test_method_futures_with_get_list_input_order():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        futs = [c.call("add", 1), c.call("add", 1), c.call("add", 1)]
        assert [r.value for r in rt.get(futs)] == [1, 2, 3]  # FIFO per actor


def test_method_future_with_wait():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        f = c.call("add", 4)
        ready, not_ready = rt.wait([f], num_returns=1)
        assert f in ready and not not_ready
        assert ready[0].result().value == 4


def test_method_futures_with_as_completed():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        futs = [c.call("add", 1) for _ in range(4)]
        seen = list(rt.as_completed(futs))
        assert len(seen) == 4
        assert {id(f) for f in seen} == {id(f) for f in futs}
        assert all(isinstance(f, RuntimeFuture) for f in seen)


def test_actor_and_op_futures_mixed_in_get():
    with Runtime() as rt:
        c = rt.create_actor("counter", 10)
        f_actor = c.call("add", 5)
        f_op = rt.submit_operation("square", 4)
        r_actor, r_op = rt.get([f_actor, f_op])
        assert r_actor.value == 15
        assert r_op.value == 16
        assert r_actor.row["actor_id"].startswith("rt-act-")
        assert r_op.row["actor_id"].startswith("rt-hpx-")


def test_actor_and_op_futures_mixed_in_wait():
    # Order-independent: wait() returns ALL currently-ready futures, so assert only
    # the partition invariant (>=1 ready, total preserved) and that it is
    # non-consuming -- NOT which of the actor/op future is ready first.
    with Runtime() as rt:
        c = rt.create_actor("counter", 10)
        f_actor = c.call("add", 5)
        f_op = rt.submit_operation("square", 4)
        ready, not_ready = rt.wait([f_actor, f_op], num_returns=1)
        assert len(ready) >= 1
        assert len(ready) + len(not_ready) == 2
        # non-consuming: both still retire afterward, in input order
        r_actor, r_op = rt.get([f_actor, f_op])
        assert r_actor.value == 15
        assert r_op.value == 16


def test_actor_and_op_futures_mixed_in_as_completed():
    # Order-independent: assert each future is yielded exactly once (set equality +
    # count), NOT the completion order of the actor vs op future.
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        f_actor = c.call("add", 7)
        f_op = rt.submit_operation("square", 3)
        futs = [f_actor, f_op]
        seen = list(rt.as_completed(futs))
        assert all(isinstance(f, RuntimeFuture) for f in seen)   # handles, not results
        assert {id(f) for f in seen} == {id(f) for f in futs}
        assert len(seen) == 2
        # as_completed is non-consuming; the caller still retires each future
        assert f_actor.result().value == 7
        assert f_op.result().value == 9


# --- cancellation token path for actor futures ------------------------------


def test_actor_future_cancel_complete_consistency():
    """Actor-future cancel/complete state machine is consistent.

    Races cancel() against the instantaneous add over a loop. The MVP actor methods
    are instantaneous, so we CANNOT deterministically hold the lane to force a
    cancel; instead we assert the INVARIANT that holds regardless of who wins:
    cancel() returns True iff the call retired as "cancelled" (a queued-skip) -- never
    True+completed. A completed add advances the state by exactly 1, so the value of
    the k-th completed add equals the running count of completed adds (deterministic
    even though WHICH iterations cancel is timing-dependent).
    """
    with Runtime(num_lanes=1, hpx_threads=2) as rt:
        c = rt.create_actor("counter", 0)
        completed = 0
        for _ in range(64):
            f = c.call("add", 1)
            cancelled = f.cancel()
            res = f.result()
            st = res.row["status"]
            assert cancelled == (st == "cancelled")   # never True + completed
            if st == "completed":
                completed += 1
                assert res.value == completed         # state advanced by exactly 1
            else:
                assert st == "cancelled"
                with pytest.raises(OperationCancelledError):
                    _ = res.value
        # Final state equals the number of adds that actually ran.
        assert c.call("get").result().value == completed


def test_actor_future_terminal_cancel_is_false():
    # Once an actor-method future has retired (terminal), cancel() must return False
    # (it cannot claim to cancel an already-fulfilled future) and cancelled() False.
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        f = c.call("add", 1)
        assert f.result().value == 1
        assert f.cancel() is False
        assert f.cancelled() is False


# --- row contract -----------------------------------------------------------


def test_actor_row_shape_and_value_separation():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        res = c.call("add", 1).result()
        assert set(res.row) == set(ROW_FIELDS)
        for f in FORBIDDEN_ROW_FIELDS:
            assert f not in res.row
        assert "value" not in res.row
        aid = res.row["actor_id"]
        assert aid.startswith("rt-act-")
        assert ACTOR_ID_RE.fullmatch(aid)


def test_actor_row_shape_get_and_reset():
    # Same exact 9-field / value-separated row contract as `add`, for the other two
    # registered methods (one mutating: reset; one read: get).
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        for res in (c.call("reset", 3).result(), c.call("get").result()):
            assert set(res.row) == set(ROW_FIELDS)
            for f in FORBIDDEN_ROW_FIELDS:
                assert f not in res.row
            assert "value" not in res.row
            aid = res.row["actor_id"]
            assert aid.startswith("rt-act-")
            assert ACTOR_ID_RE.fullmatch(aid)


def test_happy_path_value_returns_typed():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        v = c.call("add", 21).result().value
        assert isinstance(v, int) and v == 21


# --- validation happens before any future -----------------------------------


def test_create_unknown_type_raises_value_error():
    with Runtime() as rt:
        with pytest.raises(ValueError):
            rt.create_actor("nope", 0)


def test_call_unknown_method_raises_value_error():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        with pytest.raises(ValueError):
            c.call("missing")


def test_wrong_type_raises_type_error():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        with pytest.raises(TypeError):
            c.call("add", "x")
        with pytest.raises(TypeError):
            rt.create_actor("counter", "x")


def test_validation_before_future_leaves_state_untouched():
    # Invalid calls are rejected at the Python boundary BEFORE any native crossing /
    # future creation, so they cannot mutate actor state: a rejected call between two
    # valid ones leaves the counter exactly where the valid calls left it.
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        assert c.call("add", 5).result().value == 5
        with pytest.raises(TypeError):
            c.call("add", "x")        # wrong arg type -> rejected before dispatch
        with pytest.raises(ValueError):
            c.call("missing")         # unknown method -> rejected before dispatch
        assert c.call("get").result().value == 5   # state untouched


def test_no_remote_and_no_attribute_method_surface():
    # The closed registered-method contract: the ONLY dispatch surface is call().
    # There is deliberately no Ray-style .remote() and no attribute-style
    # actor.method(...) (which would imply an open/arbitrary method set).
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        assert not hasattr(c, "remote")
        assert not hasattr(ActorHandle, "remote")
        with pytest.raises(AttributeError):
            c.add(1)                  # no attribute-style dispatch
        assert callable(c.call)       # call() is the sole method-dispatch surface
        assert c.call("add", 1).result().value == 1


# --- post-shutdown -----------------------------------------------------------


def test_create_after_shutdown_raises():
    rt = Runtime()
    rt.shutdown()
    with pytest.raises(RuntimeError):
        rt.create_actor("counter", 0)


def test_call_after_shutdown_raises():
    rt = Runtime()
    c = rt.create_actor("counter", 0)
    rt.shutdown()
    with pytest.raises(RuntimeError):
        c.call("add", 1)


def test_clean_shutdown_with_unretired_actor_futures():
    """Clean shutdown with un-retired actor futures (then retire-after, reinit).

    NOTE (honest scope): this does NOT prove actor-lane teardown ordering or
    shared_ptr-state survival under in-flight work. CounterActor methods are
    instantaneous, so the actor lane has almost certainly drained before shutdown()
    observes any backlog -- the cancel-then-drain / actors_-clear-after-join paths are
    not deterministically exercised here. It protects that shutdown is clean (no
    hang / no raise) when method futures were never retired, that every such future
    still retires AFTER shutdown with a valid terminal row (no broken promise --
    completed vs cancelled is timing-dependent and never asserted), and that a fresh
    process-singleton Runtime constructs and works afterward.
    """
    rt = Runtime()
    c = rt.create_actor("counter", 0)
    futs = [c.call("add", 1) for _ in range(5)]   # not retired before shutdown
    rt.shutdown()                     # must not hang or raise
    # No broken promise: every held future retires cleanly after shutdown with a
    # terminal row. Which status each got is timing-dependent -- never asserted.
    for f in futs:
        res = f.result()
        st = res.row["status"]
        assert st in ("completed", "cancelled")
        if st == "cancelled":
            with pytest.raises(OperationCancelledError):
                _ = res.value
        else:
            assert isinstance(res.value, int)
    # process-singleton: a fresh Runtime must construct cleanly after the prior one
    # was torn down, and a new actor must work.
    with Runtime() as rt2:
        c2 = rt2.create_actor("counter", 0)
        assert c2.call("add", 1).result().value == 1


# --- busy_get: read-only checkpointed method (C5) ---------------------------
#
# busy_get(work_n) performs synthetic on-core diagnostic/calibration work (the
# actor-method analog of the op-level busy_sum), is READ-ONLY (never mutates the
# counter), and returns the CURRENT value. With work_n > BUSY_SUM_STRIDE (8192) it is
# running-cancellable through the actor dispatch path. NO timing assertions, NO
# sleeps, NO performance claim anywhere below. These C5 tests predate
# ActorHandle.stats() and stay race-invariant-scoped on purpose; the deterministic
# stats-gated counterparts live in the next section.

# Just above BUSY_SUM_STRIDE so checkpoint_count > 1 (running-cancel armed) while a
# full completion stays sub-millisecond -- safe to run inside a loop. Never a huge
# value in a loop (see the one-shot shutdown test for the large-work_n case).
_BUSY_GET_WORK_N = 100_000


def test_busy_get_completion_is_read_only():
    # Deterministic: busy_get returns the current state and mutates nothing, so its
    # completed value and a following get() both equal what the add() left.
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        c.call("add", 7).result()
        res = c.call("busy_get", _BUSY_GET_WORK_N).result()
        assert res.row["status"] == "completed"
        assert res.value == 7                          # current state, returned
        assert isinstance(res.value, int)
        assert c.call("get").result().value == 7       # read-only: unchanged


def test_busy_get_cancel_complete_invariant():
    """Invariant coverage of the ARMED actor checkpoint/cancel path via busy_get.

    busy_get with a checkpointed (but cheap) work_n arms running-cancellability via
    MethodEntry.checkpoint_count, so a cancel() that lands while the method is in the
    service slot CAN settle as a running cancel at a chunk boundary -- unlike the
    instantaneous add() path, which is queued-cancelable only. We race cancel()
    against the call over a loop and assert the race-INDEPENDENT invariant that holds
    no matter which branch (queued-skip, running stop, or cancel-loss) wins:

      * cancel() returns True iff the call retired as "cancelled" (never True+completed);
      * a cancelled call exposes no value (.value raises OperationCancelledError);
      * a completed call returns the CURRENT counter value, and busy_get is read-only
        so that value never changes across the loop;
      * the final get() equals the value the add() left.

    HONEST SCOPE: this is INVARIANT coverage of the armed checkpoint/cancel state
    machine reached through actor dispatch. It is NOT a deterministic proof that the
    specific running-cancel (chunk-boundary) branch executed: this loop deliberately
    does not gate on stats(), so it cannot assert WHICH branch won on a given
    iteration (test_actor_running_cancel_deterministic is the stats-gated
    deterministic version). work_n is
    kept just above BUSY_SUM_STRIDE so checkpoint_count > 1 (running-cancel armed)
    while a full completion stays cheap -- never a huge value inside this loop.
    """
    with Runtime(num_lanes=1, hpx_threads=2) as rt:
        c = rt.create_actor("counter", 0)
        c.call("add", 3).result()                      # fixed read-only target value
        for _ in range(64):
            f = c.call("busy_get", _BUSY_GET_WORK_N)
            cancelled = f.cancel()
            res = f.result()
            st = res.row["status"]
            assert cancelled == (st == "cancelled")    # never True + completed
            if st == "cancelled":
                with pytest.raises(OperationCancelledError):
                    _ = res.value
            else:
                assert st == "completed"
                assert res.value == 3                  # read-only: state unchanged
        assert c.call("get").result().value == 3       # state never moved


def test_shutdown_with_unretired_busy_get():
    """Clean shutdown/reinit with CHECKPOINTED actor work un-retired at shutdown.

    Submits one large busy_get plus a few more calls, retires NONE of them before
    shutting the Runtime down (they are retired only after, asserting no broken
    promise). A large work_n makes it more likely (vs the instantaneous-add case)
    that the actor lane is still mid-method when shutdown() runs, so the
    cancel-then-drain path is exercised rather than a lane that already drained. A huge
    work_n is acceptable here ONLY because this is a one-shot test, not a loop, and the
    shutdown cancel_pending() stops the in-flight method at its next chunk boundary.

    HONEST SCOPE: this asserts that shutdown is CLEAN (no hang, no raise), that every
    held future retires AFTER shutdown with a valid terminal row (no broken promise;
    completed vs cancelled is timing-dependent and never asserted), and that a fresh
    process-singleton Runtime + actor works afterward. It does NOT prove actor-lane
    teardown ORDERING or shared_ptr-state survival under in-flight work -- those
    internal orderings are not assertable from Python (the stats-gated
    test_actor_shutdown_fulfills_all_futures proves the provably-in-flight
    fulfillment case, not ordering).
    """
    rt = Runtime()
    c = rt.create_actor("counter", 0)
    futs = [c.call("busy_get", 2_000_000_000)]  # large, likely in-flight
    for _ in range(5):
        futs.append(c.call("busy_get", _BUSY_GET_WORK_N))  # queued
    rt.shutdown()                         # must not hang or raise
    # No broken promise: retire everything after shutdown; terminal rows only.
    for f in futs:
        res = f.result()
        st = res.row["status"]
        assert st in ("completed", "cancelled")
        if st == "cancelled":
            with pytest.raises(OperationCancelledError):
                _ = res.value
        else:
            assert isinstance(res.value, int)
    with Runtime() as rt2:
        c2 = rt2.create_actor("counter", 0)
        assert c2.call("add", 1).result().value == 1


# --- ActorHandle.stats(): per-actor observability ----------------------------
#
# stats() is a NON-consuming, point-in-time, racy snapshot of the actor's
# dedicated lane ({actor_id, queue_depth, active}) -- the actor analogue of one
# Runtime.lane_stats() element. The tests below gate on it (wait_until polling,
# the established op-level pattern) instead of sleeps, and assert only
# direction-of-change / provably-held states -- NO timing assertions, NO
# performance claims. They convert the C4/C5 "cannot assert without actor
# observability" omissions (deterministic QueueFull, deterministic running
# cancel, in-flight-at-shutdown) into deterministic coverage.

# Large checkpointed service-slot occupant: ~244k checkpoints (order-of-a-second
# of native work vs the tests' microsecond-scale windows -- an engineering margin
# of several orders of magnitude, not a formal bound), so once observed active it
# still has boundaries ahead for cancel/shutdown to stop at -- it is never run to
# completion in these tests.
_BIG_BUSY_GET_N = 2_000_000_000


def test_actor_stats_shape_idle_and_nonconsuming():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        s = c.stats()
        assert set(s) == {"actor_id", "queue_depth", "active"}
        assert s["actor_id"] == c.actor_id
        assert ACTOR_ID_RE.fullmatch(s["actor_id"])
        assert s["queue_depth"] == 0
        assert s["active"] is False
        # Non-consuming: the snapshot changed no call semantics or state.
        assert c.call("add", 1).result().value == 1
        s2 = c.stats()
        assert s2["queue_depth"] == 0 and s2["active"] is False


def test_actor_queue_full_deterministic(wait_until):
    """Deterministic actor-path QueueFull via stats gating (no sleeps).

    cap=1 on the actor's dedicated lane. Occupy the service slot with a large
    checkpointed busy_get and GATE on stats() showing it active (popped -> not
    counted against the cap, queue provably empty again); admit exactly one
    queued call (depth == cap); the next call MUST raise QueueFullError and
    create nothing. Same stats-gated pattern as the op-level
    test_bounded_admission_queue_full. HONEST SCOPE: the hold relies on the
    occupant (order-of-a-second of native work) not retiring within the
    microsecond-scale enqueue window -- an engineering margin of several orders
    of magnitude, not a formal guarantee; stats gating removes the start-side
    race, the work size bounds the finish-side one. Cleanup follows the same
    pattern: cancel the occupant at its next checkpoint and the queued survivor
    still completes FIFO.
    """
    with Runtime(num_lanes=1, max_queue_depth_per_lane=1, hpx_threads=2) as rt:
        c = rt.create_actor("counter", 5)
        big = c.call("busy_get", _BIG_BUSY_GET_N)        # occupies the slot
        wait_until(lambda: c.stats()["active"], where="busy_get active")
        assert c.stats()["queue_depth"] == 0             # occupant not counted
        queued = c.call("get")                           # fills the one slot
        assert c.stats()["queue_depth"] == 1             # pinned at the cap
        with pytest.raises(QueueFullError):
            c.call("get")                                # cap reached: rejected
        assert c.stats()["queue_depth"] == 1             # rejection left no entry
        assert big.cancel() is True                      # stop at next checkpoint
        assert big.result().row["status"] == "cancelled"
        assert queued.result().value == 5                # survivor completes FIFO


def test_actor_running_cancel_deterministic(wait_until):
    """cancel() == True once stats() shows the call active (stats-gated).

    Once active is observed, the call was popped into the service slot with
    ~244k checkpoint boundaries ahead (work_n >> BUSY_SUM_STRIDE), so cancel()
    settles a cancellation and the row retires status="cancelled". HONEST
    SCOPE, two residues: (1) like the QueueFull test, this relies on the
    order-of-a-second occupant not completing within the microsecond gap
    between the active observation and cancel() -- a several-orders-of-magnitude
    engineering margin, not a formal guarantee; (2) active is set at pop, an
    instruction before begin_service, so the settled cancel could in principle
    be a queued-phase skip rather than a chunk-boundary stop. Every assertion
    below holds on either branch; the chunk-boundary branch is the
    overwhelmingly exercised path.
    """
    with Runtime(num_lanes=1, hpx_threads=2) as rt:
        c = rt.create_actor("counter", 3)
        f = c.call("busy_get", _BIG_BUSY_GET_N)
        wait_until(lambda: c.stats()["active"], where="busy_get active")
        assert f.cancel() is True                        # guaranteed to settle
        res = f.result()
        assert res.row["status"] == "cancelled"
        with pytest.raises(OperationCancelledError):
            _ = res.value
        assert c.call("get").result().value == 3         # read-only: untouched


def test_actor_shutdown_fulfills_all_futures(wait_until):
    """No broken promise at teardown: every future retires AFTER shutdown.

    Stats gating proves checkpointed work is in flight when shutdown() runs
    (the case the C5 shutdown test could only make likely). Shutdown
    cancel-then-drains and fulfills every promise BEFORE the lane worker joins,
    so each future still retires cleanly after the runtime is gone, with a
    valid terminal row. Honest scope: which calls retired cancelled vs
    completed is timing-dependent, so only terminal validity and
    cancelled/value consistency are asserted -- never a specific status split.
    """
    rt = Runtime(num_lanes=1, hpx_threads=2)
    c = rt.create_actor("counter", 0)
    futs = [c.call("busy_get", _BIG_BUSY_GET_N)]
    futs += [c.call("add", 1) for _ in range(4)]
    wait_until(lambda: c.stats()["active"], where="busy_get active")
    rt.shutdown()                                        # must not hang or raise
    for f in futs:
        res = f.result()                                 # retire after shutdown
        st = res.row["status"]
        assert st in ("completed", "cancelled")
        if st == "cancelled":
            with pytest.raises(OperationCancelledError):
                _ = res.value
        else:
            assert isinstance(res.value, int)


def test_actor_stats_after_shutdown_raises():
    rt = Runtime()
    c = rt.create_actor("counter", 0)
    assert c.stats()["active"] is False
    rt.shutdown()
    with pytest.raises(RuntimeError):
        c.stats()


def test_lane_stats_excludes_actor_lanes():
    # Runtime.lane_stats() keeps its existing contract -- op lanes only, same
    # length and id prefix -- even with actor lanes live. Actor lanes are
    # visible only through the per-handle ActorHandle.stats() snapshot.
    with Runtime(num_lanes=2) as rt:
        rt.create_actor("counter", 0)
        rt.create_actor("counter", 1)
        stats = rt.lane_stats()
        assert len(stats) == 2
        assert all(s["actor_id"].startswith("rt-hpx-") for s in stats)


# --- Runtime.release_actor: explicit local actor lifecycle -------------------
#
# release_actor(actor) is shutdown()'s actor teardown scoped to ONE actor:
# synchronous and bounded (cancel queued calls; ask an in-flight checkpointed
# method to stop at its next boundary; drain; join; only then drop the native
# state). Strict lifecycle: call/stats/second release on a released handle all
# raise RuntimeError. LOCAL explicit release only -- not ray.kill, no
# distributed ownership, no refcounting, no GC lifecycle. As everywhere in this
# module: stats/wait_until gating instead of sleeps, no timing assertions, and
# no exact completed-vs-cancelled split asserted for in-flight work.


def test_release_idle_actor_returns_none():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        assert c.call("add", 1).result().value == 1
        assert rt.release_actor(c) is None


def test_call_after_release_raises():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        rt.release_actor(c)
        with pytest.raises(RuntimeError, match="released"):
            c.call("get")


def test_stats_after_release_raises():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        rt.release_actor(c)
        with pytest.raises(RuntimeError, match="released"):
            c.stats()


def test_double_release_raises():
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        rt.release_actor(c)
        with pytest.raises(RuntimeError, match="already released"):
            rt.release_actor(c)


def test_release_non_handle_raises_type_error():
    with Runtime() as rt:
        with pytest.raises(TypeError):
            rt.release_actor("rt-act-0123456789abcdef")


def test_release_after_runtime_shutdown_raises():
    rt = Runtime()
    c = rt.create_actor("counter", 0)
    rt.shutdown()
    with pytest.raises(RuntimeError, match="shut down"):
        rt.release_actor(c)


def test_release_then_runtime_shutdown_clean():
    rt = Runtime()
    c = rt.create_actor("counter", 0)
    rt.release_actor(c)
    rt.shutdown()  # must not hang or raise; actor record is already gone
    with Runtime() as rt2:  # process guard released cleanly
        assert rt2.create_actor("counter", 0).call("get").result().value == 0


def test_unretired_completed_future_retirable_after_release(wait_until):
    # A future whose work completed BEFORE release must still retire normally
    # after release (release resolves/affects nothing already fulfilled).
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        f = c.call("add", 7)
        ready, _ = rt.wait([f], num_returns=1)  # fulfilled, not retired
        assert f in ready
        rt.release_actor(c)
        res = f.result()
        assert res.row["status"] == "completed"
        assert res.value == 7


def test_release_cancels_queued_calls_behind_active_busy_get(wait_until):
    """Queued calls at release retire cancelled; every future resolves.

    Stats gating proves the busy_get occupies the service slot, so the adds
    behind it are provably QUEUED when release_actor runs: release must cancel
    them (status="cancelled", value raises). The occupant itself is in-flight
    checkpointed work whose completed-vs-cancelled outcome is timing-dependent
    and deliberately NOT pinned (same honest scope as the shutdown tests).
    """
    with Runtime(num_lanes=1, hpx_threads=2) as rt:
        c = rt.create_actor("counter", 0)
        big = c.call("busy_get", _BIG_BUSY_GET_N)
        wait_until(lambda: c.stats()["active"], where="busy_get active")
        queued = [c.call("add", 1) for _ in range(3)]
        assert c.stats()["queue_depth"] == 3
        rt.release_actor(c)  # blocks until the lane drained and joined
        for f in queued:
            res = f.result()
            assert res.row["status"] == "cancelled"
            with pytest.raises(OperationCancelledError):
                _ = res.value
        st = big.result().row["status"]
        assert st in ("completed", "cancelled")


def test_release_with_active_busy_get_terminal(wait_until):
    """Release returns only after the in-flight method is terminal.

    Once release_actor returns, the lane worker has joined, so the occupant's
    future is necessarily fulfilled -- ready without any further wait, with a
    valid terminal row (cancelled at a checkpoint boundary on the overwhelmingly
    exercised path, completed if it raced the final chunk; never pinned).
    """
    with Runtime(num_lanes=1, hpx_threads=2) as rt:
        c = rt.create_actor("counter", 0)
        big = c.call("busy_get", _BIG_BUSY_GET_N)
        wait_until(lambda: c.stats()["active"], where="busy_get active")
        rt.release_actor(c)
        assert big.ready()  # worker joined => promise already fulfilled
        res = big.result()
        st = res.row["status"]
        assert st in ("completed", "cancelled")
        if st == "cancelled":
            with pytest.raises(OperationCancelledError):
                _ = res.value
        else:
            assert isinstance(res.value, int)


def test_release_one_actor_leaves_other_unaffected():
    with Runtime() as rt:
        a = rt.create_actor("counter", 0)
        b = rt.create_actor("counter", 100)
        a.call("add", 1).result()
        b.call("add", 5).result()
        rt.release_actor(a)
        # b's state and lane are untouched by a's release.
        assert b.call("get").result().value == 105
        assert b.stats()["active"] is False
        with pytest.raises(RuntimeError, match="released"):
            a.call("get")


def test_released_rows_keep_actor_id():
    # Rows are stamped with the lane's rt-act- id at task build time (string
    # copies); release frees the lane/state but never mutates produced rows.
    with Runtime() as rt:
        c = rt.create_actor("counter", 0)
        aid = c.actor_id
        res_before = c.call("add", 1).result()
        f_unretired = c.call("get")
        rt.release_actor(c)
        assert res_before.row["actor_id"] == aid
        assert ACTOR_ID_RE.fullmatch(res_before.row["actor_id"])
        assert f_unretired.result().row["actor_id"] == aid


def test_lane_stats_op_lanes_only_after_release():
    with Runtime(num_lanes=2) as rt:
        c = rt.create_actor("counter", 0)
        rt.release_actor(c)
        stats = rt.lane_stats()
        assert len(stats) == 2
        assert all(s["actor_id"].startswith("rt-hpx-") for s in stats)
