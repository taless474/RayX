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
    OperationResult,
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
