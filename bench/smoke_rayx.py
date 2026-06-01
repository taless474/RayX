#!/usr/bin/env python3
"""Tiny shape-only contract smoke for the rayx Python frontend.

Exercises the core rayx public facade (Engine / SyntheticActor / Future) over a
built `_rayx` extension and asserts the *shape* of what comes back -- field
presence and basic ordering -- not timing values (those are workload/host
dependent). Mirrors the style of bench/smoke_diag.py: print a PASS line per
section, exit 0 on success, exit 1 with a reason on the first failed assertion.

Requires the rayx extension to be BUILT (python/src/rayx/_rayx*.so); it adds
python/src to sys.path automatically. It does NOT run any benchmark matrix.

Notes on the rayx contract this locks:
  * The HPX runtime is a process singleton -- only one active Engine /
    SyntheticActor at a time -- so the Engine is fully shut down (context exit)
    before the SyntheticActor section runs.
  * rayx's Future.result() does NOT expose a request_id (request ids are
    driver-assigned, not part of the facade), so this does not assert one. The
    lane identity is `actor_id`.

Usage:
    python bench/smoke_rayx.py
Exit code 0 == pass, 1 == fail.
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)


def _fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def _expect_raise(fn, where):
    # Shape-only: assert fn() raises *some* exception (we check that the
    # contract rejects bad input, not the exact exception type/message).
    try:
        fn()
    except Exception:
        return
    _fail(f"{where}: expected an exception, none raised")


# The exact field set rayx's Future.result() returns. request_id is
# deliberately absent (not part of the facade).
RESULT_FIELDS = (
    "status",
    "error",
    "actor_id",
    "submit_ns",
    "start_ns",
    "end_ns",
    "total_ms",
    "queue_wait_ms",
    "service_ms_observed",
)


def _check_row(row, where):
    if not isinstance(row, dict):
        _fail(f"{where}: result() returned {type(row).__name__}, expected dict")
    for field in RESULT_FIELDS:
        if field not in row:
            _fail(f"{where}: result missing field {field!r} (got {sorted(row)})")
    if row["status"] != "completed":
        _fail(f"{where}: status is {row['status']!r}, expected 'completed'")
    if row["error"] is not None:
        _fail(f"{where}: error is {row['error']!r}, expected None")
    if not row["actor_id"]:
        _fail(f"{where}: actor_id is empty")
    # Shape-only ordering/non-negativity sanity (no timing thresholds).
    if row["end_ns"] < row["start_ns"]:
        _fail(f"{where}: end_ns < start_ns")
    for non_neg in ("total_ms", "queue_wait_ms", "service_ms_observed"):
        if row[non_neg] < 0:
            _fail(f"{where}: {non_neg} is negative ({row[non_neg]})")


def check_import():
    try:
        import rayx  # noqa: F401
    except Exception as exc:  # ImportError or HPX/runtime load failure
        _fail(f"could not import rayx (build the _rayx extension first): {exc}")
    print("PASS: import rayx")


def check_hpx_smoke():
    # hpx_smoke() is the retained no-arg debug helper: it starts HPX as a
    # library, runs a trivial async returning 42, and shuts down cleanly.
    import rayx
    out = rayx.hpx_smoke()
    if not isinstance(out, dict):
        _fail(f"hpx_smoke returned {type(out).__name__}, expected dict")
    if out.get("status") != "ok":
        _fail(f"hpx_smoke status is {out.get('status')!r}, expected 'ok'")
    if out.get("value") != 42:
        _fail(f"hpx_smoke value is {out.get('value')!r}, expected 42")
    print("PASS: hpx_smoke() -> ok/value=42")


def check_engine():
    from rayx import Engine
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        if engine.num_lanes() != 2:
            _fail(f"num_lanes() is {engine.num_lanes()}, expected 2")

        # One no-op request.
        _check_row(engine.submit(service_ms=0).result(), "engine.submit noop")

        # One tiny sleep request (shape only, no timing assertion).
        _check_row(engine.submit(service_ms=1).result(), "engine.submit sleep1")

        # Multiple requests.
        futures = [engine.submit(service_ms=0) for _ in range(5)]
        for i, fut in enumerate(futures):
            _check_row(fut.result(), f"engine.submit multi[{i}]")

        # Batch: one Python->C++ crossing enqueues count requests.
        batch = engine.submit_batch(service_ms=0, count=5)
        if len(batch) != 5:
            _fail(f"submit_batch returned {len(batch)} futures, expected 5")
        for i, fut in enumerate(batch):
            _check_row(fut.result(), f"engine.submit_batch[{i}]")
    print("PASS: Engine submit / multi / submit_batch -> well-formed results")


def check_ready_and_wait():
    # Future.ready() shape, Engine.wait(..., num_returns=1) partition, and a
    # wait loop that retires multiple futures as-completed. Shape-only.
    from rayx import Engine
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        # Future.ready() returns a bool. A no-op should become ready; poll a
        # bounded number of times (NOT a contract busy-loop, just a shape probe).
        f = engine.submit(service_ms=0)
        r = f.ready()
        if not isinstance(r, bool):
            _fail(f"Future.ready() returned {type(r).__name__}, expected bool")
        _check_row(f.result(), "ready-probe retire")  # retire it cleanly
        # ready() on a retired future must raise (consumed/invalid).
        try:
            f.ready()
        except Exception:
            pass
        else:
            _fail("ready() on a retired future should raise")

        # Engine.wait(num_returns=1) -> (ready, not_ready) partition of the SAME
        # objects; counts conserved, ready non-empty, every entry a Future.
        futs = [engine.submit(service_ms=0) for _ in range(6)]
        ready, not_ready = engine.wait(futs, num_returns=1)
        if len(ready) + len(not_ready) != len(futs):
            _fail(f"wait partition lost futures: {len(ready)}+{len(not_ready)} "
                  f"!= {len(futs)}")
        if not ready:
            _fail("wait(num_returns=1) returned empty ready list")
        for f in (*ready, *not_ready):
            if not hasattr(f, "result"):
                _fail("wait returned a non-Future entry")

        # As-completed wait loop: drain all remaining futures, one shared
        # recv_ns per sweep, and verify every retired row is well-formed.
        import time
        inflight = list(futs)
        retired = 0
        while inflight:
            ready, not_ready = engine.wait(inflight, num_returns=1)
            recv_ns = time.perf_counter_ns()
            for f in ready:
                _check_row(f.result(recv_ns=recv_ns), f"wait-loop retire[{retired}]")
                retired += 1
            inflight = not_ready
        if retired != len(futs):
            _fail(f"wait loop retired {retired}, expected {len(futs)}")
    print("PASS: Future.ready / Engine.wait / as-completed loop -> well-formed")


def check_as_completed():
    # Engine.as_completed(futures) is an ergonomic generator over Engine.wait:
    # it yields the ORIGINAL Future objects as they become ready, each exactly
    # once, and the caller retires them with .result(). Shape-only: no timing.
    from rayx import Engine
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        futs = [engine.submit(service_ms=0) for _ in range(6)]
        original = set(id(f) for f in futs)
        seen = []
        for f in engine.as_completed(futs):
            if not hasattr(f, "result"):
                _fail("as_completed yielded a non-Future entry")
            if id(f) not in original:
                _fail("as_completed yielded an object not in the input list")
            seen.append(id(f))
            # Caller retires each yielded future; validate the row shape.
            _check_row(f.result(), f"as_completed retire[{len(seen) - 1}]")
        if len(seen) != len(futs):
            _fail(f"as_completed yielded {len(seen)} futures, expected "
                  f"{len(futs)}")
        if len(set(seen)) != len(futs):
            _fail("as_completed yielded a future more than once")
        if set(seen) != original:
            _fail("as_completed did not yield every input future")
    print("PASS: Engine.as_completed -> yields each original future once, "
          "well-formed")


def check_double_result():
    # result() consumes the Future: a first call succeeds, a second call raises
    # cleanly (not a raw HPX error), and ready() after retire raises too. Same
    # contract holds for futures yielded by as_completed. Shape-only.
    from rayx import Engine
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        # Direct submit: first result() ok, second raises, ready() after raises.
        f = engine.submit(service_ms=0)
        _check_row(f.result(), "double-result first retire")
        _expect_raise(lambda: f.result(), "second result() on retired future")
        _expect_raise(lambda: f.ready(), "ready() after retire")

        # Through as_completed: each yielded future retires once; a second
        # result() on a yielded future raises.
        futs = [engine.submit(service_ms=0) for _ in range(4)]
        retired = 0
        for fut in engine.as_completed(futs):
            _check_row(fut.result(), f"double-result as_completed retire[{retired}]")
            _expect_raise(lambda fut=fut: fut.result(),
                          f"second result() on as_completed future[{retired}]")
            retired += 1
        if retired != len(futs):
            _fail(f"as_completed retired {retired}, expected {len(futs)}")
    print("PASS: Future.result double-call guard (second result / ready after "
          "retire raise; as_completed futures retire once) -> clean")


def check_repr():
    # Future.__repr__ is a Python-only debug aid: it must be non-blocking and
    # must NOT consume the future. Shape-only -- assert the substring tokens
    # (rayx.Future, pending/ready, retired), never timing values.
    from rayx import Engine
    with Engine(num_lanes=1, hpx_threads=2) as engine:
        f = engine.submit(service_ms=0)
        live = repr(f)
        if not isinstance(live, str):
            _fail(f"repr(future) returned {type(live).__name__}, expected str")
        if "rayx.Future" not in live:
            _fail(f"live repr missing 'rayx.Future': {live!r}")
        if ("pending" not in live) and ("ready" not in live):
            _fail(f"live repr missing pending/ready state: {live!r}")
        # repr must not consume the future: result() still succeeds afterward.
        _check_row(f.result(), "repr-then-result retire")
        retired = repr(f)
        if "retired" not in retired:
            _fail(f"repr after result() missing 'retired': {retired!r}")
    print("PASS: Future.__repr__ (rayx.Future + pending/ready live; retired "
          "after result(); does not consume the future) -> shape ok")


def check_wait_contracts():
    # Negative-input contracts for Engine.wait: every bad call must raise, and
    # must NOT consume the in-flight futures it was handed (the C++ layer
    # validates args / borrows before moving any future out). Shape-only.
    from rayx import Engine

    with Engine(num_lanes=2, hpx_threads=2) as engine:
        # A set of valid, still-in-flight futures to probe the guards with.
        futs = [engine.submit(service_ms=0) for _ in range(4)]

        # Empty list (caught by the Python facade before C++).
        _expect_raise(lambda: engine.wait([]), "wait([])")
        # num_returns out of range (validated before any future is borrowed).
        _expect_raise(lambda: engine.wait(futs, num_returns=0),
                      "wait(num_returns=0)")
        _expect_raise(lambda: engine.wait(futs, num_returns=len(futs) + 1),
                      "wait(num_returns>len)")
        # Wrong object type (no _cf attribute / not a _Future).
        _expect_raise(lambda: engine.wait([123]), "wait([wrong_object])")

        # The four bad calls above must have left futs untouched: still waitable.
        ready, not_ready = engine.wait(futs, num_returns=1)
        if len(ready) + len(not_ready) != len(futs):
            _fail(f"wait after bad calls lost futures: {len(ready)}+"
                  f"{len(not_ready)} != {len(futs)}")

        # Duplicate Future in the list must raise (the same underlying _Future
        # may appear at most once). Use a dedicated future so the futs accounting
        # below is unaffected. Detection happens before any borrow/take, so the
        # future stays usable -- prove that by waiting/retiring it cleanly.
        dup = engine.submit(service_ms=0)
        _expect_raise(lambda: engine.wait([dup, dup]), "wait([f, f])")
        ready, _not_ready = engine.wait([dup], num_returns=1)
        if dup not in ready:
            _fail("wait([f]) after a rejected duplicate did not return f ready")
        _check_row(dup.result(), "wait-contracts post-duplicate retire")

        # Retire one future, then waiting on it must raise (already retired).
        retired = futs[0]
        _check_row(retired.result(), "wait-contracts retire-one")
        _expect_raise(lambda: engine.wait([retired]),
                      "wait([already_retired_future])")

        # Drain the remaining still-valid futures cleanly (as-completed loop).
        import time
        inflight = futs[1:]
        drained = 0
        while inflight:
            ready, not_ready = engine.wait(inflight, num_returns=1)
            recv_ns = time.perf_counter_ns()
            for f in ready:
                _check_row(f.result(recv_ns=recv_ns),
                           f"wait-contracts drain[{drained}]")
                drained += 1
            inflight = not_ready
        if drained != len(futs) - 1:
            _fail(f"drained {drained}, expected {len(futs) - 1}")

    # Shutdown-lifetime contract: shutdown() is a graceful drain. Submit a
    # no-op and a tiny sleep (plus probe/keep futures) BEFORE shutting down;
    # after shutdown the drained futures stay valid -- ready()==True and
    # result() retires them -- while NEW work (wait / as_completed, both via the
    # engine) raises. Own engine + explicit shutdown so this never overlaps the
    # singleton runtime above. Shape-only: no timing thresholds.
    engine2 = Engine(num_lanes=1, hpx_threads=2)
    fut_noop = engine2.submit(service_ms=0)
    fut_sleep = engine2.submit(service_ms=1)
    fut_probe = engine2.submit(service_ms=0)
    fut_keep = engine2.submit(service_ms=0)  # left unretired for raise checks
    engine2.shutdown()
    # Drained futures remain valid after shutdown: ready() is True, result()
    # retires a well-formed row.
    r = fut_probe.ready()
    if r is not True:
        _fail(f"ready() after shutdown returned {r!r}, expected True")
    _check_row(fut_noop.result(), "post-shutdown noop retire")
    _check_row(fut_sleep.result(), "post-shutdown sleep retire")
    _check_row(fut_probe.result(), "post-shutdown probe retire")
    # New work after shutdown still raises (engine is shut down).
    _expect_raise(lambda: engine2.wait([fut_keep]), "wait after shutdown")
    _expect_raise(lambda: list(engine2.as_completed([fut_keep])),
                  "as_completed after shutdown")

    print("PASS: Engine.wait negative contracts (empty / num_returns / wrong "
          "type / retired / post-shutdown) -> raise; post-shutdown drained "
          "futures retire (ready/result), new work raises")


def check_synthetic_actor():
    # Runs only after the Engine context above has exited (HPX runtime is a
    # process singleton; one active Engine/SyntheticActor at a time).
    from rayx import SyntheticActor
    with SyntheticActor(num_lanes=1, hpx_threads=2) as actor:
        if actor.num_lanes() != 1:
            _fail(f"actor.num_lanes() is {actor.num_lanes()}, expected 1")
        _check_row(actor.remote(service_ms=0).result(), "actor.remote noop")

        # remote_batch is part of the facade; exercise it if present.
        if hasattr(actor, "remote_batch"):
            batch = actor.remote_batch(service_ms=0, count=3)
            if len(batch) != 3:
                _fail(f"remote_batch returned {len(batch)} futures, expected 3")
            for i, fut in enumerate(batch):
                _check_row(fut.result(), f"actor.remote_batch[{i}]")

        # SyntheticActor.wait forwards to Engine.wait: conserved partition and
        # the same empty-input rejection. Drain the partition cleanly.
        wfuts = [actor.remote(service_ms=0) for _ in range(4)]
        ready, not_ready = actor.wait(wfuts, num_returns=1)
        if len(ready) + len(not_ready) != len(wfuts):
            _fail(f"actor.wait partition lost futures: {len(ready)}+"
                  f"{len(not_ready)} != {len(wfuts)}")
        if not ready:
            _fail("actor.wait(num_returns=1) returned empty ready list")
        _expect_raise(lambda: actor.wait([]), "actor.wait([])")
        # Duplicate rejection forwards through the facade too; the future stays
        # usable, so drain it cleanly afterward.
        dup = actor.remote(service_ms=0)
        _expect_raise(lambda: actor.wait([dup, dup]), "actor.wait([f, f])")
        for i, fut in enumerate(ready + not_ready + [dup]):
            _check_row(fut.result(), f"actor.wait drain[{i}]")

        # as_completed forwards to Engine.as_completed: yields each original
        # future once; caller retires with .result(). Shape-only.
        acfuts = [actor.remote(service_ms=0) for _ in range(4)]
        original = set(id(f) for f in acfuts)
        seen = []
        for fut in actor.as_completed(acfuts):
            if id(fut) not in original:
                _fail("actor.as_completed yielded an object not in the input")
            seen.append(id(fut))
            _check_row(fut.result(), f"actor.as_completed retire[{len(seen)-1}]")
        if set(seen) != original or len(seen) != len(acfuts):
            _fail(f"actor.as_completed yielded {len(seen)} of {len(acfuts)} "
                  "futures (expected each exactly once)")

        # Shutdown-lifetime contract through the actor facade: submit a future
        # before the context exits (which shuts the actor down via __exit__);
        # the drained future must still retire afterward.
        post_shutdown = actor.remote(service_ms=1)
    # Context has exited here -> actor shut down (graceful drain).
    _check_row(post_shutdown.result(), "actor post-shutdown retire")
    print("PASS: SyntheticActor remote / remote_batch / wait / as_completed / "
          "post-shutdown retire -> well-formed results")


def main():
    check_import()
    check_hpx_smoke()
    check_engine()
    check_ready_and_wait()
    check_as_completed()
    check_double_result()
    check_repr()
    check_wait_contracts()
    check_synthetic_actor()
    print("OK: rayx smoke passed")


if __name__ == "__main__":
    main()
