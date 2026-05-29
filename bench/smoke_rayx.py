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
    print("PASS: SyntheticActor remote / remote_batch -> well-formed results")


def main():
    check_import()
    check_hpx_smoke()
    check_engine()
    check_ready_and_wait()
    check_synthetic_actor()
    print("OK: rayx smoke passed")


if __name__ == "__main__":
    main()
