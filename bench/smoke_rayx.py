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
# deliberately absent (not part of the facade); `label` is the optional
# client-side annotation, always present (None when not supplied at submit).
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
    "label",
    # Chunked-service echo (always present; 1 / 0.0 when unchunked).
    "chunks",
    "chunk_delay_ms",
    # Lane-determined active chunks actually run (== chunks completed, 0 queued
    # cancel, [1, chunks-1] running cancel). Always present.
    "chunks_completed",
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
    # A normally-completed request ran every requested active chunk.
    if row["chunks_completed"] != row["chunks"]:
        _fail(f"{where}: chunks_completed {row['chunks_completed']} != chunks "
              f"{row['chunks']} for a completed row")


def _check_cancelled_row(row, where, label="__unset__"):
    # A cancelled request still retires to a well-formed row, but with
    # status="cancelled" and ~zero observed service (the lane skipped service).
    if not isinstance(row, dict):
        _fail(f"{where}: result() returned {type(row).__name__}, expected dict")
    for field in RESULT_FIELDS:
        if field not in row:
            _fail(f"{where}: result missing field {field!r} (got {sorted(row)})")
    if row["status"] != "cancelled":
        _fail(f"{where}: status is {row['status']!r}, expected 'cancelled'")
    if row["error"] is not None:
        _fail(f"{where}: error is {row['error']!r}, expected None")
    if row["end_ns"] < row["start_ns"]:
        _fail(f"{where}: end_ns < start_ns")
    if row["service_ms_observed"] < 0:
        _fail(f"{where}: service_ms_observed negative ({row['service_ms_observed']})")
    if label != "__unset__" and row["label"] != label:
        _fail(f"{where}: label is {row['label']!r}, expected {label!r}")
    # Cancelled rows are a strictly-partial run: 0 (queued) up to chunks-1
    # (running). It is never a full run (that would be a completed row).
    cc = row["chunks_completed"]
    if not (0 <= cc < row["chunks"]):
        _fail(f"{where}: chunks_completed {cc} not in [0, {row['chunks']}) "
              f"for a cancelled row")


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


def check_get():
    # Engine.get(...) retires one Future or a list and returns measurement
    # row(s): single Future -> row dict, list -> list of rows in input order.
    # It consumes each Future once (a second get/result raises). Shape-only.
    import time
    from rayx import Engine
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        # Single Future -> one row dict.
        f = engine.submit(service_ms=0)
        row = engine.get(f)
        if isinstance(row, list):
            _fail("engine.get(single Future) returned a list, expected a dict")
        _check_row(row, "engine.get single")
        # Consumed once: a second get / result on the same Future must raise.
        _expect_raise(lambda: engine.get(f), "second engine.get on retired future")
        _expect_raise(lambda: f.result(), "result() after engine.get")

        # List -> list of rows in input order, same count.
        futs = [engine.submit(service_ms=0) for _ in range(4)]
        rows = engine.get(futs)
        if not isinstance(rows, list):
            _fail(f"engine.get(list) returned {type(rows).__name__}, expected list")
        if len(rows) != len(futs):
            _fail(f"engine.get(list) returned {len(rows)} rows, expected "
                  f"{len(futs)}")
        for i, r in enumerate(rows):
            _check_row(r, f"engine.get list[{i}]")
        # Every Future in the list is now retired: a second result() raises.
        for i, fut in enumerate(futs):
            _expect_raise(lambda fut=fut: fut.result(),
                          f"result() after engine.get list[{i}]")

        # Shared recv_ns across a small ready list (ergonomic override). Wait
        # until all are ready so the supplied recv_ns is valid for each, then
        # get the SAME (still-unretired) futures with that one timestamp.
        batch = [engine.submit(service_ms=0) for _ in range(3)]
        inflight = list(batch)
        while inflight:
            _ready, inflight = engine.wait(inflight, num_returns=1)
        recv_ns = time.perf_counter_ns()
        rows = engine.get(batch, recv_ns=recv_ns)
        if len(rows) != len(batch):
            _fail(f"engine.get(list, recv_ns) returned {len(rows)} rows, "
                  f"expected {len(batch)}")
        for i, r in enumerate(rows):
            _check_row(r, f"engine.get shared-recv_ns[{i}]")
    print("PASS: Engine.get (single -> row; list -> rows in input order; "
          "consumes once; recv_ns override) -> well-formed")


def check_label():
    # Optional client-side `label` round-trips through result()["label"] on the
    # single-request submit paths; omitted -> None; bad type -> TypeError; batch
    # paths reject it. Label is facade-only metadata (never crosses C++, not in
    # benchmark JSONL). Shape-only.
    from rayx import Engine, SyntheticActor
    with Engine(num_lanes=1, hpx_threads=2) as engine:
        # Supplied label echoes back through result().
        row = engine.submit(service_ms=0, label="req-a").result()
        if row["label"] != "req-a":
            _fail(f"engine.submit label not echoed: {row['label']!r}")
        # Omitted label -> None.
        row = engine.submit(service_ms=0).result()
        if row["label"] is not None:
            _fail(f"omitted label should be None, got {row['label']!r}")
        # Survives through Engine.get (single).
        grow = engine.get(engine.submit(service_ms=0, label="g"))
        if grow["label"] != "g":
            _fail(f"label lost through engine.get: {grow['label']!r}")
        # Survives through wait (same Future object is the one retired).
        wf = engine.submit(service_ms=0, label="w")
        ready, _nr = engine.wait([wf], num_returns=1)
        if ready[0].result()["label"] != "w":
            _fail("label lost through wait")
        # Survives through as_completed (yields the original Future).
        af = engine.submit(service_ms=0, label="ac")
        seen = None
        for f in engine.as_completed([af]):
            seen = f.result()["label"]
        if seen != "ac":
            _fail(f"label lost through as_completed: {seen!r}")
        # Bad label type raises TypeError (validated before the C++ crossing).
        _expect_raise(lambda: engine.submit(service_ms=0, label=123),
                      "engine.submit(label=int)")
        # Batch paths do NOT accept label in v1 (unexpected keyword -> raises).
        _expect_raise(lambda: engine.submit_batch(service_ms=0, count=1, label="x"),
                      "engine.submit_batch(label=...)")
    # Actor facade (own context; HPX runtime is a process singleton).
    with SyntheticActor(num_lanes=1, hpx_threads=2) as actor:
        if actor.remote(service_ms=0, label="r").result()["label"] != "r":
            _fail("actor.remote label not echoed")
        if actor.serve.remote(service_ms=0, label="s").result()["label"] != "s":
            _fail("actor.serve.remote label not echoed")
        _expect_raise(lambda: actor.remote(service_ms=0, label=1.5),
                      "actor.remote(label=float)")
        _expect_raise(lambda: actor.remote_batch(service_ms=0, count=1, label="x"),
                      "actor.remote_batch(label=...)")
        _expect_raise(
            lambda: actor.serve.remote_batch(service_ms=0, count=1, label="x"),
            "actor.serve.remote_batch(label=...)")
    print("PASS: label (single-submit round-trip via result/get/wait/"
          "as_completed; omitted -> None; bad type -> TypeError; batch rejects "
          "label) -> ok")


def check_work_mode():
    # work_mode selects the synthetic service-time shape: "sleep" (default; parks
    # the lane) vs "spin" (CPU-bound; busy on-core). Both return the SAME row
    # shape -- work_mode is an execution input and is NOT echoed in the row.
    # Validated at the Python boundary (unsupported / non-str rejected before the
    # C++ crossing) on every funnel path. Shape-only: NO timing thresholds (the
    # sleep/spin duration is host-dependent).
    from rayx import Engine, SyntheticActor
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        # Default stays "sleep" (existing behavior preserved).
        _check_row(engine.submit(service_ms=0).result(), "default work_mode")
        # spin no-op + tiny spin -> well-formed completed rows (no timing assert).
        _check_row(engine.submit(service_ms=0, work_mode="spin").result(),
                   "engine.submit spin noop")
        spin_row = engine.submit(service_ms=1, work_mode="spin").result()
        _check_row(spin_row, "engine.submit spin tiny")
        # Row schema unchanged: there is NO work_mode field in the result row.
        if "work_mode" in spin_row:
            _fail("result row carries a work_mode field (schema should be "
                  f"unchanged); got {sorted(spin_row)}")
        # spin through the batch path (batch accepts work_mode like submit).
        sbatch = engine.submit_batch(service_ms=0, count=3, work_mode="spin")
        if len(sbatch) != 3:
            _fail(f"submit_batch(spin) returned {len(sbatch)}, expected 3")
        for i, fut in enumerate(sbatch):
            _check_row(fut.result(), f"engine.submit_batch spin[{i}]")
        # Unsupported mode -> ValueError; non-str -> TypeError. Rejected before
        # the C++ crossing on both the single and batch paths.
        _expect_raise(lambda: engine.submit(service_ms=0, work_mode="bogus"),
                      "engine.submit(work_mode='bogus')")
        _expect_raise(lambda: engine.submit(service_ms=0, work_mode=123),
                      "engine.submit(work_mode=int)")
        _expect_raise(
            lambda: engine.submit_batch(service_ms=0, count=1, work_mode="nope"),
            "engine.submit_batch(work_mode='nope')")
    # Actor facade forwards work_mode through to the same validated funnel
    # (remote / serve.remote / serve.remote_batch).
    with SyntheticActor(num_lanes=1, hpx_threads=2) as actor:
        _check_row(actor.remote(service_ms=0, work_mode="spin").result(),
                   "actor.remote spin")
        _check_row(actor.serve.remote(service_ms=0, work_mode="spin").result(),
                   "actor.serve.remote spin")
        sb = actor.serve.remote_batch(service_ms=0, count=2, work_mode="spin")
        if len(sb) != 2:
            _fail(f"serve.remote_batch(spin) returned {len(sb)}, expected 2")
        for i, fut in enumerate(sb):
            _check_row(fut.result(), f"actor.serve.remote_batch spin[{i}]")
        _expect_raise(lambda: actor.remote(service_ms=0, work_mode="x"),
                      "actor.remote(work_mode='x')")
        _expect_raise(lambda: actor.serve.remote(service_ms=0, work_mode="x"),
                      "actor.serve.remote(work_mode='x')")
    print("PASS: work_mode 'spin' (single / batch / actor / serve; default stays "
          "sleep; row schema unchanged; unsupported -> ValueError, non-str -> "
          "TypeError) -> ok")


def check_varied_batch():
    # Variable service time: submit_batch accepts a list/tuple of per-request
    # service times (one request per element, input order preserved, count
    # inferred). Single-request submit stays scalar-only. Result-row schema is
    # unchanged. Shape/order/error only -- no timing thresholds (order is checked
    # via spin's deterministic, well-separated service_ms_observed).
    from rayx import Engine, SyntheticActor
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        # Varied list -> one future per element, in input order; well-formed rows.
        # Values are well separated and NON-monotonic so the order check below
        # proves the C++ varied batch maps each service time to the right request
        # without depending on exact-millisecond fidelity.
        req = [2, 16, 8]
        rows = [f.result() for f in
                engine.submit_batch(service_ms=req, work_mode="spin")]
        if len(rows) != 3:
            _fail(f"varied submit_batch returned {len(rows)} rows, expected 3")
        for i, r in enumerate(rows):
            _check_row(r, f"varied submit_batch[{i}]")
        # Order/mapping preserved: the RANK ordering of observed service times must
        # match the rank ordering of the requested list (relative order, not exact
        # ms -- NO timing threshold). This is robust to sleep/spin overshoot, which
        # inflates observed service under CI core contention (the documented spin
        # core-boundary / sleep-timer effect); with well-separated values request
        # i's observed service still ranks where its own req[i] does.
        def _order(xs):
            return sorted(range(len(xs)), key=lambda i: xs[i])
        observed = [r["service_ms_observed"] for r in rows]
        if _order(observed) != _order(req):
            _fail(f"varied batch order/mapping mismatch: observed {observed} "
                  f"orders {_order(observed)} != requested {req} "
                  f"orders {_order(req)}")
        # Tuple also accepted.
        if len(engine.submit_batch(service_ms=(1, 1), work_mode="spin")) != 2:
            _fail("tuple service_ms did not yield 2 futures")
        # count matching len is accepted; mismatch is rejected.
        if len(engine.submit_batch(service_ms=[1, 1, 1], count=3)) != 3:
            _fail("submit_batch(list, count==len) did not yield len futures")
        _expect_raise(lambda: engine.submit_batch(service_ms=[1, 2, 3], count=2),
                      "submit_batch(list, count != len)")
        # Row schema unchanged: no work_mode / service_ms_requested field added.
        vr = engine.submit_batch(service_ms=[3], work_mode="spin")[0].result()
        for absent in ("work_mode", "service_ms_requested"):
            if absent in vr:
                _fail(f"varied batch row unexpectedly carries {absent!r} "
                      f"(schema should be unchanged); got {sorted(vr)}")
        # Bad varied inputs are rejected at the Python boundary (before C++).
        _expect_raise(lambda: engine.submit_batch(service_ms=[]),
                      "submit_batch(empty list)")
        _expect_raise(lambda: engine.submit_batch(service_ms=[1, -2]),
                      "submit_batch(negative element)")
        _expect_raise(lambda: engine.submit_batch(service_ms=[1, 0]),
                      "submit_batch(zero element)")
        _expect_raise(lambda: engine.submit_batch(service_ms=[1, float("nan")]),
                      "submit_batch(NaN element)")
        _expect_raise(lambda: engine.submit_batch(service_ms=[1, float("inf")]),
                      "submit_batch(inf element)")
        _expect_raise(lambda: engine.submit_batch(service_ms=[1, "x"]),
                      "submit_batch(non-numeric element)")
        _expect_raise(lambda: engine.submit_batch(service_ms=[True, 1]),
                      "submit_batch(bool element)")
        # Scalar form unchanged: default count -> 1; explicit count honored.
        if len(engine.submit_batch(service_ms=1, work_mode="spin")) != 1:
            _fail("scalar submit_batch default count != 1 request")
        if len(engine.submit_batch(service_ms=0, count=4)) != 4:
            _fail("scalar submit_batch(count=4) != 4 requests")
        # Single-request submit stays scalar-only: a list is not a valid service.
        _expect_raise(lambda: engine.submit(service_ms=[1, 2]),
                      "submit(list) should be rejected (scalar-only)")
    # Varied form is inherited by the actor + serve batch facades.
    with SyntheticActor(num_lanes=2, hpx_threads=2) as actor:
        if len(actor.remote_batch(service_ms=[1, 2, 3], work_mode="spin")) != 3:
            _fail("actor.remote_batch(list) did not yield 3 futures")
        sb = actor.serve.remote_batch(service_ms=[1, 2], work_mode="spin")
        if len(sb) != 2:
            _fail("actor.serve.remote_batch(list) did not yield 2 futures")
        for i, fut in enumerate(sb):
            _check_row(fut.result(), f"serve.remote_batch varied[{i}]")
        _expect_raise(lambda: actor.remote_batch(service_ms=[1, 2], count=5),
                      "actor.remote_batch(list, count != len)")
    print("PASS: varied submit_batch (list/tuple per-request service; order "
          "preserved; count inferred/checked; scalar + single-submit unchanged; "
          "schema unchanged; bad input rejected; actor/serve inherit) -> ok")


def check_cancel():
    # Queued-only cancellation: engine.cancel(future) succeeds ONLY if the
    # request is still queued. We create congestion with a single lane and one
    # long-running occupier so the requests submitted behind it are definitely
    # queued; cancelling those returns True and they retire as status="cancelled"
    # (preserving label). A request the lane has already serviced cancels False.
    # Structural only: durations create queue ordering, never timing assertions.
    # The occupier uses spin so it definitely holds the lane on-core; victims are
    # cancelled (so they never run) and the occupier is drained at the end.
    from rayx import Engine, SyntheticActor
    OCC = 120  # occupier service_ms: >> the microseconds to call cancel()

    with Engine(num_lanes=1, hpx_threads=2) as engine:
        # (1) Cancel a queued request succeeds.
        occ = engine.submit(service_ms=OCC, work_mode="spin")  # holds the lane
        victim = engine.submit(service_ms=5, label="v")        # queued behind it
        if engine.cancel(victim) is not True:
            _fail("cancel(queued victim) did not return True")
        if victim.cancelled() is not True:
            _fail("victim.cancelled() not True after a successful cancel")
        # Cancel fulfilled the promise immediately: the victim is ready now.
        if victim.ready() is not True:
            _fail("cancelled victim is not ready()")
        row = victim.result()
        _check_cancelled_row(row, "cancel queued victim", label="v")
        # consume-once still holds for a cancelled future.
        _expect_raise(lambda: victim.result(), "second result() on cancelled future")
        # cancel after retire raises.
        _expect_raise(lambda: engine.cancel(victim), "cancel after result()")

        # (2) Cancel a request the lane already serviced fails (False).
        occ.result()  # drain the occupier so the lane is idle
        done = engine.submit(service_ms=0)
        # Wait until it is ready (= serviced) without retiring it.
        for _ in range(100000):
            if done.ready():
                break
        if not done.ready():
            _fail("noop future never became ready")
        if engine.cancel(done) is not False:
            _fail("cancel(already-serviced future) did not return False")
        if done.cancelled() is not False:
            _fail("served future reports cancelled() True")
        _check_row(done.result(), "post-service cancel -> completed")

        # (3) A cancelled future flows through wait / as_completed / get.
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        vs = [engine.submit(service_ms=5, label=f"f{i}") for i in range(3)]
        for v in vs:
            if engine.cancel(v) is not True:
                _fail("cancel(queued victim, flow) did not return True")
        # wait: the cancelled futures are ready immediately.
        ready, _nr = engine.wait(vs, num_returns=1)
        if not ready:
            _fail("wait returned no ready cancelled futures")
        # get a list of the cancelled futures -> cancelled rows in input order.
        rows = engine.get(vs)
        for i, r in enumerate(rows):
            _check_cancelled_row(r, f"cancelled via get[{i}]", label=f"f{i}")
        occ.result()  # drain occupier
        # as_completed yields the cancelled futures (original objects).
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        avs = [engine.submit(service_ms=5, label=f"a{i}") for i in range(2)]
        for v in avs:
            engine.cancel(v)
        seen = 0
        for fut in engine.as_completed(avs):
            _check_cancelled_row(fut.result(), f"cancelled via as_completed[{seen}]")
            seen += 1
        if seen != len(avs):
            _fail(f"as_completed yielded {seen} cancelled futures, expected "
                  f"{len(avs)}")
        occ.result()

        # (4) Cancelling one future does not affect unrelated futures.
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        target = engine.submit(service_ms=5, label="target")
        bystander = engine.submit(service_ms=5, label="bystander")
        if engine.cancel(target) is not True:
            _fail("cancel(target) did not return True")
        if bystander.cancelled() is not False:
            _fail("unrelated bystander reports cancelled() True")
        _check_cancelled_row(target.result(), "isolated cancel target", label="target")
        occ.result()                       # drain occupier so bystander runs
        _check_row(bystander.result(), "bystander after unrelated cancel")

        # (5) Non-cancelable (batch) future raises on cancel; cancelled() False.
        bf = engine.submit_batch(service_ms=0, count=1)[0]
        _expect_raise(lambda: engine.cancel(bf), "cancel(batch future)")
        if bf.cancelled() is not False:
            _fail("batch future reports cancelled() True")
        bf.result()

    # (6) Actor forwarding: actor.cancel works for a queued serve.remote request.
    with SyntheticActor(num_lanes=1, hpx_threads=2) as actor:
        occ = actor.remote(service_ms=OCC, work_mode="spin")
        v = actor.serve.remote(service_ms=5, label="s")
        if actor.cancel(v) is not True:
            _fail("actor.cancel(queued serve future) did not return True")
        if v.cancelled() is not True:
            _fail("actor serve victim.cancelled() not True")
        _check_cancelled_row(v.result(), "actor.cancel serve victim", label="s")
        occ.result()

    # (7) Cancel after shutdown raises (own engine + explicit shutdown).
    engine2 = Engine(num_lanes=1, hpx_threads=2)
    f = engine2.submit(service_ms=0)
    engine2.shutdown()  # graceful drain; f stays valid/retirable
    _expect_raise(lambda: engine2.cancel(f), "cancel after shutdown")
    _check_row(f.result(), "post-shutdown drained retire (after cancel raise)")

    print("PASS: cancel (queued -> True + status=cancelled + label; served -> "
          "False; flows via wait/as_completed/get; actor forwards; batch/"
          "retired/post-shutdown raise; unrelated futures unaffected) -> ok")


def check_chunks():
    # Chunked synthetic service (v1 streaming): chunks splits TOTAL active
    # service_ms into chunks equal active steps; chunk_delay_ms is a PARKED gap
    # between chunks. One request -> one future -> one final row (no per-chunk
    # rows/events). Default chunks=1/delay=0 is unchanged. With chunk_delay_ms>0,
    # service_ms_observed is lifecycle/lane-occupancy time (active + parked gaps),
    # NOT active-only. Shape-only: NO timing thresholds (durations are
    # host-dependent and the parked gap carries the sleep-timer overshoot).
    from rayx import Engine, SyntheticActor
    with Engine(num_lanes=1, hpx_threads=2) as engine:
        # (1) Default path unchanged: row echoes chunks==1, chunk_delay_ms==0.
        row = engine.submit(service_ms=1, work_mode="spin").result()
        _check_row(row, "default chunks")
        if row["chunks"] != 1 or row["chunk_delay_ms"] != 0.0:
            _fail(f"default chunks/delay not 1/0.0: {row['chunks']}/"
                  f"{row['chunk_delay_ms']}")

        # (2) Chunked single request: one completed row, label preserved, echoes.
        crow = engine.submit(service_ms=4, chunks=4, chunk_delay_ms=0,
                             work_mode="spin", label="chk").result()
        _check_row(crow, "chunked spin (4x, no delay)")
        if crow["chunks"] != 4:
            _fail(f"chunked row chunks {crow['chunks']} != 4")
        if crow["chunk_delay_ms"] != 0.0:
            _fail(f"chunked row delay {crow['chunk_delay_ms']} != 0.0")
        if crow["label"] != "chk":
            _fail(f"chunked row label {crow['label']!r} != 'chk'")

        # (3) Chunk delay: structural only -- completed row, echoes the delay,
        # non-negative observed lifecycle (no fragile timing threshold).
        drow = engine.submit(service_ms=4, chunks=4, chunk_delay_ms=1,
                             work_mode="spin").result()
        _check_row(drow, "chunked spin with delay")
        if drow["chunk_delay_ms"] != 1.0:
            _fail(f"delayed chunk row delay {drow['chunk_delay_ms']} != 1.0")
        if drow["service_ms_observed"] < 0:
            _fail("delayed chunk row service_ms_observed negative")

        # (6) Invalid inputs rejected at the Python boundary (before C++).
        for bad in ({"chunks": 0}, {"chunks": -1}, {"chunks": 2.0},
                    {"chunks": True}, {"chunks": "x"},
                    {"chunk_delay_ms": -1}, {"chunk_delay_ms": float("nan")},
                    {"chunk_delay_ms": float("inf")},
                    {"chunk_delay_ms": True}, {"chunk_delay_ms": "x"}):
            _expect_raise(lambda b=bad: engine.submit(service_ms=1, **b),
                          f"engine.submit({bad})")
        # Batch APIs are UNCHUNKED in v1: chunk kwargs are rejected (TypeError
        # via the unexpected keyword, since batch signatures take no chunks).
        _expect_raise(
            lambda: engine.submit_batch(service_ms=1, count=1, chunks=2),
            "submit_batch(chunks=...)")
        _expect_raise(
            lambda: engine.submit_batch(service_ms=1, count=1, chunk_delay_ms=1),
            "submit_batch(chunk_delay_ms=...)")

        # (5) Cancellation interaction: a queued chunked request cancels before
        # start; the row is status="cancelled" and still echoes chunks/delay.
        occ = engine.submit(service_ms=120, work_mode="spin")  # holds the lane
        victim = engine.submit(service_ms=8, chunks=4, chunk_delay_ms=1,
                               work_mode="spin", label="cv")
        if engine.cancel(victim) is not True:
            _fail("cancel(queued chunked victim) did not return True")
        crow = victim.result()
        _check_cancelled_row(crow, "cancelled chunked request", label="cv")
        if crow["chunks"] != 4 or crow["chunk_delay_ms"] != 1.0:
            _fail(f"cancelled chunked row lost chunks/delay: {crow['chunks']}/"
                  f"{crow['chunk_delay_ms']}")
        occ.result()  # drain occupier

    # (4) Actor and serve forwarding carry chunks/chunk_delay_ms through.
    with SyntheticActor(num_lanes=1, hpx_threads=2) as actor:
        arow = actor.remote(service_ms=4, chunks=2, work_mode="spin").result()
        _check_row(arow, "actor.remote chunked")
        if arow["chunks"] != 2:
            _fail(f"actor.remote chunked chunks {arow['chunks']} != 2")
        srow = actor.serve.remote(service_ms=4, chunks=2, chunk_delay_ms=0,
                                  work_mode="spin").result()
        _check_row(srow, "actor.serve.remote chunked")
        if srow["chunks"] != 2:
            _fail(f"serve.remote chunked chunks {srow['chunks']} != 2")
        # serve batch + actor batch remain unchunked: reject chunk kwargs.
        _expect_raise(
            lambda: actor.remote_batch(service_ms=1, count=1, chunks=2),
            "actor.remote_batch(chunks=...)")
        _expect_raise(
            lambda: actor.serve.remote_batch(service_ms=1, count=1, chunks=2),
            "actor.serve.remote_batch(chunks=...)")
    print("PASS: chunks (default 1/0 unchanged; chunked single req -> one row "
          "echoing chunks/delay + label; delay structural; actor/serve forward; "
          "queued cancel preserves chunks/delay; bad input + batch chunk kwargs "
          "rejected) -> ok")


def check_running_cancel():
    # Running (chunk-boundary) cancellation: a chunked request that has STARTED
    # but still has a future chunk boundary can be stopped early. cancel() returns
    # True ("guaranteed to stop at the next boundary", NOT ready-now); the request
    # retires status="cancelled" with a STRICTLY-PARTIAL run
    # (1 <= chunks_completed < chunks), labels preserved, consumed once. An active
    # chunk is never interrupted mid-flight; a started single-chunk request has no
    # boundary and cancels False (completes normally). Structural + RANGE checks
    # only -- no exact chunks_completed and no timing thresholds (durations are
    # host-dependent). Robustness: running-cancellability is armed at start
    # (begin_service), BEFORE the first boundary, so a cancel anywhere from start
    # up to the final-chunk boundary settles True regardless of host speed; the
    # only timing requirement is that the request has started (the 50ms wait, vs
    # microsecond lane pickup) and is not yet on its final chunk (cancel at ~50ms
    # of a 240ms/6-chunk request -> far from the ~200ms final-chunk boundary).
    import time
    from rayx import Engine
    with Engine(num_lanes=1, hpx_threads=2) as engine:
        # (1) Running cancel of a started chunked request with boundaries ahead.
        f = engine.submit(service_ms=240, work_mode="sleep", chunks=6,
                          chunk_delay_ms=0, label="rc")
        time.sleep(0.05)  # started; many boundaries remain
        if engine.cancel(f) is not True:
            _fail("cancel(running chunked, boundaries ahead) did not return True")
        # The outcome can settle (StopRequested) before the cancelled row is ready.
        if f.cancelled() is not True:
            _fail("running-cancelled future.cancelled() not True after cancel")
        row = f.result()
        _check_cancelled_row(row, "running chunk-boundary cancel", label="rc")
        # Partial run: at least one chunk ran (distinguishes from a queued skip),
        # strictly fewer than requested. (_check_cancelled_row already bounds it;
        # this pins the >= 1 lower edge that makes it a RUNNING, not queued, stop.)
        if row["chunks_completed"] < 1:
            _fail(f"running cancel chunks_completed {row['chunks_completed']} < 1 "
                  "(should be a partial run, not a queued skip)")
        # cancel() is not a retire: consume-once still holds for a running cancel.
        _expect_raise(lambda: f.result(), "second result() on running-cancelled")

        # (2) Started single-chunk request: no boundary exists, so a running
        # cancel is never settled -> False, and the request completes normally.
        s1 = engine.submit(service_ms=120, work_mode="sleep", chunks=1)
        time.sleep(0.05)
        if engine.cancel(s1) is not False:
            _fail("cancel(started chunks=1) did not return False")
        if s1.cancelled() is not False:
            _fail("started chunks=1 reports cancelled() True")
        _check_row(s1.result(), "started chunks=1 completes after cancel False")

        # (3) A running cancel flows through Engine.get (retire path), not just
        # Future.result -- same cancelled row, partial-run range holds.
        g = engine.submit(service_ms=240, work_mode="sleep", chunks=6, label="rcg")
        time.sleep(0.05)
        if engine.cancel(g) is not True:
            _fail("cancel(running chunked, get flow) did not return True")
        grow = engine.get(g)
        _check_cancelled_row(grow, "running cancel via get", label="rcg")
        if not (1 <= grow["chunks_completed"] < grow["chunks"]):
            _fail("running cancel via get chunks_completed not in [1, chunks)")
    print("PASS: running cancel (started chunked w/ boundary -> True + "
          "cancelled() + status=cancelled + 1<=chunks_completed<chunks + label "
          "+ consume-once; started chunks=1 -> False completes; flows via get) "
          "-> ok")


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


def check_bounded_wait():
    # Bounded / non-blocking wait: Engine.wait(..., timeout=<seconds>).
    #   timeout=None -> block (existing behaviour, unchanged).
    #   timeout=0    -> non-blocking poll: partition into (ready_now, pending_now),
    #                   NON-consuming, same num_returns/dup/type/retired/empty
    #                   guards as the blocking path. A cancelled future is ready
    #                   only once its row exists.
    #   timeout>0    -> NotImplementedError (no non-consuming timed multi-future
    #                   wait on HPX v1.11.0).
    # Structural only: congestion creates queue ordering; the one place a timing
    # margin is relied on (running-cancel pending-before-boundary) uses a wide
    # boundary gap, matching check_running_cancel's existing timing model.
    import time
    from rayx import Engine, SyntheticActor
    OCC = 120  # spin occupier service_ms >> the microseconds to poll

    with Engine(num_lanes=1, hpx_threads=2) as engine:
        # (1) timeout=0 returns immediately; a queued future is pending, not
        # consumed (we then cancel + retire it to prove it stayed usable).
        occ = engine.submit(service_ms=OCC, work_mode="spin")  # holds the lane
        q = engine.submit(service_ms=5, work_mode="spin", label="q")  # queued
        ready, pending = engine.wait([q], num_returns=1, timeout=0)
        if len(ready) + len(pending) != 1:
            _fail(f"poll partition lost futures: {len(ready)}+{len(pending)} != 1")
        if q not in pending or ready:
            _fail("timeout=0 poll: queued future should be pending, ready empty")
        if engine.cancel(q) is not True:
            _fail("queued future not cancelable after a timeout=0 poll (consumed?)")
        _check_cancelled_row(q.result(), "poll-pending then cancel", label="q")
        occ.result()  # drain occupier

        # (2) A completed future appears in ready, and the poll does NOT consume it.
        d = engine.submit(service_ms=0)
        engine.wait([d], num_returns=1)            # block until ready (timeout=None)
        ready, pending = engine.wait([d], num_returns=1, timeout=0)
        if d not in ready or pending:
            _fail("timeout=0 poll: completed future should be ready, pending empty")
        # (3) get/result still consumes once AFTER the poll (poll left it intact).
        _check_row(d.result(), "poll-ready then retire")
        _expect_raise(lambda: d.result(), "second result() after poll+retire")

        # (4) A cancelled QUEUED future is ready in a timeout=0 poll (row exists).
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        v = engine.submit(service_ms=5, work_mode="spin", label="cv")
        if engine.cancel(v) is not True:
            _fail("cancel(queued) did not return True")
        ready, pending = engine.wait([v], num_returns=1, timeout=0)
        if v not in ready or pending:
            _fail("timeout=0 poll: cancelled queued future should be ready")
        _check_cancelled_row(v.result(), "poll cancelled-queued ready", label="cv")
        occ.result()

        # (5) Running chunk-boundary cancel: stop requested, but the row is NOT
        # ready until the boundary retires. service_ms=240/6 chunks (sleep) -> ~40ms
        # active chunks; cancel at ~50ms is mid-chunk with the next boundary ~30ms
        # away, so the immediate poll sees it pending; after a blocking wait it is
        # ready. (Same timing model as check_running_cancel.)
        rc = engine.submit(service_ms=240, work_mode="sleep", chunks=6, label="rc")
        time.sleep(0.05)                            # started; boundaries ahead
        if engine.cancel(rc) is not True:
            _fail("cancel(running chunked) did not return True")
        ready, pending = engine.wait([rc], num_returns=1, timeout=0)
        if rc not in pending or ready:
            _fail("timeout=0 poll: running-cancel (stop pending) should be pending "
                  "until the boundary retires")
        engine.wait([rc], num_returns=1)            # block until boundary retire
        ready, pending = engine.wait([rc], num_returns=1, timeout=0)
        if rc not in ready or pending:
            _fail("timeout=0 poll: running-cancel should be ready after boundary")
        row = rc.result()
        _check_cancelled_row(row, "poll running-cancel after boundary", label="rc")
        if not (1 <= row["chunks_completed"] < row["chunks"]):
            _fail("running-cancel chunks_completed not in [1, chunks) after poll")

        # (6) num_returns preserved under timeout=0: range guard raises, and the
        # poll returns ALL currently-ready (a readiness partition, not capped at
        # num_returns). Use four ready no-ops.
        futs = [engine.submit(service_ms=0) for _ in range(4)]
        inflight = list(futs)
        while inflight:                             # ensure all are ready
            _r, inflight = engine.wait(inflight, num_returns=1)
        ready, pending = engine.wait(futs, num_returns=2, timeout=0)
        if len(ready) != 4 or pending:
            _fail(f"timeout=0 poll expected all 4 ready, got {len(ready)} ready / "
                  f"{len(pending)} pending")
        _expect_raise(lambda: engine.wait(futs, num_returns=0, timeout=0),
                      "wait(num_returns=0, timeout=0)")
        _expect_raise(lambda: engine.wait(futs, num_returns=len(futs) + 1, timeout=0),
                      "wait(num_returns>len, timeout=0)")
        for i, f in enumerate(futs):                # guards did not consume futs
            _check_row(f.result(), f"poll num_returns-guard drain[{i}]")

        # (7) Existing blocking wait (timeout=None) unchanged: still partitions.
        b = [engine.submit(service_ms=0) for _ in range(4)]
        ready, pending = engine.wait(b, num_returns=1)   # default timeout=None
        if len(ready) + len(pending) != len(b) or not ready:
            _fail("blocking wait(timeout=None) partition changed")
        for f in (*ready, *pending):
            _check_row(f.result(), "blocking-wait-unchanged drain")

        # (8) Finite-positive timeout -> NotImplementedError. (9) Invalid timeout
        # values -> clean raise. Probe with still-inflight futures and prove every
        # rejected call left them usable (validation happens before any partition).
        probe = [engine.submit(service_ms=0) for _ in range(2)]
        _expect_raise(lambda: engine.wait(probe, timeout=0.05),
                      "wait(timeout=0.05) finite-positive -> NotImplementedError")
        for bad in (-1, -0.001, "x", float("nan"), float("inf"), True):
            _expect_raise(lambda b=bad: engine.wait(probe, timeout=b),
                          f"wait(timeout={bad!r})")
        # Duplicate / wrong-type / empty still rejected on the poll path.
        _expect_raise(lambda: engine.wait([probe[0], probe[0]], timeout=0),
                      "wait([f, f], timeout=0)")
        _expect_raise(lambda: engine.wait([123], timeout=0),
                      "wait([wrong_object], timeout=0)")
        _expect_raise(lambda: engine.wait([], timeout=0), "wait([], timeout=0)")
        for i, f in enumerate(probe):               # none of the above consumed it
            _check_row(f.result(), f"poll bad-timeout probe drain[{i}]")

    # A retired future is rejected by a timeout=0 poll too (ready() raises after
    # retire), in a fresh engine so it never overlaps the singleton above.
    with Engine(num_lanes=1, hpx_threads=2) as engine:
        rf = engine.submit(service_ms=0)
        engine.wait([rf], num_returns=1)            # ensure ready
        _check_row(rf.result(), "poll-retired setup retire")
        _expect_raise(lambda: engine.wait([rf], timeout=0),
                      "wait([retired], timeout=0)")

    # Actor facade forwards timeout through to Engine.wait.
    with SyntheticActor(num_lanes=1, hpx_threads=2) as actor:
        afuts = [actor.remote(service_ms=0) for _ in range(3)]
        inflight = list(afuts)
        while inflight:
            _r, inflight = actor.wait(inflight, num_returns=1)
        ready, pending = actor.wait(afuts, num_returns=1, timeout=0)
        if len(ready) != 3 or pending:
            _fail(f"actor.wait timeout=0 poll expected 3 ready, got {len(ready)}")
        _expect_raise(lambda: actor.wait(afuts, timeout=0.05),
                      "actor.wait(timeout=0.05) -> NotImplementedError")
        for i, f in enumerate(afuts):
            _check_row(f.result(), f"actor poll drain[{i}]")

    print("PASS: bounded wait (timeout=0 non-blocking poll: ready/pending "
          "partition, non-consuming, cancelled-queued ready, running-cancel "
          "pending-until-boundary, num_returns + dup/type/retired/empty guards; "
          "timeout=None unchanged; finite>0 -> NotImplementedError; "
          "negative/NaN/inf/bool/non-numeric rejected; actor forwards) -> ok")


def _check_stat_rows(rows, where, expected_lanes):
    # Shape/type checks for a lane_stats() snapshot: a list with one well-typed
    # row per lane (actor_id str, queue_depth int >= 0, active bool).
    if not isinstance(rows, list):
        _fail(f"{where}: lane_stats returned {type(rows).__name__}, expected list")
    if len(rows) != expected_lanes:
        _fail(f"{where}: lane_stats returned {len(rows)} rows, expected "
              f"{expected_lanes}")
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            _fail(f"{where}[{i}]: stat row is {type(r).__name__}, expected dict")
        for field in ("actor_id", "queue_depth", "active"):
            if field not in r:
                _fail(f"{where}[{i}]: stat row missing {field!r} (got {sorted(r)})")
        if not isinstance(r["actor_id"], str) or not r["actor_id"]:
            _fail(f"{where}[{i}]: actor_id not a non-empty str: {r['actor_id']!r}")
        # bool is an int subclass; queue_depth must be a real int, not a bool.
        if isinstance(r["queue_depth"], bool) or not isinstance(r["queue_depth"], int):
            _fail(f"{where}[{i}]: queue_depth not int: {r['queue_depth']!r}")
        if r["queue_depth"] < 0:
            _fail(f"{where}[{i}]: queue_depth negative: {r['queue_depth']}")
        if not isinstance(r["active"], bool):
            _fail(f"{where}[{i}]: active not bool: {r['active']!r}")


def check_lane_stats():
    # lane_stats() is an observability snapshot (debugging only): one row per
    # ServiceLane in stable lane order, {actor_id, queue_depth, active}. It is
    # non-consuming, can race (snapshot), and is NOT scheduler state / placement
    # control / JSONL schema. Structural only -- bounded polling, never a timing
    # assertion (the occupier uses a long spin so it definitely holds the lane).
    from rayx import Engine, SyntheticActor
    OCC = 120  # occupier service_ms (spin): >> the microseconds to snapshot

    # (1)+(2) Fresh engine: one well-typed row per lane; idle (queue 0, inactive).
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        rows = engine.lane_stats()
        _check_stat_rows(rows, "fresh engine", expected_lanes=2)
        if any(r["active"] for r in rows) or any(r["queue_depth"] for r in rows):
            _fail(f"fresh engine lanes not idle: {rows}")
        # actor_ids are the stable lane identities reported on result rows.
        if len({r["actor_id"] for r in rows}) != 2:
            _fail(f"fresh engine lane actor_ids not distinct: {rows}")

    # (3) One long occupier + queued work behind it on a single lane: at least one
    # lane reports active and total queue depth > 0. Poll (bounded) until the lane
    # has picked up the occupier (active True) -- never a sleep/timing assert.
    with Engine(num_lanes=1, hpx_threads=2) as engine:
        occ = engine.submit(service_ms=OCC, work_mode="spin")   # holds the lane
        victims = [engine.submit(service_ms=5, work_mode="spin", label=f"q{i}")
                   for i in range(3)]                            # queued behind it
        snap = None
        for _ in range(200000):
            snap = engine.lane_stats()
            _check_stat_rows(snap, "occupied engine", expected_lanes=1)
            if snap[0]["active"]:
                break
        if not (snap and snap[0]["active"]):
            _fail("occupier never observed active in lane_stats()")
        total_queue = sum(r["queue_depth"] for r in snap)
        if total_queue <= 0:
            _fail(f"expected queued work behind the occupier, got queue_depth "
                  f"{total_queue} ({snap})")

        # (4) Drain: cancel the queued victims, retire them, drain the occupier,
        # then (bounded poll) the lane returns to idle -- all queue_depth == 0 and
        # active == False.
        for v in victims:
            engine.cancel(v)            # queued -> skipped
            v.result()                  # retire the cancelled row
        occ.result()                    # drain the occupier
        idle = None
        for _ in range(200000):
            idle = engine.lane_stats()
            if not idle[0]["active"] and idle[0]["queue_depth"] == 0:
                break
        _check_stat_rows(idle, "drained engine", expected_lanes=1)
        if idle[0]["active"] or idle[0]["queue_depth"] != 0:
            _fail(f"lane did not return to idle after draining: {idle}")

    # (5) Actor facade forwards lane_stats to the Engine.
    with SyntheticActor(num_lanes=2, hpx_threads=2) as actor:
        _check_stat_rows(actor.lane_stats(), "actor.lane_stats", expected_lanes=2)

    # (6) Post-shutdown: lanes are destroyed, so lane_stats() raises like the
    # other coordination/new-work APIs (own engine + explicit shutdown).
    engine2 = Engine(num_lanes=1, hpx_threads=2)
    engine2.shutdown()
    _expect_raise(lambda: engine2.lane_stats(), "lane_stats after shutdown")

    print("PASS: lane_stats (one well-typed row per lane; fresh idle; occupier "
          "active + queued depth>0; idle after drain; actor forwards; "
          "post-shutdown raises) -> ok")


def _expect_exc(fn, exc_type, where, *, not_subtype=None):
    # Assert fn() raises exactly exc_type (isinstance). Optionally assert the
    # raised exception is NOT an instance of not_subtype (e.g. a plain
    # RuntimeError that must not be a QueueFullError). Returns the exception.
    try:
        fn()
    except exc_type as ex:
        if not_subtype is not None and isinstance(ex, not_subtype):
            _fail(f"{where}: raised {type(ex).__name__}, must not be "
                  f"{not_subtype.__name__}")
        return ex
    except Exception as ex:  # noqa: BLE001
        _fail(f"{where}: raised {type(ex).__name__}, expected {exc_type.__name__}")
    _fail(f"{where}: expected {exc_type.__name__}, none raised")


def _poll_until(fn, where, tries=200000):
    # Bounded poll (no sleep/timing assert) until fn() is truthy; returns its
    # last value. Used to observe lane state transitions structurally.
    val = None
    for _ in range(tries):
        val = fn()
        if val:
            return val
    _fail(f"{where}: condition never became true within {tries} polls")
    return val


def check_admission_control():
    # Bounded admission (max_queue_depth_per_lane): each lane admits at most N
    # queued-but-not-started requests; submit() raises QueueFullError when the
    # round-robin target lane is full. Structural only -- a long-spin occupier
    # holds the lane's active slot so queued work cannot be popped mid-check, and
    # state transitions are observed by BOUNDED POLLING lane_stats(), never by a
    # timing assertion. Runs after the lane_stats Engine context has exited
    # (HPX runtime is a process singleton; one active engine at a time).
    from rayx import Engine, SyntheticActor, QueueFullError
    OCC = 120  # occupier service_ms (spin): >> the microseconds to fill+probe

    # (8) Constructor validation: None and positive int accepted; 0 / negative ->
    # ValueError, bool / float / str -> TypeError. Rejections raise inside
    # __init__ BEFORE the HPX runtime starts, so they leave no active engine.
    with Engine(num_lanes=1, hpx_threads=1, max_queue_depth_per_lane=None):
        pass
    with Engine(num_lanes=1, hpx_threads=1, max_queue_depth_per_lane=3):
        pass
    for bad in (0, -1):
        _expect_exc(lambda b=bad: Engine(num_lanes=1, max_queue_depth_per_lane=b),
                    ValueError, f"ctor cap={bad!r}")
    for bad in (True, 1.5, "x"):
        _expect_exc(lambda b=bad: Engine(num_lanes=1, max_queue_depth_per_lane=b),
                    TypeError, f"ctor cap={bad!r}")

    # (1) Default None unchanged: a deep backlog is fully accepted (no cap means
    # no admission check; submit never raises under load).
    with Engine(num_lanes=1, hpx_threads=2) as engine:
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        _poll_until(lambda: engine.lane_stats()[0]["active"], "default occ active")
        backlog = [engine.submit(service_ms=5, work_mode="spin") for _ in range(20)]
        if engine.lane_stats()[0]["queue_depth"] != 20:
            _fail(f"default None: expected queue_depth 20, got "
                  f"{engine.lane_stats()[0]['queue_depth']}")
        engine.get([occ, *backlog])

    # (2) Cap enforced on a single lane: occupier active, fill the queue to the
    # cap, the next submit raises QueueFullError (a RuntimeError subtype).
    with Engine(num_lanes=1, hpx_threads=2, max_queue_depth_per_lane=2) as engine:
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        _poll_until(lambda: engine.lane_stats()[0]["active"], "cap occ active")
        a = engine.submit(service_ms=5, work_mode="spin")   # queued depth 1
        b = engine.submit(service_ms=5, work_mode="spin")   # queued depth 2 (full)
        if engine.lane_stats()[0]["queue_depth"] != 2:
            _fail(f"cap: expected queue_depth 2 before reject, got "
                  f"{engine.lane_stats()[0]['queue_depth']}")
        ex = _expect_exc(lambda: engine.submit(service_ms=5, work_mode="spin"),
                         QueueFullError, "cap full submit")
        if not isinstance(ex, RuntimeError):
            _fail("QueueFullError must subclass RuntimeError")
        # The rejected submit created no Future: queue depth is still exactly 2.
        if engine.lane_stats()[0]["queue_depth"] != 2:
            _fail("rejected submit changed queue_depth (should be a no-op)")
        engine.get([occ, a, b])

    # (3) Per-lane, not global: with 2 lanes and cap 1, a full lane 0 does NOT
    # block admission to lane 1 (rr_ advances on every call, so the rotation is
    # not stalled by one full lane).
    with Engine(num_lanes=2, hpx_threads=3, max_queue_depth_per_lane=1) as engine:
        occ0 = engine.submit(service_ms=OCC, work_mode="spin")  # lane 0 active
        occ1 = engine.submit(service_ms=OCC, work_mode="spin")  # lane 1 active
        _poll_until(lambda: all(r["active"] for r in engine.lane_stats()),
                    "both lanes active")
        s0 = engine.submit(service_ms=5, work_mode="spin")   # -> lane 0, fills it
        if engine.lane_stats()[0]["queue_depth"] != 1:
            _fail("per-lane: lane 0 not full after its queued submit")
        s1 = engine.submit(service_ms=5, work_mode="spin")   # -> lane 1, accepted
        if engine.lane_stats()[1]["queue_depth"] != 1:
            _fail("per-lane: lane 1 did not accept while lane 0 was full")
        _expect_exc(lambda: engine.submit(service_ms=5, work_mode="spin"),
                    QueueFullError, "per-lane: lane 0 full again")
        engine.get([occ0, occ1, s0, s1])

    # (4) Draining reopens admission: once the lane pops the queued request (its
    # slot frees), a new submit is admitted again.
    with Engine(num_lanes=1, hpx_threads=2, max_queue_depth_per_lane=1) as engine:
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        _poll_until(lambda: engine.lane_stats()[0]["active"], "drain occ active")
        a = engine.submit(service_ms=5, work_mode="spin")    # queued (full)
        _expect_exc(lambda: engine.submit(service_ms=5, work_mode="spin"),
                    QueueFullError, "drain: full before drain")
        occ.result()                                          # lane pops `a` next
        _poll_until(lambda: engine.lane_stats()[0]["queue_depth"] == 0,
                    "drain: queue emptied after pop")
        c = engine.submit(service_ms=5, work_mode="spin")    # admitted again
        engine.get([a, c])

    # (5) Cancel does NOT free a slot until the lane pops it. A queued cancel
    # settles the future but leaves the item in the deque (experiment 17), so the
    # cap is still saturated and a further submit still rejects.
    with Engine(num_lanes=1, hpx_threads=2, max_queue_depth_per_lane=1) as engine:
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        _poll_until(lambda: engine.lane_stats()[0]["active"], "cancel occ active")
        a = engine.submit(service_ms=5, work_mode="spin")    # queued (full)
        if engine.cancel(a) is not True:
            _fail("cancel: expected queued cancel to win (True)")
        if not a.cancelled():
            _fail("cancel: future not marked cancelled")
        # cancel did not dequeue: depth still 1, so admission still rejects.
        if engine.lane_stats()[0]["queue_depth"] != 1:
            _fail("cancel: queue_depth dropped on cancel (should count until pop)")
        _expect_exc(lambda: engine.submit(service_ms=5, work_mode="spin"),
                    QueueFullError, "cancel: still full after cancel (not popped)")
        occ.result()                                          # lane pops + skips a
        a.result()                                            # retire cancelled row

    # (6) Batch under a cap refuses loudly with a plain RuntimeError (NOT a
    # QueueFullError -- no lane is "full"; the op is unsupported under a cap).
    with Engine(num_lanes=1, hpx_threads=2, max_queue_depth_per_lane=2) as engine:
        _expect_exc(lambda: engine.submit_batch(service_ms=5, count=3),
                    RuntimeError, "batch under cap", not_subtype=QueueFullError)
        _expect_exc(lambda: engine.submit_batch(service_ms=[1, 2, 3]),
                    RuntimeError, "varied batch under cap",
                    not_subtype=QueueFullError)
    # Batch WITHOUT a cap is unchanged: returns one Future per request.
    with Engine(num_lanes=2, hpx_threads=2) as engine:
        rows = engine.get(engine.submit_batch(service_ms=1, count=6))
        if len(rows) != 6:
            _fail(f"uncapped batch returned {len(rows)} rows, expected 6")

    # (7) Actor facade forwards the cap: remote() raises QueueFullError when full;
    # remote_batch() refuses under the cap.
    with SyntheticActor(num_lanes=1, hpx_threads=2,
                        max_queue_depth_per_lane=1) as actor:
        occ = actor.remote(service_ms=OCC, work_mode="spin")
        _poll_until(lambda: actor.lane_stats()[0]["active"], "actor occ active")
        a = actor.remote(service_ms=5, work_mode="spin")     # queued (full)
        _expect_exc(lambda: actor.remote(service_ms=5, work_mode="spin"),
                    QueueFullError, "actor.remote full")
        _expect_exc(lambda: actor.remote_batch(service_ms=5, count=3),
                    RuntimeError, "actor.remote_batch under cap",
                    not_subtype=QueueFullError)
        actor.get([occ, a])

    # (9) Shutdown precedence: after shutdown the running-engine guard fires
    # first, so submit raises the "shut down" RuntimeError, NOT QueueFullError.
    engine2 = Engine(num_lanes=1, hpx_threads=2, max_queue_depth_per_lane=1)
    engine2.shutdown()
    ex = _expect_exc(lambda: engine2.submit(service_ms=5), RuntimeError,
                     "submit after shutdown (capped)", not_subtype=QueueFullError)
    if "shut down" not in str(ex):
        _fail(f"post-shutdown submit message unexpected: {ex!r}")

    print("PASS: admission control (ctor validation; default None unchanged; cap "
          "enforced -> QueueFullError; per-lane not global; drain reopens; cancel "
          "does not free a slot until popped; batch refuses under cap; actor "
          "forwards; shutdown precedence) -> ok")


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

        # SyntheticActor.get forwards to Engine.get: single -> row dict, list ->
        # rows in input order, consumes once. Shape-only.
        gfut = actor.remote(service_ms=0)
        grow = actor.get(gfut)
        if isinstance(grow, list):
            _fail("actor.get(single Future) returned a list, expected a dict")
        _check_row(grow, "actor.get single")
        _expect_raise(lambda: actor.get(gfut), "second actor.get on retired future")
        gfuts = [actor.remote(service_ms=0) for _ in range(3)]
        grows = actor.get(gfuts)
        if len(grows) != len(gfuts):
            _fail(f"actor.get(list) returned {len(grows)} rows, expected "
                  f"{len(gfuts)}")
        for i, r in enumerate(grows):
            _check_row(r, f"actor.get list[{i}]")

        # Method-style façade: actor.serve is the ONE named synthetic operation,
        # sugar over actor.remote. serve.remote -> Future, serve.remote_batch ->
        # list[Future], same objects/semantics as the primary API (consume-once,
        # get/result). Shape-only.
        srow = actor.get(actor.serve.remote(service_ms=0))
        _check_row(srow, "actor.serve.remote")
        sbatch = actor.serve.remote_batch(service_ms=0, count=3)
        if len(sbatch) != 3:
            _fail(f"actor.serve.remote_batch returned {len(sbatch)}, expected 3")
        for i, fut in enumerate(sbatch):
            _check_row(fut.result(), f"actor.serve.remote_batch[{i}]")
        # Consume-once is inherited: retire via serve, then a second result/get
        # on the same Future raises through the existing guard.
        sfut = actor.serve.remote(service_ms=0)
        _check_row(sfut.result(), "actor.serve consume-once first retire")
        _expect_raise(lambda: sfut.result(), "second result() on serve future")
        _expect_raise(lambda: actor.get(sfut), "actor.get on retired serve future")
        # Scope honesty: exactly ONE method exists. Any other dotted access
        # raises AttributeError (no arbitrary actor-method dispatch).
        _expect_raise(lambda: actor.does_not_exist, "actor.does_not_exist")

        # Shutdown-lifetime contract through the actor facade: submit a future
        # before the context exits (which shuts the actor down via __exit__);
        # the drained future must still retire afterward.
        post_shutdown = actor.remote(service_ms=1)
    # Context has exited here -> actor shut down (graceful drain).
    _check_row(post_shutdown.result(), "actor post-shutdown retire")
    # New work after shutdown raises through serve too (it forwards to the
    # shut-down Engine, same guard as actor.remote).
    _expect_raise(lambda: actor.serve.remote(service_ms=0),
                  "actor.serve.remote after shutdown")
    print("PASS: SyntheticActor remote / remote_batch / wait / as_completed / "
          "get / serve.remote(_batch) (single op; AttributeError on others) / "
          "post-shutdown retire -> well-formed results")


# ---- lane_impl backend selection + hpx-backend parity -------------------
#
# lane_impl="std" (default) is the std::thread ServiceLane stable comparison
# anchor; lane_impl="hpx" is the opt-in cooperative HpxLane. Both honor the
# IDENTICAL lane contract and result-row schema -- the only visible difference is
# the lane actor_id prefix (act-hpx- vs act-hpxl-). The checks below are
# semantic/parity only (no timing thresholds): the selection check compares
# prefixes + validation across both backends; the hpx-backend check re-runs the
# core lane semantics (basic, wait/as_completed, chunked, queued + running
# cancel, bounded admission, lane_stats) on lane_impl="hpx", reusing the same
# _check_row / _check_cancelled_row / _expect_exc / _poll_until contract helpers
# as the std checks. The existing std checks above are unchanged.

# Lane actor_id prefixes. act-hpxl- does NOT start with "act-hpx-" (the trailing
# dash makes the two prefixes unambiguous), so a startswith test cleanly
# distinguishes the backends.
_STD_PREFIX = "act-hpx-"
_HPX_PREFIX = "act-hpxl-"


def _check_prefix(actor_id, prefix, where):
    if not isinstance(actor_id, str) or not actor_id.startswith(prefix):
        _fail(f"{where}: actor_id {actor_id!r} does not start with {prefix!r}")


def check_lane_impl_selection():
    # Backend selection + validation, shared across std and hpx. Default and
    # explicit "std" -> act-hpx-; "hpx" -> act-hpxl-; SyntheticActor forwards the
    # selector; invalid values raise BEFORE the HPX runtime starts (non-str ->
    # TypeError, unknown str -> ValueError) on both Engine and SyntheticActor.
    from rayx import Engine, SyntheticActor

    # (1) Default Engine == std behavior (act-hpx- prefix).
    with Engine(num_lanes=1, hpx_threads=2) as engine:
        row = engine.submit(service_ms=0).result()
        _check_row(row, "default Engine (implicit std)")
        _check_prefix(row["actor_id"], _STD_PREFIX, "default Engine prefix")

    # (2) Explicit lane_impl="std" matches the default.
    with Engine(num_lanes=1, hpx_threads=2, lane_impl="std") as engine:
        row = engine.submit(service_ms=0).result()
        _check_row(row, "explicit std Engine")
        _check_prefix(row["actor_id"], _STD_PREFIX, "explicit std prefix")

    # (3) Explicit lane_impl="hpx" -> HpxLane backend (act-hpxl- prefix).
    with Engine(num_lanes=1, hpx_threads=2, lane_impl="hpx") as engine:
        row = engine.submit(service_ms=0).result()
        _check_row(row, "explicit hpx Engine")
        _check_prefix(row["actor_id"], _HPX_PREFIX, "explicit hpx prefix")

    # (4) SyntheticActor forwards lane_impl to its Engine (hpx -> act-hpxl-).
    with SyntheticActor(num_lanes=1, hpx_threads=2, lane_impl="hpx") as actor:
        row = actor.remote(service_ms=0).result()
        _check_row(row, "actor hpx remote")
        _check_prefix(row["actor_id"], _HPX_PREFIX, "actor hpx prefix")

    # (5) Invalid lane_impl rejected up front on Engine AND the forwarding
    # SyntheticActor: non-string -> TypeError, unknown string -> ValueError.
    _expect_exc(lambda: Engine(lane_impl=123), TypeError, "Engine lane_impl int")
    _expect_exc(lambda: Engine(lane_impl="fake"), ValueError,
                "Engine lane_impl unknown")
    _expect_exc(lambda: SyntheticActor(lane_impl=3.5), TypeError,
                "SyntheticActor lane_impl float")
    _expect_exc(lambda: SyntheticActor(lane_impl="nope"), ValueError,
                "SyntheticActor lane_impl unknown")

    print("PASS: lane_impl selection (default/explicit std -> act-hpx-; hpx -> "
          "act-hpxl-; actor forwards; non-str -> TypeError, unknown -> "
          "ValueError) -> ok")


def check_hpx_backend():
    # Re-run the core lane semantics on lane_impl="hpx" to confirm the HpxLane
    # backend honors the same contract as ServiceLane (and that every op is
    # correctly entered from an HPX thread by the adapter). Parity/semantics only,
    # no timing thresholds; every row also carries the act-hpxl- prefix.
    import time
    from rayx import Engine, QueueFullError
    OCC = 120  # occupier service_ms (spin): >> microseconds to fill/probe/cancel

    # (1) Basic submit + get, multi-lane, with the hpx prefix on every row.
    with Engine(num_lanes=2, hpx_threads=2, lane_impl="hpx") as engine:
        if engine.num_lanes() != 2:
            _fail(f"hpx num_lanes() is {engine.num_lanes()}, expected 2")
        row = engine.get(engine.submit(service_ms=0))
        _check_row(row, "hpx submit/get noop")
        _check_prefix(row["actor_id"], _HPX_PREFIX, "hpx submit/get prefix")
        for i, fut in enumerate(engine.submit(service_ms=1) for _ in range(4)):
            r = fut.result()
            _check_row(r, f"hpx submit multi[{i}]")
            _check_prefix(r["actor_id"], _HPX_PREFIX, f"hpx multi prefix[{i}]")

    # (2) wait + as_completed over hpx futures (non-consuming wait; each future
    # yielded once by as_completed).
    with Engine(num_lanes=2, hpx_threads=2, lane_impl="hpx") as engine:
        futs = [engine.submit(service_ms=1) for _ in range(4)]
        ready, _nr = engine.wait(futs, num_returns=1)
        if not ready:
            _fail("hpx wait returned no ready futures")
        seen = 0
        for fut in engine.as_completed(futs):
            _check_row(fut.result(), f"hpx as_completed[{seen}]")
            seen += 1
        if seen != len(futs):
            _fail(f"hpx as_completed yielded {seen}, expected {len(futs)}")

    # (3) Chunked service: one row echoing chunks/delay, full run completes.
    with Engine(num_lanes=1, hpx_threads=2, lane_impl="hpx") as engine:
        crow = engine.submit(service_ms=8, work_mode="sleep", chunks=4,
                             chunk_delay_ms=1).result()
        _check_row(crow, "hpx chunked (4x)")
        if crow["chunks"] != 4 or crow["chunk_delay_ms"] != 1:
            _fail(f"hpx chunked echo wrong: {crow}")
        _check_prefix(crow["actor_id"], _HPX_PREFIX, "hpx chunked prefix")

    # (4) Queued cancellation: a victim queued behind a long spin occupier cancels
    # True and retires status="cancelled" (0 chunks run); a served request is
    # cancel False.
    with Engine(num_lanes=1, hpx_threads=2, lane_impl="hpx") as engine:
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        _poll_until(lambda: engine.lane_stats()[0]["active"], "hpx queued occ active")
        victim = engine.submit(service_ms=5, label="hv")
        if engine.cancel(victim) is not True:
            _fail("hpx cancel(queued victim) did not return True")
        if victim.cancelled() is not True:
            _fail("hpx victim.cancelled() not True")
        vrow = victim.result()
        _check_cancelled_row(vrow, "hpx queued cancel", label="hv")
        if vrow["chunks_completed"] != 0:
            _fail(f"hpx queued cancel chunks_completed {vrow['chunks_completed']} "
                  "!= 0 (should be a queued skip)")
        occ.result()  # drain

    # (5) Running cancellation at a chunk boundary: a started chunked request with
    # boundaries ahead cancels True and retires a strictly-partial run
    # (1 <= chunks_completed < chunks). Same controls as the std running-cancel
    # check (50ms wait vs ~200ms final boundary), no exact count / timing assert.
    with Engine(num_lanes=1, hpx_threads=2, lane_impl="hpx") as engine:
        f = engine.submit(service_ms=240, work_mode="sleep", chunks=6,
                          chunk_delay_ms=0, label="hrc")
        time.sleep(0.05)  # started; many boundaries remain
        if engine.cancel(f) is not True:
            _fail("hpx cancel(running chunked) did not return True")
        if f.cancelled() is not True:
            _fail("hpx running-cancelled future.cancelled() not True")
        row = f.result()
        _check_cancelled_row(row, "hpx running chunk-boundary cancel", label="hrc")
        if not (1 <= row["chunks_completed"] < row["chunks"]):
            _fail(f"hpx running cancel chunks_completed {row['chunks_completed']} "
                  "not in [1, chunks)")

    # (6) Bounded admission: with cap 2 on one lane, fill the queue behind a spin
    # occupier, then the next submit raises QueueFullError (a no-op: queue depth
    # unchanged); admitted + occupier work still completes normally.
    with Engine(num_lanes=1, hpx_threads=2, lane_impl="hpx",
                max_queue_depth_per_lane=2) as engine:
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        _poll_until(lambda: engine.lane_stats()[0]["active"], "hpx cap occ active")
        a = engine.submit(service_ms=5, work_mode="spin")  # depth 1
        b = engine.submit(service_ms=5, work_mode="spin")  # depth 2 (full)
        if engine.lane_stats()[0]["queue_depth"] != 2:
            _fail(f"hpx cap: expected queue_depth 2, got "
                  f"{engine.lane_stats()[0]['queue_depth']}")
        ex = _expect_exc(lambda: engine.submit(service_ms=5, work_mode="spin"),
                         QueueFullError, "hpx cap full submit")
        if not isinstance(ex, RuntimeError):
            _fail("hpx QueueFullError must subclass RuntimeError")
        if engine.lane_stats()[0]["queue_depth"] != 2:
            _fail("hpx rejected submit changed queue_depth (should be a no-op)")
        for r in engine.get([occ, a, b]):  # admitted + occupier complete normally
            _check_row(r, "hpx admitted work completes")

    # (7) lane_stats on the hpx backend: well-typed rows with the act-hpxl- ids;
    # occupier observed active with queued depth > 0; returns to idle after drain.
    with Engine(num_lanes=1, hpx_threads=2, lane_impl="hpx") as engine:
        occ = engine.submit(service_ms=OCC, work_mode="spin")
        victims = [engine.submit(service_ms=5, work_mode="spin", label=f"hq{i}")
                   for i in range(3)]
        snap = _poll_until(lambda: (engine.lane_stats()
                                    if engine.lane_stats()[0]["active"] else None),
                           "hpx occupier active")
        _check_stat_rows(snap, "hpx occupied", expected_lanes=1)
        _check_prefix(snap[0]["actor_id"], _HPX_PREFIX, "hpx lane_stats prefix")
        if sum(r["queue_depth"] for r in snap) <= 0:
            _fail(f"hpx expected queued work behind occupier, got {snap}")
        for v in victims:
            engine.cancel(v)
            v.result()
        occ.result()
        idle = _poll_until(lambda: (engine.lane_stats()
                                    if engine.lane_stats()[0]["queue_depth"] == 0
                                    and not engine.lane_stats()[0]["active"]
                                    else None),
                           "hpx lane idle after drain")
        _check_stat_rows(idle, "hpx drained", expected_lanes=1)

    print("PASS: hpx backend (submit/get; wait/as_completed; chunked; queued + "
          "running cancel; bounded admission/QueueFullError; lane_stats; "
          "act-hpxl- prefix throughout) -> ok")


def main():
    check_import()
    check_hpx_smoke()
    check_engine()
    check_ready_and_wait()
    check_as_completed()
    check_get()
    check_label()
    check_work_mode()
    check_varied_batch()
    check_cancel()
    check_chunks()
    check_running_cancel()
    check_double_result()
    check_repr()
    check_wait_contracts()
    check_bounded_wait()
    check_lane_stats()
    check_admission_control()
    check_synthetic_actor()
    check_lane_impl_selection()
    check_hpx_backend()
    print("OK: rayx smoke passed")


if __name__ == "__main__":
    main()
