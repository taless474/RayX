#!/usr/bin/env python3
"""Tiny composition smoke for "Ray actor hosting a RayX Engine".

Answers exactly one question structurally: can a long-lived Ray actor host one
HPX-backed rayx.Engine cleanly, serve a handful of requests, and return
well-formed rows -- with the process-singleton constraint (one Engine per
process) demonstrated, and a clean shutdown.

Skips cleanly (exit 0) if either `ray` or the built `_rayx` extension is
unavailable, so it is safe in the native RayX smoke job and a no-op elsewhere.
On a failed assertion it prints FAIL and exits 1, mirroring bench/smoke_rayx.py.

Usage:
    python bench/smoke_ray_hosting_rayx.py
Exit code 0 == pass or skip, 1 == fail.
"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (REPO_ROOT, BENCH_DIR, RAYX_SRC):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def _skip(msg):
    print(f"SKIP: {msg}")
    sys.exit(0)


# Inner-row fields the actor's serve() returns (a plain, JSON-serializable
# subset of rayx's result row -- no RayX Future crosses the Ray boundary).
SERVE_FIELDS = (
    "actor_id", "start_ns", "end_ns", "service_ms_observed",
    "status", "error", "chunks_completed",
)


def _check_serve_row(row, where):
    if not isinstance(row, dict):
        _fail(f"{where}: serve() returned {type(row).__name__}, expected dict")
    for field in SERVE_FIELDS:
        if field not in row:
            _fail(f"{where}: row missing field {field!r} (got {sorted(row)})")
    if row["status"] != "completed":
        _fail(f"{where}: status is {row['status']!r}, expected 'completed'")
    if row["error"] is not None:
        _fail(f"{where}: error is {row['error']!r}, expected None")
    if not row["actor_id"]:
        _fail(f"{where}: actor_id (rayx lane) is empty")
    if row["end_ns"] < row["start_ns"]:
        _fail(f"{where}: end_ns < start_ns")
    if row["service_ms_observed"] < 0:
        _fail(f"{where}: service_ms_observed negative ({row['service_ms_observed']})")


def main():
    # Skip-clean if the composition's two hard deps are not both present.
    try:
        import ray  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        _skip(f"ray not importable: {exc}")
    try:
        import rayx  # noqa: F401  (needs the built _rayx extension)
    except Exception as exc:  # noqa: BLE001
        _skip(f"rayx/_rayx not importable (build the extension first): {exc}")

    import ray
    # Import the driver only after the deps are confirmed (it imports ray).
    from run_ray_hosting_rayx import RayxHostActor, _RAYX_SRC

    ray.init(num_cpus=4, include_dashboard=False,
             ignore_reinit_error=True, logging_level="error")
    try:
        actor = RayxHostActor.remote(_RAYX_SRC, 1, 1, "std", None)

        # (1) Identities: outer Ray actor id + inner rayx serving-lane id.
        ray_actor_id, lane_id = ray.get(actor.ids.remote())
        if not ray_actor_id:
            _fail("ids(): empty ray_actor_id")
        if not (lane_id and lane_id.startswith("act-")):
            _fail(f"ids(): rayx lane id {lane_id!r} missing/!startswith 'act-'")
        print(f"PASS: actor up -> ray_actor_id={ray_actor_id} "
              f"rayx_lane_id={lane_id}")

        # (2) A handful of requests, one at a time -> well-formed completed rows.
        for i, svc in enumerate((0, 0, 1, 0, 1)):
            req = {"request_id": f"req-{i}", "service_ms": svc,
                   "work_mode": "sleep", "chunks": 1, "chunk_delay_ms": 0.0}
            _check_serve_row(ray.get(actor.serve.remote(req)), f"serve[{i}]")
        # And a small in-flight window retired together (still well-formed).
        refs = [actor.serve.remote(
            {"request_id": f"w-{i}", "service_ms": 0, "work_mode": "sleep",
             "chunks": 1, "chunk_delay_ms": 0.0}) for i in range(4)]
        for i, row in enumerate(ray.get(refs)):
            _check_serve_row(row, f"window[{i}]")
        # Every row carries the SAME inner lane id (one hosted Engine, one lane).
        if any(ray.get(r)["actor_id"] != lane_id for r in
               [actor.serve.remote(
                   {"request_id": "x", "service_ms": 0, "work_mode": "sleep",
                    "chunks": 1, "chunk_delay_ms": 0.0})]):
            _fail("serve row lane id diverged from the hosted Engine's lane id")
        print("PASS: serve (single + windowed) -> well-formed completed rows, "
              "one hosted lane")

        # (3) Process-singleton constraint: a SECOND Engine in the same actor
        # process is rejected while the hosted Engine is active.
        res = ray.get(actor.try_second_engine.remote())
        if res.get("rejected") is not True:
            _fail(f"second Engine in-process was NOT rejected: {res!r}")
        if not res.get("error"):
            _fail("second-Engine rejection reported no error string")
        print(f"PASS: second Engine in same process rejected -> {res['error']}")

        # (4) Clean shutdown (graceful drain), and idempotent on a second call.
        if ray.get(actor.shutdown.remote()) is not True:
            _fail("shutdown() did not return True")
        if ray.get(actor.shutdown.remote()) is not True:
            _fail("second shutdown() did not return True (should be a no-op)")
        # Serving after shutdown raises (the hosted Engine is gone).
        try:
            ray.get(actor.serve.remote(
                {"request_id": "post", "service_ms": 0, "work_mode": "sleep",
                 "chunks": 1, "chunk_delay_ms": 0.0}))
        except Exception:
            pass
        else:
            _fail("serve() after shutdown did not raise")
        print("PASS: shutdown (drain + idempotent; serve-after-shutdown raises)")

        # (5) Multi-actor sanity: two actors, each owning its OWN RayX Engine.
        # Each Ray actor is a separate process, so the process-singleton holds
        # PER PROCESS -- both can host an Engine concurrently. hpx_threads=1 each
        # (Ray num_cpus=4 budgets the two default num_cpus=1 actors); verify both
        # serve and shut down cleanly, with distinct inner lane ids.
        def _noop(i):
            return {"request_id": f"m-{i}", "service_ms": 0, "work_mode": "sleep",
                    "chunks": 1, "chunk_delay_ms": 0.0}
        a1 = RayxHostActor.remote(_RAYX_SRC, 1, 1, "std", None)
        a2 = RayxHostActor.remote(_RAYX_SRC, 1, 1, "std", None)
        (_rid1, lane1), (_rid2, lane2) = ray.get([a1.ids.remote(), a2.ids.remote()])
        if not (lane1 and lane2):
            _fail("multi: a hosted lane id was empty")
        if lane1 == lane2:
            _fail(f"multi: two actors share lane id {lane1!r} (expected distinct "
                  "engines, one per process)")
        for who, a in (("a1", a1), ("a2", a2)):
            for i in range(2):
                _check_serve_row(ray.get(a.serve.remote(_noop(i))),
                                 f"multi {who}[{i}]")
        if ray.get(a1.shutdown.remote()) is not True or \
                ray.get(a2.shutdown.remote()) is not True:
            _fail("multi: an actor did not shut down cleanly")
        print(f"PASS: multi-actor (2 actors, distinct engines {lane1} / {lane2}; "
              "both serve + shutdown cleanly)")
    finally:
        ray.shutdown()

    print("ALL PASS: ray-hosting-rayx composition smoke")
    sys.exit(0)


if __name__ == "__main__":
    main()
