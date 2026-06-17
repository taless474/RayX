#!/usr/bin/env python3
"""Composition smoke for "Ray actor hosting a rayx.runtime.Runtime".

Slice 1 (smoke-only): a long-lived Ray actor constructs exactly one HPX-native
`rayx.runtime.Runtime` in `__init__` and runs a fixed registered native op
(`square`) through it locally. Ray owns the outer actor placement/lifecycle;
the Runtime owns local native-op execution inside the actor process.

Boundaries this proves structurally (no JSONL, no timing, no perf claim):
  * one Runtime per Ray actor process (the HPX/RayX runtime is process-global);
  * a second Runtime AND a second Engine in the same process are both rejected
    by the shared process guard;
  * the RuntimeFuture is retired INSIDE the actor -- only a plain dict crosses
    the Ray boundary (no RuntimeFuture / OperationResult, no ray.get wrapper,
    no object store, no compatibility layer);
  * clean idempotent shutdown; serve-after-shutdown raises through Ray;
  * two Ray actors can each host one Runtime with distinct rt-hpx- lane ids.

Skips cleanly (exit 0) if either `ray` or the built `_rayx` extension is
unavailable. On a failed assertion it prints FAIL and exits 1, mirroring
bench/smoke_ray_hosting_rayx.py and bench/smoke_rayx_runtime.py.

Usage:
    python bench/smoke_ray_hosting_rayx_runtime.py
Exit code 0 == pass or skip, 1 == fail.
"""
import os
import sys

# Quiet a benign Ray FutureWarning about accelerator env vars when num_gpus=0
# (matches the other Ray drivers in bench/).
os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (REPO_ROOT, BENCH_DIR, RAYX_SRC):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# NOTE: `ray` is intentionally NOT imported at module top, and the actor class
# below is NOT decorated with `@ray.remote` at module scope -- both would make
# importing this module fail when ray is absent, defeating the skip-clean check
# in main(). ray is imported and the class is decorated inside main() instead
# (mirroring bench/smoke_ray_hosting_rayx.py, which defers its ray-dependent
# import the same way).


# The plain, JSON-serializable subset the actor's call_square() returns. It is a
# deliberate projection of rayx.runtime's value + 9-field row -- NO RuntimeFuture
# and NO OperationResult cross the Ray boundary.
SQUARE_FIELDS = (
    "op_id", "value", "status", "error",
    "actor_id", "service_ms_observed", "start_ns", "end_ns",
)


class RayxRuntimeHostActor:
    """A long-lived Ray actor hosting exactly one rayx.runtime.Runtime.

    Defined undecorated here and turned into a Ray actor via ``ray.remote(...)``
    inside main() (after the skip-clean check), so importing this module never
    requires ray.

    Constructed once; runs the fixed registered native op `square` through its
    local Runtime; retires the RuntimeFuture internally and returns only plain
    dicts. The Runtime is shut down via the explicit shutdown() method.
    """

    def __init__(self, rayx_src):
        # The Ray worker is a separate process: put rayx on its path, then import
        # and construct the ONE Runtime this actor will host.
        if rayx_src not in sys.path:
            sys.path.insert(0, rayx_src)
        import uuid
        import rayx  # noqa: F401  (runs in the Ray worker)
        from rayx.runtime import Runtime

        self._rayx = rayx
        self._Runtime = Runtime
        self.ray_actor_id = "ray-" + uuid.uuid4().hex[:8]
        self.runtime = Runtime(num_lanes=1, hpx_threads=1)

    def ids(self):
        """Return (ray_actor_id, rt_hpx_lane_id).

        The inner Runtime op-lane id (rt-hpx-...) is the native serving identity
        stamped on each op row; the Ray actor id is the outer placement identity.
        """
        lanes = self.runtime.lane_stats()
        lane_id = lanes[0]["actor_id"] if lanes else None
        return self.ray_actor_id, lane_id

    def call_square(self, x):
        """Run square(x) on the local Runtime; return a plain dict.

        Submits the fixed registered op, retires the RuntimeFuture HERE (inside
        the actor), and returns only JSON-serializable fields. No RuntimeFuture
        or OperationResult leaves the actor.
        """
        res = self.runtime.submit_operation("square", x).result()
        row = res.row
        # res.value raises on a non-completed op; square always completes, but
        # guard so a failed/cancelled outcome still returns a plain dict.
        value = res.value if row["status"] == "completed" else None
        return {
            "op_id": "square",
            "value": value,
            "status": row["status"],
            "error": row["error"],
            "actor_id": row["actor_id"],          # inner rt-hpx- op-lane id
            "service_ms_observed": row["service_ms_observed"],
            "start_ns": row["start_ns"],
            "end_ns": row["end_ns"],
        }

    def try_second_runtime(self):
        """Attempt a SECOND Runtime in this process; report the outcome.

        The HPX/RayX runtime is a process singleton, so this must be rejected
        while the hosted Runtime is active. Returns {"rejected": bool, "error"}.
        """
        try:
            stray = self._Runtime(num_lanes=1, hpx_threads=1)
        except Exception as exc:  # noqa: BLE001 - report any rejection
            return {"rejected": True, "error": repr(exc)}
        stray.shutdown()
        return {"rejected": False, "error": None}

    def try_second_engine(self):
        """Attempt a rayx.Engine in this process; report the outcome.

        Engine and Runtime share ONE process-global guard, so an Engine must be
        rejected while the hosted Runtime is active.
        """
        try:
            stray = self._rayx.Engine(num_lanes=1, hpx_threads=1)
        except Exception as exc:  # noqa: BLE001 - report any rejection
            return {"rejected": True, "error": repr(exc)}
        stray.shutdown()
        return {"rejected": False, "error": None}

    def shutdown(self):
        """Graceful-drain the hosted Runtime. Idempotent-safe for repeat calls."""
        if self.runtime is not None:
            self.runtime.shutdown()
            self.runtime = None
        return True


def _fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def _skip(msg):
    print(f"SKIP: {msg}")
    sys.exit(0)


def _check_square_row(row, x, where):
    if not isinstance(row, dict):
        _fail(f"{where}: call_square returned {type(row).__name__}, expected dict")
    for field in SQUARE_FIELDS:
        if field not in row:
            _fail(f"{where}: row missing field {field!r} (got {sorted(row)})")
    # A RuntimeFuture / OperationResult must never cross the Ray boundary: the
    # returned object is a plain dict (checked above) of plain scalars only.
    for k, v in row.items():
        if not isinstance(v, (int, float, str, type(None))):
            _fail(f"{where}: field {k!r} is {type(v).__name__}, expected a plain "
                  "JSON scalar (no RuntimeFuture/OperationResult may cross)")
    if row["op_id"] != "square":
        _fail(f"{where}: op_id is {row['op_id']!r}, expected 'square'")
    if row["status"] != "completed":
        _fail(f"{where}: status is {row['status']!r}, expected 'completed'")
    if row["error"] is not None:
        _fail(f"{where}: error is {row['error']!r}, expected None")
    if row["value"] != x * x:
        _fail(f"{where}: value is {row['value']!r}, expected {x * x}")
    if not (row["actor_id"] and row["actor_id"].startswith("rt-hpx-")):
        _fail(f"{where}: actor_id {row['actor_id']!r} missing/!startswith 'rt-hpx-'")
    if row["service_ms_observed"] < 0:
        _fail(f"{where}: service_ms_observed negative ({row['service_ms_observed']})")
    if row["end_ns"] < row["start_ns"]:
        _fail(f"{where}: end_ns < start_ns")


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
    # Decorate here (not at module top) so importing this module does NOT require
    # ray -- that keeps the skip-clean check above authoritative.
    RemoteActor = ray.remote(RayxRuntimeHostActor)

    ray.init(num_cpus=4, include_dashboard=False,
             ignore_reinit_error=True, logging_level="error")
    try:
        actor = RemoteActor.remote(RAYX_SRC)

        # (1) Identities: outer Ray actor id + inner rt-hpx- op-lane id.
        ray_actor_id, lane_id = ray.get(actor.ids.remote())
        if not ray_actor_id:
            _fail("ids(): empty ray_actor_id")
        if not (lane_id and lane_id.startswith("rt-hpx-")):
            _fail(f"ids(): runtime lane id {lane_id!r} missing/!startswith 'rt-hpx-'")
        print(f"PASS: actor up -> ray_actor_id={ray_actor_id} "
              f"runtime_lane_id={lane_id}")

        # (2) Several square calls -> correct values, completed, plain dicts.
        for x in (0, 2, 5, 9, 12):
            _check_square_row(ray.get(actor.call_square.remote(x)), x,
                              f"call_square({x})")
        # Every row carries the SAME inner lane id (one hosted Runtime, one lane).
        again = ray.get(actor.call_square.remote(3))
        if again["actor_id"] != lane_id:
            _fail("call_square lane id diverged from the hosted Runtime's lane id")
        print("PASS: call_square (square(x) -> x*x, completed, plain dict only, "
              "one hosted rt-hpx- lane)")

        # (3) Process-singleton: a second Runtime in this process is rejected.
        r_rt = ray.get(actor.try_second_runtime.remote())
        if r_rt.get("rejected") is not True or not r_rt.get("error"):
            _fail(f"second Runtime in-process was NOT rejected: {r_rt!r}")
        print(f"PASS: second Runtime in same process rejected -> {r_rt['error']}")

        # (4) Shared guard: a rayx.Engine in this process is also rejected.
        r_eng = ray.get(actor.try_second_engine.remote())
        if r_eng.get("rejected") is not True or not r_eng.get("error"):
            _fail(f"Engine in-process was NOT rejected: {r_eng!r}")
        print(f"PASS: rayx.Engine in same process rejected (shared guard) -> "
              f"{r_eng['error']}")

        # (5) Clean idempotent shutdown; serve-after-shutdown raises through Ray.
        if ray.get(actor.shutdown.remote()) is not True:
            _fail("shutdown() did not return True")
        if ray.get(actor.shutdown.remote()) is not True:
            _fail("second shutdown() did not return True (should be a no-op)")
        try:
            ray.get(actor.call_square.remote(4))
        except Exception:
            pass
        else:
            _fail("call_square() after shutdown did not raise")
        print("PASS: shutdown (clean + idempotent; call_square-after-shutdown "
              "raises through Ray)")

        # (6) Multi-actor: two actors, each its OWN Runtime (separate processes),
        # distinct rt-hpx- lane ids, both serve square and shut down cleanly.
        a1 = RemoteActor.remote(RAYX_SRC)
        a2 = RemoteActor.remote(RAYX_SRC)
        (_rid1, l1), (_rid2, l2) = ray.get([a1.ids.remote(), a2.ids.remote()])
        if not (l1 and l2):
            _fail("multi: a hosted runtime lane id was empty")
        if l1 == l2:
            _fail(f"multi: two actors share lane id {l1!r} (expected distinct "
                  "runtimes, one per process)")
        for who, a in (("a1", a1), ("a2", a2)):
            _check_square_row(ray.get(a.call_square.remote(6)), 6, f"multi {who}")
        if ray.get(a1.shutdown.remote()) is not True or \
                ray.get(a2.shutdown.remote()) is not True:
            _fail("multi: an actor did not shut down cleanly")
        print(f"PASS: multi-actor (2 actors, distinct runtimes {l1} / {l2}; both "
              "serve square + shutdown cleanly)")
    finally:
        ray.shutdown()

    print("ALL PASS: ray-hosting-rayx.runtime composition smoke")
    sys.exit(0)


if __name__ == "__main__":
    main()
