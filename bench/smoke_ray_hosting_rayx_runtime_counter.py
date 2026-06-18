#!/usr/bin/env python3
"""Composition smoke for "Ray actor hosting a rayx.runtime.Runtime + counter".

Extends bench/smoke_ray_hosting_rayx_runtime.py from a fixed registered native
op (`square`) to a fixed local native actor (`counter`): a long-lived Ray actor
constructs exactly one HPX-native `rayx.runtime.Runtime` AND one
`Runtime.create_actor("counter", initial)` in `__init__`, and dispatches the
fixed registered methods (`add` / `get` / `reset`) on it locally. Ray owns the
outer actor placement/lifecycle; the Runtime owns the local native counter
actor inside the actor process.

Boundaries this proves structurally (no JSONL, no timing, no perf claim):
  * one Runtime + one local native counter actor per Ray actor process;
  * the ActorHandle / RuntimeFuture / OperationResult are retired INSIDE the
    Ray actor -- only plain ints / dicts cross the Ray boundary (no object
    store, no ray.get wrapper, no compatibility layer, no .remote() actor API);
  * counter state evolves correctly (initial -> add -> get -> reset -> get);
  * two Ray actors hold INDEPENDENT counters with distinct rt-act- ids;
  * an explicit Runtime.release_actor makes later use of that counter raise
    inside the actor, surfaced across Ray as a plain error marker dict;
  * clean idempotent shutdown; use-after-shutdown raises through Ray.

This is composition, not a Ray backend: Ray owns outer placement/lifecycle, the
hosted Runtime owns local native execution. Smoke-only -- no timing, no JSONL,
no perf comparison; the in-flight release / cancel-then-drain path is covered by
bench/smoke_rayx_runtime.py and tests/integration, not re-proven here.

Skips cleanly (exit 0) if either `ray` or the built `_rayx` extension is
unavailable. On a failed assertion it prints FAIL and exits 1, mirroring
bench/smoke_ray_hosting_rayx_runtime.py.

Usage:
    python bench/smoke_ray_hosting_rayx_runtime_counter.py
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
# (mirroring bench/smoke_ray_hosting_rayx_runtime.py).


class RayxRuntimeCounterHostActor:
    """A long-lived Ray actor hosting one rayx.runtime.Runtime + one counter.

    Defined undecorated here and turned into a Ray actor via ``ray.remote(...)``
    inside main() (after the skip-clean check), so importing this module never
    requires ray.

    Constructed once; creates one local native ``counter`` actor on its hosted
    Runtime; dispatches the fixed registered methods, retires every
    RuntimeFuture / OperationResult internally, and returns ONLY plain ints or
    small dicts. No ActorHandle / RuntimeFuture / OperationResult leaves the
    actor.
    """

    def __init__(self, rayx_src, initial):
        # The Ray worker is a separate process: put rayx on its path, then import
        # and construct the ONE Runtime + ONE counter actor this Ray actor hosts.
        if rayx_src not in sys.path:
            sys.path.insert(0, rayx_src)
        import uuid
        import rayx  # noqa: F401  (runs in the Ray worker)
        from rayx.runtime import Runtime

        self._rayx = rayx
        self.ray_actor_id = "ray-" + uuid.uuid4().hex[:8]
        self.runtime = Runtime(num_lanes=1, hpx_threads=1)
        # One local native counter actor; the ActorHandle stays inside this Ray
        # actor process and is NEVER returned across the Ray boundary.
        self.counter = self.runtime.create_actor("counter", initial)

    def ids(self):
        """Return (ray_actor_id, rt_act_id) -- both plain strings.

        The inner counter actor id (rt-act-...) is the native serving identity
        stamped on each method row; the Ray actor id is the outer placement id.
        """
        return self.ray_actor_id, self.counter.actor_id

    def _call_int(self, method_id, *args):
        """Dispatch one counter method, retire it HERE, return a plain int.

        The RuntimeFuture and OperationResult are created and consumed entirely
        inside this Ray worker; only the int value (or None on a non-completed
        outcome) crosses back.
        """
        res = self.counter.call(method_id, *args).result()
        # res.value raises on a non-completed outcome; the MVP counter methods
        # always complete, but guard so a stray failure still returns a plain
        # value rather than letting an OperationResult escape.
        return res.value if res.row["status"] == "completed" else None

    def add(self, delta):
        """counter.add(delta); return the post-add value as a plain int."""
        return self._call_int("add", delta)

    def get(self):
        """counter.get(); return the current value as a plain int."""
        return self._call_int("get")

    def reset(self, value):
        """counter.reset(value); return whatever the method yields (plain int)."""
        return self._call_int("reset", value)

    def release(self):
        """Explicitly release the hosted counter actor (idle release). -> True.

        Local explicit release only (not ray.kill). After this, calls on the
        counter raise inside the actor (see call_after_release()).
        """
        self.runtime.release_actor(self.counter)
        return True

    def call_after_release(self):
        """Attempt counter.get() after release; return a PLAIN error marker.

        Proves the released-handle rejection surfaces across the Ray boundary as
        a plain dict, not by leaking an exception object or a handle.
        """
        try:
            self.counter.call("get").result()
        except Exception as exc:  # noqa: BLE001 - report any rejection
            return {"raised": True, "error": repr(exc)}
        return {"raised": False, "error": None}

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


def _check_plain_int(v, where):
    # bool is an int subclass; the counter value model is int64, so a real int is
    # expected (a bool here would be a boundary-projection bug).
    if isinstance(v, bool) or not isinstance(v, int):
        _fail(f"{where}: expected a plain int across the Ray boundary, got "
              f"{type(v).__name__} ({v!r}) -- no RuntimeFuture/OperationResult "
              "may cross")


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
    RemoteActor = ray.remote(RayxRuntimeCounterHostActor)

    ray.init(num_cpus=4, include_dashboard=False,
             ignore_reinit_error=True, logging_level="error")
    try:
        actor = RemoteActor.remote(RAYX_SRC, 10)

        # (1) Identities: outer Ray actor id + inner rt-act- counter id.
        ray_actor_id, rt_act_id = ray.get(actor.ids.remote())
        if not ray_actor_id:
            _fail("ids(): empty ray_actor_id")
        if not (rt_act_id and rt_act_id.startswith("rt-act-")):
            _fail(f"ids(): counter id {rt_act_id!r} missing/!startswith 'rt-act-'")
        if not isinstance(rt_act_id, str):
            _fail(f"ids(): counter id is {type(rt_act_id).__name__}, expected str")
        print(f"PASS: actor up -> ray_actor_id={ray_actor_id} "
              f"counter_id={rt_act_id}")

        # (2) State sequence: initial(10) -> get -> add(5) -> get -> reset(0) ->
        # get. State is verified with get() after each mutation (robust to the
        # exact return semantics of add / reset). Every value is a plain int.
        v0 = ray.get(actor.get.remote())
        _check_plain_int(v0, "get() initial")
        if v0 != 10:
            _fail(f"initial counter value is {v0!r}, expected 10")

        v_add = ray.get(actor.add.remote(5))
        _check_plain_int(v_add, "add(5)")
        v1 = ray.get(actor.get.remote())
        _check_plain_int(v1, "get() after add")
        if v1 != 15:
            _fail(f"after add(5) value is {v1!r}, expected 15")

        v_reset = ray.get(actor.reset.remote(0))
        _check_plain_int(v_reset, "reset(0)")
        v2 = ray.get(actor.get.remote())
        _check_plain_int(v2, "get() after reset")
        if v2 != 0:
            _fail(f"after reset(0) value is {v2!r}, expected 0")
        print("PASS: state sequence (initial 10 -> add 5 -> 15 -> reset 0 -> 0; "
              "plain ints only)")

        # (3) Two Ray actors hold INDEPENDENT counters with distinct rt-act- ids.
        a1 = RemoteActor.remote(RAYX_SRC, 100)
        a2 = RemoteActor.remote(RAYX_SRC, 200)
        (_r1, id1), (_r2, id2) = ray.get([a1.ids.remote(), a2.ids.remote()])
        if not (id1 and id2):
            _fail("multi: a hosted counter id was empty")
        if id1 == id2:
            _fail(f"multi: two actors share counter id {id1!r} (expected distinct "
                  "counters, one per process)")
        ray.get(a1.add.remote(1))  # mutate a1 only
        m1 = ray.get(a1.get.remote())
        m2 = ray.get(a2.get.remote())
        _check_plain_int(m1, "multi a1.get")
        _check_plain_int(m2, "multi a2.get")
        if m1 != 101:
            _fail(f"multi: a1 value is {m1!r}, expected 101")
        if m2 != 200:
            _fail(f"multi: a2 value is {m2!r}, expected 200 (a1's add leaked!)")
        print(f"PASS: two independent counters ({id1} -> 101 / {id2} -> 200; "
              "no cross-actor state leak)")

        # (4) Explicit idle release: release() then a later call raises INSIDE the
        # actor, surfaced as a plain marker dict (in-flight cancel-then-drain is
        # covered by smoke_rayx_runtime.py / tests/integration, not here).
        if ray.get(a1.release.remote()) is not True:
            _fail("release() did not return True")
        marker = ray.get(a1.call_after_release.remote())
        if not isinstance(marker, dict):
            _fail(f"call_after_release returned {type(marker).__name__}, expected dict")
        if marker.get("raised") is not True or not marker.get("error"):
            _fail(f"call after release did NOT raise inside the actor: {marker!r}")
        # a1 released only its counter; its hosted Runtime is still live, so shut
        # it down explicitly (symmetry with `actor` / a2 below -- every hosted
        # Runtime gets a clean graceful-drain, not a process-teardown).
        if ray.get(a1.shutdown.remote()) is not True:
            _fail("a1 shutdown() after release did not return True")
        print(f"PASS: release (idle release; later call raises inside the actor; "
              f"a1 Runtime shut down cleanly) -> {marker['error']}")

        # (5) Clean idempotent shutdown; use-after-shutdown raises through Ray.
        if ray.get(actor.shutdown.remote()) is not True:
            _fail("shutdown() did not return True")
        if ray.get(actor.shutdown.remote()) is not True:
            _fail("second shutdown() did not return True (should be a no-op)")
        try:
            ray.get(actor.get.remote())
        except Exception:
            pass
        else:
            _fail("get() after shutdown did not raise")
        # a2 still owns a live Runtime + counter; shut it down cleanly too.
        if ray.get(a2.shutdown.remote()) is not True:
            _fail("a2 shutdown() did not return True")
        print("PASS: shutdown (clean + idempotent; use-after-shutdown raises "
              "through Ray)")
    finally:
        ray.shutdown()

    print("ALL PASS: ray-hosting-rayx.runtime counter composition smoke")
    sys.exit(0)


if __name__ == "__main__":
    main()
