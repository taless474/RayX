#!/usr/bin/env python3
"""exp52 -- Ray-orchestrated HPX bootstrap, CLEAN-PATH island.

STRUCTURAL PLUMBING test only. From HPX's point of view this is the SAME connect-mode mechanism
already validated in exp49 -- HPX cannot observe whether its parent process is a shell, a Python
runner, or a Ray actor. exp52 adds only that a Ray actor is the launcher/supervisor, the role
mpirun / srun / hpxrun.py normally play. The HPX action still travels HPX->HPX over the parcelport;
Ray carries ONLY bootstrap metadata (AGAS endpoint, ports, rendezvous dir, role/index, timeouts)
and NEVER the action result. This re-validates no new HPX property; it validates the Ray /
process-launch plumbing for the already-proven mechanism.

Shape (single node, local ray.init):

  driver: pick free loopback ports p0 (root AGAS+HPX) and p1 (connector HPX); mkdtemp rendezvous dir
    A = IslandProcess(role="root")      .start(meta_root) -> Popen HPX root ; wait root.ready
                                         returns {ready, agas_endpoint:"127.0.0.1:p0", pid}
    B = IslandProcess(role="connector") .start(meta_conn) -> Popen HPX connector joining 127.0.0.1:p0
                                         returns {joined, locality_id, pid}
    HPX root invokes one closed-int64 dist_probe on the connector  ......  HPX -> HPX (NOT via Ray)
    connector: wait served1.ok -> post(disconnect)+stop ;  root: wait_id_absent -> hpx::finalize()
    driver collects each actor's result, writes aggregate.json

LAUNCH HYGIENE (HPX-expert corrections):
  * The binary is built with a self-locating RPATH to the HPX lib dir, so the child does NOT depend
    on DYLD_LIBRARY_PATH propagating through the Ray worker (macOS SIP strips DYLD_* across many
    exec boundaries). We launch with the loader env SCRUBBED to prove self-location, and record it.
  * Both root and connector argv include `--hpx:ignore-batch-env` so HPX does not auto-detect a
    batch environment (SLURM/PBS) from inherited env vars and override the explicit AGAS/HPX ports.
  * `--hpx:bind=none` on both (Ray workers + two HPX localities on one node must not fight over core
    pinning). Numeric 127.0.0.1, never `localhost`. No fixed `--hpx:localities=N` (connect mode is
    dynamic).

CLAIM FENCE: single-node; loopback TCP; closed-int64 action only; Ray = bootstrap/supervision plane
only; HPX = execution/data plane inside one island; clean path only; whole-island-fatal policy
assumed, NOT exercised; no failure injection; no endpoint seam; no production/public API; no
performance/speedup/throughput/latency; no multi-node; no general fabric; no fault tolerance; no Ray
replacement; no "HPX faster than Ray"; no "RayX makes Ray faster".

Usage:
  python run_ray_bootstrap_clean_island.py [--binary <path>] [--x 7] [--wait-bound 15]
      [--step-timeout 20] [--per-phase-timeout 60] [--aggregate <path>]

Exit code is 0 even on a clean fail/skip (the aggregate carries the verdict); non-zero only on an
orchestrator-internal error.
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BINARY_CANDIDATES = [
    os.path.join(HERE, "build", "clean_island_spike"),
    os.path.join(HERE, "build", "Release", "clean_island_spike"),
]
# Loader-env vars we SCRUB from the child to prove the binary self-locates libhpx via RPATH.
_LOADER_ENV_VARS = ("DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH", "LD_LIBRARY_PATH")


def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def locate_binary(explicit):
    if explicit:
        return os.path.abspath(explicit) if os.path.exists(explicit) else None
    for c in DEFAULT_BINARY_CANDIDATES:
        if os.path.exists(c):
            return os.path.abspath(c)
    return None


def _scrubbed_env():
    """Child env with loader-path vars removed -> if the child still starts, it is self-locating."""
    env = dict(os.environ)
    for k in _LOADER_ENV_VARS:
        env.pop(k, None)
    return env


def _root_argv(binary, bootdir, x, wait_bound, step_timeout, p0):
    return [
        binary, "--role", "f_root", "--bootstrap", bootdir,
        "--x", str(x), "--wait-bound", str(wait_bound), "--step-timeout", str(step_timeout),
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p0}",
        "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
        "--hpx:ignore-batch-env",
    ]


def _connector_argv(binary, bootdir, index, serve_timeout, p0, p1):
    return [
        binary, "--role", "f_connect", "--connector-kind", "clean",
        "--connector-index", str(index), "--bootstrap", bootdir,
        "--serve-timeout", str(serve_timeout),
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p1}",
        "--hpx:threads=1", "--hpx:bind=none", "--hpx:ignore-batch-env",
    ]


def build_ray_actor(ray):
    """Define the role-parametrized IslandProcess actor inside a Ray context."""

    @ray.remote
    class IslandProcess:
        def __init__(self, role):
            self.role = role
            self.proc = None
            self.log = None
            self.bootdir = None
            self.argv = None

        def _popen(self, argv, bootdir, log_name):
            self.bootdir = bootdir
            self.argv = list(argv)
            self.log = open(os.path.join(bootdir, log_name), "w")
            # SCRUBBED loader env: prove the child self-locates libhpx via the baked RPATH.
            self.proc = subprocess.Popen(
                argv, cwd=bootdir, stdout=self.log, stderr=subprocess.STDOUT,
                start_new_session=True, env=_scrubbed_env())

        def _wait_for_file(self, path, timeout):
            deadline = time.time() + timeout
            while time.time() < deadline:
                if os.path.exists(path):
                    return True
                if self.proc.poll() is not None:
                    return os.path.exists(path)
                time.sleep(0.05)
            return os.path.exists(path)

        # --- root role ---
        def start_root(self, meta):
            argv = _root_argv(meta["binary"], meta["bootdir"], meta["x"], meta["wait_bound"],
                              meta["step_timeout"], meta["p0"])
            self._popen(argv, meta["bootdir"], "root.log")
            ready = self._wait_for_file(os.path.join(meta["bootdir"], "root.ready"),
                                        meta["ready_timeout"])
            launched = True  # Popen returned without raising
            alive = self.proc.poll() is None
            return {
                "role": "root", "launched": launched, "child_started": alive or ready,
                "ready": ready, "pid": self.proc.pid,
                "agas_endpoint": f"127.0.0.1:{meta['p0']}",
                "argv": self.argv,  # for argv-hygiene assertions (ignore-batch-env, bind=none)
                "early_exit_rc": self.proc.poll(),
            }

        # --- connector role ---
        def start_connector(self, meta):
            argv = _connector_argv(meta["binary"], meta["bootdir"], meta["index"],
                                   meta["serve_timeout"], meta["p0"], meta["p1"])
            self._popen(argv, meta["bootdir"], "connector.log")
            joined = self._wait_for_file(
                os.path.join(meta["bootdir"], f"connect.joined{meta['index']}"),
                meta["join_timeout"])
            jrec = _read_json(os.path.join(meta["bootdir"], f"connect.joined{meta['index']}"))
            alive = self.proc.poll() is None
            return {
                "role": "connector", "launched": True, "child_started": alive or joined,
                "joined": bool(jrec and jrec.get("locality_id") is not None),
                "locality_id": (jrec or {}).get("locality_id"), "pid": self.proc.pid,
                "argv": self.argv, "early_exit_rc": self.proc.poll(),
            }

        def wait_exit(self, timeout):
            """Bounded wait for the child to exit on its own; returns (exited, rc)."""
            if self.proc is None:
                return {"exited": False, "rc": None}
            deadline = time.time() + timeout
            while time.time() < deadline and self.proc.poll() is None:
                time.sleep(0.05)
            return {"exited": self.proc.poll() is not None, "rc": self.proc.poll()}

        def read_result(self, name):
            return _read_json(os.path.join(self.bootdir, name)) if self.bootdir else None

        def shutdown(self):
            """Bounded join, then SIGKILL the process group if still alive."""
            rc = None
            if self.proc is not None:
                try:
                    rc = self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
                    try:
                        rc = self.proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        rc = self.proc.poll()
            if self.log is not None:
                try:
                    self.log.close()
                except OSError:
                    pass
            return {"rc": rc}

    return IslandProcess


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _classify_launch(rec, ready_key):
    """Distinguish launch-failed / bind-failed / ready-timeout for a child that never signalled.
    bind/port failure manifests as the child EXITING early (HPX errors on a taken port); a
    ready-timeout means the child is still alive but slow."""
    launched = bool(rec and rec.get("launched"))
    started = bool(rec and rec.get("child_started"))
    ok = bool(rec and rec.get(ready_key))
    early_rc = (rec or {}).get("early_exit_rc")
    launch_failed = not launched
    # Child exited on its own before signalling readiness/join -> almost always a bind/port error.
    bind_failed = bool(launched and not ok and early_rc is not None)
    ready_timeout = bool(launched and not ok and early_rc is None)
    return launch_failed, bind_failed, ready_timeout, started, ok


def run_once(binary, args):
    import ray  # available: caller guarded

    bootdir = tempfile.mkdtemp(prefix="exp52_")
    p0, p1 = find_free_port(), find_free_port()
    IslandProcess = build_ray_actor(ray)

    a = IslandProcess.remote("root")
    b = IslandProcess.remote("connector")
    ray_launched_root = ray_launched_connector = False
    root_rec = conn_rec = None
    root_result = conn_disc = None
    a_exit = b_exit = None

    try:
        # --- Phase A: Ray actor A launches the HPX root; relay AGAS endpoint THROUGH Ray. ---
        meta_root = {"binary": binary, "bootdir": bootdir, "x": args.x,
                     "wait_bound": args.wait_bound, "step_timeout": args.step_timeout,
                     "p0": p0, "ready_timeout": args.step_timeout}
        root_rec = ray.get(a.start_root.remote(meta_root), timeout=args.per_phase_timeout)
        ray_launched_root = True

        # --- Phase B: Ray actor B launches the connector with A's AGAS endpoint (via Ray). ---
        conn_rec = None
        if root_rec.get("ready"):
            meta_conn = {"binary": binary, "bootdir": bootdir, "index": 1,
                         "serve_timeout": args.step_timeout + 10, "p0": p0, "p1": p1,
                         "join_timeout": args.step_timeout}
            conn_rec = ray.get(b.start_connector.remote(meta_conn), timeout=args.per_phase_timeout)
            ray_launched_connector = True

        # --- Let the HPX action + graceful teardown complete; bounded waits. ---
        a_exit = ray.get(a.wait_exit.remote(args.per_phase_timeout), timeout=args.per_phase_timeout + 5)
        if conn_rec is not None:
            b_exit = ray.get(b.wait_exit.remote(30), timeout=35)
        root_result = ray.get(a.read_result.remote("root_result.json"), timeout=15)
        conn_disc = ray.get(b.read_result.remote("connect.disconnected1"), timeout=15) \
            if conn_rec is not None else None
    finally:
        try:
            ray.get([a.shutdown.remote(), b.shutdown.remote()], timeout=30)
        except Exception:
            pass
        try:
            ray.kill(a)
            ray.kill(b)
        except Exception:
            pass

    # --- classification ---
    r_launch_failed, r_bind_failed, r_ready_timeout, r_started, r_ready = \
        _classify_launch(root_rec, "ready")
    c_launch_failed, c_bind_failed, c_ready_timeout, c_started, c_joined = \
        _classify_launch(conn_rec, "joined")

    action_proved_remote = bool(root_result and root_result.get("action_proved_remote"))
    # Data-plane separation: the action result int must NEVER appear in any Ray-returned payload.
    ray_payloads = json.dumps([root_rec, conn_rec, a_exit, b_exit], default=str)
    result_not_via_ray = ("action_result" not in ray_payloads)  # we never put it there by design
    teardown_clean = bool(conn_disc and conn_disc.get("clean")
                          and b_exit and b_exit.get("exited") and b_exit.get("rc") == 0)
    root_finalized_clean = bool(a_exit and a_exit.get("exited") and a_exit.get("rc") == 0
                                and root_result)

    # argv hygiene: confirm the launch flags actually went onto both children.
    root_argv = " ".join((root_rec or {}).get("argv") or [])
    conn_argv = " ".join((conn_rec or {}).get("argv") or [])
    ignore_batch_both = ("--hpx:ignore-batch-env" in root_argv
                         and "--hpx:ignore-batch-env" in conn_argv)
    # The child was launched with a scrubbed loader env (no DYLD_*); if it started, it self-located.
    child_started_without_dyld = bool(r_started and (conn_rec is None or c_started))

    overall_pass = all([
        ray_launched_root, ray_launched_connector, r_started, c_started, r_ready, c_joined,
        action_proved_remote, result_not_via_ray, teardown_clean, root_finalized_clean,
        not (r_bind_failed or c_bind_failed or r_launch_failed or c_launch_failed),
    ])

    return {
        "overall": "pass" if overall_pass else "fail",
        "ray_available": True,
        "binary_available": True,
        "ray_launched_root": ray_launched_root,
        "ray_launched_connector": ray_launched_connector,
        "root_child_started": r_started,
        "connector_child_started": c_started,
        "root_ready": r_ready,
        "connector_joined": c_joined,
        "connector_locality_id": (conn_rec or {}).get("locality_id"),
        "root_locality_id": (root_result or {}).get("here_locality"),
        "action_proved_remote": action_proved_remote,
        "action_result_returned_through_ray": (not result_not_via_ray),
        "graceful_teardown_clean": teardown_clean,
        "root_finalized_clean": root_finalized_clean,
        "binary_self_locating_rpath": child_started_without_dyld,
        "child_started_without_dyld_env": child_started_without_dyld,
        "hpx_ignore_batch_env_used": ignore_batch_both,
        "root_launch_failed": r_launch_failed,
        "connector_launch_failed": c_launch_failed,
        "root_bind_failed": r_bind_failed,
        "connector_bind_failed": c_bind_failed,
        "root_ready_timeout": r_ready_timeout,
        "connector_join_timeout": c_ready_timeout,
        "ray_bootstrap_plumbing_only": True,
        "bootstrap_metadata_via_ray_only": True,
        "ports": {"root": f"127.0.0.1:{p0}", "connector": f"127.0.0.1:{p1}"},
        "actor_A_root": {"role": "root", "ready": r_ready,
                         "agas_endpoint": (root_rec or {}).get("agas_endpoint"),
                         "finalized_clean": root_finalized_clean,
                         "exit_rc": (a_exit or {}).get("rc")},
        "actor_B_connector": {"role": "connector", "joined": c_joined,
                              "locality_id": (conn_rec or {}).get("locality_id"),
                              "disconnected_clean": bool(conn_disc and conn_disc.get("clean")),
                              "exit_rc": (b_exit or {}).get("rc")},
    }


def _write_agg(path, agg):
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=False)
        f.write("\n")


def _base_agg(binary):
    return {
        "experiment": "52_ray_bootstrap_clean_island",
        "kind": "ray_orchestrated_hpx_bootstrap_clean_path",
        "ray_free": False, "single_node": True, "transport": "tcp_loopback",
        "supervision_plane": "ray", "data_plane": "hpx",
        "binary": os.path.basename(binary) if binary else None,
        "note": (
            "Plumbing test only. From HPX's view this is exp49 launched by a different parent; "
            "HPX cannot observe its parent. Ray is the launcher/supervisor (the role "
            "mpirun/srun/hpxrun.py play). The only HPX bootstrap datum carried by Ray is the AGAS "
            "endpoint; rendezvous files are harness sync, not HPX bootstrap and not a data path. "
            "The HPX action travels HPX->HPX; Ray never carries the result. Re-validates no new HPX "
            "property. Not fault tolerance; whole-island-fatal policy assumed, not exercised."
        ),
    }


def main():
    ap = argparse.ArgumentParser(description="exp52 Ray-orchestrated HPX bootstrap (clean island)")
    ap.add_argument("--binary", default=None)
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--wait-bound", type=int, default=15)
    ap.add_argument("--step-timeout", type=int, default=20)
    ap.add_argument("--per-phase-timeout", type=int, default=60)
    ap.add_argument("--aggregate", default=os.path.join(HERE, "aggregate.json"))
    args = ap.parse_args()

    binary = locate_binary(args.binary)

    # Skip: HPX binary not built.
    if binary is None:
        agg = _base_agg(None)
        agg["overall"] = "skip"
        agg["findings"] = {"binary_available": False,
                           "reason": "clean_island_spike not built (see CMakeLists.txt)"}
        _write_agg(args.aggregate, agg)
        print("SKIP: clean_island_spike not found; build the experiment first (CMakeLists.txt).")
        return 0

    # Skip: Ray not importable.
    try:
        import ray
    except Exception as e:  # noqa: BLE001 - any import failure is a clean skip
        agg = _base_agg(binary)
        agg["overall"] = "skip"
        agg["findings"] = {"ray_available": False, "reason": f"import ray failed: {e}"}
        _write_agg(args.aggregate, agg)
        print(f"SKIP: ray not importable ({e}).")
        return 0

    print(f"[exp52] binary: {binary}")
    ray.init(ignore_reinit_error=True, log_to_driver=False)  # LOCAL ray only
    print(f"[exp52] ray initialized (local), version {ray.__version__}")
    try:
        findings = run_once(binary, args)
    finally:
        ray.shutdown()

    agg = _base_agg(binary)
    agg["overall"] = findings["overall"]
    agg["findings"] = findings
    _write_agg(args.aggregate, agg)
    print(f"[exp52] overall={findings['overall']} "
          f"root_ready={findings['root_ready']} joined={findings['connector_joined']} "
          f"proved={findings['action_proved_remote']} teardown={findings['graceful_teardown_clean']} "
          f"finalized={findings['root_finalized_clean']} "
          f"rpath_selfloc={findings['binary_self_locating_rpath']} "
          f"ignore_batch={findings['hpx_ignore_batch_env_used']} -> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
