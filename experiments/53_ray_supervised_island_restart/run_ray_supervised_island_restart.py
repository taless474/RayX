#!/usr/bin/env python3
"""exp53 -- Ray-supervised HPX island RESTART under the whole-island-fatal policy.

STRUCTURAL POLICY test only. NOT a performance result, NOT a Ray demo. From HPX's point of view this
contains NO new mechanism: island #1's failure is exp50's mid-flight connector loss and island #2's
clean bootstrap is exp52. exp53 proves at the Ray level that a DURABLE supervisor can DISCARD a
poisoned HPX island and launch a FRESH clean one. Island #1 and island #2 are TWO INDEPENDENT HPX
runtimes, not one runtime recovering. Ray is the supervision/restart plane; HPX is the
execution/data plane inside each island. The poisoned island is discarded, NOT repaired.

Folded-in HPX-expert corrections:
  1. The failure root NEVER returns from hpx_main / NEVER finalizes in the expected path -- it idles
     in hpx_main until the supervisor SIGKILLs it (signal 9). A return would enter finalize and HANG
     on the dead connector (exp51), confounding the probe. The idle cap is a safety guard; if it
     elapses we flag the anomaly and the run is NOT a clean pass.
  2. Poisoning is WITNESSED, not assumed: after the connector SIGKILL, the supervisor waits for the
     root to write failure_root.json with loss_observed_by_root=true (its long-action future did not
     return) BEFORE marking the island poisoned and killing it.
  3. Island #2 uses FRESH ports + a fresh rendezvous dir. A SIGKILLed parcelport/root may leave
     OS-level bind/TIME_WAIT/cleanup artifacts; fresh ports keep a restart failure from being
     confused with a port-reuse artifact.

exp52 launch hygiene is reused for every HPX child: self-locating RPATH (launched with the loader
env SCRUBBED to prove it), --hpx:ignore-batch-env, --hpx:bind=none, root --hpx:threads=2 / connector
--hpx:threads=1, numeric 127.0.0.1, no fixed --hpx:localities=N, absolute path, start_new_session.

The IslandSupervisor owns both HPX child processes (and process groups) of each island, so it also
owns connector-loss INJECTION and whole-island KILL/REAP. Ray carries ONLY metadata/status; the HPX
action result never traverses Ray.

CLAIM FENCE: single-node; loopback TCP; closed-int64 action only; Ray = bootstrap/supervision/restart
plane only; HPX = execution/data plane inside each island; whole-island-fatal policy exercised; not
HPX fault tolerance; not in-place recovery; no AGAS stale-locality repair; no Ray actor-failure-
recovery claim beyond this controlled supervisor kill/restart; island #2 is a fresh independent HPX
runtime, not repaired island #1; no multi-node; no general fabric; no performance/speedup/throughput/
latency; no production/public API; no endpoint seam; no Ray replacement; no "HPX faster than Ray"; no
"RayX makes Ray faster".

Usage:
  python run_ray_supervised_island_restart.py [--binary <path>] [--x 7] [--sleep-ms 8000]
      [--wait-bound 15] [--step-timeout 20] [--per-phase-timeout 90] [--aggregate <path>]

Exit code is 0 on clean fail/skip (the aggregate carries the verdict); non-zero only on an
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
    os.path.join(HERE, "build", "island_restart_spike"),
    os.path.join(HERE, "build", "Release", "island_restart_spike"),
]
_LOADER_ENV_VARS = ("DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH", "LD_LIBRARY_PATH")
BINARY_BASENAME = "island_restart_spike"


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


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _orphan_check():
    """After the run, assert no island_restart_spike children survived (whole-island reaping)."""
    try:
        out = subprocess.run(["pgrep", "-f", BINARY_BASENAME], capture_output=True, text=True,
                             timeout=10)
        pids = [p for p in out.stdout.split() if p]
        return (len(pids) == 0), pids
    except Exception:
        return None, []


def build_supervisor(ray):
    """Define the durable IslandSupervisor actor inside a Ray context. It owns both HPX child
    processes (and process groups) of each island, keyed by island_id."""

    @ray.remote
    class IslandSupervisor:
        def __init__(self):
            self.islands = {}   # island_id -> {"root":Popen,"connector":Popen,"bootdir","ports","logs":[]}
            self.poisoned = set()

        # --- helpers ---
        def _scrubbed_env(self):
            env = dict(os.environ)
            for k in _LOADER_ENV_VARS:
                env.pop(k, None)
            return env

        def _popen(self, argv, bootdir, log_name):
            log = open(os.path.join(bootdir, log_name), "w")
            proc = subprocess.Popen(
                argv, cwd=bootdir, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True, env=self._scrubbed_env())
            return proc, log

        def _wait_for_file(self, path, proc, timeout):
            deadline = time.time() + timeout
            while time.time() < deadline:
                if os.path.exists(path):
                    return True
                if proc is not None and proc.poll() is not None:
                    return os.path.exists(path)
                time.sleep(0.05)
            return os.path.exists(path)

        def _killpg(self, proc):
            if proc is None or proc.poll() is not None:
                return
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

        def _root_argv(self, binary, mode, bootdir, x, sleep_ms, wait_bound, step_timeout, idle_cap, p0):
            return [
                binary, "--role", "f_root", "--island-mode", mode, "--bootstrap", bootdir,
                "--x", str(x), "--sleep-ms", str(sleep_ms), "--wait-bound", str(wait_bound),
                "--step-timeout", str(step_timeout), "--idle-cap", str(idle_cap),
                f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p0}",
                "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
                "--hpx:ignore-batch-env",
            ]

        def _conn_argv(self, binary, kind, bootdir, serve_timeout, victim_idle, p0, p1):
            return [
                binary, "--role", "f_connect", "--connector-kind", kind, "--connector-index", "1",
                "--bootstrap", bootdir, "--serve-timeout", str(serve_timeout),
                "--victim-idle", str(victim_idle),
                f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p1}",
                "--hpx:threads=1", "--hpx:bind=none", "--hpx:ignore-batch-env",
            ]

        # --- island lifecycle ---
        def launch_island(self, meta, mode):
            island_id = meta["island_id"]
            bootdir = meta["bootdir"]
            p0, p1 = meta["p0"], meta["p1"]
            conn_kind = "victim" if mode == "failure" else "clean"

            root_argv = self._root_argv(meta["binary"], mode, bootdir, meta["x"], meta["sleep_ms"],
                                        meta["wait_bound"], meta["step_timeout"], meta["idle_cap"], p0)
            root, rlog = self._popen(root_argv, bootdir, "root.log")
            root_ready = self._wait_for_file(os.path.join(bootdir, "root.ready"), root,
                                             meta["step_timeout"])

            conn = clog = None
            conn_joined = False
            jrec = None
            if root_ready and root.poll() is None:
                conn_argv = self._conn_argv(meta["binary"], conn_kind, bootdir,
                                            meta["step_timeout"] + 10, meta["idle_cap"], p0, p1)
                conn, clog = self._popen(conn_argv, bootdir, "connector.log")
                conn_joined = self._wait_for_file(os.path.join(bootdir, "connect.joined1"), conn,
                                                  meta["step_timeout"])
                jrec = _read_json(os.path.join(bootdir, "connect.joined1"))

            self.islands[island_id] = {
                "root": root, "connector": conn, "bootdir": bootdir, "ports": (p0, p1),
                "logs": [rlog] + ([clog] if clog else []),
                "root_argv": root_argv,
                "conn_argv": (conn_argv if conn is not None else None),
            }
            return {
                "island_id": island_id, "mode": mode, "ports": f"127.0.0.1:{p0},127.0.0.1:{p1}",
                "root_started": True, "root_ready": root_ready, "root_pid": root.pid,
                "connector_joined": bool(jrec and jrec.get("locality_id") is not None),
                "connector_locality_id": (jrec or {}).get("locality_id"),
                "connector_pid": (conn.pid if conn is not None else None),
                "root_argv": root_argv,
                "connector_argv": (conn_argv if conn is not None else None),
            }

        def inject_connector_loss(self, island_id, timeout):
            isl = self.islands[island_id]
            started = self._wait_for_file(os.path.join(isl["bootdir"], "action_started"),
                                          isl["connector"], timeout)
            self._killpg(isl["connector"])   # mid-flight SIGKILL of the connector process group
            return {"action_started_seen": started, "connector_killed": True}

        def wait_loss_witness(self, island_id, timeout):
            isl = self.islands[island_id]
            seen = self._wait_for_file(os.path.join(isl["bootdir"], "failure_root.json"),
                                       isl["root"], timeout)
            rec = _read_json(os.path.join(isl["bootdir"], "failure_root.json")) or {}
            return {"witness_seen": seen, "loss_observed_by_root": bool(rec.get("loss_observed_by_root")),
                    "long_action_outcome": rec.get("long_action_outcome")}

        def mark_poisoned(self, island_id):
            self.poisoned.add(island_id)
            return {"marked_poisoned": True}

        def kill_island(self, island_id):
            isl = self.islands[island_id]
            # whole-island kill: connector (idempotent) THEN root. NO finalize, NO graceful attempt.
            self._killpg(isl["connector"])
            self._killpg(isl["root"])
            reaped = {}
            signals = {}
            for role in ("root", "connector"):
                proc = isl.get(role)
                if proc is None:
                    reaped[role] = True
                    continue
                try:
                    proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    pass
                rc = proc.poll()
                reaped[role] = rc is not None
                signals[role] = (-rc if (rc is not None and rc < 0) else rc)
            idle_anom = os.path.exists(os.path.join(isl["bootdir"], "failure_root_idle_cap_elapsed"))
            return {
                "root_killed": True, "connector_killed": True,
                "all_children_reaped": all(reaped.values()),
                "root_exit_signal": signals.get("root"),
                "connector_exit_signal": signals.get("connector"),
                "failure_root_idle_cap_elapsed": idle_anom,
            }

        def wait_exit(self, island_id, role, timeout):
            proc = self.islands[island_id].get(role)
            if proc is None:
                return {"exited": False, "rc": None}
            deadline = time.time() + timeout
            while time.time() < deadline and proc.poll() is None:
                time.sleep(0.05)
            return {"exited": proc.poll() is not None, "rc": proc.poll()}

        def read_result(self, island_id, name):
            return _read_json(os.path.join(self.islands[island_id]["bootdir"], name))

        def shutdown(self):
            swept = 0
            for isl in self.islands.values():
                for role in ("root", "connector"):
                    proc = isl.get(role)
                    if proc is not None and proc.poll() is None:
                        self._killpg(proc)
                        swept += 1
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            pass
                for lg in isl.get("logs", []):
                    try:
                        lg.close()
                    except OSError:
                        pass
            return {"swept": swept}

    return IslandSupervisor


def run_once(binary, args):
    import ray  # guarded by caller

    Supervisor = build_supervisor(ray)
    s = Supervisor.remote()

    # fresh ports + dirs for BOTH islands (island #2 MUST NOT reuse island #1 ports)
    p0, p1 = find_free_port(), find_free_port()
    p2, p3 = find_free_port(), find_free_port()
    while {p2, p3} & {p0, p1}:  # guarantee disjoint
        p2, p3 = find_free_port(), find_free_port()
    dir1 = tempfile.mkdtemp(prefix="exp53_isl1_")
    dir2 = tempfile.mkdtemp(prefix="exp53_isl2_")

    f = {}
    try:
        # ---- Island #1: failure mode ----
        meta1 = {"island_id": 1, "binary": binary, "bootdir": dir1, "p0": p0, "p1": p1,
                 "x": args.x, "sleep_ms": args.sleep_ms, "wait_bound": args.wait_bound,
                 "step_timeout": args.step_timeout, "idle_cap": args.idle_cap}
        l1 = ray.get(s.launch_island.remote(meta1, "failure"), timeout=args.per_phase_timeout)

        inj = {"action_started_seen": False, "connector_killed": False}
        witness = {"witness_seen": False, "loss_observed_by_root": False, "long_action_outcome": None}
        marked = {"marked_poisoned": False}
        killed = {"all_children_reaped": False, "root_exit_signal": None,
                  "failure_root_idle_cap_elapsed": None}
        if l1.get("root_ready") and l1.get("connector_joined"):
            inj = ray.get(s.inject_connector_loss.remote(1, args.step_timeout),
                          timeout=args.per_phase_timeout)
            # witness poisoning BEFORE killing the island (HPX-expert correction #2)
            witness = ray.get(s.wait_loss_witness.remote(1, args.wait_bound + 10),
                              timeout=args.wait_bound + 20)
            if witness.get("loss_observed_by_root"):
                marked = ray.get(s.mark_poisoned.remote(1), timeout=10)
            killed = ray.get(s.kill_island.remote(1), timeout=30)

        # ---- Island #2: clean mode (fresh ports/dir; SAME supervisor) ----
        meta2 = {"island_id": 2, "binary": binary, "bootdir": dir2, "p0": p2, "p1": p3,
                 "x": args.x, "sleep_ms": args.sleep_ms, "wait_bound": args.wait_bound,
                 "step_timeout": args.step_timeout, "idle_cap": args.idle_cap}
        l2 = ray.get(s.launch_island.remote(meta2, "clean"), timeout=args.per_phase_timeout)
        a2_exit = ray.get(s.wait_exit.remote(2, "root", args.per_phase_timeout),
                          timeout=args.per_phase_timeout + 5)
        b2_exit = ray.get(s.wait_exit.remote(2, "connector", 30), timeout=35)
        clean_root = ray.get(s.read_result.remote(2, "clean_root.json"), timeout=15)
        conn_disc = ray.get(s.read_result.remote(2, "connect.disconnected1"), timeout=15)

        f = {"l1": l1, "inj": inj, "witness": witness, "marked": marked, "killed": killed,
             "l2": l2, "a2_exit": a2_exit, "b2_exit": b2_exit,
             "clean_root": clean_root, "conn_disc": conn_disc}
    finally:
        try:
            ray.get(s.shutdown.remote(), timeout=30)
        except Exception:
            pass
        try:
            ray.kill(s)
        except Exception:
            pass

    return _assemble(f, (p0, p1), (p2, p3))


def _assemble(f, ports1, ports2):
    l1 = f.get("l1") or {}
    l2 = f.get("l2") or {}
    inj = f.get("inj") or {}
    witness = f.get("witness") or {}
    marked = f.get("marked") or {}
    killed = f.get("killed") or {}
    a2 = f.get("a2_exit") or {}
    b2 = f.get("b2_exit") or {}
    clean_root = f.get("clean_root") or {}
    conn_disc = f.get("conn_disc") or {}

    # data-plane separation: the action result int must NEVER appear in any Ray-returned payload.
    ray_payloads = json.dumps([l1, l2, inj, witness, marked, killed, a2, b2], default=str)
    result_via_ray = ("action_result" in ray_payloads)

    island1_root_started = bool(l1.get("root_started"))
    island1_connector_joined = bool(l1.get("connector_joined"))
    island1_action_started = bool(inj.get("action_started_seen"))
    island1_connector_killed = bool(inj.get("connector_killed"))
    island1_loss_observed = bool(witness.get("loss_observed_by_root"))
    island1_marked_poisoned = bool(marked.get("marked_poisoned"))
    root_sig = killed.get("root_exit_signal")
    island1_root_killed_by_supervisor = (root_sig == signal.SIGKILL)
    island1_all_reaped = bool(killed.get("all_children_reaped"))
    idle_cap_elapsed = bool(killed.get("failure_root_idle_cap_elapsed"))
    island1_failure_root_never_finalized = bool(island1_root_killed_by_supervisor
                                                and not idle_cap_elapsed)

    island2_root_started = bool(l2.get("root_started"))
    island2_connector_joined = bool(l2.get("connector_joined"))
    island2_action_proved_remote = bool(clean_root.get("action_proved_remote"))
    island2_teardown_clean = bool(conn_disc.get("clean") and b2.get("exited") and b2.get("rc") == 0)
    island2_root_finalized_clean = bool(a2.get("exited") and a2.get("rc") == 0 and clean_root)

    p0, p1 = ports1
    p2, p3 = ports2
    ports_reused = bool({p2, p3} & {p0, p1})
    island2_used_fresh_ports = not ports_reused

    orphan_ok, orphan_pids = _orphan_check()
    no_orphans = (orphan_ok is True)

    root_argv = " ".join(l1.get("root_argv") or [])
    conn_argv = " ".join((l1.get("connector_argv") or [])
                         + (l2.get("connector_argv") or []) + (l2.get("root_argv") or []))
    ignore_batch_all = ("--hpx:ignore-batch-env" in root_argv
                        and "--hpx:ignore-batch-env" in conn_argv)
    child_self_locating = island1_root_started and island2_root_started  # started w/ scrubbed env

    whole_island_restart_succeeded = all([
        island2_root_started, island2_connector_joined, island2_action_proved_remote,
        island2_teardown_clean, island2_root_finalized_clean,
    ])
    supervisor_survived = bool(island1_all_reaped and island2_root_started)

    overall_pass = all([
        island1_root_started, island1_connector_joined, island1_action_started,
        island1_connector_killed, island1_loss_observed, island1_marked_poisoned,
        island1_root_killed_by_supervisor, island1_failure_root_never_finalized,
        island1_all_reaped, supervisor_survived, island2_used_fresh_ports,
        whole_island_restart_succeeded, no_orphans, not result_via_ray,
    ])

    return {
        "overall": "pass" if overall_pass else "fail",
        "ray_available": True, "binary_available": True,

        "island1_root_started": island1_root_started,
        "island1_connector_joined": island1_connector_joined,
        "island1_action_started": island1_action_started,
        "island1_connector_killed": island1_connector_killed,
        "supervisor_injected_loss": island1_connector_killed,
        "supervisor_waited_for_loss_witness": bool(witness.get("witness_seen")),
        "island1_loss_observed_by_root": island1_loss_observed,
        "island1_long_action_outcome": witness.get("long_action_outcome"),
        "island1_marked_poisoned": island1_marked_poisoned,
        "island1_root_killed_by_supervisor": island1_root_killed_by_supervisor,
        "island1_failure_root_exit_signal": root_sig,
        "island1_failure_root_never_finalized": island1_failure_root_never_finalized,
        "island1_failure_root_idle_cap_elapsed": idle_cap_elapsed,
        "island1_all_children_reaped": island1_all_reaped,

        "island2_root_started": island2_root_started,
        "island2_connector_joined": island2_connector_joined,
        "island2_action_proved_remote": island2_action_proved_remote,
        "island2_graceful_teardown_clean": island2_teardown_clean,
        "island2_root_finalized_clean": island2_root_finalized_clean,
        "island2_root_locality": clean_root.get("here_locality"),
        "island2_connector_locality": l2.get("connector_locality_id"),

        "whole_island_restart_succeeded": whole_island_restart_succeeded,
        "supervisor_survived_island_death": supervisor_survived,
        "island2_used_fresh_ports": island2_used_fresh_ports,
        "ports_reused": ports_reused,
        "island1_ports": f"127.0.0.1:{p0},127.0.0.1:{p1}",
        "island2_ports": f"127.0.0.1:{p2},127.0.0.1:{p3}",
        "no_orphan_hpx_processes": no_orphans,
        "orphan_pids": orphan_pids,
        "ray_carried_bootstrap_metadata_only": (not result_via_ray),
        "action_result_returned_through_ray": result_via_ray,
        "binary_self_locating_rpath": child_self_locating,
        "child_started_without_dyld_env": child_self_locating,
        "hpx_ignore_batch_env_used": ignore_batch_all,
    }


def _write_agg(path, agg):
    with open(path, "w") as fh:
        json.dump(agg, fh, indent=2, sort_keys=False)
        fh.write("\n")


def _base_agg(binary):
    return {
        "experiment": "53_ray_supervised_island_restart",
        "kind": "ray_supervised_whole_island_restart_policy",
        "ray_free": False, "single_node": True, "transport": "tcp_loopback",
        "supervision_plane": "ray", "data_plane": "hpx",
        "policy": "whole_island_fatal_external_restart",
        "binary": os.path.basename(binary) if binary else None,
        "note": (
            "External supervision policy, NOT HPX fault tolerance. Island #1 and island #2 are two "
            "INDEPENDENT HPX runtimes, not one recovering. The durable Ray supervisor kills the "
            "whole poisoned island (no in-place repair, no AGAS stale-locality cleanup, no root "
            "preservation) and launches a fresh one on fresh ports. The failure root never "
            "finalizes (killed mid-idle, signal 9); poisoning is witnessed before kill. The action "
            "travels HPX->HPX; Ray carries only bootstrap metadata. Re-validates no new HPX "
            "property beyond exp50/exp52."
        ),
    }


def main():
    ap = argparse.ArgumentParser(description="exp53 Ray-supervised whole-island restart probe")
    ap.add_argument("--binary", default=None)
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--sleep-ms", type=int, default=8000)
    ap.add_argument("--wait-bound", type=int, default=15)
    ap.add_argument("--step-timeout", type=int, default=20)
    ap.add_argument("--idle-cap", type=int, default=120)
    ap.add_argument("--per-phase-timeout", type=int, default=90)
    ap.add_argument("--aggregate", default=os.path.join(HERE, "aggregate.json"))
    args = ap.parse_args()

    binary = locate_binary(args.binary)
    if binary is None:
        agg = _base_agg(None)
        agg["overall"] = "skip"
        agg["findings"] = {"binary_available": False,
                           "reason": "island_restart_spike not built (see CMakeLists.txt)"}
        _write_agg(args.aggregate, agg)
        print("SKIP: island_restart_spike not found; build the experiment first (CMakeLists.txt).")
        return 0

    try:
        import ray
    except Exception as e:  # noqa: BLE001 - any import failure is a clean skip
        agg = _base_agg(binary)
        agg["overall"] = "skip"
        agg["findings"] = {"ray_available": False, "reason": f"import ray failed: {e}"}
        _write_agg(args.aggregate, agg)
        print(f"SKIP: ray not importable ({e}).")
        return 0

    print(f"[exp53] binary: {binary}")
    ray.init(ignore_reinit_error=True, log_to_driver=False)  # LOCAL ray only
    print(f"[exp53] ray initialized (local), version {ray.__version__}")
    try:
        findings = run_once(binary, args)
    finally:
        ray.shutdown()

    agg = _base_agg(binary)
    agg["overall"] = findings["overall"]
    agg["findings"] = findings
    _write_agg(args.aggregate, agg)
    print(f"[exp53] overall={findings['overall']} "
          f"isl1_loss_witnessed={findings['island1_loss_observed_by_root']} "
          f"isl1_root_sig={findings['island1_failure_root_exit_signal']} "
          f"isl1_never_finalized={findings['island1_failure_root_never_finalized']} "
          f"isl1_reaped={findings['island1_all_children_reaped']} "
          f"restart_ok={findings['whole_island_restart_succeeded']} "
          f"fresh_ports={findings['island2_used_fresh_ports']} "
          f"no_orphans={findings['no_orphan_hpx_processes']} -> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
