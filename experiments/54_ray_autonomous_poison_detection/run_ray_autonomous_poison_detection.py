#!/usr/bin/env python3
"""exp54 -- autonomous Ray-side poisoned-island DETECTION.

STRUCTURAL DETECTION-LOGIC test only. NOT a performance result, NOT a Ray demo. From HPX's point of
view this contains NO new mechanism: the failure path is exp50's connector loss and the clean path
is exp52/exp53. exp54's point is that the Ray supervisor cannot query HPX for authoritative locality
health (exp51 found no such API), so it infers island poisoning from OS process liveness + bounded
progress/completion markers.

PRIMARY CORRECTION vs exp53: island #1's connector death is OBSERVED, not CAUSED, by the supervisor.
The connector SELF-CRASHES (std::abort -> SIGABRT/signal 6) a fixed delay after join, so the
supervisor observes an UNCAUSED death via Popen.poll(). The supervisor SIGKILL is used ONLY for
cleanup/reap AFTER poison classification, never as the cause of the connector failure.

UNIFORM DETECTOR PREDICATE -- implemented ONCE and run for BOTH the clean control (island #0) and the
failure island (island #1). For any island, classify POISONED iff all hold:
    connector_not_alive AND connector_clean_disconnect_absent
    AND (clean_completion_within_T == False) AND root_not_cleanly_exited
The predicate never references "I killed it", never reads `action_started`, and is not special-cased
to island #1. `action_started` is recorded only as the diagnostic `action_was_in_flight`.

Sequence: island #0 clean-control (detector must NOT fire, evaluated on the same bounded timeline) ->
island #1 failure (self-crash connector; detector fires) -> kill/reap island #1 -> island #2 clean
restart proof on fresh disjoint ports.

exp52/53 launch hygiene for every HPX child: self-locating RPATH (launched with the loader env
SCRUBBED to prove it), --hpx:ignore-batch-env, --hpx:bind=none, root --hpx:threads=2 / connector
--hpx:threads=1, numeric 127.0.0.1, no fixed --hpx:localities=N, absolute path, start_new_session.

CLAIM FENCE: single-node; loopback TCP; closed-int64 action only; Ray = bootstrap/supervision/
restart/detection plane only; HPX = execution/data plane inside each island; supervisor-owned
poisoned-island detection from OS process liveness + bounded progress markers; connector death
observed, not caused by the supervisor, in the primary arm; not HPX fault tolerance; not in-place
recovery; no AGAS stale-locality repair; no Ray actor-failure-recovery claim; island #2 is a fresh
independent HPX runtime, not repaired island #1; no multi-node; no general fabric; no performance/
speedup/throughput/latency; no production/public API; no endpoint seam; no Ray replacement; no "HPX
faster than Ray"; no "RayX makes Ray faster".

Usage:
  python run_ray_autonomous_poison_detection.py [--binary <path>] [--x 7] [--sleep-ms 8000]
      [--wait-bound 15] [--step-timeout 20] [--crash-delay-ms 2000] [--root-progress-timeout 12]
      [--per-phase-timeout 90] [--aggregate <path>]

Exit code 0 on clean fail/skip; non-zero only on an orchestrator-internal error.
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
    os.path.join(HERE, "build", "autonomous_poison_spike"),
    os.path.join(HERE, "build", "Release", "autonomous_poison_spike"),
]
_LOADER_ENV_VARS = ("DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH", "LD_LIBRARY_PATH")
BINARY_BASENAME = "autonomous_poison_spike"


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
    try:
        out = subprocess.run(["pgrep", "-f", BINARY_BASENAME], capture_output=True, text=True,
                             timeout=10)
        pids = [p for p in out.stdout.split() if p]
        return (len(pids) == 0), pids
    except Exception:
        return None, []


def build_supervisor(ray):
    """One durable IslandSupervisor owning all child Popens. Holds the UNIFORM detector predicate
    implemented ONCE (evaluate_health_predicate) and run for every island."""

    @ray.remote
    class IslandSupervisor:
        def __init__(self):
            self.islands = {}   # id -> {"root","connector","bootdir","ports","logs"}

        # --- launch helpers ---
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

        def _conn_argv(self, binary, kind, bootdir, serve_timeout, crash_delay_ms, p0, p1):
            return [
                binary, "--role", "f_connect", "--connector-kind", kind, "--connector-index", "1",
                "--bootstrap", bootdir, "--serve-timeout", str(serve_timeout),
                "--crash-delay-ms", str(crash_delay_ms),
                f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p1}",
                "--hpx:threads=1", "--hpx:bind=none", "--hpx:ignore-batch-env",
            ]

        def launch_island(self, meta, mode, connector_kind):
            island_id = meta["island_id"]
            bootdir, p0, p1 = meta["bootdir"], meta["p0"], meta["p1"]
            root_argv = self._root_argv(meta["binary"], mode, bootdir, meta["x"], meta["sleep_ms"],
                                        meta["wait_bound"], meta["step_timeout"], meta["idle_cap"], p0)
            root, rlog = self._popen(root_argv, bootdir, "root.log")
            root_ready = self._wait_for_file(os.path.join(bootdir, "root.ready"), root,
                                             meta["step_timeout"])
            conn = clog = conn_argv = None
            jrec = None
            if root_ready and root.poll() is None:
                conn_argv = self._conn_argv(meta["binary"], connector_kind, bootdir,
                                            meta["step_timeout"] + 10, meta["crash_delay_ms"], p0, p1)
                conn, clog = self._popen(conn_argv, bootdir, "connector.log")
                self._wait_for_file(os.path.join(bootdir, "connect.joined1"), conn,
                                    meta["step_timeout"])
                jrec = _read_json(os.path.join(bootdir, "connect.joined1"))
            self.islands[island_id] = {
                "root": root, "connector": conn, "bootdir": bootdir, "ports": (p0, p1),
                "logs": [rlog] + ([clog] if clog else []),
                "root_argv": root_argv, "conn_argv": conn_argv,
            }
            return {
                "island_id": island_id, "mode": mode, "connector_kind": connector_kind,
                "root_started": True, "root_ready": root_ready, "root_pid": root.pid,
                "connector_joined": bool(jrec and jrec.get("locality_id") is not None),
                "connector_locality_id": (jrec or {}).get("locality_id"),
                "connector_pid": (conn.pid if conn is not None else None),
                "root_argv": root_argv, "connector_argv": conn_argv,
            }

        # --- UNIFORM detector predicate (run for EVERY island; no per-island special case) ---
        def evaluate_health_predicate(self, island_id, root_progress_timeout):
            """Classify an island from OS process liveness + bounded progress/completion markers.
            Returns a neutral signal dict. The predicate never references how the connector died,
            and never reads `action_started`."""
            isl = self.islands[island_id]
            bootdir = isl["bootdir"]
            root, conn = isl["root"], isl["connector"]
            clean_path = os.path.join(bootdir, "clean_root.json")
            disc_path = os.path.join(bootdir, "connect.disconnected1")

            def snapshot():
                conn_rc = conn.poll() if conn is not None else 0
                not_alive = (conn is not None and conn_rc is not None)
                clean_disc = os.path.exists(disc_path)
                root_rc = root.poll()
                clean_done = os.path.exists(clean_path)
                # poison condition: connector dead, no clean leave, root not cleanly exited
                poison = (not_alive and (not clean_disc) and (root_rc != 0) and (not clean_done))
                return conn_rc, not_alive, clean_disc, root_rc, clean_done, poison

            # Bounded eval window. Decisive EARLY: break as soon as either a clean completion marker
            # appears (clean island) OR the poison condition holds (connector dead, no clean leave).
            # If neither resolves within T, fall through as indeterminate. Same predicate for all
            # islands -- no per-island special case, never reads `action_started`.
            deadline = time.time() + root_progress_timeout
            clean_completion = False
            poisoned = False
            while time.time() < deadline:
                _, _, _, _, clean_done, poison = snapshot()
                if clean_done:
                    clean_completion = True
                    break
                if poison:
                    poisoned = True
                    break
                time.sleep(0.05)

            conn_rc, connector_not_alive, clean_disc, root_rc, clean_done, _ = snapshot()
            clean_completion = clean_completion or clean_done
            connector_exit_status = conn_rc
            connector_exit_signal = (-conn_rc if (conn_rc is not None and conn_rc < 0) else None)
            # self-crash = exited via a signal that the supervisor did NOT send (it only sends
            # SIGKILL during kill_island, which happens AFTER this predicate). On this HPX build
            # std::abort() surfaces as SIGILL(4) (HPX's installed abort/terminate handler traps)
            # rather than SIGABRT(6); accept the whole ungraceful-crash signal family.
            connector_self_crashed = bool(connector_exit_signal is not None
                                          and connector_exit_signal not in (signal.SIGKILL,
                                                                            signal.SIGTERM))
            connector_clean_disconnect_absent = not clean_disc
            root_alive = (root_rc is None)
            root_cleanly_exited = (root_rc == 0)
            root_not_cleanly_exited = not root_cleanly_exited
            # final classification (recompute so a late transition is reflected)
            poisoned = poisoned or (connector_not_alive and connector_clean_disconnect_absent
                                    and (not clean_completion) and root_not_cleanly_exited)
            classification = "clean_complete" if clean_completion else (
                "poisoned" if poisoned else "indeterminate")

            # diagnostic-only (NOT a classifier input)
            action_was_in_flight = os.path.exists(os.path.join(bootdir, "action_started"))

            return {
                "connector_not_alive": connector_not_alive,
                "connector_exit_status": connector_exit_status,
                "connector_exit_signal": connector_exit_signal,
                "connector_self_crashed": connector_self_crashed,
                "connector_clean_disconnect_absent": connector_clean_disconnect_absent,
                "clean_completion_within_T": clean_completion,
                "clean_root_result_absent": not clean_completion,
                "root_alive_after_connector_death": root_alive,
                "root_not_cleanly_exited": root_not_cleanly_exited,
                "root_progress_timeout_elapsed": not clean_completion,
                "poisoned": poisoned,
                "classification": classification,
                "action_was_in_flight": action_was_in_flight,  # diagnostic only
            }

        def wait_connector_exit(self, island_id, timeout):
            conn = self.islands[island_id].get("connector")
            if conn is None:
                return {"exited": False, "rc": None}
            deadline = time.time() + timeout
            while time.time() < deadline and conn.poll() is None:
                time.sleep(0.05)
            rc = conn.poll()
            return {"exited": rc is not None, "rc": rc,
                    "signal": (-rc if (rc is not None and rc < 0) else None)}

        def kill_island(self, island_id):
            isl = self.islands[island_id]
            self._killpg(isl["connector"])   # idempotent (already dead if self-crashed)
            self._killpg(isl["root"])        # SIGKILL root AFTER poison classification
            reaped, signals = {}, {}
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
            return {"all_children_reaped": all(reaped.values()),
                    "root_exit_signal": signals.get("root"),
                    "failure_root_idle_cap_elapsed": idle_anom}

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


def _fresh_ports(used):
    while True:
        a, b = find_free_port(), find_free_port()
        if a != b and a not in used and b not in used:
            return a, b


def run_once(binary, args):
    import ray  # guarded by caller

    Supervisor = build_supervisor(ray)
    s = Supervisor.remote()

    used = set()
    p0c, p1c = _fresh_ports(used); used |= {p0c, p1c}     # island #0 control
    p0, p1 = _fresh_ports(used); used |= {p0, p1}          # island #1 failure
    p2, p3 = _fresh_ports(used); used |= {p2, p3}          # island #2 restart
    dir0 = tempfile.mkdtemp(prefix="exp54_isl0_")
    dir1 = tempfile.mkdtemp(prefix="exp54_isl1_")
    dir2 = tempfile.mkdtemp(prefix="exp54_isl2_")

    def meta(island_id, bootdir, pa, pb):
        return {"island_id": island_id, "binary": binary, "bootdir": bootdir, "p0": pa, "p1": pb,
                "x": args.x, "sleep_ms": args.sleep_ms, "wait_bound": args.wait_bound,
                "step_timeout": args.step_timeout, "idle_cap": args.idle_cap,
                "crash_delay_ms": args.crash_delay_ms}

    f = {}
    try:
        # ---- Island #0: CLEAN CONTROL (detector must NOT fire, same bounded timeline) ----
        c0 = ray.get(s.launch_island.remote(meta(0, dir0, p0c, p1c), "clean", "clean"),
                     timeout=args.per_phase_timeout)
        # run the SAME predicate on the SAME root_progress_timeout clock as island #1 will use
        ctrl_pred = ray.get(s.evaluate_health_predicate.remote(0, args.root_progress_timeout),
                            timeout=args.root_progress_timeout + 10)
        c0_root_exit = ray.get(s.wait_exit.remote(0, "root", args.per_phase_timeout),
                              timeout=args.per_phase_timeout + 5)
        c0_clean = ray.get(s.read_result.remote(0, "clean_root.json"), timeout=10)
        ray.get(s.kill_island.remote(0), timeout=20)  # clean control reaped after eval

        # ---- Island #1: FAILURE (self-crash connector; detector fires) ----
        l1 = ray.get(s.launch_island.remote(meta(1, dir1, p0, p1), "failure", "self_crash"),
                     timeout=args.per_phase_timeout)
        # observe the UNCAUSED connector death (we did NOT kill it)
        conn_exit = ray.get(s.wait_connector_exit.remote(1, args.crash_delay_ms / 1000.0 + 20),
                           timeout=args.crash_delay_ms / 1000.0 + 25)
        pred1 = ray.get(s.evaluate_health_predicate.remote(1, args.root_progress_timeout),
                       timeout=args.root_progress_timeout + 10)
        # kill/reap the poisoned island AFTER classification (SIGKILL is cleanup, not the cause)
        killed1 = ray.get(s.kill_island.remote(1), timeout=30)

        # ---- Island #2: CLEAN RESTART proof (fresh disjoint ports/dir; same supervisor) ----
        l2 = ray.get(s.launch_island.remote(meta(2, dir2, p2, p3), "clean", "clean"),
                     timeout=args.per_phase_timeout)
        a2_exit = ray.get(s.wait_exit.remote(2, "root", args.per_phase_timeout),
                          timeout=args.per_phase_timeout + 5)
        b2_exit = ray.get(s.wait_exit.remote(2, "connector", 30), timeout=35)
        clean_root2 = ray.get(s.read_result.remote(2, "clean_root.json"), timeout=15)
        conn_disc2 = ray.get(s.read_result.remote(2, "connect.disconnected1"), timeout=15)

        f = {"c0": c0, "ctrl_pred": ctrl_pred, "c0_root_exit": c0_root_exit, "c0_clean": c0_clean,
             "l1": l1, "conn_exit": conn_exit, "pred1": pred1, "killed1": killed1,
             "l2": l2, "a2_exit": a2_exit, "b2_exit": b2_exit,
             "clean_root2": clean_root2, "conn_disc2": conn_disc2}
    finally:
        try:
            ray.get(s.shutdown.remote(), timeout=30)
        except Exception:
            pass
        try:
            ray.kill(s)
        except Exception:
            pass

    return _assemble(f, (p0c, p1c), (p0, p1), (p2, p3))


def _assemble(f, ports0, ports1, ports2):
    c0 = f.get("c0") or {}
    ctrl = f.get("ctrl_pred") or {}
    c0_clean = f.get("c0_clean") or {}
    l1 = f.get("l1") or {}
    conn_exit = f.get("conn_exit") or {}
    pred1 = f.get("pred1") or {}
    killed1 = f.get("killed1") or {}
    l2 = f.get("l2") or {}
    a2 = f.get("a2_exit") or {}
    b2 = f.get("b2_exit") or {}
    clean_root2 = f.get("clean_root2") or {}
    conn_disc2 = f.get("conn_disc2") or {}

    # ---- control island #0 ----
    control_root_started = bool(c0.get("root_started"))
    control_connector_joined = bool(c0.get("connector_joined"))
    control_poison_detected = bool(ctrl.get("poisoned"))
    control_clean_completion = bool(ctrl.get("clean_completion_within_T"))
    control_clean_disconnect_seen = (not ctrl.get("connector_clean_disconnect_absent", True))
    control_classification = ctrl.get("classification")
    control_false_positive = control_poison_detected  # a clean island flagged poisoned == FP

    # ---- failure island #1 ----
    connector_not_alive = bool(pred1.get("connector_not_alive"))
    connector_exit_status = pred1.get("connector_exit_status")
    connector_exit_signal = pred1.get("connector_exit_signal")
    connector_self_crashed = bool(pred1.get("connector_self_crashed"))
    connector_clean_disconnect_absent = bool(pred1.get("connector_clean_disconnect_absent"))
    clean_completion_within_T = bool(pred1.get("clean_completion_within_T"))
    clean_root_result_absent = bool(pred1.get("clean_root_result_absent"))
    root_not_cleanly_exited = bool(pred1.get("root_not_cleanly_exited"))
    root_alive = bool(pred1.get("root_alive_after_connector_death"))
    progress_timeout_elapsed = bool(pred1.get("root_progress_timeout_elapsed"))
    poison_detected = bool(pred1.get("poisoned"))
    island1_classification = pred1.get("classification")
    action_was_in_flight = bool(pred1.get("action_was_in_flight"))  # diagnostic only

    # the supervisor did NOT cause the death: connector self-crashed, observed via its own exit
    connector_death_caused_by_supervisor = False
    detection_observed_uncaused_death = bool(connector_not_alive and connector_self_crashed
                                             and not connector_death_caused_by_supervisor)

    root_sig = killed1.get("root_exit_signal")
    island1_root_killed_by_supervisor = (root_sig == signal.SIGKILL)
    idle_cap_elapsed = bool(killed1.get("failure_root_idle_cap_elapsed"))
    island1_failure_root_never_finalized = bool(island1_root_killed_by_supervisor
                                                and not idle_cap_elapsed)
    island1_all_reaped = bool(killed1.get("all_children_reaped"))

    # ---- restart island #2 ----
    island2_root_started = bool(l2.get("root_started"))
    island2_connector_joined = bool(l2.get("connector_joined"))
    island2_action_proved_remote = bool(clean_root2.get("action_proved_remote"))
    island2_teardown_clean = bool(conn_disc2.get("clean") and b2.get("exited") and b2.get("rc") == 0)
    island2_root_finalized_clean = bool(a2.get("exited") and a2.get("rc") == 0 and clean_root2)

    p0c, p1c = ports0
    p0, p1 = ports1
    p2, p3 = ports2
    all_island_ports = {p0c, p1c, p0, p1, p2, p3}
    ports_reused = len(all_island_ports) != 6
    island2_used_fresh_ports = not ({p2, p3} & {p0c, p1c, p0, p1})

    orphan_ok, orphan_pids = _orphan_check()
    no_orphans = (orphan_ok is True)

    # data-plane separation
    ray_payloads = json.dumps([c0, ctrl, l1, conn_exit, pred1, killed1, l2, a2, b2], default=str)
    result_via_ray = ("action_result" in ray_payloads)

    root_argv = " ".join((l1.get("root_argv") or []) + (l2.get("root_argv") or [])
                         + (c0.get("root_argv") or []))
    conn_argv = " ".join((l1.get("connector_argv") or []) + (l2.get("connector_argv") or [])
                         + (c0.get("connector_argv") or []))
    ignore_batch_all = ("--hpx:ignore-batch-env" in root_argv
                        and "--hpx:ignore-batch-env" in conn_argv)
    child_self_locating = control_root_started and island2_root_started

    whole_island_restart_succeeded = all([
        island2_root_started, island2_connector_joined, island2_action_proved_remote,
        island2_teardown_clean, island2_root_finalized_clean,
    ])
    supervisor_survived = bool(island1_all_reaped and island2_root_started)

    overall_pass = all([
        # control: detector does NOT fire on a clean island, evaluated on same timeline
        control_root_started, control_connector_joined, not control_false_positive,
        # failure: connector self-crashed, UNCAUSED death observed, predicate fired autonomously
        connector_self_crashed, detection_observed_uncaused_death, connector_not_alive,
        not clean_completion_within_T, connector_clean_disconnect_absent, root_not_cleanly_exited,
        poison_detected, island1_classification == "poisoned",
        # kill/reap + restart
        island1_root_killed_by_supervisor, island1_failure_root_never_finalized, island1_all_reaped,
        supervisor_survived, island2_used_fresh_ports, whole_island_restart_succeeded,
        no_orphans, not result_via_ray,
    ])

    return {
        "overall": "pass" if overall_pass else "fail",
        "ray_available": True, "binary_available": True,

        # ---- control island #0 ----
        "control_island_root_started": control_root_started,
        "control_island_connector_joined": control_connector_joined,
        "control_island_evaluated_on_same_timeline": True,
        "control_island_poison_detected": control_poison_detected,
        "control_island_false_positive": control_false_positive,
        "control_island_clean_completion_within_T": control_clean_completion,
        "control_island_clean_disconnect_seen": control_clean_disconnect_seen,
        "control_island_classification": control_classification,

        # ---- failure island #1 ----
        "island1_root_started": bool(l1.get("root_started")),
        "island1_connector_joined": bool(l1.get("connector_joined")),
        "connector_kind": "self_crash",
        "connector_not_alive": connector_not_alive,
        "connector_exit_status": connector_exit_status,
        "connector_exit_signal": connector_exit_signal,
        "connector_self_crashed": connector_self_crashed,
        "connector_death_caused_by_supervisor": connector_death_caused_by_supervisor,
        "detection_observed_uncaused_death": detection_observed_uncaused_death,
        "connector_clean_disconnect_absent": connector_clean_disconnect_absent,
        "clean_completion_within_T": clean_completion_within_T,
        "clean_root_result_absent": clean_root_result_absent,
        "root_not_cleanly_exited": root_not_cleanly_exited,
        "root_alive_after_connector_death": root_alive,
        "root_progress_timeout_elapsed": progress_timeout_elapsed,
        "poison_detected_by_supervisor": poison_detected,
        "poison_detection_used_scripted_action_marker": False,
        "action_was_in_flight": action_was_in_flight,
        "island1_classification": island1_classification,
        "island1_root_killed_by_supervisor": island1_root_killed_by_supervisor,
        "island1_failure_root_exit_signal": root_sig,
        "island1_failure_root_never_finalized": island1_failure_root_never_finalized,
        "island1_failure_root_idle_cap_elapsed": idle_cap_elapsed,
        "island1_all_children_reaped": island1_all_reaped,

        # ---- restart island #2 ----
        "island2_used_fresh_ports": island2_used_fresh_ports,
        "ports_reused": ports_reused,
        "island0_ports": f"127.0.0.1:{p0c},127.0.0.1:{p1c}",
        "island1_ports": f"127.0.0.1:{p0},127.0.0.1:{p1}",
        "island2_ports": f"127.0.0.1:{p2},127.0.0.1:{p3}",
        "island2_root_started": island2_root_started,
        "island2_connector_joined": island2_connector_joined,
        "island2_action_proved_remote": island2_action_proved_remote,
        "island2_graceful_teardown_clean": island2_teardown_clean,
        "island2_root_finalized_clean": island2_root_finalized_clean,
        "island2_root_locality": clean_root2.get("here_locality"),
        "island2_connector_locality": l2.get("connector_locality_id"),

        "whole_island_restart_succeeded": whole_island_restart_succeeded,
        "supervisor_survived_island_death": supervisor_survived,
        "no_orphan_hpx_processes": no_orphans,
        "orphan_pids": orphan_pids,
        "ray_carried_bootstrap_metadata_only": (not result_via_ray),
        "action_result_returned_through_ray": result_via_ray,
        "binary_self_locating_rpath": child_self_locating,
        "hpx_ignore_batch_env_used": ignore_batch_all,
    }


def _write_agg(path, agg):
    with open(path, "w") as fh:
        json.dump(agg, fh, indent=2, sort_keys=False)
        fh.write("\n")


def _base_agg(binary):
    return {
        "experiment": "54_ray_autonomous_poison_detection",
        "kind": "ray_autonomous_poisoned_island_detection",
        "ray_free": False, "single_node": True, "transport": "tcp_loopback",
        "supervision_plane": "ray", "data_plane": "hpx",
        "policy": "supervisor_owned_liveness_progress_detection",
        "binary": os.path.basename(binary) if binary else None,
        "note": (
            "Detection-logic experiment. The supervisor cannot query HPX for authoritative locality "
            "health (exp51 -- no such API); it infers poisoning from OS process liveness + bounded "
            "progress/completion markers via a UNIFORM predicate run for both the clean control "
            "(island #0) and the failure island (island #1). Island #1's connector SELF-CRASHES "
            "(std::abort/SIGABRT), so the supervisor observes an UNCAUSED death, not one it "
            "initiated. The scripted action_started marker is NOT a classifier input. The poisoned "
            "island is killed and replaced; island #2 is a fresh independent HPX runtime, not "
            "repaired island #1. Re-validates no HPX property; NOT HPX fault tolerance."
        ),
    }


def main():
    ap = argparse.ArgumentParser(description="exp54 autonomous poisoned-island detection")
    ap.add_argument("--binary", default=None)
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--sleep-ms", type=int, default=8000)
    ap.add_argument("--wait-bound", type=int, default=15)
    ap.add_argument("--step-timeout", type=int, default=20)
    ap.add_argument("--idle-cap", type=int, default=120)
    ap.add_argument("--crash-delay-ms", type=int, default=2000)
    ap.add_argument("--root-progress-timeout", type=int, default=25,
                    help="bounded window for a clean completion marker; must exceed a healthy "
                         "island's completion (~15s here). The poison short-circuit makes failure "
                         "detection fast regardless, so this mainly governs the clean control.")
    ap.add_argument("--per-phase-timeout", type=int, default=90)
    ap.add_argument("--aggregate", default=os.path.join(HERE, "aggregate.json"))
    args = ap.parse_args()

    binary = locate_binary(args.binary)
    if binary is None:
        agg = _base_agg(None)
        agg["overall"] = "skip"
        agg["findings"] = {"binary_available": False,
                           "reason": "autonomous_poison_spike not built (see CMakeLists.txt)"}
        _write_agg(args.aggregate, agg)
        print("SKIP: autonomous_poison_spike not found; build the experiment first (CMakeLists.txt).")
        return 0

    try:
        import ray
    except Exception as e:  # noqa: BLE001
        agg = _base_agg(binary)
        agg["overall"] = "skip"
        agg["findings"] = {"ray_available": False, "reason": f"import ray failed: {e}"}
        _write_agg(args.aggregate, agg)
        print(f"SKIP: ray not importable ({e}).")
        return 0

    print(f"[exp54] binary: {binary}")
    ray.init(ignore_reinit_error=True, log_to_driver=False)  # LOCAL ray only
    print(f"[exp54] ray initialized (local), version {ray.__version__}")
    try:
        findings = run_once(binary, args)
    finally:
        ray.shutdown()

    agg = _base_agg(binary)
    agg["overall"] = findings["overall"]
    agg["findings"] = findings
    _write_agg(args.aggregate, agg)
    print(f"[exp54] overall={findings['overall']} "
          f"control_fp={findings['control_island_false_positive']} "
          f"conn_self_crashed={findings['connector_self_crashed']} "
          f"conn_sig={findings['connector_exit_signal']} "
          f"uncaused={findings['detection_observed_uncaused_death']} "
          f"poison_detected={findings['poison_detected_by_supervisor']} "
          f"class={findings['island1_classification']} "
          f"restart_ok={findings['whole_island_restart_succeeded']} "
          f"fresh_ports={findings['island2_used_fresh_ports']} "
          f"no_orphans={findings['no_orphan_hpx_processes']} -> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
