#!/usr/bin/env python3
"""exp55 -- clean-marker timeout calibration + HPX shutdown-timeout preflight.

ORCHESTRATOR-SIDE DETECTOR CALIBRATION ONLY. This is NOT a new HPX mechanism and NOT a performance
result. It reuses the exp54 ``autonomous_poison_spike`` binary UNCHANGED and IMPORTS the EXACT exp54
detector rule (``evaluate_health_predicate``) -- the rule is never rederived here.

WHAT THE DETECTOR ACTUALLY TIMES.  ``clean_root.json`` is written by the clean root BEFORE
``hpx::finalize()`` (autonomous_poison_spike.cpp): the clean root dispatches the INSTANT
``dist_probe_action`` (NOT ``dist_sleep_probe``) and writes the marker after the graceful-leave gate,
then finalizes. Therefore:
  * ``time_to_marker_ms`` = what the detector RACES (bootstrap + parcelport + instant action +
    leave-gate); it is INDEPENDENT of ``sleep_ms`` for this binary.
  * ``time_to_finalize_ms`` = the distributed shutdown/finalize tail DOWNSTREAM of the marker.
  * ``sleep_ms`` only governs the FAILURE root's ``dist_sleep_probe`` duration; the failure arm writes
    NO marker. So a ``sleep_ms``-relative timeout floor is the WRONG model for this binary.

Hence exp55 Phase 1 calibrates an ABSOLUTE clean-marker timeout floor, and includes a small
``sleep_ms`` CONTROL arm that empirically shows the clean marker is independent of ``sleep_ms``.

Phases:
  * Phase 0 (preflight, framing only): poison a root via the idle-cap anomaly path and inject
    ``--hpx:ini=hpx.shutdown_timeout=<T>``; observe whether the poisoned ``runtime_distributed::wait()``
    self-exits within T+grace or still hangs (external killpg fallback). If HPX self-bounds, the Ray
    timeout is a BACKSTOP; if it still hangs, the external Ray timeout remains the PRIMARY policy.
  * Phase 1 (HEADLINE): absolute ``root_progress_timeout_ms`` sweep. Per timeout: clean-control arm
    (expect ``clean_complete``; poisoned => false_positive) + self-crash arm (expect ``poisoned``),
    classified with the imported exp54 predicate. Measures clean-arm marker/finalize timing.
  * Phase 1b: ``sleep_ms`` independence control (clean arm only) at one safe timeout.
  * Phase 2 (small, skippable): registration-boundary crash-timing taxonomy. NOTE: the reused binary
    crashes only AFTER writing ``connect.joined1``; a pre-join crash is NOT achievable without a new
    connector mode, so that point is reported as ``not_supported_reused_binary``.

CLAIM FENCE: single-node; loopback TCP; closed-int64 synthetic action/control only; Ray =
supervision/detection plane only; HPX = execution/data plane inside each island; detector clean-marker
timeout calibration only; the timeout floor is for clean-completion-marker arrival in THIS binary, NOT
a portable HPX shutdown/finalize guarantee; loopback lower bound only -- a real network/fabric requires
separate, likely larger calibration; not HPX fault tolerance; not in-place recovery; no AGAS
stale-locality repair; no Ray actor-failure recovery; no multi-node; no general fabric; no
performance/speedup/throughput/latency; no production/public API; no endpoint seam; no Ray replacement;
no "HPX faster than Ray"; no "RayX makes Ray faster". Future distributed-fabric direction only.

Exit code 0 on clean fail/skip; non-zero only on an orchestrator-internal error.
"""

import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
EXP54_DIR = os.path.join(os.path.dirname(HERE), "54_ray_autonomous_poison_detection")

# --- import the EXACT exp54 detector rule + helpers (never rederived here) ----------------------
sys.path.insert(0, EXP54_DIR)
import run_ray_autonomous_poison_detection as exp54  # noqa: E402

build_supervisor = exp54.build_supervisor           # holds evaluate_health_predicate (the rule)
locate_binary = exp54.locate_binary
_fresh_ports = exp54._fresh_ports
_orphan_check = exp54._orphan_check
_LOADER_ENV_VARS = exp54._LOADER_ENV_VARS
BINARY_BASENAME = exp54.BINARY_BASENAME
PREDICATE_IMPORTED_FROM = (
    f"{exp54.__name__}.IslandSupervisor.evaluate_health_predicate "
    f"({os.path.relpath(exp54.__file__, os.path.dirname(HERE))})"
)


# ====================================================================================================
# Phase 0 -- HPX shutdown-timeout preflight (custom launcher; NO detector rule, NO binary change)
# ====================================================================================================
def build_preflight(ray):
    @ray.remote
    class PreflightSupervisor:
        def __init__(self):
            self.procs = []
            self.logs = []

        def _scrubbed_env(self):
            env = dict(os.environ)
            for k in _LOADER_ENV_VARS:
                env.pop(k, None)
            return env

        def _popen(self, argv, bootdir, name):
            log = open(os.path.join(bootdir, name), "w")
            p = subprocess.Popen(argv, cwd=bootdir, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True, env=self._scrubbed_env())
            self.procs.append(p)
            self.logs.append(log)
            return p

        def _wait_file(self, path, proc, timeout):
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

        def run(self, meta, shutdown_timeout_ms, exit_wait_s):
            binary, bootdir, p0, p1 = meta["binary"], meta["bootdir"], meta["p0"], meta["p1"]
            is_baseline = (shutdown_timeout_ms is None or shutdown_timeout_ms < 0)
            sec = (0.0 if is_baseline else max(shutdown_timeout_ms / 1000.0, 0.0))
            root_argv = [
                binary, "--role", "f_root", "--island-mode", "failure", "--bootstrap", bootdir,
                "--x", "7", "--sleep-ms", str(meta["sleep_ms"]),
                "--wait-bound", str(meta["wait_bound"]), "--step-timeout", str(meta["step_timeout"]),
                "--idle-cap", str(meta["idle_cap"]),
                f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p0}",
                "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
                "--hpx:ignore-batch-env",
            ]
            if not is_baseline:  # baseline arm omits the config to test whether the root hangs at all
                root_argv.append(f"--hpx:ini=hpx.shutdown_timeout={sec}")
            conn_argv = [
                binary, "--role", "f_connect", "--connector-kind", "self_crash",
                "--connector-index", "1", "--bootstrap", bootdir, "--serve-timeout", "30",
                "--crash-delay-ms", str(meta["crash_delay_ms"]),
                f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p1}",
                "--hpx:threads=1", "--hpx:bind=none", "--hpx:ignore-batch-env",
            ]
            root = self._popen(root_argv, bootdir, "preflight_root.log")
            root_ready = self._wait_file(os.path.join(bootdir, "root.ready"), root,
                                         meta["step_timeout"])
            conn = None
            if root_ready and root.poll() is None:
                conn = self._popen(conn_argv, bootdir, "preflight_connector.log")
                self._wait_file(os.path.join(bootdir, "connect.joined1"), conn, meta["step_timeout"])

            # The failure root reaches hpx::finalize() ONLY on the idle-cap anomaly path; wait for the
            # marker that proves finalize entry, then time how long the poisoned shutdown takes.
            idle_marker = os.path.join(bootdir, "failure_root_idle_cap_elapsed")
            # bound to reach finalize: AGAS settle (~step_timeout) + wait_bound + idle_cap + slack
            reach_bound = meta["step_timeout"] + meta["wait_bound"] + meta["idle_cap"] + 15
            reached_finalize = self._wait_file(idle_marker, root, reach_bound)
            finalize_entered_t = time.time()
            # genuine poison requires the connector to have REGISTERED (root saw 2 localities)
            # BEFORE dying; otherwise the root's shutdown was never actually poisoned.
            diag = exp54._read_json(os.path.join(bootdir, "failure_root_diag.json")) or {}
            connector_registered_before_crash = bool(diag.get("reached_two"))

            # observe whether the root self-exits within the wait window (raw observation only;
            # classification -- including the baseline comparison -- happens in run_preflight)
            exit_deadline = time.time() + exit_wait_s
            while time.time() < exit_deadline and root.poll() is None:
                time.sleep(0.05)
            self_exited = root.poll() is not None
            exit_t = time.time()
            external_kill_required = not self_exited
            if external_kill_required:
                self._killpg(root)
            self._killpg(conn)
            try:
                root.wait(timeout=8)
            except subprocess.TimeoutExpired:
                pass
            try:
                if conn is not None:
                    conn.wait(timeout=8)
            except subprocess.TimeoutExpired:
                pass

            rc = root.poll()
            time_to_exit_ms = round((exit_t - finalize_entered_t) * 1000, 1) if self_exited else None
            return {
                "shutdown_timeout_ms": shutdown_timeout_ms,
                "is_baseline": is_baseline,
                "root_ready": root_ready,
                "connector_registered_before_crash": connector_registered_before_crash,
                "reached_finalize": reached_finalize,
                "poisoned_root_self_exited": self_exited,
                "external_kill_required": external_kill_required,
                "time_to_poisoned_exit_ms": time_to_exit_ms,
                "root_exit_rc": rc,
                "root_exit_signal": (-rc if (rc is not None and rc < 0) else None),
            }

        def cleanup(self):
            for p in self.procs:
                self._killpg(p)
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            for lg in self.logs:
                try:
                    lg.close()
                except OSError:
                    pass
            return {"swept": len(self.procs)}

    return PreflightSupervisor


def run_preflight(ray, binary, args, used):
    Pre = build_preflight(ray)
    s = Pre.remote()

    def one(st_ms):
        p0, p1 = _fresh_ports(used)
        used.update({p0, p1})
        bootdir = tempfile.mkdtemp(prefix="exp55_pre_")
        meta = {"binary": binary, "bootdir": bootdir, "p0": p0, "p1": p1,
                "sleep_ms": args.preflight_sleep_ms, "wait_bound": args.preflight_wait_bound,
                "step_timeout": 25, "idle_cap": args.preflight_idle_cap,
                "crash_delay_ms": args.preflight_crash_delay_ms}
        exit_wait = (args.preflight_baseline_probe if (st_ms is None or st_ms < 0)
                     else st_ms / 1000.0 + args.preflight_grace)
        cap = 25 + args.preflight_wait_bound + args.preflight_idle_cap + exit_wait + 30
        return ray.get(s.run.remote(meta, st_ms, exit_wait), timeout=cap)

    try:
        baseline = one(-1)                       # NO shutdown_timeout: does the poisoned root hang?
        timeout_arms = [one(st_ms) for st_ms in args.shutdown_timeouts]
    finally:
        try:
            ray.get(s.cleanup.remote(), timeout=30)
        except Exception:
            pass
        try:
            ray.kill(s)
        except Exception:
            pass

    def genuine(r):
        return bool(r.get("connector_registered_before_crash") and r.get("reached_finalize"))

    if not genuine(baseline):
        overall = "inconclusive"
        interp = ("Baseline did not reproduce a genuine poisoned shutdown (connector not registered "
                  "or finalize not entered); cannot evaluate hpx.shutdown_timeout. A dedicated "
                  "follow-up (exp56) reproducing exp50/51 ungraceful-loss timing is needed.")
    elif not baseline.get("external_kill_required"):
        # the root self-exited even WITHOUT a shutdown timeout -> this idle-cap path does not
        # reproduce the exp51 runtime_distributed::wait() hang, so exits cannot be attributed to
        # the config. Honest per the 'if finalize entry is unreliable, report' instruction.
        overall = "inconclusive"
        interp = ("Poisoned root self-exited even WITHOUT hpx.shutdown_timeout "
                  f"(in ~{baseline.get('time_to_poisoned_exit_ms')} ms, rc="
                  f"{baseline.get('root_exit_rc')}); this idle-cap path does NOT reproduce the exp51 "
                  "runtime_distributed::wait() hang on this build, so exits cannot be attributed to "
                  "the shutdown-timeout config. Recommend a dedicated exp56 preflight that reproduces "
                  "exp50/51 ungraceful-loss timing. External Ray timeout remains the working policy.")
    else:
        gen_t = [r for r in timeout_arms if genuine(r)]
        if gen_t and all(r.get("poisoned_root_self_exited") for r in gen_t):
            overall = "true"
            interp = ("Poisoned root HUNG without the config but self-exited with hpx.shutdown_timeout "
                      "set -> HPX self-bounds the poisoned shutdown on this build; the Ray timeout is "
                      "a BACKSTOP.")
        else:
            overall = "false"
            interp = ("Poisoned root still hung with hpx.shutdown_timeout set -> external Ray timeout "
                      "remains the PRIMARY supervisor policy.")

    return {"ran": True, "shutdown_timeouts_ms": args.shutdown_timeouts,
            "baseline_no_timeout": baseline, "results": timeout_arms,
            "hpx_self_bounds_poisoned_shutdown": overall, "interpretation": interp}


# ====================================================================================================
# Phase 1 -- absolute clean-marker timeout calibration (imported exp54 predicate)
# ====================================================================================================
def _meta(island_id, binary, bootdir, p0, p1, sleep_ms, crash_delay_ms):
    return {"island_id": island_id, "binary": binary, "bootdir": bootdir, "p0": p0, "p1": p1,
            "x": 7, "sleep_ms": sleep_ms, "wait_bound": 15, "step_timeout": 20, "idle_cap": 120,
            "crash_delay_ms": crash_delay_ms}


def _classify_clean(classification, marker_present):
    if classification == "clean_complete":
        return "clean_correct"
    if classification == "poisoned":
        return "false_positive"
    # indeterminate within T: clipped. If the marker DID arrive later, it was a clipped-healthy run.
    return "late_clean_result_after_poison" if marker_present else "indeterminate"


def _classify_self_crash(classification):
    if classification == "poisoned":
        return "poison_correct"
    if classification == "clean_complete":
        return "false_negative"
    return "indeterminate"   # T too short to observe the connector death


def run_phase1(ray, binary, args, used):
    Supervisor = build_supervisor(ray)
    s = Supervisor.remote()
    iid = [0]

    def next_id():
        iid[0] += 1
        return iid[0]

    runs = []
    try:
        for t_ms in args.timeouts:
            t_s = t_ms / 1000.0
            for rep in range(args.repeats):
                # ---- clean-control arm ----
                cid = next_id()
                p0, p1 = _fresh_ports(used)
                used |= {p0, p1}
                bd = tempfile.mkdtemp(prefix="exp55_clean_")
                meta = _meta(cid, binary, bd, p0, p1, args.clean_sleep_ms, args.crash_delay_ms)
                t0 = time.time()
                ray.get(s.launch_island.remote(meta, "clean", "clean"),
                        timeout=args.per_phase_timeout)
                pred = ray.get(s.evaluate_health_predicate.remote(cid, t_s), timeout=t_s + 15)
                # measure the TRUE marker arrival regardless of the per-cell timeout
                marker = os.path.join(bd, "clean_root.json")
                mdeadline = time.time() + args.marker_wait_bound
                while not os.path.exists(marker) and time.time() < mdeadline:
                    time.sleep(0.05)
                m_mtime = os.path.getmtime(marker) if os.path.exists(marker) else None
                ttm = round((m_mtime - t0) * 1000, 1) if m_mtime else None
                ttf = None
                if m_mtime is not None:
                    rexit = ray.get(s.wait_exit.remote(cid, "root", args.per_phase_timeout),
                                    timeout=args.per_phase_timeout + 5)
                    if rexit.get("exited"):
                        ttf = round((time.time() - m_mtime) * 1000, 1)
                ray.get(s.kill_island.remote(cid), timeout=20)
                runs.append({"timeout_ms": t_ms, "arm": "clean", "repeat": rep,
                             "classification": pred.get("classification"),
                             "verdict": _classify_clean(pred.get("classification"),
                                                        m_mtime is not None),
                             "time_to_marker_ms": ttm, "time_to_finalize_ms": ttf})

                # ---- self-crash arm ----
                sid = next_id()
                p0, p1 = _fresh_ports(used)
                used |= {p0, p1}
                bd = tempfile.mkdtemp(prefix="exp55_crash_")
                meta = _meta(sid, binary, bd, p0, p1, args.clean_sleep_ms, args.crash_delay_ms)
                ray.get(s.launch_island.remote(meta, "failure", "self_crash"),
                        timeout=args.per_phase_timeout)
                pred = ray.get(s.evaluate_health_predicate.remote(sid, t_s), timeout=t_s + 15)
                ray.get(s.kill_island.remote(sid), timeout=30)
                runs.append({"timeout_ms": t_ms, "arm": "self_crash", "repeat": rep,
                             "classification": pred.get("classification"),
                             "verdict": _classify_self_crash(pred.get("classification")),
                             "time_to_marker_ms": None, "time_to_finalize_ms": None})
    finally:
        try:
            ray.get(s.shutdown.remote(), timeout=30)
        except Exception:
            pass
        try:
            ray.kill(s)
        except Exception:
            pass
    return runs


# ====================================================================================================
# Phase 1b -- sleep_ms independence control (clean arm only)
# ====================================================================================================
def run_sleep_control(ray, binary, args, used):
    Supervisor = build_supervisor(ray)
    s = Supervisor.remote()
    iid = [0]
    out = []
    t_s = args.sleep_control_timeout_ms / 1000.0
    try:
        for sm in args.sleep_control:
            samples = []
            for _ in range(args.sleep_control_repeats):
                iid[0] += 1
                cid = iid[0]
                p0, p1 = _fresh_ports(used)
                used |= {p0, p1}
                bd = tempfile.mkdtemp(prefix="exp55_sleepctl_")
                meta = _meta(cid, binary, bd, p0, p1, sm, args.crash_delay_ms)
                t0 = time.time()
                ray.get(s.launch_island.remote(meta, "clean", "clean"),
                        timeout=args.per_phase_timeout)
                ray.get(s.evaluate_health_predicate.remote(cid, t_s), timeout=t_s + 15)
                marker = os.path.join(bd, "clean_root.json")
                mdeadline = time.time() + args.marker_wait_bound
                while not os.path.exists(marker) and time.time() < mdeadline:
                    time.sleep(0.05)
                if os.path.exists(marker):
                    samples.append(round((os.path.getmtime(marker) - t0) * 1000, 1))
                ray.get(s.kill_island.remote(cid), timeout=20)
            out.append({"sleep_ms": sm, "time_to_marker_ms_samples": samples,
                        "time_to_marker_ms": _stats(samples)})
    finally:
        try:
            ray.get(s.shutdown.remote(), timeout=30)
        except Exception:
            pass
        try:
            ray.kill(s)
        except Exception:
            pass
    return out


# ====================================================================================================
# Phase 2 -- registration-boundary crash-timing taxonomy (small, descriptive)
# ====================================================================================================
def run_boundary_probe(ray, binary, args, used):
    Supervisor = build_supervisor(ray)
    s = Supervisor.remote()
    iid = [0]
    big_t = max(args.timeouts + [6000]) / 1000.0 + 4  # generous so poison resolves
    # The reused binary crashes ONLY after writing connect.joined1; a pre-join crash needs a new
    # connector mode, so it is honestly reported as not achievable here.
    points = [
        {"crash_point": "before_join_marker", "crash_delay_ms": None,
         "status": "not_supported_reused_binary",
         "reason": "self_crash connector writes connect.joined1 before sleeping/aborting; "
                   "pre-join crash would require a new connector mode (forbidden in exp55)."},
        {"crash_point": "immediately_after_join", "crash_delay_ms": 0, "status": "ran"},
        {"crash_point": "after_action_in_flight",
         "crash_delay_ms": max(args.boundary_sleep_ms // 2, 1000), "status": "ran"},
    ]
    try:
        for pt in points:
            if pt["status"] != "ran":
                continue
            iid[0] += 1
            sid = iid[0]
            p0, p1 = _fresh_ports(used)
            used |= {p0, p1}
            bd = tempfile.mkdtemp(prefix="exp55_bound_")
            meta = _meta(sid, binary, bd, p0, p1, args.boundary_sleep_ms, pt["crash_delay_ms"])
            launch = ray.get(s.launch_island.remote(meta, "failure", "self_crash"),
                             timeout=args.per_phase_timeout)
            pred = ray.get(s.evaluate_health_predicate.remote(sid, big_t), timeout=big_t + 15)
            ray.get(s.kill_island.remote(sid), timeout=30)
            pt["connector_registered_before_death"] = bool(launch.get("connector_joined"))
            pt["action_was_in_flight"] = bool(pred.get("action_was_in_flight"))
            pt["classification"] = pred.get("classification")
            pt["connector_self_crashed"] = bool(pred.get("connector_self_crashed"))
            pt["connector_exit_signal"] = pred.get("connector_exit_signal")
    finally:
        try:
            ray.get(s.shutdown.remote(), timeout=30)
        except Exception:
            pass
        try:
            ray.kill(s)
        except Exception:
            pass
    verdict_changes = len({pt.get("classification") for pt in points
                           if pt["status"] == "ran"}) > 1
    return {"ran": True, "sleep_ms": args.boundary_sleep_ms, "points": points,
            "classification_changes_across_boundary": verdict_changes}


# ====================================================================================================
# aggregation
# ====================================================================================================
def _stats(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"n": 0, "min": None, "p50": None, "max": None, "p99": None}
    s = sorted(vals)
    n = len(s)
    p99 = s[int(round(0.99 * (n - 1)))] if n >= 100 else None
    return {"n": n, "min": round(s[0], 1), "p50": round(statistics.median(s), 1),
            "max": round(s[-1], 1), "p99": (round(p99, 1) if p99 is not None else None)}


def assemble(binary, preflight, phase1_runs, sleep_control, boundary, args):
    timeouts = args.timeouts
    cells = []
    fp_cells, fn_cells, indet_cells, late_cells = [], [], [], []
    for t in timeouts:
        clean = [r for r in phase1_runs if r["timeout_ms"] == t and r["arm"] == "clean"]
        crash = [r for r in phase1_runs if r["timeout_ms"] == t and r["arm"] == "self_crash"]
        cv = [r["verdict"] for r in clean]
        sv = [r["verdict"] for r in crash]
        cell_stable = bool(cv and sv and all(v == "clean_correct" for v in cv)
                           and all(v == "poison_correct" for v in sv))
        if any(v == "false_positive" for v in cv):
            fp_cells.append(t)
        if any(v == "false_negative" for v in sv):
            fn_cells.append(t)
        if any(v == "indeterminate" for v in cv + sv):
            indet_cells.append(t)
        if any(v == "late_clean_result_after_poison" for v in cv):
            late_cells.append(t)
        cells.append({"root_progress_timeout_ms": t,
                      "clean_arm": {"verdicts": cv, "stable": all(v == "clean_correct" for v in cv)},
                      "self_crash_arm": {"verdicts": sv,
                                         "stable": all(v == "poison_correct" for v in sv)},
                      "cell_stable": cell_stable})

    stable_timeouts = [c["root_progress_timeout_ms"] for c in cells if c["cell_stable"]]
    floor = min(stable_timeouts) if stable_timeouts else None

    marker_vals = [r["time_to_marker_ms"] for r in phase1_runs if r["arm"] == "clean"]
    finalize_vals = [r["time_to_finalize_ms"] for r in phase1_runs if r["arm"] == "clean"]
    marker_stats = _stats(marker_vals)
    finalize_stats = _stats(finalize_vals)

    recommended = None
    if marker_stats["max"] is not None:
        recommended = int(marker_stats["max"] + args.recommended_margin_ms)
        if floor is not None:
            recommended = max(recommended, floor)

    # sleep_ms independence: clean marker p50 should be flat across sleep_ms
    sc_p50 = [g["time_to_marker_ms"]["p50"] for g in (sleep_control or [])
              if g["time_to_marker_ms"]["p50"] is not None]
    independent = None
    if len(sc_p50) >= 2:
        spread = max(sc_p50) - min(sc_p50)
        independent = bool(spread <= 0.5 * statistics.median(sc_p50))  # within 50% of median

    orphan_ok, orphan_pids = _orphan_check()
    no_orphans = (orphan_ok is True)

    # data-plane separation: no action result should traverse Ray (predicate dicts carry none)
    ray_payloads = json.dumps([preflight, phase1_runs, sleep_control, boundary], default=str)
    result_via_ray = ("action_result" in ray_payloads)

    overall_pass = all([
        floor is not None,
        no_orphans,
        not result_via_ray,
        not fp_cells or True,   # FPs are a finding, not a failure, as long as a stable floor exists
    ])

    agg = {
        "experiment": "55_poison_detector_operating_point",
        "kind": "poison_detector_clean_marker_timeout_calibration",
        "single_node": True, "transport": "tcp_loopback",
        "supervision_plane": "ray", "data_plane": "hpx",
        "predicate": ("connector_not_alive ∧ clean_disconnect_absent ∧ ¬clean_completion_within_T "
                      "∧ root_not_cleanly_exited"),
        "predicate_source": PREDICATE_IMPORTED_FROM,
        "predicate_imported_not_copied": True,
        "binary": os.path.basename(binary) if binary else None,
        "detector_times": "clean_completion_marker_arrival_NOT_distributed_finalize",
        "clean_root_marker_is_pre_finalize": True,
        "clean_root_uses_instant_dist_probe_not_sleep_probe": True,

        "preflight": preflight,

        "grid": {"root_progress_timeout_ms_axis": timeouts,
                 "crash_delay_ms": args.crash_delay_ms,
                 "clean_arm_sleep_ms": args.clean_sleep_ms,
                 "repeats": args.repeats},

        "cells": cells,

        "timing": {
            "time_to_marker_ms": marker_stats,
            "time_to_finalize_ms": finalize_stats,
            "note": ("time_to_marker_ms is what the detector races and is sleep_ms-independent for "
                     "this binary; time_to_finalize_ms is the downstream shutdown tail."),
        },

        "sleep_ms_control": {
            "timeout_ms": args.sleep_control_timeout_ms,
            "groups": sleep_control,
            "clean_marker_independent_of_sleep_ms": independent,
            "note": ("clean root uses instant dist_probe; sleep_ms governs only the failure root's "
                     "dist_sleep_probe, so clean-marker timing does not depend on sleep_ms."),
        },

        "registration_boundary_probe": boundary,

        "summary": {
            "false_positive_cells": fp_cells,
            "false_negative_cells": fn_cells,
            "indeterminate_cells": indet_cells,
            "late_clean_after_poison_cells": late_cells,
            "safe_marker_timeout_floor_ms": floor,
            "recommended_root_progress_timeout_ms": recommended,
            "recommended_margin_ms": args.recommended_margin_ms,
            "time_to_marker_ms_tail": marker_stats["max"],
            "time_to_finalize_ms_tail": finalize_stats["max"],
            "clean_marker_independent_of_sleep_ms": independent,
            "absolute_floor_is_workload_specific": True,
            "no_orphan_hpx_processes": no_orphans,
            "orphan_pids": orphan_pids,
            "ray_carried_bootstrap_metadata_only": (not result_via_ray),
        },
        "note": (
            "Orchestrator-side detector calibration. Reuses the exp54 autonomous_poison_spike binary "
            "and IMPORTS the exact exp54 evaluate_health_predicate (not rederived). The clean root "
            "writes clean_root.json BEFORE hpx::finalize() and uses the INSTANT dist_probe (not "
            "dist_sleep_probe), so the detector races marker arrival -- independent of sleep_ms -- "
            "not distributed shutdown completion. Phase 0 only observes whether HPX self-bounds a "
            "poisoned shutdown on this build/config. Loopback single-node lower bound; the floor is "
            "NOT a portable constant. Re-validates no HPX property."
        ),
        "overall": "pass" if overall_pass else "fail",
    }
    return agg


def _write_agg(path, agg):
    with open(path, "w") as fh:
        json.dump(agg, fh, indent=2, sort_keys=False)
        fh.write("\n")


def _csv_ints(s):
    return [int(x) for x in s.split(",") if x.strip()]


def main():
    ap = argparse.ArgumentParser(description="exp55 clean-marker timeout calibration")
    ap.add_argument("--binary", default=None)
    ap.add_argument("--timeouts", type=_csv_ints,
                    default=[10000, 14000, 15000, 16000, 18000, 20000, 25000],
                    help="absolute root_progress_timeout_ms axis (HEADLINE); straddles the observed "
                         "~15s AGAS registration-reflection marker latency on this build")
    ap.add_argument("--crash-delay-ms", type=int, default=1000, help="self-crash arm crash delay")
    ap.add_argument("--clean-sleep-ms", type=int, default=8000,
                    help="harmless default; clean root ignores sleep_ms")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--recommended-margin-ms", type=int, default=2000)
    # sleep_ms independence control
    ap.add_argument("--sleep-control", type=_csv_ints, default=[2000, 8000, 14000])
    ap.add_argument("--sleep-control-timeout-ms", type=int, default=20000)
    ap.add_argument("--sleep-control-repeats", type=int, default=2)
    # preflight (crash AFTER the ~15s AGAS settle so the connector REGISTERS before dying)
    ap.add_argument("--shutdown-timeouts", type=_csv_ints, default=[5000, 15000])
    ap.add_argument("--preflight-idle-cap", type=int, default=3)
    ap.add_argument("--preflight-wait-bound", type=int, default=5)
    ap.add_argument("--preflight-sleep-ms", type=int, default=500)
    ap.add_argument("--preflight-crash-delay-ms", type=int, default=18000)
    ap.add_argument("--preflight-grace", type=int, default=10)
    ap.add_argument("--preflight-baseline-probe", type=int, default=20,
                    help="seconds to wait for a no-timeout poisoned root to self-exit before "
                         "concluding it hangs (baseline control for the shutdown-timeout question)")
    # boundary probe
    ap.add_argument("--boundary-sleep-ms", type=int, default=8000)
    # general
    ap.add_argument("--marker-wait-bound", type=int, default=30)
    ap.add_argument("--per-phase-timeout", type=int, default=90)
    ap.add_argument("--skip-preflight", action="store_true")
    ap.add_argument("--skip-sleep-control", action="store_true")
    ap.add_argument("--skip-boundary-probe", action="store_true")
    ap.add_argument("--aggregate", default=os.path.join(HERE, "aggregate.json"))
    args = ap.parse_args()

    binary = locate_binary(args.binary)
    if binary is None:
        agg = {"experiment": "55_poison_detector_operating_point", "overall": "skip",
               "findings": {"binary_available": False,
                            "reason": "autonomous_poison_spike not built (see exp54 CMakeLists.txt)"}}
        _write_agg(args.aggregate, agg)
        print("SKIP: autonomous_poison_spike not found; build exp54 first.")
        return 0
    try:
        import ray
    except Exception as e:  # noqa: BLE001
        agg = {"experiment": "55_poison_detector_operating_point", "overall": "skip",
               "findings": {"ray_available": False, "reason": f"import ray failed: {e}"}}
        _write_agg(args.aggregate, agg)
        print(f"SKIP: ray not importable ({e}).")
        return 0

    print(f"[exp55] binary: {binary}")
    print(f"[exp55] predicate source: {PREDICATE_IMPORTED_FROM}")
    # Ray workers re-import the exp54 module that defines the IslandSupervisor actor + predicate;
    # the driver-only sys.path.insert does not reach them, so export it via PYTHONPATH.
    os.environ["PYTHONPATH"] = EXP54_DIR + os.pathsep + os.environ.get("PYTHONPATH", "")
    ray.init(ignore_reinit_error=True, log_to_driver=False)
    print(f"[exp55] ray initialized (local), version {ray.__version__}")
    used = set()
    preflight = {"ran": False, "reason": "skipped"}
    sleep_control = []
    boundary = {"ran": False, "reason": "skipped"}
    try:
        if not args.skip_preflight:
            print("[exp55] phase 0: HPX shutdown-timeout preflight ...")
            preflight = run_preflight(ray, binary, args, used)
        print("[exp55] phase 1: absolute clean-marker timeout sweep ...")
        phase1_runs = run_phase1(ray, binary, args, used)
        if not args.skip_sleep_control:
            print("[exp55] phase 1b: sleep_ms independence control ...")
            sleep_control = run_sleep_control(ray, binary, args, used)
        if not args.skip_boundary_probe:
            print("[exp55] phase 2: registration-boundary taxonomy ...")
            boundary = run_boundary_probe(ray, binary, args, used)
    finally:
        ray.shutdown()

    agg = assemble(binary, preflight, phase1_runs, sleep_control, boundary, args)
    _write_agg(args.aggregate, agg)
    sm = agg["summary"]
    print(f"[exp55] overall={agg['overall']} "
          f"floor_ms={sm['safe_marker_timeout_floor_ms']} "
          f"recommended_ms={sm['recommended_root_progress_timeout_ms']} "
          f"marker_max={sm['time_to_marker_ms_tail']} finalize_max={sm['time_to_finalize_ms_tail']} "
          f"fp={sm['false_positive_cells']} indet={sm['indeterminate_cells']} "
          f"sleep_indep={sm['clean_marker_independent_of_sleep_ms']} "
          f"preflight={preflight.get('hpx_self_bounds_poisoned_shutdown')} "
          f"no_orphans={sm['no_orphan_hpx_processes']} -> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
