#!/usr/bin/env python3
"""exp50 -- orchestrator for the Ray-free strong-L4 UNGRACEFUL connect-loss probe.

STRUCTURAL CHARACTERIZATION only. NOT a performance result, NOT a Ray demo. exp49 proved the
GRACEFUL connect-mode lifecycle (join -> remote action -> self-disconnect via
post(disconnect)+stop -> root re-admits a second connector). exp50 asks: what happens when a
connect-mode locality disappears UNGRACEFULLY (SIGKILL), the way a Ray actor process might
crash? A SIGKILLed connector is a crash ANALOG, not a real Ray actor.

Three cases, each on its own fresh root + ports + temp bootdir (so a wedged/self-terminated
root in one case cannot poison another):

  Case A (primary)     : root invokes the long dist_sleep_probe on victim connector #1; once
                         the action's `action_started` marker appears, the runner SIGKILLs the
                         connector mid-flight; the root classifies the future.
  Case B (bracketing)  : root serves one short dist_probe successfully, THEN the runner SIGKILLs
                         the connector (ungraceful, instead of a graceful disconnect).
  Case C (bracketing,  : the runner SIGKILLs the connector immediately after it joins, before
          best-effort)   the root dispatches. Timing-fragile (races whether TCP/AGAS noticed) --
                         it RUNS and is recorded but does NOT gate `overall`.

In every case the root then attempts a re-admit with a clean connector #2 using SET-DIFFERENCE
targeting (a locality id not seen before the kill), NOT a hard-coded dead id.

Root self-termination (HPX's fatal-error path aborting the root process) is a FIRST-CLASS,
likely, and VALID Case A outcome -- it is recorded distinctly from a harness-killed hang, and
is NOT a harness bug.

`overall` = "characterized" iff cases A and B each produce a classified outcome (a future
classification + definite re-admit yes/no, OR a definite root_died / root_hung) and the harness
completes. Case C is non-gating. `overall` = "inconclusive" only if A/B cannot be classified
(marker race / orchestrator error).

CLAIM FENCE: Ray-free; single-node; loopback TCP only; ungraceful connect-mode connector-loss
characterization only; SIGKILLed connector is a crash analog, not a real Ray actor; no
fault-tolerance claim (even on survival/readmit, phrase narrowly); no crash-recovery
generalization; no AGAS-root-loss recovery; no Ray actor/bootstrap claim yet; no
performance/speedup/throughput/latency; no multi-node; no general fabric; no production/public
API; no Ray replacement; no "HPX faster than Ray"; no "RayX makes Ray faster".

Usage:
  python run_strong_l4_connect_failure.py [--binary <path>] [--x 7] [--sleep-ms 8000]
      [--wait-bound 15] [--per-case-timeout 80] [--aggregate <path>]

Exit code is 0 even when a case shows root collapse (the aggregate carries the verdict); it is
non-zero only on an orchestrator-internal error.
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
    os.path.join(HERE, "build", "dist_fail_spike"),
    os.path.join(HERE, "build", "Release", "dist_fail_spike"),
]


def find_free_port():
    """Probe a free loopback TCP port (small TOCTOU window; loopback-only)."""
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


def _popen(cmd, cwd, log_path):
    log = open(log_path, "w")
    return subprocess.Popen(
        cmd, cwd=cwd, stdout=log, stderr=subprocess.STDOUT,
        start_new_session=True,  # own process group, so we can SIGKILL the whole tree
    ), log


def _kill_group(proc):
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _wait(proc, deadline):
    """Wait for proc until the absolute deadline. Returns (exited, returncode, killed)."""
    while time.time() < deadline:
        rc = proc.poll()
        if rc is not None:
            return True, rc, False
        time.sleep(0.05)
    _kill_group(proc)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    return False, proc.poll(), True


def _wait_for_file(path, proc, timeout):
    """Poll until `path` exists or `proc` dies, up to `timeout` s. Returns True if it appeared."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        if proc.poll() is not None:
            return os.path.exists(path)
        time.sleep(0.05)
    return os.path.exists(path)


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


# Per-case kill trigger: which marker the runner waits for before SIGKILLing the victim.
TRIGGERS = {"A": "action_started", "B": "served1.ok", "C": "connect.joined1"}
PRIMARY = {"A": "primary", "B": "bracketing", "C": "bracketing_best_effort"}


def run_case(kase, binary, x, sleep_ms, wait_bound, per_case_timeout):
    bootdir = tempfile.mkdtemp(prefix=f"exp50_{kase}_")
    p0, p1, p2 = find_free_port(), find_free_port(), find_free_port()
    step_timeout = max(12, per_case_timeout // 4)

    root_cmd = [
        binary, "--role", "f_root", "--case", kase, "--bootstrap", bootdir,
        "--x", str(x), "--sleep-ms", str(sleep_ms), "--wait-bound", str(wait_bound),
        "--step-timeout", str(step_timeout),
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p0}",
        "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
    ]
    victim_cmd = [
        binary, "--role", "f_connect", "--connector-kind", "victim", "--connector-index", "1",
        "--bootstrap", bootdir, "--victim-idle", str(per_case_timeout),
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p1}",
        "--hpx:threads=1", "--hpx:bind=none",
    ]

    def clean_cmd(port):
        return [
            binary, "--role", "f_connect", "--connector-kind", "clean", "--connector-index", "2",
            "--bootstrap", bootdir, "--serve-timeout", str(step_timeout + 10),
            f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{port}",
            "--hpx:threads=1", "--hpx:bind=none",
        ]

    root, rlog = _popen(root_cmd, bootdir, os.path.join(bootdir, "root.log"))
    ready = _wait_for_file(os.path.join(bootdir, "root.ready"), root, step_timeout)

    trigger_seen = False
    kill_signal_sent = False
    victim = vlog = None
    v_exited = v_killed = False
    if ready:
        victim, vlog = _popen(victim_cmd, bootdir, os.path.join(bootdir, "victim.log"))
        trigger_seen = _wait_for_file(
            os.path.join(bootdir, TRIGGERS[kase]), victim, step_timeout)
        # Ungraceful loss: SIGKILL the victim's process group. For A/B we wait for the trigger;
        # if it never appears that case is a marker race (still kill to clean up). For C we kill
        # right after join regardless (best-effort).
        _kill_group(victim)
        kill_signal_sent = True

    # Re-admit: launch clean connector #2 only if the root is still alive enough to test it.
    c2 = c2log = None
    c2_exited = c2_killed = False
    root_alive_for_readmit = root.poll() is None
    if ready and root_alive_for_readmit:
        c2, c2log = _popen(clean_cmd(p2), bootdir, os.path.join(bootdir, "clean2.log"))

    r_exited, r_rc, r_killed = _wait(root, time.time() + per_case_timeout)
    if victim is not None:
        v_exited, _vrc, v_killed = _wait(victim, time.time() + 5)
        vlog.close()
    if c2 is not None:
        c2_exited, _c2rc, c2_killed = _wait(c2, time.time() + 8)
        c2log.close()
    rlog.close()

    # --- root process-level classification (self-terminate vs hang vs clean) ---
    res = _read_json(os.path.join(bootdir, f"case_{kase}_root_result.json"))
    j1 = _read_json(os.path.join(bootdir, "connect.joined1")) or {}
    d2 = _read_json(os.path.join(bootdir, "connect.disconnected2")) or {}
    victim_survived = os.path.exists(os.path.join(bootdir, "victim_survived_idle1"))

    root_hung = bool(r_killed)
    root_killed_by_harness = bool(r_killed)
    # Self-termination = root exited on its own (not harness-killed) WITHOUT writing its result.
    root_self_terminated = bool(r_exited and not r_killed and res is None)
    root_exit_signal = (-r_rc if (r_rc is not None and r_rc < 0) else None)
    root_finalized_clean = bool(r_exited and not r_killed and r_rc == 0 and res is not None)

    if res is None:
        root_future_outcome = "root_died" if root_self_terminated else (
            "root_hung" if root_hung else "no_result")
    else:
        root_future_outcome = res.get("root_future_outcome", "no_result")

    # --- assemble case dict (schema-stable) ---
    case = {
        "case": kase,
        "primary_or_bracketing": PRIMARY[kase],
        "root_ready": ready,
        "ports": {"root": f"127.0.0.1:{p0}", "victim": f"127.0.0.1:{p1}",
                  "clean2": f"127.0.0.1:{p2}"},
        "connector1_joined": j1.get("locality_id") is not None,
        "pre_kill_localities": (res or {}).get("pre_kill_localities"),
        "long_action_started": (kase == "A" and trigger_seen) or None if kase == "A"
                               else None,
        "served1_ok": (res or {}).get("served1_ok") if kase == "B" else None,
        "trigger_marker": TRIGGERS[kase],
        "trigger_marker_seen": trigger_seen,
        "kill_signal_sent": kill_signal_sent,
        "kill_signal": "SIGKILL" if kill_signal_sent else None,
        "root_future_outcome": root_future_outcome,
        "root_self_terminated": root_self_terminated,
        "root_exit_signal": root_exit_signal,
        "root_hung": root_hung,
        "root_killed_by_harness": root_killed_by_harness,
        "future_exception_hpx_error": (res or {}).get("future_exception_hpx_error"),
        "future_exception_hpx_error_value": (res or {}).get("future_exception_hpx_error_value"),
        "future_exception_text": (res or {}).get("future_exception_text"),
        "detected_before_wait_bound": (res or {}).get("detected_before_wait_bound"),
        "detected_at_full_bound": (res or {}).get("detected_at_full_bound"),
        "timed_out_at_bound": (res or {}).get("timed_out_at_bound"),
        "localities_after_loss": (res or {}).get("localities_after_loss"),
        "dead_locality_still_present": (res or {}).get("dead_locality_still_present"),
        "new_locality_after_loss_seen": (res or {}).get("new_locality_after_loss_seen"),
        "connector2_joined": _read_json(os.path.join(bootdir, "connect.joined2")) is not None,
        "connector2_served": (res or {}).get("connector2_served"),
        "connector2_proved_remote": (res or {}).get("connector2_proved_remote"),
        "root_readmitted_after_loss": (res or {}).get("root_readmitted_after_loss"),
        "connector2_disconnected_clean": bool(d2.get("clean")) and c2_exited and not c2_killed,
        "root_finalized_clean": root_finalized_clean,
        "victim_survived_kill": victim_survived,
        "any_killed": bool(r_killed or v_killed or c2_killed),
        "harness_timed_out": bool(r_killed),
    }

    # A case is "classified" if we have a definite story for the loss: a result JSON (future
    # classification + re-admit yes/no), OR a definite self-terminate, OR a definite hang.
    have_definite = bool(res is not None or root_self_terminated or root_hung)
    if kase == "C":
        # best-effort: classified if we learned anything, else best_effort (never inconclusive-gating)
        case["classification"] = "classified" if have_definite else "best_effort"
        case["outcome_classified"] = bool(have_definite)
    else:
        marker_race = bool(ready and not trigger_seen and not root_self_terminated
                           and not root_hung and res is None)
        if marker_race or not ready:
            case["classification"] = "inconclusive"
            case["outcome_classified"] = False
        else:
            case["classification"] = "classified"
            case["outcome_classified"] = True

    case["bootdir"] = bootdir
    return case


def _summarize(c):
    return (f"outcome={c['root_future_outcome']} self_term={c['root_self_terminated']} "
            f"hung={c['root_hung']} readmit={c['root_readmitted_after_loss']} "
            f"dead_stale={c['dead_locality_still_present']} "
            f"new_seen={c['new_locality_after_loss_seen']} class={c['classification']}")


def main():
    ap = argparse.ArgumentParser(description="exp50 strong-L4 ungraceful connect-loss probe")
    ap.add_argument("--binary", default=None, help="path to dist_fail_spike")
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--sleep-ms", type=int, default=8000)
    ap.add_argument("--wait-bound", type=int, default=15)
    ap.add_argument("--per-case-timeout", type=int, default=80)
    ap.add_argument("--aggregate", default=os.path.join(HERE, "aggregate.json"))
    args = ap.parse_args()

    binary = locate_binary(args.binary)
    agg = {
        "experiment": "50_strong_l4_connect_failure",
        "kind": "ungraceful_connect_loss_characterization",
        "ray_free": True, "single_node": True, "transport": "tcp_loopback",
        "crash_model": "SIGKILL of a connect-mode locality (crash analog, not a real Ray actor)",
        "binary": os.path.basename(binary) if binary else None,
    }

    if binary is None:
        agg["overall"] = "skip"
        for k in ("A", "B", "C"):
            agg[f"case_{k}"] = {"result": "skip", "reason": "binary not built"}
        _write_agg(args.aggregate, agg)
        print("SKIP: dist_fail_spike not found; build the experiment first (see CMakeLists.txt).")
        return 0

    print(f"[exp50] binary: {binary}")
    cases = {}
    for k in ("A", "B", "C"):
        label = {"A": "mid-flight kill (primary)",
                 "B": "kill after success (bracketing)",
                 "C": "kill before dispatch (best-effort)"}[k]
        print(f"[exp50] Case {k} -- {label} ...")
        c = run_case(k, binary, args.x, args.sleep_ms, args.wait_bound, args.per_case_timeout)
        print(f"[exp50] Case {k}: {_summarize(c)}")
        cases[k] = c

    a_ok = cases["A"]["outcome_classified"]
    b_ok = cases["B"]["outcome_classified"]
    overall = "characterized" if (a_ok and b_ok) else "inconclusive"

    agg["overall"] = overall
    agg["findings"] = {
        "case_a_outcome": cases["A"]["root_future_outcome"],
        "case_b_outcome": cases["B"]["root_future_outcome"],
        "case_c_outcome": cases["C"]["root_future_outcome"],
        "root_self_terminated_any": any(cases[k]["root_self_terminated"] for k in cases),
        "root_hung_any": any(cases[k]["root_hung"] for k in cases),
        "agas_retained_dead_locality_any": any(
            bool(cases[k]["dead_locality_still_present"]) for k in cases),
        "readmit_by_set_difference_worked_any": any(
            bool(cases[k]["root_readmitted_after_loss"]) for k in cases),
        "hpx_error_enums": {k: cases[k]["future_exception_hpx_error"] for k in cases},
        "note": (
            "Characterization only. SIGKILL of a connect-mode locality is the analog of a Ray "
            "actor crash; this records HPX's observed behavior on one node over loopback TCP. "
            "Root self-termination (Case A) is a first-class, valid outcome, not a harness bug; "
            "it is distinct from a harness-killed hang. Re-admit uses set-difference targeting "
            "(a locality id unseen before the kill), robust to AGAS retaining the dead locality "
            "or reusing its id. Not fault tolerance. Case C is best-effort / non-gating."
        ),
    }
    for k in cases:
        cases[k].pop("bootdir", None)  # drop machine-specific temp paths from the curated agg
    agg["case_A"] = cases["A"]
    agg["case_B"] = cases["B"]
    agg["case_C"] = cases["C"]
    _write_agg(args.aggregate, agg)
    print(f"[exp50] overall: {overall} -> {args.aggregate}")
    return 0


def _write_agg(path, agg):
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=False)
        f.write("\n")


if __name__ == "__main__":
    sys.exit(main())
