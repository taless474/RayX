#!/usr/bin/env python3
"""exp51 -- orchestrator for the Ray-free strong-L4 STALE-LOCALITY SHUTDOWN / CLEANUP /
RECOVERY-BOUNDARY probe.

STRUCTURAL CHARACTERIZATION only. NOT a performance result, NOT a Ray demo. exp50 showed that an
ungraceful non-root connect-mode locality loss (SIGKILL) leaves AGAS/locality state STALE, still
lets the root admit + serve a fresh connector by SET-DIFFERENCE targeting, yet makes the root HANG
at collective shutdown/finalize (no self-terminate, no HPX exception enum). exp51 asks: after that
stale state, is there ANY HPX-side bounded-finalize or local-cache cleanup that lets the root shut
down cleanly -- or is the safe policy external WHOLE-ISLAND restart?

This is NOT a search for fault tolerance. The likely design answer is already that no public AGAS
stale-locality eviction API exists and the island is the failure unit. The value here is to:
  1. LOCALIZE where the root hangs (capture a backtrace of the hung root before SIGKILL);
  2. confirm whether PUBLIC bounded finalize helps on this HPX build (probe P1);
  3. confirm whether LOCAL-CACHE cleanup helps (probe P2, expected refutation);
  4. establish external WHOLE-ISLAND restart as the recovery boundary (probe P3, policy not repair).

Probes (each on its own fresh root + ports + temp bootdir):

  P1 (bounded finalize)   : reproduce the exp50 loss + set-difference re-admit; root writes
                            `reached_finalize` then calls hpx::finalize(shutdown_timeout_us, ...).
                            The collective gather has no public per-locality timeout, so this is
                            EXPECTED TO HANG -- the orchestrator owns the real wall bound, captures
                            a backtrace, then SIGKILLs.
  P2 (local-cache cleanup): like P1, but the root snapshots the victim gid/endpoints while alive
                            and, after loss+re-admit, attempts remove_resolved_locality +
                            remove_from_connection_cache (internal-ish, LOCAL-CACHE, NOT public AGAS
                            eviction) before finalize. REFUTATION EXPECTATION: the dead locality is
                            still authoritative in the locality namespace, so the shutdown gather
                            re-targets it and clearing local caches does not cure the hang.
  P3 (whole-island policy): phase 1 reproduces the poisoned root, then the orchestrator EXTERNALLY
                            kills it (no repair attempt); phase 2 starts a FRESH root + fresh
                            connector on fresh ports/bootdir, serves one dist_probe, the connector
                            gracefully disconnects, and the fresh root finalizes CLEANLY. Recorded
                            as `external_restart_yields_clean_island` -- NOT repair, NOT fault
                            tolerance.

NOTE: hpx::terminate() (the in-process non-graceful analog of killing the island) bypasses clean
collective shutdown and is deliberately NOT used as a cleanup path. HPX resiliency / task-replay
modules are not tested here and do not imply membership / locality-loss recovery.

`--diag` (default off) adds best-effort HPX logging flags to the root; if the build does not
support them the experiment still proceeds. The load-bearing diagnostic is the BACKTRACE, not the
log.

CLAIM FENCE: Ray-free; single-node; loopback TCP only; stale-locality shutdown / cleanup
characterization only; SIGKILLed connector is a crash analog, not a real Ray actor; no
fault-tolerance claim; no crash-recovery generalization; no AGAS-root-loss recovery; cleanup APIs
used are local-cache/internal-ish and not public AGAS eviction; whole-island restart is external
supervision, not HPX fault tolerance; no Ray actor/bootstrap claim yet; no
performance/speedup/throughput/latency; no multi-node; no general fabric; no production/public API;
no Ray replacement; no "HPX faster than Ray"; no "RayX makes Ray faster".

Usage:
  python run_strong_l4_stale_shutdown.py [--binary <path>] [--x 7] [--sleep-ms 8000]
      [--wait-bound 15] [--finalize-bound 20] [--per-phase-timeout 80] [--diag]
      [--aggregate <path>]

Exit code is 0 even when a probe shows the root hung/poisoned (the aggregate carries the verdict);
it is non-zero only on an orchestrator-internal error.
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
    os.path.join(HERE, "build", "stale_shutdown_spike"),
    os.path.join(HERE, "build", "Release", "stale_shutdown_spike"),
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


# ----- HPX-expert correction #1: capture a backtrace of the hung root BEFORE SIGKILL -----

def _extract_top_frames(path, want=16):
    """Best-effort: pull the diagnostic shutdown/finalize call chain so the aggregate can say WHERE
    the root appears blocked, not merely that it hung. Keeps only real stack frames (sample annotates
    them with `(in <module>)`), and matches the membership/shutdown-wait machinery -- deliberately
    NOT the binary name (which itself contains 'shutdown')."""
    keys = ("runtime_distributed::wait", "wait_helper", "condition_variable::wait",
            "runtime_distributed::run", "hpx::finalize", "finalize(", "shutdown_all",
            "shutdown_function", "::agas", "parcelport", "parcelhandler", "big_boot", "stop_evt")
    out = []
    try:
        with open(path, errors="replace") as f:
            for line in f:
                if "(in " not in line:        # skip sample header / non-frame lines
                    continue
                low = line.lower()
                if any(k in low for k in keys):
                    s = line.strip()
                    if s and s not in out:
                        out.append(s)
                if len(out) >= want:
                    break
    except OSError:
        return []
    return out


def capture_backtrace(pid, out_path):
    """macOS-first hung-process backtrace: try `sample` then `lldb`. Best-effort; never raises.
    Returns (captured: bool, top_frames: list[str])."""
    # `sample <pid> 1 -file <out>` samples the live (hung) process for ~1s.
    try:
        subprocess.run(["sample", str(pid), "1", "-file", out_path],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=25)
    except Exception:
        pass
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return True, _extract_top_frames(out_path)
    # Fallback: lldb attach, dump all thread backtraces, detach.
    try:
        with open(out_path, "w") as f:
            subprocess.run(
                ["lldb", "-p", str(pid), "-o", "thread backtrace all", "-o", "detach",
                 "-o", "quit", "-b"],
                stdout=f, stderr=subprocess.STDOUT, timeout=30)
    except Exception:
        pass
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return True, _extract_top_frames(out_path)
    return False, []


def _last_shutdown_log_line(log_path):
    """Best-effort: last line in the root log that mentions shutdown machinery (only meaningful
    when --diag enabled a build that supports HPX logging)."""
    keys = ("shutdown", "finalize", "agas", "parcel", "disconnect")
    last = None
    try:
        with open(log_path, errors="replace") as f:
            for line in f:
                low = line.lower()
                if any(k in low for k in keys):
                    last = line.strip()
    except OSError:
        return None
    return last


def _diag_args():
    # HPX-expert correction #2: best-effort shutdown/AGAS logging. These keys are tolerated as
    # no-ops on builds without HPX_HAVE_LOGGING; if a build rejects them, root.ready simply will
    # not appear and the phase records that without crashing the experiment.
    return ["--hpx:ini=hpx.logging.level=5",
            "--hpx:ini=hpx.logging.destination=cerr"]


# ----- shared loss reproduction (P1/P2 and P3 phase 1) -----

def _root_cmd(binary, probe, bootdir, x, sleep_ms, wait_bound, step_timeout, fin_us, p0, diag):
    cmd = [
        binary, "--role", "f_root", "--probe", probe, "--bootstrap", bootdir,
        "--x", str(x), "--sleep-ms", str(sleep_ms), "--wait-bound", str(wait_bound),
        "--step-timeout", str(step_timeout), "--finalize-timeout-us", str(float(fin_us)),
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p0}",
        "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
    ]
    if diag:
        cmd += _diag_args()
    return cmd


def _victim_cmd(binary, bootdir, idle, p0, p1):
    return [
        binary, "--role", "f_connect", "--connector-kind", "victim", "--connector-index", "1",
        "--bootstrap", bootdir, "--victim-idle", str(idle),
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p1}",
        "--hpx:threads=1", "--hpx:bind=none",
    ]


def _clean_cmd(binary, bootdir, index, serve_timeout, p0, port):
    return [
        binary, "--role", "f_connect", "--connector-kind", "clean",
        "--connector-index", str(index), "--bootstrap", bootdir,
        "--serve-timeout", str(serve_timeout),
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{port}",
        "--hpx:threads=1", "--hpx:bind=none",
    ]


def _reproduce_loss(binary, probe, bootdir, ports, x, sleep_ms, wait_bound, step_timeout,
                    fin_us, per_phase_timeout, diag):
    """Launch root + victim, SIGKILL victim mid-flight, launch clean connector #2 for re-admit.
    Returns a dict of the live handles + observed markers; caller decides finalize handling."""
    p0, p1, p2 = ports
    root_cmd = _root_cmd(binary, probe, bootdir, x, sleep_ms, wait_bound, step_timeout, fin_us, p0,
                         diag)
    root, rlog = _popen(root_cmd, bootdir, os.path.join(bootdir, "root.log"))
    ready = _wait_for_file(os.path.join(bootdir, "root.ready"), root, step_timeout)

    trigger_seen = False
    victim = vlog = None
    if ready:
        victim, vlog = _popen(_victim_cmd(binary, bootdir, per_phase_timeout, p0, p1),
                              bootdir, os.path.join(bootdir, "victim.log"))
        # mid-flight: wait for the action body to start running on the victim, then SIGKILL it.
        trigger_seen = _wait_for_file(os.path.join(bootdir, "action_started"), victim, step_timeout)
        _kill_group(victim)

    c2 = c2log = None
    if ready and root.poll() is None:
        c2, c2log = _popen(_clean_cmd(binary, bootdir, 2, step_timeout + 10, p0, p2),
                           bootdir, os.path.join(bootdir, "clean2.log"))

    return {
        "root": root, "rlog": rlog, "victim": victim, "vlog": vlog, "c2": c2, "c2log": c2log,
        "ready": ready, "trigger_seen": trigger_seen,
    }


def _finalize_classify(state, bootdir, probe, finalize_bound, per_phase_timeout):
    """Wait for `reached_finalize`, then bound the finalize. If the root does not exit within the
    bound, capture a backtrace (HPX-expert correction #1) and SIGKILL. Returns a result dict."""
    root = state["root"]
    reached = _wait_for_file(os.path.join(bootdir, "reached_finalize"), root, per_phase_timeout)

    bt_captured, bt_top, bt_path = False, [], None
    finalize_returned_clean = False
    finalize_hung = False

    # Poll for exit WITHOUT killing (so a hung root is still alive when we sample it).
    deadline = time.time() + finalize_bound
    while time.time() < deadline and root.poll() is None:
        time.sleep(0.05)
    rc = root.poll()
    if reached and rc is not None and rc == 0:
        finalize_returned_clean = True
    elif reached and rc is None:
        finalize_hung = True  # reached finalize but still alive at the bound -> finalize hang

    # HPX-expert correction #1: capture a backtrace of the hung root BEFORE SIGKILL, so the
    # aggregate can say WHERE it is blocked, not merely that it hung.
    if root.poll() is None:
        bt_path = os.path.join(bootdir, "root_hang_backtrace.txt")
        bt_captured, bt_top = capture_backtrace(root.pid, bt_path)
        _kill_group(root)
        try:
            root.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass

    rc = root.poll()
    r_killed = state.get("_root_killed", False) or (rc is not None and rc < 0
                                                    and -rc == signal.SIGKILL)
    root_exit_signal = (-rc if (rc is not None and rc < 0) else None)
    res = _read_json(os.path.join(bootdir, f"{probe}_root_result.json"))
    # self-terminate = exited on its own without writing result and not by our SIGKILL
    root_self_terminated = bool(rc is not None and root_exit_signal != signal.SIGKILL
                                and res is None and not finalize_returned_clean)

    return {
        "reached_finalize": reached,
        "bounded_finalize_called": reached,
        "shutdown_timeout_us": (res or {}).get("finalize_shutdown_timeout_us"),
        "finalize_returned_clean": finalize_returned_clean,
        "finalize_hung": finalize_hung,
        "root_self_terminated": root_self_terminated,
        "root_exit_signal": root_exit_signal,
        "root_killed_by_harness": bool(root_exit_signal == signal.SIGKILL),
        "root_hang_backtrace_captured": bt_captured,
        "root_hang_backtrace_path": (os.path.basename(bt_path) if bt_path else None),
        "root_hang_top_frames": bt_top or None,
        "shutdown_log_path": "root.log",
        "last_shutdown_log_line": _last_shutdown_log_line(os.path.join(bootdir, "root.log")),
        "dead_locality_still_present": (res or {}).get("dead_locality_still_present"),
        "root_readmitted_after_loss": (res or {}).get("root_readmitted_after_loss"),
        "connector2_proved_remote": (res or {}).get("connector2_proved_remote"),
    }


def _drain(state):
    for key, tmo in (("victim", 5), ("c2", 8)):
        proc = state.get(key)
        if proc is not None:
            _wait(proc, time.time() + tmo)
    for logkey in ("rlog", "vlog", "c2log"):
        lg = state.get(logkey)
        if lg is not None:
            try:
                lg.close()
            except OSError:
                pass


def run_p1_p2(probe, binary, args):
    bootdir = tempfile.mkdtemp(prefix=f"exp51_{probe}_")
    ports = (find_free_port(), find_free_port(), find_free_port())
    step_timeout = max(12, args.per_phase_timeout // 4)
    state = _reproduce_loss(binary, probe, bootdir, ports, args.x, args.sleep_ms, args.wait_bound,
                            step_timeout, args.finalize_timeout_us, args.per_phase_timeout,
                            args.diag)
    out = {
        "probe": probe,
        "role": "bounded_finalize" if probe == "P1" else "local_cache_cleanup",
        "root_ready": state["ready"],
        "ports": {"root": f"127.0.0.1:{ports[0]}", "victim": f"127.0.0.1:{ports[1]}",
                  "clean2": f"127.0.0.1:{ports[2]}"},
        "trigger_marker_seen": state["trigger_seen"],
    }
    if not state["ready"]:
        _kill_group(state["root"])
        _drain(state)
        state["rlog"].close()
        out.update({"classification": "inconclusive", "outcome_classified": False,
                    "finalize_hung": None, "finalize_returned_clean": None})
        out["bootdir"] = bootdir
        return out

    fin = _finalize_classify(state, bootdir, probe, args.finalize_bound, args.per_phase_timeout)
    out.update(fin)
    _drain(state)

    if probe == "P2":
        cl = _read_json(os.path.join(bootdir, "cleanup_result.json")) or {}
        out["dead_gid_snapshotted"] = cl.get("dead_gid_snapshotted")
        out["dead_endpoints_snapshotted"] = cl.get("dead_endpoints_snapshotted")
        out["remove_resolved_locality_called"] = cl.get("remove_resolved_locality_called")
        out["remove_resolved_locality_returned"] = cl.get("remove_resolved_locality_returned")
        out["remove_resolved_locality_threw"] = cl.get("remove_resolved_locality_threw")
        out["remove_resolved_locality_exception"] = cl.get("remove_resolved_locality_exception")
        out["remove_from_connection_cache_called"] = cl.get("remove_from_connection_cache_called")
        out["remove_from_connection_cache_returned"] = cl.get(
            "remove_from_connection_cache_returned")
        out["remove_from_connection_cache_threw"] = cl.get("remove_from_connection_cache_threw")
        out["remove_from_connection_cache_exception"] = cl.get(
            "remove_from_connection_cache_exception")
        out["cleanup_cured_finalize_hang"] = bool(fin["finalize_returned_clean"])

    # classified iff we have a definite finalize verdict (clean OR hung/self-terminate).
    definite = bool(fin["finalize_returned_clean"] or fin["finalize_hung"]
                    or fin["root_self_terminated"])
    out["classification"] = "classified" if definite else "inconclusive"
    out["outcome_classified"] = definite
    out["bootdir"] = bootdir
    return out


def run_p3(binary, args):
    step_timeout = max(12, args.per_phase_timeout // 4)

    # --- Phase 1: reproduce the poisoned root, then EXTERNALLY kill it (no repair attempt). ---
    poison_dir = tempfile.mkdtemp(prefix="exp51_P3poison_")
    pports = (find_free_port(), find_free_port(), find_free_port())
    pstate = _reproduce_loss(binary, "P1", poison_dir, pports, args.x, args.sleep_ms,
                             args.wait_bound, step_timeout, args.finalize_timeout_us,
                             args.per_phase_timeout, args.diag)
    poisoned_confirmed = False
    if pstate["ready"]:
        # confirm poisoned: root reproduced the loss + reached finalize (it will hang there).
        reached = _wait_for_file(os.path.join(poison_dir, "reached_finalize"),
                                 pstate["root"], args.per_phase_timeout)
        pres = _read_json(os.path.join(poison_dir, "P1_root_result.json")) or {}
        poisoned_confirmed = bool(reached and pres.get("dead_locality_still_present"))
    _kill_group(pstate["root"])  # external whole-island kill of the poisoned root
    old_root_killed = True
    _drain(pstate)
    pstate["rlog"].close()

    # --- Phase 2: FRESH root + fresh connector on fresh ports/bootdir; expect clean finalize. ---
    fresh_dir = tempfile.mkdtemp(prefix="exp51_P3fresh_")
    f0, f1 = find_free_port(), find_free_port()
    fresh_cmd = [
        binary, "--role", "f_root", "--probe", "clean_island", "--bootstrap", fresh_dir,
        "--x", str(args.x), "--wait-bound", str(args.wait_bound),
        "--step-timeout", str(step_timeout),
        f"--hpx:agas=127.0.0.1:{f0}", f"--hpx:hpx=127.0.0.1:{f0}",
        "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
    ]
    if args.diag:
        fresh_cmd += _diag_args()
    froot, frlog = _popen(fresh_cmd, fresh_dir, os.path.join(fresh_dir, "root.log"))
    fresh_ready = _wait_for_file(os.path.join(fresh_dir, "root.ready"), froot, step_timeout)

    fconn = fclog = None
    fresh_joined = False
    if fresh_ready:
        fconn, fclog = _popen(_clean_cmd(binary, fresh_dir, 1, step_timeout + 10, f0, f1),
                              fresh_dir, os.path.join(fresh_dir, "clean1.log"))
        fresh_joined = _wait_for_file(os.path.join(fresh_dir, "connect.joined1"), fconn,
                                      step_timeout)

    fr_exited, fr_rc, fr_killed = _wait(froot, time.time() + args.per_phase_timeout)
    if fconn is not None:
        _wait(fconn, time.time() + 8)
        fclog.close()
    frlog.close()

    fres = _read_json(os.path.join(fresh_dir, "clean_island_result.json")) or {}
    d1 = _read_json(os.path.join(fresh_dir, "connect.disconnected1")) or {}
    fresh_finalized_clean = bool(fr_exited and not fr_killed and fr_rc == 0 and fres)
    fresh_proved = bool(fres.get("fresh_connector_proved_remote"))
    fresh_disc_clean = bool(d1.get("clean"))

    classified = bool(poisoned_confirmed) and (fresh_finalized_clean or fresh_ready)
    return {
        "probe": "P3",
        "role": "whole_island_restart_policy",
        "poisoned_root_confirmed": poisoned_confirmed,
        "old_root_killed_by_harness": old_root_killed,
        "fresh_root_ready": fresh_ready,
        "fresh_connector_joined": fresh_joined,
        "fresh_connector_proved_remote": fresh_proved,
        "fresh_connector_disconnected_clean": fresh_disc_clean,
        "fresh_root_finalized_clean": fresh_finalized_clean,
        "external_restart_yields_clean_island": bool(
            fresh_finalized_clean and fresh_proved and fresh_disc_clean),
        "ports": {"poison_root": f"127.0.0.1:{pports[0]}", "fresh_root": f"127.0.0.1:{f0}"},
        "classification": "classified" if classified else "inconclusive",
        "outcome_classified": classified,
        "bootdir": poison_dir,
        "fresh_bootdir": fresh_dir,
    }


def _summarize_p1p2(c):
    return (f"reached_finalize={c.get('reached_finalize')} "
            f"finalize_clean={c.get('finalize_returned_clean')} "
            f"finalize_hung={c.get('finalize_hung')} "
            f"bt={c.get('root_hang_backtrace_captured')} "
            f"readmit={c.get('root_readmitted_after_loss')} "
            f"class={c.get('classification')}")


def _summarize_p3(c):
    return (f"poisoned={c.get('poisoned_root_confirmed')} "
            f"fresh_clean={c.get('fresh_root_finalized_clean')} "
            f"clean_island={c.get('external_restart_yields_clean_island')} "
            f"class={c.get('classification')}")


def _write_agg(path, agg):
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=False)
        f.write("\n")


def main():
    ap = argparse.ArgumentParser(description="exp51 strong-L4 stale-locality shutdown probe")
    ap.add_argument("--binary", default=None, help="path to stale_shutdown_spike")
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--sleep-ms", type=int, default=8000)
    ap.add_argument("--wait-bound", type=int, default=15)
    ap.add_argument("--finalize-bound", type=int, default=20,
                    help="orchestrator wall bound on the root's bounded finalize")
    ap.add_argument("--finalize-timeout-us", type=float, default=5000000.0,
                    help="public hpx::finalize shutdown_timeout passed to the root (microseconds)")
    ap.add_argument("--per-phase-timeout", type=int, default=80)
    ap.add_argument("--diag", action="store_true",
                    help="add best-effort HPX shutdown/AGAS logging (tolerated if unsupported)")
    ap.add_argument("--aggregate", default=os.path.join(HERE, "aggregate.json"))
    args = ap.parse_args()

    binary = locate_binary(args.binary)
    agg = {
        "experiment": "51_strong_l4_stale_locality_shutdown",
        "kind": "stale_locality_shutdown_cleanup_characterization",
        "ray_free": True, "single_node": True, "transport": "tcp_loopback",
        "crash_model": "SIGKILL of a connect-mode locality (crash analog, not a real Ray actor)",
        "binary": os.path.basename(binary) if binary else None,
    }

    if binary is None:
        agg["overall"] = "skip"
        for k in ("P1", "P2", "P3"):
            agg[f"probe_{k}"] = {"result": "skip", "reason": "binary not built"}
        _write_agg(args.aggregate, agg)
        print("SKIP: stale_shutdown_spike not found; build the experiment first (CMakeLists.txt).")
        return 0

    print(f"[exp51] binary: {binary}")
    print("[exp51] P1 -- bounded finalize only ...")
    p1 = run_p1_p2("P1", binary, args)
    print(f"[exp51] P1: {_summarize_p1p2(p1)}")
    print("[exp51] P2 -- explicit local-cache cleanup, then finalize ...")
    p2 = run_p1_p2("P2", binary, args)
    print(f"[exp51] P2: {_summarize_p1p2(p2)}")
    print("[exp51] P3 -- whole-island external restart policy ...")
    p3 = run_p3(binary, args)
    print(f"[exp51] P3: {_summarize_p3(p3)}")

    overall = ("characterized"
               if (p1["outcome_classified"] and p2["outcome_classified"]
                   and p3["outcome_classified"])
               else "inconclusive")
    agg["overall"] = overall
    agg["findings"] = {
        "bounded_finalize_cured_hang": bool(p1.get("finalize_returned_clean")),
        "local_cleanup_apis_used": ["remove_resolved_locality", "remove_from_connection_cache"],
        "local_cleanup_cured_hang": bool(p2.get("cleanup_cured_finalize_hang")),
        "whole_island_restart_clean": bool(p3.get("external_restart_yields_clean_island")),
        "recovery_boundary": (
            "whole_island_external_restart"
            if not (p1.get("finalize_returned_clean") or p2.get("cleanup_cured_finalize_hang"))
            else "hpx_side_cleanup_available"),
        "root_hang_backtrace_captured_any": bool(
            p1.get("root_hang_backtrace_captured") or p2.get("root_hang_backtrace_captured")),
        "note": (
            "Characterization only. No PUBLIC AGAS stale-locality eviction API exists; the P2 "
            "cleanup calls are LOCAL-CACHE / internal-ish (remove_resolved_locality, "
            "remove_from_connection_cache), NOT authoritative eviction -- the dead locality stays "
            "in the locality namespace, so the shutdown gather can re-target it. Bounded "
            "hpx::finalize's shutdown_timeout governs LOCAL thread drain, not the collective "
            "membership gather. Whole-island restart (P3) is EXTERNAL supervision, not HPX fault "
            "tolerance, and demonstrates a clean fresh island, not repair of the poisoned root. "
            "hpx::terminate() and HPX resiliency modules are out of scope. Not fault tolerance."
        ),
    }
    for c in (p1, p2, p3):
        c.pop("bootdir", None)        # drop machine-specific temp paths from the curated aggregate
        c.pop("fresh_bootdir", None)
    agg["probe_P1"] = p1
    agg["probe_P2"] = p2
    agg["probe_P3"] = p3
    _write_agg(args.aggregate, agg)
    print(f"[exp51] overall: {overall} -> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
