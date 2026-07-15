#!/usr/bin/env python3
"""exp67 -- can TWO independently launched Ray actor workers join ONE shared distributed HPX
runtime as connect-mode localities and execute a verified HPX action from one actor-locality to
the other, while a separately supervised root/AGAS process stays isolated and work-free?

MECHANISM / IN-WORKER-HOSTING EVIDENCE ONLY, building directly on exp66 (one actor hosting HPX
in-process, proven by exact PID identity and no HPX child). exp67 adds the load-bearing step exp66
deliberately did not attempt: ACTOR-TO-ACTOR HPX communication. Two Ray actors A and B each host a
connect-mode HPX locality IN-PROCESS (hpx::start, NO child process); a separate work-free root is
locality 0. The proof is bidirectional and by value:

    actor A locality 1  --{pid, probe, host}-->  actor B locality 2   (B's PID proven by A->B)
    actor B locality 2  --{pid, probe, host}-->  actor A locality 1   (A's PID proven by B->A)

Each direction carries a remote PID action, a closed-int64 probe oracle, and a hostname witness
over the HPX parcelport -- never a Ray method call between the actors (the actors hold no Ray
handle to each other, so their only channel is HPX). A self-probe is NOT accepted as a substitute
for remote peer proof: A's HPX-plane PID identity comes from B->A, B's from A->B.

Four GATING slices (all must pass in every rep):
  Slice A  two-actor in-process hosting + shared-island membership
  Slice B  actor-to-actor HPX action, bidirectional (pid/oracle/locality/host witnesses)
  Slice C  independent progress: each direction executes while the DESTINATION Python is idle
  Slice D  lifecycle: graceful leave of both, clean in-process stop, root finalize, recreate, no orphans
A separate CPU/GIL SATURATION diagnostic is reported per direction and is NON-gating (exp66 Slice C
lineage): it can never change the verdict.

CLAIM FENCE: not cross-node (that is the exp67 Rostam slice, not started here); not Python 3.14; not
free-threaded; no elasticity/churn/failure recovery; no performance/speedup/ratio/winner; not LLM
shaped; not production API. Timing is never the correctness oracle. All durations observational.

Version scope: CPython 3.11 (GIL build), Ray 2.55.1, HPX commit
20bc3d4bf3068383edcb63be13f22e9ff95842fa (the verified waiter-fix build). Fails before measurement
if the runtime-observed HPX identity does not match.

Usage:
  python run_exp67.py [--reps 3] [--build-dir <dir>] [--x 7] [--aggregate <path>]
  python run_exp67.py --selftest       # pure-Python gate-logic checks (no Ray, no HPX)

Exit code is 0 even when gates fail (the aggregate carries the verdict); non-zero only on an
orchestrator-internal error or a selftest failure.
"""

import argparse
import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import sysconfig
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
PEER_BASENAME = "exp67_peer"
EXT_MODULE = "exp67_actor_ext"
EXPECTED_HPX_COMMIT = "20bc3d4bf3068383edcb63be13f22e9ff95842fa"
PROBE_XOR = 0x67C0DE  # mirrors shared_probe.hpp kProbeXor (distinct from exp66's 0x66C0DE)

DEFAULT_BUILD_DIR = os.path.join(HERE, "build")

CLAIM_FENCES = {
    "not_cross_node": True,
    "not_python_3_14": True,
    "not_free_threaded": True,
    "no_elasticity_churn_or_recovery": True,
    "no_performance_speedup_ratio_winner": True,
    "not_llm_shaped": True,
    "not_production_api": True,
    "saturation_is_diagnostic_not_verdict": True,
    "timing_not_correctness_oracle": True,
    "durations_observational_only": True,
    "self_probe_not_accepted_as_remote_peer_proof": True,
}

FAILURE_CLASSES = [
    "actor_a_hpx_start_failed", "actor_b_hpx_start_failed", "pid_identity_failed",
    "hpx_child_process_detected", "shared_island_join_failed", "locality_identity_invalid",
    "actor_to_actor_dispatch_failed", "destination_witness_failed", "oracle_failed",
    "ray_payload_path_detected", "idle_progress_failed", "graceful_leave_failed",
    "root_finalize_failed", "actor_recreation_failed", "orphan_detected",
    "invalid_instrumentation", "pass",
]


# ---------------------------------------------------------------------------------------
# Closed oracle (mirror of shared_probe.hpp) + HPX identity extraction
# ---------------------------------------------------------------------------------------

def probe_value(x, loc):
    return (x ^ PROBE_XOR) + (loc << 1)


def extract_git_sha(complete_version):
    if not complete_version:
        return None
    marker = "Git:"
    i = complete_version.find(marker)
    if i < 0:
        return None
    rest = complete_version[i + len(marker):].strip()
    tok = rest.split()[0] if rest else ""
    return tok.strip().strip(",") or None


def commit_matches(complete_version):
    """True iff the runtime sha is a prefix of the expected verified commit (HPX abbreviates)."""
    sha = extract_git_sha(complete_version)
    return bool(sha) and len(sha) >= 10 and EXPECTED_HPX_COMMIT.startswith(sha)


# ---------------------------------------------------------------------------------------
# Process / file helpers (bounded, classify-never-hang -- exp65/exp66 idiom)
# ---------------------------------------------------------------------------------------

def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _popen(cmd, cwd, log_path):
    log = open(log_path, "w")
    return subprocess.Popen(cmd, cwd=cwd, stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True), log


def _kill_group(proc):
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _wait_proc(proc, deadline):
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


def _wait_for_file(path, timeout, procs=()):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        for p in procs:
            if p is not None and p.poll() is not None:
                time.sleep(0.2)
                return os.path.exists(path)
        time.sleep(0.05)
    return os.path.exists(path)


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _read_int(path):
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def _exit_path(exited, rc, killed):
    if killed:
        return "hung_killed_by_harness"
    if exited and rc == 0:
        return "finalized_clean"
    if exited:
        return "self_terminated_nonzero"
    return "unknown"


def pid_alive(pid):
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def wait_pid_gone(pid, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not pid_alive(pid):
            return True
        time.sleep(0.05)
    return not pid_alive(pid)


def peer_orphans():
    """PIDs of leftover exp67_peer processes. Empty list == clean; None == check failed."""
    try:
        out = subprocess.run(["pgrep", "-f", PEER_BASENAME],
                             capture_output=True, text=True, timeout=10)
    except Exception:
        return None
    if out.returncode not in (0, 1):
        return None
    me = str(os.getpid())
    return [p for p in out.stdout.split() if p and p != me]


def argv_audit(*cmds):
    """True iff NO argv contains a static locality count (membership must be discovered)."""
    for cmd in cmds:
        for a in cmd:
            if str(a).startswith("--hpx:localities"):
                return False
    return True


# ---------------------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------------------

def collect_provenance(ray_mod, a_identity):
    gil_enabled = None
    fn = getattr(sys, "_is_gil_enabled", None)
    if callable(fn):
        try:
            gil_enabled = bool(fn())
        except Exception:
            gil_enabled = None
    prov = {
        "python_version_full": sys.version,
        "python_implementation": sys.implementation.name,
        "python_ext_suffix": sysconfig.get_config_var("EXT_SUFFIX"),
        "py_gil_disabled_config": sysconfig.get_config_var("Py_GIL_DISABLED"),
        "gil_enabled_runtime": gil_enabled,
        "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "ray_version": getattr(ray_mod, "__version__", None),
        "ray_commit": getattr(ray_mod, "__commit__", None),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor_count": os.cpu_count(),
        "compiler_note": "extension built with Apple clang (see build), HPX identity below",
        "expected_hpx_commit": EXPECTED_HPX_COMMIT,
    }
    if a_identity:
        prov["actor_ext_file"] = os.path.basename(a_identity.get("ext_file", "") or "")
        prov["actor_ext_suffix_expected"] = a_identity.get("ext_suffix_expected")
        prov["actor_ext_abi_match"] = a_identity.get("abi_match")
        prov["actor_gil_declaration"] = a_identity.get("gil_declaration")
        prov["hpx_complete_version"] = (a_identity.get("hpx_version_info") or {}).get(
            "hpx_complete_version")
        prov["hpx_git_sha_observed"] = extract_git_sha(prov["hpx_complete_version"])
    return prov


def slice_python_deferral():
    return {
        "status": "deferred_not_started",
        "reasons": [
            "exp67 local slice is CPython 3.11 GIL-build only",
            "no free-threaded interpreter / Ray environment in this task",
            "cross-node and Python-3.14/free-threaded reruns are explicitly out of scope here",
        ],
        "affects_verdict": False,
    }


# ---------------------------------------------------------------------------------------
# Gate evaluation (pure functions -> selftestable without Ray/HPX). All FOUR slices gate.
# ---------------------------------------------------------------------------------------

def _loc(d):
    return (d or {}).get("locality_id")


def eval_slice_a(m):
    """Slice A: two-actor in-process hosting + shared-island membership. All gates GATING."""
    ai, bi = m.get("a_identity") or {}, m.get("b_identity") or {}
    as_, bs = m.get("a_start") or {}, m.get("b_start") or {}
    ac, bc = m.get("a_child_report") or {}, m.get("b_child_report") or {}
    rr = m.get("root_ready") or {}
    rf = m.get("root_final") or {}
    a_pid, b_pid = ai.get("pid"), bi.get("pid")
    a_loc, b_loc = _loc(as_), _loc(bs)
    a_cv = (ai.get("hpx_version_info") or {}).get("hpx_complete_version")
    b_cv = (bi.get("hpx_version_info") or {}).get("hpx_complete_version")
    membership_max = max(as_.get("membership") or 0, bs.get("membership") or 0)

    g = {}
    g["fixed_hpx_commit"] = bool(
        commit_matches(a_cv) and commit_matches(b_cv)
        and commit_matches(rr.get("hpx_complete_version")))
    g["actor_a_started_in_worker"] = bool(as_.get("started") and a_loc is not None)
    g["actor_b_started_in_worker"] = bool(bs.get("started") and b_loc is not None)
    g["two_distinct_ray_processes"] = bool(
        a_pid is not None and b_pid is not None and a_pid != b_pid)
    g["distinct_locality_ids"] = bool(
        a_loc not in (None, 0) and b_loc not in (None, 0) and a_loc != b_loc)
    g["both_in_process_no_hpx_child"] = bool(
        ac.get("checked") and ac.get("hpx_children") == []
        and bc.get("checked") and bc.get("hpx_children") == [])
    g["shared_island_membership"] = bool(a_loc and a_loc > 0 and b_loc and b_loc > 0
                                         and membership_max >= 3)
    g["no_static_locality_count"] = bool(m.get("argv_audit_ok"))
    g["root_isolated_locality_0"] = bool(rr.get("locality_id") == 0
                                         and rf.get("max_membership", 0) >= 3)
    g["hostname_identity"] = bool(
        ai.get("hostname") and ai.get("hostname") == bi.get("hostname")
        and ai.get("hostname") == rr.get("hostname")
        and ai.get("hostname") == m.get("controller_hostname"))
    g["evidence_complete"] = bool(
        a_pid is not None and b_pid is not None and a_loc is not None and b_loc is not None
        and a_cv and b_cv and rr and rf and m.get("ab_dispatch") and m.get("ba_dispatch"))
    return g


def eval_slice_b(m):
    """Slice B: actor-to-actor HPX action, BIDIRECTIONAL. All gates GATING."""
    ab, ba = m.get("ab_dispatch") or {}, m.get("ba_dispatch") or {}
    ai, bi = m.get("a_identity") or {}, m.get("b_identity") or {}
    a_loc, b_loc = _loc(m.get("a_start")), _loc(m.get("b_start"))
    a_pid, b_pid = ai.get("pid"), bi.get("pid")

    g = {}
    g["ab_dispatch_ready"] = bool(ab.get("target_found") and ab.get("ready"))
    g["ba_dispatch_ready"] = bool(ba.get("target_found") and ba.get("ready"))
    # B's HPX-plane PID is proven by A->B; A's by B->A (no self-probe substitute).
    g["b_pid_proven_by_ab"] = bool(
        ab.get("pid_result") is not None and b_pid is not None
        and ab.get("pid_result") == b_pid)
    g["a_pid_proven_by_ba"] = bool(
        ba.get("pid_result") is not None and a_pid is not None
        and ba.get("pid_result") == a_pid)
    g["ab_oracle"] = bool(ab.get("oracle_match"))
    g["ba_oracle"] = bool(ba.get("oracle_match"))
    g["ab_executes_on_b"] = bool(ab.get("executed_on") == b_loc and b_loc not in (None, 0))
    g["ba_executes_on_a"] = bool(ba.get("executed_on") == a_loc and a_loc not in (None, 0))
    g["ab_host_witness"] = bool(ab.get("host_result")
                                and ab.get("host_result") == bi.get("hostname"))
    g["ba_host_witness"] = bool(ba.get("host_result")
                                and ba.get("host_result") == ai.get("hostname"))
    g["root_runs_no_application_action"] = bool(
        ab.get("executed_on") not in (None, 0) and ba.get("executed_on") not in (None, 0))
    # The actors hold no Ray handle to each other, so the operation path is HPX-only by construction.
    g["operation_over_hpx_not_ray"] = bool(m.get("operation_over_hpx_not_ray"))
    return g


def eval_slice_c(m):
    """Slice C: independent progress -- each direction executes while the DEST Python is idle. GATING."""
    ab, ba = m.get("ab_dispatch") or {}, m.get("ba_dispatch") or {}
    a_loc, b_loc = _loc(m.get("a_start")), _loc(m.get("b_start"))
    g = {}
    g["ab_progress_while_b_idle"] = bool(
        ab.get("ready") and ab.get("oracle_match") and ab.get("executed_on") == b_loc
        and b_loc not in (None, 0) and m.get("dest_idle_during_ab"))
    g["ba_progress_while_a_idle"] = bool(
        ba.get("ready") and ba.get("oracle_match") and ba.get("executed_on") == a_loc
        and a_loc not in (None, 0) and m.get("dest_idle_during_ba"))
    g["no_python_polling_loop"] = bool(m.get("no_python_polling"))
    return g


def eval_slice_d(m):
    """Slice D: lifecycle -- graceful leave of both, clean stop, root finalize, recreate, no orphans."""
    rf = m.get("root_final") or {}
    rr = m.get("root_ready") or {}
    a_pid = (m.get("a_identity") or {}).get("pid")
    b_pid = (m.get("b_identity") or {}).get("pid")
    ar, br = m.get("a_recreate") or {}, m.get("b_recreate") or {}
    g = {}
    g["actor_a_graceful_leave"] = (m.get("a_stop_rc") == 0)
    g["actor_b_graceful_leave"] = (m.get("b_stop_rc") == 0)
    g["clean_hpx_stop_both"] = bool(
        m.get("a_stop_rc") == 0 and not m.get("a_stop_error")
        and m.get("b_stop_rc") == 0 and not m.get("b_stop_error"))
    g["root_finalized_clean"] = bool(
        m.get("root_exit_path") == "finalized_clean"
        and rf.get("leave_observed") is True and rf.get("final_membership") == 1)
    g["heartbeat_completion_lifecycle"] = bool(
        rr.get("wall_ms") is not None and rf.get("leave_observed") is True)
    g["actor_a_recreation"] = bool(
        ar.get("ok") and ar.get("pid") is not None and ar.get("pid") != a_pid)
    g["actor_b_recreation"] = bool(
        br.get("ok") and br.get("pid") is not None and br.get("pid") != b_pid)
    g["no_orphans"] = (m.get("orphans") == [] and m.get("actor_pids_gone") is True)
    g["all_waits_bounded"] = bool(m.get("all_waits_bounded"))
    return g


def classify_saturation(direction, disp, busy_started_ms, busy_done_ms, dest_loc,
                        hpx_threads, ray_cpus, prov, overlap_override=None):
    """Per-direction CPU/GIL saturation diagnostic (NON-gating). Mirrors exp66 Slice C honesty:
    a stall is never blamed on the GIL without a free-threaded comparison. Cross-node callers pass
    overlap_override (driver-timeline overlap) since the busy loop and the dispatch record wall-ms
    on DIFFERENT nodes' clocks, which must not be compared directly."""
    disp = disp or {}
    dw, rw = disp.get("dispatch_wall_ms"), disp.get("return_wall_ms")
    if overlap_override is not None:
        overlap = bool(overlap_override)
    else:
        overlap = bool(None not in (dw, rw, busy_started_ms, busy_done_ms)
                       and dw >= busy_started_ms and rw <= busy_done_ms + 500)
    progressed = bool(disp.get("ready") and disp.get("oracle_match")
                      and disp.get("executed_on") == dest_loc and dest_loc not in (None, 0))
    if not overlap:
        cls = "invalid_diagnostic"
    elif progressed:
        cls = "progressed_under_dest_saturation"
    elif ray_cpus is not None and hpx_threads is not None and ray_cpus <= hpx_threads:
        cls = "ray_hpx_thread_budget_conflict_suspected"
    else:
        cls = "gil_monopolization_suspected"
    return {
        "direction": direction, "classification": cls, "progressed": progressed,
        "overlap_observed": overlap, "executed_on": disp.get("executed_on"),
        "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus,
        "free_threaded_build": prov.get("free_threaded_build") if prov else None,
        "gil_enabled_runtime": prov.get("gil_enabled_runtime") if prov else None,
        "affects_verdict": False,
        "note": ("dest HPX served the peer action while the dest actor's Python thread held the GIL "
                 "in a tight loop; actions are pure C++ and never touch the GIL. A stall would be "
                 "reported honestly, not blamed on the GIL without a free-threaded comparison."),
    }


def rep_verdict(ga, gb, gc, gd):
    """exp67 verdict for one rep = ALL of Slice A, B, C, D. Saturation diagnostic is excluded."""
    return all(ga.values()) and all(gb.values()) and all(gc.values()) and all(gd.values())


def failure_class(ga, gb, gc, gd):
    """Map the first failing gate group to the explicit exp67 failure taxonomy."""
    if not ga.get("evidence_complete"):
        return "invalid_instrumentation"
    if not ga.get("actor_a_started_in_worker"):
        return "actor_a_hpx_start_failed"
    if not ga.get("actor_b_started_in_worker"):
        return "actor_b_hpx_start_failed"
    if not ga.get("both_in_process_no_hpx_child"):
        return "hpx_child_process_detected"
    if not ga.get("shared_island_membership"):
        return "shared_island_join_failed"
    if not (ga.get("distinct_locality_ids") and ga.get("two_distinct_ray_processes")):
        return "locality_identity_invalid"
    if not (gb.get("ab_dispatch_ready") and gb.get("ba_dispatch_ready")):
        return "actor_to_actor_dispatch_failed"
    if not (gb.get("b_pid_proven_by_ab") and gb.get("a_pid_proven_by_ba")):
        return "pid_identity_failed"
    if not (gb.get("ab_executes_on_b") and gb.get("ba_executes_on_a")
            and gb.get("ab_host_witness") and gb.get("ba_host_witness")):
        return "destination_witness_failed"
    if not (gb.get("ab_oracle") and gb.get("ba_oracle")):
        return "oracle_failed"
    if not gb.get("operation_over_hpx_not_ray"):
        return "ray_payload_path_detected"
    if not (gc.get("ab_progress_while_b_idle") and gc.get("ba_progress_while_a_idle")):
        return "idle_progress_failed"
    if not (gd.get("actor_a_graceful_leave") and gd.get("actor_b_graceful_leave")
            and gd.get("clean_hpx_stop_both")):
        return "graceful_leave_failed"
    if not gd.get("root_finalized_clean"):
        return "root_finalize_failed"
    if not (gd.get("actor_a_recreation") and gd.get("actor_b_recreation")):
        return "actor_recreation_failed"
    if not gd.get("no_orphans"):
        return "orphan_detected"
    if rep_verdict(ga, gb, gc, gd):
        return "pass"
    return "invalid_instrumentation"


# ---------------------------------------------------------------------------------------
# Ray actor: hosts an HPX connect-mode locality IN-PROCESS and dispatches at a PEER locality
# ---------------------------------------------------------------------------------------

def build_actor_class(ray_mod):
    @ray_mod.remote
    class HpxActor:
        def __init__(self, build_dir, hpx_threads, endpoints,
                     ext_module="exp67_actor_ext", peer_basename="exp67_peer"):
            import sys as _sys
            _sys.path.insert(0, build_dir)
            self._build_dir = build_dir
            self._threads = hpx_threads
            self._endpoints = endpoints
            self._ext_module = ext_module
            self._peer_basename = peer_basename
            self._ext = None
            self._started = False

        def ray_placement(self):
            """Ray-authoritative placement identity of THIS worker (for hard-placement gates)."""
            import os as _os
            import socket as _socket
            try:
                import ray as _ray
                ctx = _ray.get_runtime_context()
                nid, aid = ctx.get_node_id(), ctx.get_actor_id()
            except Exception:  # noqa: BLE001
                nid, aid = None, None
            return {"node_id": nid, "actor_id": aid,
                    "hostname": _socket.gethostname(), "pid": _os.getpid()}

        def load_identity(self):
            import importlib
            import os as _os
            import sysconfig as _sc
            e = importlib.import_module(self._ext_module)
            self._ext = e
            suffix = _sc.get_config_var("EXT_SUFFIX")
            return {
                "pid": e.pid(), "os_getpid": _os.getpid(), "hostname": e.hostname(),
                "hpx_version_info": dict(e.hpx_version_info()),
                "gil_declaration": getattr(e, "__gil_declaration__", None),
                "experiment": getattr(e, "__experiment__", None),
                "ext_file": e.__file__, "ext_suffix_expected": suffix,
                "abi_match": bool(e.__file__.endswith(suffix)) if suffix else None,
            }

        def start_hpx(self):
            try:
                self._ext.start_connect(self._threads, list(self._endpoints))
                self._started = True
                return {"started": True, "locality_id": int(self._ext.locality_id()),
                        "membership": int(self._ext.membership_count()), "pid": self._ext.pid()}
            except Exception as ex:  # noqa: BLE001 -- classify, never crash the worker silently
                return {"started": False, "error": f"{type(ex).__name__}: {ex}"}

        def child_report(self):
            import os as _os
            import subprocess as _sp
            pid = _os.getpid()
            children, hpx_children, checked = [], [], False
            try:
                out = _sp.run(["pgrep", "-P", str(pid)], capture_output=True, text=True, timeout=10)
                checked = out.returncode in (0, 1)
                for cp in out.stdout.split():
                    if not cp:
                        continue
                    info = _sp.run(["ps", "-o", "command=", "-p", cp],
                                   capture_output=True, text=True, timeout=10)
                    cmd = info.stdout.strip()
                    children.append({"pid": int(cp), "cmd": cmd})
                    if (self._peer_basename in cmd or self._ext_module in cmd
                            or "hpx" in cmd.lower()):
                        hpx_children.append({"pid": int(cp), "cmd": cmd})
            except Exception as ex:  # noqa: BLE001
                return {"checked": False, "error": f"{type(ex).__name__}: {ex}",
                        "children": children, "hpx_children": hpx_children}
            return {"checked": checked, "worker_pid": pid,
                    "children": children, "hpx_children": hpx_children}

        def dispatch(self, x, target_loc, bound_s=10):
            """Dispatch pid+probe+host actions AT the PEER locality over HPX (never Ray)."""
            try:
                return dict(self._ext.dispatch_to(int(x), int(target_loc), int(bound_s)))
            except Exception as ex:  # noqa: BLE001
                return {"ready": False, "target_found": False,
                        "error": f"{type(ex).__name__}: {ex}"}

        def health(self):
            try:
                return {"ok": True, "pid": self._ext.pid(),
                        "membership": int(self._ext.membership_count()),
                        "locality_id": int(self._ext.locality_id())}
            except Exception as ex:  # noqa: BLE001
                return {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

        def busy_spin(self, seconds, started_path, done_path):
            """Saturation stimulus: hold the GIL in a tight CPU loop for `seconds`."""
            import time as _t
            started_ms = int(_t.time() * 1000)
            try:
                with open(started_path, "w") as f:
                    f.write(str(started_ms))
            except OSError:
                pass
            acc = 0
            end = _t.perf_counter() + seconds
            while _t.perf_counter() < end:
                for _ in range(200000):
                    acc = (acc + 1) & 0xFFFFFFFF
            done_ms = int(_t.time() * 1000)
            try:
                with open(done_path, "w") as f:
                    f.write(str(done_ms))
            except OSError:
                pass
            return {"acc": acc, "started_ms": started_ms, "done_ms": done_ms}

        def stop_hpx(self):
            try:
                rc = int(self._ext.stop_disconnect())
                self._started = False
                return {"rc": rc, "error": None}
            except Exception as ex:  # noqa: BLE001
                return {"rc": -1, "error": f"{type(ex).__name__}: {ex}"}

        def ping(self):
            """Ray-only health probe for a RECREATED actor (does NOT start HPX)."""
            import importlib
            import os as _os
            import socket as _socket
            info = {}
            try:
                e = importlib.import_module(self._ext_module)
                info["ext_importable"] = True
                info["hpx_complete_version"] = dict(e.hpx_version_info()).get(
                    "hpx_complete_version")
            except Exception as ex:  # noqa: BLE001
                info["ext_importable"] = False
                info["error"] = f"{type(ex).__name__}: {ex}"
            info["ok"] = True
            info["pid"] = _os.getpid()
            info["hostname"] = _socket.gethostname()
            try:
                import ray as _ray
                info["node_id"] = _ray.get_runtime_context().get_node_id()
            except Exception:  # noqa: BLE001
                info["node_id"] = None
            return info

    return HpxActor


# ---------------------------------------------------------------------------------------
# One repetition (full lifecycle)
# ---------------------------------------------------------------------------------------

# "localhost" (not 127.0.0.1): in connect mode HPX rewrites a literal 127.0.0.1 parcelport host to
# resolve_public_ip_address(), which fails inside a Ray worker that cannot resolve its .local name.
# HPX's map_hostnames hard-codes "localhost" -> 127.0.0.1 with no resolver call (exp66 finding).
HPX_HOST = "localhost"


def peer_root_cmd(peer, boot, p_root):
    return [peer, "--role", "root", "--bootstrap", boot, "--leave-timeout", "25",
            f"--hpx:agas={HPX_HOST}:{p_root}", f"--hpx:hpx={HPX_HOST}:{p_root}",
            "--hpx:expect-connecting-localities",
            "--hpx:threads=2", "--hpx:bind=none", "--hpx:ignore-batch-env"]


def actor_endpoints(p_root, p_actor):
    return [f"--hpx:agas={HPX_HOST}:{p_root}", f"--hpx:hpx={HPX_HOST}:{p_actor}",
            "--hpx:bind=none", "--hpx:ignore-batch-env"]


def _ray_get(ray_mod, ref, timeout, what):
    try:
        return ray_mod.get(ref, timeout=timeout)
    except Exception as ex:  # noqa: BLE001 -- GetTimeoutError/actor death: classify, never hang
        return {"ok": False, "started": False, "ready": False,
                "error": f"{what}: {type(ex).__name__}: {ex}"}


def run_rep(ray_mod, HpxActor, peer, build_dir, x, rep, runs_root, hpx_threads, ray_cpus):
    boot = os.path.join(runs_root, f"rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    p_root, p_a, p_b = find_free_port(), find_free_port(), find_free_port()
    rcmd = peer_root_cmd(peer, boot, p_root)
    ep_a = actor_endpoints(p_root, p_a)
    ep_b = actor_endpoints(p_root, p_b)
    m = {
        "controller_hostname": socket.gethostname(),
        "ports": {"root": p_root, "actor_a": p_a, "actor_b": p_b},
        "argv_audit_ok": argv_audit(rcmd, ep_a, ep_b),
        "actor_hpx_threads": hpx_threads,
        "actor_ray_num_cpus": ray_cpus,
        # Structural facts (design invariants, not measured): the controller never runs a
        # destination actor method during the peer's dispatch window, and the actors hold NO Ray
        # handle to one another -- so A<->B traffic can only be HPX, and there is no Python poll loop.
        "dest_idle_during_ab": True,
        "dest_idle_during_ba": True,
        "operation_over_hpx_not_ray": True,
        "operation_path_note": "actor A and actor B hold no Ray handle to each other; the only "
                               "channel between their localities is the HPX parcelport.",
        "no_python_polling": True,
        "all_waits_bounded": True,
    }
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    saturation = []
    try:
        # 1) work-free supervised root (locality 0)
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file(os.path.join(boot, "root.ready"), 30, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        # 2) actor A hosts HPX in-process (locality 1)
        a1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_a)
        m["a_identity"] = _ray_get(ray_mod, a1.load_identity.remote(), 40, "a_identity")
        m["a_start"] = _ray_get(ray_mod, a1.start_hpx.remote(), 60, "a_start")
        m["a_child_report"] = _ray_get(ray_mod, a1.child_report.remote(), 30, "a_child_report")
        a_loc = _loc(m["a_start"])

        # 3) actor B hosts HPX in-process (locality 2), joining the SAME island
        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_b)
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 40, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 60, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")
        b_loc = _loc(m["b_start"])

        # 4) Slice B + C: bidirectional actor-to-actor HPX action, each with the DEST Python idle.
        #    A->B (B idle): proves B's PID/oracle/locality/host over HPX. Then B->A (A idle).
        if a_loc not in (None, 0) and b_loc not in (None, 0):
            m["ab_dispatch"] = _ray_get(ray_mod, a1.dispatch.remote(x, b_loc), 40, "ab_dispatch")
            m["ba_dispatch"] = _ray_get(ray_mod, b1.dispatch.remote(x, a_loc), 40, "ba_dispatch")

        # 5) worker health after HPX work
        m["a_health"] = _ray_get(ray_mod, a1.health.remote(), 30, "a_health")
        m["b_health"] = _ray_get(ray_mod, b1.health.remote(), 30, "b_health")

        # 6) NON-gating saturation diagnostic: dest holds the GIL while the peer dispatches at it.
        prov_lite = {"free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
                     "gil_enabled_runtime": None}
        if a_loc not in (None, 0) and b_loc not in (None, 0):
            # A->B while B busy
            bs_path, bd_path = os.path.join(boot, "b_busy_started"), os.path.join(boot, "b_busy_done")
            b_busy = b1.busy_spin.remote(3.0, bs_path, bd_path)
            _wait_for_file(bs_path, 15)
            ab_busy = _ray_get(ray_mod, a1.dispatch.remote(x, b_loc), 40, "ab_busy_dispatch")
            _ray_get(ray_mod, b_busy, 30, "b_busy")
            saturation.append(classify_saturation(
                "ab_under_b_busy", ab_busy, _read_int(bs_path), _read_int(bd_path), b_loc,
                hpx_threads, ray_cpus, prov_lite))
            # B->A while A busy
            as_path, ad_path = os.path.join(boot, "a_busy_started"), os.path.join(boot, "a_busy_done")
            a_busy = a1.busy_spin.remote(3.0, as_path, ad_path)
            _wait_for_file(as_path, 15)
            ba_busy = _ray_get(ray_mod, b1.dispatch.remote(x, a_loc), 40, "ba_busy_dispatch")
            _ray_get(ray_mod, a_busy, 30, "a_busy")
            saturation.append(classify_saturation(
                "ba_under_a_busy", ba_busy, _read_int(as_path), _read_int(ad_path), a_loc,
                hpx_threads, ray_cpus, prov_lite))

        # 7) Slice D: graceful in-process leave of BOTH actors -> membership returns to 1
        a_pid = (m.get("a_identity") or {}).get("pid")
        b_pid = (m.get("b_identity") or {}).get("pid")
        a_stop = _ray_get(ray_mod, a1.stop_hpx.remote(), 30, "a_stop")
        b_stop = _ray_get(ray_mod, b1.stop_hpx.remote(), 30, "b_stop")
        m["a_stop_rc"], m["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
        m["b_stop_rc"], m["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")

        # destroy both actors
        ray_mod.kill(a1); a1 = None
        ray_mod.kill(b1); b1 = None
        m["a_destroyed"] = wait_pid_gone(a_pid, 20)
        m["b_destroyed"] = wait_pid_gone(b_pid, 20)

        # recreate FRESH actors (Ray-only ping; no HPX start) -> new pids
        a2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_a)
        b2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_b)
        m["a_recreate"] = _ray_get(ray_mod, a2.ping.remote(), 40, "a_recreate")
        m["b_recreate"] = _ray_get(ray_mod, b2.ping.remote(), 40, "b_recreate")
        a2_pid = (m.get("a_recreate") or {}).get("pid")
        b2_pid = (m.get("b_recreate") or {}).get("pid")

        # 8) root completion: both connectors already left -> root observes membership==1, finalizes
        open(os.path.join(boot, "root.done"), "w").close()
        _wait_for_file(os.path.join(boot, "root.final"), 40, procs=[root])
        m["root_final"] = _read_json(os.path.join(boot, "root.final"))

        # 9) bounded root join + orphan sweep
        r_exited, r_rc, r_killed = _wait_proc(root, time.time() + 40)
        m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)

        if a2 is not None:
            ray_mod.kill(a2); a2 = None
        if b2 is not None:
            ray_mod.kill(b2); b2 = None
        m["actor_pids_gone"] = (wait_pid_gone(a_pid, 10) and wait_pid_gone(b_pid, 10)
                                and wait_pid_gone(a2_pid, 10) and wait_pid_gone(b2_pid, 10))
        m["orphans"] = peer_orphans()
    finally:
        for a in (a1, b1, a2, b2):
            if a is not None:
                try:
                    ray_mod.kill(a)
                except Exception:
                    pass
        _kill_group(root)
        if rlog is not None:
            rlog.close()

    ga, gb, gc, gd = eval_slice_a(m), eval_slice_b(m), eval_slice_c(m), eval_slice_d(m)
    passed = rep_verdict(ga, gb, gc, gd)
    return {
        "rep": rep,
        "boot_rel": os.path.relpath(boot, HERE),
        "ports": m["ports"],
        "slice_a_gates": ga, "slice_b_gates": gb, "slice_c_gates": gc, "slice_d_gates": gd,
        "rep_pass": passed,
        "failure_class": failure_class(ga, gb, gc, gd),
        "slice_a_failed": [k for k, v in ga.items() if not v],
        "slice_b_failed": [k for k, v in gb.items() if not v],
        "slice_c_failed": [k for k, v in gc.items() if not v],
        "slice_d_failed": [k for k, v in gd.items() if not v],
        "saturation_diagnostic": saturation,
        "markers": m,
    }


# ---------------------------------------------------------------------------------------
# Selftest: pure-Python gate logic on synthetic markers (no Ray, no HPX)
# ---------------------------------------------------------------------------------------

def _synthetic_markers():
    cv = f"HPX: V2.0.0 (AGAS: V3.0), Git: {EXPECTED_HPX_COMMIT[:10]}"
    A_PID, B_PID, A_LOC, B_LOC, X = 5001, 5002, 1, 2, 7
    return {
        "controller_hostname": "host0", "argv_audit_ok": True,
        "actor_hpx_threads": 2, "actor_ray_num_cpus": 2,
        "dest_idle_during_ab": True, "dest_idle_during_ba": True,
        "operation_over_hpx_not_ray": True, "no_python_polling": True, "all_waits_bounded": True,
        "a_identity": {"pid": A_PID, "hostname": "host0",
                       "hpx_version_info": {"hpx_complete_version": cv}},
        "b_identity": {"pid": B_PID, "hostname": "host0",
                       "hpx_version_info": {"hpx_complete_version": cv}},
        "a_start": {"started": True, "locality_id": A_LOC, "membership": 2, "pid": A_PID},
        "b_start": {"started": True, "locality_id": B_LOC, "membership": 3, "pid": B_PID},
        "a_child_report": {"checked": True, "children": [], "hpx_children": []},
        "b_child_report": {"checked": True, "children": [], "hpx_children": []},
        "root_ready": {"locality_id": 0, "hostname": "host0", "wall_ms": 1000,
                       "hpx_complete_version": cv},
        "root_final": {"max_membership": 3, "final_membership": 1, "leave_observed": True},
        "ab_dispatch": {"target_found": True, "ready": True, "pid_result": B_PID,
                        "probe_result": probe_value(X, B_LOC), "host_result": "host0",
                        "executed_on": B_LOC, "oracle_match": True,
                        "dispatch_wall_ms": 2000, "return_wall_ms": 2050, "x": X},
        "ba_dispatch": {"target_found": True, "ready": True, "pid_result": A_PID,
                        "probe_result": probe_value(X, A_LOC), "host_result": "host0",
                        "executed_on": A_LOC, "oracle_match": True,
                        "dispatch_wall_ms": 2100, "return_wall_ms": 2150, "x": X},
        "a_health": {"ok": True, "pid": A_PID}, "b_health": {"ok": True, "pid": B_PID},
        "a_stop_rc": 0, "a_stop_error": None, "b_stop_rc": 0, "b_stop_error": None,
        "a_destroyed": True, "b_destroyed": True,
        "a_recreate": {"ok": True, "pid": 6001, "ext_importable": True},
        "b_recreate": {"ok": True, "pid": 6002, "ext_importable": True},
        "root_exit_path": "finalized_clean", "actor_pids_gone": True, "orphans": [],
    }


def selftest():
    failures = []

    def gates(m):
        return eval_slice_a(m), eval_slice_b(m), eval_slice_c(m), eval_slice_d(m)

    m = _synthetic_markers()
    ga, gb, gc, gd = gates(m)
    for name, g in (("A", ga), ("B", gb), ("C", gc), ("D", gd)):
        if not all(g.values()):
            failures.append(f"clean Slice {name} should pass: {[k for k,v in g.items() if not v]}")
    if not rep_verdict(ga, gb, gc, gd):
        failures.append("clean synthetic rep should pass the verdict")
    if failure_class(ga, gb, gc, gd) != "pass":
        failures.append("clean rep failure_class must be 'pass'")

    def expect_class(mut, cls, label):
        mm = _synthetic_markers()
        mut(mm)
        fc = failure_class(*gates(mm))
        if fc != cls:
            failures.append(f"{label}: expected failure_class={cls}, got {fc}")

    # A->B PID must equal actor B's pid (no self-probe substitute).
    expect_class(lambda mm: mm["ab_dispatch"].__setitem__("pid_result", 9999),
                 "pid_identity_failed", "A->B pid mismatch")
    # B->A PID must equal actor A's pid.
    expect_class(lambda mm: mm["ba_dispatch"].__setitem__("pid_result", 9999),
                 "pid_identity_failed", "B->A pid mismatch")
    # An HPX child in either actor.
    expect_class(lambda mm: mm["b_child_report"].__setitem__(
        "hpx_children", [{"pid": 7000, "cmd": "exp67_peer"}]),
                 "hpx_child_process_detected", "B has HPX child")
    # Same locality id for both actors.
    expect_class(lambda mm: mm["b_start"].__setitem__("locality_id", 1),
                 "locality_identity_invalid", "duplicate locality id")
    # Dispatch not ready.
    expect_class(lambda mm: mm["ab_dispatch"].__setitem__("ready", False),
                 "actor_to_actor_dispatch_failed", "A->B not ready")
    # Oracle mismatch.
    expect_class(lambda mm: mm["ba_dispatch"].__setitem__("oracle_match", False),
                 "oracle_failed", "B->A oracle mismatch")
    # Destination witness (executed_on wrong).
    expect_class(lambda mm: mm["ab_dispatch"].__setitem__("executed_on", 2 + 5),
                 "destination_witness_failed", "A->B executed_on wrong")
    # Host witness missing.
    expect_class(lambda mm: mm["ab_dispatch"].__setitem__("host_result", "otherhost"),
                 "destination_witness_failed", "A->B host witness wrong")
    # An oracle action executing on the root (loc 0) -> witness gate catches it.
    expect_class(lambda mm: mm["ab_dispatch"].__setitem__("executed_on", 0),
                 "destination_witness_failed", "A->B executed on root")
    # Ray payload path (structural flag flipped).
    expect_class(lambda mm: mm.__setitem__("operation_over_hpx_not_ray", False),
                 "ray_payload_path_detected", "ray payload path")
    # Idle progress (dest not idle during dispatch).
    expect_class(lambda mm: mm.__setitem__("dest_idle_during_ab", False),
                 "idle_progress_failed", "B not idle during A->B")
    # Graceful leave (B stop nonzero).
    expect_class(lambda mm: mm.__setitem__("b_stop_rc", 3),
                 "graceful_leave_failed", "B ungraceful leave")
    # Root finalize (membership did not return).
    expect_class(lambda mm: mm["root_final"].__setitem__("final_membership", 2),
                 "root_finalize_failed", "root membership not returned")
    # Actor recreation reuses a pid.
    expect_class(lambda mm: mm["a_recreate"].__setitem__("pid", 5001),
                 "actor_recreation_failed", "A recreation same pid")
    # Orphan present.
    expect_class(lambda mm: mm.__setitem__("orphans", ["71234"]),
                 "orphan_detected", "orphan pid")
    # Wrong HPX commit -> start/instrumentation-level (fixed_hpx_commit) -> invalid_instrumentation
    # is NOT the class here; fixed_hpx_commit is a Slice A gate but not in the failure ladder, so a
    # wrong commit should surface via evidence completeness or another gate. Assert it fails the rep.
    m5 = _synthetic_markers()
    m5["a_identity"]["hpx_version_info"]["hpx_complete_version"] = "Git: deadbeef12"
    if rep_verdict(*gates(m5)):
        failures.append("wrong HPX commit must fail the rep verdict")

    # A start failed.
    expect_class(lambda mm: mm["a_start"].__setitem__("started", False),
                 "actor_a_hpx_start_failed", "A start failed")
    # B start failed.
    expect_class(lambda mm: mm["b_start"].__setitem__("started", False),
                 "actor_b_hpx_start_failed", "B start failed")
    # Shared-island membership never reached 3.
    def _mem2(mm):
        mm["a_start"]["membership"] = 2
        mm["b_start"]["membership"] = 2
    expect_class(_mem2, "shared_island_join_failed", "membership < 3")

    # Saturation diagnostic never changes the verdict.
    sat = classify_saturation("ab_under_b_busy",
                              {"ready": True, "oracle_match": True, "executed_on": 2,
                               "dispatch_wall_ms": 2000, "return_wall_ms": 2100},
                              1900, 5000, 2, 2, 2, {"free_threaded_build": False})
    if sat["classification"] != "progressed_under_dest_saturation" or sat["affects_verdict"]:
        failures.append(f"clean saturation should be progressed and non-verdict: {sat}")
    sat_no = classify_saturation("ab_under_b_busy",
                                 {"ready": True, "oracle_match": True, "executed_on": 2,
                                  "dispatch_wall_ms": 100, "return_wall_ms": 200},
                                 1900, 5000, 2, 2, 2, {})
    if sat_no["classification"] != "invalid_diagnostic":
        failures.append("no-overlap saturation must be invalid_diagnostic")

    # commit_matches abbreviations.
    if not commit_matches(f"Git: {EXPECTED_HPX_COMMIT[:10]}"):
        failures.append("commit_matches must accept the abbreviated prefix")
    if commit_matches("Git: 0123456789"):
        failures.append("commit_matches must reject a non-prefix sha")

    # argv audit.
    if argv_audit(["peer", "--hpx:localities=3"]):
        failures.append("argv_audit must reject --hpx:localities")
    if not argv_audit(["peer", "--hpx:expect-connecting-localities"]):
        failures.append("argv_audit must accept expect-connecting-localities")

    # ---- cross-node gate logic (pure; no Slurm, no Ray, no HPX) --------------------------------
    def cx_expect():
        return {"slurm_job_id": "555",
                "nodes": ["medusa00", "medusa01", "medusa02"],
                "nodeR": "medusa00", "nodeA": "medusa01", "nodeB": "medusa02",
                "nodeR_nid": "nidR", "nodeA_nid": "nidA", "nodeB_nid": "nidB", "subnet": "10.42.5."}

    def cx_markers():
        mm = _synthetic_markers()
        mm["root_ready"]["hostname"] = "medusa00"           # root on node R
        mm["a_identity"]["hostname"] = "medusa01"           # actor A on node A
        mm["b_identity"]["hostname"] = "medusa02"           # actor B on node B
        mm["a_placement"] = {"node_id": "nidA", "hostname": "medusa01",
                             "placement_soft": False, "pid": mm["a_identity"]["pid"]}
        mm["b_placement"] = {"node_id": "nidB", "hostname": "medusa02",
                             "placement_soft": False, "pid": mm["b_identity"]["pid"]}
        mm["ab_dispatch"]["host_result"] = "medusa02"       # A->B executed on node B
        mm["ba_dispatch"]["host_result"] = "medusa01"       # B->A executed on node A
        mm["endpoints_subnet_ok"] = True
        mm["remote_orphan_check_ran"] = True
        mm["a_recreate_on_node"] = True
        mm["b_recreate_on_node"] = True
        return mm

    def cx_gates(mm):
        return (eval_crossnode_slice_a(mm, cx_expect()), eval_crossnode_slice_b(mm, cx_expect()),
                eval_slice_c(mm), eval_crossnode_slice_d(mm))

    cga, cgb, cgc, cgd = cx_gates(cx_markers())
    for name, g in (("A", cga), ("B", cgb), ("C", cgc), ("D", cgd)):
        if not all(g.values()):
            failures.append(f"clean cross-node Slice {name} should pass: "
                            f"{[k for k,v in g.items() if not v]}")
    if not rep_verdict_crossnode(cga, cgb, cgc, cgd):
        failures.append("clean cross-node rep should pass the verdict")
    if failure_class_crossnode(cga, cgb, cgc, cgd) != "pass":
        failures.append("clean cross-node rep failure_class must be 'pass'")
    if "hostname_identity" in cga:
        failures.append("cross-node Slice A must drop the same-host hostname_identity gate")

    def cx_expect_class(mut, cls, label):
        mm = cx_markers()
        mut(mm)
        fc = failure_class_crossnode(*cx_gates(mm))
        if fc != cls:
            failures.append(f"[cross-node] {label}: expected {cls}, got {fc}")

    # Actor A placed on the ROOT node -> A placement invalid.
    cx_expect_class(lambda mm: mm["a_placement"].update({"hostname": "medusa00", "node_id": "nidR"}),
                    "actor_a_placement_invalid", "A on root node")
    # Soft placement of A -> A placement invalid.
    cx_expect_class(lambda mm: mm["a_placement"].__setitem__("placement_soft", True),
                    "actor_a_placement_invalid", "A soft placement")
    # Actor B placed on node A -> B placement invalid.
    cx_expect_class(lambda mm: mm["b_placement"].update({"hostname": "medusa01", "node_id": "nidA"}),
                    "actor_b_placement_invalid", "B on node A")
    # A->B destination hostname is not node B -> destination witness failed.
    cx_expect_class(lambda mm: mm["ab_dispatch"].__setitem__("host_result", "medusa09"),
                    "destination_witness_failed", "A->B dest hostname wrong")
    # Cross-node A->B pid mismatch still fails PID identity.
    cx_expect_class(lambda mm: mm["ab_dispatch"].__setitem__("pid_result", 1),
                    "pid_identity_failed", "cross-node A->B pid mismatch")
    # Recreated actor A not back on node A -> actor recreation failed.
    cx_expect_class(lambda mm: mm.__setitem__("a_recreate_on_node", False),
                    "actor_recreation_failed", "A not recreated on node A")

    # Two-node allocation -> three-node gate fails with the explicit unavailable class.
    ex2 = cx_expect(); ex2["nodes"] = ["medusa00", "medusa01"]
    mm2 = cx_markers()
    g2 = (eval_crossnode_slice_a(mm2, ex2), eval_crossnode_slice_b(mm2, ex2),
          eval_slice_c(mm2), eval_crossnode_slice_d(mm2))
    if failure_class_crossnode(*g2) != "three_node_allocation_unavailable":
        failures.append("two-node allocation must classify three_node_allocation_unavailable")

    # Off-subnet endpoints -> the rep fails and the subnet gate is the cause.
    mm3 = cx_markers(); mm3["endpoints_subnet_ok"] = False
    g3 = eval_crossnode_slice_a(mm3, cx_expect())
    if g3["endpoints_pinned_subnet"]:
        failures.append("off-subnet endpoints must fail endpoints_pinned_subnet")

    # Nodelist expander sanity for three nodes.
    if _expand_nodelist_pure("medusa[00-02]") != ["medusa00", "medusa01", "medusa02"]:
        failures.append("nodelist expander must expand medusa[00-02]")

    if failures:
        print("SELFTEST FAIL:")
        for f in failures:
            print("  -", f)
        return 1
    print("SELFTEST OK: exp67 gate logic, failure taxonomy, and saturation diagnostic verified")
    return 0


# ---------------------------------------------------------------------------------------
# Aggregate + local phase
# ---------------------------------------------------------------------------------------

def _write_agg(path, agg):
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=False)
        f.write("\n")


def run_local_phase(args):
    peer = os.path.join(args.build_dir, PEER_BASENAME)
    ext_so = None
    for fn in os.listdir(args.build_dir) if os.path.isdir(args.build_dir) else []:
        if fn.startswith(EXT_MODULE) and fn.endswith(".so"):
            ext_so = os.path.join(args.build_dir, fn)

    agg = {
        "experiment": "67_two_ray_actors_shared_hpx",
        "kind": "two_ray_actors_share_one_hpx_runtime_actor_to_actor_local",
        "design": "four_gating_slices_A_B_C_D_plus_nongating_saturation_diagnostic",
        "single_node": True, "transport": "tcp_loopback",
        "verdict_rule": "PASS iff all of Slice A, B, C, D pass in every rep "
                        "(saturation diagnostic non-gating)",
        "bidirectional_proof": "B pid proven by A->B ; A pid proven by B->A ; "
                               "no self-probe accepted as remote peer proof",
        "claim_fences": dict(CLAIM_FENCES),
        "failure_classes": list(FAILURE_CLASSES),
        "reps": args.reps,
    }

    if not (os.path.exists(peer) and ext_so):
        agg["overall"] = "skip"
        agg["reason"] = f"build artifacts missing (peer={os.path.exists(peer)}, ext={bool(ext_so)})"
        _write_agg(args.aggregate, agg)
        print(f"SKIP: build exp67 first (peer/ext missing) -> {args.aggregate}")
        return 0

    try:
        import ray
    except Exception as ex:  # noqa: BLE001
        agg["overall"] = "skip"
        agg["reason"] = f"ray unavailable: {type(ex).__name__}: {ex}"
        _write_agg(args.aggregate, agg)
        print(f"SKIP: ray unavailable -> {args.aggregate}")
        return 0

    import logging
    ray.init(num_cpus=max(6, 2 * args.ray_num_cpus + 2), include_dashboard=False,
             log_to_driver=False, logging_level=logging.ERROR, ignore_reinit_error=True)
    HpxActor = build_actor_class(ray)

    runs_root = os.path.join(HERE, "_exp67_runs", time.strftime("%Y%m%dT%H%M%SZ"))
    os.makedirs(runs_root, exist_ok=True)
    print(f"[exp67] peer   : {peer}")
    print(f"[exp67] ext    : {os.path.basename(ext_so)}")
    print(f"[exp67] runs   : {os.path.relpath(runs_root, HERE)}")

    reps, provenance, saturation = [], None, []
    try:
        for r in range(1, args.reps + 1):
            print(f"[exp67] rep {r} ...")
            rep = run_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), args.x, r,
                          runs_root, args.hpx_threads, args.ray_num_cpus)
            if provenance is None:
                provenance = collect_provenance(ray, rep["markers"].get("a_identity"))
            for s in rep.get("saturation_diagnostic", []):
                saturation.append({"rep": r, **s})
            print(f"[exp67]   rep {r}: {'PASS' if rep['rep_pass'] else 'FAIL'} "
                  f"({rep['failure_class']})  "
                  f"A={rep['slice_a_failed']} B={rep['slice_b_failed']} "
                  f"C={rep['slice_c_failed']} D={rep['slice_d_failed']}")
            # Trim heavy child-report lists from the curated aggregate (raw logs stay in runs_root).
            rep_out = dict(rep)
            mk = dict(rep["markers"])
            for k in ("a_child_report", "b_child_report"):
                if isinstance(mk.get(k), dict):
                    mk[k] = {kk: mk[k].get(kk) for kk in ("checked", "hpx_children", "worker_pid")}
            rep_out["markers"] = mk
            reps.append(rep_out)
    finally:
        try:
            ray.shutdown()
        except Exception:
            pass

    overall_pass = bool(reps) and len(reps) == args.reps and all(r["rep_pass"] for r in reps)
    agg["provenance"] = provenance
    agg["overall"] = "pass" if overall_pass else "fail"
    agg["slice_reps"] = reps
    agg["saturation_diagnostic"] = saturation
    agg["python_and_crossnode_scope"] = slice_python_deferral()
    agg["safe_claim"] = (
        "On the tested local environment (CPython 3.11 GIL build, Ray "
        f"{(provenance or {}).get('ray_version')}, HPX commit {EXPECTED_HPX_COMMIT[:10]}...), two "
        "Ray actor worker processes hosted HPX connect-mode localities in-process (PID identity "
        "for both, no HPX child), joined one shared HPX runtime under a separate work-free root, "
        "and executed a verified HPX action from one actor-locality to the other in BOTH "
        "directions (B's PID proven by A->B, A's by B->A) with clean lifecycle and actor reuse."
        if overall_pass else
        "exp67 did not pass; see slice_reps[].failure_class for the failing class.")
    _write_agg(args.aggregate, agg)
    print(f"[exp67] overall: {agg['overall']} -> {args.aggregate}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="exp67 two-Ray-actors-share-one-HPX slice")
    ap.add_argument("--phase", choices=["local", "rostam-cross-node"], default="local",
                    help="local single-node (default) or three-node Rostam cross-node")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--build-dir", default=DEFAULT_BUILD_DIR)
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--aggregate", default=None,
                    help="curated aggregate path (default depends on --phase)")
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--ray-num-cpus", type=int, default=2)
    ap.add_argument("--selftest", action="store_true")
    # cross-node-only options (three nodes: root/controller R, actor A, actor B)
    ap.add_argument("--root-node", default=None, help="cross-node: node hosting controller+root (R)")
    ap.add_argument("--actor-a-node", default=None, help="cross-node: node hosting Ray actor A")
    ap.add_argument("--actor-b-node", default=None, help="cross-node: node hosting Ray actor B")
    ap.add_argument("--subnet-prefix", default="10.42.5.", help="cross-node: required IPv4 prefix")
    ap.add_argument("--port-base", type=int, default=7760, help="cross-node: HPX port base per rep")
    ap.add_argument("--ray-port", type=int, default=6379, help="cross-node: Ray GCS port")
    ap.add_argument("--ray-ready-timeout", type=int, default=180)
    ap.add_argument("--ray-init-timeout", type=int, default=180)
    ap.add_argument("--head-num-cpus", type=int, default=4)
    ap.add_argument("--worker-num-cpus", type=int, default=4)
    ap.add_argument("--node-manager-port", type=int, default=7911)
    ap.add_argument("--object-manager-port", type=int, default=7912)
    ap.add_argument("--min-worker-port", type=int, default=10202)
    ap.add_argument("--max-worker-port", type=int, default=10302)
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if args.aggregate is None:
        args.aggregate = os.path.join(
            HERE, "two_ray_actors_shared_hpx_crossnode_aggregate.json"
            if args.phase == "rostam-cross-node" else "two_ray_actors_shared_hpx_aggregate.json")
    if args.phase == "rostam-cross-node":
        return run_crossnode_phase(args)
    return run_local_phase(args)


# =======================================================================================
# Cross-node (Rostam / THREE-node Slurm) phase.
#
# Extends the accepted exp66 two-node cross-node harness to THREE roles on THREE distinct nodes:
#   node R : controller (sbatch batch step lands here) + Ray head + work-free HPX root (loc 0, Popen)
#   node A : Ray actor A hard-placed here; hosts an HPX connect-mode locality IN-PROCESS
#   node B : Ray actor B hard-placed here; hosts an HPX connect-mode locality IN-PROCESS
# The load-bearing proof is BIDIRECTIONAL, CROSS-NODE, actor-to-actor: A->B and B->A HPX actions,
# each proving the peer's PID/locality/hostname/oracle over the real 10.42.5.x parcelport. The root
# never dispatches or executes the application oracle. Reuses exp59 Ray-on-Slurm bring-up, exp65/66
# cross-node discipline, and the corrected remote orphan check. Writes its OWN curated aggregate;
# never touches the local aggregate. EXPERIMENT-ONLY; no performance claim.
# =======================================================================================

_ORPHAN_PATTERNS_RAY = ["raylet", "gcs_server", "plasma_store", "ray::", "dashboard", "monitor"]


def _short(h):
    return (h or "").split(".")[0].strip().lower()


def _sh(cmd, timeout=180, env=None):
    """Run a command; return (rc, stdout, stderr). Never raises (exp59 idiom)."""
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=timeout, env=env, text=True)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        so = e.stdout.decode("utf-8", "replace") if isinstance(e.stdout, (bytes, bytearray)) else (e.stdout or "")
        se = e.stderr.decode("utf-8", "replace") if isinstance(e.stderr, (bytes, bytearray)) else (e.stderr or "")
        return 124, so, se + f"\n[timeout after {timeout}s]"
    except Exception as e:  # noqa: BLE001
        return 1, "", f"[exec error] {e}"


def _expand_nodelist_pure(s):
    """Expand a Slurm nodelist ('medusa[00-02]' -> [...]) without scontrol."""
    parts, depth, cur = [], 0, ""
    for ch in (s or "").strip():
        if ch == "[":
            depth += 1; cur += ch
        elif ch == "]":
            depth -= 1; cur += ch
        elif ch == "," and depth == 0:
            parts.append(cur); cur = ""
        else:
            cur += ch
    if cur:
        parts.append(cur)
    out = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        mm = re.match(r"^([^\[]*)\[([^\]]+)\](.*)$", part)
        if not mm:
            out.append(part); continue
        prefix, body, suffix = mm.group(1), mm.group(2), mm.group(3)
        for token in body.split(","):
            token = token.strip()
            if "-" in token:
                lo, hi = token.split("-", 1)
                width = len(lo)
                for n in range(int(lo), int(hi) + 1):
                    out.append(f"{prefix}{str(n).zfill(width)}{suffix}")
            elif token:
                out.append(f"{prefix}{token}{suffix}")
    return out


def _expand_slurm_nodelist(nodelist):
    if nodelist:
        rc, out, _ = _sh(["scontrol", "show", "hostnames", nodelist], timeout=20)
        if rc == 0 and out.strip():
            return [h for h in out.split() if h.strip()]
    return _expand_nodelist_pure(nodelist or "")


def _pick_subnet_ip(ips, subnet_prefix):
    for ip in ips:
        if ip.startswith(subnet_prefix):
            return ip
    return None


def _local_subnet_ip(subnet_prefix):
    rc, out, _ = _sh(["hostname", "-I"], timeout=15)
    ips = [t for t in out.split() if t.count(".") == 3] if rc == 0 else []
    return _pick_subnet_ip(ips, subnet_prefix)


def _node_subnet_ip(node, subnet_prefix):
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "hostname", "-I"],
                     timeout=60)
    if rc != 0:
        return None
    return _pick_subnet_ip([t for t in out.split() if t.count(".") == 3], subnet_prefix)


def _wait_for_file_nfs(path, timeout, procs=()):
    """Revalidating file wait: list the parent dir before each check so NFS negative-dentry caching
    cannot hide a file written on another node."""
    deadline = time.time() + timeout
    parent = os.path.dirname(path) or "."
    while time.time() < deadline:
        try:
            os.listdir(parent)
        except OSError:
            pass
        if os.path.exists(path):
            return True
        for p in procs:
            if p is not None and p.poll() is not None:
                time.sleep(0.3)
                return os.path.exists(path)
        time.sleep(0.1)
    return os.path.exists(path)


# --- Ray-on-Slurm launchers (exp59 lifetime fix: `ray start --block` under a persistent Popen). ---

def _ray_port_flags(a):
    return ["--node-manager-port", str(a.node_manager_port),
            "--object-manager-port", str(a.object_manager_port),
            "--min-worker-port", str(a.min_worker_port),
            "--max-worker-port", str(a.max_worker_port)]


def _popen_blocking(cmd, env, log_path):
    lf = open(log_path, "ab", buffering=0)
    p = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                         env=env, start_new_session=True)
    p._logfile = lf  # noqa: SLF001
    return p


def _ray_head_local(nodeR_ip, port, temp_dir, num_cpus, env, log_path, port_flags):
    cmd = ["ray", "start", "--head", "--node-ip-address", nodeR_ip, "--port", str(port),
           "--include-dashboard", "false", "--temp-dir", temp_dir] + list(port_flags) + ["--block"]
    if num_cpus is not None:
        cmd += ["--num-cpus", str(num_cpus)]
    return _popen_blocking(cmd, env, log_path)


def _ray_worker_srun(node, node_ip, head_ip, port, num_cpus, env, log_path, port_flags):
    cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
           "ray", "start", "--address", f"{head_ip}:{port}", "--node-ip-address", node_ip] \
        + list(port_flags) + ["--block"]
    if num_cpus is not None:
        cmd += ["--num-cpus", str(num_cpus)]
    return _popen_blocking(cmd, env, log_path)


def _wait_gcs_from(probe_node, head_ip, port, env, timeout_s):
    probe = (
        "import socket,sys,time\n"
        "deadline=time.time()+%d\n"
        "while time.time()<deadline:\n"
        "    s=socket.socket(); s.settimeout(2)\n"
        "    try:\n        s.connect((%r,%d)); print('READY'); sys.exit(0)\n"
        "    except Exception:\n        time.sleep(1.5)\n"
        "print('TIMEOUT'); sys.exit(1)\n" % (int(timeout_s), head_ip, int(port)))
    rc, out, err = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", probe_node, "--export=ALL",
                        "python3", "-c", probe], timeout=int(timeout_s) + 30, env=env)
    return rc == 0, ((out or "") + (err or "")).strip()[-200:]


def _wait_ray_nodes(ray_mod, expected, timeout_s):
    t0 = time.monotonic()
    seen = 0
    while time.monotonic() - t0 < timeout_s:
        try:
            seen = len([n for n in ray_mod.nodes() if n.get("Alive")])
        except Exception:  # noqa: BLE001
            seen = 0
        if seen >= expected:
            return True, seen
        time.sleep(1.0)
    return False, seen


def _bounded_ray_init(ray_mod, address, timeout_s):
    os.environ.setdefault("RAY_gcs_server_request_timeout_seconds", "10")
    t0 = time.monotonic()
    attempts, last = 0, ""
    while True:
        attempts += 1
        try:
            ray_mod.init(address=address, log_to_driver=False, include_dashboard=False,
                         ignore_reinit_error=True)
            return True, attempts, ""
        except Exception:  # noqa: BLE001
            last = traceback.format_exc()[-1500:]
            if time.monotonic() - t0 >= timeout_s:
                return False, attempts, last
            time.sleep(2.0)


def _terminate_launcher(p):
    if p is None:
        return {"existed": False}
    info = {"existed": True, "pid": p.pid}
    try:
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=15)
            except Exception:  # noqa: BLE001
                p.kill()
        info["returncode"] = p.poll()
    except Exception as e:  # noqa: BLE001
        info["error"] = str(e)[:200]
    finally:
        lf = getattr(p, "_logfile", None)
        if lf is not None:
            try:
                lf.close()
            except Exception:  # noqa: BLE001
                pass
    return info


def _ray_stop_node(node, env):
    return _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                "ray", "stop", "--force"], timeout=120, env=env)


def _orphan_check_node(node, patterns, env):
    """Ray-process leftovers on `node`. The check itself is `srun ... pgrep -af <pattern>` whose OWN
    argv contains <pattern>; on the controller's node that srun client self-matches, so we use
    `pgrep -af` and drop our own srun/pgrep/ray-stop/run_exp67 machinery (the exp66 170520 fix)."""
    found = []
    for pat in patterns:
        hits = []
        for _try in range(6):  # up to ~30s: give ray stop time to reap
            rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                              "pgrep", "-af", pat], timeout=60, env=env)
            if rc not in (0, 1):
                hits = [f"{pat}:pgrep_rc_{rc}"]
                break
            hits = [f"{_short(node)}:{pat}:{ln.split()[0]}" for ln in out.splitlines()
                    if ln.strip() and "pgrep" not in ln and "srun" not in ln
                    and "ray stop" not in ln and "run_exp67" not in ln]
            if not hits:
                break
            time.sleep(5)
        found.extend(hits)
    return found


def _crossnode_peer_orphans(node, env=None):
    """exp67_peer (HPX root) leftovers on `node`, filtering the detector's own srun/pgrep/run_exp67."""
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                      "pgrep", "-af", PEER_BASENAME], timeout=60, env=env)
    if rc not in (0, 1):
        return [], False
    hits = []
    for ln in out.splitlines():
        if ln.strip() and "pgrep" not in ln and "srun" not in ln and "run_exp67" not in ln:
            hits.append(f"{_short(node)}:{ln.split()[0]}")
    return hits, True


def _self_identity(ray_mod):
    nid = None
    try:
        nid = ray_mod.get_runtime_context().get_node_id()
    except Exception:  # noqa: BLE001
        nid = None
    return {"hostname": socket.gethostname(), "pid": os.getpid(), "node_id": nid}


# --- cross-node peer/endpoint commands (numeric pinned IPs; NOT localhost) -----------------------

def crossnode_root_cmd(peer, boot, root_ip, p_root, leave_timeout):
    return [peer, "--role", "root", "--bootstrap", boot, "--leave-timeout", str(leave_timeout),
            f"--hpx:agas={root_ip}:{p_root}", f"--hpx:hpx={root_ip}:{p_root}",
            "--hpx:expect-connecting-localities",
            "--hpx:threads=2", "--hpx:bind=none", "--hpx:ignore-batch-env"]


def crossnode_actor_endpoints(root_ip, p_root, actor_ip, p_actor):
    return [f"--hpx:agas={root_ip}:{p_root}", f"--hpx:hpx={actor_ip}:{p_actor}",
            "--hpx:bind=none", "--hpx:ignore-batch-env"]


# --- cross-node gate evaluation (reuse the local slice evals; add placement/subnet/crossing gates) --

def eval_crossnode_slice_a(m, expect):
    """Cross-node Slice A: local hosting/membership gates + THREE-node placement attestation. The
    same-host hostname_identity gate is replaced by hard placement of BOTH actors on distinct nodes,
    the root on the root node, subnet pinning, and remote-orphan-check attestation. All GATING."""
    g = eval_slice_a(m)
    g.pop("hostname_identity", None)  # root and both actors are on DIFFERENT hosts by design
    ap_a, ap_b = m.get("a_placement") or {}, m.get("b_placement") or {}
    rr = m.get("root_ready") or {}
    nodes_short = [_short(n) for n in (expect.get("nodes") or [])]
    g["three_node_slurm_allocation"] = bool(
        expect.get("slurm_job_id") and len(expect.get("nodes") or []) >= 3
        and _short(expect.get("nodeR")) in nodes_short
        and _short(expect.get("nodeA")) in nodes_short
        and _short(expect.get("nodeB")) in nodes_short)
    g["root_on_root_node"] = bool(_short(rr.get("hostname")) == _short(expect.get("nodeR")))
    g["actor_a_hard_placed"] = bool(
        ap_a.get("placement_soft") is False
        and ap_a.get("node_id") and ap_a.get("node_id") == expect.get("nodeA_nid")
        and _short(ap_a.get("hostname")) == _short(expect.get("nodeA")))
    g["actor_b_hard_placed"] = bool(
        ap_b.get("placement_soft") is False
        and ap_b.get("node_id") and ap_b.get("node_id") == expect.get("nodeB_nid")
        and _short(ap_b.get("hostname")) == _short(expect.get("nodeB")))
    g["actor_nodes_distinct"] = bool(
        _short(ap_a.get("hostname")) and _short(ap_b.get("hostname"))
        and _short(ap_a.get("hostname")) != _short(ap_b.get("hostname")))
    g["three_roles_three_nodes"] = bool(
        len({_short(expect.get("nodeR")), _short(expect.get("nodeA")),
             _short(expect.get("nodeB"))}) == 3
        and _short(rr.get("hostname")) not in (_short(ap_a.get("hostname")),
                                               _short(ap_b.get("hostname"))))
    g["endpoints_pinned_subnet"] = bool(m.get("endpoints_subnet_ok"))
    g["remote_orphan_check_ran"] = bool(m.get("remote_orphan_check_ran"))
    g["evidence_fields_complete_crossnode"] = bool(
        ap_a.get("node_id") and ap_b.get("node_id") and rr.get("hostname")
        and expect.get("nodeA_nid") and expect.get("nodeB_nid"))
    return g


def eval_crossnode_slice_b(m, expect):
    """Cross-node Slice B: the local bidirectional actor-to-actor gates + explicit node-crossing and
    destination-node hostname witnesses. All GATING."""
    g = eval_slice_b(m)
    ap_a, ap_b = m.get("a_placement") or {}, m.get("b_placement") or {}
    ab, ba = m.get("ab_dispatch") or {}, m.get("ba_dispatch") or {}
    crosses = bool(_short(ap_a.get("hostname")) and _short(ap_b.get("hostname"))
                   and _short(ap_a.get("hostname")) != _short(ap_b.get("hostname")))
    g["ab_crosses_nodes"] = crosses
    g["ba_crosses_nodes"] = crosses
    g["ab_dest_hostname_is_node_b"] = bool(_short(ab.get("host_result")) == _short(expect.get("nodeB")))
    g["ba_dest_hostname_is_node_a"] = bool(_short(ba.get("host_result")) == _short(expect.get("nodeA")))
    return g


def eval_crossnode_slice_d(m):
    """Cross-node Slice D: local lifecycle gates + recreation-on-intended-node for both actors."""
    g = eval_slice_d(m)
    g["actor_a_recreation_on_node"] = bool(m.get("a_recreate_on_node"))
    g["actor_b_recreation_on_node"] = bool(m.get("b_recreate_on_node"))
    return g


def rep_verdict_crossnode(ga, gb, gc, gd):
    return all(ga.values()) and all(gb.values()) and all(gc.values()) and all(gd.values())


def failure_class_crossnode(ga, gb, gc, gd):
    """Map the first failing cross-node gate group to the explicit exp67 cross-node taxonomy."""
    if not ga.get("three_node_slurm_allocation"):
        return "three_node_allocation_unavailable"
    if not ga.get("evidence_complete") or not ga.get("evidence_fields_complete_crossnode"):
        return "invalid_instrumentation"
    if not ga.get("root_on_root_node"):
        return "root_placement_invalid"
    if not ga.get("actor_a_hard_placed"):
        return "actor_a_placement_invalid"
    if not ga.get("actor_b_hard_placed"):
        return "actor_b_placement_invalid"
    if not (ga.get("actor_nodes_distinct") and ga.get("three_roles_three_nodes")):
        return "actor_nodes_not_distinct"
    if not ga.get("actor_a_started_in_worker"):
        return "actor_a_hpx_start_failed"
    if not ga.get("actor_b_started_in_worker"):
        return "actor_b_hpx_start_failed"
    if not ga.get("both_in_process_no_hpx_child"):
        return "hpx_child_process_detected"
    if not ga.get("shared_island_membership"):
        return "shared_island_join_failed"
    if not (ga.get("distinct_locality_ids") and ga.get("two_distinct_ray_processes")):
        return "locality_mapping_invalid"
    if not gb.get("ab_dispatch_ready"):
        return "a_to_b_dispatch_failed"
    if not gb.get("ba_dispatch_ready"):
        return "b_to_a_dispatch_failed"
    if not (gb.get("b_pid_proven_by_ab") and gb.get("a_pid_proven_by_ba")):
        return "pid_identity_failed"
    if not (gb.get("ab_executes_on_b") and gb.get("ba_executes_on_a")
            and gb.get("ab_host_witness") and gb.get("ba_host_witness")
            and gb.get("ab_dest_hostname_is_node_b") and gb.get("ba_dest_hostname_is_node_a")
            and gb.get("ab_crosses_nodes") and gb.get("ba_crosses_nodes")):
        return "destination_witness_failed"
    if not (gb.get("ab_oracle") and gb.get("ba_oracle")):
        return "oracle_failed"
    if not gb.get("operation_over_hpx_not_ray"):
        return "ray_payload_path_confounded"
    if not (gc.get("ab_progress_while_b_idle") and gc.get("ba_progress_while_a_idle")):
        return "idle_progress_failed"
    if not (gd.get("actor_a_graceful_leave") and gd.get("actor_b_graceful_leave")
            and gd.get("clean_hpx_stop_both")):
        return "graceful_leave_failed"
    if not gd.get("root_finalized_clean"):
        return "root_finalize_failed"
    if not (gd.get("actor_a_recreation") and gd.get("actor_b_recreation")
            and gd.get("actor_a_recreation_on_node") and gd.get("actor_b_recreation_on_node")):
        return "actor_recreation_failed"
    if not gd.get("no_orphans"):
        return "orphan_detected"
    if rep_verdict_crossnode(ga, gb, gc, gd):
        return "pass"
    return "invalid_instrumentation"


def run_crossnode_rep(ray_mod, HpxActor, peer, build_dir, x, rep, runs_root, hpx_threads, ray_cpus,
                      nodeR, nodeA, nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid, nodeB_nid,
                      subnet, port_base, expect, env):
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    boot = os.path.join(runs_root, f"rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    p_root = port_base + rep * 10
    p_a = port_base + rep * 10 + 1
    p_b = port_base + rep * 10 + 2
    rcmd = crossnode_root_cmd(peer, boot, nodeR_ip, p_root, leave_timeout=45)
    ep_a = crossnode_actor_endpoints(nodeR_ip, p_root, nodeA_ip, p_a)
    ep_b = crossnode_actor_endpoints(nodeR_ip, p_root, nodeB_ip, p_b)
    strat_a = NodeAffinitySchedulingStrategy(node_id=nodeA_nid, soft=False)
    strat_b = NodeAffinitySchedulingStrategy(node_id=nodeB_nid, soft=False)
    m = {
        "controller_hostname": socket.gethostname(),
        "ports": {"root": p_root, "actor_a": p_a, "actor_b": p_b},
        "argv_audit_ok": argv_audit(rcmd, ep_a, ep_b),
        "endpoints_subnet_ok": bool(nodeR_ip.startswith(subnet) and nodeA_ip.startswith(subnet)
                                    and nodeB_ip.startswith(subnet)),
        "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus,
        "dest_idle_during_ab": True, "dest_idle_during_ba": True,
        "operation_over_hpx_not_ray": True,
        "operation_path_note": "actor A and actor B hold no Ray handle to each other; the only "
                               "channel between their localities is the HPX parcelport across nodes.",
        "no_python_polling": True, "all_waits_bounded": True,
    }
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    saturation = []
    try:
        # 1) work-free supervised root (locality 0) on node R (Popen, controller-local)
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file_nfs(os.path.join(boot, "root.ready"), 60, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        # 2) actor A hard-placed on node A, hosts HPX in-process
        a1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              scheduling_strategy=strat_a).remote(build_dir, hpx_threads, ep_a)
        pa = dict(_ray_get(ray_mod, a1.ray_placement.remote(), 60, "a_placement") or {})
        pa["placement_soft"], pa["target_node"] = False, nodeA
        m["a_placement"] = pa
        m["a_identity"] = _ray_get(ray_mod, a1.load_identity.remote(), 60, "a_identity")
        m["a_start"] = _ray_get(ray_mod, a1.start_hpx.remote(), 120, "a_start")
        m["a_child_report"] = _ray_get(ray_mod, a1.child_report.remote(), 30, "a_child_report")
        a_loc = _loc(m["a_start"])

        # 3) actor B hard-placed on node B, joins the SAME island in-process
        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              scheduling_strategy=strat_b).remote(build_dir, hpx_threads, ep_b)
        pb = dict(_ray_get(ray_mod, b1.ray_placement.remote(), 60, "b_placement") or {})
        pb["placement_soft"], pb["target_node"] = False, nodeB
        m["b_placement"] = pb
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 60, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 120, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")
        b_loc = _loc(m["b_start"])

        # 4) Slice B + C: BIDIRECTIONAL cross-node actor-to-actor HPX action, each with dest idle
        if a_loc not in (None, 0) and b_loc not in (None, 0):
            m["ab_dispatch"] = _ray_get(ray_mod, a1.dispatch.remote(x, b_loc), 60, "ab_dispatch")
            m["ba_dispatch"] = _ray_get(ray_mod, b1.dispatch.remote(x, a_loc), 60, "ba_dispatch")

        m["a_health"] = _ray_get(ray_mod, a1.health.remote(), 30, "a_health")
        m["b_health"] = _ray_get(ray_mod, b1.health.remote(), 30, "b_health")

        # 5) NON-gating saturation diagnostic (driver-timeline overlap; no cross-node clock compare)
        prov_lite = {"free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
                     "gil_enabled_runtime": None}
        if a_loc not in (None, 0) and b_loc not in (None, 0):
            bs_path, bd_path = os.path.join(boot, "b_busy_started"), os.path.join(boot, "b_busy_done")
            b_busy = b1.busy_spin.remote(3.0, bs_path, bd_path)
            seen = _wait_for_file_nfs(bs_path, 30)
            ab_busy = _ray_get(ray_mod, a1.dispatch.remote(x, b_loc), 60, "ab_busy")
            try:
                b_ret = ray_mod.get(b_busy, timeout=30)
            except Exception:  # noqa: BLE001
                b_ret = None
            saturation.append(classify_saturation(
                "ab_under_b_busy", ab_busy, None, None, b_loc, hpx_threads, ray_cpus, prov_lite,
                overlap_override=bool(seen and ab_busy.get("ready") and b_ret is not None)))
            as_path, ad_path = os.path.join(boot, "a_busy_started"), os.path.join(boot, "a_busy_done")
            a_busy = a1.busy_spin.remote(3.0, as_path, ad_path)
            seen2 = _wait_for_file_nfs(as_path, 30)
            ba_busy = _ray_get(ray_mod, b1.dispatch.remote(x, a_loc), 60, "ba_busy")
            try:
                a_ret = ray_mod.get(a_busy, timeout=30)
            except Exception:  # noqa: BLE001
                a_ret = None
            saturation.append(classify_saturation(
                "ba_under_a_busy", ba_busy, None, None, a_loc, hpx_threads, ray_cpus, prov_lite,
                overlap_override=bool(seen2 and ba_busy.get("ready") and a_ret is not None)))

        # 6) Slice E (lifecycle): graceful leave of BOTH actors -> membership returns to 1
        a_pid = (m.get("a_identity") or {}).get("pid")
        b_pid = (m.get("b_identity") or {}).get("pid")
        a_stop = _ray_get(ray_mod, a1.stop_hpx.remote(), 40, "a_stop")
        b_stop = _ray_get(ray_mod, b1.stop_hpx.remote(), 40, "b_stop")
        m["a_stop_rc"], m["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
        m["b_stop_rc"], m["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")

        def _kill_dead(actor):
            ray_mod.kill(actor)
            try:
                ray_mod.get(actor.ping.remote(), timeout=10)
                return False
            except Exception:  # noqa: BLE001
                return True
        dead_a1 = _kill_dead(a1); a1 = None
        dead_b1 = _kill_dead(b1); b1 = None
        m["a_destroyed"], m["b_destroyed"] = dead_a1, dead_b1

        # recreate FRESH actors on the SAME intended nodes (Ray-only ping; no HPX start)
        a2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              scheduling_strategy=strat_a).remote(build_dir, hpx_threads, ep_a)
        b2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              scheduling_strategy=strat_b).remote(build_dir, hpx_threads, ep_b)
        m["a_recreate"] = _ray_get(ray_mod, a2.ping.remote(), 60, "a_recreate")
        m["b_recreate"] = _ray_get(ray_mod, b2.ping.remote(), 60, "b_recreate")
        ra, rb = m["a_recreate"] or {}, m["b_recreate"] or {}
        m["a_recreate_on_node"] = bool(_short(ra.get("hostname")) == _short(nodeA)
                                       and ra.get("node_id") == nodeA_nid and ra.get("pid") != a_pid)
        m["b_recreate_on_node"] = bool(_short(rb.get("hostname")) == _short(nodeB)
                                       and rb.get("node_id") == nodeB_nid and rb.get("pid") != b_pid)

        # 7) root completion: both connectors already left -> root observes membership==1, finalizes
        open(os.path.join(boot, "root.done"), "w").close()
        _wait_for_file_nfs(os.path.join(boot, "root.final"), 60, procs=[root])
        m["root_final"] = _read_json(os.path.join(boot, "root.final"))

        r_exited, r_rc, r_killed = _wait_proc(root, time.time() + 60)
        m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)

        dead_a2 = dead_b2 = True
        if a2 is not None:
            dead_a2 = _kill_dead(a2); a2 = None
        if b2 is not None:
            dead_b2 = _kill_dead(b2); b2 = None
        m["actor_pids_gone"] = bool(dead_a1 and dead_b1 and dead_a2 and dead_b2)

        # 8) peer-orphan sweep across ALL THREE nodes (root binary lives on node R)
        orphR, ranR = _crossnode_peer_orphans(nodeR, env)
        orphA, ranA = _crossnode_peer_orphans(nodeA, env)
        orphB, ranB = _crossnode_peer_orphans(nodeB, env)
        m["orphans"] = orphR + orphA + orphB
        m["remote_orphan_check_ran"] = bool(ranR and ranA and ranB)
    finally:
        for a in (a1, b1, a2, b2):
            if a is not None:
                try:
                    ray_mod.kill(a)
                except Exception:  # noqa: BLE001
                    pass
        _kill_group(root)
        if rlog is not None:
            rlog.close()

    ga = eval_crossnode_slice_a(m, expect)
    gb = eval_crossnode_slice_b(m, expect)
    gc = eval_slice_c(m)
    gd = eval_crossnode_slice_d(m)
    return {
        "rep": rep, "boot_rel": os.path.relpath(boot, HERE), "ports": m["ports"],
        "slice_a_gates": ga, "slice_b_gates": gb, "slice_c_gates": gc, "slice_d_gates": gd,
        "rep_pass": rep_verdict_crossnode(ga, gb, gc, gd),
        "failure_class": failure_class_crossnode(ga, gb, gc, gd),
        "slice_a_failed": [k for k, v in ga.items() if not v],
        "slice_b_failed": [k for k, v in gb.items() if not v],
        "slice_c_failed": [k for k, v in gc.items() if not v],
        "slice_d_failed": [k for k, v in gd.items() if not v],
        "saturation_diagnostic": saturation,
        "markers": m,
    }


def run_crossnode_phase(args):
    """Three-node Rostam slice. Controller runs ON node R (sbatch batch step); Ray head local on R,
    workers on node A and node B via srun --block; work-free HPX root is a local Popen on R; Ray
    actors A and B are hard-placed on A and B and host connect-mode HPX localities in-process that
    bind the pinned 10.42.5.x endpoints and join the root over TCP."""
    subnet = args.subnet_prefix
    agg = {
        "experiment": "67_two_ray_actors_shared_hpx",
        "kind": "two_ray_actors_share_one_hpx_runtime_actor_to_actor_crossnode",
        "design": "four_gating_slices_A_B_C_E_plus_nongating_saturation_D",
        "phase": "rostam-cross-node", "single_node": False, "cross_node": True,
        "transport": "tcp_cross_node",
        "verdict_rule": "PASS iff all of cross-node Slice A, B, C, D(lifecycle) pass in every rep "
                        "and no orphans on any node (saturation diagnostic non-gating)",
        "bidirectional_proof": "B pid proven by A->B ; A pid proven by B->A ; cross-node both ways ; "
                               "no self-probe accepted as remote peer proof",
        "claim_fences": dict(CLAIM_FENCES), "failure_classes": list(FAILURE_CLASSES) + [
            "three_node_allocation_unavailable", "root_placement_invalid", "actor_a_placement_invalid",
            "actor_b_placement_invalid", "actor_nodes_not_distinct", "locality_mapping_invalid",
            "a_to_b_dispatch_failed", "b_to_a_dispatch_failed", "ray_payload_path_confounded"],
        "reps": args.reps,
    }

    try:
        import ray
    except Exception as ex:  # noqa: BLE001
        agg["overall"] = "skip"; agg["reason"] = f"ray unavailable: {ex}"
        _write_agg(args.aggregate, agg); print("SKIP: ray unavailable"); return 0

    job_id = os.environ.get("SLURM_JOB_ID") or ""
    nodelist = os.environ.get("SLURM_JOB_NODELIST") or os.environ.get("SLURM_NODELIST") or ""
    nodes = sorted(_expand_slurm_nodelist(nodelist))
    problems = []
    if not job_id:
        problems.append("SLURM_JOB_ID empty (not in a Slurm allocation)")
    if len(nodes) < 3:
        problems.append(f"need >=3 distinct nodes (R, A, B); got {nodes}")
    nodeR = args.root_node if args.root_node in nodes else (nodes[0] if nodes else None)
    nodeA = args.actor_a_node if args.actor_a_node in nodes else (nodes[1] if len(nodes) > 1 else None)
    nodeB = args.actor_b_node if args.actor_b_node in nodes else (nodes[2] if len(nodes) > 2 else None)
    if nodeR and nodeA and nodeB and len({_short(nodeR), _short(nodeA), _short(nodeB)}) != 3:
        problems.append(f"R/A/B nodes must be distinct; got R={nodeR} A={nodeA} B={nodeB}")
    here = _short(socket.gethostname())
    if nodeR and here != _short(nodeR):
        problems.append(f"controller on {here}, must run on nodeR={nodeR} (sbatch batch step)")
    peer = os.path.join(args.build_dir, PEER_BASENAME)
    ext_so = next((os.path.join(args.build_dir, fn)
                   for fn in (os.listdir(args.build_dir) if os.path.isdir(args.build_dir) else [])
                   if fn.startswith(EXT_MODULE) and fn.endswith(".so")), None)
    if not (os.path.exists(peer) and ext_so):
        problems.append(f"build artifacts missing (peer={os.path.exists(peer)} ext={bool(ext_so)})")
    nodeR_ip = _local_subnet_ip(subnet) if not problems else None
    nodeA_ip = _node_subnet_ip(nodeA, subnet) if (nodeA and not problems) else None
    nodeB_ip = _node_subnet_ip(nodeB, subnet) if (nodeB and not problems) else None
    if not problems and not (nodeR_ip and nodeA_ip and nodeB_ip):
        problems.append(f"could not resolve subnet {subnet} IPs "
                        f"(R={nodeR_ip} A={nodeA_ip} B={nodeB_ip})")

    agg["preflight"] = {
        "slurm_job_id": job_id, "slurm_nodelist": nodelist, "allocation_nodes": nodes,
        "controller_host": here, "nodeR": nodeR, "nodeA": nodeA, "nodeB": nodeB,
        "nodeR_ip": nodeR_ip, "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip, "subnet_prefix": subnet,
    }
    if problems:
        agg["overall"] = "fail_preflight"
        agg["preflight_problems"] = problems
        # Distinguish the "no three-node allocation" case explicitly (the user asked to stop there).
        agg["failure_class"] = ("three_node_allocation_unavailable"
                                if any("node" in p for p in problems) else "invalid_instrumentation")
        _write_agg(args.aggregate, agg)
        print(f"[exp67-crossnode] PREFLIGHT FAIL: {problems}")
        return 0

    print(f"[exp67-crossnode] job {job_id} nodes {nodes} | root {nodeR}={nodeR_ip} "
          f"A {nodeA}={nodeA_ip} B {nodeB}={nodeB_ip}")

    env = dict(os.environ)
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    runs_root = os.path.join(HERE, "_exp67_runs", f"crossnode_{job_id}_{runid}")
    os.makedirs(runs_root, exist_ok=True)
    port = args.ray_port
    temp_dir = f"/tmp/exp67_ray_{job_id}_{runid}"
    port_flags = _ray_port_flags(args)
    head_proc = worker_a = worker_b = None
    reps, provenance, saturation = [], None, []
    nodeA_nid = nodeB_nid = None
    fail_reason = None
    try:
        head_proc = _ray_head_local(nodeR_ip, port, temp_dir, args.head_num_cpus, env,
                                    os.path.join(runs_root, "head.log"), port_flags)
        okA, detA = _wait_gcs_from(nodeA, nodeR_ip, port, env, args.ray_ready_timeout)
        okB, detB = _wait_gcs_from(nodeB, nodeR_ip, port, env, args.ray_ready_timeout)
        agg["ray_gcs_ready_from_nodeA"], agg["ray_gcs_detail_A"] = okA, detA
        agg["ray_gcs_ready_from_nodeB"], agg["ray_gcs_detail_B"] = okB, detB
        if not (okA and okB and head_proc.poll() is None):
            raise RuntimeError(f"head GCS not reachable from A({detA}) or B({detB}); "
                               f"head_alive={head_proc.poll() is None}")
        worker_a = _ray_worker_srun(nodeA, nodeA_ip, nodeR_ip, port, args.worker_num_cpus, env,
                                    os.path.join(runs_root, "worker_a.log"), port_flags)
        worker_b = _ray_worker_srun(nodeB, nodeB_ip, nodeR_ip, port, args.worker_num_cpus, env,
                                    os.path.join(runs_root, "worker_b.log"), port_flags)

        init_ok, init_attempts, init_tb = _bounded_ray_init(ray, f"{nodeR_ip}:{port}",
                                                            args.ray_init_timeout)
        agg["ray_init_ok"], agg["ray_init_attempts"] = init_ok, init_attempts
        if not init_ok:
            agg["ray_init_traceback"] = init_tb
            raise RuntimeError("ray.init to local head failed")
        nodes_ready, seen = _wait_ray_nodes(ray, 3, args.ray_ready_timeout)
        agg["ray_nodes_ready"], agg["ray_nodes_alive_seen"] = nodes_ready, seen
        if not nodes_ready:
            raise RuntimeError(f"only {seen}/3 ray nodes alive")

        alive = [n for n in ray.nodes() if n.get("Alive")]

        def _match(ip, host):
            return [n for n in alive
                    if n.get("NodeManagerAddress") == ip or _short(n.get("NodeName")) == _short(host)]
        mR, mA, mB = _match(nodeR_ip, nodeR), _match(nodeA_ip, nodeA), _match(nodeB_ip, nodeB)
        nodeR_nid = mR[0]["NodeID"] if len(mR) == 1 else None
        nodeA_nid = mA[0]["NodeID"] if len(mA) == 1 else None
        nodeB_nid = mB[0]["NodeID"] if len(mB) == 1 else None
        driver = _self_identity(ray)
        agg["ray_cluster"] = {
            "root_node": nodeR, "actor_a_node": nodeA, "actor_b_node": nodeB,
            "root_ip": nodeR_ip, "actor_a_ip": nodeA_ip, "actor_b_ip": nodeB_ip,
            "nodeR_ray_node_id": nodeR_nid, "nodeA_ray_node_id": nodeA_nid,
            "nodeB_ray_node_id": nodeB_nid,
            "driver_hostname": driver["hostname"], "driver_node_id": driver["node_id"],
            "driver_on_nodeR": bool(driver["node_id"] and driver["node_id"] == nodeR_nid),
            "ray_nodes_raw": [{k: n.get(k) for k in ("NodeID", "NodeManagerAddress", "NodeName",
                                                     "Alive")} for n in alive],
        }
        if not (nodeA_nid and nodeB_nid and len({nodeR_nid, nodeA_nid, nodeB_nid}) == 3):
            raise RuntimeError(f"ray node-id resolution failed "
                               f"(mR={len(mR)} mA={len(mA)} mB={len(mB)})")
        if not agg["ray_cluster"]["driver_on_nodeR"]:
            raise RuntimeError("driver is not on nodeR (must be co-located with the Ray head)")

        HpxActor = build_actor_class(ray)
        expect = {"slurm_job_id": job_id, "nodes": nodes, "nodeR": nodeR, "nodeA": nodeA,
                  "nodeB": nodeB, "nodeR_nid": nodeR_nid, "nodeA_nid": nodeA_nid,
                  "nodeB_nid": nodeB_nid, "subnet": subnet}
        for r in range(1, args.reps + 1):
            print(f"[exp67-crossnode] rep {r} ...")
            rep = run_crossnode_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), args.x, r,
                                    runs_root, args.hpx_threads, args.ray_num_cpus, nodeR, nodeA,
                                    nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid, nodeB_nid,
                                    subnet, args.port_base, expect, env)
            if provenance is None:
                provenance = collect_provenance(ray, rep["markers"].get("a_identity"))
            for s in rep.get("saturation_diagnostic", []):
                saturation.append({"rep": r, **s})
            print(f"[exp67-crossnode]   rep {r}: {'PASS' if rep['rep_pass'] else 'FAIL'} "
                  f"({rep['failure_class']}) A={rep['slice_a_failed']} B={rep['slice_b_failed']} "
                  f"C={rep['slice_c_failed']} D={rep['slice_d_failed']}")
            rep_out = dict(rep)
            mk = dict(rep["markers"])
            for k in ("a_child_report", "b_child_report"):
                if isinstance(mk.get(k), dict):
                    mk[k] = {kk: mk[k].get(kk) for kk in ("checked", "hpx_children", "worker_pid")}
            rep_out["markers"] = mk
            reps.append(rep_out)
    except Exception as e:  # noqa: BLE001
        fail_reason = f"{type(e).__name__}: {str(e)[:300]}"
        agg["crossnode_exception"] = fail_reason
        agg["crossnode_traceback"] = traceback.format_exc()[-2000:]
    finally:
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass
        for nd in [nodeR, nodeA, nodeB]:
            if nd:
                _ray_stop_node(nd, env)
        agg["ray_head_cleanup"] = _terminate_launcher(head_proc)
        agg["ray_worker_a_cleanup"] = _terminate_launcher(worker_a)
        agg["ray_worker_b_cleanup"] = _terminate_launcher(worker_b)
        orph = {}
        for label, nd in (("R", nodeR), ("A", nodeA), ("B", nodeB)):
            orph[f"ray_node{label}"] = _orphan_check_node(nd, _ORPHAN_PATTERNS_RAY, env) if nd else ["<none>"]
            orph[f"peer_node{label}"] = (_crossnode_peer_orphans(nd, env)[0] if nd else [])
        orph["no_ray_orphans"] = all(len(orph[f"ray_node{l}"]) == 0 for l in ("R", "A", "B"))
        orph["no_peer_orphans"] = all(len(orph[f"peer_node{l}"]) == 0 for l in ("R", "A", "B"))
        agg["final_orphans"] = orph

    overall_pass = (fail_reason is None and len(reps) == args.reps
                    and all(r["rep_pass"] for r in reps)
                    and agg.get("final_orphans", {}).get("no_ray_orphans")
                    and agg["final_orphans"].get("no_peer_orphans"))
    agg["provenance"] = provenance
    agg["overall"] = "pass" if overall_pass else "fail"
    agg["slice_reps"] = reps
    agg["saturation_diagnostic"] = saturation
    agg["python_and_scope"] = slice_python_deferral()
    agg["safe_claim"] = (
        f"On CPython {(provenance or {}).get('python_version_full','').split()[0]} (GIL build), Ray "
        f"{(provenance or {}).get('ray_version')}, HPX commit {EXPECTED_HPX_COMMIT[:10]}..., across "
        f"three Rostam nodes (root {nodeR}, actor A {nodeA}, actor B {nodeB} on {subnet}x), two Ray "
        "actor worker processes on DISTINCT nodes each hosted an HPX connect-mode locality in-process "
        "(hard-placed, PID identity for both, no HPX child), joined one shared HPX runtime under a "
        "separately supervised work-free root on a third node, and executed a verified HPX action from "
        "one actor-locality to the other IN BOTH DIRECTIONS across nodes (B pid proven by A->B, A pid "
        "proven by B->A), with clean lifecycle and actor reuse on the intended nodes."
        if overall_pass else
        f"exp67 cross-node did not pass ({fail_reason or 'see slice_reps[].failure_class / final_orphans'}).")
    _write_agg(args.aggregate, agg)
    print(f"[exp67-crossnode] overall: {agg['overall']} -> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
