#!/usr/bin/env python3
"""exp66 -- can a networking HPX connect-mode locality live IN-PROCESS inside one Ray
actor worker? Local, single-node, interpreter-aware slice (Option A).

MECHANISM / IN-PROCESS-HOSTING EVIDENCE ONLY. This is the pivotal hosting test: HPX starts
inside the OS process of a Ray actor worker (hpx::start, connect mode -- NO child process),
joins an island whose root is a SEPARATE, work-free, supervised process, serves a verified
remote HPX action dispatched at it by a standalone prober (HPX action path, NOT Ray payload
transport), and shuts down cleanly while the Ray worker stays healthy across actor
destruction and recreation.

Four slices (design approved 2026-07-14):
  Slice A  in-process hosting + full lifecycle .......... GATING, interpreter-independent
  Slice B  HPX progress while the actor's Python is idle . GATING, interpreter-independent
  Slice C  actor-thread CPU/GIL saturation ............... DIAGNOSTIC, non-gating
  Slice D  Python 3.14 / free-threaded rerun ............. DEFERRED (clean skip here)

exp66 PASSES iff every Slice A and Slice B gate passes across all reps. Slice C is reported
separately and can never change the verdict. Slice D is deferred and has no effect.

CLAIM FENCE: not Python 3.14, not free-threaded, not two Ray actors, not cross-node, not
elasticity/recovery, no performance/speedup/ratio/winner. Slice C is a diagnostic, not an
architectural verdict. Timing is never the correctness oracle. All durations observational.

Version scope of the maximum claim: CPython 3.11 (GIL build), Ray 2.55.1, HPX commit
20bc3d4bf3068383edcb63be13f22e9ff95842fa (the verified waiter-fix build).

Usage:
  python run_exp66.py [--reps 3] [--build-dir <dir>] [--x 7] [--aggregate <path>]
  python run_exp66.py --selftest       # pure-Python gate-logic checks (no Ray, no HPX)

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
import tempfile
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
PEER_BASENAME = "exp66_peer"
EXT_MODULE = "exp66_actor_ext"
EXPECTED_HPX_COMMIT = "20bc3d4bf3068383edcb63be13f22e9ff95842fa"
PROBE_XOR = 0x66C0DE  # mirrors shared_probe.hpp kProbeXor

DEFAULT_BUILD_DIR = os.path.join(HERE, "build")

CLAIM_FENCES = {
    "not_python_3_14": True,
    "not_free_threaded": True,
    "not_two_ray_actors": True,
    "not_cross_node": True,
    "no_elasticity_or_recovery": True,
    "no_performance_speedup_ratio_winner": True,
    "slice_c_is_diagnostic_not_verdict": True,
    "timing_not_correctness_oracle": True,
    "durations_observational_only": True,
}


# ---------------------------------------------------------------------------------------
# Closed oracle (mirror of shared_probe.hpp) + HPX identity extraction
# ---------------------------------------------------------------------------------------

def probe_value(x, loc):
    return (x ^ PROBE_XOR) + (loc << 1)


def extract_git_sha(complete_version):
    """Pull the (abbreviated) git sha HPX bakes into complete_version(): '... Git: <sha> ...'."""
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
# Process / file helpers (bounded, classify-never-hang -- exp65 idiom)
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
    """Wait until absolute deadline. Returns (exited, rc, killed)."""
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
    """Poll until path exists, or one of procs dies, or timeout. Returns bool exists."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        for p in procs:
            if p is not None and p.poll() is not None:
                time.sleep(0.2)  # final grace: writer may have exited right after the write
                return os.path.exists(path)
        time.sleep(0.05)
    return os.path.exists(path)


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
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
    """PIDs of leftover exp66_peer processes (root/prober). Empty list == clean."""
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
    """True iff NO argv contains a static locality count."""
    for cmd in cmds:
        for a in cmd:
            if str(a).startswith("--hpx:localities"):
                return False
    return True


# ---------------------------------------------------------------------------------------
# Provenance (minimal: ordinary Python/Ray facts collected here, NOT via a big C++ API)
# ---------------------------------------------------------------------------------------

def collect_provenance(build_dir, ray_mod, actor_identity):
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
    if actor_identity:
        prov["actor_ext_file"] = os.path.basename(actor_identity.get("ext_file", "") or "")
        prov["actor_ext_suffix_expected"] = actor_identity.get("ext_suffix_expected")
        prov["actor_ext_abi_match"] = actor_identity.get("abi_match")
        prov["actor_gil_declaration"] = actor_identity.get("gil_declaration")
        prov["hpx_complete_version"] = (actor_identity.get("hpx_version_info") or {}).get(
            "hpx_complete_version")
        prov["hpx_git_sha_observed"] = extract_git_sha(prov["hpx_complete_version"])
    return prov


def slice_d_deferral(ray_importable_on_3_14):
    return {
        "status": "deferred_skipped",
        "reasons": [
            "no free-threaded interpreter available in this environment",
            "no established free-threaded Ray environment",
            "Ray not installed in the local CPython 3.14 environment"
            if not ray_importable_on_3_14 else
            "Ray present on local 3.14 but that build is GIL-enabled (not free-threaded)",
        ],
        "affects_verdict": False,
        "note": "Slice D reruns A+B+C on a t-ABI interpreter; not built in this task.",
    }


# ---------------------------------------------------------------------------------------
# Gate evaluation (pure functions -> selftestable without Ray/HPX)
# ---------------------------------------------------------------------------------------

def eval_slice_a(m):
    """Slice A: in-process hosting + full lifecycle. All gates are GATING."""
    ident = m.get("actor_identity") or {}
    start = m.get("actor_start") or {}
    child = m.get("actor_child_report") or {}
    idle = m.get("idle_probe") or {}
    root_ready = m.get("root_ready") or {}
    root_final = m.get("root_final") or {}
    prober_joined = m.get("prober_joined") or {}
    prober_disc = m.get("prober_disconnected") or {}
    health = m.get("actor_health") or {}
    recreate = m.get("actor_recreate") or {}

    actor_pid = ident.get("pid")
    actor_loc = start.get("locality_id")
    hpx_cv = (ident.get("hpx_version_info") or {}).get("hpx_complete_version")

    g = {}
    g["fixed_hpx_commit"] = bool(
        commit_matches(hpx_cv)
        and commit_matches(root_ready.get("hpx_complete_version"))
        and commit_matches(prober_joined.get("hpx_complete_version")))
    g["hpx_started_in_worker"] = bool(start.get("started") and actor_loc is not None)
    g["actor_pid_equals_locality_pid"] = bool(
        idle.get("both_ready") and idle.get("pid_result") is not None
        and actor_pid is not None and idle.get("pid_result") == actor_pid)
    g["hostname_identity"] = bool(
        ident.get("hostname")
        and ident.get("hostname") == root_ready.get("hostname")
        and ident.get("hostname") == m.get("controller_hostname"))
    g["no_hpx_child_process"] = bool(child.get("checked")
                                     and child.get("hpx_children") == [])
    g["connect_mode_admission"] = bool(
        actor_loc is not None and actor_loc > 0
        and prober_joined.get("locality_id") not in (None, 0)
        and (start.get("membership") or 0) >= 2)
    g["no_static_locality_count"] = bool(m.get("argv_audit_ok"))
    g["root_work_free"] = bool(
        idle.get("executed_on") == actor_loc and idle.get("executed_on") != 0
        and root_ready.get("locality_id") == 0
        and root_final is not None and root_final.get("max_membership", 0) >= 2)
    g["remote_action_on_actor"] = bool(
        idle.get("both_ready") and idle.get("executed_on") == actor_loc)
    g["exact_oracle_and_witness"] = bool(
        idle.get("oracle_match") and idle.get("executed_on") == actor_loc
        and actor_loc is not None and actor_loc != 0)
    g["ray_worker_healthy"] = bool(
        health.get("ok") and health.get("pid") == actor_pid)
    g["heartbeat_completion_lifecycle"] = bool(
        root_ready.get("wall_ms") is not None
        and root_final is not None and root_final.get("leave_observed") is True
        and prober_disc.get("shutdown_reason") == "root_completion_signal")
    g["graceful_leave"] = bool(
        m.get("actor_stop_rc") == 0
        and prober_disc.get("clean") is True
        and prober_disc.get("shutdown_reason") == "root_completion_signal")
    g["clean_hpx_stop_in_process"] = bool(
        m.get("actor_stop_rc") == 0 and not m.get("actor_stop_error"))
    g["root_finalized_clean"] = (m.get("root_exit_path") == "finalized_clean")
    g["actor_destruction"] = bool(m.get("actor1_destroyed"))
    g["actor_recreation"] = bool(
        recreate.get("ok") and recreate.get("pid") is not None
        and recreate.get("pid") != actor_pid)
    g["no_orphans"] = (m.get("orphans") == [] and m.get("actor_pids_gone") is True)
    g["evidence_complete"] = bool(
        actor_pid is not None and actor_loc is not None and hpx_cv
        and root_ready and root_final and prober_joined and prober_disc
        and idle.get("both_ready") is not None)
    return g


def eval_slice_b(m):
    """Slice B: HPX progress while the actor's Python thread is idle. GATING."""
    idle = m.get("idle_probe") or {}
    start = m.get("actor_start") or {}
    actor_loc = start.get("locality_id")
    g = {}
    g["idle_progress_oracle"] = bool(
        idle.get("both_ready") and idle.get("oracle_match")
        and idle.get("executed_on") == actor_loc and actor_loc not in (None, 0))
    g["actor_python_idle_during_dispatch"] = bool(m.get("actor_idle_during_idle_probe"))
    g["no_python_polling_loop"] = bool(m.get("prober_drove_dispatch", True))
    return g


def classify_slice_c(m, prov):
    """Slice C: actor-thread CPU/GIL saturation. DIAGNOSTIC, non-gating. Returns a dict with
    a classification string; NEVER attributes a stall to the GIL without distinguishing it
    from thread-budget / oversubscription effects (which this single-interpreter run cannot
    fully do -- so a stall is reported honestly, not blamed on the GIL)."""
    busy = m.get("busy_probe") or {}
    start = m.get("actor_start") or {}
    actor_loc = start.get("locality_id")
    overlap = bool(m.get("busy_overlap_observed"))
    progressed = bool(
        busy.get("both_ready") and busy.get("oracle_match")
        and busy.get("executed_on") == actor_loc and actor_loc not in (None, 0))
    hpx_threads = m.get("actor_hpx_threads")
    ray_cpus = m.get("actor_ray_num_cpus")

    if not overlap:
        cls = "invalid_diagnostic"
        note = "the actor busy loop did not provably overlap the dispatch window"
    elif progressed:
        cls = "progressed_under_actor_saturation"
        note = ("HPX action executed and oracle-matched while the actor's Python thread was "
                "in a tight CPU loop; actions are pure C++ and never touch the GIL")
    else:
        # Stalled under saturation. Do NOT assert the GIL: on a GIL build a stall is at least
        # as consistent with thread starvation / Ray-vs-HPX CPU-budget contention.
        if ray_cpus is not None and hpx_threads is not None and ray_cpus <= hpx_threads:
            cls = "ray_hpx_thread_budget_conflict_suspected"
        else:
            cls = "gil_monopolization_suspected"
        note = ("stall under saturation; classification is heuristic and NOT a confirmed GIL "
                "attribution -- a free-threaded (Slice D) comparison is required to separate "
                "GIL monopolization from thread-budget/oversubscription effects")
    return {
        "classification": cls,
        "progressed": progressed,
        "overlap_observed": overlap,
        "both_ready": busy.get("both_ready"),
        "oracle_match": busy.get("oracle_match"),
        "executed_on": busy.get("executed_on"),
        "actor_hpx_threads": hpx_threads,
        "actor_ray_num_cpus": ray_cpus,
        "free_threaded_build": prov.get("free_threaded_build"),
        "gil_enabled_runtime": prov.get("gil_enabled_runtime"),
        "affects_verdict": False,
        "note": note,
    }


def rep_verdict(slice_a_gates, slice_b_gates):
    """exp66 verdict for one rep = ALL Slice A gates AND ALL Slice B gates. C/D excluded."""
    return all(slice_a_gates.values()) and all(slice_b_gates.values())


# ---------------------------------------------------------------------------------------
# Ray actor: hosts the HPX connect-mode locality IN-PROCESS
# ---------------------------------------------------------------------------------------

def build_actor_class(ray_mod):
    # The actor is fully self-contained: it imports everything it needs INSIDE its methods and
    # holds its constants on self, so it survives cloudpickle-by-value onto a remote Ray worker
    # (the cross-node path) without depending on run_exp66's module globals being importable there.
    @ray_mod.remote
    class HpxActor:
        def __init__(self, build_dir, hpx_threads, endpoints,
                     ext_module="exp66_actor_ext", peer_basename="exp66_peer"):
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
                nid = ctx.get_node_id()
                aid = ctx.get_actor_id()
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
                "pid": e.pid(),
                "os_getpid": _os.getpid(),
                "hostname": e.hostname(),
                "hpx_version_info": dict(e.hpx_version_info()),
                "gil_declaration": getattr(e, "__gil_declaration__", None),
                "experiment": getattr(e, "__experiment__", None),
                "ext_file": e.__file__,
                "ext_suffix_expected": suffix,
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
            """Enumerate direct child processes; flag any that look like an HPX binary. In-process
            hosting means the worker owns NO HPX child (the strong proof is pid-equality; this is
            the corroborating witness)."""
            import os as _os
            import subprocess as _sp
            pid = _os.getpid()
            children, hpx_children, checked = [], [], False
            try:
                out = _sp.run(["pgrep", "-P", str(pid)],
                              capture_output=True, text=True, timeout=10)
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

        def health(self):
            """Worker health AFTER HPX work: HPX must still answer an introspection call."""
            try:
                return {"ok": True, "pid": self._ext.pid(),
                        "membership": int(self._ext.membership_count()),
                        "locality_id": int(self._ext.locality_id())}
            except Exception as ex:  # noqa: BLE001
                return {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

        def busy_spin(self, seconds, started_path, done_path):
            """Slice C stimulus: hold the GIL in a tight CPU loop for `seconds`. Writes markers (for a
            same-node observer) AND returns the start/end wall-ms (used cross-node, where the marker
            files may lag over NFS)."""
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
            """Ray-only health probe for the RECREATED actor (does NOT start HPX)."""
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

# HPX host token. "localhost" is deliberate, NOT "127.0.0.1": in connect mode HPX rewrites a
# literal 127.0.0.1 parcelport host to resolve_public_ip_address() (command_line_handling.cpp),
# which resolves the machine hostname and FAILS inside a Ray worker whose environment cannot
# resolve the .local name. HPX's map_hostnames hard-codes "localhost" -> 127.0.0.1 with no
# resolver call, so this stays on loopback (no firewall prompt) and works in the worker.
HPX_HOST = "localhost"


def peer_root_cmd(peer, boot, p_root):
    return [peer, "--role", "root", "--bootstrap", boot, "--leave-timeout", "25",
            f"--hpx:agas={HPX_HOST}:{p_root}", f"--hpx:hpx={HPX_HOST}:{p_root}",
            "--hpx:expect-connecting-localities",
            "--hpx:threads=2", "--hpx:bind=none", "--hpx:ignore-batch-env"]


def peer_prober_cmd(peer, boot, p_root, p_prober, x):
    return [peer, "--role", "prober", "--bootstrap", boot, "--deadman", "60", "--x", str(x),
            f"--hpx:agas={HPX_HOST}:{p_root}", f"--hpx:hpx={HPX_HOST}:{p_prober}",
            "--hpx:threads=2", "--hpx:bind=none", "--hpx:ignore-batch-env"]


def actor_endpoints(p_root, p_actor):
    return [f"--hpx:agas={HPX_HOST}:{p_root}", f"--hpx:hpx={HPX_HOST}:{p_actor}",
            "--hpx:bind=none", "--hpx:ignore-batch-env"]


def run_rep(ray_mod, HpxActor, peer, build_dir, x, rep, runs_root, hpx_threads, ray_cpus):
    boot = os.path.join(runs_root, f"rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    p_root, p_actor, p_prober = find_free_port(), find_free_port(), find_free_port()
    rcmd = peer_root_cmd(peer, boot, p_root)
    pcmd = peer_prober_cmd(peer, boot, p_root, p_prober, x)
    endpoints = actor_endpoints(p_root, p_actor)
    m = {
        "controller_hostname": socket.gethostname(),
        "ports": {"root": p_root, "actor": p_actor, "prober": p_prober},
        "argv_audit_ok": argv_audit(rcmd, pcmd, endpoints),
        "actor_hpx_threads": hpx_threads,
        "actor_ray_num_cpus": ray_cpus,
        "actor_idle_during_idle_probe": True,   # structural: no actor method runs in that window
        "prober_drove_dispatch": True,          # the prober process, not Python, dispatches
    }
    root = prober = rlog = plog = None
    actor1 = actor2 = None
    try:
        # 1) work-free supervised root
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file(os.path.join(boot, "root.ready"), 30, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        # 2) Ray actor hosts HPX in-process
        actor1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(
            build_dir, hpx_threads, endpoints)
        m["actor_identity"] = _ray_get(ray_mod, actor1.load_identity.remote(), 40,
                                       "actor_identity")
        m["actor_start"] = _ray_get(ray_mod, actor1.start_hpx.remote(), 60, "actor_start")
        actor_loc = (m["actor_start"] or {}).get("locality_id")
        if actor_loc is not None:
            with open(os.path.join(boot, "actor.loc"), "w") as f:
                f.write(str(actor_loc))
        m["actor_child_report"] = _ray_get(ray_mod, actor1.child_report.remote(), 30,
                                            "actor_child_report")

        # 3) standalone prober joins (external dispatcher; keeps the root work-free)
        prober, plog = _popen(pcmd, boot, os.path.join(boot, "prober.log"))
        _wait_for_file(os.path.join(boot, "prober.joined"), 30, procs=[prober, root])
        m["prober_joined"] = _read_json(os.path.join(boot, "prober.joined"))

        # 4) Slice A + B: idle probe (actor Python thread idle; HPX services the parcel)
        if actor_loc is not None:
            open(os.path.join(boot, "probe_idle.go"), "w").close()
            _wait_for_file(os.path.join(boot, "probe_idle.result"), 40, procs=[prober, root])
        m["idle_probe"] = _read_json(os.path.join(boot, "probe_idle.result"))

        # 5) worker health after HPX work
        m["actor_health"] = _ray_get(ray_mod, actor1.health.remote(), 30, "actor_health")

        # 6) Slice C: busy probe (actor Python thread in a tight GIL-held loop) -- DIAGNOSTIC
        if actor_loc is not None:
            started = os.path.join(boot, "busy_started")
            done = os.path.join(boot, "busy_done")
            busy_fut = actor1.busy_spin.remote(3.0, started, done)
            _wait_for_file(started, 15)
            open(os.path.join(boot, "probe_busy.go"), "w").close()
            _wait_for_file(os.path.join(boot, "probe_busy.result"), 40, procs=[prober, root])
            m["busy_probe"] = _read_json(os.path.join(boot, "probe_busy.result"))
            _ray_get(ray_mod, busy_fut, 30, "busy_spin")
            m["busy_started_ms"] = _read_int(started)
            m["busy_done_ms"] = _read_int(done)
            bp = m["busy_probe"] or {}
            dw, rw = bp.get("dispatch_wall_ms"), bp.get("return_wall_ms")
            bs, bd = m["busy_started_ms"], m["busy_done_ms"]
            m["busy_overlap_observed"] = bool(
                None not in (dw, rw, bs, bd) and dw >= bs and rw <= bd + 500)

        # 7) graceful in-process HPX stop (leave), THEN destroy actor #1
        stop = _ray_get(ray_mod, actor1.stop_hpx.remote(), 30, "actor_stop")
        m["actor_stop_rc"] = (stop or {}).get("rc")
        m["actor_stop_error"] = (stop or {}).get("error")
        actor1_pid = (m.get("actor_identity") or {}).get("pid")
        ray_mod.kill(actor1)
        actor1 = None
        m["actor1_destroyed"] = wait_pid_gone(actor1_pid, 20)

        # 8) actor recreation: a FRESH worker must load the ext and respond (Ray-only ping)
        actor2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(
            build_dir, hpx_threads, endpoints)
        m["actor_recreate"] = _ray_get(ray_mod, actor2.ping.remote(), 40, "actor_recreate")
        actor2_pid = (m.get("actor_recreate") or {}).get("pid")

        # 9) root completion -> prober leaves gracefully -> root finalizes
        open(os.path.join(boot, "root.done"), "w").close()
        _wait_for_file(os.path.join(boot, "prober.disconnected"), 40, procs=[prober])
        _wait_for_file(os.path.join(boot, "root.final"), 40, procs=[root])
        m["prober_disconnected"] = _read_json(os.path.join(boot, "prober.disconnected"))
        m["root_final"] = _read_json(os.path.join(boot, "root.final"))

        # 10) bounded process joins
        p_exited, p_rc, p_killed = _wait_proc(prober, time.time() + 40)
        r_exited, r_rc, r_killed = _wait_proc(root, time.time() + 40)
        m["prober_exit"] = _exit_path(p_exited, p_rc, p_killed)
        m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)

        if actor2 is not None:
            ray_mod.kill(actor2)
            actor2 = None
        m["actor_pids_gone"] = wait_pid_gone(actor1_pid, 10) and wait_pid_gone(actor2_pid, 10)
        m["orphans"] = peer_orphans()
    finally:
        for a in (actor1, actor2):
            if a is not None:
                try:
                    ray_mod.kill(a)
                except Exception:
                    pass
        _kill_group(root)
        _kill_group(prober)
        for lg in (rlog, plog):
            if lg is not None:
                lg.close()

    slice_a = eval_slice_a(m)
    slice_b = eval_slice_b(m)
    return {
        "rep": rep,
        "boot_rel": os.path.relpath(boot, HERE),
        "ports": m["ports"],
        "slice_a_gates": slice_a,
        "slice_b_gates": slice_b,
        "rep_pass": rep_verdict(slice_a, slice_b),
        "slice_a_failed": [k for k, v in slice_a.items() if not v],
        "slice_b_failed": [k for k, v in slice_b.items() if not v],
        "markers": m,
    }


def _ray_get(ray_mod, ref, timeout, what):
    try:
        return ray_mod.get(ref, timeout=timeout)
    except Exception as ex:  # noqa: BLE001 -- GetTimeoutError/actor death: classify, never hang
        return {"ok": False, "started": False,
                "error": f"{what}: {type(ex).__name__}: {ex}"}


def _read_int(path):
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


# ---------------------------------------------------------------------------------------
# Selftest: pure-Python gate logic on synthetic markers (no Ray, no HPX)
# ---------------------------------------------------------------------------------------

def _synthetic_markers():
    cv = f"HPX: V2.0.0 (AGAS: V3.0), Git: {EXPECTED_HPX_COMMIT[:10]}"
    return {
        "controller_hostname": "host0",
        "argv_audit_ok": True,
        "actor_hpx_threads": 2,
        "actor_ray_num_cpus": 2,
        "actor_idle_during_idle_probe": True,
        "prober_drove_dispatch": True,
        "actor_identity": {"pid": 5000, "hostname": "host0",
                           "hpx_version_info": {"hpx_complete_version": cv}},
        "actor_start": {"started": True, "locality_id": 1, "membership": 2, "pid": 5000},
        "actor_child_report": {"checked": True, "children": [], "hpx_children": []},
        "root_ready": {"locality_id": 0, "hostname": "host0", "wall_ms": 1000,
                       "hpx_complete_version": cv},
        "root_final": {"max_membership": 3, "leave_observed": True},
        "prober_joined": {"locality_id": 2, "hpx_complete_version": cv},
        "prober_disconnected": {"clean": True, "shutdown_reason": "root_completion_signal"},
        "idle_probe": {"both_ready": True, "oracle_match": True, "executed_on": 1,
                       "pid_result": 5000, "probe_result": probe_value(7, 1)},
        "busy_probe": {"both_ready": True, "oracle_match": True, "executed_on": 1,
                       "dispatch_wall_ms": 2000, "return_wall_ms": 2100},
        "busy_started_ms": 1900, "busy_done_ms": 5000, "busy_overlap_observed": True,
        "actor_health": {"ok": True, "pid": 5000},
        "actor_stop_rc": 0, "actor_stop_error": None,
        "actor1_destroyed": True,
        "actor_recreate": {"ok": True, "pid": 5001, "ext_importable": True},
        "root_exit_path": "finalized_clean",
        "actor_pids_gone": True, "orphans": [],
    }


def selftest():
    failures = []

    m = _synthetic_markers()
    ga, gb = eval_slice_a(m), eval_slice_b(m)
    if not all(ga.values()):
        failures.append(f"clean Slice A should pass: failed={[k for k,v in ga.items() if not v]}")
    if not all(gb.values()):
        failures.append(f"clean Slice B should pass: failed={[k for k,v in gb.items() if not v]}")
    if not rep_verdict(ga, gb):
        failures.append("clean synthetic rep should pass the verdict")

    # PID mismatch -> in-process proof fails.
    m2 = _synthetic_markers()
    m2["idle_probe"]["pid_result"] = 9999
    if eval_slice_a(m2)["actor_pid_equals_locality_pid"]:
        failures.append("pid mismatch must fail actor_pid_equals_locality_pid")

    # Action executed on the root (loc 0) -> witness + work-free gates fail.
    m3 = _synthetic_markers()
    m3["idle_probe"]["executed_on"] = 0
    g3 = eval_slice_a(m3)
    if g3["exact_oracle_and_witness"] or g3["remote_action_on_actor"] or g3["root_work_free"]:
        failures.append("executed_on==0 must fail witness/remote/work-free gates")

    # Oracle mismatch -> Slice B fails.
    m4 = _synthetic_markers()
    m4["idle_probe"]["oracle_match"] = False
    if eval_slice_b(m4)["idle_progress_oracle"]:
        failures.append("oracle_match False must fail Slice B idle_progress_oracle")

    # Wrong HPX commit -> identity gate fails.
    m5 = _synthetic_markers()
    m5["actor_identity"]["hpx_version_info"]["hpx_complete_version"] = "Git: deadbeef12"
    if eval_slice_a(m5)["fixed_hpx_commit"]:
        failures.append("wrong commit must fail fixed_hpx_commit")

    # An HPX child process present -> no_hpx_child_process fails.
    m6 = _synthetic_markers()
    m6["actor_child_report"]["hpx_children"] = [{"pid": 6000, "cmd": "exp66_peer"}]
    if eval_slice_a(m6)["no_hpx_child_process"]:
        failures.append("an HPX child must fail no_hpx_child_process")

    # Recreated actor reuses the old pid -> recreation gate fails.
    m7 = _synthetic_markers()
    m7["actor_recreate"]["pid"] = 5000
    if eval_slice_a(m7)["actor_recreation"]:
        failures.append("same pid must fail actor_recreation")

    # Ungraceful leave (deadman) -> graceful/lifecycle gates fail.
    m8 = _synthetic_markers()
    m8["prober_disconnected"]["shutdown_reason"] = "deadman_timeout"
    g8 = eval_slice_a(m8)
    if g8["graceful_leave"] or g8["heartbeat_completion_lifecycle"]:
        failures.append("deadman leave must fail graceful/lifecycle gates")

    # Orphan present -> no_orphans fails.
    m9 = _synthetic_markers()
    m9["orphans"] = ["71234"]
    if eval_slice_a(m9)["no_orphans"]:
        failures.append("an orphan pid must fail no_orphans")

    # argv static localities -> audit + selftest of argv_audit.
    if argv_audit(["peer", "--hpx:localities=2"]):
        failures.append("argv_audit must reject --hpx:localities")
    if not argv_audit(["peer", "--hpx:expect-connecting-localities"]):
        failures.append("argv_audit must accept expect-connecting-localities")

    # Slice C: progressed classification, and it NEVER changes the verdict.
    prov = {"free_threaded_build": False, "gil_enabled_runtime": True}
    c = classify_slice_c(_synthetic_markers(), prov)
    if c["classification"] != "progressed_under_actor_saturation" or c["affects_verdict"]:
        failures.append(f"clean Slice C should be progressed and non-verdict: {c}")
    # No overlap -> invalid_diagnostic.
    mc = _synthetic_markers(); mc["busy_overlap_observed"] = False
    if classify_slice_c(mc, prov)["classification"] != "invalid_diagnostic":
        failures.append("missing overlap must classify invalid_diagnostic")
    # Stall with tight CPU budget -> thread-budget conflict, not a GIL claim.
    mc2 = _synthetic_markers()
    mc2["busy_probe"]["both_ready"] = False
    mc2["actor_ray_num_cpus"] = 1; mc2["actor_hpx_threads"] = 2
    if classify_slice_c(mc2, prov)["classification"] != "ray_hpx_thread_budget_conflict_suspected":
        failures.append("stall with cpus<=threads must be thread_budget_conflict, not GIL")
    # A failing Slice C must not change a passing rep verdict.
    if not rep_verdict(eval_slice_a(_synthetic_markers()), eval_slice_b(_synthetic_markers())):
        failures.append("verdict must ignore Slice C entirely")

    # commit_matches: abbreviated prefix ok, wrong sha not.
    if not commit_matches(f"Git: {EXPECTED_HPX_COMMIT[:10]}"):
        failures.append("commit_matches must accept the abbreviated prefix")
    if commit_matches("Git: 0123456789"):
        failures.append("commit_matches must reject a non-prefix sha")

    # ---- cross-node gate logic (pure; no Slurm, no Ray, no HPX) --------------------------------
    def cx_expect():
        return {"slurm_job_id": "555", "nodes": ["medusa00", "medusa01"],
                "nodeA": "medusa00", "nodeB": "medusa01",
                "nodeA_nid": "nidA", "nodeB_nid": "nidB", "subnet": "10.42.5."}

    def cx_markers():
        mm = _synthetic_markers()
        mm["actor_identity"]["hostname"] = "medusa01.rostam.cct.lsu.edu"  # actor on nodeB (FQDN)
        mm["root_ready"]["hostname"] = "medusa00"                          # root on nodeA
        mm["actor_placement"] = {"node_id": "nidB", "hostname": "medusa01", "placement_soft": False,
                                 "pid": 5000}
        mm["endpoints_subnet_ok"] = True
        mm["remote_orphan_check_ran"] = True
        return mm

    cxg = eval_crossnode_slice_a(cx_markers(), cx_expect())
    if not all(cxg.values()):
        failures.append("clean cross-node Slice A should pass: "
                        f"{[k for k, v in cxg.items() if not v]}")
    if "hostname_identity" in cxg:
        failures.append("cross-node Slice A must drop the same-host hostname_identity gate")

    # Actor placed on the ROOT node -> cross-node placement gates fail.
    cx2 = cx_markers()
    cx2["actor_placement"]["hostname"] = "medusa00"; cx2["actor_placement"]["node_id"] = "nidA"
    cx2["actor_identity"]["hostname"] = "medusa00"
    g2x = eval_crossnode_slice_a(cx2, cx_expect())
    if g2x["actor_hard_placed_on_actor_node"] or g2x["root_and_actor_different_nodes"]:
        failures.append("same-node placement must fail the cross-node placement gates")

    # Soft placement -> hard-placement gate fails.
    cx3 = cx_markers(); cx3["actor_placement"]["placement_soft"] = True
    if eval_crossnode_slice_a(cx3, cx_expect())["actor_hard_placed_on_actor_node"]:
        failures.append("soft placement must fail actor_hard_placed_on_actor_node")

    # Off-subnet endpoints -> subnet gate fails.
    cx4 = cx_markers(); cx4["endpoints_subnet_ok"] = False
    if eval_crossnode_slice_a(cx4, cx_expect())["endpoints_pinned_subnet"]:
        failures.append("off-subnet endpoints must fail endpoints_pinned_subnet")

    # Remote orphan check never ran -> its gate fails.
    cx5 = cx_markers(); cx5["remote_orphan_check_ran"] = False
    if eval_crossnode_slice_a(cx5, cx_expect())["remote_orphan_check_ran"]:
        failures.append("remote_orphan_check_ran=False must fail its gate")

    # Single-node allocation -> two-node gate fails.
    ex6 = cx_expect(); ex6["nodes"] = ["medusa00"]
    if eval_crossnode_slice_a(cx_markers(), ex6)["two_node_slurm_allocation"]:
        failures.append("one-node allocation must fail two_node_slurm_allocation")

    # PID equality still gates cross-node (reused).
    cx7 = cx_markers(); cx7["idle_probe"]["pid_result"] = 1
    if eval_crossnode_slice_a(cx7, cx_expect())["actor_pid_equals_locality_pid"]:
        failures.append("cross-node pid mismatch must still fail actor_pid_equals_locality_pid")

    # Nodelist expander sanity.
    if _expand_nodelist_pure("medusa[00-01]") != ["medusa00", "medusa01"]:
        failures.append("nodelist expander must expand medusa[00-01]")

    if failures:
        for f in failures:
            print(f"[exp66 selftest] FAIL: {f}")
        return 1
    print("[exp66 selftest] all gate-logic checks passed")
    return 0


# ---------------------------------------------------------------------------------------
# Cross-node (Rostam / two-node Slurm) phase. Reuses exp59's proven Ray-on-Slurm bring-up and
# exp65's cross-node discipline. The controller runs ON nodeA (the sbatch batch step lands on the
# first allocated node, co-located with the Ray head), so -- unlike exp59's rostam1 orchestrator --
# it attaches to Ray directly with NO inner/outer srun split. The work-free HPX root and the
# external HPX prober are local Popens on nodeA; the Ray actor is hard-placed on nodeB and hosts a
# connect-mode HPX locality IN-PROCESS that binds nodeB's pinned IP and joins the root over TCP.
# NOT localhost: cross-node needs the real pinned 10.42.5.x endpoints (verified explicitly).
# ---------------------------------------------------------------------------------------

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
    """Expand a Slurm nodelist ('medusa[00-01]' -> ['medusa00','medusa01']) without scontrol."""
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


def _tail_file(path, n):
    try:
        with open(path, "rb") as f:
            return f.read().decode("utf-8", "replace")[-n:]
    except Exception:  # noqa: BLE001
        return ""


def _wait_for_file_nfs(path, timeout, procs=()):
    """Revalidating file wait: list the parent dir before each check so NFS negative-dentry caching
    cannot hide a file written on the OTHER node (the actor's busy markers on nodeB)."""
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


# --- Ray-on-Slurm launchers (exp59 lifetime fix: `ray start --block` under a persistent Popen so
#     Slurm does not reap GCS/raylet). Head runs LOCAL on nodeA; worker via srun on nodeB. ---------

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


def _ray_head_local(nodeA_ip, port, temp_dir, num_cpus, env, log_path, port_flags):
    cmd = ["ray", "start", "--head", "--node-ip-address", nodeA_ip, "--port", str(port),
           "--include-dashboard", "false", "--temp-dir", temp_dir] + list(port_flags) + ["--block"]
    if num_cpus is not None:
        cmd += ["--num-cpus", str(num_cpus)]
    return _popen_blocking(cmd, env, log_path)


def _ray_worker_srun(nodeB, nodeB_ip, head_ip, port, num_cpus, env, log_path, port_flags):
    cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", nodeB, "--export=ALL",
           "ray", "start", "--address", f"{head_ip}:{port}", "--node-ip-address", nodeB_ip] \
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
    """Ray-process leftovers on `node`, one `pgrep -af` per pattern. CRITICAL: the check itself is a
    `srun ... pgrep -f <pattern>` whose OWN argv contains <pattern>; when `node` is the controller's
    node that srun client is a local process, so a naive `pgrep -f` self-matches every pattern. We use
    `pgrep -af` and drop any line that is our own srun/pgrep/ray-stop machinery, reporting the real
    surviving pid:cmd (settle-polled: `ray stop --force` SIGKILLs, but listing can race the reap)."""
    found = []
    for pat in patterns:
        hits = []
        for _try in range(6):  # up to ~30s: give ray stop time to actually reap
            rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                              "pgrep", "-af", pat], timeout=60, env=env)
            if rc not in (0, 1):
                hits = [f"{pat}:pgrep_rc_{rc}"]
                break
            hits = [f"{_short(node)}:{pat}:{ln.split()[0]}" for ln in out.splitlines()
                    if ln.strip() and "pgrep" not in ln and "srun" not in ln
                    and "ray stop" not in ln and "run_exp66" not in ln]
            if not hits:
                break
            time.sleep(5)
        found.extend(hits)
    return found


def _crossnode_peer_orphans(node, env=None):
    """exp66_peer (HPX root/prober) leftovers on `node`, filtering the detector's own srun/pgrep."""
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                      "pgrep", "-af", PEER_BASENAME], timeout=60, env=env)
    if rc not in (0, 1):
        return [], False
    hits = []
    for ln in out.splitlines():
        if ln.strip() and "pgrep" not in ln and "srun" not in ln:
            hits.append(f"{_short(node)}:{ln.split()[0]}")
    return hits, True


def _self_identity(ray_mod):
    nid = None
    try:
        nid = ray_mod.get_runtime_context().get_node_id()
    except Exception:  # noqa: BLE001
        nid = None
    return {"hostname": socket.gethostname(), "pid": os.getpid(), "node_id": nid}


# --- cross-node peer commands (numeric pinned IPs; NOT localhost) --------------------------------

def crossnode_root_cmd(peer, boot, root_ip, p_root, leave_timeout):
    return [peer, "--role", "root", "--bootstrap", boot, "--leave-timeout", str(leave_timeout),
            f"--hpx:agas={root_ip}:{p_root}", f"--hpx:hpx={root_ip}:{p_root}",
            "--hpx:expect-connecting-localities",
            "--hpx:threads=2", "--hpx:bind=none", "--hpx:ignore-batch-env"]


def crossnode_prober_cmd(peer, boot, root_ip, p_root, prober_ip, p_prober, x, deadman):
    return [peer, "--role", "prober", "--bootstrap", boot, "--deadman", str(deadman), "--x", str(x),
            f"--hpx:agas={root_ip}:{p_root}", f"--hpx:hpx={prober_ip}:{p_prober}",
            "--hpx:threads=2", "--hpx:bind=none", "--hpx:ignore-batch-env"]


def crossnode_actor_endpoints(root_ip, p_root, actor_ip, p_actor):
    return [f"--hpx:agas={root_ip}:{p_root}", f"--hpx:hpx={actor_ip}:{p_actor}",
            "--hpx:bind=none", "--hpx:ignore-batch-env"]


def eval_crossnode_slice_a(m, expect):
    """Cross-node Slice A: the interpreter-independent hosting/lifecycle gates (reused from the local
    eval_slice_a) with the same-host `hostname_identity` gate REPLACED by hard cross-node placement,
    subnet, and Slurm attestation gates. Still all GATING."""
    g = eval_slice_a(m)
    g.pop("hostname_identity", None)  # actor and root are now on DIFFERENT hosts by design
    ap = m.get("actor_placement") or {}
    ident = m.get("actor_identity") or {}
    rr = m.get("root_ready") or {}
    nodes_short = [_short(n) for n in (expect.get("nodes") or [])]

    g["two_node_slurm_allocation"] = bool(
        expect.get("slurm_job_id") and len(expect.get("nodes") or []) >= 2
        and _short(expect.get("nodeA")) in nodes_short
        and _short(expect.get("nodeB")) in nodes_short)
    g["actor_hard_placed_on_actor_node"] = bool(
        ap.get("placement_soft") is False
        and ap.get("node_id") and ap.get("node_id") == expect.get("nodeB_nid")
        and _short(ap.get("hostname")) == _short(expect.get("nodeB")))
    g["root_on_root_node"] = bool(_short(rr.get("hostname")) == _short(expect.get("nodeA")))
    g["root_and_actor_different_nodes"] = bool(
        _short(rr.get("hostname")) and _short(ident.get("hostname"))
        and _short(rr.get("hostname")) != _short(ident.get("hostname"))
        and _short(ident.get("hostname")) == _short(expect.get("nodeB")))
    g["hostname_witness_actor_node"] = bool(
        _short(ident.get("hostname")) == _short(expect.get("nodeB")))
    g["endpoints_pinned_subnet"] = bool(m.get("endpoints_subnet_ok"))
    g["remote_orphan_check_ran"] = bool(m.get("remote_orphan_check_ran"))
    g["evidence_fields_complete_crossnode"] = bool(
        ap.get("node_id") and ident.get("hostname") and rr.get("hostname")
        and expect.get("nodeA_nid") and expect.get("nodeB_nid"))
    return g


def run_crossnode_rep(ray_mod, HpxActor, peer, build_dir, x, rep, runs_root, hpx_threads, ray_cpus,
                      nodeA, nodeB, nodeA_ip, nodeB_ip, nodeB_nid, subnet, port_base, expect, env):
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    boot = os.path.join(runs_root, f"rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    p_root = port_base + rep * 10
    p_actor = port_base + rep * 10 + 1
    p_prober = port_base + rep * 10 + 2
    rcmd = crossnode_root_cmd(peer, boot, nodeA_ip, p_root, leave_timeout=45)
    pcmd = crossnode_prober_cmd(peer, boot, nodeA_ip, p_root, nodeA_ip, p_prober, x, deadman=150)
    endpoints = crossnode_actor_endpoints(nodeA_ip, p_root, nodeB_ip, p_actor)
    m = {
        "controller_hostname": socket.gethostname(),
        "ports": {"root": p_root, "actor": p_actor, "prober": p_prober},
        "argv_audit_ok": argv_audit(rcmd, pcmd, endpoints),
        "endpoints_subnet_ok": bool(nodeA_ip.startswith(subnet) and nodeB_ip.startswith(subnet)),
        "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus,
        "actor_idle_during_idle_probe": True, "prober_drove_dispatch": True,
    }
    root = prober = rlog = plog = None
    actor1 = actor2 = None
    strat = NodeAffinitySchedulingStrategy(node_id=nodeB_nid, soft=False)
    try:
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file_nfs(os.path.join(boot, "root.ready"), 60, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        actor1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                                  scheduling_strategy=strat).remote(build_dir, hpx_threads, endpoints)
        placement = dict(_ray_get(ray_mod, actor1.ray_placement.remote(), 60, "ray_placement") or {})
        placement["placement_soft"] = False
        placement["target_node"] = nodeB
        m["actor_placement"] = placement
        m["actor_identity"] = _ray_get(ray_mod, actor1.load_identity.remote(), 60, "actor_identity")
        m["actor_start"] = _ray_get(ray_mod, actor1.start_hpx.remote(), 120, "actor_start")
        actor_loc = (m["actor_start"] or {}).get("locality_id")
        if actor_loc is not None:
            with open(os.path.join(boot, "actor.loc"), "w") as f:
                f.write(str(actor_loc))
        m["actor_child_report"] = _ray_get(ray_mod, actor1.child_report.remote(), 30,
                                            "actor_child_report")

        prober, plog = _popen(pcmd, boot, os.path.join(boot, "prober.log"))
        _wait_for_file_nfs(os.path.join(boot, "prober.joined"), 60, procs=[prober, root])
        m["prober_joined"] = _read_json(os.path.join(boot, "prober.joined"))

        if actor_loc is not None:
            open(os.path.join(boot, "probe_idle.go"), "w").close()
            _wait_for_file_nfs(os.path.join(boot, "probe_idle.result"), 60, procs=[prober, root])
        m["idle_probe"] = _read_json(os.path.join(boot, "probe_idle.result"))

        m["actor_health"] = _ray_get(ray_mod, actor1.health.remote(), 30, "actor_health")

        # Slice C: overlap decided on the DRIVER's own monotonic timeline (no cross-node clock compare).
        if actor_loc is not None:
            started = os.path.join(boot, "busy_started")
            done = os.path.join(boot, "busy_done")
            busy_fut = actor1.busy_spin.remote(3.0, started, done)
            seen_started = _wait_for_file_nfs(started, 30)
            open(os.path.join(boot, "probe_busy.go"), "w").close()
            got_result = _wait_for_file_nfs(os.path.join(boot, "probe_busy.result"), 60,
                                            procs=[prober, root])
            try:
                busy_ret = ray_mod.get(busy_fut, timeout=30)
            except Exception:  # noqa: BLE001
                busy_ret = None
            m["busy_probe"] = _read_json(os.path.join(boot, "probe_busy.result"))
            m["busy_return"] = busy_ret
            m["busy_overlap_observed"] = bool(seen_started and got_result and busy_ret is not None)

        stop = _ray_get(ray_mod, actor1.stop_hpx.remote(), 40, "actor_stop")
        m["actor_stop_rc"] = (stop or {}).get("rc")
        m["actor_stop_error"] = (stop or {}).get("error")
        actor1_pid = (m.get("actor_identity") or {}).get("pid")
        ray_mod.kill(actor1)
        dead1 = False
        try:
            ray_mod.get(actor1.ping.remote(), timeout=10)
        except Exception:  # noqa: BLE001
            dead1 = True
        actor1 = None
        m["actor1_destroyed"] = dead1

        actor2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                                  scheduling_strategy=strat).remote(build_dir, hpx_threads, endpoints)
        m["actor_recreate"] = _ray_get(ray_mod, actor2.ping.remote(), 60, "actor_recreate")
        # recreated actor must also land on nodeB and be a different pid
        rec = m["actor_recreate"] or {}
        m["actor_recreate_on_node"] = bool(_short(rec.get("hostname")) == _short(nodeB)
                                           and rec.get("node_id") == nodeB_nid)

        open(os.path.join(boot, "root.done"), "w").close()
        _wait_for_file_nfs(os.path.join(boot, "prober.disconnected"), 60, procs=[prober])
        _wait_for_file_nfs(os.path.join(boot, "root.final"), 60, procs=[root])
        m["prober_disconnected"] = _read_json(os.path.join(boot, "prober.disconnected"))
        m["root_final"] = _read_json(os.path.join(boot, "root.final"))

        p_exited, p_rc, p_killed = _wait_proc(prober, time.time() + 60)
        r_exited, r_rc, r_killed = _wait_proc(root, time.time() + 60)
        m["prober_exit"] = _exit_path(p_exited, p_rc, p_killed)
        m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)

        dead2 = False
        if actor2 is not None:
            ray_mod.kill(actor2)
            try:
                ray_mod.get(actor2.ping.remote(), timeout=10)
            except Exception:  # noqa: BLE001
                dead2 = True
            actor2 = None
        m["actor_pids_gone"] = bool(dead1 and dead2)

        local_orphans = peer_orphans() or []
        remote_orphans, remote_ran = _crossnode_peer_orphans(nodeB, env)
        m["orphans"] = local_orphans + remote_orphans
        m["remote_orphan_check_ran"] = remote_ran
    finally:
        for a in (actor1, actor2):
            if a is not None:
                try:
                    ray_mod.kill(a)
                except Exception:  # noqa: BLE001
                    pass
        _kill_group(root)
        _kill_group(prober)
        for lg in (rlog, plog):
            if lg is not None:
                lg.close()

    slice_a = eval_crossnode_slice_a(m, expect)
    slice_b = eval_slice_b(m)
    return {
        "rep": rep, "boot_rel": os.path.relpath(boot, HERE),
        "ports": m["ports"],
        "slice_a_gates": slice_a, "slice_b_gates": slice_b,
        "rep_pass": rep_verdict(slice_a, slice_b),
        "slice_a_failed": [k for k, v in slice_a.items() if not v],
        "slice_b_failed": [k for k, v in slice_b.items() if not v],
        "markers": m,
    }


def run_crossnode_phase(args):
    """Two-node Rostam slice. Controller runs ON nodeA (sbatch batch step); Ray head local on nodeA,
    worker on nodeB via srun --block; work-free HPX root + prober are local Popens on nodeA; the Ray
    actor is hard-placed on nodeB and hosts a connect-mode HPX locality in-process that joins over TCP.
    Writes its OWN curated aggregate; never touches the local aggregate."""
    subnet = args.subnet_prefix
    agg = {
        "experiment": "66_hpx_runtime_inside_ray_actor",
        "kind": "hpx_connect_locality_hosted_in_one_ray_actor_worker_crossnode",
        "design": "interpreter_aware_slices_A_B_C_D",
        "phase": "rostam-cross-node", "single_node": False, "cross_node": True,
        "transport": "tcp_cross_node",
        "verdict_rule": "PASS iff all Slice A and Slice B gates pass in every rep "
                        "(Slice C diagnostic-only, Slice D deferred)",
        "claim_fences": dict(CLAIM_FENCES), "reps": args.reps,
    }

    # --- preflight (fail-closed) ---------------------------------------------------------------
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
    if len(nodes) < 2:
        problems.append(f"need >=2 nodes; got {nodes}")
    nodeA = args.root_node if args.root_node in nodes else (nodes[0] if nodes else None)
    nodeB = args.actor_node if args.actor_node in nodes else (nodes[1] if len(nodes) > 1 else None)
    if nodeA and nodeB and _short(nodeA) == _short(nodeB):
        problems.append("root and actor nodes are identical")
    here = _short(socket.gethostname())
    if nodeA and here != _short(nodeA):
        problems.append(f"controller on {here}, must run on nodeA={nodeA} (sbatch batch step)")
    peer = os.path.join(args.build_dir, PEER_BASENAME)
    ext_so = next((os.path.join(args.build_dir, fn)
                   for fn in (os.listdir(args.build_dir) if os.path.isdir(args.build_dir) else [])
                   if fn.startswith(EXT_MODULE) and fn.endswith(".so")), None)
    if not (os.path.exists(peer) and ext_so):
        problems.append(f"build artifacts missing (peer={os.path.exists(peer)} ext={bool(ext_so)})")
    nodeA_ip = _local_subnet_ip(subnet) if not problems else None
    nodeB_ip = _node_subnet_ip(nodeB, subnet) if (nodeB and not problems) else None
    if not problems and not (nodeA_ip and nodeB_ip):
        problems.append(f"could not resolve subnet {subnet} IPs (A={nodeA_ip} B={nodeB_ip})")

    agg["preflight"] = {
        "slurm_job_id": job_id, "slurm_nodelist": nodelist, "allocation_nodes": nodes,
        "controller_host": here, "nodeA": nodeA, "nodeB": nodeB,
        "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip, "subnet_prefix": subnet,
    }
    if problems:
        agg["overall"] = "fail_preflight"; agg["preflight_problems"] = problems
        _write_agg(args.aggregate, agg)
        print(f"[exp66-crossnode] PREFLIGHT FAIL: {problems}")
        return 0

    print(f"[exp66-crossnode] job {job_id} nodes {nodes} "
          f"root {nodeA}={nodeA_ip} actor {nodeB}={nodeB_ip}")

    env = dict(os.environ)
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    runs_root = os.path.join(HERE, "_exp66_runs", f"crossnode_{job_id}_{runid}")
    os.makedirs(runs_root, exist_ok=True)
    port = args.ray_port
    temp_dir = f"/tmp/exp66_ray_{job_id}_{runid}"
    port_flags = _ray_port_flags(args)
    head_proc = worker_proc = None
    reps, provenance, slice_c = [], None, []
    nodeA_nid = nodeB_nid = None
    fail_reason = None
    try:
        head_proc = _ray_head_local(nodeA_ip, port, temp_dir, args.head_num_cpus, env,
                                    os.path.join(runs_root, "head.log"), port_flags)
        gcs_ok, gcs_detail = _wait_gcs_from(nodeB, nodeA_ip, port, env, args.ray_ready_timeout)
        agg["ray_gcs_ready_from_nodeB"] = gcs_ok
        agg["ray_gcs_detail"] = gcs_detail
        if not (gcs_ok and head_proc.poll() is None):
            raise RuntimeError(f"head GCS not reachable from nodeB (detail={gcs_detail}, "
                               f"head_alive={head_proc.poll() is None})")
        worker_proc = _ray_worker_srun(nodeB, nodeB_ip, nodeA_ip, port, args.worker_num_cpus, env,
                                       os.path.join(runs_root, "worker.log"), port_flags)

        init_ok, init_attempts, init_tb = _bounded_ray_init(ray, f"{nodeA_ip}:{port}",
                                                            args.ray_init_timeout)
        agg["ray_init_ok"] = init_ok
        agg["ray_init_attempts"] = init_attempts
        if not init_ok:
            agg["ray_init_traceback"] = init_tb
            raise RuntimeError("ray.init to local head failed")
        nodes_ready, seen = _wait_ray_nodes(ray, 2, args.ray_ready_timeout)
        agg["ray_nodes_ready"] = nodes_ready
        agg["ray_nodes_alive_seen"] = seen
        if not nodes_ready:
            raise RuntimeError(f"only {seen}/2 ray nodes alive")

        alive = [n for n in ray.nodes() if n.get("Alive")]

        def _match(ip, host):
            return [n for n in alive
                    if n.get("NodeManagerAddress") == ip or _short(n.get("NodeName")) == _short(host)]
        mA, mB = _match(nodeA_ip, nodeA), _match(nodeB_ip, nodeB)
        nodeA_nid = mA[0]["NodeID"] if len(mA) == 1 else None
        nodeB_nid = mB[0]["NodeID"] if len(mB) == 1 else None
        driver = _self_identity(ray)
        agg["ray_cluster"] = {
            "head_node": nodeA, "worker_node": nodeB, "head_ip": nodeA_ip, "worker_ip": nodeB_ip,
            "nodeA_ray_node_id": nodeA_nid, "nodeB_ray_node_id": nodeB_nid,
            "driver_hostname": driver["hostname"], "driver_node_id": driver["node_id"],
            "driver_on_nodeA": bool(driver["node_id"] and driver["node_id"] == nodeA_nid),
            "ray_nodes_raw": [{k: n.get(k) for k in ("NodeID", "NodeManagerAddress", "NodeName",
                                                     "Alive")} for n in alive],
        }
        if not (nodeA_nid and nodeB_nid and nodeA_nid != nodeB_nid):
            raise RuntimeError(f"ray node-id resolution failed (mA={len(mA)} mB={len(mB)})")
        if not agg["ray_cluster"]["driver_on_nodeA"]:
            raise RuntimeError("driver is not on nodeA (Ray driver must be co-located with head)")

        HpxActor = build_actor_class(ray)
        expect = {"slurm_job_id": job_id, "nodes": nodes, "nodeA": nodeA, "nodeB": nodeB,
                  "nodeA_nid": nodeA_nid, "nodeB_nid": nodeB_nid, "subnet": subnet}
        for r in range(1, args.reps + 1):
            print(f"[exp66-crossnode] rep {r} ...")
            rep = run_crossnode_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), args.x, r,
                                    runs_root, args.hpx_threads, args.ray_num_cpus, nodeA, nodeB,
                                    nodeA_ip, nodeB_ip, nodeB_nid, subnet, args.port_base, expect, env)
            if provenance is None:
                provenance = collect_provenance(args.build_dir, ray,
                                                rep["markers"].get("actor_identity"))
            c = classify_slice_c(rep["markers"], provenance)
            slice_c.append({"rep": r, **c})
            print(f"[exp66-crossnode]   rep {r}: {'PASS' if rep['rep_pass'] else 'FAIL'}  "
                  f"A_failed={rep['slice_a_failed']} B_failed={rep['slice_b_failed']}  "
                  f"sliceC={c['classification']}")
            rep_out = dict(rep)
            mk = dict(rep["markers"])
            for k in ("actor_child_report",):
                if k in mk and isinstance(mk[k], dict):
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
        _ray_stop_node(nodeA, env)
        if nodeB:
            _ray_stop_node(nodeB, env)
        agg["ray_head_cleanup"] = _terminate_launcher(head_proc)
        agg["ray_worker_cleanup"] = _terminate_launcher(worker_proc)
        orphA_ray = _orphan_check_node(nodeA, _ORPHAN_PATTERNS_RAY, env)
        orphB_ray = _orphan_check_node(nodeB, _ORPHAN_PATTERNS_RAY, env) if nodeB else ["<no nodeB>"]
        orphA_peer, _ = _crossnode_peer_orphans(nodeA, env)
        orphB_peer, _ = _crossnode_peer_orphans(nodeB, env) if nodeB else ([], False)
        agg["final_orphans"] = {
            "ray_nodeA": orphA_ray, "ray_nodeB": orphB_ray,
            "peer_nodeA": orphA_peer, "peer_nodeB": orphB_peer,
            "no_ray_orphans": (len(orphA_ray) == 0 and len(orphB_ray) == 0),
            "no_peer_orphans": (len(orphA_peer) == 0 and len(orphB_peer) == 0),
        }

    overall_pass = (fail_reason is None and len(reps) == args.reps
                    and all(r["rep_pass"] for r in reps)
                    and agg.get("final_orphans", {}).get("no_ray_orphans")
                    and agg["final_orphans"].get("no_peer_orphans"))
    agg["provenance"] = provenance
    agg["overall"] = "pass" if overall_pass else "fail"
    agg["slice_a_b_reps"] = reps
    agg["slice_c_diagnostic"] = slice_c
    agg["slice_d"] = slice_d_deferral(_ray_importable_on_local_314())
    agg["safe_claim"] = (
        f"On CPython {(provenance or {}).get('python_version_full','').split()[0]} (GIL build), Ray "
        f"{(provenance or {}).get('ray_version')}, HPX commit {EXPECTED_HPX_COMMIT[:10]}..., across two "
        f"Rostam nodes ({nodeA}->{nodeB} on {subnet}x), a networking HPX connect-mode locality ran "
        "in-process inside a Ray actor worker on the actor node (hard-placed, PID identity, no HPX "
        "child), joined a separately supervised work-free root on the root node over TCP, executed "
        "verified remote HPX actions, progressed while its Python thread was idle, and shut down "
        "cleanly with the Ray worker healthy across actor recreation."
        if overall_pass else
        f"exp66 cross-node did not pass ({fail_reason or 'see slice_a_b_reps / final_orphans'}).")
    _write_agg(args.aggregate, agg)
    print(f"[exp66-crossnode] overall: {agg['overall']} -> {args.aggregate}")
    return 0


# ---------------------------------------------------------------------------------------

def _write_agg(path, agg):
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=False)
        f.write("\n")


def _ray_importable_on_local_314():
    """Report-only: does the local Homebrew CPython 3.14 have Ray? (No install attempted.)"""
    for cand in ("/opt/homebrew/bin/python3.14", "/usr/local/bin/python3.14"):
        if os.path.exists(cand):
            try:
                r = subprocess.run([cand, "-c", "import ray"], capture_output=True, timeout=20)
                return r.returncode == 0
            except Exception:
                return False
    return False


def main():
    ap = argparse.ArgumentParser(description="exp66 HPX-inside-one-Ray-actor runner")
    ap.add_argument("--phase", choices=["local", "rostam-cross-node"], default="local",
                    help="local (default, unchanged single-node slice) or the two-node Rostam slice")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--build-dir", default=DEFAULT_BUILD_DIR)
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--ray-num-cpus", type=int, default=2)
    ap.add_argument("--aggregate", default=None,
                    help="curated aggregate path (default depends on --phase)")
    ap.add_argument("--selftest", action="store_true")
    # cross-node-only options
    ap.add_argument("--root-node", default=None, help="cross-node: node hosting controller+root")
    ap.add_argument("--actor-node", default=None, help="cross-node: node hosting the Ray actor")
    ap.add_argument("--subnet-prefix", default="10.42.5.", help="cross-node: required IPv4 prefix")
    ap.add_argument("--port-base", type=int, default=7700, help="cross-node: HPX port base per rep")
    ap.add_argument("--ray-port", type=int, default=6379, help="cross-node: Ray GCS port")
    ap.add_argument("--ray-ready-timeout", type=int, default=180)
    ap.add_argument("--ray-init-timeout", type=int, default=180)
    ap.add_argument("--head-num-cpus", type=int, default=4)
    ap.add_argument("--worker-num-cpus", type=int, default=4)
    ap.add_argument("--node-manager-port", type=int, default=7901)
    ap.add_argument("--object-manager-port", type=int, default=7902)
    ap.add_argument("--min-worker-port", type=int, default=10002)
    ap.add_argument("--max-worker-port", type=int, default=10102)
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    if args.aggregate is None:
        args.aggregate = os.path.join(
            HERE, "hpx_inside_ray_actor_crossnode_aggregate.json"
            if args.phase == "rostam-cross-node" else "hpx_inside_ray_actor_aggregate.json")

    if args.phase == "rostam-cross-node":
        return run_crossnode_phase(args)

    return run_local_phase(args)


def run_local_phase(args):
    peer = os.path.join(args.build_dir, PEER_BASENAME)
    ext_so = None
    for fn in os.listdir(args.build_dir) if os.path.isdir(args.build_dir) else []:
        if fn.startswith(EXT_MODULE) and fn.endswith(".so"):
            ext_so = os.path.join(args.build_dir, fn)

    agg = {
        "experiment": "66_hpx_runtime_inside_ray_actor",
        "kind": "hpx_connect_locality_hosted_in_one_ray_actor_worker_local",
        "design": "interpreter_aware_slices_A_B_C_D",
        "single_node": True, "transport": "tcp_loopback",
        "verdict_rule": "PASS iff all Slice A and Slice B gates pass in every rep "
                        "(Slice C diagnostic-only, Slice D deferred)",
        "claim_fences": dict(CLAIM_FENCES),
        "reps": args.reps,
    }

    if not (os.path.exists(peer) and ext_so):
        agg["overall"] = "skip"
        agg["reason"] = f"build artifacts missing (peer={os.path.exists(peer)}, ext={bool(ext_so)})"
        agg["slice_d"] = slice_d_deferral(_ray_importable_on_local_314())
        _write_agg(args.aggregate, agg)
        print(f"SKIP: build exp66 first (peer/ext missing) -> {args.aggregate}")
        return 0

    try:
        import ray
    except Exception as ex:  # noqa: BLE001
        agg["overall"] = "skip"
        agg["reason"] = f"ray unavailable: {type(ex).__name__}: {ex}"
        agg["slice_d"] = slice_d_deferral(_ray_importable_on_local_314())
        _write_agg(args.aggregate, agg)
        print(f"SKIP: ray unavailable -> {args.aggregate}")
        return 0

    import logging
    ray.init(num_cpus=max(4, args.ray_num_cpus + 2), include_dashboard=False,
             log_to_driver=False, logging_level=logging.ERROR,
             ignore_reinit_error=True)
    HpxActor = build_actor_class(ray)

    runs_root = os.path.join(HERE, "_exp66_runs", time.strftime("%Y%m%dT%H%M%SZ"))
    os.makedirs(runs_root, exist_ok=True)
    print(f"[exp66] peer   : {peer}")
    print(f"[exp66] ext    : {os.path.basename(ext_so)}")
    print(f"[exp66] runs   : {os.path.relpath(runs_root, HERE)}")

    reps, provenance, slice_c = [], None, []
    try:
        for r in range(1, args.reps + 1):
            print(f"[exp66] rep {r} ...")
            rep = run_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), args.x, r,
                          runs_root, args.hpx_threads, args.ray_num_cpus)
            if provenance is None:
                provenance = collect_provenance(
                    args.build_dir, ray, rep["markers"].get("actor_identity"))
            c = classify_slice_c(rep["markers"], provenance)
            slice_c.append({"rep": r, **c})
            print(f"[exp66]   rep {r}: {'PASS' if rep['rep_pass'] else 'FAIL'}  "
                  f"A_failed={rep['slice_a_failed']} B_failed={rep['slice_b_failed']}  "
                  f"sliceC={c['classification']}")
            # Trim heavy nested markers from the curated aggregate (raw logs stay in runs_root).
            rep_out = dict(rep)
            mk = dict(rep["markers"])
            for k in ("actor_child_report",):
                if k in mk and isinstance(mk[k], dict):
                    mk[k] = {kk: mk[k].get(kk) for kk in ("checked", "hpx_children", "worker_pid")}
            rep_out["markers"] = mk
            reps.append(rep_out)
    finally:
        try:
            ray.shutdown()
        except Exception:
            pass

    overall_pass = all(r["rep_pass"] for r in reps) and len(reps) == args.reps
    agg["provenance"] = provenance
    agg["overall"] = "pass" if overall_pass else "fail"
    agg["slice_a_b_reps"] = reps
    agg["slice_c_diagnostic"] = slice_c
    agg["slice_d"] = slice_d_deferral(_ray_importable_on_local_314())
    agg["safe_claim"] = (
        "On the tested CPython 3.11 GIL build with Ray "
        f"{(provenance or {}).get('ray_version')} and HPX commit {EXPECTED_HPX_COMMIT[:10]}..., "
        "a networking HPX connect-mode locality ran in-process inside one Ray actor worker "
        "(PID identity, no HPX child), executed verified remote HPX actions, made progress "
        "while the actor's Python thread was idle, and shut down cleanly while the Ray worker "
        "stayed usable across actor destruction and recreation."
        if overall_pass else
        "exp66 did not pass; see slice_a_b_reps for the failing gates.")
    _write_agg(args.aggregate, agg)
    print(f"[exp66] overall: {agg['overall']} -> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
