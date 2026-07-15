#!/usr/bin/env python3
"""exp65 -- orchestrator for the demand-triggered connect-mode admission spike.

STRUCTURAL MECHANISM EVIDENCE ONLY. Single node, loopback TCP, Ray-free. Drives the
standalone ``demand_admission_spike`` binary (built from this directory's CMakeLists.txt)
through two arms, each repeated (default 3x):

  demand arm    : root starts ALONE (no --hpx:localities, no expected connector count),
                  proves K local oracle calls plus liveness ticks while ZERO connectors
                  exist, dwells; the controller then records demand.issued and ONLY AFTER
                  THAT launches one connect-mode connector; the root discovers it by
                  membership set-difference, serves ONE bounded oracle-verified remote
                  action, signals completion (root.done), observes the graceful leave
                  (post(disconnect)+stop), runs one more local oracle action, finalizes.
  no-demand arm : identical root; the controller NEVER launches a connector and instead
                  writes no_demand.finish after the dwell; the root must finalize cleanly
                  with zero connectors ever joining.

The controller is this plain-Python script -- NOT Ray. Lifetime contract is exp63's:
root.alive heartbeat + root.done completion sentinel; the connector serve-timeout is a
DEADMAN only, never the normal release path.

CLAIM FENCE (see demand_triggered_admission.md): no Ray / Ray actors; no
HPX-inside-Ray-worker result; no elasticity-under-load; no concurrent membership churn; no
failure-recovery result; no lazy-TCP-socket-establishment result; no performance
comparison; no multi-node claim. All recorded durations are OBSERVATIONAL ONLY and never
participate in pass/fail gates.

Usage:
  python run_exp65_demand_admission.py [--binary <path>] [--reps 3] [--dwell 3.0]
                                       [--x 7] [--aggregate <path>]
  python run_exp65_demand_admission.py --phase rostam-cross-node   # two-node Slurm slice
  python run_exp65_demand_admission.py --selftest   # pure-Python gate-logic checks

The default --phase local is the original single-node loopback experiment, unchanged. The
rostam-cross-node phase is a SEPARATE slice: controller+root on --root-node (medusa00),
connector srun-launched on --connector-node (medusa01) strictly after the demand event,
explicit numeric endpoints pinned to --subnet-prefix (10.42.5.), placement attested by
per-marker hostnames, and its own curated aggregate
(demand_admission_crossnode_aggregate.json). Same fences plus a two-node-only scope fence;
still not HPX-inside-Ray-actors, not elasticity, not lazy-TCP-socket evidence.

Exit code is 0 even when gates fail (the aggregate carries the verdict); non-zero only on
an orchestrator-internal error (or selftest failure).
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
BINARY_BASENAME = "demand_admission_spike"
DEFAULT_BINARY_CANDIDATES = [
    os.path.join(HERE, "build", BINARY_BASENAME),
    os.path.join(HERE, "build", "Release", BINARY_BASENAME),
]

# Structural facts about THIS experiment's launch model, recorded verbatim in the aggregate.
STRUCTURAL_FLAGS = {
    "static_locality_count_used": False,        # no --hpx:localities=N anywhere (audited)
    "expected_connector_count_used": False,     # no count parameter exists in binary/runner
    "admission_intent_flag_required": True,     # --hpx:expect-connecting-localities (exp49)
}

CLAIM_FENCES = {
    "no_ray_or_ray_actors": True,
    "no_hpx_inside_ray_worker_result": True,
    "no_elasticity_under_load_result": True,
    "no_concurrent_membership_churn": True,
    "no_failure_recovery_result": True,
    "no_lazy_tcp_socket_establishment_result": True,
    "no_performance_comparison": True,
    "no_multi_node_claim": True,
    "durations_observational_only": True,
}

# Cross-node slice fences (--phase rostam-cross-node): the scope widens from loopback to
# exactly TWO real nodes over the TCP parcelport -- nothing else. The loopback fences that
# survive are restated; `no_multi_node_claim` is replaced by the two-node-only scope fence.
CROSSNODE_CLAIM_FENCES = {
    "no_ray_or_ray_actors": True,
    "no_hpx_inside_ray_worker_result": True,
    "no_ray_autoscaling_result": True,
    "no_elasticity_under_load_result": True,
    "no_concurrent_membership_churn": True,
    "no_failure_recovery_result": True,
    "no_lazy_tcp_socket_establishment_result": True,
    "no_performance_comparison": True,
    "cross_node_two_node_scope_only": True,
    "no_general_hpx_claim": True,
    "durations_observational_only": True,
}


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
    """Wait until the absolute deadline. Returns (exited, returncode, killed)."""
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


def _wait_for_file(path, proc, timeout, revalidate_dir=False):
    """Poll until `path` exists or `proc` dies. `revalidate_dir=True` lists the parent
    directory before each existence check so NFS negative-dentry caching cannot hide a
    file written from another node (cross-node phase); the local loopback phase keeps the
    plain local-FS behavior."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if revalidate_dir:
            try:
                os.listdir(os.path.dirname(path) or ".")
            except OSError:
                pass
        if os.path.exists(path):
            return True
        if proc is not None and proc.poll() is not None:
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
    """exp50-style exit-path classification."""
    if killed:
        return "hung_killed_by_harness"
    if exited and rc == 0:
        return "finalized_clean"
    if exited:
        return "self_terminated_nonzero"
    return "unknown"


def orphan_pids():
    """PIDs of any leftover spike processes (matched on the build-path binary string)."""
    try:
        out = subprocess.run(["pgrep", "-f", "build/" + BINARY_BASENAME],
                             capture_output=True, text=True, timeout=10)
    except Exception:
        return None
    if out.returncode not in (0, 1):  # 1 == no matches (clean), not an error
        return None
    return [p for p in out.stdout.split() if p]


def argv_audit(*cmds):
    """True iff NO argv contains a static locality count or any expected-count flag."""
    for cmd in cmds:
        for a in cmd:
            if a.startswith("--hpx:localities"):
                return False
            if "expected" in a and "count" in a:
                return False
    return True


# ---------------------------------------------------------------------------------------
# Cross-node (Rostam/Slurm) helpers -- used only by --phase rostam-cross-node
# ---------------------------------------------------------------------------------------

def short_host(h):
    """FQDN/short-hostname normalization (exp59 lesson)."""
    return (h or "").split(".", 1)[0].strip().lower()


def _sh(cmd, timeout=60):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except Exception as e:  # noqa: BLE001 -- classify, never crash the orchestrator
        return -1, "", f"{type(e).__name__}: {e}"


def subnet_ip_local(prefix):
    """First local IPv4 on the preferred subnet, via `hostname -I` (Linux)."""
    rc, out, _ = _sh(["hostname", "-I"], timeout=15)
    if rc != 0:
        return None
    for tok in out.split():
        if tok.startswith(prefix):
            return tok
    return None


def subnet_ip_of_node(node, prefix):
    """First IPv4 of `node` on the preferred subnet, via `srun --overlap hostname -I`
    (the exp64 pattern). None on miss."""
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", f"--nodelist={node}",
                      "hostname", "-I"], timeout=90)
    if rc != 0:
        return None
    for tok in out.split():
        if tok.startswith(prefix):
            return tok
    return None


def remote_clock_skew_observational(node):
    """Coarse remote-minus-local wall-clock estimate (ms). OBSERVATIONAL ONLY -- srun
    launch latency dominates the bracket, so this is a sanity record, never a gate."""
    t0 = time.time()
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", f"--nodelist={node}",
                      "date", "+%s%3N"], timeout=90)
    t1 = time.time()
    if rc != 0 or not out.strip():
        return None
    try:
        remote_ms = int(out.strip().splitlines()[-1])
    except ValueError:
        return None
    return {"remote_minus_local_ms_midpoint_estimate": remote_ms - int(((t0 + t1) / 2) * 1000),
            "bracket_ms": int((t1 - t0) * 1000),
            "note": "observational only; srun launch latency dominates the bracket"}


def crossnode_preflight(root_node, conn_node, prefix):
    """Fail-closed environment facts for the cross-node phase. Returns (facts, problems)."""
    facts = {
        "slurm_job_id": os.environ.get("SLURM_JOB_ID") or "",
        "slurm_nodelist_raw": os.environ.get("SLURM_JOB_NODELIST") or "",
        "controller_host": short_host(socket.gethostname()),
        "root_node": root_node, "connector_node": conn_node, "subnet_prefix": prefix,
    }
    problems = []
    if not facts["slurm_job_id"]:
        problems.append("SLURM_JOB_ID is empty (not inside a Slurm allocation)")
    hosts = []
    if facts["slurm_nodelist_raw"]:
        rc, out, err = _sh(["scontrol", "show", "hostnames", facts["slurm_nodelist_raw"]], 30)
        if rc == 0:
            hosts = [short_host(h) for h in out.split()]
        else:
            problems.append(f"scontrol show hostnames failed: {err.strip()}")
    else:
        problems.append("SLURM_JOB_NODELIST is empty")
    facts["allocation_hostnames"] = hosts
    for n in (root_node, conn_node):
        if short_host(n) not in hosts:
            problems.append(f"{n} not in allocation {hosts}")
    if facts["controller_host"] != short_host(root_node):
        problems.append(f"controller on {facts['controller_host']}, expected {root_node} "
                        "(root+controller must share the root node for same-clock ordering)")
    facts["root_subnet_ip"] = subnet_ip_local(prefix)
    if not facts["root_subnet_ip"]:
        problems.append(f"no local IPv4 on subnet {prefix} (root node)")
    facts["connector_subnet_ip"] = subnet_ip_of_node(conn_node, prefix)
    if not facts["connector_subnet_ip"]:
        problems.append(f"no IPv4 on subnet {prefix} on {conn_node}")
    facts["clock_skew_observational"] = remote_clock_skew_observational(conn_node)
    return facts, problems


def crossnode_orphans(conn_node):
    """Combined local + remote leftover-spike check. Remote side uses `pgrep -af` and
    filters the detector's own srun/pgrep argv (the exp64 orphan-detector pattern).
    Returns (orphans_list_or_None, remote_check_ran)."""
    local = orphan_pids()
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", f"--nodelist={conn_node}",
                      "pgrep", "-af", "build/" + BINARY_BASENAME], timeout=90)
    if rc not in (0, 1):
        return (local, False)
    remote = []
    for ln in out.splitlines():
        if ln.strip() and "pgrep" not in ln and "srun" not in ln:
            remote.append(f"{short_host(conn_node)}:{ln.split()[0]}")
    if local is None:
        return (None, True)
    return (local + remote, True)


# ---------------------------------------------------------------------------------------
# Gate evaluation (pure functions -> selftestable without the binary)
# ---------------------------------------------------------------------------------------

def eval_demand_gates(m):
    """m: marker/fact dict for one demand-arm rep. Returns {gate_name: bool}. Durations are
    deliberately NOT inputs to any gate."""
    ready = m.get("root_ready") or {}
    local = m.get("local_phase") or {}
    demand = m.get("demand") or {}
    spawned = m.get("connector_spawned") or {}
    joined = m.get("connector_joined") or {}
    adm = m.get("admission") or {}
    served = m.get("served") or {}
    disc = m.get("connector_disconnected") or {}
    leave = m.get("leave_observed") or {}
    fin = m.get("final_local") or {}
    root_res = m.get("root_result") or {}

    demand_wall = demand.get("wall_ms")
    spawn_wall = spawned.get("wall_ms")
    local_wall = local.get("wall_ms")
    adm_wall = adm.get("wall_ms")

    g = {}
    g["root_started_alone"] = (ready.get("membership_count") == 1
                               and ready.get("membership_ids") == [0])
    g["local_progress_before_demand"] = bool(
        local.get("all_local_ok")
        and local.get("membership_count") == 1
        and local_wall is not None and demand_wall is not None
        and local_wall < demand_wall
        and adm.get("liveness_ok_before_admission", 0) >= 1
        and adm.get("first_liveness_wall_ms", 0) > 0
        and adm.get("first_liveness_wall_ms", 0) < demand_wall)
    g["connector_absent_before_demand"] = bool(
        demand_wall is not None and spawn_wall is not None
        and demand_wall < spawn_wall
        and not m.get("connector_launched_before_demand", False))
    g["demand_precedes_creation_and_admission"] = bool(
        demand_wall is not None and spawn_wall is not None and adm_wall is not None
        and demand_wall < spawn_wall < adm_wall + 1)  # +1ms tolerance: same-ms spawn/adm
    g["discovery_without_count_gate"] = bool(
        adm.get("new_locality_id") is not None
        and adm.get("baseline_ids") == [0]
        and adm.get("new_locality_id") not in (adm.get("baseline_ids") or [])
        and joined.get("locality_id") == adm.get("new_locality_id"))
    g["remote_oracle_on_connector"] = bool(
        served.get("dispatch_status") == "returned"
        and served.get("match") and served.get("proved_remote")
        and served.get("executed_on_locality") == adm.get("new_locality_id")
        and served.get("executed_on_locality") != 0)
    g["connector_left_gracefully"] = bool(
        disc.get("clean")
        and disc.get("shutdown_reason") == "root_completion_signal"
        and m.get("connector_exited") and not m.get("connector_killed"))
    g["root_operational_after_leave"] = bool(
        leave.get("observed") and fin.get("ok"))
    g["clean_exits"] = bool(
        m.get("root_exit_path") == "finalized_clean"
        and m.get("connector_exited") and m.get("connector_rc") == 0
        and not m.get("root_killed") and not m.get("connector_killed"))
    g["no_orphans"] = m.get("orphans") == []
    g["argv_no_static_localities"] = bool(m.get("argv_audit_ok"))
    g["no_expected_count_gate"] = bool(
        root_res.get("remote_work_started_before_admission") is False
        and not m.get("connector_launched_before_demand", False)
        and m.get("argv_audit_ok"))
    return g


def eval_no_demand_gates(m):
    ready = m.get("root_ready") or {}
    local = m.get("local_phase") or {}
    fin = m.get("final_local") or {}
    root_res = m.get("root_result") or {}

    g = {}
    g["root_started_alone"] = (ready.get("membership_count") == 1
                               and ready.get("membership_ids") == [0])
    g["local_progress_alone"] = bool(
        local.get("all_local_ok") and local.get("membership_count") == 1)
    g["liveness_during_dwell"] = root_res.get("liveness_ok", 0) >= 1
    g["zero_connectors_ever"] = bool(
        root_res.get("max_localities_ever") == 1
        and root_res.get("admission_detected") is False
        and root_res.get("no_demand_finish_seen") is True)
    g["root_finalized_clean_alone"] = bool(
        m.get("root_exit_path") == "finalized_clean" and fin.get("ok"))
    g["no_orphans"] = m.get("orphans") == []
    g["argv_no_static_localities"] = bool(m.get("argv_audit_ok"))
    return g


def eval_crossnode_demand_gates(m, expect):
    """Cross-node demand-arm gates. Reuses the loopback structural gates, then (a) replaces
    the two orderings that would otherwise compare wall clocks ACROSS nodes with same-clock
    versions (controller + root share the root node; the connector process cannot exist
    before its srun Popen), and (b) adds placement / subnet / Slurm / completeness gates.
    `expect`: {root_node, conn_node, root_ip, conn_ip, subnet_prefix, slurm_job_id,
    allocation_hostnames}. Durations are still never gate inputs."""
    g = eval_demand_gates(m)
    demand_wall = (m.get("demand") or {}).get("wall_ms")
    popen_wall = m.get("controller_popen_wall_ms")
    adm_wall = (m.get("admission") or {}).get("wall_ms")
    spawned = m.get("connector_spawned") or {}

    # Same-clock replacements (controller clock == root-node clock). The spawn marker must
    # still exist (process attestation), but its wall_ms (connector-node clock) is not
    # compared against a root-node clock.
    g["connector_absent_before_demand"] = bool(
        demand_wall is not None and popen_wall is not None
        and demand_wall <= popen_wall
        and spawned.get("wall_ms") is not None
        and not m.get("connector_launched_before_demand", False))
    g["demand_precedes_creation_and_admission"] = bool(
        demand_wall is not None and popen_wall is not None and adm_wall is not None
        and demand_wall <= popen_wall < adm_wall + 1)

    ready = m.get("root_ready") or {}
    joined = m.get("connector_joined") or {}
    rh = short_host(ready.get("host"))
    sh = short_host(spawned.get("host"))
    jh = short_host(joined.get("host"))
    g["root_on_root_node"] = bool(rh and rh == short_host(expect.get("root_node")))
    g["connector_on_connector_node"] = bool(
        sh and sh == short_host(expect.get("conn_node")) and jh == sh)
    g["cross_node_placement"] = bool(rh and sh and rh != sh)
    prefix = expect.get("subnet_prefix") or ""
    g["subnet_pinned_both_endpoints"] = bool(
        prefix
        and (expect.get("root_ip") or "").startswith(prefix)
        and (expect.get("conn_ip") or "").startswith(prefix)
        and m.get("endpoints_in_argv_ok"))
    g["slurm_allocation_recorded"] = bool(
        expect.get("slurm_job_id")
        and short_host(expect.get("root_node")) in (expect.get("allocation_hostnames") or [])
        and short_host(expect.get("conn_node")) in (expect.get("allocation_hostnames") or []))
    g["remote_orphan_check_ran"] = bool(m.get("remote_orphan_check_ran"))
    g["evidence_fields_complete"] = bool(
        ready.get("host") and ready.get("pid")
        and spawned.get("host") and spawned.get("pid")
        and joined.get("host") and joined.get("locality_id") is not None
        and (m.get("admission") or {}).get("new_locality_id") is not None
        and (m.get("served") or {}).get("dispatch_status") is not None
        and (m.get("root_result") or {}).get("here_locality") == 0)
    return g


def eval_crossnode_no_demand_gates(m, expect):
    """Cross-node no-demand control gates: loopback control gates plus root placement,
    subnet pinning, Slurm recording, remote orphan coverage, and field completeness."""
    g = eval_no_demand_gates(m)
    ready = m.get("root_ready") or {}
    rh = short_host(ready.get("host"))
    prefix = expect.get("subnet_prefix") or ""
    g["root_on_root_node"] = bool(rh and rh == short_host(expect.get("root_node")))
    g["subnet_pinned_root_endpoint"] = bool(
        prefix and (expect.get("root_ip") or "").startswith(prefix)
        and m.get("endpoints_in_argv_ok"))
    g["slurm_allocation_recorded"] = bool(
        expect.get("slurm_job_id")
        and short_host(expect.get("root_node")) in (expect.get("allocation_hostnames") or [])
        and short_host(expect.get("conn_node")) in (expect.get("allocation_hostnames") or []))
    g["remote_orphan_check_ran"] = bool(m.get("remote_orphan_check_ran"))
    g["evidence_fields_complete"] = bool(
        ready.get("host") and ready.get("pid")
        and (m.get("root_result") or {}).get("max_localities_ever") == 1)
    return g


# ---------------------------------------------------------------------------------------
# Arms
# ---------------------------------------------------------------------------------------

def root_cmd(binary, arm, bootdir, x, p0, dwell, wait_probe="sliced"):
    # --hpx:threads=2 (exp50 correction: waits/serves/polling must not contend on one
    # worker); --hpx:bind=none (macOS warns+ignores; harmless); --hpx:ignore-batch-env
    # (exp52/exp61 lesson: never let batch-env autodetect override explicit endpoints);
    # numeric 127.0.0.1 always. NO --hpx:localities. NO expected-count option exists.
    # wait_probe: "sliced" keeps the exp65 dispatch-wait semantics unchanged;
    # "full_bound_instrumented" is the waiter-fix verification re-check hook only.
    finish_timeout = max(15, int(dwell) + 12)
    return [
        binary, "--role", "root", "--arm", arm, "--bootstrap", bootdir, "--x", str(x),
        "--local-k", "5", "--admit-timeout", "60", "--finish-timeout", str(finish_timeout),
        "--leave-timeout", "20", "--dispatch-bound", "15", "--tick-ms", "200",
        "--wait-probe", wait_probe,
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p0}",
        "--hpx:expect-connecting-localities",
        "--hpx:threads=2", "--hpx:bind=none", "--hpx:ignore-batch-env",
    ]


def connector_cmd(binary, bootdir, p0, p1):
    return [
        binary, "--role", "connector", "--bootstrap", bootdir, "--serve-timeout", "30",
        f"--hpx:agas=127.0.0.1:{p0}", f"--hpx:hpx=127.0.0.1:{p1}",
        "--hpx:threads=1", "--hpx:bind=none", "--hpx:ignore-batch-env",
    ]


def collect_root_markers(bootdir):
    return {
        "root_ready": _read_json(os.path.join(bootdir, "root.ready")),
        "local_phase": _read_json(os.path.join(bootdir, "local_phase.done")),
        "admission": _read_json(os.path.join(bootdir, "admission_detected")),
        "served": _read_json(os.path.join(bootdir, "served")),
        "leave_observed": _read_json(os.path.join(bootdir, "leave_observed")),
        "final_local": _read_json(os.path.join(bootdir, "final_local.done")),
        "root_result": _read_json(os.path.join(bootdir, "root_result.json")),
    }


def run_demand_rep(binary, x, rep, dwell, wait_probe="sliced"):
    bootdir = tempfile.mkdtemp(prefix=f"exp65_demand_r{rep}_")
    p0 = find_free_port()
    p1 = find_free_port()
    rcmd = root_cmd(binary, "demand", bootdir, x, p0, dwell, wait_probe)
    ccmd = connector_cmd(binary, bootdir, p0, p1)
    audit_ok = argv_audit(rcmd, ccmd)

    root, rlog = _popen(rcmd, bootdir, os.path.join(bootdir, "root.log"))
    ready = _wait_for_file(os.path.join(bootdir, "root.ready"), root, 30)
    local_done = ready and _wait_for_file(
        os.path.join(bootdir, "local_phase.done"), root, 30)

    # Deliberate zero-connector dwell: the connector process DOES NOT EXIST yet.
    time.sleep(dwell)

    # Demand event: recorded BEFORE the connector process is created. The ordering
    # demand.issued < connector Popen < connector.spawned is the load-bearing proof.
    demand = {"wall_ms": int(time.time() * 1000), "monotonic_s": time.monotonic()}
    with open(os.path.join(bootdir, "demand.issued"), "w") as f:
        json.dump(demand, f)
    popen_wall_ms = None
    conn = clog = None
    c_exited = c_killed = False
    c_rc = None
    if local_done:
        popen_wall_ms = int(time.time() * 1000)
        conn, clog = _popen(ccmd, bootdir, os.path.join(bootdir, "connector.log"))

    # Bounded waits for the rest of the lifecycle; classify, never hang.
    if conn is not None:
        _wait_for_file(os.path.join(bootdir, "served"), root, 60)
        c_exited, c_rc, c_killed = _wait(conn, time.time() + 60)
        clog.close()
    r_exited, r_rc, r_killed = _wait(root, time.time() + 60)
    rlog.close()

    m = collect_root_markers(bootdir)
    m["demand"] = demand
    m["controller_popen_wall_ms"] = popen_wall_ms
    m["connector_spawned"] = _read_json(os.path.join(bootdir, "connector.spawned"))
    m["connector_joined"] = _read_json(os.path.join(bootdir, "connector.joined"))
    m["connector_disconnected"] = _read_json(
        os.path.join(bootdir, "connector.disconnected"))
    m["connector_launched_before_demand"] = False  # structural: Popen strictly after demand
    m["connector_exited"] = c_exited
    m["connector_rc"] = c_rc
    m["connector_killed"] = c_killed
    m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)
    m["root_rc"] = r_rc
    m["root_killed"] = r_killed
    m["argv_audit_ok"] = audit_ok
    m["orphans"] = orphan_pids()

    gates = eval_demand_gates(m)

    spawned = m.get("connector_spawned") or {}
    adm = m.get("admission") or {}
    served = m.get("served") or {}
    durations = {  # OBSERVATIONAL ONLY -- never gate inputs
        "demand_to_spawn_ms": (spawned.get("wall_ms") - demand["wall_ms"])
        if spawned.get("wall_ms") else None,
        "demand_to_admission_ms": (adm.get("wall_ms") - demand["wall_ms"])
        if adm.get("wall_ms") else None,
        "admission_to_served_ms": (served.get("wall_ms") - adm.get("wall_ms"))
        if served.get("wall_ms") and adm.get("wall_ms") else None,
    }
    rep_fields = {
        "connector_existed_before_demand": bool(
            spawned.get("wall_ms") is not None
            and spawned.get("wall_ms") <= demand["wall_ms"]),
        "remote_work_started_before_admission": (m.get("root_result") or {}).get(
            "remote_work_started_before_admission"),
    }
    return {
        "rep": rep, "arm": "demand", "bootdir": bootdir,
        "ports": {"root": p0, "connector": p1},
        "wait_probe": wait_probe,
        "gates": gates, "rep_pass": all(gates.values()),
        "fields": rep_fields,
        "durations_observational_ms": durations,
        "markers": m,
    }


def run_no_demand_rep(binary, x, rep, dwell):
    bootdir = tempfile.mkdtemp(prefix=f"exp65_nodemand_r{rep}_")
    p0 = find_free_port()
    rcmd = root_cmd(binary, "no_demand", bootdir, x, p0, dwell)
    audit_ok = argv_audit(rcmd)

    root, rlog = _popen(rcmd, bootdir, os.path.join(bootdir, "root.log"))
    ready = _wait_for_file(os.path.join(bootdir, "root.ready"), root, 30)
    _ = ready and _wait_for_file(os.path.join(bootdir, "local_phase.done"), root, 30)

    time.sleep(dwell)  # same zero-connector dwell; NO connector is ever launched

    with open(os.path.join(bootdir, "no_demand.finish"), "w") as f:
        f.write("finish\n")
    r_exited, r_rc, r_killed = _wait(root, time.time() + 60)
    rlog.close()

    m = collect_root_markers(bootdir)
    m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)
    m["root_rc"] = r_rc
    m["root_killed"] = r_killed
    m["argv_audit_ok"] = audit_ok
    m["orphans"] = orphan_pids()

    gates = eval_no_demand_gates(m)
    return {
        "rep": rep, "arm": "no_demand", "bootdir": bootdir,
        "ports": {"root": p0},
        "gates": gates, "rep_pass": all(gates.values()),
        "markers": m,
    }


# ---------------------------------------------------------------------------------------
# Cross-node arms (--phase rostam-cross-node): root+controller on the root node, connector
# srun-launched on the connector node ONLY AFTER the demand event. Same binary, same
# oracle, same event ordering, same heartbeat/completion-sentinel lifetime contract.
# ---------------------------------------------------------------------------------------

def crossnode_root_cmd(binary, arm, bootdir, x, root_ip, p0, dwell, admit_timeout,
                       leave_timeout):
    # Same shape as the loopback root, with explicit numeric endpoints on the preferred
    # subnet. leave-timeout is generous cross-node: the connector sees root.done over NFS
    # (attribute-cache latency is bounded but nonzero). NO --hpx:localities anywhere.
    finish_timeout = max(15, int(dwell) + 25)
    return [
        binary, "--role", "root", "--arm", arm, "--bootstrap", bootdir, "--x", str(x),
        "--local-k", "5", "--admit-timeout", str(admit_timeout),
        "--finish-timeout", str(finish_timeout),
        "--leave-timeout", str(leave_timeout), "--dispatch-bound", "15", "--tick-ms", "200",
        f"--hpx:agas={root_ip}:{p0}", f"--hpx:hpx={root_ip}:{p0}",
        "--hpx:expect-connecting-localities",
        "--hpx:threads=2", "--hpx:bind=none", "--hpx:ignore-batch-env",
    ]


def crossnode_connector_cmd(binary, bootdir, conn_node, root_ip, p0, conn_ip, p1,
                            serve_timeout):
    # srun --overlap --cpu-bind=none is the proven exp63/exp64 connector-launch shape.
    # Explicit numeric endpoints: the connector binds ITS OWN subnet IP (bind fails if the
    # process lands elsewhere) and points AGAS at the root endpoint.
    return [
        "srun", "--overlap", "--cpu-bind=none", "--nodes=1", "--ntasks=1",
        f"--nodelist={conn_node}", "--cpus-per-task=2",
        binary, "--role", "connector", "--bootstrap", bootdir,
        "--serve-timeout", str(serve_timeout),
        f"--hpx:agas={root_ip}:{p0}", f"--hpx:hpx={conn_ip}:{p1}",
        "--hpx:threads=1", "--hpx:bind=none", "--hpx:ignore-batch-env",
    ]


def _endpoints_in_argv(rcmd, ccmd, root_ip, p0, conn_ip, p1):
    ok = (f"--hpx:hpx={root_ip}:{p0}" in rcmd and f"--hpx:agas={root_ip}:{p0}" in rcmd)
    if ccmd is not None:
        ok = ok and (f"--hpx:hpx={conn_ip}:{p1}" in ccmd
                     and f"--hpx:agas={root_ip}:{p0}" in ccmd)
    return bool(ok)


def run_crossnode_demand_rep(binary, x, rep, dwell, pf, ports, timeouts, runs_root):
    p0, p1 = ports
    bootdir = os.path.join(runs_root, f"demand_r{rep}")
    os.makedirs(bootdir, exist_ok=True)
    rcmd = crossnode_root_cmd(binary, "demand", bootdir, x, pf["root_subnet_ip"], p0,
                              dwell, timeouts["admit"], timeouts["leave"])
    ccmd = crossnode_connector_cmd(binary, bootdir, pf["connector_node"],
                                   pf["root_subnet_ip"], p0, pf["connector_subnet_ip"], p1,
                                   timeouts["serve"])
    audit_ok = argv_audit(rcmd, ccmd)
    endpoints_ok = _endpoints_in_argv(rcmd, ccmd, pf["root_subnet_ip"], p0,
                                      pf["connector_subnet_ip"], p1)

    root, rlog = _popen(rcmd, bootdir, os.path.join(bootdir, "root.log"))
    ready = _wait_for_file(os.path.join(bootdir, "root.ready"), root, 60)
    local_done = ready and _wait_for_file(
        os.path.join(bootdir, "local_phase.done"), root, 60)

    # Deliberate zero-connector dwell: no connector process exists anywhere.
    time.sleep(dwell)

    # Demand event: recorded strictly BEFORE the connector srun Popen (same controller
    # process, same root-node clock). demand.issued < srun Popen < remote process is the
    # load-bearing causal chain.
    demand = {"wall_ms": int(time.time() * 1000), "monotonic_s": time.monotonic()}
    with open(os.path.join(bootdir, "demand.issued"), "w") as f:
        json.dump(demand, f)
    popen_wall_ms = None
    conn = clog = None
    c_exited = c_killed = False
    c_rc = None
    if local_done:
        popen_wall_ms = int(time.time() * 1000)
        conn, clog = _popen(ccmd, bootdir, os.path.join(bootdir, "connector.log"))

    if conn is not None:
        _wait_for_file(os.path.join(bootdir, "served"), root, timeouts["served_wait"])
        c_exited, c_rc, c_killed = _wait(conn, time.time() + timeouts["conn_exit_wait"])
        clog.close()
    r_exited, r_rc, r_killed = _wait(root, time.time() + timeouts["root_exit_wait"])
    rlog.close()

    # Connector markers are written on the OTHER node: bounded revalidating waits before
    # the final read, so NFS caching cannot fake absence.
    for fname in ("connector.spawned", "connector.joined", "connector.disconnected"):
        _wait_for_file(os.path.join(bootdir, fname), None, 30, revalidate_dir=True)

    m = collect_root_markers(bootdir)
    m["demand"] = demand
    m["controller_popen_wall_ms"] = popen_wall_ms
    m["connector_spawned"] = _read_json(os.path.join(bootdir, "connector.spawned"))
    m["connector_joined"] = _read_json(os.path.join(bootdir, "connector.joined"))
    m["connector_disconnected"] = _read_json(
        os.path.join(bootdir, "connector.disconnected"))
    m["connector_launched_before_demand"] = False  # structural: Popen strictly after demand
    m["connector_exited"] = c_exited
    m["connector_rc"] = c_rc
    m["connector_killed"] = c_killed
    m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)
    m["root_rc"] = r_rc
    m["root_killed"] = r_killed
    m["argv_audit_ok"] = audit_ok
    m["endpoints_in_argv_ok"] = endpoints_ok
    orphans, remote_ran = crossnode_orphans(pf["connector_node"])
    m["orphans"] = orphans
    m["remote_orphan_check_ran"] = remote_ran

    expect = {"root_node": pf["root_node"], "conn_node": pf["connector_node"],
              "root_ip": pf["root_subnet_ip"], "conn_ip": pf["connector_subnet_ip"],
              "subnet_prefix": pf["subnet_prefix"], "slurm_job_id": pf["slurm_job_id"],
              "allocation_hostnames": pf["allocation_hostnames"]}
    gates = eval_crossnode_demand_gates(m, expect)

    spawned = m.get("connector_spawned") or {}
    adm = m.get("admission") or {}
    served = m.get("served") or {}
    durations = {  # OBSERVATIONAL ONLY -- never gate inputs
        "demand_to_srun_popen_ms": (popen_wall_ms - demand["wall_ms"])
        if popen_wall_ms else None,
        "demand_to_admission_ms": (adm.get("wall_ms") - demand["wall_ms"])
        if adm.get("wall_ms") else None,
        "admission_to_served_ms": (served.get("wall_ms") - adm.get("wall_ms"))
        if served.get("wall_ms") and adm.get("wall_ms") else None,
        "spawn_wall_ms_cross_clock_note":
            "connector-node clock; not compared against root-node clocks in any gate",
    }
    rep_fields = {
        "connector_existed_before_demand": False,  # structural: no pre-demand srun exists
        "remote_work_started_before_admission": (m.get("root_result") or {}).get(
            "remote_work_started_before_admission"),
        "connector_spawn_wall_ms_remote_clock": spawned.get("wall_ms"),
    }
    return {
        "rep": rep, "arm": "demand", "bootdir_rel": os.path.relpath(bootdir, HERE),
        "ports": {"root": p0, "connector": p1},
        "gates": gates, "rep_pass": all(gates.values()),
        "fields": rep_fields,
        "durations_observational_ms": durations,
        "markers": m,
    }


def run_crossnode_no_demand_rep(binary, x, rep, dwell, pf, ports, timeouts, runs_root):
    p0 = ports[0]
    bootdir = os.path.join(runs_root, f"nodemand_r{rep}")
    os.makedirs(bootdir, exist_ok=True)
    rcmd = crossnode_root_cmd(binary, "no_demand", bootdir, x, pf["root_subnet_ip"], p0,
                              dwell, timeouts["admit"], timeouts["leave"])
    audit_ok = argv_audit(rcmd)
    endpoints_ok = _endpoints_in_argv(rcmd, None, pf["root_subnet_ip"], p0, None, None)

    root, rlog = _popen(rcmd, bootdir, os.path.join(bootdir, "root.log"))
    ready = _wait_for_file(os.path.join(bootdir, "root.ready"), root, 60)
    _ = ready and _wait_for_file(os.path.join(bootdir, "local_phase.done"), root, 60)

    time.sleep(dwell)  # same zero-connector dwell; NO connector is ever launched anywhere

    with open(os.path.join(bootdir, "no_demand.finish"), "w") as f:
        f.write("finish\n")
    r_exited, r_rc, r_killed = _wait(root, time.time() + timeouts["root_exit_wait"])
    rlog.close()

    m = collect_root_markers(bootdir)
    m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)
    m["root_rc"] = r_rc
    m["root_killed"] = r_killed
    m["argv_audit_ok"] = audit_ok
    m["endpoints_in_argv_ok"] = endpoints_ok
    orphans, remote_ran = crossnode_orphans(pf["connector_node"])
    m["orphans"] = orphans
    m["remote_orphan_check_ran"] = remote_ran

    expect = {"root_node": pf["root_node"], "conn_node": pf["connector_node"],
              "root_ip": pf["root_subnet_ip"], "conn_ip": pf["connector_subnet_ip"],
              "subnet_prefix": pf["subnet_prefix"], "slurm_job_id": pf["slurm_job_id"],
              "allocation_hostnames": pf["allocation_hostnames"]}
    gates = eval_crossnode_no_demand_gates(m, expect)
    return {
        "rep": rep, "arm": "no_demand", "bootdir_rel": os.path.relpath(bootdir, HERE),
        "ports": {"root": p0},
        "gates": gates, "rep_pass": all(gates.values()),
        "markers": m,
    }


# ---------------------------------------------------------------------------------------
# Selftest: pure-Python gate logic on synthetic markers (no binary, no HPX)
# ---------------------------------------------------------------------------------------

def _synthetic_demand_markers():
    return {
        "root_ready": {"membership_count": 1, "membership_ids": [0]},
        "local_phase": {"all_local_ok": True, "membership_count": 1, "wall_ms": 1000},
        "demand": {"wall_ms": 5000},
        "connector_spawned": {"wall_ms": 5010},
        "connector_joined": {"locality_id": 1},
        "admission": {"wall_ms": 6000, "baseline_ids": [0], "new_locality_id": 1,
                      "liveness_ok_before_admission": 12,
                      "first_liveness_wall_ms": 1100, "last_liveness_wall_ms": 5900},
        "served": {"dispatch_status": "returned", "match": True, "proved_remote": True,
                   "executed_on_locality": 1, "wall_ms": 6100},
        "connector_disconnected": {"clean": True,
                                   "shutdown_reason": "root_completion_signal"},
        "leave_observed": {"observed": True},
        "final_local": {"ok": True},
        "root_result": {"remote_work_started_before_admission": False},
        "connector_launched_before_demand": False,
        "connector_exited": True, "connector_rc": 0, "connector_killed": False,
        "root_exit_path": "finalized_clean", "root_killed": False,
        "argv_audit_ok": True, "orphans": [],
    }


def selftest():
    failures = []

    m = _synthetic_demand_markers()
    g = eval_demand_gates(m)
    if not all(g.values()):
        failures.append(f"clean synthetic demand rep should pass all gates: {g}")

    # Connector spawned BEFORE demand -> ordering gates must fail.
    m2 = _synthetic_demand_markers()
    m2["connector_spawned"] = {"wall_ms": 4000}
    g2 = eval_demand_gates(m2)
    if g2["connector_absent_before_demand"] or g2["demand_precedes_creation_and_admission"]:
        failures.append("early connector spawn must fail the ordering gates")

    # Oracle executed on the root -> remote gate must fail.
    m3 = _synthetic_demand_markers()
    m3["served"]["executed_on_locality"] = 0
    if eval_demand_gates(m3)["remote_oracle_on_connector"]:
        failures.append("executed_on_locality==0 must fail remote_oracle_on_connector")

    # Deadman-based exit (not completion sentinel) -> graceful-leave gate must fail.
    m4 = _synthetic_demand_markers()
    m4["connector_disconnected"]["shutdown_reason"] = "serve_timeout_expired"
    if eval_demand_gates(m4)["connector_left_gracefully"]:
        failures.append("serve_timeout_expired must fail connector_left_gracefully")

    # No liveness tick before demand -> local-progress gate must fail.
    m5 = _synthetic_demand_markers()
    m5["admission"]["first_liveness_wall_ms"] = 5500
    if eval_demand_gates(m5)["local_progress_before_demand"]:
        failures.append("first liveness after demand must fail local_progress_before_demand")

    # Static localities flag anywhere -> argv audit must fail.
    if argv_audit(["bin", "--hpx:localities=2"]):
        failures.append("argv_audit must reject --hpx:localities")
    if not argv_audit(["bin", "--hpx:expect-connecting-localities"]):
        failures.append("argv_audit must accept the expect-connecting-localities boolean")

    # Clean no-demand rep passes; a rep where a connector appeared must fail.
    nd = {
        "root_ready": {"membership_count": 1, "membership_ids": [0]},
        "local_phase": {"all_local_ok": True, "membership_count": 1},
        "final_local": {"ok": True},
        "root_result": {"liveness_ok": 10, "max_localities_ever": 1,
                        "admission_detected": False, "no_demand_finish_seen": True},
        "root_exit_path": "finalized_clean", "root_killed": False,
        "argv_audit_ok": True, "orphans": [],
    }
    if not all(eval_no_demand_gates(nd).values()):
        failures.append(f"clean synthetic no-demand rep should pass: {eval_no_demand_gates(nd)}")
    nd2 = json.loads(json.dumps(nd))
    nd2["root_result"]["max_localities_ever"] = 2
    if eval_no_demand_gates(nd2)["zero_connectors_ever"]:
        failures.append("max_localities_ever==2 must fail zero_connectors_ever")

    # ---- cross-node gate logic (pure; no Slurm, no binary) ------------------------------
    def cx_expect():
        return {"root_node": "medusa00", "conn_node": "medusa01",
                "root_ip": "10.42.5.30", "conn_ip": "10.42.5.31",
                "subnet_prefix": "10.42.5.", "slurm_job_id": "999999",
                "allocation_hostnames": ["medusa00", "medusa01"]}

    def cx_markers():
        m = _synthetic_demand_markers()
        m["root_ready"].update({"host": "medusa00", "pid": 101})
        m["connector_spawned"] = {"wall_ms": 5400, "host": "medusa01", "pid": 202}
        m["connector_joined"] = {"locality_id": 1, "host": "medusa01"}
        m["controller_popen_wall_ms"] = 5003
        m["endpoints_in_argv_ok"] = True
        m["remote_orphan_check_ran"] = True
        m["root_result"]["here_locality"] = 0
        return m

    cxm = cx_markers()
    cxg = eval_crossnode_demand_gates(cxm, cx_expect())
    if not all(cxg.values()):
        failures.append("clean synthetic cross-node demand rep should pass all gates: "
                        f"{ {k: v for k, v in cxg.items() if not v} }")

    # srun Popen recorded BEFORE demand -> same-clock ordering gates must fail.
    cx2 = cx_markers()
    cx2["controller_popen_wall_ms"] = 4990
    g2x = eval_crossnode_demand_gates(cx2, cx_expect())
    if g2x["connector_absent_before_demand"] or g2x["demand_precedes_creation_and_admission"]:
        failures.append("popen before demand must fail the cross-node ordering gates")

    # Connector attested on the ROOT node -> placement gates must fail.
    cx3 = cx_markers()
    cx3["connector_spawned"]["host"] = "medusa00"
    cx3["connector_joined"]["host"] = "medusa00"
    g3x = eval_crossnode_demand_gates(cx3, cx_expect())
    if g3x["cross_node_placement"] or g3x["connector_on_connector_node"]:
        failures.append("same-host connector must fail the cross-node placement gates")

    # Root endpoint off the pinned subnet -> subnet gate must fail.
    ex4 = cx_expect()
    ex4["root_ip"] = "10.42.6.30"
    if eval_crossnode_demand_gates(cx_markers(), ex4)["subnet_pinned_both_endpoints"]:
        failures.append("off-subnet root endpoint must fail subnet_pinned_both_endpoints")

    # Missing connector host attestation -> completeness gate must fail.
    cx5 = cx_markers()
    del cx5["connector_joined"]["host"]
    if eval_crossnode_demand_gates(cx5, cx_expect())["evidence_fields_complete"]:
        failures.append("missing joined.host must fail evidence_fields_complete")

    # Remote orphan check that never ran -> its gate must fail.
    cx6 = cx_markers()
    cx6["remote_orphan_check_ran"] = False
    if eval_crossnode_demand_gates(cx6, cx_expect())["remote_orphan_check_ran"]:
        failures.append("remote_orphan_check_ran=False must fail its gate")

    # FQDN vs short hostname must still match (exp59 normalization lesson).
    cx7 = cx_markers()
    cx7["root_ready"]["host"] = "medusa00.rostam.cct.lsu.edu"
    if not eval_crossnode_demand_gates(cx7, cx_expect())["root_on_root_node"]:
        failures.append("FQDN root host must normalize and pass root_on_root_node")

    # Cross-node no-demand: clean passes; root on the wrong node fails.
    ndx = json.loads(json.dumps(nd))
    ndx["root_ready"].update({"host": "medusa00", "pid": 101})
    ndx["endpoints_in_argv_ok"] = True
    ndx["remote_orphan_check_ran"] = True
    gndx = eval_crossnode_no_demand_gates(ndx, cx_expect())
    if not all(gndx.values()):
        failures.append("clean synthetic cross-node no-demand rep should pass: "
                        f"{ {k: v for k, v in gndx.items() if not v} }")
    ndx2 = json.loads(json.dumps(ndx))
    ndx2["root_ready"]["host"] = "medusa01"
    if eval_crossnode_no_demand_gates(ndx2, cx_expect())["root_on_root_node"]:
        failures.append("no-demand root on the wrong node must fail root_on_root_node")

    if failures:
        for f in failures:
            print(f"[exp65 selftest] FAIL: {f}")
        return 1
    print("[exp65 selftest] all gate-logic checks passed")
    return 0


# ---------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="exp65 demand-triggered admission runner")
    ap.add_argument("--binary", default=None, help=f"path to {BINARY_BASENAME}")
    ap.add_argument("--x", type=int, default=7, help="closed int64 action input base")
    ap.add_argument("--reps", type=int, default=3, help="repetitions per arm")
    ap.add_argument("--dwell", type=float, default=3.0,
                    help="zero-connector dwell seconds before demand/finish")
    ap.add_argument("--phase", choices=["local", "rostam-cross-node"], default="local",
                    help="local = the original single-node loopback experiment (default, "
                         "unchanged); rostam-cross-node = the two-node Slurm slice")
    ap.add_argument("--root-node", default="medusa00",
                    help="cross-node: node hosting controller + root")
    ap.add_argument("--connector-node", default="medusa01",
                    help="cross-node: node the connector is srun-launched on after demand")
    ap.add_argument("--subnet-prefix", default="10.42.5.",
                    help="cross-node: required IPv4 prefix for both endpoints")
    ap.add_argument("--port-base", type=int, default=7841,
                    help="cross-node: fresh port pair per rep from this base")
    ap.add_argument("--admit-timeout", type=int, default=120,
                    help="cross-node root: max seconds in the available-wait loop")
    ap.add_argument("--leave-timeout", type=int, default=90,
                    help="cross-node root: max seconds waiting for the departure "
                         "(generous: root.done travels over NFS)")
    ap.add_argument("--serve-timeout", type=int, default=150,
                    help="cross-node connector DEADMAN seconds (never the release path)")
    ap.add_argument("--aggregate", default=None,
                    help="curated aggregate path (default depends on --phase)")
    ap.add_argument("--wait-probe", choices=["sliced", "full_bound_instrumented"],
                    default="sliced",
                    help="local-phase dispatch wait probe. sliced (default) keeps exp65 "
                         "semantics unchanged; full_bound_instrumented is the waiter-fix "
                         "verification re-check (ONE full-bound wait_for + readiness "
                         "witness). Non-default runs never write the curated exp65 "
                         "aggregate by default.")
    ap.add_argument("--selftest", action="store_true",
                    help="run pure-Python gate-logic selftests and exit")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    if args.aggregate is None:
        if args.phase == "local" and args.wait_probe != "sliced":
            # Waiter-fix re-check runs must not clobber the curated exp65 evidence aggregate.
            os.makedirs(os.path.join(HERE, "_exp65_runs"), exist_ok=True)
            args.aggregate = os.path.join(
                HERE, "_exp65_runs", f"demand_admission_waiter_recheck_{args.wait_probe}.json")
        else:
            args.aggregate = os.path.join(
                HERE, "demand_admission_crossnode_aggregate.json"
                if args.phase == "rostam-cross-node" else "demand_admission_aggregate.json")

    if args.phase == "rostam-cross-node":
        return run_crossnode_phase(args)

    binary = locate_binary(args.binary)
    agg = {
        "experiment": "65_demand_admission",
        "kind": "demand_triggered_connect_admission_mechanism",
        "ray_free": True, "single_node": True, "transport": "tcp_loopback",
        "controller": "plain_python_orchestrator",
        "structural_flags": dict(STRUCTURAL_FLAGS),
        "claim_fences": dict(CLAIM_FENCES),
        "reps_per_arm": args.reps,
        "dwell_s": args.dwell,
        "wait_probe": args.wait_probe,
        "binary": os.path.basename(binary) if binary else None,
    }
    if binary is None:
        agg["overall"] = "skip"
        agg["reason"] = "binary not built (see CMakeLists.txt)"
        _write_agg(args.aggregate, agg)
        print("SKIP: demand_admission_spike not found; build the experiment first.")
        return 0

    print(f"[exp65] binary: {binary}")
    print(f"[exp65] wait_probe: {args.wait_probe}")
    demand_reps, nodemand_reps = [], []
    for r in range(1, args.reps + 1):
        print(f"[exp65] demand arm rep {r} ...")
        rep = run_demand_rep(binary, args.x, r, args.dwell, args.wait_probe)
        print(f"[exp65]   rep {r}: {'pass' if rep['rep_pass'] else 'FAIL'} "
              f"(gates_failed={[k for k, v in rep['gates'].items() if not v]})")
        demand_reps.append(rep)
    for r in range(1, args.reps + 1):
        print(f"[exp65] no-demand arm rep {r} ...")
        rep = run_no_demand_rep(binary, args.x, r, args.dwell)
        print(f"[exp65]   rep {r}: {'pass' if rep['rep_pass'] else 'FAIL'} "
              f"(gates_failed={[k for k, v in rep['gates'].items() if not v]})")
        nodemand_reps.append(rep)

    demand_pass = all(r["rep_pass"] for r in demand_reps)
    nodemand_pass = all(r["rep_pass"] for r in nodemand_reps)
    agg["demand_arm"] = {"all_pass": demand_pass, "reps": demand_reps}
    agg["no_demand_arm"] = {"all_pass": nodemand_pass, "reps": nodemand_reps}
    agg["overall"] = "pass" if (demand_pass and nodemand_pass) else "fail"

    # Drop machine-specific temp paths from the curated aggregate (raw logs stay in the
    # temp bootdirs printed during the run).
    for reps in (agg["demand_arm"]["reps"], agg["no_demand_arm"]["reps"]):
        for rep in reps:
            rep.pop("bootdir", None)

    _write_agg(args.aggregate, agg)
    print(f"[exp65] overall: {agg['overall']} -> {args.aggregate}")
    return 0


def run_crossnode_phase(args):
    """--phase rostam-cross-node: the two-node slice. Root+controller on --root-node,
    connector srun-launched on --connector-node strictly after each rep's demand event.
    Writes its OWN curated aggregate; never touches the loopback aggregate."""
    binary = locate_binary(args.binary)
    agg = {
        "experiment": "65_demand_admission",
        "kind": "demand_triggered_connect_admission_mechanism_crossnode",
        "phase": "rostam-cross-node",
        "ray_free": True, "single_node": False, "cross_node": True,
        "transport": "tcp_cross_node",
        "controller": "plain_python_orchestrator_under_slurm",
        "structural_flags": dict(STRUCTURAL_FLAGS),
        "claim_fences": dict(CROSSNODE_CLAIM_FENCES),
        "reps_per_arm": args.reps,
        "dwell_s": args.dwell,
        "binary": os.path.basename(binary) if binary else None,
    }
    if binary is None:
        agg["overall"] = "skip"
        agg["reason"] = "binary not built (see CMakeLists.txt)"
        _write_agg(args.aggregate, agg)
        print("SKIP: demand_admission_spike not found; build the experiment first.")
        return 0

    pf, problems = crossnode_preflight(args.root_node, args.connector_node,
                                       args.subnet_prefix)
    agg["preflight"] = pf
    if problems:
        agg["overall"] = "fail_preflight"
        agg["preflight_problems"] = problems
        _write_agg(args.aggregate, agg)
        print(f"[exp65-crossnode] PREFLIGHT FAIL: {problems}")
        return 0

    print(f"[exp65-crossnode] binary: {binary}")
    print(f"[exp65-crossnode] job {pf['slurm_job_id']} nodes {pf['allocation_hostnames']} "
          f"root {pf['root_node']}={pf['root_subnet_ip']} "
          f"connector {pf['connector_node']}={pf['connector_subnet_ip']}")
    runs_root = os.path.join(HERE, "_exp65_runs", f"crossnode_{pf['slurm_job_id']}")
    os.makedirs(runs_root, exist_ok=True)
    timeouts = {"admit": args.admit_timeout, "leave": args.leave_timeout,
                "serve": args.serve_timeout,
                "served_wait": args.admit_timeout + 60,
                "conn_exit_wait": args.serve_timeout + 90,
                "root_exit_wait": 240}

    demand_reps, nodemand_reps = [], []
    for r in range(1, args.reps + 1):
        ports = (args.port_base + (r - 1) * 4, args.port_base + (r - 1) * 4 + 1)
        print(f"[exp65-crossnode] demand arm rep {r} (ports {ports}) ...")
        rep = run_crossnode_demand_rep(binary, args.x, r, args.dwell, pf, ports,
                                       timeouts, runs_root)
        print(f"[exp65-crossnode]   rep {r}: {'pass' if rep['rep_pass'] else 'FAIL'} "
              f"(gates_failed={[k for k, v in rep['gates'].items() if not v]})")
        demand_reps.append(rep)
    for r in range(1, args.reps + 1):
        ports = (args.port_base + 100 + (r - 1) * 4,)
        print(f"[exp65-crossnode] no-demand arm rep {r} (ports {ports}) ...")
        rep = run_crossnode_no_demand_rep(binary, args.x, r, args.dwell, pf, ports,
                                          timeouts, runs_root)
        print(f"[exp65-crossnode]   rep {r}: {'pass' if rep['rep_pass'] else 'FAIL'} "
              f"(gates_failed={[k for k, v in rep['gates'].items() if not v]})")
        nodemand_reps.append(rep)

    demand_pass = all(r["rep_pass"] for r in demand_reps)
    nodemand_pass = all(r["rep_pass"] for r in nodemand_reps)
    agg["nodes"] = {"root": pf["root_node"], "connector": pf["connector_node"],
                    "root_subnet_ip": pf["root_subnet_ip"],
                    "connector_subnet_ip": pf["connector_subnet_ip"],
                    "subnet_prefix": pf["subnet_prefix"]}
    agg["slurm"] = {"job_id": pf["slurm_job_id"],
                    "nodelist_raw": pf["slurm_nodelist_raw"],
                    "allocation_hostnames": pf["allocation_hostnames"]}
    agg["clock_skew_observational"] = pf.get("clock_skew_observational")
    agg["demand_arm"] = {"all_pass": demand_pass, "reps": demand_reps}
    agg["no_demand_arm"] = {"all_pass": nodemand_pass, "reps": nodemand_reps}
    agg["overall"] = "pass" if (demand_pass and nodemand_pass) else "fail"

    _write_agg(args.aggregate, agg)
    print(f"[exp65-crossnode] overall: {agg['overall']} -> {args.aggregate}")
    return 0


def _write_agg(path, agg):
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=False)
        f.write("\n")


if __name__ == "__main__":
    sys.exit(main())
