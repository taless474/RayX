#!/usr/bin/env python3
"""exp56 -- Ray-free TWO-NODE HPX TCP parcelport connect-mode probe (orchestrator).

ORCHESTRATOR ONLY. No Ray. Reuses no RayX/production code. Drives the standalone
``two_node_tcp_spike`` binary (built from this directory's CMakeLists against an HPX prefix whose TCP
parcelport is enabled).

PRECONDITIONS ARE GATES, NOT MID-RUN DISCOVERIES:
  * Part 0a/b: confirm the HPX build has the TCP parcelport and read back the exact config key names
    (``check-config``). If TCP is not confirmed -> aggregate ``overall=skip``,
    ``tcp_parcelport_available=false`` and STOP. No MPI/LCI substitution.
  * Part 0c: diagnose the exp55 ~15 s loopback settle on 127.0.0.1 with the new binary + parcel logging
    (``diag-loopback``); record advertised endpoints, retries/wrong-interface signs, settle duration.
  * Part 1: two-node probe (``reachability`` + ``run``) -- requires a >=2-node Slurm allocation; two
    independent single-task ``srun`` steps; explicit routable IPs (never 127.0.0.1); bidirectional
    socket reachability preflight; three-way remote proof. Skips cleanly off a 2-node allocation.

CLAIM FENCE: first two-node probe only; TCP parcelport only; closed-int64 action only; Ray-free; no
performance/speedup/throughput/latency claim (``agas_settle_ms`` is structural readiness, not latency);
no HPX fault tolerance; no Ray actor-failure recovery; no production/public API; no object store; no
arbitrary Python; no Ray replacement; no general fabric claim from one TCP two-node probe; no MPI/LCI
performance-path claim; dynamic-supervision / high-performance-parcelport tension unresolved. Future
distributed-fabric direction only.

Exit code 0 on clean fail/skip; non-zero only on an orchestrator-internal error.
"""

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BINARY_CANDIDATES = [
    os.path.join(HERE, "build", "two_node_tcp_spike"),
    os.path.join(HERE, "build", "Release", "two_node_tcp_spike"),
]
BINARY_BASENAME = "two_node_tcp_spike"

# TCP-pinning ini flags. Key names confirmed from `--hpx:dump-config` ([parcel] bootstrap=tcp,
# [parcel][tcp] enable). IMPORTANT: HPX REJECTS unknown ini keys (HPX(no_success)), so a parcelport
# may be disabled ONLY when it is actually present in this build -- see tcp_pin_flags(cfg).
TCP_ENABLE_FLAGS = [
    "--hpx:ini=hpx.parcel.bootstrap=tcp",
    "--hpx:ini=hpx.parcel.tcp.enable=1",
]
PARCEL_LOG_FLAGS = ["--hpx:ini=hpx.logging.parcel.level=5"]


def tcp_pin_flags(cfg):
    """Force TCP and disable only the OTHER parcelports that are actually built in (disabling an
    absent parcelport's key would be rejected by HPX as an unknown entry)."""
    flags = list(TCP_ENABLE_FLAGS)
    for pp, present in (cfg or {}).get("parcelports_present", {}).items():
        if pp != "tcp" and present:
            flags.append(f"--hpx:ini=hpx.parcel.{pp}.enable=0")
    return flags


# ---------------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------------
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


def _child_env():
    """Inherit the FULL environment. The exp56 binary is dynamically linked against HPX/Boost/hwloc
    and on Rostam resolves them via LD_LIBRARY_PATH set by `module load`; stripping loader vars (as a
    macOS RPATH-proof would) breaks the binary on Rostam. On macOS it resolves HPX via RPATH, so
    keeping the env is harmless there too."""
    return dict(os.environ)


def _popen(argv, bootdir, log_name):
    log = open(os.path.join(bootdir, log_name), "w")
    proc = subprocess.Popen(argv, cwd=bootdir, stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True, env=_child_env())
    return proc, log


def _wait_for_file(path, proc, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        if proc is not None and proc.poll() is not None:
            return os.path.exists(path)
        time.sleep(0.05)
    return os.path.exists(path)


def _killpg(proc):
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _orphan_check():
    try:
        out = subprocess.run(["pgrep", "-f", BINARY_BASENAME], capture_output=True, text=True,
                             timeout=10)
        pids = [p for p in out.stdout.split() if p]
        return (len(pids) == 0), pids
    except Exception:
        return None, []


def _run(argv, timeout=30):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except Exception:  # noqa: BLE001
        return None


def _tail(path, n=20):
    try:
        with open(path, errors="ignore") as f:
            lines = f.readlines()
    except OSError:
        return None
    return ("".join(lines[-n:]).strip() or None)


def _resolve_shared_dir(args):
    """Two-node rendezvous MUST be on a shared filesystem -- node-local /tmp is invisible across
    nodes (root writes markers on node A that the launcher/connector cannot see). Default to an
    experiment-local dir, which is on shared /work when the repo lives there."""
    if args.shared_dir:
        return os.path.abspath(args.shared_dir), "explicit --shared-dir"
    return os.path.join(HERE, "_two_node_runs"), "default (experiment-local _two_node_runs)"


def _orphan_check_node(node):
    """pgrep exits 1 when there are NO matches -> that is the clean/no-orphans case, not an error."""
    o = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "pgrep", "-f", BINARY_BASENAME], timeout=20)
    if o is None:
        return None, []                        # could not determine
    pids = [p for p in o.stdout.split() if p] if o.returncode == 0 else []
    return (len(pids) == 0), pids              # rc==1 (no match) -> clean


# ---------------------------------------------------------------------------------------------------
# Part 0a/b -- TCP parcelport availability + config key discovery
# ---------------------------------------------------------------------------------------------------
def check_config(binary):
    """Confirm the HPX build advertises the TCP parcelport and capture a config summary. Returns a
    dict; tcp_parcelport_available drives the Part 0 gate."""
    # --hpx:dump-config dumps the resolved ini then still runs the root; give it explicit endpoints
    # + a 1s ready-timeout so it dumps and exits in ~1s instead of waiting for a connector.
    p = find_free_port()
    bd = tempfile.mkdtemp(prefix="exp56_cfg_")
    out = _run([binary, "--role", "root", "--bootstrap", bd, "--ready-timeout", "1",
                "--leave-timeout", "1", f"--hpx:agas=127.0.0.1:{p}", f"--hpx:hpx=127.0.0.1:{p}",
                "--hpx:ignore-batch-env", "--hpx:dump-config"], timeout=30)
    dump = (out.stdout + out.stderr) if out else ""
    ver = _run([binary, "--hpx:version"], timeout=20)
    version = ((ver.stdout + ver.stderr).strip().splitlines()[0] if ver and (ver.stdout or ver.stderr)
               else None)

    # parse the [parcel] section: bootstrap, and which parcelports appear with enable=1
    bootstrap = None
    m = re.search(r"'bootstrap'\s*:\s*'([^']+)'", dump)
    if m:
        bootstrap = m.group(1)
    present = {}
    for pp in ("tcp", "mpi", "lci", "gasnet"):
        # a parcelport subsection looks like "[tcp]" with an 'enable' line resolving to 0/1
        sect = re.search(r"\[" + pp + r"\](.*?)(?=\n\s*\[|\Z)", dump, re.S)
        if sect:
            en = re.search(r"'enable'\s*:.*?->\s*'(\d)'", sect.group(1))
            present[pp] = (en.group(1) == "1") if en else True
    tcp_available = bool(present.get("tcp")) or (bootstrap == "tcp")
    others = {k: v for k, v in present.items() if k != "tcp"}

    return {
        "tcp_parcelport_available": tcp_available,
        "parcel_bootstrap": bootstrap,
        "parcelports_present": present,
        "other_parcelports_present": others,
        "hpx_version": version,
        "hpx_parcelport_config": (f"bootstrap={bootstrap}; "
                                  + ", ".join(f"{k}={'on' if v else 'off'}"
                                              for k, v in present.items())),
    }


# ---------------------------------------------------------------------------------------------------
# Part 0c -- loopback settle diagnostic (single node, 127.0.0.1)
# ---------------------------------------------------------------------------------------------------
def _base_hpx_flags(role, threads, agas_ip, agas_port, hpx_ip, hpx_port, pin_flags, extra=()):
    argv = [
        "--hpx:agas={}:{}".format(agas_ip, agas_port),
        "--hpx:hpx={}:{}".format(hpx_ip, hpx_port),
        "--hpx:threads={}".format(threads),
        "--hpx:ignore-batch-env",
    ]
    argv += list(pin_flags)
    if role == "root":
        argv.append("--hpx:expect-connecting-localities")
    argv += list(extra)
    return argv


def diag_loopback(binary, args, cfg):
    """Reproduce the exp55 ~15 s settle on loopback with TCP pinned + parcel logging; record advertised
    endpoints and any retry / wrong-interface signs."""
    pin = tcp_pin_flags(cfg)
    bootdir = tempfile.mkdtemp(prefix="exp56_loop_")
    pagas, phpx = find_free_port(), find_free_port()
    root_argv = [binary, "--role", "root", "--bootstrap", bootdir, "--x", "7",
                 "--ready-timeout", str(args.ready_timeout), "--leave-timeout", str(args.leave_timeout)]
    root_argv += _base_hpx_flags("root", 2, "127.0.0.1", pagas, "127.0.0.1", pagas, pin,
                                 PARCEL_LOG_FLAGS)
    conn_argv = [binary, "--role", "connect", "--bootstrap", bootdir,
                 "--serve-timeout", str(args.serve_timeout)]
    conn_argv += _base_hpx_flags("connect", 1, "127.0.0.1", pagas, "127.0.0.1", phpx, pin,
                                 PARCEL_LOG_FLAGS)

    root, rlog = _popen(root_argv, bootdir, "root.log")
    root_ready = _wait_for_file(os.path.join(bootdir, "root.ready"), root, args.ready_timeout)
    conn = clog = None
    if root_ready and root.poll() is None:
        conn, clog = _popen(conn_argv, bootdir, "connector.log")
        _wait_for_file(os.path.join(bootdir, "connect.joined1"), conn, args.ready_timeout)

    # wait for the root to finish (serve + observe leave + finalize) within a generous bound
    deadline = time.time() + args.ready_timeout + args.leave_timeout + 30
    while time.time() < deadline and root.poll() is None:
        time.sleep(0.1)
    root_exited = root.poll() is not None
    root_rc = root.poll()
    _killpg(conn)
    _killpg(root)
    for p, lg in ((root, rlog), (conn, clog)):
        if p is not None:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                pass
        if lg is not None:
            try:
                lg.close()
            except OSError:
                pass

    rr = _read_json(os.path.join(bootdir, "root_result.json")) or {}
    a_root = _read_json(os.path.join(bootdir, "attest_root.json")) or {}
    a_conn = _read_json(os.path.join(bootdir, "attest_connect.json")) or {}
    disc = _read_json(os.path.join(bootdir, "connect.disconnected1")) or {}

    # scan any parcel/hpx logs for retries / resolve issues
    retries = wrong_iface = False
    log_hits = []
    for fn in os.listdir(bootdir):
        if fn.endswith(".log") or "parcel" in fn:
            try:
                with open(os.path.join(bootdir, fn), errors="ignore") as fh:
                    txt = fh.read()
            except OSError:
                continue
            for pat, flag in (("retry", "retry"), ("retrying", "retry"),
                              ("connection refused", "refused"), ("resolve", "resolve"),
                              ("timed out", "timeout")):
                if pat in txt.lower():
                    log_hits.append(f"{fn}:{flag}")
                    if flag in ("retry", "refused", "timeout"):
                        retries = True

    settle_ms = rr.get("settle_ms")
    root_adv = a_root.get("advertised_hpx_endpoint")
    conn_adv = a_conn.get("advertised_hpx_endpoint")
    markers_present = bool(root_adv and conn_adv)
    # intended endpoints were 127.0.0.1:<port>; flag if a process advertised something else
    intended_ok = bool(root_adv and root_adv.startswith("127.0.0.1")
                       and conn_adv and conn_adv.startswith("127.0.0.1"))
    if markers_present and not intended_ok:
        wrong_iface = True

    # tails when the diagnostic did not produce markers (e.g. the binary failed to start)
    root_tail = _tail(os.path.join(bootdir, "root.log"))
    conn_tail = _tail(os.path.join(bootdir, "connector.log"))

    note_bits = []
    if settle_ms is not None and settle_ms >= 0:
        note_bits.append(f"loopback settle ~{settle_ms} ms")
    if not markers_present:
        note_bits.append("MARKERS MISSING (root/connector did not start or write attestation; "
                         "see loopback_root_log_tail)")
    elif intended_ok:
        note_bits.append("advertised endpoints match intended 127.0.0.1")
    else:
        note_bits.append("ADVERTISED ENDPOINT MISMATCH (investigate interface selection)")
    note_bits.append("log retries seen" if retries else "no retry/refused/timeout in logs")
    rootcause = "; ".join(note_bits)

    return {
        "bootdir": bootdir,
        "loopback_settle_diagnosed": True,
        "loopback_settle_ms": settle_ms,
        "loopback_advertised_addresses": {"root": root_adv, "connector": conn_adv},
        "loopback_markers_present": markers_present,
        "loopback_advertised_intended_ok": intended_ok,
        "loopback_log_retry_or_resolve_signs": retries,
        "loopback_wrong_interface_advertised": wrong_iface,
        "loopback_log_hits": sorted(set(log_hits)),
        "loopback_root_log_tail": (root_tail if not markers_present else None),
        "loopback_connector_log_tail": (conn_tail if not markers_present else None),
        "loopback_settle_rootcause_note": rootcause,
        # single-node proof captured for evidence (NOT a two-node remote claim)
        "loopback_diag": {
            "root_started": root_ready,
            "connector_joined": bool(_read_json(os.path.join(bootdir, "connect.joined1"))),
            "reached_two": bool(rr.get("reached_two")),
            "proved_remote_by_oracle": bool(rr.get("proved_remote_by_oracle")),
            "remote_locality_id_differs": bool(rr.get("remote_locality_id_differs")),
            "graceful_disconnect_clean": bool(disc.get("clean")),
            "root_finalized_clean": bool(root_exited and root_rc == 0),
            "note": "single-node loopback diagnostic; hostnames are identical so it does NOT prove "
                    "two-node remote execution -- that is Part 1 only.",
        },
    }


# ---------------------------------------------------------------------------------------------------
# Part 1 -- two-node probe (requires a >=2-node Slurm allocation)
# ---------------------------------------------------------------------------------------------------
def slurm_nodes():
    n = os.environ.get("SLURM_JOB_NUM_NODES")
    nodelist = os.environ.get("SLURM_JOB_NODELIST")
    if not n or not nodelist or int(n) < 2:
        return None
    out = _run(["scontrol", "show", "hostnames", nodelist], timeout=20)
    if not out or out.returncode != 0:
        return None
    hosts = [h for h in out.stdout.split() if h]
    return hosts[:2] if len(hosts) >= 2 else None


def select_node_ip(node, prefer_subnet):
    """Pick an explicit routable IPv4 for `node` via an srun step (never 127.0.0.1)."""
    out = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "bash", "-c",
                "ip -4 -o addr show scope global | awk '{print $2, $4}'"], timeout=30)
    if not out or out.returncode != 0:
        return None, None
    cand = []
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            iface = parts[0]
            ip = parts[1].split("/")[0]
            if ip and not ip.startswith("127."):
                cand.append((iface, ip))
    if not cand:
        return None, None
    if prefer_subnet:
        for iface, ip in cand:
            if ip.startswith(prefer_subnet):
                return iface, ip
    return cand[0]  # (iface, ip)


def reachability_check(nodeA, nodeB, A_ip, B_ip, pagas, phpx):
    """Bidirectional socket preflight via single-task srun steps: B->A on pagas, A->B on phpx."""
    def probe(listen_node, listen_ip, port, connect_node, connect_ip):
        # start a listener on listen_node, then connect from connect_node
        listener = subprocess.Popen(
            ["srun", "-N1", "-n1", "--nodelist=" + listen_node, "python3", "-c",
             f"import socket;s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);"
             f"s.bind(('{listen_ip}',{port}));s.listen(1);c,_=s.accept();c.close();s.close()"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2.0)
        c = _run(["srun", "-N1", "-n1", "--nodelist=" + connect_node, "python3", "-c",
                  f"import socket;s=socket.create_connection(('{connect_ip}',{port}),timeout=10);"
                  f"s.close();print('OK')"], timeout=30)
        ok = bool(c and "OK" in (c.stdout or ""))
        try:
            listener.wait(timeout=5)
        except Exception:
            listener.kill()
        return ok

    b_to_a = probe(nodeA, A_ip, pagas, nodeB, A_ip)
    a_to_b = probe(nodeB, B_ip, phpx, nodeA, B_ip)
    return b_to_a, a_to_b


def select_and_reachability(args):
    """Part 1 socket-only preflight: pick nodes/IPs and run the bidirectional reachability checks.
    Launches NO HPX. Returns a dict, or None when there is no >=2-node allocation."""
    nodes = slurm_nodes()
    if not nodes:
        return None
    nodeA, nodeB = nodes
    ifaceA, A_ip = select_node_ip(nodeA, args.prefer_subnet)
    ifaceB, B_ip = select_node_ip(nodeB, args.prefer_subnet)
    sel = {"two_node_run": True, "nodeA": nodeA, "nodeB": nodeB, "nodeA_ip": A_ip, "nodeB_ip": B_ip,
           "selected_interface": (f"{ifaceA}/{ifaceB}" if (ifaceA and ifaceB) else None),
           "selected_subnet": args.prefer_subnet or (A_ip.rsplit('.', 1)[0] + ".0/24" if A_ip else None)}
    if not A_ip or not B_ip:
        sel["bidirectional_port_check_passed"] = False
        sel["overall"] = "fail"
        sel["reason"] = "could not select routable IPs (check Ethernet vs IPoIB / --prefer-subnet)"
        return sel
    b_to_a, a_to_b = reachability_check(nodeA, nodeB, A_ip, B_ip, args.agas_port, args.hpx_port)
    sel["reachability_b_to_a"] = b_to_a
    sel["reachability_a_to_b"] = a_to_b
    sel["bidirectional_port_check_passed"] = bool(b_to_a and a_to_b)
    return sel


def run_two_node(binary, args, cfg):
    """Full Part 1: reachability preflight, then the two-node HPX launch + three-way remote proof.
    Returns a findings dict, or None when there is no >=2-node allocation."""
    sel = select_and_reachability(args)
    if sel is None:
        return None
    if not sel.get("bidirectional_port_check_passed"):
        sel.setdefault("overall", "fail")
        sel.setdefault("reason", "bidirectional reachability failed (firewall/interface); HPX not "
                                 "launched")
        return sel
    return _launch_two_node(binary, args, cfg, sel)


def _launch_two_node(binary, args, cfg, sel):
    pin = tcp_pin_flags(cfg)
    nodeA, nodeB = sel["nodeA"], sel["nodeB"]
    A_ip, B_ip = sel["nodeA_ip"], sel["nodeB_ip"]
    pagas, phpx = args.agas_port, args.hpx_port

    shared, src = _resolve_shared_dir(args)
    os.makedirs(shared, exist_ok=True)
    bootdir = tempfile.mkdtemp(prefix="exp56_2n_", dir=shared)
    sel["bootstrap_dir"] = bootdir
    sel["shared_dir_source"] = src
    sel["bootstrap_dir_node_local_warning"] = bool(bootdir.startswith("/tmp") or "/tmp/" in bootdir)

    root_argv = ["srun", "-N1", "-n1", "--nodelist=" + nodeA, binary,
                 "--role", "root", "--bootstrap", bootdir, "--x", "7",
                 "--ready-timeout", str(args.ready_timeout), "--leave-timeout", str(args.leave_timeout)]
    root_argv += _base_hpx_flags("root", args.threads, A_ip, pagas, A_ip, pagas, pin)
    conn_argv = ["srun", "-N1", "-n1", "--nodelist=" + nodeB, binary,
                 "--role", "connect", "--bootstrap", bootdir, "--serve-timeout", str(args.serve_timeout)]
    conn_argv += _base_hpx_flags("connect", args.threads, A_ip, pagas, B_ip, phpx, pin)
    sel["root_argv"] = " ".join(root_argv)
    sel["connector_argv"] = " ".join(conn_argv)

    # persist stdout and stderr SEPARATELY (shared FS so the launcher can read both nodes' logs)
    r_out = open(os.path.join(bootdir, "root.stdout"), "w")
    r_err = open(os.path.join(bootdir, "root.stderr"), "w")
    root = subprocess.Popen(root_argv, stdout=r_out, stderr=r_err, env=_child_env())
    root_ready = _wait_for_file(os.path.join(bootdir, "root.ready"), root, args.ready_timeout)
    conn = c_out = c_err = None
    if root_ready:
        c_out = open(os.path.join(bootdir, "connector.stdout"), "w")
        c_err = open(os.path.join(bootdir, "connector.stderr"), "w")
        conn = subprocess.Popen(conn_argv, stdout=c_out, stderr=c_err, env=_child_env())

    deadline = time.time() + args.ready_timeout + args.leave_timeout + 60
    while time.time() < deadline and root.poll() is None:
        time.sleep(0.2)
    root_rc = root.poll()
    if root_rc is None:
        root.kill()
    if conn is not None:
        try:
            conn.wait(timeout=15)
        except Exception:
            conn.kill()
    for fh in (r_out, r_err, c_out, c_err):
        if fh is not None:
            try:
                fh.close()
            except OSError:
                pass

    rr = _read_json(os.path.join(bootdir, "root_result.json")) or {}
    a_root = _read_json(os.path.join(bootdir, "attest_root.json"))
    a_conn = _read_json(os.path.join(bootdir, "attest_connect.json"))
    disc = _read_json(os.path.join(bootdir, "connect.disconnected1")) or {}

    attest_root_present = a_root is not None
    attest_connector_present = a_conn is not None
    host_differs = bool(attest_root_present and attest_connector_present
                        and a_root.get("hostname") and a_conn.get("hostname")
                        and a_root["hostname"] != a_conn["hostname"])
    proved_oracle = bool(rr.get("proved_remote_by_oracle"))
    id_differs = bool(rr.get("remote_locality_id_differs"))
    proved_remote = bool(proved_oracle and id_differs and host_differs)

    # distinguish a MISSING attestation marker from a real wrong-interface/hostname mismatch
    if not (attest_root_present and attest_connector_present):
        blocked = "marker_missing"
    elif not host_differs:
        blocked = "hostname_match_or_endpoint_mismatch"
    else:
        blocked = None

    root_started = root_ready
    connector_joined = bool(_read_json(os.path.join(bootdir, "connect.joined1")))

    # surface log tails when a side did not start/join, to keep the diagnosis in the aggregate
    tails = {}
    if not root_started or not connector_joined:
        tails = {
            "root_stdout_tail": _tail(os.path.join(bootdir, "root.stdout")),
            "root_stderr_tail": _tail(os.path.join(bootdir, "root.stderr")),
            "connector_stdout_tail": _tail(os.path.join(bootdir, "connector.stdout")),
            "connector_stderr_tail": _tail(os.path.join(bootdir, "connector.stderr")),
        }

    no_orphans = True
    orphan_pids = []
    for nd in (nodeA, nodeB):
        ok, pids = _orphan_check_node(nd)
        if ok is False:
            no_orphans = False
            orphan_pids += [f"{nd}:{p}" for p in pids]

    sel.update({
        "root_started": root_started,
        "connector_joined": connector_joined,
        "agas_settle_ms": rr.get("settle_ms"),
        "action_proved_remote_by_oracle": proved_oracle,
        "remote_locality_id_differs": id_differs,
        "remote_hostname_or_ip_differs": host_differs,
        "action_proved_remote": proved_remote,
        "attest_root_present": attest_root_present,
        "attest_connector_present": attest_connector_present,
        "remote_proof_blocked_reason": blocked,
        "graceful_disconnect_clean": bool(disc.get("clean")),
        "root_finalized_clean": bool(root_rc == 0 and rr),
        "attest_root_hostname": (a_root or {}).get("hostname"),
        "attest_connector_hostname": (a_conn or {}).get("hostname"),
        "no_orphans": no_orphans,
        "orphan_pids": orphan_pids,
        "overall": "pass" if (proved_remote and bool(disc.get("clean")) and root_rc == 0
                              and no_orphans) else "fail",
    })
    sel.update(tails)
    return sel


# ---------------------------------------------------------------------------------------------------
# aggregate assembly
# ---------------------------------------------------------------------------------------------------
def _null_two_node():
    return {k: None for k in (
        "nodeA", "nodeB", "nodeA_ip", "nodeB_ip", "selected_interface", "selected_subnet",
        "bidirectional_port_check_passed", "root_started", "connector_joined", "agas_settle_ms",
        "action_proved_remote_by_oracle", "remote_locality_id_differs", "remote_hostname_or_ip_differs",
        "action_proved_remote", "graceful_disconnect_clean", "root_finalized_clean")}


def assemble(binary, cfg, loop, twonode, overall, reason):
    agg = {
        "experiment": "56_two_node_hpx_tcp_parcelport",
        "kind": "ray_free_two_node_hpx_tcp_connect_mode_probe",
        "ray_free": True, "transport": "tcp_parcelport",
        "binary": os.path.basename(binary) if binary else None,
        # --- Part 0 ---
        "tcp_parcelport_available": (cfg or {}).get("tcp_parcelport_available"),
        "tcp_parcelport_pinned": bool(cfg and cfg.get("tcp_parcelport_available")),
        "other_parcelports_disabled_or_not_selected": (
            not any((cfg or {}).get("other_parcelports_present", {}).values()) if cfg else None),
        "hpx_version": (cfg or {}).get("hpx_version"),
        "hpx_parcelport_config": (cfg or {}).get("hpx_parcelport_config"),
        "loopback_settle_diagnosed": (loop or {}).get("loopback_settle_diagnosed", False),
        "loopback_settle_ms": (loop or {}).get("loopback_settle_ms"),
        "loopback_advertised_addresses": (loop or {}).get("loopback_advertised_addresses"),
        "loopback_wrong_interface_advertised": (loop or {}).get("loopback_wrong_interface_advertised"),
        "loopback_markers_present": (loop or {}).get("loopback_markers_present"),
        "loopback_log_retry_or_resolve_signs": (loop or {}).get("loopback_log_retry_or_resolve_signs"),
        "loopback_root_log_tail": (loop or {}).get("loopback_root_log_tail"),
        "loopback_connector_log_tail": (loop or {}).get("loopback_connector_log_tail"),
        "loopback_settle_rootcause_note": (loop or {}).get("loopback_settle_rootcause_note"),
        "loopback_diag": (loop or {}).get("loopback_diag"),
    }
    # --- Part 1 ---
    if twonode:
        agg["two_node_run"] = True
        for k in ("nodeA", "nodeB", "nodeA_ip", "nodeB_ip", "selected_interface", "selected_subnet",
                  "bidirectional_port_check_passed", "root_started", "connector_joined",
                  "agas_settle_ms", "action_proved_remote_by_oracle", "remote_locality_id_differs",
                  "remote_hostname_or_ip_differs", "action_proved_remote", "graceful_disconnect_clean",
                  "root_finalized_clean"):
            agg[k] = twonode.get(k)
        agg["no_orphans"] = twonode.get("no_orphans")
        agg["orphan_pids"] = twonode.get("orphan_pids", [])
        for extra in ("root_argv", "connector_argv", "reachability_b_to_a", "reachability_a_to_b",
                      "attest_root_hostname", "attest_connector_hostname", "reason",
                      "bootstrap_dir", "shared_dir_source", "bootstrap_dir_node_local_warning",
                      "attest_root_present", "attest_connector_present", "remote_proof_blocked_reason",
                      "root_stdout_tail", "root_stderr_tail", "connector_stdout_tail",
                      "connector_stderr_tail"):
            if extra in twonode:
                agg[extra] = twonode[extra]
    else:
        agg["two_node_run"] = False
        agg.update(_null_two_node())
        agg["no_orphans"] = None
        agg["orphan_pids"] = []

    agg["overall"] = overall
    if reason:
        agg["skip_or_fail_reason"] = reason
    agg["note"] = (
        "Ray-free two-node HPX TCP parcelport connect-mode probe. Part 0 (TCP availability + loopback "
        "settle diagnostic) runs single-node; Part 1 (two-node remote proof) requires a >=2-node Slurm "
        "allocation and skips cleanly otherwise. agas_settle_ms / loopback_settle_ms are STRUCTURAL "
        "readiness durations, not latency/performance. TCP is load-bearing for connect-mode (MPI/LCI "
        "generally assume a static communicator); this proves the dynamic mechanism over TCP, not the "
        "HPC performance transport path."
    )
    agg["claim_fence"] = (
        "first two-node probe only; TCP parcelport only; closed-int64 action only; Ray-free; no "
        "performance/speedup/throughput/latency; no HPX fault tolerance; no Ray actor-failure recovery; "
        "no production/public API; no object store; no arbitrary Python; no Ray replacement; no general "
        "fabric claim from one TCP two-node probe; no MPI/LCI performance-path claim; future "
        "distributed-fabric direction only."
    )
    return agg


def _write_agg(path, agg):
    with open(path, "w") as fh:
        json.dump(agg, fh, indent=2, sort_keys=False)
        fh.write("\n")


def main():
    ap = argparse.ArgumentParser(description="exp56 two-node HPX TCP parcelport probe")
    ap.add_argument("--binary", default=None)
    ap.add_argument("--phase", choices=["check-config", "diag-loopback", "reachability", "run", "all"],
                    default="all")
    ap.add_argument("--ready-timeout", type=int, default=60)
    ap.add_argument("--leave-timeout", type=int, default=60)
    ap.add_argument("--serve-timeout", type=int, default=60)
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--agas-port", type=int, default=7910)
    ap.add_argument("--hpx-port", type=int, default=7911)
    ap.add_argument("--prefer-subnet", default=None,
                    help="IPv4 prefix to prefer when selecting the routable compute-node interface")
    ap.add_argument("--shared-dir", default=None,
                    help="shared-FS dir for two-node rendezvous; default is experiment-local "
                         "_two_node_runs/ (on /work when the repo lives there). NEVER node-local /tmp.")
    ap.add_argument("--aggregate", default=os.path.join(HERE, "aggregate.json"))
    args = ap.parse_args()

    binary = locate_binary(args.binary)
    if binary is None:
        _write_agg(args.aggregate, assemble(None, None, None, None, "skip",
                                            "two_node_tcp_spike not built (see CMakeLists.txt)"))
        print("SKIP: two_node_tcp_spike not found; build it first (CMakeLists.txt).")
        return 0
    print(f"[exp56] binary: {binary}")

    # Part 0a/b -- gate
    cfg = check_config(binary)
    print(f"[exp56] tcp_parcelport_available={cfg['tcp_parcelport_available']} "
          f"config=({cfg['hpx_parcelport_config']}) version={cfg['hpx_version']}")
    if not cfg["tcp_parcelport_available"]:
        _write_agg(args.aggregate, assemble(binary, cfg, None, None, "skip",
                                            "TCP parcelport not confirmed in this HPX build; STOP "
                                            "(no MPI/LCI substitution)."))
        print("SKIP: TCP parcelport not confirmed; STOP (no MPI/LCI substitution).")
        return 0
    if args.phase == "check-config":
        _write_agg(args.aggregate, assemble(binary, cfg, None, None, "skip", "check-config only"))
        print(f"[exp56] check-config done -> {args.aggregate}")
        return 0

    # Part 0c -- loopback diagnostic
    loop = None
    if args.phase in ("diag-loopback", "all"):
        print("[exp56] phase 0c: loopback settle diagnostic ...")
        loop = diag_loopback(binary, args, cfg)
        print(f"[exp56] loopback settle_ms={loop['loopback_settle_ms']} "
              f"advertised={loop['loopback_advertised_addresses']} "
              f"wrong_iface={loop['loopback_wrong_interface_advertised']} "
              f"note=({loop['loopback_settle_rootcause_note']})")
        if loop.get("loopback_wrong_interface_advertised"):
            _write_agg(args.aggregate, assemble(binary, cfg, loop, None, "fail",
                       "loopback diagnostic shows wrong-interface advertisement; fix endpoint "
                       "selection before two-node run"))
            print("FAIL: wrong-interface advertisement on loopback; fix endpoint selection first.")
            return 0
    if args.phase == "diag-loopback":
        _write_agg(args.aggregate, assemble(binary, cfg, loop, None, "skip",
                                            "diag-loopback only (Part 1 not attempted)"))
        print(f"[exp56] diag-loopback done -> {args.aggregate}")
        return 0

    # Part 1a -- reachability ONLY (pure socket preflight; no HPX launch)
    if args.phase == "reachability":
        print("[exp56] phase 1a: reachability (socket-only) ...")
        sel = select_and_reachability(args)
        if sel is None:
            _write_agg(args.aggregate, assemble(binary, cfg, loop, None, "skip",
                       "no >=2-node Slurm allocation; reachability deferred to a Rostam allocation"))
            print("SKIP: no >=2-node Slurm allocation; reachability deferred to Rostam.")
            return 0
        _write_agg(args.aggregate, assemble(binary, cfg, loop, sel, "skip",
                   sel.get("reason") or "reachability-only (no HPX launch)"))
        print(f"[exp56] reachability bidi={sel.get('bidirectional_port_check_passed')} "
              f"b_to_a={sel.get('reachability_b_to_a')} a_to_b={sel.get('reachability_a_to_b')} "
              f"nodes={sel.get('nodeA')}/{sel.get('nodeB')} ips={sel.get('nodeA_ip')}/"
              f"{sel.get('nodeB_ip')} -> {args.aggregate}")
        return 0

    # Part 1 -- two-node (requires >=2-node Slurm allocation)
    twonode = None
    if args.phase in ("run", "all"):
        print("[exp56] phase 1: two-node probe ...")
        twonode = run_two_node(binary, args, cfg)
        if twonode is None:
            _write_agg(args.aggregate, assemble(binary, cfg, loop, None, "skip",
                       "no >=2-node Slurm allocation; Part 0 (config + loopback) completed, Part 1 "
                       "deferred to a Rostam two-node allocation"))
            print("SKIP: no >=2-node Slurm allocation; Part 0 done, two-node deferred to Rostam.")
            return 0

    overall = twonode.get("overall", "fail")
    _write_agg(args.aggregate, assemble(binary, cfg, loop, twonode, overall, twonode.get("reason")))
    print(f"[exp56] overall={overall} proved_remote={twonode.get('action_proved_remote')} "
          f"settle_ms={twonode.get('agas_settle_ms')} no_orphans={twonode.get('no_orphans')} "
          f"-> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
