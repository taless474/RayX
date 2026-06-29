#!/usr/bin/env python3
"""exp57 -- Ray/Slurm-supervised two-node HPX island arc (orchestrator).

SLICE 0 + Slice A2b (this file implements the Ray-FREE settle-attribution diagnostic plus the
Ray/Slurm-supervised clean two-node HPX launch under --phase run; no failure/restart -- that is exp58).

  * --phase check-config : confirm the HPX build advertises the TCP parcelport (Part 0 gate).
  * --phase reachability : pure socket-only bidirectional preflight on a >=2-node Slurm allocation.
  * --phase settle-diag  : RAY-FREE two-node launch (root on node A, connector on node B) that
                           ATTRIBUTES the ~30 s readiness duration exp56 observed (loopback was
                           ~100 ms). It collects in-binary monotonic markers from both nodes, the
                           orchestrator's own shared-FS marker-visibility timings, and HPX/parcel log
                           tails, then CLASSIFIES where the delay goes. Writes settle_attribution.json.
  * --phase run          : Slice A2b -- Ray/Slurm-SUPERVISED clean two-node HPX launch. After the A2
                           preflight (pre-Ray env snapshot + preserve-list child env + both-node GCC15
                           `ldd` gate), Ray (lazy-imported here, NEVER at module top) issues the root and
                           connector `srun` from INSIDE Ray actors using the preserved child env, near-
                           concurrently and WITHOUT gating the connector on root.ready. The connector
                           uses its AGAS TCP pre-probe for readiness; HPX carries the closed-int64 action
                           over the TCP parcelport. All marker waits use the revalidating readers. NO
                           failure/restart (exp58). Writes run_aggregate.json (design_invariants +
                           measured results). Skips clean (rc 0) without a >=2-node allocation.

CLOCK-DOMAIN DISCIPLINE: three separate, non-comparable clocks are kept apart in the output:
  * root_in_binary      -- steady_clock deltas on the ROOT process (node A); the AUTHORITATIVE settle.
  * connector_in_binary -- steady_clock deltas on the CONNECTOR process (node B); NOT comparable to
                           the root clock (no NTP assumption across nodes).
  * supervisor_wall     -- this orchestrator's monotonic clock; shared-FS marker VISIBILITY only,
                           NEVER labeled AGAS settle.

CLAIM FENCE: first Ray/Slurm-supervised two-node island arc; Slice 0 is Ray-free; TCP parcelport only;
closed-int64 action only; no performance/speedup/throughput/latency claim; the settle is a STRUCTURAL
READINESS duration and is SUSPICIOUS until attributed (must NOT be reused for restart/detector timing
-- exp55's single-node calibration does not transfer to two-node); no HPX fault tolerance; no Ray
actor-failure recovery; no production/public API; no object store; no arbitrary Python; no Ray
replacement; no general fabric claim; no MPI/LCI performance-path claim. Future distributed-fabric
direction only.

Exit code 0 on clean skip/fail/attributed/inconclusive; non-zero only on an orchestrator-internal
error.
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
    os.path.join(HERE, "build", "two_node_island_spike"),
    os.path.join(HERE, "build", "Release", "two_node_island_spike"),
]
BINARY_BASENAME = "two_node_island_spike"

# TCP-pinning ini flags (key names confirmed via exp56 `--hpx:dump-config`). HPX REJECTS unknown ini
# keys (HPX(no_success)), so a parcelport may be disabled ONLY when it is actually present.
TCP_ENABLE_FLAGS = [
    "--hpx:ini=hpx.parcel.bootstrap=tcp",
    "--hpx:ini=hpx.parcel.tcp.enable=1",
]
PARCEL_LOG_FLAGS = ["--hpx:ini=hpx.logging.parcel.level=5"]

# log scan patterns for settle attribution
_RETRY_PATTERNS = ("retry", "retrying", "reconnect", "connection refused", "timed out",
                   "timeout", "backoff")
_RESOLVE_PATTERNS = ("resolve", "resolving", "getaddrinfo", "gethostbyname", "dns",
                     "reverse lookup", "name resolution")

# --- Slice A2 scaffold: PRE-RAY environment baseline ------------------------------------------------
# Captured at MODULE IMPORT time. This file NEVER imports Ray at module top (any Ray use in --phase run
# would be a lazy import inside that phase), so this snapshot is genuinely PRE-RAY and precedes any
# ray.init(). The deferred launch-from-Ray-actor slice will anchor srun children to this baseline so
# Ray's env rewrites (PATH / LD_LIBRARY_PATH / CUDA_VISIBLE_DEVICES / OMP_*) cannot strip the GCC15
# loader path off the HPX spike.
_PRE_RAY_ENV_SNAPSHOT = dict(os.environ)

# Load-bearing variables that MUST survive into srun children. This is a PRESERVE-LIST (allow-list),
# not a deny-list: a too-aggressive scrub that drops LD_LIBRARY_PATH makes the loader pick the system
# /usr/lib64 libstdc++ (GLIBCXX symbol-version failure against the GCC15-built spike); dropping the
# SLURM_JOB_* context detaches the step from the allocation.
_ENV_PRESERVE_KEYS = (
    "PATH", "LD_LIBRARY_PATH", "HOME", "TMPDIR",
    "SLURM_JOB_ID", "SLURM_JOB_NODELIST", "SLURM_NODELIST",
    "SLURM_JOB_NUM_NODES", "SLURM_EXPORT_ENV",
)
# Additional loader/toolchain vars the Rostam module flow may set; restored only when present in the
# baseline (Lmod bookkeeping is intentionally NOT here -- exp57 execs the binary directly, not via
# `bash -l`, so the already-expanded LD_LIBRARY_PATH is what matters, per the slice0 correction).
_ENV_PRESERVE_OPTIONAL = (
    "LD_PRELOAD", "LIBRARY_PATH", "CPATH", "CPLUS_INCLUDE_PATH", "PKG_CONFIG_PATH",
)
# Subset whose ABSENCE fails the scaffold preflight (the rest are best-effort).
_ENV_LOAD_BEARING_REQUIRED = (
    "PATH", "LD_LIBRARY_PATH", "HOME",
    "SLURM_JOB_ID", "SLURM_JOB_NODELIST", "SLURM_JOB_NUM_NODES",
)

# System libstdc++ locations that the GCC15 ldd gate must NOT resolve to (silent false-pass case).
_SYSTEM_LIBSTDCXX_PREFIXES = ("/lib64/", "/usr/lib64/", "/lib/", "/usr/lib/")


def tcp_pin_flags(cfg):
    """Force TCP and disable only the OTHER parcelports actually built in."""
    flags = list(TCP_ENABLE_FLAGS)
    for pp, present in (cfg or {}).get("parcelports_present", {}).items():
        if pp != "tcp" and present:
            flags.append(f"--hpx:ini=hpx.parcel.{pp}.enable=0")
    return flags


# ---------------------------------------------------------------------------------------------------
# helpers (shared shape with exp56)
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


def read_json_eventually(path, timeout_s=15.0, poll_s=0.25, required=False):
    """Robust marker read that tolerates shared-FS (NFS) visibility lag, which on the last Rostam run
    hid complete markers from the orchestrator and produced a FALSE 'fail'. Retries exists/open/parse,
    forces parent-directory revalidation between attempts (defeats negative-dentry / attribute caching),
    and handles missing / empty / partial-JSON files. Returns (obj_or_None, diag) where diag carries
    attempts / first_seen_ms / parsed_ms / last_error / file_size_at_parse / required / ok."""
    diag = {"path": os.path.basename(path), "attempts": 0, "first_seen_ms": None,
            "parsed_ms": None, "last_error": "missing", "file_size_at_parse": None,
            "required": bool(required), "ok": False}
    parent = os.path.dirname(path) or "."
    t0 = time.monotonic()
    deadline = t0 + max(0.0, timeout_s)
    while True:
        diag["attempts"] += 1
        try:                                  # force a fresh directory lookup (revalidate NFS cache)
            os.listdir(parent)
        except OSError:
            pass
        if os.path.exists(path):
            if diag["first_seen_ms"] is None:
                diag["first_seen_ms"] = int((time.monotonic() - t0) * 1000)
            try:
                with open(path) as f:
                    data = f.read()
                diag["file_size_at_parse"] = len(data)
                obj = json.loads(data)
                diag["parsed_ms"] = int((time.monotonic() - t0) * 1000)
                diag["last_error"] = None
                diag["ok"] = True
                return obj, diag
            except OSError as e:
                diag["last_error"] = "OSError: " + str(e)[:120]
            except ValueError as e:           # empty / partial / invalid JSON -> retry
                diag["last_error"] = "JSONDecodeError: " + str(e)[:120]
        else:
            diag["last_error"] = "missing"
        if time.monotonic() >= deadline:
            return None, diag
        time.sleep(poll_s)


def exists_eventually(path, timeout_s=15.0, poll_s=0.25):
    """Existence-only robust read for NON-JSON flag markers (e.g. served1.ok holds 'served\\n', not
    JSON). Same shared-FS revalidation as read_json_eventually, but returns as soon as the file appears
    -- it never burns the full timeout trying to JSON-parse a flag file. Returns (present_bool, diag)
    with the same metadata style: attempts / first_seen_ms / last_error / file_size_at_seen / ok."""
    diag = {"path": os.path.basename(path), "attempts": 0, "first_seen_ms": None,
            "last_error": "missing", "file_size_at_seen": None, "ok": False}
    parent = os.path.dirname(path) or "."
    t0 = time.monotonic()
    deadline = t0 + max(0.0, timeout_s)
    while True:
        diag["attempts"] += 1
        try:                                  # force a fresh directory lookup (revalidate NFS cache)
            os.listdir(parent)
        except OSError:
            pass
        if os.path.exists(path):
            diag["first_seen_ms"] = int((time.monotonic() - t0) * 1000)
            try:
                diag["file_size_at_seen"] = os.path.getsize(path)
            except OSError as e:
                diag["last_error"] = "OSError: " + str(e)[:120]
            else:
                diag["last_error"] = None
            diag["ok"] = True
            return True, diag
        diag["last_error"] = "missing"
        if time.monotonic() >= deadline:
            return False, diag
        time.sleep(poll_s)


def srun_launch_probe(node, timeout_s=120):
    """Measure how long a trivial one-task srun STEP takes to launch on `node` (independent of HPX).
    Supervisor-wall timing only. Returns (elapsed_ms, stdout_or_None)."""
    issue = time.monotonic()
    out = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "bash", "-lc", "date +%s.%N; hostname"],
               timeout=timeout_s)
    elapsed_ms = int((time.monotonic() - issue) * 1000)
    stdout = (out.stdout.strip() if (out and out.stdout) else None)
    return elapsed_ms, stdout


def _big(x, thr=5000):
    return x is not None and x >= thr


def _child_env():
    """Inherit the FULL environment. The binary is dynamically linked against HPX/Boost/hwloc and on
    Rostam resolves them via LD_LIBRARY_PATH set by `module load` (GCC 15 libstdc++ must be present);
    stripping loader vars breaks the binary on Rostam. SLURM_EXPORT_ENV=ALL should already be set by
    the caller so srun children inherit it."""
    return dict(os.environ)


def _run(argv, timeout=30):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except Exception:  # noqa: BLE001
        return None


def _timed_run(argv, timeout=120):
    """Supervisor-wall timed command (Slice A.0 localization). Returns elapsed_ms + rc + compact
    stdout/stderr tails. No HPX, no Ray; purely measures how long a shell/srun step takes to return."""
    t = time.monotonic()
    out = _run(argv, timeout=timeout)
    ms = int((time.monotonic() - t) * 1000)
    if out is None:
        return {"argv": " ".join(argv), "elapsed_ms": ms, "rc": None,
                "stdout_tail": None, "stderr_tail": None, "ok": False}
    return {"argv": " ".join(argv), "elapsed_ms": ms, "rc": out.returncode,
            "stdout_tail": ((out.stdout or "").strip()[-400:] or None),
            "stderr_tail": ((out.stderr or "").strip()[-400:] or None),
            "ok": out.returncode == 0}


def _tail(path, n=25):
    try:
        with open(path, errors="ignore") as f:
            lines = f.readlines()
    except OSError:
        return None
    return ("".join(lines[-n:]).strip() or None)


def _is_numeric_ipv4(ip):
    try:
        socket.inet_aton(ip or "")
        return bool(ip) and all(part.isdigit() for part in ip.split("."))
    except OSError:
        return False


def _resolve_shared_dir(args):
    """Two-node rendezvous MUST be on a shared filesystem -- node-local /tmp is invisible across nodes.
    Default to an experiment-local dir (on shared /work when the repo lives there)."""
    if args.shared_dir:
        return os.path.abspath(args.shared_dir), "explicit --shared-dir"
    return os.path.join(HERE, "_two_node_runs"), "default (experiment-local _two_node_runs)"


def _orphan_check_node(node):
    """pgrep exits 1 when there are NO matches -> the clean/no-orphans case, not an error."""
    o = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "pgrep", "-f", BINARY_BASENAME], timeout=20)
    if o is None:
        return None, []
    pids = [p for p in o.stdout.split() if p] if o.returncode == 0 else []
    return (len(pids) == 0), pids


def _base_hpx_flags(role, threads, agas_ip, agas_port, hpx_ip, hpx_port, pin_flags, extra=()):
    argv = [
        "--hpx:agas={}:{}".format(agas_ip, agas_port),
        "--hpx:hpx={}:{}".format(hpx_ip, hpx_port),
        "--hpx:threads={}".format(threads),
        "--hpx:bind=none",
        "--hpx:ignore-batch-env",
    ]
    argv += list(pin_flags)
    if role == "root":
        argv.append("--hpx:expect-connecting-localities")
    argv += list(extra)
    return argv


# ---------------------------------------------------------------------------------------------------
# Part 0a/b -- TCP parcelport availability + config key discovery
# ---------------------------------------------------------------------------------------------------
def check_config(binary):
    p = find_free_port()
    bd = tempfile.mkdtemp(prefix="exp57_cfg_")
    out = _run([binary, "--role", "root", "--bootstrap", bd, "--ready-timeout", "1",
                "--leave-timeout", "1", f"--hpx:agas=127.0.0.1:{p}", f"--hpx:hpx=127.0.0.1:{p}",
                "--hpx:ignore-batch-env", "--hpx:dump-config"], timeout=30)
    dump = (out.stdout + out.stderr) if out else ""
    ver = _run([binary, "--hpx:version"], timeout=20)
    version = ((ver.stdout + ver.stderr).strip().splitlines()[0] if ver and (ver.stdout or ver.stderr)
               else None)

    bootstrap = None
    m = re.search(r"'bootstrap'\s*:\s*'([^']+)'", dump)
    if m:
        bootstrap = m.group(1)
    present = {}
    for pp in ("tcp", "mpi", "lci", "gasnet"):
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
# node / IP selection + reachability (socket-only; no HPX launch)
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
    return cand[0]


def reachability_check(nodeA, nodeB, A_ip, B_ip, pagas, phpx):
    def probe(listen_node, listen_ip, port, connect_node, connect_ip):
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
    """Pick nodes/IPs and run the bidirectional reachability checks. Launches NO HPX. Returns a dict,
    or None when there is no >=2-node allocation."""
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


# ---------------------------------------------------------------------------------------------------
# Slice A.0 -- Ray-free Slurm / name-resolution localization of the ~25 s node-specific srun stall
# ---------------------------------------------------------------------------------------------------
_LOCALIZE_FENCE = (
    "exp57 Slice A.0 Ray-free localization diagnostic ONLY. It attributes WHERE the ~25 s node-specific "
    "srun launch stall comes from (name resolution / non-overlapping Slurm steps / node prolog-cgroup) "
    "before Slice A is built on top of it. It is NOT Slice A, NOT Ray supervision, NOT an HPX launch, "
    "NOT a performance/latency/throughput claim, NOT a fabric claim; supervisor-wall timing only."
)
_PROLOG_PATTERNS = ("prolog", "cgroup", "spank", "job_container", "task_p", "step creation",
                    "launch_tasks", "waiting for nodes", "node configuration", "cred")


def _overlap_unsupported(r):
    """True when --overlap was rejected by this Slurm (older than ~20.11), so it is not a real signal."""
    s = (r.get("stderr_tail") or "").lower()
    return (not r.get("ok")) and ("overlap" in s and
                                  ("unrecognized" in s or "invalid" in s or "no such option" in s))


def _ladder_5s(ms):
    """~5 s-multiple latency (5/10/15/20/25/30 ...) -- the classic resolver-retry timeout ladder."""
    if ms is None or ms < 4500:
        return False
    nearest = round(ms / 5000.0) * 5000
    return abs(ms - nearest) <= 1500


def _classify_localize(slow_no_ovl_ms, overlap_supported, overlap_delta_ms, slow_ovl_ms,
                       getent_ladder, vvv_prolog_hit):
    """Pick the localized cause from independent supervisor-wall signals. Order follows the approved
    guidance: overlap (directly actionable) -> resolver ladder -> prolog/cgroup -> inconclusive."""
    overlap_helps = bool(overlap_supported and _big(slow_no_ovl_ms) and overlap_delta_ms is not None
                         and overlap_delta_ms >= 5000 and slow_ovl_ms is not None
                         and slow_ovl_ms < slow_no_ovl_ms * 0.5)
    if overlap_helps:
        cause = "slurm_non_overlapping_steps"
        rec = ("add --overlap to the root and connector srun steps in Slice A (non-overlapping Slurm "
               "steps were serializing placement)")
    elif getent_ladder:
        cause = "name_resolution_or_reverse_dns"
        rec = ("fix/record host resolution (e.g. /etc/hosts or resolver config); HPX endpoints already "
               "use numeric IPs -- the stall is in srun/PMI host lookup, not HPX")
    elif _big(slow_no_ovl_ms) and vvv_prolog_hit:
        cause = "node_prolog_or_cgroup"
        rec = ("treat as irreducible per-node placement cost; Slice A should rely on concurrent srun "
               "issuance + the connector AGAS pre-probe rather than try to remove it")
    else:
        cause = "inconclusive"
        rec = "none determined; rerun A.0 and inspect srun_vvv_tail manually before designing Slice A"
    return cause, rec, overlap_helps


def slurm_localize(args):
    """RAY-FREE localization of the ~25 s node-specific srun stall. Launches NO HPX. Returns a findings
    dict (with bootstrap_dir for per-run preservation), or None when there is no >=2-node allocation."""
    nodes = slurm_nodes()
    if nodes is None:
        return None
    nodeA, nodeB = nodes
    ifaceA, A_ip = select_node_ip(nodeA, args.prefer_subnet)
    ifaceB, B_ip = select_node_ip(nodeB, args.prefer_subnet)

    shared, src = _resolve_shared_dir(args)
    os.makedirs(shared, exist_ok=True)
    bootdir = tempfile.mkdtemp(prefix="exp57_localize_", dir=shared)

    # (a) name resolution: node names + (when known) their selected IPs (forward + reverse)
    getent = {}
    targets = [("nodeA_name", nodeA), ("nodeB_name", nodeB)]
    if A_ip:
        targets.append(("nodeA_ip", A_ip))
    if B_ip:
        targets.append(("nodeB_ip", B_ip))
    for label, tgt in targets:
        getent[label] = _timed_run(["getent", "hosts", tgt], timeout=60)
    getent_ladder = any(_ladder_5s(getent[k]["elapsed_ms"]) for k in getent)

    # (b) trivial srun step launch, WITHOUT and WITH --overlap, per node
    srun_true = {
        "nodeA_no_overlap": _timed_run(["srun", "-N1", "-n1", "--nodelist=" + nodeA, "true"]),
        "nodeB_no_overlap": _timed_run(["srun", "-N1", "-n1", "--nodelist=" + nodeB, "true"]),
        "nodeA_overlap": _timed_run(["srun", "--overlap", "-N1", "-n1", "--nodelist=" + nodeA, "true"]),
        "nodeB_overlap": _timed_run(["srun", "--overlap", "-N1", "-n1", "--nodelist=" + nodeB, "true"]),
    }
    overlap_supported = not (_overlap_unsupported(srun_true["nodeA_overlap"])
                             or _overlap_unsupported(srun_true["nodeB_overlap"]))

    # (c) existing trivial date/hostname probe style (supervisor-wall), per node
    a_ms, a_out = srun_launch_probe(nodeA)
    b_ms, b_out = srun_launch_probe(nodeB)

    # determine the SLOW node from the no-overlap srun-true timings and compute the overlap delta there
    no_ovl = {"A": srun_true["nodeA_no_overlap"]["elapsed_ms"],
              "B": srun_true["nodeB_no_overlap"]["elapsed_ms"]}
    ovl = {"A": srun_true["nodeA_overlap"]["elapsed_ms"], "B": srun_true["nodeB_overlap"]["elapsed_ms"]}
    slow = "A" if (no_ovl["A"] or 0) >= (no_ovl["B"] or 0) else "B"
    slow_node = nodeA if slow == "A" else nodeB
    slow_no_ovl_ms, slow_ovl_ms = no_ovl[slow], ovl[slow]
    overlap_delta_ms = (slow_no_ovl_ms - slow_ovl_ms
                        if (overlap_supported and slow_no_ovl_ms is not None and slow_ovl_ms is not None)
                        else None)

    # (d) one bounded/noisy srun -vvv on the slow node; full output to the ignored bootdir, tail in JSON
    vvv_path = os.path.join(bootdir, "srun_vvv_slow_node.txt")
    tvvv = time.monotonic()
    vvv_out = _run(["srun", "-vvv", "-N1", "-n1", "--nodelist=" + slow_node, "hostname"], timeout=120)
    vvv_ms = int((time.monotonic() - tvvv) * 1000)
    vvv_full = (((vvv_out.stdout or "") + (vvv_out.stderr or "")) if vvv_out else "")
    with open(vvv_path, "w") as fh:
        fh.write(vvv_full)
    srun_vvv_tail = _tail(vvv_path, n=30)
    vvv_prolog_hit = any(p in (srun_vvv_tail or "").lower() for p in _PROLOG_PATTERNS)

    cause, rec, overlap_helps = _classify_localize(slow_no_ovl_ms, overlap_supported, overlap_delta_ms,
                                                   slow_ovl_ms, getent_ladder, vvv_prolog_hit)

    return {
        "bootstrap_dir": bootdir,
        "shared_dir_source": src,
        "run_id": os.path.basename(bootdir),
        "nodeA": nodeA, "nodeB": nodeB, "nodeA_ip": A_ip, "nodeB_ip": B_ip,
        "selected_interface": (f"{ifaceA}/{ifaceB}" if (ifaceA and ifaceB) else None),
        "selected_subnet": args.prefer_subnet or (A_ip.rsplit('.', 1)[0] + ".0/24" if A_ip else None),
        "slow_node": slow_node,
        "getent": getent,
        "getent_ladder_suspected": getent_ladder,
        "srun_true": srun_true,
        "srun_true_no_overlap_ms": {"nodeA": no_ovl["A"], "nodeB": no_ovl["B"]},
        "srun_true_overlap_ms": {"nodeA": ovl["A"], "nodeB": ovl["B"]},
        "overlap_supported": overlap_supported,
        "overlap_delta_ms": overlap_delta_ms,
        "overlap_helps": overlap_helps,
        "srun_probe_nodeA_ms": a_ms, "srun_probe_nodeB_ms": b_ms,
        "srun_probe_nodeA_stdout": a_out, "srun_probe_nodeB_stdout": b_out,
        "srun_vvv_slow_node_ms": vvv_ms,
        "srun_vvv_tail": srun_vvv_tail,
        "localized_cause": cause,
        "recommended_slice_a_srun_flags": rec,
        "overall": ("localized" if cause != "inconclusive" else "inconclusive"),
        "claim_fence": _LOCALIZE_FENCE,
    }


_LOCALIZE_DEEP_FENCE = (
    "exp57 Slice A.0-deep Ray-free localization ONLY. It SEPARATES login-shell startup (off the real "
    "launch path) from direct-binary/loader cost and shared-FS marker write->visibility lag, because "
    "the earlier A.0 slow probe used a login shell (bash -lc) the real HPX launch (direct srun exec of "
    "the binary) does NOT use. It is NOT Slice A, NOT Ray supervision, NOT an HPX/AGAS timing or "
    "performance/latency/fabric claim; all timings are supervisor-wall, and writer_unix stamps are "
    "NEVER subtracted across nodes authoritatively.")


def _classify_localize_deep(login_max, direct_max, binary_max, marker_max, getent_ladder, vvv_prolog):
    """Separate the candidate causes. Real-launch-path signals (direct binary/loader, shared-FS marker
    visibility) take precedence; login-shell is reported but treated as OFF the real launch path (the
    HPX binary is exec'd directly, no bash -l). Conservative: if BOTH real-path signals are slow ->
    inconclusive. Returns (localized_cause, compound_suspect_list, recommendation)."""
    login_slow = _big(login_max)
    direct_slow = _big(direct_max)
    binary_slow = _big(binary_max)
    marker_slow = _big(marker_max)

    compound = []
    if getent_ladder:
        compound.append("name_resolution_or_reverse_dns")
    if login_slow:
        compound.append("login_shell_startup")
    if binary_slow:
        compound.append("binary_direct_exec_or_loader")
    if marker_slow:
        compound.append("shared_fs_marker_visibility")
    if direct_slow and vvv_prolog:
        compound.append("node_prolog_or_cgroup")

    multi_real_path = (1 if binary_slow else 0) + (1 if marker_slow else 0) > 1
    if multi_real_path:
        cause = "inconclusive"
    elif marker_slow and not binary_slow and not direct_slow:
        cause = "shared_fs_marker_visibility"
    elif binary_slow and not direct_slow:
        cause = "binary_direct_exec_or_loader"
    elif login_slow and not binary_slow and not marker_slow and not direct_slow:
        cause = "login_shell_startup"
    elif direct_slow and vvv_prolog:
        cause = "node_prolog_or_cgroup"
    elif getent_ladder:
        cause = "name_resolution_or_reverse_dns"
    else:
        cause = "inconclusive"

    rec = {
        "shared_fs_marker_visibility":
            "Slice A must NOT gate connector launch on a /work marker's VISIBILITY; issue root+connector "
            "srun concurrently and use a direct TCP/AGAS readiness signal (the planned connector pre-"
            "probe), not marker polling. This also likely explains Slice 0's ~30 s.",
        "binary_direct_exec_or_loader":
            "first HPX-binary exec per node pays this NFS dynamic-load/pre-main cost; stage/warm the "
            "binary or accept it as bounded startup -- it is NOT HPX/AGAS settle and NOT a perf claim.",
        "login_shell_startup":
            "OFF the real launch path: the HPX binary is exec'd directly (no bash -l), so this login-"
            "shell cost does NOT apply to Slice A launches. If the binary + marker probes are fast, "
            "Slice 0's ~30 s is NOT explained by these probes and must be re-opened.",
        "node_prolog_or_cgroup":
            "per-node placement cost; Slice A relies on concurrent issuance + connector AGAS pre-probe.",
        "name_resolution_or_reverse_dns":
            "fix/record host resolution; HPX endpoints already use numeric IPs.",
        "inconclusive":
            "multiple or no clear signals; inspect compound_suspect + srun_vvv_tail before finalizing "
            "the Slice 0 attribution or starting Slice A.",
    }[cause]
    return cause, compound, rec


def slurm_localize_deep(args, binary):
    """RAY-FREE deep localization: separate login-shell vs direct-binary/loader vs shared-FS marker
    visibility. Launches NO HPX island (the binary probe is --hpx:version only). Returns a findings
    dict (with bootstrap_dir), or None when there is no >=2-node allocation."""
    nodes = slurm_nodes()
    if nodes is None:
        return None
    nodeA, nodeB = nodes
    ifaceA, A_ip = select_node_ip(nodeA, args.prefer_subnet)
    ifaceB, B_ip = select_node_ip(nodeB, args.prefer_subnet)
    shared, src = _resolve_shared_dir(args)
    os.makedirs(shared, exist_ok=True)
    bootdir = tempfile.mkdtemp(prefix="exp57_localize_deep_", dir=shared)

    # quick getent (keep DNS ruled out in the deep run too)
    getent = {"nodeA_name": _timed_run(["getent", "hosts", nodeA], timeout=60),
              "nodeB_name": _timed_run(["getent", "hosts", nodeB], timeout=60)}
    getent_ladder = any(_ladder_5s(getent[k]["elapsed_ms"]) for k in getent)

    # (1) login-shell probe (bash -lc): the A.0 slow path
    login_A = _timed_run(["srun", "-N1", "-n1", "--nodelist=" + nodeA, "bash", "-lc",
                          "date +%s.%N; hostname"])
    login_B = _timed_run(["srun", "-N1", "-n1", "--nodelist=" + nodeB, "bash", "-lc",
                          "date +%s.%N; hostname"])
    # (2) direct non-login shell probe
    direct_A = _timed_run(["srun", "-N1", "-n1", "--nodelist=" + nodeA, "/bin/hostname"])
    direct_B = _timed_run(["srun", "-N1", "-n1", "--nodelist=" + nodeB, "/bin/hostname"])

    # (3) actual exp57 binary direct-exec probe (--hpx:version: no role/AGAS/port, exercises NFS
    #     dynamic-load/pre-main cost; the runner already uses --hpx:version standalone in check_config)
    if binary:
        bin_argv_A = ["srun", "-N1", "-n1", "--nodelist=" + nodeA, binary, "--hpx:version"]
        bin_argv_B = ["srun", "-N1", "-n1", "--nodelist=" + nodeB, binary, "--hpx:version"]
        binp_A = _timed_run(bin_argv_A, timeout=120)
        binp_B = _timed_run(bin_argv_B, timeout=120)
        binary_probe_argv = " ".join(bin_argv_A)
        binary_probe_available = True
        binary_probe_note = ("srun execs the actual exp57 binary directly (NO bash -l); --hpx:version "
                             "exits after HPX command-line handling, so it exercises the NFS dynamic-"
                             "load / pre-main startup cost without role/AGAS/port state.")
    else:
        binp_A = binp_B = {"argv": None, "elapsed_ms": None, "rc": None,
                           "stdout_tail": None, "stderr_tail": None, "ok": False}
        binary_probe_argv = None
        binary_probe_available = False
        binary_probe_note = ("two_node_island_spike not built; no safe direct binary probe was run and "
                             "NO C++ was added. Build the binary (Rostam-native) and rerun for probe 3.")

    # (4) shared-FS marker write->visibility probe (the Slice 0 NFS-visibility hypothesis)
    def _vis(node, label):
        mp = os.path.join(bootdir, "vis_%s.json" % label)
        py = ("import json,os,time;p=%r;f=open(p,'w');"
              "json.dump({'writer_node':os.uname()[1],'writer_unix':time.time()},f);"
              "f.flush();os.fsync(f.fileno());f.close()") % mp
        t0 = time.monotonic()
        w = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "python3", "-c", py], timeout=90)
        obj, diag = read_json_eventually(mp, timeout_s=90, poll_s=0.1)
        return {"ms": int((time.monotonic() - t0) * 1000), "attempts": diag["attempts"],
                "visible": diag["ok"], "writer_ok": bool(w and w.returncode == 0),
                "writer_unix": (obj or {}).get("writer_unix")}
    vis_A = _vis(nodeA, "nodeA")
    vis_B = _vis(nodeB, "nodeB")

    # (5) srun -vvv on the node that is SLOW by login/binary (fix the A.0 tie-break blind spot), bash -l
    scoreA = max(login_A["elapsed_ms"] or 0, binp_A["elapsed_ms"] or 0)
    scoreB = max(login_B["elapsed_ms"] or 0, binp_B["elapsed_ms"] or 0)
    vvv_node = nodeA if scoreA >= scoreB else nodeB
    vvv_reason = ("chose the node with the larger max(login_shell_ms, binary_probe_ms): "
                  "%s=%d vs %s=%d (NOT srun-true, which tie-broke wrong in A.0)"
                  % (nodeA, scoreA, nodeB, scoreB))
    vvv_path = os.path.join(bootdir, "srun_vvv_deep.txt")
    tvvv = time.monotonic()
    vvv_out = _run(["srun", "-vvv", "-N1", "-n1", "--nodelist=" + vvv_node, "bash", "-lc",
                    "date +%s.%N; hostname"], timeout=120)
    vvv_ms = int((time.monotonic() - tvvv) * 1000)
    with open(vvv_path, "w") as fh:
        fh.write((((vvv_out.stdout or "") + (vvv_out.stderr or "")) if vvv_out else ""))
    vvv_tail = _tail(vvv_path, n=30)
    vvv_prolog = any(p in (vvv_tail or "").lower() for p in _PROLOG_PATTERNS)

    login_max = max(login_A["elapsed_ms"] or 0, login_B["elapsed_ms"] or 0)
    direct_max = max(direct_A["elapsed_ms"] or 0, direct_B["elapsed_ms"] or 0)
    binary_max = (max(binp_A["elapsed_ms"] or 0, binp_B["elapsed_ms"] or 0) if binary else None)
    marker_max = max(vis_A["ms"] or 0, vis_B["ms"] or 0)
    cause, compound, rec = _classify_localize_deep(login_max, direct_max, binary_max, marker_max,
                                                   getent_ladder, vvv_prolog)

    return {
        "bootstrap_dir": bootdir, "shared_dir_source": src, "run_id": os.path.basename(bootdir),
        "nodeA": nodeA, "nodeB": nodeB, "nodeA_ip": A_ip, "nodeB_ip": B_ip,
        "selected_interface": (f"{ifaceA}/{ifaceB}" if (ifaceA and ifaceB) else None),
        "selected_subnet": args.prefer_subnet or (A_ip.rsplit('.', 1)[0] + ".0/24" if A_ip else None),
        "getent": getent, "getent_ladder_suspected": getent_ladder,
        # (1) login-shell
        "srun_login_shell_nodeA_ms": login_A["elapsed_ms"],
        "srun_login_shell_nodeB_ms": login_B["elapsed_ms"],
        "srun_login_shell_nodeA_stdout": login_A["stdout_tail"],
        "srun_login_shell_nodeB_stdout": login_B["stdout_tail"],
        "srun_login_shell_nodeA_stderr": login_A["stderr_tail"],
        "srun_login_shell_nodeB_stderr": login_B["stderr_tail"],
        # (2) direct non-login
        "srun_direct_hostname_nodeA_ms": direct_A["elapsed_ms"],
        "srun_direct_hostname_nodeB_ms": direct_B["elapsed_ms"],
        # (3) binary direct-exec
        "binary_probe_available": binary_probe_available,
        "binary_probe_argv": binary_probe_argv,
        "binary_probe_note": binary_probe_note,
        "srun_binary_probe_nodeA_ms": binp_A["elapsed_ms"],
        "srun_binary_probe_nodeB_ms": binp_B["elapsed_ms"],
        "srun_binary_probe_nodeA_rc": binp_A["rc"], "srun_binary_probe_nodeB_rc": binp_B["rc"],
        "srun_binary_probe_nodeA_stderr": binp_A["stderr_tail"],
        "srun_binary_probe_nodeB_stderr": binp_B["stderr_tail"],
        # (4) shared-FS marker visibility
        "marker_visibility_nodeA_ms": vis_A["ms"], "marker_visibility_nodeB_ms": vis_B["ms"],
        "marker_visibility_nodeA_attempts": vis_A["attempts"],
        "marker_visibility_nodeB_attempts": vis_B["attempts"],
        "marker_visibility_nodeA_visible": vis_A["visible"],
        "marker_visibility_nodeB_visible": vis_B["visible"],
        "marker_visibility_note": (
            "supervisor-observed visibility of a remote fsync+close JSON marker over shared /work "
            "(close-to-open); the ms INCLUDES srun exec time. This is NOT HPX/AGAS timing. writer_unix "
            "is a cross-node wall stamp recorded for context and is NEVER subtracted authoritatively."),
        # (5) vvv on the correct slow node
        "srun_vvv_node": vvv_node, "srun_vvv_reason_for_node_choice": vvv_reason,
        "srun_vvv_ms": vvv_ms, "srun_vvv_tail": vvv_tail,
        # verdict
        "localized_cause": cause, "compound_suspect": compound,
        "recommended_slice_a_srun_flags": rec,
        "overall": ("localized" if cause != "inconclusive" else "inconclusive"),
        "claim_fence": _LOCALIZE_DEEP_FENCE,
    }


_NFS_NEG_FENCE = (
    "exp57 NFS negative-dentry / pre-existence-poll diagnostic ONLY. It tests whether the SUPERVISOR "
    "polling for a /work marker BEFORE it exists delays visibility by ~25-30 s (the leading hypothesis "
    "for Slice 0's re-opened ~30 s, after slurm_step_launch_latency was superseded). It is NOT Slice A, "
    "NOT Ray, NOT an HPX/AGAS settle, NOT a network-latency, fabric, or general-filesystem-performance "
    "claim. It is a control-path / marker-polling artifact check under THIS launch model only; "
    "writer_unix is a cross-node wall stamp never subtracted authoritatively.")


def _nfs_writer_argv(node, marker_path):
    """Direct srun (NO bash -l) python writer: create + fsync + close the marker, then exit."""
    py = ("import json,os,time;p=%r;f=open(p,'w');"
          "json.dump({'writer_node':os.uname()[1],'writer_pid':os.getpid(),"
          "'writer_unix':time.time(),'marker':os.path.basename(p)},f);"
          "f.flush();os.fsync(f.fileno());f.close()") % marker_path
    return ["srun", "-N1", "-n1", "--nodelist=" + node, "python3", "-c", py]


def _negative_poll_probe(marker_path, node, pre_existence_s, revalidate, post_timeout_s=90,
                         poll_s=0.1):
    """Poll for `marker_path` for `pre_existence_s` BEFORE it exists (seeding any negative-dentry /
    dir-attribute cache), then issue a direct-srun writer that creates+fsyncs it, then keep polling
    until visible. `revalidate=True` mirrors read_json_eventually (os.listdir parent each attempt);
    `revalidate=False` mirrors Slice 0's LIVE gating loop (plain os.path.exists, no revalidation).
    All timings are supervisor-wall; visible_ms is measured from WRITER ISSUE."""
    parent = os.path.dirname(marker_path) or "."
    attempts = 0
    failed_before = 0
    t_pre = time.monotonic()
    while time.monotonic() - t_pre < pre_existence_s:        # pre-existence negative polling
        attempts += 1
        if revalidate:
            try:
                os.listdir(parent)
            except OSError:
                pass
        if os.path.exists(marker_path):                      # unexpected this early
            break
        failed_before += 1
        time.sleep(poll_s)

    t_issue = time.monotonic()
    w = _run(_nfs_writer_argv(node, marker_path), timeout=post_timeout_s)
    writer_ok = bool(w and w.returncode == 0)

    first_seen_ms = parsed_ms = file_size = None
    last_error = "missing"
    ok = False
    obj = None
    failed_after = 0
    deadline = t_issue + post_timeout_s
    while True:                                              # post-write polling until visible
        attempts += 1
        if revalidate:
            try:
                os.listdir(parent)
            except OSError:
                pass
        if os.path.exists(marker_path):
            if first_seen_ms is None:
                first_seen_ms = int((time.monotonic() - t_issue) * 1000)
            try:
                with open(marker_path) as f:
                    data = f.read()
                file_size = len(data)
                obj = json.loads(data)
                parsed_ms = int((time.monotonic() - t_issue) * 1000)
                last_error = None
                ok = True
                break
            except OSError as e:
                last_error = "OSError: " + str(e)[:120]
            except ValueError as e:
                last_error = "JSONDecodeError: " + str(e)[:120]
        else:
            failed_after += 1
            last_error = "missing"
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_s)

    visible_ms = (parsed_ms if parsed_ms is not None
                  else first_seen_ms if first_seen_ms is not None
                  else int((time.monotonic() - t_issue) * 1000))
    return {
        "node": node, "marker": os.path.basename(marker_path),
        "parent_dir_revalidation": bool(revalidate), "pre_existence_poll_s": pre_existence_s,
        "writer_ok": writer_ok, "writer_unix": (obj or {}).get("writer_unix"),
        "failed_polls_before_write": failed_before, "failed_polls_after_write": failed_after,
        "attempts": attempts, "first_seen_ms": first_seen_ms, "parsed_ms": parsed_ms,
        "visible_ms": visible_ms, "file_size_at_parse": file_size, "last_error": last_error, "ok": ok,
    }


def _classify_nfs_negative(ctrl_max, neg_simple_max, neg_robust_max):
    """The SIMPLE (exists-only) negative poll replicates Slice 0's live gating; the control should be
    ~100 ms. Returns (localized_cause, negative_poll_delay_seen, negative_poll_delay_ms,
    revalidation_appears_to_help)."""
    delay_seen = bool(_big(neg_simple_max) and not _big(ctrl_max))
    if delay_seen:
        cause = "nfs_negative_dentry_or_attribute_cache"
    elif not _big(neg_simple_max) and not _big(neg_robust_max) and not _big(ctrl_max):
        cause = "no_negative_poll_delay_observed"
    else:
        cause = "inconclusive"
    reval_helps = bool(_big(neg_simple_max) and not _big(neg_robust_max))
    return cause, delay_seen, neg_simple_max, reval_helps


def nfs_negative_poll(args):
    """RAY-FREE supervisor-side NFS negative-dentry / pre-existence-poll test. Launches NO HPX. Returns
    a findings dict (with bootstrap_dir), or None when there is no >=2-node allocation."""
    nodes = slurm_nodes()
    if nodes is None:
        return None
    nodeA, nodeB = nodes
    ifaceA, A_ip = select_node_ip(nodeA, args.prefer_subnet)
    ifaceB, B_ip = select_node_ip(nodeB, args.prefer_subnet)
    shared, src = _resolve_shared_dir(args)
    os.makedirs(shared, exist_ok=True)
    bootdir = tempfile.mkdtemp(prefix="exp57_nfsneg_", dir=shared)
    pre_s = args.neg_poll_pre_s

    per_node = {}
    for tag, node in (("nodeA", nodeA), ("nodeB", nodeB)):
        # A. control (no pre-existence negative polling); B. negative robust; C. negative simple (Slice 0 replica)
        ctrl = _negative_poll_probe(os.path.join(bootdir, f"ctrl_{tag}.json"), node,
                                    pre_existence_s=0, revalidate=True)
        neg_robust = _negative_poll_probe(os.path.join(bootdir, f"neg_robust_{tag}.json"), node,
                                          pre_existence_s=pre_s, revalidate=True)
        neg_simple = _negative_poll_probe(os.path.join(bootdir, f"neg_simple_{tag}.json"), node,
                                          pre_existence_s=pre_s, revalidate=False)
        per_node[tag] = {"control": ctrl, "negative_robust": neg_robust, "negative_simple": neg_simple}

    ctrl_max = max(per_node[t]["control"]["visible_ms"] or 0 for t in per_node)
    neg_simple_max = max(per_node[t]["negative_simple"]["visible_ms"] or 0 for t in per_node)
    neg_robust_max = max(per_node[t]["negative_robust"]["visible_ms"] or 0 for t in per_node)
    cause, delay_seen, delay_ms, reval_helps = _classify_nfs_negative(ctrl_max, neg_simple_max,
                                                                      neg_robust_max)

    return {
        "bootstrap_dir": bootdir, "shared_dir_source": src, "run_id": os.path.basename(bootdir),
        "nodeA": nodeA, "nodeB": nodeB, "nodeA_ip": A_ip, "nodeB_ip": B_ip,
        "selected_interface": (f"{ifaceA}/{ifaceB}" if (ifaceA and ifaceB) else None),
        "selected_subnet": args.prefer_subnet or (A_ip.rsplit('.', 1)[0] + ".0/24" if A_ip else None),
        "pre_existence_poll_s": pre_s,
        "per_node": per_node,
        "control_visible_ms": {"nodeA": per_node["nodeA"]["control"]["visible_ms"],
                               "nodeB": per_node["nodeB"]["control"]["visible_ms"]},
        "negative_simple_visible_ms": {"nodeA": per_node["nodeA"]["negative_simple"]["visible_ms"],
                                       "nodeB": per_node["nodeB"]["negative_simple"]["visible_ms"]},
        "negative_robust_visible_ms": {"nodeA": per_node["nodeA"]["negative_robust"]["visible_ms"],
                                       "nodeB": per_node["nodeB"]["negative_robust"]["visible_ms"]},
        "control_visible_max_ms": ctrl_max,
        "negative_simple_visible_max_ms": neg_simple_max,
        "negative_robust_visible_max_ms": neg_robust_max,
        "negative_poll_delay_seen": delay_seen,
        "negative_poll_delay_ms": delay_ms,
        "revalidation_appears_to_help": reval_helps,
        "simple_variant_replicates_slice0_gating": True,
        "localized_cause": cause,
        "overall": ("localized" if cause != "inconclusive" else "inconclusive"),
        "claim_fence": _NFS_NEG_FENCE,
    }


def _preserve_named_artifacts(obj, bootdir, basename, index_name, row):
    """Generic per-run preservation (mirrors _preserve_run_artifacts): write the FULL obj into the
    bootdir and append a one-line index, both under the ignored shared run dir. No-op without a bootdir.
    Returns (per_run_path, index_path) or (None, None)."""
    if not bootdir or not os.path.isdir(bootdir):
        return None, None
    per_run_path = os.path.join(bootdir, basename)
    _write_agg(per_run_path, obj)
    index_path = os.path.join(os.path.dirname(bootdir), index_name)
    with open(index_path, "a") as fh:
        fh.write(json.dumps(row, sort_keys=False) + "\n")
    return per_run_path, index_path


# ---------------------------------------------------------------------------------------------------
# Slice 0 -- two-node settle attribution (RAY-FREE)
# ---------------------------------------------------------------------------------------------------
def _scan_logs_for(bootdir, patterns):
    """Return (hit_bool, sorted hit tags) across all stdout/stderr/log files in bootdir."""
    hits = []
    try:
        names = os.listdir(bootdir)
    except OSError:
        return False, []
    for fn in names:
        if not (fn.endswith(".stdout") or fn.endswith(".stderr") or fn.endswith(".log")):
            continue
        try:
            with open(os.path.join(bootdir, fn), errors="ignore") as fh:
                txt = fh.read().lower()
        except OSError:
            continue
        for pat in patterns:
            if pat in txt:
                hits.append(f"{fn}:{pat}")
    return (len(hits) > 0), sorted(set(hits))


def _classify_settle(s):
    """Attribute the settle delay ONLY when a concrete signal supports it; otherwise inconclusive.

    Authoritative span: s['settle_ms'] (root_in_binary, single clock). The added Slurm signal compares
    that against independent supervisor-wall measurements (srun launch probes + srun-issue-to-first-
    marker), so a ~30 s root settle that coincides with ~30 s srun step launch -- while the root is
    ready early and the connector's in-binary JOIN is sub-second once the connector PROCESS exists --
    is named `slurm_step_launch_latency` rather than an HPX/AGAS property. The launch-speed signal is
    the connector-local JOIN span (conn_joined_ms: process start -> locality observable), NOT the full
    connector lifecycle (conn_lifecycle_ms), which folds in serve-wait and is polluted whenever the
    root observes/serves the peer late (so a fast-launching connector can still show a ~30 s lifecycle).
    If conn_joined_ms is missing/ambiguous we do NOT infer slurm_step_launch_latency from root settle
    alone. Log-based classes
    (parcel_retry_or_timeout / name_resolution_or_reverse_dns) require TRUSTWORTHY logs; empty logs are
    NOT a clean negative and never attribute by themselves. Returns (classification, attributed,
    suspects_str)."""
    settle = s.get("settle_ms")
    logs_trustworthy = bool(s.get("logs_trustworthy"))
    retry_seen = bool(s.get("retry_seen"))
    resolve_seen = bool(s.get("resolve_seen"))

    settle_big = _big(settle)
    root_early = s.get("root_ready_ms") is not None and s["root_ready_ms"] < 2000
    # connector-local JOIN span (process start -> locality observable), NOT the full lifecycle: the
    # lifecycle includes serve-wait and is polluted when the root serves the peer late. Missing/None ->
    # ambiguous -> NOT fast (stay conservative; do not infer slurm latency from root settle alone).
    conn_joined_fast = s.get("conn_joined_ms") is not None and s["conn_joined_ms"] < 2000
    slurm_signal = (_big(s.get("srun_probe_a_ms")) or _big(s.get("srun_probe_b_ms"))
                    or _big(s.get("root_srun_to_ready_ms")) or _big(s.get("conn_srun_to_join_ms")))

    conn_visible = s.get("conn_join_visible_ms")
    conn_up_early_vs_root = (conn_visible is not None and settle is not None
                             and conn_visible < max(2000, settle * 0.5))

    slurm_match = root_early and conn_joined_fast and settle_big and slurm_signal
    agas_match = settle_big and conn_up_early_vs_root and not slurm_signal

    suspects = []
    if logs_trustworthy and retry_seen:
        suspects.append("parcel_retry_or_timeout")
    if logs_trustworthy and resolve_seen:
        suspects.append("name_resolution_or_reverse_dns")
    if slurm_match:
        suspects.append("slurm_step_launch_latency")
    if agas_match:
        suspects.append("agas_registration_reflection_delay")
    if not suspects:
        suspects = ["slurm_step_launch_latency", "name_resolution_or_reverse_dns",
                    "parcel_retry_or_timeout", "agas_registration_reflection_delay"]

    if logs_trustworthy and retry_seen:
        return "parcel_retry_or_timeout", True, ", ".join(suspects)
    if logs_trustworthy and resolve_seen:
        return "name_resolution_or_reverse_dns", True, ", ".join(suspects)
    if slurm_match:
        return "slurm_step_launch_latency", True, ", ".join(suspects)
    if agas_match:
        return "agas_registration_reflection_delay", True, ", ".join(suspects)
    return "unattributed_fixed_delay", False, ", ".join(suspects)


def settle_diag(binary, args, cfg):
    """RAY-FREE two-node launch + settle attribution. Returns a findings dict, or None when there is
    no >=2-node allocation."""
    sel = select_and_reachability(args)
    if sel is None:
        return None
    if not sel.get("bidirectional_port_check_passed"):
        sel.setdefault("overall", "fail")
        sel.setdefault("reason", "bidirectional reachability failed; HPX not launched")
        return sel
    return _launch_settle_diag(binary, args, cfg, sel)


def _launch_settle_diag(binary, args, cfg, sel):
    pin = tcp_pin_flags(cfg)
    extra = PARCEL_LOG_FLAGS if args.hpx_logs else ()
    nodeA, nodeB = sel["nodeA"], sel["nodeB"]
    A_ip, B_ip = sel["nodeA_ip"], sel["nodeB_ip"]
    pagas, phpx = args.agas_port, args.hpx_port

    shared, src = _resolve_shared_dir(args)
    os.makedirs(shared, exist_ok=True)
    bootdir = tempfile.mkdtemp(prefix="exp57_settle_", dir=shared)
    sel["bootstrap_dir"] = bootdir
    sel["shared_dir_source"] = src
    sel["bootstrap_dir_node_local_warning"] = bool(bootdir.startswith("/tmp") or "/tmp/" in bootdir)

    intended_root_ep = f"{A_ip}:{pagas}"
    intended_conn_ep = f"{B_ip}:{phpx}"
    sel["intended_root_endpoint"] = intended_root_ep
    sel["intended_connector_endpoint"] = intended_conn_ep
    sel["numeric_ip_used"] = bool(_is_numeric_ipv4(A_ip) and _is_numeric_ipv4(B_ip))
    sel["preflight_tested_same_endpoint"] = True  # reachability used the same A_ip:pagas / B_ip:phpx

    # --- (3) explicit srun launch-latency probes (independent of HPX; supervisor-wall only) ---
    a_ms, a_out = srun_launch_probe(nodeA)
    b_ms, b_out = srun_launch_probe(nodeB)
    sel["srun_probe_nodeA_ms"] = a_ms
    sel["srun_probe_nodeB_ms"] = b_ms
    sel["srun_probe_nodeA_stdout"] = a_out
    sel["srun_probe_nodeB_stdout"] = b_out

    root_argv = ["srun", "-N1", "-n1", "--nodelist=" + nodeA, binary,
                 "--role", "root", "--bootstrap", bootdir, "--x", "7",
                 "--ready-timeout", str(args.ready_timeout), "--leave-timeout", str(args.leave_timeout)]
    root_argv += _base_hpx_flags("root", args.threads, A_ip, pagas, A_ip, pagas, pin, extra)
    conn_argv = ["srun", "-N1", "-n1", "--nodelist=" + nodeB, binary,
                 "--role", "connect", "--bootstrap", bootdir, "--serve-timeout", str(args.serve_timeout)]
    conn_argv += _base_hpx_flags("connect", args.threads, A_ip, pagas, B_ip, phpx, pin, extra)
    sel["root_argv"] = " ".join(root_argv)
    sel["connector_argv"] = " ".join(conn_argv)

    # supervisor_wall marker-visibility tracker (this orchestrator's clock; FS visibility only)
    ROOT_MARKERS = ["attest_root.json", "root.ready", "served1.ok", "root_result.json",
                    "root_timing.json", "root_finalize_done.json"]
    CONN_MARKERS = ["attest_connect.json", "connect.joined1", "connect.disconnected1",
                    "connect_timing.json"]
    visible = {n: None for n in (ROOT_MARKERS + CONN_MARKERS)}

    def stamp(t0):
        for name in visible:
            if visible[name] is None and os.path.exists(os.path.join(bootdir, name)):
                visible[name] = int((time.monotonic() - t0) * 1000)

    r_out = open(os.path.join(bootdir, "root.stdout"), "w")
    r_err = open(os.path.join(bootdir, "root.stderr"), "w")
    t0 = time.monotonic()  # supervisor_wall reference: root srun issued
    root = subprocess.Popen(root_argv, stdout=r_out, stderr=r_err, env=_child_env())
    root_srun_issue_ms = 0

    # wait for the root rendezvous marker before launching the connector
    deadline = time.time() + args.ready_timeout
    while time.time() < deadline and not os.path.exists(os.path.join(bootdir, "root.ready")):
        if root.poll() is not None:
            break
        stamp(t0)
        time.sleep(0.05)
    stamp(t0)
    root_ready = os.path.exists(os.path.join(bootdir, "root.ready"))

    conn = c_out = c_err = None
    connector_srun_issue_ms = None
    if root_ready:
        c_out = open(os.path.join(bootdir, "connector.stdout"), "w")
        c_err = open(os.path.join(bootdir, "connector.stderr"), "w")
        connector_srun_issue_ms = int((time.monotonic() - t0) * 1000)
        conn = subprocess.Popen(conn_argv, stdout=c_out, stderr=c_err, env=_child_env())

    # poll until the root exits (serve + observe leave + finalize) within a generous bound, stamping
    overall_deadline = time.time() + args.ready_timeout + args.leave_timeout + 60
    while time.time() < overall_deadline and root.poll() is None:
        stamp(t0)
        time.sleep(0.1)
    stamp(t0)
    root_rc = root.poll()
    if root_rc is None:
        root.kill()
    if conn is not None:
        try:
            conn.wait(timeout=15)
        except Exception:
            conn.kill()
    stamp(t0)
    for fh in (r_out, r_err, c_out, c_err):
        if fh is not None:
            try:
                fh.close()
            except OSError:
                pass

    # --- (1) EARLY single-attempt parse (to detect a shared-FS false negative vs the final parse) ---
    early_rr = _read_json(os.path.join(bootdir, "root_result.json")) or {}
    early_joined = _read_json(os.path.join(bootdir, "connect.joined1"))
    early_complete = bool(early_rr.get("reached_two") and early_joined)

    # --- (2) FINAL bounded marker-read pass after BOTH children exit; verdict computed from THIS ---
    marker_diag = {}

    def reread(name, required=False, timeout=15.0):
        obj, diag = read_json_eventually(os.path.join(bootdir, name), timeout_s=timeout, poll_s=0.25,
                                         required=required)
        marker_diag[name] = diag
        return obj

    rr = reread("root_result.json", required=True) or {}
    root_timing = reread("root_timing.json") or {}
    root_finalize_done = reread("root_finalize_done.json") or {}
    conn_timing = reread("connect_timing.json") or {}
    a_root = reread("attest_root.json", required=True)
    a_conn = reread("attest_connect.json", required=True)
    joined = reread("connect.joined1", required=True)
    disc = reread("connect.disconnected1") or {}
    # served1.ok is a flag file (not JSON) -> existence-only helper (returns on first sight; never
    # burns the full timeout trying to JSON-parse a flag file)
    served_present, served_diag = exists_eventually(os.path.join(bootdir, "served1.ok"),
                                                    timeout_s=15.0, poll_s=0.25)
    marker_diag["served1.ok"] = served_diag

    # merge finalize-done into the root_in_binary block
    root_in_binary = dict(root_timing)
    root_in_binary["root_finalize_done_ms"] = (root_finalize_done or {}).get("root_finalize_done_ms", -1)

    connector_joined = bool(joined)
    reached_two = bool(rr.get("reached_two"))
    proved_oracle = bool(rr.get("proved_remote_by_oracle"))
    id_differs = bool(rr.get("remote_locality_id_differs"))
    observed_leave = bool(rr.get("observed_connector_leave"))
    disc_clean = bool(disc.get("clean"))
    settle_ms = rr.get("settle_ms")
    if settle_ms is None:
        settle_ms = root_timing.get("settle_ms")

    host_differs = bool(a_root and a_conn and a_root.get("hostname") and a_conn.get("hostname")
                        and a_root["hostname"] != a_conn["hostname"])

    mechanism_markers_complete = bool(rr and connector_joined and disc and served_present
                                      and a_root and a_conn)
    mechanism_success_on_disk = bool(reached_two and proved_oracle and id_differs and host_differs
                                     and disc_clean and served_present and observed_leave)

    # (6) shared-FS false-negative detection: early parse said incomplete but the robust final parse
    # recovered a complete/successful mechanism.
    marker_read_false_negative_suspected = bool((not early_complete) and mechanism_success_on_disk)
    final_marker_reparse_changed_verdict = bool((not early_complete) and mechanism_markers_complete)

    # endpoint advertisement correctness (advertised == intended numeric IP:port)
    root_adv = (a_root or {}).get("advertised_hpx_endpoint")
    conn_adv = (a_conn or {}).get("advertised_hpx_endpoint")
    endpoint_ok = bool(root_adv == intended_root_ep and conn_adv == intended_conn_ep)
    sel["advertised_root_endpoint"] = root_adv
    sel["advertised_connector_endpoint"] = conn_adv
    sel["endpoint_advertisement_correct"] = endpoint_ok

    # supervisor_wall block (shared-FS visibility, NOT AGAS settle)
    def _min_vis(names):
        vals = [visible[n] for n in names if visible[n] is not None]
        return min(vals) if vals else None

    root_ready_visible = visible["root.ready"]
    root_first_marker_visible_ms = _min_vis(ROOT_MARKERS)
    conn_join_visible = visible["connect.joined1"]
    connector_first_marker_visible_ms = _min_vis(CONN_MARKERS)
    root_srun_to_ready_ms = (root_ready_visible - root_srun_issue_ms
                             if root_ready_visible is not None else None)
    conn_srun_to_join_ms = (conn_join_visible - connector_srun_issue_ms
                            if (conn_join_visible is not None and connector_srun_issue_ms is not None)
                            else None)
    supervisor_wall = {
        "reference": "ms since root srun issued on the orchestrator's monotonic clock; shared-FS "
                     "marker VISIBILITY + srun launch timing only, NOT AGAS settle",
        "root_srun_issue_ms": root_srun_issue_ms,
        "connector_srun_issue_ms": connector_srun_issue_ms,
        "root_first_marker_visible_ms": root_first_marker_visible_ms,
        "root_ready_marker_visible_ms": root_ready_visible,
        "root_srun_to_root_ready_visible_ms": root_srun_to_ready_ms,
        "connector_first_marker_visible_ms": connector_first_marker_visible_ms,
        "connector_join_marker_visible_ms": conn_join_visible,
        "connector_srun_to_join_visible_ms": conn_srun_to_join_ms,
        "root_result_marker_visible_ms": visible["root_result.json"],
        "served_marker_visible_ms": visible["served1.ok"],
        "connector_disconnect_marker_visible_ms": visible["connect.disconnected1"],
    }
    # promote the requested flat per-role srun-timing fields (supervisor-wall; do not compare to
    # connector/root in-binary clocks)
    sel["root_srun_issue_ms"] = root_srun_issue_ms
    sel["root_first_marker_visible_ms"] = root_first_marker_visible_ms
    sel["root_srun_to_root_ready_visible_ms"] = root_srun_to_ready_ms
    sel["connector_srun_issue_ms"] = connector_srun_issue_ms
    sel["connector_first_marker_visible_ms"] = connector_first_marker_visible_ms
    sel["connector_srun_to_join_visible_ms"] = conn_srun_to_join_ms

    # (9) HPX log-capture verification -- empty logs are NOT a clean negative
    log_files = ["root.stdout", "root.stderr", "connector.stdout", "connector.stderr"]
    logs_have_output = any((os.path.getsize(os.path.join(bootdir, f))
                            if os.path.exists(os.path.join(bootdir, f)) else 0) > 0 for f in log_files)
    if not args.hpx_logs:
        hpx_log_capture_available = False
        hpx_log_capture_reason = "--hpx-logs not enabled"
    elif not logs_have_output:
        hpx_log_capture_available = False
        hpx_log_capture_reason = ("hpx.logging.parcel.level=5 set but no output captured to "
                                  "stdout/stderr (likely needs hpx.logging.parcel.destination); cannot "
                                  "rule out retry/resolve from empty logs")
    else:
        hpx_log_capture_available = True
        hpx_log_capture_reason = None
    logs_trustworthy = hpx_log_capture_available

    retry_seen, retry_hits = _scan_logs_for(bootdir, _RETRY_PATTERNS)
    resolve_seen, resolve_hits = _scan_logs_for(bootdir, _RESOLVE_PATTERNS)

    root_tail = _tail(os.path.join(bootdir, "root.stdout")) or _tail(os.path.join(bootdir, "root.stderr"))
    conn_tail = (_tail(os.path.join(bootdir, "connector.stdout"))
                 or _tail(os.path.join(bootdir, "connector.stderr")))

    no_orphans = True
    orphan_pids = []
    for nd in (nodeA, nodeB):
        ok, pids = _orphan_check_node(nd)
        if ok is False:
            no_orphans = False
            orphan_pids += [f"{nd}:{p}" for p in pids]

    # connector in-binary lifecycle span (node-B clock, single process -> a valid delta). NOTE: the
    # lifecycle folds in serve-wait (time blocked waiting for the root to serve it) and is therefore
    # NOT the launch-speed signal -- conn_joined_ms below is.
    cj = conn_timing.get("connector_join_issued_ms")
    cd = conn_timing.get("connector_disconnect_done_ms")
    conn_lifecycle_ms = (cd - cj if isinstance(cj, (int, float)) and isinstance(cd, (int, float))
                         and cj >= 0 and cd >= 0 else None)
    # connector-local JOIN span: process start -> locality observable (connector_joined_ms is ms since
    # the connector main()/start entry). This is the launch/join-speed signal the slurm classifier uses.
    cjoin = conn_timing.get("connector_joined_ms")
    conn_joined_ms = cjoin if isinstance(cjoin, (int, float)) and cjoin >= 0 else None

    # ---- verdict (computed from the FINAL reparse, NOT early absence) ----
    if not root_ready:
        classification, attributed = "unattributed_fixed_delay", False
        suspect = "root never became ready (root.ready not visible); see srun probes + log tails"
        overall = "fail"
    elif not mechanism_success_on_disk:
        classification, attributed = "unattributed_fixed_delay", False
        suspect = ("mechanism markers incomplete/inconsistent after the final bounded reparse "
                   "(not a shared-FS false negative); see marker_read_diagnostics + log tails")
        overall = "fail"
    else:
        signals = {
            "settle_ms": settle_ms,
            "root_ready_ms": root_timing.get("root_ready_ms"),
            "conn_lifecycle_ms": conn_lifecycle_ms,
            "conn_joined_ms": conn_joined_ms,
            "srun_probe_a_ms": a_ms, "srun_probe_b_ms": b_ms,
            "root_srun_to_ready_ms": root_srun_to_ready_ms,
            "conn_srun_to_join_ms": conn_srun_to_join_ms,
            "conn_join_visible_ms": conn_join_visible,
            "retry_seen": retry_seen, "resolve_seen": resolve_seen,
            "logs_trustworthy": logs_trustworthy,
        }
        classification, attributed, suspect = _classify_settle(signals)
        overall = "attributed" if attributed else "inconclusive"

    sel.update({
        "root_started": root_ready,
        "connector_joined": connector_joined,
        "reached_two": reached_two,
        "mechanism_markers_complete": mechanism_markers_complete,
        "mechanism_success_on_disk": mechanism_success_on_disk,
        "marker_read_false_negative_suspected": marker_read_false_negative_suspected,
        "final_marker_reparse_changed_verdict": final_marker_reparse_changed_verdict,
        "marker_read_diagnostics": marker_diag,
        "settle_delay_ms": settle_ms,
        "settle_delay_attributed": attributed,
        "settle_delay_classification": classification,
        "settle_delay_suspect": suspect,
        "reverse_dns_or_hostname_seen_in_logs": resolve_seen,
        "parcel_retry_or_timeout_seen": retry_seen,
        "agas_registration_delay_seen": (classification == "agas_registration_reflection_delay"),
        "shared_fs_marker_delay_seen": marker_read_false_negative_suspected,
        "slurm_step_launch_latency_seen": (classification == "slurm_step_launch_latency"),
        "log_retry_hits": retry_hits,
        "log_resolve_hits": resolve_hits,
        "hpx_log_capture_available": hpx_log_capture_available,
        "hpx_log_capture_reason": hpx_log_capture_reason,
        "connector_lifecycle_ms": conn_lifecycle_ms,
        "root_in_binary": root_in_binary,
        "connector_in_binary": conn_timing,
        "supervisor_wall": supervisor_wall,
        "root_log_tail": root_tail,
        "connector_log_tail": conn_tail,
        "graceful_disconnect_clean": disc_clean,
        "root_finalized_clean": bool(root_rc == 0 and rr),
        "attest_root_hostname": (a_root or {}).get("hostname"),
        "attest_connector_hostname": (a_conn or {}).get("hostname"),
        "no_orphans": no_orphans,
        "orphan_pids": orphan_pids,
        "overall": overall,
    })
    return sel


# ---------------------------------------------------------------------------------------------------
# settle_attribution.json assembly
# ---------------------------------------------------------------------------------------------------
CLOCK_DOMAINS = {
    "root_in_binary": "steady_clock deltas on the ROOT process (node A); AUTHORITATIVE settle span "
                      "(root_wait_two_start_ms -> root_first_observed_two_localities_ms).",
    "connector_in_binary": "steady_clock deltas on the CONNECTOR process (node B); NOT comparable to "
                           "the root clock (no cross-node time sync assumed).",
    "supervisor_wall": "this orchestrator's monotonic clock; shared-FS marker VISIBILITY only, never "
                       "labeled AGAS settle.",
}

_FENCE = (
    "first Ray/Slurm-supervised two-node island arc; Slice 0 is Ray-free; TCP parcelport only; "
    "closed-int64 action only; no performance/speedup/throughput/latency; the settle is a STRUCTURAL "
    "READINESS duration and is SUSPICIOUS until attributed (must NOT be reused for restart/detector "
    "timing -- exp55's single-node calibration does not transfer to two-node); no HPX fault tolerance; "
    "no Ray actor-failure recovery; no production/public API; no object store; no arbitrary Python; no "
    "Ray replacement; no general fabric claim; no MPI/LCI performance-path claim; future "
    "distributed-fabric direction only."
)

_NOTE = (
    "exp57 Slice 0 RAY-FREE two-node settle attribution (HARDENED runner). exp56 loopback settle was "
    "~100 ms; the two-node settle was ~30 s. The first Slice 0 Rostam run was a runner FALSE NEGATIVE: "
    "complete success markers on disk were hidden from the orchestrator by shared-FS (NFS) visibility "
    "lag, so it wrongly reported fail. This runner now: reads markers robustly with retry + parent-dir "
    "revalidation (read_json_eventually); computes the verdict ONLY from a final bounded reparse after "
    "both children exit (never permanently fails on early absence); and flags "
    "marker_read_false_negative_suspected / final_marker_reparse_changed_verdict. It also measures srun "
    "STEP launch latency directly (srun_probe_node{A,B}_ms and per-role srun-issue->first-marker "
    "timings) so a ~30 s root settle that coincides with ~30 s srun launch -- root ready early, "
    "connector in-binary JOIN sub-second once the connector PROCESS exists (the full connector "
    "lifecycle may fold in serve-wait and is NOT the launch-speed signal; the launch-speed signal is "
    "the connector-local join span connector_joined_ms) -- is attributed to "
    "slurm_step_launch_latency rather than HPX/AGAS. Three clock domains are kept separate "
    "(root_in_binary authoritative; connector_in_binary node-B, never subtracted from root; "
    "supervisor_wall is shared-FS visibility + srun timing, NEVER AGAS settle). settle_delay_ms is the "
    "root-local readiness span; it is NOT latency, NOT performance, and NOT a network-behavior claim. "
    "slurm_step_launch_latency, when attributed, only explains the observed readiness duration IN THIS "
    "LAUNCH MODEL (sequential srun steps are load-bearing for supervisor timing); it is NOT an HPX "
    "fabric claim. Empty HPX logs are NOT a clean negative: if hpx_log_capture_available is false, "
    "retry/resolve cannot be ruled out. If signals are insufficient, overall=inconclusive and "
    "classification=unattributed_fixed_delay rather than a guess. Allowed classifications: "
    "slurm_step_launch_latency, name_resolution_or_reverse_dns, parcel_retry_or_timeout, "
    "agas_registration_reflection_delay, shared_fs_marker_visibility, orchestrator_artifact (only "
    "reproducible under Ray -- not testable in Ray-free Slice 0), unattributed_fixed_delay. "
    "SLICE A IMPLICATION (not implemented here): Slice A should NOT gate the connector launch on "
    "root.ready if that needlessly serializes two ~30 s srun steps; Ray supervision should issue the "
    "root and connector srun steps as concurrently as safely possible while still preserving correct "
    "readiness/teardown gates."
)


def _null_settle():
    return {k: None for k in (
        "nodeA", "nodeB", "nodeA_ip", "nodeB_ip", "selected_interface", "selected_subnet",
        "bidirectional_port_check_passed", "root_started", "connector_joined", "reached_two",
        "mechanism_markers_complete", "mechanism_success_on_disk",
        "marker_read_false_negative_suspected", "final_marker_reparse_changed_verdict",
        "settle_delay_ms", "endpoint_advertisement_correct", "numeric_ip_used",
        "reverse_dns_or_hostname_seen_in_logs", "parcel_retry_or_timeout_seen",
        "agas_registration_delay_seen", "shared_fs_marker_delay_seen",
        "slurm_step_launch_latency_seen", "hpx_log_capture_available", "hpx_log_capture_reason",
        "srun_probe_nodeA_ms", "srun_probe_nodeB_ms", "srun_probe_nodeA_stdout",
        "srun_probe_nodeB_stdout", "root_srun_issue_ms", "root_first_marker_visible_ms",
        "root_srun_to_root_ready_visible_ms", "connector_srun_issue_ms",
        "connector_first_marker_visible_ms", "connector_srun_to_join_visible_ms",
        "connector_lifecycle_ms", "marker_read_diagnostics",
        "root_in_binary", "connector_in_binary", "supervisor_wall", "root_log_tail",
        "connector_log_tail")}


def assemble_settle(binary, cfg, diag, overall, reason):
    agg = {
        "experiment": "57_ray_slurm_supervised_two_node_island",
        "phase": "settle_diagnostic",
        "kind": "ray_free_two_node_hpx_tcp_settle_attribution",
        "ray_free": True,
        "transport": "tcp_parcelport",
        "binary": os.path.basename(binary) if binary else None,
        "tcp_parcelport_available": (cfg or {}).get("tcp_parcelport_available"),
        "hpx_version": (cfg or {}).get("hpx_version"),
        "hpx_parcelport_config": (cfg or {}).get("hpx_parcelport_config"),
        "clock_domains": CLOCK_DOMAINS,
    }
    keys = (
        "nodeA", "nodeB", "nodeA_ip", "nodeB_ip", "selected_interface", "selected_subnet",
        "bidirectional_port_check_passed", "reachability_b_to_a", "reachability_a_to_b",
        "intended_root_endpoint", "intended_connector_endpoint",
        "advertised_root_endpoint", "advertised_connector_endpoint",
        "preflight_tested_same_endpoint",
        "root_started", "connector_joined", "reached_two",
        "mechanism_markers_complete", "mechanism_success_on_disk",
        "marker_read_false_negative_suspected", "final_marker_reparse_changed_verdict",
        "settle_delay_ms", "settle_delay_attributed", "settle_delay_classification",
        "settle_delay_suspect", "endpoint_advertisement_correct", "numeric_ip_used",
        "reverse_dns_or_hostname_seen_in_logs", "parcel_retry_or_timeout_seen",
        "agas_registration_delay_seen", "shared_fs_marker_delay_seen",
        "slurm_step_launch_latency_seen",
        "hpx_log_capture_available", "hpx_log_capture_reason",
        "log_retry_hits", "log_resolve_hits",
        "srun_probe_nodeA_ms", "srun_probe_nodeB_ms", "srun_probe_nodeA_stdout",
        "srun_probe_nodeB_stdout",
        "root_srun_issue_ms", "root_first_marker_visible_ms", "root_srun_to_root_ready_visible_ms",
        "connector_srun_issue_ms", "connector_first_marker_visible_ms",
        "connector_srun_to_join_visible_ms", "connector_lifecycle_ms",
        "root_in_binary", "connector_in_binary", "supervisor_wall", "marker_read_diagnostics",
        "root_log_tail", "connector_log_tail",
        "graceful_disconnect_clean", "root_finalized_clean",
        "attest_root_hostname", "attest_connector_hostname", "no_orphans", "orphan_pids",
        "root_argv", "connector_argv", "bootstrap_dir", "shared_dir_source",
        "bootstrap_dir_node_local_warning",
    )
    if diag:
        for k in keys:
            if k in diag:
                agg[k] = diag[k]
    else:
        agg.update(_null_settle())

    agg["overall"] = overall
    if reason:
        agg["skip_or_fail_reason"] = reason
    agg["note"] = _NOTE
    agg["claim_fence"] = _FENCE
    return agg


def _write_agg(path, agg):
    with open(path, "w") as fh:
        json.dump(agg, fh, indent=2, sort_keys=False)
        fh.write("\n")


def _preserve_run_artifacts(agg, diag, args):
    """Durably preserve a settle-diag run's FULL verdict so replication runs are never lost to the
    overwritten top-level latest JSON. Writes a per-run copy of the SAME `agg` object into the run's
    bootdir and appends a one-line stability index. Both live under the ignored shared run dir
    (default _two_node_runs/); nothing here is made trackable. No-op when there is no bootstrap_dir
    (skip/no-allocation cases have no per-run dir). Returns (per_run_path, index_path) or (None, None)."""
    bootdir = (diag or {}).get("bootstrap_dir")
    if not bootdir or not os.path.isdir(bootdir):
        return None, None
    per_run_path = os.path.join(bootdir, "settle_attribution.json")
    _write_agg(per_run_path, agg)  # identical full verdict object as the top-level latest JSON

    # one-line stability index, co-located with the per-run dirs (follows --shared-dir); JSONL append.
    index_path = os.path.join(os.path.dirname(bootdir), "settle_attribution_index.jsonl")
    row = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "run_id": os.path.basename(bootdir.rstrip("/")),
        "bootdir": bootdir,
        "subnet": diag.get("selected_subnet"),
        "agas_port": args.agas_port,
        "hpx_port": args.hpx_port,
        "nodeA": diag.get("nodeA"),
        "nodeB": diag.get("nodeB"),
        "overall": agg.get("overall"),
        "settle_delay_ms": agg.get("settle_delay_ms"),
        "settle_delay_classification": agg.get("settle_delay_classification"),
        "settle_delay_attributed": agg.get("settle_delay_attributed"),
        "mechanism_success_on_disk": agg.get("mechanism_success_on_disk"),
        "no_orphans": agg.get("no_orphans"),
    }
    with open(index_path, "a") as fh:
        fh.write(json.dumps(row, sort_keys=False) + "\n")
    return per_run_path, index_path


# ---------------------------------------------------------------------------------------------------
# Slice A2 preflight building blocks (pre-Ray env snapshot + compute-node ldd) reused by A2b launch
# ---------------------------------------------------------------------------------------------------
_RUN_FENCE = (
    "exp57 Slice A2b -- Ray/Slurm-SUPERVISED clean two-node HPX launch. Ray is bootstrap/supervision ONLY "
    "(it issues the root & connector srun from INSIDE Ray actors under the pre-Ray-anchored child env); "
    "HPX carries the closed-int64 action/data path over the TCP parcelport. Connector readiness is the "
    "connector-side AGAS TCP pre-probe, NOT shared-FS root.ready gating. NO failure/restart, NO poison "
    "detection, NO detector timing (deferred to exp58). Ray is NEVER imported at module top (lazy import "
    "inside --phase run only). NOT a performance/latency/throughput/fabric claim; no duration is an "
    "HPX/AGAS settle; TCP parcelport only; closed-int64 action only; no HPX fault tolerance; no Ray "
    "actor-failure recovery; no production/public API; no object store for HPX payloads."
)


def _preserve_child_env(baseline=None, current=None):
    """Build the env handed to srun children, ANCHORED to the pre-Ray baseline snapshot.

    This scaffold is launched directly (not from a Ray actor), so `current` == the pre-Ray baseline and
    this is effectively a pass-through. The mechanism is in place for the deferred launch-from-Ray-actor
    slice: there Ray may rewrite PATH / LD_LIBRARY_PATH / CUDA_VISIBLE_DEVICES / OMP_* in the actor env,
    and we OVERLAY the pre-Ray values for the preserve-list keys back on top so the GCC15 loader path
    survives. HONEST LABEL: the Ray-mutation defense is NOT exercised/validated in this scaffold.
    """
    base = dict(_PRE_RAY_ENV_SNAPSHOT if baseline is None else baseline)
    child = dict(os.environ if current is None else current)
    for k in _ENV_PRESERVE_KEYS:           # restore load-bearing keys from the pre-Ray baseline
        if k in base:
            child[k] = base[k]
        else:
            child.pop(k, None)             # match the baseline: absent in baseline -> absent in child
    for k in _ENV_PRESERVE_OPTIONAL:       # best-effort loader/toolchain vars, only when set
        if k in base:
            child[k] = base[k]
    child.setdefault("SLURM_EXPORT_ENV", "ALL")  # belt-and-suspenders for explicit srun --export=ALL
    return child


def _env_preserve_report(child_env, baseline=None):
    """Structural report on the child env: which preserve-list keys are present, whether each matches
    the pre-Ray baseline, and which REQUIRED load-bearing keys are missing. No values are dumped."""
    base = dict(_PRE_RAY_ENV_SNAPSHOT if baseline is None else baseline)
    present = {k: (k in child_env) for k in _ENV_PRESERVE_KEYS}
    matches = {k: (child_env.get(k) == base.get(k)) for k in _ENV_PRESERVE_KEYS if k in base}
    missing = [k for k in _ENV_LOAD_BEARING_REQUIRED if not child_env.get(k)]
    return {
        "child_env_anchored_to_pre_ray_baseline": True,
        "preserve_keys": list(_ENV_PRESERVE_KEYS),
        "preserve_present": present,
        "preserve_value_matches_baseline": matches,
        "load_bearing_required": list(_ENV_LOAD_BEARING_REQUIRED),
        "load_bearing_missing": missing,
        "has_ld_library_path": bool(child_env.get("LD_LIBRARY_PATH")),
        "ray_mutation_defense_validated": False,
        "ray_mutation_defense_note": ("child env anchored to the pre-Ray baseline; the Ray-mutation "
                                      "scrub is exercised only when srun is issued from inside a Ray "
                                      "actor (deferred launch slice)"),
    }


def _parse_ldd_libstdcxx(stdout):
    """Pull the RESOLVED libstdc++.so.6 path from `ldd` output ('libstdc++.so.6 => /path (0x..)')."""
    if not stdout:
        return None
    for line in stdout.splitlines():
        if "libstdc++.so.6" in line and "=>" in line:
            rhs = line.split("=>", 1)[1].strip()
            path = rhs.split(" (")[0].strip()   # drop trailing load address
            return path or None
    return None


def _is_system_libstdcxx(path):
    """True when the resolved libstdc++ is a SYSTEM copy (the silent false-pass the GCC15 gate rejects)."""
    p = (path or "").strip()
    return bool(p) and p.startswith(_SYSTEM_LIBSTDCXX_PREFIXES)


def _gxx_expected_libstdcxx(child_env, timeout_s=30):
    """Derive the EXPECTED GCC libstdc++ from the SAME child env future srun children receive, via
    `g++ -print-file-name=libstdc++.so`. g++ prints the full toolchain path when it can resolve one, or
    the bare 'libstdc++.so' (no '/') when it cannot -- the latter is treated as 'unknown' so the gate
    cannot pass on a missing toolchain. Returns rc, tails, and the realpath'd expected path + dir."""
    argv = ["g++", "-print-file-name=libstdc++.so"]
    rc = None
    stdout = None
    stderr = None
    try:
        out = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_s, env=child_env)
        rc, stdout, stderr = out.returncode, out.stdout, out.stderr
    except Exception as e:  # noqa: BLE001
        stderr = "exec_error: " + str(e)[:200]
    raw = ((stdout or "").strip().splitlines()[0].strip() if (stdout and stdout.strip()) else None)
    expected_path = None
    expected_dir = None
    if raw and ("/" in raw):                 # a real toolchain path, not the bare 'libstdc++.so' fallback
        expected_path = os.path.realpath(raw)
        expected_dir = os.path.realpath(os.path.dirname(raw))
    return {
        "gxx_probe_argv": " ".join(argv),
        "gxx_probe_rc": rc,
        "gxx_probe_stdout_tail": ((stdout or "").strip()[-400:] or None),
        "gxx_probe_stderr_tail": ((stderr or "").strip()[-400:] or None),
        "gxx_print_file_name_raw": raw,
        "expected_libstdcxx_path": expected_path,
        "expected_libstdcxx_dir": expected_dir,
        "expected_resolved": bool(expected_dir),
    }


def _ldd_check_node(node, binary, child_env, expected, timeout_s=60):
    """Compute-node loader-hygiene gate: `srun -N1 -n1 --nodelist=<node> --export=ALL ldd <binary>` run
    UNDER the preserved child env (so the same env future srun children receive is exercised now). A
    bare login-node ldd would resolve with the wrong loader path -- this is why it must be remote.

    Pass requires BOTH: (a) the resolved libstdc++.so.6 is the same file as, or under the same realpath'd
    directory as, the EXPECTED GCC libstdc++ from `g++ -print-file-name` (the positive requirement); and
    (b) it is still NOT a system copy (kept as an additional guard, not the only guard). rc==0 alone is
    the false-pass case. Boolean loader hygiene only; elapsed time is NOT recorded (no perf field)."""
    # NFS negative-dentry guard: revalidate the shared-FS binary path before the remote ldd, so a stale
    # negative cache does not produce a false "missing binary" (same fix as the slice0 correction).
    binary_present, seen_diag = exists_eventually(binary, timeout_s=15, poll_s=0.25)
    argv = ["srun", "-N1", "-n1", "--nodelist=" + node, "--export=ALL", "ldd", binary]
    rc = None
    stdout = None
    stderr = None
    try:
        out = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_s, env=child_env)
        rc, stdout, stderr = out.returncode, out.stdout, out.stderr
    except Exception as e:  # noqa: BLE001
        stderr = "exec_error: " + str(e)[:200]
    resolved_raw = _parse_ldd_libstdcxx(stdout)
    resolved_path = os.path.realpath(resolved_raw) if resolved_raw else None
    resolved_dir = os.path.realpath(os.path.dirname(resolved_raw)) if resolved_raw else None
    looks_system = _is_system_libstdcxx(resolved_raw) if resolved_raw else None
    not_system = bool(resolved_raw) and not _is_system_libstdcxx(resolved_raw)

    exp_path = (expected or {}).get("expected_libstdcxx_path")
    exp_dir = (expected or {}).get("expected_libstdcxx_dir")
    same_file = bool(exp_path and resolved_path and resolved_path == exp_path)
    same_dir = bool(exp_dir and resolved_dir and resolved_dir == exp_dir)
    uses_expected = bool(exp_dir) and (same_file or same_dir)   # positive GCC-toolchain requirement
    gate_ok = bool(uses_expected and not_system)                # positive match AND system-path guard
    return {
        "node": node,
        "binary_revalidated_present": bool(binary_present),
        "binary_revalidate_attempts": seen_diag.get("attempts"),
        "srun_argv": " ".join(argv),
        "srun_export_all": True,
        "ran_under_preserved_child_env": True,
        "rc": rc,
        "ldd_ok": rc == 0,
        "resolved_libstdcxx": resolved_raw,
        "resolved_libstdcxx_path": resolved_path,
        "resolved_libstdcxx_dir": resolved_dir,
        "expected_libstdcxx_path": exp_path,
        "expected_libstdcxx_dir": exp_dir,
        "resolved_matches_expected_file": same_file,
        "resolved_matches_expected_dir": same_dir,
        "ldd_uses_expected_gcc_libstdcxx": uses_expected,
        "resolved_not_system": not_system,
        "resolved_looks_system": looks_system,
        "gcc15_libstdcxx_ok": gate_ok,
        "stdout_tail": ((stdout or "").strip()[-600:] or None),
        "stderr_tail": ((stderr or "").strip()[-400:] or None),
    }


def _a2b_design_invariants():
    """Code-policy CONSTANTS (properties of this orchestrator, safe to emit unconditionally). A2b now
    ACTIVELY uses the connector AGAS TCP pre-probe, so both the planned and the active flags are true."""
    return {
        "slice0_settle_delay_classification": "nfs_negative_dentry_or_attribute_cache",
        "slice0_attribution_superseded_label": "slurm_step_launch_latency",
        "marker_waits_revalidating": True,
        "plain_exists_critical_waits": False,
        "shared_fs_readiness_gate_on_connector_launch": False,
        "connector_uses_agas_tcp_preprobe_planned": True,
        "connector_uses_agas_tcp_preprobe": True,
        "agas_preprobe_ok_means": "TCP endpoint accepted connection, not AGAS semantic readiness",
        "run_phase_must_not_template_settle_diag": True,
        "failure_restart_deferred_to_exp58": True,
    }


def _a2b_results_skeleton():
    """RUN-OUTCOME slots. On the skip path everything is None; failure/restart is always False (never
    exercised in A2b). run_phase_a2b fills these with real measured outcomes on an actual launch."""
    return {
        "root_connector_launched": False,
        "root_srun_issue_ms": None,
        "connector_srun_issue_ms": None,
        "srun_issue_gap_ms": None,
        "ldd_both_use_expected_gcc_libstdcxx": None,
        "agas_preprobe_active": None,
        "agas_preprobe_ok": None,
        "agas_preprobe_ms": None,
        "reached_two": None,
        "proved_remote_by_oracle": None,
        "oracle_match": None,
        "remote_locality_id_differs": None,
        "observed_connector_leave": None,
        "graceful_disconnect_clean": None,
        "root_finalized_clean": None,
        "no_orphans": None,
        "mechanism_success_on_disk": None,
        "ray_supervisor_used": None,
        "failure_restart_used": False,
    }


def run_slice_a_scaffold(binary, nodes, args, cfg, child=None):
    """Preflight building block: pre-Ray-anchored child env + positive GCC ldd gate on BOTH nodes under
    that child env. Launches NO HPX root/connector and exercises NO AGAS pre-probe -- it is the A2b
    preflight gate (and the standalone A2 scaffold). `child` lets a caller (A2b) pass the SAME child env
    it will hand the HPX srun children, so ldd and the real launch are provably the same env. Returns a
    findings dict, or None when there is no >=2-node allocation."""
    if not nodes:
        return None
    nodeA, nodeB = nodes
    child = _preserve_child_env() if child is None else child
    env_report = _env_preserve_report(child)
    gxx = _gxx_expected_libstdcxx(child)         # EXPECTED GCC libstdc++ from the same child env

    shared, src = _resolve_shared_dir(args)
    os.makedirs(shared, exist_ok=True)
    bootdir = tempfile.mkdtemp(prefix="exp57_runscaffold_", dir=shared)

    findings = {
        "two_node_run": True, "nodeA": nodeA, "nodeB": nodeB,
        "bootstrap_dir": bootdir, "shared_dir_source": src,
        "binary_present": bool(binary),
        "hpx_root_connector_launched": False,    # invariant for this scaffold
        "agas_preprobe_exercised": False,        # invariant for this scaffold
        "child_env_report": env_report,
        "gxx_probe": gxx,
        "expected_libstdcxx_dir": gxx.get("expected_libstdcxx_dir"),
    }
    env_ok = bool(env_report.get("has_ld_library_path")) and not env_report.get("load_bearing_missing")
    findings["child_env_load_bearing_ok"] = env_ok

    if not binary:
        findings["ldd_nodeA"] = None
        findings["ldd_nodeB"] = None
        findings["ldd_both_gcc15_ok"] = False
        findings["preflight_pass"] = False
        findings["overall"] = "fail"
        findings["reason"] = ("two_node_island_spike not built; cannot run the compute-node ldd gate "
                              "(build it on Rostam first, see CMakeLists.txt)")
        return findings

    lddA = _ldd_check_node(nodeA, binary, child, gxx)
    lddB = _ldd_check_node(nodeB, binary, child, gxx)
    findings["ldd_nodeA"] = lddA
    findings["ldd_nodeB"] = lddB
    both_use_expected = bool(lddA.get("ldd_uses_expected_gcc_libstdcxx")
                             and lddB.get("ldd_uses_expected_gcc_libstdcxx"))
    findings["ldd_both_use_expected_gcc_libstdcxx"] = both_use_expected
    both_ok = bool(lddA.get("gcc15_libstdcxx_ok") and lddB.get("gcc15_libstdcxx_ok"))
    findings["ldd_both_gcc15_ok"] = both_ok

    pass_gate = bool(env_ok and both_ok)
    findings["preflight_pass"] = pass_gate
    findings["overall"] = "pass" if pass_gate else "fail"
    if pass_gate:
        findings["reason"] = ("scaffold preflight passed: child env anchored to pre-Ray baseline; both "
                              f"nodes' ldd resolve the expected GCC libstdc++ dir "
                              f"({gxx.get('expected_libstdcxx_dir')}); no HPX launch; no AGAS pre-probe")
    else:
        reasons = []
        if not env_ok:
            reasons.append("child env missing load-bearing keys: "
                           + (",".join(env_report.get("load_bearing_missing")) or "LD_LIBRARY_PATH"))
        if not gxx.get("expected_resolved"):
            reasons.append("g++ -print-file-name=libstdc++.so did not resolve a toolchain path under "
                           f"the child env (rc={gxx.get('gxx_probe_rc')}, "
                           f"raw={gxx.get('gxx_print_file_name_raw')})")
        if not both_ok:
            reasons.append("ldd did not use the expected GCC libstdc++ dir on both nodes "
                           f"(expected_dir={gxx.get('expected_libstdcxx_dir')}; "
                           f"A_resolved={lddA.get('resolved_libstdcxx_path')} "
                           f"uses_expected={lddA.get('ldd_uses_expected_gcc_libstdcxx')} "
                           f"not_system={lddA.get('resolved_not_system')}; "
                           f"B_resolved={lddB.get('resolved_libstdcxx_path')} "
                           f"uses_expected={lddB.get('ldd_uses_expected_gcc_libstdcxx')} "
                           f"not_system={lddB.get('resolved_not_system')})")
        findings["reason"] = "; ".join(reasons)
    return findings


def assemble_run_a2b(binary, cfg, findings, overall, reason, args):
    """Assemble the A2b run aggregate: design_invariants (code-policy constants) kept STRICTLY apart
    from results (measured launch outcomes). The single full verdict lives under findings['results'];
    other findings keys are merged for provenance. Skip path leaves results as the None skeleton."""
    agg = {
        "experiment": "57_ray_slurm_supervised_two_node_island",
        "phase": "run_ray_supervised",
        "run_slice": "A2b (Ray/Slurm-supervised clean two-node HPX launch; no failure/restart)",
        "ray_imported_at_module_top": False,
        "overall": overall,
        "reason": reason,
        "binary": binary,
        "hpx_version": (cfg or {}).get("hpx_version"),
        "tcp_parcelport_available": (cfg or {}).get("tcp_parcelport_available"),
        "design_invariants": _a2b_design_invariants(),
        "results": ((findings or {}).get("results") or _a2b_results_skeleton()),
        "claim_fence": _RUN_FENCE,
    }
    if findings:
        for k, v in findings.items():
            if k not in agg and k != "results":
                agg[k] = v
    return agg


# Minimal Ray supervisor worker. Ray is the BOOTSTRAP/SUPERVISION control plane ONLY: it issues a single
# srun child per role under the supplied pre-Ray-anchored child env and waits for it to exit. Ray does
# NOT carry the HPX action/data path and stores no HPX payload. Launching the srun child from INSIDE the
# Ray actor with an explicit env= override is what makes the pre-Ray env defense validatable: Ray's own
# mutation of the actor env cannot reach the HPX child.
class _SrunRunner:
    def run(self, role, argv, env, stdout_path, stderr_path, timeout_s):
        t0 = time.monotonic()
        rc = None
        timed_out = False
        try:
            o = open(stdout_path, "w")
            e = open(stderr_path, "w")
        except OSError as ex:
            return {"role": role, "rc": None, "timed_out": False,
                    "launch_error": "open_failed: " + str(ex)[:160], "launched_from_ray_actor": True}
        with o, e:
            try:
                p = subprocess.Popen(argv, stdout=o, stderr=e, env=env)
            except Exception as ex:  # noqa: BLE001
                return {"role": role, "rc": None, "timed_out": False,
                        "launch_error": str(ex)[:200], "launched_from_ray_actor": True}
            launch_offset_ms = int((time.monotonic() - t0) * 1000)
            try:
                rc = p.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                timed_out = True
                p.kill()
                rc = p.wait()
        return {"role": role, "rc": rc, "timed_out": timed_out,
                "launch_offset_ms": launch_offset_ms, "launched_from_ray_actor": True}


def _read_a2b_markers(bootdir):
    """Read EVERY A2b marker via the REVALIDATING readers (read_json_eventually / exists_eventually).
    No plain os.path.exists on a not-yet-existing critical marker. Timeouts are short: the srun children
    have already exited when this runs, so this only needs to defeat shared-FS (NFS) visibility lag."""
    def rj(name, t=30):
        return read_json_eventually(os.path.join(bootdir, name), timeout_s=t, poll_s=0.25)
    def ex(name, t=30):
        return exists_eventually(os.path.join(bootdir, name), timeout_s=t, poll_s=0.25)
    rr, rr_d = rj("root_result.json")
    rt, _ = rj("root_timing.json")
    rf, rf_d = rj("root_finalize_done.json")
    cp, cp_d = rj("connect_preprobe.json")
    cj, _ = rj("connect.joined1")
    di, _ = rj("connect.disconnect_initiated.json")
    dd, dd_d = rj("connect.disconnected1")
    ct, _ = rj("connect_timing.json")
    served, served_d = ex("served1.ok")
    pp_ok, pp_d = ex("connect.preprobe_ok")
    return {
        "root_result": rr, "root_timing": rt, "root_finalize_done": rf,
        "connect_preprobe": cp, "connect_joined1": cj,
        "connect_disconnect_initiated": di, "connect_disconnected1": dd,
        "connect_timing": ct,
        "served1_ok_present": bool(served), "connect_preprobe_ok_present": bool(pp_ok),
        "read_diag": {"root_result": rr_d, "connect_disconnected1": dd_d,
                      "connect_preprobe": cp_d, "connect_preprobe_ok": pp_d,
                      "served1_ok": served_d, "root_finalize_done": rf_d},
    }


def run_phase_a2b(binary, args, cfg):
    """A2b: Ray/Slurm-supervised CLEAN two-node HPX launch. Ray = bootstrap/supervision control plane
    ONLY (it issues the root & connector srun from INSIDE Ray actors using the pre-Ray-anchored child
    env); HPX carries the closed-int64 action/data path over the TCP parcelport. Connector readiness is
    the connector-side AGAS TCP pre-probe -- NOT shared-FS root.ready gating. No failure/restart. Returns
    a findings dict (with a 'results' block), or None when there is no >=2-node allocation.

    This is a FRESH launch path -- it deliberately does NOT reuse _launch_settle_diag's marker-gated
    shape (no root.ready gate, no plain os.path.exists on critical markers)."""
    nodes = slurm_nodes()
    if not nodes:
        return None

    results = _a2b_results_skeleton()
    findings = {"two_node_run": True, "nodeA": nodes[0], "nodeB": nodes[1], "results": results}

    # --- lazy Ray import (NEVER at module top) + minimal control-plane init ---
    try:
        import ray
    except Exception as e:  # noqa: BLE001
        findings["overall"] = "fail"
        findings["ray_import_ok"] = False
        findings["reason"] = "ray import failed (Ray not available in this env): " + str(e)[:160]
        return findings
    findings["ray_import_ok"] = True
    findings["ray_imported_lazily_in_run_phase"] = True
    findings["ray_version"] = getattr(ray, "__version__", None)
    try:
        ray.init(ignore_reinit_error=True, log_to_driver=False, include_dashboard=False)
    except Exception as e:  # noqa: BLE001
        findings["overall"] = "fail"
        findings["ray_init_ok"] = False
        findings["reason"] = "ray.init failed: " + str(e)[:160]
        return findings
    findings["ray_init_ok"] = True

    # child env built AFTER ray.init so the pre-Ray overlay DEMONSTRABLY defends any ray.init mutation
    child = _preserve_child_env()

    # --- preflight gate: env preserve-list + positive GCC ldd on BOTH nodes (same child env) ---
    pf = run_slice_a_scaffold(binary, nodes, args, cfg, child=child)
    findings["preflight"] = pf
    bootdir = (pf or {}).get("bootstrap_dir")
    findings["bootstrap_dir"] = bootdir
    env_report = (pf or {}).get("child_env_report") or {}
    findings["child_env_report"] = env_report
    findings["gxx_probe"] = (pf or {}).get("gxx_probe")
    findings["expected_libstdcxx_dir"] = (pf or {}).get("expected_libstdcxx_dir")
    ldd_both = bool((pf or {}).get("ldd_both_use_expected_gcc_libstdcxx"))
    env_ok = bool((pf or {}).get("child_env_load_bearing_ok"))
    results["ldd_both_use_expected_gcc_libstdcxx"] = ldd_both
    if not ((pf or {}).get("preflight_pass") and ldd_both and env_ok):
        _ray_shutdown_quiet(ray)
        findings["overall"] = "fail"
        findings["reason"] = "preflight gate failed (env/ldd); HPX launch ABORTED -- " + str(
            (pf or {}).get("reason"))
        return findings

    # --- node IP selection + socket-only reachability (no HPX launch) ---
    sel = select_and_reachability(args)
    findings["selection"] = sel
    if not sel or not sel.get("nodeA_ip") or not sel.get("nodeB_ip"):
        _ray_shutdown_quiet(ray)
        findings["overall"] = "fail"
        findings["reason"] = "could not select routable node IPs (check Ethernet vs IPoIB / --prefer-subnet)"
        return findings
    nodeA, nodeB = sel["nodeA"], sel["nodeB"]
    A_ip, B_ip = sel["nodeA_ip"], sel["nodeB_ip"]
    pagas, phpx = args.agas_port, args.hpx_port
    findings["bidirectional_port_check_passed"] = sel.get("bidirectional_port_check_passed")

    # --- build FRESH root & connector srun command lines ---
    pin = tcp_pin_flags(cfg)
    extra = PARCEL_LOG_FLAGS if args.hpx_logs else ()
    preprobe_timeout_ms = args.agas_preprobe_timeout_ms
    preprobe_port = args.agas_preprobe_port if args.agas_preprobe_port is not None else pagas

    root_argv = ["srun", "-N1", "-n1", "--nodelist=" + nodeA, "--export=ALL", binary,
                 "--role", "root", "--bootstrap", bootdir, "--x", "7",
                 "--ready-timeout", str(args.ready_timeout), "--leave-timeout", str(args.leave_timeout)]
    root_argv += _base_hpx_flags("root", args.threads, A_ip, pagas, A_ip, pagas, pin, extra)
    conn_argv = ["srun", "-N1", "-n1", "--nodelist=" + nodeB, "--export=ALL", binary,
                 "--role", "connect", "--bootstrap", bootdir,
                 "--serve-timeout", str(args.serve_timeout),
                 "--agas-preprobe-host", A_ip,                       # connector AGAS TCP pre-probe target
                 "--agas-preprobe-port", str(preprobe_port),
                 "--agas-preprobe-timeout-ms", str(preprobe_timeout_ms)]
    conn_argv += _base_hpx_flags("connect", args.threads, A_ip, pagas, B_ip, phpx, pin, extra)
    findings["root_argv"] = " ".join(root_argv)
    findings["connector_argv"] = " ".join(conn_argv)
    findings["connector_agas_preprobe_target"] = f"{A_ip}:{preprobe_port}"

    # --- Ray supervisor: one actor per role; issue both srun NEAR-CONCURRENTLY. The connector is NOT
    #     gated on root.ready -- it is issued back-to-back and relies on its own AGAS TCP pre-probe. ---
    root_timeout_s = args.ready_timeout + args.leave_timeout + 60
    conn_timeout_s = (args.ready_timeout + args.serve_timeout + 60 + int(preprobe_timeout_ms / 1000))
    Runner = ray.remote(num_cpus=1)(_SrunRunner)
    root_actor = Runner.remote()
    conn_actor = Runner.remote()
    findings["ray_supervisor_used"] = True
    results["ray_supervisor_used"] = True
    findings["ray_supervisor_shape"] = "one _SrunRunner Ray actor per role; back-to-back .remote() issue"

    r_out = os.path.join(bootdir, "root.stdout")
    r_err = os.path.join(bootdir, "root.stderr")
    c_out = os.path.join(bootdir, "connect.stdout")
    c_err = os.path.join(bootdir, "connect.stderr")

    t0 = time.monotonic()                                    # supervisor_wall reference (issue timing)
    fut_root = root_actor.run.remote("root", root_argv, child, r_out, r_err, root_timeout_s)
    root_issue_ms = int((time.monotonic() - t0) * 1000)
    fut_conn = conn_actor.run.remote("connect", conn_argv, child, c_out, c_err, conn_timeout_s)
    conn_issue_ms = int((time.monotonic() - t0) * 1000)
    results["root_srun_issue_ms"] = root_issue_ms
    results["connector_srun_issue_ms"] = conn_issue_ms
    results["srun_issue_gap_ms"] = conn_issue_ms - root_issue_ms
    results["root_connector_launched"] = True
    findings["connector_gated_on_root_ready"] = False        # invariant: no root.ready wait before issue

    # --- collect process results (bounded blocking until both srun children exit) ---
    get_timeout = max(root_timeout_s, conn_timeout_s) + 30
    proc = {"root": None, "connect": None}
    get_timed_out = False
    try:
        out_root, out_conn = ray.get([fut_root, fut_conn], timeout=get_timeout)
        proc["root"], proc["connect"] = out_root, out_conn
    except Exception as e:  # noqa: BLE001  (GetTimeoutError / actor error)
        get_timed_out = True
        findings["ray_get_error"] = str(e)[:200]
    findings["proc"] = proc
    findings["ray_get_timed_out"] = get_timed_out
    root_rc = (proc["root"] or {}).get("rc")
    conn_rc = (proc["connect"] or {}).get("rc")
    results["root_rc"] = root_rc
    results["connector_rc"] = conn_rc

    # env-mutation defense is VALIDATED iff the srun children were launched from inside a Ray actor with
    # the explicit pre-Ray-anchored env (the env= override beats any Ray mutation of the actor env)
    launched_in_actor = bool((proc["root"] or {}).get("launched_from_ray_actor")
                             and (proc["connect"] or {}).get("launched_from_ray_actor"))
    env_report["ray_mutation_defense_validated"] = launched_in_actor
    findings["ray_mutation_defense_validated"] = launched_in_actor
    if not launched_in_actor:
        env_report["ray_mutation_defense_note"] = ("srun children not confirmed launched from inside a "
                                                   "Ray actor; env defense remains unvalidated")

    _ray_shutdown_quiet(ray)

    # --- read markers with REVALIDATING readers ONLY ---
    mk = _read_a2b_markers(bootdir)
    findings["markers"] = mk
    rr = mk.get("root_result") or {}
    ct = mk.get("connect_timing") or {}
    cp = mk.get("connect_preprobe") or {}
    dd = mk.get("connect_disconnected1") or {}

    reached_two = bool(rr.get("reached_two"))
    proved_remote = bool(rr.get("proved_remote_by_oracle"))
    remote_diff = bool(rr.get("remote_locality_id_differs"))
    observed_leave = bool(rr.get("observed_connector_leave"))
    res_val, ora_val = rr.get("result"), rr.get("oracle")
    oracle_match = (res_val is not None and ora_val is not None and res_val == ora_val)

    preprobe_active = bool(cp.get("agas_preprobe_active") if cp.get("agas_preprobe_active") is not None
                           else ct.get("agas_preprobe_active"))
    preprobe_ok = bool(cp.get("agas_preprobe_ok") if cp.get("agas_preprobe_ok") is not None
                       else ct.get("agas_preprobe_ok"))
    preprobe_ms = (cp.get("agas_preprobe_ms") if cp.get("agas_preprobe_ms") is not None
                   else ct.get("agas_preprobe_ms"))
    preprobe_ok_marker = bool(mk.get("connect_preprobe_ok_present"))
    served_present = bool(mk.get("served1_ok_present"))
    disconnected_clean = bool(dd.get("clean"))
    finalize_present = (mk.get("root_finalize_done") is not None)
    root_finalized_clean = bool(finalize_present and root_rc == 0)

    a_clean, a_pids = _orphan_check_node(nodeA)
    b_clean, b_pids = _orphan_check_node(nodeB)
    no_orphans = bool(a_clean and b_clean)
    findings["orphans"] = {"nodeA_clean": a_clean, "nodeA_pids": a_pids,
                           "nodeB_clean": b_clean, "nodeB_pids": b_pids}

    mech = bool(reached_two and proved_remote and oracle_match and remote_diff and observed_leave
                and disconnected_clean and root_finalized_clean and no_orphans
                and preprobe_ok and preprobe_ok_marker)

    results.update({
        "agas_preprobe_active": preprobe_active,
        "agas_preprobe_ok": preprobe_ok,
        "agas_preprobe_ms": preprobe_ms,
        "connect_preprobe_ok_marker_present": preprobe_ok_marker,
        "served1_ok_present": served_present,
        "reached_two": reached_two,
        "proved_remote_by_oracle": proved_remote,
        "oracle_match": oracle_match,
        "remote_locality_id_differs": remote_diff,
        "observed_connector_leave": observed_leave,
        "graceful_disconnect_clean": disconnected_clean,
        "root_finalized_clean": root_finalized_clean,
        "no_orphans": no_orphans,
        "mechanism_success_on_disk": mech,
        "failure_restart_used": False,
    })

    gate = bool(
        findings.get("ray_imported_lazily_in_run_phase")
        and env_ok and ldd_both
        and results["root_connector_launched"]
        and not findings.get("connector_gated_on_root_ready")
        and preprobe_active and preprobe_ok and preprobe_ok_marker
        and reached_two and oracle_match and remote_diff
        and observed_leave and disconnected_clean
        and root_finalized_clean and no_orphans
        and not results["failure_restart_used"]
    )
    findings["overall"] = "pass" if gate else "fail"
    findings["reason"] = (
        "Ray-supervised clean two-node HPX launch passed: connector AGAS pre-probe ok, root proved the "
        "remote closed-int64 action via oracle on a differing locality, root observed a clean connector "
        "leave, root finalized clean, no orphans"
        if gate else
        "A2b gate not fully met; inspect results (preprobe/reached_two/oracle/leave/disconnect/finalize/"
        "orphans)"
    )
    return findings


def _ray_shutdown_quiet(ray):
    try:
        ray.shutdown()
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="exp57 Ray/Slurm-supervised two-node HPX island (Slice 0)")
    ap.add_argument("--binary", default=None)
    ap.add_argument("--phase",
                    choices=["check-config", "reachability", "slurm-localize", "slurm-localize-deep",
                             "nfs-negative-poll", "settle-diag", "run"],
                    default="settle-diag")
    ap.add_argument("--ready-timeout", type=int, default=60)
    ap.add_argument("--leave-timeout", type=int, default=60)
    ap.add_argument("--serve-timeout", type=int, default=60)
    ap.add_argument("--threads", type=int, default=2,
                    help="HPX worker threads per locality; keep <= the Slurm cpuset granted per node "
                         "or record it as intentionally small")
    ap.add_argument("--agas-port", type=int, default=7940)
    ap.add_argument("--hpx-port", type=int, default=7941)
    ap.add_argument("--prefer-subnet", default=None,
                    help="IPv4 prefix to prefer when selecting the routable compute-node interface")
    ap.add_argument("--shared-dir", default=None,
                    help="shared-FS dir for two-node rendezvous; default experiment-local "
                         "_two_node_runs/ (on /work when the repo lives there). NEVER node-local /tmp.")
    ap.add_argument("--hpx-logs", action="store_true",
                    help="enable HPX parcel logging (level 5) on both roles; raw logs stay under the "
                         "ignored _two_node_runs/ bootdir, only compact tails enter the JSON")
    ap.add_argument("--aggregate", default=os.path.join(HERE, "settle_attribution.json"))
    ap.add_argument("--localize-aggregate", default=os.path.join(HERE, "slurm_localize.json"),
                    help="top-level latest path for the Slice A.0 slurm-localize diagnostic JSON")
    ap.add_argument("--localize-deep-aggregate", default=os.path.join(HERE, "slurm_localize_deep.json"),
                    help="top-level latest path for the Slice A.0-deep slurm-localize-deep JSON")
    ap.add_argument("--nfs-neg-aggregate", default=os.path.join(HERE, "nfs_negative_poll.json"),
                    help="top-level latest path for the nfs-negative-poll diagnostic JSON")
    ap.add_argument("--neg-poll-pre-s", type=float, default=8.0,
                    help="seconds to poll for the marker BEFORE it exists (seeds the negative-dentry "
                         "cache) in nfs-negative-poll")
    # --- Slice A2 SCAFFOLD args (parsed/stored only; the launch path is deferred) ---
    ap.add_argument("--run-aggregate", default=os.path.join(HERE, "run_aggregate.json"),
                    help="top-level latest path for the Slice A2b Ray-supervised run aggregate JSON")
    ap.add_argument("--issue-gap-ms", type=int, default=0,
                    help="reserved; A2b issues root/connector srun back-to-back and records the measured "
                         "srun_issue_gap_ms (this knob is not used to insert a delay)")
    ap.add_argument("--agas-preprobe-port", type=int, default=None,
                    help="connector AGAS TCP pre-probe port (A2b); defaults to --agas-port")
    ap.add_argument("--agas-preprobe-timeout-ms", type=int, default=60000,
                    help="connector AGAS TCP pre-probe timeout (ms) passed to the connector in A2b")
    args = ap.parse_args()

    if args.phase == "slurm-localize":
        # Slice A.0: RAY-FREE localization of the ~25 s node-specific srun stall. No HPX, no binary.
        print("[exp57] phase: slurm-localize (Ray-free ~25 s srun-stall localization) ...")
        loc = slurm_localize(args)
        if loc is None:
            skip = {"experiment": "57_ray_slurm_supervised_two_node_island", "phase": "slurm_localize",
                    "ray_free": True, "overall": "skip",
                    "skip_or_fail_reason": "no >=2-node Slurm allocation; A.0 deferred to a Rostam "
                                           "two-node allocation", "claim_fence": _LOCALIZE_FENCE}
            _write_agg(args.localize_aggregate, skip)
            print("SKIP: no >=2-node Slurm allocation; slurm-localize deferred to Rostam.")
            return 0
        loc = {"experiment": "57_ray_slurm_supervised_two_node_island", "phase": "slurm_localize",
               "ray_free": True, **loc}
        _write_agg(args.localize_aggregate, loc)
        row = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "run_id": loc.get("run_id"),
               "bootdir": loc.get("bootstrap_dir"), "nodeA": loc.get("nodeA"), "nodeB": loc.get("nodeB"),
               "slow_node": loc.get("slow_node"), "selected_subnet": loc.get("selected_subnet"),
               "localized_cause": loc.get("localized_cause"), "overall": loc.get("overall"),
               "overlap_helps": loc.get("overlap_helps"), "overlap_delta_ms": loc.get("overlap_delta_ms"),
               "getent_ladder_suspected": loc.get("getent_ladder_suspected"),
               "srun_probe_nodeA_ms": loc.get("srun_probe_nodeA_ms"),
               "srun_probe_nodeB_ms": loc.get("srun_probe_nodeB_ms")}
        per_run_path, index_path = _preserve_named_artifacts(
            loc, loc.get("bootstrap_dir"), "slurm_localize.json", "slurm_localize_index.jsonl", row)
        print(f"[exp57] localized_cause={loc.get('localized_cause')} overall={loc.get('overall')} "
              f"slow_node={loc.get('slow_node')} overlap_helps={loc.get('overlap_helps')} "
              f"getent_ladder={loc.get('getent_ladder_suspected')}")
        print(f"[exp57] recommend -> {loc.get('recommended_slice_a_srun_flags')}")
        print(f"[exp57] wrote latest   -> {args.localize_aggregate}")
        if per_run_path:
            print(f"[exp57] wrote per-run  -> {per_run_path}")
            print(f"[exp57] appended index -> {index_path}")
        return 0

    if args.phase == "slurm-localize-deep":
        # Slice A.0-deep: RAY-FREE separation of login-shell vs binary/loader vs marker visibility.
        print("[exp57] phase: slurm-localize-deep (login-shell vs binary/loader vs marker visibility) ...")
        dbin = locate_binary(args.binary)  # optional: binary probe runs only if built
        loc = slurm_localize_deep(args, dbin)
        if loc is None:
            skip = {"experiment": "57_ray_slurm_supervised_two_node_island",
                    "phase": "slurm_localize_deep", "ray_free": True, "overall": "skip",
                    "skip_or_fail_reason": "no >=2-node Slurm allocation; A.0-deep deferred to a Rostam "
                                           "two-node allocation", "claim_fence": _LOCALIZE_DEEP_FENCE}
            _write_agg(args.localize_deep_aggregate, skip)
            print("SKIP: no >=2-node Slurm allocation; slurm-localize-deep deferred to Rostam.")
            return 0
        loc = {"experiment": "57_ray_slurm_supervised_two_node_island", "phase": "slurm_localize_deep",
               "ray_free": True, **loc}
        _write_agg(args.localize_deep_aggregate, loc)
        row = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "run_id": loc.get("run_id"),
               "bootdir": loc.get("bootstrap_dir"), "nodeA": loc.get("nodeA"), "nodeB": loc.get("nodeB"),
               "selected_subnet": loc.get("selected_subnet"), "localized_cause": loc.get("localized_cause"),
               "compound_suspect": loc.get("compound_suspect"), "overall": loc.get("overall"),
               "srun_login_shell_nodeA_ms": loc.get("srun_login_shell_nodeA_ms"),
               "srun_direct_hostname_nodeA_ms": loc.get("srun_direct_hostname_nodeA_ms"),
               "srun_binary_probe_nodeA_ms": loc.get("srun_binary_probe_nodeA_ms"),
               "marker_visibility_nodeA_ms": loc.get("marker_visibility_nodeA_ms"),
               "marker_visibility_nodeB_ms": loc.get("marker_visibility_nodeB_ms")}
        per_run_path, index_path = _preserve_named_artifacts(
            loc, loc.get("bootstrap_dir"), "slurm_localize_deep.json",
            "slurm_localize_deep_index.jsonl", row)
        print(f"[exp57] localized_cause={loc.get('localized_cause')} overall={loc.get('overall')} "
              f"compound={loc.get('compound_suspect')}")
        print(f"[exp57] login_shell A/B={loc.get('srun_login_shell_nodeA_ms')}/"
              f"{loc.get('srun_login_shell_nodeB_ms')} ms | direct A/B="
              f"{loc.get('srun_direct_hostname_nodeA_ms')}/{loc.get('srun_direct_hostname_nodeB_ms')} ms")
        print(f"[exp57] binary A/B={loc.get('srun_binary_probe_nodeA_ms')}/"
              f"{loc.get('srun_binary_probe_nodeB_ms')} ms (avail={loc.get('binary_probe_available')}) | "
              f"marker_vis A/B={loc.get('marker_visibility_nodeA_ms')}/"
              f"{loc.get('marker_visibility_nodeB_ms')} ms")
        print(f"[exp57] recommend -> {loc.get('recommended_slice_a_srun_flags')}")
        print(f"[exp57] wrote latest   -> {args.localize_deep_aggregate}")
        if per_run_path:
            print(f"[exp57] wrote per-run  -> {per_run_path}")
            print(f"[exp57] appended index -> {index_path}")
        return 0

    if args.phase == "nfs-negative-poll":
        # RAY-FREE supervisor-side NFS negative-dentry / pre-existence-poll test. No HPX, no binary.
        print("[exp57] phase: nfs-negative-poll (supervisor pre-existence marker-polling test) ...")
        loc = nfs_negative_poll(args)
        if loc is None:
            skip = {"experiment": "57_ray_slurm_supervised_two_node_island",
                    "phase": "nfs_negative_poll", "ray_free": True, "overall": "skip",
                    "skip_or_fail_reason": "no >=2-node Slurm allocation; nfs-negative-poll deferred to "
                                           "a Rostam two-node allocation", "claim_fence": _NFS_NEG_FENCE}
            _write_agg(args.nfs_neg_aggregate, skip)
            print("SKIP: no >=2-node Slurm allocation; nfs-negative-poll deferred to Rostam.")
            return 0
        loc = {"experiment": "57_ray_slurm_supervised_two_node_island", "phase": "nfs_negative_poll",
               "ray_free": True, **loc}
        _write_agg(args.nfs_neg_aggregate, loc)
        row = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "run_id": loc.get("run_id"),
               "bootdir": loc.get("bootstrap_dir"), "nodeA": loc.get("nodeA"), "nodeB": loc.get("nodeB"),
               "selected_subnet": loc.get("selected_subnet"), "localized_cause": loc.get("localized_cause"),
               "overall": loc.get("overall"), "negative_poll_delay_seen": loc.get("negative_poll_delay_seen"),
               "negative_poll_delay_ms": loc.get("negative_poll_delay_ms"),
               "control_visible_max_ms": loc.get("control_visible_max_ms"),
               "negative_simple_visible_max_ms": loc.get("negative_simple_visible_max_ms"),
               "negative_robust_visible_max_ms": loc.get("negative_robust_visible_max_ms"),
               "revalidation_appears_to_help": loc.get("revalidation_appears_to_help")}
        per_run_path, index_path = _preserve_named_artifacts(
            loc, loc.get("bootstrap_dir"), "nfs_negative_poll.json", "nfs_negative_poll_index.jsonl", row)
        print(f"[exp57] localized_cause={loc.get('localized_cause')} overall={loc.get('overall')} "
              f"negative_poll_delay_seen={loc.get('negative_poll_delay_seen')}")
        print(f"[exp57] control_max={loc.get('control_visible_max_ms')} ms | "
              f"negative_simple_max={loc.get('negative_simple_visible_max_ms')} ms (Slice0 replica) | "
              f"negative_robust_max={loc.get('negative_robust_visible_max_ms')} ms | "
              f"revalidation_helps={loc.get('revalidation_appears_to_help')}")
        print(f"[exp57] wrote latest   -> {args.nfs_neg_aggregate}")
        if per_run_path:
            print(f"[exp57] wrote per-run  -> {per_run_path}")
            print(f"[exp57] appended index -> {index_path}")
        return 0

    if args.phase == "run":
        # Slice A2b: Ray/Slurm-SUPERVISED clean two-node HPX launch. Ray (lazy-imported inside this phase
        # ONLY) issues root & connector srun from inside Ray actors under the pre-Ray-anchored child env,
        # near-concurrently and WITHOUT gating the connector on root.ready; the connector uses its AGAS
        # TCP pre-probe; HPX carries the closed-int64 action. No failure/restart (exp58).
        print("[exp57] phase: run (Slice A2b -- Ray/Slurm-supervised CLEAN two-node HPX launch; connector "
              "AGAS TCP pre-probe readiness; NO failure/restart) ...")
        nodes = slurm_nodes()
        binary = locate_binary(args.binary)
        cfg = check_config(binary) if (binary and nodes) else None
        findings = run_phase_a2b(binary, args, cfg)
        if findings is None:
            agg = assemble_run_a2b(binary, cfg, None, "skip",
                                   "no >=2-node Slurm allocation; A2b Ray-supervised run deferred to a "
                                   "Rostam two-node allocation", args)
            _write_agg(args.run_aggregate, agg)
            print("SKIP: no >=2-node Slurm allocation; A2b run deferred to Rostam.")
            return 0
        overall = findings.get("overall", "fail")
        agg = assemble_run_a2b(binary, cfg, findings, overall, findings.get("reason"), args)
        _write_agg(args.run_aggregate, agg)
        res = agg.get("results") or {}
        row = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
               "run_id": (os.path.basename(findings["bootstrap_dir"].rstrip("/"))
                          if findings.get("bootstrap_dir") else None),
               "bootdir": findings.get("bootstrap_dir"),
               "nodeA": findings.get("nodeA"), "nodeB": findings.get("nodeB"),
               "overall": overall,
               "mechanism_success_on_disk": res.get("mechanism_success_on_disk"),
               "ldd_both_use_expected_gcc_libstdcxx": res.get("ldd_both_use_expected_gcc_libstdcxx"),
               "agas_preprobe_active": res.get("agas_preprobe_active"),
               "agas_preprobe_ok": res.get("agas_preprobe_ok"),
               "agas_preprobe_ms": res.get("agas_preprobe_ms"),
               "reached_two": res.get("reached_two"), "oracle_match": res.get("oracle_match"),
               "remote_locality_id_differs": res.get("remote_locality_id_differs"),
               "observed_connector_leave": res.get("observed_connector_leave"),
               "graceful_disconnect_clean": res.get("graceful_disconnect_clean"),
               "root_finalized_clean": res.get("root_finalized_clean"),
               "no_orphans": res.get("no_orphans"),
               "srun_issue_gap_ms": res.get("srun_issue_gap_ms"),
               "ray_mutation_defense_validated": findings.get("ray_mutation_defense_validated")}
        per_run_path, index_path = _preserve_named_artifacts(
            agg, findings.get("bootstrap_dir"), "run_aggregate.json", "run_aggregate_index.jsonl", row)
        print(f"[exp57] A2b overall={overall} mechanism_success_on_disk={res.get('mechanism_success_on_disk')} "
              f"nodes={findings.get('nodeA')}/{findings.get('nodeB')}")
        print(f"[exp57] preprobe active={res.get('agas_preprobe_active')} ok={res.get('agas_preprobe_ok')} "
              f"ms={res.get('agas_preprobe_ms')} | reached_two={res.get('reached_two')} "
              f"oracle_match={res.get('oracle_match')} remote_differs={res.get('remote_locality_id_differs')}")
        print(f"[exp57] leave={res.get('observed_connector_leave')} "
              f"disconnect_clean={res.get('graceful_disconnect_clean')} "
              f"root_finalized_clean={res.get('root_finalized_clean')} no_orphans={res.get('no_orphans')} "
              f"issue_gap_ms={res.get('srun_issue_gap_ms')}")
        print(f"[exp57] wrote latest   -> {args.run_aggregate}")
        if per_run_path:
            print(f"[exp57] wrote per-run  -> {per_run_path}")
            print(f"[exp57] appended index -> {index_path}")
        return 0

    binary = locate_binary(args.binary)
    if binary is None:
        _write_agg(args.aggregate, assemble_settle(None, None, None, "skip",
                   "two_node_island_spike not built (see CMakeLists.txt)"))
        print("SKIP: two_node_island_spike not found; build it first (CMakeLists.txt).")
        return 0
    print(f"[exp57] binary: {binary}")

    cfg = check_config(binary)
    print(f"[exp57] tcp_parcelport_available={cfg['tcp_parcelport_available']} "
          f"config=({cfg['hpx_parcelport_config']}) version={cfg['hpx_version']}")
    if not cfg["tcp_parcelport_available"]:
        _write_agg(args.aggregate, assemble_settle(binary, cfg, None, "skip",
                   "TCP parcelport not confirmed in this HPX build; STOP (no MPI/LCI substitution)."))
        print("SKIP: TCP parcelport not confirmed; STOP (no MPI/LCI substitution).")
        return 0
    if args.phase == "check-config":
        _write_agg(args.aggregate, assemble_settle(binary, cfg, None, "skip", "check-config only"))
        print(f"[exp57] check-config done -> {args.aggregate}")
        return 0

    if args.phase == "reachability":
        print("[exp57] phase: reachability (socket-only) ...")
        sel = select_and_reachability(args)
        if sel is None:
            _write_agg(args.aggregate, assemble_settle(binary, cfg, None, "skip",
                       "no >=2-node Slurm allocation; reachability deferred to a Rostam allocation"))
            print("SKIP: no >=2-node Slurm allocation; reachability deferred to Rostam.")
            return 0
        _write_agg(args.aggregate, assemble_settle(binary, cfg, sel, "skip",
                   sel.get("reason") or "reachability-only (no HPX launch)"))
        print(f"[exp57] reachability bidi={sel.get('bidirectional_port_check_passed')} "
              f"b_to_a={sel.get('reachability_b_to_a')} a_to_b={sel.get('reachability_a_to_b')} "
              f"nodes={sel.get('nodeA')}/{sel.get('nodeB')} ips={sel.get('nodeA_ip')}/"
              f"{sel.get('nodeB_ip')} -> {args.aggregate}")
        return 0

    # settle-diag (RAY-FREE two-node launch + attribution)
    print("[exp57] phase: settle-diag (Ray-free two-node settle attribution) ...")
    diag = settle_diag(binary, args, cfg)
    if diag is None:
        _write_agg(args.aggregate, assemble_settle(binary, cfg, None, "skip",
                   "no >=2-node Slurm allocation; settle-diag deferred to a Rostam two-node allocation"))
        print("SKIP: no >=2-node Slurm allocation; settle-diag deferred to Rostam.")
        return 0
    overall = diag.get("overall", "inconclusive")
    agg = assemble_settle(binary, cfg, diag, overall, diag.get("reason"))
    _write_agg(args.aggregate, agg)                      # top-level "latest result" (still written)
    per_run_path, index_path = _preserve_run_artifacts(agg, diag, args)  # durable per-run copy + index
    print(f"[exp57] overall={overall} settle_delay_ms={diag.get('settle_delay_ms')} "
          f"classification={diag.get('settle_delay_classification')} "
          f"attributed={diag.get('settle_delay_attributed')}")
    print(f"[exp57] wrote latest   -> {args.aggregate}")
    if per_run_path:
        print(f"[exp57] wrote per-run  -> {per_run_path}")
        print(f"[exp57] appended index -> {index_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
