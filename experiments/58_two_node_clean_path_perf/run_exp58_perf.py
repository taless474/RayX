#!/usr/bin/env python3
"""exp58 Slice 1 -- RAY-FREE two-node HPX TCP clean-path performance baseline runner.

This runner launches the exp58 perf spike (two_node_perf_spike) as a two-node HPX island over SLURM
`srun` and collects the Class-B timing characterization the root emits (depth-1 QD1 RTT floor +
pipelined throughput at queue depths [8,32,128]). It is the FAIR INTERNAL BASELINE: same spike, same
workload, NO Ray supervisor / control plane.

  * NO Ray import anywhere (module top or any phase). The Ray-supervised runner is a LATER slice.
  * NO failure/restart, NO poison detection, NO detector timing.
  * root + connector are launched via plain `srun --export=ALL`, NOT Ray actors.
  * Loader hygiene (GCC-15 libstdc++ ldd gate), node/IP selection (`--prefer-subnet`), bidirectional
    reachability, and revalidating shared-FS marker reads are ported in spirit from exp57.
  * All critical marker waits use revalidating reads (os.listdir parent revalidation) -- NEVER plain
    os.path.exists polling on a not-yet-existing critical marker (the exp57 NFS negative-dentry fix).

Primary phase:  --phase rayfree-baseline
Preflight:      --phase check-config   (TCP parcelport availability + HPX version)

CLAIM FENCE: clean-path characterization only; TCP parcelport only; closed-int64 only; Rostam/
allocation-specific; no network/fabric performance claim; no Ray-vs-RayX claim; no single-run speedup
claim; per-island stats are primary, pooled K*R only supplementary; QD1 is an RTT floor, not per-action
cost.
"""

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BINARY_BASENAME = "two_node_perf_spike"
DEFAULT_BINARY_CANDIDATES = [
    os.path.join(HERE, "build", BINARY_BASENAME),
    os.path.join(HERE, "build", "Release", BINARY_BASENAME),
]

TCP_ENABLE_FLAGS = [
    "--hpx:ini=hpx.parcel.bootstrap=tcp",
    "--hpx:ini=hpx.parcel.tcp.enable=1",
]
# Primary QD1 RTT-floor runs disable scheduler idle backoff so the floor is not inflated by
# worker wake-from-idle latency. Recorded honestly either way by the spike (scheduler_tuning block).
IDLE_BACKOFF_DISABLE_FLAG = "--hpx:ini=hpx.max_idle_backoff_time=0"

_SYSTEM_LIBSTDCXX_PREFIXES = ("/lib64/", "/usr/lib64/", "/lib/", "/usr/lib/")


# ---------------------------------------------------------------------------------------------------
# small helpers (shared shape with exp56/exp57; Ray-free)
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


def _run(argv, timeout=30):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except Exception:  # noqa: BLE001
        return None


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def read_json_eventually(path, timeout_s=15.0, poll_s=0.25, required=False):
    """Robust marker read tolerant of shared-FS (NFS) visibility lag. Retries exists/open/parse and
    forces parent-directory revalidation between attempts (defeats negative-dentry / attribute caching)
    -- the exp57 fix. Returns (obj_or_None, diag)."""
    diag = {"path": os.path.basename(path), "attempts": 0, "first_seen_ms": None,
            "parsed_ms": None, "last_error": "missing", "required": bool(required), "ok": False}
    parent = os.path.dirname(path) or "."
    t0 = time.monotonic()
    deadline = t0 + max(0.0, timeout_s)
    while True:
        diag["attempts"] += 1
        try:
            os.listdir(parent)            # force a fresh directory lookup (revalidate NFS cache)
        except OSError:
            pass
        if os.path.exists(path):
            if diag["first_seen_ms"] is None:
                diag["first_seen_ms"] = int((time.monotonic() - t0) * 1000)
            try:
                with open(path) as f:
                    data = f.read()
                obj = json.loads(data)
                diag["parsed_ms"] = int((time.monotonic() - t0) * 1000)
                diag["last_error"] = None
                diag["ok"] = True
                return obj, diag
            except OSError as e:
                diag["last_error"] = "OSError: " + str(e)[:120]
            except ValueError as e:
                diag["last_error"] = "JSONDecodeError: " + str(e)[:120]
        else:
            diag["last_error"] = "missing"
        if time.monotonic() >= deadline:
            return None, diag
        time.sleep(poll_s)


def exists_eventually(path, timeout_s=15.0, poll_s=0.25):
    """Existence-only revalidating read for non-JSON flag markers (e.g. served1.ok). Returns
    (present_bool, diag). Same NFS revalidation, returns on first sight."""
    diag = {"path": os.path.basename(path), "attempts": 0, "first_seen_ms": None,
            "last_error": "missing", "ok": False}
    parent = os.path.dirname(path) or "."
    t0 = time.monotonic()
    deadline = t0 + max(0.0, timeout_s)
    while True:
        diag["attempts"] += 1
        try:
            os.listdir(parent)
        except OSError:
            pass
        if os.path.exists(path):
            diag["first_seen_ms"] = int((time.monotonic() - t0) * 1000)
            diag["last_error"] = None
            diag["ok"] = True
            return True, diag
        diag["last_error"] = "missing"
        if time.monotonic() >= deadline:
            return False, diag
        time.sleep(poll_s)


def exists_eventually_revalidating(path, timeout_s, poll_s, proc=None):
    """Revalidating wait used by the launch handshake (root.ready). Returns True as soon as the marker
    is visible; bails early if the watched process has already exited. NEVER a plain os.path.exists
    poll on a not-yet-existing critical marker."""
    present, _ = _wait_marker(path, timeout_s, poll_s, proc)
    return present


def _wait_marker(path, timeout_s, poll_s, proc=None):
    parent = os.path.dirname(path) or "."
    t0 = time.monotonic()
    deadline = t0 + max(0.0, timeout_s)
    attempts = 0
    while True:
        attempts += 1
        try:
            os.listdir(parent)
        except OSError:
            pass
        if os.path.exists(path):
            return True, {"attempts": attempts,
                          "first_seen_ms": int((time.monotonic() - t0) * 1000)}
        if proc is not None and proc.poll() is not None:
            return False, {"attempts": attempts, "proc_exited": True}
        if time.monotonic() >= deadline:
            return False, {"attempts": attempts, "timed_out": True}
        time.sleep(poll_s)


def _child_env():
    """Inherit the FULL environment. The binary is dynamically linked against HPX/Boost/hwloc and on
    Rostam resolves them via LD_LIBRARY_PATH set by `module load` (GCC 15 libstdc++ must be present).
    SLURM_EXPORT_ENV=ALL should already be set so srun children inherit the loader path."""
    env = dict(os.environ)
    env.setdefault("SLURM_EXPORT_ENV", "ALL")
    return env


# --- pre-Ray environment anchoring (used ONLY by the Ray-supervised phase) --------------------------
# Captured at MODULE IMPORT. This file NEVER imports Ray at module top, so this snapshot genuinely
# precedes any ray.init(). The Ray-supervised path overlays these preserve-list keys back onto the Ray
# actor's child env so Ray's own env rewrites (PATH / LD_LIBRARY_PATH / CUDA_VISIBLE_DEVICES / OMP_*)
# cannot strip the GCC-15 loader path off the HPX child. The HPX spike, workload, and timing are
# unchanged -- only how the srun child env is anchored differs.
_PRE_RAY_ENV_SNAPSHOT = dict(os.environ)
_ENV_PRESERVE_KEYS = (
    "PATH", "LD_LIBRARY_PATH", "HOME", "TMPDIR",
    "SLURM_JOB_ID", "SLURM_JOB_NODELIST", "SLURM_NODELIST",
    "SLURM_JOB_NUM_NODES", "SLURM_EXPORT_ENV",
)
_ENV_PRESERVE_OPTIONAL = (
    "LD_PRELOAD", "LIBRARY_PATH", "CPATH", "CPLUS_INCLUDE_PATH", "PKG_CONFIG_PATH",
)
_ENV_LOAD_BEARING_REQUIRED = (
    "PATH", "LD_LIBRARY_PATH", "HOME",
    "SLURM_JOB_ID", "SLURM_JOB_NODELIST", "SLURM_JOB_NUM_NODES",
)


def _preserve_child_env():
    """Build the Ray-actor child env: overlay the pre-Ray preserve-list keys back over the current
    (post-ray.init) env so the GCC-15 loader path survives Ray's mutation. Same allow-list shape as
    exp57 A2b."""
    base = dict(_PRE_RAY_ENV_SNAPSHOT)
    child = dict(os.environ)
    for k in _ENV_PRESERVE_KEYS:
        if k in base:
            child[k] = base[k]
        else:
            child.pop(k, None)
    for k in _ENV_PRESERVE_OPTIONAL:
        if k in base:
            child[k] = base[k]
    child.setdefault("SLURM_EXPORT_ENV", "ALL")
    return child


def _env_preserve_report(child_env):
    """Structural report on the child env (no values dumped): which required load-bearing keys are
    present, and whether the GCC-15 loader path survived."""
    missing = [k for k in _ENV_LOAD_BEARING_REQUIRED if not child_env.get(k)]
    return {
        "child_env_anchored_to_pre_ray_baseline": True,
        "load_bearing_required": list(_ENV_LOAD_BEARING_REQUIRED),
        "load_bearing_missing": missing,
        "has_ld_library_path": bool(child_env.get("LD_LIBRARY_PATH")),
        "load_bearing_ok": (len(missing) == 0),
    }


class _SrunRunner:
    """Minimal Ray actor body. Launches ONE srun child with an EXPLICIT pre-Ray-anchored env override
    (env=) -- so Ray's own actor-env mutation cannot reach the HPX child -- waits for it, and returns
    only process status (rc, launch offset). It carries NO HPX action/data payload and stores no HPX
    result in the Ray object store: HPX owns the action/data path over the TCP parcelport."""
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


def _is_numeric_ipv4(ip):
    try:
        socket.inet_aton(ip or "")
        return bool(ip) and all(part.isdigit() for part in ip.split("."))
    except OSError:
        return False


def _resolve_shared_dir(args):
    """Two-node rendezvous MUST be on a shared filesystem; node-local /tmp is invisible across nodes."""
    if args.shared_dir:
        return os.path.abspath(args.shared_dir), "explicit --shared-dir"
    return os.path.join(HERE, "_perf_runs"), "default (experiment-local _perf_runs)"


def _orphan_check_node(node):
    """pgrep exits 1 when there are NO matches -> the clean/no-orphans case, not an error."""
    o = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "pgrep", "-f", BINARY_BASENAME], timeout=20)
    if o is None:
        return None, []
    pids = [p for p in o.stdout.split() if p] if o.returncode == 0 else []
    return (len(pids) == 0), pids


def _base_hpx_flags(role, threads, bind, agas_ip, agas_port, hpx_ip, hpx_port, pin_flags, extra=()):
    argv = [
        "--hpx:agas={}:{}".format(agas_ip, agas_port),
        "--hpx:hpx={}:{}".format(hpx_ip, hpx_port),
        "--hpx:threads={}".format(threads),
        "--hpx:bind={}".format(bind),
        "--hpx:ignore-batch-env",
    ]
    argv += list(pin_flags)
    if role == "root":
        argv.append("--hpx:expect-connecting-localities")
    argv += list(extra)
    return argv


def tcp_pin_flags(cfg):
    flags = list(TCP_ENABLE_FLAGS)
    for pp, present in (cfg or {}).get("parcelports_present", {}).items():
        if pp != "tcp" and present:
            flags.append(f"--hpx:ini=hpx.parcel.{pp}.enable=0")
    return flags


# ---------------------------------------------------------------------------------------------------
# TCP parcelport availability + version (preflight)
# ---------------------------------------------------------------------------------------------------
def check_config(binary):
    p = find_free_port()
    bd = tempfile.mkdtemp(prefix="exp58_cfg_")
    out = _run([binary, "--role", "root", "--bootstrap", bd, "--ready-timeout", "1",
                "--leave-timeout", "1", f"--hpx:agas=127.0.0.1:{p}", f"--hpx:hpx=127.0.0.1:{p}",
                "--hpx:ignore-batch-env", "--hpx:dump-config"], timeout=30)
    dump = (out.stdout + out.stderr) if out else ""
    ver = _run([binary, "--hpx:version"], timeout=20)
    ver_text = ((ver.stdout or "") + (ver.stderr or "")) if ver else ""
    banner = (ver_text.strip().splitlines()[0] if ver_text.strip() else None)
    # provenance fix: the banner line carries no version digits; parse the real version from the FULL
    # --hpx:version output. Match ONLY the HPX-specific line ("HPX: V1.11.0 ...") -- do NOT fall back to
    # any x.y.z triple, which would mistakenly grab the Boost/compiler version. Never fabricate.
    parsed = None
    m = re.search(r"HPX:?\s*V?(\d+\.\d+\.\d+)", ver_text)
    if m:
        parsed = m.group(1)
    if parsed:
        version = parsed
        capture_note = "parsed from the HPX version line of --hpx:version output"
    elif banner:
        version = banner
        capture_note = "only the HPX banner line was available; no HPX version line parsed"
    else:
        version = None
        capture_note = "--hpx:version produced no parseable output"
    # opportunistic provenance: allocator + HPX build type from the same output (null when absent)
    am = re.search(r"Allocator:\s*(\S+)", ver_text)
    bt = re.search(r"\n\s*Type:\s*(\S+)", ver_text)
    gm = re.search(r"Git:\s*(\S+)", ver_text)
    hpx_allocator = am.group(1) if am else None
    hpx_build_type_reported = bt.group(1) if bt else None
    hpx_git = gm.group(1) if gm else None
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
    return {
        "tcp_parcelport_available": tcp_available,
        "parcel_bootstrap": bootstrap,
        "parcelports_present": present,
        "hpx_version": version,
        "hpx_version_parsed": parsed,
        "hpx_version_banner": banner,
        "hpx_version_raw_tail": (ver_text.strip()[-400:] or None),
        "hpx_version_capture_note": capture_note,
        "hpx_allocator": hpx_allocator,
        "hpx_build_type_reported": hpx_build_type_reported,
        "hpx_git": hpx_git,
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
    nodes = slurm_nodes()
    if not nodes:
        return None
    nodeA, nodeB = nodes
    ifaceA, A_ip = select_node_ip(nodeA, args.prefer_subnet)
    ifaceB, B_ip = select_node_ip(nodeB, args.prefer_subnet)
    sel = {"two_node_run": True, "nodeA": nodeA, "nodeB": nodeB, "nodeA_ip": A_ip, "nodeB_ip": B_ip,
           "parcel_interface": (f"{ifaceA}/{ifaceB}" if (ifaceA and ifaceB) else None),
           "selected_interface_nodeA": ifaceA, "selected_interface_nodeB": ifaceB,
           "selected_subnet": args.prefer_subnet or (A_ip.rsplit('.', 1)[0] + "." if A_ip else None)}
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
# GCC-15 libstdc++ loader-hygiene gate (ported compact from exp57)
# ---------------------------------------------------------------------------------------------------
def _parse_ldd_libstdcxx(stdout):
    for line in (stdout or "").splitlines():
        if "libstdc++.so.6" in line and "=>" in line:
            rhs = line.split("=>", 1)[1].strip()
            return rhs.split(" ")[0].strip() or None
    return None


def _is_system_libstdcxx(path):
    rp = os.path.realpath(path) if path else ""
    return any(rp.startswith(pfx) for pfx in _SYSTEM_LIBSTDCXX_PREFIXES)


def _gxx_expected_libstdcxx(child_env, timeout_s=30):
    try:
        out = subprocess.run(["g++", "-print-file-name=libstdc++.so"], capture_output=True,
                             text=True, timeout=timeout_s, env=child_env)
    except Exception:  # noqa: BLE001
        return {"expected_libstdcxx_path": None, "expected_libstdcxx_dir": None}
    raw = (out.stdout or "").strip()
    if raw and ("/" in raw):
        rp = os.path.realpath(raw)
        return {"expected_libstdcxx_path": rp, "expected_libstdcxx_dir": os.path.dirname(rp)}
    return {"expected_libstdcxx_path": None, "expected_libstdcxx_dir": None}


def _ldd_check_node(node, binary, child_env, expected, timeout_s=60):
    try:
        out = subprocess.run(["srun", "-N1", "-n1", "--nodelist=" + node, "--export=ALL", "ldd",
                             binary], capture_output=True, text=True, timeout=timeout_s, env=child_env)
    except Exception:  # noqa: BLE001
        out = None
    stdout = (out.stdout if out else "") or ""
    resolved = _parse_ldd_libstdcxx(stdout)
    resolved_rp = os.path.realpath(resolved) if resolved else None
    exp_dir = (expected or {}).get("expected_libstdcxx_dir")
    not_system = bool(resolved) and not _is_system_libstdcxx(resolved)
    uses_expected = bool(resolved_rp and exp_dir
                         and os.path.dirname(resolved_rp) == os.path.realpath(exp_dir))
    gate_ok = bool(uses_expected and not_system)
    return {
        "node": node,
        "resolved_libstdcxx": resolved,
        "resolved_libstdcxx_dir": (os.path.dirname(resolved_rp) if resolved_rp else None),
        "expected_libstdcxx_dir": exp_dir,
        "ldd_uses_expected_gcc_libstdcxx": uses_expected,
        "resolved_is_not_system": not_system,
        "gcc15_libstdcxx_ok": gate_ok,
    }


def ldd_gate(binary, nodeA, nodeB, child_env):
    gxx = _gxx_expected_libstdcxx(child_env)
    lddA = _ldd_check_node(nodeA, binary, child_env, gxx)
    lddB = _ldd_check_node(nodeB, binary, child_env, gxx)
    both_ok = bool(lddA.get("gcc15_libstdcxx_ok") and lddB.get("gcc15_libstdcxx_ok"))
    return {
        "expected_libstdcxx_path": gxx.get("expected_libstdcxx_path"),
        "expected_libstdcxx_dir": gxx.get("expected_libstdcxx_dir"),
        "nodeA": lddA, "nodeB": lddB,
        "ldd_both_use_expected_gcc_libstdcxx": both_ok,
        "passed": both_ok,
    }


# ---------------------------------------------------------------------------------------------------
# best-effort environment metadata (CPU governor, endpoint binding)
# ---------------------------------------------------------------------------------------------------
def cpu_governor(node):
    o = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "bash", "-c",
              "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null"], timeout=20)
    if o and o.returncode == 0:
        g = (o.stdout or "").strip()
        return g or None
    return None


def verify_bound_ip(node, ip, port):
    """Best-effort: is something LISTENING on ip:port on `node`? Verifies endpoint BINDING (not just
    advertising). Returns (verified_bool_or_None, raw_tail)."""
    o = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "bash", "-c",
              f"ss -ltn 2>/dev/null | grep ':{port}' || true"], timeout=20)
    if o is None or o.returncode != 0:
        return None, None
    raw = (o.stdout or "").strip()
    if not raw:
        return False, None
    return (ip in raw), raw[-300:]


# ---------------------------------------------------------------------------------------------------
# per-island / across-island statistics over the raw depth-1 ns arrays
# ---------------------------------------------------------------------------------------------------
def _pct(sorted_vals, p):
    if not sorted_vals:
        return None
    import math
    k = int(math.ceil(p * len(sorted_vals))) - 1
    k = max(0, min(k, len(sorted_vals) - 1))
    return sorted_vals[k]


def island_stats(raw_ns):
    if not raw_ns:
        return {"count": 0}
    s = sorted(raw_ns)
    return {
        "count": len(s),
        "min_ns": s[0], "max_ns": s[-1],
        "mean_ns": int(sum(s) / len(s)),
        "p50_ns": _pct(s, 0.50), "p90_ns": _pct(s, 0.90), "p99_ns": _pct(s, 0.99),
    }


def across_island_summary(per_island):
    """Median + spread of the PER-ISLAND summaries (so a single anomalous island is visible rather than
    hidden in a pooled p99)."""
    def med(vals):
        v = sorted(x for x in vals if x is not None)
        if not v:
            return None
        n = len(v)
        return v[n // 2] if n % 2 else int((v[n // 2 - 1] + v[n // 2]) / 2)
    out = {}
    for key in ("p50_ns", "p90_ns", "p99_ns", "mean_ns"):
        vals = [isl.get(key) for isl in per_island if isl.get("count")]
        clean = [x for x in vals if x is not None]
        out[key + "_median"] = med(clean)
        out[key + "_min"] = (min(clean) if clean else None)
        out[key + "_max"] = (max(clean) if clean else None)
    return out


# ---------------------------------------------------------------------------------------------------
# one island: build argv, launch (direct OR Ray-supervised), collect + validate (shared collection)
# ---------------------------------------------------------------------------------------------------
def _build_island_argv(binary, args, cfg, sel, bootdir):
    """SAME binary / workload / flags for both launch strategies. The connector always gets the AGAS
    TCP pre-probe (belt-and-suspenders in the Ray-free path, the readiness mechanism in the Ray-
    supervised near-concurrent path)."""
    pin = tcp_pin_flags(cfg)
    nodeA, nodeB = sel["nodeA"], sel["nodeB"]
    A_ip, B_ip = sel["nodeA_ip"], sel["nodeB_ip"]
    pagas, phpx = args.agas_port, args.hpx_port
    idle_flag = [IDLE_BACKOFF_DISABLE_FLAG] if args.disable_idle_backoff else []
    root_argv = ["srun", "-N1", "-n1", "--nodelist=" + nodeA, "--export=ALL", binary,
                 "--role", "root", "--bootstrap", bootdir, "--x", str(args.x),
                 "--k", str(args.k), "--w", str(args.w), "--pipeline-depths", args.pipeline_depths,
                 "--ready-timeout", str(args.ready_timeout), "--leave-timeout", str(args.leave_timeout)]
    root_argv += _base_hpx_flags("root", args.threads, args.bind, A_ip, pagas, A_ip, pagas, pin,
                                 idle_flag)
    conn_argv = ["srun", "-N1", "-n1", "--nodelist=" + nodeB, "--export=ALL", binary,
                 "--role", "connect", "--bootstrap", bootdir, "--serve-timeout", str(args.serve_timeout)]
    conn_extra = idle_flag + ["--agas-preprobe-host", A_ip, "--agas-preprobe-port", str(pagas),
                              "--agas-preprobe-timeout-ms", str(args.agas_preprobe_timeout_ms)]
    conn_argv += _base_hpx_flags("connect", args.threads, args.bind, A_ip, pagas, B_ip, phpx, pin,
                                 conn_extra)
    return {"root_argv": root_argv, "conn_argv": conn_argv,
            "intended_root_ep": f"{A_ip}:{pagas}", "intended_conn_ep": f"{B_ip}:{phpx}"}


def _collect_island(args, sel, shared, rep_index, bootdir, argvs, root_rc, conn_rc, root_ready,
                    launch_meta, phase):
    """SHARED post-exit collection + validation for BOTH launch strategies. The gate set, timing
    schema, and artifact layout are IDENTICAL regardless of how the two srun processes were launched --
    this is what makes the Ray-free and Ray-supervised runs comparable. Reads only via the revalidating
    markers; no plain os.path.exists on a not-yet-existing critical marker."""
    nodeA, nodeB = sel["nodeA"], sel["nodeB"]
    A_ip, B_ip = sel["nodeA_ip"], sel["nodeB_ip"]
    intended_root_ep, intended_conn_ep = argvs["intended_root_ep"], argvs["intended_conn_ep"]

    perf, perf_diag = read_json_eventually(os.path.join(bootdir, "perf_root_result.json"),
                                           timeout_s=20.0, required=True)
    perf = perf or {}
    a_root, _ = read_json_eventually(os.path.join(bootdir, "attest_root.json"), timeout_s=15.0)
    a_conn, _ = read_json_eventually(os.path.join(bootdir, "attest_connect.json"), timeout_s=15.0)
    joined, _ = read_json_eventually(os.path.join(bootdir, "connect.joined1"), timeout_s=15.0)
    disc, _ = read_json_eventually(os.path.join(bootdir, "connect.disconnected1"), timeout_s=15.0)
    disc = disc or {}
    served_present, _ = exists_eventually(os.path.join(bootdir, "served1.ok"), timeout_s=15.0)
    preprobe_ok_present, _ = exists_eventually(os.path.join(bootdir, "connect.preprobe_ok"),
                                               timeout_s=5.0)

    host_differs = bool(a_root and a_conn and a_root.get("hostname") and a_conn.get("hostname")
                        and a_root["hostname"] != a_conn["hostname"])
    root_adv = (a_root or {}).get("advertised_hpx_endpoint")
    conn_adv = (a_conn or {}).get("advertised_hpx_endpoint")
    endpoint_advertise_ok = bool(root_adv == intended_root_ep and conn_adv == intended_conn_ep)

    # connector AGAS pre-probe disclosure (recorded; not a hard gate, matching the plan's gate list).
    # active/ok come from connect.disconnected1; per-attempt ms is not emitted by the current spike.
    agas_preprobe_active = disc.get("agas_preprobe_active")
    agas_preprobe_ok = disc.get("agas_preprobe_ok")

    # endpoint BINDING verification (best-effort; markers are read post-exit so a None/False here is
    # informational, not a hard gate)
    bound_verified, bound_raw = (None, None)

    orphanA_ok, orphanA = _orphan_check_node(nodeA)
    orphanB_ok, orphanB = _orphan_check_node(nodeB)
    no_orphans = bool(orphanA_ok and orphanB_ok)

    d1 = perf.get("remote_action_rtt_floor_depth1", {}) or {}
    raw_ns = d1.get("per_action_duration_ns_raw", []) or []

    # correctness gates -- IDENTICAL set for Ray-free and Ray-supervised
    gates = {
        "perf_json_present": bool(perf),
        "reached_two": bool(perf.get("reached_two")),
        "remote_locality_id_differs": bool(perf.get("remote_locality_id_differs")),
        "proved_remote_by_oracle": bool(perf.get("proved_remote_by_oracle")),
        "depth1_all_correct": bool(d1.get("steady_count") and d1.get("correct_count") == d1.get("K")),
        "pipeline_first_last_all_ok": all(
            row.get("pipeline_remote_proof_first_last")
            for row in (perf.get("remote_action_pipeline", []) or [])) if perf.get(
                "remote_action_pipeline") else False,
        "observed_connector_leave": bool(perf.get("observed_connector_leave")),
        "connector_disconnect_clean": bool(disc.get("clean")),
        "served_signal_present": bool(served_present),
        "host_differs": host_differs,
        "no_orphans": no_orphans,
        "no_shared_fs_marker_wait_in_steady_state": (
            perf.get("steady_state_contains_shared_fs_marker_wait") is False),
        "timing_on_hpx_thread": (perf.get("timing_loop_context") == "hpx_thread"),
        "perf_valid_build_tier": bool(perf.get("perf_valid")),
    }
    island_valid = all(gates.values())

    run_rec = {
        "rep_index": rep_index,
        "phase": phase,
        "bootstrap_dir": bootdir,
        "root_rc": root_rc,
        "connector_rc": conn_rc,
        "root_ready": root_ready,
        "launch": launch_meta,
        "root_argv": " ".join(argvs["root_argv"]),
        "connector_argv": " ".join(argvs["conn_argv"]),
        "intended_root_endpoint": intended_root_ep,
        "intended_connector_endpoint": intended_conn_ep,
        "advertised_root_endpoint": root_adv,
        "advertised_connector_endpoint": conn_adv,
        "endpoint_advertise_ok": endpoint_advertise_ok,
        "endpoints_bound_subnet_verified": bound_verified,
        "endpoint_bound_raw_tail": bound_raw,
        "root_bound_ip": A_ip, "connector_bound_ip": B_ip,
        "return_path_interface_verified": None,
        "numeric_ip_used": bool(_is_numeric_ipv4(A_ip) and _is_numeric_ipv4(B_ip)),
        "agas_preprobe_active": agas_preprobe_active,
        "agas_preprobe_ok": agas_preprobe_ok,
        "agas_preprobe_ms": None,
        "agas_preprobe_ms_note": "per-attempt ms is not emitted by the current spike artifacts",
        "connect_preprobe_ok_marker_present": bool(preprobe_ok_present),
        "perf_marker_diag": perf_diag,
        "orphanA_pids": orphanA, "orphanB_pids": orphanB,
        "gates": gates,
        "island_valid": island_valid,
        "island_stats_from_raw": island_stats(raw_ns),
        "perf_root_result": perf,   # full spike artifact (includes raw per-action arrays)
    }
    with open(os.path.join(bootdir, "run_aggregate.json"), "w") as f:
        json.dump(run_rec, f, indent=2)
    with open(os.path.join(shared, "perf_index.jsonl"), "a") as f:
        f.write(json.dumps({
            "phase": phase, "rep_index": rep_index, "bootstrap_dir": bootdir,
            "island_valid": island_valid, "root_rc": root_rc, "connector_rc": conn_rc,
            "launched_from_ray_actor": bool(launch_meta.get("launched_from_ray_actor")),
            "depth1": island_stats(raw_ns),
            "perf_valid": bool(perf.get("perf_valid")),
            "idle_backoff_mode": (perf.get("scheduler_tuning", {}) or {}).get("idle_backoff_mode"),
            "ts": time.time(),
        }) + "\n")
    return run_rec, raw_ns


def run_island(binary, args, cfg, sel, shared, rep_index):
    """Ray-free island: root via direct subprocess, connector launched after a revalidating root.ready
    wait. UNCHANGED measurement semantics from Slice 1/2."""
    bootdir = tempfile.mkdtemp(prefix=f"exp58_perf_r{rep_index}_", dir=shared)
    argvs = _build_island_argv(binary, args, cfg, sel, bootdir)
    child = _child_env()
    r_out = open(os.path.join(bootdir, "root.stdout"), "w")
    r_err = open(os.path.join(bootdir, "root.stderr"), "w")
    root = subprocess.Popen(argvs["root_argv"], stdout=r_out, stderr=r_err, env=child)

    root_ready = exists_eventually_revalidating(os.path.join(bootdir, "root.ready"),
                                                args.ready_timeout, 0.05, proc=root)
    conn = c_out = c_err = None
    if root_ready:
        c_out = open(os.path.join(bootdir, "connector.stdout"), "w")
        c_err = open(os.path.join(bootdir, "connector.stderr"), "w")
        conn = subprocess.Popen(argvs["conn_argv"], stdout=c_out, stderr=c_err, env=child)

    overall_deadline = time.time() + args.ready_timeout + args.leave_timeout + args.run_budget
    while time.time() < overall_deadline and root.poll() is None:
        time.sleep(0.1)
    root_rc = root.poll()
    if root_rc is None:
        root.kill()
    conn_rc = None
    if conn is not None:
        try:
            conn.wait(timeout=20)
        except Exception:
            conn.kill()
        conn_rc = conn.poll()
    for fh in (r_out, r_err, c_out, c_err):
        if fh is not None:
            try:
                fh.close()
            except OSError:
                pass

    launch_meta = {
        "launched_from_ray_actor": False,
        "launch_mode": "direct_subprocess",
        "connector_gated_on_root_ready": True,
        "root_srun_issue_ms": None, "connector_srun_issue_ms": None, "srun_issue_gap_ms": None,
        "root_actor_launch_offset_ms": None, "connector_actor_launch_offset_ms": None,
    }
    return _collect_island(args, sel, shared, rep_index, bootdir, argvs, root_rc, conn_rc, root_ready,
                           launch_meta, "rayfree-baseline")


def run_island_ray(binary, args, cfg, sel, shared, rep_index, ray, child_env):
    """Ray-supervised island: one _SrunRunner Ray actor per role, both srun issued NEAR-CONCURRENTLY
    (back-to-back .remote()), the connector NOT gated on root.ready -- it relies on its own AGAS TCP
    pre-probe. Ray carries only bootstrap/control + process status; HPX owns the action/data path.
    Same binary, workload, flags, env-preservation, and collection as the Ray-free path."""
    bootdir = tempfile.mkdtemp(prefix=f"exp58_perf_ray_r{rep_index}_", dir=shared)
    argvs = _build_island_argv(binary, args, cfg, sel, bootdir)

    root_timeout_s = args.ready_timeout + args.leave_timeout + args.run_budget
    conn_timeout_s = (args.ready_timeout + args.serve_timeout + 60
                      + int(args.agas_preprobe_timeout_ms / 1000))
    Runner = ray.remote(num_cpus=1)(_SrunRunner)
    root_actor = Runner.remote()
    conn_actor = Runner.remote()

    r_out = os.path.join(bootdir, "root.stdout")
    r_err = os.path.join(bootdir, "root.stderr")
    c_out = os.path.join(bootdir, "connector.stdout")
    c_err = os.path.join(bootdir, "connector.stderr")

    t0 = time.monotonic()
    fut_root = root_actor.run.remote("root", argvs["root_argv"], child_env, r_out, r_err, root_timeout_s)
    root_issue_ms = int((time.monotonic() - t0) * 1000)
    fut_conn = conn_actor.run.remote("connect", argvs["conn_argv"], child_env, c_out, c_err,
                                     conn_timeout_s)
    conn_issue_ms = int((time.monotonic() - t0) * 1000)

    get_timeout = max(root_timeout_s, conn_timeout_s) + 30
    proc_root = proc_conn = None
    ray_get_error = None
    try:
        proc_root, proc_conn = ray.get([fut_root, fut_conn], timeout=get_timeout)
    except Exception as e:  # noqa: BLE001  (GetTimeoutError / actor error -- still collect markers)
        ray_get_error = str(e)[:200]
    root_rc = (proc_root or {}).get("rc")
    conn_rc = (proc_conn or {}).get("rc")

    launch_meta = {
        "launched_from_ray_actor": bool((proc_root or {}).get("launched_from_ray_actor")
                                        and (proc_conn or {}).get("launched_from_ray_actor")),
        "launch_mode": "ray_actor_srun",
        "ray_supervisor_shape": "one _SrunRunner Ray actor per role; back-to-back .remote() issue",
        "connector_gated_on_root_ready": False,
        "root_srun_issue_ms": root_issue_ms,
        "connector_srun_issue_ms": conn_issue_ms,
        "srun_issue_gap_ms": conn_issue_ms - root_issue_ms,
        "root_actor_launch_offset_ms": (proc_root or {}).get("launch_offset_ms"),
        "connector_actor_launch_offset_ms": (proc_conn or {}).get("launch_offset_ms"),
        "ray_get_error": ray_get_error,
        "proc_root": proc_root, "proc_connect": proc_conn,
    }
    return _collect_island(args, sel, shared, rep_index, bootdir, argvs, root_rc, conn_rc, None,
                           launch_meta, "ray-supervised")


# ---------------------------------------------------------------------------------------------------
# aggregate builder (shared by both phases; identical schema so the runs are comparable)
# ---------------------------------------------------------------------------------------------------
def _build_aggregate(*, phase, baseline_kind, fair_note, overall, ray_imported, failure_restart_used,
                     cfg, sel, gate, governors, args, shared, src, islands, pooled_raw, extra=None):
    per_island = [isl["island_stats_from_raw"] for isl in islands]
    agg = {
        "phase": phase,
        "baseline_kind": baseline_kind,
        "fair_comparison_note": fair_note,
        "overall": overall,
        "ray_imported": ray_imported,
        "failure_restart_used": failure_restart_used,
        "cfg": cfg,
        "hpx_version": cfg.get("hpx_version"),
        "hpx_version_parsed": cfg.get("hpx_version_parsed"),
        "hpx_version_capture_note": cfg.get("hpx_version_capture_note"),
        "selection": {k: sel.get(k) for k in (
            "nodeA", "nodeB", "nodeA_ip", "nodeB_ip", "parcel_interface",
            "selected_interface_nodeA", "selected_interface_nodeB", "selected_subnet",
            "reachability_b_to_a", "reachability_a_to_b", "bidirectional_port_check_passed")},
        "root_advertised_ip": sel.get("nodeA_ip"),
        "connector_advertised_ip": sel.get("nodeB_ip"),
        "ldd_gate": {"passed": gate.get("passed"),
                     "expected_libstdcxx_dir": gate.get("expected_libstdcxx_dir"),
                     "ldd_both_use_expected_gcc_libstdcxx": gate.get(
                         "ldd_both_use_expected_gcc_libstdcxx")},
        "cpu_frequency_policy_recorded": bool(governors["cpu_governor_nodeA"]
                                              or governors["cpu_governor_nodeB"]),
        "turbo_or_dvfs_note": ("CPU governor recorded if accessible; system policy NOT changed; "
                               "unknown governor is a low-latency confound"),
        **governors,
        "connector_locality_idle_except_actions": True,
        "connector_background_work_note": "connector runs no workload besides serving dist_probe actions",
        "params": {"K": args.k, "W": args.w, "pipeline_depths": args.pipeline_depths,
                   "repetitions": args.repetitions, "x": args.x, "threads": args.threads,
                   "bind": args.bind, "agas_port": args.agas_port, "hpx_port": args.hpx_port,
                   "disable_idle_backoff": args.disable_idle_backoff,
                   "agas_preprobe_timeout_ms": args.agas_preprobe_timeout_ms,
                   "prefer_subnet": args.prefer_subnet},
        "statistics_policy": {
            "per_island_primary": True,
            "pooled_stats_allowed": False,
            "pooled_note": ("pooled K*R is supplementary ONLY; per-island percentiles + across-island "
                            "spread are the primary view so an anomalous island is not hidden"),
            "node_pair_stable_across_R": True,
        },
        "per_island_stats": per_island,
        "across_island_stats": across_island_summary(per_island),
        "pooled_stats_supplementary": island_stats(pooled_raw),
        "loopback_control_available": False,
        "loopback_control_run": False,
        "loopback_control_purpose": ("same-node two-locality TCP control to decompose parcel-stack vs "
                                     "network; reserved, not run yet"),
        "claim_fences": [
            "clean-path characterization only (exp59 = failure/restart)",
            "QD1 = serialized RTT floor at queue depth 1; NOT per-action cost; NOT pure network RTT",
            "QD1 may include scheduler idle-backoff wake latency unless disabled",
            "pipeline amortized time is NOT a latency; may include coalescing + parcel-pool scheduling",
            "no network/fabric performance claim; TCP parcelport only; closed-int64 only",
            "no Ray-vs-RayX claim; no single-run speedup claim; Rostam/allocation-specific only",
            "Ray-supervised Class-B is NOT a Ray/HPX speed comparison: Ray is only the control plane; "
            "steady-state should be Ray-independent, any delta is supervisor interference to investigate",
        ],
        "shared_dir": shared, "shared_dir_source": src,
        "islands": [{k: v for k, v in isl.items() if k != "perf_root_result"} for isl in islands],
    }
    if extra:
        agg.update(extra)
    return agg


# ---------------------------------------------------------------------------------------------------
# phase: rayfree-baseline
# ---------------------------------------------------------------------------------------------------
def phase_rayfree_baseline(binary, args):
    cfg = check_config(binary)
    if not cfg.get("tcp_parcelport_available"):
        return {"phase": "rayfree-baseline", "overall": "skip",
                "reason": "TCP parcelport not available in this HPX build", "cfg": cfg}
    sel = select_and_reachability(args)
    if sel is None:
        return {"phase": "rayfree-baseline", "overall": "skip",
                "reason": "no >=2-node SLURM allocation (need salloc -N2)", "cfg": cfg}
    if not sel.get("bidirectional_port_check_passed"):
        sel["overall"] = "fail"
        sel["reason"] = "bidirectional reachability failed; HPX not launched"
        return {"phase": "rayfree-baseline", "cfg": cfg, "selection": sel}

    child = _child_env()
    gate = ldd_gate(binary, sel["nodeA"], sel["nodeB"], child)
    if not gate.get("passed") and not args.skip_ldd_gate:
        return {"phase": "rayfree-baseline", "overall": "fail",
                "reason": "GCC-15 libstdc++ ldd gate failed on a compute node",
                "cfg": cfg, "selection": sel, "ldd_gate": gate}

    shared, src = _resolve_shared_dir(args)
    os.makedirs(shared, exist_ok=True)
    governors = {"cpu_governor_nodeA": cpu_governor(sel["nodeA"]),
                 "cpu_governor_nodeB": cpu_governor(sel["nodeB"])}

    islands, pooled_raw = [], []
    for r in range(args.repetitions):
        rec, raw_ns = run_island(binary, args, cfg, sel, shared, r)
        islands.append(rec)
        pooled_raw.extend(raw_ns)
    all_valid = all(isl["island_valid"] for isl in islands) and len(islands) > 0

    return _build_aggregate(
        phase="rayfree-baseline", baseline_kind="ray_free_hpx_two_node",
        fair_note=("Ray-free baseline: same spike/workload, NO Ray supervisor/control plane. The fair "
                   "internal comparison for the Ray-supervised run."),
        overall=("pass" if all_valid else "fail"), ray_imported=False, failure_restart_used=False,
        cfg=cfg, sel=sel, gate=gate, governors=governors, args=args, shared=shared, src=src,
        islands=islands, pooled_raw=pooled_raw)


# ---------------------------------------------------------------------------------------------------
# phase: ray-supervised (Ray = control plane only; SAME spike / workload / gates / schema)
# ---------------------------------------------------------------------------------------------------
def phase_ray_supervised(binary, args):
    cfg = check_config(binary)
    if not cfg.get("tcp_parcelport_available"):
        return {"phase": "ray-supervised", "overall": "skip", "ray_imported": False,
                "reason": "TCP parcelport not available in this HPX build", "cfg": cfg}
    sel = select_and_reachability(args)
    if sel is None:
        return {"phase": "ray-supervised", "overall": "skip", "ray_imported": False,
                "reason": "no >=2-node SLURM allocation (need salloc -N2)", "cfg": cfg}
    if not sel.get("bidirectional_port_check_passed"):
        sel["overall"] = "fail"
        sel["reason"] = "bidirectional reachability failed; HPX not launched"
        return {"phase": "ray-supervised", "overall": "fail", "ray_imported": False,
                "cfg": cfg, "selection": sel}

    # --- lazy Ray import (NEVER at module top) ---
    try:
        import ray
    except Exception as e:  # noqa: BLE001
        return {"phase": "ray-supervised", "overall": "skip", "ray_imported": False,
                "ray_import_ok": False,
                "reason": "ray import failed (Ray not available in this env): " + str(e)[:160],
                "cfg": cfg}
    ray_version = getattr(ray, "__version__", None)
    try:
        ray.init(ignore_reinit_error=True, log_to_driver=False, include_dashboard=False)
    except Exception as e:  # noqa: BLE001
        return {"phase": "ray-supervised", "overall": "fail", "ray_imported": True,
                "ray_import_ok": True, "ray_init_ok": False, "ray_version": ray_version,
                "reason": "ray.init failed: " + str(e)[:160], "cfg": cfg}

    # child env built AFTER ray.init so the pre-Ray overlay demonstrably defends any ray.init mutation
    child = _preserve_child_env()
    env_report = _env_preserve_report(child)

    gate = ldd_gate(binary, sel["nodeA"], sel["nodeB"], child)
    if not gate.get("passed") and not args.skip_ldd_gate:
        _ray_shutdown_quiet(ray)
        return {"phase": "ray-supervised", "overall": "fail", "ray_imported": True,
                "ray_init_ok": True, "ray_version": ray_version,
                "reason": "GCC-15 libstdc++ ldd gate failed on a compute node (Ray child env)",
                "cfg": cfg, "selection": sel, "ldd_gate": gate, "env_preserve_report": env_report}

    shared, src = _resolve_shared_dir(args)
    os.makedirs(shared, exist_ok=True)
    governors = {"cpu_governor_nodeA": cpu_governor(sel["nodeA"]),
                 "cpu_governor_nodeB": cpu_governor(sel["nodeB"])}

    islands, pooled_raw = [], []
    try:
        for r in range(args.repetitions):
            rec, raw_ns = run_island_ray(binary, args, cfg, sel, shared, r, ray, child)
            islands.append(rec)
            pooled_raw.extend(raw_ns)
    finally:
        _ray_shutdown_quiet(ray)

    all_valid = all(isl["island_valid"] for isl in islands) and len(islands) > 0
    launched_in_actor = all(bool(isl.get("launch", {}).get("launched_from_ray_actor"))
                            for isl in islands) and len(islands) > 0
    not_gated = all(isl.get("launch", {}).get("connector_gated_on_root_ready") is False
                    for isl in islands) and len(islands) > 0

    extra = {
        "ray_import_ok": True,
        "ray_init_ok": True,
        "ray_version": ray_version,
        "ray_supervisor_used": True,
        "ray_supervisor_shape": "one _SrunRunner Ray actor per role; back-to-back .remote() issue",
        "ray_in_action_data_path": False,
        "ray_object_store_used_for_action_results": False,
        "launched_from_ray_actor": launched_in_actor,
        "ray_mutation_defense_validated": launched_in_actor,
        "connector_gated_on_root_ready": (False if not_gated else None),
        "env_preserve_report": env_report,
        "class_b_ray_independence_note": ("Ray touches only bootstrap/control + process status; the "
                                          "HPX action/data path, cached remote id, idle-backoff control, "
                                          "and the entire Class-B timing loop are identical to the Ray-"
                                          "free spike. Steady-state Class-B should be Ray-independent; do "
                                          "NOT read any delta as a Ray-vs-HPX speed result."),
    }
    return _build_aggregate(
        phase="ray-supervised", baseline_kind="ray_supervised_hpx_two_node",
        fair_note=("Ray-supervised clean path: SAME spike/workload as the Ray-free baseline, with Ray "
                   "as the control plane (srun launched from Ray actors). Same node pair / subnet / "
                   "interface / ports / idle-backoff flag. To be diffed against the Ray-free baseline."),
        overall=("pass" if all_valid else "fail"), ray_imported=True, failure_restart_used=False,
        cfg=cfg, sel=sel, gate=gate, governors=governors, args=args, shared=shared, src=src,
        islands=islands, pooled_raw=pooled_raw, extra=extra)


def _ray_shutdown_quiet(ray):
    try:
        ray.shutdown()
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------------------------------
# artifact-write hardening: phase-specific top-level aggregates, atomic writes, and an overwrite guard
# so a skip/fail/local run can NEVER clobber a curated cluster pass. Per-run _perf_runs artifacts stay
# authoritative and are untouched by this layer.
# ---------------------------------------------------------------------------------------------------
# Phase -> curated PASS top-level basename. The generic "perf_aggregate.json" is DEPRECATED for writes.
_PHASE_PASS_BASENAME = {
    "rayfree-baseline": "perf_aggregate_rayfree.json",
    "ray-supervised": "perf_aggregate_ray_supervised.json",
}


def _phase_pass_path(phase):
    return os.path.join(HERE, _PHASE_PASS_BASENAME.get(phase, "perf_aggregate_other.json"))


def _run_id():
    return time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"


def _sibling(path, suffix):
    base, ext = os.path.splitext(path)
    return base + suffix + ext


def _atomic_write_json(path, payload):
    """Write JSON via temp-file + fsync + atomic rename in the SAME directory (no half-written file is
    ever visible at `path`)."""
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".agg_", suffix=".tmp", dir=d)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _read_overall(path):
    try:
        with open(path) as f:
            d = json.load(f)
        return d.get("overall"), d.get("phase")
    except (OSError, ValueError):
        return None, None


def safe_write_aggregate(intended_path, payload, *, allow_overwrite_pass=False, phase=None):
    """Overwrite-guarded, atomic aggregate write.

    Policy (never silently downgrade pass -> skip/fail):
      * destination missing                          -> write at intended_path.
      * new=pass, old missing/skip/fail              -> write at intended_path (upgrade allowed).
      * new=pass, old=pass:
          - same phase AND allow_overwrite_pass=True -> overwrite intended_path.
          - else                                     -> REFUSE; redirect to a run-id sibling so the
                                                        curated pass is preserved.
      * new=skip/fail                                -> ALWAYS redirect to a `_<overall>` sibling; the
                                                        curated pass path is never touched.
    Returns a small dict describing what happened; annotates the payload with the policy fields."""
    new_overall = payload.get("overall")
    old_overall, old_phase = (_read_overall(intended_path)
                              if os.path.exists(intended_path) else (None, None))
    redirected_from = None
    overwrite_refused = False
    target = intended_path

    if new_overall == "pass":
        if old_overall == "pass":
            same_phase = (phase is None or old_phase is None or phase == old_phase)
            if not (allow_overwrite_pass and same_phase):
                target = _sibling(intended_path, "_redirected_" + _run_id())
                redirected_from = intended_path
                overwrite_refused = True
        # else: missing / skip / fail destination -> safe to write the pass at intended_path
    else:
        # skip/fail must NEVER land on the curated pass path
        target = _sibling(intended_path, "_" + (new_overall or "unknown"))
        if old_overall == "pass":
            redirected_from = intended_path

    payload["artifact_write_policy"] = (
        "phase-specific top-level aggregates; skip/fail never overwrite a curated pass; pass-over-pass "
        "requires same phase + --allow-overwrite-pass; atomic temp+fsync+rename writes")
    payload["top_level_overwrite_guard_active"] = True
    payload["top_level_aggregate_path"] = os.path.basename(target)
    payload["redirected_from_path"] = (os.path.basename(redirected_from) if redirected_from else None)
    payload["overwrite_refused"] = overwrite_refused
    _atomic_write_json(target, payload)
    return {"written_path": target, "redirected_from": redirected_from,
            "overwrite_refused": overwrite_refused}


# ---------------------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="exp58 Ray-free two-node HPX clean-path perf baseline")
    ap.add_argument("--phase", choices=["check-config", "rayfree-baseline", "ray-supervised"],
                    default="rayfree-baseline")
    ap.add_argument("--binary", default=None, help="path to two_node_perf_spike (else build/ default)")
    ap.add_argument("--shared-dir", default=None, help="shared-FS rendezvous dir (default _perf_runs)")
    ap.add_argument("--prefer-subnet", default="10.42.5.",
                    help="IP prefix to pin the parcel interface/subnet (default eno16 10.42.5.)")
    ap.add_argument("--agas-port", type=int, default=7910)
    ap.add_argument("--hpx-port", type=int, default=7912)
    ap.add_argument("--k", type=int, default=1000, help="measured steady-state actions (QD1 floor)")
    ap.add_argument("--w", type=int, default=100, help="warmup actions dropped from stats")
    ap.add_argument("--pipeline-depths", default="8,32,128")
    ap.add_argument("--repetitions", type=int, default=1,
                    help="island repetitions R (Slice 1 default 1; full R=5 is Slice 4)")
    ap.add_argument("--x", type=int, default=7, help="closed-int64 action input")
    ap.add_argument("--threads", type=int, default=4, help="--hpx:threads per locality")
    ap.add_argument("--bind", default="none", help="--hpx:bind policy")
    ap.add_argument("--disable-idle-backoff", dest="disable_idle_backoff", action="store_true",
                    default=True, help="pass hpx.max_idle_backoff_time=0 (default ON for QD1 floor)")
    ap.add_argument("--keep-idle-backoff", dest="disable_idle_backoff", action="store_false",
                    help="do NOT disable idle backoff (QD1 then labeled possibly-inflated)")
    ap.add_argument("--skip-ldd-gate", action="store_true",
                    help="skip the GCC-15 libstdc++ ldd gate (NOT recommended)")
    ap.add_argument("--ready-timeout", type=int, default=60)
    ap.add_argument("--leave-timeout", type=int, default=60)
    ap.add_argument("--serve-timeout", type=int, default=120)
    ap.add_argument("--agas-preprobe-timeout-ms", type=int, default=60000,
                    help="connector AGAS TCP pre-probe bound (ms); used as the readiness mechanism in "
                         "the Ray-supervised near-concurrent launch")
    ap.add_argument("--run-budget", type=int, default=120,
                    help="extra seconds beyond ready+leave for the measurement loop")
    ap.add_argument("--out", default=None,
                    help="explicit PASS aggregate path (overwrite-guarded; default is the phase-specific "
                         "perf_aggregate_<phase>.json). skip/fail/local writes are redirected away from it")
    ap.add_argument("--allow-overwrite-pass", action="store_true",
                    help="permit overwriting an EXISTING pass aggregate when the new result is also a "
                         "pass of the SAME phase (default: refuse and redirect to a run-id sibling)")
    args = ap.parse_args()

    binary = locate_binary(args.binary)
    if not binary:
        # LOCAL / unbuilt -> stdout ONLY. Never writes a top-level aggregate, so it cannot clobber a
        # curated cluster pass.
        print(json.dumps({"overall": "skip", "phase": args.phase,
                          "reason": f"binary '{BINARY_BASENAME}' not built; configure+build via "
                                    "CMakeLists.txt on an HPX-capable host (e.g. Rostam)",
                          "note": "local/unbuilt skip: stdout only, no aggregate file written",
                          "candidates": DEFAULT_BINARY_CANDIDATES}, indent=2))
        return 0

    if args.phase == "check-config":
        print(json.dumps(check_config(binary), indent=2))
        return 0

    if args.phase == "ray-supervised":
        agg = phase_ray_supervised(binary, args)
    else:
        agg = phase_rayfree_baseline(binary, args)

    # intended PASS path is phase-specific; the guard redirects skip/fail (and pass-over-pass) away
    # from it so a curated cluster pass is never clobbered by a local/skip/fail run.
    intended = args.out or _phase_pass_path(agg.get("phase", args.phase))
    res = safe_write_aggregate(intended, agg, allow_overwrite_pass=args.allow_overwrite_pass,
                               phase=agg.get("phase", args.phase))
    print(json.dumps({k: agg.get(k) for k in ("phase", "baseline_kind", "overall", "reason",
                                              "ray_imported", "ray_version", "failure_restart_used",
                                              "hpx_version", "hpx_version_parsed",
                                              "top_level_aggregate_path", "redirected_from_path",
                                              "overwrite_refused")}, indent=2))
    if res["redirected_from"]:
        print(f"[exp58] guard: '{os.path.basename(res['redirected_from'])}' is a curated/existing "
              f"result; wrote to '{os.path.basename(res['written_path'])}' instead")
    print(f"[exp58] wrote {res['written_path']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
