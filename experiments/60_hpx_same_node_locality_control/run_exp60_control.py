#!/usr/bin/env python3
"""exp60 -- HPX SAME-NODE two-locality TCP control (within-HPX decomposition pre-Slice-5).

WHAT THIS IS
------------
A small HPX-side decomposition control that is the symmetric analog of exp59 Slice 1's
Ray same-host control. It launches TWO HPX localities on ONE Slurm node (default medusa00),
root + connector, over the TCP parcelport advertising the SAME node IP, so the physical
network leg collapses to KERNEL LOOPBACK while the full HPX parcel/serialization/scheduler
stack and the caller-observed `hpx::async(...).get()` RTT are byte-identical to exp58.

It REUSES the exp58 measurement binary (two_node_perf_spike) UNMODIFIED, by explicit path.
exp60 builds nothing and copies no C++. The measurement core is therefore identical to exp58
by construction; only the launch topology differs (both localities co-located on one node,
distinct ports, DISJOINT core bindings).

DECOMPOSITION IT ENABLES (within HPX only)
------------------------------------------
  L1 = exp60 same-node two-locality TCP (this experiment): local HPX stack + parcel + loopback
  L2 = exp58 inter-node two-locality TCP over eno16: L1 + physical wire
  => (L2 - L1) approximates the inter-node network leg, caller-observed, TCP, warm.

CLAIM FENCES (also embedded in every aggregate)
-----------------------------------------------
  * WITHIN-HPX decomposition only. NO Ray-vs-HPX comparison. NO speedup. NO ratio.
  * NO production / failure / restart / multi-node / fabric claim.
  * caller-observed C++ hpx::async(...).get() RTT; closed-int64; TCP parcelport;
    idle-backoff disabled; warm-path (1 prewarm + W warmups dropped).
  * tcp_nodelay NOT verified -> Nagle/delayed-ACK confound HELD CONSTANT vs exp58.
  * network leg = kernel loopback (same advertised eno16 IP on one node); loopback != zero cost.
  * Rostam-allocation-specific; parity gated to medusa00 / 10.42.5.

Phases:
  --phase check-config                    TCP-parcelport availability + HPX version (binary preflight)
  --phase same-node-two-locality-control  the L1 control measurement (R reps, per-island-primary band)
  --phase selftest                        pure-Python helper checks (no Slurm, no binary)

Reuses exp58 binary by default at: ../58_two_node_clean_path_perf/build/two_node_perf_spike
"""

import argparse
import hashlib
import json
import math
import os
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
EXP58_DIR = os.path.normpath(os.path.join(HERE, "..", "58_two_node_clean_path_perf"))
BINARY_BASENAME = "two_node_perf_spike"
DEFAULT_BINARY_CANDIDATES = [
    os.path.join(EXP58_DIR, "build", BINARY_BASENAME),
    os.path.join(EXP58_DIR, "build", "Release", BINARY_BASENAME),
]
EXP58_SOURCE_CANDIDATE = os.path.join(EXP58_DIR, "two_node_perf_spike.cpp")

TOP_AGGREGATE = "hpx_same_node_control_aggregate.json"
CONTROL_RUNS = os.path.join(HERE, "_control_runs")
CONTROL_INDEX = os.path.join(CONTROL_RUNS, "control_index.jsonl")

TCP_ENABLE_FLAGS = [
    "--hpx:ini=hpx.parcel.bootstrap=tcp",
    "--hpx:ini=hpx.parcel.tcp.enable=1",
]
IDLE_BACKOFF_DISABLE_FLAG = "--hpx:ini=hpx.max_idle_backoff_time=0"

CLAIM_FENCES = [
    "HPX same-node two-locality TCP control: WITHIN-HPX decomposition only "
    "(local stack + parcel + kernel loopback; the L1 rung below exp58 inter-node L2).",
    "NO Ray-vs-HPX comparison; NO speedup; NO ratio; NO HPX-beats-Ray claim.",
    "NO production / failure / restart / multi-node / general-fabric claim.",
    "caller-observed C++ hpx::async(...).get() RTT; closed-int64; TCP parcelport; "
    "idle-backoff disabled; warm-path (1 prewarm + W warmups dropped).",
    "tcp_nodelay NOT verified unless actually verified -> Nagle/delayed-ACK confound "
    "held constant vs exp58, not eliminated.",
    "network leg = kernel loopback (same advertised eno16 IP on one node); loopback != zero cost.",
    "reuses the exp58 binary UNMODIFIED by explicit path; measurement core byte-identical to exp58.",
    "Rostam medusa-allocation-specific; parity gated to medusa00 / 10.42.5.",
]


# ---------------------------------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------------------------------
def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def pick_two_distinct_ports():
    """Two distinct free TCP ports: port_root (root agas+hpx) and port_conn (connector hpx)."""
    a = find_free_port()
    b = find_free_port()
    tries = 0
    while b == a and tries < 50:
        b = find_free_port()
        tries += 1
    if b == a:
        raise RuntimeError("could not obtain two distinct free ports")
    return a, b


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


def read_json_eventually(path, timeout_s=20.0, poll_s=0.25, required=False):
    """Shared-FS (NFS) tolerant read: revalidate the parent dir between attempts so a freshly
    written marker is not hidden by negative-dentry/attribute caching. Returns (obj_or_None, diag)."""
    diag = {"path": os.path.basename(path), "attempts": 0, "first_seen_ms": None,
            "parsed_ms": None, "last_error": "missing", "required": bool(required), "ok": False}
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
            if diag["first_seen_ms"] is None:
                diag["first_seen_ms"] = int((time.monotonic() - t0) * 1000)
            try:
                with open(path) as f:
                    obj = json.loads(f.read())
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


def wait_marker(path, timeout_s, poll_s=0.05, proc=None):
    """Revalidating wait for a launch handshake marker (root.ready). Bails early if the watched
    process has already exited. Returns True as soon as the marker is visible."""
    parent = os.path.dirname(path) or "."
    deadline = time.monotonic() + max(0.0, timeout_s)
    while True:
        try:
            os.listdir(parent)
        except OSError:
            pass
        if os.path.exists(path):
            return True
        if proc is not None and proc.poll() is not None and not os.path.exists(path):
            # give the FS one last revalidation grace before declaring the process gone
            time.sleep(poll_s)
            try:
                os.listdir(parent)
            except OSError:
                pass
            return os.path.exists(path)
        if time.monotonic() >= deadline:
            return os.path.exists(path)
        time.sleep(poll_s)


def _child_env():
    env = dict(os.environ)
    env.setdefault("SLURM_EXPORT_ENV", "ALL")
    return env


def sha256_file(path):
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def locate_binary(explicit):
    if explicit:
        return os.path.abspath(explicit) if os.path.exists(explicit) else None
    for c in DEFAULT_BINARY_CANDIDATES:
        if os.path.exists(c):
            return os.path.abspath(c)
    return None


def binary_provenance(binary):
    """Record exp58 binary path/hash + best-effort exp58 source provenance (NO copy, NO build)."""
    prov = {
        "binary_path": binary,
        "binary_present": bool(binary and os.path.exists(binary)),
        "binary_sha256": sha256_file(binary) if binary else None,
        "binary_size_bytes": (os.path.getsize(binary) if binary and os.path.exists(binary) else None),
        "binary_mtime": (os.path.getmtime(binary) if binary and os.path.exists(binary) else None),
        "reused_from": "exp58 (experiments/58_two_node_clean_path_perf) -- UNMODIFIED, by explicit path",
        "exp58_source_path": (EXP58_SOURCE_CANDIDATE if os.path.exists(EXP58_SOURCE_CANDIDATE)
                              else None),
        "exp58_source_sha256": (sha256_file(EXP58_SOURCE_CANDIDATE)
                                if os.path.exists(EXP58_SOURCE_CANDIDATE) else None),
        "exp60_builds_nothing": True,
        "exp60_copies_no_cpp": True,
    }
    return prov


# ---------------------------------------------------------------------------------------------------
# core-split / binding (DISJOINTNESS is the #1 validity gate: oversubscription would inflate L1)
# ---------------------------------------------------------------------------------------------------
def _parse_cpu_list(spec):
    """Parse '0-3,8,10-11' into a sorted set of ints. Raises ValueError on malformed input."""
    out = set()
    for tok in str(spec).split(","):
        tok = tok.strip()
        if not tok:
            continue
        if "-" in tok:
            a, b = tok.split("-", 1)
            a, b = int(a), int(b)
            if b < a:
                raise ValueError(f"bad cpu range {tok!r}")
            out.update(range(a, b + 1))
        else:
            out.add(int(tok))
    return out


def _cpus_to_mask(cpus):
    """Render a sorted set of cpu ids as a compact taskset -c list, e.g. {0,1,2,3} -> '0-3'."""
    s = sorted(cpus)
    if not s:
        return ""
    parts = []
    start = prev = s[0]
    for c in s[1:]:
        if c == prev + 1:
            prev = c
            continue
        parts.append(f"{start}-{prev}" if start != prev else f"{start}")
        start = prev = c
    parts.append(f"{start}-{prev}" if start != prev else f"{start}")
    return ",".join(parts)


def compute_core_split(ncpu, threads, spec="auto"):
    """Return (root_cpus:set, conn_cpus:set). 'auto' => root=[0,T), conn=[T,2T). Explicit
    'a-b:c-d' => parse each side. Raises ValueError if insufficient cores or not disjoint."""
    if spec and spec != "auto":
        if ":" not in spec:
            raise ValueError("explicit --core-split must be 'rootlist:connlist'")
        rs, cs = spec.split(":", 1)
        root_cpus, conn_cpus = _parse_cpu_list(rs), _parse_cpu_list(cs)
    else:
        if threads < 1:
            raise ValueError("threads must be >= 1")
        if 2 * threads > ncpu:
            raise ValueError(f"insufficient cores: need 2*threads={2 * threads}, have ncpu={ncpu}")
        root_cpus = set(range(0, threads))
        conn_cpus = set(range(threads, 2 * threads))
    if not root_cpus or not conn_cpus:
        raise ValueError("empty core set")
    if root_cpus & conn_cpus:
        raise ValueError(f"core sets overlap: {sorted(root_cpus & conn_cpus)}")
    if max(root_cpus | conn_cpus) >= ncpu:
        raise ValueError(f"core id exceeds ncpu={ncpu}")
    return root_cpus, conn_cpus


def bindings_disjoint(a, b):
    return bool(a) and bool(b) and not (set(a) & set(b))


def node_ncpu(node):
    """Best-effort online-cpu count on `node` via srun nproc; fallback to SLURM_CPUS_ON_NODE."""
    o = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "--overlap", "nproc"], timeout=30)
    if o and o.returncode == 0:
        try:
            return int((o.stdout or "").strip())
        except ValueError:
            pass
    try:
        return int(os.environ.get("SLURM_CPUS_ON_NODE", "0"))
    except ValueError:
        return 0


# ---------------------------------------------------------------------------------------------------
# node IP selection (single node) + TCP-parcelport preflight
# ---------------------------------------------------------------------------------------------------
def select_node_ip(node, prefer_subnet):
    out = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "--overlap", "bash", "-c",
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


def check_config(binary):
    """TCP parcelport availability + HPX version, derived from the binary's own --hpx:dump-config /
    --hpx:version. Re-implemented compactly; NO HPX runtime topology is mutated."""
    import re
    p = find_free_port()
    bd = tempfile.mkdtemp(prefix="exp60_cfg_")
    out = _run([binary, "--role", "root", "--bootstrap", bd, "--ready-timeout", "1",
                "--leave-timeout", "1", f"--hpx:agas=127.0.0.1:{p}", f"--hpx:hpx=127.0.0.1:{p}",
                "--hpx:ignore-batch-env", "--hpx:dump-config"], timeout=30)
    dump = ((out.stdout or "") + (out.stderr or "")) if out else ""
    ver = _run([binary, "--hpx:version"], timeout=20)
    ver_text = ((ver.stdout or "") + (ver.stderr or "")) if ver else ""
    m = re.search(r"HPX:?\s*V?(\d+\.\d+\.\d+)", ver_text)
    version = m.group(1) if m else None
    bootstrap = None
    mb = re.search(r"'bootstrap'\s*:\s*'([^']+)'", dump)
    if mb:
        bootstrap = mb.group(1)
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
    }


def tcp_pin_flags(cfg):
    flags = list(TCP_ENABLE_FLAGS)
    for pp, present in (cfg or {}).get("parcelports_present", {}).items():
        if pp != "tcp" and present:
            flags.append(f"--hpx:ini=hpx.parcel.{pp}.enable=0")
    return flags


# ---------------------------------------------------------------------------------------------------
# per-island / across-island statistics (IDENTICAL policy to exp58: nearest-rank + median-of-medians)
# ---------------------------------------------------------------------------------------------------
def _pct(sorted_vals, p):
    if not sorted_vals:
        return None
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
    """Median + min/max spread of the PER-ISLAND summaries (per-island is PRIMARY; an anomalous
    island is visible rather than hidden in a pooled p99)."""
    def med(vals):
        v = sorted(x for x in vals if x is not None)
        if not v:
            return None
        n = len(v)
        return v[n // 2] if n % 2 else int((v[n // 2 - 1] + v[n // 2]) / 2)
    out = {"islands_in_stats": sum(1 for isl in per_island if isl.get("count")),
           "per_island_primary": True, "pooled_distribution_used": False}
    for key in ("p50_ns", "p90_ns", "p99_ns", "mean_ns", "min_ns", "max_ns"):
        vals = [isl.get(key) for isl in per_island if isl.get("count")]
        clean = [x for x in vals if x is not None]
        out[key + "_median"] = med(clean)
        out[key + "_min"] = (min(clean) if clean else None)
        out[key + "_max"] = (max(clean) if clean else None)
    return out


# ---------------------------------------------------------------------------------------------------
# one same-node island: build argv, launch co-located root+connector, collect + gate
# ---------------------------------------------------------------------------------------------------
def _build_same_node_argv(binary, args, cfg, node, node_ip, port_root, port_conn,
                          root_mask, conn_mask, bootdir):
    """Both localities on ONE node, same advertised IP (kernel loopback), distinct ports, disjoint
    taskset core masks. Measurement flags (K/W/x/pipeline/idle) are IDENTICAL to exp58."""
    pin = tcp_pin_flags(cfg)
    idle_flag = [IDLE_BACKOFF_DISABLE_FLAG] if args.disable_idle_backoff else []

    def hpx_flags(role, agas_ip, agas_port, hpx_ip, hpx_port, extra=()):
        argv = [
            f"--hpx:agas={agas_ip}:{agas_port}",
            f"--hpx:hpx={hpx_ip}:{hpx_port}",
            f"--hpx:threads={args.threads}",
            "--hpx:ignore-batch-env",
        ]
        argv += list(pin)
        if role == "root":
            argv.append("--hpx:expect-connecting-localities")
        argv += list(extra)
        return argv

    srun_pre = ["srun", "-N1", "-n1", "--nodelist=" + node, "--overlap", "--export=ALL"]
    root_argv = srun_pre + ["taskset", "-c", root_mask, binary,
                            "--role", "root", "--bootstrap", bootdir, "--x", str(args.x),
                            "--k", str(args.k), "--w", str(args.w),
                            "--pipeline-depths", args.pipeline_depths,
                            "--ready-timeout", str(args.ready_timeout),
                            "--leave-timeout", str(args.leave_timeout)]
    root_argv += hpx_flags("root", node_ip, port_root, node_ip, port_root, idle_flag)

    conn_extra = idle_flag + ["--agas-preprobe-host", node_ip,
                              "--agas-preprobe-port", str(port_root),
                              "--agas-preprobe-timeout-ms", str(args.agas_preprobe_timeout_ms)]
    conn_argv = srun_pre + ["taskset", "-c", conn_mask, binary,
                            "--role", "connect", "--bootstrap", bootdir,
                            "--serve-timeout", str(args.serve_timeout)]
    conn_argv += hpx_flags("connect", node_ip, port_root, node_ip, port_conn, conn_extra)

    return {
        "root_argv": root_argv, "conn_argv": conn_argv,
        "intended_root_ep": f"{node_ip}:{port_root}",
        "intended_conn_ep": f"{node_ip}:{port_conn}",
    }


def _adv_ip_port(ep):
    if not ep or ":" not in ep:
        return None, None
    ip, _, port = ep.rpartition(":")
    return ip or None, (port or None)


# ---------------------------------------------------------------------------------------------------
# orphan check + cleanup (UNIQUE per-island match only; never a global binary-name kill)
# ---------------------------------------------------------------------------------------------------
def _orphan_check_node(node, pattern):
    """Diagnostic pgrep on `node` for processes whose cmdline UNIQUELY contains `pattern` -- this
    island's per-run bootstrap basename (e.g. 'exp60_ctl_r0_<rand>'), which is present only in the
    `--bootstrap <dir>` cmdline of the root/connector spike processes THIS runner launched for THIS
    island. pgrep exits 1 when there are NO matches -> the clean case, not an error.
    Returns (no_orphans_bool_or_None, pids_list); None means the check itself could not run."""
    o = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "--overlap", "pgrep", "-f", pattern],
             timeout=20)
    if o is None or o.returncode not in (0, 1):
        return None, []
    pids = [p for p in (o.stdout or "").split() if p] if o.returncode == 0 else []
    return (len(pids) == 0), pids


def _cleanup_node(node, pattern):
    """Targeted, best-effort SIGTERM of ONLY processes whose cmdline UNIQUELY contains `pattern`
    (this island's bootstrap basename) -- i.e. exactly the root/connector spike processes THIS runner
    launched via srun for THIS island. This is NEVER a global binary-name kill and never matches
    unrelated user processes. Returns True if a cleanup command was issued."""
    o = _run(["srun", "-N1", "-n1", "--nodelist=" + node, "--overlap", "pkill", "-TERM", "-f",
              pattern], timeout=20)
    return o is not None


def _cleanup_and_check(node, pattern):
    """Run in the per-island finally: diagnose orphans uniquely matching this island, targeted-kill
    only those, then re-verify. Records what was found/killed; never raises; never touches unrelated
    processes. `no_orphan_hpx_processes` is True/False after cleanup, or None if the check could not
    run (which must NOT be read as a confirmed-clean result)."""
    info = {"cleanup_checked_node": node, "orphan_pattern": pattern,
            "orphan_pattern_note": "unique per-island bootstrap basename present only in this "
                                   "runner's --bootstrap cmdline; not a global binary-name match",
            "cleanup_attempted": False, "orphan_pids_before_cleanup": [],
            "orphan_hpx_processes": None, "no_orphan_hpx_processes": None}
    try:
        _, before_pids = _orphan_check_node(node, pattern)
        info["orphan_pids_before_cleanup"] = before_pids
        if before_pids:
            info["cleanup_attempted"] = _cleanup_node(node, pattern)
            time.sleep(0.5)
        after_clean, after_pids = _orphan_check_node(node, pattern)
        info["orphan_hpx_processes"] = after_pids
        info["no_orphan_hpx_processes"] = after_clean
    except Exception:  # noqa: BLE001
        pass
    return info


def run_same_node_island(binary, args, cfg, node, node_ip, root_cpus, conn_cpus, rep_index):
    bootdir = tempfile.mkdtemp(prefix=f"exp60_ctl_r{rep_index}_", dir=CONTROL_RUNS)
    orphan_pattern = os.path.basename(bootdir)   # unique per-island; appears only in --bootstrap arg
    port_root, port_conn = pick_two_distinct_ports()
    root_mask, conn_mask = _cpus_to_mask(root_cpus), _cpus_to_mask(conn_cpus)
    argvs = _build_same_node_argv(binary, args, cfg, node, node_ip, port_root, port_conn,
                                  root_mask, conn_mask, bootdir)
    child = _child_env()

    island = None
    raw_ns = []
    cleanup = {"cleanup_checked_node": node, "orphan_pattern": orphan_pattern,
               "cleanup_attempted": False, "orphan_pids_before_cleanup": [],
               "orphan_hpx_processes": None, "no_orphan_hpx_processes": None}
    try:
        r_out = open(os.path.join(bootdir, "root.stdout"), "w")
        r_err = open(os.path.join(bootdir, "root.stderr"), "w")
        root = subprocess.Popen(argvs["root_argv"], stdout=r_out, stderr=r_err, env=child)
        root_ready = wait_marker(os.path.join(bootdir, "root.ready"), args.ready_timeout, 0.05,
                                 proc=root)

        conn = c_out = c_err = None
        if root_ready:
            c_out = open(os.path.join(bootdir, "connector.stdout"), "w")
            c_err = open(os.path.join(bootdir, "connector.stderr"), "w")
            conn = subprocess.Popen(argvs["conn_argv"], stdout=c_out, stderr=c_err, env=child)

        deadline = time.time() + args.ready_timeout + args.leave_timeout + args.run_budget
        while time.time() < deadline and root.poll() is None:
            time.sleep(0.1)
        root_rc = root.poll()
        if root_rc is None:
            root.kill()
            root_rc = root.wait()
        conn_rc = None
        if conn is not None:
            try:
                conn.wait(timeout=20)
            except Exception:  # noqa: BLE001
                conn.kill()
            conn_rc = conn.poll()
        for fh in (r_out, r_err, c_out, c_err):
            if fh is not None:
                try:
                    fh.close()
                except OSError:
                    pass

        perf, perf_diag = read_json_eventually(os.path.join(bootdir, "perf_root_result.json"),
                                               timeout_s=20.0, required=True)
        perf = perf or {}
        a_root, _ = read_json_eventually(os.path.join(bootdir, "attest_root.json"), timeout_s=10.0)
        a_conn, _ = read_json_eventually(os.path.join(bootdir, "attest_connect.json"), timeout_s=10.0)
        a_root, a_conn = a_root or {}, a_conn or {}

        d1 = perf.get("remote_action_rtt_floor_depth1", {}) or {}
        raw_ns = d1.get("per_action_duration_ns_raw", []) or []
        sched = perf.get("scheduler_tuning", {}) or {}

        root_host = a_root.get("hostname")
        conn_host = a_conn.get("hostname")
        root_adv = a_root.get("advertised_hpx_endpoint")
        conn_adv = a_conn.get("advertised_hpx_endpoint")
        root_adv_ip, _ = _adv_ip_port(root_adv)
        conn_adv_ip, _ = _adv_ip_port(conn_adv)
        short = lambda h: (h.split(".")[0] if h else None)

        gates = {
            "perf_valid": bool(perf.get("perf_valid")),
            "root_ready": bool(root_ready),
            "root_rc_zero": (root_rc == 0),
            "two_localities": bool(perf.get("remote_locality_id_differs")),
            "proved_remote_by_oracle": bool(perf.get("proved_remote_by_oracle")),
            "depth1_all_correct": bool(d1.get("K") and d1.get("correct_count") == d1.get("K")),
            "same_physical_node": bool(root_host and conn_host
                                       and short(root_host) == short(conn_host) == args.node),
            "loopback_same_advertised_ip": bool(root_adv_ip and conn_adv_ip
                                                and root_adv_ip == conn_adv_ip == node_ip),
            "distinct_ports": (port_root != port_conn) and (root_adv != conn_adv),
            "bindings_disjoint": bindings_disjoint(root_cpus, conn_cpus),
            "idle_backoff_disabled": (sched.get("idle_backoff_mode") == "disabled"),
            "k_w_as_expected": (d1.get("K") == args.k and d1.get("W") == args.w),
        }
        island_valid = all(gates.values())

        island = {
            "rep_index": rep_index,
            "bootstrap_dir": bootdir,
            "node": node, "node_ip": node_ip,
            "network_leg": "kernel_loopback",
            "colocated_localities": True,
            "port_root": port_root, "port_conn": port_conn,
            "core_binding_root": root_mask, "core_binding_conn": conn_mask,
            "core_binding_root_set": sorted(root_cpus), "core_binding_conn_set": sorted(conn_cpus),
            "bindings_disjoint": bindings_disjoint(root_cpus, conn_cpus),
            "root_rc": root_rc, "connector_rc": conn_rc, "root_ready": bool(root_ready),
            "root_hostname": root_host, "connector_hostname": conn_host,
            "advertised_root_endpoint": root_adv, "advertised_connector_endpoint": conn_adv,
            "intended_root_endpoint": argvs["intended_root_ep"],
            "intended_connector_endpoint": argvs["intended_conn_ep"],
            "here_locality": perf.get("here_locality"),
            "remote_locality": perf.get("remote_locality"),
            "clock_type": perf.get("clock_type"),
            "timestamp_overhead_ns": perf.get("timestamp_overhead_ns"),
            "prewarm_action_duration_ns": perf.get("prewarm_action_duration_ns"),
            "first_action_duration_ns": perf.get("first_action_duration_ns"),
            "idle_backoff_mode": sched.get("idle_backoff_mode"),
            "tcp_nodelay_verified": (perf.get("parcelport_config", {}) or {}).get(
                "tcp_nodelay_verified", False),
            "hpx_threads": perf.get("hpx_threads"),
            "K": d1.get("K"), "W": d1.get("W"),
            "perf_marker_diag": perf_diag,
            "gates": gates,
            "island_valid": island_valid,
            "island_stats_from_raw": island_stats(raw_ns),
            "root_argv": " ".join(argvs["root_argv"]),
            "connector_argv": " ".join(argvs["conn_argv"]),
            # full spike artifact kept for provenance; pipeline present but EXCLUDED from comparison
            "perf_root_result": perf,
            "pipeline_present_but_excluded_from_comparison": True,
        }
    finally:
        # ALWAYS run targeted orphan cleanup/diagnostics for THIS island (unique bootstrap match).
        cleanup = _cleanup_and_check(node, orphan_pattern)

    if island is None:
        # launch/collect raised before the island was built: still record a failed island + cleanup.
        island = {
            "rep_index": rep_index, "bootstrap_dir": bootdir, "node": node, "node_ip": node_ip,
            "network_leg": "kernel_loopback", "colocated_localities": True,
            "port_root": port_root, "port_conn": port_conn,
            "core_binding_root": root_mask, "core_binding_conn": conn_mask,
            "core_binding_root_set": sorted(root_cpus), "core_binding_conn_set": sorted(conn_cpus),
            "bindings_disjoint": bindings_disjoint(root_cpus, conn_cpus),
            "gates": {"perf_valid": False}, "island_valid": False,
            "island_stats_from_raw": {"count": 0},
            "pipeline_present_but_excluded_from_comparison": True,
        }
        raw_ns = []

    # fold the orphan/cleanup result into the island record and the validity gate. A confirmed orphan
    # (no_orphan_hpx_processes is False) fails the island; an unrunnable check (None) does NOT.
    island.update(cleanup)
    no_orphans_gate = (cleanup["no_orphan_hpx_processes"] is not False)
    island["gates"]["no_orphans"] = no_orphans_gate
    island["island_valid"] = bool(island.get("island_valid")) and no_orphans_gate

    with open(os.path.join(bootdir, "run_aggregate.json"), "w") as f:
        json.dump(island, f, indent=2)
    return island, raw_ns


# ---------------------------------------------------------------------------------------------------
# top-level aggregate writer (atomic; pass-over-pass guarded; skip/fail never clobber a pass)
# ---------------------------------------------------------------------------------------------------
def _atomic_write(path, payload):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def safe_write_top_aggregate(path, payload, allow_overwrite_pass=False):
    """Returns (written_path, overwrite_refused, redirected_from). A curated PASS is never silently
    clobbered: skip/fail are redirected to a sibling; pass-over-pass needs same phase + the flag."""
    overall = payload.get("overall")
    existing = _read_json(path) if os.path.exists(path) else None
    refused = False
    redirected_from = None
    if existing is not None and existing.get("overall") == "pass":
        same_phase = existing.get("phase") == payload.get("phase")
        if overall != "pass":
            redirected_from = path
            base, ext = os.path.splitext(path)
            tag = "skip" if overall == "skip" else "fail"
            path = f"{base}_{tag}{ext}"
            refused = True
        elif not (same_phase and allow_overwrite_pass):
            redirected_from = path
            base, ext = os.path.splitext(path)
            stamp = time.strftime("%Y%m%dT%H%M%S")
            path = f"{base}_redirected_{stamp}_{os.getpid()}{ext}"
            refused = True
    payload = dict(payload)
    payload["top_level_aggregate_path"] = os.path.basename(path)
    payload["overwrite_refused"] = refused
    payload["redirected_from_path"] = (os.path.basename(redirected_from) if redirected_from else None)
    _atomic_write(path, payload)
    return path, refused, redirected_from


def _append_index(row):
    os.makedirs(CONTROL_RUNS, exist_ok=True)
    with open(CONTROL_INDEX, "a") as f:
        f.write(json.dumps(row) + "\n")


# ---------------------------------------------------------------------------------------------------
# phase: same-node-two-locality-control
# ---------------------------------------------------------------------------------------------------
def phase_same_node_control(binary, args):
    prov = binary_provenance(binary)
    cfg = check_config(binary)
    os.makedirs(CONTROL_RUNS, exist_ok=True)
    run_id = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"

    node = args.node
    iface, node_ip = select_node_ip(node, args.prefer_subnet)
    ncpu = node_ncpu(node)
    try:
        root_cpus, conn_cpus = compute_core_split(ncpu, args.threads, args.core_split)
        split_error = None
    except ValueError as e:
        root_cpus, conn_cpus, split_error = set(), set(), str(e)

    base = {
        "phase": "same-node-two-locality-control",
        "baseline_kind": "hpx_same_node_two_locality_tcp_control",
        "rung": "L1",
        "decomposition_partner": "exp58 inter-node two-locality TCP (L2)",
        "run_id": run_id,
        "binary_provenance": prov,
        "hpx_version": cfg.get("hpx_version"),
        "tcp_parcelport_available": cfg.get("tcp_parcelport_available"),
        "parcelports_present": cfg.get("parcelports_present"),
        "node": node, "node_ip": node_ip, "selected_interface": iface,
        "selected_subnet": args.prefer_subnet,
        "cleanup_checked_node": node,
        "orphan_match_policy": ("per-island unique bootstrap basename only; targeted SIGTERM never "
                                "uses a global binary-name kill and never touches unrelated processes"),
        "network_leg": "kernel_loopback",
        "node_ncpu": ncpu, "threads_per_locality": args.threads,
        "core_split_spec": args.core_split,
        "core_binding_root_set": sorted(root_cpus), "core_binding_conn_set": sorted(conn_cpus),
        "bindings_disjoint": bindings_disjoint(root_cpus, conn_cpus),
        "core_split_error": split_error,
        "idle_backoff_disabled": bool(args.disable_idle_backoff),
        "tcp_nodelay_verified": False,
        "r_count": args.reps,
        "per_island_primary": True, "pooled_distribution_used": False,
        "pipeline_present_but_excluded_from_comparison": True,
        "measurement_point": "caller_observed_cpp_async_get_rtt_on_root_locality",
        "is_within_hpx_decomposition": True,
        "is_ray_vs_hpx_comparison": False,
        "perf_claim_allowed": False,
        "claim_fences": CLAIM_FENCES,
        "artifact_write_policy": ("phase-specific top-level aggregate; skip/fail never overwrite a "
                                  "curated pass; pass-over-pass requires same phase + "
                                  "--allow-overwrite-pass; atomic temp+fsync+rename writes"),
    }

    # hard preflight refusals: bail before launching anything
    if not prov["binary_present"]:
        base.update({"overall": "fail", "reason": "exp58 binary missing -- REFUSED",
                     "binary_expected_candidates": DEFAULT_BINARY_CANDIDATES})
        return base, "fail"
    if not cfg.get("tcp_parcelport_available"):
        base.update({"overall": "fail", "reason": "TCP parcelport not available in binary"})
        return base, "fail"
    if not node_ip:
        base.update({"overall": "fail",
                     "reason": f"could not select a routable IP on {node} for subnet "
                               f"{args.prefer_subnet!r}"})
        return base, "fail"
    if split_error or not bindings_disjoint(root_cpus, conn_cpus):
        base.update({"overall": "fail",
                     "reason": "core split invalid / not disjoint: " + (split_error or "overlap")})
        return base, "fail"

    islands, pooled_raw = [], []
    for r in range(args.reps):
        island, raw = run_same_node_island(binary, args, cfg, node, node_ip,
                                           root_cpus, conn_cpus, r)
        islands.append(island)
        if island["island_valid"]:
            pooled_raw.extend(raw)
        _append_index({
            "phase": "same-node-two-locality-control", "run_id": run_id, "rep_index": r,
            "node": node, "node_ip": node_ip, "network_leg": "kernel_loopback",
            "island_valid": island["island_valid"], "bindings_disjoint": island["bindings_disjoint"],
            "depth1": island["island_stats_from_raw"], "ts": time.time(),
        })

    per_island = [isl["island_stats_from_raw"] for isl in islands if isl["island_valid"]]
    islands_valid = sum(1 for isl in islands if isl["island_valid"])
    all_valid = (islands_valid == args.reps and args.reps > 0)

    gates = {
        "binary_present": True,
        "tcp_parcelport_available": True,
        "node_ip_on_selected_subnet": bool(node_ip and (not args.prefer_subnet
                                                        or node_ip.startswith(args.prefer_subnet))),
        "bindings_disjoint": bindings_disjoint(root_cpus, conn_cpus),
        "all_islands_valid": all_valid,
        "every_island_same_node": all(isl["gates"]["same_physical_node"] for isl in islands),
        "every_island_loopback_ip": all(isl["gates"]["loopback_same_advertised_ip"]
                                        for isl in islands),
        "every_island_two_localities": all(isl["gates"]["two_localities"] for isl in islands),
        "every_island_distinct_ports": all(isl["gates"]["distinct_ports"] for isl in islands),
        "every_island_qd1_correct": all(isl["gates"]["depth1_all_correct"] for isl in islands),
        "every_island_idle_backoff_disabled": all(isl["gates"]["idle_backoff_disabled"]
                                                  for isl in islands),
        "every_island_no_orphans": all(isl["gates"].get("no_orphans", True) for isl in islands),
        "r_count_ge_2": (args.reps >= 2),
    }
    # r_count_ge_2 is informational for the full band; a single-rep validation still passes
    must_pass = {k: v for k, v in gates.items() if k != "r_count_ge_2"}
    overall = "pass" if all(must_pass.values()) else "fail"

    base.update({
        "islands": islands,
        "islands_valid": islands_valid,
        "all_islands_valid": all_valid,
        "is_full_band": bool(args.reps >= 2 and all_valid),
        "per_island_stats": per_island,
        "across_island_stats": across_island_summary(per_island),
        "pooled_stats_supplementary": island_stats(pooled_raw),
        "gates": gates,
        "overall": overall,
        "fairness_caveats": [
            "WITHIN-HPX decomposition: L1 (this) collapses the network to kernel loopback while "
            "keeping the full HPX TCP parcel + scheduler stack and the caller-observed async().get() "
            "RTT. (L2 - L1) approximates only the inter-node wire, caller-observed, TCP, warm.",
            "per-island stats are PRIMARY; pooled is supplementary only.",
            "this is the symmetric analog of exp59 Slice 1's Ray same-host control, BUT the two are "
            "different runtimes/IPC and are NEVER cross-compared into a speedup.",
            "tcp_nodelay unverified -> Nagle/delayed-ACK confound held constant vs exp58, not removed.",
        ],
    })
    return base, overall


# ---------------------------------------------------------------------------------------------------
# phase: selftest (pure-Python helper checks; no Slurm, no binary)
# ---------------------------------------------------------------------------------------------------
def phase_selftest():
    checks = []

    def chk(name, cond):
        checks.append((name, bool(cond)))

    # 1. port distinctness
    pa, pb = pick_two_distinct_ports()
    chk("ports_distinct", isinstance(pa, int) and isinstance(pb, int) and pa != pb)

    # 2. core split auto disjoint
    rc, cc = compute_core_split(ncpu=32, threads=4, spec="auto")
    chk("core_split_auto_values", rc == {0, 1, 2, 3} and cc == {4, 5, 6, 7})
    chk("core_split_auto_disjoint", bindings_disjoint(rc, cc))
    chk("cpus_to_mask_compact", _cpus_to_mask(rc) == "0-3" and _cpus_to_mask({0, 2, 4}) == "0,2,4")

    # 3. insufficient cores raises
    try:
        compute_core_split(ncpu=6, threads=4, spec="auto")
        chk("core_split_insufficient_raises", False)
    except ValueError:
        chk("core_split_insufficient_raises", True)

    # 4. explicit overlap raises
    try:
        compute_core_split(ncpu=32, threads=4, spec="0-3:2-5")
        chk("core_split_overlap_raises", False)
    except ValueError:
        chk("core_split_overlap_raises", True)
    chk("bindings_disjoint_detects_overlap", not bindings_disjoint({0, 1}, {1, 2}))

    # 5. stat construction (nearest-rank + median-of-medians)
    chk("pct_nearest_rank", _pct(list(range(1, 101)), 0.50) == 50
        and _pct(list(range(1, 101)), 0.90) == 90 and _pct(list(range(1, 101)), 0.99) == 99)
    isl = island_stats(list(range(1, 1001)))
    chk("island_stats_basic", isl["count"] == 1000 and isl["min_ns"] == 1 and isl["max_ns"] == 1000
        and isl["p50_ns"] == 500)
    per = [{"count": 1, "p50_ns": 100, "p90_ns": 200, "p99_ns": 300, "mean_ns": 110,
            "min_ns": 90, "max_ns": 400},
           {"count": 1, "p50_ns": 120, "p90_ns": 220, "p99_ns": 320, "mean_ns": 130,
            "min_ns": 95, "max_ns": 420},
           {"count": 1, "p50_ns": 110, "p90_ns": 210, "p99_ns": 310, "mean_ns": 120,
            "min_ns": 92, "max_ns": 410}]
    ai = across_island_summary(per)
    chk("across_island_median", ai["p50_ns_median"] == 110 and ai["p50_ns_min"] == 100
        and ai["p50_ns_max"] == 120 and ai["per_island_primary"] is True)

    # 6. overwrite guard
    tmpd = tempfile.mkdtemp(prefix="exp60_selftest_")
    p = os.path.join(tmpd, "agg.json")
    _atomic_write(p, {"overall": "pass", "phase": "same-node-two-locality-control"})
    _, refused_skip, _ = safe_write_top_aggregate(
        p, {"overall": "skip", "phase": "same-node-two-locality-control"})
    chk("skip_never_overwrites_pass", refused_skip and _read_json(p)["overall"] == "pass")
    _, refused_fail, _ = safe_write_top_aggregate(
        p, {"overall": "fail", "phase": "same-node-two-locality-control"})
    chk("fail_never_overwrites_pass", refused_fail and _read_json(p)["overall"] == "pass")
    _, refused_noflag, _ = safe_write_top_aggregate(
        p, {"overall": "pass", "phase": "same-node-two-locality-control"}, allow_overwrite_pass=False)
    chk("pass_over_pass_needs_flag", refused_noflag)
    wp, refused_flag, _ = safe_write_top_aggregate(
        p, {"overall": "pass", "phase": "same-node-two-locality-control", "marker": "new"},
        allow_overwrite_pass=True)
    chk("pass_over_pass_with_flag_writes", (not refused_flag)
        and _read_json(wp).get("marker") == "new")

    # 7. missing-binary refusal
    chk("locate_missing_binary_returns_none",
        locate_binary(os.path.join(tmpd, "nope_binary")) is None)
    prov = binary_provenance(None)
    chk("provenance_missing_binary_flagged", prov["binary_present"] is False
        and prov["binary_sha256"] is None)

    # 8. orphan check / cleanup degrade gracefully when srun is unavailable (off-cluster):
    #    an unrunnable check must report None (NOT a confirmed-clean True), and must not raise.
    nc, pids = _orphan_check_node("no-such-node", "exp60_ctl_rX_unique")
    chk("orphan_check_no_srun_graceful", nc is None and pids == [])
    info = _cleanup_and_check("no-such-node", "exp60_ctl_rX_unique")
    chk("cleanup_keys_and_none_when_uncheckable",
        info["cleanup_checked_node"] == "no-such-node"
        and info["no_orphan_hpx_processes"] is None
        and info["cleanup_attempted"] is False
        and set(("orphan_pattern", "orphan_hpx_processes")).issubset(info))
    chk("uncheckable_orphans_do_not_fail_gate", (info["no_orphan_hpx_processes"] is not False))

    passed = sum(1 for _, ok in checks if ok)
    total = len(checks)
    for name, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    print(f"\nexp60 selftest: {passed}/{total} helper checks passed")
    return passed == total


# ---------------------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------------------
def _have_slurm():
    return bool(os.environ.get("SLURM_JOB_ID") and os.environ.get("SLURM_JOB_NODELIST"))


def main():
    ap = argparse.ArgumentParser(description="exp60 HPX same-node two-locality TCP control")
    ap.add_argument("--phase", required=True,
                    choices=["check-config", "same-node-two-locality-control", "selftest"])
    ap.add_argument("--binary", default=None, help="explicit path to the exp58 two_node_perf_spike")
    ap.add_argument("--node", default="medusa00", help="Slurm node to co-locate both localities on")
    ap.add_argument("--prefer-subnet", default="10.42.5.", help="advertised-IP subnet prefix")
    ap.add_argument("--reps", "-R", type=int, default=5)
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--k", type=int, default=1000)
    ap.add_argument("--w", type=int, default=100)
    ap.add_argument("--threads", type=int, default=4, help="HPX worker threads per locality")
    ap.add_argument("--core-split", default="auto",
                    help="'auto' => root=[0,T) conn=[T,2T); or explicit 'rootlist:connlist'")
    ap.add_argument("--pipeline-depths", default="8,32,128",
                    help="binary emits pipeline rows (recorded but EXCLUDED from comparison)")
    ap.add_argument("--ready-timeout", type=int, default=60)
    ap.add_argument("--leave-timeout", type=int, default=30)
    ap.add_argument("--serve-timeout", type=int, default=120)
    ap.add_argument("--run-budget", type=int, default=120)
    ap.add_argument("--agas-preprobe-timeout-ms", type=int, default=8000)
    ap.add_argument("--disable-idle-backoff", action="store_true", default=True)
    ap.add_argument("--allow-overwrite-pass", action="store_true", default=False)
    args = ap.parse_args()

    if args.phase == "selftest":
        return 0 if phase_selftest() else 1

    binary = locate_binary(args.binary)

    if args.phase == "check-config":
        if not binary:
            print("REFUSED: exp58 binary missing. Looked for:")
            for c in DEFAULT_BINARY_CANDIDATES:
                print("  ", c)
            print("Pass --binary <path> or build exp58 first (cmake/ninja in exp58/build).")
            return 2
        cfg = check_config(binary)
        cfg["binary_provenance"] = binary_provenance(binary)
        print(json.dumps(cfg, indent=2))
        return 0 if cfg.get("tcp_parcelport_available") else 1

    # same-node-two-locality-control
    if not _have_slurm():
        print("SKIP: no Slurm allocation detected (SLURM_JOB_ID / SLURM_JOB_NODELIST unset). "
              "exp60 needs a single-node medusa allocation; nothing measured, nothing written.")
        return 0
    if not binary:
        print("REFUSED: exp58 binary missing. Looked for:")
        for c in DEFAULT_BINARY_CANDIDATES:
            print("  ", c)
        print("Pass --binary <path> or build exp58 first. NO aggregate written.")
        return 2

    payload, overall = phase_same_node_control(binary, args)
    written, refused, redirected = safe_write_top_aggregate(
        os.path.join(HERE, TOP_AGGREGATE), payload, allow_overwrite_pass=args.allow_overwrite_pass)
    print(json.dumps({
        "overall": overall,
        "run_id": payload.get("run_id"),
        "node": payload.get("node"), "node_ip": payload.get("node_ip"),
        "islands_valid": payload.get("islands_valid"), "r_count": payload.get("r_count"),
        "is_full_band": payload.get("is_full_band"),
        "across_island_stats": payload.get("across_island_stats"),
        "written": os.path.basename(written), "overwrite_refused": refused,
        "reason": payload.get("reason"),
    }, indent=2))
    return 0 if overall == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
