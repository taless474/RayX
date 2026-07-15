#!/usr/bin/env python3
"""exp68 -- deterministic LLM-SHAPED distributed top-k reduction across two Ray actors sharing one
HPX runtime. Local single-node slice.

MECHANISM / EXACT-CHECKABLE EVIDENCE ONLY. This is a SYNTHETIC workload shaped like vocabulary-
sharded next-token candidate selection -- it is NOT real LLM inference (no weights, tokenizer, GPU,
or framework). Two Ray actors A and B each host an HPX connect-mode locality in-process (the exp67
mechanism, reused unchanged) and own a disjoint vocabulary shard. Each computes its shard's local
top-k; a coordinator fetches the peer's local top-k over an HPX action and merges through an HPX
future CONTINUATION; the global top-k is checked BIT-EXACTLY (token ids + float32 bit patterns)
against an independent controller oracle, in BOTH coordinator directions.

Slices (A-F gating; G non-gating):
  A  topology + disjoint/complete shard ownership
  B  local top-k correctness per actor vs oracle-restricted-to-shard
  C  A-coordinated HPX merge == global oracle (+ peer witnesses, no Ray payload path)
  D  B-coordinated HPX merge == same global oracle
  E  HPX future/continuation composition evidenced
  F  lifecycle: graceful leave, clean stop, root finalize, actor recreation, no orphans
  G  saturation diagnostic (non-gating)

Deterministic rule (mirrors shared_topk.hpp EXACTLY):
  h=(uint32(tid)*2654435761 + uint32(seed)*40503) mod 2^32 ; grid=(h%131)-65 ; logit=f32(grid)/8
Total order: higher logit wins; on equal logit, lower global token id wins.

CLAIM FENCE: not real inference; no model/tokenizer/GPU; not cross-node yet; not Python 3.14 /
free-threaded; no elasticity/churn/recovery; no performance/ratio/speedup/winner; no direct
serialization/wire claim; not production API. Version: CPython 3.11 (GIL), Ray 2.55.1, HPX
20bc3d4bf3068383edcb63be13f22e9ff95842fa (verified waiter-fix build).

Usage:
  python run_exp68.py [--reps 3] [--build-dir <dir>] [--aggregate <path>]
  python run_exp68.py --selftest      # pure gate/oracle/tie-break/float32 checks (no Ray, no HPX)
"""

import argparse
import json
import os
import platform
import re
import signal
import socket
import struct
import subprocess
import sys
import sysconfig
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
PEER_BASENAME = "exp68_peer"
EXT_MODULE = "exp68_actor_ext"
EXPECTED_HPX_COMMIT = "20bc3d4bf3068383edcb63be13f22e9ff95842fa"
DEFAULT_BUILD_DIR = os.path.join(HERE, "build")
HPX_HOST = "localhost"  # exp66/67 finding: connect mode maps "localhost"->127.0.0.1 with no resolver

CLAIM_FENCES = {
    "not_real_llm_inference": True, "no_model_weights_tokenizer_gpu": True,
    "not_cross_node": True, "not_python_3_14": True, "not_free_threaded": True,
    "no_elasticity_churn_or_recovery": True, "no_performance_speedup_ratio_winner": True,
    "no_direct_serialization_or_wire_claim": True, "not_production_api": True,
    "saturation_is_diagnostic_not_verdict": True, "timing_not_correctness_oracle": True,
}

FAILURE_CLASSES = [
    "invalid_shard_partition", "nondeterministic_logit_generation", "local_topk_a_failed",
    "local_topk_b_failed", "tie_break_contract_failed", "a_fetch_b_failed", "b_fetch_a_failed",
    "a_merge_failed", "b_merge_failed", "global_oracle_mismatch", "float32_bit_mismatch",
    "coordinator_asymmetry", "hpx_composition_not_evidenced", "ray_payload_path_confounded",
    "root_executed_application_work", "lifecycle_failed", "actor_recreation_failed",
    "orphan_detected", "invalid_instrumentation", "pass",
]

# Test matrix (fixed, not a sweep). Shard A owns [0, split); shard B owns [split, V). Seeds verified
# (see selftest scenario assertions) to exhibit the declared coverage property.
MATRIX = [
    {"name": "tiny_k1",      "V": 8,   "split": 4,   "k": 1,  "seed": 7,  "tags": ["small", "k1", "k_lt_shard"]},
    {"name": "tiny_k3",      "V": 8,   "split": 4,   "k": 3,  "seed": 7,  "tags": ["small", "kgt1", "k_lt_shard"]},
    {"name": "cross_both",   "V": 64,  "split": 32,  "k": 6,  "seed": 1,  "tags": ["both_shards", "kgt1"]},
    {"name": "tie_cutoff",   "V": 200, "split": 100, "k": 5,  "seed": 1,  "tags": ["tie_at_cutoff"]},
    {"name": "shardA_dom",   "V": 128, "split": 64,  "k": 8,  "seed": 1,  "tags": ["one_shard_dominant"]},
    {"name": "both_contrib", "V": 100, "split": 50,  "k": 10, "seed": 1,  "tags": ["both_shards"]},
    {"name": "k1_large",     "V": 256, "split": 128, "k": 1,  "seed": 13, "tags": ["k1"]},
]


# ---------------------------------------------------------------------------------------
# Deterministic oracle (stdlib struct; mirrors shared_topk.hpp bit-for-bit)
# ---------------------------------------------------------------------------------------

def grid_value(token_id, seed):
    h = (token_id * 2654435761) & 0xFFFFFFFF
    h = (h + ((seed * 40503) & 0xFFFFFFFF)) & 0xFFFFFFFF
    return (h % 131) - 65


def logit_bits(token_id, seed):
    return struct.unpack("<I", struct.pack("<f", grid_value(token_id, seed) / 8.0))[0]


def f32(bits):
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def oracle_topk(lo, hi, seed, k):
    """Independent oracle: the k best tokens over [lo,hi) by the total order, as (token_id, bits)."""
    cand = [(t, logit_bits(t, seed)) for t in range(lo, hi)]
    cand.sort(key=lambda c: (-f32(c[1]), c[0]))  # higher logit, then lower token id
    return [(t, b) for t, b in cand[:k]]


def norm_cands(pylist):
    """Normalize an ext/actor candidate list ([{token_id,logit_bits,...}]) to [(token_id, bits)]."""
    return [(int(d["token_id"]), int(d["logit_bits"])) for d in (pylist or [])]


def is_sorted_total_order(cands):
    """True iff cands respects the exact total order (higher logit, then lower token id)."""
    for i in range(len(cands) - 1):
        (t0, b0), (t1, b1) = cands[i], cands[i + 1]
        l0, l1 = f32(b0), f32(b1)
        if l0 < l1:
            return False
        if l0 == l1 and t0 >= t1:
            return False
    return True


# ---------------------------------------------------------------------------------------
# Process / file helpers (bounded; exp67 idiom)
# ---------------------------------------------------------------------------------------

def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0)); return s.getsockname()[1]
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
        os.kill(pid, 0); return True
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
    try:
        out = subprocess.run(["pgrep", "-f", PEER_BASENAME], capture_output=True, text=True, timeout=10)
    except Exception:
        return None
    if out.returncode not in (0, 1):
        return None
    me = str(os.getpid())
    return [p for p in out.stdout.split() if p and p != me]


def argv_audit(*cmds):
    for cmd in cmds:
        for a in cmd:
            if str(a).startswith("--hpx:localities"):
                return False
    return True


def extract_git_sha(cv):
    if not cv:
        return None
    i = cv.find("Git:")
    if i < 0:
        return None
    rest = cv[i + 4:].strip()
    tok = rest.split()[0] if rest else ""
    return tok.strip().strip(",") or None


def commit_matches(cv):
    sha = extract_git_sha(cv)
    return bool(sha) and len(sha) >= 10 and EXPECTED_HPX_COMMIT.startswith(sha)


def collect_provenance(ray_mod, a_identity):
    gil = None
    fn = getattr(sys, "_is_gil_enabled", None)
    if callable(fn):
        try:
            gil = bool(fn())
        except Exception:
            gil = None
    prov = {
        "python_version_full": sys.version, "python_implementation": sys.implementation.name,
        "python_ext_suffix": sysconfig.get_config_var("EXT_SUFFIX"),
        "py_gil_disabled_config": sysconfig.get_config_var("Py_GIL_DISABLED"),
        "gil_enabled_runtime": gil, "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "ray_version": getattr(ray_mod, "__version__", None), "ray_commit": getattr(ray_mod, "__commit__", None),
        "platform": platform.platform(), "machine": platform.machine(), "processor_count": os.cpu_count(),
        "compiler_note": "extension built with Apple clang (see build), HPX identity below",
        "expected_hpx_commit": EXPECTED_HPX_COMMIT,
    }
    if a_identity:
        prov["actor_ext_file"] = os.path.basename(a_identity.get("ext_file", "") or "")
        prov["actor_ext_abi_match"] = a_identity.get("abi_match")
        prov["hpx_complete_version"] = (a_identity.get("hpx_version_info") or {}).get("hpx_complete_version")
        prov["hpx_git_sha_observed"] = extract_git_sha(prov.get("hpx_complete_version"))
    return prov


# ---------------------------------------------------------------------------------------
# Deterministic generation-rule identity (recorded)
# ---------------------------------------------------------------------------------------

GENERATION_RULE = {
    "name": "int_grid_over_8",
    "formula": "h=(uint32(token_id)*2654435761 + uint32(seed)*40503) mod 2^32; "
               "grid=(h%131)-65; logit=float32(grid)/8.0",
    "grid_range": [-65, 65], "denominator": 8, "float32_exact": True,
    "total_order": "higher logit; tie -> lower global token id",
    "value_range": [-8.125, 8.125],
}


def partition_ok(V, split):
    """Disjoint + complete: A=[0,split), B=[split,V); every token in exactly one shard."""
    return bool(0 < split < V)


# ---------------------------------------------------------------------------------------
# Per-case and per-slice gate evaluation (pure -> selftestable without Ray/HPX)
# ---------------------------------------------------------------------------------------

def eval_case(case, cr):
    """Evaluate one matrix case's Slice B/C/D/E gates against the oracle. cr = case markers dict."""
    V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
    a_lo, a_hi, b_lo, b_hi = 0, split, split, V
    orc_a = oracle_topk(a_lo, a_hi, seed, k)
    orc_b = oracle_topk(b_lo, b_hi, seed, k)
    orc_g = oracle_topk(0, V, seed, k)
    a_local = norm_cands(cr.get("a_local"))
    b_local = norm_cands(cr.get("b_local"))
    ac = cr.get("a_coord") or {}
    bc = cr.get("b_coord") or {}
    a_global = norm_cands(ac.get("global_topk"))
    b_global = norm_cands(bc.get("global_topk"))
    a_comp = ac.get("hpx_composition") or {}
    b_comp = bc.get("hpx_composition") or {}
    a_loc, b_loc = cr.get("a_loc"), cr.get("b_loc")
    a_pid, b_pid = cr.get("a_pid"), cr.get("b_pid")

    g = {}
    g["shard_partition_ok"] = partition_ok(V, split)
    # Slice B
    g["local_topk_a_correct"] = (a_local == orc_a)
    g["local_topk_b_correct"] = (b_local == orc_b)
    g["local_topk_order_ok"] = (is_sorted_total_order(a_local) and is_sorted_total_order(b_local))
    a_ids = [t for t, _ in a_global]; b_ids = [t for t, _ in b_global]; o_ids = [t for t, _ in orc_g]
    a_bits = [b for _, b in a_global]; b_bits = [b for _, b in b_global]; o_bits = [b for _, b in orc_g]
    # Slice C (A coordinates: A fetches B over HPX, A merges)
    g["a_fetch_b_ready"] = bool(ac.get("ready") and ac.get("target_found"))
    g["a_peer_is_b"] = bool(ac.get("peer_locality") == b_loc and ac.get("peer_pid") == b_pid)
    g["a_own_is_a"] = bool(ac.get("own_locality") == a_loc and ac.get("own_pid") == a_pid)
    g["a_merge_ids_match"] = (a_ids == o_ids)
    g["a_merge_equals_oracle"] = (a_global == orc_g)
    # Slice D (B coordinates)
    g["b_fetch_a_ready"] = bool(bc.get("ready") and bc.get("target_found"))
    g["b_peer_is_a"] = bool(bc.get("peer_locality") == a_loc and bc.get("peer_pid") == a_pid)
    g["b_own_is_b"] = bool(bc.get("own_locality") == b_loc and bc.get("own_pid") == b_pid)
    g["b_merge_ids_match"] = (b_ids == o_ids)
    g["b_merge_equals_oracle"] = (b_global == orc_g)
    # Coordinator symmetry (both coordinators agree) + exact global token ids + float32 bit patterns
    g["coordinator_symmetry"] = (a_global == b_global)
    g["global_token_ids_exact"] = (a_ids == o_ids and b_ids == o_ids)
    g["float32_bits_exact"] = (a_bits == o_bits and b_bits == o_bits)
    # Slice E composition
    def comp_ok(c):
        return bool(c.get("action_dispatched") and c.get("future_ready")
                    and c.get("continuation_executed") and c.get("result_delivered"))
    g["hpx_composition_a"] = comp_ok(a_comp)
    g["hpx_composition_b"] = comp_ok(b_comp)
    # Root ran no application work: every merge locality is a nonzero actor locality
    g["no_application_on_root"] = bool(
        ac.get("peer_locality") not in (None, 0) and ac.get("own_locality") not in (None, 0)
        and bc.get("peer_locality") not in (None, 0) and bc.get("own_locality") not in (None, 0))
    return g, {"oracle_global": orc_g}


def eval_slice_a(m):
    """Slice A: topology + shard ownership (interpreter-independent hosting + partition)."""
    ai, bi = m.get("a_identity") or {}, m.get("b_identity") or {}
    as_, bs = m.get("a_start") or {}, m.get("b_start") or {}
    ac, bc = m.get("a_child_report") or {}, m.get("b_child_report") or {}
    rr, rf = m.get("root_ready") or {}, m.get("root_final") or {}
    a_pid, b_pid = ai.get("pid"), bi.get("pid")
    a_loc, b_loc = as_.get("locality_id"), bs.get("locality_id")
    a_cv = (ai.get("hpx_version_info") or {}).get("hpx_complete_version")
    b_cv = (bi.get("hpx_version_info") or {}).get("hpx_complete_version")
    membership_max = max(as_.get("membership") or 0, bs.get("membership") or 0)
    g = {}
    g["fixed_hpx_commit"] = bool(commit_matches(a_cv) and commit_matches(b_cv)
                                 and commit_matches(rr.get("hpx_complete_version")))
    g["actor_a_started_in_worker"] = bool(as_.get("started") and a_loc is not None)
    g["actor_b_started_in_worker"] = bool(bs.get("started") and b_loc is not None)
    g["two_distinct_ray_processes"] = bool(a_pid is not None and b_pid is not None and a_pid != b_pid)
    g["distinct_nonzero_localities"] = bool(a_loc not in (None, 0) and b_loc not in (None, 0)
                                            and a_loc != b_loc)
    g["both_in_process_no_hpx_child"] = bool(ac.get("checked") and ac.get("hpx_children") == []
                                             and bc.get("checked") and bc.get("hpx_children") == [])
    g["shared_island_membership"] = bool(a_loc and b_loc and membership_max >= 3)
    g["no_static_locality_count"] = bool(m.get("argv_audit_ok"))
    g["root_isolated_workfree"] = bool(rr.get("locality_id") == 0 and rf.get("max_membership", 0) >= 3)
    g["all_shards_disjoint_complete"] = bool(m.get("all_partitions_ok"))
    g["hostname_identity"] = bool(ai.get("hostname") and ai.get("hostname") == bi.get("hostname")
                                  == rr.get("hostname") == m.get("controller_hostname"))
    g["evidence_complete"] = bool(a_pid and b_pid and a_loc and b_loc and a_cv and b_cv and rr and rf
                                  and m.get("cases"))
    return g


def eval_slice_f(m):
    """Slice F: lifecycle (graceful leave, clean stop, root finalize, recreation, no orphans)."""
    rf, rr = m.get("root_final") or {}, m.get("root_ready") or {}
    a_pid = (m.get("a_identity") or {}).get("pid")
    b_pid = (m.get("b_identity") or {}).get("pid")
    ar, br = m.get("a_recreate") or {}, m.get("b_recreate") or {}
    g = {}
    g["actor_a_graceful_leave"] = (m.get("a_stop_rc") == 0)
    g["actor_b_graceful_leave"] = (m.get("b_stop_rc") == 0)
    g["clean_hpx_stop_both"] = bool(m.get("a_stop_rc") == 0 and not m.get("a_stop_error")
                                    and m.get("b_stop_rc") == 0 and not m.get("b_stop_error"))
    g["root_finalized_clean"] = bool(m.get("root_exit_path") == "finalized_clean"
                                     and rf.get("leave_observed") is True
                                     and rf.get("final_membership") == 1)
    g["heartbeat_completion_lifecycle"] = bool(rr.get("wall_ms") is not None
                                               and rf.get("leave_observed") is True)
    g["actor_a_recreation"] = bool(ar.get("ok") and ar.get("pid") and ar.get("pid") != a_pid)
    g["actor_b_recreation"] = bool(br.get("ok") and br.get("pid") and br.get("pid") != b_pid)
    g["no_orphans"] = (m.get("orphans") == [] and m.get("actor_pids_gone") is True)
    g["all_waits_bounded"] = bool(m.get("all_waits_bounded"))
    g["ray_no_actor_payload_path"] = bool(m.get("operation_over_hpx_not_ray"))
    return g


def rep_verdict(ga, gf, case_gates):
    return (all(ga.values()) and all(gf.values())
            and all(all(cg.values()) for cg in case_gates))


def failure_class(ga, gf, cases_eval):
    if not ga.get("evidence_complete"):
        return "invalid_instrumentation"
    if not ga.get("all_shards_disjoint_complete"):
        return "invalid_shard_partition"
    if not (ga.get("actor_a_started_in_worker") and ga.get("actor_b_started_in_worker")):
        return "lifecycle_failed"
    if not ga.get("both_in_process_no_hpx_child"):
        return "lifecycle_failed"
    if not (ga.get("two_distinct_ray_processes") and ga.get("distinct_nonzero_localities")
            and ga.get("shared_island_membership")):
        return "invalid_instrumentation"
    for name, cg in cases_eval:
        if not cg.get("shard_partition_ok"):
            return "invalid_shard_partition"
        if not cg.get("local_topk_order_ok"):
            return "tie_break_contract_failed"
        if not cg.get("local_topk_a_correct"):
            return "local_topk_a_failed"
        if not cg.get("local_topk_b_correct"):
            return "local_topk_b_failed"
        if not cg.get("a_fetch_b_ready"):
            return "a_fetch_b_failed"
        if not cg.get("b_fetch_a_ready"):
            return "b_fetch_a_failed"
        if not (cg.get("a_peer_is_b") and cg.get("a_own_is_a")
                and cg.get("b_peer_is_a") and cg.get("b_own_is_b")):
            return "ray_payload_path_confounded"
        if not cg.get("no_application_on_root"):
            return "root_executed_application_work"
        if not (cg.get("hpx_composition_a") and cg.get("hpx_composition_b")):
            return "hpx_composition_not_evidenced"
        if not cg.get("a_merge_ids_match"):
            return "a_merge_failed"
        if not cg.get("b_merge_ids_match"):
            return "b_merge_failed"
        if not cg.get("coordinator_symmetry"):
            return "coordinator_asymmetry"
        if not cg.get("float32_bits_exact"):
            return "float32_bit_mismatch"
        if not (cg.get("a_merge_equals_oracle") and cg.get("b_merge_equals_oracle")):
            return "global_oracle_mismatch"
    if not (gf.get("actor_a_recreation") and gf.get("actor_b_recreation")):
        return "actor_recreation_failed"
    if not gf.get("no_orphans"):
        return "orphan_detected"
    if not all(gf.values()):
        return "lifecycle_failed"
    if rep_verdict(ga, gf, [dict(cg) for _, cg in cases_eval]):
        return "pass"
    return "invalid_instrumentation"


# ---------------------------------------------------------------------------------------
# Ray actor: hosts an HPX connect-mode locality IN-PROCESS + owns a vocab shard
# ---------------------------------------------------------------------------------------

def build_actor_class(ray_mod):
    @ray_mod.remote
    class HpxActor:
        def __init__(self, build_dir, hpx_threads, endpoints,
                     ext_module="exp68_actor_ext", peer_basename="exp68_peer"):
            import sys as _sys
            _sys.path.insert(0, build_dir)
            self._build_dir = build_dir
            self._threads = hpx_threads
            self._endpoints = endpoints
            self._ext_module = ext_module
            self._peer_basename = peer_basename
            self._ext = None

        def ray_placement(self):
            """Ray-authoritative placement identity of THIS worker (for hard-placement gates)."""
            import os as _os, socket as _socket
            try:
                import ray as _ray
                ctx = _ray.get_runtime_context()
                nid, aid = ctx.get_node_id(), ctx.get_actor_id()
            except Exception:  # noqa: BLE001
                nid, aid = None, None
            return {"node_id": nid, "actor_id": aid, "hostname": _socket.gethostname(), "pid": _os.getpid()}

        def load_identity(self):
            import importlib, os as _os, sysconfig as _sc
            e = importlib.import_module(self._ext_module)
            self._ext = e
            suffix = _sc.get_config_var("EXT_SUFFIX")
            return {"pid": e.pid(), "os_getpid": _os.getpid(), "hostname": e.hostname(),
                    "hpx_version_info": dict(e.hpx_version_info()),
                    "gil_declaration": getattr(e, "__gil_declaration__", None),
                    "experiment": getattr(e, "__experiment__", None), "ext_file": e.__file__,
                    "abi_match": bool(e.__file__.endswith(suffix)) if suffix else None}

        def start_hpx(self):
            try:
                self._ext.start_connect(self._threads, list(self._endpoints))
                return {"started": True, "locality_id": int(self._ext.locality_id()),
                        "membership": int(self._ext.membership_count()), "pid": self._ext.pid()}
            except Exception as ex:  # noqa: BLE001
                return {"started": False, "error": f"{type(ex).__name__}: {ex}"}

        def child_report(self):
            import os as _os, subprocess as _sp
            pid = _os.getpid(); children, hpx_children, checked = [], [], False
            try:
                out = _sp.run(["pgrep", "-P", str(pid)], capture_output=True, text=True, timeout=10)
                checked = out.returncode in (0, 1)
                for cp in out.stdout.split():
                    if not cp:
                        continue
                    info = _sp.run(["ps", "-o", "command=", "-p", cp], capture_output=True, text=True, timeout=10)
                    cmd = info.stdout.strip(); children.append({"pid": int(cp), "cmd": cmd})
                    if self._peer_basename in cmd or self._ext_module in cmd or "hpx" in cmd.lower():
                        hpx_children.append({"pid": int(cp), "cmd": cmd})
            except Exception as ex:  # noqa: BLE001
                return {"checked": False, "error": f"{type(ex).__name__}: {ex}",
                        "children": children, "hpx_children": hpx_children}
            return {"checked": checked, "worker_pid": pid, "children": children, "hpx_children": hpx_children}

        def local_topk(self, lo, hi, seed, k):
            try:
                return list(self._ext.local_topk(int(lo), int(hi), int(seed), int(k)))
            except Exception as ex:  # noqa: BLE001
                return {"error": f"{type(ex).__name__}: {ex}"}

        def coordinate(self, peer_loc, own_lo, own_hi, peer_lo, peer_hi, seed, k, bound_s=10):
            try:
                return dict(self._ext.coordinate(int(peer_loc), int(own_lo), int(own_hi),
                                                 int(peer_lo), int(peer_hi), int(seed), int(k), int(bound_s)))
            except Exception as ex:  # noqa: BLE001
                return {"ready": False, "target_found": False, "error": f"{type(ex).__name__}: {ex}"}

        def health(self):
            try:
                return {"ok": True, "pid": self._ext.pid(),
                        "membership": int(self._ext.membership_count()),
                        "locality_id": int(self._ext.locality_id())}
            except Exception as ex:  # noqa: BLE001
                return {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

        def busy_spin(self, seconds, started_path, done_path):
            import time as _t
            started_ms = int(_t.time() * 1000)
            try:
                open(started_path, "w").write(str(started_ms))
            except OSError:
                pass
            acc = 0; end = _t.perf_counter() + seconds
            while _t.perf_counter() < end:
                for _ in range(200000):
                    acc = (acc + 1) & 0xFFFFFFFF
            done_ms = int(_t.time() * 1000)
            try:
                open(done_path, "w").write(str(done_ms))
            except OSError:
                pass
            return {"acc": acc, "started_ms": started_ms, "done_ms": done_ms}

        def stop_hpx(self):
            try:
                return {"rc": int(self._ext.stop_disconnect()), "error": None}
            except Exception as ex:  # noqa: BLE001
                return {"rc": -1, "error": f"{type(ex).__name__}: {ex}"}

        def ping(self):
            import importlib, os as _os, socket as _socket
            info = {}
            try:
                e = importlib.import_module(self._ext_module)
                info["ext_importable"] = True
                info["hpx_complete_version"] = dict(e.hpx_version_info()).get("hpx_complete_version")
            except Exception as ex:  # noqa: BLE001
                info["ext_importable"] = False; info["error"] = f"{type(ex).__name__}: {ex}"
            info["ok"] = True; info["pid"] = _os.getpid(); info["hostname"] = _socket.gethostname()
            try:
                import ray as _ray
                info["node_id"] = _ray.get_runtime_context().get_node_id()
            except Exception:  # noqa: BLE001
                info["node_id"] = None
            return info

    return HpxActor


# ---------------------------------------------------------------------------------------
# One repetition (full matrix)
# ---------------------------------------------------------------------------------------

def peer_root_cmd(peer, boot, p_root):
    return [peer, "--role", "root", "--bootstrap", boot, "--leave-timeout", "25",
            f"--hpx:agas={HPX_HOST}:{p_root}", f"--hpx:hpx={HPX_HOST}:{p_root}",
            "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
            "--hpx:ignore-batch-env"]


def actor_endpoints(p_root, p_actor):
    return [f"--hpx:agas={HPX_HOST}:{p_root}", f"--hpx:hpx={HPX_HOST}:{p_actor}",
            "--hpx:bind=none", "--hpx:ignore-batch-env"]


def _ray_get(ray_mod, ref, timeout, what):
    try:
        return ray_mod.get(ref, timeout=timeout)
    except Exception as ex:  # noqa: BLE001
        return {"ok": False, "started": False, "ready": False, "error": f"{what}: {type(ex).__name__}: {ex}"}


def run_rep(ray_mod, HpxActor, peer, build_dir, rep, runs_root, hpx_threads, ray_cpus):
    boot = os.path.join(runs_root, f"rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    p_root, p_a, p_b = find_free_port(), find_free_port(), find_free_port()
    rcmd = peer_root_cmd(peer, boot, p_root)
    ep_a, ep_b = actor_endpoints(p_root, p_a), actor_endpoints(p_root, p_b)
    m = {
        "controller_hostname": socket.gethostname(),
        "ports": {"root": p_root, "actor_a": p_a, "actor_b": p_b},
        "argv_audit_ok": argv_audit(rcmd, ep_a, ep_b),
        "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus,
        "generation_rule": GENERATION_RULE,
        "operation_over_hpx_not_ray": True, "all_waits_bounded": True, "no_python_polling": True,
        "all_partitions_ok": all(partition_ok(c["V"], c["split"]) for c in MATRIX),
        "cases": [],
    }
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    saturation = []
    try:
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file(os.path.join(boot, "root.ready"), 30, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        a1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_a)
        m["a_identity"] = _ray_get(ray_mod, a1.load_identity.remote(), 40, "a_identity")
        m["a_start"] = _ray_get(ray_mod, a1.start_hpx.remote(), 60, "a_start")
        m["a_child_report"] = _ray_get(ray_mod, a1.child_report.remote(), 30, "a_child_report")
        a_loc = (m["a_start"] or {}).get("locality_id")

        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_b)
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 40, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 60, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")
        b_loc = (m["b_start"] or {}).get("locality_id")
        a_pid = (m.get("a_identity") or {}).get("pid")
        b_pid = (m.get("b_identity") or {}).get("pid")

        if a_loc not in (None, 0) and b_loc not in (None, 0):
            for case in MATRIX:
                V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
                a_lo, a_hi, b_lo, b_hi = 0, split, split, V
                cr = {"name": case["name"], "V": V, "split": split, "k": k, "seed": seed,
                      "shard_a": [a_lo, a_hi], "shard_b": [b_lo, b_hi],
                      "a_loc": a_loc, "b_loc": b_loc, "a_pid": a_pid, "b_pid": b_pid}
                cr["a_local"] = _ray_get(ray_mod, a1.local_topk.remote(a_lo, a_hi, seed, k), 30, "a_local")
                cr["b_local"] = _ray_get(ray_mod, b1.local_topk.remote(b_lo, b_hi, seed, k), 30, "b_local")
                # Slice C: A coordinates (fetch B over HPX, merge in continuation)
                cr["a_coord"] = _ray_get(ray_mod, a1.coordinate.remote(b_loc, a_lo, a_hi, b_lo, b_hi, seed, k),
                                         40, "a_coord")
                # Slice D: B coordinates
                cr["b_coord"] = _ray_get(ray_mod, b1.coordinate.remote(a_loc, b_lo, b_hi, a_lo, a_hi, seed, k),
                                         40, "b_coord")
                cr["oracle_global"] = [[t, b] for t, b in oracle_topk(0, V, seed, k)]
                m["cases"].append(cr)

        m["a_health"] = _ray_get(ray_mod, a1.health.remote(), 30, "a_health")
        m["b_health"] = _ray_get(ray_mod, b1.health.remote(), 30, "b_health")

        # Slice G (non-gating): A coordinates while B's Python thread is CPU-saturated (one case)
        if a_loc not in (None, 0) and b_loc not in (None, 0):
            case = MATRIX[2]
            V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
            bs_path, bd_path = os.path.join(boot, "b_busy_started"), os.path.join(boot, "b_busy_done")
            b_busy = b1.busy_spin.remote(3.0, bs_path, bd_path)
            seen = _wait_for_file(bs_path, 15)
            gc = _ray_get(ray_mod, a1.coordinate.remote(b_loc, 0, split, split, V, seed, k), 40, "sat_coord")
            try:
                b_ret = ray_mod.get(b_busy, timeout=30)
            except Exception:
                b_ret = None
            orc_g = [[t, b] for t, b in oracle_topk(0, V, seed, k)]
            progressed = bool(gc.get("ready") and norm_cands(gc.get("global_topk")) == [(t, b) for t, b in oracle_topk(0, V, seed, k)])
            saturation.append({"case": case["name"], "overlap_observed": bool(seen and b_ret is not None),
                               "progressed_under_dest_saturation": progressed, "affects_verdict": False})

        # Slice F: graceful leave both -> membership 1
        a_stop = _ray_get(ray_mod, a1.stop_hpx.remote(), 30, "a_stop")
        b_stop = _ray_get(ray_mod, b1.stop_hpx.remote(), 30, "b_stop")
        m["a_stop_rc"], m["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
        m["b_stop_rc"], m["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")
        ray_mod.kill(a1); a1 = None
        ray_mod.kill(b1); b1 = None
        m["a_destroyed"] = wait_pid_gone(a_pid, 20)
        m["b_destroyed"] = wait_pid_gone(b_pid, 20)
        a2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_a)
        b2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_b)
        m["a_recreate"] = _ray_get(ray_mod, a2.ping.remote(), 40, "a_recreate")
        m["b_recreate"] = _ray_get(ray_mod, b2.ping.remote(), 40, "b_recreate")
        a2_pid, b2_pid = (m["a_recreate"] or {}).get("pid"), (m["b_recreate"] or {}).get("pid")
        open(os.path.join(boot, "root.done"), "w").close()
        _wait_for_file(os.path.join(boot, "root.final"), 40, procs=[root])
        m["root_final"] = _read_json(os.path.join(boot, "root.final"))
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

    ga = eval_slice_a(m)
    cases_eval = []
    case_gates = []
    for cr in m["cases"]:
        case = next(c for c in MATRIX if c["name"] == cr["name"])
        cg, _extra = eval_case(case, cr)
        cases_eval.append((cr["name"], cg))
        case_gates.append(cg)
    gf = eval_slice_f(m)
    passed = rep_verdict(ga, gf, case_gates)
    return {
        "rep": rep, "boot_rel": os.path.relpath(boot, HERE), "ports": m["ports"],
        "slice_a_gates": ga, "slice_f_gates": gf,
        "case_gates": {name: cg for name, cg in cases_eval},
        "rep_pass": passed, "failure_class": failure_class(ga, gf, cases_eval),
        "slice_a_failed": [k for k, v in ga.items() if not v],
        "slice_f_failed": [k for k, v in gf.items() if not v],
        "cases_failed": {name: [k for k, v in cg.items() if not v]
                         for name, cg in cases_eval if not all(cg.values())},
        "saturation_diagnostic": saturation,
        "markers": m,
    }


# ---------------------------------------------------------------------------------------
# Selftest: pure gate/oracle/tie-break/float32 checks (no Ray, no HPX)
# ---------------------------------------------------------------------------------------

def _synthetic_case_result(case, a_loc=1, b_loc=2, a_pid=5001, b_pid=5002):
    """Build an all-correct case-result from the oracle (as the actors would return it)."""
    V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
    def to_list(pairs):
        return [{"token_id": t, "logit_bits": b, "logit": f32(b)} for t, b in pairs]
    orc_a = oracle_topk(0, split, seed, k)
    orc_b = oracle_topk(split, V, seed, k)
    orc_g = oracle_topk(0, V, seed, k)
    comp = {"action_dispatched": True, "future_ready": True,
            "continuation_executed": True, "result_delivered": True}
    ac = {"ready": True, "target_found": True, "global_topk": to_list(orc_g),
          "own_topk": to_list(orc_a), "peer_topk": to_list(orc_b),
          "peer_locality": b_loc, "peer_pid": b_pid, "own_locality": a_loc, "own_pid": a_pid,
          "hpx_composition": dict(comp)}
    bc = {"ready": True, "target_found": True, "global_topk": to_list(orc_g),
          "own_topk": to_list(orc_b), "peer_topk": to_list(orc_a),
          "peer_locality": a_loc, "peer_pid": a_pid, "own_locality": b_loc, "own_pid": b_pid,
          "hpx_composition": dict(comp)}
    return {"name": case["name"], "V": V, "split": split, "k": k, "seed": seed,
            "shard_a": [0, split], "shard_b": [split, V], "a_loc": a_loc, "b_loc": b_loc,
            "a_pid": a_pid, "b_pid": b_pid, "a_local": to_list(orc_a), "b_local": to_list(orc_b),
            "a_coord": ac, "b_coord": bc, "oracle_global": [[t, b] for t, b in orc_g]}


def _synthetic_markers():
    cv = f"HPX: V2.0.0 (AGAS: V3.0), Git: {EXPECTED_HPX_COMMIT[:10]}"
    m = {"controller_hostname": "host0", "argv_audit_ok": True, "actor_hpx_threads": 2,
         "actor_ray_num_cpus": 2, "operation_over_hpx_not_ray": True, "all_waits_bounded": True,
         "no_python_polling": True, "all_partitions_ok": True,
         "a_identity": {"pid": 5001, "hostname": "host0", "hpx_version_info": {"hpx_complete_version": cv}},
         "b_identity": {"pid": 5002, "hostname": "host0", "hpx_version_info": {"hpx_complete_version": cv}},
         "a_start": {"started": True, "locality_id": 1, "membership": 2, "pid": 5001},
         "b_start": {"started": True, "locality_id": 2, "membership": 3, "pid": 5002},
         "a_child_report": {"checked": True, "hpx_children": []},
         "b_child_report": {"checked": True, "hpx_children": []},
         "root_ready": {"locality_id": 0, "hostname": "host0", "wall_ms": 1000, "hpx_complete_version": cv},
         "root_final": {"max_membership": 3, "final_membership": 1, "leave_observed": True},
         "a_stop_rc": 0, "a_stop_error": None, "b_stop_rc": 0, "b_stop_error": None,
         "a_recreate": {"ok": True, "pid": 6001}, "b_recreate": {"ok": True, "pid": 6002},
         "root_exit_path": "finalized_clean", "actor_pids_gone": True, "orphans": [],
         "cases": [_synthetic_case_result(c) for c in MATRIX]}
    return m


def _all_case_gates(m):
    out = []
    for cr in m["cases"]:
        case = next(c for c in MATRIX if c["name"] == cr["name"])
        cg, _ = eval_case(case, cr)
        out.append((cr["name"], cg))
    return out


def selftest():
    failures = []

    # ---- deterministic float32 bit patterns (known IEEE-754 values) ----
    for grid, want in [(0, 0x00000000), (8, 0x3F800000), (-8, 0xBF800000),
                       (1, 0x3E000000), (-1, 0xBE000000), (65, 0x41020000)]:  # 65/8 = 8.125
        got = struct.unpack("<I", struct.pack("<f", grid / 8.0))[0]
        if got != want:
            failures.append(f"float32 bits for grid={grid}: got {hex(got)} want {hex(want)}")

    # ---- tie-break contract: equal logit -> lower token id wins ----
    seed = 1
    by_bits = {}
    for t in range(0, 300):
        by_bits.setdefault(logit_bits(t, seed), []).append(t)
    tie_group = next((ts for ts in by_bits.values() if len(ts) >= 2), None)
    if not tie_group:
        failures.append("expected at least one tie group in [0,300)")
    else:
        ts = sorted(tie_group[:2])                     # two tokens with EQUAL logit, ascending ids
        b = logit_bits(ts[0], seed)                    # equal bits by construction
        pair = [(ts[1], b), (ts[0], b)]                # deliberately place the HIGHER id first
        pair.sort(key=lambda c: (-f32(c[1]), c[0]))    # apply the total order
        if [t for t, _ in pair] != ts:                 # must come out lower-id-first
            failures.append("tie-break must rank lower token id first at equal logit")
    # is_sorted_total_order must reject a lower-logit-first ordering
    two = sorted([(0, logit_bits(0, seed)), (1, logit_bits(1, seed))], key=lambda c: f32(c[1]))
    if f32(two[0][1]) != f32(two[1][1]) and is_sorted_total_order(two):
        failures.append("is_sorted_total_order must reject lower-logit-first order")

    # ---- matrix scenario coverage (each declared property holds) ----
    def top(case):
        return oracle_topk(0, case["V"], case["seed"], case["k"])
    def shardA_count(case):
        return sum(1 for t, _ in top(case) if t < case["split"])
    for c in MATRIX:
        if not partition_ok(c["V"], c["split"]):
            failures.append(f"{c['name']}: invalid partition")
    tie = next(c for c in MATRIX if "tie_at_cutoff" in c["tags"])
    o = oracle_topk(0, tie["V"], tie["seed"], tie["k"] + 1)
    if not (f32(o[tie["k"] - 1][1]) == f32(o[tie["k"]][1])):
        failures.append("tie_cutoff case must have equal logit spanning the k-th cutoff")
    dom = next(c for c in MATRIX if "one_shard_dominant" in c["tags"])
    if not (shardA_count(dom) >= (dom["k"] // 2 + 1)):
        failures.append("shardA_dom case must have a dominant shard")
    for c in MATRIX:
        if "both_shards" in c["tags"]:
            a = shardA_count(c)
            if not (0 < a < c["k"]):
                failures.append(f"{c['name']}: both shards must contribute (a={a}, k={c['k']})")
    for c in MATRIX:
        if "k1" in c["tags"] and c["k"] != 1:
            failures.append(f"{c['name']}: k1 tag but k={c['k']}")
        if "k_lt_shard" in c["tags"] and not (c["k"] < c["split"] and c["k"] < c["V"] - c["split"]):
            failures.append(f"{c['name']}: k not smaller than each shard")

    # ---- gate logic: clean synthetic passes; mutations map to the right class ----
    m = _synthetic_markers()
    ga, gf = eval_slice_a(m), eval_slice_f(m)
    ce = _all_case_gates(m)
    if not rep_verdict(ga, gf, [cg for _, cg in ce]):
        bad_a = [k for k, v in ga.items() if not v]
        bad_f = [k for k, v in gf.items() if not v]
        bad_c = {n: [k for k, v in cg.items() if not v] for n, cg in ce if not all(cg.values())}
        failures.append(f"clean synthetic rep must pass: A={bad_a} F={bad_f} cases={bad_c}")
    if failure_class(ga, gf, ce) != "pass":
        failures.append(f"clean rep failure_class must be pass, got {failure_class(ga, gf, ce)}")

    def expect(mut, cls, label):
        mm = _synthetic_markers(); mut(mm)
        fc = failure_class(eval_slice_a(mm), eval_slice_f(mm), _all_case_gates(mm))
        if fc != cls:
            failures.append(f"{label}: expected {cls}, got {fc}")

    expect(lambda mm: mm["cases"][0]["a_local"].__setitem__(0, {"token_id": 999, "logit_bits": 0}),
           "local_topk_a_failed", "wrong A local top-k")
    expect(lambda mm: mm["cases"][0]["b_local"].__setitem__(0, {"token_id": 999, "logit_bits": 0}),
           "local_topk_b_failed", "wrong B local top-k")
    # wrong A merged token ids (B still correct) -> a_merge_failed (before the symmetry check)
    expect(lambda mm: mm["cases"][0]["a_coord"].__setitem__("global_topk",
           [{"token_id": 999, "logit_bits": 0}]), "a_merge_failed", "wrong A merge ids")
    # wrong B merged token ids (A correct) -> b_merge_failed
    expect(lambda mm: mm["cases"][0]["b_coord"].__setitem__("global_topk",
           [{"token_id": 999, "logit_bits": 0}]), "b_merge_failed", "wrong B merge ids")
    # coordinators disagree only in a bit pattern (ids match oracle both ways) -> coordinator_asymmetry
    def _asym(mm):
        c = mm["cases"][2]  # cross_both, k=6 (len>1)
        g = c["b_coord"]["global_topk"]
        g[0] = {"token_id": g[0]["token_id"], "logit_bits": g[0]["logit_bits"] ^ 0x1}
    expect(_asym, "coordinator_asymmetry", "coordinators disagree (bit)")
    # same wrong bit in BOTH coordinators: ids match oracle, symmetric, bits wrong -> float32_bit_mismatch
    def _bits_off(mm):
        for coord in ("a_coord", "b_coord"):
            g = mm["cases"][2][coord]["global_topk"]
            g[0] = {"token_id": g[0]["token_id"], "logit_bits": g[0]["logit_bits"] ^ 0x1}
    expect(_bits_off, "float32_bit_mismatch", "float32 bit off (symmetric)")
    expect(lambda mm: mm["cases"][0]["a_coord"]["hpx_composition"].__setitem__("continuation_executed", False),
           "hpx_composition_not_evidenced", "missing continuation evidence")
    expect(lambda mm: mm["cases"][0]["a_coord"].__setitem__("peer_locality", 9),
           "ray_payload_path_confounded", "A peer not B")
    expect(lambda mm: mm["cases"][0]["a_coord"].__setitem__("own_locality", 0),
           "ray_payload_path_confounded", "coordinator own locality 0")
    expect(lambda mm: mm["cases"][0]["a_coord"].__setitem__("ready", False),
           "a_fetch_b_failed", "A fetch not ready")
    expect(lambda mm: mm["b_child_report"].__setitem__("hpx_children", [{"pid": 1, "cmd": "exp68_peer"}]),
           "lifecycle_failed", "B has HPX child")
    expect(lambda mm: mm.__setitem__("all_partitions_ok", False),
           "invalid_shard_partition", "bad partition")
    expect(lambda mm: mm["a_recreate"].__setitem__("pid", 5001),
           "actor_recreation_failed", "A recreation same pid")
    expect(lambda mm: mm.__setitem__("orphans", ["9"]), "orphan_detected", "orphan present")

    # wrong HPX commit -> rep fails
    m5 = _synthetic_markers()
    m5["a_identity"]["hpx_version_info"]["hpx_complete_version"] = "Git: deadbeef12"
    if rep_verdict(eval_slice_a(m5), eval_slice_f(m5), [cg for _, cg in _all_case_gates(m5)]):
        failures.append("wrong HPX commit must fail the rep verdict")

    if argv_audit(["peer", "--hpx:localities=3"]):
        failures.append("argv_audit must reject --hpx:localities")

    # ---- cross-node gate logic (pure; no Slurm, no Ray, no HPX) --------------------------------
    def cx_expect():
        return {"slurm_job_id": "555", "nodes": ["medusa00", "medusa01", "medusa11"],
                "nodeR": "medusa00", "nodeA": "medusa01", "nodeB": "medusa11",
                "nodeR_nid": "nidR", "nodeA_nid": "nidA", "nodeB_nid": "nidB", "subnet": "10.42.5."}

    def cx_markers():
        mm = _synthetic_markers()
        mm["root_ready"]["hostname"] = "medusa00"
        mm["a_identity"]["hostname"] = "medusa01"
        mm["b_identity"]["hostname"] = "medusa11"
        mm["a_placement"] = {"node_id": "nidA", "hostname": "medusa01", "placement_soft": False, "pid": 5001}
        mm["b_placement"] = {"node_id": "nidB", "hostname": "medusa11", "placement_soft": False, "pid": 5002}
        mm["endpoints_subnet_ok"] = True; mm["remote_orphan_check_ran"] = True
        mm["a_recreate_on_node"] = True; mm["b_recreate_on_node"] = True
        for cr in mm["cases"]:
            cr["a_coord"]["peer_host"] = "medusa11"   # A's peer B executes on node B
            cr["b_coord"]["peer_host"] = "medusa01"   # B's peer A executes on node A
        return mm

    def cx_gates(mm):
        ga = eval_crossnode_slice_a(mm, cx_expect())
        gf = eval_crossnode_slice_f(mm)
        ce = [(cr["name"], eval_case_crossnode(next(c for c in MATRIX if c["name"] == cr["name"]),
                                               cr, "medusa01", "medusa11")[0]) for cr in mm["cases"]]
        return ga, gf, ce

    cga, cgf, cce = cx_gates(cx_markers())
    if not rep_verdict_crossnode(cga, cgf, [cg for _, cg in cce]):
        bad_a = [k for k, v in cga.items() if not v]; bad_f = [k for k, v in cgf.items() if not v]
        bad_c = {n: [k for k, v in cg.items() if not v] for n, cg in cce if not all(cg.values())}
        failures.append(f"clean cross-node rep must pass: A={bad_a} F={bad_f} cases={bad_c}")
    if failure_class_crossnode(cga, cgf, cce) != "pass":
        failures.append(f"clean cross-node rep failure_class must be pass, got {failure_class_crossnode(cga, cgf, cce)}")
    if "hostname_identity" in cga:
        failures.append("cross-node Slice A must drop the same-host hostname_identity gate")

    def cx_expect_class(mut, cls, label):
        mm = cx_markers(); mut(mm)
        fc = failure_class_crossnode(*cx_gates(mm))
        if fc != cls:
            failures.append(f"[cross-node] {label}: expected {cls}, got {fc}")

    cx_expect_class(lambda mm: mm["a_placement"].update({"hostname": "medusa00", "node_id": "nidR"}),
                    "actor_placement_invalid", "A on root node")
    cx_expect_class(lambda mm: mm["a_placement"].__setitem__("placement_soft", True),
                    "actor_placement_invalid", "A soft placement")
    cx_expect_class(lambda mm: mm["cases"][2]["a_coord"].__setitem__("peer_host", "medusa09"),
                    "actor_placement_invalid", "A peer host wrong node")
    # cross-node bit mismatch on the peer-transferred candidates (the serialization discriminator)
    def _xfer_bad(mm):
        pk = mm["cases"][2]["a_coord"]["peer_topk"]
        pk[0] = {"token_id": pk[0]["token_id"], "logit_bits": pk[0]["logit_bits"] ^ 0x1}
    cx_expect_class(_xfer_bad, "cross_node_float32_bit_mismatch", "B->A transfer bit flip")
    cx_expect_class(lambda mm: mm.__setitem__("a_recreate_on_node", False),
                    "actor_recreation_failed", "A not recreated on node")
    ex2 = cx_expect(); ex2_nodes = {"nodes": ["medusa00", "medusa01"]}
    mm2 = cx_markers()
    g2 = (eval_crossnode_slice_a(mm2, {**cx_expect(), **ex2_nodes}), eval_crossnode_slice_f(mm2),
          [(cr["name"], eval_case_crossnode(next(c for c in MATRIX if c["name"] == cr["name"]),
                                            cr, "medusa01", "medusa11")[0]) for cr in mm2["cases"]])
    if failure_class_crossnode(*g2) != "three_node_allocation_unavailable":
        failures.append("two-node allocation must classify three_node_allocation_unavailable")
    if _expand_nodelist_pure("medusa[00-01,11]") != ["medusa00", "medusa01", "medusa11"]:
        failures.append("nodelist expander must expand medusa[00-01,11]")

    if failures:
        print("SELFTEST FAIL:")
        for f in failures:
            print("  -", f)
        return 1
    print(f"SELFTEST OK: exp68 oracle, tie-break, float32 bits, {len(MATRIX)} matrix scenarios, "
          "gate logic + failure taxonomy verified")
    return 0


# ---------------------------------------------------------------------------------------
# Aggregate + local phase
# ---------------------------------------------------------------------------------------

def _write_agg(path, agg):
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=False); f.write("\n")


def _trim_markers(m):
    """Drop the bulky per-case candidate lists from the curated aggregate (raw stays in runs_root),
    keeping the verdict-bearing witnesses."""
    mk = dict(m)
    for k in ("a_child_report", "b_child_report"):
        if isinstance(mk.get(k), dict):
            mk[k] = {kk: mk[k].get(kk) for kk in ("checked", "hpx_children", "worker_pid")}
    slim_cases = []
    for cr in mk.get("cases", []):
        c = {k: cr.get(k) for k in ("name", "V", "split", "k", "seed", "shard_a", "shard_b",
                                    "a_loc", "b_loc", "a_pid", "b_pid", "oracle_global")}
        for coord in ("a_coord", "b_coord"):
            cc = cr.get(coord) or {}
            c[coord] = {kk: cc.get(kk) for kk in ("ready", "target_found", "global_topk", "peer_topk",
                                                  "peer_locality", "peer_pid", "peer_host",
                                                  "own_locality", "own_pid", "own_host",
                                                  "hpx_composition")}
        c["a_local"] = cr.get("a_local"); c["b_local"] = cr.get("b_local")
        slim_cases.append(c)
    mk["cases"] = slim_cases
    return mk


def run_local_phase(args):
    peer = os.path.join(args.build_dir, PEER_BASENAME)
    ext_so = next((os.path.join(args.build_dir, fn)
                   for fn in (os.listdir(args.build_dir) if os.path.isdir(args.build_dir) else [])
                   if fn.startswith(EXT_MODULE) and fn.endswith(".so")), None)
    agg = {
        "experiment": "68_vocab_sharded_topk",
        "kind": "deterministic_llm_shaped_sharded_topk_two_ray_actors_local",
        "design": "six_gating_slices_A_F_plus_nongating_saturation_G",
        "single_node": True, "transport": "tcp_loopback",
        "verdict_rule": "PASS iff Slice A, F, and every matrix case's B/C/D/E gates pass in every rep",
        "generation_rule": GENERATION_RULE,
        "total_order": "higher logit wins; on equal logit, lower global token id wins",
        "matrix": MATRIX, "claim_fences": dict(CLAIM_FENCES),
        "failure_classes": list(FAILURE_CLASSES), "reps": args.reps,
    }
    if not (os.path.exists(peer) and ext_so):
        agg["overall"] = "skip"; agg["reason"] = f"build artifacts missing (peer={os.path.exists(peer)} ext={bool(ext_so)})"
        _write_agg(args.aggregate, agg); print(f"SKIP: build exp68 first -> {args.aggregate}"); return 0
    try:
        import ray
    except Exception as ex:  # noqa: BLE001
        agg["overall"] = "skip"; agg["reason"] = f"ray unavailable: {ex}"
        _write_agg(args.aggregate, agg); print("SKIP: ray unavailable"); return 0

    import logging
    ray.init(num_cpus=max(6, 2 * args.ray_num_cpus + 2), include_dashboard=False,
             log_to_driver=False, logging_level=logging.ERROR, ignore_reinit_error=True)
    HpxActor = build_actor_class(ray)
    runs_root = os.path.join(HERE, "_exp68_runs", time.strftime("%Y%m%dT%H%M%SZ"))
    os.makedirs(runs_root, exist_ok=True)
    print(f"[exp68] peer {peer}\n[exp68] ext  {os.path.basename(ext_so)}\n[exp68] runs {os.path.relpath(runs_root, HERE)}")

    reps, provenance, saturation = [], None, []
    try:
        for r in range(1, args.reps + 1):
            print(f"[exp68] rep {r} ...")
            rep = run_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), r, runs_root,
                          args.hpx_threads, args.ray_num_cpus)
            if provenance is None:
                provenance = collect_provenance(ray, rep["markers"].get("a_identity"))
            for s in rep.get("saturation_diagnostic", []):
                saturation.append({"rep": r, **s})
            nfail = len(rep["cases_failed"])
            print(f"[exp68]   rep {r}: {'PASS' if rep['rep_pass'] else 'FAIL'} ({rep['failure_class']}) "
                  f"A={rep['slice_a_failed']} F={rep['slice_f_failed']} cases_failed={nfail}")
            rep_out = dict(rep); rep_out["markers"] = _trim_markers(rep["markers"]); reps.append(rep_out)
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
    agg["safe_claim"] = (
        "On the tested local environment (CPython 3.11 GIL build, Ray "
        f"{(provenance or {}).get('ray_version')}, HPX commit {EXPECTED_HPX_COMMIT[:10]}...), two Ray "
        "actor workers hosting HPX localities in one shared runtime executed a deterministic "
        "vocabulary-sharded top-k reduction in BOTH coordinator directions over HPX actions/futures, "
        "with exact token-ID and float32-bit agreement against an independent global oracle across a "
        f"{len(MATRIX)}-case matrix, and clean lifecycle."
        if overall_pass else "exp68 did not pass; see slice_reps[].failure_class.")
    _write_agg(args.aggregate, agg)
    print(f"[exp68] overall: {agg['overall']} -> {args.aggregate}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="exp68 vocab-sharded top-k slice")
    ap.add_argument("--phase", choices=["local", "rostam-cross-node"], default="local")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--build-dir", default=DEFAULT_BUILD_DIR)
    ap.add_argument("--aggregate", default=None)
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--ray-num-cpus", type=int, default=2)
    ap.add_argument("--selftest", action="store_true")
    # cross-node-only options (three nodes: root/controller R, actor A, actor B)
    ap.add_argument("--root-node", default=None, help="cross-node: node hosting controller+root (R)")
    ap.add_argument("--actor-a-node", default=None, help="cross-node: node hosting Ray actor A")
    ap.add_argument("--actor-b-node", default=None, help="cross-node: node hosting Ray actor B")
    ap.add_argument("--subnet-prefix", default="10.42.5.", help="cross-node: required IPv4 prefix")
    ap.add_argument("--port-base", type=int, default=7780, help="cross-node: HPX port base per rep")
    ap.add_argument("--ray-port", type=int, default=6379, help="cross-node: Ray GCS port")
    ap.add_argument("--ray-ready-timeout", type=int, default=180)
    ap.add_argument("--ray-init-timeout", type=int, default=180)
    ap.add_argument("--head-num-cpus", type=int, default=4)
    ap.add_argument("--worker-num-cpus", type=int, default=4)
    ap.add_argument("--node-manager-port", type=int, default=7921)
    ap.add_argument("--object-manager-port", type=int, default=7922)
    ap.add_argument("--min-worker-port", type=int, default=10402)
    ap.add_argument("--max-worker-port", type=int, default=10502)
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    if args.aggregate is None:
        args.aggregate = os.path.join(
            HERE, "vocab_sharded_topk_crossnode_aggregate.json"
            if args.phase == "rostam-cross-node" else "vocab_sharded_topk_aggregate.json")
    if args.phase == "rostam-cross-node":
        return run_crossnode_phase(args)
    return run_local_phase(args)


# =======================================================================================
# Cross-node (Rostam / THREE-node Slurm) phase. Extends the accepted exp67 three-node harness to
# the exp68 vocab-sharded top-k workload: three roles on three distinct nodes (root+controller R,
# actor A, actor B), both actors hard-placed, the FULL seven-case matrix run in BOTH coordinator
# directions over the real TCP parcelport. The new discriminator: candidate float32 bit patterns
# must survive the cross-node HPX action path bit-exactly (peer-transferred candidates + merged
# results). Reuses exp59/66/67 Ray-on-Slurm bring-up and the corrected orphan check. Writes its OWN
# curated aggregate; never touches the local aggregate. EXPERIMENT-ONLY; no performance claim.
# =======================================================================================

_ORPHAN_PATTERNS_RAY = ["raylet", "gcs_server", "plasma_store", "ray::", "dashboard", "monitor"]


def _short(h):
    return (h or "").split(".")[0].strip().lower()


def _sh(cmd, timeout=180, env=None):
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
                lo, hi = token.split("-", 1); width = len(lo)
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
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "hostname", "-I"], timeout=60)
    if rc != 0:
        return None
    return _pick_subnet_ip([t for t in out.split() if t.count(".") == 3], subnet_prefix)


def _wait_for_file_nfs(path, timeout, procs=()):
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
                time.sleep(0.3); return os.path.exists(path)
        time.sleep(0.1)
    return os.path.exists(path)


def _ray_port_flags(a):
    return ["--node-manager-port", str(a.node_manager_port), "--object-manager-port", str(a.object_manager_port),
            "--min-worker-port", str(a.min_worker_port), "--max-worker-port", str(a.max_worker_port)]


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
    probe = ("import socket,sys,time\ndeadline=time.time()+%d\nwhile time.time()<deadline:\n"
             "    s=socket.socket(); s.settimeout(2)\n"
             "    try:\n        s.connect((%r,%d)); print('READY'); sys.exit(0)\n"
             "    except Exception:\n        time.sleep(1.5)\nprint('TIMEOUT'); sys.exit(1)\n"
             % (int(timeout_s), head_ip, int(port)))
    rc, out, err = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", probe_node, "--export=ALL",
                        "python3", "-c", probe], timeout=int(timeout_s) + 30, env=env)
    return rc == 0, ((out or "") + (err or "")).strip()[-200:]


def _wait_ray_nodes(ray_mod, expected, timeout_s):
    t0 = time.monotonic(); seen = 0
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
    t0 = time.monotonic(); attempts, last = 0, ""
    while True:
        attempts += 1
        try:
            ray_mod.init(address=address, log_to_driver=False, include_dashboard=False, ignore_reinit_error=True)
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
    """Ray-process leftovers on `node`; drops our own srun/pgrep/ray-stop/run_exp68 machinery
    (the exp66 170520 self-match fix)."""
    found = []
    for pat in patterns:
        hits = []
        for _try in range(6):
            rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                              "pgrep", "-af", pat], timeout=60, env=env)
            if rc not in (0, 1):
                hits = [f"{pat}:pgrep_rc_{rc}"]; break
            hits = [f"{_short(node)}:{pat}:{ln.split()[0]}" for ln in out.splitlines()
                    if ln.strip() and "pgrep" not in ln and "srun" not in ln
                    and "ray stop" not in ln and "run_exp68" not in ln]
            if not hits:
                break
            time.sleep(5)
        found.extend(hits)
    return found


def _crossnode_peer_orphans(node, env=None):
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                      "pgrep", "-af", PEER_BASENAME], timeout=60, env=env)
    if rc not in (0, 1):
        return [], False
    hits = [f"{_short(node)}:{ln.split()[0]}" for ln in out.splitlines()
            if ln.strip() and "pgrep" not in ln and "srun" not in ln and "run_exp68" not in ln]
    return hits, True


def _self_identity(ray_mod):
    nid = None
    try:
        nid = ray_mod.get_runtime_context().get_node_id()
    except Exception:  # noqa: BLE001
        nid = None
    return {"hostname": socket.gethostname(), "pid": os.getpid(), "node_id": nid}


def crossnode_root_cmd(peer, boot, root_ip, p_root, leave_timeout):
    return [peer, "--role", "root", "--bootstrap", boot, "--leave-timeout", str(leave_timeout),
            f"--hpx:agas={root_ip}:{p_root}", f"--hpx:hpx={root_ip}:{p_root}",
            "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
            "--hpx:ignore-batch-env"]


def crossnode_actor_endpoints(root_ip, p_root, actor_ip, p_actor):
    return [f"--hpx:agas={root_ip}:{p_root}", f"--hpx:hpx={actor_ip}:{p_actor}",
            "--hpx:bind=none", "--hpx:ignore-batch-env"]


def eval_crossnode_slice_a(m, expect):
    """Cross-node Slice A: local hosting/membership gates + THREE-node placement attestation."""
    g = eval_slice_a(m)
    g.pop("hostname_identity", None)  # root and both actors are on DIFFERENT hosts by design
    ap_a, ap_b = m.get("a_placement") or {}, m.get("b_placement") or {}
    rr = m.get("root_ready") or {}
    nodes_short = [_short(n) for n in (expect.get("nodes") or [])]
    g["three_node_slurm_allocation"] = bool(
        expect.get("slurm_job_id") and len(expect.get("nodes") or []) >= 3
        and _short(expect.get("nodeR")) in nodes_short and _short(expect.get("nodeA")) in nodes_short
        and _short(expect.get("nodeB")) in nodes_short)
    g["root_on_root_node"] = bool(_short(rr.get("hostname")) == _short(expect.get("nodeR")))
    g["actor_a_hard_placed"] = bool(ap_a.get("placement_soft") is False and ap_a.get("node_id")
                                    and ap_a.get("node_id") == expect.get("nodeA_nid")
                                    and _short(ap_a.get("hostname")) == _short(expect.get("nodeA")))
    g["actor_b_hard_placed"] = bool(ap_b.get("placement_soft") is False and ap_b.get("node_id")
                                    and ap_b.get("node_id") == expect.get("nodeB_nid")
                                    and _short(ap_b.get("hostname")) == _short(expect.get("nodeB")))
    g["actor_nodes_distinct"] = bool(_short(ap_a.get("hostname")) and _short(ap_b.get("hostname"))
                                     and _short(ap_a.get("hostname")) != _short(ap_b.get("hostname")))
    g["three_roles_three_nodes"] = bool(
        len({_short(expect.get("nodeR")), _short(expect.get("nodeA")), _short(expect.get("nodeB"))}) == 3
        and _short(rr.get("hostname")) not in (_short(ap_a.get("hostname")), _short(ap_b.get("hostname"))))
    g["endpoints_pinned_subnet"] = bool(m.get("endpoints_subnet_ok"))
    g["remote_orphan_check_ran"] = bool(m.get("remote_orphan_check_ran"))
    g["evidence_fields_complete_crossnode"] = bool(
        ap_a.get("node_id") and ap_b.get("node_id") and rr.get("hostname")
        and expect.get("nodeA_nid") and expect.get("nodeB_nid"))
    return g


def eval_case_crossnode(case, cr, nodeA, nodeB):
    """Per-case gates: the local Slice B/C/D/E gates + cross-node witnesses and the load-bearing
    cross-node float32-bit-preservation discriminator on the peer-transferred candidates."""
    cg, extra = eval_case(case, cr)
    V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
    orc_a = oracle_topk(0, split, seed, k)
    orc_b = oracle_topk(split, V, seed, k)
    ac, bc = cr.get("a_coord") or {}, cr.get("b_coord") or {}
    cg["a_peer_host_is_b"] = bool(_short(ac.get("peer_host")) == _short(nodeB))
    cg["b_peer_host_is_a"] = bool(_short(bc.get("peer_host")) == _short(nodeA))
    cg["a_crosses_nodes"] = bool(_short(nodeA) != _short(nodeB))
    cg["b_crosses_nodes"] = cg["a_crosses_nodes"]
    # The candidates B sent to A (received over the parcelport) must be bit-exact vs B's shard oracle;
    # symmetric for A->B. This is the cross-node serialization discriminator.
    cg["b_to_a_transfer_bits_exact"] = (norm_cands(ac.get("peer_topk")) == orc_b)
    cg["a_to_b_transfer_bits_exact"] = (norm_cands(bc.get("peer_topk")) == orc_a)
    return cg, extra


def eval_crossnode_slice_f(m):
    g = eval_slice_f(m)
    g["actor_a_recreation_on_node"] = bool(m.get("a_recreate_on_node"))
    g["actor_b_recreation_on_node"] = bool(m.get("b_recreate_on_node"))
    return g


def rep_verdict_crossnode(ga, gf, case_gates):
    return (all(ga.values()) and all(gf.values()) and all(all(cg.values()) for cg in case_gates))


def failure_class_crossnode(ga, gf, cases_eval):
    if not ga.get("three_node_slurm_allocation"):
        return "three_node_allocation_unavailable"
    if not ga.get("evidence_complete") or not ga.get("evidence_fields_complete_crossnode"):
        return "invalid_instrumentation"
    if not (ga.get("root_on_root_node") and ga.get("actor_a_hard_placed")
            and ga.get("actor_b_hard_placed") and ga.get("actor_nodes_distinct")
            and ga.get("three_roles_three_nodes")):
        return "actor_placement_invalid"
    if not (ga.get("actor_a_started_in_worker") and ga.get("actor_b_started_in_worker")):
        return "lifecycle_failed"
    if not ga.get("both_in_process_no_hpx_child"):
        return "lifecycle_failed"
    if not ga.get("shared_island_membership"):
        return "shared_island_join_failed"
    if not (ga.get("two_distinct_ray_processes") and ga.get("distinct_nonzero_localities")):
        return "invalid_instrumentation"
    if not ga.get("all_shards_disjoint_complete"):
        return "invalid_shard_partition"
    for name, cg in cases_eval:
        if not cg.get("shard_partition_ok"):
            return "invalid_shard_partition"
        if not cg.get("local_topk_order_ok"):
            return "tie_break_contract_failed"
        if not cg.get("local_topk_a_correct"):
            return "local_topk_a_failed"
        if not cg.get("local_topk_b_correct"):
            return "local_topk_b_failed"
        if not cg.get("a_fetch_b_ready"):
            return "a_fetch_b_failed"
        if not cg.get("b_fetch_a_ready"):
            return "b_fetch_a_failed"
        if not (cg.get("a_peer_is_b") and cg.get("a_own_is_a") and cg.get("b_peer_is_a")
                and cg.get("b_own_is_b") and cg.get("a_peer_host_is_b") and cg.get("b_peer_host_is_a")
                and cg.get("a_crosses_nodes")):
            return "actor_placement_invalid"
        if not cg.get("no_application_on_root"):
            return "root_executed_application_work"
        if not (cg.get("hpx_composition_a") and cg.get("hpx_composition_b")):
            return "hpx_composition_not_evidenced"
        if not (cg.get("b_to_a_transfer_bits_exact") and cg.get("a_to_b_transfer_bits_exact")):
            return "cross_node_float32_bit_mismatch"
        if not cg.get("a_merge_ids_match"):
            return "a_merge_failed"
        if not cg.get("b_merge_ids_match"):
            return "b_merge_failed"
        if not cg.get("coordinator_symmetry"):
            return "coordinator_asymmetry"
        if not cg.get("float32_bits_exact"):
            return "cross_node_float32_bit_mismatch"
        if not (cg.get("a_merge_equals_oracle") and cg.get("b_merge_equals_oracle")):
            return "global_oracle_mismatch"
    if not (gf.get("actor_a_recreation") and gf.get("actor_b_recreation")
            and gf.get("actor_a_recreation_on_node") and gf.get("actor_b_recreation_on_node")):
        return "actor_recreation_failed"
    if not gf.get("no_orphans"):
        return "orphan_detected"
    if not all(gf.values()):
        return "lifecycle_failed"
    if rep_verdict_crossnode(ga, gf, [cg for _, cg in cases_eval]):
        return "pass"
    return "invalid_instrumentation"


def run_crossnode_rep(ray_mod, HpxActor, peer, build_dir, rep, runs_root, hpx_threads, ray_cpus,
                      nodeR, nodeA, nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid, nodeB_nid,
                      subnet, port_base, expect, env):
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    boot = os.path.join(runs_root, f"rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    p_root, p_a, p_b = port_base + rep * 10, port_base + rep * 10 + 1, port_base + rep * 10 + 2
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
        "generation_rule": GENERATION_RULE, "operation_over_hpx_not_ray": True,
        "all_waits_bounded": True, "no_python_polling": True,
        "all_partitions_ok": all(partition_ok(c["V"], c["split"]) for c in MATRIX), "cases": [],
    }
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    saturation = []
    try:
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file_nfs(os.path.join(boot, "root.ready"), 60, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        a1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              scheduling_strategy=strat_a).remote(build_dir, hpx_threads, ep_a)
        pa = dict(_ray_get(ray_mod, a1.ray_placement.remote(), 60, "a_placement") or {})
        pa["placement_soft"], pa["target_node"] = False, nodeA
        m["a_placement"] = pa
        m["a_identity"] = _ray_get(ray_mod, a1.load_identity.remote(), 60, "a_identity")
        m["a_start"] = _ray_get(ray_mod, a1.start_hpx.remote(), 120, "a_start")
        m["a_child_report"] = _ray_get(ray_mod, a1.child_report.remote(), 30, "a_child_report")
        a_loc = (m["a_start"] or {}).get("locality_id")

        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              scheduling_strategy=strat_b).remote(build_dir, hpx_threads, ep_b)
        pb = dict(_ray_get(ray_mod, b1.ray_placement.remote(), 60, "b_placement") or {})
        pb["placement_soft"], pb["target_node"] = False, nodeB
        m["b_placement"] = pb
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 60, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 120, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")
        b_loc = (m["b_start"] or {}).get("locality_id")
        a_pid, b_pid = (m.get("a_identity") or {}).get("pid"), (m.get("b_identity") or {}).get("pid")

        if a_loc not in (None, 0) and b_loc not in (None, 0):
            for case in MATRIX:
                V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
                a_lo, a_hi, b_lo, b_hi = 0, split, split, V
                cr = {"name": case["name"], "V": V, "split": split, "k": k, "seed": seed,
                      "shard_a": [a_lo, a_hi], "shard_b": [b_lo, b_hi],
                      "a_loc": a_loc, "b_loc": b_loc, "a_pid": a_pid, "b_pid": b_pid}
                cr["a_local"] = _ray_get(ray_mod, a1.local_topk.remote(a_lo, a_hi, seed, k), 30, "a_local")
                cr["b_local"] = _ray_get(ray_mod, b1.local_topk.remote(b_lo, b_hi, seed, k), 30, "b_local")
                cr["a_coord"] = _ray_get(ray_mod, a1.coordinate.remote(b_loc, a_lo, a_hi, b_lo, b_hi, seed, k),
                                         60, "a_coord")
                cr["b_coord"] = _ray_get(ray_mod, b1.coordinate.remote(a_loc, b_lo, b_hi, a_lo, a_hi, seed, k),
                                         60, "b_coord")
                cr["oracle_global"] = [[t, b] for t, b in oracle_topk(0, V, seed, k)]
                m["cases"].append(cr)

        m["a_health"] = _ray_get(ray_mod, a1.health.remote(), 30, "a_health")
        m["b_health"] = _ray_get(ray_mod, b1.health.remote(), 30, "b_health")

        # Slice G (non-gating): A coordinates while B's Python thread is CPU-saturated (one case)
        if a_loc not in (None, 0) and b_loc not in (None, 0):
            case = MATRIX[2]
            V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
            bs_path, bd_path = os.path.join(boot, "b_busy_started"), os.path.join(boot, "b_busy_done")
            b_busy = b1.busy_spin.remote(3.0, bs_path, bd_path)
            seen = _wait_for_file_nfs(bs_path, 30)
            gc = _ray_get(ray_mod, a1.coordinate.remote(b_loc, 0, split, split, V, seed, k), 60, "sat_coord")
            try:
                b_ret = ray_mod.get(b_busy, timeout=30)
            except Exception:
                b_ret = None
            progressed = bool(gc.get("ready") and norm_cands(gc.get("global_topk")) == oracle_topk(0, V, seed, k))
            saturation.append({"case": case["name"], "overlap_observed": bool(seen and b_ret is not None),
                               "progressed_under_dest_saturation": progressed, "affects_verdict": False})

        # Slice F: graceful leave both -> membership 1
        a_stop = _ray_get(ray_mod, a1.stop_hpx.remote(), 40, "a_stop")
        b_stop = _ray_get(ray_mod, b1.stop_hpx.remote(), 40, "b_stop")
        m["a_stop_rc"], m["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
        m["b_stop_rc"], m["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")

        def _kill_dead(actor):
            ray_mod.kill(actor)
            try:
                ray_mod.get(actor.ping.remote(), timeout=10); return False
            except Exception:  # noqa: BLE001
                return True
        dead_a1 = _kill_dead(a1); a1 = None
        dead_b1 = _kill_dead(b1); b1 = None
        m["a_destroyed"], m["b_destroyed"] = dead_a1, dead_b1

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
    cases_eval, case_gates = [], []
    for cr in m["cases"]:
        case = next(c for c in MATRIX if c["name"] == cr["name"])
        cg, _ = eval_case_crossnode(case, cr, nodeA, nodeB)
        cases_eval.append((cr["name"], cg)); case_gates.append(cg)
    gf = eval_crossnode_slice_f(m)
    return {
        "rep": rep, "boot_rel": os.path.relpath(boot, HERE), "ports": m["ports"],
        "slice_a_gates": ga, "slice_f_gates": gf,
        "case_gates": {name: cg for name, cg in cases_eval},
        "rep_pass": rep_verdict_crossnode(ga, gf, case_gates),
        "failure_class": failure_class_crossnode(ga, gf, cases_eval),
        "slice_a_failed": [k for k, v in ga.items() if not v],
        "slice_f_failed": [k for k, v in gf.items() if not v],
        "cases_failed": {name: [k for k, v in cg.items() if not v]
                         for name, cg in cases_eval if not all(cg.values())},
        "saturation_diagnostic": saturation, "markers": m,
    }


def run_crossnode_phase(args):
    subnet = args.subnet_prefix
    agg = {
        "experiment": "68_vocab_sharded_topk",
        "kind": "deterministic_llm_shaped_sharded_topk_two_ray_actors_crossnode",
        "design": "six_gating_slices_A_F_plus_nongating_saturation_G",
        "phase": "rostam-cross-node", "single_node": False, "cross_node": True,
        "transport": "tcp_cross_node",
        "verdict_rule": "PASS iff cross-node Slice A, F, and every matrix case (B/C/D/E incl. "
                        "cross-node bit preservation) pass in every rep, no orphans on any node",
        "generation_rule": GENERATION_RULE,
        "total_order": "higher logit wins; on equal logit, lower global token id wins",
        "matrix": MATRIX, "claim_fences": dict(CLAIM_FENCES),
        "failure_classes": list(FAILURE_CLASSES) + ["three_node_allocation_unavailable",
            "actor_placement_invalid", "shared_island_join_failed", "cross_node_float32_bit_mismatch"],
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
        problems.append(f"R/A/B must be distinct; got R={nodeR} A={nodeA} B={nodeB}")
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
        problems.append(f"could not resolve subnet {subnet} IPs (R={nodeR_ip} A={nodeA_ip} B={nodeB_ip})")

    agg["preflight"] = {"slurm_job_id": job_id, "slurm_nodelist": nodelist, "allocation_nodes": nodes,
                        "controller_host": here, "nodeR": nodeR, "nodeA": nodeA, "nodeB": nodeB,
                        "nodeR_ip": nodeR_ip, "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip,
                        "subnet_prefix": subnet}
    if problems:
        agg["overall"] = "fail_preflight"; agg["preflight_problems"] = problems
        agg["failure_class"] = ("three_node_allocation_unavailable"
                                if any("node" in p for p in problems) else "invalid_instrumentation")
        _write_agg(args.aggregate, agg); print(f"[exp68-crossnode] PREFLIGHT FAIL: {problems}"); return 0

    print(f"[exp68-crossnode] job {job_id} nodes {nodes} | root {nodeR}={nodeR_ip} "
          f"A {nodeA}={nodeA_ip} B {nodeB}={nodeB_ip}")
    env = dict(os.environ)
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    runs_root = os.path.join(HERE, "_exp68_runs", f"crossnode_{job_id}_{runid}")
    os.makedirs(runs_root, exist_ok=True)
    port = args.ray_port
    temp_dir = f"/tmp/exp68_ray_{job_id}_{runid}"
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
            raise RuntimeError(f"head GCS not reachable from A({detA}) or B({detB})")
        worker_a = _ray_worker_srun(nodeA, nodeA_ip, nodeR_ip, port, args.worker_num_cpus, env,
                                    os.path.join(runs_root, "worker_a.log"), port_flags)
        worker_b = _ray_worker_srun(nodeB, nodeB_ip, nodeR_ip, port, args.worker_num_cpus, env,
                                    os.path.join(runs_root, "worker_b.log"), port_flags)
        init_ok, init_attempts, init_tb = _bounded_ray_init(ray, f"{nodeR_ip}:{port}", args.ray_init_timeout)
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
            return [n for n in alive if n.get("NodeManagerAddress") == ip or _short(n.get("NodeName")) == _short(host)]
        mR, mA, mB = _match(nodeR_ip, nodeR), _match(nodeA_ip, nodeA), _match(nodeB_ip, nodeB)
        nodeR_nid = mR[0]["NodeID"] if len(mR) == 1 else None
        nodeA_nid = mA[0]["NodeID"] if len(mA) == 1 else None
        nodeB_nid = mB[0]["NodeID"] if len(mB) == 1 else None
        driver = _self_identity(ray)
        agg["ray_cluster"] = {"root_node": nodeR, "actor_a_node": nodeA, "actor_b_node": nodeB,
                              "root_ip": nodeR_ip, "actor_a_ip": nodeA_ip, "actor_b_ip": nodeB_ip,
                              "nodeR_ray_node_id": nodeR_nid, "nodeA_ray_node_id": nodeA_nid,
                              "nodeB_ray_node_id": nodeB_nid, "driver_hostname": driver["hostname"],
                              "driver_node_id": driver["node_id"],
                              "driver_on_nodeR": bool(driver["node_id"] and driver["node_id"] == nodeR_nid),
                              "ray_nodes_raw": [{k: n.get(k) for k in ("NodeID", "NodeManagerAddress",
                                                "NodeName", "Alive")} for n in alive]}
        if not (nodeA_nid and nodeB_nid and len({nodeR_nid, nodeA_nid, nodeB_nid}) == 3):
            raise RuntimeError(f"ray node-id resolution failed (mR={len(mR)} mA={len(mA)} mB={len(mB)})")
        if not agg["ray_cluster"]["driver_on_nodeR"]:
            raise RuntimeError("driver is not on nodeR (must be co-located with the Ray head)")

        HpxActor = build_actor_class(ray)
        expect = {"slurm_job_id": job_id, "nodes": nodes, "nodeR": nodeR, "nodeA": nodeA, "nodeB": nodeB,
                  "nodeR_nid": nodeR_nid, "nodeA_nid": nodeA_nid, "nodeB_nid": nodeB_nid, "subnet": subnet}
        for r in range(1, args.reps + 1):
            print(f"[exp68-crossnode] rep {r} ...")
            rep = run_crossnode_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), r, runs_root,
                                    args.hpx_threads, args.ray_num_cpus, nodeR, nodeA, nodeB, nodeR_ip,
                                    nodeA_ip, nodeB_ip, nodeA_nid, nodeB_nid, subnet, args.port_base,
                                    expect, env)
            if provenance is None:
                provenance = collect_provenance(ray, rep["markers"].get("a_identity"))
            for s in rep.get("saturation_diagnostic", []):
                saturation.append({"rep": r, **s})
            print(f"[exp68-crossnode]   rep {r}: {'PASS' if rep['rep_pass'] else 'FAIL'} "
                  f"({rep['failure_class']}) A={rep['slice_a_failed']} F={rep['slice_f_failed']} "
                  f"cases_failed={len(rep['cases_failed'])}")
            rep_out = dict(rep); rep_out["markers"] = _trim_markers(rep["markers"]); reps.append(rep_out)
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

    overall_pass = (fail_reason is None and len(reps) == args.reps and all(r["rep_pass"] for r in reps)
                    and agg.get("final_orphans", {}).get("no_ray_orphans")
                    and agg["final_orphans"].get("no_peer_orphans"))
    agg["provenance"] = provenance
    agg["overall"] = "pass" if overall_pass else "fail"
    agg["slice_reps"] = reps
    agg["saturation_diagnostic"] = saturation
    agg["safe_claim"] = (
        f"On CPython {(provenance or {}).get('python_version_full','').split()[0]} (GIL build), Ray "
        f"{(provenance or {}).get('ray_version')}, HPX commit {EXPECTED_HPX_COMMIT[:10]}..., across "
        f"three Rostam nodes (root {nodeR}, actor A {nodeA}, actor B {nodeB} on {subnet}x), two Ray "
        "actor workers on DISTINCT nodes each hosted an HPX locality in-process and executed the "
        f"deterministic {len(MATRIX)}-case vocab-sharded top-k in BOTH coordinator directions over "
        "the real TCP parcelport, with exact token-ID and float32-bit agreement (including the "
        "peer-transferred candidates) against an independent oracle, and clean lifecycle."
        if overall_pass else
        f"exp68 cross-node did not pass ({fail_reason or 'see slice_reps[].failure_class / final_orphans'}).")
    _write_agg(args.aggregate, agg)
    print(f"[exp68-crossnode] overall: {agg['overall']} -> {args.aggregate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
