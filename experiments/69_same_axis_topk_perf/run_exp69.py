#!/usr/bin/env python3
"""exp69 -- same-axis Ray-mediated vs HPX-mediated distributed top-k composition. Local slices.

SAME-AXIS QD1 LATENCY PROBE over the accepted exp68 workload (SYNTHETIC LLM-shaped vocabulary-
sharded top-k -- NOT real inference). One shared topology per repetition: a separately supervised
WORK-FREE HPX root (locality 0), Ray actor A hosting HPX locality 1 in-process, Ray actor B hosting
HPX locality 2 in-process (the exp66/67/68 mechanism, unchanged). Both actors expose BOTH arms:

  ray_coordinate  -- own-shard native local_topk; peer candidates fetched over a NESTED SYNCHRONOUS
                     ray.get on the peer actor handle as (token_id, logit_bits) tuples; merge via
                     the SAME native merge_topk entered from Python (merge_topk_native).
  hpx_coordinate  -- own-shard native local_topk; peer candidates fetched over an HPX action;
                     merge via the SAME native merge_topk inside an HPX future CONTINUATION.

MEASURED CONTRAST (exact wording): Ray-mediated peer-candidate transport and Python boundary
handling versus HPX-mediated peer-candidate transport and native future/continuation composition,
with identical local top-k and merge algorithms. NOT a pure network-transport comparison; the Ray
arm intrinsically includes C++->Python conversion, Ray serialization/transport, Python
materialization at the coordinator, and conversion back into native merge input. HPX remains
RESIDENT in both actors during Ray-arm requests (strongest same-axis control): describe results as
"Ray-mediated and HPX-mediated paths inside the same Ray-hosted, HPX-resident actor topology".

THE ONLY TIMED BOUNDARY (both arms, one shared helper, controller clock only):
    t0 = time.perf_counter_ns(); result = ray.get(<arm>_coordinate.remote(...)); t1 = ...
Correctness verification happens strictly AFTER t1; any incorrect timed result invalidates the
COMPLETE repetition. Mechanism-witness counters are read ONLY outside timed windows (pair-boundary
snapshots + exact block deltas).

CLAIM FENCE: per-arm distributions ONLY. speedup_computed=false, ratio_reported=false,
arms_differenced=false, placement_bands_differenced=false in every artifact this driver writes.
No ratio, no speedup, no winner, no "relative latency". QD1 latency only; throughput is a separate
deferred slice. Not cross-node yet; not Python 3.14 / free-threaded; not production API.
Version policy: HPX 20bc3d4bf3068383edcb63be13f22e9ff95842fa (verified waiter-fix build), fail
closed before any timing on identity mismatch.

Usage:
  python run_exp69.py --selftest                 # pure gate/oracle/stats/witness checks (no Ray/HPX)
  python run_exp69.py --phase smoke              # Slice 0 instrument validation (W=5 K=30 R=1)
  python run_exp69.py --phase accepted           # Slice 1 QD1 latency (A: W=50 K=500; B-ctl: W=20 K=200; R=3)
"""

import argparse
import json
import os
import platform
import queue
import re
import signal
import socket
import struct
import subprocess
import sys
import sysconfig
import threading
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
PEER_BASENAME = "exp69_peer"
EXT_MODULE = "exp69_actor_ext"
EXPECTED_HPX_COMMIT = "20bc3d4bf3068383edcb63be13f22e9ff95842fa"
DEFAULT_BUILD_DIR = os.path.join(HERE, "build")
HPX_HOST = "localhost"  # exp66/67/68 finding: connect mode maps "localhost"->127.0.0.1, no resolver

TIMER_OVERHEAD_P50_FACTOR = 1000  # approved: flag case if per-rep p50 < 1000x median clock overhead
P99_MIN_N = 100                   # p99 annotated unreliable below this n (non-gating in smoke)

RATIO_FENCES = {
    "speedup_computed": False, "ratio_reported": False,
    "arms_differenced": False, "placement_bands_differenced": False,
}

CLAIM_FENCES = {
    "not_real_llm_inference": True, "no_model_weights_tokenizer_gpu": True,
    "not_cross_node": True, "not_python_3_14": True, "not_free_threaded": True,
    "no_elasticity_churn_or_recovery": True, "not_production_api": True,
    "no_ratio_speedup_winner_claim": True, "per_arm_distributions_only": True,
    "qd1_latency_only_no_throughput": True, "timing_not_correctness_oracle": True,
    "ray_arm_not_pure_standalone_ray": True, "b_control_reported_separately_never_pooled": True,
}

FAILURE_CLASSES = [
    "hpx_identity_mismatch", "actor_placement_invalid", "resource_mismatch_between_arms",
    "same_axis_boundary_mismatch", "outer_ray_trigger_mismatch", "native_compute_mismatch",
    "native_merge_mismatch", "payload_contract_mismatch", "warmup_incomplete",
    "timer_overhead_too_high", "clock_nonmonotonic", "cross_node_clock_subtraction_detected",
    "arm_order_violation", "ray_peer_witness_missing", "ray_peer_used_by_hpx_arm",
    "hpx_action_witness_missing", "hpx_action_used_by_ray_arm",
    "membership_changed_during_measurement", "actor_recreated_during_measurement",
    "locality_changed_during_measurement", "oracle_mismatch_in_timed_sample",
    "float32_bit_mismatch", "insufficient_valid_samples", "insufficient_samples_for_p99",
    "timeout", "lifecycle_failed", "actor_recreation_failed", "orphan_detected",
    "phase_mixing", "invalid_instrumentation", "pass",
]

# Classification priority: first flagged class wins (structural/setup defects outrank sample-level).
CLASS_PRIORITY = [
    "hpx_identity_mismatch", "invalid_instrumentation", "actor_placement_invalid",
    "resource_mismatch_between_arms", "same_axis_boundary_mismatch", "outer_ray_trigger_mismatch",
    "native_compute_mismatch", "native_merge_mismatch", "payload_contract_mismatch",
    "cross_node_clock_subtraction_detected", "clock_nonmonotonic", "warmup_incomplete",
    "arm_order_violation", "ray_peer_used_by_hpx_arm", "hpx_action_used_by_ray_arm",
    "ray_peer_witness_missing", "hpx_action_witness_missing", "float32_bit_mismatch",
    "oracle_mismatch_in_timed_sample", "timeout", "membership_changed_during_measurement",
    "actor_recreated_during_measurement", "locality_changed_during_measurement",
    "timer_overhead_too_high", "insufficient_valid_samples", "insufficient_samples_for_p99",
    "actor_recreation_failed", "orphan_detected", "lifecycle_failed", "phase_mixing",
]

# Untimed correctness pre-gate: the SEVEN accepted exp68 cases, parameters/semantics UNCHANGED.
CORRECTNESS_MATRIX = [
    {"name": "tiny_k1",      "V": 8,   "split": 4,   "k": 1,  "seed": 7,  "tags": ["small", "k1", "k_lt_shard"]},
    {"name": "tiny_k3",      "V": 8,   "split": 4,   "k": 3,  "seed": 7,  "tags": ["small", "kgt1", "k_lt_shard"]},
    {"name": "cross_both",   "V": 64,  "split": 32,  "k": 6,  "seed": 1,  "tags": ["both_shards", "kgt1"]},
    {"name": "tie_cutoff",   "V": 200, "split": 100, "k": 5,  "seed": 1,  "tags": ["tie_at_cutoff"]},
    {"name": "shardA_dom",   "V": 128, "split": 64,  "k": 8,  "seed": 1,  "tags": ["one_shard_dominant"]},
    {"name": "both_contrib", "V": 100, "split": 50,  "k": 10, "seed": 1,  "tags": ["both_shards"]},
    {"name": "k1_large",     "V": 256, "split": 128, "k": 1,  "seed": 13, "tags": ["k1"]},
]

# Timed performance matrix (approved; compact, NOT a sweep -- do not silently broaden).
TIMED_MATRIX = [
    {"name": "P0",  "V": 64,        "split": 32,        "k": 1,      "seed": 1, "purpose": "fixed_overhead_control"},
    {"name": "P1",  "V": 2_000_000, "split": 1_000_000, "k": 10,     "seed": 1, "purpose": "local_compute_dominated"},
    {"name": "P2",  "V": 200_000,   "split": 100_000,   "k": 10_000, "seed": 1, "purpose": "larger_peer_payload"},
    {"name": "P3a", "V": 100_000,   "split": 50_000,    "k": 10,     "seed": 1, "purpose": "balanced_small_list"},
    {"name": "P3b", "V": 100_000,   "split": 50_000,    "k": 100,    "seed": 1, "purpose": "balanced_medium_list"},
    {"name": "P3c", "V": 100_000,   "split": 50_000,    "k": 1_000,  "seed": 1, "purpose": "balanced_larger_list"},
]
B_CONTROL_CASES = ["P0", "P2", "P3b"]  # approved limited directional control

SAMPLING = {
    "smoke":    {"A": {"W": 5,  "K": 30},  "B": None,                 "reps": 1},
    "accepted": {"A": {"W": 50, "K": 500}, "B": {"W": 20, "K": 200},  "reps": 3},
}

GENERATION_RULE = {
    "name": "int_grid_over_8",
    "formula": "h=(uint32(token_id)*2654435761 + uint32(seed)*40503) mod 2^32; "
               "grid=(h%131)-65; logit=float32(grid)/8.0",
    "grid_range": [-65, 65], "denominator": 8, "float32_exact": True,
    "total_order": "higher logit; tie -> lower global token id",
    "value_range": [-8.125, 8.125],
}

TOTAL_ORDER = "higher logit wins; on equal logit, lower global token id wins"
MEASURED_CONTRAST = ("Ray-mediated peer-candidate transport and Python boundary handling versus "
                     "HPX-mediated peer-candidate transport and native future/continuation "
                     "composition, with identical local top-k and merge algorithms")
SHARED_RUNTIME_CAVEAT = ("Ray-mediated and HPX-mediated paths inside the same Ray-hosted, "
                         "HPX-resident actor topology; the Ray arm is NOT a pure standalone-Ray "
                         "deployment")
TIMED_BOUNDARY = ("controller-side time.perf_counter_ns immediately around ONE "
                  "ray.get(coordinator.<arm>_coordinate.remote(...)); verification strictly after t1; "
                  "no cross-node/cross-process timestamp arithmetic")


# ---------------------------------------------------------------------------------------
# Deterministic oracle (stdlib struct; mirrors shared_topk.hpp bit-for-bit; exp68 verbatim)
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


def norm_pairs(x):
    """Normalize a (token_id, logit_bits) pair list (tuples or lists) to [(int, int)]."""
    return [(int(p[0]), int(p[1])) for p in (x or [])]


def is_sorted_total_order(cands):
    for i in range(len(cands) - 1):
        (t0, b0), (t1, b1) = cands[i], cands[i + 1]
        l0, l1 = f32(b0), f32(b1)
        if l0 < l1:
            return False
        if l0 == l1 and t0 >= t1:
            return False
    return True


def partition_ok(V, split):
    return bool(0 < split < V)


_ORACLE_CACHE = {}


def case_oracle(case):
    """Per-case oracle triple {global, a, b} as pair lists; deterministic, cached across reps."""
    key = (case["name"], case["V"], case["split"], case["k"], case["seed"])
    if key not in _ORACLE_CACHE:
        V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
        _ORACLE_CACHE[key] = {"global": oracle_topk(0, V, seed, k),
                              "a": oracle_topk(0, split, seed, k),
                              "b": oracle_topk(split, V, seed, k)}
    return _ORACLE_CACHE[key]


def shard_args(case, direction):
    """(own_lo, own_hi, peer_lo, peer_hi, seed, k) for the coordinator of `direction`."""
    V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
    if direction == "A":
        return (0, split, split, V, seed, k)
    return (split, V, 0, split, seed, k)


# ---------------------------------------------------------------------------------------
# Timer calibration + statistics (pure -> selftestable)
# ---------------------------------------------------------------------------------------

def percentile(sorted_vals, p):
    """Linear-interpolation percentile over an ascending-sorted list."""
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    idx = (p / 100.0) * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def median(vals):
    return percentile(sorted(vals), 50) if vals else None


def stats_block(elapsed_ns, requested_k, phase):
    """Per (arm x case x direction x repetition) statistics; diagnostics labeled as such."""
    xs = sorted(elapsed_ns)
    n = len(xs)
    p50 = percentile(xs, 50)
    mad = percentile(sorted(abs(x - p50) for x in xs), 50) if xs else None
    p25, p75 = percentile(xs, 25), percentile(xs, 75)
    return {
        "n": n, "requested_k": requested_k,
        "p50_ns": p50, "p90_ns": percentile(xs, 90),
        "p99_ns": percentile(xs, 99), "p99_scope": f"n={n}",
        "p99_reliable": bool(n >= P99_MIN_N),
        "iqr_ns": (p75 - p25) if n else None, "mad_ns": mad,
        "min_ns_diagnostic": xs[0] if xs else None,
        "max_ns_diagnostic": xs[-1] if xs else None,
        "phase": phase,
    }


def calibrate_timer(n_reads=10000):
    """>=10k adjacent perf_counter_ns reads; overhead distribution (includes loop cost -- honest)."""
    pc = time.perf_counter_ns
    reads = [pc() for _ in range(n_reads)]
    deltas = sorted(reads[i + 1] - reads[i] for i in range(len(reads) - 1))
    info = time.get_clock_info("perf_counter")
    return {
        "clock": "time.perf_counter_ns", "n_reads": n_reads,
        "reported_resolution_s": info.resolution, "clock_monotonic_declared": info.monotonic,
        "median_overhead_ns": percentile(deltas, 50), "p90_overhead_ns": percentile(deltas, 90),
        "max_overhead_ns_diagnostic": deltas[-1] if deltas else None,
        "nonnegative_deltas": bool(deltas and deltas[0] >= 0),
    }


# ---------------------------------------------------------------------------------------
# Arm scheduler + mechanism-witness delta logic (pure -> selftestable)
# ---------------------------------------------------------------------------------------

def pair_order(i):
    """Approved paired alternating order: even pair Ray->HPX, odd pair HPX->Ray."""
    return ("ray", "hpx") if i % 2 == 0 else ("hpx", "ray")


def order_violations(order_log):
    """Re-derive the expected arm from (pair, pos) and list every mismatch in a recorded log."""
    return [(pair, pos, arm) for (pair, pos, arm) in order_log if pair_order(pair)[pos] != arm]


def pair_witness_check(prev, cur, coord_key, peer_key):
    """Exact per-pair counter deltas (one Ray-arm + one HPX-arm request per pair).

    Expected: coordinator outgoing_ray_fetch +1, peer serving_ray_fetch +1,
    peer hpx_actions_served +1, everything else +0. Counters are monotonic and read only
    OUTSIDE timed windows.
    """
    def d(who, key):
        return int((cur.get(who) or {}).get(key) or 0) - int((prev.get(who) or {}).get(key) or 0)

    out_d = d(coord_key, "outgoing_ray_fetch")
    srv_d = d(peer_key, "serving_ray_fetch")
    act_d = d(peer_key, "hpx_actions_served")
    if srv_d > 1 or out_d > 1:
        return False, "ray_peer_used_by_hpx_arm"
    if act_d > 1:
        return False, "hpx_action_used_by_ray_arm"
    if srv_d < 1 or out_d < 1:
        return False, "ray_peer_witness_missing"
    if act_d < 1:
        return False, "hpx_action_witness_missing"
    if (d(coord_key, "serving_ray_fetch") != 0 or d(coord_key, "hpx_actions_served") != 0
            or d(peer_key, "outgoing_ray_fetch") != 0):
        return False, "invalid_instrumentation"
    return True, None


def block_delta_check(before, after, coord_key, peer_key, expected_n):
    """Exact block-level deltas: after `expected_n` pairs each counter moved by exactly n."""
    def d(who, key):
        return int((after.get(who) or {}).get(key) or 0) - int((before.get(who) or {}).get(key) or 0)
    return (d(coord_key, "outgoing_ray_fetch") == expected_n
            and d(peer_key, "serving_ray_fetch") == expected_n
            and d(peer_key, "hpx_actions_served") == expected_n
            and d(coord_key, "serving_ray_fetch") == 0
            and d(coord_key, "hpx_actions_served") == 0
            and d(peer_key, "outgoing_ray_fetch") == 0)


# ---------------------------------------------------------------------------------------
# Result verification (pure -> selftestable). ALWAYS called strictly after t1.
# ---------------------------------------------------------------------------------------

def verify_timed_result(arm, res, oracle_global, expect_peer, expect_own):
    """Bit-exact + witness verification of one coordinate() result. Returns (ok, reasons)."""
    if not isinstance(res, dict) or res.get("error"):
        err = res.get("error") if isinstance(res, dict) else type(res).__name__
        return False, [f"call_error:{err}"]
    reasons = []
    got = norm_pairs(res.get("global_topk"))
    if got != oracle_global:
        if [t for t, _ in got] == [t for t, _ in oracle_global]:
            reasons.append("float32_bit_mismatch")
        else:
            reasons.append("oracle_mismatch")
    if res.get("arm") != arm:
        reasons.append("arm_label_mismatch")
    if res.get("peer_pid") != expect_peer.get("pid"):
        reasons.append("peer_pid_mismatch")
    if res.get("peer_host") != expect_peer.get("host"):
        reasons.append("peer_host_mismatch")
    if res.get("own_pid") != expect_own.get("pid"):
        reasons.append("own_pid_mismatch")
    if arm == "hpx":
        if not (res.get("ready") and res.get("target_found")):
            reasons.append("hpx_not_ready")
        comp = res.get("hpx_composition") or {}
        if not (comp.get("action_dispatched") and comp.get("future_ready")
                and comp.get("continuation_executed") and comp.get("result_delivered")):
            reasons.append("hpx_composition_witness_missing")
        if res.get("peer_locality") != expect_peer.get("locality"):
            reasons.append("peer_locality_mismatch")
        if res.get("own_locality") != expect_own.get("locality"):
            reasons.append("own_locality_mismatch")
    else:
        if "hpx_composition" in res:
            reasons.append("hpx_witness_in_ray_arm")
        if res.get("peer_node_id") != expect_peer.get("node_id"):
            reasons.append("peer_node_mismatch")
    return (not reasons), reasons


# ---------------------------------------------------------------------------------------
# Process / file helpers (bounded; exp67/68 idiom, verbatim)
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
        "gil_enabled_runtime": gil,
        "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "ray_version": getattr(ray_mod, "__version__", None),
        "ray_commit": getattr(ray_mod, "__commit__", None),
        "platform": platform.platform(), "machine": platform.machine(),
        "processor_count": os.cpu_count(),
        "expected_hpx_commit": EXPECTED_HPX_COMMIT,
    }
    if a_identity:
        prov["actor_ext_file"] = os.path.basename(a_identity.get("ext_file", "") or "")
        prov["actor_ext_abi_match"] = a_identity.get("abi_match")
        prov["hpx_complete_version"] = (a_identity.get("hpx_version_info") or {}).get("hpx_complete_version")
        prov["hpx_git_sha_observed"] = extract_git_sha(prov.get("hpx_complete_version"))
    return prov


# ---------------------------------------------------------------------------------------
# Ray actor: hosts an HPX connect-mode locality IN-PROCESS + exposes BOTH same-axis arms
# ---------------------------------------------------------------------------------------

def build_actor_class(ray_mod):
    @ray_mod.remote
    class HpxActor:
        def __init__(self, build_dir, hpx_threads, endpoints,
                     ext_module="exp69_actor_ext", peer_basename="exp69_peer"):
            import sys as _sys
            _sys.path.insert(0, build_dir)
            self._build_dir = build_dir
            self._threads = hpx_threads
            self._endpoints = endpoints
            self._ext_module = ext_module
            self._peer_basename = peer_basename
            self._ext = None
            self._peer = None
            self._peer_loc = None
            # Mechanism-witness counters (monotonic; read via counters(), never reset mid-rep).
            self._outgoing_ray_fetch = 0
            self._serving_ray_fetch = 0
            # Cached identity scalars so witness fields add no per-request lookup cost.
            import os as _os, socket as _socket
            self._pid = _os.getpid()
            self._host = _socket.gethostname()
            try:
                import ray as _ray
                self._ray = _ray
                self._node_id = _ray.get_runtime_context().get_node_id()
            except Exception:  # noqa: BLE001
                self._ray = None
                self._node_id = None
            self._loc = None
            # Slice 2 (throughput) thread-safety. Under QD1 (max_concurrency=1) these are
            # uncontended and inert; witness values/deltas are IDENTICAL to Slice 1.
            import threading as _threading
            self._counter_lock = _threading.Lock()
            self._active_lock = _threading.Lock()
            self._coord_active_cur = 0
            self._coord_active_max = 0
            self._raypeer_active_cur = 0
            self._raypeer_active_max = 0
            self._active_underflow = False
            self._peer_frozen = False

        def ray_placement(self):
            return {"node_id": self._node_id, "hostname": self._host, "pid": self._pid}

        def load_identity(self):
            import importlib, os as _os, sysconfig as _sc
            e = importlib.import_module(self._ext_module)
            self._ext = e
            suffix = _sc.get_config_var("EXT_SUFFIX")
            return {"pid": e.pid(), "os_getpid": _os.getpid(), "hostname": e.hostname(),
                    "node_id": self._node_id,
                    "hpx_version_info": dict(e.hpx_version_info()),
                    "gil_declaration": getattr(e, "__gil_declaration__", None),
                    "experiment": getattr(e, "__experiment__", None), "ext_file": e.__file__,
                    "abi_match": bool(e.__file__.endswith(suffix)) if suffix else None}

        def start_hpx(self):
            try:
                self._ext.start_connect(self._threads, list(self._endpoints))
                self._loc = int(self._ext.locality_id())
                return {"started": True, "locality_id": self._loc,
                        "membership": int(self._ext.membership_count()), "pid": self._ext.pid()}
            except Exception as ex:  # noqa: BLE001
                return {"started": False, "error": f"{type(ex).__name__}: {ex}"}

        def set_peer(self, peer_handle, peer_locality):
            """Mutual Ray handles ARE held (needed by the Ray arm); the HPX arm proves non-use
            through the serving/outgoing fetch counters staying flat. Slice 2: refused once frozen."""
            if self._peer_frozen:
                raise RuntimeError("peer configuration is frozen (no set_peer during warm-up "
                                   "or measurement)")
            self._peer = peer_handle
            self._peer_loc = int(peer_locality)
            return True

        def freeze_peer(self):
            """Slice 2: latch peer handle + locality immutable BEFORE any warm-up/measurement, so
            no set_peer or peer-locality write can occur during a throughput batch."""
            self._peer_frozen = True
            return {"peer_frozen": True, "peer_locality": self._peer_loc}

        def _active_enter(self, kind):
            with self._active_lock:
                if kind == "coord":
                    self._coord_active_cur += 1
                    if self._coord_active_cur > self._coord_active_max:
                        self._coord_active_max = self._coord_active_cur
                else:  # "ray_peer"
                    self._raypeer_active_cur += 1
                    if self._raypeer_active_cur > self._raypeer_active_max:
                        self._raypeer_active_max = self._raypeer_active_cur

        def _active_exit(self, kind):
            with self._active_lock:
                if kind == "coord":
                    self._coord_active_cur -= 1
                    if self._coord_active_cur < 0:
                        self._active_underflow = True
                else:
                    self._raypeer_active_cur -= 1
                    if self._raypeer_active_cur < 0:
                        self._active_underflow = True

        def reset_active(self):
            """Slice 2: reset active-call MAXES to the current in-flight (0 between batches). Call
            ONLY between batches / outside measured windows, never mid-repetition."""
            with self._active_lock:
                self._coord_active_max = self._coord_active_cur
                self._raypeer_active_max = self._raypeer_active_cur
                underflow = self._active_underflow
            try:
                if self._ext is not None:
                    self._ext.reset_hpx_action_active()
            except Exception:  # noqa: BLE001
                pass
            return {"ok": True, "active_underflow_seen": underflow}

        def active_witness(self):
            """Slice 2: snapshot of active-call witnesses (coordinator Python methods + Ray-peer
            Python service + native peer HPX action). Read BETWEEN batches / outside measurement."""
            with self._active_lock:
                cc, cm = self._coord_active_cur, self._coord_active_max
                rc, rm = self._raypeer_active_cur, self._raypeer_active_max
                uf = self._active_underflow
            hac = ham = None
            try:
                if self._ext is not None:
                    hac = int(self._ext.hpx_action_active_current())
                    ham = int(self._ext.hpx_action_active_max())
            except Exception:  # noqa: BLE001
                pass
            return {"coordinate_active_current": cc, "coordinate_active_max": cm,
                    "ray_peer_active_current": rc, "ray_peer_active_max": rm,
                    "hpx_action_active_current": hac, "hpx_action_active_max": ham,
                    "active_underflow_seen": uf}

        def child_report(self):
            import os as _os, subprocess as _sp
            pid = _os.getpid(); children, hpx_children, checked = [], [], False
            try:
                out = _sp.run(["pgrep", "-P", str(pid)], capture_output=True, text=True, timeout=10)
                checked = out.returncode in (0, 1)
                for cp in out.stdout.split():
                    if not cp:
                        continue
                    info = _sp.run(["ps", "-o", "command=", "-p", cp], capture_output=True,
                                   text=True, timeout=10)
                    cmd = info.stdout.strip(); children.append({"pid": int(cp), "cmd": cmd})
                    if self._peer_basename in cmd or self._ext_module in cmd or "hpx" in cmd.lower():
                        hpx_children.append({"pid": int(cp), "cmd": cmd})
            except Exception as ex:  # noqa: BLE001
                return {"checked": False, "error": f"{type(ex).__name__}: {ex}",
                        "children": children, "hpx_children": hpx_children}
            return {"checked": checked, "worker_pid": pid, "children": children,
                    "hpx_children": hpx_children}

        def counters(self):
            """Monotonic mechanism-witness counters. META method: touches no workload counter;
            called by the controller only OUTSIDE timed windows. Snapshot under the counter lock so
            the values are exact under max_concurrency>1; identical to Slice 1 under QD1."""
            with self._counter_lock:
                out_r, srv_r = self._outgoing_ray_fetch, self._serving_ray_fetch
            return {"outgoing_ray_fetch": out_r,
                    "serving_ray_fetch": srv_r,
                    "hpx_actions_served": int(self._ext.action_serve_count())}

        def local_topk_cands(self, lo, hi, seed, k):
            """Ray-arm peer-fetch endpoint: the SAME native local_topk, returned as ordered
            (token_id, logit_bits) tuples (approved transport contract) + identity witnesses."""
            self._active_enter("ray_peer")
            try:
                with self._counter_lock:
                    self._serving_ray_fetch += 1
                return {"cands": self._ext.local_topk_pairs(int(lo), int(hi), int(seed), int(k)),
                        "pid": self._pid, "host": self._host, "node_id": self._node_id}
            finally:
                self._active_exit("ray_peer")

        def ray_coordinate(self, own_lo, own_hi, peer_lo, peer_hi, seed, k, include_locals=False):
            """Ray arm: native own-shard local_topk -> NESTED SYNCHRONOUS ray.get peer fetch ->
            SAME native merge (merge_topk_native). No Python merge; no HPX call anywhere."""
            self._active_enter("coord")
            try:
                own = self._ext.local_topk_pairs(int(own_lo), int(own_hi), int(seed), int(k))
                with self._counter_lock:
                    self._outgoing_ray_fetch += 1
                reply = self._ray.get(
                    self._peer.local_topk_cands.remote(int(peer_lo), int(peer_hi), int(seed), int(k)),
                    timeout=30)
                merged = self._ext.merge_topk_native(own, reply["cands"], int(k))
                out = {"arm": "ray", "ready": True, "target_found": True, "global_topk": merged,
                       "peer_pid": reply["pid"], "peer_host": reply["host"],
                       "peer_node_id": reply["node_id"],
                       "own_pid": self._pid, "own_locality": self._loc, "own_host": self._host}
                if include_locals:
                    out["own_topk"] = own
                    out["peer_topk"] = reply["cands"]
                return out
            except Exception as ex:  # noqa: BLE001
                return {"arm": "ray", "ready": False, "error": f"{type(ex).__name__}: {ex}"}
            finally:
                self._active_exit("coord")

        def hpx_coordinate(self, own_lo, own_hi, peer_lo, peer_hi, seed, k, include_locals=False):
            """HPX arm: exp68's load-bearing coordinate() -- HPX action + future continuation merge.
            Never touches the peer Ray handle."""
            self._active_enter("coord")
            try:
                d = dict(self._ext.coordinate(self._peer_loc, int(own_lo), int(own_hi),
                                              int(peer_lo), int(peer_hi), int(seed), int(k),
                                              30, bool(include_locals)))
                d["arm"] = "hpx"
                return d
            except Exception as ex:  # noqa: BLE001
                return {"arm": "hpx", "ready": False, "target_found": False,
                        "error": f"{type(ex).__name__}: {ex}"}
            finally:
                self._active_exit("coord")

        # ---- Slice 3 DIAGNOSTIC-ONLY timed arms (Phase B) -------------------------------------
        # Same logical work, same witnesses, and same result fields as the accepted arms above
        # (verify_timed_result passes unchanged), PLUS per-stage durations. Used ONLY by the Slice 3
        # matrix; the accepted ray_coordinate / hpx_coordinate stay byte-identical. No ratio claim.
        def hpx_coordinate_timed(self, own_lo, own_hi, peer_lo, peer_hi, seed, k,
                                 include_locals=False):
            """HPX arm (timed): coordinate_diag_timed mirrors coordinate() exactly and returns the
            same result/witnesses PLUS coordinator-local stage_ns, worker identities, and peer-local
            durations (peer-local values are attribution-only, never subtracted)."""
            self._active_enter("coord")
            try:
                d = dict(self._ext.coordinate_diag_timed(self._peer_loc, int(own_lo), int(own_hi),
                                                         int(peer_lo), int(peer_hi), int(seed),
                                                         int(k), 30, bool(include_locals)))
                d["arm"] = "hpx"
                return d
            except Exception as ex:  # noqa: BLE001
                return {"arm": "hpx", "ready": False, "target_found": False,
                        "error": f"{type(ex).__name__}: {ex}"}
            finally:
                self._active_exit("coord")

        def ray_coordinate_timed(self, own_lo, own_hi, peer_lo, peer_hi, seed, k,
                                 include_locals=False):
            """Ray arm (timed): identical to ray_coordinate PLUS Python perf_counter_ns stage deltas
            (own local-top-k, nested peer-call CALLER-OBSERVED duration, native merge, total method).
            peer_call_ns is caller-observed and includes Ray queue/transport/peer service/return -- it
            is NOT pure transport time. Same counters/witnesses as ray_coordinate."""
            self._active_enter("coord")
            t_entry = time.perf_counter_ns()
            try:
                o0 = time.perf_counter_ns()
                own = self._ext.local_topk_pairs(int(own_lo), int(own_hi), int(seed), int(k))
                o1 = time.perf_counter_ns()
                with self._counter_lock:
                    self._outgoing_ray_fetch += 1
                p0 = time.perf_counter_ns()
                reply = self._ray.get(
                    self._peer.local_topk_cands.remote(int(peer_lo), int(peer_hi), int(seed), int(k)),
                    timeout=30)
                p1 = time.perf_counter_ns()
                merged = self._ext.merge_topk_native(own, reply["cands"], int(k))
                m1 = time.perf_counter_ns()
                out = {"arm": "ray", "ready": True, "target_found": True, "global_topk": merged,
                       "peer_pid": reply["pid"], "peer_host": reply["host"],
                       "peer_node_id": reply["node_id"],
                       "own_pid": self._pid, "own_locality": self._loc, "own_host": self._host,
                       "stage_ns": {"own_topk_ns": o1 - o0, "peer_call_ns": p1 - p0,
                                    "merge_ns": m1 - p1, "total_ns": m1 - t_entry}}
                if include_locals:
                    out["own_topk"] = own
                    out["peer_topk"] = reply["cands"]
                return out
            except Exception as ex:  # noqa: BLE001
                return {"arm": "ray", "ready": False, "error": f"{type(ex).__name__}: {ex}"}
            finally:
                self._active_exit("coord")

        def health(self):
            try:
                return {"ok": True, "pid": self._pid,
                        "membership": int(self._ext.membership_count()),
                        "locality_id": int(self._ext.locality_id())}
            except Exception as ex:  # noqa: BLE001
                return {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

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
            return info

    return HpxActor


# ---------------------------------------------------------------------------------------
# THE timed boundary: one shared helper for BOTH arms (identical outer Ray trigger)
# ---------------------------------------------------------------------------------------

def timed_sample(ray_mod, coordinator, arm, args, timeout_s=60):
    """t0/perf_counter_ns -> ONE ray.get -> t1. The ONLY place a timing is ever taken."""
    method = getattr(coordinator, arm + "_coordinate")
    t0 = time.perf_counter_ns()
    res = ray_mod.get(method.remote(*args), timeout=timeout_s)
    t1 = time.perf_counter_ns()
    return t0, t1, res


# ---------------------------------------------------------------------------------------
# Untimed correctness pre-gate: full exp68 seven-case matrix through BOTH arms, BOTH directions
# ---------------------------------------------------------------------------------------

def eval_pregate_case(case, r, oc, meta):
    """Gates for one pre-gate case. r = {a_hpx, a_ray, b_hpx, b_ray} coordinate results."""
    g = {}
    g["shard_partition_ok"] = partition_ok(case["V"], case["split"])
    og, oa, ob = oc["global"], oc["a"], oc["b"]
    ng = {kk: norm_pairs((r.get(kk) or {}).get("global_topk")) for kk in r}
    # Locals: identical native local_topk on both arms (native_compute_mismatch gate input).
    g["a_local_correct_both_arms"] = (norm_pairs((r.get("a_hpx") or {}).get("own_topk")) == oa
                                      and norm_pairs((r.get("a_ray") or {}).get("own_topk")) == oa)
    g["b_local_correct_both_arms"] = (norm_pairs((r.get("b_hpx") or {}).get("own_topk")) == ob
                                      and norm_pairs((r.get("b_ray") or {}).get("own_topk")) == ob)
    g["peer_lists_correct"] = (norm_pairs((r.get("a_hpx") or {}).get("peer_topk")) == ob
                               and norm_pairs((r.get("a_ray") or {}).get("peer_topk")) == ob
                               and norm_pairs((r.get("b_hpx") or {}).get("peer_topk")) == oa
                               and norm_pairs((r.get("b_ray") or {}).get("peer_topk")) == oa)
    g["local_order_ok"] = all(is_sorted_total_order(norm_pairs((r.get(kk) or {}).get("own_topk")))
                              for kk in r)
    # Globals: every arm/direction equals the independent oracle (ids + float32 bits, one compare).
    g["global_equals_oracle_all"] = all(ng[kk] == og for kk in ng)
    g["global_ids_exact"] = all([t for t, _ in ng[kk]] == [t for t, _ in og] for kk in ng)
    g["float32_bits_exact"] = all([b for _, b in ng[kk]] == [b for _, b in og] for kk in ng)
    # Arm agreement + coordinator symmetry.
    g["ray_equals_hpx_A"] = (ng["a_ray"] == ng["a_hpx"])
    g["ray_equals_hpx_B"] = (ng["b_ray"] == ng["b_hpx"])
    g["coordinator_symmetry"] = (ng["a_hpx"] == ng["b_hpx"])
    # Witnesses.
    def comp_ok(kk):
        c = (r.get(kk) or {}).get("hpx_composition") or {}
        return bool(c.get("action_dispatched") and c.get("future_ready")
                    and c.get("continuation_executed") and c.get("result_delivered"))
    g["hpx_composition_evidenced"] = comp_ok("a_hpx") and comp_ok("b_hpx")
    g["no_hpx_witness_in_ray_arm"] = ("hpx_composition" not in (r.get("a_ray") or {})
                                      and "hpx_composition" not in (r.get("b_ray") or {}))
    a, b = meta["a"], meta["b"]
    g["hpx_peer_identity"] = bool(
        (r.get("a_hpx") or {}).get("peer_pid") == b["pid"]
        and (r.get("a_hpx") or {}).get("peer_locality") == b["locality"]
        and (r.get("b_hpx") or {}).get("peer_pid") == a["pid"]
        and (r.get("b_hpx") or {}).get("peer_locality") == a["locality"])
    g["ray_peer_identity"] = bool(
        (r.get("a_ray") or {}).get("peer_pid") == b["pid"]
        and (r.get("a_ray") or {}).get("peer_node_id") == b["node_id"]
        and (r.get("b_ray") or {}).get("peer_pid") == a["pid"]
        and (r.get("b_ray") or {}).get("peer_node_id") == a["node_id"])
    g["no_application_on_root"] = bool(
        (r.get("a_hpx") or {}).get("own_locality") not in (None, 0)
        and (r.get("a_hpx") or {}).get("peer_locality") not in (None, 0)
        and (r.get("b_hpx") or {}).get("own_locality") not in (None, 0)
        and (r.get("b_hpx") or {}).get("peer_locality") not in (None, 0))
    return g


def pregate_failure_flags(cases_eval):
    """Map pre-gate sub-gate failures onto the taxonomy (deterministic, first-match order)."""
    flags = []
    for name, g in cases_eval:
        if not g.get("shard_partition_ok"):
            flags.append("invalid_instrumentation")
        if not (g.get("a_local_correct_both_arms") and g.get("b_local_correct_both_arms")
                and g.get("local_order_ok")):
            flags.append("native_compute_mismatch")
        if not g.get("peer_lists_correct"):
            flags.append("payload_contract_mismatch")
        if not g.get("float32_bits_exact"):
            flags.append("float32_bit_mismatch")
        if g.get("float32_bits_exact") and not g.get("global_equals_oracle_all"):
            flags.append("native_merge_mismatch")
        if not (g.get("ray_equals_hpx_A") and g.get("ray_equals_hpx_B")
                and g.get("coordinator_symmetry")):
            flags.append("payload_contract_mismatch")
        if not g.get("hpx_composition_evidenced"):
            flags.append("hpx_action_witness_missing")
        if not g.get("no_hpx_witness_in_ray_arm"):
            flags.append("hpx_action_used_by_ray_arm")
        if not (g.get("hpx_peer_identity") and g.get("ray_peer_identity")):
            flags.append("invalid_instrumentation")
        if not g.get("no_application_on_root"):
            flags.append("invalid_instrumentation")
    return flags


def run_pregate(ray_mod, meta):
    out = {"pass": True, "cases": [], "flags": []}
    a, b = meta["a"]["handle"], meta["b"]["handle"]
    cases_eval = []
    for case in CORRECTNESS_MATRIX:
        oc = case_oracle(case)
        argsA, argsB = shard_args(case, "A"), shard_args(case, "B")
        r = {"a_hpx": ray_mod.get(a.hpx_coordinate.remote(*argsA, True), timeout=60),
             "a_ray": ray_mod.get(a.ray_coordinate.remote(*argsA, True), timeout=60),
             "b_hpx": ray_mod.get(b.hpx_coordinate.remote(*argsB, True), timeout=60),
             "b_ray": ray_mod.get(b.ray_coordinate.remote(*argsB, True), timeout=60)}
        g = eval_pregate_case(case, r, oc, meta)
        cases_eval.append((case["name"], g))
        out["cases"].append({"name": case["name"], "gates": g,
                             "failed": [k for k, v in g.items() if not v]})
        if not all(g.values()):
            out["pass"] = False
    out["flags"] = pregate_failure_flags(cases_eval) if not out["pass"] else []
    return out


# ---------------------------------------------------------------------------------------
# One timed case block (W warm-up pairs + K timed pairs, paired alternating order)
# ---------------------------------------------------------------------------------------

def run_timed_case(ray_mod, meta, coord_key, peer_key, case, W, K, direction, phase,
                   timer_median_ns, sample_writer, rep):
    coord = meta[coord_key]["handle"]
    expect_peer, expect_own = meta[peer_key], meta[coord_key]
    args = shard_args(case, direction)
    oracle_global = case_oracle(case)["global"]
    cr = {"case": case["name"], "direction": direction, "W": W, "K": K, "phase": phase,
          "flags": [], "witness_pairs_checked": 0, "arm_order_ok": True,
          "invalid": {"ray": 0, "hpx": 0}, "timeouts": 0, "elapsed": {"ray": [], "hpx": []}}
    order_log = []

    def counters():
        return {"a": ray_mod.get(meta["a"]["handle"].counters.remote(), timeout=30),
                "b": ray_mod.get(meta["b"]["handle"].counters.remote(), timeout=30)}

    def healths():
        return {"a": ray_mod.get(meta["a"]["handle"].health.remote(), timeout=30),
                "b": ray_mod.get(meta["b"]["handle"].health.remote(), timeout=30)}

    def stability_flags(h):
        fl = []
        for kk in ("a", "b"):
            hh = h.get(kk) or {}
            if hh.get("pid") != meta[kk]["pid"]:
                fl.append("actor_recreated_during_measurement")
            if hh.get("locality_id") != meta[kk]["locality"]:
                fl.append("locality_changed_during_measurement")
            if hh.get("membership") != 3:
                fl.append("membership_changed_during_measurement")
        return fl

    h0 = healths()
    fl = stability_flags(h0)
    if fl:
        cr["flags"].extend(fl)
        return cr

    snap0 = counters()
    # --- W warm-up pairs (timings DISCARDED; results still verified -- never toward timing) ---
    for i in range(W):
        for pos, arm in enumerate(pair_order(i)):
            try:
                t0, t1, res = timed_sample(ray_mod, coord, arm, args)
            except Exception as ex:  # noqa: BLE001
                cr["flags"].append("warmup_incomplete")
                cr["warmup_error"] = f"{type(ex).__name__}: {ex}"
                return cr
            ok, reasons = verify_timed_result(arm, res, oracle_global, expect_peer, expect_own)
            sample_writer({"rep": rep, "case": case["name"], "direction": direction, "phase": phase,
                           "warmup": True, "pair": i, "pos": pos, "arm": arm,
                           "t0_ns": t0, "t1_ns": t1, "elapsed_ns": t1 - t0,
                           "valid": ok, "reasons": reasons})
            if not ok:
                cr["flags"].append("warmup_incomplete")
                cr["warmup_verify_failed"] = {"pair": i, "arm": arm, "reasons": reasons}
                return cr
    snapW = counters()
    if not block_delta_check(snap0, snapW, coord_key, peer_key, W):
        cr["flags"].append("warmup_incomplete")
        cr["warmup_block_delta"] = {"before": snap0, "after": snapW, "expected_pairs": W}
        return cr

    # --- K timed pairs; per-pair witness snapshots strictly OUTSIDE the timed windows ---
    prev = snapW
    for i in range(K):
        for pos, arm in enumerate(pair_order(i)):
            try:
                t0, t1, res = timed_sample(ray_mod, coord, arm, args)
            except Exception as ex:  # noqa: BLE001
                cr["timeouts"] += 1
                cr["flags"].append("timeout")
                cr["timeout_error"] = f"pair {i} {arm}: {type(ex).__name__}: {ex}"
                return cr
            elapsed = t1 - t0
            ok, reasons = verify_timed_result(arm, res, oracle_global, expect_peer, expect_own)
            order_log.append((i, pos, arm))
            sample_writer({"rep": rep, "case": case["name"], "direction": direction, "phase": phase,
                           "warmup": False, "pair": i, "pos": pos, "arm": arm,
                           "t0_ns": t0, "t1_ns": t1, "elapsed_ns": elapsed,
                           "valid": ok, "reasons": reasons})
            if elapsed < 0:
                cr["flags"].append("clock_nonmonotonic")
                return cr
            if not ok:
                cr["invalid"][arm] += 1
                cr["flags"].append("float32_bit_mismatch" if "float32_bit_mismatch" in reasons
                                   else "oracle_mismatch_in_timed_sample")
                cr["first_invalid"] = {"pair": i, "arm": arm, "reasons": reasons}
                return cr
            cr["elapsed"][arm].append(elapsed)
        cur = counters()
        okw, cls = pair_witness_check(prev, cur, coord_key, peer_key)
        cr["witness_pairs_checked"] += 1
        if not okw:
            cr["flags"].append(cls)
            cr["witness_violation"] = {"pair": i, "class": cls, "before": prev, "after": cur}
            return cr
        prev = cur

    # --- end-of-block checks (order re-derivation, block delta, stability, stats) ---
    cr["block_delta_ok"] = block_delta_check(snapW, prev, coord_key, peer_key, K)
    if not cr["block_delta_ok"]:
        cr["flags"].append("invalid_instrumentation")
        return cr
    viol = order_violations(order_log)
    cr["arm_order_ok"] = not viol
    if viol:
        cr["flags"].append("arm_order_violation")
        return cr
    fl = stability_flags(healths())
    if fl:
        cr["flags"].extend(fl)
        return cr
    cr["stats"] = {arm: stats_block(cr["elapsed"][arm], K, phase) for arm in ("ray", "hpx")}
    for arm in ("ray", "hpx"):
        st = cr["stats"][arm]
        if st["n"] < K:
            cr["flags"].append("insufficient_valid_samples")
        if phase == "accepted" and st["n"] < P99_MIN_N:
            cr["flags"].append("insufficient_samples_for_p99")
        if timer_median_ns and st["p50_ns"] is not None \
                and st["p50_ns"] < TIMER_OVERHEAD_P50_FACTOR * timer_median_ns:
            cr["flags"].append("timer_overhead_too_high")
    del cr["elapsed"]  # raw samples live in the per-rep JSONL, not the summary
    return cr


# ---------------------------------------------------------------------------------------
# Rep-level gate table + classification (pure over the marker dict -> selftestable)
# ---------------------------------------------------------------------------------------

def eval_rep_gates(m):
    ai, bi = m.get("a_identity") or {}, m.get("b_identity") or {}
    as_, bs = m.get("a_start") or {}, m.get("b_start") or {}
    rr, rf = m.get("root_ready") or {}, m.get("root_final") or {}
    a_cv = (ai.get("hpx_version_info") or {}).get("hpx_complete_version")
    b_cv = (bi.get("hpx_version_info") or {}).get("hpx_complete_version")
    tm = m.get("timer") or {}
    pre = m.get("pregate") or {}
    timed = list(m.get("timed_A") or []) + list(m.get("timed_B") or [])
    case_flags = [f for cr in timed for f in cr.get("flags", [])]
    expected_a = [c["name"] for c in TIMED_MATRIX]
    expected_b = list(B_CONTROL_CASES) if m.get("b_control_expected") else []
    g = {}
    g["fixed_hpx_commit"] = bool(commit_matches(a_cv) and commit_matches(b_cv)
                                 and commit_matches(rr.get("hpx_complete_version")))
    g["actors_started_distinct_nonzero_localities"] = bool(
        as_.get("started") and bs.get("started")
        and as_.get("locality_id") not in (None, 0) and bs.get("locality_id") not in (None, 0)
        and as_.get("locality_id") != bs.get("locality_id"))
    g["two_distinct_ray_processes"] = bool(ai.get("pid") and bi.get("pid")
                                           and ai.get("pid") != bi.get("pid"))
    g["both_in_process_no_hpx_child"] = bool(
        (m.get("a_child_report") or {}).get("checked")
        and (m.get("a_child_report") or {}).get("hpx_children") == []
        and (m.get("b_child_report") or {}).get("checked")
        and (m.get("b_child_report") or {}).get("hpx_children") == [])
    g["membership_reached_three"] = bool(max(as_.get("membership") or 0,
                                             bs.get("membership") or 0) >= 3
                                         and rf.get("max_membership", 0) >= 3)
    g["no_static_locality_count"] = bool(m.get("argv_audit_ok"))
    g["root_isolated_workfree"] = bool(rr.get("locality_id") == 0
                                       and rf.get("actions_served_on_root", -1) == 0)
    g["resources_identical_between_arms"] = bool(
        m.get("actor_hpx_threads") is not None and m.get("actor_ray_num_cpus") is not None
        and m.get("resources_single_constants") is True)
    g["actor_placement_valid"] = bool(ai.get("hostname") and ai.get("hostname") == bi.get("hostname")
                                      == rr.get("hostname") == m.get("controller_hostname"))
    g["timer_calibration_ok"] = bool(tm.get("clock") == "time.perf_counter_ns"
                                     and tm.get("clock_monotonic_declared")
                                     and tm.get("nonnegative_deltas")
                                     and (tm.get("n_reads") or 0) >= 10000)
    g["pregate_seven_cases_pass"] = bool(pre.get("pass")
                                         and len(pre.get("cases") or []) == len(CORRECTNESS_MATRIX))
    g["timed_blocks_complete"] = ([cr.get("case") for cr in (m.get("timed_A") or [])] == expected_a
                                  and [cr.get("case") for cr in (m.get("timed_B") or [])] == expected_b)
    g["all_timed_samples_valid"] = bool(timed) and all(
        cr.get("invalid", {}).get("ray", 1) == 0 and cr.get("invalid", {}).get("hpx", 1) == 0
        and cr.get("timeouts", 1) == 0 for cr in timed)
    g["full_sample_counts"] = bool(timed) and all(
        (cr.get("stats") or {}).get(arm, {}).get("n") == cr.get("K")
        for cr in timed for arm in ("ray", "hpx"))
    g["arm_order_ok"] = bool(timed) and all(cr.get("arm_order_ok") for cr in timed)
    g["witness_exclusivity_ok"] = bool(timed) and all(
        cr.get("witness_pairs_checked") == cr.get("K") and cr.get("block_delta_ok")
        for cr in timed)
    g["timer_overhead_acceptable"] = "timer_overhead_too_high" not in case_flags
    g["no_stability_change_during_timing"] = not any(
        f in case_flags for f in ("membership_changed_during_measurement",
                                  "actor_recreated_during_measurement",
                                  "locality_changed_during_measurement"))
    g["single_controller_clock_only"] = bool(m.get("single_controller_clock_only"))
    g["graceful_leave_clean_stop"] = bool(m.get("a_stop_rc") == 0 and not m.get("a_stop_error")
                                          and m.get("b_stop_rc") == 0 and not m.get("b_stop_error"))
    g["root_finalized_clean"] = bool(m.get("root_exit_path") == "finalized_clean"
                                     and rf.get("leave_observed") is True
                                     and rf.get("final_membership") == 1)
    g["actors_destroyed"] = bool(m.get("a_destroyed") and m.get("b_destroyed"))
    g["actors_recreated"] = bool((m.get("a_recreate") or {}).get("ok")
                                 and (m.get("a_recreate") or {}).get("pid")
                                 and (m.get("a_recreate") or {}).get("pid") != ai.get("pid")
                                 and (m.get("b_recreate") or {}).get("ok")
                                 and (m.get("b_recreate") or {}).get("pid")
                                 and (m.get("b_recreate") or {}).get("pid") != bi.get("pid"))
    g["no_orphans"] = (m.get("orphans") == [] and m.get("actor_pids_gone") is True)
    g["all_waits_bounded"] = bool(m.get("all_waits_bounded"))
    g["ratio_fences_false"] = all(v is False for v in (m.get("ratio_fences") or {}).values()) \
        and set(m.get("ratio_fences") or {}) == set(RATIO_FENCES)
    g["evidence_complete"] = bool(ai and bi and as_ and bs and rr and rf and tm and pre
                                  and m.get("timed_A"))
    return g


def collect_rep_flags(m, gates):
    """All taxonomy flags raised anywhere in the rep (case flags + gate-mapped flags)."""
    flags = []
    timed = list(m.get("timed_A") or []) + list(m.get("timed_B") or [])
    for cr in timed:
        flags.extend(cr.get("flags", []))
    flags.extend((m.get("pregate") or {}).get("flags", []))
    if not gates["fixed_hpx_commit"]:
        flags.append("hpx_identity_mismatch")
    if not gates["evidence_complete"] or not gates["no_static_locality_count"] \
            or not gates["root_isolated_workfree"] or not gates["timer_calibration_ok"] \
            or not gates["timed_blocks_complete"]:
        flags.append("invalid_instrumentation")
    if not gates["actor_placement_valid"]:
        flags.append("actor_placement_invalid")
    if not gates["resources_identical_between_arms"]:
        flags.append("resource_mismatch_between_arms")
    if not gates["single_controller_clock_only"]:
        flags.append("cross_node_clock_subtraction_detected")
    if not gates["full_sample_counts"]:
        flags.append("insufficient_valid_samples")
    if not gates["actors_recreated"]:
        flags.append("actor_recreation_failed")
    if not gates["no_orphans"]:
        flags.append("orphan_detected")
    if not (gates["actors_started_distinct_nonzero_localities"]
            and gates["two_distinct_ray_processes"] and gates["both_in_process_no_hpx_child"]
            and gates["membership_reached_three"] and gates["graceful_leave_clean_stop"]
            and gates["root_finalized_clean"] and gates["actors_destroyed"]
            and gates["all_waits_bounded"]):
        flags.append("lifecycle_failed")
    if not gates["ratio_fences_false"]:
        flags.append("invalid_instrumentation")
    return flags


def classify_rep(gates, flags):
    if all(gates.values()) and not flags:
        return "pass"
    for cls in CLASS_PRIORITY:
        if cls in flags:
            return cls
    return "invalid_instrumentation"


# ---------------------------------------------------------------------------------------
# One repetition: fresh island -> gates -> warm -> pre-gate -> timed A (+ timed B control)
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
        return {"ok": False, "started": False, "ready": False,
                "error": f"{what}: {type(ex).__name__}: {ex}"}


def run_rep(ray_mod, HpxActor, peer, build_dir, rep, runs_root, hpx_threads, ray_cpus, samp, phase):
    boot = os.path.join(runs_root, f"rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    p_root, p_a, p_b = find_free_port(), find_free_port(), find_free_port()
    rcmd = peer_root_cmd(peer, boot, p_root)
    ep_a, ep_b = actor_endpoints(p_root, p_a), actor_endpoints(p_root, p_b)
    m = {
        "controller_hostname": socket.gethostname(),
        "controller_pid": os.getpid(),
        "ports": {"root": p_root, "actor_a": p_a, "actor_b": p_b},
        "argv_audit_ok": argv_audit(rcmd, ep_a, ep_b),
        "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus,
        "resources_single_constants": True,  # ONE constant pair configures BOTH actors/arms
        "single_controller_clock_only": True,  # every elapsed pairs two controller perf stamps
        "all_waits_bounded": True,
        "generation_rule": GENERATION_RULE,
        "ratio_fences": dict(RATIO_FENCES),
        "sampling": samp, "phase": phase,
        "b_control_expected": bool(samp.get("B")),
        "timed_A": [], "timed_B": [],
    }
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    samples_path = os.path.join(boot, "samples.jsonl")
    sf = open(samples_path, "w")

    def sample_writer(row):
        sf.write(json.dumps(row) + "\n")

    try:
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file(os.path.join(boot, "root.ready"), 30, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        a1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_a)
        m["a_identity"] = _ray_get(ray_mod, a1.load_identity.remote(), 40, "a_identity")
        m["a_start"] = _ray_get(ray_mod, a1.start_hpx.remote(), 60, "a_start")
        m["a_child_report"] = _ray_get(ray_mod, a1.child_report.remote(), 30, "a_child_report")
        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0).remote(build_dir, hpx_threads, ep_b)
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 40, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 60, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")

        a_loc = (m["a_start"] or {}).get("locality_id")
        b_loc = (m["b_start"] or {}).get("locality_id")
        a_pid = (m.get("a_identity") or {}).get("pid")
        b_pid = (m.get("b_identity") or {}).get("pid")
        a2_pid = b2_pid = None

        # FAIL CLOSED before any timing: identity, hosting, distinct localities.
        identity_ok = (commit_matches((m["a_identity"].get("hpx_version_info") or {})
                                      .get("hpx_complete_version"))
                       and commit_matches((m["b_identity"].get("hpx_version_info") or {})
                                          .get("hpx_complete_version"))
                       and commit_matches((m.get("root_ready") or {}).get("hpx_complete_version")))
        hosting_ok = a_loc not in (None, 0) and b_loc not in (None, 0) and a_loc != b_loc

        if identity_ok and hosting_ok:
            _ray_get(ray_mod, a1.set_peer.remote(b1, b_loc), 30, "a_set_peer")
            _ray_get(ray_mod, b1.set_peer.remote(a1, a_loc), 30, "b_set_peer")
            meta = {"a": {"handle": a1, "pid": a_pid,
                          "host": (m["a_identity"] or {}).get("hostname"),
                          "node_id": (m["a_identity"] or {}).get("node_id"), "locality": a_loc},
                    "b": {"handle": b1, "pid": b_pid,
                          "host": (m["b_identity"] or {}).get("hostname"),
                          "node_id": (m["b_identity"] or {}).get("node_id"), "locality": b_loc}}

            # Timer calibration (controller clock; the ONLY timing source anywhere).
            m["timer"] = calibrate_timer()
            timer_med = m["timer"]["median_overhead_ns"]

            # Warm the outer Ray path, the nested Ray peer channel (both directions), and the
            # HPX parcelport (verified action, both directions) BEFORE the pre-gate.
            tiny = CORRECTNESS_MATRIX[0]
            oc = case_oracle(tiny)
            warm_ok = True
            for _ in range(3):
                _ray_get(ray_mod, a1.health.remote(), 30, "warm_a")
                _ray_get(ray_mod, b1.health.remote(), 30, "warm_b")
            for handle, direction in ((a1, "A"), (b1, "B")):
                wargs = shard_args(tiny, direction)
                for arm in ("ray", "hpx"):
                    for _ in range(2):
                        res = _ray_get(ray_mod, getattr(handle, arm + "_coordinate")
                                       .remote(*wargs), 60, f"warm_{arm}_{direction}")
                        if norm_pairs((res or {}).get("global_topk")) != oc["global"]:
                            warm_ok = False
            m["channel_warm_ok"] = warm_ok

            # Untimed correctness pre-gate: full seven-case exp68 matrix, both arms, both directions.
            m["pregate"] = run_pregate(ray_mod, meta)

            if warm_ok and m["pregate"]["pass"]:
                # Timed A-coordinated primary band (all six cases).
                for case in TIMED_MATRIX:
                    print(f"[exp69]   rep {rep} timed A/{case['name']} "
                          f"(W={samp['A']['W']} K={samp['A']['K']}) ...", flush=True)
                    cr = run_timed_case(ray_mod, meta, "a", "b", case, samp["A"]["W"],
                                        samp["A"]["K"], "A", phase, timer_med, sample_writer, rep)
                    m["timed_A"].append(cr)
                    if cr.get("flags"):
                        break
                # Timed B-coordinated directional control (approved limited set), only if A clean.
                if samp.get("B") and all(not cr.get("flags") for cr in m["timed_A"]):
                    for case in [c for c in TIMED_MATRIX if c["name"] in B_CONTROL_CASES]:
                        print(f"[exp69]   rep {rep} timed B/{case['name']} "
                              f"(W={samp['B']['W']} K={samp['B']['K']}) ...", flush=True)
                        cr = run_timed_case(ray_mod, meta, "b", "a", case, samp["B"]["W"],
                                            samp["B"]["K"], "B", phase, timer_med,
                                            sample_writer, rep)
                        m["timed_B"].append(cr)
                        if cr.get("flags"):
                            break

        # Lifecycle: graceful leave -> destroy -> recreate -> root completion -> orphan check.
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
        a2_pid = (m["a_recreate"] or {}).get("pid")
        b2_pid = (m["b_recreate"] or {}).get("pid")
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
        sf.close()

    gates = eval_rep_gates(m)
    flags = collect_rep_flags(m, gates)
    fc = classify_rep(gates, flags)
    return {
        "rep": rep, "boot_rel": os.path.relpath(boot, HERE), "ports": m["ports"],
        "samples_file_rel": os.path.relpath(samples_path, HERE),
        "gates": gates, "gates_failed": [k for k, v in gates.items() if not v],
        "flags": sorted(set(flags)), "rep_pass": fc == "pass", "failure_class": fc,
        "timer": m.get("timer"),
        "pregate": {"pass": (m.get("pregate") or {}).get("pass"),
                    "cases": [{"name": c["name"], "failed": c["failed"]}
                              for c in (m.get("pregate") or {}).get("cases", [])]},
        "timed_A": m.get("timed_A"), "timed_B": m.get("timed_B"),
        "markers": m,
    }


# ---------------------------------------------------------------------------------------
# Across-repetition statistics (per arm x case x direction; NEVER pooled across anything)
# ---------------------------------------------------------------------------------------

def across_reps_stats(reps, direction_key):
    out = {}
    for rep_out in reps:
        for cr in rep_out.get(direction_key) or []:
            st = cr.get("stats") or {}
            for arm in ("ray", "hpx"):
                if arm not in st:
                    continue
                slot = out.setdefault(cr["case"], {}).setdefault(arm, {"per_rep": []})
                slot["per_rep"].append({"rep": rep_out["rep"], **st[arm]})
    for case_name, arms in out.items():
        for arm, slot in arms.items():
            p50s = [r["p50_ns"] for r in slot["per_rep"] if r.get("p50_ns") is not None]
            slot["median_of_per_rep_p50_ns"] = median(p50s)
            slot["p50_spread_ns"] = (max(p50s) - min(p50s)) if p50s else None
            slot["p50_min_ns"] = min(p50s) if p50s else None
            slot["p50_max_ns"] = max(p50s) if p50s else None
    return out


def build_aggregate(phase, samp, reps, provenance):
    overall_pass = bool(reps) and len(reps) == samp["reps"] and all(r["rep_pass"] for r in reps)
    slim_reps = []
    for r in reps:
        rr = {k: r[k] for k in ("rep", "boot_rel", "samples_file_rel", "gates", "gates_failed",
                                "flags", "rep_pass", "failure_class", "timer", "pregate",
                                "timed_A", "timed_B")}
        slim_reps.append(rr)
    agg = {
        "experiment": "69_same_axis_topk_perf",
        "kind": ("slice0_smoke_instrument_validation" if phase == "smoke"
                 else "slice1_same_axis_qd1_latency_local"),
        "phase": phase, "single_node": True,
        "transport_hpx_arm": "HPX TCP parcelport (loopback)",
        "transport_ray_arm": "Ray actor call path (local)",
        "measured_contrast": MEASURED_CONTRAST,
        "shared_runtime_caveat": SHARED_RUNTIME_CAVEAT,
        "timed_boundary": TIMED_BOUNDARY,
        "hpx_arm_wait_strategy": (
            "hpx::post + action -> future -> .then continuation merge, UNTIMED merged.get() on the "
            "HPX thread; caller bound via std::future::wait_for on the calling OS thread (GIL "
            "released). Sustained-volume hardening: exp68's hpx timed wait (future::wait_for on a "
            "run_as_hpx_thread trampoline) crashes this build/platform after a few hundred "
            "sequential calls (Ray-free diagnostic coordinate_diag isolates it; untimed variants "
            "stable at 2000/5000 iterations)."),
        "arm_order_policy": "paired alternating: even pair Ray->HPX, odd pair HPX->Ray",
        "queue_depth": "QD1 (one request in flight; latency only; throughput deferred)",
        "generation_rule": GENERATION_RULE, "total_order": TOTAL_ORDER,
        "correctness_matrix": CORRECTNESS_MATRIX, "timed_matrix": TIMED_MATRIX,
        "b_control_cases": B_CONTROL_CASES, "sampling": samp,
        "timer_overhead_rule": f"flag case if per-rep p50 < {TIMER_OVERHEAD_P50_FACTOR}x median "
                               "adjacent-read overhead",
        "ratio_fences": dict(RATIO_FENCES), "claim_fences": dict(CLAIM_FENCES),
        "failure_classes": list(FAILURE_CLASSES),
        "provenance": provenance,
        "reps": slim_reps,
        "across_reps_A_coordinated": across_reps_stats(reps, "timed_A"),
        "across_reps_B_control_separate_never_pooled": across_reps_stats(reps, "timed_B"),
        "overall": "pass" if overall_pass else "fail",
    }
    if phase == "smoke":
        agg["smoke_note"] = ("Slice 0 instrument validation ONLY: no performance conclusion, no "
                             "ratio, no speedup, no winner; W/K/R too small for distribution claims.")
        agg["safe_claim"] = ("Instrumentation validated (timer, boundary, witnesses, pair "
                             "scheduler, verification-after-t1, lifecycle) on the tested local "
                             "environment." if overall_pass
                             else "Slice 0 did not pass; see reps[].failure_class.")
    else:
        agg["safe_claim"] = (
            "On the tested local single-node environment (see provenance), the identical "
            "deterministic exp68 vocabulary-sharded top-k request was measured at the same Python "
            "caller boundary through a Ray-mediated and an HPX-mediated peer-candidate path inside "
            "the same Ray-hosted, HPX-resident actor topology; per-arm QD1 latency distributions "
            "were collected with every timed sample verified bit-exactly after t1. Per-arm "
            "distributions only; NO ratio, speedup, difference, or winner is computed or implied."
            if overall_pass else "Slice 1 did not pass; see reps[].failure_class.")
    return agg


def _write_agg(path, agg):
    with open(path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=False); f.write("\n")


def run_phase(args):
    phase = args.phase
    samp = dict(SAMPLING[phase])
    if args.reps is not None:
        samp["reps"] = args.reps
    peer = os.path.join(args.build_dir, PEER_BASENAME)
    ext_so = next((os.path.join(args.build_dir, fn)
                   for fn in (os.listdir(args.build_dir) if os.path.isdir(args.build_dir) else [])
                   if fn.startswith(EXT_MODULE) and fn.endswith(".so")), None)
    if not (os.path.exists(peer) and ext_so):
        print(f"SKIP: build exp69 first (peer={os.path.exists(peer)} ext={bool(ext_so)})")
        return 0
    try:
        import ray
    except Exception as ex:  # noqa: BLE001
        print(f"SKIP: ray unavailable: {ex}")
        return 0

    import logging
    ray.init(num_cpus=max(6, 2 * args.ray_num_cpus + 2), include_dashboard=False,
             log_to_driver=False, logging_level=logging.ERROR, ignore_reinit_error=True)
    HpxActor = build_actor_class(ray)
    runs_root = os.path.join(HERE, "_exp69_runs", f"{phase}_{time.strftime('%Y%m%dT%H%M%SZ')}")
    os.makedirs(runs_root, exist_ok=True)
    print(f"[exp69] phase {phase}  peer {peer}\n[exp69] ext  {os.path.basename(ext_so)}\n"
          f"[exp69] runs {os.path.relpath(runs_root, HERE)}")

    reps, provenance = [], None
    try:
        for r in range(1, samp["reps"] + 1):
            print(f"[exp69] rep {r} ...", flush=True)
            rep = run_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), r, runs_root,
                          args.hpx_threads, args.ray_num_cpus, samp, phase)
            if provenance is None:
                provenance = collect_provenance(ray, rep["markers"].get("a_identity"))
            # Full raw markers per rep stay in the ignored runs dir (retention requirement).
            with open(os.path.join(runs_root, f"rep_{r}", "markers.json"), "w") as f:
                json.dump(rep["markers"], f, indent=2)
            rep.pop("markers", None)
            print(f"[exp69]   rep {r}: {'PASS' if rep['rep_pass'] else 'FAIL'} "
                  f"({rep['failure_class']}) gates_failed={rep['gates_failed']} "
                  f"flags={rep['flags']}", flush=True)
            reps.append(rep)
    finally:
        try:
            ray.shutdown()
        except Exception:
            pass

    agg = build_aggregate(phase, samp, reps, provenance)
    out_path = args.aggregate or os.path.join(runs_root, f"{phase}_aggregate.json")
    _write_agg(out_path, agg)
    print(f"[exp69] overall: {agg['overall']} -> {out_path}")
    # The curated aggregate is written ONLY by an accepted (passing) Slice 1 run.
    if phase == "accepted" and agg["overall"] == "pass" and args.aggregate is None:
        curated = os.path.join(HERE, "same_axis_topk_qd1_local_aggregate.json")
        _write_agg(curated, agg)
        print(f"[exp69] curated aggregate -> {curated}")
    return 0 if agg["overall"] == "pass" else 1


# =======================================================================================
# Cross-node (Rostam / THREE-node Slurm) phase.  EXPERIMENT-ONLY; no performance claim.
#
# Wraps the EXACT same-axis measurement body used by the local `run_rep` (timer -> warm ->
# seven-case pre-gate -> timed A [+ timed B control]) in the accepted exp68 three-node
# placement scaffold: three roles on three DISTINCT physical nodes (root+controller R,
# actor A, actor B), both actors HARD-placed via NodeAffinitySchedulingStrategy(soft=False),
# HPX AGAS/parcelport endpoints pinned to each node's selected 10.42.5.x IP.  The local
# same-host `actor_placement_valid` gate is REPLACED (only here) by three-node placement
# attestation; every same-axis measurement gate and all four ratio fences are preserved.
# Reuses exp59/66/67/68 Ray-on-Slurm bring-up and the corrected orphan check.  Writes its OWN
# curated aggregate; NEVER touches the local aggregate.  Ports run_exp68's helpers verbatim.
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
    """_wait_for_file, but re-list the parent dir each poll to defeat NFS attribute caching."""
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
    return ["--node-manager-port", str(a.node_manager_port),
            "--object-manager-port", str(a.object_manager_port),
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
    """Ray-process leftovers on `node`; drops our own srun/pgrep/ray-stop/run_exp69 machinery."""
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
                    and "ray stop" not in ln and "run_exp69" not in ln]
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
            if ln.strip() and "pgrep" not in ln and "srun" not in ln and "run_exp69" not in ln]
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


# ---------------------------------------------------------------------------------------
# Cross-node gate table: reuse every local same-axis gate; REPLACE the same-host placement
# gate with three-node placement attestation (only here).  Pure over (markers, expect).
# ---------------------------------------------------------------------------------------

def eval_rep_gates_crossnode(m, expect):
    g = eval_rep_gates(m)
    g.pop("actor_placement_valid", None)  # same-host gate -> replaced by three-node attestation
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
    g["actor_a_recreated_on_node"] = bool(m.get("a_recreate_on_node"))
    g["actor_b_recreated_on_node"] = bool(m.get("b_recreate_on_node"))
    g["evidence_fields_complete_crossnode"] = bool(
        ap_a.get("node_id") and ap_b.get("node_id") and rr.get("hostname")
        and expect.get("nodeA_nid") and expect.get("nodeB_nid"))
    return g


def collect_rep_flags_crossnode(m, gates):
    """Reuse the local flag mapping (with the same-host gate neutralized) + map the cross-node
    placement/recreation/evidence gates onto the shared taxonomy."""
    base = dict(gates)
    base["actor_placement_valid"] = True  # local same-host gate is replaced, not evaluated here
    flags = list(collect_rep_flags(m, base))
    if not (gates.get("three_node_slurm_allocation") and gates.get("root_on_root_node")
            and gates.get("actor_a_hard_placed") and gates.get("actor_b_hard_placed")
            and gates.get("actor_nodes_distinct") and gates.get("three_roles_three_nodes")
            and gates.get("endpoints_pinned_subnet")):
        flags.append("actor_placement_invalid")
    if not (gates.get("actor_a_recreated_on_node") and gates.get("actor_b_recreated_on_node")):
        flags.append("actor_recreation_failed")
    if not (gates.get("evidence_fields_complete_crossnode") and gates.get("remote_orphan_check_ran")):
        flags.append("invalid_instrumentation")
    return sorted(set(flags))


CROSSNODE_FAILURE_CLASSES = list(FAILURE_CLASSES) + [
    "three_node_allocation_unavailable", "subnet_mismatch", "ray_cluster_failed",
    "shared_island_join_failed"]


# ---------------------------------------------------------------------------------------
# One cross-node repetition: hard-placed three-node island -> cross-node gates -> the SAME
# measurement body as run_rep (verbatim) -> three-node lifecycle + orphan checks.
# ---------------------------------------------------------------------------------------

def run_crossnode_rep(ray_mod, HpxActor, peer, build_dir, rep, runs_root, hpx_threads, ray_cpus,
                      nodeR, nodeA, nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid, nodeB_nid,
                      subnet, port_base, samp, phase, expect, env):
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
        "controller_pid": os.getpid(),
        "ports": {"root": p_root, "actor_a": p_a, "actor_b": p_b},
        "argv_audit_ok": argv_audit(rcmd, ep_a, ep_b),
        "endpoints_subnet_ok": bool(nodeR_ip.startswith(subnet) and nodeA_ip.startswith(subnet)
                                    and nodeB_ip.startswith(subnet)),
        "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus,
        "resources_single_constants": True,   # ONE constant pair configures BOTH actors/arms
        "single_controller_clock_only": True,  # every elapsed pairs two controller perf stamps
        "all_waits_bounded": True,
        "generation_rule": GENERATION_RULE,
        "ratio_fences": dict(RATIO_FENCES),
        "sampling": samp, "phase": phase,
        "b_control_expected": bool(samp.get("B")),
        "remote_orphan_check_ran": False,
        "timed_A": [], "timed_B": [],
    }
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    samples_path = os.path.join(boot, "samples.jsonl")
    sf = open(samples_path, "w")

    def sample_writer(row):
        sf.write(json.dumps(row) + "\n")

    def _kill_dead(actor):
        ray_mod.kill(actor)
        try:
            ray_mod.get(actor.ping.remote(), timeout=10); return False
        except Exception:  # noqa: BLE001
            return True

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

        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              scheduling_strategy=strat_b).remote(build_dir, hpx_threads, ep_b)
        pb = dict(_ray_get(ray_mod, b1.ray_placement.remote(), 60, "b_placement") or {})
        pb["placement_soft"], pb["target_node"] = False, nodeB
        m["b_placement"] = pb
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 60, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 120, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")

        a_loc = (m["a_start"] or {}).get("locality_id")
        b_loc = (m["b_start"] or {}).get("locality_id")
        a_pid = (m.get("a_identity") or {}).get("pid")
        b_pid = (m.get("b_identity") or {}).get("pid")

        # FAIL CLOSED before any timing: identity, hosting, distinct localities.
        identity_ok = (commit_matches((m["a_identity"].get("hpx_version_info") or {})
                                      .get("hpx_complete_version"))
                       and commit_matches((m["b_identity"].get("hpx_version_info") or {})
                                          .get("hpx_complete_version"))
                       and commit_matches((m.get("root_ready") or {}).get("hpx_complete_version")))
        hosting_ok = a_loc not in (None, 0) and b_loc not in (None, 0) and a_loc != b_loc

        if identity_ok and hosting_ok:
            _ray_get(ray_mod, a1.set_peer.remote(b1, b_loc), 30, "a_set_peer")
            _ray_get(ray_mod, b1.set_peer.remote(a1, a_loc), 30, "b_set_peer")
            meta = {"a": {"handle": a1, "pid": a_pid,
                          "host": (m["a_identity"] or {}).get("hostname"),
                          "node_id": (m["a_identity"] or {}).get("node_id"), "locality": a_loc},
                    "b": {"handle": b1, "pid": b_pid,
                          "host": (m["b_identity"] or {}).get("hostname"),
                          "node_id": (m["b_identity"] or {}).get("node_id"), "locality": b_loc}}

            # ----- BEGIN measurement body (identical to run_rep; the same-axis contract) -----
            m["timer"] = calibrate_timer()
            timer_med = m["timer"]["median_overhead_ns"]

            tiny = CORRECTNESS_MATRIX[0]
            oc = case_oracle(tiny)
            warm_ok = True
            for _ in range(3):
                _ray_get(ray_mod, a1.health.remote(), 30, "warm_a")
                _ray_get(ray_mod, b1.health.remote(), 30, "warm_b")
            for handle, direction in ((a1, "A"), (b1, "B")):
                wargs = shard_args(tiny, direction)
                for arm in ("ray", "hpx"):
                    for _ in range(2):
                        res = _ray_get(ray_mod, getattr(handle, arm + "_coordinate")
                                       .remote(*wargs), 60, f"warm_{arm}_{direction}")
                        if norm_pairs((res or {}).get("global_topk")) != oc["global"]:
                            warm_ok = False
            m["channel_warm_ok"] = warm_ok

            m["pregate"] = run_pregate(ray_mod, meta)

            if warm_ok and m["pregate"]["pass"]:
                for case in TIMED_MATRIX:
                    print(f"[exp69-xn]   rep {rep} timed A/{case['name']} "
                          f"(W={samp['A']['W']} K={samp['A']['K']}) ...", flush=True)
                    cr = run_timed_case(ray_mod, meta, "a", "b", case, samp["A"]["W"],
                                        samp["A"]["K"], "A", phase, timer_med, sample_writer, rep)
                    m["timed_A"].append(cr)
                    if cr.get("flags"):
                        break
                if samp.get("B") and all(not cr.get("flags") for cr in m["timed_A"]):
                    for case in [c for c in TIMED_MATRIX if c["name"] in B_CONTROL_CASES]:
                        print(f"[exp69-xn]   rep {rep} timed B/{case['name']} "
                              f"(W={samp['B']['W']} K={samp['B']['K']}) ...", flush=True)
                        cr = run_timed_case(ray_mod, meta, "b", "a", case, samp["B"]["W"],
                                            samp["B"]["K"], "B", phase, timer_med,
                                            sample_writer, rep)
                        m["timed_B"].append(cr)
                        if cr.get("flags"):
                            break
            # ----- END measurement body -----

        # Lifecycle: graceful leave -> destroy -> recreate ON NODE -> root completion -> orphans.
        a_stop = _ray_get(ray_mod, a1.stop_hpx.remote(), 40, "a_stop")
        b_stop = _ray_get(ray_mod, b1.stop_hpx.remote(), 40, "b_stop")
        m["a_stop_rc"], m["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
        m["b_stop_rc"], m["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")
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
                                       and ra.get("pid") and ra.get("pid") != a_pid)
        m["b_recreate_on_node"] = bool(_short(rb.get("hostname")) == _short(nodeB)
                                       and rb.get("pid") and rb.get("pid") != b_pid)

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
        sf.close()

    gates = eval_rep_gates_crossnode(m, expect)
    flags = collect_rep_flags_crossnode(m, gates)
    fc = classify_rep(gates, flags)
    return {
        "rep": rep, "boot_rel": os.path.relpath(boot, HERE), "ports": m["ports"],
        "samples_file_rel": os.path.relpath(samples_path, HERE),
        "gates": gates, "gates_failed": [k for k, v in gates.items() if not v],
        "flags": flags, "rep_pass": fc == "pass", "failure_class": fc,
        "timer": m.get("timer"),
        "pregate": {"pass": (m.get("pregate") or {}).get("pass"),
                    "cases": [{"name": c["name"], "failed": c["failed"]}
                              for c in (m.get("pregate") or {}).get("cases", [])]},
        "timed_A": m.get("timed_A"), "timed_B": m.get("timed_B"),
        "placement": {"a": m.get("a_placement"), "b": m.get("b_placement"),
                      "root_host": (m.get("root_ready") or {}).get("hostname"),
                      "a_recreate_on_node": m.get("a_recreate_on_node"),
                      "b_recreate_on_node": m.get("b_recreate_on_node")},
        "markers": m,
    }


def build_crossnode_aggregate(sampling_mode, samp, reps, provenance, cluster, preflight):
    overall_pass = bool(reps) and len(reps) == samp["reps"] and all(r["rep_pass"] for r in reps)
    slim_reps = []
    for r in reps:
        rr = {k: r[k] for k in ("rep", "boot_rel", "samples_file_rel", "gates", "gates_failed",
                                "flags", "rep_pass", "failure_class", "timer", "pregate",
                                "timed_A", "timed_B", "placement")}
        slim_reps.append(rr)
    cf = dict(CLAIM_FENCES); cf["not_cross_node"] = False  # this IS the cross-node evidence
    agg = {
        "experiment": "69_same_axis_topk_perf",
        "kind": ("slice0_smoke_crossnode_instrument_validation" if sampling_mode == "smoke"
                 else "slice1_same_axis_qd1_latency_crossnode"),
        "phase": "rostam-cross-node", "sampling_mode": sampling_mode,
        "single_node": False, "cross_node": True,
        "transport_hpx_arm": "HPX TCP parcelport (cross-node, per-node 10.42.5.x endpoints)",
        "transport_ray_arm": "Ray actor call path (cross-node, hard-placed actors)",
        "measured_contrast": MEASURED_CONTRAST,
        "shared_runtime_caveat": SHARED_RUNTIME_CAVEAT,
        "timed_boundary": TIMED_BOUNDARY,
        "hpx_arm_wait_strategy": (
            "hpx::post + action -> future -> .then continuation merge, UNTIMED merged.get() on the "
            "HPX thread; caller bound via std::future::wait_for on the calling OS thread (GIL "
            "released). The macOS sustained timed-wait crash is a separate diagnostic finding and "
            "does NOT alter this measured path (see the timed-wait diagnostic artifact)."),
        "arm_order_policy": "paired alternating: even pair Ray->HPX, odd pair HPX->Ray",
        "queue_depth": "QD1 (one request in flight; latency only; throughput deferred)",
        "generation_rule": GENERATION_RULE, "total_order": TOTAL_ORDER,
        "correctness_matrix": CORRECTNESS_MATRIX, "timed_matrix": TIMED_MATRIX,
        "b_control_cases": B_CONTROL_CASES, "sampling": samp,
        "timer_overhead_rule": f"flag case if per-rep p50 < {TIMER_OVERHEAD_P50_FACTOR}x median "
                               "adjacent-read overhead",
        "ratio_fences": dict(RATIO_FENCES), "claim_fences": cf,
        "failure_classes": list(CROSSNODE_FAILURE_CLASSES),
        "ray_cluster": cluster, "preflight": preflight,
        "provenance": provenance,
        "reps": slim_reps,
        "across_reps_A_coordinated": across_reps_stats(reps, "timed_A"),
        "across_reps_B_control_separate_never_pooled": across_reps_stats(reps, "timed_B"),
        "overall": "pass" if overall_pass else "fail",
    }
    if sampling_mode == "smoke":
        agg["smoke_note"] = ("Slice 0 cross-node instrument validation ONLY: no performance "
                             "conclusion, no ratio, no speedup, no winner; W/K/R too small for "
                             "distribution claims.")
        agg["safe_claim"] = ("Cross-node instrumentation validated (three-node hard placement, "
                             "timer, boundary, witnesses, pair scheduler, verification-after-t1, "
                             "lifecycle) on the tested three Rostam nodes." if overall_pass
                             else "Slice 0 cross-node did not pass; see reps[].failure_class.")
    else:
        r = cluster or {}
        agg["safe_claim"] = (
            "Across three Rostam nodes (root " + str(r.get("root_node")) + ", actor A "
            + str(r.get("actor_a_node")) + ", actor B " + str(r.get("actor_b_node")) + " on "
            + str((preflight or {}).get("subnet_prefix")) + "x), the identical deterministic exp68 "
            "vocabulary-sharded top-k request was measured at the same Python caller boundary "
            "through a Ray-mediated and an HPX-mediated peer-candidate path inside the same "
            "Ray-hosted, HPX-resident hard-placed actor topology; per-arm QD1 latency "
            "distributions were collected with every timed sample verified bit-exactly after t1. "
            "Per-arm distributions only; NO ratio, speedup, difference, or winner is computed or "
            "implied." if overall_pass
            else "Slice 1 cross-node did not pass; see reps[].failure_class / final_orphans.")
    return agg


def run_crossnode_phase(args):
    subnet = args.subnet_prefix
    sampling_mode = args.sampling
    samp = dict(SAMPLING[sampling_mode])
    if args.reps is not None:
        samp["reps"] = args.reps
    agg = {"experiment": "69_same_axis_topk_perf", "phase": "rostam-cross-node",
           "sampling_mode": sampling_mode, "single_node": False, "cross_node": True,
           "ratio_fences": dict(RATIO_FENCES)}
    try:
        import ray
    except Exception as ex:  # noqa: BLE001
        agg["overall"] = "skip"; agg["reason"] = f"ray unavailable: {ex}"
        out = args.aggregate or os.path.join(HERE, "_exp69_runs", "crossnode_skip_aggregate.json")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        _write_agg(out, agg); print("SKIP: ray unavailable"); return 0

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

    preflight = {"slurm_job_id": job_id, "slurm_nodelist": nodelist, "allocation_nodes": nodes,
                 "controller_host": here, "nodeR": nodeR, "nodeA": nodeA, "nodeB": nodeB,
                 "nodeR_ip": nodeR_ip, "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip,
                 "subnet_prefix": subnet}
    agg["preflight"] = preflight
    out_default = os.path.join(HERE, "_exp69_runs", f"crossnode_{job_id or 'noalloc'}_"
                               f"{time.strftime('%Y%m%dT%H%M%SZ')}", "crossnode_aggregate.json")
    out_path = args.aggregate or out_default
    if problems:
        agg["overall"] = "fail_preflight"; agg["preflight_problems"] = problems
        agg["failure_class"] = ("three_node_allocation_unavailable"
                                if any("node" in p for p in problems)
                                else ("subnet_mismatch" if any("subnet" in p for p in problems)
                                      else "invalid_instrumentation"))
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        _write_agg(out_path, agg); print(f"[exp69-crossnode] PREFLIGHT FAIL: {problems}"); return 0

    print(f"[exp69-crossnode] job {job_id} nodes {nodes} | root {nodeR}={nodeR_ip} "
          f"A {nodeA}={nodeA_ip} B {nodeB}={nodeB_ip} | sampling {sampling_mode} reps {samp['reps']}")
    env = dict(os.environ)
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    runs_root = os.path.join(HERE, "_exp69_runs", f"crossnode_{sampling_mode}_{job_id}_{runid}")
    os.makedirs(runs_root, exist_ok=True)
    out_path = args.aggregate or os.path.join(runs_root, "crossnode_aggregate.json")
    port = args.ray_port
    temp_dir = f"/tmp/exp69_ray_{job_id}_{runid}"
    port_flags = _ray_port_flags(args)
    head_proc = worker_a = worker_b = None
    reps, provenance = [], None
    fail_reason = None
    cluster = {}
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
            return [n for n in alive if n.get("NodeManagerAddress") == ip
                    or _short(n.get("NodeName")) == _short(host)]
        mR, mA, mB = _match(nodeR_ip, nodeR), _match(nodeA_ip, nodeA), _match(nodeB_ip, nodeB)
        nodeR_nid = mR[0]["NodeID"] if len(mR) == 1 else None
        nodeA_nid = mA[0]["NodeID"] if len(mA) == 1 else None
        nodeB_nid = mB[0]["NodeID"] if len(mB) == 1 else None
        driver = _self_identity(ray)
        cluster = {"root_node": nodeR, "actor_a_node": nodeA, "actor_b_node": nodeB,
                   "root_ip": nodeR_ip, "actor_a_ip": nodeA_ip, "actor_b_ip": nodeB_ip,
                   "nodeR_ray_node_id": nodeR_nid, "nodeA_ray_node_id": nodeA_nid,
                   "nodeB_ray_node_id": nodeB_nid, "driver_hostname": driver["hostname"],
                   "driver_node_id": driver["node_id"],
                   "driver_on_nodeR": bool(driver["node_id"] and driver["node_id"] == nodeR_nid),
                   "ray_nodes_raw": [{k: n.get(k) for k in ("NodeID", "NodeManagerAddress",
                                     "NodeName", "Alive")} for n in alive]}
        agg["ray_cluster"] = cluster
        if not (nodeA_nid and nodeB_nid and len({nodeR_nid, nodeA_nid, nodeB_nid}) == 3):
            raise RuntimeError(f"ray node-id resolution failed (mR={len(mR)} mA={len(mA)} mB={len(mB)})")
        if not cluster["driver_on_nodeR"]:
            raise RuntimeError("driver is not on nodeR (must be co-located with the Ray head)")

        HpxActor = build_actor_class(ray)
        expect = {"slurm_job_id": job_id, "nodes": nodes, "nodeR": nodeR, "nodeA": nodeA,
                  "nodeB": nodeB, "nodeR_nid": nodeR_nid, "nodeA_nid": nodeA_nid,
                  "nodeB_nid": nodeB_nid, "subnet": subnet}
        for r in range(1, samp["reps"] + 1):
            print(f"[exp69-crossnode] rep {r} ...", flush=True)
            rep = run_crossnode_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), r,
                                    runs_root, args.hpx_threads, args.ray_num_cpus, nodeR, nodeA,
                                    nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid, nodeB_nid,
                                    subnet, args.port_base, samp, sampling_mode, expect, env)
            if provenance is None:
                provenance = collect_provenance(ray, rep["markers"].get("a_identity"))
            with open(os.path.join(runs_root, f"rep_{r}", "markers.json"), "w") as f:
                json.dump(rep["markers"], f, indent=2)
            rep.pop("markers", None)
            print(f"[exp69-crossnode]   rep {r}: {'PASS' if rep['rep_pass'] else 'FAIL'} "
                  f"({rep['failure_class']}) gates_failed={rep['gates_failed']} "
                  f"flags={rep['flags']}", flush=True)
            reps.append(rep)
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

    full = build_crossnode_aggregate(sampling_mode, samp, reps, provenance, cluster, preflight)
    full["ray_cluster"] = agg.get("ray_cluster"); full["final_orphans"] = agg.get("final_orphans")
    full["ray_bringup"] = {k: agg.get(k) for k in ("ray_gcs_ready_from_nodeA", "ray_gcs_detail_A",
        "ray_gcs_ready_from_nodeB", "ray_gcs_detail_B", "ray_init_ok", "ray_init_attempts",
        "ray_nodes_ready", "ray_nodes_alive_seen")}
    if fail_reason:
        full["crossnode_exception"] = fail_reason
        full["crossnode_traceback"] = agg.get("crossnode_traceback")
    overall_pass = (fail_reason is None and len(reps) == samp["reps"] and all(r["rep_pass"] for r in reps)
                    and agg.get("final_orphans", {}).get("no_ray_orphans")
                    and agg.get("final_orphans", {}).get("no_peer_orphans"))
    full["overall"] = "pass" if overall_pass else "fail"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    _write_agg(out_path, full)
    print(f"[exp69-crossnode] overall: {full['overall']} -> {out_path}")
    # The curated cross-node aggregate is written ONLY by a passing ACCEPTED run with no override.
    if sampling_mode == "accepted" and full["overall"] == "pass" and args.aggregate is None:
        curated = os.path.join(HERE, "same_axis_topk_qd1_crossnode_aggregate.json")
        _write_agg(curated, full)
        print(f"[exp69-crossnode] curated cross-node aggregate -> {curated}")
    return 0 if full["overall"] == "pass" else 1


# =======================================================================================
# Slice 2: closed-loop bounded-concurrency GOODPUT + latency-under-load (EXPERIMENT-ONLY).
#
# Additive over Slice 1: reuses the exact same actors, HPX island, hard placement, correctness
# oracle, and mechanism witnesses, but drives a CLOSED-LOOP fixed-count scheduler that keeps C
# requests in flight and enqueues each completed result to ONE bounded verifier thread. Every ratio
# fence stays FALSE; no throughput/latency ratio, difference, or winner is computed. Slice 1 QD1
# scheduling, gate evaluators, aggregate builders, and curated artifacts are untouched.
# =======================================================================================

THROUGHPUT_CASES = ["P0", "P2", "P3b"]     # first accepted throughput matrix (A-coordinated)
THROUGHPUT_CONCURRENCY = [2, 4]            # first accepted bands
THROUGHPUT_SAMPLING = {
    "smoke":    {"warmup_n": 10,  "N": 50,   "reps": 1},
    "accepted": {"warmup_n": 100, "N": 1000, "reps": 3},
}
VERIFIER_QUEUE_CAPACITY = 256              # bounded; high-water < capacity and enqueue must not block
SOAK_CASE = "P3b"                          # moderate case used for the per-C concurrency-safety soak
CAPABILITY_CASE = SOAK_CASE                # P3b: the case carrying the peer-concurrency capability proof
CAPABILITY_CONCURRENCY = 4                 # the capability proof is the P3b soak at C>=4 (the C=4 island)


def _capability_regime(case_name, C):
    """Slice 2: the ONLY regime in which peer-action overlap is a REQUIRED concurrency-capability
    proof rather than an observation. The P3b soak at C>=4 has a long-enough peer service window and
    enough concurrent demand to reliably witness two peer action bodies coexisting. Short (P0) or
    low-C regimes cannot, so there peer overlap is RECORDED but never gated -- active_max==1 there
    means only that no two short action bodies happened to coexist, NOT that the path is serialized."""
    return case_name == CAPABILITY_CASE and C >= CAPABILITY_CONCURRENCY


def peer_overlap_annotations(observed):
    """Non-gating batch annotation. Emitted iff no simultaneous peer-action body execution was
    observed in the batch; the valid capability evidence is the repetition-level P3b/C=4 soak."""
    return [] if observed else ["peer_overlap_not_observed_short_service_window"]


THROUGHPUT_FAILURE_CLASSES = [
    "concurrency_soak_failed", "reentrancy_unsafe", "coordinator_service_serialized",
    "ray_peer_concurrency_capability_not_proven", "hpx_peer_concurrency_capability_not_proven",
    "peer_concurrency_capability_not_available", "active_call_counter_underflow",
    "active_call_counter_leak", "witness_counter_race_detected", "achieved_concurrency_below_target",
    "verifier_backpressure_contaminated_batch", "verifier_exception",
    "throughput_batch_invalidated_by_invalid_result", "hpx_thread_starvation_under_load",
    "latency_under_load_mixed_with_qd1", "oversubscription_gate_failed",
    "peer_config_mutated_during_measurement", "ray_peer_used_by_hpx_arm",
    "hpx_action_used_by_ray_arm", "hpx_identity_mismatch", "actor_placement_invalid",
    "three_node_allocation_unavailable", "subnet_mismatch", "ray_cluster_failed",
    "shared_island_join_failed", "timeout", "lifecycle_failed", "actor_recreation_failed",
    "orphan_detected", "invalid_instrumentation", "pass",
]


# ---------------------------------------------------------------------------------------
# One dedicated bounded verifier worker thread. The scheduler performs NO oracle computation; it
# enqueues (metadata, result, t0, t1) after t1 + pipeline replenishment. Backlog stays bounded.
# ---------------------------------------------------------------------------------------

class ThroughputVerifier:
    def __init__(self, capacity, verify):
        self.q = queue.Queue(maxsize=capacity)
        self.capacity = capacity
        self._verify = verify
        self.verified = 0
        self.invalid = 0
        self.first_invalid = None
        self.exception = None
        self.high_water = 0
        self.enqueue_block_ns = 0
        self.enqueue_attempts = 0
        self.max_verify_latency_ns = 0
        self._stop = object()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def _run(self):
        while True:
            item = self.q.get()
            if item is self._stop:
                self.q.task_done(); return
            try:
                a = time.perf_counter_ns()
                ok, reasons = self._verify(item)
                dt = time.perf_counter_ns() - a
                if dt > self.max_verify_latency_ns:
                    self.max_verify_latency_ns = dt
                if ok:
                    self.verified += 1
                else:
                    self.invalid += 1
                    if self.first_invalid is None:
                        self.first_invalid = {"req_id": item.get("req_id"), "reasons": reasons}
            except Exception as ex:  # noqa: BLE001
                self.exception = f"{type(ex).__name__}: {ex}"
            finally:
                self.q.task_done()

    def enqueue(self, item):
        self.enqueue_attempts += 1
        qs = self.q.qsize()
        if qs > self.high_water:
            self.high_water = qs
        try:
            self.q.put_nowait(item)   # must never block on the scheduler thread
        except queue.Full:
            a = time.perf_counter_ns()
            self.q.put(item)          # blocked: contaminated batch (recorded + gated)
            self.enqueue_block_ns += time.perf_counter_ns() - a

    def finish(self, timeout=180):
        self.q.put(self._stop)
        self._thread.join(timeout=timeout)


def _tp_verify_fn(oracle_global, expect_peer, expect_own):
    def _v(item):
        return verify_timed_result(item["arm"], item["result"], oracle_global, expect_peer, expect_own)
    return _v


def _closed_loop_warmup(ray_mod, method, args, C, warmup_n, arm, oracle_global, expect_peer, expect_own):
    """Discarded warm-up batch at the same C: results ARE verified inline (fail-closed), timings
    discarded, and the mechanism/active counters snapshotted only AFTER this returns."""
    inflight = {}
    submitted = completed = 0
    for _ in range(min(C, warmup_n)):
        inflight[method.remote(*args)] = True; submitted += 1
    while completed < warmup_n:
        ready, _ = ray_mod.wait(list(inflight.keys()), num_returns=1, timeout=120)
        if not ready:
            return False
        ref = ready[0]; inflight.pop(ref, None)
        result = ray_mod.get(ref, timeout=120); completed += 1
        ok, _ = verify_timed_result(arm, result, oracle_global, expect_peer, expect_own)
        if not ok:
            return False
        if submitted < warmup_n:
            inflight[method.remote(*args)] = True; submitted += 1
    return True


def eval_soak_gates(arm, case_name, C, completed, n_soak, bad, aw_c, aw_p, h_c, h_p):
    """Pure PASS/FAIL evaluation for one concurrency soak (no Ray/HPX). Coordinator overlap, exact
    correctness, stable membership, and leak/underflow-free counters are ALWAYS required. Peer-action
    overlap (ray_peer/hpx_action active max >= 2) is a REQUIRED capability proof only in the capability
    regime (P3b, C>=4); elsewhere it is recorded (peer_overlap_observed) but never gates the soak."""
    coord_overlap = (aw_c.get("coordinate_active_max") or 0) >= 2
    peer_max = (aw_p.get("ray_peer_active_max") or 0) if arm == "ray" \
        else (aw_p.get("hpx_action_active_max") or 0)
    peer_overlap_observed = peer_max >= 2
    capability_regime = _capability_regime(case_name, C)
    cur_ok = (aw_c.get("coordinate_active_current") == 0 and aw_c.get("ray_peer_active_current") == 0
              and (aw_c.get("hpx_action_active_current") in (0, None))
              and aw_p.get("coordinate_active_current") == 0 and aw_p.get("ray_peer_active_current") == 0
              and (aw_p.get("hpx_action_active_current") in (0, None)))
    underflow = bool(aw_c.get("active_underflow_seen") or aw_p.get("active_underflow_seen"))
    stable = (h_c.get("membership") == 3 and h_p.get("membership") == 3)
    peer_required_ok = peer_overlap_observed or not capability_regime
    ok = (not bad) and completed == n_soak and coord_overlap and peer_required_ok and cur_ok \
        and not underflow and stable
    cls = "pass"
    if bad:
        cls = "concurrency_soak_failed"
    elif not stable:
        cls = "reentrancy_unsafe"
    elif underflow:
        cls = "active_call_counter_underflow"
    elif not cur_ok:
        cls = "active_call_counter_leak"
    elif not coord_overlap:
        cls = "coordinator_service_serialized"
    elif capability_regime and not peer_overlap_observed:
        cls = "ray_peer_concurrency_capability_not_proven" if arm == "ray" \
            else "hpx_peer_concurrency_capability_not_proven"
    return {"pass": ok, "failure_class": cls if not ok else "pass",
            "coordinate_active_max": aw_c.get("coordinate_active_max"),
            "ray_peer_active_max": aw_p.get("ray_peer_active_max"),
            "hpx_action_active_max": aw_p.get("hpx_action_active_max"),
            "peer_overlap_observed": peer_overlap_observed,
            "capability_regime": capability_regime,
            "capability_proven": (peer_overlap_observed if capability_regime else None)}


def concurrency_soak(ray_mod, meta, coord_key, peer_key, arm, case, C):
    """Overlapping-call soak through one arm at concurrency C: exact correctness for every request +
    GENUINE service overlap from the server-side active-call witnesses + stability + no leak."""
    coord = meta[coord_key]["handle"]; peer = meta[peer_key]["handle"]
    peer_meta, own_meta = meta[peer_key], meta[coord_key]
    direction = "A" if coord_key == "a" else "B"
    args = shard_args(case, direction); oracle_global = case_oracle(case)["global"]
    method = getattr(coord, arm + "_coordinate")
    _ray_get(ray_mod, coord.reset_active.remote(), 30, "soak_reset_coord")
    _ray_get(ray_mod, peer.reset_active.remote(), 30, "soak_reset_peer")
    n_soak = max(4 * C, 20)
    inflight = {}; submitted = completed = 0; bad = []
    for _ in range(min(C, n_soak)):
        inflight[method.remote(*args)] = True; submitted += 1
    while completed < n_soak:
        ready, _ = ray_mod.wait(list(inflight.keys()), num_returns=1, timeout=120)
        if not ready:
            bad.append("timeout"); break
        ref = ready[0]; inflight.pop(ref, None)
        try:
            result = ray_mod.get(ref, timeout=120)
        except Exception as ex:  # noqa: BLE001
            bad.append(f"get:{type(ex).__name__}"); break
        completed += 1
        ok, reasons = verify_timed_result(arm, result, oracle_global, peer_meta, own_meta)
        if not ok:
            bad.append(f"verify:{reasons}"); break
        if submitted < n_soak:
            inflight[method.remote(*args)] = True; submitted += 1
    aw_c = _ray_get(ray_mod, coord.active_witness.remote(), 30, "soak_aw_coord") or {}
    aw_p = _ray_get(ray_mod, peer.active_witness.remote(), 30, "soak_aw_peer") or {}
    h_c = _ray_get(ray_mod, coord.health.remote(), 30, "soak_h_coord") or {}
    h_p = _ray_get(ray_mod, peer.health.remote(), 30, "soak_h_peer") or {}
    ev = eval_soak_gates(arm, case["name"], C, completed, n_soak, bad, aw_c, aw_p, h_c, h_p)
    return {"arm": arm, "C": C, "case": case["name"], "n_soak": n_soak, "completed": completed,
            "bad": bad, **ev}


def eval_throughput_batch_gates(bm):
    arm, C, N = bm["arm"], bm["C"], bm["N"]
    v = bm.get("verifier") or {}
    wd = bm.get("witness_deltas") or {}
    aw = bm.get("active_witness") or {}
    out = bm.get("outstanding") or {}
    g = {}
    g["full_completions"] = (bm.get("n_completed") == N and bm.get("timeouts", 1) == 0
                             and "timeout" not in bm.get("flags", []))
    g["all_verified"] = (v.get("verified") == N and v.get("invalid") == 0 and v.get("exception") is None)
    g["verifier_bounded"] = (v.get("high_water", 10 ** 9) < v.get("capacity", 0)
                             and v.get("enqueue_block_ns", 1) == 0)
    if arm == "ray":
        g["witness_deltas_ok"] = (wd.get("coord_outgoing_ray_fetch") == N
                                  and wd.get("peer_serving_ray_fetch") == N
                                  and wd.get("peer_hpx_actions_served") == 0
                                  and wd.get("coord_serving_ray_fetch") == 0
                                  and wd.get("coord_hpx_actions_served") == 0
                                  and wd.get("peer_outgoing_ray_fetch") == 0)
    else:
        g["witness_deltas_ok"] = (wd.get("peer_hpx_actions_served") == N
                                  and wd.get("coord_outgoing_ray_fetch") == 0
                                  and wd.get("peer_serving_ray_fetch") == 0
                                  and wd.get("coord_serving_ray_fetch") == 0
                                  and wd.get("coord_hpx_actions_served") == 0
                                  and wd.get("peer_outgoing_ray_fetch") == 0)
    # Coordinator overlap is ALWAYS mandatory for C>1 (proves the actor genuinely services overlapping
    # requests, not merely queued object refs). Mechanism-specific peer overlap is OBSERVATION-ONLY for
    # a measured batch (recorded as bm["peer_overlap_observed"]); the peer-concurrency capability is
    # proven separately by the repetition-level P3b/C=4 soak and linked in as capability_proof_available.
    g["coordinator_service_overlap"] = ((aw.get("coordinate_active_max") or 0) >= 2) if C > 1 else True
    cc, pc = aw.get("coord_currents") or {}, aw.get("peer_currents") or {}
    g["no_counter_leak"] = all(cc.get(k) == 0 for k in cc) and all(pc.get(k) == 0 for k in pc)
    g["no_counter_underflow"] = not (aw.get("coord_underflow") or aw.get("peer_underflow"))
    g["achieved_concurrency_ok"] = (out.get("mean") or 0) >= (C - 0.5)
    g["peer_immutable"] = bool(bm.get("peer_frozen"))
    return g


def classify_throughput_batch(bm, g):
    if all(g.values()):
        return "pass"
    v = bm.get("verifier") or {}
    wd = bm.get("witness_deltas") or {}
    if not g["verifier_bounded"]:
        return "verifier_backpressure_contaminated_batch"
    if v.get("exception"):
        return "verifier_exception"
    if not g["all_verified"]:
        return "throughput_batch_invalidated_by_invalid_result"
    if not g["no_counter_underflow"]:
        return "active_call_counter_underflow"
    if not g["no_counter_leak"]:
        return "active_call_counter_leak"
    if not g["witness_deltas_ok"]:
        if bm["arm"] == "ray" and wd.get("peer_hpx_actions_served", 0) != 0:
            return "hpx_action_used_by_ray_arm"
        if bm["arm"] == "hpx" and (wd.get("peer_serving_ray_fetch", 0) != 0
                                   or wd.get("coord_outgoing_ray_fetch", 0) != 0):
            return "ray_peer_used_by_hpx_arm"
        return "invalid_instrumentation"
    if not g["coordinator_service_overlap"]:
        return "coordinator_service_serialized"
    if not g.get("capability_proof_available", True):
        return "peer_concurrency_capability_not_available"
    if not g["achieved_concurrency_ok"]:
        return "achieved_concurrency_below_target"
    if not g["full_completions"]:
        return "timeout"
    if not g["peer_immutable"]:
        return "peer_config_mutated_during_measurement"
    return "invalid_instrumentation"


def run_throughput_batch(ray_mod, meta, coord_key, peer_key, arm, case, C, N, warmup_n,
                         sampling_mode, sample_writer, rep):
    """One (arm, case, C, rep) closed-loop fixed-count batch. t1 is stamped AFTER ray.get(ref);
    replenishment happens before the completed result is enqueued to the bounded verifier."""
    coord = meta[coord_key]["handle"]
    peer_meta, own_meta = meta[peer_key], meta[coord_key]
    direction = "A" if coord_key == "a" else "B"
    args = shard_args(case, direction)
    oracle_global = case_oracle(case)["global"]
    method = getattr(coord, arm + "_coordinate")
    bm = {"arm": arm, "case": case["name"], "C": C, "N": N, "warmup_n": warmup_n,
          "direction": direction, "rep": rep, "sampling_mode": sampling_mode,
          "flags": [], "timeouts": 0, "peer_frozen": True}

    # (2) discarded warm-up batch at the same C (results verified inline; timings discarded)
    try:
        if not _closed_loop_warmup(ray_mod, method, args, C, warmup_n, arm, oracle_global,
                                   peer_meta, own_meta):
            bm["flags"].append("throughput_batch_invalidated_by_invalid_result")
            bm["warmup_verify_failed"] = True
            bm["gates"] = {"warmup": False}; bm["gates_failed"] = ["warmup"]
            bm["failure_class"] = "throughput_batch_invalidated_by_invalid_result"
            bm["batch_pass"] = False
            return bm
    except Exception as ex:  # noqa: BLE001
        bm["flags"].append("timeout"); bm["warmup_error"] = f"{type(ex).__name__}: {ex}"
        bm["gates"] = {"warmup": False}; bm["gates_failed"] = ["warmup"]
        bm["failure_class"] = "timeout"; bm["batch_pass"] = False
        return bm

    # (3) reset active maxes BETWEEN warm-up and measured (currents must be 0) + snapshot counters
    _ray_get(ray_mod, coord.reset_active.remote(), 30, "reset_coord")
    _ray_get(ray_mod, meta[peer_key]["handle"].reset_active.remote(), 30, "reset_peer")
    c_before = _ray_get(ray_mod, coord.counters.remote(), 30, "c_before")
    p_before = _ray_get(ray_mod, meta[peer_key]["handle"].counters.remote(), 30, "p_before")

    verifier = ThroughputVerifier(VERIFIER_QUEUE_CAPACITY,
                                  _tp_verify_fn(oracle_global, peer_meta, own_meta))
    verifier.start()
    inflight = {}
    submitted = completed = 0
    latencies, outstanding_samples = [], []

    # (4) start_ns immediately before submitting the first measured request; (5) issue exactly N
    start_ns = time.perf_counter_ns()
    for _ in range(min(C, N)):
        t0 = time.perf_counter_ns()
        inflight[method.remote(*args)] = {"t0": t0, "req_id": submitted}; submitted += 1
    while completed < N:
        ready, _ = ray_mod.wait(list(inflight.keys()), num_returns=1, timeout=120)
        if not ready:
            bm["timeouts"] += 1; bm["flags"].append("timeout"); break
        ref = ready[0]; req = inflight.pop(ref)
        outstanding_samples.append(len(inflight) + 1)  # outstanding just before this slot frees
        try:
            result = ray_mod.get(ref, timeout=120)
        except Exception as ex:  # noqa: BLE001
            bm["timeouts"] += 1; bm["flags"].append("timeout")
            bm["get_error"] = f"{type(ex).__name__}: {ex}"; break
        t1 = time.perf_counter_ns()                     # (2) stamp AFTER ray.get returns
        latencies.append(t1 - req["t0"]); completed += 1
        if submitted < N:                               # replenish immediately, before verification
            t0 = time.perf_counter_ns()
            inflight[method.remote(*args)] = {"t0": t0, "req_id": submitted}; submitted += 1
        verifier.enqueue({"arm": arm, "result": result, "req_id": req["req_id"]})
        sample_writer({"rep": rep, "case": case["name"], "arm": arm, "C": C, "direction": direction,
                       "sampling_mode": sampling_mode, "req_id": req["req_id"],
                       "completion_index": completed, "t0_ns": req["t0"], "t1_ns": t1,
                       "latency_ns": t1 - req["t0"], "outstanding": outstanding_samples[-1]})
    end_ns = time.perf_counter_ns()                     # (7) after the Nth ray.get returned
    verifier.finish()                                   # (8) wait for the verifier to drain

    c_after = _ray_get(ray_mod, coord.counters.remote(), 30, "c_after")
    p_after = _ray_get(ray_mod, meta[peer_key]["handle"].counters.remote(), 30, "p_after")
    aw_c = _ray_get(ray_mod, coord.active_witness.remote(), 30, "aw_coord") or {}
    aw_p = _ray_get(ray_mod, meta[peer_key]["handle"].active_witness.remote(), 30, "aw_peer") or {}

    elapsed_s = (end_ns - start_ns) / 1e9
    bm["start_ns"], bm["end_ns"], bm["elapsed_s"] = start_ns, end_ns, elapsed_s
    bm["completion_rate_per_s"] = (completed / elapsed_s) if elapsed_s > 0 else None
    bm["n_completed"] = completed
    bm["latency_stats"] = stats_block(latencies, N, sampling_mode) if latencies else None
    bm["verifier"] = {"capacity": verifier.capacity, "high_water": verifier.high_water,
                      "enqueue_block_ns": verifier.enqueue_block_ns,
                      "enqueue_attempts": verifier.enqueue_attempts, "verified": verifier.verified,
                      "invalid": verifier.invalid, "first_invalid": verifier.first_invalid,
                      "exception": verifier.exception,
                      "max_verify_latency_ns": verifier.max_verify_latency_ns}

    def d(before, after, key):
        return int((after or {}).get(key, 0)) - int((before or {}).get(key, 0))
    bm["witness_deltas"] = {
        "coord_outgoing_ray_fetch": d(c_before, c_after, "outgoing_ray_fetch"),
        "peer_serving_ray_fetch": d(p_before, p_after, "serving_ray_fetch"),
        "peer_hpx_actions_served": d(p_before, p_after, "hpx_actions_served"),
        "coord_serving_ray_fetch": d(c_before, c_after, "serving_ray_fetch"),
        "coord_hpx_actions_served": d(c_before, c_after, "hpx_actions_served"),
        "peer_outgoing_ray_fetch": d(p_before, p_after, "outgoing_ray_fetch")}
    bm["active_witness"] = {
        "coordinate_active_max": aw_c.get("coordinate_active_max"),
        "ray_peer_active_max": aw_p.get("ray_peer_active_max"),
        "hpx_action_active_max": aw_p.get("hpx_action_active_max"),
        "coord_currents": {"coordinate_active_current": aw_c.get("coordinate_active_current"),
                           "ray_peer_active_current": aw_c.get("ray_peer_active_current"),
                           "hpx_action_active_current": aw_c.get("hpx_action_active_current")},
        "peer_currents": {"coordinate_active_current": aw_p.get("coordinate_active_current"),
                          "ray_peer_active_current": aw_p.get("ray_peer_active_current"),
                          "hpx_action_active_current": aw_p.get("hpx_action_active_current")},
        "coord_underflow": aw_c.get("active_underflow_seen"),
        "peer_underflow": aw_p.get("active_underflow_seen")}
    bm["outstanding"] = {
        "mean": (sum(outstanding_samples) / len(outstanding_samples)) if outstanding_samples else None,
        "max": max(outstanding_samples) if outstanding_samples else None,
        "fraction_at_C": (sum(1 for o in outstanding_samples if o >= C) / len(outstanding_samples))
        if outstanding_samples else None, "target_C": C}
    _peer_max = (bm["active_witness"].get("ray_peer_active_max") or 0) if arm == "ray" \
        else (bm["active_witness"].get("hpx_action_active_max") or 0)
    bm["peer_overlap_observed"] = _peer_max >= 2
    bm["annotations"] = peer_overlap_annotations(bm["peer_overlap_observed"])
    gates = eval_throughput_batch_gates(bm)
    bm["gates"] = gates
    bm["gates_failed"] = [k for k, v in gates.items() if not v]
    # capability_proof_available is a CROSS-ISLAND gate filled in post-hoc by apply_capability_linkage
    # (the C=4 P3b soak of THIS repetition); at batch time it defaults available so local gates classify.
    bm["failure_class"] = classify_throughput_batch(bm, gates)
    bm["batch_pass"] = bm["failure_class"] == "pass"
    return bm


def eval_throughput_island_gates(m, expect):
    ai, bi = m.get("a_identity") or {}, m.get("b_identity") or {}
    as_, bs = m.get("a_start") or {}, m.get("b_start") or {}
    rr, rf = m.get("root_ready") or {}, m.get("root_final") or {}
    ap_a, ap_b = m.get("a_placement") or {}, m.get("b_placement") or {}
    g = {}
    g["fixed_hpx_commit"] = bool(commit_matches((ai.get("hpx_version_info") or {}).get("hpx_complete_version"))
                                 and commit_matches((bi.get("hpx_version_info") or {}).get("hpx_complete_version"))
                                 and commit_matches(rr.get("hpx_complete_version")))
    g["actors_distinct_nonzero_localities"] = bool(as_.get("locality_id") not in (None, 0)
                                                   and bs.get("locality_id") not in (None, 0)
                                                   and as_.get("locality_id") != bs.get("locality_id"))
    g["both_in_process_no_hpx_child"] = bool((m.get("a_child_report") or {}).get("hpx_children") == []
                                             and (m.get("b_child_report") or {}).get("hpx_children") == [])
    g["membership_reached_three"] = bool(rf.get("max_membership", 0) >= 3)
    g["root_isolated_workfree"] = bool(rr.get("locality_id") == 0 and rf.get("actions_served_on_root", -1) == 0)
    g["root_on_root_node"] = bool(_short(rr.get("hostname")) == _short(expect.get("nodeR")))
    g["actor_a_hard_placed"] = bool(ap_a.get("placement_soft") is False
                                    and ap_a.get("node_id") == expect.get("nodeA_nid")
                                    and _short(ap_a.get("hostname")) == _short(expect.get("nodeA")))
    g["actor_b_hard_placed"] = bool(ap_b.get("placement_soft") is False
                                    and ap_b.get("node_id") == expect.get("nodeB_nid")
                                    and _short(ap_b.get("hostname")) == _short(expect.get("nodeB")))
    g["actor_nodes_distinct"] = bool(_short(ap_a.get("hostname"))
                                     and _short(ap_a.get("hostname")) != _short(ap_b.get("hostname")))
    g["endpoints_pinned_subnet"] = bool(m.get("endpoints_subnet_ok"))
    g["peer_frozen_both"] = bool(m.get("a_peer_frozen") and m.get("b_peer_frozen"))
    g["pregate_seven_cases_pass"] = bool((m.get("pregate") or {}).get("pass")
                                         and len((m.get("pregate") or {}).get("cases") or []) == len(CORRECTNESS_MATRIX))
    g["resources_parity"] = bool(m.get("actor_hpx_threads") is not None
                                 and m.get("actor_ray_num_cpus") is not None
                                 and m.get("max_concurrency") is not None
                                 and m.get("resources_single_constants") is True)
    g["graceful_leave_clean_stop"] = bool(m.get("a_stop_rc") == 0 and m.get("b_stop_rc") == 0
                                          and not m.get("a_stop_error") and not m.get("b_stop_error"))
    g["root_finalized_clean"] = bool(m.get("root_exit_path") == "finalized_clean"
                                     and rf.get("leave_observed") is True and rf.get("final_membership") == 1)
    g["actors_recreated_on_node"] = bool(m.get("a_recreate_on_node") and m.get("b_recreate_on_node"))
    g["no_orphans"] = bool(m.get("orphans") == [] and m.get("actor_pids_gone") is True)
    g["remote_orphan_check_ran"] = bool(m.get("remote_orphan_check_ran"))
    g["ratio_fences_false"] = (all(v is False for v in (m.get("ratio_fences") or {}).values())
                               and set(m.get("ratio_fences") or {}) == set(RATIO_FENCES))
    g["soaks_passed"] = bool(m.get("soaks_all_passed"))
    g["all_batches_passed"] = bool(m.get("all_batches_passed"))
    return g


def run_throughput_rep(ray_mod, HpxActor, peer, build_dir, C, rep, runs_root, hpx_threads, ray_cpus,
                       samp, cases, nodeR, nodeA, nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid,
                       nodeB_nid, subnet, port_base, expect, env):
    """One (C, rep) island: hard-placed root+A+B at max_concurrency=C, frozen peers, 7-case
    correctness pre-gate, per-arm concurrency soak, then closed-loop batches for each case."""
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    boot = os.path.join(runs_root, f"C{C}_rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    base = port_base + C * 1000 + rep * 10
    p_root, p_a, p_b = base, base + 1, base + 2
    rcmd = crossnode_root_cmd(peer, boot, nodeR_ip, p_root, leave_timeout=45)
    ep_a = crossnode_actor_endpoints(nodeR_ip, p_root, nodeA_ip, p_a)
    ep_b = crossnode_actor_endpoints(nodeR_ip, p_root, nodeB_ip, p_b)
    strat_a = NodeAffinitySchedulingStrategy(node_id=nodeA_nid, soft=False)
    strat_b = NodeAffinitySchedulingStrategy(node_id=nodeB_nid, soft=False)
    m = {"controller_hostname": socket.gethostname(), "controller_pid": os.getpid(),
         "C": C, "rep": rep, "ports": {"root": p_root, "actor_a": p_a, "actor_b": p_b},
         "endpoints_subnet_ok": bool(nodeR_ip.startswith(subnet) and nodeA_ip.startswith(subnet)
                                     and nodeB_ip.startswith(subnet)),
         "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus, "max_concurrency": C,
         "resources_single_constants": True, "ratio_fences": dict(RATIO_FENCES),
         "remote_orphan_check_ran": False, "soaks": [], "batches": []}
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    samples_path = os.path.join(boot, "samples.jsonl")
    sf = open(samples_path, "w")

    def sample_writer(row):
        sf.write(json.dumps(row) + "\n")

    def _kill_dead(actor):
        ray_mod.kill(actor)
        try:
            ray_mod.get(actor.ping.remote(), timeout=10); return False
        except Exception:  # noqa: BLE001
            return True

    try:
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file_nfs(os.path.join(boot, "root.ready"), 60, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        a1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0, max_concurrency=C,
                              scheduling_strategy=strat_a).remote(build_dir, hpx_threads, ep_a)
        pa = dict(_ray_get(ray_mod, a1.ray_placement.remote(), 60, "a_placement") or {})
        pa["placement_soft"] = False; m["a_placement"] = pa
        m["a_identity"] = _ray_get(ray_mod, a1.load_identity.remote(), 60, "a_identity")
        m["a_start"] = _ray_get(ray_mod, a1.start_hpx.remote(), 120, "a_start")
        m["a_child_report"] = _ray_get(ray_mod, a1.child_report.remote(), 30, "a_child_report")
        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0, max_concurrency=C,
                              scheduling_strategy=strat_b).remote(build_dir, hpx_threads, ep_b)
        pb = dict(_ray_get(ray_mod, b1.ray_placement.remote(), 60, "b_placement") or {})
        pb["placement_soft"] = False; m["b_placement"] = pb
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 60, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 120, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")

        a_loc = (m["a_start"] or {}).get("locality_id")
        b_loc = (m["b_start"] or {}).get("locality_id")
        a_pid = (m.get("a_identity") or {}).get("pid")
        b_pid = (m.get("b_identity") or {}).get("pid")
        identity_ok = (commit_matches((m["a_identity"].get("hpx_version_info") or {}).get("hpx_complete_version"))
                       and commit_matches((m["b_identity"].get("hpx_version_info") or {}).get("hpx_complete_version"))
                       and commit_matches((m.get("root_ready") or {}).get("hpx_complete_version")))
        hosting_ok = a_loc not in (None, 0) and b_loc not in (None, 0) and a_loc != b_loc

        soaks_all_passed = all_batches_passed = False
        if identity_ok and hosting_ok:
            _ray_get(ray_mod, a1.set_peer.remote(b1, b_loc), 30, "a_set_peer")
            _ray_get(ray_mod, b1.set_peer.remote(a1, a_loc), 30, "b_set_peer")
            fa = _ray_get(ray_mod, a1.freeze_peer.remote(), 30, "a_freeze")
            fb = _ray_get(ray_mod, b1.freeze_peer.remote(), 30, "b_freeze")
            m["a_peer_frozen"] = bool((fa or {}).get("peer_frozen"))
            m["b_peer_frozen"] = bool((fb or {}).get("peer_frozen"))
            meta = {"a": {"handle": a1, "pid": a_pid,
                          "host": (m["a_identity"] or {}).get("hostname"),
                          "node_id": (m["a_identity"] or {}).get("node_id"), "locality": a_loc},
                    "b": {"handle": b1, "pid": b_pid,
                          "host": (m["b_identity"] or {}).get("hostname"),
                          "node_id": (m["b_identity"] or {}).get("node_id"), "locality": b_loc}}

            m["pregate"] = run_pregate(ray_mod, meta)   # untimed 7-case correctness, every rep
            if m["pregate"]["pass"]:
                soak_case = next(c for c in TIMED_MATRIX if c["name"] == SOAK_CASE)
                soaks_all_passed = True
                for arm in ("ray", "hpx"):
                    s = concurrency_soak(ray_mod, meta, "a", "b", arm, soak_case, C)
                    m["soaks"].append(s)
                    if not s["pass"]:
                        soaks_all_passed = False
                if soaks_all_passed:
                    all_batches_passed = True
                    for case in cases:
                        for arm in ("ray", "hpx"):
                            print(f"[exp69-tput]   C={C} rep={rep} {arm}/{case['name']} "
                                  f"(N={samp['N']} warm={samp['warmup_n']}) ...", flush=True)
                            bmr = run_throughput_batch(ray_mod, meta, "a", "b", arm, case, C,
                                                       samp["N"], samp["warmup_n"], samp["mode"],
                                                       sample_writer, rep)
                            m["batches"].append(bmr)
                            if not bmr["batch_pass"]:
                                all_batches_passed = False
        m["soaks_all_passed"] = soaks_all_passed
        m["all_batches_passed"] = all_batches_passed

        # Lifecycle: graceful leave -> destroy -> recreate on node -> root completion -> orphans.
        a_stop = _ray_get(ray_mod, a1.stop_hpx.remote(), 40, "a_stop")
        b_stop = _ray_get(ray_mod, b1.stop_hpx.remote(), 40, "b_stop")
        m["a_stop_rc"], m["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
        m["b_stop_rc"], m["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")
        dead_a1 = _kill_dead(a1); a1 = None
        dead_b1 = _kill_dead(b1); b1 = None
        a2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0, max_concurrency=C,
                              scheduling_strategy=strat_a).remote(build_dir, hpx_threads, ep_a)
        b2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0, max_concurrency=C,
                              scheduling_strategy=strat_b).remote(build_dir, hpx_threads, ep_b)
        m["a_recreate"] = _ray_get(ray_mod, a2.ping.remote(), 60, "a_recreate")
        m["b_recreate"] = _ray_get(ray_mod, b2.ping.remote(), 60, "b_recreate")
        ra, rb = m["a_recreate"] or {}, m["b_recreate"] or {}
        m["a_recreate_on_node"] = bool(_short(ra.get("hostname")) == _short(nodeA)
                                       and ra.get("pid") and ra.get("pid") != a_pid)
        m["b_recreate_on_node"] = bool(_short(rb.get("hostname")) == _short(nodeB)
                                       and rb.get("pid") and rb.get("pid") != b_pid)
        open(os.path.join(boot, "root.done"), "w").close()
        _wait_for_file_nfs(os.path.join(boot, "root.final"), 60, procs=[root])
        m["root_final"] = _read_json(os.path.join(boot, "root.final"))
        r_exited, r_rc, r_killed = _wait_proc(root, time.time() + 60)
        m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)
        dead_a2 = _kill_dead(a2) if a2 is not None else True; a2 = None
        dead_b2 = _kill_dead(b2) if b2 is not None else True; b2 = None
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
        sf.close()

    ig = eval_throughput_island_gates(m, expect)
    batch_classes = [b.get("failure_class") for b in m["batches"] if not b.get("batch_pass")]
    soak_classes = [s.get("failure_class") for s in m["soaks"] if not s.get("pass")]
    rep_pass = all(ig.values()) and all(b.get("batch_pass") for b in m["batches"]) \
        and all(s.get("pass") for s in m["soaks"]) and bool(m["batches"])
    if rep_pass:
        failure_class = "pass"
    elif soak_classes:
        failure_class = soak_classes[0]
    elif batch_classes:
        failure_class = batch_classes[0]
    elif not ig.get("fixed_hpx_commit"):
        failure_class = "hpx_identity_mismatch"
    elif not (ig.get("actor_a_hard_placed") and ig.get("actor_b_hard_placed")
              and ig.get("root_on_root_node") and ig.get("actor_nodes_distinct")):
        failure_class = "actor_placement_invalid"
    elif not ig.get("no_orphans"):
        failure_class = "orphan_detected"
    else:
        failure_class = "invalid_instrumentation"
    return {"C": C, "rep": rep, "boot_rel": os.path.relpath(boot, HERE),
            "samples_file_rel": os.path.relpath(samples_path, HERE),
            "island_gates": ig, "island_gates_failed": [k for k, v in ig.items() if not v],
            "rep_pass": rep_pass, "failure_class": failure_class,
            "soaks": m["soaks"],
            "batches": [{k: b.get(k) for k in ("arm", "case", "C", "N", "warmup_n", "direction",
                        "rep", "n_completed", "completion_rate_per_s", "elapsed_s", "latency_stats",
                        "verifier", "witness_deltas", "active_witness", "outstanding", "gates",
                        "gates_failed", "failure_class", "batch_pass", "peer_overlap_observed",
                        "annotations")} for b in m["batches"]],
            "pregate": {"pass": (m.get("pregate") or {}).get("pass"),
                        "cases": [{"name": c["name"], "failed": c["failed"]}
                                  for c in (m.get("pregate") or {}).get("cases", [])]},
            "placement": {"a": m.get("a_placement"), "b": m.get("b_placement"),
                          "root_host": (m.get("root_ready") or {}).get("hostname")},
            "markers": m}


def build_throughput_aggregate(sampling_mode, samp, concurrency, cases, rep_results, provenance,
                               cluster, preflight):
    across = {}
    for rr in rep_results:
        for b in rr.get("batches", []):
            slot = across.setdefault(b["case"], {}).setdefault(str(b["C"]), {}) \
                .setdefault(b["arm"], {"per_rep": []})
            ls = b.get("latency_stats") or {}
            aw = b.get("active_witness") or {}
            slot["per_rep"].append({
                "rep": b["rep"], "completion_rate_per_s": b.get("completion_rate_per_s"),
                "n_completed": b.get("n_completed"),
                "latency_p50_ns": ls.get("p50_ns"), "latency_p90_ns": ls.get("p90_ns"),
                "latency_p99_ns": ls.get("p99_ns"), "latency_iqr_ns": ls.get("iqr_ns"),
                "latency_mad_ns": ls.get("mad_ns"),
                "coordinate_active_max": aw.get("coordinate_active_max"),
                "peer_active_max": (aw.get("ray_peer_active_max") if b["arm"] == "ray"
                                    else aw.get("hpx_action_active_max")),
                "peer_overlap_observed": b.get("peer_overlap_observed"),
                "capability_proof_available": b.get("capability_proof_available"),
                "capability_proof_source": b.get("capability_proof_source"),
                "mean_outstanding": (b.get("outstanding") or {}).get("mean"),
                "fraction_at_C": (b.get("outstanding") or {}).get("fraction_at_C"),
                "verifier_high_water": (b.get("verifier") or {}).get("high_water"),
                "verifier_enqueue_block_ns": (b.get("verifier") or {}).get("enqueue_block_ns"),
                "batch_pass": b.get("batch_pass")})
    overall_pass = bool(rep_results) and all(r["rep_pass"] for r in rep_results)
    cf = dict(CLAIM_FENCES)
    cf["not_cross_node"] = False
    cf["qd1_latency_only_no_throughput"] = False   # this IS the throughput slice
    slim = [{k: r.get(k) for k in ("C", "rep", "boot_rel", "samples_file_rel", "island_gates",
             "island_gates_failed", "rep_pass", "failure_class", "soaks", "batches", "pregate",
             "placement", "rep_capability")} for r in rep_results]
    return {
        "experiment": "69_same_axis_topk_perf",
        "kind": ("slice2_smoke_crossnode_throughput_instrument" if sampling_mode == "smoke"
                 else "slice2_crossnode_closed_loop_throughput"),
        "phase": "rostam-cross-node-throughput", "sampling_mode": sampling_mode,
        "single_node": False, "cross_node": True,
        "load_model": "Closed-loop bounded-concurrency goodput and latency under load at concurrency C",
        "goodput_definition": ("Verified-completion goodput: timing covers request submission "
                               "through caller-observed completion (t1 after ray.get), and "
                               "correctness is established by a bounded verifier OUTSIDE the "
                               "measured request intervals before the batch is accepted."),
        "coordinator": "A", "concurrency": list(concurrency),
        "cases": [c["name"] for c in cases], "sampling": samp,
        "resources": {"num_cpus_per_actor": None, "hpx_threads_per_locality": None,
                      "max_concurrency": "C (per band)"},
        "verifier": {"queue_capacity": VERIFIER_QUEUE_CAPACITY,
                     "note": "one dedicated worker thread; high_water<capacity and enqueue_block==0 gated"},
        "measured_contrast": MEASURED_CONTRAST, "generation_rule": GENERATION_RULE,
        "total_order": TOTAL_ORDER, "correctness_matrix": CORRECTNESS_MATRIX,
        "ratio_fences": dict(RATIO_FENCES), "claim_fences": cf,
        "failure_classes": list(THROUGHPUT_FAILURE_CLASSES),
        "ray_cluster": cluster, "preflight": preflight, "provenance": provenance,
        "reps": slim,
        "across_reps_per_arm_never_differenced": across,
        "overall": "pass" if overall_pass else "fail",
        "safe_claim": (
            "Across three Rostam nodes, the identical deterministic exp68 vocabulary-sharded top-k "
            "request was driven closed-loop at bounded concurrency C through a Ray-mediated and an "
            "HPX-mediated path (A coordinator, hard placement), collecting per-arm verified-completion "
            "goodput and latency-under-load distributions with proven server-side service overlap and "
            "a bounded correctness verifier. Per-arm distributions ONLY; NO throughput/latency ratio, "
            "difference, or winner is computed or implied." if overall_pass
            else "Slice 2 did not pass; see reps[].failure_class / batches[].failure_class."),
    }


def apply_capability_linkage(rep_results):
    """POST-HOC cross-island linkage (pure; no Ray/HPX). With one island per (C, rep) and
    max_concurrency=C, only each repetition's C=4 island can host the P3b/C=4 peer-concurrency
    capability proof. Every measured batch -- including C=2 batches whose own island cannot run a
    C=4 soak -- is linked STRICTLY to its OWN repetition's C=4 P3b soak (keyed by rep index; a proof
    from a different repetition can never satisfy it). A batch may keep peer_overlap_observed=false
    only when that soak proved peer overlap for the SAME arm; a missing or failed capability proof
    invalidates the repetition's batches with peer_concurrency_capability_not_available. Batches are
    re-classified and rep pass/fail is re-derived; no timing, correctness, or scheduler state changes."""
    proven = {}   # rep -> arm -> bool, from the C>=4 P3b capability soaks only
    for r in rep_results:
        if (r.get("C") or 0) >= CAPABILITY_CONCURRENCY:
            for s in r.get("soaks", []):
                if s.get("case") == CAPABILITY_CASE and s.get("capability_regime"):
                    proven.setdefault(r.get("rep"), {})[s.get("arm")] = bool(s.get("capability_proven"))
    for r in rep_results:
        rep = r.get("rep")
        rp = proven.get(rep, {})
        src = f"rep_{rep}_C{CAPABILITY_CONCURRENCY}_{CAPABILITY_CASE}_soak"
        r["rep_capability"] = {
            "capability_case": CAPABILITY_CASE, "capability_concurrency": CAPABILITY_CONCURRENCY,
            "ray_peer_concurrency_capability_proven": rp.get("ray"),
            "hpx_peer_concurrency_capability_proven": rp.get("hpx"),
            "capability_proof_source": src}
        for b in r.get("batches", []):
            avail = bool(rp.get(b.get("arm")))
            b["capability_proof_source"] = src
            b["capability_proof_available"] = avail
            g = dict(b.get("gates") or {})
            g["capability_proof_available"] = avail
            b["gates"] = g
            b["gates_failed"] = [k for k, v in g.items() if not v]
            b["failure_class"] = classify_throughput_batch(b, g)
            b["batch_pass"] = b["failure_class"] == "pass"
        ig = dict(r.get("island_gates") or {})
        ig["all_batches_passed"] = bool(r.get("batches")) and all(b.get("batch_pass") for b in r["batches"])
        ig["peer_capability_available"] = ((not r.get("batches"))
                                           or all(b.get("capability_proof_available") for b in r["batches"]))
        r["island_gates"] = ig
        r["island_gates_failed"] = [k for k, v in ig.items() if not v]
        r["rep_pass"] = (all(ig.values()) and bool(r.get("batches"))
                         and all(b.get("batch_pass") for b in r["batches"])
                         and all(s.get("pass") for s in r.get("soaks", [])))
        if not r["rep_pass"] and r.get("failure_class") == "pass":
            r["failure_class"] = next(
                (b["failure_class"] for b in r["batches"]
                 if b.get("failure_class") == "peer_concurrency_capability_not_available"),
                next((b["failure_class"] for b in r["batches"] if not b.get("batch_pass")),
                     "peer_concurrency_capability_not_available"))
    return rep_results


def run_throughput_crossnode_phase(args):
    subnet = args.subnet_prefix
    sampling_mode = args.sampling
    samp = dict(THROUGHPUT_SAMPLING[sampling_mode])
    samp["mode"] = sampling_mode
    if args.reps is not None:
        samp["reps"] = args.reps
    if args.batch_n is not None:
        samp["N"] = args.batch_n
    if args.warmup_n is not None:
        samp["warmup_n"] = args.warmup_n
    concurrency = [int(x) for x in str(args.concurrency).split(",") if x.strip()]
    cases = [c for c in TIMED_MATRIX if c["name"] in THROUGHPUT_CASES]
    agg = {"experiment": "69_same_axis_topk_perf", "phase": "rostam-cross-node-throughput",
           "sampling_mode": sampling_mode, "single_node": False, "cross_node": True,
           "ratio_fences": dict(RATIO_FENCES)}
    try:
        import ray
    except Exception as ex:  # noqa: BLE001
        agg["overall"] = "skip"; agg["reason"] = f"ray unavailable: {ex}"
        out = args.aggregate or os.path.join(HERE, "_exp69_runs", "throughput_skip_aggregate.json")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        _write_agg(out, agg); print("SKIP: ray unavailable"); return 0

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

    preflight = {"slurm_job_id": job_id, "slurm_nodelist": nodelist, "allocation_nodes": nodes,
                 "controller_host": here, "nodeR": nodeR, "nodeA": nodeA, "nodeB": nodeB,
                 "nodeR_ip": nodeR_ip, "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip,
                 "subnet_prefix": subnet}
    agg["preflight"] = preflight
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    runs_root = os.path.join(HERE, "_exp69_runs", f"throughput_{sampling_mode}_{job_id}_{runid}")
    out_path = args.aggregate or os.path.join(runs_root, "throughput_aggregate.json")
    if problems:
        agg["overall"] = "fail_preflight"; agg["preflight_problems"] = problems
        agg["failure_class"] = ("three_node_allocation_unavailable"
                                if any("node" in p for p in problems)
                                else ("subnet_mismatch" if any("subnet" in p for p in problems)
                                      else "invalid_instrumentation"))
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        _write_agg(out_path, agg); print(f"[exp69-tput] PREFLIGHT FAIL: {problems}"); return 0

    print(f"[exp69-tput] job {job_id} nodes {nodes} | root {nodeR}={nodeR_ip} A {nodeA}={nodeA_ip} "
          f"B {nodeB}={nodeB_ip} | {sampling_mode} C={concurrency} N={samp['N']} reps {samp['reps']}")
    env = dict(os.environ)
    os.makedirs(runs_root, exist_ok=True)
    out_path = args.aggregate or os.path.join(runs_root, "throughput_aggregate.json")
    port = args.ray_port
    temp_dir = f"/tmp/exp69_tput_{job_id}_{runid}"
    port_flags = _ray_port_flags(args)
    head_proc = worker_a = worker_b = None
    rep_results, provenance = [], None
    fail_reason = None
    cluster = {}
    try:
        head_proc = _ray_head_local(nodeR_ip, port, temp_dir, args.head_num_cpus, env,
                                    os.path.join(runs_root, "head.log"), port_flags)
        okA, detA = _wait_gcs_from(nodeA, nodeR_ip, port, env, args.ray_ready_timeout)
        okB, detB = _wait_gcs_from(nodeB, nodeR_ip, port, env, args.ray_ready_timeout)
        agg["ray_gcs_ready_from_nodeA"], agg["ray_gcs_ready_from_nodeB"] = okA, okB
        if not (okA and okB and head_proc.poll() is None):
            raise RuntimeError(f"head GCS not reachable from A({detA}) or B({detB})")
        worker_a = _ray_worker_srun(nodeA, nodeA_ip, nodeR_ip, port, args.worker_num_cpus, env,
                                    os.path.join(runs_root, "worker_a.log"), port_flags)
        worker_b = _ray_worker_srun(nodeB, nodeB_ip, nodeR_ip, port, args.worker_num_cpus, env,
                                    os.path.join(runs_root, "worker_b.log"), port_flags)
        init_ok, init_attempts, init_tb = _bounded_ray_init(ray, f"{nodeR_ip}:{port}",
                                                            args.ray_init_timeout)
        agg["ray_init_ok"] = init_ok
        if not init_ok:
            agg["ray_init_traceback"] = init_tb; raise RuntimeError("ray.init to local head failed")
        nodes_ready, seen = _wait_ray_nodes(ray, 3, args.ray_ready_timeout)
        if not nodes_ready:
            raise RuntimeError(f"only {seen}/3 ray nodes alive")
        alive = [n for n in ray.nodes() if n.get("Alive")]

        def _match(ip, host):
            return [n for n in alive if n.get("NodeManagerAddress") == ip
                    or _short(n.get("NodeName")) == _short(host)]
        mR, mA, mB = _match(nodeR_ip, nodeR), _match(nodeA_ip, nodeA), _match(nodeB_ip, nodeB)
        nodeR_nid = mR[0]["NodeID"] if len(mR) == 1 else None
        nodeA_nid = mA[0]["NodeID"] if len(mA) == 1 else None
        nodeB_nid = mB[0]["NodeID"] if len(mB) == 1 else None
        driver = _self_identity(ray)
        cluster = {"root_node": nodeR, "actor_a_node": nodeA, "actor_b_node": nodeB,
                   "root_ip": nodeR_ip, "actor_a_ip": nodeA_ip, "actor_b_ip": nodeB_ip,
                   "nodeR_ray_node_id": nodeR_nid, "nodeA_ray_node_id": nodeA_nid,
                   "nodeB_ray_node_id": nodeB_nid, "driver_node_id": driver["node_id"],
                   "driver_on_nodeR": bool(driver["node_id"] and driver["node_id"] == nodeR_nid)}
        agg["ray_cluster"] = cluster
        if not (nodeA_nid and nodeB_nid and len({nodeR_nid, nodeA_nid, nodeB_nid}) == 3):
            raise RuntimeError(f"ray node-id resolution failed (mR={len(mR)} mA={len(mA)} mB={len(mB)})")
        if not cluster["driver_on_nodeR"]:
            raise RuntimeError("driver is not on nodeR (must be co-located with the Ray head)")

        HpxActor = build_actor_class(ray)
        expect = {"slurm_job_id": job_id, "nodes": nodes, "nodeR": nodeR, "nodeA": nodeA,
                  "nodeB": nodeB, "nodeR_nid": nodeR_nid, "nodeA_nid": nodeA_nid,
                  "nodeB_nid": nodeB_nid, "subnet": subnet}
        for C in concurrency:
            for rep in range(1, samp["reps"] + 1):
                print(f"[exp69-tput] C={C} rep {rep} ...", flush=True)
                rr = run_throughput_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir), C, rep,
                                        runs_root, args.hpx_threads, args.ray_num_cpus, samp, cases,
                                        nodeR, nodeA, nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid,
                                        nodeB_nid, subnet, args.port_base, expect, env)
                if provenance is None:
                    provenance = collect_provenance(ray, (rr.get("markers") or {}).get("a_identity"))
                with open(os.path.join(runs_root, f"C{C}_rep_{rep}", "markers.json"), "w") as f:
                    json.dump(rr["markers"], f, indent=2)
                rr.pop("markers", None)
                print(f"[exp69-tput]   C={C} rep {rep}: {'PASS' if rr['rep_pass'] else 'FAIL'} "
                      f"({rr['failure_class']}) island_failed={rr['island_gates_failed']}", flush=True)
                rep_results.append(rr)
    except Exception as e:  # noqa: BLE001
        fail_reason = f"{type(e).__name__}: {str(e)[:300]}"
        agg["throughput_exception"] = fail_reason
        agg["throughput_traceback"] = traceback.format_exc()[-2000:]
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

    # Cross-island capability linkage BEFORE the aggregate/overall verdict: each measured batch is
    # bound to its own repetition's C=4 P3b soak; C=2 batches are invalidated if that proof failed.
    rep_results = apply_capability_linkage(rep_results)
    full = build_throughput_aggregate(sampling_mode, samp, concurrency, cases, rep_results,
                                      provenance, cluster, preflight)
    full["resources"]["num_cpus_per_actor"] = args.ray_num_cpus
    full["resources"]["hpx_threads_per_locality"] = args.hpx_threads
    full["ray_cluster"] = agg.get("ray_cluster")
    full["final_orphans"] = agg.get("final_orphans")
    if fail_reason:
        full["throughput_exception"] = fail_reason
        full["throughput_traceback"] = agg.get("throughput_traceback")
    overall_pass = (fail_reason is None and bool(rep_results)
                    and len(rep_results) == len(concurrency) * samp["reps"]
                    and all(r["rep_pass"] for r in rep_results)
                    and agg.get("final_orphans", {}).get("no_ray_orphans")
                    and agg.get("final_orphans", {}).get("no_peer_orphans"))
    full["overall"] = "pass" if overall_pass else "fail"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    _write_agg(out_path, full)
    print(f"[exp69-tput] overall: {full['overall']} -> {out_path}")
    if sampling_mode == "accepted" and full["overall"] == "pass" and args.aggregate is None:
        curated = os.path.join(HERE, "same_axis_topk_throughput_crossnode_aggregate.json")
        _write_agg(curated, full)
        print(f"[exp69-tput] curated throughput aggregate -> {curated}")
    return 0 if full["overall"] == "pass" else 1


# =======================================================================================
# Slice 3 (exp69) DIAGNOSTIC decomposition -- matched-resource matrix + native stage timing
# =======================================================================================
# ADDITIVE Slice-3-only. Tests the static-audit hypothesis (a HYPOTHESIS, not an established
# cause) classified as `thread_supply_resource_asymmetry`: the Ray arm's native execution scales
# to max_concurrency=C, while the accepted HPX configuration supplies only hpx_threads for posted
# coordinator work + peer-action execution + continuations + merge -- so at C>hpx_threads the HPX
# arm is compute-thread-starved relative to the Ray arm. (NOT `same_axis_work_mismatch`: the logical
# native work stays matched; only the thread SUPPLY differs.) P3b only. Reuses the Slice 2 verifier,
# correctness contract, soak, and lifecycle gates UNCHANGED, and uses the *_coordinate_timed arms so
# every request also yields a coordinator-local stage decomposition + peer-LOCAL durations (never
# subtracted). No ratio/speedup/difference/winner is computed. Accepted Slice 1/2 aggregates untouched.

SLICE3_CASE = "P3b"
SLICE3_BANDS = [
    {"band": "C2_cpu2_ht2", "C": 2, "num_cpus": 2, "hpx_threads": 2, "kind": "baseline_matched"},
    {"band": "C4_cpu2_ht2", "C": 4, "num_cpus": 2, "hpx_threads": 2,
     "kind": "accepted_oversubscribed_control"},
    {"band": "C4_cpu4_ht4", "C": 4, "num_cpus": 4, "hpx_threads": 4, "kind": "matched_decisive"},
]
SLICE3_SAMPLING = {"smoke": {"N": 100, "warmup_n": 20, "reps": 1, "mode": "smoke"},
                   "accepted": {"N": 1000, "warmup_n": 100, "reps": 3, "mode": "accepted"}}
SLICE3_HPX_STAGES = ("entry_to_post_ns", "post_to_start_ns", "own_topk_ns", "dispatch_ns",
                     "dispatch_to_cont_ns", "reply_get_ns", "merge_ns", "cont_to_delivered_ns",
                     "total_native_ns")
SLICE3_RAY_STAGES = ("own_topk_ns", "peer_call_ns", "merge_ns", "total_ns")


def _s3_dist(xs):
    """Compact non-negative distribution summary for a diagnostic stage (ns)."""
    xs = sorted(x for x in xs if isinstance(x, (int, float)) and x >= 0)
    if not xs:
        return None
    n = len(xs)

    def q(p):
        return xs[min(n - 1, max(0, int(round(p * (n - 1)))))]
    return {"n": n, "p50": q(0.50), "p90": q(0.90), "p99": q(0.99),
            "mean": sum(xs) / n, "min": xs[0], "max": xs[-1]}


def run_slice3_batch(ray_mod, meta, coord_key, peer_key, arm, case, C, N, warmup_n,
                     sampling_mode, sample_writer, rep, band):
    """One (band, arm, P3b, rep) closed-loop fixed-count DIAGNOSTIC batch. Identical closed-loop
    schedule, verifier, and gates as run_throughput_batch, but drives the *_coordinate_timed arm and
    records the coordinator-local stage decomposition (+ peer-LOCAL durations, never subtracted). The
    accepted run_throughput_batch is left untouched."""
    coord = meta[coord_key]["handle"]
    peer_meta, own_meta = meta[peer_key], meta[coord_key]
    direction = "A" if coord_key == "a" else "B"
    args = shard_args(case, direction)
    oracle_global = case_oracle(case)["global"]
    method = getattr(coord, arm + "_coordinate_timed")   # DIAGNOSTIC timed arm
    bm = {"arm": arm, "case": case["name"], "C": C, "N": N, "warmup_n": warmup_n, "band": band,
          "direction": direction, "rep": rep, "sampling_mode": sampling_mode,
          "flags": [], "timeouts": 0, "peer_frozen": True}

    # discarded warm-up at the same C (results verified inline; timings discarded)
    try:
        if not _closed_loop_warmup(ray_mod, method, args, C, warmup_n, arm, oracle_global,
                                   peer_meta, own_meta):
            bm["flags"].append("throughput_batch_invalidated_by_invalid_result")
            bm["gates"] = {"warmup": False}; bm["gates_failed"] = ["warmup"]
            bm["failure_class"] = "throughput_batch_invalidated_by_invalid_result"
            bm["batch_pass"] = False
            return bm
    except Exception as ex:  # noqa: BLE001
        bm["flags"].append("timeout"); bm["warmup_error"] = f"{type(ex).__name__}: {ex}"
        bm["gates"] = {"warmup": False}; bm["gates_failed"] = ["warmup"]
        bm["failure_class"] = "timeout"; bm["batch_pass"] = False
        return bm

    _ray_get(ray_mod, coord.reset_active.remote(), 30, "reset_coord")
    _ray_get(ray_mod, meta[peer_key]["handle"].reset_active.remote(), 30, "reset_peer")
    c_before = _ray_get(ray_mod, coord.counters.remote(), 30, "c_before")
    p_before = _ray_get(ray_mod, meta[peer_key]["handle"].counters.remote(), 30, "p_before")

    verifier = ThroughputVerifier(VERIFIER_QUEUE_CAPACITY,
                                  _tp_verify_fn(oracle_global, peer_meta, own_meta))
    verifier.start()
    inflight = {}
    submitted = completed = 0
    latencies, outstanding_samples = [], []
    # stage accumulators (coordinator-local; peer_local kept separate + never subtracted)
    hpx_stage = {s: [] for s in SLICE3_HPX_STAGES}
    ray_stage = {s: [] for s in SLICE3_RAY_STAGES}
    same_worker, own_workers, cont_workers, hpx_worker_counts = [], [], [], []
    peer_local = {"peer_local_topk_ns": [], "peer_action_body_ns": []}

    def _absorb(result):
        st = (result or {}).get("stage_ns") or {}
        if arm == "hpx":
            for s in SLICE3_HPX_STAGES:
                hpx_stage[s].append(st.get(s))
            wk = (result or {}).get("workers") or {}
            same_worker.append(bool(wk.get("same_worker")))
            own_workers.append(wk.get("own_worker")); cont_workers.append(wk.get("cont_worker"))
            hpx_worker_counts.append(wk.get("hpx_worker_count"))
            pl = (result or {}).get("peer_local") or {}
            peer_local["peer_local_topk_ns"].append(pl.get("peer_local_topk_ns"))
            peer_local["peer_action_body_ns"].append(pl.get("peer_action_body_ns"))
        else:
            for s in SLICE3_RAY_STAGES:
                ray_stage[s].append(st.get(s))

    start_ns = time.perf_counter_ns()
    for _ in range(min(C, N)):
        t0 = time.perf_counter_ns()
        inflight[method.remote(*args)] = {"t0": t0, "req_id": submitted}; submitted += 1
    while completed < N:
        ready, _ = ray_mod.wait(list(inflight.keys()), num_returns=1, timeout=120)
        if not ready:
            bm["timeouts"] += 1; bm["flags"].append("timeout"); break
        ref = ready[0]; req = inflight.pop(ref)
        outstanding_samples.append(len(inflight) + 1)
        try:
            result = ray_mod.get(ref, timeout=120)
        except Exception as ex:  # noqa: BLE001
            bm["timeouts"] += 1; bm["flags"].append("timeout")
            bm["get_error"] = f"{type(ex).__name__}: {ex}"; break
        t1 = time.perf_counter_ns()
        latencies.append(t1 - req["t0"]); completed += 1
        if submitted < N:
            t0 = time.perf_counter_ns()
            inflight[method.remote(*args)] = {"t0": t0, "req_id": submitted}; submitted += 1
        verifier.enqueue({"arm": arm, "result": result, "req_id": req["req_id"]})
        _absorb(result)
        row = {"rep": rep, "band": band, "case": case["name"], "arm": arm, "C": C,
               "direction": direction, "sampling_mode": sampling_mode, "req_id": req["req_id"],
               "completion_index": completed, "t0_ns": req["t0"], "t1_ns": t1,
               "latency_ns": t1 - req["t0"], "outstanding": outstanding_samples[-1],
               "stage_ns": (result or {}).get("stage_ns")}
        if arm == "hpx":
            row["workers"] = (result or {}).get("workers")
            row["peer_local"] = (result or {}).get("peer_local")
        sample_writer(row)
    end_ns = time.perf_counter_ns()
    verifier.finish()

    c_after = _ray_get(ray_mod, coord.counters.remote(), 30, "c_after")
    p_after = _ray_get(ray_mod, meta[peer_key]["handle"].counters.remote(), 30, "p_after")
    aw_c = _ray_get(ray_mod, coord.active_witness.remote(), 30, "aw_coord") or {}
    aw_p = _ray_get(ray_mod, meta[peer_key]["handle"].active_witness.remote(), 30, "aw_peer") or {}

    elapsed_s = (end_ns - start_ns) / 1e9
    bm["start_ns"], bm["end_ns"], bm["elapsed_s"] = start_ns, end_ns, elapsed_s
    bm["completion_rate_per_s"] = (completed / elapsed_s) if elapsed_s > 0 else None
    bm["n_completed"] = completed
    bm["latency_stats"] = stats_block(latencies, N, sampling_mode) if latencies else None
    bm["verifier"] = {"capacity": verifier.capacity, "high_water": verifier.high_water,
                      "enqueue_block_ns": verifier.enqueue_block_ns,
                      "enqueue_attempts": verifier.enqueue_attempts, "verified": verifier.verified,
                      "invalid": verifier.invalid, "first_invalid": verifier.first_invalid,
                      "exception": verifier.exception,
                      "max_verify_latency_ns": verifier.max_verify_latency_ns}

    def d(before, after, key):
        return int((after or {}).get(key, 0)) - int((before or {}).get(key, 0))
    bm["witness_deltas"] = {
        "coord_outgoing_ray_fetch": d(c_before, c_after, "outgoing_ray_fetch"),
        "peer_serving_ray_fetch": d(p_before, p_after, "serving_ray_fetch"),
        "peer_hpx_actions_served": d(p_before, p_after, "hpx_actions_served"),
        "coord_serving_ray_fetch": d(c_before, c_after, "serving_ray_fetch"),
        "coord_hpx_actions_served": d(c_before, c_after, "hpx_actions_served"),
        "peer_outgoing_ray_fetch": d(p_before, p_after, "outgoing_ray_fetch")}
    bm["active_witness"] = {
        "coordinate_active_max": aw_c.get("coordinate_active_max"),
        "ray_peer_active_max": aw_p.get("ray_peer_active_max"),
        "hpx_action_active_max": aw_p.get("hpx_action_active_max"),
        "coord_currents": {"coordinate_active_current": aw_c.get("coordinate_active_current"),
                           "ray_peer_active_current": aw_c.get("ray_peer_active_current"),
                           "hpx_action_active_current": aw_c.get("hpx_action_active_current")},
        "peer_currents": {"coordinate_active_current": aw_p.get("coordinate_active_current"),
                          "ray_peer_active_current": aw_p.get("ray_peer_active_current"),
                          "hpx_action_active_current": aw_p.get("hpx_action_active_current")},
        "coord_underflow": aw_c.get("active_underflow_seen"),
        "peer_underflow": aw_p.get("active_underflow_seen")}
    bm["outstanding"] = {
        "mean": (sum(outstanding_samples) / len(outstanding_samples)) if outstanding_samples else None,
        "max": max(outstanding_samples) if outstanding_samples else None,
        "fraction_at_C": (sum(1 for o in outstanding_samples if o >= C) / len(outstanding_samples))
        if outstanding_samples else None, "target_C": C}
    _peer_max = (bm["active_witness"].get("ray_peer_active_max") or 0) if arm == "ray" \
        else (bm["active_witness"].get("hpx_action_active_max") or 0)
    bm["peer_overlap_observed"] = _peer_max >= 2
    bm["annotations"] = peer_overlap_annotations(bm["peer_overlap_observed"])

    # Coordinator-local stage decomposition (per-arm; peer_local kept separate + never subtracted).
    if arm == "hpx":
        wc = [w for w in hpx_worker_counts if isinstance(w, (int, float))]
        bm["stage_decomp"] = {
            "arm": "hpx",
            "coordinator_stage_ns": {s: _s3_dist(hpx_stage[s]) for s in SLICE3_HPX_STAGES},
            "same_worker_fraction": (sum(1 for x in same_worker if x) / len(same_worker))
            if same_worker else None,
            "hpx_worker_count_observed": (max(wc) if wc else None),
            "distinct_own_workers": sorted({w for w in own_workers if isinstance(w, (int, float))}),
            "distinct_cont_workers": sorted({w for w in cont_workers if isinstance(w, (int, float))}),
            "peer_local_ns_NOT_SUBTRACTED": {k: _s3_dist(v) for k, v in peer_local.items()}}
    else:
        bm["stage_decomp"] = {
            "arm": "ray",
            "coordinator_stage_ns": {s: _s3_dist(ray_stage[s]) for s in SLICE3_RAY_STAGES},
            "peer_call_ns_note": "caller-observed; includes Ray queue/transport/peer service/return; "
                                 "NOT pure transport time"}

    gates = eval_throughput_batch_gates(bm)
    bm["gates"] = gates
    bm["gates_failed"] = [k for k, v in gates.items() if not v]
    bm["failure_class"] = classify_throughput_batch(bm, gates)
    bm["batch_pass"] = bm["failure_class"] == "pass"
    return bm


def eval_slice3_local_gates(m):
    """Local (loopback) Slice-3 island lifecycle gates -- placement is loopback (no hard NodeAffinity),
    so this is the local-appropriate subset of the cross-node island gates."""
    ai, bi = m.get("a_identity") or {}, m.get("b_identity") or {}
    as_, bs = m.get("a_start") or {}, m.get("b_start") or {}
    rr, rf = m.get("root_ready") or {}, m.get("root_final") or {}
    g = {}
    g["fixed_hpx_commit"] = bool(commit_matches((ai.get("hpx_version_info") or {}).get("hpx_complete_version"))
                                 and commit_matches((bi.get("hpx_version_info") or {}).get("hpx_complete_version"))
                                 and commit_matches(rr.get("hpx_complete_version")))
    g["actors_distinct_nonzero_localities"] = bool(as_.get("locality_id") not in (None, 0)
                                                   and bs.get("locality_id") not in (None, 0)
                                                   and as_.get("locality_id") != bs.get("locality_id"))
    g["both_in_process_no_hpx_child"] = bool((m.get("a_child_report") or {}).get("hpx_children") == []
                                             and (m.get("b_child_report") or {}).get("hpx_children") == [])
    g["membership_reached_three"] = bool(rf.get("max_membership", 0) >= 3)
    g["root_isolated_workfree"] = bool(rr.get("locality_id") == 0 and rf.get("actions_served_on_root", -1) == 0)
    g["peer_frozen_both"] = bool(m.get("a_peer_frozen") and m.get("b_peer_frozen"))
    g["pregate_seven_cases_pass"] = bool((m.get("pregate") or {}).get("pass")
                                         and len((m.get("pregate") or {}).get("cases") or []) == len(CORRECTNESS_MATRIX))
    g["resources_recorded"] = bool(m.get("actor_hpx_threads") is not None
                                   and m.get("actor_ray_num_cpus") is not None
                                   and m.get("max_concurrency") is not None)
    g["graceful_leave_clean_stop"] = bool(m.get("a_stop_rc") == 0 and m.get("b_stop_rc") == 0
                                          and not m.get("a_stop_error") and not m.get("b_stop_error"))
    g["root_finalized_clean"] = bool(m.get("root_exit_path") == "finalized_clean"
                                     and rf.get("leave_observed") is True and rf.get("final_membership") == 1)
    g["actors_recreated"] = bool((m.get("a_recreate") or {}).get("ok") and (m.get("b_recreate") or {}).get("ok"))
    g["no_orphans"] = bool(m.get("orphans") == [] and m.get("actor_pids_gone") is True)
    g["ratio_fences_false"] = (all(v is False for v in (m.get("ratio_fences") or {}).values())
                               and set(m.get("ratio_fences") or {}) == set(RATIO_FENCES))
    g["soaks_passed"] = bool(m.get("soaks_all_passed"))
    g["all_batches_passed"] = bool(m.get("all_batches_passed"))
    return g


def run_slice3_local_rep(ray_mod, HpxActor, peer, build_dir, band, rep, runs_root):
    """One (band, rep) LOOPBACK island: root + A + B in-process, max_concurrency=C, frozen peers,
    7-case correctness pre-gate, per-arm P3b soak, then a P3b timed diagnostic batch per arm. LOCAL
    instrument validation only -- ht=4 may oversubscribe the Mac; no causal conclusion drawn here."""
    C, ray_cpus, hpx_threads = band["C"], band["num_cpus"], band["hpx_threads"]
    boot = os.path.join(runs_root, f"{band['band']}_rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    p_root, p_a, p_b = find_free_port(), find_free_port(), find_free_port()
    rcmd = peer_root_cmd(peer, boot, p_root)
    ep_a, ep_b = actor_endpoints(p_root, p_a), actor_endpoints(p_root, p_b)
    m = {"controller_hostname": socket.gethostname(), "controller_pid": os.getpid(),
         "band": band["band"], "band_kind": band["kind"], "C": C, "rep": rep,
         "ports": {"root": p_root, "actor_a": p_a, "actor_b": p_b},
         "argv_audit_ok": argv_audit(rcmd, ep_a, ep_b),
         "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus, "max_concurrency": C,
         "resources_single_constants": True, "ratio_fences": dict(RATIO_FENCES),
         "hypothesis": "thread_supply_resource_asymmetry", "soaks": [], "batches": []}
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    sf = open(os.path.join(boot, "samples.jsonl"), "w")

    def sample_writer(row):
        sf.write(json.dumps(row) + "\n")

    try:
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file(os.path.join(boot, "root.ready"), 30, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        a1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              max_concurrency=C).remote(build_dir, hpx_threads, ep_a)
        m["a_identity"] = _ray_get(ray_mod, a1.load_identity.remote(), 40, "a_identity")
        m["a_start"] = _ray_get(ray_mod, a1.start_hpx.remote(), 60, "a_start")
        m["a_child_report"] = _ray_get(ray_mod, a1.child_report.remote(), 30, "a_child_report")
        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              max_concurrency=C).remote(build_dir, hpx_threads, ep_b)
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 40, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 60, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")

        a_loc = (m["a_start"] or {}).get("locality_id")
        b_loc = (m["b_start"] or {}).get("locality_id")
        a_pid = (m.get("a_identity") or {}).get("pid")
        b_pid = (m.get("b_identity") or {}).get("pid")
        identity_ok = (commit_matches((m["a_identity"].get("hpx_version_info") or {}).get("hpx_complete_version"))
                       and commit_matches((m["b_identity"].get("hpx_version_info") or {}).get("hpx_complete_version"))
                       and commit_matches((m.get("root_ready") or {}).get("hpx_complete_version")))
        hosting_ok = a_loc not in (None, 0) and b_loc not in (None, 0) and a_loc != b_loc

        soaks_all_passed = all_batches_passed = False
        if identity_ok and hosting_ok:
            _ray_get(ray_mod, a1.set_peer.remote(b1, b_loc), 30, "a_set_peer")
            _ray_get(ray_mod, b1.set_peer.remote(a1, a_loc), 30, "b_set_peer")
            fa = _ray_get(ray_mod, a1.freeze_peer.remote(), 30, "a_freeze")
            fb = _ray_get(ray_mod, b1.freeze_peer.remote(), 30, "b_freeze")
            m["a_peer_frozen"] = bool((fa or {}).get("peer_frozen"))
            m["b_peer_frozen"] = bool((fb or {}).get("peer_frozen"))
            meta = {"a": {"handle": a1, "pid": a_pid,
                          "host": (m["a_identity"] or {}).get("hostname"),
                          "node_id": (m["a_identity"] or {}).get("node_id"), "locality": a_loc},
                    "b": {"handle": b1, "pid": b_pid,
                          "host": (m["b_identity"] or {}).get("hostname"),
                          "node_id": (m["b_identity"] or {}).get("node_id"), "locality": b_loc}}
            m["pregate"] = run_pregate(ray_mod, meta)
            if m["pregate"]["pass"]:
                soak_case = next(c for c in TIMED_MATRIX if c["name"] == SLICE3_CASE)
                soaks_all_passed = True
                for arm in ("ray", "hpx"):
                    s = concurrency_soak(ray_mod, meta, "a", "b", arm, soak_case, C)
                    m["soaks"].append(s)
                    if not s["pass"]:
                        soaks_all_passed = False
                if soaks_all_passed:
                    all_batches_passed = True
                    case = next(c for c in TIMED_MATRIX if c["name"] == SLICE3_CASE)
                    for arm in ("ray", "hpx"):
                        print(f"[exp69-s3]   {band['band']} rep={rep} {arm}/{case['name']} "
                              f"(N={m_samp_N()} C={C}) ...", flush=True)
                        bmr = run_slice3_batch(ray_mod, meta, "a", "b", arm, case, C,
                                               _SLICE3_N, _SLICE3_WARMUP, _SLICE3_MODE,
                                               sample_writer, rep, band["band"])
                        m["batches"].append(bmr)
                        if not bmr["batch_pass"]:
                            all_batches_passed = False
        m["soaks_all_passed"] = soaks_all_passed
        m["all_batches_passed"] = all_batches_passed

        # Lifecycle: graceful leave -> destroy -> recreate -> root completion -> orphan check.
        a_stop = _ray_get(ray_mod, a1.stop_hpx.remote(), 30, "a_stop")
        b_stop = _ray_get(ray_mod, b1.stop_hpx.remote(), 30, "b_stop")
        m["a_stop_rc"], m["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
        m["b_stop_rc"], m["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")
        ray_mod.kill(a1); a1 = None
        ray_mod.kill(b1); b1 = None
        m["a_destroyed"] = wait_pid_gone(a_pid, 20)
        m["b_destroyed"] = wait_pid_gone(b_pid, 20)
        a2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              max_concurrency=C).remote(build_dir, hpx_threads, ep_a)
        b2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0,
                              max_concurrency=C).remote(build_dir, hpx_threads, ep_b)
        m["a_recreate"] = _ray_get(ray_mod, a2.ping.remote(), 40, "a_recreate")
        m["b_recreate"] = _ray_get(ray_mod, b2.ping.remote(), 40, "b_recreate")
        a2_pid = (m["a_recreate"] or {}).get("pid")
        b2_pid = (m["b_recreate"] or {}).get("pid")
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
                except Exception:  # noqa: BLE001
                    pass
        _kill_group(root)
        if rlog is not None:
            rlog.close()
        sf.close()

    gates = eval_slice3_local_gates(m)
    m["island_gates"] = gates
    m["island_gates_failed"] = [k for k, v in gates.items() if not v]
    rep_pass = all(gates.values())
    return {"band": band["band"], "band_kind": band["kind"], "C": C, "num_cpus": ray_cpus,
            "hpx_threads": hpx_threads, "rep": rep, "rep_pass": rep_pass,
            "island_gates": gates, "island_gates_failed": m["island_gates_failed"],
            "batches": m["batches"], "soaks": m["soaks"], "markers": m}


# Module-level slice3 sampling scalars, set by the orchestrator before running reps.
_SLICE3_N = 100
_SLICE3_WARMUP = 20
_SLICE3_MODE = "smoke"


def m_samp_N():
    return _SLICE3_N


def build_slice3_aggregate(placement, samp, band_rep_results, provenance):
    """Diagnostic aggregate: per band, per arm, per rep -- goodput + latency-under-load + coordinator
    stage decomposition + peer-local (never subtracted). NO cross-arm ratio/difference/winner."""
    bands = []
    for br in band_rep_results:
        arms = {}
        for b in br["batches"]:
            arms.setdefault(b["arm"], []).append({
                "rep": b["rep"], "goodput_per_s": b.get("completion_rate_per_s"),
                "n_completed": b.get("n_completed"), "verified": (b.get("verifier") or {}).get("verified"),
                "invalid": (b.get("verifier") or {}).get("invalid"), "timeouts": b.get("timeouts"),
                "latency_stats": b.get("latency_stats"),
                "coordinate_active_max": (b.get("active_witness") or {}).get("coordinate_active_max"),
                "peer_active_max": ((b.get("active_witness") or {}).get("hpx_action_active_max")
                                    if b["arm"] == "hpx"
                                    else (b.get("active_witness") or {}).get("ray_peer_active_max")),
                "stage_decomp": b.get("stage_decomp"), "batch_pass": b.get("batch_pass"),
                "failure_class": b.get("failure_class")})
        bands.append({"band": br["band"], "band_kind": br["band_kind"], "C": br["C"],
                      "num_cpus": br["num_cpus"], "hpx_threads": br["hpx_threads"],
                      "rep": br["rep"], "rep_pass": br["rep_pass"],
                      "island_gates_failed": br["island_gates_failed"],
                      "capability": br.get("rep_capability"), "arms": arms})
    return {
        "experiment": "69_same_axis_topk_perf",
        "kind": "slice3_causal_decomposition_matrix",
        "placement": placement, "case": SLICE3_CASE,
        "hypothesis_under_test": "thread_supply_resource_asymmetry",
        "hypothesis_note": ("HYPOTHESIS not established cause: Ray native execution scales to "
                            "max_concurrency=C while the accepted HPX config supplies only "
                            "hpx_threads for posted work + peer actions + continuations + merge. "
                            "NOT same_axis_work_mismatch (logical native work stays matched)."),
        "bands": [dict(b) for b in SLICE3_BANDS],
        "sampling": samp,
        "measured_contrast": MEASURED_CONTRAST,
        "peer_local_policy": "peer-local durations are attribution-only and NEVER subtracted from "
                             "coordinator timestamps",
        "generation_rule": GENERATION_RULE, "total_order": TOTAL_ORDER,
        "ratio_fences": dict(RATIO_FENCES), "claim_fences": dict(CLAIM_FENCES),
        "provenance": provenance,
        "band_reps": bands,
    }


def run_slice3_local_phase(args):
    """Slice 3 LOCAL (loopback, single-node) instrument validation: P3b, all three resource bands.
    Instrument-check ONLY -- ht=4 may oversubscribe this machine; draws NO causal conclusion and
    writes NO curated aggregate. Accepted Slice 1/2 aggregates are never touched."""
    global _SLICE3_N, _SLICE3_WARMUP, _SLICE3_MODE
    import ray
    peer = os.path.join(args.build_dir, "exp69_peer")
    if not os.path.exists(peer):
        print(f"[exp69-s3] missing peer binary: {peer}"); return 1
    samp = dict(SLICE3_SAMPLING["smoke" if args.slice3_mode == "smoke" else "accepted"])
    if args.reps is not None:
        samp["reps"] = args.reps
    _SLICE3_N = args.batch_n or samp["N"]
    _SLICE3_WARMUP = args.warmup_n if args.warmup_n is not None else samp["warmup_n"]
    _SLICE3_MODE = samp["mode"]
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    runs_root = os.path.join(HERE, "_exp69_runs", f"slice3_local_{args.slice3_mode}_{runid}")
    os.makedirs(runs_root, exist_ok=True)
    out_path = args.aggregate or os.path.join(runs_root, "slice3_local_aggregate.json")
    print(f"[exp69-s3] LOCAL {args.slice3_mode}: P3b x {len(SLICE3_BANDS)} bands "
          f"N={_SLICE3_N} warm={_SLICE3_WARMUP} reps={samp['reps']} -> {runs_root}")
    ray.init(num_cpus=max(8, 2 * max(b['num_cpus'] for b in SLICE3_BANDS) + 4),
             include_dashboard=False, log_to_driver=False, configure_logging=False)
    HpxActor = build_actor_class(ray)
    band_rep_results, provenance = [], None
    try:
        for band in SLICE3_BANDS:
            for rep in range(1, samp["reps"] + 1):
                print(f"[exp69-s3] band {band['band']} ({band['kind']}) rep {rep} ...", flush=True)
                br = run_slice3_local_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir),
                                          band, rep, runs_root)
                if provenance is None:
                    provenance = collect_provenance(ray, (br.get("markers") or {}).get("a_identity"))
                with open(os.path.join(runs_root, f"{band['band']}_rep_{rep}", "markers.json"), "w") as f:
                    json.dump(br["markers"], f, indent=2)
                br.pop("markers", None)
                print(f"[exp69-s3]   band {band['band']} rep {rep}: "
                      f"{'PASS' if br['rep_pass'] else 'FAIL'} failed={br['island_gates_failed']}",
                      flush=True)
                band_rep_results.append(br)
    finally:
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass
    full = build_slice3_aggregate("local_loopback_single_node", samp, band_rep_results, provenance)
    all_pass = bool(band_rep_results) and all(br["rep_pass"] for br in band_rep_results)
    full["overall"] = "pass" if all_pass else "fail"
    full["instrument_check_only"] = True
    full["local_oversubscription_caveat"] = (
        "LOCAL loopback single-node run. The ht=4/cpu=4 band may oversubscribe this machine's cores; "
        "local goodput/latency are INSTRUMENT-CHECK ONLY and are NOT evidence for the causal "
        "conclusion. The decisive matrix runs on the three-node Rostam topology.")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    _write_agg(out_path, full)
    print(f"[exp69-s3] LOCAL overall: {full['overall']} -> {out_path}")
    return 0 if all_pass else 1


# =======================================================================================
# Slice 3 (exp69) CROSS-NODE decisive matrix -- additive mirror of the accepted Slice 2 cross-node
# orchestration, driving the Slice 3 timed diagnostic instead of the accepted throughput batch.
# =======================================================================================
# The three-node bring-up, hard placement, locality identity, NFS marker gates, actor recreation,
# graceful leave, root finalize, and cross-node orphan checks are the ACCEPTED Slice 2 machinery,
# UNCHANGED. Each of the three resource bands (C2/cpu2/ht2, C4/cpu2/ht2, C4/cpu4/ht4) is a FRESH
# island configured and gated under its OWN (C, num_cpus, hpx_threads); the two C=4 bands are fully
# independent and share NO capability proof. Below the capability regime (C=2) peer overlap is
# observation-only (peer_active_max==1 neither fails the band nor proves serialization); in the
# capability regime (P3b, C>=4) each band's OWN per-arm P3b soak must witness peer overlap >=2. No
# ratio/speedup/difference/winner is computed; all four ratio fences stay false. The curated Slice 3
# aggregate is written ONLY by one uninterrupted accepted invocation completing ALL bands x reps.


def slice3_rep_capability(soaks):
    """Per-(band, rep) peer-concurrency capability record derived STRICTLY from THIS band-rep's own
    soaks (pure; no Ray/HPX). In the capability regime (P3b, C>=4) each arm's value is True/False from
    its own soak; below it (C=2) it is None -- peer overlap there is observation-only, NOT a proof. A
    capability result from any other band or repetition can never appear here."""
    def cap(arm):
        s = next((x for x in soaks if x.get("arm") == arm), None)
        return (s or {}).get("capability_proven")
    return {"ray_peer_concurrency_capability_proven": cap("ray"),
            "hpx_peer_concurrency_capability_proven": cap("hpx")}


def slice3_crossnode_claim_fences():
    """Claim fences for the CROSS-NODE slice3 run (pure). Start from the shared defaults but flip the
    two that the default (local/QD1-shaped) build_slice3_aggregate leaves set: this run IS cross-node
    and IS bounded-concurrency goodput + latency-under-load, not single-node QD1-only. Mirrors exactly
    the two flips the Slice 2 throughput phase makes. Every other fence -- including
    no_ratio_speedup_winner_claim and per_arm_distributions_only -- stays True."""
    cf = dict(CLAIM_FENCES)
    cf["not_cross_node"] = False
    cf["qd1_latency_only_no_throughput"] = False
    return cf


def slice3_matrix_complete(band_rep_results, n_bands, reps):
    """Accepted-curation completeness (pure): True ONLY when exactly one repetition is present for
    every (band, rep) in the full n_bands x reps matrix -- no missing band, no missing repetition, no
    duplicated/pooled repetition. Any of those makes it False and blocks curation of the Slice 3
    aggregate. Band identity is the (band-name) key, which already encodes (C, num_cpus, hpx_threads)."""
    expected = {(b["band"], r) for b in SLICE3_BANDS for r in range(1, reps + 1)}
    seen = [(br.get("band"), br.get("rep")) for br in band_rep_results]
    return (len(band_rep_results) == n_bands * reps
            and len(seen) == len(set(seen)) and set(seen) == expected)


def run_slice3_crossnode_rep(ray_mod, HpxActor, peer, build_dir, band, band_idx, rep, runs_root, samp,
                             nodeR, nodeA, nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid, nodeB_nid,
                             subnet, port_base, expect, env):
    """One (band, rep) HARD-PLACED cross-node island for the Slice 3 diagnostic. Cross-node bring-up,
    placement, lifecycle, and orphan handling reuse the accepted Slice 2 rep machinery; the measured
    body drives the *_coordinate_timed arms (run_slice3_batch) at THIS band's (C, num_cpus, hpx_threads)
    after a per-band per-arm P3b soak. Coordinator overlap is always mandatory. In the capability regime
    (P3b, C>=4) the band's OWN soak must prove peer overlap; below it (C=2) peer overlap is observed but
    not gated. Ports/boot dir/markers are keyed on band_idx so the two C=4 bands never collide."""
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    C, ray_cpus, hpx_threads = band["C"], band["num_cpus"], band["hpx_threads"]
    boot = os.path.join(runs_root, f"{band['band']}_rep_{rep}")
    os.makedirs(boot, exist_ok=True)
    base = port_base + band_idx * 1000 + rep * 10
    p_root, p_a, p_b = base, base + 1, base + 2
    rcmd = crossnode_root_cmd(peer, boot, nodeR_ip, p_root, leave_timeout=45)
    ep_a = crossnode_actor_endpoints(nodeR_ip, p_root, nodeA_ip, p_a)
    ep_b = crossnode_actor_endpoints(nodeR_ip, p_root, nodeB_ip, p_b)
    strat_a = NodeAffinitySchedulingStrategy(node_id=nodeA_nid, soft=False)
    strat_b = NodeAffinitySchedulingStrategy(node_id=nodeB_nid, soft=False)
    m = {"controller_hostname": socket.gethostname(), "controller_pid": os.getpid(),
         "band": band["band"], "band_kind": band["kind"], "band_idx": band_idx,
         "C": C, "rep": rep, "ports": {"root": p_root, "actor_a": p_a, "actor_b": p_b},
         "endpoints_subnet_ok": bool(nodeR_ip.startswith(subnet) and nodeA_ip.startswith(subnet)
                                     and nodeB_ip.startswith(subnet)),
         "actor_hpx_threads": hpx_threads, "actor_ray_num_cpus": ray_cpus, "max_concurrency": C,
         "resources_single_constants": True, "ratio_fences": dict(RATIO_FENCES),
         "hypothesis": "thread_supply_resource_asymmetry",
         "remote_orphan_check_ran": False, "soaks": [], "batches": []}
    root = rlog = None
    a1 = b1 = a2 = b2 = None
    samples_path = os.path.join(boot, "samples.jsonl")
    sf = open(samples_path, "w")

    def sample_writer(row):
        sf.write(json.dumps(row) + "\n")

    def _kill_dead(actor):
        ray_mod.kill(actor)
        try:
            ray_mod.get(actor.ping.remote(), timeout=10); return False
        except Exception:  # noqa: BLE001
            return True

    try:
        root, rlog = _popen(rcmd, boot, os.path.join(boot, "root.log"))
        _wait_for_file_nfs(os.path.join(boot, "root.ready"), 60, procs=[root])
        m["root_ready"] = _read_json(os.path.join(boot, "root.ready"))

        a1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0, max_concurrency=C,
                              scheduling_strategy=strat_a).remote(build_dir, hpx_threads, ep_a)
        pa = dict(_ray_get(ray_mod, a1.ray_placement.remote(), 60, "a_placement") or {})
        pa["placement_soft"] = False; m["a_placement"] = pa
        m["a_identity"] = _ray_get(ray_mod, a1.load_identity.remote(), 60, "a_identity")
        m["a_start"] = _ray_get(ray_mod, a1.start_hpx.remote(), 120, "a_start")
        m["a_child_report"] = _ray_get(ray_mod, a1.child_report.remote(), 30, "a_child_report")
        b1 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0, max_concurrency=C,
                              scheduling_strategy=strat_b).remote(build_dir, hpx_threads, ep_b)
        pb = dict(_ray_get(ray_mod, b1.ray_placement.remote(), 60, "b_placement") or {})
        pb["placement_soft"] = False; m["b_placement"] = pb
        m["b_identity"] = _ray_get(ray_mod, b1.load_identity.remote(), 60, "b_identity")
        m["b_start"] = _ray_get(ray_mod, b1.start_hpx.remote(), 120, "b_start")
        m["b_child_report"] = _ray_get(ray_mod, b1.child_report.remote(), 30, "b_child_report")

        a_loc = (m["a_start"] or {}).get("locality_id")
        b_loc = (m["b_start"] or {}).get("locality_id")
        a_pid = (m.get("a_identity") or {}).get("pid")
        b_pid = (m.get("b_identity") or {}).get("pid")
        identity_ok = (commit_matches((m["a_identity"].get("hpx_version_info") or {}).get("hpx_complete_version"))
                       and commit_matches((m["b_identity"].get("hpx_version_info") or {}).get("hpx_complete_version"))
                       and commit_matches((m.get("root_ready") or {}).get("hpx_complete_version")))
        hosting_ok = a_loc not in (None, 0) and b_loc not in (None, 0) and a_loc != b_loc

        soaks_all_passed = all_batches_passed = False
        if identity_ok and hosting_ok:
            _ray_get(ray_mod, a1.set_peer.remote(b1, b_loc), 30, "a_set_peer")
            _ray_get(ray_mod, b1.set_peer.remote(a1, a_loc), 30, "b_set_peer")
            fa = _ray_get(ray_mod, a1.freeze_peer.remote(), 30, "a_freeze")
            fb = _ray_get(ray_mod, b1.freeze_peer.remote(), 30, "b_freeze")
            m["a_peer_frozen"] = bool((fa or {}).get("peer_frozen"))
            m["b_peer_frozen"] = bool((fb or {}).get("peer_frozen"))
            meta = {"a": {"handle": a1, "pid": a_pid,
                          "host": (m["a_identity"] or {}).get("hostname"),
                          "node_id": (m["a_identity"] or {}).get("node_id"), "locality": a_loc},
                    "b": {"handle": b1, "pid": b_pid,
                          "host": (m["b_identity"] or {}).get("hostname"),
                          "node_id": (m["b_identity"] or {}).get("node_id"), "locality": b_loc}}
            m["pregate"] = run_pregate(ray_mod, meta)   # untimed 7-case correctness, every band-rep
            if m["pregate"]["pass"]:
                soak_case = next(c for c in TIMED_MATRIX if c["name"] == SLICE3_CASE)
                soaks_all_passed = True
                for arm in ("ray", "hpx"):
                    # Per-band, per-arm P3b soak. For C>=4 this IS the band's own capability proof;
                    # for C=2 it gates coordinator overlap + correctness only (peer overlap observed).
                    s = concurrency_soak(ray_mod, meta, "a", "b", arm, soak_case, C)
                    m["soaks"].append(s)
                    if not s["pass"]:
                        soaks_all_passed = False
                if soaks_all_passed:
                    all_batches_passed = True
                    case = next(c for c in TIMED_MATRIX if c["name"] == SLICE3_CASE)
                    for arm in ("ray", "hpx"):
                        print(f"[exp69-s3xn]   {band['band']} rep={rep} {arm}/{case['name']} "
                              f"(N={samp['N']} C={C}) ...", flush=True)
                        bmr = run_slice3_batch(ray_mod, meta, "a", "b", arm, case, C,
                                               samp["N"], samp["warmup_n"], samp["mode"],
                                               sample_writer, rep, band["band"])
                        m["batches"].append(bmr)
                        if not bmr["batch_pass"]:
                            all_batches_passed = False
        m["soaks_all_passed"] = soaks_all_passed
        m["all_batches_passed"] = all_batches_passed
        m["rep_capability"] = slice3_rep_capability(m["soaks"])   # strictly this band-rep's own soaks

        # Lifecycle: graceful leave -> destroy -> recreate on node -> root completion -> orphans.
        a_stop = _ray_get(ray_mod, a1.stop_hpx.remote(), 40, "a_stop")
        b_stop = _ray_get(ray_mod, b1.stop_hpx.remote(), 40, "b_stop")
        m["a_stop_rc"], m["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
        m["b_stop_rc"], m["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")
        dead_a1 = _kill_dead(a1); a1 = None
        dead_b1 = _kill_dead(b1); b1 = None
        a2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0, max_concurrency=C,
                              scheduling_strategy=strat_a).remote(build_dir, hpx_threads, ep_a)
        b2 = HpxActor.options(num_cpus=ray_cpus, max_restarts=0, max_concurrency=C,
                              scheduling_strategy=strat_b).remote(build_dir, hpx_threads, ep_b)
        m["a_recreate"] = _ray_get(ray_mod, a2.ping.remote(), 60, "a_recreate")
        m["b_recreate"] = _ray_get(ray_mod, b2.ping.remote(), 60, "b_recreate")
        ra, rb = m["a_recreate"] or {}, m["b_recreate"] or {}
        m["a_recreate_on_node"] = bool(_short(ra.get("hostname")) == _short(nodeA)
                                       and ra.get("pid") and ra.get("pid") != a_pid)
        m["b_recreate_on_node"] = bool(_short(rb.get("hostname")) == _short(nodeB)
                                       and rb.get("pid") and rb.get("pid") != b_pid)
        open(os.path.join(boot, "root.done"), "w").close()
        _wait_for_file_nfs(os.path.join(boot, "root.final"), 60, procs=[root])
        m["root_final"] = _read_json(os.path.join(boot, "root.final"))
        r_exited, r_rc, r_killed = _wait_proc(root, time.time() + 60)
        m["root_exit_path"] = _exit_path(r_exited, r_rc, r_killed)
        dead_a2 = _kill_dead(a2) if a2 is not None else True; a2 = None
        dead_b2 = _kill_dead(b2) if b2 is not None else True; b2 = None
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
        sf.close()

    ig = eval_throughput_island_gates(m, expect)   # accepted cross-node placement/lifecycle gate table
    ig_failed = [k for k, v in ig.items() if not v]
    rep_pass = (all(ig.values()) and bool(m["batches"])
                and all(b.get("batch_pass") for b in m["batches"])
                and all(s.get("pass") for s in m["soaks"]))
    return {"band": band["band"], "band_kind": band["kind"], "band_idx": band_idx,
            "C": C, "num_cpus": ray_cpus, "hpx_threads": hpx_threads, "rep": rep,
            "rep_pass": rep_pass, "island_gates": ig, "island_gates_failed": ig_failed,
            "rep_capability": m.get("rep_capability"),
            "boot_rel": os.path.relpath(boot, HERE),
            "samples_file_rel": os.path.relpath(samples_path, HERE),
            "batches": m["batches"], "soaks": m["soaks"],
            "placement": {"a": m.get("a_placement"), "b": m.get("b_placement"),
                          "root_host": (m.get("root_ready") or {}).get("hostname")},
            "markers": m}


def run_slice3_crossnode_phase(args):
    """Slice 3 DECISIVE cross-node causal-decomposition matrix on the accepted three-node topology
    (work-free root on nodeR; Ray actors A/B hard-placed on distinct medusa nodes; 10.42.5.x endpoints).
    Iterates the three resource bands x R reps, each a fresh island under its OWN resources, driving the
    timed diagnostic arms and recording per-arm goodput/latency-under-load + coordinator stage
    decomposition + peer-LOCAL (never-subtracted) timings + per-band capability. NO cross-arm
    ratio/difference/winner is computed; all four ratio fences stay false. The curated Slice 3 aggregate
    is written ONLY by one uninterrupted accepted invocation that completes the WHOLE matrix passing."""
    subnet = args.subnet_prefix
    mode = args.slice3_mode
    samp = dict(SLICE3_SAMPLING[mode])
    if args.reps is not None:
        samp["reps"] = args.reps
    if args.batch_n is not None:
        samp["N"] = args.batch_n
    if args.warmup_n is not None:
        samp["warmup_n"] = args.warmup_n
    agg = {"experiment": "69_same_axis_topk_perf", "phase": "rostam-cross-node-slice3",
           "slice3_mode": mode, "single_node": False, "cross_node": True,
           "ratio_fences": dict(RATIO_FENCES)}
    try:
        import ray
    except Exception as ex:  # noqa: BLE001
        agg["overall"] = "skip"; agg["reason"] = f"ray unavailable: {ex}"
        out = args.aggregate or os.path.join(HERE, "_exp69_runs", "slice3_skip_aggregate.json")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        _write_agg(out, agg); print("SKIP: ray unavailable"); return 0

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

    preflight = {"slurm_job_id": job_id, "slurm_nodelist": nodelist, "allocation_nodes": nodes,
                 "controller_host": here, "nodeR": nodeR, "nodeA": nodeA, "nodeB": nodeB,
                 "nodeR_ip": nodeR_ip, "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip,
                 "subnet_prefix": subnet}
    agg["preflight"] = preflight
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    runs_root = os.path.join(HERE, "_exp69_runs", f"slice3_{mode}_{job_id}_{runid}")
    out_path = args.aggregate or os.path.join(runs_root, "slice3_crossnode_aggregate.json")
    if problems:
        agg["overall"] = "fail_preflight"; agg["preflight_problems"] = problems
        agg["failure_class"] = ("three_node_allocation_unavailable"
                                if any("node" in p for p in problems)
                                else ("subnet_mismatch" if any("subnet" in p for p in problems)
                                      else "invalid_instrumentation"))
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        _write_agg(out_path, agg); print(f"[exp69-s3xn] PREFLIGHT FAIL: {problems}"); return 0

    print(f"[exp69-s3xn] job {job_id} nodes {nodes} | root {nodeR}={nodeR_ip} A {nodeA}={nodeA_ip} "
          f"B {nodeB}={nodeB_ip} | {mode} bands={[b['band'] for b in SLICE3_BANDS]} "
          f"N={samp['N']} reps {samp['reps']}")
    env = dict(os.environ)
    os.makedirs(runs_root, exist_ok=True)
    port = args.ray_port
    temp_dir = f"/tmp/exp69_s3_{job_id}_{runid}"
    port_flags = _ray_port_flags(args)
    head_proc = worker_a = worker_b = None
    band_rep_results, provenance = [], None
    fail_reason = None
    cluster = {}
    try:
        head_proc = _ray_head_local(nodeR_ip, port, temp_dir, args.head_num_cpus, env,
                                    os.path.join(runs_root, "head.log"), port_flags)
        okA, detA = _wait_gcs_from(nodeA, nodeR_ip, port, env, args.ray_ready_timeout)
        okB, detB = _wait_gcs_from(nodeB, nodeR_ip, port, env, args.ray_ready_timeout)
        agg["ray_gcs_ready_from_nodeA"], agg["ray_gcs_ready_from_nodeB"] = okA, okB
        if not (okA and okB and head_proc.poll() is None):
            raise RuntimeError(f"head GCS not reachable from A({detA}) or B({detB})")
        worker_a = _ray_worker_srun(nodeA, nodeA_ip, nodeR_ip, port, args.worker_num_cpus, env,
                                    os.path.join(runs_root, "worker_a.log"), port_flags)
        worker_b = _ray_worker_srun(nodeB, nodeB_ip, nodeR_ip, port, args.worker_num_cpus, env,
                                    os.path.join(runs_root, "worker_b.log"), port_flags)
        init_ok, init_attempts, init_tb = _bounded_ray_init(ray, f"{nodeR_ip}:{port}",
                                                            args.ray_init_timeout)
        agg["ray_init_ok"] = init_ok
        if not init_ok:
            agg["ray_init_traceback"] = init_tb; raise RuntimeError("ray.init to local head failed")
        nodes_ready, seen = _wait_ray_nodes(ray, 3, args.ray_ready_timeout)
        if not nodes_ready:
            raise RuntimeError(f"only {seen}/3 ray nodes alive")
        alive = [n for n in ray.nodes() if n.get("Alive")]

        def _match(ip, host):
            return [n for n in alive if n.get("NodeManagerAddress") == ip
                    or _short(n.get("NodeName")) == _short(host)]
        mR, mA, mB = _match(nodeR_ip, nodeR), _match(nodeA_ip, nodeA), _match(nodeB_ip, nodeB)
        nodeR_nid = mR[0]["NodeID"] if len(mR) == 1 else None
        nodeA_nid = mA[0]["NodeID"] if len(mA) == 1 else None
        nodeB_nid = mB[0]["NodeID"] if len(mB) == 1 else None
        driver = _self_identity(ray)
        cluster = {"root_node": nodeR, "actor_a_node": nodeA, "actor_b_node": nodeB,
                   "root_ip": nodeR_ip, "actor_a_ip": nodeA_ip, "actor_b_ip": nodeB_ip,
                   "nodeR_ray_node_id": nodeR_nid, "nodeA_ray_node_id": nodeA_nid,
                   "nodeB_ray_node_id": nodeB_nid, "driver_node_id": driver["node_id"],
                   "driver_on_nodeR": bool(driver["node_id"] and driver["node_id"] == nodeR_nid)}
        agg["ray_cluster"] = cluster
        if not (nodeA_nid and nodeB_nid and len({nodeR_nid, nodeA_nid, nodeB_nid}) == 3):
            raise RuntimeError(f"ray node-id resolution failed (mR={len(mR)} mA={len(mA)} mB={len(mB)})")
        if not cluster["driver_on_nodeR"]:
            raise RuntimeError("driver is not on nodeR (must be co-located with the Ray head)")

        HpxActor = build_actor_class(ray)
        expect = {"slurm_job_id": job_id, "nodes": nodes, "nodeR": nodeR, "nodeA": nodeA,
                  "nodeB": nodeB, "nodeR_nid": nodeR_nid, "nodeA_nid": nodeA_nid,
                  "nodeB_nid": nodeB_nid, "subnet": subnet}
        for band_idx, band in enumerate(SLICE3_BANDS):
            for rep in range(1, samp["reps"] + 1):
                print(f"[exp69-s3xn] band {band['band']} ({band['kind']}) rep {rep} ...", flush=True)
                rr = run_slice3_crossnode_rep(ray, HpxActor, peer, os.path.abspath(args.build_dir),
                                              band, band_idx, rep, runs_root, samp, nodeR, nodeA,
                                              nodeB, nodeR_ip, nodeA_ip, nodeB_ip, nodeA_nid,
                                              nodeB_nid, subnet, args.port_base, expect, env)
                if provenance is None:
                    provenance = collect_provenance(ray, (rr.get("markers") or {}).get("a_identity"))
                with open(os.path.join(runs_root, f"{band['band']}_rep_{rep}", "markers.json"), "w") as f:
                    json.dump(rr["markers"], f, indent=2)
                rr.pop("markers", None)
                print(f"[exp69-s3xn]   band {band['band']} rep {rep}: "
                      f"{'PASS' if rr['rep_pass'] else 'FAIL'} failed={rr['island_gates_failed']} "
                      f"cap={rr.get('rep_capability')}", flush=True)
                band_rep_results.append(rr)
    except Exception as e:  # noqa: BLE001
        fail_reason = f"{type(e).__name__}: {str(e)[:300]}"
        agg["slice3_exception"] = fail_reason
        agg["slice3_traceback"] = traceback.format_exc()[-2000:]
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

    full = build_slice3_aggregate("crossnode_three_node_hard_placement", samp, band_rep_results,
                                  provenance)
    full["phase"] = "rostam-cross-node-slice3"
    full["slice3_mode"] = mode
    full["single_node"] = False
    full["cross_node"] = True
    full["ray_cluster"] = agg.get("ray_cluster")
    full["preflight"] = preflight
    full["final_orphans"] = agg.get("final_orphans")
    full["claim_fences"] = slice3_crossnode_claim_fences()   # cross-node + throughput-shaped
    full["resources"] = {
        "per_band": [{"band": b["band"], "C": b["C"], "num_cpus": b["num_cpus"],
                      "hpx_threads": b["hpx_threads"]} for b in SLICE3_BANDS],
        "note": ("resources are configured per band; the (C, num_cpus, hpx_threads) tuple is the band "
                 "identity; within a band the Ray and HPX arms receive the SAME resources -- never given "
                 "to only one arm")}
    if fail_reason:
        full["slice3_exception"] = fail_reason
        full["slice3_traceback"] = agg.get("slice3_traceback")
    complete = slice3_matrix_complete(band_rep_results, len(SLICE3_BANDS), samp["reps"])
    full["matrix_complete"] = complete
    overall_pass = (fail_reason is None and complete
                    and all(r["rep_pass"] for r in band_rep_results)
                    and agg.get("final_orphans", {}).get("no_ray_orphans")
                    and agg.get("final_orphans", {}).get("no_peer_orphans"))
    full["overall"] = "pass" if overall_pass else "fail"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    _write_agg(out_path, full)
    print(f"[exp69-s3xn] overall: {full['overall']} (matrix_complete={complete}) -> {out_path}")
    # Curated Slice 3 aggregate: ONE uninterrupted accepted invocation, WHOLE matrix passing, no override.
    if mode == "accepted" and full["overall"] == "pass" and args.aggregate is None:
        curated = os.path.join(HERE, "same_axis_topk_slice3_crossnode_aggregate.json")
        _write_agg(curated, full)
        print(f"[exp69-s3xn] curated slice3 aggregate -> {curated}")
    return 0 if full["overall"] == "pass" else 1


# ---------------------------------------------------------------------------------------
# Selftest: pure gate/oracle/stats/witness/scheduler checks (no Ray, no HPX)
# ---------------------------------------------------------------------------------------

def _synthetic_counters(out_f=0, srv_f=0, act=0):
    return {"a": {"outgoing_ray_fetch": out_f, "serving_ray_fetch": 0, "hpx_actions_served": 0},
            "b": {"outgoing_ray_fetch": 0, "serving_ray_fetch": srv_f, "hpx_actions_served": act}}


def _synthetic_result(arm, case, meta_a, meta_b, direction="A"):
    oc = case_oracle(case)
    own_key, peer_key = ("a", "b") if direction == "A" else ("b", "a")
    own, peer = (meta_a, meta_b) if direction == "A" else (meta_b, meta_a)
    res = {"arm": arm, "ready": True, "target_found": True,
           "global_topk": [list(p) for p in oc["global"]],
           "own_topk": [list(p) for p in oc[own_key]],
           "peer_topk": [list(p) for p in oc[peer_key]],
           "peer_pid": peer["pid"], "peer_host": peer["host"],
           "own_pid": own["pid"], "own_locality": own["locality"], "own_host": own["host"]}
    if arm == "hpx":
        res["peer_locality"] = peer["locality"]
        res["hpx_composition"] = {"action_dispatched": True, "future_ready": True,
                                  "continuation_executed": True, "result_delivered": True}
    else:
        res["peer_node_id"] = peer["node_id"]
    return res


def _synthetic_case_summary(case, direction, K, phase="accepted"):
    els = list(range(1_000_000, 1_000_000 + K))
    return {"case": case["name"], "direction": direction, "W": 5, "K": K, "phase": phase,
            "flags": [], "witness_pairs_checked": K, "arm_order_ok": True,
            "block_delta_ok": True, "invalid": {"ray": 0, "hpx": 0}, "timeouts": 0,
            "stats": {arm: stats_block(els, K, phase) for arm in ("ray", "hpx")}}


def _synthetic_rep_markers():
    cv = f"HPX: V2.0.0 (AGAS: V3.0), Git: {EXPECTED_HPX_COMMIT[:10]}"
    K = 500
    m = {
        "controller_hostname": "host0", "controller_pid": 42,
        "argv_audit_ok": True, "actor_hpx_threads": 2, "actor_ray_num_cpus": 2,
        "resources_single_constants": True, "single_controller_clock_only": True,
        "all_waits_bounded": True, "ratio_fences": dict(RATIO_FENCES),
        "b_control_expected": True,
        "a_identity": {"pid": 5001, "hostname": "host0", "node_id": "nid0",
                       "hpx_version_info": {"hpx_complete_version": cv}},
        "b_identity": {"pid": 5002, "hostname": "host0", "node_id": "nid0",
                       "hpx_version_info": {"hpx_complete_version": cv}},
        "a_start": {"started": True, "locality_id": 1, "membership": 2, "pid": 5001},
        "b_start": {"started": True, "locality_id": 2, "membership": 3, "pid": 5002},
        "a_child_report": {"checked": True, "hpx_children": []},
        "b_child_report": {"checked": True, "hpx_children": []},
        "root_ready": {"locality_id": 0, "hostname": "host0", "wall_ms": 1000,
                       "hpx_complete_version": cv},
        "root_final": {"max_membership": 3, "final_membership": 1, "leave_observed": True,
                       "actions_served_on_root": 0},
        "timer": {"clock": "time.perf_counter_ns", "n_reads": 10000,
                  "clock_monotonic_declared": True, "nonnegative_deltas": True,
                  "median_overhead_ns": 30, "p90_overhead_ns": 40,
                  "max_overhead_ns_diagnostic": 900, "reported_resolution_s": 1e-9},
        "pregate": {"pass": True, "flags": [],
                    "cases": [{"name": c["name"], "gates": {}, "failed": []}
                              for c in CORRECTNESS_MATRIX]},
        "timed_A": [_synthetic_case_summary(c, "A", K) for c in TIMED_MATRIX],
        "timed_B": [_synthetic_case_summary(c, "B", 200) for c in TIMED_MATRIX
                    if c["name"] in B_CONTROL_CASES],
        "a_stop_rc": 0, "a_stop_error": None, "b_stop_rc": 0, "b_stop_error": None,
        "a_destroyed": True, "b_destroyed": True,
        "a_recreate": {"ok": True, "pid": 6001}, "b_recreate": {"ok": True, "pid": 6002},
        "root_exit_path": "finalized_clean", "actor_pids_gone": True, "orphans": [],
    }
    for cr in m["timed_A"]:
        cr["K"] = K
    for cr in m["timed_B"]:
        cr["K"] = 200
        cr["witness_pairs_checked"] = 200
    return m


def selftest():
    failures = []

    def check(cond, label):
        if not cond:
            failures.append(label)

    # ---- deterministic float32 bit patterns (known IEEE-754 values; exp68 verbatim) ----
    for grid, want in [(0, 0x00000000), (8, 0x3F800000), (-8, 0xBF800000),
                       (1, 0x3E000000), (-1, 0xBE000000), (65, 0x41020000)]:
        got = struct.unpack("<I", struct.pack("<f", grid / 8.0))[0]
        check(got == want, f"float32 bits grid={grid}: got {hex(got)} want {hex(want)}")

    # ---- tie-break contract ----
    seed = 1
    by_bits = {}
    for t in range(0, 300):
        by_bits.setdefault(logit_bits(t, seed), []).append(t)
    tie_group = next((ts for ts in by_bits.values() if len(ts) >= 2), None)
    check(bool(tie_group), "expected at least one tie group in [0,300)")
    if tie_group:
        ts = sorted(tie_group[:2])
        b = logit_bits(ts[0], seed)
        pair = [(ts[1], b), (ts[0], b)]
        pair.sort(key=lambda c: (-f32(c[1]), c[0]))
        check([t for t, _ in pair] == ts, "tie-break must rank lower token id first")

    # ---- correctness matrix scenario coverage (exp68 semantics preserved) ----
    def top(case):
        return oracle_topk(0, case["V"], case["seed"], case["k"])

    def shardA_count(case):
        return sum(1 for t, _ in top(case) if t < case["split"])

    for c in CORRECTNESS_MATRIX + TIMED_MATRIX:
        check(partition_ok(c["V"], c["split"]), f"{c['name']}: invalid partition")
    tie = next(c for c in CORRECTNESS_MATRIX if "tie_at_cutoff" in c["tags"])
    o = oracle_topk(0, tie["V"], tie["seed"], tie["k"] + 1)
    check(f32(o[tie["k"] - 1][1]) == f32(o[tie["k"]][1]), "tie_cutoff must straddle the cutoff")
    dom = next(c for c in CORRECTNESS_MATRIX if "one_shard_dominant" in c["tags"])
    check(shardA_count(dom) >= (dom["k"] // 2 + 1), "shardA_dom must have a dominant shard")
    for c in CORRECTNESS_MATRIX:
        if "both_shards" in c["tags"]:
            a = shardA_count(c)
            check(0 < a < c["k"], f"{c['name']}: both shards must contribute")

    # ---- timed matrix contract: k strictly smaller than each shard; approved names only ----
    for c in TIMED_MATRIX:
        check(c["k"] < c["split"] and c["k"] < c["V"] - c["split"],
              f"{c['name']}: k must be < each shard size")
    check([c["name"] for c in TIMED_MATRIX] == ["P0", "P1", "P2", "P3a", "P3b", "P3c"],
          "timed matrix must be exactly the approved six cases")
    check(set(B_CONTROL_CASES) == {"P0", "P2", "P3b"}, "B control must be exactly P0/P2/P3b")
    check(SAMPLING["accepted"]["A"] == {"W": 50, "K": 500}
          and SAMPLING["accepted"]["B"] == {"W": 20, "K": 200}
          and SAMPLING["accepted"]["reps"] == 3
          and SAMPLING["smoke"]["A"] == {"W": 5, "K": 30} and SAMPLING["smoke"]["reps"] == 1,
          "sampling plans must match the approved design")

    # ---- statistics ----
    xs = list(range(0, 101))
    check(percentile(xs, 50) == 50 and percentile(xs, 90) == 90 and percentile(xs, 99) == 99,
          "percentile on 0..100")
    check(percentile(xs, 75) - percentile(xs, 25) == 50, "IQR on 0..100")
    st = stats_block([1, 2, 3, 4, 100], 5, "accepted")
    check(st["p50_ns"] == 3 and st["mad_ns"] == 1 and st["n"] == 5, "stats_block median/MAD")
    check(st["p99_scope"] == "n=5" and st["p99_reliable"] is False, "p99 scope annotation")
    check(median([5, 1, 9]) == 5, "median")

    # ---- pair scheduler + order re-derivation ----
    check(pair_order(0) == ("ray", "hpx") and pair_order(1) == ("hpx", "ray"), "pair order policy")
    good = [(i, p, pair_order(i)[p]) for i in range(6) for p in (0, 1)]
    check(order_violations(good) == [], "clean order log has no violations")
    bad = list(good)
    bad[3] = (1, 1, "hpx")  # pair 1 pos 1 must be ray
    check(order_violations(bad) == [(1, 1, "hpx")], "order validator must flag a swapped arm")

    # ---- witness delta logic ----
    p0 = _synthetic_counters(0, 0, 0)
    ok, cls = pair_witness_check(p0, _synthetic_counters(1, 1, 1), "a", "b")
    check(ok and cls is None, "clean pair deltas must pass")
    ok, cls = pair_witness_check(p0, _synthetic_counters(2, 2, 1), "a", "b")
    check(not ok and cls == "ray_peer_used_by_hpx_arm", "extra ray fetch -> ray_peer_used_by_hpx_arm")
    ok, cls = pair_witness_check(p0, _synthetic_counters(1, 1, 2), "a", "b")
    check(not ok and cls == "hpx_action_used_by_ray_arm", "extra action -> hpx_action_used_by_ray_arm")
    ok, cls = pair_witness_check(p0, _synthetic_counters(0, 0, 1), "a", "b")
    check(not ok and cls == "ray_peer_witness_missing", "missing fetch -> ray_peer_witness_missing")
    ok, cls = pair_witness_check(p0, _synthetic_counters(1, 1, 0), "a", "b")
    check(not ok and cls == "hpx_action_witness_missing", "missing action -> hpx_action_witness_missing")
    wrong = _synthetic_counters(1, 1, 1)
    wrong["a"]["hpx_actions_served"] = 1  # action on the coordinator side
    check(pair_witness_check(p0, wrong, "a", "b") == (False, "invalid_instrumentation"),
          "wrong-side delta -> invalid_instrumentation")
    check(block_delta_check(p0, _synthetic_counters(30, 30, 30), "a", "b", 30),
          "clean block delta")
    check(not block_delta_check(p0, _synthetic_counters(30, 31, 30), "a", "b", 30),
          "off-by-one block delta must fail")

    # ---- result verification (strictly-after-t1 checker) ----
    meta_a = {"pid": 5001, "host": "host0", "node_id": "nid0", "locality": 1}
    meta_b = {"pid": 5002, "host": "host0", "node_id": "nid0", "locality": 2}
    case = CORRECTNESS_MATRIX[2]
    og = case_oracle(case)["global"]
    for arm in ("ray", "hpx"):
        ok, reasons = verify_timed_result(arm, _synthetic_result(arm, case, meta_a, meta_b),
                                          og, meta_b, meta_a)
        check(ok, f"clean {arm} result must verify: {reasons}")
    r = _synthetic_result("ray", case, meta_a, meta_b)
    r["global_topk"][0] = [r["global_topk"][0][0], r["global_topk"][0][1] ^ 0x1]
    ok, reasons = verify_timed_result("ray", r, og, meta_b, meta_a)
    check(not ok and "float32_bit_mismatch" in reasons, "bit flip -> float32_bit_mismatch")
    r = _synthetic_result("ray", case, meta_a, meta_b)
    r["global_topk"][0] = [999999, 0]
    ok, reasons = verify_timed_result("ray", r, og, meta_b, meta_a)
    check(not ok and "oracle_mismatch" in reasons, "wrong ids -> oracle_mismatch")
    r = _synthetic_result("ray", case, meta_a, meta_b)
    r["hpx_composition"] = {"action_dispatched": True}
    ok, reasons = verify_timed_result("ray", r, og, meta_b, meta_a)
    check(not ok and "hpx_witness_in_ray_arm" in reasons, "hpx witness in ray arm must fail")
    r = _synthetic_result("hpx", case, meta_a, meta_b)
    r["hpx_composition"]["continuation_executed"] = False
    ok, reasons = verify_timed_result("hpx", r, og, meta_b, meta_a)
    check(not ok and "hpx_composition_witness_missing" in reasons,
          "missing continuation -> composition witness missing")
    r = _synthetic_result("hpx", case, meta_a, meta_b)
    r["peer_pid"] = 9999
    ok, reasons = verify_timed_result("hpx", r, og, meta_b, meta_a)
    check(not ok and "peer_pid_mismatch" in reasons, "wrong peer pid must fail")

    # ---- pre-gate evaluation on synthetic results ----
    meta = {"a": dict(meta_a), "b": dict(meta_b)}
    rr = {"a_hpx": _synthetic_result("hpx", case, meta_a, meta_b, "A"),
          "a_ray": _synthetic_result("ray", case, meta_a, meta_b, "A"),
          "b_hpx": _synthetic_result("hpx", case, meta_a, meta_b, "B"),
          "b_ray": _synthetic_result("ray", case, meta_a, meta_b, "B")}
    g = eval_pregate_case(case, rr, case_oracle(case), meta)
    check(all(g.values()), f"clean pre-gate case must pass: {[k for k, v in g.items() if not v]}")
    rr2 = {k: json.loads(json.dumps(v)) for k, v in rr.items()}
    rr2["a_ray"]["own_topk"][0] = [999, 0]
    g2 = eval_pregate_case(case, rr2, case_oracle(case), meta)
    check("native_compute_mismatch" in pregate_failure_flags([(case["name"], g2)]),
          "wrong ray-arm local -> native_compute_mismatch")
    rr3 = {k: json.loads(json.dumps(v)) for k, v in rr.items()}
    rr3["a_ray"]["global_topk"][0] = [rr3["a_ray"]["global_topk"][0][0],
                                      rr3["a_ray"]["global_topk"][0][1] ^ 0x1]
    g3 = eval_pregate_case(case, rr3, case_oracle(case), meta)
    check("float32_bit_mismatch" in pregate_failure_flags([(case["name"], g3)]),
          "pre-gate bit flip -> float32_bit_mismatch")

    # ---- rep gate table + classification ----
    m = _synthetic_rep_markers()
    gates = eval_rep_gates(m)
    flags = collect_rep_flags(m, gates)
    check(all(gates.values()) and not flags,
          f"clean synthetic rep must pass all gates: {[k for k, v in gates.items() if not v]} {flags}")
    check(classify_rep(gates, flags) == "pass", "clean rep must classify pass")

    def expect_class(mut, cls, label):
        mm = _synthetic_rep_markers()
        mut(mm)
        gg = eval_rep_gates(mm)
        fc = classify_rep(gg, collect_rep_flags(mm, gg))
        if fc != cls:
            failures.append(f"{label}: expected {cls}, got {fc}")

    expect_class(lambda mm: mm["a_identity"]["hpx_version_info"].__setitem__(
        "hpx_complete_version", "Git: deadbeef12"), "hpx_identity_mismatch", "wrong HPX commit")
    expect_class(lambda mm: mm["a_identity"].__setitem__("hostname", "otherhost"),
                 "actor_placement_invalid", "controller/actor host mismatch")
    expect_class(lambda mm: mm.__setitem__("resources_single_constants", False),
                 "resource_mismatch_between_arms", "non-constant resources")
    expect_class(lambda mm: mm.__setitem__("single_controller_clock_only", False),
                 "cross_node_clock_subtraction_detected", "cross-clock arithmetic")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("oracle_mismatch_in_timed_sample"),
                 "oracle_mismatch_in_timed_sample", "invalid timed sample")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("float32_bit_mismatch"),
                 "float32_bit_mismatch", "bit mismatch in timed sample")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("timeout"),
                 "timeout", "timed sample timeout")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("arm_order_violation"),
                 "arm_order_violation", "arm order violation")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("ray_peer_used_by_hpx_arm"),
                 "ray_peer_used_by_hpx_arm", "hpx arm used ray peer")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("hpx_action_used_by_ray_arm"),
                 "hpx_action_used_by_ray_arm", "ray arm used hpx action")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("timer_overhead_too_high"),
                 "timer_overhead_too_high", "timer overhead flag")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("warmup_incomplete"),
                 "warmup_incomplete", "warmup failure")
    expect_class(lambda mm: mm["timed_A"][0]["flags"].append("membership_changed_during_measurement"),
                 "membership_changed_during_measurement", "membership churn")
    expect_class(lambda mm: mm["timed_A"][0]["stats"]["ray"].__setitem__("n", 499),
                 "insufficient_valid_samples", "sample shortfall")
    expect_class(lambda mm: mm["a_recreate"].__setitem__("pid", 5001),
                 "actor_recreation_failed", "recreation same pid")
    expect_class(lambda mm: mm.__setitem__("orphans", ["9"]), "orphan_detected", "orphan present")
    expect_class(lambda mm: mm["b_child_report"].__setitem__(
        "hpx_children", [{"pid": 1, "cmd": "exp69_peer"}]), "lifecycle_failed", "HPX child")
    expect_class(lambda mm: mm["root_final"].__setitem__("actions_served_on_root", 3),
                 "invalid_instrumentation", "application work on root")
    expect_class(lambda mm: mm["timed_B"].pop(), "invalid_instrumentation",
                 "missing B-control case block")

    # ---- aggregate fences: fences false; NO ratio/speedup keys anywhere ----
    m2 = _synthetic_rep_markers()
    g2 = eval_rep_gates(m2)
    rep_out = {"rep": 1, "boot_rel": "x", "samples_file_rel": "x", "gates": g2,
               "gates_failed": [], "flags": [], "rep_pass": True, "failure_class": "pass",
               "timer": m2["timer"], "pregate": {"pass": True, "cases": []},
               "timed_A": m2["timed_A"], "timed_B": m2["timed_B"]}
    agg = build_aggregate("accepted", SAMPLING["accepted"], [rep_out], {"note": "selftest"})
    check(agg["ratio_fences"] == RATIO_FENCES and not any(agg["ratio_fences"].values()),
          "aggregate ratio fences must all be false")
    blob = json.dumps(agg).lower()
    for banned in ("times faster", "\"speedup\":", "relative latency", "\"winner\":", "\"ratio\":"):
        check(banned not in blob, f"aggregate must not contain '{banned}'")
    ar = agg["across_reps_A_coordinated"]
    check(set(ar.keys()) == {c["name"] for c in TIMED_MATRIX}, "across-reps covers all six cases")
    check(all(set(v.keys()) == {"ray", "hpx"} for v in ar.values()),
          "across-reps keeps arms separate (never differenced)")
    check("throughput" not in json.dumps(SAMPLING), "no throughput phase exists (phase_mixing fence)")

    check(argv_audit(["peer", "--hpx:agas=x"]) and not argv_audit(["peer", "--hpx:localities=3"]),
          "argv audit")
    check(set(CLASS_PRIORITY) | {"pass"} == set(FAILURE_CLASSES),
          "class priority must cover the full taxonomy")

    # ---- cross-node gate fixtures: three-node placement REPLACES the same-host gate ----
    def cx_markers():
        mm = _synthetic_rep_markers()
        mm["controller_hostname"] = "medusa00"
        mm["a_identity"].update({"hostname": "medusa01", "node_id": "nidA"})
        mm["b_identity"].update({"hostname": "medusa02", "node_id": "nidB"})
        mm["root_ready"]["hostname"] = "medusa00"
        mm["a_placement"] = {"node_id": "nidA", "hostname": "medusa01", "placement_soft": False,
                             "pid": 5001}
        mm["b_placement"] = {"node_id": "nidB", "hostname": "medusa02", "placement_soft": False,
                             "pid": 5002}
        mm["endpoints_subnet_ok"] = True
        mm["remote_orphan_check_ran"] = True
        mm["a_recreate"] = {"ok": True, "pid": 6001, "hostname": "medusa01"}
        mm["b_recreate"] = {"ok": True, "pid": 6002, "hostname": "medusa02"}
        mm["a_recreate_on_node"] = True
        mm["b_recreate_on_node"] = True
        return mm

    def cx_expect():
        return {"slurm_job_id": "12345", "nodes": ["medusa00", "medusa01", "medusa02"],
                "nodeR": "medusa00", "nodeA": "medusa01", "nodeB": "medusa02",
                "nodeR_nid": "nidR", "nodeA_nid": "nidA", "nodeB_nid": "nidB", "subnet": "10.42.5."}

    cxm = cx_markers()
    cxg = eval_rep_gates_crossnode(cxm, cx_expect())
    cxf = collect_rep_flags_crossnode(cxm, cxg)
    check(all(cxg.values()) and not cxf,
          f"clean cross-node rep must pass all gates: {[k for k, v in cxg.items() if not v]} {cxf}")
    check(classify_rep(cxg, cxf) == "pass", "clean cross-node rep must classify pass")
    check("actor_placement_valid" not in cxg,
          "cross-node gates must DROP the local same-host actor_placement_valid gate")
    for gk in ("three_node_slurm_allocation", "root_on_root_node", "actor_a_hard_placed",
               "actor_b_hard_placed", "actor_nodes_distinct", "three_roles_three_nodes",
               "endpoints_pinned_subnet", "evidence_fields_complete_crossnode",
               "actor_a_recreated_on_node", "actor_b_recreated_on_node"):
        check(gk in cxg, f"cross-node gates must add {gk}")
    # local gate semantics UNCHANGED: the same-host gate is still present + evaluated locally.
    check("actor_placement_valid" in eval_rep_gates(_synthetic_rep_markers()),
          "local gate table must still contain actor_placement_valid")

    def cx_expect_class(mut, cls, label):
        mm = cx_markers(); mut(mm)
        gg = eval_rep_gates_crossnode(mm, cx_expect())
        ff = collect_rep_flags_crossnode(mm, gg)
        fc = classify_rep(gg, ff)
        if fc != cls:
            failures.append(f"cross-node {label}: expected {cls}, got {fc}")

    cx_expect_class(lambda mm: mm["a_placement"].update({"hostname": "medusa00", "node_id": "nidR"}),
                    "actor_placement_invalid", "A on root node")
    cx_expect_class(lambda mm: mm["a_placement"].__setitem__("placement_soft", True),
                    "actor_placement_invalid", "A soft placement")
    cx_expect_class(lambda mm: mm.__setitem__("endpoints_subnet_ok", False),
                    "actor_placement_invalid", "endpoints off requested subnet")
    cx_expect_class(lambda mm: mm["b_placement"].__setitem__("hostname", "medusa01"),
                    "actor_placement_invalid", "actors not on distinct nodes")
    cx_expect_class(lambda mm: mm.__setitem__("remote_orphan_check_ran", False),
                    "invalid_instrumentation", "remote orphan check did not run")
    cx_expect_class(lambda mm: mm.__setitem__("a_recreate_on_node", False),
                    "actor_recreation_failed", "actor A recreated off its node")
    cx_expect_class(lambda mm: mm["a_identity"]["hpx_version_info"].__setitem__(
        "hpx_complete_version", "Git: deadbeef12"), "hpx_identity_mismatch", "wrong HPX commit")
    cx_expect_class(lambda mm: mm["timed_A"][0]["flags"].append("oracle_mismatch_in_timed_sample"),
                    "oracle_mismatch_in_timed_sample", "invalid cross-node timed sample")

    cx_rep_out = {"rep": 1, "boot_rel": "x", "samples_file_rel": "x", "gates": cxg,
                  "gates_failed": [], "flags": [], "rep_pass": True, "failure_class": "pass",
                  "timer": cxm["timer"], "pregate": {"pass": True, "cases": []},
                  "timed_A": cxm["timed_A"], "timed_B": cxm["timed_B"],
                  "placement": {"a": cxm["a_placement"], "b": cxm["b_placement"]}}
    cxagg = build_crossnode_aggregate(
        "accepted", SAMPLING["accepted"], [cx_rep_out], {"note": "selftest"},
        {"root_node": "medusa00", "actor_a_node": "medusa01", "actor_b_node": "medusa02"},
        {"subnet_prefix": "10.42.5."})
    check(cxagg["ratio_fences"] == RATIO_FENCES and not any(cxagg["ratio_fences"].values()),
          "cross-node aggregate ratio fences must all be false")
    check(cxagg["single_node"] is False and cxagg["cross_node"] is True,
          "cross-node aggregate must declare cross_node")
    check(cxagg["claim_fences"]["not_cross_node"] is False,
          "cross-node aggregate must flip the not_cross_node claim fence to False")
    cxblob = json.dumps(cxagg).lower()
    for banned in ("times faster", "\"speedup\":", "relative latency", "\"winner\":", "\"ratio\":"):
        check(banned not in cxblob, f"cross-node aggregate must not contain '{banned}'")
    car = cxagg["across_reps_A_coordinated"]
    check(set(car.keys()) == {c["name"] for c in TIMED_MATRIX},
          "cross-node across-reps covers all six cases")
    check(all(set(v.keys()) == {"ray", "hpx"} for v in car.values()),
          "cross-node across-reps keeps arms separate (never differenced)")

    # ---- Slice 2 throughput gate/taxonomy fixtures (pure; no Ray/HPX) ----
    def _tp_batch(arm="ray", C=4, N=1000):
        els = list(range(2_000_000, 2_000_000 + N))
        wd = ({"coord_outgoing_ray_fetch": N, "peer_serving_ray_fetch": N,
               "peer_hpx_actions_served": 0, "coord_serving_ray_fetch": 0,
               "coord_hpx_actions_served": 0, "peer_outgoing_ray_fetch": 0} if arm == "ray"
              else {"coord_outgoing_ray_fetch": 0, "peer_serving_ray_fetch": 0,
                    "peer_hpx_actions_served": N, "coord_serving_ray_fetch": 0,
                    "coord_hpx_actions_served": 0, "peer_outgoing_ray_fetch": 0})
        aw = {"coordinate_active_max": C, "ray_peer_active_max": (C if arm == "ray" else 0),
              "hpx_action_active_max": (C if arm == "hpx" else 0),
              "coord_currents": {"coordinate_active_current": 0, "ray_peer_active_current": 0,
                                 "hpx_action_active_current": 0},
              "peer_currents": {"coordinate_active_current": 0, "ray_peer_active_current": 0,
                                "hpx_action_active_current": 0},
              "coord_underflow": False, "peer_underflow": False}
        return {"arm": arm, "case": "P3b", "C": C, "N": N, "warmup_n": 100, "direction": "A",
                "rep": 1, "flags": [], "timeouts": 0, "peer_frozen": True, "n_completed": N,
                "completion_rate_per_s": 123.0, "elapsed_s": N / 123.0,
                "latency_stats": stats_block(els, N, "accepted"),
                "verifier": {"capacity": VERIFIER_QUEUE_CAPACITY, "high_water": 5,
                             "enqueue_block_ns": 0, "enqueue_attempts": N, "verified": N,
                             "invalid": 0, "first_invalid": None, "exception": None,
                             "max_verify_latency_ns": 1000},
                "witness_deltas": wd, "active_witness": aw,
                "outstanding": {"mean": C - 0.02, "max": C, "fraction_at_C": 0.99, "target_C": C}}

    for arm in ("ray", "hpx"):
        tb = _tp_batch(arm)
        tg = eval_throughput_batch_gates(tb)
        check(all(tg.values()),
              f"clean {arm} throughput batch passes all gates: {[k for k, v in tg.items() if not v]}")
        check(classify_throughput_batch(tb, tg) == "pass", f"clean {arm} throughput batch is pass")

    def tp_class(arm, mut, cls, label):
        b = _tp_batch(arm); mut(b)
        gg = eval_throughput_batch_gates(b)
        fc = classify_throughput_batch(b, gg)
        if fc != cls:
            failures.append(f"throughput {label}: expected {cls}, got {fc}")

    tp_class("ray", lambda b: b["verifier"].__setitem__("enqueue_block_ns", 5),
             "verifier_backpressure_contaminated_batch", "enqueue blocked")
    tp_class("ray", lambda b: b["verifier"].__setitem__("high_water", VERIFIER_QUEUE_CAPACITY),
             "verifier_backpressure_contaminated_batch", "queue full")
    tp_class("ray", lambda b: b["verifier"].__setitem__("exception", "Boom"),
             "verifier_exception", "verifier exception")
    tp_class("ray", lambda b: b["verifier"].__setitem__("invalid", 1),
             "throughput_batch_invalidated_by_invalid_result", "invalid result")
    tp_class("ray", lambda b: b["active_witness"].__setitem__("coord_underflow", True),
             "active_call_counter_underflow", "counter underflow")
    tp_class("ray", lambda b: b["active_witness"]["coord_currents"].__setitem__(
        "coordinate_active_current", 1), "active_call_counter_leak", "counter leak")
    tp_class("ray", lambda b: b["witness_deltas"].__setitem__("peer_hpx_actions_served", 3),
             "hpx_action_used_by_ray_arm", "ray batch used hpx action")
    tp_class("hpx", lambda b: b["witness_deltas"].__setitem__("peer_serving_ray_fetch", 3),
             "ray_peer_used_by_hpx_arm", "hpx batch used ray peer")
    tp_class("ray", lambda b: b["active_witness"].__setitem__("coordinate_active_max", 1),
             "coordinator_service_serialized", "coordinator serialized")   # (proof 5) coord max 1 fails
    tp_class("hpx", lambda b: b["active_witness"].__setitem__("coordinate_active_max", 1),
             "coordinator_service_serialized", "hpx coordinator serialized")
    # (proofs 3/4) peer overlap is OBSERVATION-ONLY for a measured batch: peer active max 1 (short
    # service window / C=2) is NOT serialization and must NOT invalidate the batch on its own.
    for _arm in ("ray", "hpx"):
        _b = _tp_batch(_arm)
        _b["active_witness"]["ray_peer_active_max"] = 1
        _b["active_witness"]["hpx_action_active_max"] = 1
        _bg = eval_throughput_batch_gates(_b)
        check("peer_service_overlap" not in _bg, f"{_arm} batch has no peer_service_overlap gate")
        check(classify_throughput_batch(_b, _bg) == "pass",
              f"{_arm} batch with peer active max 1 is pass (observation-only, not serialized)")
    tp_class("ray", lambda b: b["outstanding"].__setitem__("mean", 1.0),
             "achieved_concurrency_below_target", "achieved concurrency below target")
    tp_class("ray", lambda b: b.__setitem__("peer_frozen", False),
             "peer_config_mutated_during_measurement", "peer not frozen")
    tp_class("ray", lambda b: (b.__setitem__("n_completed", b["N"] - 1),
                               b["flags"].append("timeout")), "timeout", "incomplete batch")

    # ---- (proofs 1/2) repetition-level P3b/C=4 peer-concurrency CAPABILITY soak ----
    def _soak_aw(coord_max, peer_max, arm):
        aw_c = {"coordinate_active_max": coord_max, "coordinate_active_current": 0,
                "ray_peer_active_current": 0, "hpx_action_active_current": 0,
                "active_underflow_seen": False}
        aw_p = {"ray_peer_active_max": (peer_max if arm == "ray" else 0),
                "hpx_action_active_max": (peer_max if arm == "hpx" else 0),
                "coordinate_active_current": 0, "ray_peer_active_current": 0,
                "hpx_action_active_current": 0, "active_underflow_seen": False}
        return aw_c, aw_p, {"membership": 3}, {"membership": 3}

    for _arm, _notproven in (("hpx", "hpx_peer_concurrency_capability_not_proven"),
                             ("ray", "ray_peer_concurrency_capability_not_proven")):
        awc, awp, hc, hp = _soak_aw(4, 2, _arm)
        ev2 = eval_soak_gates(_arm, "P3b", 4, 20, 20, [], awc, awp, hc, hp)
        check(ev2["pass"] and ev2["capability_regime"] and ev2["capability_proven"],
              f"P3b/C=4 {_arm} capability soak with peer max 2 passes and proves capability")
        awc, awp, hc, hp = _soak_aw(4, 1, _arm)
        ev1 = eval_soak_gates(_arm, "P3b", 4, 20, 20, [], awc, awp, hc, hp)
        check((not ev1["pass"]) and ev1["failure_class"] == _notproven
              and ev1["capability_proven"] is False,
              f"P3b/C=4 {_arm} capability soak with peer max 1 fails {_notproven}")
    # non-capability C=2 P3b soak: peer overlap observation-only (peer max 1 still passes) ...
    awc, awp, hc, hp = _soak_aw(2, 1, "hpx")
    evc2 = eval_soak_gates("hpx", "P3b", 2, 20, 20, [], awc, awp, hc, hp)
    check(evc2["pass"] and (not evc2["capability_regime"]) and evc2["capability_proven"] is None,
          "P3b/C=2 soak with peer max 1 passes (peer overlap observation-only, not a capability proof)")
    # ... but coordinator overlap stays mandatory even in the non-capability regime
    awc, awp, hc, hp = _soak_aw(1, 1, "hpx")
    evc0 = eval_soak_gates("hpx", "P3b", 2, 20, 20, [], awc, awp, hc, hp)
    check(evc0["failure_class"] == "coordinator_service_serialized",
          "P3b/C=2 soak with coordinator max 1 still fails coordinator_service_serialized")

    # ---- (proof 3) P0/C=4 measured batch with peer active max 1 passes + emits the annotation ----
    check(peer_overlap_annotations(False) == ["peer_overlap_not_observed_short_service_window"]
          and peer_overlap_annotations(True) == [],
          "(proof 7) short-service annotation emitted only when overlap not observed")
    p0b = _tp_batch("hpx", C=4)
    p0b["case"] = "P0"
    p0b["active_witness"]["hpx_action_active_max"] = 1
    p0g = eval_throughput_batch_gates(p0b)
    check(classify_throughput_batch(p0b, p0g) == "pass",
          "(proof 3) P0/C=4 hpx batch with peer max 1 passes locally (capability linked separately)")

    # ---- (linkage 1-4) cross-island capability binding ----
    def _mk_batch(arm, C):
        b = _tp_batch(arm, C=C)
        b["active_witness"]["coordinate_active_max"] = C
        gg = eval_throughput_batch_gates(b)
        b["gates"] = gg; b["gates_failed"] = [k for k, v in gg.items() if not v]
        b["failure_class"] = classify_throughput_batch(b, gg)
        b["batch_pass"] = b["failure_class"] == "pass"
        b["peer_overlap_observed"] = False
        return b

    def _cap_soak(arm, proven):
        fc = "pass" if proven else (f"{arm}_peer_concurrency_capability_not_proven")
        return {"arm": arm, "C": 4, "case": "P3b", "pass": bool(proven), "failure_class": fc,
                "capability_regime": True, "capability_proven": proven,
                "coordinate_active_max": 4, "peer_overlap_observed": proven,
                "ray_peer_active_max": (2 if (arm == "ray" and proven) else (1 if arm == "ray" else 0)),
                "hpx_action_active_max": (2 if (arm == "hpx" and proven) else (1 if arm == "hpx" else 0))}

    def _obs_soak(arm):
        return {"arm": arm, "C": 2, "case": "P3b", "pass": True, "failure_class": "pass",
                "capability_regime": False, "capability_proven": None,
                "coordinate_active_max": 2, "peer_overlap_observed": False,
                "ray_peer_active_max": (1 if arm == "ray" else 0),
                "hpx_action_active_max": (1 if arm == "hpx" else 0)}

    def _mk_rep(C, rep, batches, soaks):
        ig = {"placement_ok": True, "soaks_passed": all(s["pass"] for s in soaks) if soaks else True,
              "all_batches_passed": bool(batches) and all(b["batch_pass"] for b in batches)}
        return {"C": C, "rep": rep, "batches": batches, "soaks": soaks, "island_gates": ig,
                "island_gates_failed": [k for k, v in ig.items() if not v],
                "rep_pass": all(ig.values()) and bool(batches)
                and all(b["batch_pass"] for b in batches), "failure_class": "pass"}

    def _c2_hpx_fc(reps):
        linked = apply_capability_linkage([dict(r) for r in reps])
        c2 = next(r for r in linked if r["C"] == 2)
        return c2["batches"][0], c2

    # linkage 1: C=2 hpx batch passes when the SAME repetition's C=4 hpx capability proof passed
    b, c2 = _c2_hpx_fc([
        _mk_rep(4, 1, [_mk_batch("hpx", 4)], [_cap_soak("hpx", True), _cap_soak("ray", True)]),
        _mk_rep(2, 1, [_mk_batch("hpx", 2)], [_obs_soak("hpx"), _obs_soak("ray")])])
    check(b["capability_proof_available"] and b["batch_pass"] and c2["rep_pass"]
          and b["capability_proof_source"] == "rep_1_C4_P3b_soak",
          "(linkage 1) C=2 batch passes when same-rep C=4 capability proof passed")
    # linkage 2: C=2 hpx batch fails when the capability proof is MISSING (no C=4 island for that rep)
    b, c2 = _c2_hpx_fc([_mk_rep(2, 2, [_mk_batch("hpx", 2)], [_obs_soak("hpx"), _obs_soak("ray")])])
    check((not b["capability_proof_available"])
          and b["failure_class"] == "peer_concurrency_capability_not_available"
          and (not c2["rep_pass"]),
          "(linkage 2) C=2 batch fails peer_concurrency_capability_not_available when proof missing")
    # linkage 3: C=2 hpx batch fails when the SAME repetition's C=4 hpx capability proof FAILED
    b, c2 = _c2_hpx_fc([
        _mk_rep(4, 3, [], [_cap_soak("hpx", False), _cap_soak("ray", True)]),
        _mk_rep(2, 3, [_mk_batch("hpx", 2)], [_obs_soak("hpx"), _obs_soak("ray")])])
    check(b["failure_class"] == "peer_concurrency_capability_not_available" and (not c2["rep_pass"]),
          "(linkage 3) C=2 batch fails when same-rep C=4 capability proof failed")
    # linkage 4: a capability proof from a DIFFERENT repetition cannot satisfy the batch
    b, c2 = _c2_hpx_fc([
        _mk_rep(4, 1, [_mk_batch("hpx", 4)], [_cap_soak("hpx", True), _cap_soak("ray", True)]),
        _mk_rep(2, 2, [_mk_batch("hpx", 2)], [_obs_soak("hpx"), _obs_soak("ray")])])
    check(b["failure_class"] == "peer_concurrency_capability_not_available",
          "(linkage 4) capability proof from a different repetition cannot satisfy the batch")

    # C=1 must not require overlap (still valid as an instrument, though not an accepted band)
    tb1 = _tp_batch("ray", C=1)
    tb1["outstanding"] = {"mean": 1.0, "max": 1, "fraction_at_C": 1.0, "target_C": 1}
    tb1["active_witness"]["coordinate_active_max"] = 1
    tb1["active_witness"]["ray_peer_active_max"] = 1
    g1 = eval_throughput_batch_gates(tb1)
    check(all(g1.values()), f"C=1 throughput batch needs no overlap: {[k for k, v in g1.items() if not v]}")

    check(set(THROUGHPUT_CASES) == {"P0", "P2", "P3b"} and THROUGHPUT_CONCURRENCY == [2, 4],
          "throughput first matrix is P0/P2/P3b and C in {2,4}")
    tp_rep = {"C": 4, "rep": 1, "boot_rel": "x", "samples_file_rel": "x",
              "island_gates": {"ok": True}, "island_gates_failed": [], "rep_pass": True,
              "failure_class": "pass", "soaks": [], "pregate": {"pass": True, "cases": []},
              "placement": {}, "batches": [_tp_batch("ray"), _tp_batch("hpx")]}
    tagg = build_throughput_aggregate(
        "accepted", {"warmup_n": 100, "N": 1000, "reps": 3, "mode": "accepted"}, [2, 4],
        [c for c in TIMED_MATRIX if c["name"] in THROUGHPUT_CASES], [tp_rep], {"note": "selftest"},
        {"root_node": "m0"}, {"subnet_prefix": "10.42.5."})
    check(tagg["ratio_fences"] == RATIO_FENCES and not any(tagg["ratio_fences"].values()),
          "throughput aggregate ratio fences all false")
    check(tagg["claim_fences"]["qd1_latency_only_no_throughput"] is False
          and tagg["claim_fences"]["not_cross_node"] is False,
          "throughput aggregate flips qd1_latency_only + not_cross_node fences to false")
    tblob = json.dumps(tagg).lower()
    for banned in ("times faster", "\"speedup\":", "relative latency", "\"winner\":", "\"ratio\":"):
        check(banned not in tblob, f"throughput aggregate must not contain '{banned}'")
    tac = tagg["across_reps_per_arm_never_differenced"]
    check("P3b" in tac and set(tac["P3b"]["4"].keys()) == {"ray", "hpx"},
          "throughput across-reps keyed case->C->arm, arms separate (never differenced)")

    # ---- Slice 3 cross-node: band identity, per-band capability, curation atomicity, fences ----
    c4_bands = [b for b in SLICE3_BANDS if b["C"] == 4]
    check(len(c4_bands) == 2 and len({b["band"] for b in c4_bands}) == 2
          and len({(b["C"], b["num_cpus"], b["hpx_threads"]) for b in c4_bands}) == 2,
          "(s3.1) two C=4 bands stay distinct by (C,num_cpus,hpx_threads) despite sharing C")

    aw_c_ok = {"coordinate_active_max": 2, "coordinate_active_current": 0, "ray_peer_active_current": 0,
               "hpx_action_active_current": 0, "active_underflow_seen": False}
    h3 = {"membership": 3}
    aw_p_max1 = {"ray_peer_active_max": 1, "hpx_action_active_max": 1, "coordinate_active_current": 0,
                 "ray_peer_active_current": 0, "hpx_action_active_current": 0, "active_underflow_seen": False}
    aw_p_max2 = {"ray_peer_active_max": 2, "hpx_action_active_max": 2, "coordinate_active_current": 0,
                 "ray_peer_active_current": 0, "hpx_action_active_current": 0, "active_underflow_seen": False}

    def _soak(arm, C, peer_aw):   # concurrency_soak wraps eval_soak_gates with the arm key; mirror that
        n = max(4 * C, 20)
        return {"arm": arm, "C": C, "case": "P3b",
                **eval_soak_gates(arm, "P3b", C, n, n, [], aw_c_ok, peer_aw, h3, h3)}
    # (s3.2) C=2 P3b soak: peer overlap is observation-only; peer_max==1 passes and proves nothing.
    s_c2_ray = _soak("ray", 2, aw_p_max1)
    s_c2_hpx = _soak("hpx", 2, aw_p_max1)
    check(s_c2_ray["pass"] and s_c2_ray["capability_regime"] is False
          and s_c2_ray["capability_proven"] is None,
          "(s3.2) C=2 P3b soak passes with peer_max=1 (observation-only; capability_proven None)")
    # (s3.3) each C=4 band's own soak fails capability-not-proven at peer_max==1, per arm.
    s_c4_ray_bad = _soak("ray", 4, aw_p_max1)
    s_c4_hpx_bad = _soak("hpx", 4, aw_p_max1)
    check((not s_c4_ray_bad["pass"]) and s_c4_ray_bad["capability_proven"] is False
          and s_c4_ray_bad["failure_class"] == "ray_peer_concurrency_capability_not_proven",
          "(s3.3a) C=4 ray soak with peer_max=1 fails capability-not-proven")
    check((not s_c4_hpx_bad["pass"]) and s_c4_hpx_bad["capability_proven"] is False
          and s_c4_hpx_bad["failure_class"] == "hpx_peer_concurrency_capability_not_proven",
          "(s3.3b) C=4 hpx soak with peer_max=1 fails capability-not-proven")
    s_c4_ray_ok = _soak("ray", 4, aw_p_max2)
    s_c4_hpx_ok = _soak("hpx", 4, aw_p_max2)
    check(s_c4_ray_ok["pass"] and s_c4_ray_ok["capability_proven"] is True,
          "(s3.3c) C=4 soak with peer_max>=2 proves capability")
    # (s3.4) capability is derived STRICTLY from a band-rep's own soaks -- no cross-band satisfaction.
    capX = slice3_rep_capability([s_c4_ray_ok, s_c4_hpx_ok])
    capY = slice3_rep_capability([s_c4_ray_bad, s_c4_hpx_bad])
    cap2 = slice3_rep_capability([s_c2_ray, s_c2_hpx])
    check(capX["ray_peer_concurrency_capability_proven"] is True
          and capX["hpx_peer_concurrency_capability_proven"] is True,
          "(s3.4a) proven C=4 band records capability True")
    check(capY["ray_peer_concurrency_capability_proven"] is False
          and capY["hpx_peer_concurrency_capability_proven"] is False,
          "(s3.4b) the OTHER C=4 band records its OWN failed capability (proof does not cross bands)")
    check(cap2["ray_peer_concurrency_capability_proven"] is None
          and cap2["hpx_peer_concurrency_capability_proven"] is None,
          "(s3.4c) C=2 band records capability None (never claimed proven)")

    # (s3.5) accepted curation requires the WHOLE 3-band x R=3 matrix; missing/dup/pooled reps block it.
    def _s3_br(band, rep, ok=True):
        return {"band": band["band"], "band_kind": band["kind"], "band_idx": SLICE3_BANDS.index(band),
                "C": band["C"], "num_cpus": band["num_cpus"], "hpx_threads": band["hpx_threads"],
                "rep": rep, "rep_pass": ok, "island_gates_failed": [] if ok else ["soaks_passed"],
                "rep_capability": slice3_rep_capability([]), "batches": [], "soaks": []}
    full_matrix = [_s3_br(b, r) for b in SLICE3_BANDS for r in (1, 2, 3)]
    check(slice3_matrix_complete(full_matrix, len(SLICE3_BANDS), 3),
          "(s3.5a) complete 3-band x R=3 matrix satisfies curation completeness")
    missing_rep = [br for br in full_matrix
                   if not (br["band"] == SLICE3_BANDS[0]["band"] and br["rep"] == 3)]
    check(not slice3_matrix_complete(missing_rep, len(SLICE3_BANDS), 3),
          "(s3.5b) a missing repetition blocks accepted curation")
    missing_band = [br for br in full_matrix if br["band"] != SLICE3_BANDS[2]["band"]]
    check(not slice3_matrix_complete(missing_band, len(SLICE3_BANDS), 3),
          "(s3.5c) a missing band blocks accepted curation")
    dup = full_matrix + [_s3_br(SLICE3_BANDS[0], 1)]
    check(not slice3_matrix_complete(dup, len(SLICE3_BANDS), 3),
          "(s3.5d) a duplicated/pooled repetition blocks accepted curation")

    # (s3.6/s3.7) slice3 cross-node aggregate: arm separation, per-band capability, peer-local
    # non-subtractable, and all four ratio/difference fences false with no winner/speedup wording.
    def _s3_batch(arm):
        sd = ({"arm": "hpx",
               "coordinator_stage_ns": {s: {"n": 1, "p50": 1} for s in SLICE3_HPX_STAGES},
               "same_worker_fraction": 0.5, "hpx_worker_count_observed": 2,
               "peer_local_ns_NOT_SUBTRACTED": {"peer_local_topk_ns": {"n": 1, "p50": 1},
                                                "peer_action_body_ns": {"n": 1, "p50": 1}}}
              if arm == "hpx"
              else {"arm": "ray",
                    "coordinator_stage_ns": {s: {"n": 1, "p50": 1} for s in SLICE3_RAY_STAGES},
                    "peer_call_ns_note": "caller-observed; NOT pure transport time"})
        return {"arm": arm, "rep": 1, "completion_rate_per_s": 10.0, "n_completed": 1000,
                "verifier": {"verified": 1000, "invalid": 0}, "timeouts": 0,
                "latency_stats": {"p50_ns": 1, "p90_ns": 2, "p99_ns": 3, "iqr_ns": 1, "max_ns": 4},
                "active_witness": {"coordinate_active_max": 4, "ray_peer_active_max": 2,
                                   "hpx_action_active_max": 2},
                "stage_decomp": sd, "batch_pass": True, "failure_class": "pass"}
    s3_br = {"band": SLICE3_BANDS[1]["band"], "band_kind": SLICE3_BANDS[1]["kind"], "band_idx": 1,
             "C": 4, "num_cpus": 2, "hpx_threads": 2, "rep": 1, "rep_pass": True,
             "island_gates_failed": [], "rep_capability": capX,
             "batches": [_s3_batch("ray"), _s3_batch("hpx")], "soaks": []}
    s3agg = build_slice3_aggregate("crossnode_three_node_hard_placement",
                                   {"N": 1000, "warmup_n": 100, "reps": 3, "mode": "accepted"},
                                   [s3_br], {"note": "selftest"})
    check(s3agg["ratio_fences"] == RATIO_FENCES and not any(s3agg["ratio_fences"].values()),
          "(s3.7a) slice3 cross-node aggregate ratio fences all false")
    br0 = s3agg["band_reps"][0]
    check(set(br0["arms"].keys()) == {"ray", "hpx"} and br0.get("capability") == capX,
          "(s3.6a) slice3 aggregate keeps arms separate and records per-band capability")
    hpx_sd = br0["arms"]["hpx"][0]["stage_decomp"]
    check("peer_local_ns_NOT_SUBTRACTED" in hpx_sd and "NEVER subtracted" in s3agg["peer_local_policy"],
          "(s3.6b) slice3 aggregate marks peer-local timings as NEVER subtracted")
    s3blob = json.dumps(s3agg).lower()
    for banned in ("times faster", "\"speedup\":", "relative latency", "\"winner\":", "\"ratio\":"):
        check(banned not in s3blob, f"(s3.7b) slice3 aggregate must not contain '{banned}'")
    # (s3.8) cross-node claim fences: flip not_cross_node + qd1-only, keep ratio/winner fenced.
    cf3 = slice3_crossnode_claim_fences()
    check(cf3["not_cross_node"] is False and cf3["qd1_latency_only_no_throughput"] is False
          and cf3["no_ratio_speedup_winner_claim"] is True and cf3["per_arm_distributions_only"] is True
          and set(cf3) == set(CLAIM_FENCES),
          "(s3.8) slice3 cross-node claim fences: cross-node + throughput-shaped, ratio/winner still fenced")

    if failures:
        print("SELFTEST FAIL:")
        for f in failures:
            print("  -", f)
        return 1
    print(f"SELFTEST OK: exp69 oracle, tie-break, float32 bits, {len(CORRECTNESS_MATRIX)}-case "
          f"pre-gate logic, timed matrix contract, stats, scheduler, witness deltas, "
          f"verification, local + cross-node gate tables + failure taxonomy, "
          f"slice3 band identity + per-band capability + curation atomicity + fences verified")
    return 0


def main():
    ap = argparse.ArgumentParser(
        description="exp69 same-axis top-k QD1 latency (local slices + Rostam cross-node)")
    ap.add_argument("--phase", choices=["smoke", "accepted", "rostam-cross-node",
                                        "rostam-cross-node-throughput", "slice3-local",
                                        "rostam-cross-node-slice3"],
                    default="smoke")
    ap.add_argument("--slice3-mode", choices=["smoke", "accepted"], default="smoke",
                    help="slice3: 'smoke' (P3b x 3 bands, N=100, R=1) or 'accepted' (N=1000, R=3)")
    ap.add_argument("--reps", type=int, default=None, help="override phase/sampling default reps")
    ap.add_argument("--build-dir", default=DEFAULT_BUILD_DIR)
    ap.add_argument("--aggregate", default=None,
                    help="override aggregate path (curated file is only written by a passing "
                         "accepted run with no override)")
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--ray-num-cpus", type=int, default=2)
    ap.add_argument("--selftest", action="store_true")
    # ---- cross-node-only arguments (ignored by the local smoke/accepted phases) ----
    ap.add_argument("--sampling", choices=["smoke", "accepted"], default="accepted",
                    help="cross-node W/K/R selection: 'smoke' (A W=5/K=30/R=1) or 'accepted' "
                         "(A W=50/K=500/R=3 + B control W=20/K=200/R=3)")
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
    ap.add_argument("--node-manager-port", type=int, default=7931)
    ap.add_argument("--object-manager-port", type=int, default=7932)
    ap.add_argument("--min-worker-port", type=int, default=10402)
    ap.add_argument("--max-worker-port", type=int, default=10502)
    # ---- Slice 2 throughput-only arguments (ignored by QD1 and QD1 cross-node phases) ----
    ap.add_argument("--concurrency", default="2,4",
                    help="throughput: comma-separated concurrency bands C (default 2,4)")
    ap.add_argument("--batch-n", type=int, default=None,
                    help="throughput: override measured completions N per batch")
    ap.add_argument("--warmup-n", type=int, default=None,
                    help="throughput: override discarded warm-up completions per batch")
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    if args.phase == "rostam-cross-node":
        return run_crossnode_phase(args)
    if args.phase == "rostam-cross-node-throughput":
        return run_throughput_crossnode_phase(args)
    if args.phase == "slice3-local":
        return run_slice3_local_phase(args)
    if args.phase == "rostam-cross-node-slice3":
        return run_slice3_crossnode_phase(args)
    return run_phase(args)


if __name__ == "__main__":
    sys.exit(main())
