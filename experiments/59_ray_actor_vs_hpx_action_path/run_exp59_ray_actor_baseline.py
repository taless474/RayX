#!/usr/bin/env python3
"""exp59 Ray actor baseline runner -- Slice 1 same-host control + Slice 2 two-node placement proof.

Slice 1 (--phase same-host-control) validates the Ray actor workload shape, the Class-B timing
schema, placement/proof metadata on ONE host, the hardened (overwrite-guarded, atomic) aggregate
writer, and clean-skip behavior when Ray is unavailable. It is the SAME-HOST Ray decomposition control
row described in `ray_actor_vs_hpx_action_path.md` (§2a, §12 Slice 1) -- analogous to the exp58
loopback control -- and carries NO HPX comparison and NO cross-node claim.

Slice 2 (--phase two-node-placement-proof) stands up a REAL two-node Ray cluster under Slurm and
proves HARD actor placement (caller on nodeA, callee on nodeB) with a value-encoded node proof. It is
placement proof ONLY -- it does NOT reuse the Slice 1 QD1/pipeline measurement loop, records
`timing_measured=false`, and makes NO performance claim. See plan §3a for the hardening it satisfies.

  * Slice 2 is placement proof only; NO Class-B timing, NO QD1/pipeline arrays, NO perf claim.
  * NO HPX comparison (that is Slice 5); exp58 aggregates are not read here.
  * NO failure/restart, NO poison detection, NO detector timing.
  * Ray is imported LAZILY inside the phases only -- never at module top.

Phases:  --phase check-config             (Ray availability + versions; no cluster)
         --phase same-host-control        (Slice 1: same-host actor path; prewarm/W/K QD1 + pipeline)
         --phase two-node-placement-proof (Slice 2: two-node Slurm Ray hard-placement proof; no perf)

MEASUREMENT-PLANE HONESTY (see plan §2a): the Ray QD1 floor is Python/Ray-observed (driver- or
pinned-caller-actor-observed), NOT the exp58 runtime-internal C++ HPX action floor. They are
comparable only as path characterizations, and that comparison is a LATER slice.

CLAIM FENCE: Slice 1 is local/same-host Ray actor path control only; Slice 2 is two-node Ray placement
proof only. Neither is compared to HPX yet, neither is a Ray-vs-HPX result, a production/API claim, or
failure/restart. closed-int64 micro-workload only.
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
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
PROBE_XOR = 0x52415958  # "RAYX" -- same oracle spirit as the exp58 spike


# ---------------------------------------------------------------------------------------------------
# Ray actor + caller bodies (PLAIN classes; wrapped with ray.remote() inside the phase after the lazy
# import, so there is NO top-level `import ray`). Methods import ray locally when they need runtime
# context, exactly so this module imports cleanly without Ray installed.
# ---------------------------------------------------------------------------------------------------
def _self_identity():
    nid = aid = None
    try:
        import ray
        ctx = ray.get_runtime_context()
        nid = ctx.get_node_id()
        try:
            aid = ctx.get_actor_id()
        except Exception:  # noqa: BLE001  (driver has no actor id)
            aid = None
    except Exception:  # noqa: BLE001
        pass
    return {"hostname": socket.gethostname(), "pid": os.getpid(), "node_id": nid, "actor_id": aid}


class ProbeActor:
    """Callee: executes the closed-int64 method and returns value + cross-node proof metadata."""
    def __init__(self, node_tag):
        self.node_tag = int(node_tag)

    def dist_probe(self, x):
        x = int(x)
        ident = _self_identity()
        return {
            "result": (x ^ PROBE_XOR) + (self.node_tag << 1),
            "x": x,
            "node_tag": self.node_tag,
            "hostname": ident["hostname"],
            "pid": ident["pid"],
            "node_id": ident["node_id"],
            "actor_id": ident["actor_id"],
        }

    def identity(self):
        return _self_identity()


class CallerActor:
    """Caller: a pinned Python actor that runs the measurement loop against the ProbeActor handle, so
    the QD1 floor is measured at a `pinned_python_caller_actor` plane (mirrors exp58's caller-on-nodeA
    better than driver-as-caller). For same-host Slice 1, caller and callee share a node."""
    def identity(self):
        return _self_identity()

    def run(self, probe, x, k, w, depths, expected_node_tag):
        return _run_measurement(probe, x, k, w, depths, expected_node_tag)

    def call_once(self, probe, x):
        """Slice 2: a single cross-node proof call (caller on nodeA -> callee on nodeB). This is NOT a
        timing loop and records no durations -- it returns the caller identity + the probe result so
        the driver can verify the value-encoded node proof and cross-node placement."""
        import ray
        return {"caller": _self_identity(), "probe_result": ray.get(probe.dist_probe.remote(x))}


# ---------------------------------------------------------------------------------------------------
# Class-B measurement (runs in the driver OR inside CallerActor.run; identical code, different plane)
# ---------------------------------------------------------------------------------------------------
def _pct(sorted_vals, p):
    if not sorted_vals:
        return None
    import math
    k = int(math.ceil(p * len(sorted_vals))) - 1
    return sorted_vals[max(0, min(k, len(sorted_vals) - 1))]


def _stats(raw):
    if not raw:
        return {"count": 0}
    s = sorted(raw)
    return {"count": len(s), "min_ns": s[0], "max_ns": s[-1], "mean_ns": int(sum(s) / len(s)),
            "p50_ns": _pct(s, 0.50), "p90_ns": _pct(s, 0.90), "p99_ns": _pct(s, 0.99)}


def _run_measurement(probe, x, k, w, depths, expected_node_tag):
    """prewarm -> W warmup (dropped) -> K serialized QD1 calls -> pipeline at depths. ns timing via
    time.perf_counter_ns. Returns the Class-B dict (raw arrays preserved)."""
    import ray
    now = time.perf_counter_ns
    oracle = (int(x) ^ PROBE_XOR) + (int(expected_node_tag) << 1)

    # timestamp-call overhead estimate
    N = 4096
    o0 = now()
    for _ in range(N):
        _ = now()
    ts_overhead_ns = (now() - o0) // N

    # prewarm (one-shot)
    t = now()
    r0 = ray.get(probe.dist_probe.remote(x))
    prewarm_ns = now() - t
    callee_proof = {key: r0.get(key) for key in ("hostname", "pid", "node_id", "actor_id", "node_tag")}

    # warmup (dropped); capture first warmup
    first_warm_ns = None
    for i in range(w):
        t = now()
        ray.get(probe.dist_probe.remote(x))
        d = now() - t
        if i == 0:
            first_warm_ns = d

    # K measured serialized QD1 calls
    raw = []
    correct = 0
    first_res = last_res = None
    agg0 = now()
    for i in range(k):
        t = now()
        rr = ray.get(probe.dist_probe.remote(x))
        raw.append(now() - t)
        if rr.get("result") == oracle:
            correct += 1
        if i == 0:
            first_res = rr.get("result")
        last_res = rr.get("result")
    agg_loop_ns = now() - agg0
    agg_mean_ns = (agg_loop_ns // k) if k > 0 else None

    st = _stats(raw)
    per_call_mean = st.get("mean_ns")
    steady_p50 = st.get("p50_ns")

    # pipeline at depths: issue N .remote() then one ray.get(list)
    pipeline = []
    for D in depths:
        p0 = now()
        futs = [probe.dist_probe.remote(x) for _ in range(D)]
        results = ray.get(futs)
        total = now() - p0
        correctp = sum(1 for rr in results if rr.get("result") == oracle)
        fl = bool(results and results[0].get("result") == oracle and results[-1].get("result") == oracle)
        pipeline.append({
            "queue_depth": D, "pipeline_actions": D, "pipeline_total_duration_ns": total,
            "pipeline_actions_per_sec": (D * 1e9 / total) if total > 0 else -1.0,
            "pipeline_amortized_action_time_ns": (total // D) if D > 0 else -1,
            "pipeline_correct_count": correctp, "pipeline_remote_proof_first_last": fl,
            "ray_get_primitive": "ray.get",
            "pipeline_batching_mechanism": "ray_object_refs_plus_ray_get_list",
            "note": ("amortized time = makespan/N, NOT a latency; tail-gated; Ray-side batching is "
                     "ray object-refs + ray.get(list), not HPX parcel coalescing"),
        })

    # warmup sufficiency: are prewarm + first warmup clearly above steady p50?
    suff = "uncertain"
    if steady_p50 and prewarm_ns and first_warm_ns:
        suff = "ok" if (prewarm_ns > steady_p50 and first_warm_ns > steady_p50) else "uncertain"

    return {
        "clock_type": "time.perf_counter_ns",
        "clock_resolution_ns": _perf_counter_resolution_ns(),
        "timestamp_overhead_ns": ts_overhead_ns,
        "oracle": oracle,
        "prewarm_call_duration_ns": prewarm_ns,
        "first_warmup_call_duration_ns": first_warm_ns,
        "steady_state_p50_ns": steady_p50,
        "warmup_sufficiency_note": ("prewarm + W warmups should absorb actor spin-up / import / "
                                    "connection / cloudpickle / object-ref warmup; reported, not forced"),
        "warmup_sufficiency_gate": suff,
        "actor_call_rtt_floor_depth1": {
            "metric_name": "actor_call_rtt_floor_depth1",
            "alias": "ray_actor_closed_int64_call_overhead_floor_qd1",
            "queue_depth": 1, "K": k, "W": w, "steady_count": len(raw), "correct_count": correct,
            "first_result": first_res, "last_result": last_res,
            "aggregate_loop_duration_ns": agg_loop_ns, "aggregate_mean_call_ns": agg_mean_ns,
            "min_ns": st.get("min_ns"), "mean_ns": per_call_mean, "p50_ns": st.get("p50_ns"),
            "p90_ns": st.get("p90_ns"), "p99_ns": st.get("p99_ns"), "max_ns": st.get("max_ns"),
            "aggregate_vs_per_call_mean_diff_ns": (
                (agg_mean_ns - per_call_mean) if (agg_mean_ns is not None and per_call_mean is not None)
                else None),
            "note": ("serialized QD1 Ray actor call-path floor (Python/Ray-observed); NOT general "
                     "per-call cost; NOT comparable to the exp58 HPX C++ floor as the same axis"),
            "per_call_duration_ns_raw": raw,
        },
        "remote_action_pipeline": pipeline,
        "callee_proof": callee_proof,
    }


def _perf_counter_resolution_ns():
    try:
        return int(time.get_clock_info("perf_counter").resolution * 1e9)
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------------------------------
# hardened artifact writer (ported from exp58 safe_write_aggregate: phase-specific PASS names, atomic
# temp+fsync+rename, overwrite guard so skip/fail/local never clobber a curated pass)
# ---------------------------------------------------------------------------------------------------
_PHASE_PASS_BASENAME = {
    "same-host-control": "ray_actor_same_host_aggregate.json",
    "two-node-placement-proof": "ray_actor_two_node_placement_aggregate.json",
    "two-node-measurement-r1": "ray_actor_two_node_measurement_r1_aggregate.json",
    "two-node-measurement-r5": "ray_actor_aggregate.json",
}


def _phase_pass_path(phase):
    return os.path.join(HERE, _PHASE_PASS_BASENAME.get(phase, "ray_actor_other_aggregate.json"))


def _run_id():
    return time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"


def _sibling(path, suffix):
    base, ext = os.path.splitext(path)
    return base + suffix + ext


def _atomic_write_json(path, payload):
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
    else:
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
# environment helpers
# ---------------------------------------------------------------------------------------------------
def _ray_available():
    try:
        import ray  # noqa: F401
        return True
    except Exception:  # noqa: BLE001
        return False


def _ray_version():
    try:
        import ray
        return getattr(ray, "__version__", None)
    except Exception:  # noqa: BLE001
        return None


def _slurm_present():
    return bool(os.environ.get("SLURM_JOB_ID") or os.environ.get("SLURM_JOB_NODELIST"))


def check_config():
    return {
        "phase": "check-config",
        "ray_available": _ray_available(),
        "ray_version": _ray_version(),
        "python_version": sys.version.split()[0],
        "slurm_present": _slurm_present(),
        "same_host_only": True,
        "not_two_node_comparison": True,
        "note": "Slice 1 same-host control only; no cluster, no HPX comparison, no failure/restart",
    }


def _ray_shutdown_quiet(ray):
    try:
        ray.shutdown()
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------------------------------
# one same-host island
# ---------------------------------------------------------------------------------------------------
def run_island_same_host(ray, ProbeCls, CallerCls, args, shared, rep_index):
    node_tag = 1  # callee tag; the caller knows it, so the oracle is caller-known + deterministic
    bootdir = tempfile.mkdtemp(prefix=f"exp59_sh_r{rep_index}_", dir=shared)

    driver_id = _self_identity()  # the driver (this process) after ray.init has a node id
    probe = ProbeCls.remote(node_tag)
    callee_id = ray.get(probe.identity.remote())

    if args.caller == "actor":
        caller = CallerCls.remote()
        caller_id = ray.get(caller.identity.remote())
        classb = ray.get(caller.run.remote(probe, args.x, args.k, args.w, args.pipeline_depths_list,
                                           node_tag))
        measurement_point = "pinned_python_caller_actor"
        measurement_plane = "python_actor_observed"
        driver_observed = False
        ray_driver_to_raylet_path_included = False
    else:  # driver-as-caller
        caller_id = driver_id
        classb = _run_measurement(probe, args.x, args.k, args.w, args.pipeline_depths_list, node_tag)
        measurement_point = "python_driver"
        measurement_plane = "python_driver_observed"
        driver_observed = True
        ray_driver_to_raylet_path_included = True

    same_host = bool(caller_id.get("hostname") and callee_id.get("hostname")
                     and caller_id["hostname"] == callee_id["hostname"])
    # same-host expectation: callee on the same host as the caller (the intended Slice 1 control)
    off_node_sample_count = 0 if same_host else 1

    d1 = classb["actor_call_rtt_floor_depth1"]
    raw = d1["per_call_duration_ns_raw"]
    pipeline_ok = all(r["pipeline_correct_count"] == r["queue_depth"]
                      and r["pipeline_remote_proof_first_last"]
                      for r in classb["remote_action_pipeline"]) if classb["remote_action_pipeline"] else False

    gates = {
        "ray_started": True,
        "actor_created": True,
        "depth1_all_correct": bool(d1["steady_count"] == args.k and d1["correct_count"] == args.k),
        "pipeline_correct_matches_depths": pipeline_ok,
        "first_last_proof": bool(d1["first_result"] == classb["oracle"]
                                 and d1["last_result"] == classb["oracle"]),
        "placement_metadata_present": bool(callee_id.get("hostname") and caller_id.get("hostname")),
        "raw_arrays_preserved": bool(len(raw) == args.k),
        "same_host_placement_verified": same_host,   # Slice 1 control expectation
        "off_node_sample_gate_passed": (off_node_sample_count == 0),
    }
    island_valid = all(gates.values())

    run_rec = {
        "rep_index": rep_index, "phase": "same-host-control", "bootstrap_dir": bootdir,
        # measurement-plane (plan §2a / §3): Ray Python-observed, NOT the exp58 HPX C++ plane
        "measurement_point": measurement_point, "measurement_plane": measurement_plane,
        "python_boundary_included": True, "driver_observed": driver_observed,
        "ray_driver_to_raylet_path_included": ray_driver_to_raylet_path_included,
        "ray_get_included": True,
        "ray_object_store_in_result_path": True, "ray_result_retrieval": "ray.get",
        "ray_result_serialization_path_included": True, "hpx_object_store_in_result_path": False,
        # comparator stance (no HPX comparison performed in this slice)
        "hpx_comparator_kind": "in_substrate_cpp_action_floor",
        "hpx_python_boundary_included": False, "python_boundary_asymmetry_disclosed": True,
        "hpx_comparison_performed": False,
        # caller shape
        "caller_role": ("pinned_caller_actor" if args.caller == "actor" else "driver"),
        "caller_is_driver": (args.caller != "actor"), "caller_is_pinned_actor": (args.caller == "actor"),
        "driver_hostname": driver_id.get("hostname"), "driver_node_id": driver_id.get("node_id"),
        "caller_hostname": caller_id.get("hostname"), "caller_node_id": caller_id.get("node_id"),
        "callee_hostname": callee_id.get("hostname"), "callee_node_id": callee_id.get("node_id"),
        "callee_pid": callee_id.get("pid"), "callee_actor_id": callee_id.get("actor_id"),
        # placement / proof (same-host control; cross-node is Slice 2)
        "placement_strategy": "same_host_default", "placement_soft": None,
        "cross_node_placement_verified": False, "same_host_placement_verified": same_host,
        "off_node_sample_count": off_node_sample_count,
        # pipeline batching disclosure
        "pipeline_batching_mechanism": "ray_object_refs_plus_ray_get_list",
        "pipeline_cross_runtime_ratio_allowed": False,
        "pipeline_ratio_note": ("no cross-runtime ratio in this slice; HPX side not measured here and "
                                "batching mechanisms differ (parcel coalescing vs ray.get(list))"),
        # same-host control identity
        "same_host_ray_control_available": True, "same_host_ray_control_run": True,
        "same_host_ray_control_purpose": ("decompose Ray software call-path overhead from the future "
                                          "inter-node leg, analogous to the exp58 loopback control"),
        "gates": gates, "island_valid": island_valid,
        "island_stats_from_raw": _stats(raw),
        "class_b": classb,
    }
    with open(os.path.join(bootdir, "run_aggregate.json"), "w") as f:
        json.dump(run_rec, f, indent=2)
    with open(os.path.join(shared, "ray_index.jsonl"), "a") as f:
        f.write(json.dumps({
            "phase": "same-host-control", "rep_index": rep_index, "bootstrap_dir": bootdir,
            "island_valid": island_valid, "measurement_point": measurement_point,
            "same_host_placement_verified": same_host, "depth1": _stats(raw),
            "warmup_sufficiency_gate": classb["warmup_sufficiency_gate"], "ts": time.time(),
        }) + "\n")
    return run_rec, raw


# ---------------------------------------------------------------------------------------------------
# phase: same-host-control
# ---------------------------------------------------------------------------------------------------
def _across_island(per_island):
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


def phase_same_host_control(args):
    if not _ray_available():
        return {"phase": "same-host-control", "overall": "skip", "ray_available": False,
                "reason": "Ray not importable in this environment (local skip)",
                "python_version": sys.version.split()[0]}
    import ray
    ray_version = _ray_version()
    try:
        ray.init(ignore_reinit_error=True, log_to_driver=False, include_dashboard=False)
    except Exception as e:  # noqa: BLE001
        return {"phase": "same-host-control", "overall": "fail", "ray_available": True,
                "ray_init_ok": False, "ray_version": ray_version,
                "reason": "ray.init failed: " + str(e)[:160]}

    shared = os.path.join(HERE, "_ray_runs")
    os.makedirs(shared, exist_ok=True)
    ProbeCls = ray.remote(ProbeActor)
    CallerCls = ray.remote(CallerActor)

    islands, pooled_raw = [], []
    try:
        for r in range(args.repetitions):
            rec, raw = run_island_same_host(ray, ProbeCls, CallerCls, args, shared, r)
            islands.append(rec)
            pooled_raw.extend(raw)
    finally:
        _ray_shutdown_quiet(ray)

    all_valid = all(i["island_valid"] for i in islands) and len(islands) > 0
    per_island = [i["island_stats_from_raw"] for i in islands]
    agg = {
        "phase": "same-host-control",
        "baseline_kind": "ray_actor_same_host_control",
        "overall": "pass" if all_valid else "fail",
        "ray_available": True, "ray_init_ok": True, "ray_version": ray_version,
        "ray_supervisor_used": False, "failure_restart_used": False,
        "python_version": sys.version.split()[0],
        "slurm_present": _slurm_present(), "same_host_only": True, "not_two_node_comparison": True,
        "hpx_comparison_performed": False,
        "params": {"K": args.k, "W": args.w, "pipeline_depths": args.pipeline_depths,
                   "repetitions": args.repetitions, "x": args.x, "caller": args.caller},
        "statistics_policy": {"per_island_primary": True, "pooled_stats_allowed": False,
                              "pooled_note": "pooled K*R supplementary only; R may be 1 in Slice 1"},
        "per_island_stats": per_island,
        "across_island_stats": _across_island(per_island),
        "pooled_stats_supplementary": _stats(pooled_raw),
        "fairness_caveats": [
            "Ray QD1 is Python/Ray-observed; the exp58 HPX QD1 is runtime-internal C++ (different planes)",
            "Ray call path includes object-store result retrieval (ray.get); HPX has no object-store analog",
            "pipeline batching differs: ray object-refs + ray.get(list) vs HPX parcel coalescing",
        ],
        "claim_fences": [
            "local/same-host Ray actor path control only",
            "NOT two-node; NOT compared to HPX; NOT a Ray-vs-HPX result",
            "NOT a production/API claim; NOT failure/restart",
            "closed-int64 micro-workload only; Rostam/local-allocation-specific",
        ],
        "shared_dir": shared,
        "islands": [{k: v for k, v in i.items() if k != "class_b"} for i in islands],
    }
    return agg


# ===================================================================================================
# Slice 2: two-node Ray cluster placement proof (PLACEMENT PROOF ONLY -- no Class-B timing, no perf).
# Does NOT reuse _run_measurement. Records timing_measured=false. See plan §3a / §12 Slice 2.
# ===================================================================================================
def _oracle(x, node_tag):
    """Closed-int64 value-encoded oracle (same spirit as exp58 (x ^ RAYX) + (loc<<1)). The node_tag is
    assigned PER TARGET NODE in Slice 2, so the returned value itself proves which node executed."""
    return (int(x) ^ PROBE_XOR) + (int(node_tag) << 1)


def _run(cmd, timeout=180, env=None):
    """Run a command; return (rc, stdout, stderr). Never raises on nonzero / timeout / exec error."""
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=timeout, env=env, text=True)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:  # noqa: PERF203
        # On timeout the partial buffers may be bytes even under text=True; decode defensively
        # so a worker-join timeout records its stderr tail instead of crashing on bytes+str.
        def _as_text(b):
            if isinstance(b, (bytes, bytearray)):
                return b.decode("utf-8", "replace")
            return b or ""
        return 124, _as_text(e.stdout), _as_text(e.stderr) + f"\n[timeout after {timeout}s]"
    except Exception as e:  # noqa: BLE001
        return 1, "", f"[exec error] {e}"


def _expand_nodelist_pure(s):
    """Expand a Slurm-style nodelist string into host names (pure-Python; no scontrol).
    'medusa[00-01]' -> ['medusa00','medusa01']; 'medusa[00-01,03]' -> [...,'medusa03'];
    'medusa00,medusa01' -> ['medusa00','medusa01']; 'a[1-2],b3' -> ['a1','a2','b3'].
    Zero-padding follows the width of each range's low bound."""
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
        m = re.match(r"^([^\[]*)\[([^\]]+)\](.*)$", part)
        if not m:
            out.append(part); continue
        prefix, body, suffix = m.group(1), m.group(2), m.group(3)
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
    """Prefer `scontrol show hostnames` (authoritative) when available; else the pure expander."""
    if nodelist:
        rc, out, _ = _run(["scontrol", "show", "hostnames", nodelist], timeout=20)
        if rc == 0 and out.strip():
            return [h for h in out.split() if h.strip()]
    return _expand_nodelist_pure(nodelist or "")


def _pick_subnet_ip(ips, subnet_prefix):
    """First IP that startswith subnet_prefix, else None (pure; unit-testable)."""
    for ip in ips:
        if ip.startswith(subnet_prefix):
            return ip
    return None


def _node_ips(node):
    """Best-effort IPv4 list for a node via `srun --nodelist=node hostname -I`."""
    rc, out, _ = _run(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "hostname", "-I"],
                      timeout=60)
    if rc != 0:
        return []
    return [tok for tok in out.split() if tok.count(".") == 3]


def _resolve_node_ip(node, subnet_prefix):
    return _pick_subnet_ip(_node_ips(node), subnet_prefix)


def _tail_file(path, n):
    """Last n chars of a log file (decoded defensively); '' if absent/unreadable."""
    try:
        with open(path, "rb") as f:
            return f.read().decode("utf-8", "replace")[-n:]
    except Exception:  # noqa: BLE001
        return ""


# --- Persistent Ray launchers (LAUNCHER-LIFETIME FIX) ----------------------------------------------
# On Slurm, `ray start` (without --block) daemonizes GCS/raylet and returns; Slurm then reaps the srun
# step cgroup, killing the daemons, so the worker can never reach GCS (observed exp59 Slice 2: head
# logged "Ray runtime started" then 6379 was actively refused). The fix is `ray start ... --block`
# launched via subprocess.Popen so the srun step stays alive (hosting the daemons) for the cluster
# lifetime while the driver continues. Logs are redirected to files (never PIPE) so the long-lived
# step never deadlocks on an unread pipe, and remain diagnosable in the per-run artifact dir.
def _head_srun_cmd(node, ip, port, temp_dir, num_cpus, port_flags=()):
    cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
           "ray", "start", "--head", "--node-ip-address", ip, "--port", str(port),
           "--include-dashboard", "false", "--temp-dir", temp_dir]
    cmd += list(port_flags)
    cmd += ["--block"]
    if num_cpus is not None:
        cmd += ["--num-cpus", str(num_cpus)]
    return cmd


def _worker_srun_cmd(node, ip, head_ip, port, num_cpus, port_flags=()):
    cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
           "ray", "start", "--address", f"{head_ip}:{port}", "--node-ip-address", ip]
    cmd += list(port_flags)
    cmd += ["--block"]
    if num_cpus is not None:
        cmd += ["--num-cpus", str(num_cpus)]
    return cmd


def _ray_port_flags(args):
    """Pin Ray SECONDARY transport ports on BOTH head and worker so cross-node actor traffic uses a
    known, bounded port set (mirrors exp57/58 firewall-aware launching). Returns (flags, chosen, notes).

    Only flags that are stable across Ray 2.x (verified-present in 2.55.1's `ray start`) are pinned:
    --node-manager-port, --object-manager-port, --min-worker-port, --max-worker-port. The
    dashboard-agent / runtime-env-agent / metrics-export ports are intentionally NOT pinned here: their
    `ray start` flag names have shifted across Ray versions, and passing an unrecognized flag would make
    `ray start --block` exit immediately. If a later run shows an agent binding to a blocked ephemeral
    port, add those flags explicitly after confirming the exact 2.55.1 names. This choice is recorded
    in the artifact (`ray_secondary_port_notes`) -- it is reported, not silently guessed."""
    if not args.pin_ray_ports:
        return [], {}, ["secondary-port pinning disabled via --no-pin-ray-ports"]
    chosen = {
        "node_manager_port": args.node_manager_port,
        "object_manager_port": args.object_manager_port,
        "min_worker_port": args.min_worker_port,
        "max_worker_port": args.max_worker_port,
    }
    flags = ["--node-manager-port", str(args.node_manager_port),
             "--object-manager-port", str(args.object_manager_port),
             "--min-worker-port", str(args.min_worker_port),
             "--max-worker-port", str(args.max_worker_port)]
    notes = [
        "pinned stable Ray 2.x transport flags: node-manager/object-manager/worker-range",
        "NOT pinned (flag-name stability varies by Ray version; would risk an unrecognized-argument "
        "exit on 2.55.1): dashboard-agent-grpc-port, dashboard-agent-listen-port, "
        "runtime-env-agent-port, metrics-export-port -- add explicitly only after confirming names",
    ]
    return flags, chosen, notes


def _popen_srun_blocking(cmd, env, log_path):
    """Launch a PERSISTENT (`--block`) srun step without waiting; merge stdout+stderr to log_path.
    Returns the Popen handle (carries the open log file so cleanup can close it)."""
    lf = open(log_path, "ab", buffering=0)
    p = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                         env=env, start_new_session=True)
    p._exp59_logfile = lf  # noqa: SLF001 (keep handle to close at cleanup)
    p._exp59_logpath = log_path  # noqa: SLF001
    return p


def _ray_head_start_blocking(node, ip, port, temp_dir, num_cpus, env, log_path, port_flags=()):
    return _popen_srun_blocking(_head_srun_cmd(node, ip, port, temp_dir, num_cpus, port_flags),
                                env, log_path)


def _ray_worker_start_blocking(node, ip, head_ip, port, num_cpus, env, log_path, port_flags=()):
    return _popen_srun_blocking(_worker_srun_cmd(node, ip, head_ip, port, num_cpus, port_flags),
                                env, log_path)


def _wait_gcs_ready(probe_node, head_ip, port, env, timeout_s):
    """Readiness gate BEFORE launching the worker: poll head GCS reachability FROM probe_node via a
    single srun step (real cross-node TCP connect, mirroring the worker's own path). One job step
    retries internally until connected or the window expires. Returns (ok, elapsed_s, detail)."""
    probe = (
        "import socket,sys,time\n"
        "deadline=time.time()+%d\n"
        "last=''\n"
        "while time.time()<deadline:\n"
        "    s=socket.socket(); s.settimeout(2)\n"
        "    try:\n"
        "        s.connect((%r,%d)); print('READY'); sys.exit(0)\n"
        "    except Exception as e:\n"
        "        last=repr(e)\n"
        "        try: s.close()\n"
        "        except Exception: pass\n"
        "        time.sleep(1.5)\n"
        "print('TIMEOUT',last); sys.exit(1)\n" % (int(timeout_s), head_ip, int(port))
    )
    t0 = time.monotonic()
    rc, out, err = _run(["srun", "-N1", "-n1", "--overlap", "--nodelist", probe_node, "--export=ALL",
                         "python3", "-c", probe], timeout=int(timeout_s) + 30, env=env)
    detail = ((out or "") + (err or "")).strip()[-300:]
    return (rc == 0), round(time.monotonic() - t0, 3), detail


def _wait_ray_nodes(ray, expected, timeout_s):
    """Readiness gate AFTER launching the worker: proceed only once >=expected Ray nodes are Alive.
    Returns (ok, alive_seen, elapsed_s)."""
    t0 = time.monotonic()
    seen = 0
    while time.monotonic() - t0 < timeout_s:
        try:
            seen = len([n for n in ray.nodes() if n.get("Alive")])
        except Exception:  # noqa: BLE001
            seen = 0
        if seen >= expected:
            return True, seen, round(time.monotonic() - t0, 3)
        time.sleep(1.0)
    return False, seen, round(time.monotonic() - t0, 3)


def _terminate_launcher(p):
    """Stop a persistent Popen-backed srun launcher. After `ray stop --force` the `--block`ed step
    should already be exiting; this terminates/kills any straggler and closes its log handle."""
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
                try:
                    p.wait(timeout=10)
                except Exception:  # noqa: BLE001
                    pass
        info["returncode"] = p.poll()
        info["terminated"] = True
    except Exception as e:  # noqa: BLE001
        info["error"] = str(e)[:200]
    finally:
        lf = getattr(p, "_exp59_logfile", None)
        if lf is not None:
            try:
                lf.close()
            except Exception:  # noqa: BLE001
                pass
    return info


def _ray_stop_node(node, env):
    return _run(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                "ray", "stop", "--force"], timeout=120, env=env)


_ORPHAN_PATTERNS = ["raylet", "gcs_server", "plasma_store", "ray::", "dashboard", "monitor"]


def _orphan_check_node(node, patterns, env):
    """Patterns still matching a live process on `node` after `ray stop` (mirrors exp57/58)."""
    found = []
    for pat in patterns:
        rc, out, _ = _run(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "--export=ALL",
                          "pgrep", "-f", pat], timeout=60, env=env)
        if rc == 0 and out.strip():
            found.append(pat)
    return found


def _skip(reason, **extra):
    rec = {"phase": "two-node-placement-proof", "overall": "skip", "local_skip_no_write": True,
           "reason": reason, "python_version": sys.version.split()[0]}
    rec.update(extra)
    return rec


# --- Driver-side reachability / readiness (the nodeB cross-node gate cannot stand in for the DRIVER
#     host's own ability to reach GCS; these run IN-PROCESS on whatever host the driver runs on) ------
def _local_ipv4s():
    """Best-effort IPv4 addresses of the DRIVER host (where ray.init runs)."""
    rc, out, _ = _run(["hostname", "-I"], timeout=10)
    ips = [t for t in out.split() if t.count(".") == 3] if rc == 0 else []
    if not ips:
        try:
            ips = [socket.gethostbyname(socket.gethostname())]
        except Exception:  # noqa: BLE001
            ips = []
    return ips


def _route_to(dest_ip):
    """First line of `ip route get <dest_ip>` from the driver host ('' if unavailable)."""
    rc, out, _ = _run(["ip", "route", "get", str(dest_ip)], timeout=10)
    if rc == 0 and out.strip():
        return out.strip().splitlines()[0].strip()
    return ""


def _driver_gcs_ready(head_ip, port, timeout_s):
    """Driver-side GCS readiness: retry an IN-PROCESS TCP connect FROM THE DRIVER HOST to head GCS
    until success or timeout. Returns (ok, elapsed_s, detail)."""
    t0 = time.monotonic()
    last = ""
    while time.monotonic() - t0 < timeout_s:
        s = socket.socket()
        s.settimeout(2)
        try:
            s.connect((head_ip, int(port)))
            s.close()
            return True, round(time.monotonic() - t0, 3), "connected"
        except Exception as e:  # noqa: BLE001
            last = repr(e)
            try:
                s.close()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1.0)
    return False, round(time.monotonic() - t0, 3), (last or "timeout")


def _bounded_ray_init(ray, address, timeout_s, per_attempt_gcs_timeout_s):
    """Bounded, retry-aware connect to an EXISTING cluster. TCP port-open can precede GCS actually
    serving, so retry over a deadline instead of trusting a single attempt -- and never hang to the
    Slurm wall time. Returns (ok, attempts, elapsed_s, full_traceback_tail)."""
    # Shorten each GCS request so a not-yet-serving GCS fails an attempt fast (respects a user override).
    os.environ.setdefault("RAY_gcs_server_request_timeout_seconds", str(int(per_attempt_gcs_timeout_s)))
    t0 = time.monotonic()
    attempts, last_tb = 0, ""
    while True:
        attempts += 1
        try:
            ray.init(address=address, log_to_driver=False, include_dashboard=False,
                     ignore_reinit_error=True)
            return True, attempts, round(time.monotonic() - t0, 3), ""
        except Exception:  # noqa: BLE001
            last_tb = traceback.format_exc()
            if time.monotonic() - t0 >= timeout_s:
                return False, attempts, round(time.monotonic() - t0, 3), last_tb[-2000:]
            time.sleep(2.0)


def _get_bounded(ray, ref, timeout_s):
    """ray.get with a HARD timeout so placement/proof can't hang on a stalled NodeAffinity placement or
    blocked secondary ports. Returns (ok, value, err)."""
    try:
        return True, ray.get(ref, timeout=timeout_s), ""
    except Exception as e:  # noqa: BLE001
        return False, None, f"{type(e).__name__}: {str(e)[:200]}"


def _short(host):
    """Short hostname (strip the DNS domain) so FQDN actor hostnames (e.g. medusa01.rostam.cct.lsu.edu,
    what socket.gethostname() returns on these nodes) compare equal to the short Slurm node names
    (medusa01). Locality is asserted PRIMARILY by Ray node_id; hostname is a secondary, normalized check."""
    return (host or "").split(".")[0]


def _inner_island(ray, cfg, nodeA, nodeB, nodeA_nid, nodeB_nid, gt, do_measure, idx,
                  cleanup_actors=False):
    """One island: create FRESH caller@nodeA / callee@nodeB actors (hard NodeAffinity), run the strict
    placement proof, and -- only if placement passes and `do_measure` -- the QD1 Class-B band measured
    INSIDE the pinned CallerActor (caller-observed plane). Returns a per-island dict.

    Cluster-level facts (Ray node-id resolution, proof_driver_on_nodeA) are decided by the CALLER, not
    here; `island_placement_ok` therefore EXCLUDES proof_driver_on_nodeA. Slice 2/3 (R=1) merge this
    record flat into `out` and re-combine with proof_driver_on_nodeA; Slice 4 (R=5) keep it per island.
    With `cleanup_actors` the island's actors are ray.kill()ed before returning so the next island
    places fresh (Slice 4 R=5 uses this; Slice 2/3 R=1 keep the prior, no-kill actor lifetime)."""
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    caller_tag, callee_tag = 1, 2
    isl = {
        "island_index": idx,
        "node_tag_assignment": {nodeA: caller_tag, nodeB: callee_tag},
        "caller_node_tag": caller_tag, "callee_node_tag": callee_tag,
        "expected_callee_node_tag": callee_tag, "value_encoded_node_proof": True,
        "island_placement_ok": False, "island_measured": False,
    }
    caller = callee = None
    try:
        ProbeCls, CallerCls = ray.remote(ProbeActor), ray.remote(CallerActor)
        callee = ProbeCls.options(
            num_cpus=cfg["probe_num_cpus"],
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=nodeB_nid, soft=False),
        ).remote(callee_tag)
        caller = CallerCls.options(
            num_cpus=cfg["caller_num_cpus"],
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=nodeA_nid, soft=False),
        ).remote()
        ok_c, caller_id, err_c = _get_bounded(ray, caller.identity.remote(), gt)
        ok_e, callee_id, err_e = _get_bounded(ray, callee.identity.remote(), gt)
        if not (ok_c and ok_e):
            isl["actor_identity_get_error"] = {"caller": err_c, "callee": err_e}
            isl["island_reason"] = (f"actor identity get failed after {gt}s "
                                    f"(caller_ok={ok_c} callee_ok={ok_e}); likely stalled NodeAffinity "
                                    f"placement or blocked Ray secondary ports")
            return isl
        isl.update({
            "placement_strategy": "node_affinity_hard", "placement_soft": False,
            "caller_target_node": nodeA, "callee_target_node": nodeB,
            "caller_resolved_node_id": caller_id.get("node_id"),
            "callee_resolved_node_id": callee_id.get("node_id"),
            "caller_hostname": caller_id.get("hostname"), "callee_hostname": callee_id.get("hostname"),
            "caller_pid": caller_id.get("pid"), "callee_pid": callee_id.get("pid"),
            "caller_actor_id": caller_id.get("actor_id"), "callee_actor_id": callee_id.get("actor_id"),
        })
        oracle = _oracle(cfg["x"], callee_tag)
        isl["oracle"] = oracle
        n_proof = max(1, int(cfg["proof_calls"]))
        results, off_node, proof_err = [], 0, None
        for _ in range(n_proof):
            ok_p, pr, err_p = _get_bounded(ray, caller.call_once.remote(callee, cfg["x"]), gt)
            if not ok_p:
                proof_err = err_p
                break
            results.append(pr)
            pres = pr.get("probe_result", {})
            # locality: node_id is authoritative; hostname compared short-name (FQDN-normalized)
            if pres.get("node_id") != nodeB_nid or _short(pres.get("hostname")) != nodeB:
                off_node += 1
        if proof_err is not None:
            isl["proof_get_error"] = proof_err
            isl["island_reason"] = f"proof call failed after {gt}s: {proof_err}"
            return isl
        first = results[0]["probe_result"] if results else {}
        last = results[-1]["probe_result"] if results else {}
        all_correct = bool(results and all(
            r["probe_result"].get("result") == oracle and r["probe_result"].get("node_tag") == callee_tag
            for r in results))
        isl.update({
            "proof_calls": n_proof, "proof_call_correct": all_correct,
            "first_last_proof": bool(results and first.get("result") == oracle
                                     and last.get("result") == oracle),
            "off_node_sample_count": off_node, "off_node_sample_gate_passed": (off_node == 0),
            "intended_callee_node_matched": bool(callee_id.get("node_id") == nodeB_nid
                                                 and _short(callee_id.get("hostname")) == nodeB),
            "intended_caller_node_matched": bool(caller_id.get("node_id") == nodeA_nid
                                                 and _short(caller_id.get("hostname")) == nodeA),
            "cross_node_placement_verified": bool(
                _short(caller_id.get("hostname")) == nodeA and _short(callee_id.get("hostname")) == nodeB
                and caller_id.get("node_id") != callee_id.get("node_id")
                and callee_id.get("node_id") == nodeB_nid),
            "locality_assert": "node_id authoritative; hostname compared short-name (FQDN-normalized)",
        })
        island_placement_ok = bool(all_correct and off_node == 0)
        isl["island_placement_ok"] = island_placement_ok
        if not island_placement_ok:
            isl["island_reason"] = f"proof gate not all true (correct={all_correct} off_node={off_node})"
            return isl

        # QD1 Class-B band, ONLY after this island's placement passes (reuses the Slice-1 measurement).
        if do_measure:
            mk, mw = int(cfg.get("measure_k", 1000)), int(cfg.get("measure_w", 100))
            depths = list(cfg.get("measure_depths") or [])
            mt = int(cfg.get("measure_timeout", 180))
            ok_m, classb, err_m = _get_bounded(
                ray, caller.run.remote(callee, cfg["x"], mk, mw, depths, callee_tag), mt)
            if not ok_m:
                isl["measurement_get_error"] = err_m
                isl["island_reason"] = f"measurement get failed after {mt}s: {err_m}"
                return isl
            isl.update({
                "class_b": classb, "measurement_ran": True, "island_measured": True,
                "measure_k": mk, "measure_w": mw, "measure_depths": depths,
                "measurement_point": "pinned_caller_actor_nodeA_to_callee_actor_nodeB",
                "measurement_plane": ("ray_python_ray_actor_observed_path "
                                      "(NOT hpx_cpp_runtime_internal)"),
            })
        return isl
    finally:
        if cleanup_actors:
            for h in (caller, callee):
                if h is not None:
                    try:
                        ray.kill(h)
                    except Exception:  # noqa: BLE001
                        pass


def _across_island_qd1(islands):
    """Across-island summary of the per-island QD1 floors: the MEDIAN of p50/p90/p99/mean/min/max plus
    the across-island min/max SPREAD. Per-island stats are PRIMARY; calls are NOT pooled into one
    distribution. Carries the pre-registered decision rule so a single median cannot be over-read."""
    floors = [i["class_b"]["actor_call_rtt_floor_depth1"]
              for i in islands if i.get("island_measured") and isinstance(i.get("class_b"), dict)]

    def med(vals):
        v = sorted(x for x in vals if x is not None)
        if not v:
            return None
        n = len(v)
        return v[n // 2] if n % 2 else int((v[n // 2 - 1] + v[n // 2]) / 2)

    out = {
        "islands_in_stats": len(floors),
        "per_island_primary": True, "pooled_distribution_used": False,
        "decision_rule": ("a gap inside the across-island jitter band is NOT a separable effect; the "
                          "across-island min/max spread bounds what the per-island median can claim"),
    }
    for key in ("p50_ns", "p90_ns", "p99_ns", "mean_ns", "min_ns", "max_ns"):
        clean = [f.get(key) for f in floors if f.get(key) is not None]
        out[key + "_median"] = med(clean)
        out[key + "_min"] = (min(clean) if clean else None)
        out[key + "_max"] = (max(clean) if clean else None)
    return out


def _inner_two_node_proof(args):
    """INNER proof driver -- MUST run ON nodeA (a Ray CLUSTER node), launched by the outer orchestrator
    via `srun --nodelist nodeA`. A Ray driver using ray.init(address=...) resolves a LOCAL raylet to
    attach to, so it must be co-located with one (the head raylet on nodeA); the rostam1 orchestrator
    is not a cluster node and cannot attach (observed: get_node_to_connect_for_driver -> 'No node info').

    This step connects to the EXISTING cluster, resolves node ids, hard-places caller@nodeA /
    callee@nodeB, runs the value/oracle proof with bounded gets, and writes a structured JSON result to
    `--inner-output`. It NEVER starts/stops Ray and NEVER calls srun -- the outer orchestrator owns Ray
    process lifetime and cleanup. It only ray.shutdown()s ITS OWN driver connection on exit."""
    with open(args.inner_input) as f:
        cfg = json.load(f)
    out = {
        "inner_phase": "two-node-inner-proof",
        "proof_driver_hostname": socket.gethostname(), "proof_driver_pid": os.getpid(),
        "inner_overall": "fail", "inner_reason": None,
        # claim fences mirrored into the inner artifact
        "timing_measured": False, "class_b_timing_present": False,
        "perf_claim_allowed": False, "hpx_comparison_performed": False,
    }
    nodeA, nodeB, subnet = cfg["nodeA"], cfg["nodeB"], cfg["subnet"]
    nodeA_ip, nodeB_ip = cfg["nodeA_ip"], cfg["nodeB_ip"]
    address = f"{cfg['head_ip']}:{int(cfg['port'])}"
    gt = int(cfg["ray_get_timeout"])
    ray = None
    try:
        import ray as _ray_mod  # actor placement strategy is imported inside _inner_island
        ray = _ray_mod

        init_ok, init_attempts, init_wait_s, init_tb = _bounded_ray_init(
            ray, address, cfg["ray_init_timeout"], cfg["ray_gcs_request_timeout"])
        out.update({"ray_address": address, "ray_init_ok": init_ok, "ray_init_attempts": init_attempts,
                    "ray_init_wait_s": init_wait_s, "ray_init_timeout_s": cfg["ray_init_timeout"],
                    "ray_gcs_request_timeout_s": cfg["ray_gcs_request_timeout"]})
        if not init_ok:
            out["ray_init_traceback"] = init_tb
            out["inner_reason"] = f"ray.init failed after {init_wait_s}s / {init_attempts} attempts"
            return out
        ident = _self_identity()
        out["proof_driver_node_id"] = ident.get("node_id")
        out["proof_driver_ray_hostname"] = ident.get("hostname")

        nodes_ready, seen, wait = _wait_ray_nodes(ray, 2, cfg["ray_ready_timeout"])
        out.update({"ray_nodes_ready": nodes_ready, "ray_nodes_ready_seen": seen,
                    "ray_nodes_ready_wait_s": wait})
        if not nodes_ready:
            out["inner_reason"] = f"only {seen}/2 ray nodes alive after {wait}s"
            return out

        nodes_raw = ray.nodes()
        alive = [n for n in nodes_raw if n.get("Alive")]
        out["ray_cluster_nodes_seen"] = len(alive)
        out["ray_cluster_resources"] = ray.cluster_resources()
        out["ray_nodes_raw"] = [{k: n.get(k) for k in
                                 ("NodeID", "NodeManagerAddress", "NodeName", "Alive", "Resources")}
                                for n in nodes_raw]
        out["ray_node_resolution_method"] = ("match alive ray node NodeManagerAddress / NodeName to "
                                             "the slurm node IP/hostname on the selected subnet")

        def _match(ip, host):
            return [n for n in alive
                    if n.get("NodeManagerAddress") == ip or n.get("NodeName") in (host, ip)]
        mA, mB = _match(nodeA_ip, nodeA), _match(nodeB_ip, nodeB)
        nodeA_nid = mA[0].get("NodeID") if len(mA) == 1 else None
        nodeB_nid = mB[0].get("NodeID") if len(mB) == 1 else None
        nodeA_nip = mA[0].get("NodeManagerAddress") if len(mA) == 1 else None
        nodeB_nip = mB[0].get("NodeManagerAddress") if len(mB) == 1 else None
        nodes_subnet_ok = bool(nodeA_nip and nodeB_nip
                               and nodeA_nip.startswith(subnet) and nodeB_nip.startswith(subnet))
        resolution_ok = bool(len(mA) == 1 and len(mB) == 1 and nodeA_nid and nodeB_nid
                             and nodeA_nid != nodeB_nid and nodes_subnet_ok)
        proof_on_nodeA = bool(ident.get("node_id") and ident.get("node_id") == nodeA_nid)
        out.update({
            "nodeA_ray_node_match_count": len(mA), "nodeB_ray_node_match_count": len(mB),
            "nodeA_ray_node_id": nodeA_nid, "nodeB_ray_node_id": nodeB_nid,
            "nodeA_ray_node_ip": nodeA_nip, "nodeB_ray_node_ip": nodeB_nip,
            "ray_nodes_on_selected_subnet": nodes_subnet_ok,
            "ray_node_id_resolution_ok": resolution_ok,
            "proof_driver_is_ray_cluster_node": proof_on_nodeA,
        })
        if not resolution_ok:
            out["inner_reason"] = (f"node-id resolution failed (mA={len(mA)} mB={len(mB)} "
                                   f"subnet_ok={nodes_subnet_ok})")
            return out

        out["nodeA_cpus"] = (mA[0].get("Resources") or {}).get("CPU")
        out["nodeB_cpus"] = (mB[0].get("Resources") or {}).get("CPU")
        out["ray_resources_by_node"] = {nodeA: mA[0].get("Resources"), nodeB: mB[0].get("Resources")}
        out["caller_actor_num_cpus"] = cfg["caller_num_cpus"]
        out["probe_actor_num_cpus"] = cfg["probe_num_cpus"]
        if (out["nodeA_cpus"] or 0) < cfg["caller_num_cpus"] or (out["nodeB_cpus"] or 0) < cfg["probe_num_cpus"]:
            out["inner_reason"] = f"insufficient CPU (nodeA={out['nodeA_cpus']} nodeB={out['nodeB_cpus']})"
            return out

        out["ray_get_timeout_s"] = gt
        do_measure = bool(cfg.get("measure"))
        r_count = max(1, int(cfg.get("r_count", 1)))
        out["r_count"] = r_count

        if r_count <= 1:
            # --- Slice 2/3 SINGLE-ISLAND path: one fresh caller@nodeA / callee@nodeB, strict placement
            #     proof, then (Slice 3) the R=1 QD1 Class-B band. The per-island record is merged FLAT
            #     into `out` so the Slice 2/3 aggregate schema is byte-for-byte unchanged. -------------
            isl = _inner_island(ray, cfg, nodeA, nodeB, nodeA_nid, nodeB_nid, gt, do_measure, 0)
            out.update({k: v for k, v in isl.items()
                        if k not in ("island_index", "island_placement_ok", "island_measured",
                                     "island_reason")})
            placement_ok = bool(isl.get("island_placement_ok") and proof_on_nodeA)
            out["placement_ok"] = placement_ok
            if not placement_ok:
                out["inner_reason"] = (isl.get("island_reason")
                                       or f"proof gate not all true / proof_on_nodeA={proof_on_nodeA}")
                return out
            if do_measure and not isl.get("island_measured"):
                out["inner_reason"] = isl.get("island_reason") or "measurement did not complete"
                return out
            out["inner_overall"] = "pass"
        else:
            # --- Slice 4 R=5 MULTI-ISLAND path on ONE cluster: per island, FRESH caller/callee actors,
            #     rerun the SAME strict placement gates, then a QD1 Class-B band. Per-island PRIMARY;
            #     calls are NOT pooled. An island that fails placement records NO timing for itself and
            #     the run is marked failed/partial honestly (overall pass requires every island valid).
            islands = []
            for i in range(r_count):
                isl = _inner_island(ray, cfg, nodeA, nodeB, nodeA_nid, nodeB_nid, gt, do_measure, i,
                                    cleanup_actors=True)
                isl["proof_driver_on_nodeA"] = proof_on_nodeA
                isl["island_valid"] = bool(isl.get("island_placement_ok") and proof_on_nodeA
                                           and (not do_measure or isl.get("island_measured")))
                islands.append(isl)
            placed = sum(1 for i in islands if i.get("island_placement_ok"))
            measured = sum(1 for i in islands if i.get("island_measured"))
            valid = sum(1 for i in islands if i.get("island_valid"))
            out.update({
                "islands": islands, "islands_placed": placed, "islands_measured": measured,
                "islands_valid": valid, "all_islands_valid": bool(valid == r_count and r_count > 0),
                "across_island_stats": _across_island_qd1(islands),
                "measurement_point": "pinned_caller_actor_nodeA_to_callee_actor_nodeB",
                "measurement_plane": ("ray_python_ray_actor_observed_path "
                                      "(NOT hpx_cpp_runtime_internal)"),
                "per_island_primary": True, "pooled_distribution_used": False,
            })
            if out["all_islands_valid"]:
                out["inner_overall"] = "pass"
            else:
                out["inner_reason"] = (f"only {valid}/{r_count} islands valid "
                                       f"(placed={placed} measured={measured})")
    except Exception as e:  # noqa: BLE001
        out["inner_reason"] = f"inner exception: {type(e).__name__}: {str(e)[:200]}"
        out["inner_traceback"] = traceback.format_exc()[-2000:]
    finally:
        if ray is not None:
            _ray_shutdown_quiet(ray)  # disconnect THIS driver only; cluster stays up (outer owns it)
        with open(args.inner_output, "w") as f:
            json.dump(out, f, indent=2)
    return out


def _two_node_orchestrate(args, measure=False, r_count=1):
    """Two-node Ray orchestrator. `measure=False` is the Slice 2 PLACEMENT PROOF (no Class-B timing).
    `measure=True, r_count=1` is the Slice 3 R=1 MEASUREMENT (placement gates, then ONE QD1 Class-B band
    at the pinned caller@nodeA -> callee@nodeB plane). `measure=True, r_count>1` is the Slice 4 R=5
    REPLICATED MEASUREMENT: ONE cluster, and per island FRESH caller/callee actors re-run the SAME strict
    placement gates before timing; per-island stats are PRIMARY and calls are NOT pooled. All three share
    the orchestrator-on-rostam1 + inner-driver-on-nodeA split and the same cleanup. NO HPX comparison, NO
    Ray-vs-HPX/perf claim, NO failure/restart in any mode."""
    multi = r_count > 1
    phase = ("two-node-measurement-r5" if multi else
             ("two-node-measurement-r1" if measure else "two-node-placement-proof"))
    baseline_kind = ("ray_actor_two_node_measurement_r5" if multi else
                     ("ray_actor_two_node_measurement_r1" if measure
                      else "ray_actor_two_node_placement_proof"))
    # Hard preconditions -> clean skip (stdout only, never writes/clobbers a top-level aggregate).
    if not _ray_available():
        return _skip("Ray not importable in this environment (local skip)", ray_available=False,
                     phase=phase)
    if not _slurm_present():
        return _skip("requires an active two-node Slurm allocation (local skip)",
                     ray_available=True, slurm_present=False, phase=phase)
    nodelist = os.environ.get("SLURM_JOB_NODELIST") or os.environ.get("SLURM_NODELIST") or ""
    nodes = sorted(_expand_slurm_nodelist(nodelist))
    if len(nodes) < 2:
        return _skip(f"need >=2 nodes; got {nodes}", ray_available=True, slurm_present=True,
                     slurm_nodelist=nodelist, phase=phase)

    subnet, nodeA, nodeB = args.prefer_subnet, nodes[0], nodes[1]
    runid = _run_id()
    matches = (nodeA == "medusa00" and nodeB == "medusa01")
    # measurement honesty flags: R=1 carries not_r5; R=5 carries per_island_primary/pooled fences.
    if multi:
        _meas_flags = {"r_count": r_count, "per_island_primary": True,
                       "pooled_distribution_used": False, "not_hpx_comparison": True,
                       "measurement_plane_asymmetry": True}
    elif measure:
        _meas_flags = {"r_count": 1, "not_r5": True, "not_hpx_comparison": True,
                       "measurement_plane_asymmetry": True}
    else:
        _meas_flags = {}
    rec = {
        "phase": phase,
        "baseline_kind": baseline_kind,
        "ray_available": True, "slurm_present": True,
        "ray_supervisor_used": False, "failure_restart_used": False,
        "python_version": sys.version.split()[0], "ray_version": _ray_version(),
        # timing fences: placement phase is timing-free; measurement phase flips timing_measured/
        # class_b_timing_present to True ONLY after placement passes and a Class-B band is recorded.
        "timing_measured": False, "class_b_timing_present": False, "perf_claim_allowed": False,
        "hpx_comparison_performed": False, "not_two_node_comparison": False,
        # measurement honesty flags (constant for the measurement phase)
        "measurement_phase": measure,
        **_meas_flags,
        # deterministic node selection (plan §3a.3)
        "node_pair_selection_rule": "expand SLURM nodelist; sort; nodeA=first, nodeB=second",
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"), "slurm_nodelist": nodelist,
        "nodeA": nodeA, "nodeB": nodeB,
        "expected_exp58_nodeA": "medusa00", "expected_exp58_nodeB": "medusa01",
        "matches_exp58_node_pair": matches, "node_pair_parity_with_exp58": matches,
        # subnet / interface parity (plan §3a.4)
        "selected_subnet": subnet,
        "expected_interface": ("eno16" if subnet == "10.42.5." else None),
        "run_id": runid,
    }

    nodeA_ip = _resolve_node_ip(nodeA, subnet)
    nodeB_ip = _resolve_node_ip(nodeB, subnet)
    ips_on_subnet = bool(nodeA_ip and nodeB_ip
                         and nodeA_ip.startswith(subnet) and nodeB_ip.startswith(subnet))
    rec.update({"nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip,
                "ray_node_ips_on_selected_subnet": ips_on_subnet,
                "interface_parity_with_exp58": bool(ips_on_subnet and subnet == "10.42.5.")})
    if not matches:
        rec["comparison_parity_note"] = ("allocation differs from medusa00/medusa01; no direct exp58 "
                                         "comparison without same-allocation HPX recapture")
    if not ips_on_subnet:
        rec["overall"] = "fail"
        rec["reason"] = f"could not resolve nodeA/nodeB IPs on subnet {subnet}: A={nodeA_ip} B={nodeB_ip}"
        rec["gates"] = {"subnet_ip_resolved": False}
        return _finish_two_node(rec, runid)

    # --- Ray cluster launch + driver attach + proof, ALL under ONE try/finally so the persistent
    #     --block srun launchers are ALWAYS torn down even if import/ray.init/proof raises (B1). ------
    # head (nodeA) and worker (nodeB) each run `ray start ... --block` under a long-lived Popen-backed
    # srun step so Slurm does not reap GCS/raylet. Two readiness gates protect the driver: a cross-node
    # nodeB->nodeA gate before worker launch, then a DRIVER-side nodeA gate + bounded retry-aware
    # ray.init (TCP port-open can precede GCS actually serving).
    temp_dir = f"/tmp/exp59_ray_{runid}"
    port = args.ray_port
    env = dict(os.environ)
    rundir = os.path.join(HERE, "_ray_runs", runid)
    os.makedirs(rundir, exist_ok=True)
    head_log = os.path.join(rundir, "head_launch.log")
    worker_log = os.path.join(rundir, "worker_launch.log")
    ready_timeout = args.ray_ready_timeout
    port_flags, chosen_ports, port_notes = _ray_port_flags(args)
    rec.update({"ray_secondary_ports_pinned": bool(port_flags),
                "ray_secondary_ports": chosen_ports, "ray_secondary_port_notes": port_notes})

    head_proc = worker_proc = None
    fail_reason = None
    startup_ok = False
    gcs_ready, gcs_wait_s, gcs_detail = False, None, ""
    worker_launched = False
    ray_address = f"{nodeA_ip}:{port}"
    try:
        head_proc = _ray_head_start_blocking(nodeA, nodeA_ip, port, temp_dir,
                                             args.head_num_cpus, env, head_log, port_flags)
        # readiness gate BEFORE worker launch: head GCS reachable from nodeB (cross-node)
        gcs_ready, gcs_wait_s, gcs_detail = _wait_gcs_ready(nodeB, nodeA_ip, port, env, ready_timeout)
        if gcs_ready and head_proc.poll() is None:
            worker_proc = _ray_worker_start_blocking(nodeB, nodeB_ip, nodeA_ip, port,
                                                     args.worker_num_cpus, env, worker_log, port_flags)
            worker_launched = (worker_proc.poll() is None)

        head_alive = bool(head_proc and head_proc.poll() is None)
        startup_ok = bool(gcs_ready and head_alive and worker_launched)
        rec.update({
            "ray_head_node": nodeA, "ray_worker_node": nodeB,
            "ray_head_ip": nodeA_ip, "ray_worker_ip": nodeB_ip, "ray_address": ray_address,
            "ray_block_mode": True,
            "ray_head_launch_pid": (head_proc.pid if head_proc else None),
            "ray_worker_launch_pid": (worker_proc.pid if worker_proc else None),
            "ray_head_alive_after_ready": head_alive,
            "gcs_ready": gcs_ready, "gcs_ready_wait_s": gcs_wait_s,
            "gcs_ready_probe_from": nodeB, "gcs_ready_timeout_s": ready_timeout,
            "gcs_ready_detail": gcs_detail,
            "ray_worker_launched": worker_launched,
            "ray_startup_ok": startup_ok,
            "ray_head_stderr_tail": _tail_file(head_log, 500),
            "ray_worker_stderr_tail": _tail_file(worker_log, 500),
            "ray_temp_dir": temp_dir, "ray_port": port,
        })
        if not startup_ok:
            raise RuntimeError(f"ray cluster startup failed (gcs_ready={gcs_ready} "
                               f"head_alive={head_alive} worker_launched={worker_launched})")

        # --- OUTER driver diagnostics (informational): the orchestrator on rostam1 does NOT attach to
        #     Ray. A Ray driver must run ON a cluster node (resolves a LOCAL raylet); rostam1 is not one.
        #     We still record outer-host reachability for provenance, but it is NOT the attach path. ----
        outer_host = socket.gethostname()
        outer_ips = _local_ipv4s()
        outer_route = _route_to(nodeA_ip)
        od_ready, od_wait_s, od_detail = _driver_gcs_ready(nodeA_ip, port, ready_timeout)
        rec.update({
            "outer_driver_hostname": outer_host,
            "outer_driver_ip_candidates": outer_ips,
            "outer_driver_ip_on_selected_subnet": any(ip.startswith(subnet) for ip in outer_ips),
            "outer_driver_route_to_head": outer_route,
            "outer_driver_gcs_probe": od_detail, "outer_driver_gcs_ready": od_ready,
            "outer_driver_gcs_ready_wait_s": od_wait_s,
            "outer_driver_is_on_slurm_node": outer_host in nodes,
            "outer_driver_not_in_caller_plane": True,
            "head_ip": nodeA_ip, "head_port": port,
            "caller_plane_defined_by": "pinned_caller_actor",
        })

        # --- INNER proof on nodeA: a Ray driver must be a CLUSTER node, so run THIS file in inner mode
        #     via `srun --nodelist nodeA` (a SIBLING srun from the orchestrator; the inner step never
        #     starts/stops Ray nor calls srun). JSON is handed off via files on the shared /work FS. ----
        inner_in = os.path.join(rundir, "inner_input.json")
        inner_out = os.path.join(rundir, "inner_output.json")
        inner_log = os.path.join(rundir, "inner_proof.log")
        with open(inner_in, "w") as f:
            json.dump({
                "head_ip": nodeA_ip, "port": port, "nodeA": nodeA, "nodeB": nodeB,
                "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip, "subnet": subnet, "x": args.x,
                "proof_calls": args.proof_calls,
                "caller_num_cpus": args.caller_num_cpus, "probe_num_cpus": args.probe_num_cpus,
                "ray_init_timeout": args.ray_init_timeout,
                "ray_gcs_request_timeout": args.ray_gcs_request_timeout,
                "ray_get_timeout": args.ray_get_timeout, "ray_ready_timeout": ready_timeout,
                # Slice 3/4: the inner runs the QD1 Class-B band ONLY when measure=True (and after gates);
                # r_count>1 runs R placement-gated islands on the one cluster (Slice 4 R=5).
                "measure": bool(measure), "r_count": r_count, "multi_island": multi,
                "measure_k": args.measure_k, "measure_w": args.measure_w,
                "measure_depths": args.measure_depths_list, "measure_timeout": args.measure_timeout,
            }, f, indent=2)
        inner_cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", nodeA, "--export=ALL",
                     sys.executable, os.path.abspath(__file__),
                     "--phase", "_two-node-inner-proof",
                     "--inner-input", inner_in, "--inner-output", inner_out]
        # R islands each pay (proof calls + one measurement get); scale the bound by r_count so a
        # multi-island R=5 run does not trip the inner srun timeout. R=1 keeps the prior budget.
        _per_island = ((max(1, args.proof_calls) + 2) * args.ray_get_timeout
                       + (args.measure_timeout if measure else 0))
        inner_timeout = args.ray_init_timeout + ready_timeout + r_count * _per_island + 60
        inner_rc, _io, _ie = _run(inner_cmd, timeout=inner_timeout, env=env)
        with open(inner_log, "w") as f:
            f.write((_io or "") + "\n--- stderr ---\n" + (_ie or ""))
        rec.update({
            "inner_proof_invoked": True, "inner_proof_srun_node": nodeA, "inner_proof_rc": inner_rc,
            "inner_input_path": os.path.relpath(inner_in, HERE),
            "inner_output_path": os.path.relpath(inner_out, HERE),
            "inner_proof_log_tail": _tail_file(inner_log, 600),
        })
        inner = None
        if os.path.exists(inner_out):
            try:
                with open(inner_out) as f:
                    inner = json.load(f)
            except Exception as e:  # noqa: BLE001
                rec["inner_output_parse_error"] = str(e)[:200]
        if inner is None:
            raise RuntimeError(f"inner proof produced no parseable output (srun rc={inner_rc}); "
                               f"see inner_proof_log_tail")
        # merge the inner proof's structured result (placement/proof/resolution/proof_driver_*) into the
        # aggregate; the outer orchestrator records it but did NOT itself touch Ray.
        rec.update(inner)
        pdh = (rec.get("proof_driver_hostname") or "").split(".")[0]
        rec["proof_driver_on_nodeA"] = bool(pdh == nodeA and rec.get("proof_driver_is_ray_cluster_node"))
        # Flip the timing fences to True ONLY now -- placement passed AND a Class-B band was recorded.
        # perf_claim_allowed / hpx_comparison_performed stay False (path band, not a verdict). For R=5,
        # timing is present once at least one island recorded a valid band (overall PASS still requires
        # EVERY island valid; an honest partial keeps timing_measured=True with overall=fail).
        if multi:
            if (inner.get("islands_measured") or 0) > 0:
                rec.update({
                    "timing_measured": True, "class_b_timing_present": True,
                    "measurement_point": inner.get("measurement_point"),
                    "measurement_plane": inner.get("measurement_plane"),
                })
        elif measure and inner.get("measurement_ran") and inner.get("class_b"):
            rec.update({
                "timing_measured": True, "class_b_timing_present": True,
                "measurement_point": inner.get("measurement_point"),
                "measurement_plane": inner.get("measurement_plane"),
            })
        if inner.get("inner_overall") != "pass":
            raise RuntimeError(f"inner proof failed: {inner.get('inner_reason')}")
    except Exception as e:  # noqa: BLE001
        if fail_reason is None:
            fail_reason = str(e)[:300]
    finally:
        # GUARANTEED teardown (covers inner-proof srun failures too, not just the success path): the
        # orchestrator owns Ray process lifetime -- ray stop --force on both nodes (releases the
        # `--block`ed steps), then terminate the Popen-backed launchers, then the no-orphan check.
        sh_rc, _, _ = _ray_stop_node(nodeA, env)
        sw_rc, _, _ = _ray_stop_node(nodeB, env)
        head_cleanup = _terminate_launcher(head_proc)
        worker_cleanup = _terminate_launcher(worker_proc)
        orphA = _orphan_check_node(nodeA, _ORPHAN_PATTERNS, env)
        orphB = _orphan_check_node(nodeB, _ORPHAN_PATTERNS, env)
        rec.update({
            "ray_stop_head_ok": (sh_rc == 0), "ray_stop_worker_ok": (sw_rc == 0),
            "ray_head_launcher_cleanup": head_cleanup, "ray_worker_launcher_cleanup": worker_cleanup,
            "orphan_check_patterns": _ORPHAN_PATTERNS,
            "orphan_ray_processes_nodeA": orphA, "orphan_ray_processes_nodeB": orphB,
            "no_orphan_ray_processes_nodeA": (len(orphA) == 0),
            "no_orphan_ray_processes_nodeB": (len(orphB) == 0),
        })
    if fail_reason:
        rec["reason"] = fail_reason

    # --- gates (plan §3a.8) ----------------------------------------------------------------------
    g = rec.get  # shorthand
    if multi:
        # Slice 4 R=5: cluster-level gates + EVERY-ISLAND gates (each island independently passes the
        # full placement proof AND records a valid QD1 band). Per-island placement fields live inside
        # rec["islands"], so the per-island checks scan that list (not flat rec fields).
        islands = rec.get("islands") or []

        def _all_isl(pred):
            return bool(islands) and all(pred(i) for i in islands)

        def _floor(i):
            cb = i.get("class_b") if isinstance(i.get("class_b"), dict) else {}
            return cb.get("actor_call_rtt_floor_depth1", {}) if isinstance(cb, dict) else {}

        gates = {
            "slurm_two_node_allocation": len(nodes) >= 2,
            "ray_startup_ok": bool(startup_ok),
            "gcs_ready": bool(g("gcs_ready")),
            "inner_proof_ran": bool(g("inner_overall")),
            "proof_driver_on_nodeA": bool(g("proof_driver_on_nodeA")),
            "ray_init_ok": bool(g("ray_init_ok")),
            "ray_nodes_visible": bool(g("ray_nodes_ready")),
            "nodeA_single_match": g("nodeA_ray_node_match_count") == 1,
            "nodeB_single_match": g("nodeB_ray_node_match_count") == 1,
            "ray_nodes_on_selected_subnet": bool(g("ray_nodes_on_selected_subnet")),
            "ray_stop_ok": bool(g("ray_stop_head_ok") and g("ray_stop_worker_ok")),
            "no_orphans": bool(g("no_orphan_ray_processes_nodeA") and g("no_orphan_ray_processes_nodeB")),
            # every-island placement + timing gates (genuine replication; exact R=5 is informational,
            # see is_full_r5 -- so a smaller exploratory --measure-islands run is not forced to fail)
            "r_count_ge_2": (g("r_count") or 0) >= 2,
            "all_islands_placed": g("islands_placed") == g("r_count"),
            "all_islands_measured": g("islands_measured") == g("r_count"),
            "all_islands_valid": bool(g("all_islands_valid")),
            "every_island_soft_false": _all_isl(lambda i: i.get("placement_soft") is False),
            "every_island_caller_on_nodeA": _all_isl(
                lambda i: bool(g("nodeA_ray_node_id"))
                and i.get("caller_resolved_node_id") == g("nodeA_ray_node_id")),
            "every_island_callee_on_nodeB": _all_isl(
                lambda i: bool(g("nodeB_ray_node_id"))
                and i.get("callee_resolved_node_id") == g("nodeB_ray_node_id")),
            "every_island_value_proof": _all_isl(lambda i: bool(i.get("proof_call_correct"))),
            "every_island_off_node_zero": _all_isl(lambda i: i.get("off_node_sample_count") == 0),
            "every_island_qd1_correct": _all_isl(
                lambda i: bool(_floor(i).get("K")
                               and _floor(i).get("correct_count") == _floor(i).get("K"))),
            "timing_measured_true": g("timing_measured") is True,
        }
        rec["is_full_r5"] = (g("r_count") == 5)
        rec["gates"] = gates
        rec["overall"] = "pass" if (all(gates.values()) and fail_reason is None) else "fail"
        rec["fairness_caveats"] = [
            "R=5 per-island QD1 path bands at the pinned caller-actor (nodeA) -> callee-actor (nodeB) "
            "plane; per-island stats are PRIMARY and calls are NOT pooled into one distribution",
            "across-island summary is the MEDIAN of per-island p50/p90/p99 + the min/max spread; a gap "
            "inside the across-island jitter band is NOT a separable effect",
            "Ray Python/Ray-actor-observed path -- NOT the exp58 HPX C++ runtime-internal action floor; "
            "the two are not on the same measurement axis (measurement_plane_asymmetry)",
            "no HPX side measured here; exp58 aggregates not read; no Ray-vs-HPX comparison",
        ]
        rec["claim_fences"] = [
            "two-node Ray actor R=5 replicated QD1 path MEASUREMENT (each island placement-gated)",
            "NO performance verdict; NO HPX comparison; NO Ray-vs-HPX result",
            "NO production/API claim; NO failure/restart claim",
            "closed-int64 micro-workload; Rostam-allocation-specific; parity gated to medusa00/medusa01",
        ]
        return _finish_two_node(rec, runid, phase)

    gates = {
        "slurm_two_node_allocation": len(nodes) >= 2,
        "ray_startup_ok": bool(startup_ok),
        "gcs_ready": bool(g("gcs_ready")),
        "inner_proof_ran": bool(g("inner_overall")),
        "proof_driver_on_nodeA": bool(g("proof_driver_on_nodeA")),
        "ray_init_ok": bool(g("ray_init_ok")),
        "ray_nodes_visible": bool(g("ray_nodes_ready")),
        "nodeA_single_match": g("nodeA_ray_node_match_count") == 1,
        "nodeB_single_match": g("nodeB_ray_node_match_count") == 1,
        "ray_nodes_on_selected_subnet": bool(g("ray_nodes_on_selected_subnet")),
        "hard_node_affinity_soft_false": g("placement_soft") is False,
        "caller_on_nodeA": bool(g("nodeA_ray_node_id")
                                and g("caller_resolved_node_id") == g("nodeA_ray_node_id")),
        "callee_on_nodeB": bool(g("nodeB_ray_node_id")
                                and g("callee_resolved_node_id") == g("nodeB_ray_node_id")),
        "value_encoded_node_proof_matches": bool(g("proof_call_correct")),
        "hostnames_differ": bool(g("caller_hostname") and g("callee_hostname")
                                 and g("caller_hostname") != g("callee_hostname")),
        "node_ids_differ": bool(g("caller_resolved_node_id") and g("callee_resolved_node_id")
                                and g("caller_resolved_node_id") != g("callee_resolved_node_id")),
        "off_node_sample_zero": g("off_node_sample_count") == 0,
        "ray_stop_ok": bool(g("ray_stop_head_ok") and g("ray_stop_worker_ok")),
        "no_orphans": bool(g("no_orphan_ray_processes_nodeA") and g("no_orphan_ray_processes_nodeB")),
    }
    if measure:
        # measurement phase: placement gates above must ALL hold, then the Class-B band must be present
        # and every measured QD1 call correct. (timing_free is intentionally NOT a gate here.)
        cb = g("class_b") if isinstance(g("class_b"), dict) else {}
        floor = cb.get("actor_call_rtt_floor_depth1", {}) if isinstance(cb, dict) else {}
        gates.update({
            "measurement_ran": bool(g("measurement_ran")),
            "class_b_present": bool(cb),
            "timing_measured_true": g("timing_measured") is True,
            "qd1_all_correct": bool(floor.get("K") and floor.get("correct_count") == floor.get("K")),
            "r_count_is_1": g("r_count") == 1,
        })
    else:
        gates["timing_free"] = g("timing_measured") is False
    rec["gates"] = gates
    rec["overall"] = "pass" if (all(gates.values()) and fail_reason is None) else "fail"
    if measure:
        rec["fairness_caveats"] = [
            "R=1 single-allocation QD1 path band at the pinned caller-actor (nodeA) -> callee-actor "
            "(nodeB) plane; NOT a verdict and NOT averaged over islands (R=1, not R=5)",
            "Ray Python/Ray-actor-observed path -- NOT the exp58 HPX C++ runtime-internal action floor; "
            "the two are not on the same measurement axis (measurement_plane_asymmetry)",
            "no HPX side measured here; exp58 aggregates not read; no Ray-vs-HPX comparison",
        ]
        rec["claim_fences"] = [
            "two-node Ray actor R=1 QD1 path MEASUREMENT (after the Slice 2 placement gates pass)",
            "NO performance verdict; NO HPX comparison; NO Ray-vs-HPX result; NO R=5",
            "NO production/API claim; NO failure/restart claim",
            "closed-int64 micro-workload; Rostam-allocation-specific; parity gated to medusa00/medusa01",
        ]
    else:
        rec["fairness_caveats"] = [
            "placement proof only; NO timing measured; NOT a performance or latency number",
            "Ray node placement proven via NodeAffinity hard affinity + value-encoded node proof",
            "no HPX side measured here; exp58 aggregates not read; no Ray-vs-HPX comparison",
        ]
        rec["claim_fences"] = [
            "two-node Ray actor PLACEMENT proof only",
            "NO performance/timing claim; NO HPX comparison; NO Ray-vs-HPX result",
            "NO production/API claim; NO failure/restart claim",
            "closed-int64 micro-workload; Rostam-allocation-specific; parity gated to medusa00/medusa01",
        ]
    return _finish_two_node(rec, runid, phase)


def phase_two_node_placement_proof(args):
    """Slice 2: two-node Ray HARD caller@nodeA / callee@nodeB placement proof (no Class-B timing)."""
    return _two_node_orchestrate(args, measure=False)


def phase_two_node_measurement_r1(args):
    """Slice 3: R=1 two-node Ray actor QD1 Class-B measurement, gated behind the Slice 2 placement
    proof. Same orchestrator/inner split; no HPX comparison, no R=5, no Ray-vs-HPX/perf claim."""
    return _two_node_orchestrate(args, measure=True)


def phase_two_node_measurement_r5(args):
    """Slice 4: R=5 REPLICATED two-node Ray actor QD1 measurement on ONE cluster. Each island re-runs the
    full Slice 2 placement gates with FRESH caller/callee actors before timing; per-island stats are
    PRIMARY and calls are NOT pooled. Same orchestrator/inner split; no HPX comparison, no Ray-vs-HPX/perf
    claim, no failure/restart. `--measure-islands` sets R (>=2; default 5)."""
    return _two_node_orchestrate(args, measure=True, r_count=max(2, int(args.measure_islands)))


def _finish_two_node(rec, runid, phase="two-node-placement-proof"):
    """Write the per-run record + index line for a two-node phase (raw, ignored under _ray_runs)."""
    shared = os.path.join(HERE, "_ray_runs")
    os.makedirs(shared, exist_ok=True)
    rundir = os.path.join(shared, runid)
    os.makedirs(rundir, exist_ok=True)
    with open(os.path.join(rundir, "run_aggregate.json"), "w") as f:
        json.dump(rec, f, indent=2)
    with open(os.path.join(shared, "ray_index.jsonl"), "a") as f:
        f.write(json.dumps({
            "phase": phase, "run_id": runid, "overall": rec.get("overall"),
            "nodeA": rec.get("nodeA"), "nodeB": rec.get("nodeB"),
            "matches_exp58_node_pair": rec.get("matches_exp58_node_pair"),
            "cross_node_placement_verified": rec.get("cross_node_placement_verified"),
            "off_node_sample_count": rec.get("off_node_sample_count"),
            "timing_measured": rec.get("timing_measured"),
            "measurement_ran": rec.get("measurement_ran"),
            "r_count": rec.get("r_count"), "islands_valid": rec.get("islands_valid"),
            "ts": time.time(),
        }) + "\n")
    return rec


# ---------------------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------------------
_SUMMARY_KEYS = {
    "same-host-control": ("phase", "baseline_kind", "overall", "reason", "ray_version",
                          "same_host_only", "not_two_node_comparison", "top_level_aggregate_path",
                          "redirected_from_path", "overwrite_refused"),
    "two-node-placement-proof": ("phase", "baseline_kind", "overall", "reason", "ray_version",
                                 "nodeA", "nodeB", "matches_exp58_node_pair",
                                 "ray_startup_ok", "gcs_ready", "proof_driver_hostname",
                                 "proof_driver_on_nodeA", "ray_init_ok", "ray_nodes_ready",
                                 "cross_node_placement_verified", "off_node_sample_count",
                                 "timing_measured", "top_level_aggregate_path",
                                 "redirected_from_path", "overwrite_refused"),
    "two-node-measurement-r1": ("phase", "baseline_kind", "overall", "reason", "ray_version",
                                "nodeA", "nodeB", "matches_exp58_node_pair",
                                "proof_driver_on_nodeA", "cross_node_placement_verified",
                                "off_node_sample_count", "measurement_ran", "measurement_point",
                                "measurement_plane", "timing_measured", "class_b_timing_present",
                                "r_count", "not_r5", "not_hpx_comparison",
                                "measurement_plane_asymmetry", "perf_claim_allowed",
                                "hpx_comparison_performed", "top_level_aggregate_path",
                                "redirected_from_path", "overwrite_refused"),
    "two-node-measurement-r5": ("phase", "baseline_kind", "overall", "reason", "ray_version",
                                "nodeA", "nodeB", "matches_exp58_node_pair",
                                "proof_driver_on_nodeA", "r_count", "is_full_r5", "islands_placed",
                                "islands_measured", "islands_valid", "all_islands_valid",
                                "across_island_stats", "measurement_point", "measurement_plane",
                                "timing_measured", "class_b_timing_present", "per_island_primary",
                                "pooled_distribution_used", "not_hpx_comparison",
                                "measurement_plane_asymmetry", "perf_claim_allowed",
                                "hpx_comparison_performed", "top_level_aggregate_path",
                                "redirected_from_path", "overwrite_refused"),
}


def main():
    ap = argparse.ArgumentParser(description="exp59 Ray actor baseline (Slice 1 same-host / "
                                             "Slice 2 two-node placement proof)")
    ap.add_argument("--phase", choices=["check-config", "same-host-control",
                                        "two-node-placement-proof", "two-node-measurement-r1",
                                        "two-node-measurement-r5", "_two-node-inner-proof"],
                    default="same-host-control",
                    help="(_two-node-inner-proof is INTERNAL: the on-nodeA proof/measure driver launched "
                         "by the orchestrator via srun; not for direct use)")
    ap.add_argument("--caller", choices=["actor", "driver"], default="actor",
                    help="pinned caller actor (default, mirrors exp58) or driver-as-caller [Slice 1]")
    ap.add_argument("--k", type=int, default=1000, help="measured QD1 serialized calls [Slice 1]")
    ap.add_argument("--w", type=int, default=100, help="warmup calls dropped from stats [Slice 1]")
    ap.add_argument("--pipeline-depths", default="8,32,128", help="[Slice 1]")
    ap.add_argument("--repetitions", type=int, default=1, help="islands (Slice 1 default 1)")
    ap.add_argument("--x", type=int, default=7, help="closed-int64 input")
    ap.add_argument("--allow-overwrite-pass", action="store_true",
                    help="permit overwriting an existing same-phase pass aggregate")
    ap.add_argument("--out", default=None, help="explicit PASS aggregate path (overwrite-guarded)")
    # Slice 2 (two-node placement proof) options
    ap.add_argument("--prefer-subnet", default="10.42.5.",
                    help="subnet prefix for nodeA/nodeB IP resolution [Slice 2]")
    ap.add_argument("--ray-port", type=int, default=6379, help="Ray head GCS port [Slice 2]")
    ap.add_argument("--ray-ready-timeout", type=int, default=90,
                    help="bounded seconds for each readiness gate: nodeB->head GCS, driver->head GCS, "
                         "and both Ray nodes visible [Slice 2]")
    ap.add_argument("--ray-init-timeout", type=int, default=60,
                    help="bounded seconds for retry-aware ray.init to an existing cluster (TCP "
                         "port-open may precede GCS serving) [Slice 2]")
    ap.add_argument("--ray-gcs-request-timeout", type=int, default=5,
                    help="per-attempt GCS request timeout (RAY_gcs_server_request_timeout_seconds) so "
                         "a not-yet-serving GCS fails an attempt fast [Slice 2]")
    ap.add_argument("--ray-get-timeout", type=int, default=30,
                    help="hard ray.get timeout for placement/proof so a stalled NodeAffinity placement "
                         "or blocked secondary port fails cleanly instead of hanging [Slice 2]")
    ap.add_argument("--proof-calls", type=int, default=3,
                    help="tiny cross-node proof calls (NOT a timing loop) [Slice 2]")
    ap.add_argument("--caller-num-cpus", type=int, default=1, help="pinned caller actor cpus [Slice 2]")
    ap.add_argument("--probe-num-cpus", type=int, default=1, help="pinned probe actor cpus [Slice 2]")
    ap.add_argument("--head-num-cpus", type=int, default=None, help="ray head --num-cpus [Slice 2]")
    ap.add_argument("--worker-num-cpus", type=int, default=None, help="ray worker --num-cpus [Slice 2]")
    # Slice 2 secondary-port pinning (stable Ray 2.x transport flags only; see _ray_port_flags)
    ap.add_argument("--pin-ray-ports", action=argparse.BooleanOptionalAction, default=True,
                    help="pin Ray node-manager/object-manager/worker-range ports on head+worker "
                         "(--no-pin-ray-ports to disable) [Slice 2]")
    ap.add_argument("--node-manager-port", type=int, default=6380, help="pinned raylet port [Slice 2]")
    ap.add_argument("--object-manager-port", type=int, default=6381,
                    help="pinned object-manager port [Slice 2]")
    ap.add_argument("--min-worker-port", type=int, default=10002,
                    help="pinned worker port range low [Slice 2]")
    ap.add_argument("--max-worker-port", type=int, default=10999,
                    help="pinned worker port range high [Slice 2]")
    # Slice 3 (two-node R=1 measurement) options
    ap.add_argument("--measure-k", type=int, default=1000,
                    help="measured QD1 serialized calls (caller@nodeA -> callee@nodeB) [Slice 3]")
    ap.add_argument("--measure-w", type=int, default=100,
                    help="warmup calls dropped from stats [Slice 3]")
    ap.add_argument("--measure-depths", default="8,32,128",
                    help="pipeline sanity depths (comma-sep; empty to skip pipeline) [Slice 3]")
    ap.add_argument("--measure-timeout", type=int, default=180,
                    help="bounded seconds for the inner caller.run measurement get [Slice 3]")
    # Slice 4 (two-node R=5 replicated measurement) options
    ap.add_argument("--measure-islands", type=int, default=5,
                    help="R: placement-gated islands on the one cluster, fresh actors each (>=2) "
                         "[Slice 4 r5]")
    # INTERNAL: inner on-nodeA proof/measure handoff files (orchestrator-set; not for direct use)
    ap.add_argument("--inner-input", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--inner-output", default=None, help=argparse.SUPPRESS)
    args = ap.parse_args()
    args.pipeline_depths_list = [int(d) for d in args.pipeline_depths.split(",") if d.strip()]
    args.measure_depths_list = [int(d) for d in args.measure_depths.split(",") if d.strip()]

    if args.phase == "_two-node-inner-proof":
        # INTERNAL on-nodeA proof step: run the proof, write inner JSON, exit 0/1. No aggregate writer.
        if not (args.inner_input and args.inner_output):
            print(json.dumps({"inner_overall": "fail",
                              "inner_reason": "missing --inner-input/--inner-output"}))
            return 2
        out = _inner_two_node_proof(args)
        return 0 if out.get("inner_overall") == "pass" else 1

    if args.phase == "check-config":
        print(json.dumps(check_config(), indent=2))
        return 0

    if args.phase == "two-node-placement-proof":
        agg = phase_two_node_placement_proof(args)
    elif args.phase == "two-node-measurement-r1":
        agg = phase_two_node_measurement_r1(args)
    elif args.phase == "two-node-measurement-r5":
        agg = phase_two_node_measurement_r5(args)
    else:
        agg = phase_same_host_control(args)

    # LOCAL clean skip -> stdout only; never writes/clobbers a top-level aggregate.
    local_skip = agg.get("overall") == "skip" and (agg.get("local_skip_no_write")
                                                    or not _ray_available())
    if local_skip:
        agg.setdefault("note", "local clean skip: stdout only, no aggregate file written")
        print(json.dumps(agg, indent=2))
        return 0

    intended = args.out or _phase_pass_path(agg.get("phase", args.phase))
    res = safe_write_aggregate(intended, agg, allow_overwrite_pass=args.allow_overwrite_pass,
                               phase=agg.get("phase", args.phase))
    keys = _SUMMARY_KEYS.get(agg.get("phase", args.phase), _SUMMARY_KEYS["same-host-control"])
    print(json.dumps({k: agg.get(k) for k in keys}, indent=2))
    if res["redirected_from"]:
        print(f"[exp59] guard: redirected away from '{os.path.basename(res['redirected_from'])}' -> "
              f"'{os.path.basename(res['written_path'])}'")
    print(f"[exp59] wrote {res['written_path']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
