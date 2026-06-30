#!/usr/bin/env python3
"""exp61 -- same-axis Python-boundary experiment (Ray actor vs experiment-only HPX/RayX).

WHY exp61 EXISTS
----------------
exp58 measured the HPX path from C++ around `hpx::async(...).get()`; exp59 measured the Ray path
from Python around `ray.get(actor.method.remote(...))`. Those are different caller boundaries, so
exp58/exp59 are NOT a same-axis comparison. exp61 drives BOTH arms through ONE identical Python
timing harness at the SAME caller boundary:

    t0 = perf_counter_ns(); result = <blocking op>; t1 = perf_counter_ns()
      Ray:      ray.get(actor.dist_probe.remote(x))
      HPX/RayX: ext.dist_probe(x)   (experiment-only pybind/HPX extension)

SLICES SO FAR
-------------
* Slice 0 -- HPX-side embedding smoke (`--phase hpx-smoke`): single-locality (find_here), proves
  Python -> pybind/HPX -> fixed closed-int64 action -> result, GIL released, clean lifecycle.
* Slice 1 -- Ray-side smoke (`--phase ray-smoke`): a LOCAL single-node Ray actor timed at the same
  Python boundary with the same K/W/prewarm/clock and the same closed-int64 oracle family.

Slice 1 proves both arms can be driven by ONE harness. It is NOT a same-axis comparison: the two
arms are reported in SEPARATE artifacts and are NEVER differenced/ratioed here.

CLAIM FENCES (also embedded in every artifact)
----------------------------------------------
  same_axis_comparison=false, same_axis_claim_allowed=false, speedup_computed=false,
  ratio_reported=false, qd1_only=true, pipeline_excluded_from_comparison=true. Two-node placement,
  the connector, and any same-axis verdict are deferred. This does NOT imply the shipped
  rayx.runtime gained distributed actions, and it is NOT the HPX-calls-Python-callback experiment.

Phases:
  --phase hpx-smoke   import dist_probe_ext, start HPX, timed dist_probe calls, oracle, clean stop.
                      Skips cleanly if the extension is not built.
  --phase ray-smoke   local Ray actor, ray.get(actor.dist_probe.remote(x)) timed identically.
                      Skips cleanly if Ray is not installed.
  --phase selftest    pure-Python helper checks (no extension, no Ray, no HPX).
"""

import argparse
import json
import math
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EXT_DIRS = [os.path.join(HERE, "build"), os.path.join(HERE, "build", "Release")]
DEFAULT_CONNECTOR_DIRS = [os.path.join(HERE, "build"), os.path.join(HERE, "build", "Release")]
EXP61_RUNS = os.path.join(HERE, "_exp61_runs")
TOP_AGGREGATES = {
    "hpx-smoke": os.path.join(HERE, "hpx_smoke_aggregate.json"),
    "ray-smoke": os.path.join(HERE, "ray_smoke_aggregate.json"),
    "hpx-connected": os.path.join(HERE, "hpx_connected_aggregate.json"),
    "ray-remote": os.path.join(HERE, "ray_remote_aggregate.json"),
}

DIST_PROBE_XOR = 0x52415958  # "RAYX"
MEASUREMENT_BOUNDARY = "python_caller_perf_counter_ns_around_blocking_call"

FORBIDDEN_CLAIMS = [
    "same-axis Ray-vs-HPX comparison",
    "speedup",
    "ratio",
    "HPX beats Ray",
    "Ray is slower than HPX",
    "RayX makes Ray faster",
    "distributed rayx.runtime actions",
    "arbitrary remote Python execution",
    "production / fault-tolerance / fabric",
]


def oracle(x, node_tag):
    """Closed-int64 oracle: result = (x ^ 0x52415958) + (node_tag << 1). Correctness only --
    proves the intended actor/locality executed and returned the expected value, NOT placement."""
    return (int(x) ^ DIST_PROBE_XOR) + (int(node_tag) << 1)


def _pct(sorted_vals, p):
    if not sorted_vals:
        return None
    k = int(math.ceil(p * len(sorted_vals))) - 1
    return sorted_vals[max(0, min(k, len(sorted_vals) - 1))]


def _stats(ns):
    if not ns:
        return {"count": 0}
    s = sorted(ns)
    return {"count": len(s), "min_ns": s[0], "max_ns": s[-1],
            "mean_ns": int(sum(s) / len(s)),
            "p50_ns": _pct(s, 0.50), "p90_ns": _pct(s, 0.90), "p99_ns": _pct(s, 0.99)}


def _median(vals):
    """True median (average of the two middle values for even counts). None on empty."""
    s = sorted(v for v in vals if v is not None)
    if not s:
        return None
    n = len(s)
    m = n // 2
    return s[m] if n % 2 else (s[m - 1] + s[m]) / 2


def _across(vals):
    """Across-island summary for ONE metric of ONE arm: median + min/max + spread. Never mixes arms,
    never subtracts arms, never ratios. None when no island reported the metric."""
    s = sorted(v for v in vals if v is not None)
    if not s:
        return None
    return {"median": _median(s), "min": s[0], "max": s[-1], "spread": s[-1] - s[0]}


def timed_qd1(call, x, expected, k, w):
    """THE shared QD1 timing harness, used identically by BOTH arms: one prewarm (absorbs
    spin-up/warmup) + W warmups (dropped) + K timed `perf_counter_ns` measurements around the
    blocking `call(x)`, with a per-call oracle check against `expected`. Returns
    (raw_ns, correct_count, prewarm_result)."""
    prewarm_result = int(call(x))
    for _ in range(w):
        call(x)
    raw_ns = []
    correct = 0
    for _ in range(k):
        t0 = time.perf_counter_ns()
        r = int(call(x))
        t1 = time.perf_counter_ns()
        raw_ns.append(t1 - t0)
        if r == expected:
            correct += 1
    return raw_ns, correct, prewarm_result


def _atomic_write(path, payload):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def safe_write_top_aggregate(path, payload, allow_overwrite_pass=False):
    """skip/fail never overwrite a curated pass; pass-over-pass needs the flag. Returns
    (written_path, overwrite_refused)."""
    overall = payload.get("overall")
    existing = _read_json(path) if os.path.exists(path) else None
    refused = False
    if existing is not None and existing.get("overall") == "pass":
        base, ext = os.path.splitext(path)
        if overall != "pass":
            path = f"{base}_{'skip' if overall == 'skip' else 'fail'}{ext}"
            refused = True
        elif not allow_overwrite_pass:
            stamp = time.strftime("%Y%m%dT%H%M%S")
            path = f"{base}_redirected_{stamp}_{os.getpid()}{ext}"
            refused = True
    payload = dict(payload)
    payload["overwrite_refused"] = refused
    payload["top_level_aggregate_path"] = os.path.basename(path)
    _atomic_write(path, payload)
    return path, refused


def _fence_fields(phase, overall, k, w, prewarm):
    """Common claim-fence + shared-harness fields for EVERY phase, so both arms carry identical
    boundary/harness provenance and the same fences."""
    return {
        "experiment_id": "exp61",
        "phase": phase,
        "overall": overall,
        "comparison_kind": "python_boundary_smoke_only_per_arm",
        "same_axis_comparison": False,
        "same_axis_claim_allowed": False,
        "speedup_computed": False,
        "ratio_reported": False,
        "qd1_only": True,
        "pipeline_excluded_from_comparison": True,
        "clock": "perf_counter_ns",
        "k": k,
        "w": w,
        "prewarm": prewarm,
        "measurement_boundary": MEASUREMENT_BOUNDARY,
        # the SHARED harness both arms use -- proves identical timing semantics, not a comparison.
        "shared_harness": {
            "clock": "perf_counter_ns",
            "k": k,
            "w": w,
            "prewarm": prewarm,
            "qd1_only": True,
            "blocking_call_only": True,
            "pipeline_excluded": True,
            "per_call_oracle_validation": True,
            "measurement_boundary": MEASUREMENT_BOUNDARY,
            "timing_helper": "timed_qd1 (one function used by both hpx-smoke and ray-smoke)",
        },
        "forbidden_claims": FORBIDDEN_CLAIMS,
        "not_hpx_calls_python_callback_experiment": True,
        "not_distributed_rayx_runtime": True,
    }


# ---------------------------------------------------------------------------------------------------
# phase: hpx-smoke (Slice 0)
# ---------------------------------------------------------------------------------------------------
def _import_ext(ext_dirs):
    for d in ext_dirs:
        if os.path.isdir(d):
            sys.path.insert(0, d)
    try:
        import dist_probe_ext  # noqa: E402
        return dist_probe_ext
    except Exception:  # noqa: BLE001
        return None


def phase_hpx_smoke(args):
    ext_dirs = [args.ext_dir] if args.ext_dir else DEFAULT_EXT_DIRS
    ext = _import_ext(ext_dirs)
    if ext is None:
        print("SKIP: dist_probe_ext not importable (extension not built). Looked in:")
        for d in ext_dirs:
            print("  ", d)
        print("Build it: cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release "
              "-DPYBIND11_FINDPYTHON=ON -DCMAKE_PREFIX_PATH='<hpx-install>;$(python -m pybind11 "
              "--cmakedir)' && cmake --build build. Nothing measured, nothing written.")
        return None, "skip"

    os.makedirs(EXP61_RUNS, exist_ok=True)
    run_id = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"
    x = args.x

    started = locality_id = expected = None
    raw_ns, correct, prewarm_result, error = [], 0, None, None
    clean_shutdown = False
    try:
        ext.start(hpx_threads=args.hpx_threads)
        started = True
        locality_id = int(ext.local_locality_id())
        expected = oracle(x, locality_id)
        raw_ns, correct, prewarm_result = timed_qd1(ext.dist_probe, x, expected, args.k, args.w)
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
    finally:
        try:
            ext.shutdown()
            clean_shutdown = True
        except Exception as e:  # noqa: BLE001
            error = error or f"shutdown: {type(e).__name__}: {e}"

    oracle_correct = (args.k > 0 and correct == args.k)
    gil_released = bool(getattr(ext, "gil_released_around_blocking_call", False))
    gates = {
        "ext_imported": True,
        "hpx_started": bool(started),
        "locality_id_resolved": locality_id is not None,
        "prewarm_correct": (prewarm_result == expected) if expected is not None else False,
        "every_call_oracle_correct": oracle_correct,
        "hpx_gil_released": gil_released,
        "clean_shutdown": clean_shutdown,
        "no_error": error is None,
    }
    overall = "pass" if all(gates.values()) else "fail"

    payload = _fence_fields("hpx-smoke", overall, args.k, args.w, 1)
    payload.update({
        "run_id": run_id,
        "x": x,
        "measurement_boundary_hpx": MEASUREMENT_BOUNDARY,
        "hpx_extension_experiment_only": True,
        "hpx_call_primitive": "ext.dist_probe(x) -> hpx::async<dist_probe_action>(find_here,x).get()",
        "node_tag_locality_id": locality_id,
        "oracle_expected": expected,
        "hpx_oracle_correct": oracle_correct,
        "hpx_gil_released": gil_released,
        "hpx_threads": args.hpx_threads,
        "ext_dir": next((d for d in ext_dirs if os.path.isdir(d)), None),
        "single_locality_find_here": True,
        "connector_used": False,
        "ray_side_measured": False,
        "gates": gates,
        "error": error,
        "boundary_call_ns_observation_only": _stats(raw_ns),
        "timing_is_observation_only_not_a_claim": True,
        "claim_fences": [
            "Slice 0: HPX-side embedding smoke ONLY (single-locality find_here). No Ray side here, "
            "no two-node, no connector.",
            "NO same-axis comparison, NO speedup, NO ratio, NO 'HPX beats Ray'. Timing is "
            "observation-only and machine-specific.",
            "experiment-only pybind/HPX extension; NOT rayx.runtime, NOT _rayx, NO Ray.",
        ],
    })
    return _finish(payload, overall, run_id, args.allow_overwrite_pass)


# ---------------------------------------------------------------------------------------------------
# phase: ray-smoke (Slice 1)
# ---------------------------------------------------------------------------------------------------
def phase_ray_smoke(args):
    try:
        import ray
    except Exception:  # noqa: BLE001
        print("SKIP: ray not importable (Ray not installed). Nothing measured, nothing written.")
        return None, "skip"

    os.makedirs(EXP61_RUNS, exist_ok=True)
    run_id = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"
    x = args.x
    node_tag = args.ray_node_tag           # fixed, explicit; need NOT match HPX locality_id
    expected = oracle(x, node_tag)

    ray_init_ok = False
    raw_ns, correct, prewarm_result, error = [], 0, None, None
    clean_shutdown = False
    actor = None
    try:
        ray.init(ignore_reinit_error=True, include_dashboard=False, num_cpus=2,
                 log_to_driver=False, configure_logging=False)
        ray_init_ok = True

        @ray.remote
        class DistProbe:
            def __init__(self, tag):
                self.tag = int(tag)

            def dist_probe(self, xx):
                # the SAME closed-int64 oracle family as the HPX arm; tag is the actor's fixed tag.
                return (int(xx) ^ 0x52415958) + (self.tag << 1)

        actor = DistProbe.remote(node_tag)
        call = lambda xx: ray.get(actor.dist_probe.remote(xx))  # noqa: E731 (the timed boundary)
        raw_ns, correct, prewarm_result = timed_qd1(call, x, expected, args.k, args.w)
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
    finally:
        try:
            if actor is not None:
                ray.kill(actor)
        except Exception:  # noqa: BLE001
            pass
        try:
            ray.shutdown()
            clean_shutdown = True
        except Exception as e:  # noqa: BLE001
            error = error or f"shutdown: {type(e).__name__}: {e}"

    oracle_correct = (args.k > 0 and correct == args.k)
    st = _stats(raw_ns)
    gates = {
        "ray_imported": True,
        "ray_init_ok": ray_init_ok,
        "prewarm_correct": (prewarm_result == expected),
        "every_call_oracle_correct": oracle_correct,
        "clean_shutdown": clean_shutdown,
        "no_error": error is None,
    }
    overall = "pass" if all(gates.values()) else "fail"

    payload = _fence_fields("ray-smoke", overall, args.k, args.w, 1)
    payload.update({
        "run_id": run_id,
        "x": x,
        "measurement_boundary_ray": MEASUREMENT_BOUNDARY,
        "ray_side_measured": True,
        "ray_call_primitive": "ray.get(actor.dist_probe.remote(x))",
        "ray_actor_local_only": True,
        "ray_version": getattr(ray, "__version__", None),
        "ray_node_tag": node_tag,
        "ray_node_tag_note": ("fixed, explicit tag for this single-node smoke; chosen for the "
                              "same closed-int64 oracle family as the HPX arm. It is NOT a "
                              "placement proof and need NOT match the HPX locality_id."),
        "oracle_expected": expected,
        "ray_oracle_correct": oracle_correct,
        "ray_p50_ns": st.get("p50_ns"),
        "ray_p90_ns": st.get("p90_ns"),
        "ray_p99_ns": st.get("p99_ns"),
        "ray_mean_ns": st.get("mean_ns"),
        "gates": gates,
        "error": error,
        "boundary_call_ns_observation_only": st,
        "timing_is_observation_only_not_a_claim": True,
        "two_node_placement_used": False,
        "claim_fences": [
            "Slice 1: LOCAL single-node Ray actor smoke ONLY, timed at the same Python boundary "
            "and harness as the HPX arm.",
            "NOT a same-axis comparison: the Ray and HPX arms are separate artifacts and are NEVER "
            "differenced or ratioed. No speedup, no ratio, no 'HPX beats Ray', no 'Ray is slower "
            "than HPX', no 'RayX makes Ray faster'.",
            "single-node, QD1, blocking-only; two-node placement and the connector are deferred.",
        ],
    })
    return _finish(payload, overall, run_id, args.allow_overwrite_pass)


def _finish(payload, overall, run_id, allow_overwrite_pass):
    """Shared artifact write: phase-specific top aggregate (guarded) + a per-run copy."""
    phase = payload["phase"]
    written, refused = safe_write_top_aggregate(TOP_AGGREGATES[phase], payload,
                                                allow_overwrite_pass=allow_overwrite_pass)
    with open(os.path.join(EXP61_RUNS, f"{phase}_{run_id}.json"), "w") as f:
        json.dump(payload, f, indent=2)
    payload["written"] = os.path.basename(written)
    payload["overwrite_refused"] = refused
    return payload, overall


def _finish_scaffold(payload, overall, run_id, allow_overwrite_pass):
    """Slice-2A two-node-capable phases: a real two-node PASS lands in the curated base aggregate;
    a local SKIP or a two-node FAIL lands ONLY in an ignored `<base>_skip.json` / `<base>_fail.json`
    sibling so the curated name NEVER carries an unexercised-two-node result. Always writes a
    per-run provenance copy."""
    os.makedirs(EXP61_RUNS, exist_ok=True)
    phase = payload["phase"]
    base = TOP_AGGREGATES[phase]
    if overall == "pass":
        written, refused = safe_write_top_aggregate(base, payload,
                                                    allow_overwrite_pass=allow_overwrite_pass)
    else:
        root, ext = os.path.splitext(base)
        written = f"{root}_{overall}{ext}"   # _skip / _fail -- gitignored, never the curated base
        payload = dict(payload)
        payload["top_level_aggregate_path"] = os.path.basename(written)
        payload["overwrite_refused"] = False
        _atomic_write(written, payload)
        refused = False
    with open(os.path.join(EXP61_RUNS, f"{phase}_{run_id}.json"), "w") as f:
        json.dump(payload, f, indent=2)
    payload["written"] = os.path.basename(written)
    payload["overwrite_refused"] = refused
    return payload, overall


def _finish_matched(payload, overall, run_id, arm):
    """Slice 3: route a matched-run arm result to the PAIR-SCOPED name slice3_<id>_<arm>.json (plus a
    per-run copy). It NEVER writes the Slice-2B curated base aggregate, so the existing curated passes
    (hpx_connected_aggregate.json / ray_remote_aggregate.json) are left completely untouched."""
    os.makedirs(EXP61_RUNS, exist_ok=True)
    mid = payload["matched_run_id"]
    path = os.path.join(HERE, f"slice3_{mid}_{arm}.json")
    payload = dict(payload)
    payload["top_level_aggregate_path"] = os.path.basename(path)
    _atomic_write(path, payload)
    with open(os.path.join(EXP61_RUNS, f"{payload['phase']}_{mid}_{run_id}.json"), "w") as f:
        json.dump(payload, f, indent=2)
    payload["written"] = os.path.basename(path)
    return payload, overall


def _finish_arm(payload, overall, run_id, arm, args):
    """Route an arm result: a matched run (--matched-run-id) -> pair-scoped slice3 artifact; otherwise
    the existing Slice-2A/2B scaffold behavior (curated base on a real pass, ignored sibling on
    skip/fail)."""
    if getattr(args, "matched_run_id", None):
        return _finish_matched(payload, overall, run_id, arm)
    return _finish_scaffold(payload, overall, run_id, args.allow_overwrite_pass)


# ---------------------------------------------------------------------------------------------------
# two-node placement helpers (pure where possible, so selftest can exercise them hermetically)
# ---------------------------------------------------------------------------------------------------
def slurm_two_node_from_env(env, hostlist_resolver=None, job_resolver=None):
    """Decide whether we are inside a real Slurm ALLOCATION of >= 2 nodes -- the JOB, not the current
    srun STEP. PURE w.r.t. `env`; `hostlist_resolver(nodelist)->[hosts]` and `job_resolver(job_id)->
    (num_nodes, nodelist)` are injected so selftest stays hermetic.

    Patch A (Bug A): a one-node `srun` step of a two-node job exposes STEP-scoped env
    (`SLURM_JOB_NODELIST=medusa00`, `SLURM_NNODES=1`) while the allocation is still
    `SLURM_NODELIST=medusa[00-01]` (and authoritative via `scontrol show job`). We therefore take the
    LARGEST node count across: the authoritative job allocation, the `SLURM_JOB_*` keys, and the
    `SLURM_*` alternates -- so a shrunk step can never mask a true two-node allocation, and we never
    over-count (off-cluster has no job_id => two_node stays False)."""
    job_id = env.get("SLURM_JOB_ID") or env.get("SLURM_JOBID")

    def expand(nl):
        if nl and hostlist_resolver is not None:
            try:
                return [h for h in hostlist_resolver(nl) if h]
            except Exception:  # noqa: BLE001
                return []
        return []

    def count_from(nl, raw):
        c = 0
        if raw:
            try:
                c = int(raw)
            except (TypeError, ValueError):
                c = 0
        hosts = expand(nl)
        # take the LARGER of the numeric count and the expanded nodelist: a step-scoped
        # SLURM_NNODES=1 must not shrink a SLURM_NODELIST that expands to the full allocation.
        return max(c, len(hosts)), hosts

    candidates = []  # (node_count, nodelist, hostnames, source)
    # 1) authoritative job allocation (step-independent)
    if job_id and job_resolver is not None:
        try:
            jn, jl = job_resolver(job_id)
            c, hosts = count_from(jl, jn)
            if c or jl:
                candidates.append((c, jl, hosts, "scontrol_job"))
        except Exception:  # noqa: BLE001
            pass
    # 2) job-level env keys, then 3) plain alternates (either can be step-scoped under srun)
    c1, h1 = count_from(env.get("SLURM_JOB_NODELIST"), env.get("SLURM_JOB_NUM_NODES"))
    candidates.append((c1, env.get("SLURM_JOB_NODELIST"), h1, "env_job"))
    c2, h2 = count_from(env.get("SLURM_NODELIST"), env.get("SLURM_NNODES"))
    candidates.append((c2, env.get("SLURM_NODELIST"), h2, "env_alt"))

    n, nodelist, hostnames, source = 0, None, [], "none"
    for c, nl, hosts, src in candidates:
        if c > n:
            n, nodelist, hostnames, source = c, nl, hosts, src
    if not hostnames:  # keep any resolved hostnames even if the count came from a numeric signal
        for _, _, hosts, _ in candidates:
            if hosts:
                hostnames = hosts
                break

    two_node = bool(job_id) and n >= 2
    return {"slurm_job_id": job_id, "slurm_job_nodelist": nodelist, "slurm_num_nodes": n,
            "hostnames": hostnames, "two_node": two_node, "alloc_source": source}


def ray_cluster_two_node(node_ids):
    """True iff at least two DISTINCT Ray node ids are present -- the gate that prevents a local
    single-node Ray cluster from masquerading as a remote-placement run."""
    return len({n for n in node_ids if n}) >= 2


def pick_remote_node(node_ids, driver_node_id):
    """Pick a node id distinct from the driver's node, or None. The actor is hard-pinned here so the
    timed call is genuinely off the driver node (verified again by the actor's own reported node)."""
    for n in node_ids:
        if n and n != driver_node_id:
            return n
    return None


def _short_host(h):
    """Normalize a hostname to its short form ('medusa01.rostam.cct.lsu.edu' -> 'medusa01') so the
    HPX arm (mixed short/FQDN) and the Ray arm (FQDN) produce a COMPARABLE node_pair. None/'' -> None."""
    if not h:
        return None
    return str(h).split(".")[0]


# Fields the manifest must find EQUAL across the two arms (Slice 3 matched-structure invariants).
MATCHED_INVARIANT_KEYS = ("matched_run_id", "slurm_job_id", "node_pair", "selected_subnet",
                          "k", "w", "prewarm", "clock", "measurement_boundary")
# Slice 5 same-node variant: ONE node (node_single) instead of a node_pair, plus an explicit
# placement_mode the two arms must agree on. matched_run_id stays in (per-pair structure check).
SAME_NODE_MATCHED_INVARIANT_KEYS = ("matched_run_id", "slurm_job_id", "node_single",
                                    "selected_subnet", "k", "w", "prewarm", "clock",
                                    "measurement_boundary", "placement_mode")


def matched_invariants(hpx, ray, placement_mode="cross_node"):
    """PURE matched-structure check. Returns (ok, checks). Proves the two arms ran in the SAME
    allocation / subnet / K-W / prewarm / boundary and each passed its own oracle. It does NOT
    compare or relate their timings -- this is a STRUCTURE proof, never a same-axis comparison.

    placement_mode='cross_node' (default, Slice 3/4): gates two_node_exercised and agrees on a
    node_pair. placement_mode='same_node' (Slice 5): gates same_node_exercised and agrees on a
    node_single plus placement_mode. Default behavior is byte-for-byte the original cross-node path."""
    same = placement_mode == "same_node"
    keys = SAME_NODE_MATCHED_INVARIANT_KEYS if same else MATCHED_INVARIANT_KEYS
    if same:
        checks = {
            "hpx_pass": hpx.get("overall") == "pass",
            "ray_pass": ray.get("overall") == "pass",
            "hpx_same_node_exercised": hpx.get("same_node_exercised") is True,
            "ray_same_node_exercised": ray.get("same_node_exercised") is True,
            "hpx_oracle_true": hpx.get("hpx_oracle_correct") is True,
            "ray_oracle_true": ray.get("ray_oracle_correct") is True,
        }
    else:
        checks = {
            "hpx_pass": hpx.get("overall") == "pass",
            "ray_pass": ray.get("overall") == "pass",
            "hpx_two_node_exercised": hpx.get("two_node_exercised") is True,
            "ray_two_node_exercised": ray.get("two_node_exercised") is True,
            "hpx_oracle_true": hpx.get("hpx_oracle_correct") is True,
            "ray_oracle_true": ray.get("ray_oracle_correct") is True,
        }
    for key in keys:
        hv, rv = hpx.get(key), ray.get(key)
        checks[f"same_{key}"] = (hv is not None and rv is not None and hv == rv)
    return all(checks.values()), checks


# Keys every island must AGREE on across the band. NOTE: matched_run_id is intentionally EXCLUDED --
# each island has its own distinct id; everything else (allocation/placement/K-W-prewarm/boundary)
# must match.
CROSS_ISLAND_KEYS = ("slurm_job_id", "node_pair", "selected_subnet", "k", "w", "prewarm", "clock",
                     "measurement_boundary")
# Slice 5 same-node band agreement keys: node_single instead of node_pair, plus placement_mode.
SAME_NODE_CROSS_ISLAND_KEYS = ("slurm_job_id", "node_single", "selected_subnet", "k", "w", "prewarm",
                               "clock", "measurement_boundary", "placement_mode")


def _canon(v):
    """Hashable canonical form for agreement comparison (lists -> tuples)."""
    return tuple(v) if isinstance(v, list) else v


def _decanon(v):
    """Inverse of _canon for readable reporting."""
    return list(v) if isinstance(v, tuple) else v


def _cross_island_agreement(canon_list, keys=CROSS_ISLAND_KEYS):
    """PURE. canon_list is [(id, {key: canon_value})]. Agreement requires every island to carry a
    non-None value for each key AND all islands to agree. Returns (agree, disagreements)."""
    agree = True
    disagreements = {}
    for k in keys:
        vals = [c.get(k) for _, c in canon_list]
        if any(v is None for v in vals) or len(set(vals)) != 1:
            agree = False
            disagreements[k] = [_decanon(v) for v in vals]
    return agree, disagreements


def _shared_from_canon(canon_list, keys=CROSS_ISLAND_KEYS):
    """The agreed-on invariants (from the first island) for the manifest body; assumes agreement."""
    if not canon_list:
        return None
    first = canon_list[0][1]
    return {k: _decanon(first.get(k)) for k in keys}


def _scontrol_hostnames(nodelist):
    """Expand a Slurm nodelist to hostnames via `scontrol show hostnames`. Best-effort; [] on any
    failure (then slurm_two_node_from_env falls back to SLURM_JOB_NUM_NODES)."""
    try:
        out = subprocess.run(["scontrol", "show", "hostnames", nodelist],
                             capture_output=True, text=True, timeout=15)
        if out.returncode == 0:
            return [h for h in out.stdout.split() if h]
    except Exception:  # noqa: BLE001
        pass
    return []


def _scontrol_job_alloc(job_id):
    """Authoritative JOB allocation (num_nodes, nodelist) via `scontrol show job` -- step-independent,
    so it sees the whole allocation even when the driver runs as a one-node srun step (Patch A /
    Bug A). Returns (num_nodes:int|None, nodelist:str|None); (None, None) on any failure. The `\\b`
    anchors avoid matching `ReqNodeList=`/`ExcNodeList=`."""
    import re
    try:
        out = subprocess.run(["scontrol", "show", "job", str(job_id)],
                             capture_output=True, text=True, timeout=15)
        if out.returncode == 0:
            txt = out.stdout
            m_n = re.search(r"\bNumNodes=(\d+)", txt)
            m_l = re.search(r"\bNodeList=(\S+)", txt)
            num = int(m_n.group(1)) if m_n else None
            nl = m_l.group(1) if (m_l and m_l.group(1) not in ("(null)", "")) else None
            return num, nl
    except Exception:  # noqa: BLE001
        pass
    return None, None


def _find_connector_bin(args, dirs):
    """Locate the built dist_probe_connector executable, or None (a skip-clean precondition)."""
    if args.connector_bin:
        return args.connector_bin if os.path.isfile(args.connector_bin) else None
    for d in dirs:
        cand = os.path.join(d, "dist_probe_connector")
        if os.path.isfile(cand):
            return cand
    return None


def _first_ip_for_subnet(prefer_subnet):
    """Best-effort first local IPv4 whose prefix matches `prefer_subnet` (e.g. '10.42.5.'). None if
    none match. On Rostam eno16 -> 10.42.5.x (see CLAUDE.md); the runner can also pass --root-ip."""
    if not prefer_subnet:
        return None
    try:
        out = subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=10)
        if out.returncode == 0:
            for ip in out.stdout.split():
                if ip.startswith(prefer_subnet):
                    return ip
    except Exception:  # noqa: BLE001
        pass
    return None


# ---------------------------------------------------------------------------------------------------
# phase: hpx-connected (Slice 2A) -- two-node-capable; skips cleanly off a >=2-node Slurm allocation
# ---------------------------------------------------------------------------------------------------
def _connected_base_payload(args, overall, slurm, connector_bin, ext, run_id):
    """Build the hpx-connected provenance payload with EVERY required placement field present
    (null/false until a real two-node run fills them). Carries all the same fences as the smokes."""
    payload = _fence_fields("hpx-connected", overall, args.k, args.w, 1)
    payload.update({
        "run_id": run_id,
        "x": args.x,
        "measurement_boundary_hpx": MEASUREMENT_BOUNDARY,
        "hpx_extension_experiment_only": True,
        "hpx_connector_used": True,
        "hpx_call_primitive": "ext.dist_probe_remote(x) -> "
                              "hpx::async<exp61_dist_probe_action>(remote_id, x).get()",
        "hpx_action_registration_name": "exp61_dist_probe_action",
        "hpx_parcelport": "tcp",
        "hpx_tcp_nodelay_verified": False,
        "two_node_capable_scaffold": True,
        "two_node_exercised": False,
        "single_locality_find_here": False,
        "ray_side_measured": False,
        # placement / provenance -- null until a real two-node run proves them
        "hpx_root_hostname": None,
        "hpx_connector_hostname": None,
        "hpx_root_ip": None,
        "hpx_connector_ip": None,
        "hpx_root_locality_id": None,
        "hpx_remote_locality_id": None,
        "hpx_here_locality_differs_from_remote": None,
        "node_tag_remote_locality_id": None,
        "oracle_expected": None,
        "hpx_oracle_correct": False,
        # Patch B provenance defaults (filled when a real two-node run executes)
        "hpx_prefer_subnet": getattr(args, "prefer_subnet", None),
        "hpx_root_port": getattr(args, "root_port", None),
        "hpx_ignore_batch_env": None,
        "hpx_root_extra_args": None,
        "hpx_connector_self_binds_own_subnet_ip": None,
        "slurm_alloc_source": slurm.get("alloc_source"),
        # Slice 3 matched-structure provenance (shared, normalized; node_pair filled when exercised)
        "matched_run_id": getattr(args, "matched_run_id", None),
        "selected_subnet": getattr(args, "prefer_subnet", None),
        "node_pair": None,
        "slurm_job_id": slurm.get("slurm_job_id"),
        "slurm": {k: slurm.get(k) for k in
                  ("slurm_job_id", "slurm_job_nodelist", "slurm_num_nodes", "two_node",
                   "alloc_source")},
        "connector_bin": connector_bin,
        "ext_two_node_capable": bool(ext is not None and getattr(ext, "two_node_capable", False)),
        "boundary_call_ns_observation_only": _stats([]),
        "timing_is_observation_only_not_a_claim": True,
        "error": None,
        "claim_fences": [
            "Slice 2A: two-node-CAPABLE scaffolding. A reachable remote locality is a MECHANISM, "
            "not a performance result. Rostam two-node validation is Slice 2B.",
            "NOT a same-axis comparison: the Ray and HPX arms are separate artifacts and are NEVER "
            "differenced or ratioed. No speedup, no ratio, no 'HPX beats Ray', no 'Ray is slower "
            "than HPX', no 'RayX makes Ray faster'.",
            "oracle correctness proves intended closed-int64 execution on a locality id; physical "
            "placement proof needs hostnames/node ids/hard gates (recorded here as provenance).",
        ],
    })
    return payload


def phase_hpx_connected(args):
    run_id = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"
    ext_dirs = [args.ext_dir] if args.ext_dir else DEFAULT_EXT_DIRS
    conn_dirs = [args.connector_dir] if args.connector_dir else DEFAULT_CONNECTOR_DIRS
    slurm = slurm_two_node_from_env(os.environ, _scontrol_hostnames, _scontrol_job_alloc)
    connector_bin = _find_connector_bin(args, conn_dirs)
    ext = _import_ext(ext_dirs)
    ext_ready = (ext is not None and hasattr(ext, "dist_probe_remote")
                 and bool(getattr(ext, "two_node_capable", False)))

    pre = {"slurm_two_node": slurm["two_node"], "connector_built": connector_bin is not None,
           "ext_remote_capable": ext_ready}
    if not all(pre.values()):
        reasons = [k for k, v in pre.items() if not v]
        payload = _connected_base_payload(args, "skip", slurm, connector_bin, ext, run_id)
        payload["skip_reasons"] = reasons
        payload["gates"] = pre
        print("SKIP hpx-connected: unmet two-node preconditions [" + ", ".join(reasons) + "]. "
              "Two-node scaffolding is present but is NOT exercised locally; nothing measured, no "
              "curated pass written.")
        return _finish_arm(payload, "skip", run_id, "hpx", args)

    return _run_hpx_connected_two_node(args, slurm, connector_bin, ext, run_id)


def _run_hpx_connected_two_node(args, slurm, connector_bin, ext, run_id):
    """UNVALIDATED Slice-2A two-node orchestration; Rostam Slice-2B confirms/tunes the exact HPX
    networking flags and transport. Runs ONLY under a real >=2-node Slurm allocation (the caller
    gated that). Any failure is captured as overall='fail' (written to the ignored _fail sibling) --
    it can NEVER emit a fake pass, and it never falls back to find_here."""
    import tempfile
    payload = _connected_base_payload(args, "fail", slurm, connector_bin, ext, run_id)
    hostnames = slurm["hostnames"] or []
    root_host = hostnames[0] if hostnames else ext.hostname()
    remote_host = pick_remote_node(hostnames, root_host)
    bootdir = args.bootstrap_dir or tempfile.mkdtemp(prefix=f"exp61_conn_{run_id}_", dir=EXP61_RUNS)
    root_ip = args.root_ip or _first_ip_for_subnet(args.prefer_subnet)
    root_port = args.root_port

    proc = None
    started = False
    error = None
    raw_ns, correct, prewarm_result = [], 0, None
    remote_loc = -1
    root_loc = None
    expected = None
    try:
        if not remote_host:
            raise RuntimeError("no remote host distinct from root in SLURM hostnames")
        if not root_ip:
            raise RuntimeError("no root IP for --prefer-subnet; pass --root-ip explicitly")
        os.makedirs(bootdir, exist_ok=True)
        # 1) embedded HPX runtime as the AGAS ROOT with TCP networking on the selected subnet.
        #    Patch B (Bug B): --hpx:ignore-batch-env FIRST, so HPX does NOT build its node list from
        #    the Slurm hostname batch env (which rejected our explicit IP with "Requested AGAS host
        #    not found in node list"); then our explicit IP endpoints are authoritative.
        root_extra = ["--hpx:ignore-batch-env",
                      f"--hpx:hpx={root_ip}:{root_port}", f"--hpx:agas={root_ip}:{root_port}",
                      "--hpx:expect-connecting-localities"]
        ext.start(hpx_threads=args.hpx_threads, extra_args=root_extra)
        started = True
        root_loc = int(ext.local_locality_id())
        # 2) launch the connector on the remote node; it joins root AGAS and serves the action.
        #    Patch B: the connector binds ITS OWN node's selected-subnet IP -- it self-detects via
        #    --prefer-subnet (NOT the root IP). It also ignores the batch env, same as the root.
        conn_cmd = ["srun", "--nodes=1", "--ntasks=1", f"--nodelist={remote_host}",
                    connector_bin, "--role=connect", f"--bootstrap={bootdir}",
                    f"--serve-timeout={args.serve_timeout}",
                    f"--prefer-subnet={args.prefer_subnet}",
                    f"--agas-preprobe-host={root_ip}", f"--agas-preprobe-port={root_port}",
                    "--hpx:ignore-batch-env", f"--hpx:agas={root_ip}:{root_port}"]
        proc = subprocess.Popen(conn_cmd)
        # 3) wait for the remote locality, cache it (no single-node fallback: <0 means no remote).
        remote_loc = int(ext.await_remote(timeout_s=args.await_timeout))
        if remote_loc < 0:
            raise RuntimeError("no remote locality joined within await timeout")
        node_tag = remote_loc
        expected = oracle(args.x, node_tag)
        # 4) timed QD1 boundary -- the SAME shared harness every arm uses.
        raw_ns, correct, prewarm_result = timed_qd1(ext.dist_probe_remote, args.x, expected,
                                                    args.k, args.w)
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
    finally:
        # signal the connector to leave, then reap it.
        try:
            with open(os.path.join(bootdir, "served1.ok"), "w") as f:
                f.write("served\n")
        except OSError:
            pass
        if proc is not None:
            try:
                proc.wait(timeout=max(10, args.serve_timeout))
            except Exception:  # noqa: BLE001
                proc.kill()
        if started:
            try:
                ext.shutdown()
            except Exception as e:  # noqa: BLE001
                error = error or f"shutdown: {type(e).__name__}: {e}"

    oracle_correct = (args.k > 0 and correct == args.k and error is None)
    st = _stats(raw_ns)
    here_differs = (root_loc is not None and remote_loc >= 0 and remote_loc != root_loc)
    gates = {
        "slurm_two_node": True,
        "hpx_started": started,
        "remote_locality_joined": remote_loc >= 0,
        "remote_differs_from_root": here_differs,
        "prewarm_correct": (prewarm_result == expected) if expected is not None else False,
        "every_call_oracle_correct": oracle_correct,
        "no_error": error is None,
    }
    overall = "pass" if all(gates.values()) else "fail"

    payload = _connected_base_payload(args, overall, slurm, connector_bin, ext, run_id)
    payload.update({
        "two_node_exercised": True,
        "hpx_root_hostname": root_host,
        "hpx_connector_hostname": remote_host,
        "hpx_root_ip": root_ip,
        "hpx_connector_ip": None,   # filled from connector attestation below if available
        "hpx_root_locality_id": root_loc,
        "hpx_remote_locality_id": remote_loc if remote_loc >= 0 else None,
        "hpx_here_locality_differs_from_remote": here_differs,
        "node_tag_remote_locality_id": remote_loc if remote_loc >= 0 else None,
        "oracle_expected": expected,
        "hpx_oracle_correct": oracle_correct,
        "boundary_call_ns_observation_only": st,
        "gates": gates,
        "error": error,
        "bootstrap_dir": bootdir,
        # Patch B provenance: explicit subnet/endpoints + the batch-env override that was applied.
        "hpx_prefer_subnet": args.prefer_subnet,
        "hpx_root_port": root_port,
        "hpx_ignore_batch_env": True,
        "hpx_root_extra_args": root_extra,
        "hpx_connector_self_binds_own_subnet_ip": True,
        "slurm_alloc_source": slurm.get("alloc_source"),
        # Slice 3 matched-structure: normalized ordered node_pair (root -> remote), short hostnames.
        "selected_subnet": args.prefer_subnet,
        "node_pair": [_short_host(root_host), _short_host(remote_host)],
        "slurm_job_id": slurm.get("slurm_job_id"),
    })
    attest = _read_json(os.path.join(bootdir, "attest_connect.json"))
    if attest:
        payload["hpx_connector_ip"] = attest.get("advertised_ip")
        payload["hpx_connector_hostname"] = attest.get("hostname") or remote_host
        payload["connector_attestation"] = attest
        # re-normalize node_pair from the connector's attested hostname (authoritative remote host)
        payload["node_pair"] = [_short_host(root_host),
                                _short_host(attest.get("hostname") or remote_host)]
    return _finish_arm(payload, overall, run_id, "hpx", args)


# ---------------------------------------------------------------------------------------------------
# phase: ray-remote (Slice 2A) -- two-node-capable; skips cleanly without a >=2-node Ray cluster
# ---------------------------------------------------------------------------------------------------
def _ray_remote_base_payload(args, overall, slurm, run_id):
    payload = _fence_fields("ray-remote", overall, args.k, args.w, 1)
    payload.update({
        "run_id": run_id,
        "x": args.x,
        "measurement_boundary_ray": MEASUREMENT_BOUNDARY,
        "ray_side_measured": True,
        "ray_call_primitive": "ray.get(actor.dist_probe.remote(x))",
        "ray_placement_strategy": "NodeAffinitySchedulingStrategy(soft=False)",
        "ray_actor_remote_only": True,
        "two_node_capable_scaffold": True,
        "two_node_exercised": False,
        "ray_version": None,
        "ray_node_tag": args.ray_node_tag,
        "ray_node_ids": None,
        "ray_hostnames": None,
        "ray_ips": None,
        "ray_driver_node_id": None,
        "ray_target_node_id": None,
        "ray_off_node_sample_count": 0,
        "ray_driver_ip": None,
        "ray_target_ip": None,
        "ray_ips_on_selected_subnet": None,
        "oracle_expected": None,
        "ray_oracle_correct": False,
        "ray_p50_ns": None, "ray_p90_ns": None, "ray_p99_ns": None, "ray_mean_ns": None,
        # Slice 3 matched-structure provenance (shared, normalized; node_pair filled when exercised)
        "matched_run_id": getattr(args, "matched_run_id", None),
        "selected_subnet": getattr(args, "prefer_subnet", None),
        "node_pair": None,
        "slurm_job_id": slurm.get("slurm_job_id"),
        "slurm_alloc_source": slurm.get("alloc_source"),
        "slurm": {k: slurm.get(k) for k in
                  ("slurm_job_id", "slurm_job_nodelist", "slurm_num_nodes", "two_node",
                   "alloc_source")},
        "boundary_call_ns_observation_only": _stats([]),
        "timing_is_observation_only_not_a_claim": True,
        "error": None,
        "claim_fences": [
            "Slice 2A: two-node-CAPABLE scaffolding. The actor is HARD-pinned off the driver node "
            "(NodeAffinitySchedulingStrategy soft=False) and verified off-node; no local actor is "
            "ever called 'remote'. Rostam two-node validation is Slice 2B.",
            "NOT a same-axis comparison: separate artifact from the HPX arm; NEVER differenced or "
            "ratioed. No speedup, no ratio, no 'HPX beats Ray', no 'Ray is slower than HPX'.",
            "exp59 numbers are NOT reused here; this phase only runs against a live >=2-node Ray "
            "cluster it connects to.",
        ],
    })
    return payload


def phase_ray_remote(args):
    run_id = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"
    slurm = slurm_two_node_from_env(os.environ, _scontrol_hostnames, _scontrol_job_alloc)

    if not slurm["two_node"]:
        payload = _ray_remote_base_payload(args, "skip", slurm, run_id)
        payload["skip_reasons"] = ["slurm_two_node"]
        print("SKIP ray-remote: not inside a >=2-node Slurm allocation. No local-actor fallback; "
              "nothing measured, no curated pass written.")
        return _finish_arm(payload, "skip", run_id, "ray", args)

    try:
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    except Exception:  # noqa: BLE001
        payload = _ray_remote_base_payload(args, "skip", slurm, run_id)
        payload["skip_reasons"] = ["ray_import"]
        print("SKIP ray-remote: ray not importable. Nothing measured, no curated pass written.")
        return _finish_arm(payload, "skip", run_id, "ray", args)

    # connect to an EXISTING cluster only (never start a local one): address='auto' / RAY_ADDRESS.
    address = args.ray_address or os.environ.get("RAY_ADDRESS") or "auto"
    try:
        ray.init(address=address, ignore_reinit_error=True, log_to_driver=False,
                 configure_logging=False)
    except Exception as e:  # noqa: BLE001
        payload = _ray_remote_base_payload(args, "skip", slurm, run_id)
        payload["skip_reasons"] = ["ray_cluster_connect"]
        payload["error"] = f"{type(e).__name__}: {e}"
        print(f"SKIP ray-remote: could not connect to a running Ray cluster at '{address}'. No "
              "local-actor fallback; nothing measured, no curated pass written.")
        return _finish_arm(payload, "skip", run_id, "ray", args)

    error = None
    raw_ns, correct, prewarm_result = [], 0, None
    off_node_count = 0
    actor = None
    target_node = driver_node = None
    expected = None
    node_ids = hostnames = ips = None
    driver_ip = target_ip = node_pair = None
    ips_on_subnet = False
    try:
        alive = [n for n in ray.nodes() if n.get("Alive")]
        node_ids = [n.get("NodeID") for n in alive]
        hostnames = [n.get("NodeManagerHostname") for n in alive]
        ips = [n.get("NodeManagerAddress") for n in alive]
        driver_node = ray.get_runtime_context().get_node_id()
        if not ray_cluster_two_node(node_ids):
            ray.shutdown()
            payload = _ray_remote_base_payload(args, "skip", slurm, run_id)
            payload["skip_reasons"] = ["ray_cluster_two_node"]
            payload.update({"ray_version": getattr(ray, "__version__", None),
                            "ray_node_ids": node_ids, "ray_hostnames": hostnames, "ray_ips": ips,
                            "ray_driver_node_id": driver_node})
            print("SKIP ray-remote: connected Ray cluster has < 2 distinct alive nodes. No "
                  "single-node fallback; nothing measured, no curated pass written.")
            return _finish_arm(payload, "skip", run_id, "ray", args)

        target_node = pick_remote_node(node_ids, driver_node)
        # node-id -> hostname / ip maps for normalized node_pair + the selected-subnet gate
        host_by_id = {n.get("NodeID"): n.get("NodeManagerHostname") for n in alive}
        ip_by_id = {n.get("NodeID"): n.get("NodeManagerAddress") for n in alive}
        driver_ip = ip_by_id.get(driver_node)
        target_ip = ip_by_id.get(target_node)
        subnet = args.prefer_subnet or ""
        ips_on_subnet = bool(subnet) and bool(driver_ip) and bool(target_ip) \
            and driver_ip.startswith(subnet) and target_ip.startswith(subnet)
        node_pair = [_short_host(host_by_id.get(driver_node)), _short_host(host_by_id.get(target_node))]
        node_tag = args.ray_node_tag
        expected = oracle(args.x, node_tag)

        @ray.remote(num_cpus=1)
        class DistProbe:
            def __init__(self, tag):
                self.tag = int(tag)

            def dist_probe(self, xx):
                return (int(xx) ^ 0x52415958) + (self.tag << 1)

            def where(self):
                ctx = ray.get_runtime_context()
                import socket
                return {"node_id": ctx.get_node_id(), "hostname": socket.gethostname()}

        strategy = NodeAffinitySchedulingStrategy(node_id=target_node, soft=False)
        actor = DistProbe.options(scheduling_strategy=strategy).remote(node_tag)

        # verify the actor is genuinely OFF the driver node before timing anything.
        for _ in range(min(5, max(1, args.k))):
            placed = ray.get(actor.where.remote())
            if placed["node_id"] == target_node and placed["node_id"] != driver_node:
                off_node_count += 1
        if off_node_count == 0:
            raise RuntimeError("actor placement not honored off the driver node")

        call = lambda xx: ray.get(actor.dist_probe.remote(xx))  # noqa: E731 (the timed boundary)
        raw_ns, correct, prewarm_result = timed_qd1(call, args.x, expected, args.k, args.w)
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
    finally:
        try:
            if actor is not None:
                ray.kill(actor)
        except Exception:  # noqa: BLE001
            pass
        try:
            ray.shutdown()
        except Exception as e:  # noqa: BLE001
            error = error or f"shutdown: {type(e).__name__}: {e}"

    oracle_correct = (args.k > 0 and correct == args.k and error is None)
    st = _stats(raw_ns)
    gates = {
        "slurm_two_node": True,
        "ray_cluster_two_node": True,
        "actor_off_driver_node": off_node_count > 0,
        "driver_target_ips_on_selected_subnet": ips_on_subnet,
        "prewarm_correct": (prewarm_result == expected) if expected is not None else False,
        "every_call_oracle_correct": oracle_correct,
        "no_error": error is None,
    }
    overall = "pass" if all(gates.values()) else "fail"

    payload = _ray_remote_base_payload(args, overall, slurm, run_id)
    payload.update({
        "two_node_exercised": True,
        "ray_version": getattr(ray, "__version__", None),
        "ray_node_ids": node_ids, "ray_hostnames": hostnames, "ray_ips": ips,
        "ray_driver_node_id": driver_node, "ray_target_node_id": target_node,
        "ray_off_node_sample_count": off_node_count,
        "ray_driver_ip": driver_ip, "ray_target_ip": target_ip,
        "ray_ips_on_selected_subnet": ips_on_subnet,
        "oracle_expected": expected, "ray_oracle_correct": oracle_correct,
        "ray_p50_ns": st.get("p50_ns"), "ray_p90_ns": st.get("p90_ns"),
        "ray_p99_ns": st.get("p99_ns"), "ray_mean_ns": st.get("mean_ns"),
        # Slice 3 matched-structure: normalized ordered node_pair (driver -> actor/target).
        "node_pair": node_pair, "selected_subnet": args.prefer_subnet,
        "slurm_job_id": slurm.get("slurm_job_id"),
        "boundary_call_ns_observation_only": st, "gates": gates, "error": error,
    })
    return _finish_arm(payload, overall, run_id, "ray", args)


# ---------------------------------------------------------------------------------------------------
# phase: pair-manifest (Slice 3) -- correlate the two pair-scoped arm artifacts into a matched-run
# STRUCTURE manifest. This is NOT a same-axis comparison; it never relates the two arms' timings.
# ---------------------------------------------------------------------------------------------------
MANIFEST_ARM_KEYS = ("phase", "overall", "two_node_exercised", "matched_run_id", "slurm_job_id",
                     "node_pair", "selected_subnet", "k", "w", "prewarm", "clock",
                     "measurement_boundary", "hpx_oracle_correct", "ray_oracle_correct",
                     "boundary_call_ns_observation_only")
# Slice 5 same-node manifest arm fields: node_single + placement_mode + the HPX-arm witnesses
# (TCP_NODELAY + disjoint core binding) carried for provenance.
SAME_NODE_MANIFEST_ARM_KEYS = ("phase", "overall", "same_node_exercised", "placement_mode",
                               "matched_run_id", "slurm_job_id", "node_single", "selected_subnet",
                               "k", "w", "prewarm", "clock", "measurement_boundary",
                               "hpx_oracle_correct", "ray_oracle_correct",
                               "hpx_tcp_nodelay_verified", "disjoint_core_binding_verified",
                               "hpx_root_cpuset_effective", "hpx_root_affinity_enforced",
                               "hpx_connector_cpuset_effective", "hpx_connector_affinity_enforced",
                               "boundary_call_ns_observation_only")


def phase_pair_manifest(args):
    mid = getattr(args, "matched_run_id", None)
    if not mid:
        print("SKIP pair-manifest: --matched-run-id is required.")
        return None, "skip"
    hpx_path = os.path.join(HERE, f"slice3_{mid}_hpx.json")
    ray_path = os.path.join(HERE, f"slice3_{mid}_ray.json")
    hpx = _read_json(hpx_path)
    ray = _read_json(ray_path)
    missing = [n for n, d in (("hpx", hpx), ("ray", ray)) if d is None]
    if missing:
        print(f"SKIP pair-manifest: missing pair-scoped artifact(s) {missing} for "
              f"matched_run_id={mid}. Run BOTH arms with --matched-run-id {mid} first; no manifest "
              "is written and no success is claimed.")
        return None, "skip"

    ok, checks = matched_invariants(hpx, ray)
    overall = "pass" if ok else "fail"
    shared = {k: hpx.get(k) for k in MATCHED_INVARIANT_KEYS} if ok else None

    manifest = {
        "experiment_id": "exp61",
        "phase": "pair-manifest",
        "matched_run_id": mid,
        "overall": overall,
        "matched_structure_validated": ok,
        # the WHOLE point: structure-only, NEVER a comparison.
        "same_axis_comparison": False,
        "same_axis_claim_allowed": False,
        "speedup_computed": False,
        "ratio_reported": False,
        "comparison_kind": "matched_run_structure_proof_only",
        "checks": checks,
        "shared_matched_invariants": shared,
        "hpx_arm": {k: hpx.get(k) for k in MANIFEST_ARM_KEYS},
        "ray_arm": {k: ray.get(k) for k in MANIFEST_ARM_KEYS},
        "hpx_arm_artifact": os.path.basename(hpx_path),
        "ray_arm_artifact": os.path.basename(ray_path),
        "interpretation": ("Slice 3 matched-run STRUCTURE proof: both arms ran in the same "
                           "allocation / node pair / selected subnet / K-W-prewarm / Python caller "
                           "boundary, and each passed its OWN oracle. The per-arm boundary times are "
                           "carried for provenance only and are NEVER differenced or ratioed. A "
                           "same-axis band (R>=5) with explicit same-axis gates is a LATER step."),
        "forbidden_claims": FORBIDDEN_CLAIMS,
        "not_a_same_axis_comparison": True,
        "run_id": time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}",
    }
    path = os.path.join(HERE, f"slice3_{mid}_manifest.json")
    _atomic_write(path, manifest)
    os.makedirs(EXP61_RUNS, exist_ok=True)
    with open(os.path.join(EXP61_RUNS, f"pair-manifest_{mid}_{manifest['run_id']}.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    manifest["written"] = os.path.basename(path)
    if not ok:
        failed = [k for k, v in checks.items() if not v]
        print(f"pair-manifest: matched_structure_validated=FALSE for {mid}; failed checks: {failed}")
    return manifest, overall


# ---------------------------------------------------------------------------------------------------
# phase: band-aggregate (Slice 4) -- the FIRST real same-axis band. Consumes R>=5 matched-run
# manifests + their pair-scoped arm artifacts, gates them, and summarizes EACH ARM SEPARATELY across
# islands. It NEVER subtracts arms, NEVER ratios, NEVER computes speedup. same_axis_comparison flips
# to true ONLY when every Slice-4 gate passes.
# ---------------------------------------------------------------------------------------------------
BAND_COMPARISON_KIND = "r5_matched_same_axis_band_no_ratio"
SAME_NODE_BAND_COMPARISON_KIND = "r5_matched_same_node_band_no_ratio"


def _clock_overhead(n=4096):
    """Back-to-back perf_counter_ns deltas in the aggregator process: the clock's own read overhead,
    recorded so the band carries its measurement-plane provenance. Observation-only, never a claim."""
    samples = []
    for _ in range(n):
        a = time.perf_counter_ns()
        b = time.perf_counter_ns()
        samples.append(b - a)
    s = sorted(samples)
    return {"clock": "perf_counter_ns", "count": len(s), "min_ns": s[0],
            "median_ns": _median(s), "mean_ns": int(sum(s) / len(s))}


def _load_band_inputs(ids):
    """IO: for each matched_run_id, read its manifest + both pair-scoped arm artifacts from HERE.
    Missing files come back as None (a gate failure, never a silent pass)."""
    inputs = []
    for mid in ids:
        inputs.append({
            "matched_run_id": mid,
            "manifest": _read_json(os.path.join(HERE, f"slice3_{mid}_manifest.json")),
            "hpx": _read_json(os.path.join(HERE, f"slice3_{mid}_hpx.json")),
            "ray": _read_json(os.path.join(HERE, f"slice3_{mid}_ray.json")),
        })
    return inputs


def _checks_all_true(m):
    c = (m or {}).get("checks")
    return isinstance(c, dict) and len(c) > 0 and all(bool(v) for v in c.values())


def _arm_band(inputs, arm):
    """Per-arm band for ONE arm only. Carries each island's p50/p90/p99/mean and the across-island
    median/min/max/spread. `differenced_with_other_arm` is hard-coded False as a structural witness."""
    per_island = []
    cols = {"p50_ns": [], "p90_ns": [], "p99_ns": [], "mean_ns": []}
    for it in inputs:
        b = ((it.get(arm) or {}).get("boundary_call_ns_observation_only") or {})
        row = {"matched_run_id": it["matched_run_id"]}
        for key in cols:
            row[key] = b.get(key)
            cols[key].append(b.get(key))
        per_island.append(row)
    return {
        "arm": arm,
        "per_island": per_island,
        "across_island": {key: _across(vals) for key, vals in cols.items()},
        "differenced_with_other_arm": False,
    }


def build_band_aggregate(inputs, clock_overhead, min_r=5, placement_mode="cross_node"):
    """PURE. inputs is [{matched_run_id, manifest, hpx, ray}] (any value may be None for a missing
    artifact). Returns (payload, overall). same_axis_comparison is true ONLY if every gate passes;
    speedup_computed / ratio_reported / arms_differenced / placement_bands_differenced stay False
    unconditionally.

    placement_mode='cross_node' (default, Slice 4) is byte-for-byte the original cross-node band.
    placement_mode='same_node' (Slice 5) gates same_node_exercised plus the HPX-arm TCP_NODELAY and
    disjoint-core-binding witnesses, and agrees on node_single instead of node_pair. The same-node
    band is a WITHIN-ARM placement control: it is never differenced against the cross-node band."""
    same = placement_mode == "same_node"
    island_keys = SAME_NODE_CROSS_ISLAND_KEYS if same else CROSS_ISLAND_KEYS
    per_island_gates = {}
    canon_list = []
    for it in inputs:
        mid = it["matched_run_id"]
        m, h, r = it.get("manifest"), it.get("hpx"), it.get("ray")
        g = {
            "manifest_present": m is not None,
            "hpx_present": h is not None,
            "ray_present": r is not None,
            "manifest_pass": bool(m) and m.get("overall") == "pass",
            "structure_validated": bool(m) and m.get("matched_structure_validated") is True,
            "checks_all_true": _checks_all_true(m),
            "hpx_pass": bool(h) and h.get("overall") == "pass",
            "ray_pass": bool(r) and r.get("overall") == "pass",
            "hpx_oracle_true": bool(h) and h.get("hpx_oracle_correct") is True,
            "ray_oracle_true": bool(r) and r.get("ray_oracle_correct") is True,
        }
        if same:
            g["hpx_same_node"] = bool(h) and h.get("same_node_exercised") is True
            g["ray_same_node"] = bool(r) and r.get("same_node_exercised") is True
            # HPX-expert-required, FAIL-CLOSED: absent/false keeps the band from passing.
            g["hpx_nodelay_verified"] = bool(h) and h.get("hpx_tcp_nodelay_verified") is True
            # STRICT: enforced+effective disjoint affinity, NOT requested-only recorded metadata.
            g["hpx_disjoint_core_binding"] = bool(h) and \
                h.get("disjoint_core_binding_verified") is True
        else:
            g["hpx_two_node"] = bool(h) and h.get("two_node_exercised") is True
            g["ray_two_node"] = bool(r) and r.get("two_node_exercised") is True
        per_island_gates[mid] = g
        src = (m or {}).get("shared_matched_invariants") or h or r or {}
        canon_list.append((mid, {k: _canon(src.get(k)) for k in island_keys}))

    def _all(key):
        return bool(per_island_gates) and all(g.get(key) for g in per_island_gates.values())

    agree, disagreements = _cross_island_agreement(canon_list, keys=island_keys)
    gates = {
        "r_at_least_5": len(inputs) >= min_r,
        "all_islands_present": _all("manifest_present") and _all("hpx_present")
        and _all("ray_present"),
        "all_manifests_pass": _all("manifest_pass"),
        "all_structure_validated": _all("structure_validated"),
        "all_island_checks_true": _all("checks_all_true"),
        "all_arms_pass": _all("hpx_pass") and _all("ray_pass"),
        "all_hpx_oracle_true": _all("hpx_oracle_true"),
        "all_ray_oracle_true": _all("ray_oracle_true"),
        "cross_island_agreement": agree,
        "clock_overhead_captured": bool(clock_overhead) and clock_overhead.get("count", 0) > 0,
    }
    if same:
        gates["all_arms_same_node"] = _all("hpx_same_node") and _all("ray_same_node")
        gates["all_hpx_nodelay_verified"] = _all("hpx_nodelay_verified")
        gates["all_hpx_disjoint_core_binding"] = _all("hpx_disjoint_core_binding")
    else:
        gates["all_arms_two_node"] = _all("hpx_two_node") and _all("ray_two_node")
    same_axis = all(gates.values())
    overall = "pass" if same_axis else "fail"
    shared = _shared_from_canon(canon_list, keys=island_keys) if agree else None
    band_id = (f"band_{shared['slurm_job_id']}"
               if shared and shared.get("slurm_job_id") else None)
    comparison_kind = SAME_NODE_BAND_COMPARISON_KIND if same else BAND_COMPARISON_KIND
    interp = ("Slice 5 R>=5 matched SAME-NODE control band: every island ran both arms in one "
              "allocation on ONE node (node_single) / subnet / K-W-prewarm / Python boundary, each "
              "passing its own oracle, with the HPX arm over two DISTINCT localities on the TCP "
              "parcelport (loopback) and TCP_NODELAY + disjoint-core-binding witnessed. Per-arm "
              "p50/p90/p99 are summarized across islands SEPARATELY. Same-node vs cross-node is a "
              "WITHIN-ARM placement control only: the arms are never differenced or ratioed, and the "
              "same-node band is never differenced against the cross-node band.") if same else (
              "Slice 4 R>=5 matched same-axis band: every island ran both arms in one "
              "allocation / node pair / subnet / K-W-prewarm / Python boundary, each "
              "passing its own oracle. Per-arm p50/p90/p99 are summarized across islands "
              "SEPARATELY. The arms are never differenced or ratioed and no speedup is "
              "computed.")

    payload = {
        "experiment_id": "exp61",
        "phase": "band-aggregate-samenode" if same else "band-aggregate",
        "placement_mode": placement_mode,
        "band_id": band_id,
        "matched_run_ids": [it["matched_run_id"] for it in inputs],
        "R": len(inputs),
        "min_r": min_r,
        "overall": overall,
        # The whole point: a same-axis band is allowed ONLY when every gate passes -- and even then
        # the arms are reported side by side, NEVER subtracted or ratioed.
        "same_axis_comparison": same_axis,
        "same_axis_claim_allowed": same_axis,
        "speedup_computed": False,
        "ratio_reported": False,
        "arms_differenced": False,
        # the same-node band is NEVER differenced/ratioed against the cross-node band either.
        "placement_bands_differenced": False,
        "comparison_kind": comparison_kind,
        "gates": gates,
        "per_island_gates": per_island_gates,
        "cross_island_disagreements": disagreements or None,
        "shared_invariants": shared,
        "clock_overhead_perf_counter_ns": clock_overhead,
        # arms are two fully separate sub-trees; nothing here relates one arm's numbers to the other.
        "arms": {"hpx": _arm_band(inputs, "hpx"), "ray": _arm_band(inputs, "ray")},
        "interpretation": interp,
        "forbidden_claims": FORBIDDEN_CLAIMS,
    }
    return payload, overall


def phase_band_aggregate(args):
    ids = [s.strip() for s in (getattr(args, "matched_run_ids", None) or "").split(",") if s.strip()]
    if not ids:
        print("SKIP band-aggregate: --matched-run-ids id1,...,idR (R>=5) is required.")
        return None, "skip"
    inputs = _load_band_inputs(ids)
    payload, overall = build_band_aggregate(inputs, _clock_overhead(), min_r=5)
    payload["run_id"] = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"

    out = getattr(args, "band_out", None) or (
        f"slice4_{payload['band_id']}_aggregate.json" if payload["band_id"]
        else "slice4_band_unknown_aggregate.json")
    path = out if os.path.isabs(out) else os.path.join(HERE, out)
    _atomic_write(path, payload)
    os.makedirs(EXP61_RUNS, exist_ok=True)
    with open(os.path.join(EXP61_RUNS, f"band-aggregate_{payload['run_id']}.json"), "w") as f:
        json.dump(payload, f, indent=2)
    payload["written"] = os.path.basename(path)
    if overall != "pass":
        failed = [k for k, v in payload["gates"].items() if not v]
        print(f"band-aggregate: same_axis_comparison=FALSE; failed gates: {failed}")
    return payload, overall


# ---------------------------------------------------------------------------------------------------
# phase: manifest-summary (Slice 3R) -- orchestration-stability summary over repeated matched smokes.
# STRUCTURE ONLY: it confirms each repeat passed and that the repeats agree; it NEVER touches timing.
# same_axis_comparison is hard-locked False. It reuses the band loader and agreement helpers.
# ---------------------------------------------------------------------------------------------------
def build_manifest_summary(inputs):
    """PURE. Confirms each repeat's manifest passed and validated, the ids are distinct, and the
    repeats agree on allocation/placement/K-W-prewarm/boundary. No timing is ever compared."""
    per_run = {}
    for it in inputs:
        m = it.get("manifest")
        per_run[it["matched_run_id"]] = {
            "present": m is not None,
            "overall_pass": bool(m) and m.get("overall") == "pass",
            "structure_validated": bool(m) and m.get("matched_structure_validated") is True,
            "checks_all_true": _checks_all_true(m),
        }
    ids = [it["matched_run_id"] for it in inputs]
    distinct = len(set(ids)) == len(ids)
    canon_list = [(it["matched_run_id"],
                   {k: _canon(((it.get("manifest") or {}).get("shared_matched_invariants") or {})
                              .get(k)) for k in CROSS_ISLAND_KEYS}) for it in inputs]
    agree, disagreements = _cross_island_agreement(canon_list)
    all_ok = bool(per_run) and all(all(g.values()) for g in per_run.values())
    overall = "pass" if (all_ok and distinct and agree) else "fail"
    return {
        "experiment_id": "exp61",
        "phase": "manifest-summary",
        "purpose": "orchestration_stability_structure_only",
        "matched_run_ids": ids,
        "R": len(inputs),
        "overall": overall,
        # structure-stability only -- NEVER a comparison, regardless of outcome.
        "same_axis_comparison": False,
        "speedup_computed": False,
        "ratio_reported": False,
        "timing_compared": False,
        "comparison_kind": "matched_run_structure_stability_only",
        "distinct_matched_run_ids": distinct,
        "all_manifests_pass_and_validated": all_ok,
        "cross_run_agreement": agree,
        "cross_run_disagreements": disagreements or None,
        "per_run": per_run,
        "shared_invariants": _shared_from_canon(canon_list) if agree else None,
        "forbidden_claims": FORBIDDEN_CLAIMS,
    }


def phase_manifest_summary(args):
    ids = [s.strip() for s in (getattr(args, "matched_run_ids", None) or "").split(",") if s.strip()]
    if not ids:
        print("SKIP manifest-summary: --matched-run-ids id1,id2,... is required.")
        return None, "skip"
    summary = build_manifest_summary(_load_band_inputs(ids))
    summary["run_id"] = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"
    shared = summary.get("shared_invariants") or {}
    out = getattr(args, "summary_out", None) or (
        f"slice3r_{shared['slurm_job_id']}_summary.json" if shared.get("slurm_job_id")
        else "slice3r_unknown_summary.json")
    path = out if os.path.isabs(out) else os.path.join(HERE, out)
    _atomic_write(path, summary)
    summary["written"] = os.path.basename(path)
    if summary["overall"] != "pass":
        print(f"manifest-summary: overall=FALSE (distinct={summary['distinct_matched_run_ids']}, "
              f"agree={summary['cross_run_agreement']}, "
              f"all_ok={summary['all_manifests_pass_and_validated']})")
    return summary, summary["overall"]


# ---------------------------------------------------------------------------------------------------
# phase: selftest (pure Python; no extension, no Ray, no HPX)
# ---------------------------------------------------------------------------------------------------
def phase_selftest():
    checks = []

    def chk(name, cond):
        checks.append((name, bool(cond)))

    chk("oracle_x7_tag0", oracle(7, 0) == ((7 ^ 0x52415958) + 0))
    chk("oracle_x7_tag1", oracle(7, 1) == ((7 ^ 0x52415958) + 2))
    chk("oracle_x0_tag0", oracle(0, 0) == 0x52415958)

    chk("pct_nearest_rank", _pct(list(range(1, 101)), 0.50) == 50
        and _pct(list(range(1, 101)), 0.99) == 99)
    st = _stats(list(range(1, 1001)))
    chk("stats_basic", st["count"] == 1000 and st["min_ns"] == 1 and st["p50_ns"] == 500)

    # shared timing harness: same function drives any callable identically
    counter = {"n": 0}
    def fake(xx):
        counter["n"] += 1
        return oracle(xx, 0)
    raw, correct, pre = timed_qd1(fake, 7, oracle(7, 0), k=5, w=3)
    chk("timed_qd1_counts", len(raw) == 5 and correct == 5 and pre == oracle(7, 0)
        and counter["n"] == 1 + 3 + 5)  # prewarm + W + K

    all_phases = ("hpx-smoke", "ray-smoke", "hpx-connected", "ray-remote")
    for phase in all_phases:
        f = _fence_fields(phase, "pass", 1000, 100, 1)
        required = ("experiment_id", "phase", "overall", "same_axis_comparison",
                    "same_axis_claim_allowed", "speedup_computed", "ratio_reported",
                    "comparison_kind", "measurement_boundary", "clock", "k", "w", "prewarm",
                    "qd1_only", "pipeline_excluded_from_comparison", "forbidden_claims",
                    "shared_harness")
        chk(f"fence_fields_present[{phase}]", all(k in f for k in required))
        chk(f"fence_values_locked[{phase}]", f["same_axis_comparison"] is False
            and f["same_axis_claim_allowed"] is False and f["speedup_computed"] is False
            and f["ratio_reported"] is False and f["qd1_only"] is True
            and f["pipeline_excluded_from_comparison"] is True
            and f["measurement_boundary"] == MEASUREMENT_BOUNDARY
            and f["clock"] == "perf_counter_ns"
            and f["shared_harness"]["measurement_boundary"] == MEASUREMENT_BOUNDARY)

    # ALL four arms share the SAME boundary string (proves identical harness, not a comparison)
    boundaries = {_fence_fields(p, "pass", 1, 1, 1)["measurement_boundary"] for p in all_phases}
    chk("all_arms_same_boundary", len(boundaries) == 1 and boundaries.pop() == MEASUREMENT_BOUNDARY)
    fh = _fence_fields("hpx-smoke", "pass", 1, 1, 1)
    chk("forbidden_claims_nonempty", isinstance(fh["forbidden_claims"], list)
        and len(fh["forbidden_claims"]) >= 4)

    import tempfile
    tmp = os.path.join(tempfile.mkdtemp(prefix="exp61_selftest_"), "agg.json")
    _atomic_write(tmp, {"overall": "pass", "phase": "ray-smoke"})
    _, refused = safe_write_top_aggregate(tmp, {"overall": "skip", "phase": "ray-smoke"})
    chk("skip_never_overwrites_pass", refused and _read_json(tmp)["overall"] == "pass")

    # --- Slice 2A/2B: two-node gate helpers (pure) ---
    def _resolver(nl):
        # toy expander: "n[1-2]"/"medusa[00-01]" -> 2 hosts; "n1"/"medusa00" -> 1 host
        return ["x", "y"] if "[" in (nl or "") else ([nl] if nl else [])
    off_cluster = slurm_two_node_from_env({}, lambda nl: ["a", "b"])
    chk("slurm_off_cluster_not_two_node", off_cluster["two_node"] is False)
    one_node = slurm_two_node_from_env(
        {"SLURM_JOB_ID": "1", "SLURM_JOB_NODELIST": "n1", "SLURM_JOB_NUM_NODES": "1"},
        lambda nl: ["n1"])
    chk("slurm_one_node_not_two_node", one_node["two_node"] is False)
    two_node = slurm_two_node_from_env(
        {"SLURM_JOB_ID": "1", "SLURM_JOB_NODELIST": "n[1-2]", "SLURM_JOB_NUM_NODES": "2"},
        lambda nl: ["n1", "n2"])
    chk("slurm_two_node_detected", two_node["two_node"] is True
        and two_node["hostnames"] == ["n1", "n2"])

    # Patch A (Bug A): a 1-node srun STEP of a 2-node job exposes step-scoped JOB keys, but the
    # allocation is still visible via SLURM_NODELIST -- must NOT false-skip.
    step_scoped = slurm_two_node_from_env(
        {"SLURM_JOB_ID": "1", "SLURM_JOB_NODELIST": "medusa00", "SLURM_NNODES": "1",
         "SLURM_NODELIST": "medusa[00-01]"}, _resolver)
    chk("slurm_step_scoped_still_two_node", step_scoped["two_node"] is True
        and step_scoped["slurm_num_nodes"] == 2 and step_scoped["alloc_source"] == "env_alt")
    # Patch A: authoritative scontrol-show-job allocation wins even if ALL env is step-scoped to 1.
    job_auth = slurm_two_node_from_env(
        {"SLURM_JOB_ID": "9", "SLURM_JOB_NODELIST": "medusa00", "SLURM_NNODES": "1",
         "SLURM_NODELIST": "medusa00"}, _resolver,
        job_resolver=lambda jid: (2, "medusa[00-01]"))
    chk("slurm_job_resolver_authoritative", job_auth["two_node"] is True
        and job_auth["slurm_num_nodes"] == 2 and job_auth["alloc_source"] == "scontrol_job")
    # off-cluster with a (defensive) resolver present still must not fabricate two-node
    chk("slurm_off_cluster_resolver_safe",
        slurm_two_node_from_env({}, _resolver, lambda jid: (5, "x[0-4]"))["two_node"] is False)

    # no local fallback masquerading as remote: a single Ray node id is NOT a two-node cluster
    chk("ray_one_node_not_two_node", ray_cluster_two_node(["A"]) is False)
    chk("ray_dupe_node_not_two_node", ray_cluster_two_node(["A", "A"]) is False)
    chk("ray_two_node_detected", ray_cluster_two_node(["A", "B"]) is True)
    chk("pick_remote_skips_driver", pick_remote_node(["A", "B"], "A") == "B"
        and pick_remote_node(["A"], "A") is None)

    # skip payloads carry the required provenance fields, marked NOT exercised, fences false
    class _A:  # minimal args stand-in
        k = 10; w = 2; x = 7; ray_node_tag = 0
    skip_slurm = {"slurm_job_id": None, "slurm_job_nodelist": None, "slurm_num_nodes": 0,
                  "two_node": False}
    cp = _connected_base_payload(_A, "skip", skip_slurm, None, None, "rid")
    conn_required = ("phase", "hpx_connector_used", "measurement_boundary_hpx",
                     "hpx_extension_experiment_only", "hpx_root_hostname", "hpx_connector_hostname",
                     "hpx_root_ip", "hpx_connector_ip", "hpx_root_locality_id",
                     "hpx_remote_locality_id", "hpx_here_locality_differs_from_remote",
                     "hpx_parcelport", "hpx_tcp_nodelay_verified", "hpx_action_registration_name",
                     "hpx_oracle_correct", "qd1_only", "pipeline_excluded_from_comparison",
                     "forbidden_claims")
    chk("connected_skip_fields_present", all(k in cp for k in conn_required))
    chk("connected_skip_not_exercised", cp["two_node_exercised"] is False
        and cp["hpx_connector_used"] is True and cp["hpx_parcelport"] == "tcp"
        and cp["same_axis_comparison"] is False and cp["hpx_oracle_correct"] is False)
    rp = _ray_remote_base_payload(_A, "skip", skip_slurm, "rid")
    ray_required = ("phase", "measurement_boundary_ray", "ray_call_primitive",
                    "ray_placement_strategy", "ray_node_ids", "ray_hostnames", "ray_ips",
                    "ray_actor_remote_only", "ray_off_node_sample_count", "ray_oracle_correct",
                    "qd1_only", "pipeline_excluded_from_comparison", "forbidden_claims")
    chk("ray_remote_skip_fields_present", all(k in rp for k in ray_required))
    chk("ray_remote_skip_not_exercised", rp["two_node_exercised"] is False
        and rp["ray_actor_remote_only"] is True and rp["ray_off_node_sample_count"] == 0
        and rp["ray_placement_strategy"] == "NodeAffinitySchedulingStrategy(soft=False)"
        and rp["same_axis_comparison"] is False and rp["ray_oracle_correct"] is False)

    # _finish_scaffold: a skip/fail NEVER writes the curated base name
    sd = tempfile.mkdtemp(prefix="exp61_scaffold_")
    saved_runs, saved_top = EXP61_RUNS, dict(TOP_AGGREGATES)
    try:
        globals()["EXP61_RUNS"] = sd
        TOP_AGGREGATES["hpx-connected"] = os.path.join(sd, "hpx_connected_aggregate.json")
        sp = _connected_base_payload(_A, "skip", skip_slurm, None, None, "rid")
        _finish_scaffold(sp, "skip", "rid", False)
        curated = os.path.join(sd, "hpx_connected_aggregate.json")
        sib = os.path.join(sd, "hpx_connected_aggregate_skip.json")
        chk("scaffold_skip_not_curated", (not os.path.exists(curated)) and os.path.exists(sib))
    finally:
        globals()["EXP61_RUNS"] = saved_runs
        TOP_AGGREGATES.clear()
        TOP_AGGREGATES.update(saved_top)

    # --- Slice 3: matched-run structure (pure) ---
    chk("short_host_fqdn", _short_host("medusa01.rostam.cct.lsu.edu") == "medusa01")
    chk("short_host_plain", _short_host("medusa00") == "medusa00" and _short_host(None) is None)

    def _arm(phase, **over):
        d = {"phase": phase, "overall": "pass", "two_node_exercised": True,
             "matched_run_id": "pairX", "slurm_job_id": "42",
             "node_pair": ["medusa00", "medusa01"], "selected_subnet": "10.42.5.",
             "k": 50, "w": 10, "prewarm": 1, "clock": "perf_counter_ns",
             "measurement_boundary": MEASUREMENT_BOUNDARY}
        d["hpx_oracle_correct" if phase == "hpx-connected" else "ray_oracle_correct"] = True
        d.update(over)
        return d

    chk("matched_invariants_success",
        matched_invariants(_arm("hpx-connected"), _arm("ray-remote"))[0] is True)
    chk("matched_fail_job", matched_invariants(
        _arm("hpx-connected", slurm_job_id="42"), _arm("ray-remote", slurm_job_id="99"))[0] is False)
    chk("matched_fail_node_pair", matched_invariants(
        _arm("hpx-connected"), _arm("ray-remote", node_pair=["medusa00", "medusa02"]))[0] is False)
    chk("matched_fail_k", matched_invariants(
        _arm("hpx-connected", k=50), _arm("ray-remote", k=200))[0] is False)
    chk("matched_fail_w", matched_invariants(
        _arm("hpx-connected", w=10), _arm("ray-remote", w=50))[0] is False)
    chk("matched_fail_subnet", matched_invariants(
        _arm("hpx-connected"), _arm("ray-remote", selected_subnet="10.42.6."))[0] is False)
    chk("matched_fail_boundary", matched_invariants(
        _arm("hpx-connected"), _arm("ray-remote", measurement_boundary="other"))[0] is False)
    chk("matched_fail_hpx_oracle", matched_invariants(
        _arm("hpx-connected", hpx_oracle_correct=False), _arm("ray-remote"))[0] is False)
    chk("matched_fail_not_pass", matched_invariants(
        _arm("hpx-connected", overall="fail"), _arm("ray-remote"))[0] is False)

    # both arm base payloads now carry the shared matched-structure fields
    bp_hpx = _connected_base_payload(_A, "skip", skip_slurm, None, None, "rid")
    bp_ray = _ray_remote_base_payload(_A, "skip", skip_slurm, "rid")
    shared_keys = ("matched_run_id", "selected_subnet", "node_pair", "slurm_job_id")
    chk("slice3_shared_fields_present",
        all(k in bp_hpx for k in shared_keys) and all(k in bp_ray for k in shared_keys)
        and "ray_ips_on_selected_subnet" in bp_ray)

    # _finish_matched writes the PAIR-SCOPED name, never a curated base aggregate
    sd2 = tempfile.mkdtemp(prefix="exp61_matched_")
    saved_here2, saved_runs2 = HERE, EXP61_RUNS
    try:
        globals()["HERE"] = sd2
        globals()["EXP61_RUNS"] = sd2
        _finish_matched({"phase": "hpx-connected", "matched_run_id": "pairZ"}, "pass", "rid", "hpx")
        pair_path = os.path.join(sd2, "slice3_pairZ_hpx.json")
        curated = os.path.join(sd2, "hpx_connected_aggregate.json")
        chk("matched_writes_pair_scoped_not_curated",
            os.path.exists(pair_path) and not os.path.exists(curated))
    finally:
        globals()["HERE"] = saved_here2
        globals()["EXP61_RUNS"] = saved_runs2

    # --- Slice 4: band-aggregate gates (pure, in-memory; NO fixtures touch disk) ---
    def _b_inv(**over):
        inv = {"slurm_job_id": "158722", "node_pair": ["medusa00", "medusa01"],
               "selected_subnet": "10.42.5.", "k": 1000, "w": 100, "prewarm": 1,
               "clock": "perf_counter_ns", "measurement_boundary": MEASUREMENT_BOUNDARY}
        inv.update(over)
        return inv

    def _b_island(mid, inv_over=None, hpx_over=None, ray_over=None, drop_arm=None):
        inv = _b_inv(**(inv_over or {}))
        manifest = {"overall": "pass", "matched_structure_validated": True,
                    "checks": {"all_invariants": True}, "shared_matched_invariants": inv}
        boundary = {"count": 1000, "min_ns": 400_000, "mean_ns": 600_000, "p50_ns": 540_000,
                    "p90_ns": 950_000, "p99_ns": 1_020_000, "max_ns": 1_020_000}
        hpx = {"overall": "pass", "two_node_exercised": True, "hpx_oracle_correct": True,
               "boundary_call_ns_observation_only": dict(boundary)}
        ray = {"overall": "pass", "two_node_exercised": True, "ray_oracle_correct": True,
               "boundary_call_ns_observation_only": dict(boundary)}
        hpx.update(hpx_over or {})
        ray.update(ray_over or {})
        it = {"matched_run_id": mid, "manifest": manifest, "hpx": hpx, "ray": ray}
        if drop_arm:
            it[drop_arm] = None
        return it

    def _b_band(n=5, island0=None):
        islands = [island0 or _b_island("band_158722_i1")]
        islands += [_b_island(f"band_158722_i{i + 1}") for i in range(1, n)]
        return islands

    _b_ov = {"clock": "perf_counter_ns", "count": 8, "min_ns": 1, "median_ns": 1, "mean_ns": 1}

    def _b_agg(inputs):
        return build_band_aggregate(inputs, _b_ov, min_r=5)[0]

    pv = _b_agg(_b_band())
    chk("band_valid_r5_same_axis_true",
        pv["same_axis_comparison"] is True and pv["overall"] == "pass"
        and all(pv["gates"].values()))
    chk("band_speedup_false", pv["speedup_computed"] is False)
    chk("band_ratio_false", pv["ratio_reported"] is False)
    chk("band_arms_differenced_false",
        pv["arms_differenced"] is False
        and pv["arms"]["hpx"]["differenced_with_other_arm"] is False
        and pv["arms"]["ray"]["differenced_with_other_arm"] is False)
    chk("band_arms_summarized_separately",
        set(pv["arms"]) == {"hpx", "ray"}
        and len(pv["arms"]["hpx"]["per_island"]) == 5
        and "across_island" in pv["arms"]["ray"])

    p4 = _b_agg(_b_band(n=4))
    chk("band_r_lt_5_fails",
        p4["same_axis_comparison"] is False and p4["gates"]["r_at_least_5"] is False)

    def _b_mismatch(**inv):
        return _b_agg(_b_band(island0=_b_island("band_158722_i1", inv_over=inv)))

    pj = _b_mismatch(slurm_job_id="999")
    chk("band_fail_job",
        pj["gates"]["cross_island_agreement"] is False and pj["same_axis_comparison"] is False)
    chk("band_fail_node_pair",
        _b_mismatch(node_pair=["medusa00", "medusa02"])["same_axis_comparison"] is False)
    chk("band_fail_subnet",
        _b_mismatch(selected_subnet="10.42.6.")["same_axis_comparison"] is False)
    chk("band_fail_k", _b_mismatch(k=500)["same_axis_comparison"] is False)
    chk("band_fail_w", _b_mismatch(w=50)["same_axis_comparison"] is False)
    chk("band_fail_prewarm", _b_mismatch(prewarm=0)["same_axis_comparison"] is False)
    chk("band_fail_boundary",
        _b_mismatch(measurement_boundary="not_the_python_caller_boundary")["same_axis_comparison"]
        is False)

    pm = _b_agg(_b_band(island0=_b_island("band_158722_i1", drop_arm="hpx")))
    chk("band_fail_missing_arm",
        pm["gates"]["all_islands_present"] is False and pm["same_axis_comparison"] is False)
    po = _b_agg(_b_band(island0=_b_island("band_158722_i1",
                                          hpx_over={"hpx_oracle_correct": False})))
    chk("band_fail_oracle_false",
        po["gates"]["all_hpx_oracle_true"] is False and po["same_axis_comparison"] is False)
    pc = _b_agg(_b_band())  # valid band, but starve the clock-overhead gate
    pc_no_clock = build_band_aggregate(_b_band(), {"count": 0}, min_r=5)[0]
    chk("band_fail_no_clock_overhead",
        pc_no_clock["gates"]["clock_overhead_captured"] is False
        and pc_no_clock["same_axis_comparison"] is False and pc["same_axis_comparison"] is True)

    # REGRESSION GUARD: the default (cross_node) band keeps its two-node gate and NO same-node gates.
    chk("xnode_band_has_two_node_gate",
        "all_arms_two_node" in pv["gates"] and "all_arms_same_node" not in pv["gates"]
        and "all_hpx_nodelay_verified" not in pv["gates"]
        and pv.get("placement_mode") == "cross_node"
        and pv["comparison_kind"] == BAND_COMPARISON_KIND)

    # --- Slice 5: cpuset helpers (pure) ---
    chk("parse_cpuset_range", _parse_cpuset("0-3,8") == {0, 1, 2, 3, 8})
    chk("parse_cpuset_empty", _parse_cpuset("") == set() and _parse_cpuset(None) == set())
    chk("cpusets_disjoint_true", _cpusets_disjoint("0-3", "4-7") is True)
    chk("cpusets_overlap_false", _cpusets_disjoint("0-3", "3-5") is False)
    chk("cpusets_empty_false", _cpusets_disjoint("", "0-3") is False
        and _cpusets_disjoint(None, None) is False)
    chk("cpuset_to_mask", _cpuset_to_mask("4-7") == "0xf0" and _cpuset_to_mask("4") == "0x10"
        and _cpuset_to_mask("0-3") == "0xf" and _cpuset_to_mask("") == "")

    # --- Slice 5: TCP_NODELAY attestation mapping (pure; mirrors the connector getsockopt probe) ---
    nd_ok = {"tcp_nodelay_attested": True, "tcp_nodelay": True, "tcp_nodelay_source": "getsockopt",
             "tcp_nodelay_scope": "hpx_root_parcelport_peer_sockets", "tcp_nodelay_matched_sockets": 2}
    v_ok, m_ok = _nodelay_verified_from_attest(nd_ok)
    chk("nodelay_attest_true_verifies", v_ok is True and "getsockopt" in m_ok)
    # attested socket(s) but NODELAY disabled -> fail-closed
    chk("nodelay_attest_disabled_fails", _nodelay_verified_from_attest(
        {"tcp_nodelay_attested": True, "tcp_nodelay": False})[0] is False)
    # no socket found / not attested -> fail-closed (the enabled flag must not rescue it)
    chk("nodelay_attest_not_attested_fails", _nodelay_verified_from_attest(
        {"tcp_nodelay_attested": False, "tcp_nodelay": True,
         "tcp_nodelay_error": "no established TCP socket to peer found"})[0] is False)
    # missing fields / empty / None attestation -> fail-closed
    chk("nodelay_attest_missing_fails",
        _nodelay_verified_from_attest({})[0] is False
        and _nodelay_verified_from_attest(None)[0] is False)
    chk("nodelay_attest_method_none_when_unverified",
        _nodelay_verified_from_attest({"tcp_nodelay_attested": True})[1] is None)

    # --- Slice 5: same-node matched_invariants (pure) ---
    def _sn_arm(phase, **over):
        d = {"phase": phase, "overall": "pass", "same_node_exercised": True,
             "placement_mode": "same_node", "matched_run_id": "snX", "slurm_job_id": "159900",
             "node_single": "medusa00", "selected_subnet": "10.42.5.", "k": 1000, "w": 100,
             "prewarm": 1, "clock": "perf_counter_ns", "measurement_boundary": MEASUREMENT_BOUNDARY}
        d["hpx_oracle_correct" if phase == "hpx-colocated" else "ray_oracle_correct"] = True
        d.update(over)
        return d

    chk("sn_matched_success", matched_invariants(
        _sn_arm("hpx-colocated"), _sn_arm("ray-colocated"), placement_mode="same_node")[0] is True)
    chk("sn_matched_fail_node_single", matched_invariants(
        _sn_arm("hpx-colocated"), _sn_arm("ray-colocated", node_single="medusa01"),
        placement_mode="same_node")[0] is False)
    chk("sn_matched_fail_placement_mode", matched_invariants(
        _sn_arm("hpx-colocated"), _sn_arm("ray-colocated", placement_mode="cross_node"),
        placement_mode="same_node")[0] is False)
    chk("sn_matched_fail_not_exercised", matched_invariants(
        _sn_arm("hpx-colocated", same_node_exercised=False), _sn_arm("ray-colocated"),
        placement_mode="same_node")[0] is False)
    chk("sn_matched_fail_oracle", matched_invariants(
        _sn_arm("hpx-colocated", hpx_oracle_correct=False), _sn_arm("ray-colocated"),
        placement_mode="same_node")[0] is False)
    # same-node mode does NOT key off node_pair: a node_pair mismatch is irrelevant when matched
    chk("sn_matched_ignores_node_pair", matched_invariants(
        _sn_arm("hpx-colocated", node_pair=["a", "b"]),
        _sn_arm("ray-colocated", node_pair=["c", "d"]), placement_mode="same_node")[0] is True)

    # --- Slice 5: same-node band-aggregate gates (pure, in-memory) ---
    def _sn_inv(**over):
        inv = {"slurm_job_id": "159900", "node_single": "medusa00", "selected_subnet": "10.42.5.",
               "k": 1000, "w": 100, "prewarm": 1, "clock": "perf_counter_ns",
               "measurement_boundary": MEASUREMENT_BOUNDARY, "placement_mode": "same_node"}
        inv.update(over)
        return inv

    def _sn_island(mid, inv_over=None, hpx_over=None, ray_over=None, drop_arm=None, drop_hpx_key=None):
        inv = _sn_inv(**(inv_over or {}))
        manifest = {"overall": "pass", "matched_structure_validated": True,
                    "checks": {"all_invariants": True}, "shared_matched_invariants": inv}
        boundary = {"count": 1000, "min_ns": 150_000, "mean_ns": 190_000, "p50_ns": 185_000,
                    "p90_ns": 258_000, "p99_ns": 322_000, "max_ns": 322_000}
        hpx = {"overall": "pass", "same_node_exercised": True, "hpx_oracle_correct": True,
               "hpx_tcp_nodelay_verified": True,
               # requested-only recorded True AND strict verified True (enforced+effective+disjoint)
               "disjoint_core_binding_recorded": True, "disjoint_core_binding_verified": True,
               "hpx_root_affinity_enforced": True, "hpx_root_cpuset_effective": [0, 1, 2, 3],
               "hpx_connector_affinity_enforced": True, "hpx_connector_cpuset_effective": [4],
               "boundary_call_ns_observation_only": dict(boundary)}
        ray = {"overall": "pass", "same_node_exercised": True, "ray_oracle_correct": True,
               "boundary_call_ns_observation_only": dict(boundary)}
        hpx.update(hpx_over or {})
        ray.update(ray_over or {})
        if drop_hpx_key:
            hpx.pop(drop_hpx_key, None)
        it = {"matched_run_id": mid, "manifest": manifest, "hpx": hpx, "ray": ray}
        if drop_arm:
            it[drop_arm] = None
        return it

    def _sn_band(n=5, island0=None):
        islands = [island0 or _sn_island("sn_159900_i1")]
        islands += [_sn_island(f"sn_159900_i{i + 1}") for i in range(1, n)]
        return islands

    def _sn_agg(inputs):
        return build_band_aggregate(inputs, _b_ov, min_r=5, placement_mode="same_node")[0]

    sv = _sn_agg(_sn_band())
    chk("sn_band_valid_r5_same_axis_true",
        sv["same_axis_comparison"] is True and sv["overall"] == "pass" and all(sv["gates"].values()))
    chk("sn_band_placement_mode_and_kind",
        sv["placement_mode"] == "same_node"
        and sv["comparison_kind"] == SAME_NODE_BAND_COMPARISON_KIND
        and sv["phase"] == "band-aggregate-samenode")
    chk("sn_band_locks_false",
        sv["speedup_computed"] is False and sv["ratio_reported"] is False
        and sv["arms_differenced"] is False and sv["placement_bands_differenced"] is False)
    chk("sn_band_arms_summarized_separately",
        set(sv["arms"]) == {"hpx", "ray"} and len(sv["arms"]["hpx"]["per_island"]) == 5
        and sv["arms"]["hpx"]["differenced_with_other_arm"] is False
        and sv["arms"]["ray"]["differenced_with_other_arm"] is False)
    chk("sn_band_has_same_node_gate_not_two_node",
        "all_arms_same_node" in sv["gates"] and "all_arms_two_node" not in sv["gates"])

    chk("sn_band_r_lt_5_fails",
        _sn_agg(_sn_band(n=4))["same_axis_comparison"] is False)

    # FAIL-CLOSED: TCP_NODELAY false or missing keeps the band from passing.
    sn_nd_false = _sn_agg(_sn_band(island0=_sn_island(
        "sn_159900_i1", hpx_over={"hpx_tcp_nodelay_verified": False})))
    chk("sn_band_fail_nodelay_false",
        sn_nd_false["gates"]["all_hpx_nodelay_verified"] is False
        and sn_nd_false["same_axis_comparison"] is False)
    sn_nd_missing = _sn_agg(_sn_band(island0=_sn_island(
        "sn_159900_i1", drop_hpx_key="hpx_tcp_nodelay_verified")))
    chk("sn_band_fail_nodelay_missing",
        sn_nd_missing["gates"]["all_hpx_nodelay_verified"] is False
        and sn_nd_missing["same_axis_comparison"] is False)

    # FAIL-CLOSED: strict disjoint_core_binding_verified false or missing keeps the band from passing.
    sn_core_false = _sn_agg(_sn_band(island0=_sn_island(
        "sn_159900_i1", hpx_over={"disjoint_core_binding_verified": False})))
    chk("sn_band_fail_core_binding_false",
        sn_core_false["gates"]["all_hpx_disjoint_core_binding"] is False
        and sn_core_false["same_axis_comparison"] is False)
    sn_core_missing = _sn_agg(_sn_band(island0=_sn_island(
        "sn_159900_i1", drop_hpx_key="disjoint_core_binding_verified")))
    chk("sn_band_fail_core_binding_missing",
        sn_core_missing["gates"]["all_hpx_disjoint_core_binding"] is False
        and sn_core_missing["same_axis_comparison"] is False)
    # KEY: recorded-only (requested disjoint True) but NOT strictly verified must FAIL -- requested
    # metadata can never pass the gate on its own.
    sn_recorded_only = _sn_agg(_sn_band(island0=_sn_island(
        "sn_159900_i1",
        hpx_over={"disjoint_core_binding_recorded": True, "disjoint_core_binding_verified": False})))
    chk("sn_band_fail_recorded_only_not_verified",
        sn_recorded_only["same_axis_comparison"] is False)

    # --- Slice 5: affinity-enforcement helpers (pure; mirror os.sched_*affinity / connector attest) ---
    chk("affinity_enforced_exact_match", _affinity_enforced("0-3", {0, 1, 2, 3}) is True)
    chk("affinity_enforced_single", _affinity_enforced("4", [4]) is True)
    chk("affinity_not_enforced_superset", _affinity_enforced("0-3", {0, 1, 2, 3, 4, 5}) is False)
    chk("affinity_not_enforced_empty", _affinity_enforced("0-3", None) is False
        and _affinity_enforced("0-3", []) is False)
    chk("affinity_not_enforced_no_request", _affinity_enforced("", {0, 1}) is False)
    chk("disjoint_verified_true",
        _disjoint_binding_verified(True, True, [0, 1, 2, 3], [4]) is True)
    chk("disjoint_verified_overlap_false",
        _disjoint_binding_verified(True, True, [0, 1, 2, 3], [3, 4]) is False)
    chk("disjoint_verified_unenforced_false",
        _disjoint_binding_verified(False, True, [0, 1, 2, 3], [4]) is False
        and _disjoint_binding_verified(True, False, [0, 1, 2, 3], [4]) is False)
    chk("disjoint_verified_empty_false",
        _disjoint_binding_verified(True, True, [], [4]) is False
        and _disjoint_binding_verified(True, True, [0], None) is False)

    chk("sn_band_fail_node_single",
        _sn_agg(_sn_band(island0=_sn_island("sn_159900_i1", inv_over={"node_single": "medusa01"})))
        ["same_axis_comparison"] is False)
    chk("sn_band_fail_placement_mode_disagree",
        _sn_agg(_sn_band(island0=_sn_island("sn_159900_i1",
                                            inv_over={"placement_mode": "cross_node"})))
        ["same_axis_comparison"] is False)
    chk("sn_band_fail_missing_arm",
        _sn_agg(_sn_band(island0=_sn_island("sn_159900_i1", drop_arm="ray")))
        ["same_axis_comparison"] is False)
    chk("sn_band_fail_same_node_false",
        _sn_agg(_sn_band(island0=_sn_island("sn_159900_i1",
                                            hpx_over={"same_node_exercised": False})))
        ["gates"]["all_arms_same_node"] is False)

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    print(f"\nexp61 selftest: {passed}/{len(checks)} helper checks passed")
    return passed == len(checks)


# ===================================================================================================
# Slice 5 -- SAME-NODE matched Python-boundary CONTROL band (additive; slice5_sn_* namespace)
# ---------------------------------------------------------------------------------------------------
# A within-arm placement control for the Slice-4 cross-node band: hold each arm's call primitive and
# parcelport software path constant, vary only physical co-location. The HPX arm still uses TWO
# distinct localities over the TCP parcelport on ONE host (loopback) -- NOT find_here / not an
# in-process shortcut. The Ray arm still uses a GENUINE remote actor pinned to the driver's OWN node.
# The two arms are NEVER differenced, ratioed, ranked, or called speedup; same-node vs cross-node is a
# within-arm control only. Off-cluster these phases SKIP cleanly (no curated artifact).
# ===================================================================================================
def _parse_cpuset(spec):
    """PURE. Parse a cpuset spec like '0-3,8' -> {0,1,2,3,8}. None/'' -> empty set. Tolerant of junk."""
    out = set()
    if not spec:
        return out
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, _, hi = part.partition("-")
            try:
                out.update(range(int(lo), int(hi) + 1))
            except ValueError:
                continue
        else:
            try:
                out.add(int(part))
            except ValueError:
                continue
    return out


def _cpusets_disjoint(a, b):
    """PURE. True iff both cpuset specs parse to NON-EMPTY, DISJOINT integer sets -- the witness that
    the two same-node HPX localities were *requested* on non-overlapping cores. NOTE: requested-only;
    the gate uses the stricter _disjoint_binding_verified() over EFFECTIVE affinity below."""
    sa, sb = _parse_cpuset(a), _parse_cpuset(b)
    return bool(sa) and bool(sb) and sa.isdisjoint(sb)


def _cpuset_to_mask(spec):
    """PURE. Hex CPU-affinity mask string (e.g. '0xf0' for '4-7') for `srun --cpu-bind=mask_cpu`, so a
    connector can be pinned to a MULTI-core cpuset. `map_cpu` binds only ONE core per task, which is
    why a 1-core connector ended up single-threaded; a mask exposes the whole set. '' if empty."""
    cpus = _parse_cpuset(spec)
    if not cpus:
        return ""
    mask = 0
    for c in cpus:
        mask |= (1 << c)
    return f"0x{mask:x}"


def _affinity_enforced(requested_spec, effective_set):
    """PURE. True iff an EFFECTIVE cpu set (from os.sched_getaffinity on the root, or the connector's
    attested sched_getaffinity) is non-empty and EXACTLY equals the requested cpuset. Requested-only /
    missing / mismatched effective all fail closed. effective_set is an iterable[int] or None."""
    req = _parse_cpuset(requested_spec)
    if not req or not effective_set:
        return False
    return set(int(c) for c in effective_set) == req


def _disjoint_binding_verified(root_enforced, conn_enforced, root_eff, conn_eff):
    """PURE. Strict disjoint-core witness for the same-node band: BOTH sides' affinity actually
    enforced/attested AND their EFFECTIVE cpu sets non-empty and disjoint. Recorded-only, faked,
    overlapping, or un-enforced data all fail closed."""
    if not (root_enforced and conn_enforced):
        return False
    r = set(int(c) for c in (root_eff or []))
    c = set(int(c) for c in (conn_eff or []))
    return bool(r) and bool(c) and r.isdisjoint(c)


def _enforce_root_affinity(requested_spec):
    """Enforce + read back THIS process's CPU affinity from `requested_spec` (e.g. '0-3'). Returns
    (effective_sorted_list_or_None, enforced_bool, error_or_None). Linux-only: os.sched_setaffinity /
    sched_getaffinity. Non-Linux or any failure => (None/effective, False, error) i.e. fail-closed.
    Run BEFORE embedding the HPX root so its worker threads inherit this mask (paired with
    --hpx:bind=none so HPX does not impose its own thread binding)."""
    req = _parse_cpuset(requested_spec)
    if not (hasattr(os, "sched_setaffinity") and hasattr(os, "sched_getaffinity")):
        return None, False, "os.sched_*affinity unavailable (non-Linux)"
    if not req:
        return None, False, "empty/invalid root cpuset"
    err = None
    try:
        os.sched_setaffinity(0, req)
    except OSError as e:
        err = f"sched_setaffinity: {type(e).__name__}: {e}"
    try:
        eff = sorted(os.sched_getaffinity(0))
    except OSError as e:
        return None, False, err or f"sched_getaffinity: {type(e).__name__}: {e}"
    return eff, _affinity_enforced(requested_spec, set(eff)), err


def _nodelay_verified_from_attest(attest):
    """PURE. Map a connector attestation dict -> (verified, method). The connector REALLY samples
    getsockopt(TCP_NODELAY) on its live HPX parcelport peer sockets (see dist_probe_connector.cpp).
    verified is True ONLY when it sampled >=1 such socket (tcp_nodelay_attested) AND every matched
    socket had NODELAY enabled (tcp_nodelay). Missing / false / unavailable (no socket found, non-Linux,
    getsockopt error) all FAIL CLOSED to (False, None). No value is ever fabricated."""
    a = attest or {}
    attested = a.get("tcp_nodelay_attested") is True
    enabled = a.get("tcp_nodelay") is True
    verified = attested and enabled
    if not verified:
        return False, None
    return True, f"{a.get('tcp_nodelay_source') or 'getsockopt'}:{a.get('tcp_nodelay_scope') or ''}"


def _finish_samenode_arm(payload, overall, run_id, arm, args):
    """Route a Slice-5 same-node arm result. With --matched-run-id it writes the PAIR-SCOPED name
    slice5_sn_<id>_<arm>.json (gitignored, reproducible) plus a per-run copy; it NEVER writes any
    curated base aggregate, so the Slice-2B/Slice-4 curated files are left untouched. Without an id it
    writes only an ignored per-run scratch copy."""
    os.makedirs(EXP61_RUNS, exist_ok=True)
    mid = getattr(args, "matched_run_id", None)
    if mid:
        path = os.path.join(HERE, f"slice5_sn_{mid}_{arm}.json")
        payload = dict(payload)
        payload["top_level_aggregate_path"] = os.path.basename(path)
        _atomic_write(path, payload)
        written = path
    else:
        written = os.path.join(EXP61_RUNS, f"{payload['phase']}_{run_id}.json")
        _atomic_write(written, payload)
    with open(os.path.join(EXP61_RUNS,
                           f"{payload['phase']}_{mid or 'nomid'}_{run_id}.json"), "w") as f:
        json.dump(payload, f, indent=2)
    payload["written"] = os.path.basename(written)
    return payload, overall


# --- phase: hpx-colocated (Slice 5) -- two distinct localities on ONE host over loopback TCP --------
def _colocated_base_payload(args, overall, slurm, connector_bin, ext, run_id):
    """Build the hpx-colocated provenance payload with EVERY placement/witness field present
    (null/false until a real same-node run fills them). TCP_NODELAY and disjoint-core-binding default
    to NOT-verified and are FAIL-CLOSED (see the module blocker note)."""
    payload = _fence_fields("hpx-colocated", overall, args.k, args.w, 1)
    payload.update({
        "run_id": run_id,
        "x": args.x,
        "placement_mode": "same_node",
        "comparison_kind": "python_boundary_same_node_per_arm",
        "measurement_boundary_hpx": MEASUREMENT_BOUNDARY,
        "hpx_extension_experiment_only": True,
        "hpx_connector_used": True,
        "hpx_call_primitive": "ext.dist_probe_remote(x) -> "
                              "hpx::async<exp61_dist_probe_action>(same_node_remote_id, x).get()",
        "hpx_action_registration_name": "exp61_dist_probe_action",
        "hpx_parcelport": "tcp",
        "loopback_path": True,
        "transport_note": ("loopback TCP; parcelport software path held constant; physical NIC/switch "
                           "removed. Same-node is a WITHIN-ARM placement control, not a broad "
                           "performance claim."),
        "same_node_capable_scaffold": True,
        "same_node_exercised": False,
        "single_locality_find_here": False,
        "ray_side_measured": False,
        # placement / locality witnesses -- null until a real same-node run proves them
        "hpx_root_hostname": None,
        "hpx_connector_hostname": None,
        "hpx_root_ip": None,
        "hpx_connector_ip": None,
        "hpx_root_locality_id": None,
        "hpx_remote_locality_id": None,
        "hpx_localities_distinct": None,
        "hpx_same_node_colocated": None,
        "node_tag_remote_locality_id": None,
        "oracle_expected": None,
        "hpx_oracle_correct": False,
        # HPX-expert-required witnesses (FAIL-CLOSED: default not-verified)
        "hpx_tcp_nodelay_verified": False,
        "hpx_tcp_nodelay_verification_method": None,
        "hpx_tcp_nodelay_source": None,
        "hpx_tcp_nodelay_scope": None,
        "hpx_tcp_nodelay_matched_sockets": None,
        "hpx_tcp_nodelay_peer": None,
        "hpx_tcp_nodelay_error": None,
        "hpx_parcelport_transport": None,
        "disjoint_core_binding_recorded": False,   # requested-only (provenance)
        "disjoint_core_binding_verified": False,    # strict: effective + enforced + disjoint
        "hpx_root_threads": getattr(args, "hpx_threads", None),
        "hpx_connector_threads": getattr(args, "connector_threads", None),
        "hpx_root_cpuset": getattr(args, "root_cpuset", None),
        "hpx_root_cpuset_requested": getattr(args, "root_cpuset", None),
        "hpx_root_cpuset_effective": None,
        "hpx_root_affinity_enforced": False,
        "hpx_connector_cpuset": getattr(args, "connector_cpuset", None),
        "hpx_connector_cpuset_requested": getattr(args, "connector_cpuset", None),
        "hpx_connector_cpuset_effective": None,
        "hpx_connector_affinity_enforced": False,
        "hpx_connector_affinity_source": None,
        "numa_layout": None,
        "hpx_parcel_coalescing": None,
        "hpx_root_endpoint": None,
        "hpx_connector_endpoint": None,
        "hpx_prefer_subnet": getattr(args, "prefer_subnet", None),
        "hpx_root_port": getattr(args, "root_port", None),
        "hpx_connector_port": getattr(args, "connector_port", None),
        "hpx_ignore_batch_env": None,
        "slurm_alloc_source": slurm.get("alloc_source"),
        # matched-structure provenance -- same-node uses node_single, NOT node_pair
        "matched_run_id": getattr(args, "matched_run_id", None),
        "selected_subnet": getattr(args, "prefer_subnet", None),
        "node_single": None,
        "slurm_job_id": slurm.get("slurm_job_id"),
        "slurm": {k: slurm.get(k) for k in
                  ("slurm_job_id", "slurm_job_nodelist", "slurm_num_nodes", "two_node",
                   "alloc_source")},
        "connector_bin": connector_bin,
        "ext_two_node_capable": bool(ext is not None and getattr(ext, "two_node_capable", False)),
        "boundary_call_ns_observation_only": _stats([]),
        "timing_is_observation_only_not_a_claim": True,
        "error": None,
        "claim_fences": [
            "Slice 5: same-node two-locality TCP control. HPX still uses TWO DISTINCT localities over "
            "the TCP parcelport on ONE host (loopback), NOT find_here / not an in-process shortcut.",
            "Same-node vs cross-node is a WITHIN-ARM placement control only. The Ray and HPX arms are "
            "separate artifacts and are NEVER differenced, ratioed, ranked, or called speedup. The "
            "same-node band is NEVER differenced against the cross-node band.",
            "TCP_NODELAY verification is FAIL-CLOSED: hpx_tcp_nodelay_verified stays False until a real "
            "probe attests it; the same-node band cannot pass while it is False. No fake 'verified'.",
            "oracle correctness proves intended closed-int64 execution on a locality id; physical "
            "co-location proof needs EQUAL hostnames + DISTINCT locality ids (recorded as provenance).",
        ],
    })
    return payload


def phase_hpx_colocated(args):
    run_id = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"
    ext_dirs = [args.ext_dir] if args.ext_dir else DEFAULT_EXT_DIRS
    conn_dirs = [args.connector_dir] if args.connector_dir else DEFAULT_CONNECTOR_DIRS
    slurm = slurm_two_node_from_env(os.environ, _scontrol_hostnames, _scontrol_job_alloc)
    connector_bin = _find_connector_bin(args, conn_dirs)
    ext = _import_ext(ext_dirs)
    ext_ready = (ext is not None and hasattr(ext, "dist_probe_remote")
                 and bool(getattr(ext, "two_node_capable", False)))

    pre = {"slurm_present": slurm.get("slurm_job_id") is not None,
           "connector_built": connector_bin is not None,
           "ext_remote_capable": ext_ready}
    if not all(pre.values()):
        reasons = [k for k, v in pre.items() if not v]
        payload = _colocated_base_payload(args, "skip", slurm, connector_bin, ext, run_id)
        payload["skip_reasons"] = reasons
        payload["gates"] = pre
        print("SKIP hpx-colocated: unmet same-node preconditions [" + ", ".join(reasons) + "]. "
              "Same-node scaffolding is present but NOT exercised locally; nothing measured, no "
              "curated pass written.")
        return _finish_samenode_arm(payload, "skip", run_id, "hpx", args)

    return _run_hpx_colocated_one_node(args, slurm, connector_bin, ext, run_id)


def _run_hpx_colocated_one_node(args, slurm, connector_bin, ext, run_id):
    """UNVALIDATED Slice-5 same-node orchestration; a Rostam run confirms/tunes the exact HPX flags,
    the second-locality port, the disjoint core binding, and the TCP_NODELAY attestation. Runs ONLY
    under a real Slurm allocation (caller gated slurm_present) and pins BOTH localities to ONE node.
    Any failure is captured as overall='fail' -- it can NEVER emit a fake pass and never falls back to
    find_here. TCP_NODELAY is read from the connector attestation only; it is never fabricated."""
    import tempfile
    payload = _colocated_base_payload(args, "fail", slurm, connector_bin, ext, run_id)
    hostnames = slurm["hostnames"] or []
    root_host = hostnames[0] if hostnames else ext.hostname()
    same_host = root_host  # same-node: the connector targets the SAME host as the embedded root
    bootdir = args.bootstrap_dir or tempfile.mkdtemp(prefix=f"exp61_sn_{run_id}_", dir=EXP61_RUNS)
    root_ip = args.root_ip or _first_ip_for_subnet(args.prefer_subnet)
    root_port = args.root_port
    conn_port = args.connector_port
    root_cpuset, conn_cpuset = args.root_cpuset, args.connector_cpuset

    proc = None
    started = False
    error = None
    raw_ns, correct, prewarm_result = [], 0, None
    remote_loc = -1
    root_loc = None
    expected = None
    root_eff, root_affinity_enforced = None, False
    try:
        if not root_ip:
            raise RuntimeError("no root IP for --prefer-subnet; pass --root-ip explicitly")
        if not _cpusets_disjoint(root_cpuset, conn_cpuset):
            raise RuntimeError("same-node control requires non-empty DISJOINT --root-cpuset / "
                               "--connector-cpuset (no core oversubscription between localities)")
        os.makedirs(bootdir, exist_ok=True)
        # ENFORCE this process's CPU affinity to --root-cpuset BEFORE embedding the HPX root, so the
        # root's worker threads inherit the mask; --hpx:bind=none stops HPX from imposing its own
        # thread binding. We read the EFFECTIVE mask back; the gate uses that, not the request.
        root_eff, root_affinity_enforced, root_aff_err = _enforce_root_affinity(root_cpuset)
        if root_aff_err:
            error = f"root_affinity: {root_aff_err}"
        # embedded HPX runtime as the AGAS ROOT on the selected subnet; ignore the Slurm batch env.
        root_extra = ["--hpx:ignore-batch-env", "--hpx:bind=none",
                      f"--hpx:hpx={root_ip}:{root_port}", f"--hpx:agas={root_ip}:{root_port}",
                      "--hpx:expect-connecting-localities"]
        ext.start(hpx_threads=args.hpx_threads, extra_args=root_extra)
        started = True
        root_loc = int(ext.local_locality_id())
        # connector on the SAME node, bound to a DISTINCT port, pinned to the disjoint cpuset. Use a
        # CPU MASK (not map_cpu, which binds a single core per task) so a multi-core connector cpuset
        # exposes all its cores; pass --hpx:threads explicitly so the connector is not thread-starved,
        # and --hpx:bind=none so HPX floats its threads within the srun mask.
        conn_cmd = ["srun", "--nodes=1", "--ntasks=1", f"--nodelist={same_host}",
                    f"--cpu-bind=mask_cpu:{_cpuset_to_mask(conn_cpuset)}",
                    connector_bin, "--role=connect", f"--bootstrap={bootdir}",
                    f"--serve-timeout={args.serve_timeout}",
                    f"--prefer-subnet={args.prefer_subnet}",
                    f"--agas-preprobe-host={root_ip}", f"--agas-preprobe-port={root_port}",
                    "--hpx:ignore-batch-env", "--hpx:bind=none",
                    f"--hpx:threads={args.connector_threads}",
                    f"--hpx:hpx={root_ip}:{conn_port}", f"--hpx:agas={root_ip}:{root_port}"]
        proc = subprocess.Popen(conn_cmd)
        remote_loc = int(ext.await_remote(timeout_s=args.await_timeout))
        if remote_loc < 0:
            raise RuntimeError("no same-node second locality joined within await timeout")
        node_tag = remote_loc
        expected = oracle(args.x, node_tag)
        raw_ns, correct, prewarm_result = timed_qd1(ext.dist_probe_remote, args.x, expected,
                                                    args.k, args.w)
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
    finally:
        try:
            with open(os.path.join(bootdir, "served1.ok"), "w") as f:
                f.write("served\n")
        except OSError:
            pass
        if proc is not None:
            try:
                proc.wait(timeout=max(10, args.serve_timeout))
            except Exception:  # noqa: BLE001
                proc.kill()
        if started:
            try:
                ext.shutdown()
            except Exception as e:  # noqa: BLE001
                error = error or f"shutdown: {type(e).__name__}: {e}"

    oracle_correct = (args.k > 0 and correct == args.k and error is None)
    st = _stats(raw_ns)
    localities_distinct = (root_loc is not None and remote_loc >= 0 and remote_loc != root_loc)
    disjoint_recorded = _cpusets_disjoint(root_cpuset, conn_cpuset)   # requested-only, provenance
    attest = _read_json(os.path.join(bootdir, "attest_connect.json")) or {}
    conn_host = attest.get("hostname") or same_host
    same_node_colocated = (_short_host(root_host) is not None
                           and _short_host(root_host) == _short_host(conn_host))
    # CONNECTOR effective affinity from the connector's OWN sched_getaffinity attestation (never the
    # requested map_cpu); absent => not enforced => fail-closed.
    conn_eff = attest.get("connector_cpuset_effective")
    conn_affinity_enforced = _affinity_enforced(conn_cpuset, conn_eff)
    # STRICT disjoint-core gate: both sides' EFFECTIVE affinity enforced AND non-empty AND disjoint.
    disjoint_verified = _disjoint_binding_verified(root_affinity_enforced, conn_affinity_enforced,
                                                   root_eff, conn_eff)
    # FAIL-CLOSED: TCP_NODELAY verified ONLY from a real connector getsockopt attestation (the
    # connector probed its live HPX parcelport peer sockets); absent/unavailable => False, never faked.
    nodelay_verified, nodelay_method = _nodelay_verified_from_attest(attest)
    gates = {
        "slurm_present": True,
        "connector_built": True,
        "ext_remote_capable": True,
        "hpx_started": started,
        "second_locality_joined": remote_loc >= 0,
        "localities_distinct": localities_distinct,
        "same_node_colocated": same_node_colocated,
        "tcp_parcelport": True,
        "hpx_tcp_nodelay_verified": nodelay_verified,
        "disjoint_core_binding_verified": disjoint_verified,
        "prewarm_correct": (prewarm_result == expected) if expected is not None else False,
        "every_call_oracle_correct": oracle_correct,
        "no_error": error is None,
    }
    overall = "pass" if all(gates.values()) else "fail"

    payload = _colocated_base_payload(args, overall, slurm, connector_bin, ext, run_id)
    payload.update({
        "same_node_exercised": True,
        "hpx_root_hostname": root_host,
        "hpx_connector_hostname": conn_host,
        "hpx_root_ip": root_ip,
        "hpx_connector_ip": attest.get("advertised_ip"),
        "hpx_root_locality_id": root_loc,
        "hpx_remote_locality_id": remote_loc if remote_loc >= 0 else None,
        "hpx_localities_distinct": localities_distinct,
        "hpx_same_node_colocated": same_node_colocated,
        "node_tag_remote_locality_id": remote_loc if remote_loc >= 0 else None,
        "oracle_expected": expected,
        "hpx_oracle_correct": oracle_correct,
        "hpx_tcp_nodelay_verified": nodelay_verified,
        "hpx_tcp_nodelay_verification_method": nodelay_method,
        "hpx_tcp_nodelay_source": attest.get("tcp_nodelay_source"),
        "hpx_tcp_nodelay_scope": attest.get("tcp_nodelay_scope"),
        "hpx_tcp_nodelay_matched_sockets": attest.get("tcp_nodelay_matched_sockets"),
        "hpx_tcp_nodelay_peer": attest.get("tcp_nodelay_peer"),
        "hpx_tcp_nodelay_error": attest.get("tcp_nodelay_error"),
        "hpx_parcelport_transport": attest.get("hpx_parcelport_transport"),
        "disjoint_core_binding_recorded": disjoint_recorded,   # requested-only (provenance)
        "disjoint_core_binding_verified": disjoint_verified,   # strict: effective + enforced + disjoint
        "hpx_root_cpuset": root_cpuset,                        # back-compat alias of requested
        "hpx_root_cpuset_requested": root_cpuset,
        "hpx_root_cpuset_effective": root_eff,
        "hpx_root_affinity_enforced": root_affinity_enforced,
        "hpx_connector_cpuset": conn_cpuset,                   # back-compat alias of requested
        "hpx_connector_cpuset_requested": conn_cpuset,
        "hpx_connector_cpuset_effective": conn_eff,
        "hpx_connector_affinity_enforced": conn_affinity_enforced,
        "hpx_connector_affinity_source": attest.get("connector_affinity_source"),
        "hpx_root_threads": args.hpx_threads,
        "hpx_connector_threads": args.connector_threads,
        "hpx_root_endpoint": f"{root_ip}:{root_port}" if root_ip else None,
        "hpx_connector_endpoint": f"{root_ip}:{conn_port}" if root_ip else None,
        "hpx_ignore_batch_env": True,
        "hpx_parcel_coalescing": attest.get("parcel_coalescing"),
        "numa_layout": attest.get("numa_layout"),
        "boundary_call_ns_observation_only": st,
        "gates": gates,
        "error": error,
        "bootstrap_dir": bootdir,
        "selected_subnet": args.prefer_subnet,
        "node_single": _short_host(root_host),
        "slurm_job_id": slurm.get("slurm_job_id"),
        "connector_attestation": attest or None,
    })
    return _finish_samenode_arm(payload, overall, run_id, "hpx", args)


# --- phase: ray-colocated (Slice 5) -- genuine actor pinned to the DRIVER's OWN node ----------------
def _ray_colocated_base_payload(args, overall, slurm, run_id):
    payload = _fence_fields("ray-colocated", overall, args.k, args.w, 1)
    payload.update({
        "run_id": run_id,
        "x": args.x,
        "placement_mode": "same_node",
        "comparison_kind": "python_boundary_same_node_per_arm",
        "measurement_boundary_ray": MEASUREMENT_BOUNDARY,
        "ray_side_measured": True,
        "ray_call_primitive": "ray.get(actor.dist_probe.remote(x))",
        "ray_placement_strategy": "NodeAffinitySchedulingStrategy(node_id=driver_node, soft=False)",
        "ray_actor_same_node_as_driver": True,
        "same_node_capable_scaffold": True,
        "same_node_exercised": False,
        "ray_version": None,
        "ray_node_tag": args.ray_node_tag,
        "ray_node_ids": None,
        "ray_hostnames": None,
        "ray_ips": None,
        "ray_driver_node_id": None,
        "ray_target_node_id": None,
        "ray_actor_on_driver_node": None,
        "ray_on_node_sample_count": 0,
        "ray_driver_ip": None,
        "oracle_expected": None,
        "ray_oracle_correct": False,
        "ray_p50_ns": None, "ray_p90_ns": None, "ray_p99_ns": None, "ray_mean_ns": None,
        # matched-structure provenance -- same-node uses node_single, NOT node_pair
        "matched_run_id": getattr(args, "matched_run_id", None),
        "selected_subnet": getattr(args, "prefer_subnet", None),
        "node_single": None,
        "slurm_job_id": slurm.get("slurm_job_id"),
        "slurm_alloc_source": slurm.get("alloc_source"),
        "slurm": {k: slurm.get(k) for k in
                  ("slurm_job_id", "slurm_job_nodelist", "slurm_num_nodes", "two_node",
                   "alloc_source")},
        "boundary_call_ns_observation_only": _stats([]),
        "timing_is_observation_only_not_a_claim": True,
        "error": None,
        "claim_fences": [
            "Slice 5: same-node Ray control. The actor is a GENUINE remote actor hard-pinned to the "
            "DRIVER's OWN node (NodeAffinitySchedulingStrategy soft=False) and verified on-node; it is "
            "NOT an in-process call.",
            "Same-node vs cross-node is a WITHIN-ARM placement control only. Separate artifact from "
            "the HPX arm; NEVER differenced, ratioed, ranked, or called speedup. The same-node band "
            "is NEVER differenced against the cross-node band.",
            "exp59 / Slice-4 numbers are NOT reused here; this phase runs against a live Ray cluster "
            "it connects to.",
        ],
    })
    return payload


def phase_ray_colocated(args):
    run_id = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"
    slurm = slurm_two_node_from_env(os.environ, _scontrol_hostnames, _scontrol_job_alloc)

    if slurm.get("slurm_job_id") is None:
        payload = _ray_colocated_base_payload(args, "skip", slurm, run_id)
        payload["skip_reasons"] = ["slurm_present"]
        print("SKIP ray-colocated: not inside a Slurm allocation. No local fallback; nothing "
              "measured, no curated pass written.")
        return _finish_samenode_arm(payload, "skip", run_id, "ray", args)

    try:
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    except Exception:  # noqa: BLE001
        payload = _ray_colocated_base_payload(args, "skip", slurm, run_id)
        payload["skip_reasons"] = ["ray_import"]
        print("SKIP ray-colocated: ray not importable. Nothing measured, no curated pass written.")
        return _finish_samenode_arm(payload, "skip", run_id, "ray", args)

    address = args.ray_address or os.environ.get("RAY_ADDRESS") or "auto"
    try:
        ray.init(address=address, ignore_reinit_error=True, log_to_driver=False,
                 configure_logging=False)
    except Exception as e:  # noqa: BLE001
        payload = _ray_colocated_base_payload(args, "skip", slurm, run_id)
        payload["skip_reasons"] = ["ray_cluster_connect"]
        payload["error"] = f"{type(e).__name__}: {e}"
        print(f"SKIP ray-colocated: could not connect to a running Ray cluster at '{address}'. "
              "Nothing measured, no curated pass written.")
        return _finish_samenode_arm(payload, "skip", run_id, "ray", args)

    error = None
    raw_ns, correct, prewarm_result = [], 0, None
    on_node_count = 0
    actor = None
    driver_node = None
    expected = None
    node_ids = hostnames = ips = None
    driver_ip = node_single = None
    try:
        alive = [n for n in ray.nodes() if n.get("Alive")]
        node_ids = [n.get("NodeID") for n in alive]
        hostnames = [n.get("NodeManagerHostname") for n in alive]
        ips = [n.get("NodeManagerAddress") for n in alive]
        driver_node = ray.get_runtime_context().get_node_id()
        host_by_id = {n.get("NodeID"): n.get("NodeManagerHostname") for n in alive}
        ip_by_id = {n.get("NodeID"): n.get("NodeManagerAddress") for n in alive}
        driver_ip = ip_by_id.get(driver_node)
        node_single = _short_host(host_by_id.get(driver_node))
        node_tag = args.ray_node_tag
        expected = oracle(args.x, node_tag)

        @ray.remote(num_cpus=1)
        class DistProbe:
            def __init__(self, tag):
                self.tag = int(tag)

            def dist_probe(self, xx):
                return (int(xx) ^ 0x52415958) + (self.tag << 1)

            def where(self):
                ctx = ray.get_runtime_context()
                import socket
                return {"node_id": ctx.get_node_id(), "hostname": socket.gethostname()}

        # same-node: hard-pin the actor to the DRIVER's OWN node (still a genuine remote actor).
        strategy = NodeAffinitySchedulingStrategy(node_id=driver_node, soft=False)
        actor = DistProbe.options(scheduling_strategy=strategy).remote(node_tag)

        # verify the actor is genuinely ON the driver node before timing anything.
        for _ in range(min(5, max(1, args.k))):
            placed = ray.get(actor.where.remote())
            if placed["node_id"] == driver_node:
                on_node_count += 1
        if on_node_count == 0:
            raise RuntimeError("actor placement not honored on the driver node")

        call = lambda xx: ray.get(actor.dist_probe.remote(xx))  # noqa: E731 (the timed boundary)
        raw_ns, correct, prewarm_result = timed_qd1(call, args.x, expected, args.k, args.w)
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
    finally:
        try:
            if actor is not None:
                ray.kill(actor)
        except Exception:  # noqa: BLE001
            pass
        try:
            ray.shutdown()
        except Exception as e:  # noqa: BLE001
            error = error or f"shutdown: {type(e).__name__}: {e}"

    oracle_correct = (args.k > 0 and correct == args.k and error is None)
    st = _stats(raw_ns)
    gates = {
        "slurm_present": True,
        "ray_imported": True,
        "ray_cluster_connect": True,
        "actor_on_driver_node": on_node_count > 0,
        "prewarm_correct": (prewarm_result == expected) if expected is not None else False,
        "every_call_oracle_correct": oracle_correct,
        "no_error": error is None,
    }
    overall = "pass" if all(gates.values()) else "fail"

    payload = _ray_colocated_base_payload(args, overall, slurm, run_id)
    payload.update({
        "same_node_exercised": True,
        "ray_version": getattr(ray, "__version__", None),
        "ray_node_ids": node_ids, "ray_hostnames": hostnames, "ray_ips": ips,
        "ray_driver_node_id": driver_node, "ray_target_node_id": driver_node,
        "ray_actor_on_driver_node": on_node_count > 0,
        "ray_on_node_sample_count": on_node_count,
        "ray_driver_ip": driver_ip,
        "oracle_expected": expected, "ray_oracle_correct": oracle_correct,
        "ray_p50_ns": st.get("p50_ns"), "ray_p90_ns": st.get("p90_ns"),
        "ray_p99_ns": st.get("p99_ns"), "ray_mean_ns": st.get("mean_ns"),
        "node_single": node_single, "selected_subnet": args.prefer_subnet,
        "slurm_job_id": slurm.get("slurm_job_id"),
        "boundary_call_ns_observation_only": st, "gates": gates, "error": error,
    })
    return _finish_samenode_arm(payload, overall, run_id, "ray", args)


# --- phase: pair-manifest-samenode (Slice 5) -- same-node matched-run STRUCTURE proof ---------------
def phase_pair_manifest_samenode(args):
    mid = getattr(args, "matched_run_id", None)
    if not mid:
        print("SKIP pair-manifest-samenode: --matched-run-id is required.")
        return None, "skip"
    hpx_path = os.path.join(HERE, f"slice5_sn_{mid}_hpx.json")
    ray_path = os.path.join(HERE, f"slice5_sn_{mid}_ray.json")
    hpx = _read_json(hpx_path)
    ray = _read_json(ray_path)
    missing = [n for n, d in (("hpx", hpx), ("ray", ray)) if d is None]
    if missing:
        print(f"SKIP pair-manifest-samenode: missing pair-scoped artifact(s) {missing} for "
              f"matched_run_id={mid}. Run BOTH same-node arms with --matched-run-id {mid} first; no "
              "manifest is written and no success is claimed.")
        return None, "skip"

    ok, checks = matched_invariants(hpx, ray, placement_mode="same_node")
    overall = "pass" if ok else "fail"
    shared = {k: hpx.get(k) for k in SAME_NODE_MATCHED_INVARIANT_KEYS} if ok else None

    manifest = {
        "experiment_id": "exp61",
        "phase": "pair-manifest-samenode",
        "placement_mode": "same_node",
        "matched_run_id": mid,
        "overall": overall,
        "matched_structure_validated": ok,
        # structure-only, NEVER a comparison.
        "same_axis_comparison": False,
        "same_axis_claim_allowed": False,
        "speedup_computed": False,
        "ratio_reported": False,
        "placement_bands_differenced": False,
        "comparison_kind": "matched_same_node_structure_proof_only",
        "checks": checks,
        "shared_matched_invariants": shared,
        "hpx_arm": {k: hpx.get(k) for k in SAME_NODE_MANIFEST_ARM_KEYS},
        "ray_arm": {k: ray.get(k) for k in SAME_NODE_MANIFEST_ARM_KEYS},
        "hpx_arm_artifact": os.path.basename(hpx_path),
        "ray_arm_artifact": os.path.basename(ray_path),
        "interpretation": ("Slice 5 same-node matched-run STRUCTURE proof: both arms ran in the same "
                           "allocation on ONE node / subnet / K-W-prewarm / Python caller boundary, "
                           "each passing its OWN oracle, with the HPX arm over two DISTINCT localities "
                           "on the TCP parcelport (loopback). Per-arm boundary times are carried for "
                           "provenance only and are NEVER differenced or ratioed. The R>=5 same-node "
                           "band is a LATER step."),
        "forbidden_claims": FORBIDDEN_CLAIMS,
        "not_a_same_axis_comparison": True,
        "run_id": time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}",
    }
    path = os.path.join(HERE, f"slice5_sn_{mid}_manifest.json")
    _atomic_write(path, manifest)
    os.makedirs(EXP61_RUNS, exist_ok=True)
    with open(os.path.join(EXP61_RUNS,
                           f"pair-manifest-samenode_{mid}_{manifest['run_id']}.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    manifest["written"] = os.path.basename(path)
    if not ok:
        failed = [k for k, v in checks.items() if not v]
        print(f"pair-manifest-samenode: matched_structure_validated=FALSE for {mid}; "
              f"failed checks: {failed}")
    return manifest, overall


# --- phase: band-aggregate-samenode (Slice 5) -- the R>=5 same-node CONTROL band --------------------
def _load_band_inputs_samenode(ids):
    """IO: for each matched_run_id, read its same-node manifest + both pair-scoped arm artifacts from
    HERE. Missing files come back as None (a gate failure, never a silent pass)."""
    inputs = []
    for mid in ids:
        inputs.append({
            "matched_run_id": mid,
            "manifest": _read_json(os.path.join(HERE, f"slice5_sn_{mid}_manifest.json")),
            "hpx": _read_json(os.path.join(HERE, f"slice5_sn_{mid}_hpx.json")),
            "ray": _read_json(os.path.join(HERE, f"slice5_sn_{mid}_ray.json")),
        })
    return inputs


def phase_band_aggregate_samenode(args):
    ids = [s.strip() for s in (getattr(args, "matched_run_ids", None) or "").split(",") if s.strip()]
    if not ids:
        print("SKIP band-aggregate-samenode: --matched-run-ids id1,...,idR (R>=5) is required.")
        return None, "skip"
    inputs = _load_band_inputs_samenode(ids)
    payload, overall = build_band_aggregate(inputs, _clock_overhead(), min_r=5,
                                            placement_mode="same_node")
    payload["run_id"] = time.strftime("%Y%m%dT%H%M%S") + f"_{os.getpid()}"

    out = getattr(args, "band_out", None) or (
        f"slice5_samenode_{payload['band_id']}_aggregate.json" if payload["band_id"]
        else "slice5_samenode_band_unknown_aggregate.json")
    path = out if os.path.isabs(out) else os.path.join(HERE, out)
    _atomic_write(path, payload)
    os.makedirs(EXP61_RUNS, exist_ok=True)
    with open(os.path.join(EXP61_RUNS,
                           f"band-aggregate-samenode_{payload['run_id']}.json"), "w") as f:
        json.dump(payload, f, indent=2)
    payload["written"] = os.path.basename(path)
    if overall != "pass":
        failed = [k for k, v in payload["gates"].items() if not v]
        print(f"band-aggregate-samenode: same_axis_comparison=FALSE; failed gates: {failed}")
    return payload, overall


PHASE_RUNNERS = {
    "hpx-smoke": lambda a: phase_hpx_smoke(a),
    "ray-smoke": lambda a: phase_ray_smoke(a),
    "hpx-connected": lambda a: phase_hpx_connected(a),
    "ray-remote": lambda a: phase_ray_remote(a),
    "pair-manifest": lambda a: phase_pair_manifest(a),
    "band-aggregate": lambda a: phase_band_aggregate(a),
    "manifest-summary": lambda a: phase_manifest_summary(a),
    "hpx-colocated": lambda a: phase_hpx_colocated(a),
    "ray-colocated": lambda a: phase_ray_colocated(a),
    "pair-manifest-samenode": lambda a: phase_pair_manifest_samenode(a),
    "band-aggregate-samenode": lambda a: phase_band_aggregate_samenode(a),
}


def main():
    ap = argparse.ArgumentParser(description="exp61 -- same-axis Python-boundary smoke (per arm)")
    ap.add_argument("--phase", required=True,
                    choices=["hpx-smoke", "ray-smoke", "hpx-connected", "ray-remote",
                             "pair-manifest", "band-aggregate", "manifest-summary", "selftest",
                             # Slice 5 same-node control
                             "hpx-colocated", "ray-colocated", "pair-manifest-samenode",
                             "band-aggregate-samenode"])
    ap.add_argument("--ext-dir", default=None, help="dir containing the built dist_probe_ext module")
    ap.add_argument("--connector-dir", default=None,
                    help="dir containing the built dist_probe_connector binary")
    ap.add_argument("--connector-bin", default=None, help="explicit path to dist_probe_connector")
    ap.add_argument("--hpx-threads", type=int, default=4)
    ap.add_argument("--ray-node-tag", type=int, default=0,
                    help="fixed explicit node_tag for the Ray oracle")
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--k", type=int, default=1000)
    ap.add_argument("--w", type=int, default=100)
    # two-node (Slice 2A) knobs -- only consulted when actually inside a >=2-node allocation/cluster
    ap.add_argument("--root-ip", default=None, help="hpx-connected: AGAS root IP (else --prefer-subnet)")
    ap.add_argument("--root-port", type=int, default=7910, help="hpx-connected: AGAS root port")
    # Slice 5 same-node (hpx-colocated) knobs -- only consulted inside a real Slurm allocation
    ap.add_argument("--connector-port", type=int, default=7911,
                    help="hpx-colocated: DISTINCT parcelport port for the 2nd same-node locality")
    ap.add_argument("--connector-threads", type=int, default=4,
                    help="hpx-colocated: HPX worker threads for the connector locality")
    ap.add_argument("--root-cpuset", default=None,
                    help="hpx-colocated: cpuset for the embedded root, e.g. '0-3' (must be disjoint "
                         "from --connector-cpuset; both required for the disjoint-core gate)")
    ap.add_argument("--connector-cpuset", default=None,
                    help="hpx-colocated: cpuset for the connector locality, e.g. '4-7'")
    ap.add_argument("--prefer-subnet", default="10.42.5.",
                    help="hpx-connected: pick a local IP with this prefix as the root IP")
    ap.add_argument("--await-timeout", type=int, default=60,
                    help="hpx-connected: seconds to wait for the connector to join")
    ap.add_argument("--serve-timeout", type=int, default=120,
                    help="hpx-connected: connector serve-timeout seconds")
    ap.add_argument("--bootstrap-dir", default=None,
                    help="hpx-connected: shared-FS rendezvous dir (else a per-run temp dir)")
    ap.add_argument("--ray-address", default=None,
                    help="ray-remote: existing cluster address (else RAY_ADDRESS or 'auto')")
    ap.add_argument("--matched-run-id", default=None,
                    help="Slice 3: pair tag. When set, hpx-connected/ray-remote write pair-scoped "
                         "slice3_<id>_{hpx,ray}.json (NOT the 2B curated base); pair-manifest "
                         "correlates them into slice3_<id>_manifest.json.")
    ap.add_argument("--allow-overwrite-pass", action="store_true", default=False)
    ap.add_argument("--matched-run-ids", default=None,
                    help="band-aggregate (Slice 4) / manifest-summary (Slice 3R): comma-separated "
                         "list of matched_run_ids, e.g. band_158722_i1,band_158722_i2,...")
    ap.add_argument("--band-out", default=None,
                    help="band-aggregate: output filename (default slice4_band_<job>_aggregate.json)")
    ap.add_argument("--summary-out", default=None,
                    help="manifest-summary: output filename (default slice3r_<job>_summary.json)")
    args = ap.parse_args()

    if args.phase == "selftest":
        return 0 if phase_selftest() else 1

    payload, overall = PHASE_RUNNERS[args.phase](args)
    if payload is None:   # legacy clean skip (hpx-smoke/ray-smoke): nothing written
        return 0

    summary = {"experiment_id": "exp61", "phase": args.phase, "overall": overall,
               "run_id": payload.get("run_id"), "oracle_expected": payload.get("oracle_expected"),
               "written": payload.get("written"), "error": payload.get("error"),
               "boundary_call_ns_observation_only": payload.get("boundary_call_ns_observation_only")}
    if args.phase == "hpx-smoke":
        summary.update({"node_tag_locality_id": payload.get("node_tag_locality_id"),
                        "hpx_oracle_correct": payload.get("hpx_oracle_correct"),
                        "hpx_gil_released": payload.get("hpx_gil_released")})
    elif args.phase == "ray-smoke":
        summary.update({"ray_version": payload.get("ray_version"),
                        "ray_node_tag": payload.get("ray_node_tag"),
                        "ray_oracle_correct": payload.get("ray_oracle_correct")})
    elif args.phase == "hpx-connected":
        summary.update({"two_node_exercised": payload.get("two_node_exercised"),
                        "skip_reasons": payload.get("skip_reasons"),
                        "hpx_remote_locality_id": payload.get("hpx_remote_locality_id"),
                        "hpx_here_locality_differs_from_remote":
                            payload.get("hpx_here_locality_differs_from_remote")})
    elif args.phase == "ray-remote":
        summary.update({"two_node_exercised": payload.get("two_node_exercised"),
                        "skip_reasons": payload.get("skip_reasons"),
                        "ray_target_node_id": payload.get("ray_target_node_id"),
                        "ray_off_node_sample_count": payload.get("ray_off_node_sample_count"),
                        "ray_ips_on_selected_subnet": payload.get("ray_ips_on_selected_subnet")})
    elif args.phase in ("band-aggregate", "band-aggregate-samenode"):
        summary.update({"band_id": payload.get("band_id"), "R": payload.get("R"),
                        "placement_mode": payload.get("placement_mode"),
                        "same_axis_comparison": payload.get("same_axis_comparison"),
                        "speedup_computed": payload.get("speedup_computed"),
                        "ratio_reported": payload.get("ratio_reported"),
                        "placement_bands_differenced": payload.get("placement_bands_differenced"),
                        "gates": payload.get("gates")})
    elif args.phase == "hpx-colocated":
        summary.update({"same_node_exercised": payload.get("same_node_exercised"),
                        "skip_reasons": payload.get("skip_reasons"),
                        "hpx_remote_locality_id": payload.get("hpx_remote_locality_id"),
                        "hpx_same_node_colocated": payload.get("hpx_same_node_colocated"),
                        "hpx_tcp_nodelay_verified": payload.get("hpx_tcp_nodelay_verified"),
                        "disjoint_core_binding_recorded":
                            payload.get("disjoint_core_binding_recorded")})
    elif args.phase == "ray-colocated":
        summary.update({"same_node_exercised": payload.get("same_node_exercised"),
                        "skip_reasons": payload.get("skip_reasons"),
                        "ray_target_node_id": payload.get("ray_target_node_id"),
                        "ray_on_node_sample_count": payload.get("ray_on_node_sample_count")})
    elif args.phase == "manifest-summary":
        summary.update({"R": payload.get("R"),
                        "distinct_matched_run_ids": payload.get("distinct_matched_run_ids"),
                        "cross_run_agreement": payload.get("cross_run_agreement"),
                        "all_manifests_pass_and_validated":
                            payload.get("all_manifests_pass_and_validated"),
                        "same_axis_comparison": payload.get("same_axis_comparison")})
    else:  # pair-manifest
        summary.update({"matched_run_id": payload.get("matched_run_id"),
                        "matched_structure_validated": payload.get("matched_structure_validated"),
                        "same_axis_comparison": payload.get("same_axis_comparison"),
                        "checks": payload.get("checks")})
    print(json.dumps(summary, indent=2))
    # skip is a clean exit (precondition not met); only a real two-node FAIL is non-zero.
    return 0 if overall in ("pass", "skip") else 1


if __name__ == "__main__":
    sys.exit(main())
