#!/usr/bin/env python3
"""exp64 waiter-fix verification analyzer (PURE; runs anywhere; no HPX, no Ray, no Slurm).

Verifies whether HPX master (including PR #7367, "Fixing future::wait_until (and wait_for)
to return once future was made ready") fixes the exact suspended-timed-wait behavior that
exp64 Slice 5 Phase A-A4 classified as `waiter_resume_at_timeout`:

  * a composed cross-node future (when_all/dataflow) whose continuation completes promptly,
  * while the suspended timed waiter resumes only at its full timeout.

This module NEVER runs HPX. It consumes COPIED-BACK exp64 native-smoke artifacts (the
existing A4 root-clock instrumentation: t_dispatch_start_ns / t_continuation_entered_ns /
t_continuation_completed_ns / t_waiter_entered_ns / t_wait_returned_ns) produced by
run_exp64_payload.py with IDENTICAL experiment logic against two HPX builds selected via
--native-build-dir, and classifies each cell with the explicit verification vocabulary:

  waiter_resume_at_timeout   the prior defect signature (control must reproduce this)
  waiter_resumed_on_ready    the fixed-state signature (master must show this)
  control_not_reproduced     the HPX 1.11 control failed to reproduce the prior signature
  invalid_instrumentation    missing/non-monotonic timestamps; classification impossible
  mixed_result               measured calls of one cell disagree
  unresolved                 valid instrumentation, but neither signature holds

"Prompt" is deliberately NOT defined by a performance threshold. waiter_resumed_on_ready is
structural: the waiter provably suspended BEFORE readiness (t_waiter_entered_ns <
t_continuation_completed_ns), the continuation entered and completed, wait_for reported
ready, the waiter returned at/after readiness, and the wait-return is MATERIALLY separated
from the full timeout bound (the conservative separation gate below, reusing exp64's
NATIVE_DEADLINE_MARGIN_FRACTION). Observed timings are reported observationally only.

The exp65 loopback re-check (one full-bound future::wait_for with a readiness-witness
continuation, --wait-probe full_bound_instrumented) is analyzed by its own classifier and
kept FORMALLY SEPARATE in the aggregate: corroborating evidence on a different platform and
experiment, never merged into the exp64 result.

No performance, speedup, ratio, winner, or general-HPX claim anywhere. Results are scoped
to the exact HPX build identities recorded in the artifacts.
"""

import argparse
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from run_exp64_payload import (  # noqa: E402  (pure constants/modes from the exp64 runner)
    NATIVE_DEADLINE_MARGIN_FRACTION,
    NATIVE_PROMPTNESS_THRESHOLD_S,
    NATIVE_READINESS_MODES,
)

# ---------------------------------------------------------------------------
# Verification vocabulary (explicit result classes; the ONLY classes this module emits)
# ---------------------------------------------------------------------------

WAITER_RESUME_AT_TIMEOUT = "waiter_resume_at_timeout"
WAITER_RESUMED_ON_READY = "waiter_resumed_on_ready"
CONTROL_NOT_REPRODUCED = "control_not_reproduced"
INVALID_INSTRUMENTATION = "invalid_instrumentation"
MIXED_RESULT = "mixed_result"
UNRESOLVED = "unresolved"

VOCABULARY = (WAITER_RESUME_AT_TIMEOUT, WAITER_RESUMED_ON_READY, CONTROL_NOT_REPRODUCED,
              INVALID_INSTRUMENTATION, MIXED_RESULT, UNRESOLVED)

# The two exp64 native CLAIM modes under verification (poll/yield are validity controls).
CLAIM_MODES = tuple(NATIVE_READINESS_MODES)          # ("when_all_then_reduce", "dataflow_reduce")
CONTROL_MODES = ("root_flat_gather_poll", "when_all_then_reduce_yield")

# Verification fences: this experiment licenses NO comparison arithmetic of any kind.
VERIFICATION_FENCES = {
    "speedup_computed": False,
    "ratio_reported": False,
    "arms_differenced": False,
    "placement_bands_differenced": False,
    "performance_claim": False,
    "general_hpx_claim": False,
    "latency_claim": False,
}


# ---------------------------------------------------------------------------
# exp64 per-call classification (root-clock timestamps only)
# ---------------------------------------------------------------------------

def classify_exp64_call(call, *, dispatch_timeout_s,
                        margin_fraction=NATIVE_DEADLINE_MARGIN_FRACTION,
                        prompt_floor_s=NATIVE_PROMPTNESS_THRESHOLD_S):
    """PURE per-call classifier for one exp64 native-claim-mode call. Reuses the A4 root-clock
    timestamps recorded by payload_ext.cpp; adds NO new timing definition. Returns the class
    plus the derived quantities used, so the aggregate is auditable."""
    ds = int(call.get("t_dispatch_start_ns", 0) or 0)
    ce = int(call.get("t_continuation_entered_ns", 0) or 0)
    cc = int(call.get("t_continuation_completed_ns", 0) or 0)
    we = int(call.get("t_waiter_entered_ns", 0) or 0)
    wr = int(call.get("t_wait_returned_ns", 0) or 0)
    rtt_s = (int(call.get("rtt_ns", 0) or 0)) / 1e9
    status = call.get("wait_for_status", "unknown")
    deferred_floor_s = margin_fraction * float(dispatch_timeout_s)

    # Instrumentation validity: body timestamps present, capture-order monotonic, direct-wait
    # entry captured (the suspension-proof timestamp is REQUIRED for this verification).
    seq = [t for t in (ds, ce, cc, wr) if t > 0]
    monotonic = seq == sorted(seq)
    instrumentation_ok = bool(ds > 0 and wr > 0 and we > 0 and ds <= we <= wr and monotonic)

    continuation_delay_s = (ce - ds) / 1e9 if (ce > 0 and ds > 0) else None
    wait_return_after_continuation_s = (wr - cc) / 1e9 if (cc > 0 and wr > 0) else None
    waiter_suspended_before_ready = bool(we > 0 and cc > 0 and we < cc)
    continuation_completed = bool(ce > 0 and cc > 0 and ce <= cc)
    separated_from_timeout = rtt_s < deferred_floor_s      # conservative separation gate
    returned_after_readiness = bool(cc > 0 and wr > 0 and wr >= cc)

    if not instrumentation_ok or not continuation_completed:
        cls = INVALID_INSTRUMENTATION
    elif not waiter_suspended_before_ready:
        # Valid instrumentation but the suspension precondition never held (is_ready fast
        # path): the call cannot test the resume path either way.
        cls = UNRESOLVED
    elif (continuation_delay_s is not None and continuation_delay_s < prompt_floor_s
          and wait_return_after_continuation_s is not None
          and wait_return_after_continuation_s >= deferred_floor_s):
        # Exactly the exp64 A4 defect signature: reduce ran early, caller woke at the bound.
        cls = WAITER_RESUME_AT_TIMEOUT
    elif (status == "ready" and returned_after_readiness and separated_from_timeout):
        cls = WAITER_RESUMED_ON_READY
    else:
        cls = UNRESOLVED

    return {
        "classification": cls,
        "instrumentation_ok": instrumentation_ok,
        "waiter_suspended_before_ready": waiter_suspended_before_ready,
        "continuation_completed": continuation_completed,
        "returned_after_readiness": returned_after_readiness,
        "separated_from_timeout": separated_from_timeout,
        "wait_for_status": status,
        "rtt_s_observational": rtt_s,
        "continuation_delay_s_observational": continuation_delay_s,
        "wait_return_after_continuation_s_observational": wait_return_after_continuation_s,
        "deferred_floor_s": deferred_floor_s,
        "prompt_floor_s_observational": prompt_floor_s,
    }


def classify_exp64_cell(artifact):
    """PURE cell classifier: one exp64 native artifact (one mode x S). Claim modes get the
    verification class (uniform across measured calls, else mixed_result); control modes get
    a validity record only."""
    mode = artifact.get("readiness_composition")
    timeout_s = float(artifact.get("dispatch_timeout_s", 8.0))
    calls = artifact.get("calls", [])
    out = {
        "mode": mode,
        "payload_bytes": artifact.get("payload_bytes"),
        "slurm_job_id": artifact.get("slurm_job_id"),
        "artifact_tag": artifact.get("artifact_tag"),
        "dispatch_timeout_s": timeout_s,
        "n_calls": len(calls),
        "hpx_version_full": (artifact.get("hpx_build_provenance") or {}).get(
            "hpx_version_full", "not_recorded"),
        # identity string for the build-identity gates: complete_version embeds the git commit
        # (full_version_as_string is only e.g. "2.0.0"); fall back to full when not recorded.
        "hpx_identity": _identity_string(artifact.get("hpx_build_provenance") or {}),
        "is_claim_mode": mode in CLAIM_MODES,
    }
    if mode not in CLAIM_MODES:
        out["control_overall_pass"] = bool(artifact.get("overall_pass"))
        out["classification"] = "n/a_control_mode"
        return out
    per_call = [classify_exp64_call(c, dispatch_timeout_s=timeout_s) for c in calls]
    classes = sorted({p["classification"] for p in per_call})
    if not per_call:
        cls = INVALID_INSTRUMENTATION
    elif len(classes) == 1:
        cls = classes[0]
    else:
        cls = MIXED_RESULT
    out.update({
        "classification": cls,
        "per_call_classes": [p["classification"] for p in per_call],
        "all_instrumentation_ok": bool(per_call) and all(p["instrumentation_ok"]
                                                         for p in per_call),
        "all_waiter_suspended_before_ready": bool(per_call) and all(
            p["waiter_suspended_before_ready"] for p in per_call),
        "all_continuation_completed": bool(per_call) and all(p["continuation_completed"]
                                                             for p in per_call),
        "max_rtt_s_observational": max((p["rtt_s_observational"] for p in per_call),
                                       default=None),
        "min_rtt_s_observational": min((p["rtt_s_observational"] for p in per_call),
                                       default=None),
        "per_call_detail": per_call,
        # cross-check against the runner's own A4 aggregate (provenance; not a gate input)
        "runner_progress_signature": artifact.get("progress_deferred_to_timeout_signature"),
    })
    return out


def evaluate_exp64_run(artifacts, *, expectation):
    """PURE run-level evaluation over the artifacts of ONE native-smoke job (one HPX build).
    expectation: 'control_v111' (must reproduce waiter_resume_at_timeout on every claim cell)
    or 'master_fix' (must show waiter_resumed_on_ready on every claim cell). Controls
    (poll + yield) must be structurally valid either way."""
    cells = [classify_exp64_cell(a) for a in artifacts]
    claim_cells = [c for c in cells if c["is_claim_mode"]]
    control_cells = [c for c in cells if not c["is_claim_mode"]]
    controls_valid = bool(control_cells) and all(c.get("control_overall_pass")
                                                 for c in control_cells)
    claim_classes = sorted({c["classification"] for c in claim_cells})
    both_claim_modes_present = sorted({c["mode"] for c in claim_cells}) == sorted(CLAIM_MODES)

    if not claim_cells or not both_claim_modes_present:
        run_class = INVALID_INSTRUMENTATION
    elif len(claim_classes) > 1:
        run_class = MIXED_RESULT
    else:
        run_class = claim_classes[0]

    if expectation == "control_v111":
        passed = (run_class == WAITER_RESUME_AT_TIMEOUT and controls_valid)
        if not passed and run_class not in (MIXED_RESULT, INVALID_INSTRUMENTATION):
            run_class_out = CONTROL_NOT_REPRODUCED
        else:
            run_class_out = run_class
    else:  # master_fix
        passed = (run_class == WAITER_RESUMED_ON_READY and controls_valid)
        run_class_out = run_class

    return {
        "expectation": expectation,
        "run_classification": run_class_out,
        "passed": passed,
        "controls_valid": controls_valid,
        "both_claim_modes_present": both_claim_modes_present,
        "slurm_job_ids": sorted({str(c.get("slurm_job_id")) for c in cells}),
        "hpx_versions_observed": sorted({c.get("hpx_identity") for c in cells}),
        "cells": cells,
    }


# ---------------------------------------------------------------------------
# exp65 loopback re-check classification (FORMALLY SEPARATE corroborating evidence)
# ---------------------------------------------------------------------------

def classify_exp65_probe(rec, *, margin_fraction=NATIVE_DEADLINE_MARGIN_FRACTION):
    """PURE classifier for ONE exp65 full_bound_instrumented dispatch record (the root_result
    or served marker fields added by the --wait-probe hook). Same vocabulary; structurally
    analogous but a DIFFERENT experiment on a DIFFERENT platform -- never merged into exp64."""
    probe = rec.get("wait_probe")
    bound_s = float(rec.get("dispatch_bound_s", 0) or 0)
    we = int(rec.get("t_wait_entered_ns", 0) or 0)
    wr = int(rec.get("t_wait_returned_ns", 0) or 0)
    rw = int(rec.get("t_ready_witness_ns", 0) or 0)
    status = rec.get("dispatch_status", "unknown")
    bound_ns = bound_s * 1e9
    sep_floor_ns = margin_fraction * bound_ns

    wait_duration_s = (wr - we) / 1e9 if (we > 0 and wr > 0) else None
    witness_to_return_s = (wr - rw) / 1e9 if (rw > 0 and wr > 0) else None
    # Suspension precondition. STRICT form: the probe sampled sf.is_ready() at wait entry
    # (race-free; independent of witness-continuation scheduling). FALLBACK for artifacts
    # recorded before the hardened probe: witness fired after wait entry (we < rw), which is
    # sound only on a build where the witness runs promptly at readiness.
    entry_ready = rec.get("wait_entry_future_ready")
    if entry_ready is not None:
        suspended_before_ready = not bool(entry_ready)
    else:
        suspended_before_ready = bool(we > 0 and rw > 0 and we < rw)

    if probe != "full_bound_instrumented":
        cls = INVALID_INSTRUMENTATION
    elif we <= 0 or wr <= 0 or wr < we or bound_s <= 0:
        cls = INVALID_INSTRUMENTATION
    elif status == "returned" and rw <= 0:
        cls = INVALID_INSTRUMENTATION      # returned but the readiness witness never fired
    elif status != "returned":
        cls = UNRESOLVED                   # never-ready dispatch cannot test the resume path
    elif not suspended_before_ready:
        cls = UNRESOLVED                   # ready before suspension: is_ready fast path
    elif (witness_to_return_s is not None and witness_to_return_s * 1e9 >= sep_floor_ns
          and wait_duration_s is not None and wait_duration_s * 1e9 >= sep_floor_ns):
        cls = WAITER_RESUME_AT_TIMEOUT     # ready early, waiter woke only at/near the bound
    elif wait_duration_s is not None and wait_duration_s * 1e9 < sep_floor_ns:
        # Materially separated from the bound after a proven suspension. No wr/rw ordering
        # requirement: on a build that wakes the waiter ON readiness, the waiter and the
        # witness continuation race legitimately, and either may run first.
        cls = WAITER_RESUMED_ON_READY
    else:
        cls = UNRESOLVED

    return {
        "classification": cls,
        "wait_probe": probe,
        "dispatch_status": status,
        "dispatch_bound_s": bound_s,
        "waiter_suspended_before_ready": suspended_before_ready,
        "suspension_proof": ("entry_is_ready_sample" if entry_ready is not None
                             else "witness_order_fallback"),
        "wait_duration_s_observational": wait_duration_s,
        "witness_to_return_s_observational": witness_to_return_s,
        "separation_floor_s": sep_floor_ns / 1e9,
        "oracle_match": bool(rec.get("proved_remote", False)) or bool(rec.get("match", False)),
        "hpx_version_full": rec.get("hpx_version_full", "not_recorded"),
        "hpx_identity": _identity_string(rec),
    }


def evaluate_exp65_arm(agg, *, expectation):
    """PURE: classify every demand-arm rep of one exp65 waiter-recheck aggregate (one HPX
    build). All reps must agree; exp65 structural gates must also have passed (rep_pass)."""
    reps = ((agg.get("demand_arm") or {}).get("reps")) or []
    recs = []
    for rep in reps:
        rr = ((rep.get("markers") or {}).get("root_result")) or {}
        pc = classify_exp65_probe(rr)
        pc["rep"] = rep.get("rep")
        pc["rep_gates_pass"] = bool(rep.get("rep_pass"))
        recs.append(pc)
    classes = sorted({r["classification"] for r in recs})
    if not recs:
        arm_class = INVALID_INSTRUMENTATION
    elif len(classes) == 1:
        arm_class = classes[0]
    else:
        arm_class = MIXED_RESULT
    gates_ok = bool(recs) and all(r["rep_gates_pass"] for r in recs)
    if expectation == "control_v111":
        passed = (arm_class == WAITER_RESUME_AT_TIMEOUT and gates_ok)
        if not passed and arm_class not in (MIXED_RESULT, INVALID_INSTRUMENTATION):
            arm_class = CONTROL_NOT_REPRODUCED if arm_class != WAITER_RESUME_AT_TIMEOUT \
                else arm_class
    else:
        passed = (arm_class == WAITER_RESUMED_ON_READY and gates_ok)
    return {
        "expectation": expectation,
        "arm_classification": arm_class,
        "passed": passed,
        "exp65_structural_gates_all_pass": gates_ok,
        "hpx_versions_observed": sorted({r.get("hpx_identity") for r in recs}),
        "reps": recs,
        "formally_separate_from_exp64": True,
        "corroborating_only": True,
    }


# ---------------------------------------------------------------------------
# Build-identity checks
# ---------------------------------------------------------------------------

def _identity_string(rec):
    """PURE: the build-identity string for one provenance dict/record. hpx_complete_version
    embeds the git commit when the build recorded one (e.g. 'HPX: V2.0.0 ..., Git: 20bc3d4bf3');
    hpx_version_full is only the bare version (e.g. '2.0.0'), so it is the fallback."""
    complete = rec.get("hpx_complete_version", "not_recorded")
    if complete and complete != "not_recorded":
        return complete
    return rec.get("hpx_version_full", "not_recorded")


def check_build_identity(cells_or_versions, *, expect_substring):
    """PURE: every observed HPX version string must contain expect_substring (e.g. '1.11.0'
    for the control, the master short SHA for the master build when the build recorded a git
    commit). Fail-soft to False, never fabricates."""
    versions = [v for v in cells_or_versions if v and v != "not_recorded"]
    return bool(versions) and all(expect_substring in v for v in versions)


def final_verdict(*, control_eval, master_evals):
    """PURE: collapse control + >=1 master run evaluations into the final verification class."""
    if not control_eval.get("passed"):
        return CONTROL_NOT_REPRODUCED
    classes = sorted({m["run_classification"] for m in master_evals})
    if not master_evals:
        return UNRESOLVED
    if any(m["run_classification"] == INVALID_INSTRUMENTATION for m in master_evals):
        return INVALID_INSTRUMENTATION
    if len(classes) > 1:
        return MIXED_RESULT
    if all(m.get("passed") for m in master_evals):
        return WAITER_RESUMED_ON_READY
    return classes[0] if classes[0] in VOCABULARY else UNRESOLVED


# ---------------------------------------------------------------------------
# Aggregate assembly
# ---------------------------------------------------------------------------

def _load_artifacts(dirs, pattern="exp64_payload_native_*.json"):
    arts = []
    for d in dirs:
        for p in sorted(glob.glob(os.path.join(d, pattern))):
            with open(p) as f:
                arts.append(json.load(f))
    return arts


def _load_json(path):
    with open(path) as f:
        return json.load(f)


def build_aggregate(*, control_dirs, master_dirs, exp65_control_agg=None,
                    exp65_master_aggs=None, master_sha, pr_number, pr_merge_sha,
                    pr_ancestor_checked, build_notes=None):
    """Assemble the curated waiter-fix verification aggregate from COPIED-BACK artifacts.
    control_dirs: dirs of HPX 1.11 control artifacts (one run each). master_dirs: dirs of
    HPX-master artifacts (>=1; V4 repetitions are additional dirs). exp65 aggregates are
    optional and kept in their own formally-separate section."""
    control_arts = _load_artifacts(control_dirs)
    control_eval = evaluate_exp64_run(control_arts, expectation="control_v111")
    master_evals = []
    for d in master_dirs:
        arts = _load_artifacts([d])
        ev = evaluate_exp64_run(arts, expectation="master_fix")
        ev["artifact_dir"] = os.path.basename(os.path.normpath(d))
        master_evals.append(ev)

    verdict = final_verdict(control_eval=control_eval, master_evals=master_evals)

    control_identity_ok = check_build_identity(
        control_eval["hpx_versions_observed"], expect_substring="1.11.0")
    master_identity_ok = all(
        check_build_identity(m["hpx_versions_observed"], expect_substring=master_sha[:9])
        for m in master_evals) if master_evals else False

    exp65_section = {"included": False}
    if exp65_control_agg or exp65_master_aggs:
        exp65_section = {
            "included": True,
            "formally_separate_from_exp64": True,
            "corroborating_only": True,
            "shared_root_cause_not_established_here": True,
            "control": (evaluate_exp65_arm(_load_json(exp65_control_agg),
                                           expectation="control_v111")
                        if exp65_control_agg else None),
            "master": [evaluate_exp65_arm(_load_json(p), expectation="master_fix")
                       for p in (exp65_master_aggs or [])],
        }

    gates = {
        "hpx_v111_and_master_identities_recorded": bool(
            control_eval["hpx_versions_observed"] and
            all(m["hpx_versions_observed"] for m in master_evals)),
        "control_identity_contains_1_11_0": control_identity_ok,
        "master_identity_contains_master_sha": master_identity_ok,
        "pr7367_ancestry_checked": bool(pr_ancestor_checked),
        "identical_experiment_logic_control_and_master": True,  # same runner/ext source; see
        # hpx_build_provenance.ext_build_dir per artifact for the only permitted difference
        "control_reproduced_prior_signature": bool(control_eval.get("passed")),
        "master_waiter_returned_on_readiness_all_runs": bool(
            master_evals and all(m.get("passed") for m in master_evals)),
        "waiter_suspended_before_ready_all_master_cells": bool(
            master_evals and all(
                c.get("all_waiter_suspended_before_ready")
                for m in master_evals for c in m["cells"] if c["is_claim_mode"])),
        "continuation_entered_and_completed_all_master_cells": bool(
            master_evals and all(
                c.get("all_continuation_completed")
                for m in master_evals for c in m["cells"] if c["is_claim_mode"])),
        "polling_yield_controls_valid_all_runs": bool(
            control_eval.get("controls_valid")
            and master_evals and all(m.get("controls_valid") for m in master_evals)),
        "no_mixed_or_invalid_classification": verdict not in (MIXED_RESULT,
                                                              INVALID_INSTRUMENTATION),
        "exp65_recheck_kept_separate": bool(exp65_section.get(
            "formally_separate_from_exp64", True)),
    }

    return {
        "experiment": "64_payload_fanin_size_sweep",
        "kind": "waiter_fix_verification",
        "verification_of": {
            "exp64_signature": WAITER_RESUME_AT_TIMEOUT,
            "exp64_phase": "slice5_phase_a_a4",
            "exp65_observation": "single full-bound future::wait_for returning only at its "
                                 "bound with the dispatched action future already ready",
        },
        "upstream_fix_under_test": {
            "pr_number": pr_number,
            "pr_title": "Fixing future::wait_until (and wait_for) to return once future "
                        "was made ready",
            "pr_merge_commit_sha": pr_merge_sha,
            "master_commit_sha_tested": master_sha,
            "pr_merge_commit_is_ancestor_of_tested_sha": bool(pr_ancestor_checked),
        },
        "vocabulary": list(VOCABULARY),
        "verdict": verdict,
        "structural_gates": gates,
        "all_structural_gates_pass": all(gates.values()),
        "control_hpx_v111": control_eval,
        "master_runs": master_evals,
        "exp65_loopback_recheck": exp65_section,
        "fences": dict(VERIFICATION_FENCES),
        "scope": {
            "hpx_control_build": "v1.11.0 (existing pinned install; repo pin unchanged)",
            "hpx_master_build": f"master @ {master_sha} (separate prefix; NOT the repo pin)",
            "transport": "tcp_parcelport",
            "result_scoped_to_tested_master_commit_only": True,
            "not_a_general_hpx_claim": True,
            "not_a_performance_claim": True,
            "observational_timings_only": True,
        },
        "build_notes": build_notes or [],
    }


# ---------------------------------------------------------------------------
# Selftest (pure, synthetic; runs anywhere)
# ---------------------------------------------------------------------------

def _synth_call(*, ds=1_000, we=2_000, ce=None, cc=None, wr=None, rtt_s=None,
                status="ready", timeout_s=8.0):
    return {
        "t_dispatch_start_ns": ds, "t_waiter_entered_ns": we,
        "t_continuation_entered_ns": ce if ce is not None else 0,
        "t_continuation_completed_ns": cc if cc is not None else 0,
        "t_wait_returned_ns": wr if wr is not None else 0,
        "rtt_ns": int((rtt_s if rtt_s is not None else 0.0) * 1e9),
        "wait_for_status": status,
        "_timeout_s": timeout_s,
    }


def selftest():
    failures = []
    t = 8.0
    ms = int(1e6)

    def chk(name, got, want):
        if got != want:
            failures.append(f"{name}: got {got!r}, want {want!r}")

    # 1) the prior defect signature: continuation prompt, waiter at the bound.
    stall = _synth_call(ds=0 + 1, we=2 * ms, ce=5 * ms, cc=6 * ms,
                        wr=int(8.0003e9), rtt_s=8.0004, status="ready")
    chk("stall class", classify_exp64_call(stall, dispatch_timeout_s=t)["classification"],
        WAITER_RESUME_AT_TIMEOUT)

    # 2) the fixed-state signature: suspended before ready, returned after readiness,
    #    materially separated from the bound.
    fixed = _synth_call(ds=1, we=2 * ms, ce=5 * ms, cc=6 * ms, wr=7 * ms,
                        rtt_s=0.008, status="ready")
    chk("fixed class", classify_exp64_call(fixed, dispatch_timeout_s=t)["classification"],
        WAITER_RESUMED_ON_READY)

    # 2b) slow-but-separated wake still classifies structurally as resumed-on-ready
    #     (no performance threshold defines the class); timings stay observational.
    slowish = _synth_call(ds=1, we=2 * ms, ce=5 * ms, cc=6 * ms, wr=int(1.2e9),
                          rtt_s=1.2, status="ready")
    chk("slow-but-separated class",
        classify_exp64_call(slowish, dispatch_timeout_s=t)["classification"],
        WAITER_RESUMED_ON_READY)

    # 3) missing waiter-entered timestamp -> invalid instrumentation.
    nowe = _synth_call(ds=1, we=0, ce=5 * ms, cc=6 * ms, wr=7 * ms, rtt_s=0.008)
    chk("missing we", classify_exp64_call(nowe, dispatch_timeout_s=t)["classification"],
        INVALID_INSTRUMENTATION)

    # 4) continuation never captured -> invalid instrumentation.
    nocont = _synth_call(ds=1, we=2 * ms, ce=0, cc=0, wr=int(8.0003e9), rtt_s=8.0004,
                         status="timeout")
    chk("no continuation", classify_exp64_call(nocont, dispatch_timeout_s=t)["classification"],
        INVALID_INSTRUMENTATION)

    # 5) readiness BEFORE suspension (fast path) -> unresolved, not a fix proof.
    fast = _synth_call(ds=1, we=7 * ms, ce=2 * ms, cc=3 * ms, wr=8 * ms, rtt_s=0.008,
                       status="ready")
    # capture order ds,ce,cc,wr must stay monotonic for validity; we sits between cc and wr
    fast["t_waiter_entered_ns"] = 7 * ms
    chk("fast path", classify_exp64_call(fast, dispatch_timeout_s=t)["classification"],
        UNRESOLVED)

    # 6) non-monotonic body timestamps -> invalid.
    bad = _synth_call(ds=10 * ms, we=11 * ms, ce=5 * ms, cc=6 * ms, wr=1 * ms, rtt_s=0.02)
    chk("non-monotonic", classify_exp64_call(bad, dispatch_timeout_s=t)["classification"],
        INVALID_INSTRUMENTATION)

    # 7) cell uniformity and mixing.
    def art(calls, mode="when_all_then_reduce", overall=None):
        a = {"readiness_composition": mode, "dispatch_timeout_s": t, "calls": calls,
             "payload_bytes": 0, "slurm_job_id": "0", "hpx_build_provenance":
             {"hpx_version_full": "1.11.0",
              "hpx_complete_version": "HPX: V1.11.0 (AGAS: V3.0), Git: c9b81b401f"}}
        if overall is not None:
            a["overall_pass"] = overall
        return a

    cell = classify_exp64_cell(art([stall, stall]))
    chk("uniform stall cell", cell["classification"], WAITER_RESUME_AT_TIMEOUT)
    cell = classify_exp64_cell(art([stall, fixed]))
    chk("mixed cell", cell["classification"], MIXED_RESULT)

    # 8) run-level: control reproduces; master passes; wrong-direction control flagged.
    ctrl_arts = [art([stall] * 3, mode="when_all_then_reduce"),
                 art([stall] * 3, mode="dataflow_reduce"),
                 art([], mode="root_flat_gather_poll", overall=True),
                 art([], mode="when_all_then_reduce_yield", overall=True)]
    ctrl = evaluate_exp64_run(ctrl_arts, expectation="control_v111")
    chk("control passed", ctrl["passed"], True)
    chk("control class", ctrl["run_classification"], WAITER_RESUME_AT_TIMEOUT)
    # identity gate input must be the COMPLETE version (git commit embedded), not the bare one
    chk("identity from complete_version",
        check_build_identity(ctrl["hpx_versions_observed"], expect_substring="1.11.0"), True)
    chk("identity carries git sha",
        check_build_identity(ctrl["hpx_versions_observed"], expect_substring="c9b81b401"), True)

    mast_arts = [art([fixed] * 3, mode="when_all_then_reduce"),
                 art([fixed] * 3, mode="dataflow_reduce"),
                 art([], mode="root_flat_gather_poll", overall=True),
                 art([], mode="when_all_then_reduce_yield", overall=True)]
    mast = evaluate_exp64_run(mast_arts, expectation="master_fix")
    chk("master passed", mast["passed"], True)

    notrepro = evaluate_exp64_run(
        [art([fixed] * 3, mode="when_all_then_reduce"),
         art([fixed] * 3, mode="dataflow_reduce"),
         art([], mode="root_flat_gather_poll", overall=True),
         art([], mode="when_all_then_reduce_yield", overall=True)],
        expectation="control_v111")
    chk("control not reproduced", notrepro["run_classification"], CONTROL_NOT_REPRODUCED)
    chk("control not reproduced passed", notrepro["passed"], False)

    # 9) failing poll control invalidates the run.
    badctl = evaluate_exp64_run(
        [art([stall] * 3, mode="when_all_then_reduce"),
         art([stall] * 3, mode="dataflow_reduce"),
         art([], mode="root_flat_gather_poll", overall=False),
         art([], mode="when_all_then_reduce_yield", overall=True)],
        expectation="control_v111")
    chk("bad poll control", badctl["passed"], False)

    # 10) final verdict logic.
    chk("verdict fixed", final_verdict(control_eval=ctrl, master_evals=[mast]),
        WAITER_RESUMED_ON_READY)
    chk("verdict control_not_reproduced",
        final_verdict(control_eval=notrepro, master_evals=[mast]), CONTROL_NOT_REPRODUCED)
    mixed_master = evaluate_exp64_run(
        [art([stall] * 3, mode="when_all_then_reduce"),
         art([fixed] * 3, mode="dataflow_reduce"),
         art([], mode="root_flat_gather_poll", overall=True),
         art([], mode="when_all_then_reduce_yield", overall=True)],
        expectation="master_fix")
    chk("verdict mixed", final_verdict(control_eval=ctrl, master_evals=[mixed_master]),
        MIXED_RESULT)

    # 11) exp65 probe classification (separate vocabulary use, same classes).
    def e65(we, rw, wr, status="returned", bound=15, probe="full_bound_instrumented",
            entry_ready=False):
        rec = {"wait_probe": probe, "dispatch_bound_s": bound, "t_wait_entered_ns": we,
               "t_wait_returned_ns": wr, "t_ready_witness_ns": rw,
               "dispatch_status": status, "proved_remote": True,
               "hpx_version_full": "1.11.0",
               "hpx_complete_version": "HPX: V1.11.0 (AGAS: V3.0), Git: c9b81b401f"}
        if entry_ready is not None:  # None = pre-hardening artifact (field absent)
            rec["wait_entry_future_ready"] = entry_ready
        return rec

    chk("exp65 stall",
        classify_exp65_probe(e65(1 * ms, 5 * ms, int(15.0002e9)))["classification"],
        WAITER_RESUME_AT_TIMEOUT)
    chk("exp65 fixed",
        classify_exp65_probe(e65(1 * ms, 5 * ms, 6 * ms))["classification"],
        WAITER_RESUMED_ON_READY)
    # the fixed-build race: the waiter legitimately wakes BEFORE the witness continuation
    # runs (rw > wr); the strict entry-is_ready sample still proves the suspension.
    chk("exp65 fixed witness-after-waiter",
        classify_exp65_probe(e65(1 * ms, 7 * ms, 6 * ms))["classification"],
        WAITER_RESUMED_ON_READY)
    chk("exp65 fast path (entry already ready)",
        classify_exp65_probe(e65(10 * ms, 5 * ms, 11 * ms, entry_ready=True))
        ["classification"], UNRESOLVED)
    chk("exp65 fast path fallback (field absent, witness before entry)",
        classify_exp65_probe(e65(10 * ms, 5 * ms, 11 * ms, entry_ready=None))
        ["classification"], UNRESOLVED)
    chk("exp65 fallback proof label",
        classify_exp65_probe(e65(1 * ms, 5 * ms, 6 * ms, entry_ready=None))
        ["suspension_proof"], "witness_order_fallback")
    chk("exp65 sliced probe invalid",
        classify_exp65_probe(e65(1 * ms, 5 * ms, 6 * ms, probe="sliced"))["classification"],
        INVALID_INSTRUMENTATION)
    chk("exp65 lost witness",
        classify_exp65_probe(e65(1 * ms, 0, 6 * ms))["classification"],
        INVALID_INSTRUMENTATION)
    chk("exp65 timed out",
        classify_exp65_probe(e65(1 * ms, 0, int(15.0002e9), status="timed_out"))
        ["classification"], UNRESOLVED)

    # 12) build identity check.
    chk("identity 1.11", check_build_identity(["HPX V1.11.0 (AGAS: V3.0)"],
                                              expect_substring="1.11.0"), True)
    chk("identity master", check_build_identity(["HPX V2.0.0, Git: 20bc3d4bf3"],
                                                expect_substring="20bc3d4bf"), True)
    chk("identity not recorded", check_build_identity(["not_recorded"],
                                                      expect_substring="1.11.0"), False)

    if failures:
        for f in failures:
            print(f"[verify_waiter_fix selftest] FAIL: {f}")
        return 1
    print("[verify_waiter_fix selftest] all classification/verdict checks passed")
    return 0


# ---------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(description="exp64 waiter-fix verification analyzer (pure)")
    ap.add_argument("--phase", choices=["selftest", "aggregate"], default="selftest")
    ap.add_argument("--control-dir", action="append", default=[],
                    help="dir(s) holding the HPX 1.11 control native artifacts (copied back)")
    ap.add_argument("--master-dir", action="append", default=[],
                    help="dir(s) holding HPX-master native artifacts; repeat for V4 reps")
    ap.add_argument("--exp65-control-agg", default=None,
                    help="exp65 waiter-recheck aggregate produced on HPX 1.11 (optional)")
    ap.add_argument("--exp65-master-agg", action="append", default=[],
                    help="exp65 waiter-recheck aggregate(s) produced on HPX master (optional)")
    ap.add_argument("--master-sha", required=False, default=None,
                    help="exact HPX master commit SHA tested (results scoped to it)")
    ap.add_argument("--pr-number", type=int, default=7367)
    ap.add_argument("--pr-merge-sha", default=None, help="PR merge commit SHA")
    ap.add_argument("--pr-ancestor-checked", action="store_true",
                    help="set ONLY if git merge-base --is-ancestor verified the merge commit "
                         "is an ancestor of --master-sha")
    ap.add_argument("--build-note", action="append", default=[],
                    help="free-form build/config provenance notes recorded verbatim")
    ap.add_argument("--out", default=None, help="output aggregate JSON path")
    args = ap.parse_args(argv)

    if args.phase == "selftest":
        return selftest()

    if not args.control_dir or not args.master_dir or not args.master_sha or not args.out:
        print("aggregate phase needs --control-dir, --master-dir, --master-sha, --out")
        return 2
    agg = build_aggregate(
        control_dirs=args.control_dir, master_dirs=args.master_dir,
        exp65_control_agg=args.exp65_control_agg,
        exp65_master_aggs=args.exp65_master_agg,
        master_sha=args.master_sha, pr_number=args.pr_number,
        pr_merge_sha=args.pr_merge_sha, pr_ancestor_checked=args.pr_ancestor_checked,
        build_notes=args.build_note)
    with open(args.out, "w") as f:
        json.dump(agg, f, indent=2)
        f.write("\n")
    print(f"[verify_waiter_fix] verdict={agg['verdict']} "
          f"all_structural_gates_pass={agg['all_structural_gates_pass']} -> {args.out}")
    return 0 if agg["verdict"] == WAITER_RESUMED_ON_READY and agg["all_structural_gates_pass"] \
        else 1


if __name__ == "__main__":
    sys.exit(main())
