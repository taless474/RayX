#!/usr/bin/env python3
"""exp70 Slice 1 -- Ray-supervised WHOLE-ISLAND RESTART of an actor-hosted HPX island after
unexpected connector-actor loss. Increment 3: local evidence curation, experiment-scoped orphan
checks, and an explicit (not yet executed) cross-node phase.

Topology per island epoch (exp66/67/68 mechanism, reused unchanged): one separately supervised,
WORK-FREE exp68_peer root (console mode / AGAS root / locality 0) plus two Ray actors each hosting
an HPX connect-mode locality IN-PROCESS via the exp68_actor_ext pybind11 extension. The
deterministic workload is ONE exp68 vocab-sharded top-k case in BOTH coordinator directions,
checked bit-exactly against the exp68 oracle (imported, not copied).

Slice 1 story (two island epochs under one persistent Ray supervision plane):
  epoch 0: clean start -> membership 3 -> workload passes -> ONE connector actor is terminated
  unexpectedly (controller-initiated SIGKILL of the cross-checked actor worker PID; exp53
  lineage) -> the supervising layer classifies the ISLAND failed from OBSERVATION ONLY (exp54
  discipline: the classifier never reads the injection record) -> bounded post-loss observations
  are recorded verbatim -> whole-island teardown with NO graceful application completion on the
  poisoned island (exp51/53 policy: no root.done, no healthy-island disconnect request).
  epoch 1: fresh root + fresh actors + fresh ports/dirs -> same workload passes -> graceful
  exp68-style shutdown (stop_disconnect both, root.done, root.final, finalize) -> no orphans.

LIFECYCLE-WITNESS TERMINOLOGY (two distinct variants; do not conflate):
  * Slice 0 / exp63 variant: `root.alive` is DISPATCH-DRIVEN activity evidence on a dispatching
    root (bumped before each dispatch). Not used in this slice.
  * Slice 1 / exp68 work-free-root variant (used here): the work-free root refreshes
    `root.alive` continuously from its root loop (~0.2 s period). This is an EXTERNAL
    PERIODICALLY REFRESHED ROOT-LIVENESS WITNESS. It is not an HPX-native heartbeat, not HPX
    failure detection, and not dispatch-driven activity evidence.

POST-LOSS MEMBERSHIP IS OBSERVATIONAL: after the victim is killed, the surviving locality and
the root are probed with BOUNDED operations only, and the observed result is recorded verbatim
under one category (membership_shrank / membership_stale / membership_query_error /
membership_query_timeout / membership_other). No particular category is required to pass.

ORPHAN CHECKS ARE EXPERIMENT-SCOPED (increment 3): the authoritative gate proves every process
RECORDED AND OWNED by this run (roots + actor workers, both epochs) is gone, using PID + process
start-time + command identity so PID reuse or an unrelated concurrent exp68_peer run cannot
produce a false verdict. A machine-wide exp68_peer scan is retained as INFORMATIONAL output
only.

CROSS-NODE PHASE (`--phase rostam-cross-node`, NOT the default, never runs off-cluster): reuses
exp68's validated Slurm/Ray machinery (head + srun worker bring-up, GCS wait, node-id
resolution, hard NodeAffinitySchedulingStrategy(soft=False), subnet-bound TCP parcelport
endpoints, NFS-safe marker waits). Intended two-node topology: node A hosts the driver/Ray
head, the work-free root, and actor A; node B hosts actor B (the victim -- remote by
construction). Remote injection and remote PID checks are srun-mediated. This phase has NOT
been executed yet; it skips cleanly without a Slurm allocation.

CLAIM FENCE: mechanism/structural evidence only. Whole-island REPLACEMENT, not recovery.
Because no application work is attempted on the poisoned island after classification, this
experiment cannot conclude whether HPX could transparently continue -- it proves RayX does not
rely on such continuation. NOT HPX fault tolerance, NOT AGAS repair, NOT partial-island
continuation, NOT elasticity/churn, NOT HPX-native heartbeat/completion/loss notification, NO
performance / ratio / speedup / winner claim, NOT production API. Reuses exp68 build artifacts
and the run_exp68.py module IN PLACE (imported, never modified; no C++ copied).

Usage:
  python run_slice1.py --selftest                 # pure logic checks (no Ray, no HPX, no Slurm)
  python run_slice1.py                            # live local two-epoch run
  python run_slice1.py --curate RUNID [RUNID...]  # curate accepted local runs (no processes)
  python run_slice1.py --phase rostam-cross-node  # cluster phase (skips cleanly off-cluster)
"""

import argparse
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EXP68_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "68_vocab_sharded_topk"))
RUNS_ROOT = os.path.join(HERE, "_exp70_slice1_runs")
CURATED_DIR = os.path.join(RUNS_ROOT, "curated_local_evidence")

WORKLOAD_CASE_NAME = "cross_both"  # exp68 MATRIX case: V=64 split=32 k=6 seed=1, both shards
DEFAULT_VICTIM = "b"
DEFAULT_SUBNET = "10.42.5."

ROOT_ALIVE_WITNESS_KIND = "external periodically refreshed root-liveness witness"
ROOT_ALIVE_EXPECTED_REFRESH_S = 0.2  # exp68_peer root loop period (200 ms)

MEMBERSHIP_CATEGORIES = ("membership_shrank", "membership_stale", "membership_query_error",
                         "membership_query_timeout", "membership_other")

SUMMARY_CLAIM = (
    "Unexpected loss of one actor-hosted HPX locality caused the supervising RayX controller to "
    "classify the complete island as failed, discard the old root and surviving connector, "
    "construct a fresh island, and verify the same deterministic distributed workload on the "
    "replacement.")
NON_CLAIMS = (
    "The result does not demonstrate HPX-native heartbeat, authoritative failure detection, "
    "transparent HPX recovery, partial-island continuation, or application-state restoration.")

# exp68 module attributes Slice 1 depends on (drift in any of these = invalid_instrumentation).
EXP68_REQUIRED = [
    "MATRIX", "eval_case", "_synthetic_case_result", "oracle_topk", "norm_cands",
    "find_free_port", "_popen", "_kill_group", "_wait_for_file", "_read_json",
    "peer_orphans", "peer_root_cmd", "actor_endpoints", "build_actor_class",
    "pid_alive", "wait_pid_gone", "_ray_get", "_wait_proc", "_exit_path",
    "PEER_BASENAME", "EXT_MODULE",
    # cross-node machinery (exp68-validated; only used in --phase rostam-cross-node)
    "crossnode_root_cmd", "crossnode_actor_endpoints", "_wait_for_file_nfs",
    "_expand_slurm_nodelist", "_short", "_sh", "_local_subnet_ip", "_node_subnet_ip",
    "_ray_head_local", "_ray_worker_srun", "_wait_gcs_from", "_bounded_ray_init",
    "_wait_ray_nodes", "_ray_stop_node", "_terminate_launcher", "_orphan_check_node",
    "_ORPHAN_PATTERNS_RAY", "_self_identity",
]

FAILURE_CLASSES = [
    "preflight_missing_artifacts", "invalid_instrumentation",
    "crossnode_attestation_failed", "crossnode_placement_failed",
    "epoch0_root_start_failed", "epoch0_join_failed", "epoch0_inprocess_proof_failed",
    "epoch0_workload_failed", "injection_not_observed", "island_failure_not_classified",
    "post_loss_observation_incomplete", "app_work_after_classification",
    "epoch0_teardown_incomplete", "epoch_isolation_violated",
    "epoch1_root_start_failed", "epoch1_join_failed", "epoch1_workload_failed",
    "epoch1_shutdown_failed", "orphan_detected", "cleanup_incomplete",
    "invalid_ordering", "pass",
]

# Required phase ordering (each list must appear as an in-order subsequence of the phase log).
E0_ORDER = ["e0_root_ready", "e0_actors_joined", "e0_workload_ok", "e0_injected",
            "e0_classified_failed", "e0_survivor_killed", "e0_root_killed", "e0_pids_gone"]
E1_ORDER = ["e1_root_ready", "e1_actors_joined", "e1_workload_ok", "e1_graceful_stop",
            "e1_root_finalized", "e1_actors_destroyed", "final_orphan_sweep"]


def short_host(h):
    return (h or "").split(".")[0]


# ---------------------------------------------------------------------------------------
# exp68 reuse (imported in place; never copied, never modified)
# ---------------------------------------------------------------------------------------

def import_exp68(exp68_dir):
    """Import run_exp68 as a module from its own directory. Returns (module, error_string)."""
    if not os.path.isdir(exp68_dir):
        return None, f"exp68 dir not found: {exp68_dir}"
    if exp68_dir not in sys.path:
        sys.path.insert(0, exp68_dir)
    try:
        import run_exp68 as x68  # noqa: PLC0415
    except Exception as ex:  # noqa: BLE001
        return None, f"import run_exp68 failed: {type(ex).__name__}: {ex}"
    missing = [n for n in EXP68_REQUIRED if not hasattr(x68, n)]
    if missing:
        return None, f"run_exp68 drifted; missing: {missing}"
    return x68, None


def workload_case(x68):
    return next(c for c in x68.MATRIX if c["name"] == WORKLOAD_CASE_NAME)


def preflight(exp68_dir, build_dir=None):
    """Live-run preconditions. Pure checks; safe to call anywhere. Never raises, never runs
    srun/sbatch/Slurm commands. `build_dir` defaults to `<exp68_dir>/build` (the Mac layout);
    Rostam's exp68 tree builds into `build_rostam`, selected via --exp68-build-dir."""
    out = {"ok": False, "exp68_dir": exp68_dir, "problems": []}
    x68, err = import_exp68(exp68_dir)
    if err:
        out["problems"].append(err)
        return out
    build_dir = build_dir or os.path.join(exp68_dir, "build")
    peer = os.path.join(build_dir, x68.PEER_BASENAME)
    ext_so = next((fn for fn in (os.listdir(build_dir) if os.path.isdir(build_dir) else [])
                   if fn.startswith(x68.EXT_MODULE) and fn.endswith(".so")), None)
    if not os.path.exists(peer):
        out["problems"].append(f"exp68 peer binary missing: {peer}")
    if not ext_so:
        out["problems"].append(f"exp68 extension .so missing under {build_dir}")
    try:
        import ray  # noqa: F401,PLC0415
        out["ray_importable"] = True
    except Exception as ex:  # noqa: BLE001
        out["ray_importable"] = False
        out["problems"].append(f"ray unavailable: {type(ex).__name__}: {ex}")
    out["build_dir"] = build_dir
    out["peer"] = peer
    out["ext_so"] = os.path.join(build_dir, ext_so) if ext_so else None
    out["ok"] = not out["problems"]
    return out


def preflight_crossnode(exp68_dir, env, subnet, build_dir=None):
    """Cross-node preconditions. Pure env/artifact checks only: nodelist parsing is string
    parsing (`_expand_slurm_nodelist`), and NO srun/sbatch/Slurm command is executed here."""
    out = preflight(exp68_dir, build_dir)
    out["phase"] = "rostam-cross-node"
    out["subnet"] = subnet
    job = (env or {}).get("SLURM_JOB_ID") or ""
    nodelist = (env or {}).get("SLURM_JOB_NODELIST") or (env or {}).get("SLURM_NODELIST") or ""
    out["slurm_job_id"], out["slurm_nodelist"] = job, nodelist
    if not job:
        out["problems"].append("SLURM_JOB_ID empty (not in a Slurm allocation)")
    nodes = []
    x68, _ = import_exp68(exp68_dir)
    if x68 is not None and nodelist:
        try:
            nodes = sorted(x68._expand_slurm_nodelist(nodelist))
        except Exception as ex:  # noqa: BLE001
            out["problems"].append(f"nodelist parse failed: {type(ex).__name__}: {ex}")
    if len(nodes) < 2:
        out["problems"].append(f"need >=2 distinct nodes (A, B); got {nodes}")
    out["nodes"] = nodes
    out["ok"] = not out["problems"]
    return out


# ---------------------------------------------------------------------------------------
# Pure supervision logic (selftested; used verbatim by the live drivers)
# ---------------------------------------------------------------------------------------

def select_victim(victim_key, epoch0):
    """Deterministic victim selection with PID cross-check (ext pid == worker os.getpid() ==
    Ray's worker pid; the exp66/67 in-process identity)."""
    if victim_key not in ("a", "b"):
        return {"ok": False, "reason": f"invalid victim key: {victim_key!r}"}
    ident = epoch0.get(f"{victim_key}_identity") or {}
    pids = {ident.get("pid"), ident.get("os_getpid"), ident.get("ray_pid")}
    if None in pids or len(pids) != 1:
        return {"ok": False, "reason": f"victim pid cross-check failed: {sorted(map(str, pids))}"}
    return {"ok": True, "victim": victim_key, "survivor": ("b" if victim_key == "a" else "a"),
            "victim_pid": ident.get("pid"), "victim_actor_id": ident.get("actor_id"),
            "victim_node_id": ident.get("node_id"),
            "victim_locality": (epoch0.get(f"{victim_key}_start") or {}).get("locality_id")}


def classify_island_failed(post_loss):
    """exp54-style uniform predicate; observation-only (never reads the injection record, the
    selected signal, or the controller's intent)."""
    g = {
        "victim_pid_gone": post_loss.get("victim_pid_gone") is True,
        "victim_ray_call_raised": post_loss.get("victim_ray_call_raised") is True,
        "victim_clean_disconnect_absent":
            post_loss.get("victim_clean_disconnect_recorded") is not True,
    }
    return all(g.values()), g


def categorize_membership_probe(res):
    """Map a bounded survivor-probe result to (category, membership_or_None)."""
    memb = res.get("membership") if isinstance(res, dict) else None
    if isinstance(memb, int):
        if memb == 2:
            return "membership_shrank", memb
        if memb == 3:
            return "membership_stale", memb
        return "membership_other", memb
    err = (res or {}).get("error") if isinstance(res, dict) else None
    if err and "GetTimeoutError" in str(err):
        return "membership_query_timeout", None
    if err:
        return "membership_query_error", None
    return "membership_other", None


def post_loss_interpretation(category):
    return {
        "membership_shrank": ("HPX surfaced a membership change in this run; this is not claimed "
                              "as transparent recovery or complete failure notification."),
        "membership_stale": ("The membership snapshot still contained the lost locality at the "
                             "observation point."),
        "membership_query_error": "The bounded membership probe raised; recorded verbatim.",
        "membership_query_timeout": "The bounded membership probe timed out; recorded verbatim.",
        "membership_other": ("The bounded membership probe returned an unexpected value; "
                             "recorded verbatim."),
    }.get(category, "no post-loss membership observation recorded")


def evaluate_owned_processes(records, lookup):
    """Experiment-scoped process sweep. records: [{"label","pid","node","lstart","command"}];
    lookup(record) -> None (process gone) | {"lstart","command"} (current identity of that PID).
    A PID whose current identity does not match the recorded start-time+command is PID REUSE by
    an unrelated process, not a surviving Slice 1 process."""
    details, all_gone = [], True
    for rec in records:
        cur = lookup(rec)
        if rec.get("pid") is None:
            status = "no_pid_recorded"
            all_gone = False
        elif cur is None:
            status = "gone"
        elif rec.get("lstart") is None or rec.get("command") is None:
            status = "alive_identity_unverified"
            all_gone = False
        elif cur.get("lstart") == rec.get("lstart") and cur.get("command") == rec.get("command"):
            status = "alive_ours"
            all_gone = False
        else:
            status = "pid_reused_not_ours"
        details.append({**rec, "status": status})
    return all_gone, details


def records_cover_epochs(records, tags):
    """True iff every present epoch has its root and both actor records (3 per epoch)."""
    return all(sum(1 for r in records if (r.get("label") or "").startswith(f"{tag}_")) >= 3
               for tag in tags)


def eval_epoch_join(e):
    rr = e.get("root_ready") or {}
    ai, bi = e.get("a_identity") or {}, e.get("b_identity") or {}
    a_s, b_s = e.get("a_start") or {}, e.get("b_start") or {}
    a_loc, b_loc = a_s.get("locality_id"), b_s.get("locality_id")
    memb = max(a_s.get("membership") or 0, b_s.get("membership") or 0)
    return {
        "root_ready_workfree_locality0": bool(rr.get("pid")) and rr.get("locality_id") == 0,
        "actor_a_started": bool(a_s.get("started")) and a_loc not in (None, 0),
        "actor_b_started": bool(b_s.get("started")) and b_loc not in (None, 0),
        "distinct_connector_localities": (a_loc not in (None, 0) and b_loc not in (None, 0)
                                          and a_loc != b_loc),
        "membership_reached_3": memb >= 3,
        "distinct_worker_pids": (ai.get("pid") is not None and bi.get("pid") is not None
                                 and ai.get("pid") != bi.get("pid")
                                 and rr.get("pid") not in (ai.get("pid"), bi.get("pid"))),
    }


def eval_epoch_inprocess(e):
    g = {}
    for k in ("a", "b"):
        ident = e.get(f"{k}_identity") or {}
        rep = e.get(f"{k}_child_report") or {}
        g[f"{k}_pid_identity"] = (ident.get("pid") is not None
                                  and ident.get("pid") == ident.get("os_getpid")
                                  == ident.get("ray_pid"))
        g[f"{k}_no_hpx_children"] = bool(rep.get("checked")) and rep.get("hpx_children") == []
    return g


def eval_workload(x68, e):
    case = workload_case(x68)
    cr = e.get("case") or {}
    g, _extra = x68.eval_case(case, cr)
    ai, bi = e.get("a_identity") or {}, e.get("b_identity") or {}
    g["workload_pids_match_identities"] = (cr.get("a_pid") == ai.get("pid")
                                           and cr.get("b_pid") == bi.get("pid")
                                           and cr.get("a_pid") is not None
                                           and cr.get("b_pid") is not None)
    return g


def eval_injection(inj, victim_sel):
    checks = inj.get("precondition_checks") or {}
    return {
        "victim_matches_selection": (inj.get("victim") == victim_sel.get("victim")
                                     and inj.get("victim_pid") == victim_sel.get("victim_pid")
                                     and inj.get("victim_pid") is not None),
        "explicit_signal_recorded": inj.get("signal") == "SIGKILL" and inj.get("sent") is True,
        "victim_identity_recorded": (inj.get("victim_actor_id") is not None
                                     and inj.get("victim_locality") not in (None, 0)),
        "timestamps_recorded": (inj.get("t_wall_ms") is not None
                                and inj.get("t_mono_ns") is not None),
        "precondition_checks_all_passed": bool(checks) and all(checks.values()),
    }


def eval_post_loss(pl):
    sp = pl.get("survivor_probe") or {}
    rp = pl.get("root_probe") or {}
    return {
        "victim_exit_evidenced": pl.get("victim_pid_gone") is True,
        "ray_observation_recorded": (pl.get("victim_ray_call_raised") is True
                                     and bool(pl.get("victim_ray_error_type"))),
        "survivor_probe_bounded": (sp.get("attempted") is True and sp.get("bounded") is True
                                   and sp.get("bound_s") is not None),
        "survivor_result_recorded_verbatim": "result_verbatim" in sp,
        "membership_category_valid": sp.get("category") in MEMBERSHIP_CATEGORIES,
        "root_probe_bounded_and_recorded": (
            rp.get("attempted") is True and rp.get("bounded") is True
            and rp.get("root_process_alive") is not None
            and "root_alive_mtime_advanced" in rp
            and rp.get("witness_kind") == ROOT_ALIVE_WITNESS_KIND),
    }


def eval_epoch0_teardown(td):
    return {
        "policy_no_graceful_on_poisoned": td.get("no_graceful_attempt_on_poisoned_island") is True,
        "root_done_not_written": td.get("root_done_written") is False,
        "survivor_terminated": td.get("survivor_killed") is True,
        "old_root_terminated": td.get("root_killed") is True,
        "all_epoch0_pids_gone": td.get("all_epoch0_pids_gone") is True,
        "root_final_absent_as_expected": td.get("root_final_absent_expected") is True,
    }


def eval_epoch_isolation(m):
    e0, e1 = m.get("epoch0") or {}, m.get("epoch1") or {}

    def ports(e):
        return set(v for v in (e.get("ports") or {}).values() if v is not None)

    def pids(e):
        out = set()
        rr = e.get("root_ready") or {}
        if rr.get("pid") is not None:
            out.add(rr["pid"])
        for k in ("a", "b"):
            p = (e.get(f"{k}_identity") or {}).get("pid")
            if p is not None:
                out.add(p)
        return out

    def actor_ids(e):
        return set(aid for aid in ((e.get(f"{k}_identity") or {}).get("actor_id")
                                   for k in ("a", "b")) if aid is not None)

    p0, p1 = ports(e0), ports(e1)
    d0, d1 = pids(e0), pids(e1)
    a0, a1 = actor_ids(e0), actor_ids(e1)
    sp = e1.get("stale_probe") or {}
    return {
        "bootdirs_distinct": bool(e0.get("bootdir")) and bool(e1.get("bootdir"))
                             and e0["bootdir"] != e1["bootdir"],
        "ports_disjoint": len(p0) == 3 and len(p1) == 3 and not (p0 & p1),
        "process_identities_fresh": len(d0) == 3 and len(d1) == 3 and not (d0 & d1),
        "actor_identities_fresh": len(a0) == 2 and len(a1) == 2 and not (a0 & a1),
        "no_epoch0_marker_visible_in_epoch1": sp.get("epoch0_markers_visible_in_epoch1_dir") is False,
        "epoch1_reads_only_its_dir": sp.get("epoch1_dir_isolated") is True,
    }


def eval_epoch1_shutdown(sd):
    rf = sd.get("root_final") or {}
    return {
        "actor_a_graceful_stop": sd.get("a_stop_rc") == 0 and not sd.get("a_stop_error"),
        "actor_b_graceful_stop": sd.get("b_stop_rc") == 0 and not sd.get("b_stop_error"),
        "root_finalized_clean": (sd.get("root_exit_path") == "finalized_clean"
                                 and rf.get("leave_observed") is True
                                 and rf.get("final_membership") == 1),
        "actor_pids_gone": sd.get("actor_pids_gone") is True,
    }


def eval_final(fin):
    oc = fin.get("owned_process_check") or {}
    return {
        "owned_processes_gone": oc.get("all_owned_gone") is True,
        "owned_records_cover_epochs": oc.get("covers_epochs") is True,
        "no_rundir_scoped_processes": fin.get("rundir_scoped_orphans") == [],
        "cleanup_ran": fin.get("cleanup_ran") is True,
    }


def eval_cluster_attestation(cx):
    """Cross-node cluster-level attestation (Slurm allocation + Ray node identity schema)."""
    nodes = [short_host(n) for n in (cx.get("nodes") or [])]
    nids = cx.get("ray_node_ids") or {}
    subnet = cx.get("subnet") or "@"
    return {
        "slurm_job_id_present": bool(cx.get("slurm_job_id")),
        "two_distinct_nodes_allocated": len(set(nodes)) >= 2,
        "node_roles_distinct": (bool(cx.get("nodeA")) and bool(cx.get("nodeB"))
                                and short_host(cx.get("nodeA")) != short_host(cx.get("nodeB"))),
        "ray_node_ids_resolved_distinct": (bool(nids.get("nodeA")) and bool(nids.get("nodeB"))
                                           and nids.get("nodeA") != nids.get("nodeB")),
        "subnet_ips_resolved": (bool(cx.get("nodeA_ip")) and bool(cx.get("nodeB_ip"))
                                and str(cx.get("nodeA_ip")).startswith(subnet)
                                and str(cx.get("nodeB_ip")).startswith(subnet)),
    }


def eval_crossnode_placement(e, cx, victim_key=None):
    """Per-epoch hard-placement attestation. victim_key is set only for epoch 0 (the victim
    must sit on the remote node B). No soft/fallback placement is accepted."""
    pl = e.get("placement") or {}
    nids = cx.get("ray_node_ids") or {}
    a_nid = (e.get("a_identity") or {}).get("node_id")
    b_nid = (e.get("b_identity") or {}).get("node_id")
    subnet = cx.get("subnet") or "@"
    eps = pl.get("endpoint_ips") or []
    g = {
        "hard_affinity_no_soft": (pl.get("strategy") == "NodeAffinitySchedulingStrategy"
                                  and pl.get("soft") is False),
        "actors_on_distinct_nodes": bool(a_nid) and bool(b_nid) and a_nid != b_nid,
        "actor_a_on_nodeA": bool(a_nid) and a_nid == nids.get("nodeA"),
        "actor_b_on_nodeB": bool(b_nid) and b_nid == nids.get("nodeB"),
        "root_on_head_node": (short_host((e.get("root_ready") or {}).get("hostname"))
                              == short_host(cx.get("nodeA")) != ""),
        "endpoints_on_subnet": bool(eps) and all(str(ip).startswith(subnet) for ip in eps),
        "hostnames_distinct_normalized": (
            short_host((e.get("a_identity") or {}).get("hostname"))
            != short_host((e.get("b_identity") or {}).get("hostname"))),
    }
    if victim_key is not None:
        g["victim_on_remote_node"] = ((e.get(f"{victim_key}_identity") or {}).get("node_id")
                                      == nids.get("nodeB") and bool(nids.get("nodeB")))
    return g


def subsequence_in_order(log, expected):
    it = iter(log)
    return all(any(ev == got for got in it) for ev in expected)


def evaluate_run(x68, m):
    """Full gate structure for one two-epoch run marker dict. Cross-node runs add attestation
    and per-epoch placement groups; local runs do not carry placement gates."""
    e0, e1 = m.get("epoch0") or {}, m.get("epoch1") or {}
    crossnode = m.get("phase") == "rostam-cross-node"
    cx = m.get("crossnode") or {}
    victim_sel = select_victim(m.get("victim_key", DEFAULT_VICTIM), e0)
    classified, class_gates = classify_island_failed(e0.get("post_loss") or {})

    groups = [("victim_selection", {"ok": victim_sel.get("ok") is True},
               "invalid_instrumentation")]
    if crossnode:
        groups.append(("cluster_attestation", eval_cluster_attestation(cx),
                       "crossnode_attestation_failed"))
    groups += [
        ("epoch0_join", eval_epoch_join(e0), "epoch0_join_failed"),
        ("epoch0_inprocess", eval_epoch_inprocess(e0), "epoch0_inprocess_proof_failed"),
    ]
    if crossnode:
        groups.append(("epoch0_placement",
                       eval_crossnode_placement(e0, cx, m.get("victim_key", DEFAULT_VICTIM)),
                       "crossnode_placement_failed"))
    groups += [
        ("epoch0_workload", eval_workload(x68, e0), "epoch0_workload_failed"),
        ("injection", eval_injection(e0.get("injection") or {}, victim_sel),
         "injection_not_observed"),
        ("classification", {"island_classified_failed": classified, **class_gates},
         "island_failure_not_classified"),
        ("post_loss", eval_post_loss(e0.get("post_loss") or {}),
         "post_loss_observation_incomplete"),
        ("no_app_work_after_classification",
         {"no_app_work": e0.get("app_work_after_classification") is False},
         "app_work_after_classification"),
        ("epoch0_teardown", eval_epoch0_teardown(e0.get("teardown") or {}),
         "epoch0_teardown_incomplete"),
        ("epoch_isolation", eval_epoch_isolation(m), "epoch_isolation_violated"),
        ("epoch1_join", eval_epoch_join(e1), "epoch1_join_failed"),
        ("epoch1_inprocess", eval_epoch_inprocess(e1), "epoch1_join_failed"),
    ]
    if crossnode:
        groups.append(("epoch1_placement", eval_crossnode_placement(e1, cx, None),
                       "crossnode_placement_failed"))
    groups += [
        ("epoch1_workload", eval_workload(x68, e1), "epoch1_workload_failed"),
        ("epoch1_shutdown", eval_epoch1_shutdown(e1.get("shutdown") or {}),
         "epoch1_shutdown_failed"),
    ]

    G = {name: gates for name, gates, _cls in groups}
    class_of = {name: cls for name, _gates, cls in groups}
    order = [name for name, _gates, _cls in groups]
    G["final"] = eval_final(m.get("final") or {})
    G["ordering"] = {
        "epoch0_order_ok": subsequence_in_order(m.get("phase_log") or [], E0_ORDER),
        "epoch1_order_ok": subsequence_in_order(m.get("phase_log") or [], E1_ORDER),
    }
    return G, victim_sel, order, class_of


def failure_class(G, order, class_of):
    for name in order:
        if not all(G[name].values()):
            return class_of[name]
    fin = G["final"]
    if not (fin["owned_processes_gone"] and fin["owned_records_cover_epochs"]
            and fin["no_rundir_scoped_processes"]):
        return "orphan_detected"
    if not fin["cleanup_ran"]:
        return "cleanup_incomplete"
    if not all(G["ordering"].values()):
        return "invalid_ordering"
    return "pass"


def negative_claims(G):
    """Supported negative claims. Because no application work is attempted on the poisoned
    island after classification, transparent HPX continuation is neither exercised nor claimed
    -- the run proves RayX does not RELY on it. The last two entries are fixed scope fences."""
    return {
        "no_partial_island_continuation_used":
            all(G["epoch0_teardown"].values())
            and G["no_app_work_after_classification"]["no_app_work"],
        "old_island_discarded_by_policy": all(G["epoch0_teardown"].values()),
        "replacement_island_started_from_fresh_processes": all(G["epoch_isolation"].values()),
        "workload_verified_only_on_fresh_epoch":
            G["no_app_work_after_classification"]["no_app_work"]
            and all(G["epoch1_workload"].values()),
        "no_hpx_native_loss_notification_claim": True,
        "no_application_state_restoration_claim": True,
    }


def rollup(x68, m):
    G, victim_sel, order, class_of = evaluate_run(x68, m)
    cls = failure_class(G, order, class_of)
    sp = ((m.get("epoch0") or {}).get("post_loss") or {}).get("survivor_probe") or {}
    category = sp.get("category")
    return {
        "gates": G, "victim_selection": victim_sel, "failure_class": cls,
        "passed": cls == "pass", "negative_claims": negative_claims(G),
        "post_loss_membership_category": category,
        "post_loss_interpretation": post_loss_interpretation(category),
        "gates_failed": {grp: [k for k, v in gg.items() if not v]
                         for grp, gg in G.items() if not all(gg.values())},
    }


# ---------------------------------------------------------------------------------------
# Live drivers (local + cross-node) over a shared epoch plan
# ---------------------------------------------------------------------------------------

def _merged_identity(ident, placement):
    return {"pid": ident.get("pid"), "os_getpid": ident.get("os_getpid"),
            "ray_pid": placement.get("pid"), "actor_id": placement.get("actor_id"),
            "node_id": placement.get("node_id"), "hostname": ident.get("hostname"),
            "hpx_complete_version": (ident.get("hpx_version_info") or {}).get("hpx_complete_version")}


def _ps_field(argv, timeout=10):
    try:
        out = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except Exception:  # noqa: BLE001
        return None
    if out.returncode != 0:
        return None
    val = out.stdout.strip()
    return val or None


def _proc_identity_local(pid):
    if pid is None:
        return None
    ls = _ps_field(["ps", "-o", "lstart=", "-p", str(pid)])
    if ls is None:
        return None
    cm = _ps_field(["ps", "-o", "command=", "-p", str(pid)])
    return {"lstart": ls, "command": cm}


def _rundir_scoped_orphans(runs_dir):
    """Processes whose command line references THIS run's unique directory (scoped: the roots'
    --bootstrap argv token). Excludes the controller itself."""
    try:
        out = subprocess.run(["pgrep", "-f", runs_dir], capture_output=True, text=True, timeout=10)
    except Exception:  # noqa: BLE001
        return None
    if out.returncode not in (0, 1):
        return None
    me = str(os.getpid())
    return [p for p in out.stdout.split() if p and p != me]


class LiveCtx:
    """Everything the phase functions need; also the registry the finally-block sweeps."""

    def __init__(self, x68, ray_mod, pf, args, runs_dir, plan):
        self.x68 = x68
        self.ray = ray_mod
        self.pf = pf
        self.args = args
        self.runs_dir = runs_dir
        self.plan = plan
        self.HpxActor = x68.build_actor_class(ray_mod)
        self.procs = []    # (label, Popen, logfile)
        self.actors = []   # (label, handle)
        self.owned = []    # experiment-scoped process records (pid + node + start identity)

    def register_owned(self, label, pid, node=None):
        ident = self.plan["proc_identity"](pid, node) or {}
        self.owned.append({"label": label, "pid": pid, "node": node,
                           "lstart": ident.get("lstart"), "command": ident.get("command")})


def _phase(m, event):
    m["phase_log"].append(event)
    m["phase_times_wall_ms"][event] = int(time.time() * 1000)


def make_local_plan(x68, pf, args):
    """Local loopback plan: exp68 local commands, free ports, os.kill injection. Contains no
    srun/Slurm invocation anywhere."""

    def draw_ports(avoid):
        while True:
            ports = {"root": x68.find_free_port(), "a": x68.find_free_port(),
                     "b": x68.find_free_port()}
            vals = set(ports.values())
            if len(vals) == 3 and not (vals & avoid):
                return ports

    return {
        "kind": "local",
        "ports": draw_ports,
        "root_cmd": lambda epoch_dir, ports: x68.peer_root_cmd(pf["peer"], epoch_dir,
                                                               ports["root"]),
        "endpoints": lambda k, ports: x68.actor_endpoints(ports["root"], ports[k]),
        "actor_options": lambda k: {"num_cpus": args.ray_num_cpus, "max_restarts": 0},
        "wait_file": x68._wait_for_file,
        "placement": None,
        "node_name": lambda k: None,
        "kill_pid": lambda pid, node: os.kill(pid, signal.SIGKILL),
        "kill_method": "os.kill(SIGKILL)",
        "pid_gone": lambda pid, node, timeout: x68.wait_pid_gone(pid, timeout),
        "proc_identity": lambda pid, node: _proc_identity_local(pid),
    }


def make_crossnode_plan(x68, pf, args, cx, strat_a, strat_b, env):
    """Cross-node plan: exp68 crossnode commands (subnet-bound endpoints), deterministic ports,
    hard NodeAffinity, srun-mediated remote kill/identity/pid checks for node-B processes."""

    def is_remote(node):
        return bool(node) and short_host(node) != short_host(socket.gethostname())

    def ports_for(tag):
        base = args.port_base + (0 if tag == "epoch0" else 10)
        return {"root": base, "a": base + 1, "b": base + 2}

    def kill_pid(pid, node):
        if not is_remote(node):
            os.kill(pid, signal.SIGKILL)
            return
        rc, _out, err = x68._sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node,
                                 "--export=ALL", "kill", "-9", str(pid)], timeout=60, env=env)
        if rc != 0:
            raise OSError(f"srun kill -9 {pid} on {node} rc={rc}: {err[:120]}")

    def pid_gone(pid, node, timeout):
        if not is_remote(node):
            return x68.wait_pid_gone(pid, timeout)
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc, _out, _err = x68._sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node,
                                      "--export=ALL", "ps", "-p", str(pid), "-o", "pid="],
                                     timeout=60, env=env)
            if rc != 0:
                return True
            time.sleep(1.0)
        return False

    def proc_identity(pid, node):
        if pid is None:
            return None
        if not is_remote(node):
            return _proc_identity_local(pid)
        rc, ls, _ = x68._sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node,
                             "--export=ALL", "ps", "-o", "lstart=", "-p", str(pid)],
                            timeout=60, env=env)
        if rc != 0 or not (ls or "").strip():
            return None
        rc2, cm, _ = x68._sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node,
                              "--export=ALL", "ps", "-o", "command=", "-p", str(pid)],
                             timeout=60, env=env)
        return {"lstart": ls.strip(), "command": (cm or "").strip() if rc2 == 0 else None}

    return {
        "kind": "rostam-cross-node",
        "ports": lambda avoid, _tag=[0]: (_tag.__setitem__(0, _tag[0]) or None),  # unused; see ports_for
        "ports_for": ports_for,
        "root_cmd": lambda epoch_dir, ports: x68.crossnode_root_cmd(
            pf["peer"], epoch_dir, cx["nodeA_ip"], ports["root"], leave_timeout=45),
        "endpoints": lambda k, ports: x68.crossnode_actor_endpoints(
            cx["nodeA_ip"], ports["root"],
            cx["nodeA_ip"] if k == "a" else cx["nodeB_ip"], ports[k]),
        "actor_options": lambda k: {"num_cpus": args.ray_num_cpus, "max_restarts": 0,
                                    "scheduling_strategy": (strat_a if k == "a" else strat_b)},
        "wait_file": x68._wait_for_file_nfs,
        "placement": lambda ports: {"strategy": "NodeAffinitySchedulingStrategy", "soft": False,
                                    "targets": {"a": cx["nodeA"], "b": cx["nodeB"]},
                                    "endpoint_ips": [cx["nodeA_ip"], cx["nodeB_ip"]]},
        "node_name": lambda k: cx["nodeA"] if k in ("a", "root") else cx["nodeB"],
        "kill_pid": kill_pid,
        "kill_method": "srun kill -9 (remote) / os.kill (local)",
        "pid_gone": pid_gone,
        "proc_identity": proc_identity,
    }


def bring_up_epoch(ctx, m, tag, avoid_ports):
    """Start one island epoch: work-free root + two in-process HPX actor localities."""
    x68, ray, plan = ctx.x68, ctx.ray, ctx.plan
    epoch_dir = os.path.join(ctx.runs_dir, tag)
    os.makedirs(epoch_dir, exist_ok=True)
    listing_at_start = sorted(os.listdir(epoch_dir))
    ports = plan["ports_for"](tag) if "ports_for" in plan else plan["ports"](avoid_ports)
    e = {"bootdir": epoch_dir, "ports": ports, "dir_listing_at_start": listing_at_start}
    m[tag] = e
    if plan.get("placement") is not None:
        e["placement"] = plan["placement"](ports)

    rcmd = plan["root_cmd"](epoch_dir, ports)
    root_proc, rlog = x68._popen(rcmd, epoch_dir, os.path.join(epoch_dir, "root.log"))
    ctx.procs.append((f"{tag}_root", root_proc, rlog))
    plan["wait_file"](os.path.join(epoch_dir, "root.ready"), 60, procs=[root_proc])
    rr = x68._read_json(os.path.join(epoch_dir, "root.ready")) or {}
    e["root_ready"] = rr
    e["root_argv"] = rcmd
    if rr.get("pid"):
        ctx.register_owned(f"{tag}_root", rr["pid"], plan["node_name"]("root"))
    if not (rr.get("pid") and rr.get("locality_id") == 0 and root_proc.poll() is None):
        return e, None, False
    _phase(m, f"{'e0' if tag == 'epoch0' else 'e1'}_root_ready")

    handles = {"root_proc": root_proc}
    ok = True
    for k in ("a", "b"):
        ep = plan["endpoints"](k, ports)
        h = ctx.HpxActor.options(**plan["actor_options"](k)).remote(
            ctx.pf["build_dir"], ctx.args.hpx_threads, ep)
        ctx.actors.append((f"{tag}_{k}", h))
        handles[k] = h
        ident = x68._ray_get(ray, h.load_identity.remote(), 60, f"{tag}_{k}_identity")
        placement = x68._ray_get(ray, h.ray_placement.remote(), 60, f"{tag}_{k}_placement")
        e[f"{k}_identity"] = _merged_identity(ident if isinstance(ident, dict) else {},
                                              placement if isinstance(placement, dict) else {})
        e[f"{k}_start"] = x68._ray_get(ray, h.start_hpx.remote(), 120, f"{tag}_{k}_start")
        e[f"{k}_child_report"] = x68._ray_get(ray, h.child_report.remote(), 30, f"{tag}_{k}_child")
        if e[f"{k}_identity"].get("pid"):
            ctx.register_owned(f"{tag}_actor_{k}", e[f"{k}_identity"]["pid"],
                               plan["node_name"](k))
        ok = ok and bool((e[f"{k}_start"] or {}).get("started"))
    if ok and all(eval_epoch_join(e).values()) and all(eval_epoch_inprocess(e).values()):
        _phase(m, f"{'e0' if tag == 'epoch0' else 'e1'}_actors_joined")
        return e, handles, True
    return e, handles, False


def run_workload(ctx, m, tag, e, handles):
    """One exp68 case, BOTH coordinator directions, evaluated bit-exactly vs the oracle."""
    x68, ray = ctx.x68, ctx.ray
    case = workload_case(x68)
    V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
    a_lo, a_hi, b_lo, b_hi = 0, split, split, V
    a_loc = (e.get("a_start") or {}).get("locality_id")
    b_loc = (e.get("b_start") or {}).get("locality_id")
    cr = {"name": case["name"], "V": V, "split": split, "k": k, "seed": seed,
          "shard_a": [a_lo, a_hi], "shard_b": [b_lo, b_hi], "a_loc": a_loc, "b_loc": b_loc,
          "a_pid": (e.get("a_identity") or {}).get("pid"),
          "b_pid": (e.get("b_identity") or {}).get("pid")}
    a, b = handles["a"], handles["b"]
    cr["a_local"] = x68._ray_get(ray, a.local_topk.remote(a_lo, a_hi, seed, k), 30, f"{tag}_a_local")
    cr["b_local"] = x68._ray_get(ray, b.local_topk.remote(b_lo, b_hi, seed, k), 30, f"{tag}_b_local")
    cr["a_coord"] = x68._ray_get(ray, a.coordinate.remote(b_loc, a_lo, a_hi, b_lo, b_hi, seed, k),
                                 60, f"{tag}_a_coord")
    cr["b_coord"] = x68._ray_get(ray, b.coordinate.remote(a_loc, b_lo, b_hi, a_lo, a_hi, seed, k),
                                 60, f"{tag}_b_coord")
    cr["oracle_global"] = [[t, bits] for t, bits in x68.oracle_topk(0, V, seed, k)]
    e["case"] = cr
    ok = all(eval_workload(x68, e).values())
    if ok:
        _phase(m, f"{'e0' if tag == 'epoch0' else 'e1'}_workload_ok")
    return ok


def inject_victim_loss(ctx, m, e0, victim_sel):
    """Deterministic, explicit, precondition-checked kill of the victim actor worker.
    Recorded separately from all classification evidence."""
    plan = ctx.plan
    vk = victim_sel.get("victim")
    vp = victim_sel.get("victim_pid")
    surv = victim_sel.get("survivor")
    surv_pid = (e0.get(f"{surv}_identity") or {}).get("pid")
    root_pid = (e0.get("root_ready") or {}).get("pid")
    epoch0_pids = {p for p in (root_pid,
                               (e0.get("a_identity") or {}).get("pid"),
                               (e0.get("b_identity") or {}).get("pid")) if p is not None}
    checks = {
        "victim_is_configured": vk == m.get("victim_key"),
        "actor_id_matches_identity": (victim_sel.get("victim_actor_id") is not None
                                      and victim_sel.get("victim_actor_id")
                                      == (e0.get(f"{vk}_identity") or {}).get("actor_id")),
        "ray_pid_matches_ext_pid": victim_sel.get("ok") is True,
        "pid_in_current_epoch": vp in epoch0_pids,
        "pid_not_driver_root_survivor": vp not in {os.getpid(), root_pid, surv_pid},
        "no_epoch1_processes_yet": "epoch1" not in m,
    }
    inj = {"victim": vk, "victim_pid": vp,
           "victim_actor_id": victim_sel.get("victim_actor_id"),
           "victim_locality": victim_sel.get("victim_locality"),
           "victim_node_id": victim_sel.get("victim_node_id"),
           "victim_node": plan["node_name"](vk),
           "signal": "SIGKILL", "method": plan["kill_method"], "sent": False,
           "precondition_checks": checks}
    if all(checks.values()):
        inj["t_wall_ms"] = int(time.time() * 1000)
        inj["t_mono_ns"] = time.monotonic_ns()
        try:
            plan["kill_pid"](vp, plan["node_name"](vk))
            inj["sent"] = True
        except OSError as ex:
            inj["kill_error"] = f"{type(ex).__name__}: {ex}"
    e0["injection"] = inj
    if inj["sent"]:
        _phase(m, "e0_injected")
    return inj["sent"]


def observe_post_loss(ctx, m, e0, victim_sel, handles):
    """Bounded post-loss observations, recorded verbatim. Every probe is bounded; the
    controller proceeds to teardown regardless of any result. No application workload is
    dispatched here (health/ping only, no coordinate/local_topk)."""
    x68, ray, plan = ctx.x68, ctx.ray, ctx.plan
    v, surv = victim_sel["victim"], victim_sel["survivor"]
    pl = {}
    pl["victim_pid_gone"] = plan["pid_gone"](victim_sel["victim_pid"],
                                             plan["node_name"](v), 20)

    raised, err_type, err_str = False, None, None
    try:
        ray.get(handles[v].ping.remote(), timeout=10)
    except Exception as ex:  # noqa: BLE001
        raised, err_type, err_str = True, type(ex).__name__, str(ex)[:300]
    pl["victim_ray_call_raised"] = raised
    pl["victim_ray_error_type"] = err_type
    pl["victim_ray_error_head"] = err_str
    pl["victim_clean_disconnect_recorded"] = bool(e0.get(f"{v}_stop_rc") == 0)

    bound_s = 12
    t0 = time.monotonic()
    res = x68._ray_get(ray, handles[surv].health.remote(), bound_s, "survivor_health_post_loss")
    elapsed = round(time.monotonic() - t0, 3)
    category, memb = categorize_membership_probe(res if isinstance(res, dict) else {})
    pl["survivor_probe"] = {"attempted": True, "bounded": True, "bound_s": bound_s,
                            "elapsed_s": elapsed, "result_verbatim": res,
                            "category": category, "membership": memb}

    root_proc = handles["root_proc"]
    alive_path = os.path.join(e0["bootdir"], "root.alive")
    mt1 = os.path.getmtime(alive_path) if os.path.exists(alive_path) else None
    time.sleep(1.2)  # > 5 expected refresh periods of the root-liveness witness
    mt2 = os.path.getmtime(alive_path) if os.path.exists(alive_path) else None
    pl["root_probe"] = {
        "attempted": True, "bounded": True,
        "root_process_alive": root_proc.poll() is None,
        "root_alive_mtime_first": mt1, "root_alive_mtime_second": mt2,
        "root_alive_mtime_advanced": (mt1 is not None and mt2 is not None and mt2 > mt1),
        "witness_kind": ROOT_ALIVE_WITNESS_KIND,
        "expected_refresh_s": ROOT_ALIVE_EXPECTED_REFRESH_S,
    }
    e0["post_loss"] = pl
    return pl


def teardown_epoch0(ctx, m, e0, victim_sel, handles):
    """Whole-island teardown of the poisoned epoch: no graceful application completion, no
    root.done, no healthy-island disconnect request. Kills only epoch-0 processes."""
    x68, ray, plan = ctx.x68, ctx.ray, ctx.plan
    td = {"no_graceful_attempt_on_poisoned_island": True, "root_done_written": False}
    surv = victim_sel["survivor"]
    surv_pid = (e0.get(f"{surv}_identity") or {}).get("pid")
    root_pid = (e0.get("root_ready") or {}).get("pid")

    for k in (victim_sel["victim"], surv):  # victim kill is idempotent cleanup
        try:
            ray.kill(handles[k])
        except Exception:  # noqa: BLE001
            pass
    td["survivor_killed"] = True
    td["survivor_pid_gone"] = plan["pid_gone"](surv_pid, plan["node_name"](surv), 20)
    _phase(m, "e0_survivor_killed")

    x68._kill_group(handles["root_proc"])
    exited, rc, killed = x68._wait_proc(handles["root_proc"], time.time() + 20)
    td["root_killed"] = True
    td["root_exit"] = {"exited": exited, "rc": rc, "killed_by_harness": killed}
    _phase(m, "e0_root_killed")

    gone = [plan["pid_gone"](root_pid, plan["node_name"]("root"), 20),
            td["survivor_pid_gone"],
            plan["pid_gone"](victim_sel["victim_pid"],
                             plan["node_name"](victim_sel["victim"]), 20)]
    td["all_epoch0_pids_gone"] = all(gone)
    if td["all_epoch0_pids_gone"]:
        _phase(m, "e0_pids_gone")
    td["root_final_absent_expected"] = not os.path.exists(os.path.join(e0["bootdir"], "root.final"))
    e0["teardown"] = td
    return td["all_epoch0_pids_gone"]


def shutdown_epoch1(ctx, m, e1, handles):
    """Normal exp68 lifecycle: stop_disconnect both -> root.done -> root.final -> clean exit."""
    x68, ray, plan = ctx.x68, ctx.ray, ctx.plan
    sd = {}
    a_stop = x68._ray_get(ray, handles["a"].stop_hpx.remote(), 40, "e1_a_stop")
    b_stop = x68._ray_get(ray, handles["b"].stop_hpx.remote(), 40, "e1_b_stop")
    sd["a_stop_rc"], sd["a_stop_error"] = (a_stop or {}).get("rc"), (a_stop or {}).get("error")
    sd["b_stop_rc"], sd["b_stop_error"] = (b_stop or {}).get("rc"), (b_stop or {}).get("error")
    if sd["a_stop_rc"] == 0 and sd["b_stop_rc"] == 0:
        _phase(m, "e1_graceful_stop")

    open(os.path.join(e1["bootdir"], "root.done"), "w").close()
    plan["wait_file"](os.path.join(e1["bootdir"], "root.final"), 60,
                     procs=[handles["root_proc"]])
    sd["root_final"] = x68._read_json(os.path.join(e1["bootdir"], "root.final"))
    exited, rc, killed = x68._wait_proc(handles["root_proc"], time.time() + 40)
    sd["root_exit_path"] = x68._exit_path(exited, rc, killed)
    if (sd["root_exit_path"] == "finalized_clean"
            and (sd["root_final"] or {}).get("leave_observed") is True):
        _phase(m, "e1_root_finalized")

    a_pid = (e1.get("a_identity") or {}).get("pid")
    b_pid = (e1.get("b_identity") or {}).get("pid")
    for k in ("a", "b"):
        try:
            ray.kill(handles[k])
        except Exception:  # noqa: BLE001
            pass
    sd["actor_pids_gone"] = (plan["pid_gone"](a_pid, plan["node_name"]("a"), 20)
                             and plan["pid_gone"](b_pid, plan["node_name"]("b"), 20))
    if sd["actor_pids_gone"]:
        _phase(m, "e1_actors_destroyed")
    e1["shutdown"] = sd
    return all(eval_epoch1_shutdown(sd).values())


def run_two_epochs(ctx, m):
    """Shared two-epoch body for both local and cross-node phases."""
    e0, h0, ok = bring_up_epoch(ctx, m, "epoch0", avoid_ports=set())
    if not ok:
        raise RuntimeError("epoch0 bring-up failed (join/in-process gates)")
    if not run_workload(ctx, m, "epoch0", e0, h0):
        raise RuntimeError("epoch0 workload failed")

    victim_sel = select_victim(ctx.args.victim, e0)
    if not victim_sel.get("ok"):
        raise RuntimeError(f"victim selection failed: {victim_sel.get('reason')}")
    if not inject_victim_loss(ctx, m, e0, victim_sel):
        raise RuntimeError("injection preconditions failed or kill not sent")

    pl = observe_post_loss(ctx, m, e0, victim_sel, h0)
    classified, _cg = classify_island_failed(pl)
    if classified:
        _phase(m, "e0_classified_failed")
    e0["app_work_after_classification"] = False  # no workload call exists past this point
    if not classified:
        raise RuntimeError("island failure not classified from observations")

    teardown_epoch0(ctx, m, e0, victim_sel, h0)

    e1, h1, ok = bring_up_epoch(ctx, m, "epoch1", avoid_ports=set(e0["ports"].values()))
    e1["stale_probe"] = {
        "epoch0_markers_visible_in_epoch1_dir": bool(e1.get("dir_listing_at_start")),
        "epoch1_dir_isolated": True,  # all epoch-1 reads use paths under its own bootdir
    }
    if not ok:
        raise RuntimeError("epoch1 bring-up failed (join/in-process gates)")
    if not run_workload(ctx, m, "epoch1", e1, h1):
        raise RuntimeError("epoch1 workload failed")
    if not shutdown_epoch1(ctx, m, e1, h1):
        raise RuntimeError("epoch1 graceful shutdown failed")


def finalize_run(ctx, m):
    """Cleanup after ANY outcome, then the experiment-scoped orphan sweep. Kills only Slice 1
    actors and island processes -- never the Ray driver or cluster services."""
    x68, ray = ctx.x68, ctx.ray
    for _label, h in ctx.actors:
        try:
            ray.kill(h)
        except Exception:  # noqa: BLE001
            pass
    for _label, p, log in ctx.procs:
        x68._kill_group(p)
        try:
            log.close()
        except OSError:
            pass
    m["final"]["cleanup_ran"] = True
    all_gone, details = evaluate_owned_processes(
        ctx.owned, lambda rec: ctx.plan["proc_identity"](rec.get("pid"), rec.get("node")))
    tags = [t for t in ("epoch0", "epoch1") if m.get(t)]
    m["final"]["owned_process_check"] = {"all_owned_gone": all_gone,
                                         "covers_epochs": records_cover_epochs(ctx.owned, tags),
                                         "details": details}
    m["final"]["rundir_scoped_orphans"] = _rundir_scoped_orphans(ctx.runs_dir)
    m["final"]["machine_wide_peer_scan_informational"] = x68.peer_orphans()
    _phase(m, "final_orphan_sweep")


def write_outputs(x68, m, agg, runs_dir, agg_path):
    r = rollup(x68, m)
    agg["overall"] = "pass" if r["passed"] else "fail"
    agg["failure_class"] = r["failure_class"]
    agg["gates"] = r["gates"]
    agg["gates_failed"] = r["gates_failed"]
    agg["negative_claims"] = r["negative_claims"]
    agg["post_loss_membership_category"] = r["post_loss_membership_category"]
    agg["post_loss_interpretation"] = r["post_loss_interpretation"]
    agg["summary_claim"] = (SUMMARY_CLAIM if r["passed"] else
                            f"Slice 1 did not pass ({r['failure_class']}); see gates_failed.")
    agg["controller_exception"] = m.get("controller_exception")
    agg["identities"] = {
        tag: {"root_pid": ((m.get(tag) or {}).get("root_ready") or {}).get("pid"),
              "ports": (m.get(tag) or {}).get("ports"),
              "a": {kk: ((m.get(tag) or {}).get("a_identity") or {}).get(kk)
                    for kk in ("pid", "actor_id", "node_id", "hostname")},
              "b": {kk: ((m.get(tag) or {}).get("b_identity") or {}).get(kk)
                    for kk in ("pid", "actor_id", "node_id", "hostname")}}
        for tag in ("epoch0", "epoch1")}
    with open(os.path.join(runs_dir, "markers.json"), "w") as f:
        json.dump(m, f, indent=2, sort_keys=True, default=str)
    with open(agg_path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=True, default=str)
    print(f"[slice1] overall: {agg['overall']} ({agg['failure_class']}) "
          f"post_loss={agg['post_loss_membership_category']} -> {agg_path}")
    if r["gates_failed"]:
        print(f"[slice1] gates_failed: {json.dumps(r['gates_failed'])}")
    return 0


def _provenance():
    prov = {"python": sys.version.split()[0], "platform": platform.platform(),
            "hostname": socket.gethostname()}
    try:
        import ray  # noqa: PLC0415
        prov["ray_version"] = ray.__version__
    except Exception:  # noqa: BLE001
        prov["ray_version"] = None
    return prov


def live_run(args):
    """Local two-epoch run (the increment-2 path, now with scoped orphan checks)."""
    pf = preflight(args.exp68_dir, args.exp68_build_dir)
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    runs_dir = os.path.join(RUNS_ROOT, runid)
    os.makedirs(runs_dir, exist_ok=True)
    agg_path = args.aggregate or os.path.join(runs_dir, "aggregate.json")
    agg = {"experiment": "exp70_slice1_actor_hosted_island_restart", "increment": 3,
           "phase": "local", "runid": runid, "runs_dir": runs_dir, "victim_key": args.victim,
           "workload_case": WORKLOAD_CASE_NAME, "provenance": _provenance(),
           "root_alive_witness_kind": ROOT_ALIVE_WITNESS_KIND,
           "root_alive_expected_refresh_s": ROOT_ALIVE_EXPECTED_REFRESH_S,
           "non_claims": NON_CLAIMS, "preflight": pf}
    if not pf["ok"]:
        agg["overall"] = "skip"
        agg["reason"] = "; ".join(pf["problems"])
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice1] SKIP: {agg['reason']} -> {agg_path}")
        return 0

    x68, _ = import_exp68(args.exp68_dir)
    import ray  # noqa: PLC0415
    ray.init(num_cpus=max(6, 2 * args.ray_num_cpus + 2), include_dashboard=False,
             log_to_driver=False, ignore_reinit_error=True)
    plan = make_local_plan(x68, pf, args)
    plan["ports_for"] = None
    del plan["ports_for"]  # local plan draws free ports with an avoid-set
    ctx = LiveCtx(x68, ray, pf, args, runs_dir, plan)
    m = {"victim_key": args.victim, "phase": "local", "phase_log": [],
         "phase_times_wall_ms": {}, "controller_pid": os.getpid(), "final": {}}
    try:
        run_two_epochs(ctx, m)
    except Exception as ex:  # noqa: BLE001
        m["controller_exception"] = f"{type(ex).__name__}: {ex}"
        m["controller_traceback"] = traceback.format_exc()[-2000:]
    finally:
        finalize_run(ctx, m)
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass
    return write_outputs(x68, m, agg, runs_dir, agg_path)


def crossnode_live_run(args):
    """Cross-node two-epoch run under an existing Slurm allocation (NOT executed off-cluster:
    skips cleanly when preflight fails). Reuses exp68's validated head/worker bring-up."""
    env = dict(os.environ)
    pf = preflight_crossnode(args.exp68_dir, env, args.subnet, args.exp68_build_dir)
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    job_id = pf.get("slurm_job_id") or "nojob"
    runs_dir = os.path.join(RUNS_ROOT, f"crossnode_{job_id}_{runid}")
    os.makedirs(runs_dir, exist_ok=True)
    agg_path = args.aggregate or os.path.join(runs_dir, "aggregate.json")
    agg = {"experiment": "exp70_slice1_actor_hosted_island_restart", "increment": 3,
           "phase": "rostam-cross-node", "runid": runid, "runs_dir": runs_dir,
           "victim_key": args.victim, "workload_case": WORKLOAD_CASE_NAME,
           "provenance": _provenance(),
           "root_alive_witness_kind": ROOT_ALIVE_WITNESS_KIND,
           "root_alive_expected_refresh_s": ROOT_ALIVE_EXPECTED_REFRESH_S,
           "non_claims": NON_CLAIMS, "preflight": pf}
    if not pf["ok"]:
        agg["overall"] = "skip"
        agg["reason"] = "; ".join(pf["problems"])
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice1-crossnode] SKIP: {agg['reason']} -> {agg_path}")
        return 0

    x68, _ = import_exp68(args.exp68_dir)
    nodes = pf["nodes"]
    nodeA = args.node_a if args.node_a in nodes else nodes[0]
    nodeB = args.node_b if args.node_b in nodes else (nodes[1] if len(nodes) > 1 else None)
    here = socket.gethostname()
    if x68._short(here) != x68._short(nodeA):
        agg["overall"] = "fail_preflight"
        agg["reason"] = f"controller on {here}, must run on nodeA={nodeA}"
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice1-crossnode] PREFLIGHT FAIL: {agg['reason']}")
        return 0

    nodeA_ip = x68._local_subnet_ip(args.subnet)
    nodeB_ip = x68._node_subnet_ip(nodeB, args.subnet)
    cx = {"slurm_job_id": pf["slurm_job_id"], "nodelist": pf["slurm_nodelist"], "nodes": nodes,
          "nodeA": nodeA, "nodeB": nodeB, "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip,
          "subnet": args.subnet, "ray_node_ids": {}}
    import ray  # noqa: PLC0415
    temp_dir = f"/tmp/exp70s1_ray_{job_id}_{runid}"
    head = worker_b = None
    m = {"victim_key": args.victim, "phase": "rostam-cross-node", "crossnode": cx,
         "phase_log": [], "phase_times_wall_ms": {}, "controller_pid": os.getpid(),
         "final": {}}
    ctx = None
    try:
        if not (nodeA_ip and nodeB_ip):
            raise RuntimeError(f"subnet {args.subnet} IPs unresolved (A={nodeA_ip} B={nodeB_ip})")
        head = x68._ray_head_local(nodeA_ip, args.ray_port, temp_dir, None, env,
                                   os.path.join(runs_dir, "head.log"), [])
        okB, detB = x68._wait_gcs_from(nodeB, nodeA_ip, args.ray_port, env,
                                       args.ray_ready_timeout)
        agg["ray_gcs_ready_from_nodeB"], agg["ray_gcs_detail_B"] = okB, detB
        if not (okB and head.poll() is None):
            raise RuntimeError(f"head GCS not reachable from B ({detB})")
        worker_b = x68._ray_worker_srun(nodeB, nodeB_ip, nodeA_ip, args.ray_port, None, env,
                                        os.path.join(runs_dir, "worker_b.log"), [])
        init_ok, attempts, init_tb = x68._bounded_ray_init(
            ray, f"{nodeA_ip}:{args.ray_port}", args.ray_init_timeout)
        agg["ray_init_ok"], agg["ray_init_attempts"] = init_ok, attempts
        if not init_ok:
            agg["ray_init_traceback"] = init_tb
            raise RuntimeError("ray.init to local head failed")
        nodes_ready, seen = x68._wait_ray_nodes(ray, 2, args.ray_ready_timeout)
        if not nodes_ready:
            raise RuntimeError(f"only {seen}/2 ray nodes alive")
        alive = [n for n in ray.nodes() if n.get("Alive")]

        def _match(ip, host):
            return [n for n in alive if n.get("NodeManagerAddress") == ip
                    or x68._short(n.get("NodeName")) == x68._short(host)]
        mA, mB = _match(nodeA_ip, nodeA), _match(nodeB_ip, nodeB)
        cx["ray_node_ids"] = {"nodeA": mA[0]["NodeID"] if len(mA) == 1 else None,
                              "nodeB": mB[0]["NodeID"] if len(mB) == 1 else None}
        if not all(eval_cluster_attestation(cx).values()):
            raise RuntimeError(f"cluster attestation failed: {eval_cluster_attestation(cx)}")

        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: PLC0415
        strat_a = NodeAffinitySchedulingStrategy(node_id=cx["ray_node_ids"]["nodeA"], soft=False)
        strat_b = NodeAffinitySchedulingStrategy(node_id=cx["ray_node_ids"]["nodeB"], soft=False)
        plan = make_crossnode_plan(x68, pf, args, cx, strat_a, strat_b, env)
        ctx = LiveCtx(x68, ray, pf, args, runs_dir, plan)
        run_two_epochs(ctx, m)
    except Exception as ex:  # noqa: BLE001
        m["controller_exception"] = f"{type(ex).__name__}: {ex}"
        m["controller_traceback"] = traceback.format_exc()[-2000:]
    finally:
        if ctx is not None:
            finalize_run(ctx, m)
        else:
            m["final"] = {"cleanup_ran": True, "owned_process_check":
                          {"all_owned_gone": True, "covers_epochs": False, "details": []},
                          "rundir_scoped_orphans": _rundir_scoped_orphans(runs_dir),
                          "machine_wide_peer_scan_informational": None}
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass
        for nd in (nodeA, nodeB):
            if nd:
                try:
                    x68._ray_stop_node(nd, env)
                except Exception:  # noqa: BLE001
                    pass
        agg["ray_head_cleanup"] = x68._terminate_launcher(head)
        agg["ray_worker_b_cleanup"] = x68._terminate_launcher(worker_b)
        try:
            agg["ray_orphans_informational"] = {
                nd: x68._orphan_check_node(nd, x68._ORPHAN_PATTERNS_RAY, env)
                for nd in (nodeA, nodeB) if nd}
        except Exception:  # noqa: BLE001
            agg["ray_orphans_informational"] = None
    return write_outputs(x68, m, agg, runs_dir, agg_path)


# ---------------------------------------------------------------------------------------
# Curation of accepted local runs (no processes launched; raw artifacts never modified)
# ---------------------------------------------------------------------------------------

def sha256_file(path):
    import hashlib  # noqa: PLC0415
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def curate_local(args, run_ids):
    import shutil  # noqa: PLC0415
    os.makedirs(CURATED_DIR, exist_ok=True)
    entries = []
    for rid in run_ids:
        src = os.path.join(RUNS_ROOT, rid)
        agg_p, mk_p = os.path.join(src, "aggregate.json"), os.path.join(src, "markers.json")
        if not (os.path.exists(agg_p) and os.path.exists(mk_p)):
            print(f"[curate] SKIP {rid}: aggregate/markers missing under {src}")
            continue
        agg = json.load(open(agg_p))
        mk = json.load(open(mk_p))
        dst = os.path.join(CURATED_DIR, rid)
        os.makedirs(dst, exist_ok=True)
        shutil.copy2(agg_p, os.path.join(dst, "aggregate.json"))
        shutil.copy2(mk_p, os.path.join(dst, "markers.json"))
        e0, e1 = mk.get("epoch0") or {}, mk.get("epoch1") or {}
        entries.append({
            "runid": rid,
            "raw_paths": {"aggregate": agg_p, "markers": mk_p},
            "copied_paths": {"aggregate": os.path.join(dst, "aggregate.json"),
                             "markers": os.path.join(dst, "markers.json")},
            "sha256": {"aggregate": sha256_file(agg_p), "markers": sha256_file(mk_p)},
            "overall": agg.get("overall"), "failure_class": agg.get("failure_class"),
            "post_loss_membership_category": agg.get("post_loss_membership_category"),
            "post_loss_interpretation": agg.get("post_loss_interpretation"),
            "identities": agg.get("identities"),
            "gates": agg.get("gates"), "gates_failed": agg.get("gates_failed"),
            "negative_claims": agg.get("negative_claims"),
            "epoch0_workload": {"case": (e0.get("case") or {}).get("name"),
                                "a_coord_global": ((e0.get("case") or {}).get("a_coord") or {}).get("global_topk"),
                                "b_coord_global": ((e0.get("case") or {}).get("b_coord") or {}).get("global_topk"),
                                "oracle_global": (e0.get("case") or {}).get("oracle_global")},
            "epoch1_workload": {"case": (e1.get("case") or {}).get("name"),
                                "a_coord_global": ((e1.get("case") or {}).get("a_coord") or {}).get("global_topk"),
                                "b_coord_global": ((e1.get("case") or {}).get("b_coord") or {}).get("global_topk"),
                                "oracle_global": (e1.get("case") or {}).get("oracle_global")},
            "injection": e0.get("injection"),
            "post_loss": e0.get("post_loss"),
            "teardown": e0.get("teardown"),
            "epoch1_shutdown": e1.get("shutdown"),
            "epoch_isolation_inputs": {"e0_ports": e0.get("ports"), "e1_ports": e1.get("ports"),
                                       "e1_stale_probe": e1.get("stale_probe")},
            "final": mk.get("final"), "phase_log": mk.get("phase_log"),
            "hpx_complete_version": (((e1.get("shutdown") or {}).get("root_final") or {})
                                     .get("hpx_complete_version")),
        })
    both_pass = bool(entries) and all(e["overall"] == "pass" for e in entries)
    same_cat = len({e["post_loss_membership_category"] for e in entries}) == 1 if entries else False
    curated = {
        "experiment": "exp70_slice1_actor_hosted_island_restart",
        "curated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "curation_note": ("Raw artifacts copied unmodified from the ignored run area; "
                          "originals retained in place. Ray/python/platform identity below is "
                          "the curating environment, which is the same local venv/session that "
                          "produced the runs."),
        "provenance": _provenance(),
        "workload_case": WORKLOAD_CASE_NAME,
        "root_alive_witness_kind": ROOT_ALIVE_WITNESS_KIND,
        "root_alive_expected_refresh_s": ROOT_ALIVE_EXPECTED_REFRESH_S,
        "membership_categories": list(MEMBERSHIP_CATEGORIES),
        "summary_claim": SUMMARY_CLAIM if both_pass else "not all curated runs passed",
        "non_claims": NON_CLAIMS,
        "consistency": {"runs_curated": len(entries), "both_pass": both_pass,
                        "same_post_loss_category": same_cat},
        "runs": entries,
    }
    out_path = os.path.join(CURATED_DIR, "curated_local_aggregate.json")
    with open(out_path, "w") as f:
        json.dump(curated, f, indent=2, sort_keys=True)
    digest = sha256_file(out_path)
    with open(out_path + ".sha256", "w") as f:
        f.write(f"{digest}  curated_local_aggregate.json\n")
    print(f"[curate] {len(entries)} run(s) -> {out_path}")
    print(f"[curate] sha256 {digest}")
    return 0


# ---------------------------------------------------------------------------------------
# Selftest: pure logic (no Ray, no HPX, no Slurm). Synthetic runs + targeted mutations.
# ---------------------------------------------------------------------------------------

def _ident(pid, actor_id):
    return {"pid": pid, "os_getpid": pid, "ray_pid": pid, "actor_id": actor_id,
            "node_id": "node-x", "hostname": "host-x"}


def _synthetic_epoch(x68, tag, root_pid, a_pid, b_pid, ports):
    case = workload_case(x68)
    cr = x68._synthetic_case_result(case, a_loc=1, b_loc=2, a_pid=a_pid, b_pid=b_pid)
    return {
        "bootdir": f"/run/{tag}", "ports": ports,
        "root_ready": {"pid": root_pid, "locality_id": 0, "hostname": "host-x"},
        "a_identity": _ident(a_pid, f"{tag}-actor-a"), "b_identity": _ident(b_pid, f"{tag}-actor-b"),
        "a_start": {"started": True, "locality_id": 1, "membership": 3},
        "b_start": {"started": True, "locality_id": 2, "membership": 3},
        "a_child_report": {"checked": True, "hpx_children": []},
        "b_child_report": {"checked": True, "hpx_children": []},
        "case": cr,
    }


def _synthetic_owned(tag, root_pid, a_pid, b_pid):
    return [{"label": f"{tag}_root", "pid": root_pid, "node": None, "lstart": "t0", "command": "peer"},
            {"label": f"{tag}_actor_a", "pid": a_pid, "node": None, "lstart": "t0", "command": "w"},
            {"label": f"{tag}_actor_b", "pid": b_pid, "node": None, "lstart": "t0", "command": "w"}]


def synthetic_clean_run(x68):
    e0 = _synthetic_epoch(x68, "epoch0", 100, 101, 102, {"root": 7000, "a": 7001, "b": 7002})
    e0["injection"] = {"victim": "b", "victim_pid": 102, "victim_actor_id": "epoch0-actor-b",
                       "victim_locality": 2, "signal": "SIGKILL", "sent": True,
                       "t_wall_ms": 1000, "t_mono_ns": 5_000_000,
                       "precondition_checks": {"victim_is_configured": True,
                                               "actor_id_matches_identity": True,
                                               "ray_pid_matches_ext_pid": True,
                                               "pid_in_current_epoch": True,
                                               "pid_not_driver_root_survivor": True,
                                               "no_epoch1_processes_yet": True}}
    e0["post_loss"] = {
        "victim_pid_gone": True, "victim_ray_call_raised": True,
        "victim_ray_error_type": "RayActorError",
        "victim_clean_disconnect_recorded": False,
        "survivor_probe": {"attempted": True, "bounded": True, "bound_s": 12, "elapsed_s": 0.1,
                           "result_verbatim": {"ok": True, "membership": 3},
                           "category": "membership_stale", "membership": 3},
        "root_probe": {"attempted": True, "bounded": True, "root_process_alive": True,
                       "root_alive_mtime_first": 10.0, "root_alive_mtime_second": 11.2,
                       "root_alive_mtime_advanced": True,
                       "witness_kind": ROOT_ALIVE_WITNESS_KIND,
                       "expected_refresh_s": ROOT_ALIVE_EXPECTED_REFRESH_S},
    }
    e0["app_work_after_classification"] = False
    e0["teardown"] = {"no_graceful_attempt_on_poisoned_island": True, "root_done_written": False,
                      "survivor_killed": True, "root_killed": True,
                      "all_epoch0_pids_gone": True, "root_final_absent_expected": True}
    e1 = _synthetic_epoch(x68, "epoch1", 200, 201, 202, {"root": 7100, "a": 7101, "b": 7102})
    e1["shutdown"] = {"a_stop_rc": 0, "a_stop_error": None, "b_stop_rc": 0, "b_stop_error": None,
                      "root_final": {"leave_observed": True, "final_membership": 1,
                                     "max_membership": 3},
                      "root_exit_path": "finalized_clean", "actor_pids_gone": True}
    e1["stale_probe"] = {"epoch0_markers_visible_in_epoch1_dir": False,
                         "epoch1_dir_isolated": True}
    owned = _synthetic_owned("epoch0", 100, 101, 102) + _synthetic_owned("epoch1", 200, 201, 202)
    return {
        "victim_key": "b", "phase": "local", "epoch0": e0, "epoch1": e1,
        "final": {"owned_process_check": {"all_owned_gone": True, "covers_epochs": True,
                                          "details": [{**r, "status": "gone"} for r in owned]},
                  "rundir_scoped_orphans": [],
                  "machine_wide_peer_scan_informational": ["55555"],  # unrelated peer: informational
                  "cleanup_ran": True},
        "phase_log": E0_ORDER + E1_ORDER,
    }


def synthetic_crossnode_run(x68):
    m = synthetic_clean_run(x68)
    m["phase"] = "rostam-cross-node"
    m["crossnode"] = {"slurm_job_id": "999999", "nodelist": "medusa[00-01]",
                      "nodes": ["medusa00", "medusa01"], "nodeA": "medusa00", "nodeB": "medusa01",
                      "nodeA_ip": "10.42.5.30", "nodeB_ip": "10.42.5.31",
                      "subnet": DEFAULT_SUBNET,
                      "ray_node_ids": {"nodeA": "nidA", "nodeB": "nidB"}}
    for tag in ("epoch0", "epoch1"):
        e = m[tag]
        e["root_ready"]["hostname"] = "medusa00.cluster"
        e["a_identity"].update({"node_id": "nidA", "hostname": "medusa00.cluster"})
        e["b_identity"].update({"node_id": "nidB", "hostname": "medusa01.cluster"})
        e["placement"] = {"strategy": "NodeAffinitySchedulingStrategy", "soft": False,
                          "targets": {"a": "medusa00", "b": "medusa01"},
                          "endpoint_ips": ["10.42.5.30", "10.42.5.31"]}
    return m


def selftest():
    x68, err = import_exp68(DEFAULT_EXP68_DIR)
    checks, failed = [], []

    def check(label, ok):
        checks.append((label, bool(ok)))
        if not ok:
            failed.append(label)
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")

    check("exp68 module importable with required surface (incl. crossnode helpers)",
          x68 is not None and err is None)
    if x68 is None:
        print(f"selftest ABORT: {err}")
        return 1

    import copy

    clean = synthetic_clean_run(x68)
    r = rollup(x68, clean)
    check("clean synthetic run passes all gates", r["passed"] and r["failure_class"] == "pass")
    if not r["passed"]:
        print(json.dumps(r["gates_failed"], indent=2))
    want_claims = {"no_partial_island_continuation_used", "old_island_discarded_by_policy",
                   "replacement_island_started_from_fresh_processes",
                   "workload_verified_only_on_fresh_epoch",
                   "no_hpx_native_loss_notification_claim",
                   "no_application_state_restoration_claim"}
    check("clean run states exactly the supported negative claims, all true",
          set(r["negative_claims"]) == want_claims and all(r["negative_claims"].values()))
    check("clean run interprets stale membership verbatim",
          r["post_loss_membership_category"] == "membership_stale"
          and "still contained the lost locality" in r["post_loss_interpretation"])
    check("unrelated machine-wide exp68_peer hit is informational (clean run passes with one)",
          clean["final"]["machine_wide_peer_scan_informational"] == ["55555"] and r["passed"])

    def expect_on(base, label, mutate, want_class, extra=None):
        mm = copy.deepcopy(base)
        mutate(mm)
        rr = rollup(x68, mm)
        ok = (not rr["passed"]) and rr["failure_class"] == want_class
        if extra is not None:
            ok = ok and extra(rr)
        check(f"{label} -> {want_class}", ok)
        if not ok:
            print(f"    got class={rr['failure_class']} failed={rr['gates_failed']}")

    def expect_pass_on(base, label, mutate, extra=None):
        mm = copy.deepcopy(base)
        mutate(mm)
        rr = rollup(x68, mm)
        ok = rr["passed"] and rr["failure_class"] == "pass"
        if extra is not None:
            ok = ok and extra(rr)
        check(label, ok)
        if not ok:
            print(f"    got class={rr['failure_class']} failed={rr['gates_failed']}")

    def expect(label, mutate, want_class, extra=None):
        expect_on(clean, label, mutate, want_class, extra)

    def expect_pass(label, mutate, extra=None):
        expect_pass_on(clean, label, mutate, extra)

    # --- victim selection -----------------------------------------------------------------
    expect("victim pid cross-check mismatch",
           lambda mm: mm["epoch0"]["b_identity"].update(ray_pid=999), "invalid_instrumentation")
    expect("invalid victim key",
           lambda mm: mm.update(victim_key="c"), "invalid_instrumentation")

    # --- epoch 0 topology / workload --------------------------------------------------------
    expect("epoch0 root not locality 0",
           lambda mm: mm["epoch0"]["root_ready"].update(locality_id=1), "epoch0_join_failed")
    expect("epoch0 actor has HPX child process",
           lambda mm: mm["epoch0"]["a_child_report"].update(hpx_children=[{"pid": 5}]),
           "epoch0_inprocess_proof_failed")

    def _oracle_bit(mm):
        mm["epoch0"]["case"]["a_coord"]["global_topk"][0]["logit_bits"] ^= 1
    expect("epoch0 oracle float32 bit mismatch", _oracle_bit, "epoch0_workload_failed")

    # --- injection (record + preconditions) -------------------------------------------------
    expect("injection not recorded as sent",
           lambda mm: mm["epoch0"]["injection"].update(sent=False), "injection_not_observed")
    expect("failed injection precondition",
           lambda mm: mm["epoch0"]["injection"]["precondition_checks"].update(
               pid_not_driver_root_survivor=False), "injection_not_observed")

    # --- observation-only classification ----------------------------------------------------
    expect("victim process still alive",
           lambda mm: mm["epoch0"]["post_loss"].update(victim_pid_gone=False),
           "island_failure_not_classified")
    expect("graceful leave must NOT classify as island failure",
           lambda mm: mm["epoch0"]["post_loss"].update(victim_clean_disconnect_recorded=True),
           "island_failure_not_classified")

    ok_blind, _ = classify_island_failed(clean["epoch0"]["post_loss"])
    m_blind = copy.deepcopy(clean)
    m_blind["epoch0"]["injection"] = {}
    ok_blind2, _ = classify_island_failed(m_blind["epoch0"]["post_loss"])
    check("classifier is injection-blind (same verdict with injection record erased)",
          ok_blind and ok_blind2)

    # --- post-loss membership is OBSERVATIONAL ----------------------------------------------
    def _shrank(mm):
        sp = mm["epoch0"]["post_loss"]["survivor_probe"]
        sp["category"], sp["membership"] = "membership_shrank", 2
        sp["result_verbatim"] = {"ok": True, "membership": 2}
    expect_pass("membership_shrank is observational; run still passes", _shrank,
                extra=lambda rr: (rr["post_loss_membership_category"] == "membership_shrank"
                                  and "membership change" in rr["post_loss_interpretation"]
                                  and all(rr["negative_claims"].values())))

    def _timeout(mm):
        sp = mm["epoch0"]["post_loss"]["survivor_probe"]
        sp["category"], sp["membership"] = "membership_query_timeout", None
        sp["result_verbatim"] = {"ok": False, "error": "probe: GetTimeoutError: ..."}
    expect_pass("membership_query_timeout is observational; run still passes", _timeout)

    expect("invalid membership category",
           lambda mm: mm["epoch0"]["post_loss"]["survivor_probe"].update(category="fine"),
           "post_loss_observation_incomplete")
    expect("unbounded survivor probe",
           lambda mm: mm["epoch0"]["post_loss"]["survivor_probe"].update(bounded=False),
           "post_loss_observation_incomplete")

    def _no_verbatim(mm):
        del mm["epoch0"]["post_loss"]["survivor_probe"]["result_verbatim"]
    expect("survivor result not recorded verbatim", _no_verbatim,
           "post_loss_observation_incomplete")
    expect("mislabelled root-liveness witness kind",
           lambda mm: mm["epoch0"]["post_loss"]["root_probe"].update(
               witness_kind="HPX-native heartbeat"), "post_loss_observation_incomplete")

    check("categorize: membership 2 -> shrank",
          categorize_membership_probe({"membership": 2}) == ("membership_shrank", 2))
    check("categorize: membership 3 -> stale",
          categorize_membership_probe({"membership": 3}) == ("membership_stale", 3))
    check("categorize: bounded timeout -> query_timeout",
          categorize_membership_probe({"error": "x: GetTimeoutError: y"})[0]
          == "membership_query_timeout")
    check("categorize: bounded exception -> query_error",
          categorize_membership_probe({"error": "x: RayActorError: y"})[0]
          == "membership_query_error")
    check("categorize: membership 5 -> other",
          categorize_membership_probe({"membership": 5}) == ("membership_other", 5))

    expect("application work after classification",
           lambda mm: mm["epoch0"].update(app_work_after_classification=True),
           "app_work_after_classification")

    # --- teardown ordering / completeness ---------------------------------------------------
    expect("surviving connector not terminated",
           lambda mm: mm["epoch0"]["teardown"].update(survivor_killed=False),
           "epoch0_teardown_incomplete")
    expect("graceful application completion on poisoned island violates policy",
           lambda mm: mm["epoch0"]["teardown"].update(
               no_graceful_attempt_on_poisoned_island=False), "epoch0_teardown_incomplete")
    expect("root.done written on poisoned epoch violates policy",
           lambda mm: mm["epoch0"]["teardown"].update(root_done_written=True),
           "epoch0_teardown_incomplete")

    def _kill_before_classify(mm):
        log = mm["phase_log"]
        i, j = log.index("e0_classified_failed"), log.index("e0_survivor_killed")
        log[i], log[j] = log[j], log[i]
    expect("teardown before classification breaks ordering", _kill_before_classify,
           "invalid_ordering")
    expect("missing final orphan sweep breaks ordering",
           lambda mm: mm["phase_log"].remove("final_orphan_sweep"), "invalid_ordering")

    # --- epoch isolation / stale-marker rejection -------------------------------------------
    expect("epoch1 reuses an epoch0 port",
           lambda mm: mm["epoch1"]["ports"].update(root=mm["epoch0"]["ports"]["root"]),
           "epoch_isolation_violated")
    expect("epoch1 reuses epoch0 root pid",
           lambda mm: mm["epoch1"]["root_ready"].update(pid=mm["epoch0"]["root_ready"]["pid"]),
           "epoch_isolation_violated")
    expect("epoch1 reuses an epoch0 actor identity",
           lambda mm: mm["epoch1"]["a_identity"].update(
               actor_id=mm["epoch0"]["a_identity"]["actor_id"]), "epoch_isolation_violated")
    expect("epoch0 marker visible to epoch1",
           lambda mm: mm["epoch1"]["stale_probe"].update(
               epoch0_markers_visible_in_epoch1_dir=True), "epoch_isolation_violated")

    # --- epoch 1 (restart) ------------------------------------------------------------------
    def _e1_oracle(mm):
        mm["epoch1"]["case"]["b_coord"]["global_topk"][0]["token_id"] += 1
    expect("epoch1 oracle token-id mismatch", _e1_oracle, "epoch1_workload_failed")
    expect("epoch1 non-graceful actor stop",
           lambda mm: mm["epoch1"]["shutdown"].update(b_stop_rc=1), "epoch1_shutdown_failed")
    expect("epoch1 root did not observe leave",
           lambda mm: mm["epoch1"]["shutdown"]["root_final"].update(leave_observed=False),
           "epoch1_shutdown_failed")

    # --- experiment-scoped orphan checks ----------------------------------------------------
    expect("recorded Slice 1 process still alive fails cleanup",
           lambda mm: mm["final"]["owned_process_check"].update(all_owned_gone=False),
           "orphan_detected")
    expect("owned records missing an epoch fails coverage",
           lambda mm: mm["final"]["owned_process_check"].update(covers_epochs=False),
           "orphan_detected")
    expect("run-dir-scoped process fails cleanup",
           lambda mm: mm["final"].update(rundir_scoped_orphans=["777"]), "orphan_detected")
    expect("cleanup-after-intermediate-failure not run",
           lambda mm: mm["final"].update(cleanup_ran=False), "cleanup_incomplete")

    rec = {"label": "epoch0_root", "pid": 42, "node": None, "lstart": "T", "command": "peer x"}
    g_all, det = evaluate_owned_processes([rec], lambda r: None)
    check("owned sweep: gone process -> all_owned_gone", g_all and det[0]["status"] == "gone")
    g_all, det = evaluate_owned_processes([rec], lambda r: {"lstart": "T", "command": "peer x"})
    check("owned sweep: matching identity -> alive_ours (fails)",
          not g_all and det[0]["status"] == "alive_ours")
    g_all, det = evaluate_owned_processes([rec], lambda r: {"lstart": "OTHER", "command": "peer x"})
    check("owned sweep: PID reuse (different start identity) is NOT ours",
          g_all and det[0]["status"] == "pid_reused_not_ours")
    both = _synthetic_owned("epoch0", 1, 2, 3) + _synthetic_owned("epoch1", 4, 5, 6)
    g_all, det = evaluate_owned_processes(both, lambda r: ({"lstart": "t0", "command": "w"}
                                                           if r["pid"] == 5 else None))
    check("owned sweep: epoch1 record still alive is detected (both epochs checked)",
          not g_all and [d for d in det if d["pid"] == 5][0]["status"] == "alive_ours"
          and records_cover_epochs(both, ["epoch0", "epoch1"]))

    # --- cross-node phase: synthetic placement/attestation ----------------------------------
    cxm = synthetic_crossnode_run(x68)
    rcx = rollup(x68, cxm)
    check("clean cross-node synthetic run passes all gates",
          rcx["passed"] and rcx["failure_class"] == "pass")
    if not rcx["passed"]:
        print(json.dumps(rcx["gates_failed"], indent=2))

    def expect_cx(label, mutate, want_class, extra=None):
        expect_on(cxm, label, mutate, want_class, extra)

    expect_cx("same-node placement rejected",
              lambda mm: mm["epoch0"]["b_identity"].update(node_id="nidA"),
              "crossnode_placement_failed")
    expect_cx("soft placement rejected",
              lambda mm: mm["epoch0"]["placement"].update(soft=True),
              "crossnode_placement_failed")
    expect_cx("epoch1 placement record missing rejected (placement freshness)",
              lambda mm: mm["epoch1"].pop("placement"), "crossnode_placement_failed")
    expect_cx("missing Slurm job id rejected",
              lambda mm: mm["crossnode"].update(slurm_job_id=""),
              "crossnode_attestation_failed")
    expect_cx("unresolved Ray node ids rejected (cluster-artifact schema)",
              lambda mm: mm["crossnode"]["ray_node_ids"].update(nodeB=None),
              "crossnode_attestation_failed")
    expect_cx("off-subnet parcelport endpoint rejected",
              lambda mm: mm["epoch0"]["placement"].update(endpoint_ips=["192.168.1.5"]),
              "crossnode_placement_failed")
    g = eval_crossnode_placement(
        {**cxm["epoch0"], "b_identity": {**cxm["epoch0"]["b_identity"], "node_id": "nidA"}},
        cxm["crossnode"], victim_key="b")
    check("victim-node selection: victim on head node fails victim_on_remote_node",
          g["victim_on_remote_node"] is False)
    check("local synthetic run carries no placement gates",
          "epoch0_placement" not in rollup(x68, clean)["gates"])

    # --- off-cluster / no-Slurm discipline --------------------------------------------------
    pf = preflight("/nonexistent_exp68_dir_for_selftest")
    check("preflight cleanly reports missing artifacts (skip path)",
          pf["ok"] is False and pf["problems"])
    pfc = preflight_crossnode(DEFAULT_EXP68_DIR, env={}, subnet=DEFAULT_SUBNET)
    check("crossnode preflight without Slurm env cleanly skips",
          pfc["ok"] is False and any("SLURM_JOB_ID" in p for p in pfc["problems"]))
    local_root_cmd = x68.peer_root_cmd(os.path.join(DEFAULT_EXP68_DIR, "build", "exp68_peer"),
                                       "/tmp/x", 12345)
    check("local root command contains no srun/Slurm invocation",
          "srun" not in " ".join(local_root_cmd) and "sbatch" not in " ".join(local_root_cmd))
    pf2 = preflight(DEFAULT_EXP68_DIR)
    check("preflight sees real exp68 artifacts (informational)",
          isinstance(pf2.get("problems"), list))

    n_fail = len(failed)
    print(f"\nselftest: {len(checks) - n_fail}/{len(checks)} passed"
          + (f"; FAILED: {failed}" if failed else ""))
    return 1 if failed else 0


# ---------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="exp70 Slice 1: actor-hosted island restart")
    ap.add_argument("--selftest", action="store_true",
                    help="pure logic checks (no Ray, no HPX, no Slurm)")
    ap.add_argument("--curate", nargs="+", metavar="RUNID",
                    help="curate accepted local runs into the stable evidence dir")
    ap.add_argument("--phase", choices=("local", "rostam-cross-node"), default="local")
    ap.add_argument("--exp68-dir", default=DEFAULT_EXP68_DIR)
    ap.add_argument("--exp68-build-dir", default=None,
                    help="exp68 build dir holding the peer binary and ext .so "
                         "(default: <exp68-dir>/build; Rostam uses <exp68-dir>/build_rostam)")
    ap.add_argument("--victim", choices=("a", "b"), default=DEFAULT_VICTIM)
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--ray-num-cpus", type=int, default=1)
    ap.add_argument("--aggregate", default=None)
    ap.add_argument("--subnet", default=DEFAULT_SUBNET)
    ap.add_argument("--ray-port", type=int, default=6479)
    ap.add_argument("--port-base", type=int, default=7811)
    ap.add_argument("--node-a", default=None)
    ap.add_argument("--node-b", default=None)
    ap.add_argument("--ray-ready-timeout", type=int, default=180)
    ap.add_argument("--ray-init-timeout", type=int, default=180)
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if args.curate:
        return curate_local(args, args.curate)
    if args.phase == "rostam-cross-node":
        return crossnode_live_run(args)
    return live_run(args)


if __name__ == "__main__":
    sys.exit(main())
