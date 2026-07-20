#!/usr/bin/env python3
"""exp70 Slice 2A -- explicit-completion contract for an actor-hosted HPX island, EXTERNAL backend.

QUESTION: can the actor-hosted HPX island avoid connector lifetime guesses by making "no further
work will be sent" an explicit, testable lifecycle event? Slice 0 (../upstream_reproducer/)
reduced the lifetime-guess failure to two processes: a connector with a FIXED 3 s serve window
departs, and a later valid dispatch fails. Slice 2A elevates Slice 0's external-lifecycle answer
(explicit completion witness written only after the final verified result) to the exp66-68
actor-hosted shared-runtime island, behind a backend-neutral contract.

TOPOLOGY (exp66/67/68 mechanism, reused IN PLACE and unmodified): one separately supervised,
WORK-FREE exp68_peer root (locality 0) plus two Ray actors each hosting an HPX connect-mode
locality IN-PROCESS via exp68_actor_ext, under one persistent Ray supervision plane.

CONTRACT (backend-neutral; Slice 2B may later substitute an HPX-native backend WITHOUT changing
the state machine or gates): the island starts accepting work; an idle interval longer than the
former fixed serving window does NOT authorize departure; valid HPX work arrives after the idle
interval and is verified; completion is published exactly once, only after the final verified
result; after publication no application work is accepted (controller-level fence, checked
BEFORE any dispatch could reach HPX); every connector observes completion for the exact island
epoch (stale markers from prior epochs are rejected); every connector then leaves via the
validated disconnect/stop sequence; the root observes membership return to itself and finalizes
cleanly; no process remains.

STATES (linear; invalid transitions fail deterministically):
  STARTING -> READY -> WORK_1_VERIFIED -> IDLE -> WORK_2_VERIFIED -> COMPLETION_PUBLISHED
  -> CONNECTORS_LEAVING -> ROOT_ALONE -> FINALIZED
Application dispatch is permitted only in READY (work 1) and IDLE (work 2).

EXTERNAL BACKEND (this slice): an epoch-scoped `island.complete` JSON marker in the island boot
directory, published atomically, observed by bounded monotonic polling. DISTINCT from exp68's
mechanical `root.done` root-finalize trigger, still used afterwards to finalize the work-free
root. OBSERVATION IS CONTROLLER-MEDIATED: the exp68 actor surface is fixed (never modified
here), so one observer record per connector is produced in the controller and each connector's
graceful leave is dispatched ONLY after its own observation acknowledgment. A future HPX-native
backend would relocate publication/observation into the runtime; the per-connector
acknowledgment gate and state machine stay identical.

WORKLOADS: two DISTINGUISHABLE exp68 matrix cases, both coordinator directions, bit-exact vs the
imported exp68 oracle -- work 1 `cross_both` (V=64,split=32,k=6,seed=1) before the idle interval,
work 2 `both_contrib` (V=100,split=50,k=10,seed=1) after it. Different case name/V/split/k means
a stale work-1 result cannot satisfy the work-2 oracle gate.

IDLE SEMANTICS, NOT TUNING: the idle interval (default 6 s) exceeds Slice 0's former fixed 3 s
serve window to demonstrate that idleness does not end the island's obligation to accept work.
In the actor-hosted topology connectors cannot self-depart; the idle gates prove continued
availability (health, membership 3, unchanged localities) and that no stop was dispatched
during the interval.

CROSS-NODE PHASE (`--phase rostam-cross-node`, NOT the default, never runs off-cluster): reuses
exp68's validated Slurm/Ray machinery (head + srun worker bring-up, GCS wait, node-id
resolution, hard NodeAffinitySchedulingStrategy(soft=False), subnet-bound TCP parcelport
endpoints, NFS-safe marker waits). Topology: node A hosts the controller/Ray head, the
work-free root, and actor A; node B hosts actor B. Both actors participate in both the pre-idle
and post-idle workloads. Skips cleanly without a Slurm allocation.

CLAIM FENCE: application-contract / mechanism evidence only. NOT HPX-native completion, NOT an
HPX-native heartbeat, NOT loss detection, NOT runtime enforcement against all post-completion
parcels, NOT recovery, NO performance claim, NOT production API. No failure detection is
required or implied in this slice.

Usage:
  python run_slice2.py --selftest               # pure logic checks (no Ray, no HPX, no Slurm)
  python run_slice2.py --phase local            # live local single-island run
  python run_slice2.py --phase rostam-cross-node  # cluster phase (skips cleanly off-cluster)
  python run_slice2.py --curate [RUNID ...]     # curate accepted local runs (no processes)
"""

import argparse
import copy
import json
import os
import platform
import socket
import subprocess
import sys
import tempfile
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EXP68_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "68_vocab_sharded_topk"))
RUNS_ROOT = os.path.join(HERE, "_exp70_slice2_runs")

WORK1_CASE = "cross_both"    # exp68 MATRIX: V=64  split=32 k=6  seed=1
WORK2_CASE = "both_contrib"  # exp68 MATRIX: V=100 split=50 k=10 seed=1 (distinguishable)
FORMER_FIXED_SERVE_WINDOW_S = 3.0  # Slice 0 case-1 connector serve window (upstream_reproducer)
DEFAULT_IDLE_S = 6.0               # Slice 0 case-2 demonstration idle; semantics, not tuning
COMPLETION_MARKER = "island.complete"
DEFAULT_SUBNET = "10.42.5."

STATES = ("STARTING", "READY", "WORK_1_VERIFIED", "IDLE", "WORK_2_VERIFIED",
          "COMPLETION_PUBLISHED", "CONNECTORS_LEAVING", "ROOT_ALONE", "FINALIZED")
_NEXT = {a: b for a, b in zip(STATES, STATES[1:])}
DISPATCH_STATES = ("READY", "IDLE")

SUMMARY_CLAIM = (
    "In an actor-hosted HPX island, connectors remained available across an idle interval, "
    "accepted later valid work, and departed cleanly only after the supervisor explicitly "
    "published completion through the external backend.")
NON_CLAIMS = (
    "The result does not demonstrate HPX-native completion, an HPX-native heartbeat, HPX-native "
    "loss detection, runtime enforcement against all post-completion parcels, recovery, or any "
    "performance improvement.")

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
    "_ORPHAN_PATTERNS_RAY",
]

FAILURE_CLASSES = [
    "preflight_missing_artifacts", "invalid_instrumentation", "crossnode_placement_failed",
    "startup_failed", "inprocess_proof_failed", "work1_failed", "idle_availability_failed",
    "work2_failed", "completion_contract_violated", "post_completion_fence_breached",
    "completion_observation_incomplete", "departure_failed", "root_alone_not_observed",
    "finalize_failed", "epoch_scope_violated", "invalid_ordering", "orphan_detected",
    "cleanup_incomplete",
]


# ---------------------------------------------------------------------------------------
# exp68 import + preflight (pure checks, no Slurm commands anywhere)
# ---------------------------------------------------------------------------------------

def import_exp68(exp68_dir):
    if not os.path.isdir(exp68_dir):
        return None, f"exp68 dir not found: {exp68_dir}"
    if exp68_dir not in sys.path:
        sys.path.insert(0, exp68_dir)
    try:
        import run_exp68 as x68  # noqa: PLC0415
    except Exception as ex:  # noqa: BLE001
        return None, f"exp68 import failed: {type(ex).__name__}: {ex}"
    missing = [n for n in EXP68_REQUIRED if not hasattr(x68, n)]
    if missing:
        return None, f"exp68 module missing required attributes: {missing}"
    return x68, None


def preflight(exp68_dir, build_dir=None):
    """Pure checks; never raises. `build_dir` defaults to `<exp68_dir>/build` (Mac layout);
    Rostam builds into `build_rostam`, selected via --exp68-build-dir."""
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
    out["build_dir"], out["peer"] = build_dir, peer
    out["ext_so"] = os.path.join(build_dir, ext_so) if ext_so else None
    out["ok"] = not out["problems"]
    return out


def preflight_crossnode(exp68_dir, env, subnet, build_dir=None):
    """Cross-node preconditions. Pure env/artifact checks only; nodelist parsing is string
    parsing, and NO srun/sbatch/Slurm command is executed here."""
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
# State machine (selftested; used verbatim by the live drivers)
# ---------------------------------------------------------------------------------------

class InvalidTransition(Exception):
    pass


class IslandStateMachine:
    """Linear island lifecycle. `advance` validates the successor; `dispatch_guard` is consulted
    BEFORE any application dispatch and is the post-completion fence."""

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self.state = STATES[0]
        self.history = [{"state": self.state, "t_mono": self._clock()}]
        self.rejected = []

    def advance(self, to):
        if _NEXT.get(self.state) != to:
            self.rejected.append({"from": self.state, "to": to})
            raise InvalidTransition(f"invalid transition {self.state} -> {to}")
        self.state = to
        self.history.append({"state": to, "t_mono": self._clock()})

    def dispatch_guard(self, label):
        """Callers MUST NOT dispatch when allowed is False; a rejected guard record therefore
        proves the attempt never reached HPX (`reached_hpx: False`)."""
        allowed = self.state in DISPATCH_STATES
        return {"label": label, "state": self.state, "allowed": allowed,
                "reached_hpx": False if not allowed else None, "t_mono": self._clock()}


# ---------------------------------------------------------------------------------------
# Completion contract + backends (surface: publish_complete / observe_completion)
# ---------------------------------------------------------------------------------------

def backend_surface_ok(backend):
    """Future-backend substitution surface: duck-typed two-method contract + a name."""
    return (callable(getattr(backend, "publish_complete", None))
            and callable(getattr(backend, "observe_completion", None))
            and bool(getattr(backend, "name", None)))


class CompletionContract:
    """Publication guard shared by ALL backends: publish only from WORK_2_VERIFIED with a
    verified final result; duplicates rejected; every attempt recorded."""

    def __init__(self, backend, sm):
        if not backend_surface_ok(backend):
            raise ValueError("backend does not satisfy the completion surface")
        self.backend, self.sm = backend, sm
        self.attempts, self.published = [], None

    def try_publish(self, epoch_id, connector_ids, final_result):
        att = {"state_at_attempt": self.sm.state,
               "final_verified": bool((final_result or {}).get("verified") is True),
               "already_published": self.published is not None,
               "t_mono": time.monotonic()}
        att["accepted"] = (att["final_verified"] and not att["already_published"]
                          and self.sm.state == "WORK_2_VERIFIED")
        if att["accepted"]:
            self.published = self.backend.publish_complete(
                epoch_id, connector_ids,
                {"final_result_digest": final_result.get("digest"),
                 "final_case": final_result.get("case")})
            att["record"] = self.published
        self.attempts.append(att)
        return att


class SyntheticCompletionBackend:
    """In-memory backend for selftests. Same surface as the external backend."""

    name = "synthetic"

    def __init__(self):
        self._published = {}

    def publish_complete(self, epoch_id, connector_ids, payload):
        rec = {"backend": self.name, "epoch_id": epoch_id,
               "connector_ids": sorted(connector_ids), "payload": dict(payload or {}),
               "published_wall_ms": int(time.time() * 1000),
               "published_mono": time.monotonic()}
        self._published[epoch_id] = rec
        return rec

    def observe_completion(self, epoch_id, connector_id, bound_s):
        t0 = time.monotonic()
        rec = self._published.get(epoch_id)
        stale = sorted(e for e in self._published if e != epoch_id)
        return {"backend": self.name, "connector_id": connector_id, "epoch_id": epoch_id,
                "observed": rec is not None, "epoch_match": rec is not None,
                "stale_epochs_rejected": stale, "bounded": True, "bound_s": bound_s,
                "elapsed_s": time.monotonic() - t0, "verbatim": rec}


class ExternalWitnessCompletionBackend:
    """Epoch-scoped shared-filesystem completion witness (Slice 0 case-2 lineage): atomic
    publish of an `island.complete` JSON marker; bounded MONOTONIC polling observation; a marker
    carrying a different epoch id is stale and NEVER satisfies the observation."""

    name = "external_witness"

    def __init__(self, marker_dir, poll_s=0.1):
        self.marker_dir, self.poll_s = marker_dir, poll_s

    def marker_path(self):
        return os.path.join(self.marker_dir, COMPLETION_MARKER)

    def publish_complete(self, epoch_id, connector_ids, payload):
        rec = {"backend": self.name, "epoch_id": epoch_id,
               "connector_ids": sorted(connector_ids), "payload": dict(payload or {}),
               "published_wall_ms": int(time.time() * 1000),
               "published_mono": time.monotonic(), "marker_path": self.marker_path()}
        tmp = self.marker_path() + ".tmp"
        with open(tmp, "w") as f:
            json.dump(rec, f, indent=2, sort_keys=True)
        os.replace(tmp, self.marker_path())  # atomic publish
        return rec

    def observe_completion(self, epoch_id, connector_id, bound_s):
        t0 = time.monotonic()
        stale = []
        while True:
            data = None
            try:
                with open(self.marker_path()) as f:
                    data = json.load(f)
            except (OSError, ValueError):
                data = None
            if isinstance(data, dict):
                if data.get("epoch_id") == epoch_id:
                    return {"backend": self.name, "connector_id": connector_id,
                            "epoch_id": epoch_id, "observed": True, "epoch_match": True,
                            "stale_epochs_rejected": stale, "bounded": True,
                            "bound_s": bound_s, "elapsed_s": time.monotonic() - t0,
                            "verbatim": data}
                se = data.get("epoch_id")
                if se not in stale:
                    stale.append(se)
            if time.monotonic() - t0 >= bound_s:
                return {"backend": self.name, "connector_id": connector_id,
                        "epoch_id": epoch_id, "observed": False, "epoch_match": False,
                        "stale_epochs_rejected": stale, "bounded": True, "bound_s": bound_s,
                        "elapsed_s": time.monotonic() - t0, "verbatim": None}
            time.sleep(self.poll_s)


# ---------------------------------------------------------------------------------------
# Gate evaluation (pure; shared verbatim by selftests and the live drivers)
# ---------------------------------------------------------------------------------------

def case_for(x68, name):
    return next(c for c in x68.MATRIX if c["name"] == name)


def short_host(h):
    return (h or "").split(".")[0].lower()


def eval_startup(isl):
    rr = isl.get("root_ready") or {}
    a_s, b_s = isl.get("a_start") or {}, isl.get("b_start") or {}
    a_i, b_i = isl.get("a_identity") or {}, isl.get("b_identity") or {}
    a_j, b_j = isl.get("a_join_health") or {}, isl.get("b_join_health") or {}
    return {
        "root_ready_workfree_locality0": bool(rr.get("pid")) and rr.get("locality_id") == 0,
        "actor_a_started": a_s.get("started") is True,
        "actor_b_started": b_s.get("started") is True,
        # membership probed AFTER both connectors started (the first start legitimately sees 2)
        "membership_reached_3": (a_j.get("ok") is True and b_j.get("ok") is True
                                 and a_j.get("membership") == 3 and b_j.get("membership") == 3),
        "distinct_connector_localities": (a_s.get("locality_id") not in (None, 0)
                                          and b_s.get("locality_id") not in (None, 0)
                                          and a_s.get("locality_id") != b_s.get("locality_id")),
        "distinct_worker_pids": (a_i.get("pid") is not None and b_i.get("pid") is not None
                                 and a_i.get("pid") != b_i.get("pid")),
    }


def eval_inprocess(isl):
    out = {}
    for k in ("a", "b"):
        ident = isl.get(f"{k}_identity") or {}
        start = isl.get(f"{k}_start") or {}
        rep = isl.get(f"{k}_child_report") or {}
        pids = {ident.get("pid"), ident.get("os_getpid"), start.get("pid")}
        out[f"{k}_pid_identity"] = None not in pids and len(pids) == 1
        out[f"{k}_no_hpx_children"] = rep.get("checked") is True and not rep.get("hpx_children")
    return out


def eval_cluster_attestation(cx):
    return {
        "slurm_job_id_present": bool(cx.get("slurm_job_id")),
        "two_distinct_nodes": (len(cx.get("nodes") or []) >= 2
                               and short_host(cx.get("nodeA")) != short_host(cx.get("nodeB"))),
        "ray_node_ids_resolved": (bool((cx.get("ray_node_ids") or {}).get("nodeA"))
                                  and bool((cx.get("ray_node_ids") or {}).get("nodeB"))),
        "subnet_ips_resolved": (bool(cx.get("nodeA_ip")) and bool(cx.get("nodeB_ip"))
                                and (cx.get("nodeA_ip") or "").startswith(cx.get("subnet") or "!")
                                and (cx.get("nodeB_ip") or "").startswith(cx.get("subnet") or "!")),
    }


def eval_placement(m):
    """Cross-node only: hard placement + endpoint + attestation gates."""
    cx = m.get("crossnode") or {}
    isl = m.get("island") or {}
    pl = isl.get("placement") or {}
    a_i, b_i = isl.get("a_identity") or {}, isl.get("b_identity") or {}
    ep_ok = True
    for k in ("a", "b"):
        eps = isl.get(f"{k}_endpoints") or []
        ep_ok = ep_ok and any((cx.get("subnet") or "!") in e for e in eps)
    return {
        "strategy_hard_node_affinity": (pl.get("strategy") == "NodeAffinitySchedulingStrategy"
                                        and pl.get("soft") is False),
        "actor_a_on_nodeA": short_host(a_i.get("hostname")) == short_host(cx.get("nodeA")),
        "actor_b_on_nodeB": short_host(b_i.get("hostname")) == short_host(cx.get("nodeB")),
        "actors_on_distinct_ray_nodes": (bool(a_i.get("node_id")) and bool(b_i.get("node_id"))
                                         and a_i.get("node_id") != b_i.get("node_id")),
        "parcelport_endpoints_on_subnet": ep_ok,
        **{f"attest_{k}": v for k, v in eval_cluster_attestation(cx).items()},
    }


def eval_work(x68, isl, work_key, expected_case_name):
    cr = isl.get(work_key) or {}
    case = {"name": cr.get("name"), "V": cr.get("V"), "split": cr.get("split"),
            "k": cr.get("k"), "seed": cr.get("seed")}
    try:
        gates, _aux = x68.eval_case(case, cr)
    except Exception:  # noqa: BLE001
        gates = {"eval_case_crashed": False}
    gates = dict(gates)
    gates["case_is_" + work_key] = cr.get("name") == expected_case_name
    a_i, b_i = isl.get("a_identity") or {}, isl.get("b_identity") or {}
    gates["workload_pids_match_identities"] = (cr.get("a_pid") == a_i.get("pid")
                                               and cr.get("b_pid") == b_i.get("pid"))
    return gates


def eval_idle(m):
    idl = m.get("idle") or {}
    pre_a, pre_b = idl.get("pre_health_a") or {}, idl.get("pre_health_b") or {}
    post_a, post_b = idl.get("post_health_a") or {}, idl.get("post_health_b") or {}
    return {
        "idle_exceeds_former_window": (
            isinstance(idl.get("idle_elapsed_s"), (int, float))
            and idl.get("former_fixed_serve_window_s") == FORMER_FIXED_SERVE_WINDOW_S
            and idl["idle_elapsed_s"] > FORMER_FIXED_SERVE_WINDOW_S),
        "pre_idle_health_ok": pre_a.get("ok") is True and pre_b.get("ok") is True,
        "post_idle_health_ok": post_a.get("ok") is True and post_b.get("ok") is True,
        "membership_still_3_after_idle": (post_a.get("membership") == 3
                                          and post_b.get("membership") == 3),
        "localities_unchanged_after_idle": (
            post_a.get("locality_id") == pre_a.get("locality_id")
            and post_b.get("locality_id") == pre_b.get("locality_id")
            and pre_a.get("locality_id") not in (None, 0)),
        "no_stop_dispatched_during_idle": idl.get("stop_dispatched_during_idle") is False,
    }


def eval_work2_distinguishable(m):
    w1, w2 = m.get("island", {}).get("work1") or {}, m.get("island", {}).get("work2") or {}
    return {
        "work2_case_differs_from_work1": (bool(w1.get("name")) and bool(w2.get("name"))
                                          and w1.get("name") != w2.get("name")),
        "work2_oracle_differs_from_work1": (w1.get("oracle_global") is not None
                                            and w2.get("oracle_global") is not None
                                            and w1.get("oracle_global") != w2.get("oracle_global")),
        "work2_dispatched_from_idle_state": ((m.get("work2_guard") or {}).get("state") == "IDLE"
                                             and (m.get("work2_guard") or {}).get("allowed") is True),
    }


def eval_completion(m):
    comp = m.get("completion") or {}
    attempts = comp.get("attempts") or []
    accepted = [a for a in attempts if a.get("accepted")]
    early = [a for a in attempts
             if a.get("accepted") and (a.get("state_at_attempt") != "WORK_2_VERIFIED"
                                       or a.get("final_verified") is not True)]
    dup = [a for a in attempts if a.get("already_published")]
    return {
        "published_once": len(accepted) == 1 and comp.get("published") is not None,
        "publish_state_work2_verified": bool(accepted) and all(
            a.get("state_at_attempt") == "WORK_2_VERIFIED" for a in accepted),
        "publish_only_after_final_verification": not early and bool(accepted) and all(
            a.get("final_verified") is True for a in accepted),
        "pre_verification_attempts_all_rejected": all(
            a.get("accepted") is False for a in attempts if a.get("final_verified") is not True),
        "duplicate_publish_attempted": bool(dup),
        "duplicate_publish_rejected": bool(dup) and all(a.get("accepted") is False for a in dup),
    }


def eval_fence(m):
    f = m.get("post_completion_fence") or {}
    g = f.get("guard") or {}
    return {
        "post_completion_dispatch_attempted": g.get("label") is not None,
        "post_completion_dispatch_rejected": (g.get("state") == "COMPLETION_PUBLISHED"
                                              and g.get("allowed") is False),
        "post_completion_dispatch_never_reached_hpx": g.get("reached_hpx") is False,
        "no_app_work_after_publication": f.get("app_work_after_publication") is False,
    }


def eval_observation(m, epoch_id):
    obs = m.get("observations") or {}
    ok = {}
    for k in ("a", "b"):
        o = obs.get(k) or {}
        ok[f"{k}_observed"] = o.get("observed") is True
        ok[f"{k}_epoch_match"] = o.get("epoch_match") is True and o.get("epoch_id") == epoch_id
        ok[f"{k}_bounded"] = (o.get("bounded") is True
                              and isinstance(o.get("elapsed_s"), (int, float))
                              and o.get("elapsed_s") <= (o.get("bound_s") or 0))
    ok["all_connectors_acknowledged"] = ok["a_observed"] and ok["b_observed"]
    return ok


def eval_departure(m):
    dep = m.get("departure") or {}
    obs = m.get("observations") or {}
    comp = m.get("completion") or {}
    pub = (comp.get("published") or {}).get("published_mono")
    out = {}
    for k in ("a", "b"):
        stop = dep.get(f"{k}_stop") or {}
        out[f"{k}_graceful_stop"] = stop.get("rc") == 0 and stop.get("error") is None
        t_obs = (obs.get(k) or {}).get("t_mono_done")
        t_stop = stop.get("t_mono_dispatched")
        out[f"observation_precedes_departure_{k}"] = (
            isinstance(t_obs, (int, float)) and isinstance(t_stop, (int, float))
            and t_obs <= t_stop)
        out[f"no_departure_before_publication_{k}"] = (
            isinstance(pub, (int, float)) and isinstance(t_stop, (int, float))
            and pub <= t_stop)
    return out


def eval_root_alone(m):
    ra = m.get("root_alone") or {}
    rf = ra.get("root_final") or {}
    return {"root_final_present": bool(rf),
            "root_observed_leave": rf.get("leave_observed") is True}


def eval_finalize(m):
    fz = m.get("finalize") or {}
    return {"root_finalized_clean": fz.get("root_exit_path") == "finalized_clean",
            "actor_pids_gone": fz.get("actor_pids_gone") is True}


def eval_epoch_scope(m, epoch_id):
    comp = m.get("completion") or {}
    pub = comp.get("published") or {}
    sc = m.get("stale_control") or {}
    marker_ok = True
    if pub.get("marker_path") is not None:  # external backend only
        marker_ok = os.path.dirname(pub["marker_path"]) == (m.get("island") or {}).get("bootdir")
    return {
        "publication_epoch_matches_island": pub.get("epoch_id") == epoch_id,
        "marker_in_island_dir": marker_ok,
        "stale_control_attempted": sc.get("attempted") is True,
        "stale_marker_from_prior_epoch_rejected": (
            sc.get("attempted") is True
            and (sc.get("observation") or {}).get("observed") is False
            and bool((sc.get("observation") or {}).get("stale_epochs_rejected"))),
    }


def eval_ordering(m):
    hist = [h.get("state") for h in (m.get("state_history") or [])]
    return {"state_history_complete_and_ordered": tuple(hist) == STATES,
            "no_rejected_transitions": not (m.get("rejected_transitions") or [])}


def eval_final(m):
    fin = m.get("final") or {}
    oc = fin.get("owned_process_check") or {}
    rd = fin.get("rundir_scoped_orphans")
    return {"cleanup_ran": fin.get("cleanup_ran") is True,
            "owned_processes_gone": oc.get("all_owned_gone") is True,
            "owned_records_cover_island": oc.get("covers_island") is True,
            "no_rundir_scoped_processes": rd == []}


def rollup(x68, m):
    epoch_id = m.get("epoch_id")
    isl = m.get("island") or {}
    gates = {
        "startup": eval_startup(isl),
        "inprocess": eval_inprocess(isl),
        "work1": eval_work(x68, isl, "work1", WORK1_CASE),
        "idle": eval_idle(m),
        "work2": {**eval_work(x68, isl, "work2", WORK2_CASE), **eval_work2_distinguishable(m)},
        "completion": eval_completion(m),
        "post_completion_fence": eval_fence(m),
        "observation": eval_observation(m, epoch_id),
        "departure": eval_departure(m),
        "root_alone": eval_root_alone(m),
        "finalize": eval_finalize(m),
        "epoch_scope": eval_epoch_scope(m, epoch_id),
        "ordering": eval_ordering(m),
        "final": eval_final(m),
    }
    if m.get("crossnode") is not None:
        gates["placement"] = eval_placement(m)
    failed = {g: {k: v for k, v in d.items() if v is not True}
              for g, d in gates.items() if any(v is not True for v in d.values())}
    order = [("placement", "crossnode_placement_failed"),
             ("startup", "startup_failed"), ("inprocess", "inprocess_proof_failed"),
             ("work1", "work1_failed"), ("idle", "idle_availability_failed"),
             ("work2", "work2_failed"), ("completion", "completion_contract_violated"),
             ("post_completion_fence", "post_completion_fence_breached"),
             ("observation", "completion_observation_incomplete"),
             ("departure", "departure_failed"), ("root_alone", "root_alone_not_observed"),
             ("finalize", "finalize_failed"), ("epoch_scope", "epoch_scope_violated"),
             ("ordering", "invalid_ordering"), ("final", None)]
    failure_class = "pass"
    for grp, cls in order:
        if grp in failed:
            if grp == "final":
                failure_class = ("cleanup_incomplete" if "cleanup_ran" in failed["final"]
                                 else "orphan_detected")
            else:
                failure_class = cls
            break
    negative_claims = {
        "no_hpx_native_completion_claim": True,
        "no_hpx_native_heartbeat_claim": True,
        "no_hpx_native_loss_detection_claim": True,
        "no_runtime_parcel_enforcement_claim": True,
        "no_recovery_claim": True,
        "no_performance_claim": True,
        "observation_is_controller_mediated_external": True,
    }
    return {"passed": not failed, "failure_class": failure_class, "gates": gates,
            "gates_failed": failed, "negative_claims": negative_claims}


# ---------------------------------------------------------------------------------------
# Experiment-scoped process accounting (Slice 1 discipline, single-island variant)
# ---------------------------------------------------------------------------------------

def _proc_identity_local(pid):
    if pid is None:
        return None
    try:
        ls = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)],
                            capture_output=True, text=True, timeout=10)
        if ls.returncode != 0 or not ls.stdout.strip():
            return None
        cm = subprocess.run(["ps", "-o", "command=", "-p", str(pid)],
                            capture_output=True, text=True, timeout=10)
        return {"lstart": ls.stdout.strip(),
                "command": cm.stdout.strip() if cm.returncode == 0 else None}
    except Exception:  # noqa: BLE001
        return None


def evaluate_owned_processes(owned, identity_fn):
    """A recorded process is gone if no live process carries the SAME pid + start identity
    (PID reuse by an unrelated process is NOT ours)."""
    details, all_gone = [], True
    for rec in owned:
        now = identity_fn(rec)
        ours = (now is not None and now.get("lstart") == rec.get("lstart")
                and now.get("command") == rec.get("command"))
        details.append({**rec, "still_alive_ours": ours})
        all_gone = all_gone and not ours
    return all_gone, details


def records_cover_island(owned):
    labels = {r.get("label") for r in owned}
    return {"island_root", "island_actor_a", "island_actor_b"} <= labels


def _rundir_scoped_orphans(runs_dir):
    try:
        out = subprocess.run(["pgrep", "-f", runs_dir], capture_output=True, text=True,
                             timeout=10)
    except Exception:  # noqa: BLE001
        return None
    if out.returncode not in (0, 1):
        return None
    me = str(os.getpid())
    return [p for p in out.stdout.split() if p and p != me]


def _provenance():
    out = {"hostname": socket.gethostname(), "platform": platform.platform(),
           "python": platform.python_version()}
    try:
        import ray  # noqa: PLC0415
        out["ray_version"] = ray.__version__
    except Exception:  # noqa: BLE001
        out["ray_version"] = None
    return out


# ---------------------------------------------------------------------------------------
# Synthetic clean runs (schema contracts for the selftests; exercise the REAL state machine,
# contract, and synthetic backend -- no processes)
# ---------------------------------------------------------------------------------------

def _synthetic_health(loc, pid):
    return {"ok": True, "pid": pid, "membership": 3, "locality_id": loc}


def synthetic_clean_run(x68, backend=None):
    sm = IslandStateMachine()
    backend = backend if backend is not None else SyntheticCompletionBackend()
    epoch_id = "exp70s2a-synthetic-epoch"
    a_pid, b_pid, root_pid = 5001, 5002, 4000

    c1, c2 = case_for(x68, WORK1_CASE), case_for(x68, WORK2_CASE)
    w1 = x68._synthetic_case_result(c1, a_loc=1, b_loc=2, a_pid=a_pid, b_pid=b_pid)
    w2 = x68._synthetic_case_result(c2, a_loc=1, b_loc=2, a_pid=a_pid, b_pid=b_pid)

    isl = {
        "bootdir": "/synthetic/island",
        "ports": {"root": 7001, "a": 7002, "b": 7003},
        "root_ready": {"pid": root_pid, "locality_id": 0},
        "a_identity": {"pid": a_pid, "os_getpid": a_pid, "actor_id": "syn-a", "hostname": "syn"},
        "b_identity": {"pid": b_pid, "os_getpid": b_pid, "actor_id": "syn-b", "hostname": "syn"},
        "a_start": {"started": True, "locality_id": 1, "membership": 3, "pid": a_pid},
        "b_start": {"started": True, "locality_id": 2, "membership": 3, "pid": b_pid},
        "a_child_report": {"checked": True, "children": [], "hpx_children": []},
        "b_child_report": {"checked": True, "children": [], "hpx_children": []},
        "a_join_health": _synthetic_health(1, a_pid),
        "b_join_health": _synthetic_health(2, b_pid),
        "work1": w1, "work2": w2,
    }
    m = {"epoch_id": epoch_id, "backend": backend.name, "island": isl,
         "phase_log": [], "phase_times_wall_ms": {}, "final": {}}

    sm.advance("READY")
    g1 = sm.dispatch_guard("work1")
    sm.advance("WORK_1_VERIFIED")
    contract = CompletionContract(backend, sm)
    early = contract.try_publish(epoch_id, ["a", "b"], {"verified": False})
    m["idle"] = {"configured_idle_s": DEFAULT_IDLE_S,
                 "former_fixed_serve_window_s": FORMER_FIXED_SERVE_WINDOW_S,
                 "idle_elapsed_s": DEFAULT_IDLE_S + 0.05,
                 "pre_health_a": _synthetic_health(1, a_pid),
                 "pre_health_b": _synthetic_health(2, b_pid),
                 "post_health_a": _synthetic_health(1, a_pid),
                 "post_health_b": _synthetic_health(2, b_pid),
                 "stop_dispatched_during_idle": False}
    sm.advance("IDLE")
    g2 = sm.dispatch_guard("work2")
    m["work1_guard"], m["work2_guard"] = g1, g2
    sm.advance("WORK_2_VERIFIED")

    stale_backend = SyntheticCompletionBackend()
    stale_backend.publish_complete("prior-epoch-000", ["a", "b"], {})
    m["stale_control"] = {"attempted": True,
                          "observation": stale_backend.observe_completion(epoch_id, "a", 1.0)}

    att = contract.try_publish(epoch_id, ["a", "b"],
                               {"verified": True, "digest": "syn-digest", "case": WORK2_CASE})
    sm.advance("COMPLETION_PUBLISHED")
    dup = contract.try_publish(epoch_id, ["a", "b"],
                               {"verified": True, "digest": "syn-digest", "case": WORK2_CASE})
    fence_guard = sm.dispatch_guard("post_completion_probe")
    m["completion"] = {"attempts": contract.attempts, "published": contract.published,
                       "early_attempt": early, "duplicate_attempt": dup, "accepted": att}
    m["post_completion_fence"] = {"guard": fence_guard, "app_work_after_publication": False}

    obs_a = backend.observe_completion(epoch_id, "a", 5.0)
    obs_b = backend.observe_completion(epoch_id, "b", 5.0)
    obs_a["t_mono_done"], obs_b["t_mono_done"] = time.monotonic(), time.monotonic()
    m["observations"] = {"a": obs_a, "b": obs_b}

    sm.advance("CONNECTORS_LEAVING")
    m["departure"] = {"a_stop": {"rc": 0, "error": None,
                                 "t_mono_dispatched": time.monotonic()},
                      "b_stop": {"rc": 0, "error": None,
                                 "t_mono_dispatched": time.monotonic()}}
    m["root_alone"] = {"root_final": {"leave_observed": True, "final_membership": 1}}
    sm.advance("ROOT_ALONE")
    m["finalize"] = {"root_exit_path": "finalized_clean", "actor_pids_gone": True}
    sm.advance("FINALIZED")

    m["state_history"] = sm.history
    m["rejected_transitions"] = list(sm.rejected)
    m["final"] = {"cleanup_ran": True,
                  "owned_process_check": {
                      "all_owned_gone": True, "covers_island": True,
                      "details": [{"label": "island_root", "pid": root_pid},
                                  {"label": "island_actor_a", "pid": a_pid},
                                  {"label": "island_actor_b", "pid": b_pid}]},
                  "rundir_scoped_orphans": [],
                  "machine_wide_peer_scan_informational": []}
    return m


def synthetic_crossnode_run(x68):
    """Cross-node schema contract: the local clean run plus placement/attestation evidence."""
    m = synthetic_clean_run(x68)
    m["crossnode"] = {"slurm_job_id": "999999", "nodes": ["medusa00", "medusa01"],
                      "nodeA": "medusa00", "nodeB": "medusa01",
                      "nodeA_ip": "10.42.5.30", "nodeB_ip": "10.42.5.31",
                      "subnet": DEFAULT_SUBNET,
                      "ray_node_ids": {"nodeA": "raynodeA", "nodeB": "raynodeB"}}
    isl = m["island"]
    isl["placement"] = {"strategy": "NodeAffinitySchedulingStrategy", "soft": False,
                        "targets": {"a": "medusa00", "b": "medusa01"}}
    isl["a_identity"].update({"hostname": "medusa00.rostam.cct.lsu.edu", "node_id": "raynodeA"})
    isl["b_identity"].update({"hostname": "medusa01.rostam.cct.lsu.edu", "node_id": "raynodeB"})
    isl["a_endpoints"] = ["--hpx:agas=10.42.5.30:7911", "--hpx:hpx=10.42.5.30:7912"]
    isl["b_endpoints"] = ["--hpx:agas=10.42.5.30:7911", "--hpx:hpx=10.42.5.31:7913"]
    return m


# ---------------------------------------------------------------------------------------
# Selftests (pure logic; no Ray, no HPX, no Slurm)
# ---------------------------------------------------------------------------------------

def selftest():
    x68, err = import_exp68(DEFAULT_EXP68_DIR)
    checks, failed = [], []

    def check(label, ok):
        checks.append(label)
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            failed.append(label)

    check("exp68 module importable with required surface (incl. crossnode helpers)",
          err is None)
    if err is not None:
        print(f"\nselftest: cannot continue: {err}")
        return 1

    clean = synthetic_clean_run(x68)
    r = rollup(x68, clean)
    check("clean synthetic run passes all gates", r["passed"] and r["failure_class"] == "pass")
    check("clean run states exactly the supported negative claims, all true",
          all(r["negative_claims"].values()) and len(r["negative_claims"]) == 7)

    def expect(mutate, cls, label, base=None):
        mm = copy.deepcopy(base if base is not None else clean)
        mutate(mm)
        rr = rollup(x68, mm)
        check(label, (not rr["passed"]) and rr["failure_class"] == cls)

    # --- state machine ------------------------------------------------------------------
    sm = IslandStateMachine()
    ok_walk = True
    try:
        for st in STATES[1:]:
            sm.advance(st)
    except InvalidTransition:
        ok_walk = False
    check("state machine accepts the full valid walk", ok_walk and sm.state == "FINALIZED")
    for frm, to in (("READY", "WORK_2_VERIFIED"), ("IDLE", "COMPLETION_PUBLISHED"),
                    ("STARTING", "FINALIZED"), ("COMPLETION_PUBLISHED", "READY")):
        sm2 = IslandStateMachine()
        while sm2.state != frm:
            sm2.advance(_NEXT[sm2.state])
        raised = False
        try:
            sm2.advance(to)
        except InvalidTransition:
            raised = True
        check(f"invalid transition {frm} -> {to} raises deterministically",
              raised and sm2.rejected and sm2.rejected[-1] == {"from": frm, "to": to})
    smd = IslandStateMachine()
    check("dispatch is not allowed in STARTING", smd.dispatch_guard("w")["allowed"] is False)
    smd.advance("READY")
    check("dispatch is allowed in READY", smd.dispatch_guard("w")["allowed"] is True)

    # --- completion contract ------------------------------------------------------------
    smc = IslandStateMachine()
    ctr = CompletionContract(SyntheticCompletionBackend(), smc)
    a0 = ctr.try_publish("e", ["a", "b"], {"verified": True, "digest": "d"})
    check("publish attempt outside WORK_2_VERIFIED is rejected",
          a0["accepted"] is False and ctr.published is None)
    for st in ("READY", "WORK_1_VERIFIED", "IDLE"):
        smc.advance(st)
    a1 = ctr.try_publish("e", ["a", "b"], {"verified": False})
    check("publish attempt without verified final result is rejected", a1["accepted"] is False)
    smc.advance("WORK_2_VERIFIED")
    a2 = ctr.try_publish("e", ["a", "b"], {"verified": True, "digest": "d"})
    a3 = ctr.try_publish("e", ["a", "b"], {"verified": True, "digest": "d"})
    check("publish accepted exactly once from WORK_2_VERIFIED",
          a2["accepted"] is True and ctr.published is not None)
    check("duplicate publish attempt is rejected and recorded",
          a3["accepted"] is False and a3["already_published"] is True)

    # --- synthetic backend --------------------------------------------------------------
    sb = SyntheticCompletionBackend()
    rec = sb.publish_complete("e1", ["a", "b"], {"x": 1})
    ob = sb.observe_completion("e1", "a", 2.0)
    check("synthetic backend publish/observe round trip",
          ob["observed"] and ob["epoch_match"] and ob["verbatim"] == rec)
    ob2 = sb.observe_completion("e2", "a", 2.0)
    check("synthetic backend rejects a different-epoch publication as stale",
          ob2["observed"] is False and ob2["stale_epochs_rejected"] == ["e1"])

    # --- external witness backend (real tempdir) ----------------------------------------
    with tempfile.TemporaryDirectory() as td:
        eb = ExternalWitnessCompletionBackend(td, poll_s=0.02)
        t0 = time.monotonic()
        miss = eb.observe_completion("cur", "a", 0.15)
        check("external backend observation is bounded and monotonic when no marker exists",
              miss["observed"] is False and miss["elapsed_s"] >= 0.15
              and time.monotonic() - t0 < 5.0)
        eb.publish_complete("old-epoch", ["a", "b"], {})
        stale = eb.observe_completion("cur", "a", 0.15)
        check("external backend NEVER satisfies observation from a stale-epoch marker",
              stale["observed"] is False and stale["stale_epochs_rejected"] == ["old-epoch"])
        rec2 = eb.publish_complete("cur", ["a", "b"], {"final_result_digest": "d"})
        hit = eb.observe_completion("cur", "a", 2.0)
        check("external backend atomic publish then exact-epoch observation succeeds",
              hit["observed"] and hit["epoch_match"] and hit["verbatim"]["epoch_id"] == "cur"
              and os.path.basename(rec2["marker_path"]) == COMPLETION_MARKER)
        check("external backend records the marker verbatim (evidence, not inference)",
              hit["verbatim"]["payload"] == {"final_result_digest": "d"})

    # --- future-backend substitution surface --------------------------------------------
    class MinimalFutureBackend:
        name = "minimal_future_stub"

        def __init__(self):
            self._r = {}

        def publish_complete(self, epoch_id, connector_ids, payload):
            rec = {"backend": self.name, "epoch_id": epoch_id,
                   "connector_ids": sorted(connector_ids), "payload": dict(payload or {}),
                   "published_wall_ms": int(time.time() * 1000),
                   "published_mono": time.monotonic()}
            self._r[epoch_id] = rec
            return rec

        def observe_completion(self, epoch_id, connector_id, bound_s):
            rec = self._r.get(epoch_id)
            return {"backend": self.name, "connector_id": connector_id, "epoch_id": epoch_id,
                    "observed": rec is not None, "epoch_match": rec is not None,
                    "stale_epochs_rejected": sorted(e for e in self._r if e != epoch_id),
                    "bounded": True, "bound_s": bound_s, "elapsed_s": 0.0, "verbatim": rec}

    check("substitute backend satisfies the completion surface",
          backend_surface_ok(MinimalFutureBackend()))
    clean_sub = synthetic_clean_run(x68, backend=MinimalFutureBackend())
    rsub = rollup(x68, clean_sub)
    check("substitute backend passes the IDENTICAL state machine and gates",
          rsub["passed"] and clean_sub["backend"] == "minimal_future_stub")
    check("object without the surface is refused by the contract",
          not backend_surface_ok(object()))

    # --- gate mutations: idle semantics -------------------------------------------------
    expect(lambda mm: mm["idle"].__setitem__("idle_elapsed_s", 1.0),
           "idle_availability_failed", "idle shorter than the former fixed window fails")
    expect(lambda mm: mm["idle"]["post_health_b"].__setitem__("ok", False),
           "idle_availability_failed", "connector unhealthy after idle fails availability")
    expect(lambda mm: mm["idle"]["post_health_a"].__setitem__("membership", 2),
           "idle_availability_failed", "membership shrink during idle fails availability")
    expect(lambda mm: mm["idle"].__setitem__("stop_dispatched_during_idle", True),
           "idle_availability_failed", "any stop dispatched during idle fails (idle must not "
           "authorize departure)")

    # --- gate mutations: workloads ------------------------------------------------------
    expect(lambda mm: mm["island"]["work1"]["a_coord"].__setitem__("global_topk", []),
           "work1_failed", "work-1 oracle mismatch fails")
    expect(lambda mm: mm["island"]["work2"].__setitem__("name", WORK1_CASE),
           "work2_failed", "work 2 must be distinguishable from work 1 (same case id fails)")
    expect(lambda mm: mm["island"].__setitem__(
               "work2", copy.deepcopy(mm["island"]["work1"])),
           "work2_failed", "a stale work-1 result cannot satisfy the work-2 gate")
    expect(lambda mm: mm["work2_guard"].__setitem__("state", "COMPLETION_PUBLISHED"),
           "work2_failed", "work 2 dispatched from a non-IDLE state fails")

    # --- gate mutations: completion contract --------------------------------------------
    expect(lambda mm: mm["completion"]["attempts"][1].update(
               {"accepted": True, "state_at_attempt": "WORK_1_VERIFIED"}),
           "completion_contract_violated", "completion accepted before work-2 verification fails")
    expect(lambda mm: mm["completion"]["attempts"].append(
               dict(mm["completion"]["attempts"][-1], accepted=True)),
           "completion_contract_violated", "a second accepted publication fails (duplicate)")
    expect(lambda mm: mm["completion"].__setitem__("published", None),
           "completion_contract_violated", "missing publication record fails")
    expect(lambda mm: mm["completion"]["attempts"][1].update(
               {"accepted": True, "final_verified": False,
                "state_at_attempt": "WORK_2_VERIFIED"}),
           "completion_contract_violated", "publication without verified final result fails")

    # --- gate mutations: post-completion fence ------------------------------------------
    expect(lambda mm: mm["post_completion_fence"]["guard"].__setitem__("allowed", True),
           "post_completion_fence_breached", "dispatch allowed after completion fails")
    expect(lambda mm: mm["post_completion_fence"]["guard"].__setitem__("reached_hpx", True),
           "post_completion_fence_breached", "post-completion dispatch reaching HPX fails")
    expect(lambda mm: mm["post_completion_fence"].__setitem__("app_work_after_publication", True),
           "post_completion_fence_breached", "application work after publication fails")

    # --- gate mutations: observation ----------------------------------------------------
    expect(lambda mm: mm["observations"]["b"].update({"observed": False, "epoch_match": False}),
           "completion_observation_incomplete",
           "one connector observing while the other does not fails")
    expect(lambda mm: mm["observations"]["a"].__setitem__("epoch_id", "prior-epoch-000"),
           "completion_observation_incomplete", "observation for a different epoch fails")
    expect(lambda mm: mm["observations"]["a"].update({"elapsed_s": 99.0, "bound_s": 5.0}),
           "completion_observation_incomplete", "unbounded observation fails")

    # --- gate mutations: departure / root / finalize ------------------------------------
    expect(lambda mm: mm["departure"]["a_stop"].__setitem__("rc", 1),
           "departure_failed", "non-graceful connector stop fails")
    expect(lambda mm: mm["departure"]["b_stop"].__setitem__("t_mono_dispatched", 0.0),
           "departure_failed", "connector departure before publication fails")
    expect(lambda mm: mm["observations"]["a"].__setitem__(
               "t_mono_done", mm["departure"]["a_stop"]["t_mono_dispatched"] + 100.0),
           "departure_failed", "departure before that connector's observation fails")
    expect(lambda mm: mm["root_alone"]["root_final"].__setitem__("leave_observed", False),
           "root_alone_not_observed", "root finalization without observing membership return "
           "to one fails")
    expect(lambda mm: mm["root_alone"].__setitem__("root_final", None),
           "root_alone_not_observed", "missing root.final fails")
    expect(lambda mm: mm["finalize"].__setitem__("root_exit_path", "killed"),
           "finalize_failed", "non-clean root exit fails")
    expect(lambda mm: mm["finalize"].__setitem__("actor_pids_gone", False),
           "finalize_failed", "actor worker processes surviving finalize fails")

    # --- gate mutations: epoch scope / ordering / cleanup -------------------------------
    expect(lambda mm: mm["stale_control"]["observation"].__setitem__("observed", True),
           "epoch_scope_violated", "stale prior-epoch marker satisfying an observer fails")
    expect(lambda mm: mm["stale_control"].__setitem__("attempted", False),
           "epoch_scope_violated", "missing stale-rejection control fails")
    expect(lambda mm: mm["completion"]["published"].__setitem__("epoch_id", "prior-epoch-000"),
           "epoch_scope_violated", "publication under a foreign epoch id fails")
    expect(lambda mm: mm["state_history"].__setitem__(
               2, {"state": "IDLE", "t_mono": 0.0}),
           "invalid_ordering", "reordered state history fails")
    expect(lambda mm: mm["state_history"].pop(),
           "invalid_ordering", "incomplete state history fails")
    expect(lambda mm: mm["final"].__setitem__("cleanup_ran", False),
           "cleanup_incomplete", "cleanup-after-intermediate-failure not run fails")
    expect(lambda mm: mm["final"]["owned_process_check"].__setitem__("all_owned_gone", False),
           "orphan_detected", "recorded island process still alive fails")
    expect(lambda mm: mm["final"]["owned_process_check"].__setitem__("covers_island", False),
           "orphan_detected", "owned records not covering the island fails")
    expect(lambda mm: mm["final"].__setitem__("rundir_scoped_orphans", ["9999"]),
           "orphan_detected", "run-dir-scoped process fails cleanup")

    # --- owned-process sweep (PID-reuse discrimination) ---------------------------------
    owned = [{"label": "island_root", "pid": 1234, "lstart": "L1", "command": "C1"}]
    gone, _ = evaluate_owned_processes(owned, lambda rec: None)
    check("owned sweep: vanished process counts as gone", gone is True)
    gone, det = evaluate_owned_processes(owned,
                                         lambda rec: {"lstart": "L1", "command": "C1"})
    check("owned sweep: same start identity counts as alive (fails)",
          gone is False and det[0]["still_alive_ours"] is True)
    gone, _ = evaluate_owned_processes(owned,
                                       lambda rec: {"lstart": "OTHER", "command": "C1"})
    check("owned sweep: PID reuse with a different start identity is NOT ours", gone is True)
    check("owned records cover the island roles",
          records_cover_island([{"label": "island_root"}, {"label": "island_actor_a"},
                                {"label": "island_actor_b"}])
          and not records_cover_island([{"label": "island_root"}]))

    # --- cross-node schema + placement gates --------------------------------------------
    cx_clean = synthetic_crossnode_run(x68)
    rcx = rollup(x68, cx_clean)
    check("clean cross-node synthetic run passes all gates (incl. placement)",
          rcx["passed"] and "placement" in rcx["gates"])
    check("local synthetic run carries no placement gates", "placement" not in r["gates"])
    expect(lambda mm: mm["island"]["b_identity"].update(
               {"hostname": "medusa00.rostam.cct.lsu.edu", "node_id": "raynodeA"}),
           "crossnode_placement_failed", "same-node placement rejected", base=cx_clean)
    expect(lambda mm: mm["island"]["placement"].__setitem__("soft", True),
           "crossnode_placement_failed", "soft placement rejected", base=cx_clean)
    expect(lambda mm: mm["island"].__setitem__(
               "b_endpoints", ["--hpx:hpx=10.42.6.31:7913"]),
           "crossnode_placement_failed", "off-subnet parcelport endpoint rejected",
           base=cx_clean)
    expect(lambda mm: mm["crossnode"].__setitem__("slurm_job_id", ""),
           "crossnode_placement_failed", "missing Slurm job id rejected", base=cx_clean)
    expect(lambda mm: mm["crossnode"]["ray_node_ids"].__setitem__("nodeB", None),
           "crossnode_placement_failed", "unresolved Ray node ids rejected", base=cx_clean)

    # --- off-cluster / preflight discipline ---------------------------------------------
    pf = preflight("/nonexistent_exp68_dir_for_slice2a_selftest")
    check("preflight cleanly reports missing artifacts (skip path)",
          pf["ok"] is False and pf["problems"])
    pfc = preflight_crossnode(DEFAULT_EXP68_DIR, env={}, subnet=DEFAULT_SUBNET)
    check("crossnode preflight without Slurm env cleanly skips",
          pfc["ok"] is False and any("SLURM_JOB_ID" in p for p in pfc["problems"]))
    root_cmd = x68.peer_root_cmd(os.path.join(DEFAULT_EXP68_DIR, "build", "exp68_peer"),
                                 "/tmp/x", 12345)
    check("local root command contains no srun/sbatch/Slurm invocation",
          "srun" not in " ".join(root_cmd) and "sbatch" not in " ".join(root_cmd))
    pf2 = preflight(DEFAULT_EXP68_DIR)
    check("preflight sees real exp68 artifacts (informational)",
          isinstance(pf2.get("problems"), list))
    check("work-1 and work-2 cases exist in the exp68 matrix and differ",
          case_for(x68, WORK1_CASE)["name"] != case_for(x68, WORK2_CASE)["name"]
          and case_for(x68, WORK1_CASE)["V"] != case_for(x68, WORK2_CASE)["V"])

    n_fail = len(failed)
    print(f"\nselftest: {len(checks) - n_fail}/{len(checks)} passed"
          + (f"; FAILED: {failed}" if failed else ""))
    return 1 if failed else 0


# ---------------------------------------------------------------------------------------
# Live drivers (plan-based: local loopback or Rostam cross-node)
# ---------------------------------------------------------------------------------------

def _phase(m, event):
    m["phase_log"].append(event)
    m["phase_times_wall_ms"][event] = int(time.time() * 1000)


def _sha256_text(text):
    import hashlib  # noqa: PLC0415
    return hashlib.sha256(text.encode()).hexdigest()


def make_local_plan(x68, pf, args):
    """Local loopback plan. Contains no srun/Slurm invocation anywhere."""

    def draw_ports():
        while True:
            ports = {"root": x68.find_free_port(), "a": x68.find_free_port(),
                     "b": x68.find_free_port()}
            if len(set(ports.values())) == 3:
                return ports

    return {
        "kind": "local",
        "ports": draw_ports,
        "root_cmd": lambda island_dir, ports: x68.peer_root_cmd(pf["peer"], island_dir,
                                                                 ports["root"]),
        "endpoints": lambda k, ports: x68.actor_endpoints(ports["root"], ports[k]),
        "actor_options": lambda k: {"num_cpus": args.ray_num_cpus, "max_restarts": 0},
        "wait_file": x68._wait_for_file,
        "placement": None,
        "node_name": lambda k: None,
        "pid_gone": lambda pid, node, timeout: x68.wait_pid_gone(pid, timeout),
        "proc_identity": lambda pid, node: _proc_identity_local(pid),
    }


def make_crossnode_plan(x68, pf, args, cx, strat_a, strat_b, env):
    """Cross-node plan: exp68 crossnode commands (subnet-bound endpoints), deterministic ports,
    hard NodeAffinity, srun-mediated identity/pid checks for node-B processes."""

    def is_remote(node):
        return bool(node) and x68._short(node) != x68._short(socket.gethostname())

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
        "ports": lambda: {"root": args.port_base, "a": args.port_base + 1,
                          "b": args.port_base + 2},
        "root_cmd": lambda island_dir, ports: x68.crossnode_root_cmd(
            pf["peer"], island_dir, cx["nodeA_ip"], ports["root"], leave_timeout=45),
        "endpoints": lambda k, ports: x68.crossnode_actor_endpoints(
            cx["nodeA_ip"], ports["root"],
            cx["nodeA_ip"] if k == "a" else cx["nodeB_ip"], ports[k]),
        "actor_options": lambda k: {"num_cpus": args.ray_num_cpus, "max_restarts": 0,
                                    "scheduling_strategy": (strat_a if k == "a" else strat_b)},
        "wait_file": x68._wait_for_file_nfs,
        "placement": {"strategy": "NodeAffinitySchedulingStrategy", "soft": False,
                      "targets": {"a": cx["nodeA"], "b": cx["nodeB"]}},
        "node_name": lambda k: cx["nodeA"] if k in ("a", "root") else cx["nodeB"],
        "pid_gone": pid_gone,
        "proc_identity": proc_identity,
    }


def _run_case(x68, ray, isl, handles, work_key, case):
    V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
    a_lo, a_hi, b_lo, b_hi = 0, split, split, V
    a_loc = (isl.get("a_start") or {}).get("locality_id")
    b_loc = (isl.get("b_start") or {}).get("locality_id")
    cr = {"name": case["name"], "V": V, "split": split, "k": k, "seed": seed,
          "shard_a": [a_lo, a_hi], "shard_b": [b_lo, b_hi], "a_loc": a_loc, "b_loc": b_loc,
          "a_pid": (isl.get("a_identity") or {}).get("pid"),
          "b_pid": (isl.get("b_identity") or {}).get("pid")}
    a, b = handles["a"], handles["b"]
    cr["a_local"] = x68._ray_get(ray, a.local_topk.remote(a_lo, a_hi, seed, k), 30,
                                 f"{work_key}_a_local")
    cr["b_local"] = x68._ray_get(ray, b.local_topk.remote(b_lo, b_hi, seed, k), 30,
                                 f"{work_key}_b_local")
    cr["a_coord"] = x68._ray_get(ray, a.coordinate.remote(b_loc, a_lo, a_hi, b_lo, b_hi,
                                                          seed, k), 60, f"{work_key}_a_coord")
    cr["b_coord"] = x68._ray_get(ray, b.coordinate.remote(a_loc, b_lo, b_hi, a_lo, a_hi,
                                                          seed, k), 60, f"{work_key}_b_coord")
    cr["oracle_global"] = [[t, bits] for t, bits in x68.oracle_topk(0, V, seed, k)]
    isl[work_key] = cr
    return cr


def _island_lifecycle(x68, ray, args, m, sm, runs_dir, plan, pf, procs, actors,
                      register_owned, epoch_id):
    """The complete single-island explicit-completion lifecycle, plan-driven."""
    HpxActor = x68.build_actor_class(ray)
    island_dir = os.path.join(runs_dir, "island")
    os.makedirs(island_dir, exist_ok=True)
    ports = plan["ports"]()
    isl = {"bootdir": island_dir, "ports": ports}
    m["island"] = isl
    if plan.get("placement") is not None:
        isl["placement"] = plan["placement"]

    rcmd = plan["root_cmd"](island_dir, ports)
    root_proc, rlog = x68._popen(rcmd, island_dir, os.path.join(island_dir, "root.log"))
    procs.append((root_proc, rlog))
    plan["wait_file"](os.path.join(island_dir, "root.ready"), 60, procs=[root_proc])
    rr = x68._read_json(os.path.join(island_dir, "root.ready")) or {}
    isl["root_ready"], isl["root_argv"] = rr, rcmd
    if rr.get("pid"):
        register_owned("island_root", rr["pid"], plan["node_name"]("root"))
    _phase(m, "root_ready")

    handles = {"root_proc": root_proc}
    for k in ("a", "b"):
        ep = plan["endpoints"](k, ports)
        isl[f"{k}_endpoints"] = ep
        h = HpxActor.options(**plan["actor_options"](k)).remote(
            pf["build_dir"], args.hpx_threads, ep)
        actors.append(h)
        handles[k] = h
        isl[f"{k}_identity"] = x68._ray_get(ray, h.load_identity.remote(), 60, f"{k}_identity")
        placement = x68._ray_get(ray, h.ray_placement.remote(), 60, f"{k}_placement")
        if isinstance(isl[f"{k}_identity"], dict) and isinstance(placement, dict):
            isl[f"{k}_identity"].setdefault("actor_id", placement.get("actor_id"))
            isl[f"{k}_identity"].setdefault("node_id", placement.get("node_id"))
        isl[f"{k}_start"] = x68._ray_get(ray, h.start_hpx.remote(), 120, f"{k}_start")
        isl[f"{k}_child_report"] = x68._ray_get(ray, h.child_report.remote(), 30, f"{k}_child")
        if (isl[f"{k}_identity"] or {}).get("pid"):
            register_owned(f"island_actor_{k}", isl[f"{k}_identity"]["pid"],
                           plan["node_name"](k))
    isl["a_join_health"] = x68._ray_get(ray, handles["a"].health.remote(), 30, "a_join_health")
    isl["b_join_health"] = x68._ray_get(ray, handles["b"].health.remote(), 30, "b_join_health")
    if not (all(eval_startup(isl).values()) and all(eval_inprocess(isl).values())):
        raise RuntimeError("island bring-up gates failed")
    sm.advance("READY")
    _phase(m, "island_ready")

    m["work1_guard"] = sm.dispatch_guard("work1")
    if not m["work1_guard"]["allowed"]:
        raise RuntimeError("work1 dispatch not allowed in READY (instrumentation bug)")
    _run_case(x68, ray, isl, handles, "work1", case_for(x68, WORK1_CASE))
    if not all(eval_work(x68, isl, "work1", WORK1_CASE).values()):
        raise RuntimeError("work1 verification failed")
    sm.advance("WORK_1_VERIFIED")
    _phase(m, "work1_verified")

    contract = CompletionContract(ExternalWitnessCompletionBackend(island_dir), sm)
    early = contract.try_publish(epoch_id, ["a", "b"], {"verified": False})

    idle = {"configured_idle_s": args.idle_s,
            "former_fixed_serve_window_s": FORMER_FIXED_SERVE_WINDOW_S,
            "stop_dispatched_during_idle": False,
            "pre_health_a": x68._ray_get(ray, handles["a"].health.remote(), 30, "pre_h_a"),
            "pre_health_b": x68._ray_get(ray, handles["b"].health.remote(), 30, "pre_h_b")}
    t_idle0 = time.monotonic()
    time.sleep(args.idle_s)  # controller sends NOTHING to the island during this interval
    idle["idle_elapsed_s"] = time.monotonic() - t_idle0
    idle["post_health_a"] = x68._ray_get(ray, handles["a"].health.remote(), 30, "post_h_a")
    idle["post_health_b"] = x68._ray_get(ray, handles["b"].health.remote(), 30, "post_h_b")
    m["idle"] = idle
    sm.advance("IDLE")
    _phase(m, "idle_interval_survived")

    m["work2_guard"] = sm.dispatch_guard("work2")
    if not m["work2_guard"]["allowed"]:
        raise RuntimeError("work2 dispatch not allowed in IDLE (instrumentation bug)")
    w2 = _run_case(x68, ray, isl, handles, "work2", case_for(x68, WORK2_CASE))
    if not all(eval_work(x68, isl, "work2", WORK2_CASE).values()):
        raise RuntimeError("work2 verification failed")
    sm.advance("WORK_2_VERIFIED")
    _phase(m, "work2_verified")
    digest = _sha256_text(json.dumps(
        {"case": w2["name"], "oracle": w2["oracle_global"],
         "a": (w2.get("a_coord") or {}).get("global_topk"),
         "b": (w2.get("b_coord") or {}).get("global_topk")}, sort_keys=True, default=str))

    stale_dir = os.path.join(runs_dir, "stale_control")
    os.makedirs(stale_dir, exist_ok=True)
    stale_backend = ExternalWitnessCompletionBackend(stale_dir, poll_s=0.05)
    stale_backend.publish_complete("prior-epoch-000", ["a", "b"], {})
    m["stale_control"] = {"attempted": True, "marker_dir": stale_dir,
                          "observation": stale_backend.observe_completion(epoch_id, "a", 1.0)}
    _phase(m, "stale_control_rejected")

    att = contract.try_publish(epoch_id, ["a", "b"],
                               {"verified": True, "digest": digest, "case": w2["name"]})
    if not att["accepted"]:
        raise RuntimeError(f"completion publication rejected unexpectedly: {att}")
    sm.advance("COMPLETION_PUBLISHED")
    _phase(m, "completion_published")
    dup = contract.try_publish(epoch_id, ["a", "b"],
                               {"verified": True, "digest": digest, "case": w2["name"]})
    fence_guard = sm.dispatch_guard("post_completion_probe")
    # The guard is consulted BEFORE any actor call; allowed is False here, so NO dispatch is
    # performed and the attempt never reaches HPX.
    m["completion"] = {"attempts": contract.attempts, "published": contract.published,
                       "early_attempt": early, "duplicate_attempt": dup}
    m["post_completion_fence"] = {"guard": fence_guard, "app_work_after_publication": False}

    obs = {}
    for k in ("a", "b"):
        o = contract.backend.observe_completion(epoch_id, k, args.observe_bound_s)
        o["t_mono_done"] = time.monotonic()
        obs[k] = o
    m["observations"] = obs
    _phase(m, "all_connectors_observed_completion")

    sm.advance("CONNECTORS_LEAVING")
    dep = {}
    for k in ("a", "b"):
        t_disp = time.monotonic()
        stop = x68._ray_get(ray, handles[k].stop_hpx.remote(), 40, f"{k}_stop")
        dep[f"{k}_stop"] = {"rc": (stop or {}).get("rc"), "error": (stop or {}).get("error"),
                            "t_mono_dispatched": t_disp}
    m["departure"] = dep
    _phase(m, "connectors_left_gracefully")

    open(os.path.join(island_dir, "root.done"), "w").close()
    plan["wait_file"](os.path.join(island_dir, "root.final"), 60, procs=[root_proc])
    rf = x68._read_json(os.path.join(island_dir, "root.final"))
    m["root_alone"] = {"root_final": rf}
    if (rf or {}).get("leave_observed") is True:
        sm.advance("ROOT_ALONE")
        _phase(m, "root_alone_observed")

    exited, rc, killed = x68._wait_proc(root_proc, time.time() + 40)
    fz = {"root_exit_path": x68._exit_path(exited, rc, killed)}
    a_pid = (isl.get("a_identity") or {}).get("pid")
    b_pid = (isl.get("b_identity") or {}).get("pid")
    for h in actors:
        try:
            ray.kill(h)
        except Exception:  # noqa: BLE001
            pass
    fz["actor_pids_gone"] = (plan["pid_gone"](a_pid, plan["node_name"]("a"), 20)
                             and plan["pid_gone"](b_pid, plan["node_name"]("b"), 20))
    m["finalize"] = fz
    if fz["root_exit_path"] == "finalized_clean" and fz["actor_pids_gone"]:
        sm.advance("FINALIZED")
        _phase(m, "island_finalized")


def _finalize_and_write(x68, ray, m, sm, agg, runs_dir, agg_path, plan, procs, actors, owned):
    """Cleanup after ANY outcome + experiment-scoped orphan sweep, then outputs."""
    for h in actors:
        try:
            ray.kill(h)
        except Exception:  # noqa: BLE001
            pass
    for p, log in procs:
        x68._kill_group(p)
        try:
            log.close()
        except OSError:
            pass
    m["final"]["cleanup_ran"] = True
    all_gone, details = evaluate_owned_processes(
        owned, lambda rec: plan["proc_identity"](rec.get("pid"), rec.get("node")))
    m["final"]["owned_process_check"] = {"all_owned_gone": all_gone,
                                         "covers_island": records_cover_island(owned),
                                         "details": details}
    m["final"]["rundir_scoped_orphans"] = _rundir_scoped_orphans(runs_dir)
    m["final"]["machine_wide_peer_scan_informational"] = x68.peer_orphans()
    _phase(m, "final_orphan_sweep")
    try:
        ray.shutdown()
    except Exception:  # noqa: BLE001
        pass

    m["state_history"] = sm.history
    m["rejected_transitions"] = list(sm.rejected)
    r = rollup(x68, m)
    agg["overall"] = "pass" if r["passed"] else "fail"
    agg["failure_class"] = r["failure_class"]
    agg["gates"], agg["gates_failed"] = r["gates"], r["gates_failed"]
    agg["negative_claims"] = r["negative_claims"]
    agg["summary_claim"] = (SUMMARY_CLAIM if r["passed"] else
                            f"Slice 2A did not pass ({r['failure_class']}); see gates_failed.")
    agg["controller_exception"] = m.get("controller_exception")
    with open(os.path.join(runs_dir, "markers.json"), "w") as f:
        json.dump(m, f, indent=2, sort_keys=True, default=str)
    with open(agg_path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=True, default=str)
    print(f"[slice2a] overall: {agg['overall']} ({agg['failure_class']}) -> {agg_path}")
    if r["gates_failed"]:
        print(f"[slice2a] gates_failed: {json.dumps(r['gates_failed'], default=str)}")
    return 0


def _new_run(args, phase, pf):
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    prefix = "crossnode_" + (pf.get("slurm_job_id") or "nojob") + "_" if \
        phase == "rostam-cross-node" else ""
    runs_dir = os.path.join(RUNS_ROOT, f"{prefix}{runid}")
    os.makedirs(runs_dir, exist_ok=True)
    agg_path = args.aggregate or os.path.join(runs_dir, "aggregate.json")
    epoch_id = f"exp70s2a-{runid}"
    agg = {"experiment": "exp70_slice2a_explicit_completion", "increment": 2,
           "phase": phase, "backend": "external_witness", "runid": runid,
           "runs_dir": runs_dir, "epoch_id": epoch_id,
           "work1_case": WORK1_CASE, "work2_case": WORK2_CASE,
           "former_fixed_serve_window_s": FORMER_FIXED_SERVE_WINDOW_S,
           "configured_idle_s": args.idle_s, "provenance": _provenance(),
           "summary_claim_candidate": SUMMARY_CLAIM, "non_claims": NON_CLAIMS,
           "preflight": pf}
    return runid, runs_dir, agg_path, epoch_id, agg


def live_local_run(args):
    pf = preflight(args.exp68_dir, args.exp68_build_dir)
    _runid, runs_dir, agg_path, epoch_id, agg = _new_run(args, "local", pf)
    if not pf["ok"]:
        agg["overall"] = "skip"
        agg["reason"] = "; ".join(pf["problems"])
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice2a] SKIP: {agg['reason']} -> {agg_path}")
        return 0
    x68, _ = import_exp68(args.exp68_dir)
    import ray  # noqa: PLC0415
    plan = make_local_plan(x68, pf, args)
    sm = IslandStateMachine()
    m = {"epoch_id": epoch_id, "backend": "external_witness", "phase_log": [],
         "phase_times_wall_ms": {}, "controller_pid": os.getpid(), "final": {}}
    procs, actors, owned = [], [], []

    def register_owned(label, pid, node=None):
        ident = plan["proc_identity"](pid, node) or {}
        owned.append({"label": label, "pid": pid, "node": node,
                      "lstart": ident.get("lstart"), "command": ident.get("command")})

    try:
        ray.init(num_cpus=4, include_dashboard=False, ignore_reinit_error=True,
                 log_to_driver=False)
        _island_lifecycle(x68, ray, args, m, sm, runs_dir, plan, pf, procs, actors,
                          register_owned, epoch_id)
    except Exception as ex:  # noqa: BLE001
        m["controller_exception"] = f"{type(ex).__name__}: {ex}"
        m["controller_traceback"] = traceback.format_exc()[-2000:]
    return _finalize_and_write(x68, ray, m, sm, agg, runs_dir, agg_path, plan,
                               procs, actors, owned)


def crossnode_live_run(args):
    env = dict(os.environ)
    pf = preflight_crossnode(args.exp68_dir, env, args.subnet, args.exp68_build_dir)
    _runid, runs_dir, agg_path, epoch_id, agg = _new_run(args, "rostam-cross-node", pf)
    if not pf["ok"]:
        agg["overall"] = "skip"
        agg["reason"] = "; ".join(pf["problems"])
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice2a-crossnode] SKIP: {agg['reason']} -> {agg_path}")
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
        print(f"[slice2a-crossnode] PREFLIGHT FAIL: {agg['reason']}")
        return 0

    nodeA_ip = x68._local_subnet_ip(args.subnet)
    nodeB_ip = x68._node_subnet_ip(nodeB, args.subnet)
    cx = {"slurm_job_id": pf["slurm_job_id"], "nodelist": pf["slurm_nodelist"],
          "nodes": nodes, "nodeA": nodeA, "nodeB": nodeB,
          "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip, "subnet": args.subnet,
          "ray_node_ids": {}}
    import ray  # noqa: PLC0415
    temp_dir = f"/tmp/exp70s2a_ray_{pf['slurm_job_id']}_{_runid}"
    head = worker_b = None
    sm = IslandStateMachine()
    m = {"epoch_id": epoch_id, "backend": "external_witness", "crossnode": cx,
         "phase_log": [], "phase_times_wall_ms": {}, "controller_pid": os.getpid(),
         "final": {}}
    procs, actors, owned = [], [], []
    plan = None
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

        def register_owned(label, pid, node=None):
            ident = plan["proc_identity"](pid, node) or {}
            owned.append({"label": label, "pid": pid, "node": node,
                          "lstart": ident.get("lstart"), "command": ident.get("command")})

        _island_lifecycle(x68, ray, args, m, sm, runs_dir, plan, pf, procs, actors,
                          register_owned, epoch_id)
    except Exception as ex:  # noqa: BLE001
        m["controller_exception"] = f"{type(ex).__name__}: {ex}"
        m["controller_traceback"] = traceback.format_exc()[-2000:]
    finally:
        if plan is None:
            plan = make_local_plan(x68, pf, args)  # cleanup fallback (no srun anywhere)
    rc = _finalize_and_write(x68, ray, m, sm, agg, runs_dir, agg_path, plan,
                             procs, actors, owned)
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
    with open(agg_path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=True, default=str)
    return rc


# ---------------------------------------------------------------------------------------
# Curation of accepted local runs (no processes; raw artifacts never modified)
# ---------------------------------------------------------------------------------------

def json_load_quiet(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def curate_local(run_ids):
    """Curate accepted runs (no processes; raw artifacts copied unmodified). With no RUNIDs,
    curates every run whose aggregate says overall=pass."""
    import hashlib  # noqa: PLC0415
    import shutil  # noqa: PLC0415

    def sha256_file(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    curated_dir = os.path.join(RUNS_ROOT, "curated_local_evidence")
    os.makedirs(curated_dir, exist_ok=True)
    if not run_ids:
        run_ids = sorted(
            rid for rid in (os.listdir(RUNS_ROOT) if os.path.isdir(RUNS_ROOT) else [])
            if os.path.exists(os.path.join(RUNS_ROOT, rid, "aggregate.json"))
            and (json_load_quiet(os.path.join(RUNS_ROOT, rid, "aggregate.json")) or {})
            .get("overall") == "pass")
    entries = []
    for rid in run_ids:
        src = os.path.join(RUNS_ROOT, rid)
        agg_p, mk_p = os.path.join(src, "aggregate.json"), os.path.join(src, "markers.json")
        if not (os.path.exists(agg_p) and os.path.exists(mk_p)):
            print(f"[curate] SKIP {rid}: aggregate/markers missing under {src}")
            continue
        agg = json_load_quiet(agg_p) or {}
        mk = json_load_quiet(mk_p) or {}
        dst = os.path.join(curated_dir, rid)
        os.makedirs(dst, exist_ok=True)
        shutil.copy2(agg_p, os.path.join(dst, "aggregate.json"))
        shutil.copy2(mk_p, os.path.join(dst, "markers.json"))
        isl = mk.get("island") or {}
        entries.append({
            "runid": rid, "overall": agg.get("overall"),
            "failure_class": agg.get("failure_class"), "backend": agg.get("backend"),
            "phase": agg.get("phase"), "epoch_id": agg.get("epoch_id"),
            "sha256": {"aggregate": sha256_file(agg_p), "markers": sha256_file(mk_p)},
            "gates": agg.get("gates"), "gates_failed": agg.get("gates_failed"),
            "negative_claims": agg.get("negative_claims"),
            "work1_case": (isl.get("work1") or {}).get("name"),
            "work2_case": (isl.get("work2") or {}).get("name"),
            "idle": {k: (mk.get("idle") or {}).get(k) for k in
                     ("configured_idle_s", "former_fixed_serve_window_s", "idle_elapsed_s")},
            "completion_published": (mk.get("completion") or {}).get("published"),
            "observations": {k: {kk: ((mk.get("observations") or {}).get(k) or {}).get(kk)
                                 for kk in ("observed", "epoch_match", "elapsed_s", "bound_s")}
                             for k in ("a", "b")},
            "departure": mk.get("departure"),
            "root_final": (mk.get("root_alone") or {}).get("root_final"),
            "crossnode": mk.get("crossnode"),
            "final": mk.get("final"), "phase_log": mk.get("phase_log"),
        })
    all_pass = bool(entries) and all(e["overall"] == "pass" for e in entries)
    curated = {
        "experiment": "exp70_slice2a_explicit_completion",
        "curated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "curation_note": ("Raw artifacts copied unmodified from the ignored run area; "
                          "originals retained in place."),
        "provenance": _provenance(),
        "work1_case": WORK1_CASE, "work2_case": WORK2_CASE,
        "former_fixed_serve_window_s": FORMER_FIXED_SERVE_WINDOW_S,
        "summary_claim": SUMMARY_CLAIM if all_pass else "not all curated runs passed",
        "non_claims": NON_CLAIMS,
        "consistency": {"runs_curated": len(entries), "all_pass": all_pass},
        "runs": entries,
    }
    out_path = os.path.join(curated_dir, "curated_local_aggregate.json")
    with open(out_path, "w") as f:
        json.dump(curated, f, indent=2, sort_keys=True, default=str)
    digest = sha256_file(out_path)
    with open(out_path + ".sha256", "w") as f:
        f.write(f"{digest}  curated_local_aggregate.json\n")
    print(f"[curate] {len(entries)} run(s) -> {out_path}")
    print(f"[curate] sha256 {digest}")
    return 0


# ---------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="exp70 Slice 2A: explicit-completion contract, external backend")
    ap.add_argument("--selftest", action="store_true",
                    help="pure logic checks (no Ray, no HPX, no Slurm)")
    ap.add_argument("--phase", choices=("local", "rostam-cross-node"), default="local")
    ap.add_argument("--curate", nargs="*", metavar="RUNID", default=None,
                    help="curate accepted runs (default: all passing runs)")
    ap.add_argument("--exp68-dir", default=DEFAULT_EXP68_DIR)
    ap.add_argument("--exp68-build-dir", default=None,
                    help="exp68 build dir holding the peer binary and ext .so "
                         "(default: <exp68-dir>/build; Rostam uses <exp68-dir>/build_rostam)")
    ap.add_argument("--idle-s", type=float, default=DEFAULT_IDLE_S,
                    help="idle interval; must exceed the former fixed serving window "
                         f"({FORMER_FIXED_SERVE_WINDOW_S} s) to demonstrate the semantics")
    ap.add_argument("--observe-bound-s", type=float, default=10.0)
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--ray-num-cpus", type=int, default=1)
    ap.add_argument("--aggregate", default=None)
    ap.add_argument("--subnet", default=DEFAULT_SUBNET)
    ap.add_argument("--ray-port", type=int, default=6479)
    ap.add_argument("--port-base", type=int, default=7911)
    ap.add_argument("--node-a", default=None)
    ap.add_argument("--node-b", default=None)
    ap.add_argument("--ray-ready-timeout", type=int, default=180)
    ap.add_argument("--ray-init-timeout", type=int, default=180)
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    if args.curate is not None:
        return curate_local(args.curate)
    if args.phase == "rostam-cross-node":
        return crossnode_live_run(args)
    return live_local_run(args)


if __name__ == "__main__":
    sys.exit(main())
