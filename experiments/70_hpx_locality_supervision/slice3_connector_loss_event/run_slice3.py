#!/usr/bin/env python3
"""exp70 Slice 3A -- classified connector LOSS/DEPARTURE events for an actor-hosted HPX island.

QUESTION: can a connector's departure from the actor-hosted HPX island be surfaced as an
explicit, CLASSIFIED lifecycle event -- graceful departure vs unexpected loss -- decided only
from observable evidence, with NO HPX-native failure detection?

Slice 2A (../slice2_explicit_completion/) closed the FIRST half of the two-item lifecycle
contract: explicit normal completion. Slice 3A closes the SECOND half: a classified
departure/loss event. The surface stays backend-neutral so a future HPX-native backend
(Slice 3B) could implement it without changing the state machine, the classifier, or the gates.

TOPOLOGY (exp66/67/68 mechanism, reused IN PLACE and unmodified): one separately supervised,
WORK-FREE exp68_peer root (locality 0) plus two Ray actors each hosting an HPX connect-mode
locality IN-PROCESS via exp68_actor_ext, under one persistent Ray supervision plane.

TWO ARMS PER LIVE RUN, each a FRESH island epoch (fresh bootdir, disjoint ports, fresh actor
and process identities; Slice 1 epoch-isolation discipline):
  arm "graceful" -- connector B leaves through the validated disconnect/stop sequence after the
                    supervisor explicitly publishes completion (the Slice 2A path).
  arm "loss"     -- connector B is lost unexpectedly: controller-initiated SIGKILL of the
                    CROSS-CHECKED actor worker PID (exp53/Slice 1 lineage). No stop is ever
                    dispatched to it.

INJECTION-BLIND CLASSIFIER (the point of the slice): `classify_departure(evidence)` decides the
label from a FIXED allow-list of observable fields only. Blindness is structural, not a
convention: `_assert_blind` raises `BlindnessViolation` if the evidence mapping carries ANY key
outside `DEPARTURE_EVIDENCE_KEYS`, and the injection record is stored on a separate branch of
the marker tree that the collector never reads. Both arms feed the SAME collector and the SAME
classifier; the classifier never learns which arm produced the evidence.

CONTRACT (backend-neutral; two-method surface publish_event / observe_events):
  1. Each connector that joins produces a JOINED lifecycle event.
  2. A departing connector produces EXACTLY ONE terminal event.
  3. The terminal event carries a classification: `graceful_departure` or `unexpected_loss`.
  4. The classification is computed injection-blind from observable evidence only.
  5. Publication happens within a bounded interval after the departure is observed.
  6. The surviving connector observes the terminal event for the EXACT island epoch; events
     from prior epochs are rejected and can never satisfy an observation.
  7. No terminal event is published for a connector that has not departed.
  8. Teardown: the graceful arm finalizes through the root; the LOSS arm treats the island as
     poisoned and performs external whole-island teardown with NO graceful HPX attempt
     (exp51/exp53/Slice 1 lineage).
  9. No process remains (experiment-scoped owned sweep with PID-reuse discrimination).

STATES per arm (linear; invalid transitions fail deterministically):
  STARTING -> READY -> WORK_VERIFIED -> DEPARTED -> EVIDENCE_COLLECTED -> EVENT_PUBLISHED
  -> EVENT_OBSERVED -> TORN_DOWN -> FINALIZED

POST-LOSS MEMBERSHIP IS OBSERVATIONAL (Slice 1 lesson): after the loss, the survivor's bounded
membership probe is recorded and categorized but is NOT required to take any particular value.
A shrinking membership would be a FINDING, not a gate.

CROSS-NODE PHASE (`--phase rostam-cross-node`, NOT the default, never runs off-cluster): reuses
exp68's validated Slurm/Ray machinery. Node A hosts the controller/Ray head, the work-free root,
and actor A; node B hosts actor B (the departing/lost connector -- remote by construction in
both arms). Skips cleanly without a Slurm allocation.

CLAIM FENCE: application-contract / mechanism evidence only. NOT HPX-native loss detection, NOT
an HPX-native heartbeat, NOT eviction, NOT recovery or failover, NOT runtime-level membership
repair, NO performance claim, NOT production API. The classification is computed by the
supervisor from observable evidence; HPX is not claimed to report it.

Usage:
  python run_slice3.py --selftest               # pure logic checks (no Ray, no HPX, no Slurm)
  python run_slice3.py --phase local            # live local run (both arms)
  python run_slice3.py --phase rostam-cross-node  # cluster phase (skips cleanly off-cluster)
  python run_slice3.py --curate [RUNID ...]     # curate accepted local runs (no processes)
"""

import argparse
import copy
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
RUNS_ROOT = os.path.join(HERE, "_exp70_slice3_runs")

WORK_CASE = "cross_both"       # exp68 MATRIX: V=64 split=32 k=6 seed=1 (verified in BOTH arms)
EVENTS_MARKER = "island.events"
DEFAULT_SUBNET = "10.42.5."
DEFAULT_PUBLISH_BOUND_S = 15.0  # bound between departure observation and event publication
DEFAULT_OBSERVE_BOUND_S = 10.0  # bound on the survivor's observation of the terminal event

ARMS = ("graceful", "loss")
DEPARTING = "b"                # connector B departs in both arms; A is the survivor/observer
SURVIVOR = "a"

STATES = ("STARTING", "READY", "WORK_VERIFIED", "DEPARTED", "EVIDENCE_COLLECTED",
          "EVENT_PUBLISHED", "EVENT_OBSERVED", "TORN_DOWN", "FINALIZED")
_NEXT = {a: b for a, b in zip(STATES, STATES[1:])}
DISPATCH_STATES = ("READY",)

# Lifecycle event kinds (backend-neutral vocabulary).
EV_JOINED = "JOINED"
EV_DEPARTED = "DEPARTED"
CLS_GRACEFUL = "graceful_departure"
CLS_LOSS = "unexpected_loss"
CLS_INDETERMINATE = "indeterminate"

# The ONLY fields the classifier may see. Anything else -> BlindnessViolation.
#
# Every field is observable AT DEPARTURE TIME and independent of AGAS membership semantics.
# `root_leave_observed` is deliberately NOT here: exp68's work-free root reports a graceful
# leave only when membership returns to 1, i.e. after ALL connectors have gone, so it is not
# available when a single connector departs and it would make the classifier depend on
# island-wide teardown. It is still recorded per arm as corroboration, outside the evidence.
DEPARTURE_EVIDENCE_KEYS = frozenset({
    "connector",              # which connector departed
    "pid_gone",               # the hosting worker PID no longer exists (start-identity checked)
    "clean_disconnect_recorded",  # a validated disconnect/stop handshake was recorded
    "stop_rc",                # return code of the stop handshake, if any
    "actor_call_raised",      # a post-departure call to that actor failed
    "actor_error_type",       # the error type name, if any
})

SUMMARY_CLAIM = (
    "In an actor-hosted HPX island, a departing connector produced exactly one terminal "
    "lifecycle event whose graceful-vs-lost classification was computed injection-blind from "
    "observable evidence, was published within a bounded interval, and was observed by the "
    "surviving connector for the exact island epoch.")
NON_CLAIMS = (
    "The result does not demonstrate HPX-native loss detection, an HPX-native heartbeat, "
    "HPX-native eviction or membership repair, recovery or failover, runtime-level enforcement, "
    "or any performance improvement. The classification is computed by the supervisor from "
    "observable evidence; HPX is not claimed to report it.")

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
    "startup_failed", "inprocess_proof_failed", "work_failed", "departure_failed",
    "evidence_collection_failed", "classifier_blindness_violated", "misclassified",
    "event_contract_violated", "publication_unbounded", "event_observation_incomplete",
    "epoch_scope_violated", "teardown_failed", "invalid_ordering", "orphan_detected",
    "cleanup_incomplete", "arm_isolation_violated",
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
# Per-arm state machine
# ---------------------------------------------------------------------------------------

class InvalidTransition(Exception):
    pass


class ArmStateMachine:
    """Linear per-arm lifecycle. `advance` validates the successor; `dispatch_guard` is
    consulted BEFORE any application dispatch."""

    def __init__(self, arm, clock=time.monotonic):
        self.arm = arm
        self._clock = clock
        self.state = STATES[0]
        self.history = [{"state": self.state, "t_mono": self._clock()}]
        self.rejected = []

    def advance(self, to):
        if _NEXT.get(self.state) != to:
            self.rejected.append({"from": self.state, "to": to})
            raise InvalidTransition(f"[{self.arm}] invalid transition {self.state} -> {to}")
        self.state = to
        self.history.append({"state": to, "t_mono": self._clock()})

    def dispatch_guard(self, label):
        allowed = self.state in DISPATCH_STATES
        return {"label": label, "arm": self.arm, "state": self.state, "allowed": allowed,
                "reached_hpx": False if not allowed else None, "t_mono": self._clock()}


# ---------------------------------------------------------------------------------------
# Injection-blind classifier
# ---------------------------------------------------------------------------------------

class BlindnessViolation(Exception):
    """Raised when evidence offered to the classifier carries a field outside the allow-list.
    This is the structural guarantee that intent/injection data cannot reach the decision."""


def _assert_blind(evidence):
    if not isinstance(evidence, dict):
        raise BlindnessViolation(f"evidence must be a mapping, got {type(evidence).__name__}")
    extra = set(evidence) - DEPARTURE_EVIDENCE_KEYS
    if extra:
        raise BlindnessViolation(f"evidence carries non-observable fields: {sorted(extra)}")


def classify_departure(evidence):
    """Injection-blind departure classifier.

    Observable-only decision. It never sees which arm produced the evidence, whether a signal
    was sent, or any controller intent -- `_assert_blind` makes that structural.

    The event describes the HPX CONNECTOR leaving the island, not the hosting process dying.
    That distinction is the whole discriminator: a connector that leaves on purpose completes a
    validated disconnect and its host stays answerable; a connector that is lost takes its host
    with it.

      graceful_departure : a validated disconnect/stop handshake was recorded AND no
                           post-departure call to that connector's host failed.
      unexpected_loss    : the hosting PID is gone AND no clean disconnect was recorded AND a
                           post-departure call to that host failed.
      indeterminate      : anything else (recorded verbatim; never silently coerced).

    The two positive labels are complementary on BOTH `clean_disconnect_recorded` and
    `actor_call_raised`, so they cannot fire together.
    """
    _assert_blind(evidence)
    clean = evidence.get("clean_disconnect_recorded") is True
    pid_gone = evidence.get("pid_gone") is True
    raised = evidence.get("actor_call_raised") is True

    graceful = clean and not raised
    lost = pid_gone and not clean and raised
    if graceful and not lost:
        label = CLS_GRACEFUL
    elif lost and not graceful:
        label = CLS_LOSS
    else:
        label = CLS_INDETERMINATE
    return {"classification": label,
            "basis": {"clean_disconnect_recorded": clean, "pid_gone": pid_gone,
                      "actor_call_raised": raised},
            "connector": evidence.get("connector")}


def collect_departure_evidence(connector, pid_gone, clean_disconnect_recorded, stop_rc,
                               actor_call_raised, actor_error_type):
    """Builds the evidence mapping. Deliberately a fixed positional signature: there is no
    parameter through which an injection record or arm label could be smuggled in."""
    ev = {"connector": connector,
          "pid_gone": bool(pid_gone),
          "clean_disconnect_recorded": bool(clean_disconnect_recorded),
          "stop_rc": stop_rc,
          "actor_call_raised": bool(actor_call_raised),
          "actor_error_type": actor_error_type}
    _assert_blind(ev)
    return ev


def categorize_membership_probe(res):
    """Map a bounded survivor-probe result to (category, membership_or_None). Observational."""
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
        "membership_stale": ("The membership snapshot still contained the departed locality at "
                             "the observation point."),
        "membership_query_error": "The bounded membership probe raised; recorded verbatim.",
        "membership_query_timeout": "The bounded membership probe timed out; recorded verbatim.",
        "membership_other": ("The bounded membership probe returned an unexpected value; "
                             "recorded verbatim."),
    }.get(category, "no post-departure membership observation recorded")


# ---------------------------------------------------------------------------------------
# Lifecycle event contract + backends (surface: publish_event / observe_events)
# ---------------------------------------------------------------------------------------

def backend_surface_ok(backend):
    """Slice 3B substitution surface: duck-typed two-method contract + a name."""
    return (callable(getattr(backend, "publish_event", None))
            and callable(getattr(backend, "observe_events", None))
            and bool(getattr(backend, "name", None)))


class LifecycleEventContract:
    """Publication guard shared by ALL backends: a terminal DEPARTED event may be published
    only from the EVIDENCE_COLLECTED state, only for a connector recorded as departed, and only
    once. Every attempt is recorded."""

    def __init__(self, backend, sm):
        if not backend_surface_ok(backend):
            raise ValueError(f"backend does not satisfy the surface: {backend!r}")
        self.backend, self.sm = backend, sm
        self.attempts, self.terminal_published = [], {}

    def try_publish_terminal(self, epoch_id, connector, classification, evidence_digest,
                             departed_connectors):
        att = {"epoch_id": epoch_id, "connector": connector, "kind": EV_DEPARTED,
               "classification": classification, "state": self.sm.state,
               "t_mono": time.monotonic()}
        if self.sm.state != "EVIDENCE_COLLECTED":
            att.update(accepted=False, reason=f"wrong state: {self.sm.state}")
        elif connector not in departed_connectors:
            att.update(accepted=False, reason="connector has not departed")
        elif connector in self.terminal_published:
            att.update(accepted=False, reason="terminal event already published")
        else:
            self.backend.publish_event(epoch_id, connector, EV_DEPARTED,
                                       evidence_digest, classification)
            self.terminal_published[connector] = att["t_mono"]
            att.update(accepted=True, reason=None)
        self.attempts.append(att)
        return att

    def publish_joined(self, epoch_id, connector):
        self.backend.publish_event(epoch_id, connector, EV_JOINED, None, None)


class SyntheticLifecycleBackend:
    """In-memory backend; selftests only."""

    name = "synthetic"

    def __init__(self):
        self.events = []

    def publish_event(self, epoch_id, connector, kind, evidence_digest, classification):
        self.events.append({"epoch_id": epoch_id, "connector": connector, "kind": kind,
                            "evidence_digest": evidence_digest,
                            "classification": classification,
                            "published_mono": time.monotonic()})

    def observe_events(self, epoch_id, observer, bound_s, kind=EV_DEPARTED, clock=time.monotonic):
        t0 = clock()
        stale = []
        for e in self.events:
            if e["kind"] != kind:
                continue
            if e["epoch_id"] != epoch_id:
                stale.append(e["epoch_id"])
                continue
            return {"observer": observer, "observed": True, "epoch_match": True,
                    "epoch_id": epoch_id, "bounded": True, "elapsed_s": clock() - t0,
                    "stale_epochs_rejected": stale, "backend": self.name, "verbatim": e}
        return {"observer": observer, "observed": False, "epoch_match": False,
                "epoch_id": epoch_id, "bounded": (clock() - t0) <= bound_s,
                "elapsed_s": clock() - t0, "stale_epochs_rejected": stale,
                "backend": self.name, "verbatim": None}


class ExternalWitnessLifecycleBackend:
    """Live backend: an epoch-scoped `island.events` JSON document in the island boot directory,
    appended atomically (write temp + rename), observed by bounded MONOTONIC polling.

    Distinct from exp68's mechanical `root.done` / `root.final` markers, which are still used
    for the work-free root's own finalize path.
    """

    name = "external_witness"

    def __init__(self, island_dir, poll_s=0.05):
        self.island_dir = island_dir
        self.path = os.path.join(island_dir, EVENTS_MARKER)
        self.poll_s = poll_s

    def _read(self):
        try:
            with open(self.path) as f:
                doc = json.load(f)
            return doc if isinstance(doc, list) else []
        except (OSError, ValueError):
            return []

    def publish_event(self, epoch_id, connector, kind, evidence_digest, classification):
        events = self._read()
        events.append({"epoch_id": epoch_id, "connector": connector, "kind": kind,
                       "evidence_digest": evidence_digest, "classification": classification,
                       "published_mono": time.monotonic(),
                       "published_wall_ms": int(time.time() * 1000)})
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(events, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def observe_events(self, epoch_id, observer, bound_s, kind=EV_DEPARTED,
                       clock=time.monotonic):
        t0 = clock()
        stale = []
        while True:
            for e in self._read():
                if e.get("kind") != kind:
                    continue
                if e.get("epoch_id") != epoch_id:
                    if e.get("epoch_id") not in stale:
                        stale.append(e.get("epoch_id"))
                    continue
                return {"observer": observer, "observed": True, "epoch_match": True,
                        "epoch_id": epoch_id, "bounded": True, "elapsed_s": clock() - t0,
                        "stale_epochs_rejected": stale, "backend": self.name,
                        "marker_path": self.path, "verbatim": e}
            if clock() - t0 >= bound_s:
                return {"observer": observer, "observed": False, "epoch_match": False,
                        "epoch_id": epoch_id, "bounded": False, "elapsed_s": clock() - t0,
                        "stale_epochs_rejected": stale, "backend": self.name,
                        "marker_path": self.path, "verbatim": None}
            time.sleep(self.poll_s)


# ---------------------------------------------------------------------------------------
# Gate evaluators (pure functions over the per-arm marker mapping)
# ---------------------------------------------------------------------------------------

def case_for(x68, name):
    for c in x68.MATRIX:
        if c["name"] == name:
            return c
    raise KeyError(f"exp68 MATRIX has no case {name!r}")


def short_host(h):
    return (h or "").split(".")[0]


def _pid_identity_set(ident, start):
    """The in-process proof: the Ray worker pid, the worker's own os.getpid(), and the pid that
    ran the HPX action must all be ONE pid (exp66 lineage)."""
    return {ident.get("pid"), ident.get("os_getpid"), start.get("pid")}


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
        pids = _pid_identity_set(ident, start)
        out[f"{k}_pid_identity"] = None not in pids and len(pids) == 1
        out[f"{k}_no_hpx_children"] = rep.get("checked") is True and not rep.get("hpx_children")
    return out


def eval_cluster_attestation(cx):
    nodes = cx.get("nodes") or []
    ids = cx.get("ray_node_ids") or {}
    eps = cx.get("parcelport_endpoints") or []
    subnet = cx.get("subnet") or ""
    return {
        "attest_slurm_job_id_present": bool(cx.get("slurm_job_id")),
        "attest_two_distinct_nodes": (len(set(map(short_host, nodes))) >= 2
                                      and short_host(cx.get("nodeA"))
                                      != short_host(cx.get("nodeB"))),
        "attest_subnet_ips_resolved": (bool(cx.get("nodeA_ip")) and bool(cx.get("nodeB_ip"))
                                       and str(cx.get("nodeA_ip")).startswith(subnet)
                                       and str(cx.get("nodeB_ip")).startswith(subnet)),
        "attest_ray_node_ids_resolved": (bool(ids.get("nodeA")) and bool(ids.get("nodeB"))
                                         and ids.get("nodeA") != ids.get("nodeB")),
        "parcelport_endpoints_on_subnet": (bool(eps)
                                           and all(str(e).startswith(subnet) for e in eps)),
    }


def eval_placement(m):
    """Per-arm hard-placement attestation; only present in the cross-node phase."""
    cx = m.get("crossnode")
    if cx is None:
        return {}
    out = dict(eval_cluster_attestation(cx))
    ids = cx.get("ray_node_ids") or {}
    arms = m.get("arms") or {}
    for arm in ARMS:
        isl = (arms.get(arm) or {}).get("island") or {}
        a_i, b_i = isl.get("a_identity") or {}, isl.get("b_identity") or {}
        out[f"{arm}_actor_a_on_nodeA"] = (a_i.get("node_id") == ids.get("nodeA")
                                          and ids.get("nodeA") is not None)
        out[f"{arm}_actor_b_on_nodeB"] = (b_i.get("node_id") == ids.get("nodeB")
                                          and ids.get("nodeB") is not None)
        out[f"{arm}_actors_on_distinct_ray_nodes"] = (
            a_i.get("node_id") is not None and b_i.get("node_id") is not None
            and a_i["node_id"] != b_i["node_id"])
    out["strategy_hard_node_affinity"] = (
        ((m.get("placement") or {}).get("strategy") == "NodeAffinitySchedulingStrategy")
        and (m.get("placement") or {}).get("soft") is False)
    return out


def eval_work(x68, isl, expected_case_name):
    cr = isl.get("work") or {}
    if not cr:
        return {"work_recorded": False}
    case = {"name": cr.get("name"), "V": cr.get("V"), "split": cr.get("split"),
            "k": cr.get("k"), "seed": cr.get("seed")}
    try:
        gates, _aux = x68.eval_case(case, cr)
    except Exception:  # noqa: BLE001
        gates = {"eval_case_crashed": False}
    gates = dict(gates)
    gates["case_is_expected"] = cr.get("name") == expected_case_name
    a_i, b_i = isl.get("a_identity") or {}, isl.get("b_identity") or {}
    gates["workload_pids_match_identities"] = (cr.get("a_pid") == a_i.get("pid")
                                               and cr.get("b_pid") == b_i.get("pid"))
    return gates


def eval_departure(arm_m, arm):
    """Structural facts about the departure itself (NOT the classification)."""
    dep = arm_m.get("departure") or {}
    out = {
        "departure_recorded": bool(dep),
        "departing_connector_is_b": dep.get("connector") == DEPARTING,
        "departure_after_work_verified": dep.get("state_at_departure") == "WORK_VERIFIED",
    }
    if arm == "graceful":
        out["graceful_stop_dispatched"] = dep.get("stop_dispatched") is True
        out["graceful_stop_succeeded"] = dep.get("stop_rc") == 0
        out["no_signal_sent"] = dep.get("signal") is None
        # The HPX locality left; its host is expected to remain answerable until teardown.
        out["host_still_alive_after_graceful_departure"] = dep.get("pid_gone") is False
    else:
        out["loss_signal_recorded"] = (dep.get("signal") == "SIGKILL"
                                       and dep.get("signal_sent") is True)
        out["no_stop_dispatched"] = dep.get("stop_dispatched") is not True
        out["victim_pid_cross_checked"] = dep.get("pid_cross_checked") is True
        out["lost_host_pid_gone"] = dep.get("pid_gone") is True
    return out


def eval_evidence_blindness(arm_m):
    ev = arm_m.get("evidence") or {}
    bl = arm_m.get("blindness") or {}
    return {
        "evidence_recorded": bool(ev),
        "evidence_keys_within_allowlist": bool(ev) and set(ev) <= DEPARTURE_EVIDENCE_KEYS,
        "classifier_accepted_evidence": bl.get("assert_blind_ok") is True,
        "injection_record_not_in_evidence": bl.get("injection_fields_in_evidence") == [],
        "injection_record_stored_separately": bl.get("injection_stored_outside_evidence") is True,
    }


def eval_classification(arm_m, arm):
    cl = arm_m.get("classification") or {}
    expected = CLS_GRACEFUL if arm == "graceful" else CLS_LOSS
    return {
        "classification_recorded": bool(cl),
        "classification_matches_arm": cl.get("classification") == expected,
        "classification_is_terminal_label": cl.get("classification") in (CLS_GRACEFUL, CLS_LOSS),
        "basis_recorded": bool(cl.get("basis")),
    }


def eval_event_contract(arm_m, epoch_id):
    ec = arm_m.get("event_contract") or {}
    attempts = ec.get("attempts") or []
    accepted = [a for a in attempts if a.get("accepted")]
    rejected_wrong_state = [a for a in attempts
                            if not a.get("accepted") and "wrong state" in str(a.get("reason"))]
    dup = [a for a in attempts
           if not a.get("accepted") and a.get("reason") == "terminal event already published"]
    return {
        "exactly_one_terminal_accepted": len(accepted) == 1,
        "terminal_is_for_departing_connector": bool(accepted)
                                               and accepted[0].get("connector") == DEPARTING,
        "terminal_epoch_matches_island": bool(accepted)
                                         and accepted[0].get("epoch_id") == epoch_id,
        "premature_publication_attempted": bool(rejected_wrong_state),
        "premature_publication_rejected": len(rejected_wrong_state) >= 1,
        "duplicate_publication_attempted": bool(dup),
        "duplicate_publication_rejected": len(dup) >= 1,
        "no_terminal_for_non_departed": ec.get("non_departed_attempt_rejected") is True,
        "joined_events_for_both_connectors": sorted(ec.get("joined") or []) == ["a", "b"],
    }


def eval_publication_bound(arm_m, bound_s):
    pb = arm_m.get("publication") or {}
    lat = pb.get("departure_to_publication_s")
    return {
        "publication_latency_recorded": isinstance(lat, (int, float)),
        "publication_within_bound": isinstance(lat, (int, float)) and 0 <= lat <= bound_s,
    }


def eval_observation(arm_m, epoch_id):
    ob = arm_m.get("observation") or {}
    return {
        "survivor_observed": ob.get("observed") is True,
        "survivor_is_a": ob.get("observer") == SURVIVOR,
        "epoch_match": ob.get("epoch_match") is True and ob.get("epoch_id") == epoch_id,
        "bounded": ob.get("bounded") is True,
        "observed_classification_matches":
            ((ob.get("verbatim") or {}).get("classification")
             == (arm_m.get("classification") or {}).get("classification")),
    }


def eval_epoch_scope(arm_m, epoch_id):
    st = arm_m.get("stale_control") or {}
    obs = st.get("observation") or {}
    return {
        "stale_control_attempted": st.get("attempted") is True,
        "stale_event_from_prior_epoch_rejected": obs.get("observed") is not True,
        "publication_epoch_matches_island": (arm_m.get("epoch_id") == epoch_id),
        "marker_in_island_dir": str((arm_m.get("observation") or {}).get("marker_path", ""))\
            .startswith(str((arm_m.get("island") or {}).get("bootdir", "\0"))),
    }


def eval_teardown(arm_m, arm):
    td = arm_m.get("teardown") or {}
    out = {
        "teardown_recorded": bool(td),
        "actor_pids_gone": td.get("actor_pids_gone") is True,
        "root_process_gone": td.get("root_process_gone") is True,
    }
    if arm == "graceful":
        out["root_finalized_clean"] = td.get("root_exit_path") == "finalized_clean"
        out["root_leave_observed"] = td.get("root_leave_observed") is True
    else:
        # Poisoned island: exp51/exp53/Slice 1 lineage -- NO graceful HPX attempt is made.
        out["no_graceful_hpx_attempt_after_loss"] = td.get("graceful_attempted") is False
        out["external_teardown_used"] = td.get("mode") == "external_whole_island"
    return out


def eval_ordering(arm_m):
    hist = [h.get("state") for h in (arm_m.get("state_history") or [])]
    return {
        "state_history_complete_and_ordered": hist == list(STATES),
        "no_rejected_transitions": (arm_m.get("rejected_transitions") or []) == [],
    }


def eval_arm_isolation(m):
    """Both arms must be genuinely fresh islands: disjoint ports, distinct boot dirs, distinct
    actor identities and PIDs. (Ray may reuse pre-spawned idle workers, so PID ordering across
    arms carries no meaning -- disjointness is what is required.)"""
    arms = m.get("arms") or {}
    g, l = (arms.get("graceful") or {}), (arms.get("loss") or {})
    gi, li = (g.get("island") or {}), (l.get("island") or {})
    gp = set((gi.get("ports") or {}).values())
    lp = set((li.get("ports") or {}).values())
    g_ids = {(gi.get(f"{k}_identity") or {}).get("actor_id") for k in ("a", "b")}
    l_ids = {(li.get(f"{k}_identity") or {}).get("actor_id") for k in ("a", "b")}
    g_pids = {(gi.get(f"{k}_identity") or {}).get("pid") for k in ("a", "b")}
    l_pids = {(li.get(f"{k}_identity") or {}).get("pid") for k in ("a", "b")}
    return {
        "both_arms_ran": bool(g) and bool(l),
        "disjoint_ports": bool(gp) and bool(lp) and not (gp & lp),
        "distinct_bootdirs": bool(gi.get("bootdir")) and gi.get("bootdir") != li.get("bootdir"),
        "distinct_actor_ids": bool(g_ids - {None}) and not (g_ids & l_ids),
        "distinct_worker_pids": bool(g_pids - {None}) and not (g_pids & l_pids),
        "distinct_epochs": bool(g.get("epoch_id")) and g.get("epoch_id") != l.get("epoch_id"),
    }


def eval_classifier_discriminates(m):
    """The headline gate: ONE classifier, TWO arms, opposite correct labels."""
    arms = m.get("arms") or {}
    g = ((arms.get("graceful") or {}).get("classification") or {}).get("classification")
    l = ((arms.get("loss") or {}).get("classification") or {}).get("classification")
    return {
        "graceful_arm_labeled_graceful": g == CLS_GRACEFUL,
        "loss_arm_labeled_loss": l == CLS_LOSS,
        "labels_differ": bool(g) and bool(l) and g != l,
        "single_classifier_used": (m.get("classifier_fingerprint_stable") is True),
    }


def eval_final(m):
    fin = m.get("final") or {}
    chk = fin.get("owned_process_check") or {}
    return {
        "cleanup_ran": fin.get("cleanup_ran") is True,
        "owned_processes_gone": chk.get("all_owned_gone") is True,
        "owned_records_cover_island": chk.get("covers_island") is True,
        "no_rundir_scoped_processes": (fin.get("rundir_scoped_orphans") or []) == [],
    }


def rollup(x68, m, publish_bound_s):
    """Compose all gate groups; first failing group in a fixed order names the failure class."""
    epochs = {arm: ((m.get("arms") or {}).get(arm) or {}).get("epoch_id") for arm in ARMS}
    gates = {}
    for arm in ARMS:
        am = (m.get("arms") or {}).get(arm) or {}
        isl = am.get("island") or {}
        gates[f"{arm}_startup"] = eval_startup(isl)
        gates[f"{arm}_inprocess"] = eval_inprocess(isl)
        gates[f"{arm}_work"] = eval_work(x68, isl, WORK_CASE)
        gates[f"{arm}_departure"] = eval_departure(am, arm)
        gates[f"{arm}_evidence_blindness"] = eval_evidence_blindness(am)
        gates[f"{arm}_classification"] = eval_classification(am, arm)
        gates[f"{arm}_event_contract"] = eval_event_contract(am, epochs[arm])
        gates[f"{arm}_publication_bound"] = eval_publication_bound(am, publish_bound_s)
        gates[f"{arm}_observation"] = eval_observation(am, epochs[arm])
        gates[f"{arm}_epoch_scope"] = eval_epoch_scope(am, epochs[arm])
        gates[f"{arm}_teardown"] = eval_teardown(am, arm)
        gates[f"{arm}_ordering"] = eval_ordering(am)
    gates["arm_isolation"] = eval_arm_isolation(m)
    gates["classifier_discriminates"] = eval_classifier_discriminates(m)
    gates["final"] = eval_final(m)
    if m.get("crossnode") is not None:
        gates["placement"] = eval_placement(m)

    gates = {k: v for k, v in gates.items() if v}
    failed = {k: sorted([kk for kk, vv in v.items() if vv is not True])
              for k, v in gates.items() if not all(v.values())}

    order = [("placement", "crossnode_placement_failed")]
    for arm in ARMS:
        order += [
            (f"{arm}_startup", "startup_failed"),
            (f"{arm}_inprocess", "inprocess_proof_failed"),
            (f"{arm}_work", "work_failed"),
            (f"{arm}_departure", "departure_failed"),
            (f"{arm}_evidence_blindness", "classifier_blindness_violated"),
            (f"{arm}_classification", "misclassified"),
            (f"{arm}_event_contract", "event_contract_violated"),
            (f"{arm}_publication_bound", "publication_unbounded"),
            (f"{arm}_observation", "event_observation_incomplete"),
            (f"{arm}_epoch_scope", "epoch_scope_violated"),
            (f"{arm}_teardown", "teardown_failed"),
            (f"{arm}_ordering", "invalid_ordering"),
        ]
    order += [("arm_isolation", "arm_isolation_violated"),
              ("classifier_discriminates", "misclassified"),
              ("final", "cleanup_incomplete")]

    failure_class = "pass"
    for key, cls in order:
        if key in gates and key in failed:
            failure_class = cls
            break
    if m.get("controller_exception") and failure_class == "pass":
        failure_class = "invalid_instrumentation"
    passed = not failed and not m.get("controller_exception")
    return {"passed": passed, "failure_class": failure_class if not passed else "pass",
            "gates": gates, "gates_failed": failed,
            "negative_claims": {"hpx_native_loss_detection_claimed": False,
                                "hpx_native_heartbeat_claimed": False,
                                "eviction_claimed": False, "recovery_claimed": False,
                                "performance_claimed": False,
                                "speedup_computed": False, "ratio_reported": False}}


# ---------------------------------------------------------------------------------------
# Experiment-scoped process accounting (Slice 1/2 discipline, two-arm variant)
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
    need = set()
    for arm in ARMS:
        need |= {f"{arm}_island_root", f"{arm}_island_actor_a", f"{arm}_island_actor_b"}
    return need <= labels


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


def _sha256_text(text):
    import hashlib  # noqa: PLC0415
    return hashlib.sha256(text.encode()).hexdigest()


def _classifier_fingerprint():
    """Stable identity of the decision function actually used, so 'one classifier, two arms'
    is checkable rather than asserted."""
    import inspect  # noqa: PLC0415
    return _sha256_text(inspect.getsource(classify_departure)
                        + inspect.getsource(_assert_blind)
                        + repr(sorted(DEPARTURE_EVIDENCE_KEYS)))


# ---------------------------------------------------------------------------------------
# Synthetic runs (schema contracts for the selftests; exercise the REAL state machine,
# contract, classifier, and a backend -- no processes)
# ---------------------------------------------------------------------------------------

def _synthetic_health(loc, pid):
    return {"ok": True, "pid": pid, "membership": 3, "locality_id": loc}


def _synthetic_arm(x68, arm, epoch_id, ports, pids, actor_ids, backend=None,
                   publish_bound_s=DEFAULT_PUBLISH_BOUND_S):
    """One synthetic arm that walks the REAL state machine, contract, and classifier."""
    sm = ArmStateMachine(arm)
    backend = backend if backend is not None else SyntheticLifecycleBackend()
    contract = LifecycleEventContract(backend, sm)
    isl = {
        "bootdir": f"/synthetic/{arm}", "ports": ports,
        "root_ready": {"pid": pids["root"], "locality_id": 0},
        "a_identity": {"pid": pids["a"], "os_getpid": pids["a"],
                       "actor_id": actor_ids["a"], "node_id": "nodeA"},
        "b_identity": {"pid": pids["b"], "os_getpid": pids["b"],
                       "actor_id": actor_ids["b"], "node_id": "nodeB"},
        "a_start": {"started": True, "locality_id": 1, "pid": pids["a"], "membership": 2},
        "b_start": {"started": True, "locality_id": 2, "pid": pids["b"], "membership": 3},
        "a_child_report": {"checked": True, "hpx_children": 0},
        "b_child_report": {"checked": True, "hpx_children": 0},
        "a_join_health": _synthetic_health(1, pids["a"]),
        "b_join_health": _synthetic_health(2, pids["b"]),
    }
    am = {"arm": arm, "epoch_id": epoch_id, "island": isl}

    for k in ("a", "b"):
        contract.publish_joined(epoch_id, k)
    sm.advance("READY")

    cr = x68._synthetic_case_result(case_for(x68, WORK_CASE), a_loc=1, b_loc=2,
                                    a_pid=pids["a"], b_pid=pids["b"])
    isl["work"] = cr
    sm.advance("WORK_VERIFIED")

    # premature terminal publication attempt (must be rejected: wrong state)
    early = contract.try_publish_terminal(epoch_id, DEPARTING, CLS_GRACEFUL, "x", {DEPARTING})

    graceful = arm == "graceful"
    am["departure"] = {
        "connector": DEPARTING, "state_at_departure": "WORK_VERIFIED",
        "pid_gone": not graceful,
        "stop_dispatched": graceful, "stop_rc": 0 if graceful else None,
        "signal": None if graceful else "SIGKILL", "signal_sent": (not graceful) or None,
        "pid_cross_checked": True, "t_mono_departed": time.monotonic(),
    }
    if graceful:
        am["departure"]["signal_sent"] = None
    sm.advance("DEPARTED")

    ev = collect_departure_evidence(
        connector=DEPARTING, pid_gone=not graceful,
        clean_disconnect_recorded=graceful, stop_rc=0 if graceful else None,
        actor_call_raised=not graceful,
        actor_error_type=None if graceful else "RayActorError")
    am["evidence"] = ev
    am["blindness"] = {
        "assert_blind_ok": True,
        "injection_fields_in_evidence": sorted(set(ev) - DEPARTURE_EVIDENCE_KEYS),
        "injection_stored_outside_evidence": True,
    }
    am["injection_record_private"] = {"arm": arm, "signal": None if graceful else "SIGKILL"}
    sm.advance("EVIDENCE_COLLECTED")

    cl = classify_departure(ev)
    am["classification"] = cl
    digest = _sha256_text(json.dumps(ev, sort_keys=True, default=str))
    t_dep = am["departure"]["t_mono_departed"]
    att = contract.try_publish_terminal(epoch_id, DEPARTING, cl["classification"], digest,
                                        {DEPARTING})
    dup = contract.try_publish_terminal(epoch_id, DEPARTING, cl["classification"], digest,
                                        {DEPARTING})
    non_dep = contract.try_publish_terminal(epoch_id, SURVIVOR, CLS_LOSS, digest, {DEPARTING})
    sm.advance("EVENT_PUBLISHED")
    am["publication"] = {"departure_to_publication_s": max(0.0, att["t_mono"] - t_dep)}
    am["event_contract"] = {"attempts": contract.attempts, "joined": ["a", "b"],
                            "non_departed_attempt_rejected": not non_dep["accepted"],
                            "early_attempt": early, "duplicate_attempt": dup}

    am["observation"] = backend.observe_events(epoch_id, SURVIVOR, DEFAULT_OBSERVE_BOUND_S)
    am["observation"].setdefault("marker_path", isl["bootdir"] + "/" + EVENTS_MARKER)
    sm.advance("EVENT_OBSERVED")

    stale_backend = SyntheticLifecycleBackend()
    stale_backend.publish_event("prior-epoch-000", DEPARTING, EV_DEPARTED, "d", CLS_LOSS)
    am["stale_control"] = {"attempted": True,
                           "observation": stale_backend.observe_events(epoch_id, SURVIVOR, 0.2)}

    am["teardown"] = {
        "mode": "graceful_root_finalize" if graceful else "external_whole_island",
        "graceful_attempted": bool(graceful),
        "root_exit_path": "finalized_clean" if graceful else "killed",
        "root_leave_observed": graceful, "root_process_gone": True, "actor_pids_gone": True,
    }
    sm.advance("TORN_DOWN")
    sm.advance("FINALIZED")
    am["state_history"] = sm.history
    am["rejected_transitions"] = list(sm.rejected)
    return am


def synthetic_clean_run(x68, backend_factory=None):
    m = {"arms": {}, "phase_log": [], "phase_times_wall_ms": {},
         "classifier_fingerprint": _classifier_fingerprint(),
         "classifier_fingerprint_stable": True, "final": {}}
    specs = {
        "graceful": (dict(root=7911, a=7912, b=7913), dict(root=100, a=101, b=102),
                     dict(a="actA1", b="actB1")),
        "loss": (dict(root=7931, a=7932, b=7933), dict(root=200, a=201, b=202),
                 dict(a="actA2", b="actB2")),
    }
    for arm, (ports, pids, aids) in specs.items():
        bk = backend_factory() if backend_factory else None
        m["arms"][arm] = _synthetic_arm(x68, arm, f"exp70s3a-{arm}-000", ports, pids, aids,
                                        backend=bk)
    owned = []
    for arm in ARMS:
        for lbl, pid in (("island_root", specs[arm][1]["root"]),
                         ("island_actor_a", specs[arm][1]["a"]),
                         ("island_actor_b", specs[arm][1]["b"])):
            owned.append({"label": f"{arm}_{lbl}", "pid": pid, "node": None,
                          "lstart": "L", "command": "C"})
    m["final"] = {"cleanup_ran": True, "rundir_scoped_orphans": [],
                  "owned_process_check": {"all_owned_gone": True,
                                          "covers_island": records_cover_island(owned),
                                          "details": []}}
    return m


def synthetic_crossnode_run(x68):
    m = synthetic_clean_run(x68)
    for arm in ARMS:
        isl = m["arms"][arm]["island"]
        isl["a_identity"]["node_id"] = "idA"
        isl["b_identity"]["node_id"] = "idB"
    m["crossnode"] = {"slurm_job_id": "999999", "nodes": ["medusa00", "medusa01"],
                      "nodeA": "medusa00", "nodeB": "medusa01",
                      "nodeA_ip": "10.42.5.30", "nodeB_ip": "10.42.5.31",
                      "subnet": DEFAULT_SUBNET,
                      "ray_node_ids": {"nodeA": "idA", "nodeB": "idB"},
                      "parcelport_endpoints": ["10.42.5.30:7911", "10.42.5.31:7913"]}
    m["placement"] = {"strategy": "NodeAffinitySchedulingStrategy", "soft": False,
                      "targets": {"a": "medusa00", "b": "medusa01"}}
    return m


# ---------------------------------------------------------------------------------------
# Selftest
# ---------------------------------------------------------------------------------------

def selftest():
    x68, err = import_exp68(DEFAULT_EXP68_DIR)
    if err:
        print(f"selftest: SKIP (exp68 module unavailable: {err})")
        return 0

    results = []

    def check(name, cond):
        results.append((name, bool(cond)))

    def expect_class(mutate, expected_class, name, base=None):
        mm = copy.deepcopy(base if base is not None else synthetic_clean_run(x68))
        mutate(mm)
        r = rollup(x68, mm, DEFAULT_PUBLISH_BOUND_S)
        check(f"{name} -> {expected_class}",
              (not r["passed"]) and r["failure_class"] == expected_class)

    # --- classifier: blindness is structural -------------------------------------------
    good = collect_departure_evidence(DEPARTING, True, False, None, True, "RayActorError")
    check("clean loss evidence classifies as unexpected_loss",
          classify_departure(good)["classification"] == CLS_LOSS)
    grace = collect_departure_evidence(DEPARTING, False, True, 0, False, None)
    check("clean graceful evidence classifies as graceful_departure",
          classify_departure(grace)["classification"] == CLS_GRACEFUL)

    for extra, label in ((("injected", True), "injected"),
                         (("arm", "loss"), "arm label"),
                         (("signal", "SIGKILL"), "signal"),
                         (("controller_intent", "kill"), "controller intent")):
        bad = dict(good)
        bad[extra[0]] = extra[1]
        raised = False
        try:
            classify_departure(bad)
        except BlindnessViolation:
            raised = True
        check(f"classifier refuses evidence carrying {label}", raised)

    raised = False
    try:
        classify_departure(["not", "a", "mapping"])
    except BlindnessViolation:
        raised = True
    check("classifier refuses a non-mapping evidence object", raised)

    check("collect_departure_evidence emits only allow-listed keys",
          set(good) == DEPARTURE_EVIDENCE_KEYS)
    check("allow-list contains no intent/injection field",
          not (DEPARTURE_EVIDENCE_KEYS & {"arm", "injected", "signal", "signal_sent",
                                          "controller_intent", "victim"}))

    # ambiguity is never silently coerced
    amb = collect_departure_evidence(DEPARTING, True, True, 0, True, "RayActorError")
    check("contradictory evidence -> indeterminate (never coerced)",
          classify_departure(amb)["classification"] == CLS_INDETERMINATE)
    amb2 = collect_departure_evidence(DEPARTING, False, False, None, False, None)
    check("empty-signal evidence -> indeterminate",
          classify_departure(amb2)["classification"] == CLS_INDETERMINATE)
    check("a lost connector that also shows a clean disconnect is NOT called lost",
          classify_departure(collect_departure_evidence(
              DEPARTING, True, True, 0, True, "RayActorError")
          )["classification"] != CLS_LOSS)
    check("classifier fingerprint is stable across calls",
          _classifier_fingerprint() == _classifier_fingerprint())

    # --- state machine ------------------------------------------------------------------
    sm = ArmStateMachine("t")
    check("initial state is STARTING", sm.state == "STARTING")
    check("dispatch not allowed before READY", sm.dispatch_guard("w")["allowed"] is False)
    sm.advance("READY")
    check("dispatch allowed in READY", sm.dispatch_guard("w")["allowed"] is True)
    sm.advance("WORK_VERIFIED")
    check("dispatch fenced after WORK_VERIFIED",
          sm.dispatch_guard("late")["allowed"] is False
          and sm.dispatch_guard("late")["reached_hpx"] is False)
    bad = False
    try:
        sm.advance("FINALIZED")
    except InvalidTransition:
        bad = True
    check("skipping states is rejected", bad and sm.rejected)
    check("full state order is the documented one", list(STATES)[:3]
          == ["STARTING", "READY", "WORK_VERIFIED"])

    # --- event contract -----------------------------------------------------------------
    sm2 = ArmStateMachine("t2")
    c = LifecycleEventContract(SyntheticLifecycleBackend(), sm2)
    a1 = c.try_publish_terminal("e", DEPARTING, CLS_LOSS, "d", {DEPARTING})
    check("terminal publication rejected outside EVIDENCE_COLLECTED", not a1["accepted"])
    for s in ("READY", "WORK_VERIFIED", "DEPARTED", "EVIDENCE_COLLECTED"):
        sm2.advance(s)
    a2 = c.try_publish_terminal("e", SURVIVOR, CLS_LOSS, "d", {DEPARTING})
    check("terminal publication rejected for a non-departed connector", not a2["accepted"])
    a3 = c.try_publish_terminal("e", DEPARTING, CLS_LOSS, "d", {DEPARTING})
    check("terminal publication accepted in EVIDENCE_COLLECTED for a departed connector",
          a3["accepted"])
    a4 = c.try_publish_terminal("e", DEPARTING, CLS_LOSS, "d", {DEPARTING})
    check("duplicate terminal publication rejected", not a4["accepted"])
    check("exactly one terminal recorded as published", len(c.terminal_published) == 1)

    raised = False
    try:
        LifecycleEventContract(object(), sm2)
    except ValueError:
        raised = True
    check("contract rejects a backend without the two-method surface", raised)
    check("synthetic backend satisfies the surface",
          backend_surface_ok(SyntheticLifecycleBackend()))

    # --- backends: epoch scoping --------------------------------------------------------
    bk = SyntheticLifecycleBackend()
    bk.publish_event("epoch-OLD", DEPARTING, EV_DEPARTED, "d", CLS_LOSS)
    o = bk.observe_events("epoch-NEW", SURVIVOR, 0.2)
    check("event from a prior epoch cannot satisfy an observation", o["observed"] is False)
    check("stale epoch is recorded as rejected", o["stale_epochs_rejected"] == ["epoch-OLD"])
    bk.publish_event("epoch-NEW", DEPARTING, EV_DEPARTED, "d", CLS_GRACEFUL)
    o2 = bk.observe_events("epoch-NEW", SURVIVOR, 0.2)
    check("matching-epoch event is observed", o2["observed"] and o2["epoch_match"])
    check("JOINED events do not satisfy a DEPARTED observation",
          SyntheticLifecycleBackend().observe_events("e", SURVIVOR, 0.05)["observed"] is False)

    # external witness backend against a real temp dir
    import tempfile  # noqa: PLC0415
    with tempfile.TemporaryDirectory() as td:
        eb = ExternalWitnessLifecycleBackend(td, poll_s=0.01)
        check("external backend satisfies the surface", backend_surface_ok(eb))
        miss = eb.observe_events("e1", SURVIVOR, 0.15)
        check("external backend observation is bounded when nothing is published",
              miss["observed"] is False and miss["bounded"] is False
              and miss["elapsed_s"] >= 0.15)
        eb.publish_event("e1", DEPARTING, EV_DEPARTED, "dig", CLS_LOSS)
        hit = eb.observe_events("e1", SURVIVOR, 1.0)
        check("external backend observes its own published terminal event",
              hit["observed"] and hit["verbatim"]["classification"] == CLS_LOSS)
        check("external marker lives in the island dir",
              hit["marker_path"] == os.path.join(td, EVENTS_MARKER))
        eb.publish_event("e1", "a", EV_JOINED, None, None)
        check("publishing more events preserves the earlier terminal event",
              eb.observe_events("e1", SURVIVOR, 0.5)["observed"] is True)
        check("no temp file is left beside the marker",
              not os.path.exists(os.path.join(td, EVENTS_MARKER + ".tmp")))

    # backend substitution (the Slice 3B surface)
    class SubstituteBackend:
        name = "substitute"

        def __init__(self):
            self._ev = []

        def publish_event(self, epoch_id, connector, kind, evidence_digest, classification):
            self._ev.append((epoch_id, connector, kind, evidence_digest, classification))

        def observe_events(self, epoch_id, observer, bound_s, kind=EV_DEPARTED,
                           clock=time.monotonic):
            for e in self._ev:
                if e[2] == kind and e[0] == epoch_id:
                    return {"observer": observer, "observed": True, "epoch_match": True,
                            "epoch_id": epoch_id, "bounded": True, "elapsed_s": 0.0,
                            "stale_epochs_rejected": [], "backend": self.name,
                            "verbatim": {"epoch_id": e[0], "connector": e[1], "kind": e[2],
                                         "evidence_digest": e[3], "classification": e[4]}}
            return {"observer": observer, "observed": False, "epoch_match": False,
                    "epoch_id": epoch_id, "bounded": True, "elapsed_s": 0.0,
                    "stale_epochs_rejected": [], "backend": self.name, "verbatim": None}

    sub_run = synthetic_clean_run(x68, backend_factory=SubstituteBackend)
    r_sub = rollup(x68, sub_run, DEFAULT_PUBLISH_BOUND_S)
    check("a substitute backend passes the identical state machine and gates", r_sub["passed"])
    check("substitute backend actually used",
          sub_run["arms"]["loss"]["observation"]["backend"] == "substitute")

    # --- clean synthetic run ------------------------------------------------------------
    clean = synthetic_clean_run(x68)
    r = rollup(x68, clean, DEFAULT_PUBLISH_BOUND_S)
    check("clean two-arm synthetic run passes all gates", r["passed"])
    check("clean run reports failure_class pass", r["failure_class"] == "pass")
    check("clean run has no placement gates in the local shape", "placement" not in r["gates"])
    check("both arms produce a full ordered state history",
          all(all(eval_ordering(clean["arms"][a]).values()) for a in ARMS))
    check("headline gate: one classifier discriminates both arms",
          all(eval_classifier_discriminates(clean).values()))
    check("negative claims are fenced off",
          all(v is False for v in r["negative_claims"].values()))

    # --- targeted failure injection into the ROLLUP (never the classifier) ---------------
    def set_arm(arm, path, value):
        def _m(mm):
            node = mm["arms"][arm]
            for p in path[:-1]:
                node = node[p]
            node[path[-1]] = value
        return _m

    expect_class(set_arm("loss", ["classification", "classification"], CLS_GRACEFUL),
                 "misclassified", "loss arm labelled graceful")
    expect_class(set_arm("graceful", ["classification", "classification"], CLS_LOSS),
                 "misclassified", "graceful arm labelled lost")
    expect_class(set_arm("loss", ["classification", "classification"], CLS_INDETERMINATE),
                 "misclassified", "indeterminate label in the loss arm")
    expect_class(set_arm("loss", ["blindness", "injection_fields_in_evidence"], ["signal"]),
                 "classifier_blindness_violated", "injection field found in evidence")
    expect_class(set_arm("loss", ["blindness", "injection_stored_outside_evidence"], False),
                 "classifier_blindness_violated", "injection record not kept separate")
    expect_class(set_arm("graceful", ["island", "b_child_report"],
                         {"checked": True, "hpx_children": 2}),
                 "inprocess_proof_failed", "HPX child process in the graceful arm")
    expect_class(set_arm("graceful", ["island", "a_identity"],
                         {"pid": 101, "os_getpid": 999, "actor_id": "actA1",
                          "node_id": "nodeA"}),
                 "inprocess_proof_failed", "worker pid != os.getpid() (not in-process)")
    expect_class(set_arm("loss", ["island", "b_join_health"], {"ok": True, "membership": 2}),
                 "startup_failed", "island never reached membership 3")
    expect_class(set_arm("loss", ["departure", "stop_dispatched"], True),
                 "departure_failed", "stop dispatched to a connector that was lost")
    expect_class(set_arm("graceful", ["departure", "signal"], "SIGKILL"),
                 "departure_failed", "signal sent in the graceful arm")
    expect_class(set_arm("loss", ["departure", "pid_cross_checked"], False),
                 "departure_failed", "victim pid not cross-checked")
    expect_class(set_arm("loss", ["departure", "pid_gone"], False),
                 "departure_failed", "lost host pid still present")
    expect_class(set_arm("graceful", ["departure", "stop_rc"], 1),
                 "departure_failed", "graceful stop returned nonzero")
    expect_class(set_arm("loss", ["publication", "departure_to_publication_s"], 999.0),
                 "publication_unbounded", "publication outside the bound")
    expect_class(set_arm("loss", ["observation", "observed"], False),
                 "event_observation_incomplete", "survivor never observed the event")
    expect_class(set_arm("loss", ["observation", "epoch_id"], "other-epoch"),
                 "event_observation_incomplete", "observation carried the wrong epoch")
    expect_class(set_arm("loss", ["stale_control", "observation"],
                         {"observed": True, "epoch_match": True}),
                 "epoch_scope_violated", "stale prior-epoch event satisfied an observation")
    expect_class(set_arm("loss", ["teardown", "graceful_attempted"], True),
                 "teardown_failed", "graceful HPX attempt on the poisoned island")
    expect_class(set_arm("graceful", ["teardown", "root_exit_path"], "killed"),
                 "teardown_failed", "graceful arm root did not finalize cleanly")
    expect_class(set_arm("loss", ["teardown", "actor_pids_gone"], False),
                 "teardown_failed", "actor pid still alive after teardown")

    def drop_dup(mm):
        att = mm["arms"]["loss"]["event_contract"]["attempts"]
        mm["arms"]["loss"]["event_contract"]["attempts"] = [
            a for a in att if a.get("reason") != "terminal event already published"]
    expect_class(drop_dup, "event_contract_violated", "duplicate publication never attempted")

    def two_accepted(mm):
        att = mm["arms"]["loss"]["event_contract"]["attempts"]
        extra = dict(att[-1])
        extra["accepted"] = True
        att.append(extra)
    expect_class(two_accepted, "event_contract_violated", "two terminal events accepted")

    expect_class(lambda mm: mm["arms"]["loss"]["event_contract"].__setitem__(
        "non_departed_attempt_rejected", False),
        "event_contract_violated", "terminal event allowed for a non-departed connector")
    expect_class(lambda mm: mm["arms"]["loss"]["event_contract"].__setitem__(
        "joined", ["a"]),
        "event_contract_violated", "a connector produced no JOINED event")

    expect_class(lambda mm: mm["arms"]["loss"]["island"]["ports"].update(
        {"root": 7911, "a": 7912, "b": 7913}),
        "arm_isolation_violated", "arms shared a port block")
    expect_class(lambda mm: mm["arms"]["loss"]["island"]["a_identity"].__setitem__(
        "actor_id", "actA1"),
        "arm_isolation_violated", "arms shared an actor id")
    def share_worker_pid(mm):
        """Reuse the graceful arm's connector-B pid CONSISTENTLY (identity, in-process
        cross-check and the whole workload record), so the only thing left broken is arm
        isolation -- otherwise this mutation would trip the earlier work/in-process gates and
        the test would not be exercising the isolation gate at all."""
        isl = mm["arms"]["loss"]["island"]
        isl["b_identity"]["pid"] = 102
        isl["b_identity"]["os_getpid"] = 102
        isl["b_start"]["pid"] = 102
        isl["b_join_health"]["pid"] = 102
        isl["work"] = x68._synthetic_case_result(case_for(x68, WORK_CASE), a_loc=1, b_loc=2,
                                                 a_pid=isl["a_identity"]["pid"], b_pid=102)
    expect_class(share_worker_pid, "arm_isolation_violated", "arms shared a worker pid")
    expect_class(lambda mm: mm.__setitem__("classifier_fingerprint_stable", False),
                 "misclassified", "classifier changed between arms")

    expect_class(lambda mm: mm["arms"]["loss"]["state_history"].pop(),
                 "invalid_ordering", "truncated state history")
    expect_class(lambda mm: mm["arms"]["loss"]["rejected_transitions"].append(
        {"from": "READY", "to": "FINALIZED"}),
        "invalid_ordering", "a rejected transition occurred")
    expect_class(lambda mm: mm["final"]["owned_process_check"].__setitem__(
        "all_owned_gone", False),
        "cleanup_incomplete", "an owned process survived")
    expect_class(lambda mm: mm["final"].__setitem__("rundir_scoped_orphans", ["999"]),
                 "cleanup_incomplete", "run-dir-scoped process remained")
    expect_class(lambda mm: mm["final"]["owned_process_check"].__setitem__(
        "covers_island", False),
        "cleanup_incomplete", "owned records did not cover both islands")
    expect_class(lambda mm: mm.__setitem__("controller_exception", "boom"),
                 "invalid_instrumentation", "controller raised")

    # --- owned-process accounting -------------------------------------------------------
    rec = {"label": "loss_island_actor_b", "pid": 4242, "node": None,
           "lstart": "Mon Jul 19 00:00:00 2026", "command": "ray::HpxActor"}
    gone, _ = evaluate_owned_processes([rec], lambda r: None)
    check("owned sweep: vanished process counts as gone", gone)
    same, _ = evaluate_owned_processes(
        [rec], lambda r: {"lstart": rec["lstart"], "command": rec["command"]})
    check("owned sweep: same start identity counts as alive (fails)", not same)
    reuse, _ = evaluate_owned_processes(
        [rec], lambda r: {"lstart": "Tue Jul 20 11:11:11 2026", "command": "unrelated"})
    check("owned sweep: PID reuse with a different start identity is NOT ours", reuse)
    check("owned records must cover BOTH arms' islands",
          records_cover_island([{"label": f"{a}_island_{p}"}
                                for a in ARMS for p in ("root", "actor_a", "actor_b")])
          and not records_cover_island([{"label": "graceful_island_root"}]))

    # --- cross-node shape ---------------------------------------------------------------
    cx_clean = synthetic_crossnode_run(x68)
    r_cx = rollup(x68, cx_clean, DEFAULT_PUBLISH_BOUND_S)
    check("clean cross-node synthetic run passes all gates (incl. placement)", r_cx["passed"])
    check("cross-node run carries placement gates", "placement" in r_cx["gates"])
    check("local synthetic run carries no placement gates",
          "placement" not in rollup(x68, synthetic_clean_run(x68),
                                    DEFAULT_PUBLISH_BOUND_S)["gates"])
    expect_class(lambda mm: mm["crossnode"].__setitem__("nodeB", "medusa00"),
                 "crossnode_placement_failed", "same-node placement rejected", base=cx_clean)
    expect_class(lambda mm: mm["placement"].__setitem__("soft", True),
                 "crossnode_placement_failed", "soft placement rejected", base=cx_clean)
    expect_class(lambda mm: mm["crossnode"].__setitem__(
        "parcelport_endpoints", ["10.42.6.30:7911"]),
        "crossnode_placement_failed", "off-subnet parcelport endpoint rejected", base=cx_clean)
    expect_class(lambda mm: mm["crossnode"].__setitem__("slurm_job_id", ""),
                 "crossnode_placement_failed", "missing Slurm job id rejected", base=cx_clean)
    expect_class(lambda mm: mm["crossnode"]["ray_node_ids"].__setitem__("nodeB", None),
                 "crossnode_placement_failed", "unresolved Ray node ids rejected", base=cx_clean)
    expect_class(lambda mm: mm["arms"]["loss"]["island"]["b_identity"].__setitem__(
        "node_id", "idA"),
        "crossnode_placement_failed", "actor B not on node B", base=cx_clean)

    # --- preflight discipline (no Slurm commands off-cluster) ---------------------------
    pf = preflight("/definitely/not/here")
    check("preflight cleanly reports missing artifacts (skip path)",
          pf["ok"] is False and pf["problems"])
    pfc = preflight_crossnode(DEFAULT_EXP68_DIR, env={}, subnet=DEFAULT_SUBNET)
    check("crossnode preflight without Slurm env cleanly skips",
          pfc["ok"] is False and any("SLURM_JOB_ID" in p for p in pfc["problems"]))
    root_cmd = x68.peer_root_cmd("/tmp/peer", "/tmp/island", 7911)
    check("local root command contains no srun/sbatch/Slurm invocation",
          "srun" not in " ".join(root_cmd) and "sbatch" not in " ".join(root_cmd))
    real_pf = preflight(DEFAULT_EXP68_DIR)
    check("preflight sees real exp68 artifacts (informational)", isinstance(real_pf, dict))
    check("work case exists in the exp68 matrix",
          case_for(x68, WORK_CASE)["name"] == WORK_CASE)

    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    npass = sum(1 for _, ok in results if ok)
    print(f"\nselftest: {npass}/{len(results)} passed")
    return 0 if npass == len(results) else 1


# ---------------------------------------------------------------------------------------
# Live drivers (plan-based: local loopback or Rostam cross-node)
# ---------------------------------------------------------------------------------------

def _phase(m, event):
    m["phase_log"].append(event)
    m["phase_times_wall_ms"][event] = int(time.time() * 1000)


def make_local_plan(x68, pf, args):
    """Local loopback plan. Contains no srun/Slurm invocation anywhere."""

    def draw_ports(_arm):
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
        "kill_pid": lambda pid, node: os.kill(pid, signal.SIGKILL),
        "kill_method": "os.kill(SIGKILL)",
        "pid_gone": lambda pid, node, timeout: x68.wait_pid_gone(pid, timeout),
        "proc_identity": lambda pid, node: _proc_identity_local(pid),
    }


def make_crossnode_plan(x68, pf, args, cx, strat_a, strat_b, env):
    """Cross-node plan: exp68 crossnode commands (subnet-bound endpoints), per-arm disjoint
    deterministic ports, hard NodeAffinity, srun-mediated identity/kill/pid checks on node B."""

    def is_remote(node):
        return bool(node) and x68._short(node) != x68._short(socket.gethostname())

    def ports_for(arm):
        base = args.port_base + (0 if arm == "graceful" else args.arm_port_stride)
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
        "ports": ports_for,
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
        "kill_pid": kill_pid,
        "kill_method": "srun kill -9 (remote) / os.kill (local)",
        "pid_gone": pid_gone,
        "proc_identity": proc_identity,
    }


def _run_case(x68, ray, isl, handles, case):
    V, split, k, seed = case["V"], case["split"], case["k"], case["seed"]
    a_lo, a_hi, b_lo, b_hi = 0, split, split, V
    a_loc = (isl.get("a_start") or {}).get("locality_id")
    b_loc = (isl.get("b_start") or {}).get("locality_id")
    cr = {"name": case["name"], "V": V, "split": split, "k": k, "seed": seed,
          "shard_a": [a_lo, a_hi], "shard_b": [b_lo, b_hi], "a_loc": a_loc, "b_loc": b_loc,
          "a_pid": (isl.get("a_identity") or {}).get("pid"),
          "b_pid": (isl.get("b_identity") or {}).get("pid")}
    a, b = handles["a"], handles["b"]
    cr["a_local"] = x68._ray_get(ray, a.local_topk.remote(a_lo, a_hi, seed, k), 30, "a_local")
    cr["b_local"] = x68._ray_get(ray, b.local_topk.remote(b_lo, b_hi, seed, k), 30, "b_local")
    cr["a_coord"] = x68._ray_get(ray, a.coordinate.remote(b_loc, a_lo, a_hi, b_lo, b_hi,
                                                          seed, k), 60, "a_coord")
    cr["b_coord"] = x68._ray_get(ray, b.coordinate.remote(a_loc, b_lo, b_hi, a_lo, a_hi,
                                                          seed, k), 60, "b_coord")
    cr["oracle_global"] = [[t, bits] for t, bits in x68.oracle_topk(0, V, seed, k)]
    isl["work"] = cr
    return cr


def _run_arm(x68, ray, args, m, arm, runs_dir, plan, pf, procs, actors, register_owned,
             epoch_id):
    """One complete arm: fresh island -> verified work -> departure -> blind classification ->
    bounded publication -> survivor observation -> arm-appropriate teardown."""
    sm = ArmStateMachine(arm)
    am = {"arm": arm, "epoch_id": epoch_id}
    m["arms"][arm] = am

    HpxActor = x68.build_actor_class(ray)
    island_dir = os.path.join(runs_dir, arm, "island")
    os.makedirs(island_dir, exist_ok=True)
    ports = plan["ports"](arm)
    isl = {"bootdir": island_dir, "ports": ports}
    am["island"] = isl

    rcmd = plan["root_cmd"](island_dir, ports)
    root_proc, rlog = x68._popen(rcmd, island_dir, os.path.join(island_dir, "root.log"))
    procs.append((root_proc, rlog))
    plan["wait_file"](os.path.join(island_dir, "root.ready"), 60, procs=[root_proc])
    rr = x68._read_json(os.path.join(island_dir, "root.ready")) or {}
    isl["root_ready"], isl["root_argv"] = rr, rcmd
    if rr.get("pid"):
        register_owned(f"{arm}_island_root", rr["pid"], plan["node_name"]("root"))
    _phase(m, f"{arm}_root_ready")

    backend = ExternalWitnessLifecycleBackend(island_dir)
    contract = LifecycleEventContract(backend, sm)

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
            register_owned(f"{arm}_island_actor_{k}", isl[f"{k}_identity"]["pid"],
                           plan["node_name"](k))
        contract.publish_joined(epoch_id, k)
    isl["a_join_health"] = x68._ray_get(ray, handles["a"].health.remote(), 30, "a_join_health")
    isl["b_join_health"] = x68._ray_get(ray, handles["b"].health.remote(), 30, "b_join_health")
    if not (all(eval_startup(isl).values()) and all(eval_inprocess(isl).values())):
        raise RuntimeError(f"[{arm}] island bring-up gates failed")
    sm.advance("READY")
    _phase(m, f"{arm}_island_ready")

    guard = sm.dispatch_guard("work")
    am["work_guard"] = guard
    if not guard["allowed"]:
        raise RuntimeError(f"[{arm}] work dispatch not allowed in READY (instrumentation bug)")
    _run_case(x68, ray, isl, handles, case_for(x68, WORK_CASE))
    if not all(eval_work(x68, isl, WORK_CASE).values()):
        raise RuntimeError(f"[{arm}] work verification failed")
    sm.advance("WORK_VERIFIED")
    _phase(m, f"{arm}_work_verified")

    # premature terminal publication attempt: must be rejected because the state machine has
    # not reached EVIDENCE_COLLECTED. Proves the contract guard is live, not decorative.
    early = contract.try_publish_terminal(epoch_id, DEPARTING, CLS_LOSS, "premature",
                                          {DEPARTING})

    # ---- departure ---------------------------------------------------------------------
    b_ident = isl.get("b_identity") or {}
    b_start = isl.get("b_start") or {}
    b_pid = b_ident.get("pid")
    b_node = plan["node_name"]("b")
    _pids = _pid_identity_set(b_ident, b_start)
    cross_checked = (b_pid is not None and None not in _pids and len(_pids) == 1)
    dep = {"connector": DEPARTING, "state_at_departure": sm.state,
           "pid": b_pid, "node": b_node, "pid_cross_checked": cross_checked,
           "actor_id": b_ident.get("actor_id"), "locality": b_start.get("locality_id"),
           "t_mono_departed": None, "stop_dispatched": False, "stop_rc": None,
           "signal": None, "signal_sent": None}
    if not cross_checked:
        raise RuntimeError(f"[{arm}] departing-connector pid cross-check failed")

    # The injection record is kept on a SEPARATE branch of the marker tree; the evidence
    # collector below never receives it. This is what makes the classifier injection-blind.
    injection_private = None

    if arm == "graceful":
        t0 = time.monotonic()
        stop = x68._ray_get(ray, handles[DEPARTING].stop_hpx.remote(), 40, "b_stop")
        dep["stop_dispatched"] = True
        dep["stop_rc"] = (stop or {}).get("rc")
        dep["stop_error"] = (stop or {}).get("error")
        dep["t_mono_departed"] = time.monotonic()
        dep["stop_elapsed_s"] = dep["t_mono_departed"] - t0
        # The HPX locality has left; the hosting Ray actor is deliberately LEFT ALIVE. Its
        # continued answerability is exactly what distinguishes a graceful departure from a
        # loss. The actor is destroyed later, in teardown.
    else:
        injection_private = {"arm": arm, "signal": "SIGKILL", "target_pid": b_pid,
                             "node": b_node, "method": plan["kill_method"]}
        plan["kill_pid"](b_pid, b_node)
        dep["signal"] = "SIGKILL"
        dep["signal_sent"] = True
        dep["t_mono_departed"] = time.monotonic()
    # In the loss arm the host must actually be gone; in the graceful arm the host is expected
    # to still be alive at this point, so the check is short and its result is simply recorded.
    dep["pid_gone"] = plan["pid_gone"](b_pid, b_node, 30 if arm == "loss" else 3)
    am["departure"] = dep
    am["injection_record_private"] = injection_private
    sm.advance("DEPARTED")
    _phase(m, f"{arm}_departed")

    # ---- observable evidence (injection-blind by construction) --------------------------
    # `actor_call_raised` means the CALL ITSELF failed (the host is gone), not that the host
    # reported an application-level error. After a graceful stop the host is alive and answers
    # {"ok": false, "error": "HPX not started"} -- a SUCCESSFUL call, and precisely the signal
    # that separates a departed-but-healthy host from a lost one. exp68's `_ray_get` swallows
    # exceptions into an error dict, so ray.get is used directly here.
    call_raised, err_type, raw = False, None, None
    try:
        raw = ray.get(handles[DEPARTING].health.remote(), timeout=20)
    except Exception as ex:  # noqa: BLE001
        call_raised, err_type, raw = True, type(ex).__name__, str(ex)[:400]
    am["post_departure_call"] = {"raised": call_raised, "error_type": err_type,
                                 "host_answered": not call_raised, "verbatim": raw}

    evidence = collect_departure_evidence(
        connector=DEPARTING,
        pid_gone=dep["pid_gone"],
        clean_disconnect_recorded=(arm == "graceful" and dep.get("stop_rc") == 0),
        stop_rc=dep.get("stop_rc"),
        actor_call_raised=call_raised,
        actor_error_type=err_type)
    am["evidence"] = evidence
    blind_ok = True
    try:
        _assert_blind(evidence)
    except BlindnessViolation:
        blind_ok = False
    am["blindness"] = {
        "assert_blind_ok": blind_ok,
        "injection_fields_in_evidence": sorted(set(evidence) - DEPARTURE_EVIDENCE_KEYS),
        "injection_stored_outside_evidence": (
            injection_private is None
            or not (set(injection_private) & set(evidence))),
        "allowlist": sorted(DEPARTURE_EVIDENCE_KEYS),
    }
    sm.advance("EVIDENCE_COLLECTED")
    _phase(m, f"{arm}_evidence_collected")

    # survivor's bounded membership probe: OBSERVATIONAL, never a gate
    probe = {}
    try:
        probe = x68._ray_get(ray, handles[SURVIVOR].health.remote(), 20, "a_post_departure")
    except Exception as ex:  # noqa: BLE001
        probe = {"error": f"{type(ex).__name__}: {ex}"}
    cat, memb = categorize_membership_probe(probe)
    am["post_departure_survivor_probe"] = {
        "verbatim": probe, "category": cat, "membership": memb,
        "interpretation": post_loss_interpretation(cat), "observational_only": True}

    # ---- classify (one function, both arms) --------------------------------------------
    cl = classify_departure(evidence)
    am["classification"] = cl
    digest = _sha256_text(json.dumps(evidence, sort_keys=True, default=str))
    att = contract.try_publish_terminal(epoch_id, DEPARTING, cl["classification"], digest,
                                        {DEPARTING})
    if not att["accepted"]:
        raise RuntimeError(f"[{arm}] terminal event publication rejected: {att}")
    # Duplicate and non-departed attempts are made while STILL in EVIDENCE_COLLECTED, so they
    # are rejected by the contract's own idempotence/eligibility rules rather than incidentally
    # by the state guard.
    dup = contract.try_publish_terminal(epoch_id, DEPARTING, cl["classification"], digest,
                                        {DEPARTING})
    non_dep = contract.try_publish_terminal(epoch_id, SURVIVOR, CLS_LOSS, digest, {DEPARTING})
    sm.advance("EVENT_PUBLISHED")
    _phase(m, f"{arm}_event_published")
    am["publication"] = {
        "departure_to_publication_s": max(0.0, att["t_mono"] - dep["t_mono_departed"]),
        "bound_s": args.publish_bound_s, "evidence_digest": digest}
    am["event_contract"] = {"attempts": contract.attempts, "joined": ["a", "b"],
                            "non_departed_attempt_rejected": not non_dep["accepted"],
                            "early_attempt": early, "duplicate_attempt": dup}

    # ---- survivor observation + stale-epoch control ------------------------------------
    obs = backend.observe_events(epoch_id, SURVIVOR, args.observe_bound_s)
    obs["t_mono_done"] = time.monotonic()
    am["observation"] = obs
    if obs.get("observed") is not True:
        raise RuntimeError(f"[{arm}] survivor did not observe the terminal event: {obs}")
    sm.advance("EVENT_OBSERVED")
    _phase(m, f"{arm}_event_observed")

    stale_dir = os.path.join(runs_dir, arm, "stale_control")
    os.makedirs(stale_dir, exist_ok=True)
    stale_backend = ExternalWitnessLifecycleBackend(stale_dir, poll_s=0.05)
    stale_backend.publish_event("prior-epoch-000", DEPARTING, EV_DEPARTED, "d", CLS_LOSS)
    am["stale_control"] = {"attempted": True, "marker_dir": stale_dir,
                           "observation": stale_backend.observe_events(epoch_id, SURVIVOR, 1.0)}
    _phase(m, f"{arm}_stale_control_rejected")

    # ---- teardown ----------------------------------------------------------------------
    td = {"graceful_attempted": False}
    a_pid = (isl.get("a_identity") or {}).get("pid")
    if arm == "graceful":
        # The island is intact: the survivor also leaves through the validated sequence, so the
        # work-free root sees membership return to 1 and finalizes cleanly. exp68's root reports
        # `leave_observed` only at membership 1, which is why it is corroboration recorded HERE
        # and not an input to the per-connector classification above.
        td["mode"] = "graceful_root_finalize"
        td["graceful_attempted"] = True
        stop_a = x68._ray_get(ray, handles[SURVIVOR].stop_hpx.remote(), 40, "a_stop")
        td["survivor_stop_rc"] = (stop_a or {}).get("rc")
        open(os.path.join(island_dir, "root.done"), "w").close()
        try:
            plan["wait_file"](os.path.join(island_dir, "root.final"), 60, procs=[root_proc])
            rf = x68._read_json(os.path.join(island_dir, "root.final")) or {}
        except Exception:  # noqa: BLE001
            rf = {}
        am["root_final"] = rf
        td["root_leave_observed"] = rf.get("leave_observed") is True
        td["root_final_membership"] = rf.get("final_membership")
        exited, rc, killed = x68._wait_proc(root_proc, time.time() + 40)
        td["root_exit_path"] = x68._exit_path(exited, rc, killed)
    else:
        # POISONED ISLAND (exp51/exp53/Slice 1): the root's locality state still contains the
        # lost connector. No graceful HPX shutdown is attempted; the whole island is replaced
        # externally. Attempting a clean finalize here is exactly the failure those slices
        # already characterized.
        td["mode"] = "external_whole_island"
        td["root_exit_path"] = "external_teardown_no_graceful_attempt"
        x68._kill_group(root_proc)
        exited, rc, killed = x68._wait_proc(root_proc, time.time() + 30)
        td["root_terminated"] = {"exited": exited, "rc": rc, "killed": killed}
    for h in actors:
        try:
            ray.kill(h)
        except Exception:  # noqa: BLE001
            pass
    td["actor_pids_gone"] = (plan["pid_gone"](a_pid, plan["node_name"]("a"), 30)
                             and plan["pid_gone"](b_pid, b_node, 30))
    td["root_process_gone"] = root_proc.poll() is not None
    am["teardown"] = td
    sm.advance("TORN_DOWN")
    _phase(m, f"{arm}_torn_down")

    sm.advance("FINALIZED")
    _phase(m, f"{arm}_finalized")
    am["state_history"] = sm.history
    am["rejected_transitions"] = list(sm.rejected)
    return am


def _finalize_and_write(x68, ray, m, agg, runs_dir, agg_path, plan, procs, actors, owned, args):
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

    r = rollup(x68, m, args.publish_bound_s)
    agg["overall"] = "pass" if r["passed"] else "fail"
    agg["failure_class"] = r["failure_class"]
    agg["gates"], agg["gates_failed"] = r["gates"], r["gates_failed"]
    agg["negative_claims"] = r["negative_claims"]
    agg["summary_claim"] = (SUMMARY_CLAIM if r["passed"] else
                            f"Slice 3A did not pass ({r['failure_class']}); see gates_failed.")
    agg["controller_exception"] = m.get("controller_exception")
    agg["classifier_fingerprint"] = m.get("classifier_fingerprint")
    for arm in ARMS:
        am = (m.get("arms") or {}).get(arm) or {}
        agg[f"{arm}_classification"] = (am.get("classification") or {}).get("classification")
        agg[f"{arm}_post_departure_membership"] = \
            (am.get("post_departure_survivor_probe") or {}).get("category")
    with open(os.path.join(runs_dir, "markers.json"), "w") as f:
        json.dump(m, f, indent=2, sort_keys=True, default=str)
    with open(agg_path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=True, default=str)
    print(f"[slice3a] overall: {agg['overall']} ({agg['failure_class']}) -> {agg_path}")
    print(f"[slice3a] classifications: graceful={agg.get('graceful_classification')} "
          f"loss={agg.get('loss_classification')}")
    if r["gates_failed"]:
        print(f"[slice3a] gates_failed: {json.dumps(r['gates_failed'], default=str)}")
    return 0


def _new_run(args, phase, pf):
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    prefix = "crossnode_" + (pf.get("slurm_job_id") or "nojob") + "_" if \
        phase == "rostam-cross-node" else ""
    runs_dir = os.path.join(RUNS_ROOT, f"{prefix}{runid}")
    os.makedirs(runs_dir, exist_ok=True)
    agg_path = args.aggregate or os.path.join(runs_dir, "aggregate.json")
    epochs = {arm: f"exp70s3a-{arm}-{runid}" for arm in ARMS}
    agg = {"experiment": "exp70_slice3a_connector_loss_event", "increment": 3,
           "phase": phase, "backend": "external_witness", "runid": runid,
           "runs_dir": runs_dir, "epoch_ids": epochs, "arms": list(ARMS),
           "work_case": WORK_CASE, "departing_connector": DEPARTING, "survivor": SURVIVOR,
           "publish_bound_s": args.publish_bound_s,
           "observe_bound_s": args.observe_bound_s,
           "classifier_allowlist": sorted(DEPARTURE_EVIDENCE_KEYS),
           "provenance": _provenance(),
           "summary_claim_candidate": SUMMARY_CLAIM, "non_claims": NON_CLAIMS,
           "preflight": pf}
    return runid, runs_dir, agg_path, epochs, agg


def _new_markers(epochs, phase):
    fp = _classifier_fingerprint()
    return {"arms": {}, "phase_log": [], "phase_times_wall_ms": {}, "final": {},
            "phase": phase, "epoch_ids": epochs, "controller_pid": os.getpid(),
            "classifier_fingerprint": fp, "classifier_fingerprint_stable": None,
            "classifier_allowlist": sorted(DEPARTURE_EVIDENCE_KEYS)}


def _run_both_arms(x68, ray, args, m, runs_dir, plan, pf, procs, actors, register_owned,
                   epochs):
    fp_before = _classifier_fingerprint()
    for arm in ARMS:
        _run_arm(x68, ray, args, m, arm, runs_dir, plan, pf, procs, actors, register_owned,
                 epochs[arm])
    m["classifier_fingerprint_stable"] = (_classifier_fingerprint() == fp_before
                                          == m.get("classifier_fingerprint"))


def live_local_run(args):
    pf = preflight(args.exp68_dir, args.exp68_build_dir)
    _runid, runs_dir, agg_path, epochs, agg = _new_run(args, "local", pf)
    if not pf["ok"]:
        agg["overall"] = "skip"
        agg["reason"] = "; ".join(pf["problems"])
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice3a] SKIP: {agg['reason']} -> {agg_path}")
        return 0

    x68, _ = import_exp68(args.exp68_dir)
    import ray  # noqa: PLC0415
    m = _new_markers(epochs, "local")
    procs, actors, owned = [], [], []
    plan = make_local_plan(x68, pf, args)

    def register_owned(label, pid, node=None):
        owned.append({"label": label, "pid": pid, "node": node,
                      **(plan["proc_identity"](pid, node) or {})})

    try:
        ray.init(ignore_reinit_error=True, include_dashboard=False,
                 num_cpus=max(4, 2 * args.ray_num_cpus + 2), log_to_driver=False)
        _run_both_arms(x68, ray, args, m, runs_dir, plan, pf, procs, actors, register_owned,
                       epochs)
    except Exception as ex:  # noqa: BLE001
        m["controller_exception"] = f"{type(ex).__name__}: {ex}"
        m["controller_traceback"] = traceback.format_exc()
        print(f"[slice3a] controller exception: {m['controller_exception']}")
    return _finalize_and_write(x68, ray, m, agg, runs_dir, agg_path, plan, procs, actors,
                               owned, args)


def crossnode_live_run(args):
    env = dict(os.environ)
    pf = preflight_crossnode(args.exp68_dir, env, args.subnet, args.exp68_build_dir)
    _runid, runs_dir, agg_path, epochs, agg = _new_run(args, "rostam-cross-node", pf)
    if not pf["ok"]:
        agg["overall"] = "skip"
        agg["reason"] = "; ".join(pf["problems"])
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice3a-crossnode] SKIP: {agg['reason']} -> {agg_path}")
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
        print(f"[slice3a-crossnode] PREFLIGHT FAIL: {agg['reason']}")
        return 0

    nodeA_ip = x68._local_subnet_ip(args.subnet)
    nodeB_ip = x68._node_subnet_ip(nodeB, args.subnet)
    cx = {"slurm_job_id": pf["slurm_job_id"], "nodelist": pf["slurm_nodelist"],
          "nodes": nodes, "nodeA": nodeA, "nodeB": nodeB,
          "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip, "subnet": args.subnet,
          "ray_node_ids": {}}
    import ray  # noqa: PLC0415
    temp_dir = f"/tmp/exp70s3a_ray_{pf['slurm_job_id']}_{_runid}"
    head = worker_b = None
    m = _new_markers(epochs, "rostam-cross-node")
    m["crossnode"] = cx
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
            # parcelport endpoints are filled in per arm below; attest the rest now
            partial = {k: v for k, v in eval_cluster_attestation(cx).items()
                       if k != "parcelport_endpoints_on_subnet"}
            if not all(partial.values()):
                raise RuntimeError(f"cluster attestation failed: {partial}")

        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: PLC0415
        strat_a = NodeAffinitySchedulingStrategy(node_id=cx["ray_node_ids"]["nodeA"], soft=False)
        strat_b = NodeAffinitySchedulingStrategy(node_id=cx["ray_node_ids"]["nodeB"], soft=False)
        plan = make_crossnode_plan(x68, pf, args, cx, strat_a, strat_b, env)
        m["placement"] = plan["placement"]

        def register_owned(label, pid, node=None):
            owned.append({"label": label, "pid": pid, "node": node,
                          **(plan["proc_identity"](pid, node) or {})})

        _run_both_arms(x68, ray, args, m, runs_dir, plan, pf, procs, actors, register_owned,
                       epochs)
        eps = []
        for arm in ARMS:
            isl = ((m.get("arms") or {}).get(arm) or {}).get("island") or {}
            p = isl.get("ports") or {}
            if p:
                eps += [f"{nodeA_ip}:{p['root']}", f"{nodeA_ip}:{p['a']}",
                        f"{nodeB_ip}:{p['b']}"]
        cx["parcelport_endpoints"] = eps
    except Exception as ex:  # noqa: BLE001
        m["controller_exception"] = f"{type(ex).__name__}: {ex}"
        m["controller_traceback"] = traceback.format_exc()
        print(f"[slice3a-crossnode] controller exception: {m['controller_exception']}")
    finally:
        if plan is None:
            plan = make_local_plan(x68, pf, args)  # cleanup fallback (no srun anywhere)

    rc = _finalize_and_write(x68, ray, m, agg, runs_dir, agg_path, plan, procs, actors,
                             owned, args)
    for node in [n for n in (cx.get("nodeB"), cx.get("nodeA")) if n]:
        try:
            x68._ray_stop_node(node, env)
        except Exception:  # noqa: BLE001
            pass
    for launcher in (worker_b, head):
        if launcher is not None:
            try:
                x68._terminate_launcher(launcher)
            except Exception:  # noqa: BLE001
                pass
    orph = {}
    for node in [n for n in (cx.get("nodeA"), cx.get("nodeB")) if n]:
        try:
            orph[node] = x68._orphan_check_node(node, env, x68._ORPHAN_PATTERNS_RAY)
        except Exception as ex:  # noqa: BLE001
            orph[node] = f"{type(ex).__name__}: {ex}"
    agg["post_run_orphan_check"] = orph
    with open(agg_path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=True, default=str)
    return rc


# ---------------------------------------------------------------------------------------
# Curation
# ---------------------------------------------------------------------------------------

def json_load_quiet(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def curate_local(run_ids):
    if not os.path.isdir(RUNS_ROOT):
        print(f"[slice3a] no runs dir: {RUNS_ROOT}")
        return 1
    ids = run_ids or sorted(d for d in os.listdir(RUNS_ROOT)
                            if not d.startswith(("crossnode_", "curated_")))
    out_dir = os.path.join(RUNS_ROOT, "curated_local_evidence")
    os.makedirs(out_dir, exist_ok=True)
    kept = []
    for rid in ids:
        src = os.path.join(RUNS_ROOT, rid)
        agg = json_load_quiet(os.path.join(src, "aggregate.json"))
        if not agg or agg.get("overall") != "pass":
            continue
        dst = os.path.join(out_dir, rid)
        os.makedirs(dst, exist_ok=True)
        for fn in ("aggregate.json", "markers.json"):
            s = os.path.join(src, fn)
            if os.path.isfile(s):
                with open(s) as f_in, open(os.path.join(dst, fn), "w") as f_out:
                    f_out.write(f_in.read())
        kept.append({"runid": agg.get("runid"), "overall": agg.get("overall"),
                     "failure_class": agg.get("failure_class"),
                     "graceful_classification": agg.get("graceful_classification"),
                     "loss_classification": agg.get("loss_classification"),
                     "loss_post_departure_membership":
                         agg.get("loss_post_departure_membership"),
                     "gate_groups": len(agg.get("gates") or {}),
                     "gate_checks": sum(len(v) for v in (agg.get("gates") or {}).values()
                                        if isinstance(v, dict))})
    summary = {"experiment": "exp70_slice3a_connector_loss_event",
               "curated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "phase": "local", "runs": kept,
               "summary_claim": SUMMARY_CLAIM, "non_claims": NON_CLAIMS,
               "classifier_allowlist": sorted(DEPARTURE_EVIDENCE_KEYS)}
    p = os.path.join(out_dir, "curated_local_aggregate.json")
    with open(p, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    print(f"[slice3a] curated {len(kept)} local run(s) -> {p}")
    return 0


# ---------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="exp70 Slice 3A: classified connector loss/departure events")
    ap.add_argument("--selftest", action="store_true",
                    help="pure logic checks (no Ray, no HPX, no Slurm)")
    ap.add_argument("--phase", choices=("local", "rostam-cross-node"), default="local")
    ap.add_argument("--curate", nargs="*", metavar="RUNID", default=None,
                    help="curate accepted runs (default: all passing runs)")
    ap.add_argument("--exp68-dir", default=DEFAULT_EXP68_DIR)
    ap.add_argument("--exp68-build-dir", default=None,
                    help="exp68 build dir holding the peer binary and ext .so "
                         "(default: <exp68-dir>/build; Rostam uses <exp68-dir>/build_rostam)")
    ap.add_argument("--publish-bound-s", type=float, default=DEFAULT_PUBLISH_BOUND_S,
                    help="bound between departure observation and terminal publication")
    ap.add_argument("--observe-bound-s", type=float, default=DEFAULT_OBSERVE_BOUND_S)
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--ray-num-cpus", type=int, default=1)
    ap.add_argument("--aggregate", default=None)
    ap.add_argument("--subnet", default=DEFAULT_SUBNET)
    ap.add_argument("--ray-port", type=int, default=6489)
    ap.add_argument("--port-base", type=int, default=7951)
    ap.add_argument("--arm-port-stride", type=int, default=10,
                    help="port offset between the two arms (keeps arm port blocks disjoint)")
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
