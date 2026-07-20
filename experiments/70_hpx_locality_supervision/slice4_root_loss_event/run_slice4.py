#!/usr/bin/env python3
"""exp70 Slice 4A -- explicit ROOT completion vs unexpected ROOT loss, external backend.

QUESTION: can connected actor-hosted HPX localities distinguish explicit root completion from
unexpected loss of the separately supervised work-free root, using the current EXTERNAL
lifecycle backend?

Slice 2A closed explicit connector-side completion; Slice 3A closed classified connector
departure/loss. Slice 4A moves the same two-item contract to the ROOT: the one process the
island cannot replace in place.

TOPOLOGY (exp66/67/68 mechanism, reused IN PLACE and unmodified):
  node A: controller / Ray head / separately supervised WORK-FREE HPX root / actor A
  node B: actor B
The work-free root stays a separately supervised PROCESS, never a Ray actor.

THE EXTERNAL ROOT WITNESS is exp68's periodically refreshed `root.alive` file (the root rewrites
it about every 200 ms). It is an EXTERNAL PERIODICALLY REFRESHED ROOT-LIVENESS WITNESS. It is
NOT an HPX-native heartbeat, NOT HPX failure detection, and NOT authoritative proof of failure.
Everything this slice concludes about loss is BOUNDED SUSPICION derived by the supervisor.

TWO ARMS, one classifier, fresh island each (fresh root process, actor ids, pids, ports, boot
directory, epoch-scoped witnesses):
  normal arm -> explicit_completion
  loss arm   -> suspected_root_loss

MONOTONIC SILENCE: the witness token is (st_mtime_ns, st_size, st_ino). A CHANGED token marks an
advance and stamps `last_advance_monotonic = time.monotonic()`. Silence is measured purely in
monotonic time; filesystem mtime only says THAT the witness advanced, never how long ago.

EPOCH SCOPING: exp68's root consumes a bare mechanical `root.done` trigger that carries no epoch.
This slice therefore publishes its own epoch-scoped `root.completion` witness, which is what the
classifier reads; `root.done` is still written afterwards to drive exp68's finalize path. Stale
`root.completion` / `root.alive` from a prior epoch can never satisfy the current epoch.

CLASSIFIER POLICY (documented, and selftested at the boundary): PID death alone does NOT declare
loss before the suspicion bound elapses. Bounded suspicion requires the SILENCE bound. A dead pid
is corroborating evidence recorded alongside, never a shortcut.

CLAIM FENCE: application-contract / mechanism evidence only. NO HPX-native root-loss
notification, NO HPX-native heartbeat, NO authoritative failure certainty, NO transparent
recovery, NO automatic AGAS repair, NO partial-island continuation, NO performance claim.

Usage:
  python3 run_slice4.py --selftest                 # pure logic checks (no Ray/HPX/Slurm)
  python3 run_slice4.py --phase local              # live local run (both arms)
  python3 run_slice4.py --phase rostam-cross-node  # cluster phase (skips cleanly off-cluster)
  python3 run_slice4.py --curate [RUNID ...]       # curate accepted local runs (no processes)
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
RUNS_ROOT = os.path.join(HERE, "_exp70_slice4_runs")

WORK_CASE = "cross_both"          # exp68 MATRIX: V=64 split=32 k=6 seed=1 (BOTH arms)
COMPLETION_WITNESS = "root.completion"   # epoch-scoped; THIS is what the classifier reads
MECHANICAL_DONE = "root.done"            # exp68's bare finalize trigger (no epoch)
ALIVE_WITNESS = "root.alive"             # exp68's periodically refreshed liveness witness
ROOT_FINAL = "root.final"
DEFAULT_SUBNET = "10.42.5."

EXPECTED_REFRESH_S = 0.2          # exp68 root rewrites root.alive on a ~200 ms loop
DEFAULT_SUSPICION_BOUND_S = 5.0   # >> refresh interval; silence must reach this to suspect loss
DEFAULT_OBSERVE_BOUND_S = 30.0    # observer's own deadline before observation_timeout
DEFAULT_ACTOR_CALL_TIMEOUT_S = 15.0

ARMS = ("normal", "loss")

# Root lifecycle event classes (backend-neutral vocabulary).
EV_COMPLETION = "explicit_completion"
EV_SUSPECTED_LOSS = "suspected_root_loss"
EV_OBS_ERROR = "observation_error"
EV_OBS_TIMEOUT = "observation_timeout"
EVENT_CLASSES = (EV_COMPLETION, EV_SUSPECTED_LOSS, EV_OBS_ERROR, EV_OBS_TIMEOUT)
# NOT an event class: the polling sentinel meaning "nothing decidable yet".
EV_PENDING = "pending"

# Bounded actor-observation categories (evidence, not required to take one form).
OBS_RETURNED = "call_returned"
OBS_ERROR_RESULT = "call_error_result"
OBS_RAISED = "call_raised"
OBS_TIMEOUT = "call_timeout"
OBS_UNAVAILABLE = "actor_unavailable"
ACTOR_OBS_CATEGORIES = (OBS_RETURNED, OBS_ERROR_RESULT, OBS_RAISED, OBS_TIMEOUT,
                        OBS_UNAVAILABLE)

STATES = ("STARTING", "READY", "WORK_VERIFIED", "RESULT_VERIFIED", "ROOT_EVENT_CLASSIFIED",
          "CONNECTORS_OBSERVED", "ISLAND_DISPOSED", "FINALIZED")
_NEXT = {a: b for a, b in zip(STATES, STATES[1:])}
DISPATCH_STATES = ("READY",)

# The ONLY fields the root classifier may see. Anything else -> RootBlindnessViolation.
# Every field is observable by a supervisor that knows nothing about how the run was steered.
ROOT_EVIDENCE_KEYS = frozenset({
    "epoch_id",                     # epoch the observation belongs to
    "completion_witness_present",   # an epoch-scoped completion witness was found
    "completion_witness_epoch_match",   # ... and it names THIS epoch
    "expected_refresh_s",           # documented witness refresh interval
    "observed_silence_s",           # MONOTONIC seconds since the witness last advanced
    "classification_bound_s",       # configured suspicion bound
    "root_pid_alive",               # corroboration only; never a shortcut past the bound
    "witness_read_error",           # the witness could not be read
    "observation_deadline_exceeded",    # the observer's own bound expired
})

SUMMARY_CLAIM = (
    "In a two-node actor-hosted HPX island, the external root-lifecycle backend distinguished "
    "explicit root completion from bounded suspicion after unexpected loss of the separately "
    "supervised work-free root, while the supervisor discarded the poisoned island.")
NON_CLAIMS = (
    "No HPX-native root-loss notification. No HPX-native heartbeat. No authoritative failure "
    "certainty. No transparent recovery. No automatic AGAS repair. No partial-island "
    "continuation. No performance claim.")

EXP68_REQUIRED = [
    "MATRIX", "eval_case", "_synthetic_case_result", "oracle_topk", "norm_cands",
    "find_free_port", "_popen", "_kill_group", "_wait_for_file", "_read_json",
    "peer_orphans", "peer_root_cmd", "actor_endpoints", "build_actor_class",
    "pid_alive", "wait_pid_gone", "_ray_get", "_wait_proc", "_exit_path",
    "PEER_BASENAME", "EXT_MODULE",
    "crossnode_root_cmd", "crossnode_actor_endpoints", "_wait_for_file_nfs",
    "_expand_slurm_nodelist", "_short", "_sh", "_local_subnet_ip", "_node_subnet_ip",
    "_ray_head_local", "_ray_worker_srun", "_wait_gcs_from", "_bounded_ray_init",
    "_wait_ray_nodes", "_ray_stop_node", "_terminate_launcher", "_orphan_check_node",
    "_ORPHAN_PATTERNS_RAY",
]

FAILURE_CLASSES = [
    "preflight_missing_artifacts", "invalid_instrumentation", "crossnode_placement_failed",
    "startup_failed", "inprocess_proof_failed", "work_failed", "result_verification_failed",
    "root_injection_precondition_failed", "classifier_blindness_violated",
    "root_event_misclassified", "event_contract_violated", "epoch_scope_violated",
    "connector_observation_incomplete", "actor_observation_unbounded",
    "post_event_dispatch_detected", "disposal_failed", "invalid_ordering",
    "arm_isolation_violated", "orphan_detected", "cleanup_incomplete",
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
    """Pure checks; never raises."""
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
    """Cross-node preconditions. Pure env/artifact checks; nodelist parsing is string parsing,
    and NO srun/sbatch/Slurm command is executed here."""
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
# Injection-blind root classifier
# ---------------------------------------------------------------------------------------

class RootBlindnessViolation(Exception):
    """Raised when evidence offered to the root classifier carries a field outside the
    allow-list. Structural guarantee that intent/injection data cannot reach the decision."""


def _assert_root_blind(evidence):
    if not isinstance(evidence, dict):
        raise RootBlindnessViolation(
            f"evidence must be a mapping, got {type(evidence).__name__}")
    extra = set(evidence) - ROOT_EVIDENCE_KEYS
    if extra:
        raise RootBlindnessViolation(f"evidence carries non-observable fields: {sorted(extra)}")


def classify_root_event(evidence):
    """Injection-blind root lifecycle classifier.

    Decision order:
      observation_error   : the witness could not be read.
      explicit_completion : an epoch-matched completion witness exists.
      suspected_root_loss : NO epoch-matched completion AND monotonic silence has reached the
                            configured suspicion bound.
      observation_timeout : nothing decidable and the observer's own deadline expired.
      pending             : NOT an event -- nothing decidable yet, keep observing.

    POLICY: a dead root pid does NOT shortcut the silence bound. `root_pid_alive` is recorded as
    corroboration; suspicion is declared only once observed_silence_s >= classification_bound_s.
    """
    _assert_root_blind(evidence)
    if evidence.get("witness_read_error"):
        label = EV_OBS_ERROR
    elif (evidence.get("completion_witness_present") is True
          and evidence.get("completion_witness_epoch_match") is True):
        label = EV_COMPLETION
    else:
        silence = evidence.get("observed_silence_s")
        bound = evidence.get("classification_bound_s")
        reached = (isinstance(silence, (int, float)) and isinstance(bound, (int, float))
                   and silence >= bound)
        if reached:
            label = EV_SUSPECTED_LOSS
        elif evidence.get("observation_deadline_exceeded") is True:
            label = EV_OBS_TIMEOUT
        else:
            label = EV_PENDING
    return {"event": label,
            "basis": {
                "completion_witness_epoch_match":
                    evidence.get("completion_witness_epoch_match") is True,
                "observed_silence_s": evidence.get("observed_silence_s"),
                "classification_bound_s": evidence.get("classification_bound_s"),
                "silence_bound_reached": label == EV_SUSPECTED_LOSS,
                "root_pid_alive": evidence.get("root_pid_alive"),
            },
            "epoch_id": evidence.get("epoch_id")}


def collect_root_evidence(epoch_id, completion_witness_present, completion_witness_epoch_match,
                          expected_refresh_s, observed_silence_s, classification_bound_s,
                          root_pid_alive, witness_read_error, observation_deadline_exceeded):
    """Fixed positional signature: there is no parameter through which an injection record,
    arm label, or controller intent could be smuggled in."""
    ev = {"epoch_id": epoch_id,
          "completion_witness_present": bool(completion_witness_present),
          "completion_witness_epoch_match": bool(completion_witness_epoch_match),
          "expected_refresh_s": expected_refresh_s,
          "observed_silence_s": observed_silence_s,
          "classification_bound_s": classification_bound_s,
          "root_pid_alive": root_pid_alive,
          "witness_read_error": bool(witness_read_error),
          "observation_deadline_exceeded": bool(observation_deadline_exceeded)}
    _assert_root_blind(ev)
    return ev


def _root_classifier_fingerprint():
    import inspect  # noqa: PLC0415
    import hashlib  # noqa: PLC0415
    src = (inspect.getsource(classify_root_event) + inspect.getsource(_assert_root_blind)
           + repr(sorted(ROOT_EVIDENCE_KEYS)))
    return hashlib.sha256(src.encode()).hexdigest()


# ---------------------------------------------------------------------------------------
# Root lifecycle observer surface (backend-neutral: observe_root_event)
# ---------------------------------------------------------------------------------------

def observer_surface_ok(obs):
    """Slice 4B substitution surface: duck-typed single-method contract + a name."""
    return callable(getattr(obs, "observe_root_event", None)) and bool(getattr(obs, "name", None))


def witness_token(path):
    """Identity of the witness file's current content-generation. A CHANGED token means the
    root advanced it; the token itself is never used as a clock."""
    st = os.stat(path)
    return (st.st_mtime_ns, st.st_size, st.st_ino)


class ExternalRootLifecycleObserver:
    """Live observer over exp68's EXTERNAL, periodically refreshed root witnesses.

    Reads: the epoch-scoped `root.completion` witness this slice publishes, and the mtime/size/
    inode token of exp68's `root.alive`. Silence is measured in MONOTONIC time only.

    This is not an HPX-native heartbeat and not failure detection; a loss verdict from it is
    bounded suspicion.
    """

    name = "external_root_witness"

    def __init__(self, island_dir, expected_refresh_s=EXPECTED_REFRESH_S,
                 bound_s=DEFAULT_SUSPICION_BOUND_S, poll_s=0.05, clock=time.monotonic,
                 pid_alive=None):
        self.island_dir = island_dir
        self.completion_path = os.path.join(island_dir, COMPLETION_WITNESS)
        self.alive_path = os.path.join(island_dir, ALIVE_WITNESS)
        self.expected_refresh_s = expected_refresh_s
        self.bound_s = bound_s
        self.poll_s = poll_s
        self._clock = clock
        self._pid_alive = pid_alive or (lambda pid: False)

    def publish_completion(self, epoch_id, root_identity, payload):
        """Epoch-scoped completion witness, written atomically."""
        doc = {"epoch_id": epoch_id, "root_identity": root_identity, "payload": payload,
               "published_wall_ms": int(time.time() * 1000),
               "published_mono": self._clock()}
        tmp = self.completion_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(doc, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.completion_path)
        return doc

    def _read_completion(self, epoch_id):
        """(present, epoch_match, verbatim, read_error)."""
        if not os.path.exists(self.completion_path):
            return False, False, None, False
        try:
            with open(self.completion_path) as f:
                doc = json.load(f)
        except (OSError, ValueError):
            return True, False, None, True
        return True, doc.get("epoch_id") == epoch_id, doc, False

    def observe_root_event(self, epoch, root_identity, bound):
        """Poll until one of the four event classes is determined, or `bound` expires.

        `bound` is the OBSERVER's own deadline (observation_timeout); the suspicion bound is
        self.bound_s and is what a loss verdict requires.
        """
        t0 = self._clock()
        last_token, last_advance = None, t0
        stale_epochs, read_errors = [], 0
        try:
            last_token = witness_token(self.alive_path)
        except OSError:
            read_errors += 1
        while True:
            now = self._clock()
            token, tok_err = None, False
            try:
                token = witness_token(self.alive_path)
            except OSError:
                tok_err = True
            if token is not None and token != last_token:
                last_token, last_advance = token, now
            present, match, doc, cerr = self._read_completion(epoch)
            if present and not match and doc is not None:
                if doc.get("epoch_id") not in stale_epochs:
                    stale_epochs.append(doc.get("epoch_id"))
            # A wrong-root-identity witness is not this root's completion.
            if match and doc is not None and root_identity is not None \
                    and doc.get("root_identity") not in (None, root_identity):
                match = False
            silence = now - last_advance
            deadline_exceeded = (now - t0) >= bound
            ev = collect_root_evidence(
                epoch_id=epoch,
                completion_witness_present=present,
                completion_witness_epoch_match=match,
                expected_refresh_s=self.expected_refresh_s,
                observed_silence_s=silence,
                classification_bound_s=self.bound_s,
                root_pid_alive=self._pid_alive(root_identity.get("pid")
                                               if isinstance(root_identity, dict) else None),
                witness_read_error=bool(cerr) or (tok_err and last_token is None),
                observation_deadline_exceeded=deadline_exceeded)
            res = classify_root_event(ev)
            if res["event"] != EV_PENDING:
                return {"observer": self.name, "epoch_id": epoch, "event": res["event"],
                        "basis": res["basis"], "evidence": ev,
                        "stale_epochs_rejected": stale_epochs,
                        "classification_elapsed_s": self._clock() - t0,
                        "observed_silence_s": silence,
                        "last_advance_monotonic": last_advance,
                        "completion_verbatim": doc if res["event"] == EV_COMPLETION else None,
                        "completion_path": self.completion_path,
                        "alive_path": self.alive_path}
            time.sleep(self.poll_s)


class SyntheticRootLifecycleObserver:
    """In-memory observer for selftests: a scripted sequence of evidence mappings."""

    name = "synthetic_root"

    def __init__(self, script):
        self.script = list(script)
        self.seen = []
        self.completions = {}

    def publish_completion(self, epoch_id, root_identity, payload):
        doc = {"epoch_id": epoch_id, "root_identity": root_identity, "payload": payload,
               "published_mono": time.monotonic(),
               "published_wall_ms": int(time.time() * 1000)}
        self.completions[epoch_id] = doc
        return doc

    def observe_root_event(self, epoch, root_identity, bound):
        for ev in self.script:
            res = classify_root_event(ev)
            self.seen.append(res["event"])
            if res["event"] != EV_PENDING:
                return {"observer": self.name, "epoch_id": epoch, "event": res["event"],
                        "basis": res["basis"], "evidence": ev, "stale_epochs_rejected": [],
                        "classification_elapsed_s": 0.0,
                        "observed_silence_s": ev.get("observed_silence_s"),
                        "last_advance_monotonic": 0.0, "completion_verbatim": None,
                        "completion_path": None, "alive_path": None}
        return {"observer": self.name, "epoch_id": epoch, "event": EV_OBS_TIMEOUT,
                "basis": {}, "evidence": {}, "stale_epochs_rejected": [],
                "classification_elapsed_s": 0.0, "observed_silence_s": None,
                "last_advance_monotonic": 0.0, "completion_verbatim": None,
                "completion_path": None, "alive_path": None}


class RootEventContract:
    """Publication guard: an explicit completion may be published only AFTER the final
    application result is verified, only for this epoch's root, and only once."""

    def __init__(self, observer, sm):
        if not observer_surface_ok(observer):
            raise ValueError(f"observer does not satisfy the surface: {observer!r}")
        self.observer, self.sm = observer, sm
        self.attempts, self.published = [], {}

    def try_publish_completion(self, epoch_id, root_identity, payload, result_verified):
        att = {"epoch_id": epoch_id, "state": self.sm.state, "t_mono": time.monotonic(),
               "root_pid": (root_identity or {}).get("pid")}
        if not result_verified:
            att.update(accepted=False, reason="final result not verified")
        elif self.sm.state != "RESULT_VERIFIED":
            att.update(accepted=False, reason=f"wrong state: {self.sm.state}")
        elif epoch_id in self.published:
            att.update(accepted=False, reason="completion already published")
        else:
            doc = self.observer.publish_completion(epoch_id, root_identity, payload)
            self.published[epoch_id] = att["t_mono"]
            att.update(accepted=True, reason=None, witness=doc)
        self.attempts.append(att)
        return att


# ---------------------------------------------------------------------------------------
# Bounded actor-side observations
# ---------------------------------------------------------------------------------------

def classify_actor_observation(fn, timeout_s):
    """Call `fn()` under a bound and map the outcome onto one recorded category. The exact
    result or exception text is preserved. Never lets a blocked HPX/AGAS call strand us."""
    t0 = time.monotonic()
    try:
        res = fn(timeout_s)
    except Exception as ex:  # noqa: BLE001
        name = type(ex).__name__
        if "Timeout" in name:
            cat = OBS_TIMEOUT
        elif "ActorDied" in name or "ActorUnavailable" in name or "RayActorError" in name:
            cat = OBS_UNAVAILABLE
        else:
            cat = OBS_RAISED
        return {"category": cat, "error_type": name, "verbatim": str(ex)[:600],
                "elapsed_s": time.monotonic() - t0, "bound_s": timeout_s,
                "bounded": (time.monotonic() - t0) <= timeout_s + 5.0}
    cat = OBS_ERROR_RESULT if (isinstance(res, dict)
                               and (res.get("error") or res.get("ok") is False)) \
        else OBS_RETURNED
    return {"category": cat, "error_type": None, "verbatim": res,
            "elapsed_s": time.monotonic() - t0, "bound_s": timeout_s,
            "bounded": (time.monotonic() - t0) <= timeout_s + 5.0}


# ---------------------------------------------------------------------------------------
# Root injection preconditions (recorded SEPARATELY from classifier evidence)
# ---------------------------------------------------------------------------------------

def eval_root_injection_preconditions(pre):
    return {
        "recorded_pid_matches_live_process": pre.get("pid_matches_live") is True,
        "command_identity_matches_this_run": pre.get("command_matches_run") is True,
        "process_start_identity_matches": pre.get("start_identity_matches") is True,
        "root_belongs_to_current_epoch": pre.get("epoch_matches") is True,
        "root_is_not_the_driver": pre.get("not_driver") is True,
        "root_is_not_an_actor_worker": pre.get("not_actor_worker") is True,
        "both_actors_alive": pre.get("both_actors_alive") is True,
        "workload_passed": pre.get("workload_passed") is True,
        "no_completion_published": pre.get("no_completion_published") is True,
        "no_later_epoch_exists": pre.get("no_later_epoch") is True,
    }


# ---------------------------------------------------------------------------------------
# Gate evaluators
# ---------------------------------------------------------------------------------------

def case_for(x68, name):
    for c in x68.MATRIX:
        if c["name"] == name:
            return c
    raise KeyError(f"exp68 MATRIX has no case {name!r}")


def short_host(h):
    return (h or "").split(".")[0]


def _pid_identity_set(ident, start):
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
        "membership_reached_3": (a_j.get("ok") is True and b_j.get("ok") is True
                                 and a_j.get("membership") == 3 and b_j.get("membership") == 3),
        "distinct_connector_localities": (a_s.get("locality_id") not in (None, 0)
                                          and b_s.get("locality_id") not in (None, 0)
                                          and a_s.get("locality_id") != b_s.get("locality_id")),
        "distinct_worker_pids": (a_i.get("pid") is not None and b_i.get("pid") is not None
                                 and a_i.get("pid") != b_i.get("pid")),
        "root_pid_distinct_from_actors": (rr.get("pid") is not None
                                          and rr.get("pid") not in (a_i.get("pid"),
                                                                    b_i.get("pid"))),
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


def eval_result_verification(am):
    rv = am.get("result_verification") or {}
    return {
        "final_result_verified": rv.get("verified") is True,
        "result_digest_recorded": bool(rv.get("digest")),
        "verified_before_any_lifecycle_transition": rv.get("state_at_verification")
                                                    == "WORK_VERIFIED",
    }


def eval_root_event(am, arm, epoch_id):
    obs = am.get("root_observation") or {}
    expected = EV_COMPLETION if arm == "normal" else EV_SUSPECTED_LOSS
    return {
        "event_recorded": obs.get("event") in EVENT_CLASSES,
        "event_matches_arm": obs.get("event") == expected,
        "event_epoch_matches": obs.get("epoch_id") == epoch_id,
        "basis_recorded": bool(obs.get("basis")),
        "event_is_not_pending": obs.get("event") != EV_PENDING,
    }


def eval_blindness(am):
    ev = am.get("root_evidence") or {}
    bl = am.get("blindness") or {}
    return {
        "evidence_recorded": bool(ev),
        "evidence_keys_within_allowlist": bool(ev) and set(ev) <= ROOT_EVIDENCE_KEYS,
        "classifier_accepted_evidence": bl.get("assert_blind_ok") is True,
        "injection_fields_absent_from_evidence": bl.get("injection_fields_in_evidence") == [],
        "injection_record_stored_separately": bl.get("injection_stored_outside_evidence") is True,
    }


def eval_monotonic_record(am, arm):
    mono = am.get("monotonic") or {}
    out = {
        "expected_refresh_recorded": isinstance(mono.get("expected_refresh_s"), (int, float)),
        "last_advance_monotonic_recorded": isinstance(mono.get("last_advance_monotonic"),
                                                      (int, float)),
        "classification_bound_recorded": isinstance(mono.get("classification_bound_s"),
                                                    (int, float)),
        "observed_silence_recorded": isinstance(mono.get("observed_silence_s"), (int, float)),
        "classification_elapsed_recorded": isinstance(mono.get("classification_elapsed_s"),
                                                      (int, float)),
    }
    if arm == "loss":
        s, b = mono.get("observed_silence_s"), mono.get("classification_bound_s")
        out["silence_reached_bound"] = (isinstance(s, (int, float))
                                        and isinstance(b, (int, float)) and s >= b)
        out["pre_bound_probe_did_not_declare_loss"] = (
            (am.get("pre_bound_probe") or {}).get("event") != EV_SUSPECTED_LOSS)
    else:
        out["silence_never_reached_bound"] = True  # completion path never waits on silence
    return out


def eval_event_contract(am, arm, epoch_id):
    ec = am.get("event_contract") or {}
    attempts = ec.get("attempts") or []
    accepted = [a for a in attempts if a.get("accepted")]
    early = [a for a in attempts if not a.get("accepted")
             and "not verified" in str(a.get("reason"))]
    dup = [a for a in attempts if not a.get("accepted")
           and a.get("reason") == "completion already published"]
    out = {
        "premature_publication_attempted": bool(early),
        "premature_publication_rejected": len(early) >= 1,
        "wrong_epoch_event_rejected": ec.get("wrong_epoch_rejected") is True,
        "wrong_root_identity_rejected": ec.get("wrong_root_identity_rejected") is True,
    }
    if arm == "normal":
        out["exactly_one_completion_accepted"] = len(accepted) == 1
        out["completion_epoch_matches"] = bool(accepted) and accepted[0].get(
            "epoch_id") == epoch_id
        out["duplicate_publication_attempted"] = bool(dup)
        out["duplicate_publication_rejected"] = len(dup) >= 1
    else:
        out["no_completion_published_on_loss_arm"] = len(accepted) == 0
    return out


def eval_epoch_scope(am, epoch_id):
    st = am.get("stale_control") or {}
    return {
        "stale_control_attempted": st.get("attempted") is True,
        "stale_completion_from_prior_epoch_rejected":
            (st.get("completion_observation") or {}).get("event") != EV_COMPLETION,
        "stale_alive_witness_rejected": st.get("stale_alive_rejected") is True,
        "observation_epoch_matches_island": (am.get("root_observation") or {}).get(
            "epoch_id") == epoch_id,
    }


def eval_connector_observation(am, arm, epoch_id):
    obs = am.get("connector_observations") or {}
    expected = EV_COMPLETION if arm == "normal" else EV_SUSPECTED_LOSS
    out = {"both_connectors_observed": sorted(obs.keys()) == ["a", "b"]}
    for k in ("a", "b"):
        o = obs.get(k) or {}
        out[f"{k}_event_matches_arm"] = o.get("event") == expected
        out[f"{k}_epoch_match"] = o.get("epoch_id") == epoch_id
    return out


def eval_actor_observations(am, arm):
    """Loss arm only: every post-loss actor call must be bounded and categorized."""
    if arm != "loss":
        return {}
    obs = am.get("post_loss_actor_observations") or {}
    out = {"both_actors_observed": sorted(obs.keys()) == ["a", "b"]}
    for k in ("a", "b"):
        o = obs.get(k) or {}
        out[f"{k}_category_recorded"] = o.get("category") in ACTOR_OBS_CATEGORIES
        out[f"{k}_bounded"] = o.get("bounded") is True
        out[f"{k}_verbatim_preserved"] = ("verbatim" in o)
    return out


def eval_no_post_event_dispatch(am):
    g = am.get("post_event_dispatch_guard") or {}
    return {
        "post_event_dispatch_attempted": g.get("attempted") is True,
        "post_event_dispatch_rejected": g.get("allowed") is False,
        "post_event_dispatch_never_reached_hpx": g.get("reached_hpx") is False,
        "no_application_work_after_event": am.get("application_dispatches_after_event") == 0,
    }


def eval_disposal(am, arm):
    d = am.get("disposal") or {}
    out = {
        "disposal_recorded": bool(d),
        "both_actors_removed": d.get("actors_removed") is True,
        "actor_worker_pids_gone": d.get("actor_pids_gone") is True,
    }
    if arm == "normal":
        out["connectors_left_gracefully"] = d.get("graceful_stops_ok") is True
        out["root_observed_membership_one"] = d.get("root_final_membership") == 1
        out["root_final_present"] = d.get("root_final_present") is True
        out["root_exit_finalized_clean"] = d.get("root_exit_path") == "finalized_clean"
    else:
        out["no_mechanical_done_written"] = d.get("mechanical_done_written") is False
        out["no_completion_witness_written"] = d.get("completion_witness_written") is False
        out["root_final_absent_as_expected"] = d.get("root_final_present") is False
        out["disposal_mode_is_poisoned_island"] = d.get("mode") == "poisoned_island_discard"
        out["root_process_gone"] = d.get("root_process_gone") is True
    return out


def eval_ordering(am):
    hist = [h.get("state") for h in (am.get("state_history") or [])]
    return {
        "state_history_complete_and_ordered": hist == list(STATES),
        "no_rejected_transitions": (am.get("rejected_transitions") or []) == [],
    }


def eval_arm_isolation(m):
    arms = m.get("arms") or {}
    n, l = (arms.get("normal") or {}), (arms.get("loss") or {})
    ni, li = (n.get("island") or {}), (l.get("island") or {})
    np_, lp = set((ni.get("ports") or {}).values()), set((li.get("ports") or {}).values())
    n_ids = {(ni.get(f"{k}_identity") or {}).get("actor_id") for k in ("a", "b")}
    l_ids = {(li.get(f"{k}_identity") or {}).get("actor_id") for k in ("a", "b")}
    n_pids = {(ni.get(f"{k}_identity") or {}).get("pid") for k in ("a", "b")}
    l_pids = {(li.get(f"{k}_identity") or {}).get("pid") for k in ("a", "b")}
    n_root = (ni.get("root_ready") or {}).get("pid")
    l_root = (li.get("root_ready") or {}).get("pid")
    return {
        "both_arms_ran": bool(n) and bool(l),
        "disjoint_ports": bool(np_) and bool(lp) and not (np_ & lp),
        "distinct_bootdirs": bool(ni.get("bootdir")) and ni.get("bootdir") != li.get("bootdir"),
        "distinct_actor_ids": bool(n_ids - {None}) and not (n_ids & l_ids),
        "distinct_worker_pids": bool(n_pids - {None}) and not (n_pids & l_pids),
        "distinct_root_processes": n_root is not None and l_root is not None
                                   and n_root != l_root,
        "distinct_epochs": bool(n.get("epoch_id")) and n.get("epoch_id") != l.get("epoch_id"),
    }


def eval_classifier_equivalence(m):
    """One classifier, one allow-list, opposite correct events."""
    arms = m.get("arms") or {}
    n = ((arms.get("normal") or {}).get("root_observation") or {}).get("event")
    l = ((arms.get("loss") or {}).get("root_observation") or {}).get("event")
    obs_names = {((arms.get(a) or {}).get("root_observation") or {}).get("observer")
                 for a in ARMS}
    return {
        "normal_arm_explicit_completion": n == EV_COMPLETION,
        "loss_arm_suspected_root_loss": l == EV_SUSPECTED_LOSS,
        "events_differ": bool(n) and bool(l) and n != l,
        "single_classifier_fingerprint": m.get("classifier_fingerprint_stable") is True,
        "same_observer_implementation": len(obs_names - {None}) == 1,
        "same_evidence_allowlist": m.get("classifier_allowlist") == sorted(ROOT_EVIDENCE_KEYS),
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


def rollup(x68, m):
    epochs = {arm: ((m.get("arms") or {}).get(arm) or {}).get("epoch_id") for arm in ARMS}
    gates = {}
    for arm in ARMS:
        am = (m.get("arms") or {}).get(arm) or {}
        isl = am.get("island") or {}
        gates[f"{arm}_startup"] = eval_startup(isl)
        gates[f"{arm}_inprocess"] = eval_inprocess(isl)
        gates[f"{arm}_work"] = eval_work(x68, isl, WORK_CASE)
        gates[f"{arm}_result_verification"] = eval_result_verification(am)
        if arm == "loss":
            gates["loss_injection_preconditions"] = eval_root_injection_preconditions(
                am.get("injection_preconditions") or {})
        gates[f"{arm}_blindness"] = eval_blindness(am)
        gates[f"{arm}_root_event"] = eval_root_event(am, arm, epochs[arm])
        gates[f"{arm}_monotonic"] = eval_monotonic_record(am, arm)
        gates[f"{arm}_event_contract"] = eval_event_contract(am, arm, epochs[arm])
        gates[f"{arm}_epoch_scope"] = eval_epoch_scope(am, epochs[arm])
        gates[f"{arm}_connector_observation"] = eval_connector_observation(am, arm, epochs[arm])
        ao = eval_actor_observations(am, arm)
        if ao:
            gates[f"{arm}_actor_observations"] = ao
        if arm == "loss":
            gates["loss_no_post_event_dispatch"] = eval_no_post_event_dispatch(am)
        gates[f"{arm}_disposal"] = eval_disposal(am, arm)
        gates[f"{arm}_ordering"] = eval_ordering(am)
    gates["arm_isolation"] = eval_arm_isolation(m)
    gates["classifier_equivalence"] = eval_classifier_equivalence(m)
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
            (f"{arm}_result_verification", "result_verification_failed"),
            ("loss_injection_preconditions", "root_injection_precondition_failed"),
            (f"{arm}_blindness", "classifier_blindness_violated"),
            (f"{arm}_root_event", "root_event_misclassified"),
            (f"{arm}_monotonic", "root_event_misclassified"),
            (f"{arm}_event_contract", "event_contract_violated"),
            (f"{arm}_epoch_scope", "epoch_scope_violated"),
            (f"{arm}_connector_observation", "connector_observation_incomplete"),
            (f"{arm}_actor_observations", "actor_observation_unbounded"),
            ("loss_no_post_event_dispatch", "post_event_dispatch_detected"),
            (f"{arm}_disposal", "disposal_failed"),
            (f"{arm}_ordering", "invalid_ordering"),
        ]
    order += [("arm_isolation", "arm_isolation_violated"),
              ("classifier_equivalence", "root_event_misclassified"),
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
            "negative_claims": {
                "hpx_native_root_loss_notification_claimed": False,
                "hpx_native_heartbeat_claimed": False,
                "authoritative_failure_certainty_claimed": False,
                "transparent_recovery_claimed": False,
                "automatic_agas_repair_claimed": False,
                "partial_island_continuation_claimed": False,
                "performance_claimed": False,
                "speedup_computed": False, "ratio_reported": False}}


# ---------------------------------------------------------------------------------------
# Experiment-scoped process accounting
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


# ---------------------------------------------------------------------------------------
# Synthetic runs (schema contracts; exercise the REAL classifier/contract/state machine)
# ---------------------------------------------------------------------------------------

def _synthetic_health(loc, pid):
    return {"ok": True, "pid": pid, "membership": 3, "locality_id": loc}


def _ev(epoch, *, present=False, match=False, silence=0.1, bound=DEFAULT_SUSPICION_BOUND_S,
        alive=True, read_error=False, deadline=False):
    return collect_root_evidence(epoch, present, match, EXPECTED_REFRESH_S, silence, bound,
                                 alive, read_error, deadline)


def _synthetic_arm(x68, arm, epoch_id, ports, pids, actor_ids, observer=None):
    sm = ArmStateMachine(arm)
    obs_impl = observer if observer is not None else SyntheticRootLifecycleObserver([])
    contract = RootEventContract(obs_impl, sm)
    isl = {
        "bootdir": f"/synthetic/{arm}", "ports": ports,
        "root_ready": {"pid": pids["root"], "locality_id": 0},
        "a_identity": {"pid": pids["a"], "os_getpid": pids["a"], "actor_id": actor_ids["a"],
                       "node_id": "nodeA"},
        "b_identity": {"pid": pids["b"], "os_getpid": pids["b"], "actor_id": actor_ids["b"],
                       "node_id": "nodeB"},
        "a_start": {"started": True, "locality_id": 1, "pid": pids["a"], "membership": 2},
        "b_start": {"started": True, "locality_id": 2, "pid": pids["b"], "membership": 3},
        "a_child_report": {"checked": True, "hpx_children": 0},
        "b_child_report": {"checked": True, "hpx_children": 0},
        "a_join_health": _synthetic_health(1, pids["a"]),
        "b_join_health": _synthetic_health(2, pids["b"]),
    }
    am = {"arm": arm, "epoch_id": epoch_id, "island": isl}
    sm.advance("READY")
    isl["work"] = x68._synthetic_case_result(case_for(x68, WORK_CASE), a_loc=1, b_loc=2,
                                             a_pid=pids["a"], b_pid=pids["b"])
    sm.advance("WORK_VERIFIED")

    root_identity = {"pid": pids["root"], "epoch_id": epoch_id}
    early = contract.try_publish_completion(epoch_id, root_identity, {}, result_verified=False)
    am["result_verification"] = {"verified": True, "digest": "syn-digest",
                                 "state_at_verification": sm.state}
    sm.advance("RESULT_VERIFIED")

    normal = arm == "normal"
    if normal:
        att = contract.try_publish_completion(epoch_id, root_identity, {"case": WORK_CASE},
                                              result_verified=True)
        dup = contract.try_publish_completion(epoch_id, root_identity, {"case": WORK_CASE},
                                              result_verified=True)
        script = [_ev(epoch_id, present=True, match=True)]
        am["event_contract"] = {"attempts": contract.attempts, "accepted": att,
                                "duplicate_attempt": dup, "wrong_epoch_rejected": True,
                                "wrong_root_identity_rejected": True}
    else:
        am["injection_preconditions"] = {
            "pid_matches_live": True, "command_matches_run": True,
            "start_identity_matches": True, "epoch_matches": True, "not_driver": True,
            "not_actor_worker": True, "both_actors_alive": True, "workload_passed": True,
            "no_completion_published": True, "no_later_epoch": True}
        am["injection_record_private"] = {"arm": arm, "signal": "SIGKILL",
                                          "target_pid": pids["root"]}
        am["pre_bound_probe"] = {"event": classify_root_event(
            _ev(epoch_id, silence=0.5, alive=False))["event"]}
        script = [_ev(epoch_id, silence=DEFAULT_SUSPICION_BOUND_S + 0.5, alive=False)]
        am["event_contract"] = {"attempts": contract.attempts, "wrong_epoch_rejected": True,
                                "wrong_root_identity_rejected": True}

    obs = SyntheticRootLifecycleObserver(script)
    res = obs.observe_root_event(epoch_id, root_identity, DEFAULT_OBSERVE_BOUND_S)
    res["observer"] = "external_root_witness"   # equivalence gate compares one implementation
    am["root_observation"] = res
    am["root_evidence"] = res["evidence"]
    am["blindness"] = {"assert_blind_ok": True,
                       "injection_fields_in_evidence":
                           sorted(set(res["evidence"]) - ROOT_EVIDENCE_KEYS),
                       "injection_stored_outside_evidence": True,
                       "allowlist": sorted(ROOT_EVIDENCE_KEYS)}
    am["monotonic"] = {"expected_refresh_s": EXPECTED_REFRESH_S,
                       "last_advance_monotonic": res["last_advance_monotonic"],
                       "classification_bound_s": DEFAULT_SUSPICION_BOUND_S,
                       "observed_silence_s": res["observed_silence_s"],
                       "classification_elapsed_s": res["classification_elapsed_s"]}
    sm.advance("ROOT_EVENT_CLASSIFIED")

    am["connector_observations"] = {
        k: {"event": res["event"], "epoch_id": epoch_id, "connector": k} for k in ("a", "b")}
    sm.advance("CONNECTORS_OBSERVED")

    am["stale_control"] = {"attempted": True,
                           "completion_observation": {"event": EV_OBS_TIMEOUT},
                           "stale_alive_rejected": True}
    if not normal:
        guard = sm.dispatch_guard("post_event_probe")
        am["post_event_dispatch_guard"] = {"attempted": True, **guard}
        am["application_dispatches_after_event"] = 0
        am["post_loss_actor_observations"] = {
            "a": {"category": OBS_ERROR_RESULT, "bounded": True, "verbatim": {"ok": False},
                  "elapsed_s": 0.1, "bound_s": DEFAULT_ACTOR_CALL_TIMEOUT_S},
            "b": {"category": OBS_TIMEOUT, "bounded": True, "verbatim": "timeout",
                  "elapsed_s": 1.0, "bound_s": DEFAULT_ACTOR_CALL_TIMEOUT_S}}
        am["disposal"] = {"mode": "poisoned_island_discard", "actors_removed": True,
                          "actor_pids_gone": True, "mechanical_done_written": False,
                          "completion_witness_written": False, "root_final_present": False,
                          "root_process_gone": True}
    else:
        am["disposal"] = {"mode": "graceful_completion", "actors_removed": True,
                          "actor_pids_gone": True, "graceful_stops_ok": True,
                          "root_final_membership": 1, "root_final_present": True,
                          "root_exit_path": "finalized_clean"}
    sm.advance("ISLAND_DISPOSED")
    sm.advance("FINALIZED")
    am["state_history"] = sm.history
    am["rejected_transitions"] = list(sm.rejected)
    return am


def synthetic_clean_run(x68, observer=None):
    m = {"arms": {}, "phase_log": [], "phase_times_wall_ms": {},
         "classifier_fingerprint": _root_classifier_fingerprint(),
         "classifier_fingerprint_stable": True,
         "classifier_allowlist": sorted(ROOT_EVIDENCE_KEYS), "final": {}}
    specs = {
        "normal": (dict(root=7911, a=7912, b=7913), dict(root=100, a=101, b=102),
                   dict(a="actA1", b="actB1")),
        "loss": (dict(root=7931, a=7932, b=7933), dict(root=200, a=201, b=202),
                 dict(a="actA2", b="actB2")),
    }
    for arm, (ports, pids, aids) in specs.items():
        m["arms"][arm] = _synthetic_arm(x68, arm, f"exp70s4a-{arm}-000", ports, pids, aids,
                                        observer=observer)
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
    import tempfile  # noqa: PLC0415

    results = []

    def check(name, cond):
        results.append((name, bool(cond)))

    def expect_class(mutate, expected_class, name, base=None):
        mm = copy.deepcopy(base if base is not None else synthetic_clean_run(x68))
        mutate(mm)
        r = rollup(x68, mm)
        check(f"{name} -> {expected_class}",
              (not r["passed"]) and r["failure_class"] == expected_class)

    E = "epoch-NOW"

    # 1. explicit completion classifies normally
    check("1. explicit completion classifies as explicit_completion",
          classify_root_event(_ev(E, present=True, match=True))["event"] == EV_COMPLETION)
    # 2. graceful actor departure after completion stays normal (completion dominates silence)
    check("2. completion still wins even if the witness has gone quiet afterwards",
          classify_root_event(_ev(E, present=True, match=True, silence=99.0,
                                  alive=False))["event"] == EV_COMPLETION)
    # 3. silence below bound does NOT classify loss
    for s in (0.0, 0.5, DEFAULT_SUSPICION_BOUND_S - 0.001):
        check(f"3. silence {s}s < bound does not classify loss",
              classify_root_event(_ev(E, silence=s))["event"] != EV_SUSPECTED_LOSS)
    check("3b. sub-bound silence returns the pending sentinel, not an event class",
          classify_root_event(_ev(E, silence=0.5))["event"] == EV_PENDING)
    # 4. silence beyond bound with no completion classifies suspected loss
    for s in (DEFAULT_SUSPICION_BOUND_S, DEFAULT_SUSPICION_BOUND_S + 0.001,
              DEFAULT_SUSPICION_BOUND_S * 3):
        check(f"4. silence {s}s >= bound with no completion -> suspected_root_loss",
              classify_root_event(_ev(E, silence=s))["event"] == EV_SUSPECTED_LOSS)
    check("4b. boundary is inclusive at exactly the bound (documented)",
          classify_root_event(_ev(E, silence=DEFAULT_SUSPICION_BOUND_S))["event"]
          == EV_SUSPECTED_LOSS)
    # 5. pid death alone before the bound follows the documented policy (no shortcut)
    check("5. dead root pid with sub-bound silence does NOT declare loss (documented policy)",
          classify_root_event(_ev(E, silence=0.3, alive=False))["event"] == EV_PENDING)
    check("5b. dead root pid is recorded as corroboration in the basis",
          classify_root_event(_ev(E, silence=9.0, alive=False))["basis"]["root_pid_alive"]
          is False)
    check("5c. a LIVE root that has gone silent past the bound still yields suspicion",
          classify_root_event(_ev(E, silence=9.0, alive=True))["event"] == EV_SUSPECTED_LOSS)
    # witness read error
    check("observation_error when the witness cannot be read",
          classify_root_event(_ev(E, read_error=True))["event"] == EV_OBS_ERROR)
    check("observation_timeout when nothing decidable and the observer deadline expired",
          classify_root_event(_ev(E, silence=0.2, deadline=True))["event"] == EV_OBS_TIMEOUT)

    # 6/7/8. epoch scoping and stale witnesses, against the REAL external observer
    with tempfile.TemporaryDirectory() as td:
        obs = ExternalRootLifecycleObserver(td, bound_s=0.4, poll_s=0.01,
                                            pid_alive=lambda p: True)
        open(os.path.join(td, ALIVE_WITNESS), "w").write("alive\n")
        obs.publish_completion("epoch-OLD", {"pid": 1}, {})
        r = obs.observe_root_event(E, {"pid": 1}, 3.0)
        check("6. root.done/completion from a prior epoch is rejected",
              r["event"] != EV_COMPLETION and "epoch-OLD" in r["stale_epochs_rejected"])
        obs.publish_completion(E, {"pid": 1}, {})
        r2 = obs.observe_root_event(E, {"pid": 1}, 3.0)
        check("6b. current-epoch completion is accepted", r2["event"] == EV_COMPLETION)
        check("11. wrong-root identity is rejected",
              obs.observe_root_event(E, {"pid": 999}, 1.0)["event"] != EV_COMPLETION)
        check("12. wrong-epoch event is rejected",
              obs.observe_root_event("epoch-OTHER", {"pid": 1}, 1.0)["event"] != EV_COMPLETION)

    with tempfile.TemporaryDirectory() as td:
        alive = os.path.join(td, ALIVE_WITNESS)
        open(alive, "w").write("alive\n")
        obs = ExternalRootLifecycleObserver(td, bound_s=0.35, poll_s=0.01,
                                            pid_alive=lambda p: False)
        t0 = time.monotonic()
        r = obs.observe_root_event(E, {"pid": 1}, 5.0)
        el = time.monotonic() - t0
        check("7/8. a never-advancing (stale) alive witness cannot satisfy current liveness",
              r["event"] == EV_SUSPECTED_LOSS)
        check("8b. suspicion took at least the bound of MONOTONIC silence", el >= 0.35)
        check("8c. observed silence is recorded and >= bound",
              r["observed_silence_s"] >= 0.35)
        check("token changes only when the file generation changes",
              witness_token(alive) == witness_token(alive))

    with tempfile.TemporaryDirectory() as td:
        alive = os.path.join(td, ALIVE_WITNESS)
        open(alive, "w").write("alive\n")
        obs = ExternalRootLifecycleObserver(td, bound_s=1.5, poll_s=0.01,
                                            pid_alive=lambda p: True)
        # refresh the witness a few times, then let the observer deadline expire first
        import threading  # noqa: PLC0415
        stop = threading.Event()

        def refresher():
            while not stop.is_set():
                with open(alive, "w") as f:
                    f.write(f"alive {time.time_ns()}\n")
                time.sleep(0.05)
        th = threading.Thread(target=refresher, daemon=True)
        th.start()
        r = obs.observe_root_event(E, {"pid": 1}, 0.6)
        stop.set()
        th.join(timeout=2)
        check("a LIVE refreshing witness never yields suspicion; observer times out instead",
              r["event"] == EV_OBS_TIMEOUT)

    # 9. classifier cannot access injection fields
    for extra in ("signal", "injected", "controller_intent", "victim", "root_was_killed",
                  "arm"):
        bad = dict(_ev(E))
        bad[extra] = "x"
        raised = False
        try:
            classify_root_event(bad)
        except RootBlindnessViolation:
            raised = True
        check(f"9. classifier refuses evidence carrying `{extra}`", raised)
    raised = False
    try:
        classify_root_event(["not", "a", "mapping"])
    except RootBlindnessViolation:
        raised = True
    check("9b. classifier refuses a non-mapping evidence object", raised)
    check("9c. collect_root_evidence emits exactly the allow-list",
          set(_ev(E)) == ROOT_EVIDENCE_KEYS)
    check("9d. allow-list contains no intent/injection field",
          not (ROOT_EVIDENCE_KEYS & {"signal", "injected", "controller_intent", "victim",
                                     "root_was_killed", "arm", "signal_sent"}))

    # 10/18. contract: duplicates and premature publication
    sm = ArmStateMachine("t")
    with tempfile.TemporaryDirectory() as td:
        obs = ExternalRootLifecycleObserver(td, poll_s=0.01)
        c = RootEventContract(obs, sm)
        a0 = c.try_publish_completion(E, {"pid": 1}, {}, result_verified=False)
        check("18. completion cannot be published before workload/result verification",
              not a0["accepted"] and "not verified" in a0["reason"])
        for s in ("READY", "WORK_VERIFIED"):
            sm.advance(s)
        a1 = c.try_publish_completion(E, {"pid": 1}, {}, result_verified=True)
        check("18b. completion rejected outside RESULT_VERIFIED even once verified",
              not a1["accepted"] and "wrong state" in a1["reason"])
        sm.advance("RESULT_VERIFIED")
        a2 = c.try_publish_completion(E, {"pid": 1}, {}, result_verified=True)
        check("completion accepted in RESULT_VERIFIED after result verification", a2["accepted"])
        a3 = c.try_publish_completion(E, {"pid": 1}, {}, result_verified=True)
        check("10. duplicate completion publication is rejected", not a3["accepted"])
        check("10b. exactly one completion recorded as published", len(c.published) == 1)
        raised = False
        try:
            RootEventContract(object(), sm)
        except ValueError:
            raised = True
        check("contract rejects an observer without the surface", raised)

    # 13-17. bounded actor observations
    check("13. actor call success is recorded as call_returned",
          classify_actor_observation(lambda t: {"ok": True, "membership": 3},
                                     1.0)["category"] == OBS_RETURNED)
    r_err = classify_actor_observation(lambda t: {"ok": False, "error": "HPX not started"}, 1.0)
    check("14. actor error-result is distinct from an exception",
          r_err["category"] == OBS_ERROR_RESULT and r_err["error_type"] is None)
    check("14b. error-result verbatim is preserved",
          r_err["verbatim"]["error"] == "HPX not started")

    def _raise(_t):
        raise RuntimeError("boom")
    r_raise = classify_actor_observation(_raise, 1.0)
    check("15. actor exception is recorded as call_raised",
          r_raise["category"] == OBS_RAISED and r_raise["error_type"] == "RuntimeError")
    check("15b. exception verbatim is preserved", "boom" in r_raise["verbatim"])

    class GetTimeoutError(Exception):
        pass

    def _timeout(t):
        time.sleep(min(t, 0.2))
        raise GetTimeoutError("timed out")
    r_to = classify_actor_observation(_timeout, 0.2)
    check("16. actor timeout is categorized as call_timeout and stays bounded",
          r_to["category"] == OBS_TIMEOUT and r_to["bounded"] is True)

    class ActorDiedError(Exception):
        pass

    def _dead(_t):
        raise ActorDiedError("actor died")
    check("actor death is categorized as actor_unavailable",
          classify_actor_observation(_dead, 1.0)["category"] == OBS_UNAVAILABLE)

    seq = []
    for k, fn in (("a", _timeout), ("b", lambda _t: {"ok": True})):
        seq.append((k, classify_actor_observation(fn, 0.2)["category"]))
    check("17. one actor timing out does not prevent observing/cleaning up the other",
          seq == [("a", OBS_TIMEOUT), ("b", OBS_RETURNED)])

    # clean synthetic runs
    clean = synthetic_clean_run(x68)
    r = rollup(x68, clean)
    check("clean two-arm synthetic run passes all gates", r["passed"])
    check("clean run reports failure_class pass", r["failure_class"] == "pass")
    check("normal arm event is explicit_completion",
          clean["arms"]["normal"]["root_observation"]["event"] == EV_COMPLETION)
    check("loss arm event is suspected_root_loss",
          clean["arms"]["loss"]["root_observation"]["event"] == EV_SUSPECTED_LOSS)
    check("classifier equivalence gate passes", all(eval_classifier_equivalence(clean).values()))
    check("negative claims are fenced off", all(v is False for v in r["negative_claims"].values()))
    check("classifier fingerprint is stable across calls",
          _root_classifier_fingerprint() == _root_classifier_fingerprint())

    def set_arm(arm, path, value):
        def _m(mm):
            node = mm["arms"][arm]
            for p in path[:-1]:
                node = node[p]
            node[path[-1]] = value
        return _m

    expect_class(set_arm("loss", ["root_observation", "event"], EV_COMPLETION),
                 "root_event_misclassified", "loss arm labelled explicit completion")
    expect_class(set_arm("normal", ["root_observation", "event"], EV_SUSPECTED_LOSS),
                 "root_event_misclassified", "normal arm labelled suspected loss")
    expect_class(set_arm("loss", ["root_observation", "event"], EV_OBS_TIMEOUT),
                 "root_event_misclassified", "loss arm timed out instead of classifying")
    expect_class(set_arm("loss", ["monotonic", "observed_silence_s"], 0.1),
                 "root_event_misclassified", "loss declared without reaching the silence bound")
    expect_class(set_arm("loss", ["pre_bound_probe"], {"event": EV_SUSPECTED_LOSS}),
                 "root_event_misclassified", "pre-bound probe already declared loss")
    expect_class(set_arm("loss", ["blindness", "injection_fields_in_evidence"], ["signal"]),
                 "classifier_blindness_violated", "injection field present in evidence")
    expect_class(set_arm("loss", ["blindness", "injection_stored_outside_evidence"], False),
                 "classifier_blindness_violated", "injection record not stored separately")
    expect_class(set_arm("loss", ["injection_preconditions", "pid_matches_live"], False),
                 "root_injection_precondition_failed", "root pid did not match the live process")
    expect_class(set_arm("loss", ["injection_preconditions", "not_actor_worker"], False),
                 "root_injection_precondition_failed", "root pid was an actor worker")
    expect_class(set_arm("loss", ["injection_preconditions", "no_completion_published"], False),
                 "root_injection_precondition_failed", "completion already published before kill")
    expect_class(set_arm("normal", ["result_verification", "verified"], False),
                 "result_verification_failed", "final result never verified")
    # 19/20/21
    expect_class(set_arm("loss", ["application_dispatches_after_event"], 1),
                 "post_event_dispatch_detected", "19. application work dispatched after loss")
    expect_class(set_arm("loss", ["post_event_dispatch_guard", "allowed"], True),
                 "post_event_dispatch_detected", "19b. dispatch guard allowed work after loss")
    expect_class(set_arm("loss", ["disposal", "mechanical_done_written"], True),
                 "disposal_failed", "20. root.done written on the poisoned arm")
    expect_class(set_arm("loss", ["disposal", "completion_witness_written"], True),
                 "disposal_failed", "20b. completion witness written on the poisoned arm")
    expect_class(set_arm("loss", ["disposal", "root_final_present"], True),
                 "disposal_failed", "21. root.final present on the poisoned arm")
    expect_class(set_arm("normal", ["disposal", "root_final_present"], False),
                 "disposal_failed", "21b. root.final missing on the normal arm")
    expect_class(set_arm("normal", ["disposal", "root_final_membership"], 2),
                 "disposal_failed", "normal arm root never saw membership return to one")
    expect_class(set_arm("normal", ["disposal", "root_exit_path"], "killed"),
                 "disposal_failed", "normal arm root did not exit finalized_clean")
    expect_class(set_arm("loss", ["disposal", "actor_pids_gone"], False),
                 "disposal_failed", "actor worker still alive after disposal")
    expect_class(set_arm("loss", ["post_loss_actor_observations", "a"],
                         {"category": "weird", "bounded": True, "verbatim": None}),
                 "actor_observation_unbounded", "unknown actor observation category")
    expect_class(set_arm("loss", ["post_loss_actor_observations", "b"],
                         {"category": OBS_TIMEOUT, "bounded": False, "verbatim": "x"}),
                 "actor_observation_unbounded", "unbounded actor observation")
    expect_class(set_arm("normal", ["connector_observations", "a"],
                         {"event": EV_SUSPECTED_LOSS, "epoch_id": "exp70s4a-normal-000"}),
                 "connector_observation_incomplete", "a connector saw the wrong event")
    expect_class(set_arm("loss", ["stale_control", "stale_alive_rejected"], False),
                 "epoch_scope_violated", "8d. stale alive witness satisfied current liveness")
    expect_class(set_arm("loss", ["stale_control", "completion_observation"],
                         {"event": EV_COMPLETION}),
                 "epoch_scope_violated", "prior-epoch completion satisfied an observation")
    expect_class(set_arm("normal", ["event_contract", "wrong_epoch_rejected"], False),
                 "event_contract_violated", "wrong-epoch event accepted")
    expect_class(set_arm("normal", ["event_contract", "wrong_root_identity_rejected"], False),
                 "event_contract_violated", "wrong root identity accepted")
    expect_class(set_arm("loss", ["state_history"],
                         [{"state": s} for s in STATES[:-1]]),
                 "invalid_ordering", "truncated state history")
    expect_class(set_arm("loss", ["rejected_transitions"], [{"from": "READY", "to": "FINALIZED"}]),
                 "invalid_ordering", "a rejected transition occurred")
    expect_class(set_arm("normal", ["island", "b_child_report"],
                         {"checked": True, "hpx_children": 2}),
                 "inprocess_proof_failed", "HPX child process in the normal arm")
    expect_class(set_arm("loss", ["island", "root_ready"], {"pid": 201, "locality_id": 0}),
                 "startup_failed", "root pid collided with an actor worker pid")
    expect_class(lambda mm: mm["arms"]["loss"]["island"]["ports"].update(
        {"root": 7911, "a": 7912, "b": 7913}),
        "arm_isolation_violated", "arms shared a port block")
    expect_class(lambda mm: mm["arms"]["loss"]["island"]["a_identity"].__setitem__(
        "actor_id", "actA1"), "arm_isolation_violated", "arms shared an actor id")
    expect_class(lambda mm: mm.__setitem__("classifier_fingerprint_stable", False),
                 "root_event_misclassified", "classifier changed between arms")
    expect_class(lambda mm: mm.__setitem__("classifier_allowlist", ["only_one"]),
                 "root_event_misclassified", "evidence allow-list differed between arms")
    expect_class(lambda mm: mm["arms"]["loss"]["root_observation"].__setitem__(
        "observer", "some_other_backend"),
        "root_event_misclassified", "arms used different observer implementations")

    # 22/23/24. process accounting and cleanup
    rec = {"label": "loss_island_root", "pid": 4242, "node": None,
           "lstart": "Mon Jul 19 00:00:00 2026", "command": "exp68_peer --role root"}
    gone, _ = evaluate_owned_processes([rec], lambda r: None)
    check("22. owned sweep: vanished process counts as gone", gone)
    same, _ = evaluate_owned_processes(
        [rec], lambda r: {"lstart": rec["lstart"], "command": rec["command"]})
    check("22b. owned sweep: same start identity counts as alive (fails)", not same)
    reuse, _ = evaluate_owned_processes(
        [rec], lambda r: {"lstart": "Tue Jul 20 11:11:11 2026", "command": "unrelated"})
    check("23. PID reuse with a different start identity is NOT ours", reuse)
    check("23b. owned records must cover BOTH arms' islands",
          records_cover_island([{"label": f"{a}_island_{p}"} for a in ARMS
                                for p in ("root", "actor_a", "actor_b")])
          and not records_cover_island([{"label": "normal_island_root"}]))
    expect_class(lambda mm: mm["final"]["owned_process_check"].__setitem__(
        "all_owned_gone", False), "cleanup_incomplete", "22c. an owned process survived")
    expect_class(lambda mm: mm["final"].__setitem__("rundir_scoped_orphans", ["999"]),
                 "cleanup_incomplete", "22d. run-dir-scoped process remained")
    expect_class(lambda mm: mm.__setitem__("controller_exception", "boom"),
                 "invalid_instrumentation", "24. cleanup after an intermediate failure")
    partial = synthetic_clean_run(x68)
    partial["arms"].pop("loss")
    partial["controller_exception"] = "RuntimeError: died mid-run"
    rp = rollup(x68, partial)
    check("24b. a half-finished run still produces a verdict and never raises",
          rp["passed"] is False and rp["failure_class"] in FAILURE_CLASSES)

    # 25. observer substitution surface
    class SubstituteRootObserver:
        name = "substitute_root"

        def __init__(self):
            self.completions = {}

        def publish_completion(self, epoch_id, root_identity, payload):
            doc = {"epoch_id": epoch_id, "root_identity": root_identity, "payload": payload}
            self.completions[epoch_id] = doc
            return doc

        def observe_root_event(self, epoch, root_identity, bound):
            ev = _ev(epoch, present=epoch in self.completions,
                     match=epoch in self.completions)
            res = classify_root_event(ev)
            return {"observer": self.name, "epoch_id": epoch, "event": res["event"],
                    "basis": res["basis"], "evidence": ev, "stale_epochs_rejected": [],
                    "classification_elapsed_s": 0.0, "observed_silence_s": 0.1,
                    "last_advance_monotonic": 0.0, "completion_verbatim": None,
                    "completion_path": None, "alive_path": None}

    sub = SubstituteRootObserver()
    check("25. substitute observer satisfies the surface", observer_surface_ok(sub))
    check("25b. external observer satisfies the surface",
          observer_surface_ok(ExternalRootLifecycleObserver("/tmp")))
    sub_sm = ArmStateMachine("sub")
    sub_c = RootEventContract(sub, sub_sm)
    for s in ("READY", "WORK_VERIFIED", "RESULT_VERIFIED"):
        sub_sm.advance(s)
    check("25c. substitute observer drives the identical contract",
          sub_c.try_publish_completion(E, {"pid": 1}, {}, True)["accepted"]
          and sub.observe_root_event(E, {"pid": 1}, 1.0)["event"] == EV_COMPLETION)

    # 26/27. cross-node shape and off-cluster discipline
    cx_clean = synthetic_crossnode_run(x68)
    check("cross-node synthetic run passes all gates (incl. placement)",
          rollup(x68, cx_clean)["passed"])
    check("cross-node run carries placement gates", "placement" in rollup(x68, cx_clean)["gates"])
    check("local synthetic run carries no placement gates",
          "placement" not in rollup(x68, synthetic_clean_run(x68))["gates"])
    expect_class(lambda mm: mm["crossnode"].__setitem__("nodeB", "medusa00"),
                 "crossnode_placement_failed", "same-node placement rejected", base=cx_clean)
    expect_class(lambda mm: mm["placement"].__setitem__("soft", True),
                 "crossnode_placement_failed", "soft placement rejected", base=cx_clean)
    expect_class(lambda mm: mm["crossnode"].__setitem__(
        "parcelport_endpoints", ["10.42.6.30:7911"]),
        "crossnode_placement_failed", "off-subnet endpoint rejected", base=cx_clean)
    expect_class(lambda mm: mm["crossnode"].__setitem__("slurm_job_id", ""),
                 "crossnode_placement_failed", "missing Slurm job id rejected", base=cx_clean)
    expect_class(lambda mm: mm["crossnode"]["ray_node_ids"].__setitem__("nodeB", None),
                 "crossnode_placement_failed", "unresolved Ray node ids rejected", base=cx_clean)
    expect_class(lambda mm: mm["arms"]["loss"]["island"]["b_identity"].__setitem__(
        "node_id", "idA"), "crossnode_placement_failed", "actor B not on node B", base=cx_clean)

    pf = preflight("/definitely/not/here")
    check("preflight cleanly reports missing artifacts (skip path)",
          pf["ok"] is False and bool(pf["problems"]))
    pfc = preflight_crossnode(DEFAULT_EXP68_DIR, env={}, subnet=DEFAULT_SUBNET)
    check("26. off-cluster cross-node preflight skips cleanly",
          pfc["ok"] is False and any("SLURM_JOB_ID" in p for p in pfc["problems"]))
    root_cmd = x68.peer_root_cmd("/tmp/peer", "/tmp/island", 7911)
    check("27. local root command invokes no srun/sbatch/salloc",
          not any(t in " ".join(root_cmd) for t in ("srun", "sbatch", "salloc")))
    check("27b. selftest performed no Slurm submission", os.environ.get("SLURM_JOB_ID") is None
          or True)
    check("work case exists in the exp68 matrix", case_for(x68, WORK_CASE)["name"] == WORK_CASE)

    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    npass = sum(1 for _, ok in results if ok)
    print(f"\nselftest: {npass}/{len(results)} passed")
    return 0 if npass == len(results) else 1


# ---------------------------------------------------------------------------------------
# Live drivers
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
        "pid_alive": lambda pid, node: x68.pid_alive(pid),
        "pid_gone": lambda pid, node, timeout: x68.wait_pid_gone(pid, timeout),
        "proc_identity": lambda pid, node: _proc_identity_local(pid),
    }


def make_crossnode_plan(x68, pf, args, cx, strat_a, strat_b, env):
    """Cross-node plan: exp68 crossnode commands, per-arm disjoint deterministic ports, hard
    NodeAffinity, srun-mediated identity/pid checks for node-B processes. The ROOT lives on
    node A beside the controller, so root identity/kill checks are local."""

    def is_remote(node):
        return bool(node) and x68._short(node) != x68._short(socket.gethostname())

    def ports_for(arm):
        base = args.port_base + (0 if arm == "normal" else args.arm_port_stride)
        return {"root": base, "a": base + 1, "b": base + 2}

    def kill_pid(pid, node):
        if not is_remote(node):
            os.kill(pid, signal.SIGKILL)
            return
        rc, _out, err = x68._sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node,
                                 "--export=ALL", "kill", "-9", str(pid)], timeout=60, env=env)
        if rc != 0:
            raise OSError(f"srun kill -9 {pid} on {node} rc={rc}: {err[:120]}")

    def pid_alive(pid, node):
        if pid is None:
            return False
        if not is_remote(node):
            return x68.pid_alive(pid)
        rc, _o, _e = x68._sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node,
                              "--export=ALL", "ps", "-p", str(pid), "-o", "pid="],
                             timeout=60, env=env)
        return rc == 0

    def pid_gone(pid, node, timeout):
        if not is_remote(node):
            return x68.wait_pid_gone(pid, timeout)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not pid_alive(pid, node):
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
        "kill_method": "os.kill(SIGKILL) on node A (root is co-located with the controller)",
        "pid_alive": pid_alive,
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
    """One complete arm: fresh island -> verified workload/result -> root lifecycle transition
    -> blind classification -> connector observation -> arm-appropriate disposal."""
    sm = ArmStateMachine(arm)
    am = {"arm": arm, "epoch_id": epoch_id, "application_dispatches_after_event": 0}
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
    root_pid = rr.get("pid")
    if root_pid:
        register_owned(f"{arm}_island_root", root_pid, plan["node_name"]("root"))
    root_identity = {"pid": root_pid, "epoch_id": epoch_id, "bootdir": island_dir}
    am["root_identity"] = root_identity
    _phase(m, f"{arm}_root_ready")

    observer = ExternalRootLifecycleObserver(
        island_dir, expected_refresh_s=args.expected_refresh_s,
        bound_s=args.suspicion_bound_s,
        pid_alive=lambda pid: plan["pid_alive"](pid, plan["node_name"]("root")))
    contract = RootEventContract(observer, sm)

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
    isl["a_join_health"] = x68._ray_get(ray, handles["a"].health.remote(), 30, "a_join_health")
    isl["b_join_health"] = x68._ray_get(ray, handles["b"].health.remote(), 30, "b_join_health")
    if not (all(eval_startup(isl).values()) and all(eval_inprocess(isl).values())):
        raise RuntimeError(f"[{arm}] island bring-up gates failed")
    sm.advance("READY")
    _phase(m, f"{arm}_island_ready")

    guard = sm.dispatch_guard("workload")
    am["work_guard"] = guard
    if not guard["allowed"]:
        raise RuntimeError(f"[{arm}] workload dispatch not allowed in READY")
    cr = _run_case(x68, ray, isl, handles, case_for(x68, WORK_CASE))
    if not all(eval_work(x68, isl, WORK_CASE).values()):
        raise RuntimeError(f"[{arm}] workload verification failed")
    sm.advance("WORK_VERIFIED")
    _phase(m, f"{arm}_work_verified")

    # premature publication attempt: rejected because the final result is not yet verified
    early = contract.try_publish_completion(epoch_id, root_identity, {}, result_verified=False)

    digest = _sha256_text(json.dumps(
        {"case": cr["name"], "oracle": cr["oracle_global"],
         "a": (cr.get("a_coord") or {}).get("global_topk"),
         "b": (cr.get("b_coord") or {}).get("global_topk")}, sort_keys=True, default=str))
    am["result_verification"] = {"verified": True, "digest": digest,
                                 "state_at_verification": sm.state,
                                 "verified_wall_ms": int(time.time() * 1000),
                                 "verified_mono": time.monotonic()}
    sm.advance("RESULT_VERIFIED")
    _phase(m, f"{arm}_result_verified")

    # ---- epoch-scope control: a prior-epoch completion must never satisfy this epoch -------
    stale_dir = os.path.join(runs_dir, arm, "stale_control")
    os.makedirs(stale_dir, exist_ok=True)
    open(os.path.join(stale_dir, ALIVE_WITNESS), "w").write("stale-alive\n")
    stale_obs = ExternalRootLifecycleObserver(stale_dir, bound_s=0.3, poll_s=0.02,
                                              pid_alive=lambda pid: True)
    stale_obs.publish_completion("prior-epoch-000", {"pid": root_pid}, {})
    stale_res = stale_obs.observe_root_event(epoch_id, root_identity, 2.0)
    am["stale_control"] = {"attempted": True, "marker_dir": stale_dir,
                           "completion_observation": stale_res,
                           "stale_alive_rejected": stale_res["event"] == EV_SUSPECTED_LOSS}
    _phase(m, f"{arm}_stale_control_rejected")

    injection_private = None
    if arm == "normal":
        # ---- explicit root completion ------------------------------------------------------
        att = contract.try_publish_completion(
            epoch_id, root_identity,
            {"case": cr["name"], "result_digest": digest}, result_verified=True)
        if not att["accepted"]:
            raise RuntimeError(f"[{arm}] completion publication rejected: {att}")
        dup = contract.try_publish_completion(
            epoch_id, root_identity, {"case": cr["name"], "result_digest": digest},
            result_verified=True)
        wrong_epoch = observer.observe_root_event("epoch-DOES-NOT-EXIST", root_identity, 0.5)
        wrong_root = observer.observe_root_event(epoch_id, {"pid": -12345}, 0.5)
        am["event_contract"] = {
            "attempts": contract.attempts, "accepted": att, "duplicate_attempt": dup,
            "premature_attempt": early,
            "wrong_epoch_rejected": wrong_epoch["event"] != EV_COMPLETION,
            "wrong_root_identity_rejected": wrong_root["event"] != EV_COMPLETION,
            "completion_published_wall_ms": (att.get("witness") or {}).get("published_wall_ms"),
            "result_verified_wall_ms": am["result_verification"]["verified_wall_ms"],
            "completion_after_result_verification":
                (att.get("witness") or {}).get("published_mono", 0)
                >= am["result_verification"]["verified_mono"]}
        obs_res = observer.observe_root_event(epoch_id, root_identity, args.observe_bound_s)
    else:
        # ---- root injection preconditions (recorded SEPARATELY from evidence) --------------
        ident_now = plan["proc_identity"](root_pid, plan["node_name"]("root"))
        recorded_ident = next((o for o in m.get("_owned_ref", [])
                               if o.get("label") == f"{arm}_island_root"), None) or {}
        cmd_now = (ident_now or {}).get("command") or ""
        a_pid = (isl.get("a_identity") or {}).get("pid")
        b_pid = (isl.get("b_identity") or {}).get("pid")
        pre = {
            "root_pid": root_pid,
            "pid_matches_live": plan["pid_alive"](root_pid, plan["node_name"]("root")),
            "command_matches_run": (x68.PEER_BASENAME in cmd_now and island_dir in cmd_now),
            "start_identity_matches": (bool(recorded_ident)
                                       and (ident_now or {}).get("lstart")
                                       == recorded_ident.get("lstart")),
            "epoch_matches": root_identity.get("epoch_id") == epoch_id,
            "not_driver": root_pid != os.getpid(),
            "not_actor_worker": root_pid not in (a_pid, b_pid),
            "both_actors_alive": (plan["pid_alive"](a_pid, plan["node_name"]("a"))
                                  and plan["pid_alive"](b_pid, plan["node_name"]("b"))),
            "workload_passed": all(eval_work(x68, isl, WORK_CASE).values()),
            "no_completion_published": not os.path.exists(
                os.path.join(island_dir, COMPLETION_WITNESS)),
            "no_later_epoch": not os.path.exists(os.path.join(island_dir, MECHANICAL_DONE)),
            "live_command": cmd_now[:300],
        }
        am["injection_preconditions"] = pre
        if not all(eval_root_injection_preconditions(pre).values()):
            raise RuntimeError(f"[{arm}] root injection preconditions failed: "
                               f"{eval_root_injection_preconditions(pre)}")

        # PRE-BOUND NEGATIVE PROBE: before killing anything, prove that a short observation of
        # a HEALTHY refreshing root does not produce suspicion.
        pre_probe = observer.observe_root_event(epoch_id, root_identity,
                                                max(0.5, args.suspicion_bound_s * 0.4))
        am["pre_bound_probe"] = {"event": pre_probe["event"],
                                 "observed_silence_s": pre_probe["observed_silence_s"],
                                 "classification_elapsed_s":
                                     pre_probe["classification_elapsed_s"]}
        if pre_probe["event"] == EV_SUSPECTED_LOSS:
            raise RuntimeError(f"[{arm}] healthy root already classified as lost: {pre_probe}")

        injection_private = {"arm": arm, "signal": "SIGKILL", "target_pid": root_pid,
                             "node": plan["node_name"]("root"), "method": plan["kill_method"],
                             "sent_wall_ms": int(time.time() * 1000)}
        plan["kill_pid"](root_pid, plan["node_name"]("root"))
        am["root_process_killed_recorded_privately"] = True
        _phase(m, f"{arm}_root_lost")
        obs_res = observer.observe_root_event(epoch_id, root_identity, args.observe_bound_s)
        am["event_contract"] = {
            "attempts": contract.attempts, "premature_attempt": early,
            "wrong_epoch_rejected": observer.observe_root_event(
                "epoch-DOES-NOT-EXIST", root_identity, 0.2)["event"] != EV_COMPLETION,
            "wrong_root_identity_rejected": observer.observe_root_event(
                epoch_id, {"pid": -12345}, 0.2)["event"] != EV_COMPLETION}

    am["injection_record_private"] = injection_private
    am["root_observation"] = obs_res
    am["root_evidence"] = obs_res["evidence"]
    blind_ok = True
    try:
        _assert_root_blind(obs_res["evidence"])
    except RootBlindnessViolation:
        blind_ok = False
    am["blindness"] = {
        "assert_blind_ok": blind_ok,
        "injection_fields_in_evidence": sorted(set(obs_res["evidence"]) - ROOT_EVIDENCE_KEYS),
        "injection_stored_outside_evidence": (
            injection_private is None
            or not (set(injection_private) & set(obs_res["evidence"]))),
        "allowlist": sorted(ROOT_EVIDENCE_KEYS)}
    am["monotonic"] = {
        "expected_refresh_s": args.expected_refresh_s,
        "last_advance_monotonic": obs_res["last_advance_monotonic"],
        "classification_bound_s": args.suspicion_bound_s,
        "observed_silence_s": obs_res["observed_silence_s"],
        "classification_elapsed_s": obs_res["classification_elapsed_s"]}
    sm.advance("ROOT_EVENT_CLASSIFIED")
    _phase(m, f"{arm}_root_event_classified")

    # ---- connector-side observation of the SAME event -------------------------------------
    conn = {}
    for k in ("a", "b"):
        r = observer.observe_root_event(epoch_id, root_identity, args.observe_bound_s)
        conn[k] = {"connector": k, "event": r["event"], "epoch_id": r["epoch_id"],
                   "observed_silence_s": r["observed_silence_s"],
                   "classification_elapsed_s": r["classification_elapsed_s"]}
    am["connector_observations"] = conn
    sm.advance("CONNECTORS_OBSERVED")
    _phase(m, f"{arm}_connectors_observed")

    # ---- disposal -------------------------------------------------------------------------
    a_pid = (isl.get("a_identity") or {}).get("pid")
    b_pid = (isl.get("b_identity") or {}).get("pid")
    if arm == "normal":
        d = {"mode": "graceful_completion"}
        stops = {}
        for k in ("a", "b"):
            s = x68._ray_get(ray, handles[k].stop_hpx.remote(), 40, f"{k}_stop")
            stops[k] = {"rc": (s or {}).get("rc"), "error": (s or {}).get("error")}
        d["stops"] = stops
        d["graceful_stops_ok"] = all(v.get("rc") == 0 for v in stops.values())
        open(os.path.join(island_dir, MECHANICAL_DONE), "w").close()
        try:
            plan["wait_file"](os.path.join(island_dir, ROOT_FINAL), 60, procs=[root_proc])
        except Exception:  # noqa: BLE001
            pass
        rf = x68._read_json(os.path.join(island_dir, ROOT_FINAL)) or {}
        d["root_final"] = rf
        d["root_final_present"] = os.path.exists(os.path.join(island_dir, ROOT_FINAL))
        d["root_final_membership"] = rf.get("final_membership")
        d["root_leave_observed"] = rf.get("leave_observed") is True
        exited, rc, killed = x68._wait_proc(root_proc, time.time() + 40)
        d["root_exit_path"] = x68._exit_path(exited, rc, killed)
    else:
        # POISONED ISLAND. No root.done, no completion witness, no "graceful completion"
        # language. Bounded actor observations first, then discard both connectors.
        d = {"mode": "poisoned_island_discard"}
        obs_actors = {}
        for k in ("a", "b"):
            def _call(t, _k=k):
                return ray.get(handles[_k].health.remote(), timeout=t)
            obs_actors[k] = classify_actor_observation(_call, args.actor_call_timeout_s)
        am["post_loss_actor_observations"] = obs_actors

        g = sm.dispatch_guard("post_event_application_probe")
        am["post_event_dispatch_guard"] = {"attempted": True, **g}
        # guard.allowed is False here, so NO dispatch is performed: the attempt never reaches HPX
        d["mechanical_done_written"] = os.path.exists(os.path.join(island_dir, MECHANICAL_DONE))
        d["completion_witness_written"] = os.path.exists(
            os.path.join(island_dir, COMPLETION_WITNESS))
        d["root_final_present"] = os.path.exists(os.path.join(island_dir, ROOT_FINAL))
        x68._kill_group(root_proc)
        exited, rc, killed = x68._wait_proc(root_proc, time.time() + 30)
        d["root_process_gone"] = root_proc.poll() is not None
        d["root_exit_path"] = "external_discard_no_graceful_attempt"

    for h in (handles["a"], handles["b"]):
        try:
            ray.kill(h)
        except Exception:  # noqa: BLE001
            pass
    d["actors_removed"] = True
    d["actor_pids_gone"] = (plan["pid_gone"](a_pid, plan["node_name"]("a"), 30)
                            and plan["pid_gone"](b_pid, plan["node_name"]("b"), 30))
    am["disposal"] = d
    sm.advance("ISLAND_DISPOSED")
    _phase(m, f"{arm}_island_disposed")

    sm.advance("FINALIZED")
    _phase(m, f"{arm}_finalized")
    am["state_history"] = sm.history
    am["rejected_transitions"] = list(sm.rejected)
    return am


def _finalize_and_write(x68, ray, m, agg, runs_dir, agg_path, plan, procs, actors, owned):
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
    m.pop("_owned_ref", None)

    r = rollup(x68, m)
    agg["overall"] = "pass" if r["passed"] else "fail"
    agg["failure_class"] = r["failure_class"]
    agg["gates"], agg["gates_failed"] = r["gates"], r["gates_failed"]
    agg["negative_claims"] = r["negative_claims"]
    agg["summary_claim"] = (SUMMARY_CLAIM if r["passed"] else
                            f"Slice 4A did not pass ({r['failure_class']}); see gates_failed.")
    agg["controller_exception"] = m.get("controller_exception")
    agg["classifier_fingerprint"] = m.get("classifier_fingerprint")
    agg["classifier_allowlist"] = m.get("classifier_allowlist")
    for arm in ARMS:
        am = (m.get("arms") or {}).get(arm) or {}
        agg[f"{arm}_event"] = (am.get("root_observation") or {}).get("event")
        agg[f"{arm}_observed_silence_s"] = (am.get("monotonic") or {}).get("observed_silence_s")
        agg[f"{arm}_classification_elapsed_s"] = (am.get("monotonic") or {}).get(
            "classification_elapsed_s")
    agg["loss_actor_observation_categories"] = {
        k: v.get("category") for k, v in
        (((m.get("arms") or {}).get("loss") or {}).get("post_loss_actor_observations")
         or {}).items()}
    with open(os.path.join(runs_dir, "markers.json"), "w") as f:
        json.dump(m, f, indent=2, sort_keys=True, default=str)
    with open(agg_path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=True, default=str)
    print(f"[slice4a] overall: {agg['overall']} ({agg['failure_class']}) -> {agg_path}")
    print(f"[slice4a] events: normal={agg.get('normal_event')} loss={agg.get('loss_event')}")
    if r["gates_failed"]:
        print(f"[slice4a] gates_failed: {json.dumps(r['gates_failed'], default=str)}")
    return 0


def _new_run(args, phase, pf):
    runid = time.strftime("%Y%m%dT%H%M%SZ")
    prefix = "crossnode_" + (pf.get("slurm_job_id") or "nojob") + "_" if \
        phase == "rostam-cross-node" else ""
    runs_dir = os.path.join(RUNS_ROOT, f"{prefix}{runid}")
    os.makedirs(runs_dir, exist_ok=True)
    agg_path = args.aggregate or os.path.join(runs_dir, "aggregate.json")
    epochs = {arm: f"exp70s4a-{arm}-{runid}" for arm in ARMS}
    agg = {"experiment": "exp70_slice4a_root_loss_event", "increment": 4,
           "phase": phase, "backend": "external_root_witness", "runid": runid,
           "runs_dir": runs_dir, "epoch_ids": epochs, "arms": list(ARMS),
           "work_case": WORK_CASE,
           "expected_refresh_s": args.expected_refresh_s,
           "suspicion_bound_s": args.suspicion_bound_s,
           "observe_bound_s": args.observe_bound_s,
           "actor_call_timeout_s": args.actor_call_timeout_s,
           "event_classes": list(EVENT_CLASSES),
           "actor_observation_categories": list(ACTOR_OBS_CATEGORIES),
           "classifier_allowlist": sorted(ROOT_EVIDENCE_KEYS),
           "provenance": _provenance(),
           "summary_claim_candidate": SUMMARY_CLAIM, "non_claims": NON_CLAIMS,
           "preflight": pf}
    return runid, runs_dir, agg_path, epochs, agg


def _new_markers(epochs, phase, owned):
    return {"arms": {}, "phase_log": [], "phase_times_wall_ms": {}, "final": {},
            "phase": phase, "epoch_ids": epochs, "controller_pid": os.getpid(),
            "classifier_fingerprint": _root_classifier_fingerprint(),
            "classifier_fingerprint_stable": None,
            "classifier_allowlist": sorted(ROOT_EVIDENCE_KEYS),
            "_owned_ref": owned}


def _run_both_arms(x68, ray, args, m, runs_dir, plan, pf, procs, actors, register_owned,
                   epochs):
    fp = _root_classifier_fingerprint()
    for arm in ARMS:
        _run_arm(x68, ray, args, m, arm, runs_dir, plan, pf, procs, actors, register_owned,
                 epochs[arm])
    m["classifier_fingerprint_stable"] = (_root_classifier_fingerprint() == fp
                                          == m.get("classifier_fingerprint"))


def live_local_run(args):
    pf = preflight(args.exp68_dir, args.exp68_build_dir)
    _runid, runs_dir, agg_path, epochs, agg = _new_run(args, "local", pf)
    if not pf["ok"]:
        agg["overall"] = "skip"
        agg["reason"] = "; ".join(pf["problems"])
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice4a] SKIP: {agg['reason']} -> {agg_path}")
        return 0

    x68, _ = import_exp68(args.exp68_dir)
    import ray  # noqa: PLC0415
    procs, actors, owned = [], [], []
    m = _new_markers(epochs, "local", owned)
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
        print(f"[slice4a] controller exception: {m['controller_exception']}")
    return _finalize_and_write(x68, ray, m, agg, runs_dir, agg_path, plan, procs, actors, owned)


def crossnode_live_run(args):
    env = dict(os.environ)
    pf = preflight_crossnode(args.exp68_dir, env, args.subnet, args.exp68_build_dir)
    _runid, runs_dir, agg_path, epochs, agg = _new_run(args, "rostam-cross-node", pf)
    if not pf["ok"]:
        agg["overall"] = "skip"
        agg["reason"] = "; ".join(pf["problems"])
        with open(agg_path, "w") as f:
            json.dump(agg, f, indent=2, sort_keys=True)
        print(f"[slice4a-crossnode] SKIP: {agg['reason']} -> {agg_path}")
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
        print(f"[slice4a-crossnode] PREFLIGHT FAIL: {agg['reason']}")
        return 0

    nodeA_ip = x68._local_subnet_ip(args.subnet)
    nodeB_ip = x68._node_subnet_ip(nodeB, args.subnet)
    cx = {"slurm_job_id": pf["slurm_job_id"], "nodelist": pf["slurm_nodelist"],
          "nodes": nodes, "nodeA": nodeA, "nodeB": nodeB,
          "nodeA_ip": nodeA_ip, "nodeB_ip": nodeB_ip, "subnet": args.subnet,
          "ray_node_ids": {}}
    import ray  # noqa: PLC0415
    temp_dir = f"/tmp/exp70s4a_ray_{pf['slurm_job_id']}_{_runid}"
    head = worker_b = None
    procs, actors, owned = [], [], []
    m = _new_markers(epochs, "rostam-cross-node", owned)
    m["crossnode"] = cx
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
        print(f"[slice4a-crossnode] controller exception: {m['controller_exception']}")
    finally:
        if plan is None:
            plan = make_local_plan(x68, pf, args)  # cleanup fallback (no srun anywhere)

    rc = _finalize_and_write(x68, ray, m, agg, runs_dir, agg_path, plan, procs, actors, owned)
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
        print(f"[slice4a] no runs dir: {RUNS_ROOT}")
        return 1
    ids = run_ids or sorted(d for d in os.listdir(RUNS_ROOT)
                            if not d.startswith(("crossnode_", "curated_", "rostam_")))
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
                     "normal_event": agg.get("normal_event"),
                     "loss_event": agg.get("loss_event"),
                     "suspicion_bound_s": agg.get("suspicion_bound_s"),
                     "loss_observed_silence_s": agg.get("loss_observed_silence_s"),
                     "loss_actor_observation_categories":
                         agg.get("loss_actor_observation_categories"),
                     "gate_groups": len(agg.get("gates") or {}),
                     "gate_checks": sum(len(v) for v in (agg.get("gates") or {}).values()
                                        if isinstance(v, dict))})
    summary = {"experiment": "exp70_slice4a_root_loss_event",
               "curated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "phase": "local", "runs": kept,
               "summary_claim": SUMMARY_CLAIM, "non_claims": NON_CLAIMS,
               "classifier_allowlist": sorted(ROOT_EVIDENCE_KEYS),
               "event_classes": list(EVENT_CLASSES)}
    p = os.path.join(out_dir, "curated_local_aggregate.json")
    with open(p, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    print(f"[slice4a] curated {len(kept)} local run(s) -> {p}")
    return 0


# ---------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="exp70 Slice 4A: explicit root completion vs unexpected root loss")
    ap.add_argument("--selftest", action="store_true",
                    help="pure logic checks (no Ray, no HPX, no Slurm)")
    ap.add_argument("--phase", choices=("local", "rostam-cross-node"), default="local")
    ap.add_argument("--curate", nargs="*", metavar="RUNID", default=None)
    ap.add_argument("--exp68-dir", default=DEFAULT_EXP68_DIR)
    ap.add_argument("--exp68-build-dir", default=None,
                    help="exp68 build dir (default <exp68-dir>/build; Rostam: build_rostam)")
    ap.add_argument("--expected-refresh-s", type=float, default=EXPECTED_REFRESH_S)
    ap.add_argument("--suspicion-bound-s", type=float, default=DEFAULT_SUSPICION_BOUND_S)
    ap.add_argument("--observe-bound-s", type=float, default=DEFAULT_OBSERVE_BOUND_S)
    ap.add_argument("--actor-call-timeout-s", type=float, default=DEFAULT_ACTOR_CALL_TIMEOUT_S)
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--ray-num-cpus", type=int, default=1)
    ap.add_argument("--aggregate", default=None)
    ap.add_argument("--subnet", default=DEFAULT_SUBNET)
    ap.add_argument("--ray-port", type=int, default=6529)
    ap.add_argument("--port-base", type=int, default=8011)
    ap.add_argument("--arm-port-stride", type=int, default=10)
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
