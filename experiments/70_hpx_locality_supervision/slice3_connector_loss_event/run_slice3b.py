#!/usr/bin/env python3
"""exp70 Slice 3B -- CURRENT upstream HPX supervision + hpx::force_disconnect against the
silent crash of a late-connected connector locality (HPX #7390 / #7441 / merged PR #7447).

QUESTION: can current upstream HPX autonomously classify and fence the silent crash of a
late-connected connector locality, after which the surviving root can use
hpx::force_disconnect to clean up that failed locality and permit a replacement connector to
join successfully?

Slice 3A (../slice3_connector_loss_event/run_slice3.py) closed the APPLICATION-CONTRACT half of
this question: a supervisor-computed, injection-blind classifier over EXTERNAL evidence
(PID/actor-call observations), because at the time nothing in the runtime/AGAS/parcelport
independently classified a departure. That assessment is now DATED, not wrong: Slice 3B tests
whether the supervision_dispatch component that has since landed upstream
(components/supervision_dispatch, hpx::supervision::init/discover_and_join/check_admission,
hpx::force_disconnect) closes that gap NATIVELY. Slice 3A's evidence and claim fence are
UNCHANGED and are not superseded by this file -- see native_backend_gap_matrix.md for the
before/after.

TOPOLOGY (Slice 3, reused IN PLACE and unmodified): one separately supervised, WORK-FREE
standalone root locality (native/root_supervised.cpp, exp68_peer.cpp's role) plus two (later
three) Ray actors, each hosting a connect-mode HPX locality IN-PROCESS via
native/connector_ext.cpp (the exp66/67/68 mechanism, reused unchanged).

THREE SEPARATE RESPONSIBILITIES kept observably distinct (do not collapse into one boolean):
  1. HPX runtime/supervision detects+classifies the silent connector crash
     (runtime_failure_classification_observed, application_failed_event_publish_count).
  2. supervision_dispatch fences the failed incarnation (stale_incarnation_fenced, via
     hpx::supervision::check_admission() -- a pure local latch read, not the templated
     dispatch_work<Action>() path, which is recorded only as a non-gating diagnostic; see
     native/root_supervised.cpp's file comment for why).
  3. Root explicitly invokes hpx::force_disconnect() as the recovery action, ONLY after step 2
     is observed (force_disconnect_invoked/_completed/_effect_observed). This is NEVER
     automatic: upstream does not wire fencing to force_disconnect for you (per hkaiser's own
     2026-08-18 status comment on issue #7390), and this experiment must not imply otherwise.

SINGLE SCENARIO (no graceful arm -- Slice 3A already owns that comparison): connector B joins
late, does verified work, is hard-killed (SIGKILL of its Ray-actor-hosted OS process -- no
hpx::disconnect(), no graceful runtime shutdown, no application-authored event::failed, no
cleanup callback), root observes native classification+fencing, invokes force_disconnect,
then a replacement connector C joins and is verified to be a distinct incarnation.

BUILD REQUIREMENT: this slice links against components/supervision_dispatch, which upstream
gates behind -DHPX_WITH_SUPERVISION=ON (also enables HPX_HAVE_FORCE_DISCONNECT). See
native/CMakeLists.txt, which refuses to configure without it -- this driver's preflight() checks
the same thing from the Python side (binary/module presence) rather than assuming.

CLAIM FENCE: mechanism/validation evidence for the CURRENT upstream supervision_dispatch +
force_disconnect API (HPX #7390/#7441/PR #7447) only. NOT autonomous recovery (root's
force_disconnect call is explicit, not triggered by HPX), NOT a claim that fencing and
force_disconnect are wired together upstream, NOT a claim about non-late-connecting or
console/root loss (see Slice 4B), NOT a performance claim, NOT production API.

Usage:
  python run_slice3b.py --selftest        # pure logic/schema checks (no Ray, no HPX)
  python run_slice3b.py --phase local     # live local run (requires an HPX build with
                                           # HPX_WITH_SUPERVISION=ON; not available on the
                                           # project's current macOS dev build -- see the exp70
                                           # write-up for the Rostam rebuild/run commands)
  python run_slice3b.py --curate RUNID    # curate one accepted local run (no processes)
"""

import argparse
import json
import os
import signal
import socket
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
NATIVE_DIR = os.path.join(HERE, "native")
DEFAULT_EXP68_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "68_vocab_sharded_topk"))
RUNS_ROOT = os.path.join(HERE, "_exp70_slice3b_runs")

ROOT_BINARY = "exp70_slice3b_root"
EXT_MODULE = "exp70_slice3b_ext"

# The full Slice 3B gate-field list (exp70 continuation spec, Phase 5). Every field must be
# present and boolean-true for a PASS; nothing here is collapsed into one aggregate boolean.
GATE_FIELDS = [
    "connector_late_join_proven",
    "pre_crash_work_ok",
    "hard_crash_used",
    "graceful_disconnect_used_is_false",
    "application_failed_event_publish_count_is_zero",
    "runtime_failure_classification_observed",
    "failed_epoch_or_incarnation_identified",
    "stale_incarnation_fenced",
    "fenced_outcome_is_specific",
    "force_disconnect_invoked",
    "force_disconnect_completed",
    "force_disconnect_effect_observed",
    "replacement_joined",
    "replacement_incarnation_distinct",
    "replacement_work_ok",
    "stale_incarnation_not_confused_with_replacement",
]

FAILURE_CLASSES = [
    "preflight_missing_artifacts", "startup_failed", "inprocess_proof_failed",
    "eligibility_not_proven", "work_failed", "crash_injection_invalid",
    "classification_not_observed", "fencing_not_observed", "force_disconnect_not_invoked",
    "force_disconnect_effect_not_observed", "replacement_failed", "gate_failed",
    "cleanup_incomplete", "invalid_instrumentation",
]

SUMMARY_CLAIM = (
    "Current upstream HPX (components/supervision_dispatch + hpx::force_disconnect, HPX "
    "#7390/#7441/PR #7447) runtime-classified and fenced the silent crash of a late-connected, "
    "Ray-actor-hosted connector locality without the application ever publishing its own "
    "failure event, after which the surviving root explicitly invoked hpx::force_disconnect to "
    "clean up the failed locality's AGAS/connection-cache state and admit a replacement "
    "connector as a distinct incarnation.")
NON_CLAIMS = (
    "This does not demonstrate autonomous/automatic recovery: force_disconnect is an explicit "
    "root-side call made only after fencing was observed, never triggered by HPX itself. It "
    "does not demonstrate fencing and force_disconnect being wired together upstream, does not "
    "demonstrate console/root loss handling (see Slice 4B, which remains blocked), does not "
    "supersede Slice 3A's application-contract evidence, and carries no performance, Ray-vs-HPX, "
    "or general-fabric claim.")


# ---------------------------------------------------------------------------------------
# exp68 import (shared low-level process/file/port helpers only -- no exp68 workload/actor
# class reuse; Slice 3B has its own extension and its own actor wrapper below)
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
    required = ["find_free_port", "_popen", "_kill_group", "_wait_for_file", "_read_json",
                "_wait_proc", "_exit_path", "pid_alive", "wait_pid_gone", "peer_orphans",
                "actor_endpoints", "HPX_HOST"]
    missing = [n for n in required if not hasattr(x68, n)]
    if missing:
        return None, f"exp68 module missing required attributes: {missing}"
    return x68, None


def preflight(exp68_dir, build_dir=None):
    """Pure checks; never raises. build_dir defaults to native/build (Mac/Rostam-local layout)."""
    out = {"ok": False, "problems": []}
    x68, err = import_exp68(exp68_dir)
    if err:
        out["problems"].append(err)
        return out
    build_dir = build_dir or os.path.join(NATIVE_DIR, "build")
    root_bin = os.path.join(build_dir, ROOT_BINARY)
    ext_so = next((fn for fn in (os.listdir(build_dir) if os.path.isdir(build_dir) else [])
                   if fn.startswith(EXT_MODULE) and fn.endswith(".so")), None)
    if not os.path.exists(root_bin):
        out["problems"].append(
            f"exp70_slice3b_root binary missing: {root_bin} (build native/CMakeLists.txt "
            "against an HPX with -DHPX_WITH_SUPERVISION=ON first)")
    if not ext_so:
        out["problems"].append(f"exp70_slice3b_ext .so missing under {build_dir}")
    try:
        import ray  # noqa: F401,PLC0415
        out["ray_importable"] = True
    except Exception as ex:  # noqa: BLE001
        out["ray_importable"] = False
        out["problems"].append(f"ray unavailable: {type(ex).__name__}: {ex}")
    out["build_dir"], out["root_bin"] = build_dir, root_bin
    out["ext_so"] = os.path.join(build_dir, ext_so) if ext_so else None
    out["ok"] = not out["problems"]
    return out


def root_cmd(peer_bin, boot, ports, x68):
    return [peer_bin, "--bootstrap", boot,
            f"--hpx:agas={x68.HPX_HOST}:{ports['root']}",
            f"--hpx:hpx={x68.HPX_HOST}:{ports['root']}",
            "--hpx:expect-connecting-localities", "--hpx:threads=2", "--hpx:bind=none",
            "--hpx:ignore-batch-env"]


def write_roles(boot, roles):
    tmp = os.path.join(boot, "native.roles.json.tmp")
    with open(tmp, "w") as f:
        json.dump(roles, f)
    os.replace(tmp, os.path.join(boot, "native.roles.json"))


def _require_marker(x68, boot, name, timeout, root_proc, label):
    """Wait for a root.* marker and fail fast with a clear, specific message if it never
    appears -- rather than silently continuing with an empty dict, which (job 185467's first
    hardware attempt) let the driver plow through several more no-op timeouts before failing
    far downstream with a misleading, unrelated-looking error. Surfaces root.finish_note (root's
    own early-exit reason, if any) so the real cause is visible immediately."""
    path = os.path.join(boot, name)
    x68._wait_for_file(path, timeout, procs=[root_proc])
    doc = x68._read_json(path)
    if doc is None:
        finish_note = x68._read_json(os.path.join(boot, "root.finish_note"))
        raise RuntimeError(
            f"[{label}] {name} never appeared within {timeout}s"
            + (f" -- root.finish_note: {finish_note}" if finish_note else ""))
    return doc


# ---------------------------------------------------------------------------------------
# Ray actor wrapper for native/connector_ext.cpp
# ---------------------------------------------------------------------------------------

def build_actor_class(ray_mod):
    @ray_mod.remote
    class Slice3bActor:
        def __init__(self, build_dir, hpx_threads, endpoints):
            import sys as _sys
            _sys.path.insert(0, build_dir)
            self._build_dir = build_dir
            self._threads = hpx_threads
            self._endpoints = endpoints
            self._ext = None

        def ray_placement(self):
            import os as _os, socket as _socket
            try:
                import ray as _ray
                ctx = _ray.get_runtime_context()
                nid, aid = ctx.get_node_id(), ctx.get_actor_id()
            except Exception:  # noqa: BLE001
                nid, aid = None, None
            return {"node_id": nid, "actor_id": aid, "hostname": _socket.gethostname(),
                    "pid": _os.getpid()}

        def load_identity(self):
            import importlib, os as _os
            e = importlib.import_module(EXT_MODULE)
            self._ext = e
            return {"pid": e.pid(), "os_getpid": _os.getpid(), "hostname": e.hostname(),
                    "hpx_version_info": dict(e.hpx_version_info()),
                    "experiment": getattr(e, "__experiment__", None)}

        def start_hpx(self):
            try:
                self._ext.start_connect(self._threads, list(self._endpoints))
                return {"started": True, "locality_id": int(self._ext.locality_id()),
                        "membership": int(self._ext.membership_count()), "pid": self._ext.pid()}
            except Exception as ex:  # noqa: BLE001
                return {"started": False, "error": f"{type(ex).__name__}: {ex}"}

        def supervision_init(self, discovery_timeout_ms=5000):
            try:
                return dict(self._ext.supervision_init(int(discovery_timeout_ms)))
            except Exception as ex:  # noqa: BLE001
                return {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

        def supervision_discover_probe(self, discovery_timeout_ms=5000):
            """Diagnostic only: this connector's own discover_and_join(), not gated."""
            try:
                return dict(self._ext.supervision_discover_probe(int(discovery_timeout_ms)))
            except Exception as ex:  # noqa: BLE001
                return {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

        def probe_locality(self, target_locality, x, bound_s=10):
            try:
                return dict(self._ext.probe_locality(int(target_locality), int(x), int(bound_s)))
            except Exception as ex:  # noqa: BLE001
                return {"ok": False, "found": False, "error": f"{type(ex).__name__}: {ex}"}

        def health(self):
            try:
                return {"ok": True, "pid": self._ext.pid(),
                        "membership": int(self._ext.membership_count()),
                        "locality_id": int(self._ext.locality_id())}
            except Exception as ex:  # noqa: BLE001
                return {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

        def stop_hpx(self):
            try:
                return {"rc": int(self._ext.stop_disconnect()), "error": None}
            except Exception as ex:  # noqa: BLE001
                return {"rc": -1, "error": f"{type(ex).__name__}: {ex}"}

    return Slice3bActor


def _ray_get(ray_mod, ref, timeout, what):
    try:
        return ray_mod.get(ref, timeout=timeout)
    except Exception as ex:  # noqa: BLE001
        return {"ok": False, "started": False, "error": f"{what}: {type(ex).__name__}: {ex}"}


# ---------------------------------------------------------------------------------------
# Gate evaluation (pure function over the collected marker mapping)
# ---------------------------------------------------------------------------------------

def eval_gates(m):
    """Maps the collected marker/result mapping onto the exact GATE_FIELDS list. Every field is
    independently computed; none are folded together."""
    started = m.get("root_started") or {}
    joined = m.get("root_joined") or {}
    work = m.get("root_work_verified") or {}
    fencing = m.get("root_fencing_observed") or {}
    fd = m.get("root_force_disconnect") or {}
    fd_effect = m.get("root_force_disconnect_effect") or {}
    result = m.get("root_result") or {}
    departure = m.get("departure") or {}

    g = {}
    g["connector_late_join_proven"] = joined.get("b_is_connecting_before_crash") is True
    g["pre_crash_work_ok"] = bool(
        (work.get("a") or {}).get("ok") and (work.get("b") or {}).get("ok"))
    g["hard_crash_used"] = departure.get("signal") == "SIGKILL"
    g["graceful_disconnect_used_is_false"] = departure.get("graceful_disconnect_used") is False
    g["application_failed_event_publish_count_is_zero"] = (
        work.get("application_failed_event_publish_count") == 0
        and fencing.get("application_failed_event_publish_count") == 0
        and result.get("application_failed_event_publish_count") == 0)
    g["runtime_failure_classification_observed"] = (
        fencing.get("runtime_failure_classification_observed") is True)
    g["failed_epoch_or_incarnation_identified"] = (
        result.get("failed_epoch_or_incarnation_identified") is True)
    g["stale_incarnation_fenced"] = fencing.get("stale_incarnation_fenced") is True
    g["fenced_outcome_is_specific"] = fencing.get("fenced_outcome_is_specific") is True
    g["force_disconnect_invoked"] = fd.get("force_disconnect_invoked") is True
    g["force_disconnect_completed"] = fd.get("force_disconnect_completed") is True
    g["force_disconnect_effect_observed"] = (
        fd_effect.get("force_disconnect_effect_observed") is True
        and (fd_effect.get("agas_resolve_fails_after") is True
             or fd_effect.get("membership_shrank") is True
             or fd_effect.get("post_force_disconnect_dispatch_failed") is True))
    g["replacement_joined"] = result.get("replacement_joined") is True
    g["replacement_incarnation_distinct"] = result.get("replacement_incarnation_distinct") is True
    g["replacement_work_ok"] = result.get("replacement_work_ok") is True
    g["stale_incarnation_not_confused_with_replacement"] = (
        result.get("stale_incarnation_not_confused_with_replacement") is True)
    return g


def failure_class_for(gates, m):
    if not all(gates.get(k) for k in ("connector_late_join_proven",)):
        return "eligibility_not_proven"
    if not gates.get("pre_crash_work_ok"):
        return "work_failed"
    if not (gates.get("hard_crash_used") and gates.get("graceful_disconnect_used_is_false")):
        return "crash_injection_invalid"
    if not (gates.get("runtime_failure_classification_observed")
            and gates.get("application_failed_event_publish_count_is_zero")):
        return "classification_not_observed"
    if not (gates.get("stale_incarnation_fenced") and gates.get("fenced_outcome_is_specific")):
        return "fencing_not_observed"
    if not (gates.get("force_disconnect_invoked") and gates.get("force_disconnect_completed")):
        return "force_disconnect_not_invoked"
    if not gates.get("force_disconnect_effect_observed"):
        return "force_disconnect_effect_not_observed"
    if not (gates.get("replacement_joined") and gates.get("replacement_incarnation_distinct")
            and gates.get("replacement_work_ok")
            and gates.get("stale_incarnation_not_confused_with_replacement")):
        return "replacement_failed"
    if not all(gates.values()):
        return "gate_failed"
    return "pass"


def rollup(m):
    gates = eval_gates(m)
    fc = failure_class_for(gates, m)
    passed = (fc == "pass") and not m.get("controller_exception")
    if m.get("controller_exception") and fc == "pass":
        fc = "invalid_instrumentation"
    return {"passed": passed, "failure_class": fc if not passed else "pass", "gates": gates,
            "gates_failed": sorted(k for k, v in gates.items() if not v),
            "negative_claims": {
                "autonomous_recovery_claimed": False, "fencing_force_disconnect_wired_claimed":
                    False, "console_root_loss_claimed": False, "performance_claimed": False,
                "speedup_computed": False, "ratio_reported": False}}


# ---------------------------------------------------------------------------------------
# Selftest: pure logic/schema checks, no processes (mirrors slice2/3/4 convention)
# ---------------------------------------------------------------------------------------

def _synthetic_pass_markers():
    return {
        "root_started": {"started": True, "hpx_have_supervision": True,
                          "hpx_have_force_disconnect": True},
        "root_joined": {"b_is_connecting_before_crash": True,
                         "a_is_connecting_before_crash": True},
        "root_work_verified": {"a": {"ok": True}, "b": {"ok": True},
                                "application_failed_event_publish_count": 0},
        "departure": {"signal": "SIGKILL", "graceful_disconnect_used": False},
        "root_fencing_observed": {"runtime_failure_classification_observed": True,
                                   "stale_incarnation_fenced": True,
                                   "fenced_outcome_is_specific": True,
                                   "application_failed_event_publish_count": 0},
        "root_force_disconnect": {"force_disconnect_invoked": True,
                                   "force_disconnect_completed": True},
        "root_force_disconnect_effect": {"force_disconnect_effect_observed": True,
                                          "agas_resolve_fails_after": True,
                                          "membership_shrank": True,
                                          "post_force_disconnect_dispatch_failed": True},
        "root_result": {"application_failed_event_publish_count": 0,
                         "failed_epoch_or_incarnation_identified": True,
                         "replacement_joined": True, "replacement_incarnation_distinct": True,
                         "replacement_work_ok": True,
                         "stale_incarnation_not_confused_with_replacement": True},
    }


def selftest():
    failures = []

    def check(name, cond):
        if not cond:
            failures.append(name)

    # 1. A fully-synthetic PASS marker set must roll up to passed=True with every gate true.
    m = _synthetic_pass_markers()
    r = rollup(m)
    check("synthetic_pass_rolls_up", r["passed"] is True)
    check("synthetic_pass_all_gates_true", all(r["gates"].values()))
    check("synthetic_pass_no_failed_gates", r["gates_failed"] == [])
    check("synthetic_pass_failure_class", r["failure_class"] == "pass")

    # 2. Flipping ANY single required signal must flip that gate AND the overall pass -- proves
    #    gates are independently wired, not vacuously true.
    flips = [
        ("root_joined", "b_is_connecting_before_crash", False, "connector_late_join_proven"),
        ("root_work_verified", "application_failed_event_publish_count", 1,
         "application_failed_event_publish_count_is_zero"),
        ("root_fencing_observed", "stale_incarnation_fenced", False, "stale_incarnation_fenced"),
        ("root_force_disconnect", "force_disconnect_completed", False,
         "force_disconnect_completed"),
        ("root_result", "replacement_incarnation_distinct", False,
         "replacement_incarnation_distinct"),
        ("root_result", "stale_incarnation_not_confused_with_replacement", False,
         "stale_incarnation_not_confused_with_replacement"),
    ]
    for section, key, bad_value, gate_name in flips:
        m2 = _synthetic_pass_markers()
        m2[section] = dict(m2[section])
        m2[section][key] = bad_value
        r2 = rollup(m2)
        check(f"flip_{section}.{key}_breaks_{gate_name}", r2["gates"][gate_name] is False)
        check(f"flip_{section}.{key}_breaks_pass", r2["passed"] is False)

    # 3. graceful_disconnect_used must independently gate even if everything else looks fine
    #    (guards against a future accidental soft/graceful teardown of b being miscounted as
    #    the required hard-crash injection).
    m3 = _synthetic_pass_markers()
    m3["departure"] = {"signal": None, "graceful_disconnect_used": True}
    r3 = rollup(m3)
    check("graceful_departure_fails_hard_crash_gate", r3["gates"]["hard_crash_used"] is False)
    check("graceful_departure_fails_disconnect_gate",
          r3["gates"]["graceful_disconnect_used_is_false"] is False)
    check("graceful_departure_fails_overall", r3["passed"] is False)

    # 4. failure_class_for must name a specific, ordered class rather than a generic "gate_failed"
    #    for the leading missing signal.
    m4 = _synthetic_pass_markers()
    m4["root_fencing_observed"] = {"runtime_failure_classification_observed": False,
                                    "stale_incarnation_fenced": False,
                                    "fenced_outcome_is_specific": False,
                                    "application_failed_event_publish_count": 0}
    r4 = rollup(m4)
    check("missing_classification_names_specific_failure_class",
          r4["failure_class"] == "classification_not_observed")

    # 5. GATE_FIELDS and eval_gates() must stay in lockstep (no silently-dropped/renamed gate).
    check("gate_fields_match_eval_gates", sorted(GATE_FIELDS) == sorted(eval_gates(m).keys()))

    # 6. roles.json round-trip (pure file I/O, no HPX/Ray needed).
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        write_roles(td, {"a": 1, "b": 2})
        with open(os.path.join(td, "native.roles.json")) as f:
            rt = json.load(f)
        check("roles_json_roundtrip", rt == {"a": 1, "b": 2})

    ok = not failures
    print(json.dumps({"ok": ok, "failures": failures, "checks_run": True}, indent=2))
    return 0 if ok else 1


# ---------------------------------------------------------------------------------------
# Live local run
# ---------------------------------------------------------------------------------------

def _provenance():
    out = {"hostname": socket.gethostname(), "python": sys.version.split()[0]}
    try:
        import ray  # noqa: PLC0415
        out["ray_version"] = ray.__version__
    except Exception:  # noqa: BLE001
        out["ray_version"] = None
    return out


def live_local_run(args):
    pf = preflight(args.exp68_dir, args.build_dir)
    if not pf["ok"]:
        print(json.dumps({"passed": False, "failure_class": "preflight_missing_artifacts",
                          "problems": pf["problems"]}, indent=2))
        return 1
    x68, _ = import_exp68(args.exp68_dir)

    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    runs_dir = os.path.join(RUNS_ROOT, run_id)
    boot = os.path.join(runs_dir, "island")
    os.makedirs(boot, exist_ok=True)

    m = {"controller_hostname": socket.gethostname(), "provenance": _provenance(),
         "run_id": run_id, "bootdir": boot}
    procs, actors, owned = [], [], []
    root_proc = None

    def register_owned(label, pid):
        if pid:
            owned.append({"label": label, "pid": pid, "node": None})

    try:
        import ray
        ray.init(ignore_reinit_error=True, include_dashboard=False,
                 num_cpus=args.ray_num_cpus * 3 + 1)
        Slice3bActor = build_actor_class(ray)

        ports = {"root": x68.find_free_port(), "a": x68.find_free_port(),
                 "b": x68.find_free_port()}
        rcmd = root_cmd(pf["root_bin"], boot, ports, x68)
        root_proc, rlog = x68._popen(rcmd, boot, os.path.join(boot, "root.log"))
        procs.append((root_proc, rlog))
        x68._wait_for_file(os.path.join(boot, "root.started"), 30, procs=[root_proc])
        m["root_started"] = x68._read_json(os.path.join(boot, "root.started")) or {}
        if m["root_started"].get("pid"):
            register_owned("island_root", m["root_started"]["pid"])
        if not m["root_started"].get("started"):
            raise RuntimeError(f"root failed to start: {m['root_started']}")

        handles, roles = {}, {}
        for k in ("a", "b"):
            ep = x68.actor_endpoints(ports["root"], ports[k])
            h = Slice3bActor.options(num_cpus=args.ray_num_cpus, max_restarts=0).remote(
                pf["build_dir"], args.hpx_threads, ep)
            actors.append(h)
            handles[k] = h
            ident = _ray_get(ray, h.load_identity.remote(), 60, f"{k}_identity")
            m[f"{k}_identity"] = ident
            start = _ray_get(ray, h.start_hpx.remote(), 60, f"{k}_start")
            m[f"{k}_start"] = start
            if not start.get("started"):
                raise RuntimeError(f"[{k}] start_hpx failed: {start}")
            roles[k] = start["locality_id"]
            if ident.get("pid"):
                register_owned(f"island_actor_{k}", ident["pid"])
            sup = _ray_get(ray, h.supervision_init.remote(args.discovery_timeout_ms), 30,
                           f"{k}_supervision_init")
            m[f"{k}_supervision_init"] = sup
            if not sup.get("ok"):
                raise RuntimeError(f"[{k}] supervision_init failed: {sup}")

        write_roles(boot, roles)

        # Diagnostic only, not gated: connector A's OWN discover_and_join() (locality 1
        # discovering root/locality 0 and B/locality 2), mirroring the direction upstream's own
        # late_component_worker.cpp exercises. Isolates whether a discovery gap is
        # root->connector-specific or a general cross-locality symptom.
        m["a_discover_probe"] = _ray_get(
            ray, handles["a"].supervision_discover_probe.remote(args.discovery_timeout_ms), 40,
            "a_discover_probe")

        m["root_joined"] = _require_marker(x68, boot, "root.joined", args.join_timeout_s + 10,
                                           root_proc, "root_joined")

        m["root_work_verified"] = _require_marker(x68, boot, "root.work_verified", 30,
                                                   root_proc, "root_work_verified")

        # ---- hard-crash injection: SIGKILL b's Ray-actor-hosted OS process --------------
        b_pid = (m.get("b_identity") or {}).get("pid")
        b_os_getpid = (m.get("b_identity") or {}).get("os_getpid")
        if b_pid is None or b_pid != b_os_getpid:
            raise RuntimeError(f"b pid cross-check failed: identity={m.get('b_identity')}")
        t_kill = time.monotonic()
        os.kill(b_pid, signal.SIGKILL)
        pid_gone = x68.wait_pid_gone(b_pid, 30)
        m["departure"] = {"connector": "b", "pid": b_pid, "signal": "SIGKILL",
                          "signal_sent": True, "graceful_disconnect_used": False,
                          "pid_gone": pid_gone, "t_mono_killed": t_kill,
                          "t_unix_killed": time.time()}
        if not pid_gone:
            raise RuntimeError("b process did not actually terminate after SIGKILL")

        m["root_fencing_observed"] = _require_marker(
            x68, boot, "root.fencing_observed", args.fence_wait_timeout_s + 20, root_proc,
            "root_fencing_observed")

        m["root_force_disconnect"] = _require_marker(
            x68, boot, "root.force_disconnect", 20, root_proc, "root_force_disconnect")

        m["root_force_disconnect_effect"] = _require_marker(
            x68, boot, "root.force_disconnect_effect", 15, root_proc,
            "root_force_disconnect_effect")

        # bonus third-party check: A independently probes B's OLD locality post-force_disconnect
        b_old_locality = roles.get("b")
        if b_old_locality is not None and "a" in handles:
            m["a_probe_of_departed_b_post_force_disconnect"] = _ray_get(
                ray, handles["a"].probe_locality.remote(b_old_locality, 88, 5), 15,
                "a_probe_b_post_fd")

        _require_marker(x68, boot, "root.ready_for_replacement", 15, root_proc,
                        "root_ready_for_replacement")

        # ---- replacement connector c --------------------------------------------------
        ep_c = x68.actor_endpoints(ports["root"], x68.find_free_port())
        hc = Slice3bActor.options(num_cpus=args.ray_num_cpus, max_restarts=0).remote(
            pf["build_dir"], args.hpx_threads, ep_c)
        actors.append(hc)
        handles["c"] = hc
        ident_c = _ray_get(ray, hc.load_identity.remote(), 60, "c_identity")
        m["c_identity"] = ident_c
        start_c = _ray_get(ray, hc.start_hpx.remote(), 60, "c_start")
        m["c_start"] = start_c
        if not start_c.get("started"):
            raise RuntimeError(f"[c] start_hpx failed: {start_c}")
        roles["c"] = start_c["locality_id"]
        if ident_c.get("pid"):
            register_owned("island_actor_c", ident_c["pid"])
        sup_c = _ray_get(ray, hc.supervision_init.remote(args.discovery_timeout_ms), 30,
                         "c_supervision_init")
        m["c_supervision_init"] = sup_c
        if not sup_c.get("ok"):
            raise RuntimeError(f"[c] supervision_init failed: {sup_c}")
        write_roles(boot, roles)

        m["root_result"] = _require_marker(x68, boot, "root_result.json",
                                           args.replacement_timeout_s + 20, root_proc,
                                           "root_result")

        exited, rc, killed = x68._wait_proc(root_proc, time.time() + 30)
        m["root_exit_path"] = x68._exit_path(exited, rc, killed)

    except Exception as ex:  # noqa: BLE001
        m["controller_exception"] = f"{type(ex).__name__}: {ex}"
        # Best-effort, non-raising: root.discover_diag is a diagnostic-only marker (see
        # root_supervised.cpp) that may exist even when root.joined itself never appeared,
        # capturing exactly what discover_and_join() found for post-mortem analysis.
        diag = x68._read_json(os.path.join(boot, "root.discover_diag")) if x68 else None
        if diag is not None:
            m["root_discover_diag"] = diag
    finally:
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
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass

    r = rollup(m)
    agg = {**m, **r, "gate_fields": GATE_FIELDS, "summary_claim": SUMMARY_CLAIM,
           "non_claims": NON_CLAIMS, "owned_processes": owned}
    agg_path = os.path.join(runs_dir, "aggregate.json")
    with open(agg_path, "w") as f:
        json.dump(agg, f, indent=2, sort_keys=True, default=str)
    print(json.dumps({"run_id": run_id, "passed": r["passed"],
                      "failure_class": r["failure_class"], "gates_failed": r["gates_failed"],
                      "aggregate": agg_path}, indent=2))
    return 0 if r["passed"] else 1


# ---------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--phase", choices=["local"], default=None)
    ap.add_argument("--exp68-dir", default=DEFAULT_EXP68_DIR)
    ap.add_argument("--build-dir", default=None,
                    help="native/ build dir (default native/build)")
    ap.add_argument("--ray-num-cpus", type=int, default=1)
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--discovery-timeout-ms", type=int, default=5000)
    ap.add_argument("--join-timeout-s", type=int, default=30)
    ap.add_argument("--fence-wait-timeout-s", type=int, default=60)
    ap.add_argument("--replacement-timeout-s", type=int, default=30)
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if args.phase == "local":
        return live_local_run(args)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
