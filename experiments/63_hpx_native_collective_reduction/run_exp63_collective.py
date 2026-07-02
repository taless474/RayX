"""exp63 -- HPX-native collective/tree reduction at the same Python caller boundary. SLICE 0.

WHY exp63 EXISTS (from exp62's two caveats):
  1. exp62's cross-node HPX fan-in used `root_flat_gather_poll`: a root flat-gather of the leaf
     futures with a bounded is_ready poll watchdog. It is PROVEN cross-node (job 158817) but it is an
     interim, non-HPX-native composition. The native `when_all(...).then(reduce)` spike (job 158814)
     was mathematically correct but the composed future STALLED to the dispatch timeout cross-node --
     a parcelport background-progress / passive-future-readiness problem, not a math problem.
  2. exp62's payload was closed-int64 only: one scalar per leaf, QD1 outer, no payload-size /
     serialization evidence.

HPX-EXPERT REVIEW OUTCOME (direction for exp63, do not regress):
  * DO NOT start with `hpx::collectives::reduce`. Collectives ride the SAME future + TCP-parcelport
    substrate as the stalled `when_all().then()` spike, so they may reproduce the same passive-wait
    stall, and they are MORE fragile (participant membership / generation / no timeout / poisoning).
  * The real variable to fix first is PARCELPORT BACKGROUND PROGRESS, not the composition primitive.
  * Order: Slice 1 progress root-cause micro-slice -> Slice 2a retest existing native modes if
    progress is fixable -> Slice 2b tree_of_partials as the likely-robust path -> collectives LATER,
    only after progress + membership + timeout design is understood -> payload LATER.

SCOPE / FENCES (Slice 0):
  * PURE PYTHON. No C++, no CMake, no pybind, no HPX, no Ray, no Rostam, no runtime imports at load.
  * This module holds the closed-int64 scalar oracle, the vector-sum and top-k oracle STUBS (not
    wired to any real execution), and the fail-closed provenance / gate builders the later runtime
    slices will reuse: composition provenance, progress-diagnosis provenance, payload provenance,
    oracle correctness, and distribution witness.
  * It computes NOTHING about Ray-vs-HPX: no speedup, no ratio, no arm differencing, no placement-band
    differencing. Those four fences are hard-locked False here and stay False downstream. No artifact
    sets same_axis_comparison True in Slice 0.

This is experiment-only measurement scaffolding. It is NOT shipped rayx.runtime API, NOT distributed
RayX API, NOT an object store / ObjectRef, NOT arbitrary remote Python execution, NOT Ray Serve, NOT
real inference, and NOT a Ray replacement.

Conceptual workload the later slices wire to real runtimes:
    collective_reduce(x, N) -> payload
  One outer blocking Python call fans N leaves across >=2 remote localities and reduces their partials
  to one payload with an HPX-NATIVE composition (no application-level success-path poll). Slice 0 only
  models the oracle + provenance + gates; no reduction runs here.
"""

import argparse
import json
import os
import sys
import time

EXPERIMENT_ID = "exp63"
LEAF_XOR = 0x52415958  # "RAYX" -- shared closed-int64 constant with exp61/exp62 oracles.
REDUCE_OP = "sum_int64"

_MASK64 = (1 << 64) - 1


# ---------------------------------------------------------------------------
# int64 / u64 wrapping. The closed value model is int64 (scalar path).
# ---------------------------------------------------------------------------

def u64(v):
    return int(v) & _MASK64


def i64(v):
    v = int(v) & _MASK64
    return v - (1 << 64) if v >= (1 << 63) else v


# ---------------------------------------------------------------------------
# Scalar oracle (closed int64). Placement-INDEPENDENT by construction: depends only on x and the
# leaf index i, never on which locality runs the leaf. That is what lets a separate witness prove
# distribution without the oracle "knowing" placement.
# ---------------------------------------------------------------------------

def leaf_value(x, i):
    """Per-leaf scalar oracle: (x ^ 0x52415958) + (i << 1), wrapped to int64."""
    return i64((int(x) ^ LEAF_XOR) + (int(i) << 1))


def composite_oracle(x, n):
    """Composite scalar oracle: int64 sum of leaf_value(x, i) for i in 0..n-1 (order-independent)."""
    if n < 0:
        raise ValueError("n must be >= 0")
    total = 0
    for i in range(n):
        total += (int(x) ^ LEAF_XOR) + (int(i) << 1)
    return i64(total)


# ---------------------------------------------------------------------------
# Vector-sum oracle STUB (Slice 4 payload). Deterministic, shape = dim. NOT wired to real execution
# and NOT model output -- a synthetic fixed vector per leaf, reduced elementwise mod 2**64.
# ---------------------------------------------------------------------------

def leaf_vector(x, i, dim):
    """Per-leaf synthetic vector: [ (x ^ XOR) + (i<<1) + (d<<2) ]_d, each element wrapped to int64."""
    if dim < 0:
        raise ValueError("dim must be >= 0")
    return [i64((int(x) ^ LEAF_XOR) + (int(i) << 1) + (int(d) << 2)) for d in range(dim)]


def vector_sum_oracle(x, n, dim):
    """Elementwise int64 sum of leaf_vector(x, i, dim) over i in 0..n-1. Returns a length-dim list."""
    if n < 0:
        raise ValueError("n must be >= 0")
    if dim < 0:
        raise ValueError("dim must be >= 0")
    acc = [0] * dim
    for i in range(n):
        v = leaf_vector(x, i, dim)
        for d in range(dim):
            acc[d] = i64(acc[d] + v[d])
    return acc


# ---------------------------------------------------------------------------
# Top-k oracle STUB (Slice 5 payload). Deterministic, shape = min(k, n). Synthetic "logits-like"
# scores -- NOT inference, NOT model output. Merge = sort by (score, index) desc, take k.
# ---------------------------------------------------------------------------

def leaf_score(x, i):
    """Per-leaf synthetic score. Deterministic; carries no inference meaning."""
    return i64((int(x) ^ LEAF_XOR) ^ (int(i) * 0x9E3779B1))


def topk_oracle(x, n, k):
    """Deterministic top-k merge over n synthetic leaf scores. Returns [(index, score), ...] of
    length min(k, n), sorted by score desc with index as a stable deterministic tie-break."""
    if n < 0:
        raise ValueError("n must be >= 0")
    if k < 0:
        raise ValueError("k must be >= 0")
    scored = sorted(((leaf_score(x, i), i) for i in range(n)),
                    key=lambda t: (t[0], -t[1]), reverse=True)
    return [(i, s) for (s, i) in scored[:k]]


# ---------------------------------------------------------------------------
# Hard fences. Locked False for the whole exp63 arc (same as exp61/exp62).
# ---------------------------------------------------------------------------

def fence_block():
    return {
        "speedup_computed": False,
        "ratio_reported": False,
        "arms_differenced": False,
        "placement_bands_differenced": False,
    }


def _fences_locked(d):
    f = fence_block()
    return all(d.get(k) is f[k] for k in f)


# ---------------------------------------------------------------------------
# Composition provenance. Slice 0 enumerates the candidate composition primitives and a fail-closed
# consistency gate. hpx_native == not polled_in_success_path. Only the exp62-proven poll control may
# be marked cross-node validated in exp63; the native/tree/collective modes are UNVALIDATED cross-node
# until a real slice proves they make progress (i.e. wake without a success-path poll).
# ---------------------------------------------------------------------------

COMPOSITION_PRIMITIVES = (
    "root_flat_gather_poll",    # exp62 PROVEN interim control: polls is_ready in the success path.
    "when_all_then_reduce",     # HPX-native continuation; UNVALIDATED cross-node (exp62 job 158814).
    "dataflow_reduce",          # HPX-native hpx::dataflow continuation; UNVALIDATED cross-node.
    "tree_of_partials",         # per-locality partial-sum actions reduced at root; native, UNVALIDATED.
    "hpx_collective_reduce",    # hpx::collectives::reduce; native, UNVALIDATED, membership-fragile.
)

# The one composition that already survived cross-node in exp62 (job 158813 / Slice 4b 158817).
PROVEN_CROSS_NODE_COMPOSITION_MODE = "root_flat_gather_poll"

# Native/UNVALIDATED cross-node modes: correct math is NOT enough; each must PROVE parcelport progress
# in its own slice before it may be trusted cross-node.
UNVALIDATED_CROSS_NODE_COMPOSITION_MODES = (
    "when_all_then_reduce", "dataflow_reduce", "tree_of_partials", "hpx_collective_reduce",
)

# Per-mode defaults: whether the success path polls, and whether the composition is HPX-native.
_COMPOSITION_DEFAULTS = {
    "root_flat_gather_poll":  {"hpx_native": False, "polled": True},
    "when_all_then_reduce":   {"hpx_native": True,  "polled": False},
    "dataflow_reduce":        {"hpx_native": True,  "polled": False},
    "tree_of_partials":       {"hpx_native": True,  "polled": False},
    "hpx_collective_reduce":  {"hpx_native": True,  "polled": False},
}


def composition_provenance(*, composition_primitive, watchdog, ran_on_hpx_thread,
                           polled_in_success_path, hpx_native_composition,
                           cross_node_composition_validated=None):
    """Pure: normalize the HPX composition-provenance fields and a fail-closed consistency gate.

    Consistency requires: a KNOWN primitive; the composition ran on an HPX thread; polled / native
    are bools; hpx_native == not polled; a non-empty watchdog label; and only the proven control may
    claim cross_node_composition_validated. When the caller does not assert validation status, it is
    derived from proven-control identity (True only for root_flat_gather_poll).

    This is a provenance/honesty gate only -- no performance, ratio, speedup, or Ray-comparison
    meaning. Any inconsistency fails closed (composition_provenance_consistent=False)."""
    known = composition_primitive in COMPOSITION_PRIMITIVES
    native = hpx_native_composition is True
    polled = polled_in_success_path is True
    if cross_node_composition_validated is None:
        cross_node_composition_validated = (
            composition_primitive == PROVEN_CROSS_NODE_COMPOSITION_MODE)
    validated = cross_node_composition_validated is True
    consistent = (
        known
        and ran_on_hpx_thread is True
        and isinstance(polled_in_success_path, bool)
        and isinstance(hpx_native_composition, bool)
        and native == (not polled)
        and isinstance(watchdog, str) and bool(watchdog)
        and (validated is False or composition_primitive == PROVEN_CROSS_NODE_COMPOSITION_MODE))
    return {
        "composition_primitive": composition_primitive,
        "watchdog": watchdog,
        "composition_ran_on_hpx_thread": ran_on_hpx_thread is True,
        "polled_in_success_path": polled,
        "hpx_native_composition": native,
        "cross_node_composition_validated": validated,
        "native_mode_experimental": composition_primitive in UNVALIDATED_CROSS_NODE_COMPOSITION_MODES,
        "proven_cross_node_composition_mode": PROVEN_CROSS_NODE_COMPOSITION_MODE,
        "composition_provenance_consistent": bool(consistent),
    }


def composition_provenance_for(mode, *, watchdog, ran_on_hpx_thread=True):
    """Build composition provenance from the per-mode default poll/native shape. Unknown modes fall
    through composition_provenance() and fail closed."""
    d = _COMPOSITION_DEFAULTS.get(mode)
    if d is None:
        return composition_provenance(
            composition_primitive=mode, watchdog=watchdog, ran_on_hpx_thread=ran_on_hpx_thread,
            polled_in_success_path=False, hpx_native_composition=False)
    return composition_provenance(
        composition_primitive=mode, watchdog=watchdog, ran_on_hpx_thread=ran_on_hpx_thread,
        polled_in_success_path=d["polled"], hpx_native_composition=d["hpx_native"])


def collective_provenance(*, communicator, generation, all_participants_contributed, watchdog,
                          ran_on_hpx_thread=True):
    """Composition provenance for the hpx_collective_reduce mode plus the collective-specific fields
    (communicator basename, generation, all-participants-contributed). Still UNVALIDATED cross-node
    until a real slice proves progress; membership/timeout hazards are handled in the collective
    slice, not here."""
    prov = composition_provenance_for("hpx_collective_reduce", watchdog=watchdog,
                                      ran_on_hpx_thread=ran_on_hpx_thread)
    prov["collective_communicator"] = communicator
    prov["collective_generation"] = generation
    prov["all_participants_contributed"] = all_participants_contributed is True
    consistent = (prov["composition_provenance_consistent"]
                  and isinstance(communicator, str) and bool(communicator)
                  and isinstance(generation, int) and generation >= 0
                  and all_participants_contributed is True)
    prov["composition_provenance_consistent"] = bool(consistent)
    return prov


# ---------------------------------------------------------------------------
# Progress-diagnosis provenance (Slice 1 root-cause micro-slice). The exp62 job-158814 stall was a
# passive wait that was not woken promptly by parcelport completion; the poll works only because a
# yielding thread lets completion handlers run. Slice 1 sweeps runtime variables to find a config
# where a PASSIVE wait wakes cross-node. A PASS requires the wait to wake WITHOUT a success-path poll.
# ---------------------------------------------------------------------------

PROGRESS_DIAGNOSIS_MODES = (
    "passive_wait_baseline",          # reproduce the exp62 job-158814 passive stall.
    "increase_hpx_threads",           # more scheduler threads so background parcel progress can run.
    "background_yielder",             # a low-priority yielding HPX thread pumps the scheduler.
    "parcelport_background_progress", # explicit parcelport background-work scheduling.
    "mpi_parcelport",                 # alternate transport with a different progress model.
    "yield_poll_control",             # known-good is_ready poll control (root_flat_gather_poll).
)


def progress_diagnosis(*, mode, no_dispatch_timeout, woke_without_success_poll,
                       dispatch_elapsed_s, dispatch_timeout_s):
    """Fail-closed progress-diagnosis result. passive_progress_ok is True only when: the mode is
    known; there was no dispatch timeout; the measured dispatch elapsed is within the timeout budget;
    AND the wait woke WITHOUT a success-path poll (the whole point of retiring the poll). The
    yield_poll_control mode proves correctness but, by design, reports woke_without_success_poll=False
    -- it must NOT be counted as passive progress."""
    known = mode in PROGRESS_DIAGNOSIS_MODES
    within = (isinstance(dispatch_elapsed_s, (int, float))
              and not isinstance(dispatch_elapsed_s, bool)
              and isinstance(dispatch_timeout_s, (int, float))
              and not isinstance(dispatch_timeout_s, bool)
              and dispatch_timeout_s > 0
              and dispatch_elapsed_s < dispatch_timeout_s)
    progress_ok = bool(known and no_dispatch_timeout is True and within
                       and woke_without_success_poll is True)
    return {
        "progress_diagnosis_mode": mode,
        "no_dispatch_timeout": no_dispatch_timeout is True,
        "woke_without_success_poll": woke_without_success_poll is True,
        "dispatch_elapsed_s": dispatch_elapsed_s,
        "dispatch_timeout_s": dispatch_timeout_s,
        "passive_progress_ok": progress_ok,
    }


# ---------------------------------------------------------------------------
# Payload provenance. Every payload is SYNTHETIC and NOT model output; no_inference is always True.
# ---------------------------------------------------------------------------

PAYLOAD_MODES = ("scalar_int64", "fixed_vector_stub", "topk_stub")


def payload_provenance(*, payload_mode, vector_dim=None, topk=None):
    """Fail-closed payload provenance. scalar_int64: 1 x int64 (8B). fixed_vector_stub: dim x int64
    (8*dim B). topk_stub: k x (int64 index, int64 score) pairs (16*k B). Unknown/malformed raises."""
    if payload_mode == "scalar_int64":
        shape, length, nbytes = "scalar", 1, 8
    elif payload_mode == "fixed_vector_stub":
        if not isinstance(vector_dim, int) or isinstance(vector_dim, bool) or vector_dim <= 0:
            raise ValueError("fixed_vector_stub needs vector_dim >= 1")
        shape, length, nbytes = "vector", vector_dim, 8 * vector_dim
    elif payload_mode == "topk_stub":
        if not isinstance(topk, int) or isinstance(topk, bool) or topk <= 0:
            raise ValueError("topk_stub needs topk >= 1")
        shape, length, nbytes = "topk", topk, 16 * topk
    else:
        raise ValueError(f"unknown payload_mode: {payload_mode!r}")
    return {
        "payload_mode": payload_mode,
        "payload_shape": shape,
        "payload_len": length,
        "payload_bytes": nbytes,
        "payload_is_synthetic": True,
        "payload_not_model_output": True,
        "no_inference": True,
    }


# ---------------------------------------------------------------------------
# Distribution witness + fail-closed gate. Each participant contributes a partial computed from some
# of the N leaves. The witness tallies leaves per locality so a separate check proves >=2 remote
# localities, full coverage of the declared remotes, and that every declared participant contributed.
# ---------------------------------------------------------------------------

def contribution_witness(partials, root_locality):
    """Tally a list of per-participant partial records into a distribution witness.

    partials: list of {"locality": id, "n_leaves": int, ...}. Returns leaves-per-locality, the sorted
    remote localities, and the remote-leaf total. The reduced VALUE is intentionally not summed here
    (the oracle proves value; the witness proves placement)."""
    per = {}
    for p in partials:
        loc = p["locality"]
        per[loc] = per.get(loc, 0) + int(p.get("n_leaves", 0))
    remote_locs = sorted((l for l in per if l != root_locality), key=repr)
    leaves_remote = sum(per[l] for l in remote_locs)
    return {
        "participant_count": len(partials),
        "localities": sorted(per, key=repr),
        "remote_localities": remote_locs,
        "n_remote_localities": len(remote_locs),
        "leaves_per_locality": per,
        "leaves_remote": leaves_remote,
    }


def distribution_gate(*, witness, declared_remote_localities, expected_remote_leaves):
    """Fail-closed distribution gate. Empty declared set fails every coverage gate closed."""
    covered = set(witness["remote_localities"])
    declared = set(declared_remote_localities)
    per = witness["leaves_per_locality"]
    return {
        "n_remote_localities_ge_2": witness["n_remote_localities"] >= 2,
        "all_declared_remotes_covered": bool(declared) and declared.issubset(covered),
        "leaves_cover_expected": witness["leaves_remote"] == expected_remote_leaves,
        "all_participants_contributed": bool(declared) and all(per.get(l, 0) > 0 for l in declared),
    }


# ---------------------------------------------------------------------------
# Oracle correctness gates (fail closed).
# ---------------------------------------------------------------------------

def oracle_gate_scalar(measured, x, n):
    return measured == composite_oracle(x, n)


def oracle_gate_vector(measured, x, n, dim):
    try:
        return list(measured) == vector_sum_oracle(x, n, dim)
    except TypeError:
        return False


def oracle_gate_topk(measured, x, n, k):
    try:
        return [tuple(t) for t in measured] == topk_oracle(x, n, k)
    except TypeError:
        return False


# ---------------------------------------------------------------------------
# Slice 0 scaffold record: compose the provenance blocks into one honesty-fenced record. No band is
# built in Slice 0; same_axis_comparison is hard-locked False here.
# ---------------------------------------------------------------------------

def build_scaffold_record(*, composition, payload, progress=None, distribution=None):
    """Assemble a Slice 0 provenance record. Carries fences, payload, composition, and optional
    progress/distribution blocks. same_axis_comparison is ALWAYS False in Slice 0 -- there is no
    matched Ray/HPX band yet."""
    rec = {
        "experiment_id": EXPERIMENT_ID,
        "slice": 0,
        "same_axis_comparison": False,
        "fences": fence_block(),
        "composition": composition,
        "payload": payload,
    }
    if progress is not None:
        rec["progress"] = progress
    if distribution is not None:
        rec["distribution"] = distribution
    return rec


# ---------------------------------------------------------------------------
# Slice 1a -- HPX progress root-cause config matrix + per-config artifact + sweep aggregate.
#
# Pure Python: it defines the diagnostic sweep, the fail-closed per-config gates, and the aggregate
# DECISION logic. It runs NO runtime; the real cross-node driver and the copied/renamed proven exp62
# HPX ext land in Slice 1b. HPX-only progress diagnosis: no Ray, no payload, no ratios/speedups, no
# winner language. root_flat_gather_poll is a KNOWN-GOOD CONTROL, never a native success.
# ---------------------------------------------------------------------------

_WATCHDOG_BY_MODE = {
    "root_flat_gather_poll": "bounded_is_ready_poll_50us",
    "when_all_then_reduce": "composed_future_wait_for",
    "dataflow_reduce": "composed_future_wait_for",
    "tree_of_partials": "composed_partial_future_wait_for",
}


def _watchdog_for(mode):
    return _WATCHDOG_BY_MODE.get(mode, "unknown")


def _is_native_mode(mode):
    return bool(_COMPOSITION_DEFAULTS.get(mode, {}).get("hpx_native", False))


def _progress_mode_for(config):
    """Map a sweep config to the PROGRESS_DIAGNOSIS_MODES label whose mechanism it exercises."""
    mode = config["composition_primitive"]
    if not _is_native_mode(mode):
        return "yield_poll_control"
    if config.get("transport") == "mpi":
        return "mpi_parcelport"
    bg = config.get("background_progress")
    if bg == "background_yielder":
        return "background_yielder"
    if bg == "parcelport_tuning":
        return "parcelport_background_progress"
    if config.get("root_threads", 0) >= 8:
        return "increase_hpx_threads"
    return "passive_wait_baseline"


def progress_config_matrix(include_phase_b=False):
    """Phase A TCP diagnostic sweep (A0..A5) reproducing the exp62 job-158814 failure shape on the
    medusa00 root + medusa01/medusa02 connector topology (N=8 all-remote 4/4, closed-int64).

    Phase B (B0 TCP parcelport tuning, B1 MPI/IB transport) is DEFERRED and only appended when
    include_phase_b=True; the Phase-B configs are flagged deferred=True and are NOT part of the default
    diagnostic run -- they are a separated follow-up if all native Phase-A TCP configs stall."""
    phase_a = [
        dict(id="A0", role="known_good_poll_control",
             composition_primitive="root_flat_gather_poll", root_threads=4, connector_threads=8,
             background_progress="baseline", bind_mode="balanced", transport="tcp",
             phase="A", deferred=False),
        dict(id="A1", role="reproduce_158814_stall",
             composition_primitive="when_all_then_reduce", root_threads=4, connector_threads=8,
             background_progress="baseline", bind_mode="balanced", transport="tcp",
             phase="A", deferred=False),
        dict(id="A2", role="more_root_threads",
             composition_primitive="when_all_then_reduce", root_threads=8, connector_threads=8,
             background_progress="baseline", bind_mode="balanced", transport="tcp",
             phase="A", deferred=False),
        dict(id="A3", role="background_yielder_probe",
             composition_primitive="when_all_then_reduce", root_threads=4, connector_threads=8,
             background_progress="background_yielder", bind_mode="balanced", transport="tcp",
             phase="A", deferred=False),
        dict(id="A4", role="threads_plus_yielder",
             composition_primitive="when_all_then_reduce", root_threads=8, connector_threads=8,
             background_progress="background_yielder", bind_mode="balanced", transport="tcp",
             phase="A", deferred=False),
        dict(id="A5", role="dataflow_best_native_probe",
             composition_primitive="dataflow_reduce", root_threads=8, connector_threads=8,
             background_progress="background_yielder", bind_mode="balanced", transport="tcp",
             phase="A", deferred=False),
    ]
    if not include_phase_b:
        return phase_a
    phase_b = [
        dict(id="B0", role="tcp_parcelport_tuning_deferred",
             composition_primitive="when_all_then_reduce", root_threads=8, connector_threads=8,
             background_progress="parcelport_tuning", bind_mode="balanced", transport="tcp",
             phase="B", deferred=True),
        dict(id="B1", role="mpi_ib_parcelport_deferred",
             composition_primitive="when_all_then_reduce", root_threads=8, connector_threads=8,
             background_progress="baseline", bind_mode="balanced", transport="mpi",
             phase="B", deferred=True),
    ]
    return phase_a + phase_b


def build_progress_config_artifact(*, config, x, n, k, w, prewarm, dispatch_timeout_s,
                                   measured_value, dispatch_elapsed_s, no_dispatch_timeout,
                                   passive_wait_woke_without_poll, timed_out_leaf_count,
                                   partials, root_locality, declared_remote_localities,
                                   connectors=None, root_cpuset=None, connector_cpusets=None,
                                   root_hostname=None, connector_hostnames=None,
                                   remote_locality_ids=None, selected_subnet=None,
                                   slurm_job_id=None, tcp_nodelay_verified=None,
                                   hpx_config_provenance=None):
    """Compose the Slice-0 helpers into one fail-closed per-config progress artifact.

    A NATIVE config is passive-progress-capable ONLY when: it is a native mode; the passive wait woke
    WITHOUT a success-path poll and within the dispatch budget (progress_diagnosis); no timed-out
    leaves; the oracle is correct; the distribution gates hold; and composition provenance is
    consistent. The A0 poll CONTROL may pass correctness/distribution but is NEVER passive-progress-
    capable (it polls, and is non-native). cross_node_composition_validated stays False for native
    modes here -- Slice 1a is scaffold; a durable validated claim needs curated multi-run promotion."""
    mode = config["composition_primitive"]
    native = _is_native_mode(mode)
    comp = composition_provenance_for(mode, watchdog=_watchdog_for(mode))
    prog = progress_diagnosis(
        mode=_progress_mode_for(config), no_dispatch_timeout=no_dispatch_timeout,
        woke_without_success_poll=passive_wait_woke_without_poll,
        dispatch_elapsed_s=dispatch_elapsed_s, dispatch_timeout_s=dispatch_timeout_s)

    witness = contribution_witness(partials, root_locality)
    dist = distribution_gate(witness=witness, declared_remote_localities=declared_remote_localities,
                             expected_remote_leaves=n)
    per = witness["leaves_per_locality"]
    leaves_local = per.get(root_locality, 0)
    witness_leaf_count = sum(per.values())
    leaves_per_remote = {loc: per[loc] for loc in witness["remote_localities"]}

    expected_value = composite_oracle(x, n)
    oracle_ok = measured_value is not None and oracle_gate_scalar(measured_value, x, n)

    lifecycle_ok = True
    if connectors is not None:
        lifecycle_ok = bool(connectors) and all(
            c.get("joined") and c.get("served") and c.get("graceful_disconnect") for c in connectors)

    gates = {
        "composite_oracle_correct": bool(oracle_ok),
        "no_dispatch_timeout": no_dispatch_timeout is True,
        "no_timed_out_leaves": timed_out_leaf_count == 0,
        "witness_leaf_count_eq_n": witness_leaf_count == n,
        "leaves_local_zero": leaves_local == 0,
        "n_remote_localities_ge_2": dist["n_remote_localities_ge_2"],
        "all_declared_remotes_covered": dist["all_declared_remotes_covered"],
        "all_participants_contributed": dist["all_participants_contributed"],
        "leaves_cover_expected": dist["leaves_cover_expected"],
        "composition_provenance_consistent": comp["composition_provenance_consistent"],
        "connector_lifecycle_ok": bool(lifecycle_ok),
    }

    correctness_distribution_ok = all([
        gates["composite_oracle_correct"], gates["witness_leaf_count_eq_n"],
        gates["leaves_local_zero"], gates["n_remote_localities_ge_2"],
        gates["all_declared_remotes_covered"], gates["all_participants_contributed"],
        gates["leaves_cover_expected"], gates["connector_lifecycle_ok"]])

    passive_progress_capable = bool(
        native and prog["passive_progress_ok"] and gates["no_timed_out_leaves"]
        and gates["composite_oracle_correct"] and correctness_distribution_ok
        and comp["composition_provenance_consistent"])

    woke = passive_wait_woke_without_poll is True
    timed_out = (no_dispatch_timeout is not True) or (timed_out_leaf_count or 0) > 0
    if not native:
        outcome = "control_pass" if correctness_distribution_ok else "fail"
    elif passive_progress_capable:
        outcome = "native_progress_pass"
    elif (not woke) or timed_out:
        outcome = "native_stalled"  # the 158814 shape: passive wait did not progress cross-node.
    else:
        outcome = "fail"  # native, woke, no timeout, but correctness/distribution broke: a real bug.
    overall = {"control_pass": "pass", "native_progress_pass": "pass",
               "native_stalled": "stalled", "fail": "fail"}[outcome]

    return {
        "experiment_id": EXPERIMENT_ID,
        "slice": 1,
        "config_id": config["id"],
        "role": config["role"],
        "phase": config.get("phase", "A"),
        "deferred": config.get("deferred", False),
        "same_axis_comparison": False,
        "composition_primitive": mode,
        "root_threads": config.get("root_threads"),
        "connector_threads": config.get("connector_threads"),
        "background_progress": config.get("background_progress"),
        "transport": config.get("transport"),
        "bind_mode": config.get("bind_mode"),
        "n": n, "k": k, "w": w, "prewarm": prewarm,
        "dispatch_timeout_s": dispatch_timeout_s,
        "progress": {
            "progress_diagnosis_mode": prog["progress_diagnosis_mode"],
            "passive_wait_woke_without_poll": woke,
            "dispatch_elapsed_s": dispatch_elapsed_s,
            "no_dispatch_timeout": no_dispatch_timeout is True,
            "timed_out_leaf_count": timed_out_leaf_count,
        },
        "correctness": {
            "expected_value": expected_value,
            "measured_value": measured_value,
            "composite_oracle_correct": bool(oracle_ok),
        },
        "distribution": {
            "n_remote_localities": witness["n_remote_localities"],
            "leaves_local": leaves_local,
            "leaves_remote": witness["leaves_remote"],
            "witness_leaf_count": witness_leaf_count,
            "leaves_per_remote_locality": leaves_per_remote,
        },
        "provenance": {
            "polled_in_success_path": comp["polled_in_success_path"],
            "hpx_native_composition": comp["hpx_native_composition"],
            "cross_node_composition_validated": comp["cross_node_composition_validated"],
            "composition_provenance_consistent": comp["composition_provenance_consistent"],
            "watchdog": comp["watchdog"],
            "root_cpuset": root_cpuset,
            "connector_cpusets": connector_cpusets,
            "root_hostname": root_hostname,
            "connector_hostnames": connector_hostnames,
            "remote_locality_ids": remote_locality_ids,
            "selected_subnet": selected_subnet,
            "slurm_job_id": slurm_job_id,
            "tcp_nodelay_verified": tcp_nodelay_verified,
            "hpx_config": hpx_config_provenance,
        },
        "gates": gates,
        "correctness_distribution_ok": correctness_distribution_ok,
        "passive_progress_capable": passive_progress_capable,
        "outcome": outcome,
        "overall": overall,
        "fences": fence_block(),
    }


def stall_short_circuit(*, composition_primitive, first_call_timed_out, no_dispatch_timeout,
                        passive_wait_woke_without_poll, k):
    """Decide whether a NATIVE config has stalled on its FIRST timed call so the sweep should advance
    to the next config instead of spending K x dispatch_timeout on a config that will never progress.
    The poll CONTROL (non-native) never short-circuits."""
    native = _is_native_mode(composition_primitive)
    stalled = bool(native and (first_call_timed_out is True
                               or no_dispatch_timeout is not True
                               or passive_wait_woke_without_poll is not True))
    return {
        "composition_primitive": composition_primitive,
        "is_native_mode": native,
        "stalled": stalled,
        "advance_to_next_config": stalled,
        "calls_spent": 1 if stalled else k,
        "budget_saved_calls": (k - 1) if stalled else 0,
        "reason": ("first-call passive stall (native mode did not wake / timed out)"
                   if stalled else "no first-call stall; run full K"),
    }


def build_progress_sweep_aggregate(config_artifacts):
    """Aggregate the per-config artifacts into the Slice-1 DECISION. HPX-only; no ratios/speedups. The
    aggregate 'overall' reflects only whether the poll CONTROL was healthy (i.e. the harness/topology
    works); the native outcome is a finding, not a pass/fail of the sweep."""
    arts = list(config_artifacts)
    controls = [a for a in arts if not _is_native_mode(a["composition_primitive"])]
    natives = [a for a in arts if _is_native_mode(a["composition_primitive"])]
    native_tcp = [a for a in natives if a.get("transport") == "tcp"]

    passed_cd = [a["config_id"] for a in arts if a["correctness_distribution_ok"]]
    passive_capable = [a for a in natives if a["passive_progress_capable"]]
    passive_ids = [a["config_id"] for a in passive_capable]
    winning = passive_ids[0] if passive_ids else None

    poll_control_ok = any(a["outcome"] == "control_pass" for a in controls)
    poll_control_status = "pass" if poll_control_ok else ("fail" if controls else "absent")

    any_native_progress = bool(passive_capable)
    all_native_tcp_stalled = bool(native_tcp) and all(
        a["passive_progress_capable"] is False for a in native_tcp)
    phase_b_needed = bool(all_native_tcp_stalled and not any_native_progress)
    tree_of_partials_recommended = not any_native_progress

    if winning is not None:
        next_direction = "retest_when_all_then_reduce_at_band_scale"
    elif phase_b_needed:
        next_direction = "phase_b_mpi_ib_then_tree_of_partials_with_poll"
    else:
        next_direction = "tree_of_partials_with_poll"

    return {
        "experiment_id": EXPERIMENT_ID,
        "slice": 1,
        "kind": "progress_sweep_aggregate",
        "same_axis_comparison": False,
        "configs_attempted": [a["config_id"] for a in arts],
        "configs_passed_correctness_distribution": passed_cd,
        "passive_progress_capable_configs": passive_ids,
        "winning_passive_progress_config": winning,
        "poll_control_status": poll_control_status,
        "phase_b_needed": phase_b_needed,
        "tree_of_partials_recommended": tree_of_partials_recommended,
        "collectives_deferred": True,
        "next_direction": next_direction,
        "overall": "pass" if poll_control_ok else "fail",
        "fences": fence_block(),
    }


# ---------------------------------------------------------------------------
# Slice 1b-follow-up -- instrumented A1 diagnostic (pure mapping layer). The Rostam sweep (job 158870)
# showed native modes are correct-then-poisoned by "Operation not permitted"; the root cause is NOT
# yet earned. These pure helpers map the instrumented ext signals (wait_for status, exception stage,
# leaf-ready-at-timeout, unsafe abandonment) + Slurm/affinity provenance into a fail-closed artifact
# block, so a future A1-only Rostam rerun can distinguish: (a) unsafe timeout abandonment, (b) cpuset/
# affinity EPERM, (c) a genuine parcelport passive-wait wakeup failure. No perf/Ray/same-axis claim.
# ---------------------------------------------------------------------------

WAIT_FOR_STATES = ("ready", "timeout", "deferred", "unknown")
EXCEPTION_STAGES = (
    "none", "before_dispatch", "after_leaf_futures", "after_composed_future", "after_wait_for",
    "before_get", "after_get", "before_teardown", "before_disconnect", "after_disconnect",
    "finalize", "dispatch", "teardown", "unknown",
)
DIAGNOSTIC_VARIANTS = ("a1_normal", "a1_root_bind_none", "a1_max_bg_threads", "a1_roomier_cpuset")


def diagnostic_variant_root_extra(variant, *, max_bg_threads=8):
    """Extra root --hpx args for a diagnostic variant, EXCLUDING the bind flag. The bind flag is owned
    solely by _a1_root_bind_for / build_a1_root_hpx_args so it is emitted EXACTLY ONCE -- a prior
    version added a second '--hpx:bind=none' here for a1_root_bind_none, which combined with the
    always-present '--hpx:bind=<mode>' into an unparsable 'none;none' (HPX bad_parameter). Only
    low-risk, SUPPORTED non-bind flags: --hpx:ini=hpx.max_background_threads=N. 'a1_root_bind_none'
    (bind via mode) and 'a1_roomier_cpuset' (allocation/step-level --cpus-per-task) add NO code arg
    here. Unknown variants add nothing (fail safe -- never guess an unsupported flag)."""
    if variant == "a1_max_bg_threads":
        return [f"--hpx:ini=hpx.max_background_threads={int(max_bg_threads)}"]
    return []


def _a1_root_bind_for(variant):
    """The embedded-root HPX --hpx:bind mode for a variant. a1_root_bind_none unbinds the root (the
    EPERM probe); every other variant keeps 'balanced'. SINGLE source of the root bind flag so it can
    never be emitted twice."""
    return "none" if variant == "a1_root_bind_none" else "balanced"


def build_a1_root_hpx_args(*, root_ip, root_port, variant, max_bg_threads=8):
    """PURE: the embedded-root HPX args for one A1 diagnostic variant. Emits EXACTLY ONE --hpx:bind
    flag (owned here, not by diagnostic_variant_root_extra) so a1_root_bind_none can never build the
    unparsable 'none;none'. Variant-specific non-bind extras (max_background_threads) follow."""
    args = ["--hpx:ignore-batch-env",
            f"--hpx:hpx={root_ip}:{root_port}",
            f"--hpx:agas={root_ip}:{root_port}",
            "--hpx:expect-connecting-localities",
            f"--hpx:bind={_a1_root_bind_for(variant)}"]
    args += diagnostic_variant_root_extra(variant, max_bg_threads=max_bg_threads)
    return args


# Connectors always run Slurm '--cpu-bind=none' + HPX '--hpx:bind=none' so their per-node step binding
# never depends on the root driver step's --cpus-per-task. This fixes the a1_roomier_cpuset confound:
# a roomier root '-c' pushed the connector cpu bind outside the job step allocation ("CPU binding
# outside of job step allocation ... Unable to satisfy cpu bind request"), so 0 connectors joined.
CONNECTOR_BIND_MODE = "none"

# Connector-lifetime hardening: serve-timeout is a DEADMAN guard, not the normal lifetime. The root
# heartbeats (root.alive) before each dispatch and writes a completion sentinel (root.done) only after
# all calls + artifact capture. The hardened connector leaves on completion, or on the deadman only if
# the root goes silent for serve_timeout seconds. See collective_connector.cpp.
CONNECTOR_LIFETIME_MODE = "root_completion_or_heartbeat_deadman"


def _touch_heartbeat(bootdirs):
    """Bump each connector's root.alive heartbeat mtime. The hardened connector resets its serve-timeout
    DEADMAN whenever this mtime advances, so a low serve-timeout survives a long run as long as the root
    keeps dispatching. Best-effort; a write failure never breaks the run."""
    stamp = repr(time.time())
    for bd in bootdirs:
        try:
            with open(os.path.join(bd, "root.alive"), "w") as fh:
                fh.write(stamp)
        except OSError:
            pass


def _write_completion(bootdirs):
    """Write the root COMPLETION sentinels (root.done + legacy served1.ok) so connectors leave via a
    root-completion signal, not the deadman. Called ONLY after all prewarm+timed calls are done and the
    artifact is captured -- i.e. no more remote dispatches will occur."""
    stamp = repr(time.time())
    for bd in bootdirs:
        for name in ("root.done", "served1.ok"):
            try:
                with open(os.path.join(bd, name), "w") as fh:
                    fh.write(stamp)
            except OSError:
                pass


def classify_connector_shutdown(disc):
    """PURE: normalize a connect.disconnected1 dict into the exp63 connector-lifetime fields. Fail
    closed to 'unknown' for an older connector build that lacks the fields. root-completion and
    serve-timeout expiry are mutually exclusive reasons; a teardown error dominates both."""
    reason = disc.get("connector_shutdown_reason")
    if reason not in ("root_completion_signal", "serve_timeout_expired", "error"):
        reason = "unknown"
    return {
        "connector_lifetime_mode": disc.get("connector_lifetime_mode", "unknown"),
        "connector_shutdown_reason": reason,
        "root_completion_signaled": bool(disc.get("root_completion_signaled", False)),
        "serve_timeout_expired": bool(disc.get("serve_timeout_expired", False)),
        "serve_timeout_s": disc.get("serve_timeout_s"),
        "connector_stayed_alive_until_root_done": bool(
            disc.get("connector_stayed_alive_until_root_done", False)),
        "root_completion_signal_time": disc.get("root_completion_unix"),
        "connector_observed_completion_time": disc.get("connector_observed_completion_unix"),
    }


def build_connector_srun_cmd(rhost, bootstrap_dir, *, connector_bin, connector_threads, serve_timeout,
                             prefer_subnet, root_ip, root_port):
    """PURE: the srun command that launches one connector on its own node. Uses --overlap and
    --cpu-bind=none so the connector step binds cleanly WITHIN its node allocation regardless of the
    root driver step's --cpus-per-task (fixes the roomier-root cpuset confound). Connector HPX also
    runs --hpx:bind=none, so its per-node -c 8 is honored on medusa01/medusa02 no matter how roomy
    the root -c is on medusa00."""
    return ["srun", "--overlap", "--cpu-bind=none", "--nodes=1", "--ntasks=1",
            f"--nodelist={rhost}", f"--cpus-per-task={connector_threads}",
            connector_bin, "--role=connect", f"--bootstrap={bootstrap_dir}",
            f"--serve-timeout={serve_timeout}", f"--prefer-subnet={prefer_subnet}",
            f"--agas-preprobe-host={root_ip}", f"--agas-preprobe-port={root_port}",
            f"--hpx:threads={connector_threads}", "--hpx:bind=none",
            "--hpx:ignore-batch-env", f"--hpx:agas={root_ip}:{root_port}"]


def diagnostic_instrumentation(*, n, wait_for_status="unknown",
                               leaf_futures_ready_count_at_timeout=None,
                               composed_future_ready_at_timeout=False, timed_out_leaf_count=None,
                               drained_ready_leaves=False, unsafe_timeout_abandonment=None,
                               exception_stage="none", exception_type="", exception_message="",
                               root_effective_cpuset=None, root_bind_mode=None, root_threads=None,
                               connector_effective_cpusets=None, connector_threads=None,
                               slurm_cpus_per_task=None, hpx_max_background_threads_config=None,
                               diagnostic_variant="a1_normal", root_step_cpus_per_task=None,
                               connector_step_cpus_per_task=None, connector_bind_mode=None,
                               slurm_step_context=None, exception_code_value=None,
                               exception_code_category=None, resource_snapshots=None,
                               resource_trend_summary=None, exception_diagnostic_information=None,
                               exception_diagnostic_available=None):
    """PURE: normalize instrumented A1 signals into a fail-closed instrumentation block. Unknown wait
    states / exception stages / variants normalize to 'unknown' (never silently accepted). When the
    ext does not assert unsafe_timeout_abandonment, it is DERIVED fail-closed: a timeout with fewer
    than n leaves ready means unready leaves remain that HPX cannot cleanly cancel."""
    trend = resource_trend_summary or {}
    status = wait_for_status if wait_for_status in WAIT_FOR_STATES else "unknown"
    ready = status == "ready"
    timeout = status == "timeout"
    stage = exception_stage if exception_stage in EXCEPTION_STAGES else "unknown"
    variant = diagnostic_variant if diagnostic_variant in DIAGNOSTIC_VARIANTS else "unknown"
    exception_observed = bool(exception_type) or stage != "none"
    if unsafe_timeout_abandonment is None:
        if not timeout:
            unsafe_timeout_abandonment = False
        elif isinstance(leaf_futures_ready_count_at_timeout, int):
            # A timeout with < n leaves ready leaves unready leaves HPX cannot cleanly cancel.
            unsafe_timeout_abandonment = leaf_futures_ready_count_at_timeout < n
        else:
            # Timeout but readiness unknown: cannot prove safe -> fail closed.
            unsafe_timeout_abandonment = True
    return {
        "wait_for_status": status,
        "wait_for_returned_ready": ready,
        "wait_for_returned_timeout": timeout,
        "leaf_futures_ready_count_at_timeout": leaf_futures_ready_count_at_timeout,
        "composed_future_ready_at_timeout": bool(composed_future_ready_at_timeout),
        "timed_out_leaf_count": timed_out_leaf_count,
        "drained_ready_leaves": bool(drained_ready_leaves),
        "unsafe_timeout_abandonment": bool(unsafe_timeout_abandonment),
        "exception_stage": stage,
        "exception_type": exception_type or "",
        "exception_message": exception_message or "",
        "exception_observed": bool(exception_observed),
        "root_effective_cpuset": root_effective_cpuset,
        "root_bind_mode": root_bind_mode,
        "root_threads": root_threads,
        "root_step_cpus_per_task": root_step_cpus_per_task,
        "connector_effective_cpusets": connector_effective_cpusets,
        "connector_threads": connector_threads,
        "connector_step_cpus_per_task": connector_step_cpus_per_task,
        "connector_bind_mode": connector_bind_mode,
        "slurm_cpus_per_task": slurm_cpus_per_task,
        "slurm_step_context": slurm_step_context,
        "hpx_max_background_threads_config": hpx_max_background_threads_config,
        "diagnostic_variant": variant,
        "exception_code_value": exception_code_value if isinstance(exception_code_value, int)
        else None,
        "exception_code_category": exception_code_category or None,
        "exception_diagnostic_information": exception_diagnostic_information or None,
        "exception_diagnostic_available": bool(exception_diagnostic_available),
        "resource_snapshots": list(resource_snapshots) if resource_snapshots else [],
        "resource_trend_summary": dict(resource_trend_summary) if resource_trend_summary else {},
        "threads_delta": trend.get("threads_delta", "unknown"),
        "fds_delta": trend.get("fds_delta", "unknown"),
        "pids_current_delta": trend.get("pids_current_delta", "unknown"),
        "resource_limit_hit_suspected": bool(trend.get("resource_limit_hit_suspected", False)),
        "resource_limit_hit_reason": trend.get("resource_limit_hit_reason", "no resource data"),
    }


# ---------------------------------------------------------------------------
# Slice 1b-follow-up2 -- root-process resource-trend instrumentation. The corrected A1 rerun (job
# 158912) showed the before_dispatch EPERM ("Operation not permitted") is independent of bind mode and
# root cpuset size and fires deterministically after ~12 native dispatches. These PURE parsers turn
# /proc/self + cgroup reads into per-call snapshots and a fail-closed trend summary, so a future
# one-variant A1 rerun can see whether threads / open fds / cgroup pids climb toward a limit at the
# fault. Unknown/unavailable values NEVER fabricate and NEVER raise a suspicion. No perf/Ray/same-axis
# claim; composition, timeout, and matrix semantics are untouched.
# ---------------------------------------------------------------------------

PIDS_NEAR_LIMIT_FRACTION = 0.9  # pids.current within this fraction of a numeric pids.max -> suspected


def parse_proc_status_threads(status_text):
    """Threads count from /proc/<pid>/status text. 'unknown' if absent or unparseable (fail closed)."""
    if not status_text:
        return "unknown"
    for line in status_text.splitlines():
        if line.startswith("Threads:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                return int(parts[1])
            return "unknown"
    return "unknown"


def parse_proc_status_mem(status_text, key):
    """VmRSS/VmSize kB from /proc/<pid>/status text for key in ('VmRSS','VmSize'). 'unknown' if
    absent/unparseable."""
    if not status_text:
        return "unknown"
    prefix = key + ":"
    for line in status_text.splitlines():
        if line.startswith(prefix):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                return int(parts[1])
            return "unknown"
    return "unknown"


def count_open_fds(fd_dir="/proc/self/fd", lister=None):
    """Number of open fds under fd_dir. 'unknown' if the directory is missing/unreadable (fail closed
    -- never fabricate a count)."""
    try:
        names = (lister or os.listdir)(fd_dir)
    except (OSError, TypeError):
        return "unknown"
    try:
        return len(names)
    except TypeError:
        return "unknown"


def parse_cgroup_pids(current_text, max_text):
    """cgroup pids.current / pids.max. current -> int or 'unknown'; max -> int, 'max' (unlimited), or
    'unknown'. Empty/absent reads fail closed to 'unknown' (never fabricate)."""
    def _cur(t):
        t = (t or "").strip()
        return int(t) if t.isdigit() else "unknown"

    def _max(t):
        t = (t or "").strip()
        if t == "max":
            return "max"
        return int(t) if t.isdigit() else "unknown"

    return {"pids_current": _cur(current_text), "pids_max": _max(max_text)}


def resource_snapshot(call_index, stage, *, status_text=None, fd_dir="/proc/self/fd", fd_lister=None,
                      pids_current_text=None, pids_max_text=None):
    """PURE: build one fail-closed resource snapshot from provided raw reads. Every field normalizes to
    'unknown' when its source text/dir is unavailable; nothing is fabricated."""
    pids = parse_cgroup_pids(pids_current_text, pids_max_text)
    return {
        "call_index": call_index,
        "stage": stage,
        "threads": parse_proc_status_threads(status_text),
        "open_fds": count_open_fds(fd_dir, lister=fd_lister),
        "pids_current": pids["pids_current"],
        "pids_max": pids["pids_max"],
        "vm_rss_kb": parse_proc_status_mem(status_text, "VmRSS"),
        "vm_size_kb": parse_proc_status_mem(status_text, "VmSize"),
    }


def _read_text_or_none(path):
    try:
        with open(path) as fh:
            return fh.read()
    except OSError:
        return None


def _cgroup_pids_paths():
    """Best-effort cgroup pids.current/pids.max paths for THIS process (cgroup v2 unified first, then
    v1 'pids' controller). Returns (current_path, max_path) or (None, None) -- callers fail closed."""
    cg = _read_text_or_none("/proc/self/cgroup") or ""
    base = "/sys/fs/cgroup"
    # cgroup v2 unified: a single "0::<path>" line.
    for line in cg.splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[0] == "0":
            root = base + parts[2]
            cur, mx = os.path.join(root, "pids.current"), os.path.join(root, "pids.max")
            if os.path.exists(cur):
                return cur, mx
    # cgroup v1: the controller line listing "pids".
    for line in cg.splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and "pids" in parts[1].split(","):
            root = os.path.join(base, "pids") + parts[2]
            cur, mx = os.path.join(root, "pids.current"), os.path.join(root, "pids.max")
            if os.path.exists(cur):
                return cur, mx
    # last resort: unified controller files at the mount root.
    if os.path.exists(os.path.join(base, "pids.current")):
        return os.path.join(base, "pids.current"), os.path.join(base, "pids.max")
    return None, None


def capture_live_resource_snapshot(call_index, stage):
    """Live root-process snapshot from /proc/self + cgroup pids. Fail-closed 'unknown' off-Linux or
    when the files are unreadable. Read-only; no side effects on the HPX runtime."""
    status = _read_text_or_none("/proc/self/status")
    cur_path, max_path = _cgroup_pids_paths()
    cur = _read_text_or_none(cur_path) if cur_path else None
    mx = _read_text_or_none(max_path) if max_path else None
    return resource_snapshot(call_index, stage, status_text=status, fd_dir="/proc/self/fd",
                             pids_current_text=cur, pids_max_text=mx)


def resource_trend_summary(snapshots):
    """PURE: summarize a resource trend across snapshots. first->last deltas for threads / open fds /
    cgroup pids.current, monotonic-growth detection, and a suspected resource-limit flag raised ONLY
    when pids.current reaches PIDS_NEAR_LIMIT_FRACTION of a NUMERIC pids.max. Any 'unknown' in a series
    makes that series' delta/growth 'unknown' and CANNOT raise a suspicion (never overclaim)."""
    snaps = list(snapshots)

    def series(key):
        return [s.get(key) for s in snaps]

    def _all_int(vals):
        return bool(vals) and all(isinstance(v, int) for v in vals)

    def _delta(vals):
        return (vals[-1] - vals[0]) if (_all_int(vals) and len(vals) >= 2) else "unknown"

    def _strictly_increasing(vals):
        return all(b > a for a, b in zip(vals, vals[1:])) if (_all_int(vals) and len(vals) >= 2) \
            else "unknown"

    def _monotonic_nondecreasing(vals):
        return all(b >= a for a, b in zip(vals, vals[1:])) if (_all_int(vals) and len(vals) >= 2) \
            else "unknown"

    threads, fds, pids_cur = series("threads"), series("open_fds"), series("pids_current")
    threads_growing = _strictly_increasing(threads)
    fds_growing = _strictly_increasing(fds)

    numeric_max = [v for v in series("pids_max") if isinstance(v, int)]
    last_pids_cur = next((v for v in reversed(pids_cur) if isinstance(v, int)), None)
    pids_max_eff = min(numeric_max) if numeric_max else "unknown"
    near_ratio = "unknown"
    near_limit = False
    if isinstance(pids_max_eff, int) and pids_max_eff > 0 and last_pids_cur is not None:
        near_ratio = last_pids_cur / pids_max_eff
        near_limit = near_ratio >= PIDS_NEAR_LIMIT_FRACTION

    reasons = []
    if near_limit:
        reasons.append(
            f"pids.current {last_pids_cur} >= {PIDS_NEAR_LIMIT_FRACTION:.0%} of pids.max {pids_max_eff}")
    if threads_growing is True:
        reasons.append("threads strictly increasing across calls")
    if fds_growing is True:
        reasons.append("open fds strictly increasing across calls")

    return {
        "n_snapshots": len(snaps),
        "threads_first": next((v for v in threads if isinstance(v, int)), "unknown"),
        "threads_last": next((v for v in reversed(threads) if isinstance(v, int)), "unknown"),
        "threads_delta": _delta(threads),
        "threads_strictly_increasing": threads_growing,
        "threads_monotonic_nondecreasing": _monotonic_nondecreasing(threads),
        "fds_delta": _delta(fds),
        "fds_strictly_increasing": fds_growing,
        "pids_current_delta": _delta(pids_cur),
        "pids_current_last": last_pids_cur if last_pids_cur is not None else "unknown",
        "pids_max_effective": pids_max_eff,
        "pids_near_limit_ratio": near_ratio,
        # A near-limit is a real limit signal; growth alone is surfaced but does NOT flag suspected.
        "resource_limit_hit_suspected": bool(near_limit),
        "resource_limit_hit_reason": "; ".join(reasons) if reasons
        else "no near-limit or growth signal (or values unknown)",
    }


# ---------------------------------------------------------------------------
# Slice 1b runtime driver -- HPX-only 3-node progress sweep (root medusa00 + 2 connectors).
#
# The PURE seams (config_to_launch_args, _partials_from_records) are unit-tested. The on-cluster
# orchestration (_drive_progress_config, run_progress_sweep) is ADAPTED from the proven exp62
# multi-remote launcher and is Rostam-only: it cannot be exercised off-cluster and stays UNVALIDATED
# until the first real sweep. HPX-only: no Ray, no payload, no ratios/speedups, NEVER same_axis. Each
# config runs in its OWN process (orchestrator/inner-driver split) because an embedded HPX runtime
# cannot be re-init'd in one process (exp62 lesson).
# ---------------------------------------------------------------------------

EXP63_RUNS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_exp63_runs")


def config_to_launch_args(config):
    """PURE: map a sweep config to concrete launch knobs. root_bind defaults to 'balanced';
    background_yielder is on only for the background_yielder configs; the composition mode is passed
    straight to collective_ext.fanout_fanin_remote."""
    mode = config["composition_primitive"]
    bind = config.get("bind_mode") or "balanced"
    return {
        "composition_mode": mode,
        "root_hpx_threads": int(config.get("root_threads", 4)),
        "connector_threads": int(config.get("connector_threads", 8)),
        "background_yielder": config.get("background_progress") == "background_yielder",
        "root_bind": bind,
        "transport": config.get("transport", "tcp"),
    }


def _partials_from_records(records):
    """PURE: group leaf records into per-locality partials for contribution_witness. Localities that
    ran no leaf simply do not appear (fail-closed downstream via the coverage gate)."""
    per = {}
    for rec in records:
        per[rec["locality"]] = per.get(rec["locality"], 0) + 1
    return [{"locality": loc, "n_leaves": cnt} for loc, cnt in sorted(per.items())]


def _short_host(h):
    return h.split(".")[0] if isinstance(h, str) and h else h


def _effective_cpuset():
    try:
        return sorted(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return None


def _read_json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:  # noqa: BLE001 -- absent/partial rendezvous file => empty, fail closed upstream.
        return {}


def _scontrol_hostnames(nodelist):
    import subprocess
    try:
        out = subprocess.run(["scontrol", "show", "hostnames", nodelist],
                             capture_output=True, text=True, timeout=30)
        if out.returncode == 0:
            return [h.strip() for h in out.stdout.split() if h.strip()]
    except Exception:  # noqa: BLE001
        pass
    return []


def _slurm_info(env=None):
    env = os.environ if env is None else env
    job_id = env.get("SLURM_JOB_ID")
    nodelist = env.get("SLURM_JOB_NODELIST")
    hostnames = _scontrol_hostnames(nodelist) if nodelist else []
    return {"slurm_job_id": job_id, "nodelist": nodelist, "hostnames": hostnames,
            "n_nodes": len(hostnames)}


def _first_ip_for_subnet(prefer_subnet):
    if not prefer_subnet:
        return None
    import socket
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip.startswith(prefer_subnet):
                return ip
    except Exception:  # noqa: BLE001
        pass
    import subprocess
    try:
        out = subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=10)
        if out.returncode == 0:
            for ip in out.stdout.split():
                if ip.startswith(prefer_subnet):
                    return ip
    except Exception:  # noqa: BLE001
        pass
    return None


def _default_hpx_import():
    here = os.path.dirname(os.path.abspath(__file__))
    for d in (os.path.join(here, "build"), os.path.join(here, "build", "Release")):
        if os.path.isdir(d) and d not in sys.path:
            sys.path.insert(0, d)
    import collective_ext
    return collective_ext


def _find_connector_bin(here=None):
    here = os.path.dirname(os.path.abspath(__file__)) if here is None else here
    for d in (os.path.join(here, "build"), os.path.join(here, "build", "Release")):
        cand = os.path.join(d, "collective_connector")
        if os.path.isfile(cand):
            return cand
    return None


def _progress_preconditions(env, import_fn):
    """Resolve (collective_ext, slurm, connector_bin) or a SKIP reason. The 3-node 4/4 progress shape
    needs a >=3-node Slurm allocation (root + 2 connectors); off-cluster and unbuilt both skip."""
    slurm = _slurm_info(env)
    if len(slurm["hostnames"]) < 3:
        return None, (f"SKIP -- need a >=3-node Slurm allocation (root + 2 connectors) for the 3-node "
                      f"4/4 progress shape (hostnames={slurm['hostnames']}).")
    connector_bin = _find_connector_bin()
    if connector_bin is None:
        return None, "SKIP -- collective_connector not built (build the exp63 CMake target first)."
    try:
        ext = (import_fn or _default_hpx_import)()
    except ImportError as exc:
        return None, f"SKIP -- collective_ext not built ({exc})."
    return (ext, slurm, connector_bin), None


def _progress_artifact_path(job_id, cfg_id, exp_dir=None):
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    return os.path.join(exp_dir, f"exp63_progress_{job_id}_{cfg_id}_hpx.json")


def _write_progress_artifact(art, job_id):
    """Write a raw, GITIGNORED per-config progress artifact (matches .gitignore exp63_*_hpx.json)."""
    path = _progress_artifact_path(job_id, art["config_id"])
    with open(path, "w") as fh:
        json.dump(art, fh, indent=2, sort_keys=True)
    return path


def _write_progress_aggregate(agg, job_id, exp_dir=None):
    """Write the curated (TRACKABLE) sweep aggregate exp63_progress_<job>_aggregate.json."""
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    path = os.path.join(exp_dir, f"exp63_progress_{job_id}_aggregate.json")
    with open(path, "w") as fh:
        json.dump(agg, fh, indent=2, sort_keys=True)
    return path


def _print_progress_summary(tag, art):
    print(f"{tag}: outcome={art['outcome']} overall={art['overall']} "
          f"passive_progress_capable={art['passive_progress_capable']} "
          f"timed_out_leaf_count={art['progress']['timed_out_leaf_count']} "
          f"error={art.get('error')}")


def _drive_progress_config(ext, slurm, connector_bin, config, *, x=7, n=8, k=20, prewarm=5,
                           dispatch_timeout_s=8.0, prefer_subnet="10.42.5.", root_port=7912,
                           await_timeout=90, serve_timeout=180, n_remote=2):
    """ADAPTED FROM exp62 _hpx_multi_remote_island (Rostam-only, UNVALIDATED off-cluster). Start the
    embedded AGAS root for THIS config's threads/bind, launch n_remote connectors, await them, time
    K calls of collective_ext.fanout_fanin_remote(x, n, timeout, mode, background_yielder), short-
    circuit a native first-call stall, tear down, read attest/lifecycle, and build the Slice-1a
    per-config artifact. ONE config per process (no HPX re-init)."""
    import subprocess
    import tempfile

    la = config_to_launch_args(config)
    hostnames = slurm["hostnames"]
    root_host = hostnames[0]
    remote_hosts = [h for h in hostnames if h != root_host][:n_remote]
    root_ip = _first_ip_for_subnet(prefer_subnet)
    os.makedirs(EXP63_RUNS, exist_ok=True)

    procs, bootdirs = [], []
    started = False
    error = None
    last = None
    lat = []
    remote_locs = []
    root_loc = None
    root_cpuset = None
    dispatch_elapsed_s = None
    hpx_config = None
    short_circuited = False
    try:
        if len(remote_hosts) < 2:
            raise RuntimeError(f"need >=2 remote hosts distinct from root; got {remote_hosts}")
        if not root_ip:
            raise RuntimeError(f"no root IP for --prefer-subnet {prefer_subnet}")
        root_extra = ["--hpx:ignore-batch-env",
                      f"--hpx:hpx={root_ip}:{root_port}", f"--hpx:agas={root_ip}:{root_port}",
                      "--hpx:expect-connecting-localities", f"--hpx:bind={la['root_bind']}"]
        ext.start(la["root_hpx_threads"], root_extra)
        started = True
        root_loc = int(ext.local_locality_id())
        root_cpuset = _effective_cpuset()
        try:
            hpx_config = dict(ext.hpx_config_provenance())
        except Exception:  # noqa: BLE001
            hpx_config = None
        for idx, rhost in enumerate(remote_hosts):
            bd = tempfile.mkdtemp(
                prefix=f"progress_{slurm['slurm_job_id']}_{config['id']}_c{idx + 1}_", dir=EXP63_RUNS)
            bootdirs.append(bd)
            cmd = ["srun", "--nodes=1", "--ntasks=1", f"--nodelist={rhost}",
                   f"--cpus-per-task={la['connector_threads']}",
                   connector_bin, "--role=connect", f"--bootstrap={bd}",
                   f"--serve-timeout={serve_timeout}", f"--prefer-subnet={prefer_subnet}",
                   f"--agas-preprobe-host={root_ip}", f"--agas-preprobe-port={root_port}",
                   f"--hpx:threads={la['connector_threads']}", "--hpx:bind=none",
                   "--hpx:ignore-batch-env", f"--hpx:agas={root_ip}:{root_port}"]
            procs.append(subprocess.Popen(cmd))
        joined = int(ext.await_remotes(len(remote_hosts), await_timeout))
        if joined < len(remote_hosts):
            raise RuntimeError(f"only {joined} of {len(remote_hosts)} remote localities joined")
        remote_locs = [int(v) for v in ext.remote_locality_ids()]
        mode, yielder = la["composition_mode"], la["background_yielder"]
        for _ in range(prewarm):
            ext.fanout_fanin_remote(x, n, dispatch_timeout_s, mode, yielder)
        t_all0 = time.perf_counter_ns()
        for call_idx in range(k):
            t0 = time.perf_counter_ns()
            last = ext.fanout_fanin_remote(x, n, dispatch_timeout_s, mode, yielder)
            lat.append(time.perf_counter_ns() - t0)
            # Slice 1b stall short-circuit: on the FIRST timed call, if a native mode did not return
            # within budget (timed_out>0), abandon this config to spare K*timeout.
            if call_idx == 0 and _is_native_mode(mode) and int(last[6]) > 0:
                short_circuited = True
                break
        dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    finally:
        for bd in bootdirs:
            try:
                with open(os.path.join(bd, "served1.ok"), "w") as fh:
                    fh.write("served\n")
            except OSError:
                pass
        for p in procs:
            try:
                p.wait(timeout=max(10, serve_timeout))
            except Exception:  # noqa: BLE001
                p.kill()
        if started:
            try:
                ext.shutdown()
            except Exception as exc:  # noqa: BLE001
                error = error or f"shutdown: {type(exc).__name__}: {exc}"

    connectors = []
    for idx, bd in enumerate(bootdirs):
        att = _read_json(os.path.join(bd, "attest_connect.json"))
        disc = _read_json(os.path.join(bd, "connect.disconnected1"))
        connectors.append({
            "bootstrap_dir": os.path.basename(bd),
            "hostname": _short_host(att.get("hostname")
                                    or (remote_hosts[idx] if idx < len(remote_hosts) else None)),
            "locality_id": att.get("locality_id"),
            "connector_cpuset_effective": att.get("connector_cpuset_effective"),
            "transport": att.get("hpx_parcelport_transport", "unknown"),
            "tcp_nodelay_verified": bool(att.get("tcp_nodelay_attested") and att.get("tcp_nodelay")),
            "joined": os.path.isfile(os.path.join(bd, "connect.joined1")),
            "served": os.path.isfile(os.path.join(bd, "served1.ok")),
            "graceful_disconnect": os.path.isfile(os.path.join(bd, "connect.disconnected1"))
            and bool(disc.get("served")),
        })

    if last is not None:
        (value, leaves, _cprim, _rprim, _n_loc, _inner_n, timed_out,
         _watchdog, _ran_hpx, _polled, _native) = last
        records = [{"i": int(i), "value": int(v), "locality": int(loc)} for (i, v, loc) in leaves]
        measured_value = int(value)
    else:
        timed_out, records, measured_value = n, [], None

    native = _is_native_mode(la["composition_mode"])
    no_dispatch_timeout = (error is None) and int(timed_out) == 0
    passive_woke = bool(native and int(timed_out) == 0 and measured_value is not None
                        and oracle_gate_scalar(measured_value, x, n))
    all_nodelay = bool(connectors) and all(c["tcp_nodelay_verified"] for c in connectors)

    art = build_progress_config_artifact(
        config=config, x=x, n=n, k=k, w=prewarm, prewarm=prewarm,
        dispatch_timeout_s=dispatch_timeout_s, measured_value=measured_value,
        dispatch_elapsed_s=(dispatch_elapsed_s if dispatch_elapsed_s is not None
                            else dispatch_timeout_s),
        no_dispatch_timeout=no_dispatch_timeout, passive_wait_woke_without_poll=passive_woke,
        timed_out_leaf_count=int(timed_out), partials=_partials_from_records(records),
        root_locality=(root_loc if root_loc is not None else 0),
        declared_remote_localities=remote_locs, connectors=connectors, root_cpuset=root_cpuset,
        connector_cpusets=[c["connector_cpuset_effective"] for c in connectors],
        root_hostname=_short_host(root_host),
        connector_hostnames=[c["hostname"] for c in connectors], remote_locality_ids=remote_locs,
        selected_subnet=prefer_subnet, slurm_job_id=slurm["slurm_job_id"],
        tcp_nodelay_verified=all_nodelay, hpx_config_provenance=hpx_config)
    art["error"] = error
    art["node_set"] = [_short_host(root_host)] + [c["hostname"] for c in connectors]
    art["connectors"] = connectors
    art["latency_samples_ns"] = lat
    art["short_circuited"] = short_circuited
    art["bootstrap_dirs"] = [os.path.basename(b) for b in bootdirs]
    return art


def run_progress_diagnosis(*, config_id="A1", env=None, import_fn=None, write=True, x=7, n=8, k=20,
                           prewarm=5, dispatch_timeout_s=8.0, prefer_subnet="10.42.5.",
                           root_port=7912, await_timeout=90, serve_timeout=180, n_remote=2):
    """Drive ONE progress config on a >=3-node Slurm allocation. Skips cleanly off-cluster or if the
    ext/connector is unbuilt (no faked evidence). Returns (status, artifact)."""
    matrix = {c["id"]: c for c in progress_config_matrix(include_phase_b=True)}
    config = matrix.get(config_id)
    if config is None:
        print(f"exp63 progress-diagnosis: unknown config_id {config_id!r}")
        return "fail", None
    pre, reason = _progress_preconditions(env, import_fn)
    if reason:
        print(f"exp63 progress-diagnosis[{config_id}]: {reason}")
        return "skip", None
    ext, slurm, connector_bin = pre
    art = _drive_progress_config(ext, slurm, connector_bin, config, x=x, n=n, k=k, prewarm=prewarm,
                                 dispatch_timeout_s=dispatch_timeout_s, prefer_subnet=prefer_subnet,
                                 root_port=root_port, await_timeout=await_timeout,
                                 serve_timeout=serve_timeout, n_remote=n_remote)
    _print_progress_summary(f"progress-diagnosis[{config_id}]", art)
    if write:
        print(f"exp63 progress-diagnosis[{config_id}]: wrote "
              f"{os.path.basename(_write_progress_artifact(art, slurm['slurm_job_id']))} (gitignored)")
    return art["overall"], art


def run_progress_sweep(*, include_phase_b=False, env=None, write=True, python_exe=None, x=7, n=8,
                       k=20, prewarm=5, dispatch_timeout_s=8.0, prefer_subnet="10.42.5.",
                       root_port=7912, await_timeout=90, serve_timeout=180, n_remote=2):
    """Drive the full Phase-A sweep, ONE config PER SUBPROCESS (no in-process HPX re-init), then read
    the per-config artifacts back and build + write the curated aggregate. Skips cleanly off-cluster
    or if the ext/connector is unbuilt. Rostam-only; UNVALIDATED off-cluster."""
    import subprocess
    pre, reason = _progress_preconditions(env, None)
    if reason:
        print(f"exp63 progress-sweep: {reason}")
        return "skip", None
    _ext, slurm, _bin = pre
    job_id = slurm["slurm_job_id"]
    matrix = progress_config_matrix(include_phase_b=include_phase_b)
    exe = python_exe or sys.executable
    artifacts = []
    for config in matrix:
        cmd = [exe, os.path.abspath(__file__), "--phase", "progress-diagnosis",
               "--config-id", config["id"], "--x", str(x), "--n", str(n), "--k", str(k),
               "--prewarm", str(prewarm), "--dispatch-timeout-s", str(dispatch_timeout_s),
               "--prefer-subnet", prefer_subnet, "--root-port", str(root_port),
               "--n-remote", str(n_remote)]
        print(f"exp63 progress-sweep: launching config {config['id']} in a fresh process")
        subprocess.run(cmd, env=(os.environ if env is None else {**os.environ, **env}))
        art = _read_json(_progress_artifact_path(job_id, config["id"]))
        if art:
            artifacts.append(art)
        else:
            print(f"exp63 progress-sweep: WARNING no artifact for {config['id']}")
    if not artifacts:
        print("exp63 progress-sweep: no per-config artifacts produced")
        return "fail", None
    agg = build_progress_sweep_aggregate(artifacts)
    if write:
        print(f"exp63 progress-sweep: wrote "
              f"{os.path.basename(_write_progress_aggregate(agg, job_id))} (curated, trackable)")
    print(f"exp63 progress-sweep: overall={agg['overall']} "
          f"winner={agg['winning_passive_progress_config']} "
          f"phase_b_needed={agg['phase_b_needed']} "
          f"tree_of_partials_recommended={agg['tree_of_partials_recommended']}")
    return agg["overall"], agg


# A0 vs A1 share the instrumented diag path; the composition mode selects poll CONTROL vs native.
_DIAG_CONFIG_ID = {
    "root_flat_gather_poll": "A0-diag",
    "when_all_then_reduce": "A1-diag",
    "dataflow_reduce": "A5-diag",
}


def _a1_diag_artifact_path(job_id, variant, exp_dir=None, composition_mode="when_all_then_reduce"):
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    cfg = _DIAG_CONFIG_ID.get(composition_mode, "A1-diag").lower().replace("-", "")
    return os.path.join(exp_dir, f"exp63_a1diag_{job_id}_{cfg}_{variant}_hpx.json")


def _drive_a1_diag(ext, slurm, connector_bin, *, variant="a1_normal", x=7, n=8, k=20, prewarm=5,
                   dispatch_timeout_s=8.0, prefer_subnet="10.42.5.", root_port=7913,
                   await_timeout=90, serve_timeout=180, n_remote=2, max_bg_threads=8, env=None,
                   composition_mode="when_all_then_reduce", heartbeat=True):
    """ADAPTED FROM _drive_progress_config (Rostam-only, UNVALIDATED off-cluster). Runs the INSTRUMENTED
    when_all_then_reduce diagnostic up to K calls, capturing the FIRST call that does not return
    'ready' (or reports an exception) plus how many calls succeeded before it, and full affinity/HPX
    provenance. Uses collective_ext.fanout_fanin_remote_diag (safe drain, no throw). ONE process.

    heartbeat=True (default, HARDENED): the root bumps each connector's root.alive heartbeat before
    every dispatch and writes the completion sentinel only after all calls + artifact capture, so
    connectors stay alive until root completion and serve-timeout is only a deadman. heartbeat=False is
    the CONTROL: no heartbeat, no completion signal, so a connector must leave via serve_timeout_expired
    (proves the timeout classification)."""
    import subprocess
    import tempfile

    env = os.environ if env is None else env
    root_bind = _a1_root_bind_for(variant)
    la = {"root_hpx_threads": 4, "connector_threads": 8, "root_bind": root_bind}
    hostnames = slurm["hostnames"]
    root_host = hostnames[0]
    remote_hosts = [h for h in hostnames if h != root_host][:n_remote]
    root_ip = _first_ip_for_subnet(prefer_subnet)
    os.makedirs(EXP63_RUNS, exist_ok=True)

    procs, bootdirs = [], []
    started = False
    error = None
    py_stage = "none"
    root_loc = None
    root_cpuset = None
    hpx_config = None
    remote_locs = []
    first_anomaly = None
    anomaly_call_index = None
    calls_ok = 0
    resource_snaps = []
    try:
        if len(remote_hosts) < 2:
            raise RuntimeError(f"need >=2 remote hosts distinct from root; got {remote_hosts}")
        if not root_ip:
            raise RuntimeError(f"no root IP for --prefer-subnet {prefer_subnet}")
        root_extra = build_a1_root_hpx_args(root_ip=root_ip, root_port=root_port, variant=variant,
                                            max_bg_threads=max_bg_threads)
        ext.start(la["root_hpx_threads"], root_extra)
        started = True
        root_loc = int(ext.local_locality_id())
        root_cpuset = _effective_cpuset()
        try:
            hpx_config = dict(ext.hpx_config_provenance())
        except Exception:  # noqa: BLE001
            hpx_config = None
        for idx, rhost in enumerate(remote_hosts):
            bd = tempfile.mkdtemp(
                prefix=f"a1diag_{slurm['slurm_job_id']}_{variant}_c{idx + 1}_", dir=EXP63_RUNS)
            bootdirs.append(bd)
            cmd = build_connector_srun_cmd(
                rhost, bd, connector_bin=connector_bin, connector_threads=la["connector_threads"],
                serve_timeout=serve_timeout, prefer_subnet=prefer_subnet, root_ip=root_ip,
                root_port=root_port)
            procs.append(subprocess.Popen(cmd))
        joined = int(ext.await_remotes(len(remote_hosts), await_timeout))
        if joined < len(remote_hosts):
            raise RuntimeError(f"only {joined} of {len(remote_hosts)} remote localities joined")
        remote_locs = [int(v) for v in ext.remote_locality_ids()]
        for _ in range(prewarm):
            if heartbeat:
                _touch_heartbeat(bootdirs)  # keep connectors alive across the (untimed) warmup
            ext.fanout_fanin_remote_diag(x, n, dispatch_timeout_s, composition_mode, True)
        # Resource trend: snapshot the root process before the first counted call, before/after each
        # call, and at the anomaly, so a monotonic climb toward a cgroup pids / fd / thread limit at
        # the ~call-12 fault is observable. Read-only; does not touch the HPX runtime.
        resource_snaps.append(capture_live_resource_snapshot(-1, "before_first_call"))
        for call_idx in range(k):
            if heartbeat:
                _touch_heartbeat(bootdirs)  # heartbeat BEFORE each dispatch resets the connector deadman
            resource_snaps.append(capture_live_resource_snapshot(call_idx, "before_dispatch"))
            d = dict(ext.fanout_fanin_remote_diag(x, n, dispatch_timeout_s,
                                                  composition_mode, True))
            if d.get("wait_for_status") != "ready" or d.get("exception_type"):
                first_anomaly, anomaly_call_index = d, call_idx
                resource_snaps.append(capture_live_resource_snapshot(call_idx, "at_exception"))
                break
            calls_ok += 1
            resource_snaps.append(capture_live_resource_snapshot(call_idx, "after_call"))
    except Exception as exc:  # noqa: BLE001
        error, py_stage = f"{type(exc).__name__}: {exc}", "dispatch"
    finally:
        if started:
            resource_snaps.append(capture_live_resource_snapshot(anomaly_call_index, "before_teardown"))
        # Signal ROOT COMPLETION (all calls done, artifact about to be built, no more dispatches) so
        # connectors leave via root_completion_signal. heartbeat=False is the CONTROL: no completion
        # signal is written, so a connector must expire via its serve-timeout deadman.
        if heartbeat:
            _write_completion(bootdirs)
        for p in procs:
            try:
                p.wait(timeout=max(10, serve_timeout))
            except Exception:  # noqa: BLE001
                p.kill()
        if started:
            try:
                ext.shutdown()
            except Exception as exc:  # noqa: BLE001
                error = error or f"shutdown: {type(exc).__name__}: {exc}"
                py_stage = "teardown"

    connectors = []
    for idx, bd in enumerate(bootdirs):
        att = _read_json(os.path.join(bd, "attest_connect.json"))
        disc = _read_json(os.path.join(bd, "connect.disconnected1"))
        rec = {
            "bootstrap_dir": os.path.basename(bd),
            "hostname": _short_host(att.get("hostname")
                                    or (remote_hosts[idx] if idx < len(remote_hosts) else None)),
            "connector_cpuset_effective": att.get("connector_cpuset_effective"),
            "tcp_nodelay_verified": bool(att.get("tcp_nodelay_attested") and att.get("tcp_nodelay")),
            "joined": os.path.isfile(os.path.join(bd, "connect.joined1")),
            "served": os.path.isfile(os.path.join(bd, "connect.disconnected1")) and bool(disc.get("served")),
            "graceful_disconnect": os.path.isfile(os.path.join(bd, "connect.disconnected1"))
            and bool(disc.get("served")),
        }
        rec.update(classify_connector_shutdown(disc))
        connectors.append(rec)

    a = first_anomaly or {}
    instr = diagnostic_instrumentation(
        n=n,
        wait_for_status=a.get("wait_for_status", "ready" if first_anomaly is None else "unknown"),
        leaf_futures_ready_count_at_timeout=a.get("leaf_futures_ready_count_at_timeout"),
        composed_future_ready_at_timeout=a.get("composed_future_ready_at_timeout", False),
        timed_out_leaf_count=a.get("timed_out_leaf_count"),
        drained_ready_leaves=a.get("drained_ready_leaves", False),
        unsafe_timeout_abandonment=a.get("unsafe_timeout_abandonment"),
        exception_stage=(a.get("exception_stage") or (py_stage if error else "none")),
        exception_type=a.get("exception_type", ""),
        exception_message=(a.get("exception_message", "") or (error or "")),
        root_effective_cpuset=root_cpuset, root_bind_mode=la["root_bind"],
        root_threads=la["root_hpx_threads"],
        connector_effective_cpusets=[c["connector_cpuset_effective"] for c in connectors],
        connector_threads=la["connector_threads"],
        slurm_cpus_per_task=env.get("SLURM_CPUS_PER_TASK"),
        root_step_cpus_per_task=env.get("SLURM_CPUS_PER_TASK"),
        connector_step_cpus_per_task=la["connector_threads"],
        connector_bind_mode=CONNECTOR_BIND_MODE,
        slurm_step_context={
            "slurm_job_id": env.get("SLURM_JOB_ID"),
            "slurm_step_id": env.get("SLURM_STEP_ID"),
            "slurm_step_nodelist": env.get("SLURM_STEP_NODELIST"),
            "slurm_cpus_per_task": env.get("SLURM_CPUS_PER_TASK"),
        },
        hpx_max_background_threads_config=(hpx_config or {}).get("hpx.max_background_threads"),
        diagnostic_variant=variant,
        exception_code_value=a.get("exception_code_value"),
        exception_code_category=a.get("exception_code_category"),
        exception_diagnostic_information=a.get("exception_diagnostic_information"),
        exception_diagnostic_available=a.get("exception_diagnostic_available"),
        resource_snapshots=resource_snaps,
        resource_trend_summary=resource_trend_summary(resource_snaps))

    # Connector-lifetime summary + late-parcel inference. A before_dispatch std::system_error co-
    # occurring with any connector that expired via its serve-timeout deadman is the serve-window race
    # signature (a leaf parcel racing a stopped connector pool); this is an INFERENCE from the two
    # observed facts, not a direct connector-side capture.
    late_parcel_after_shutdown_detected = bool(
        (a.get("exception_stage") == "before_dispatch")
        and ("system_error" in (a.get("exception_type") or ""))
        and any(c.get("serve_timeout_expired") for c in connectors))

    return {
        "experiment_id": EXPERIMENT_ID,
        "slice": 1,
        "kind": "a1_diagnostic",
        "config_id": _DIAG_CONFIG_ID.get(composition_mode, "A1-diag"),
        "diagnostic_variant": variant,
        "same_axis_comparison": False,
        "composition_primitive": composition_mode,
        "n": n, "k": k, "prewarm": prewarm, "dispatch_timeout_s": dispatch_timeout_s,
        "calls_ok_before_anomaly": calls_ok,
        "anomaly_call_index": anomaly_call_index,
        "anomaly_observed": bool(first_anomaly is not None or error is not None),
        "diagnostic": instr,
        "connectors": connectors,
        "connector_lifetime_mode": CONNECTOR_LIFETIME_MODE,
        "heartbeat_enabled": heartbeat,
        "serve_timeout_s": serve_timeout,
        "connector_shutdown_reasons": [c.get("connector_shutdown_reason") for c in connectors],
        "root_completion_signaled_all": bool(connectors)
        and all(c.get("root_completion_signaled") for c in connectors),
        "serve_timeout_expired_any": any(c.get("serve_timeout_expired") for c in connectors),
        "connectors_stayed_alive_until_root_done": bool(connectors)
        and all(c.get("connector_stayed_alive_until_root_done") for c in connectors),
        "late_parcel_after_shutdown_detected": late_parcel_after_shutdown_detected,
        "node_set": [_short_host(root_host)] + [c["hostname"] for c in connectors],
        "remote_locality_ids": remote_locs,
        "root_locality": root_loc,
        "hpx_config": hpx_config,
        "error": error,
        "slurm_job_id": slurm["slurm_job_id"],
        "fences": fence_block(),
    }


def run_a1_diagnostic(*, variant="a1_normal", env=None, import_fn=None, write=True, x=7, n=8, k=20,
                      prewarm=5, dispatch_timeout_s=8.0, prefer_subnet="10.42.5.", root_port=7913,
                      await_timeout=90, serve_timeout=180, n_remote=2, max_bg_threads=8,
                      composition_mode="when_all_then_reduce", heartbeat=True):
    """Instrumented single-config diagnostic rerun. composition_mode selects the A1 native
    when_all_then_reduce path or the A0 root_flat_gather_poll CONTROL (same instrumented dispatch loop,
    resource snapshots, and errno/diagnostic capture) so A0 vs A1 EPERM behaviour is comparable. Skips
    cleanly off-cluster or if the ext/connector is unbuilt. Rostam-only. Returns (status, artifact)."""
    if variant not in DIAGNOSTIC_VARIANTS:
        print(f"exp63 a1-diagnostic: unknown variant {variant!r} (choices: {DIAGNOSTIC_VARIANTS})")
        return "fail", None
    if composition_mode not in _DIAG_CONFIG_ID:
        print(f"exp63 a1-diagnostic: unknown composition_mode {composition_mode!r} "
              f"(choices: {list(_DIAG_CONFIG_ID)})")
        return "fail", None
    pre, reason = _progress_preconditions(env, import_fn)
    label = _DIAG_CONFIG_ID[composition_mode]
    if reason:
        print(f"exp63 a1-diagnostic[{label}/{variant}]: {reason}")
        return "skip", None
    ext, slurm, connector_bin = pre
    art = _drive_a1_diag(ext, slurm, connector_bin, variant=variant, x=x, n=n, k=k, prewarm=prewarm,
                         dispatch_timeout_s=dispatch_timeout_s, prefer_subnet=prefer_subnet,
                         root_port=root_port, await_timeout=await_timeout, serve_timeout=serve_timeout,
                         n_remote=n_remote, max_bg_threads=max_bg_threads, env=env,
                         composition_mode=composition_mode, heartbeat=heartbeat)
    di = art["diagnostic"]
    print(f"exp63 a1-diagnostic[{label}/{variant}]: "
          f"calls_ok_before_anomaly={art['calls_ok_before_anomaly']} "
          f"anomaly_call_index={art['anomaly_call_index']} wait_for_status={di['wait_for_status']} "
          f"exception_stage={di['exception_stage']} exception_type={di['exception_type']} "
          f"exception_code={di['exception_code_value']}/{di['exception_code_category']} "
          f"diag_avail={di['exception_diagnostic_available']} "
          f"unsafe_timeout_abandonment={di['unsafe_timeout_abandonment']} "
          f"leaf_ready_at_timeout={di['leaf_futures_ready_count_at_timeout']} error={art['error']}")
    print(f"exp63 a1-diagnostic[{label}/{variant}]: heartbeat_enabled={art['heartbeat_enabled']} "
          f"connector_shutdown_reasons={art['connector_shutdown_reasons']} "
          f"connectors_stayed_alive_until_root_done={art['connectors_stayed_alive_until_root_done']} "
          f"serve_timeout_expired_any={art['serve_timeout_expired_any']} "
          f"late_parcel_after_shutdown_detected={art['late_parcel_after_shutdown_detected']}")
    if write:
        path = _a1_diag_artifact_path(art["slurm_job_id"], variant, composition_mode=composition_mode)
        with open(path, "w") as fh:
            json.dump(art, fh, indent=2, sort_keys=True)
        print(f"exp63 a1-diagnostic[{label}/{variant}]: wrote {os.path.basename(path)} (gitignored)")
    return ("anomaly" if art["anomaly_observed"] else "clean"), art


# ---------------------------------------------------------------------------
# Slice 2a -- native composition retest under the HARDENED connector-lifetime contract.
#
# The connector-lifetime hardening (heartbeat + root-completion; see connector_lifetime_hardening.md)
# removed the serve-window race that MASQUERADED as a permission/progress fault. Slice 2a asks the
# separate, honest question that fix unblocked: under a CORRECT connector lifetime (connectors alive for
# the whole dispatch window), does a NATIVE composed wait (when_all_then_reduce / dataflow_reduce)
# actually complete cross-node for the closed-int64 mechanism slice -- all K calls ready, oracle
# correct, all leaves remote, connectors leaving via root_completion_signal -- so the mode can be
# promoted from "diagnostic / UNVALIDATED cross-node" to "cross-node validated under hardened
# lifecycle"? root_flat_gather_poll is the poll CONTROL: it may pass the mechanics but
# polled_in_success_path is True, so it is NEVER counted as native validation.
#
# HPX-only, closed-int64, no Ray, no payload, no collectives, no tree_of_partials, no same-axis, no
# performance / ratio / speedup claim. The timed call is the SAFE fanout_fanin_remote_diag path (it
# catches exceptions and drains only ready leaves) so a native stall FAILS the gate cleanly instead of
# crashing the process. Rostam-only; skips cleanly off-cluster / ext-unbuilt.
# ---------------------------------------------------------------------------

_MODE_SHORT = {
    "root_flat_gather_poll": "rootflatgatherpoll",
    "when_all_then_reduce": "whenallthenreduce",
    "dataflow_reduce": "dataflowreduce",
}


def _native_smoke_artifact_path(job_id, mode, serve_timeout, root_port, exp_dir=None):
    """Non-clobbering Slice-2a artifact path: mode + serve-timeout + root-port + job id are ALL in the
    filename, so re-running a different mode / timeout / port never overwrites a prior artifact (the
    fixed-filename clobbering flagged in connector_lifetime_hardening.md). Matches gitignore
    exp63_*_hpx.json so it stays a raw gitignored run artifact."""
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    short = _MODE_SHORT.get(mode, mode.replace("_", ""))
    return os.path.join(
        exp_dir,
        f"exp63_nativesmoke_{job_id}_{short}_st{int(serve_timeout)}_p{int(root_port)}_hpx.json")


def native_composition_validation_gate(*, mode, calls, x, n, root_locality,
                                       declared_remote_localities, connectors, error, expected_k,
                                       late_parcel_after_shutdown_detected):
    """PURE fail-closed Slice-2a validation gate.

    `calls` is the list of per-call records the driver captured, each a dict with:
      wait_for_status, exception_type, timed_out_leaf_count, composite (int or None),
      leaf_localities (list[int]).

    A native composition mode is `cross_node_composition_validated` ONLY if every gate below holds AND
    the mode is HPX-native with no success-path poll (hpx_native and not polled_in_success_path). The
    poll CONTROL (root_flat_gather_poll) can satisfy the mechanics gates but polled_in_success_path is
    True, so cross_node_composition_validated is False for it -- it is a control, not native validation.
    An empty/short call list, any timeout, any exception, a wrong oracle, any root-local leaf, an
    uncovered remote, a non-graceful / serve-timeout connector, or a late parcel all fail closed. No
    performance, ratio, speedup, or Ray-comparison meaning."""
    prov = composition_provenance_for(mode, watchdog=_watchdog_for(mode))
    polled = prov["polled_in_success_path"]
    hpx_native = prov["hpx_native_composition"]

    declared = set(declared_remote_localities)
    conns = connectors or []
    have_calls = len(calls) == expected_k and expected_k > 0

    def _leaves_local(c):
        return sum(1 for loc in c["leaf_localities"] if loc == root_locality)

    def _leaves_remote(c):
        return sum(1 for loc in c["leaf_localities"] if loc != root_locality)

    gates = {
        "all_calls_completed": bool(error is None and have_calls),
        "wait_for_status_ready": bool(have_calls
                                      and all(c["wait_for_status"] == "ready" for c in calls)),
        "no_exception": bool(error is None and have_calls
                             and all(not c.get("exception_type") for c in calls)),
        "no_dispatch_timeout": bool(have_calls
                                    and all(int(c["timed_out_leaf_count"]) == 0 for c in calls)),
        "timed_out_leaf_count_zero": bool(have_calls
                                          and all(int(c["timed_out_leaf_count"]) == 0 for c in calls)),
        "composite_oracle_correct": bool(have_calls and all(
            c["composite"] is not None and int(c["composite"]) == composite_oracle(x, n)
            for c in calls)),
        "leaves_local_zero": bool(have_calls and all(_leaves_local(c) == 0 for c in calls)),
        "leaves_remote_all": bool(have_calls and all(_leaves_remote(c) == n for c in calls)),
        "both_remote_localities_covered": bool(
            have_calls and len(declared) >= 2
            and all(declared.issubset(set(c["leaf_localities"])) for c in calls)),
        "connector_lifecycle_graceful": bool(conns) and all(
            c.get("joined") and c.get("served") and c.get("graceful_disconnect") for c in conns),
        "connector_shutdown_root_completion": bool(conns) and all(
            c.get("connector_shutdown_reason") == "root_completion_signal" for c in conns),
        "no_late_parcel_after_shutdown": late_parcel_after_shutdown_detected is False,
        "fences_locked_false": _fences_locked(fence_block()),
        "same_axis_comparison_false": True,
    }
    mechanics_ok = all(gates.values())
    cross_node_composition_validated = bool(hpx_native and not polled and mechanics_ok)
    return {
        "composition_primitive": mode,
        "hpx_native_composition": hpx_native,
        "polled_in_success_path": polled,
        "watchdog": prov["watchdog"],
        "native_mode_experimental": prov["native_mode_experimental"],
        "gates": gates,
        "mechanics_ok": mechanics_ok,
        "cross_node_composition_validated": cross_node_composition_validated,
    }


def _drive_native_composition(ext, slurm, connector_bin, *, composition_mode, x=7, n=8, k=20,
                              prewarm=5, dispatch_timeout_s=8.0, prefer_subnet="10.42.5.",
                              root_port=7920, await_timeout=60, serve_timeout=90, n_remote=2,
                              heartbeat=True, env=None):
    """Rostam-only Slice-2a driver (UNVALIDATED off-cluster). Bring up the embedded AGAS root + n_remote
    HARDENED connectors ONCE, run `prewarm` warmups then K TIMED calls of the SAFE
    fanout_fanin_remote_diag path for `composition_mode`, capturing per-call {wait_for_status, composite,
    leaf localities, timed_out, exception}. Short-circuits on the FIRST non-ready call (a native stall is
    then <expected_k calls, which fails the validation gate) to spare K*timeout. Hardened lifecycle is on
    by default: heartbeat before EVERY dispatch, completion sentinel only AFTER all calls + capture, so
    connectors stay alive for the whole dispatch window and leave via root_completion_signal."""
    import subprocess
    import tempfile

    env = os.environ if env is None else env
    root_hpx_threads, connector_threads = 4, 8
    hostnames = slurm["hostnames"]
    root_host = hostnames[0]
    remote_hosts = [h for h in hostnames if h != root_host][:n_remote]
    root_ip = _first_ip_for_subnet(prefer_subnet)
    os.makedirs(EXP63_RUNS, exist_ok=True)

    procs, bootdirs = [], []
    started = False
    error = None
    root_loc = None
    root_cpuset = None
    hpx_config = None
    remote_locs = []
    calls = []
    first_anomaly_index = None
    short_circuited = False
    dispatch_elapsed_s = None
    try:
        if len(remote_hosts) < 2:
            raise RuntimeError(f"need >=2 remote hosts distinct from root; got {remote_hosts}")
        if not root_ip:
            raise RuntimeError(f"no root IP for --prefer-subnet {prefer_subnet}")
        root_extra = build_a1_root_hpx_args(root_ip=root_ip, root_port=root_port, variant="a1_normal")
        ext.start(root_hpx_threads, root_extra)
        started = True
        root_loc = int(ext.local_locality_id())
        root_cpuset = _effective_cpuset()
        try:
            hpx_config = dict(ext.hpx_config_provenance())
        except Exception:  # noqa: BLE001
            hpx_config = None
        for idx, rhost in enumerate(remote_hosts):
            bd = tempfile.mkdtemp(
                prefix=f"native_{slurm['slurm_job_id']}_{_MODE_SHORT.get(composition_mode, 'mode')}_"
                       f"c{idx + 1}_", dir=EXP63_RUNS)
            bootdirs.append(bd)
            cmd = build_connector_srun_cmd(
                rhost, bd, connector_bin=connector_bin, connector_threads=connector_threads,
                serve_timeout=serve_timeout, prefer_subnet=prefer_subnet, root_ip=root_ip,
                root_port=root_port)
            procs.append(subprocess.Popen(cmd))
        joined = int(ext.await_remotes(len(remote_hosts), await_timeout))
        if joined < len(remote_hosts):
            raise RuntimeError(f"only {joined} of {len(remote_hosts)} remote localities joined")
        remote_locs = [int(v) for v in ext.remote_locality_ids()]
        for _ in range(prewarm):
            if heartbeat:
                _touch_heartbeat(bootdirs)  # keep connectors alive across the (untimed) warmup
            ext.fanout_fanin_remote_diag(x, n, dispatch_timeout_s, composition_mode, True)
        t_all0 = time.perf_counter_ns()
        for call_idx in range(k):
            if heartbeat:
                _touch_heartbeat(bootdirs)  # reset the connector deadman BEFORE each dispatch
            d = dict(ext.fanout_fanin_remote_diag(x, n, dispatch_timeout_s, composition_mode, True))
            composite = d.get("value")
            rec = {
                "call_index": call_idx,
                "wait_for_status": d.get("wait_for_status", "unknown"),
                "exception_type": d.get("exception_type") or "",
                "exception_stage": d.get("exception_stage") or "none",
                "timed_out_leaf_count": int(d.get("timed_out_leaf_count", n)),
                "composite": (int(composite) if composite is not None else None),
                "composite_oracle_correct": (composite is not None
                                             and int(composite) == composite_oracle(x, n)),
                "leaf_localities": [int(loc) for (_i, _v, loc) in d.get("leaves", [])],
                "n_localities": int(d.get("n_localities", 0)),
            }
            calls.append(rec)
            if rec["wait_for_status"] != "ready" or rec["exception_type"] \
                    or rec["timed_out_leaf_count"] != 0:
                first_anomaly_index = call_idx
                short_circuited = call_idx < k - 1
                break
        dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    finally:
        # Signal ROOT COMPLETION (all calls done, artifact about to be built, no more dispatches) so
        # connectors leave via root_completion_signal. heartbeat=False leaves it unwritten (CONTROL).
        if heartbeat:
            _write_completion(bootdirs)
        for p in procs:
            try:
                p.wait(timeout=max(10, serve_timeout))
            except Exception:  # noqa: BLE001
                p.kill()
        if started:
            try:
                ext.shutdown()
            except Exception as exc:  # noqa: BLE001
                error = error or f"shutdown: {type(exc).__name__}: {exc}"

    connectors = []
    for idx, bd in enumerate(bootdirs):
        att = _read_json(os.path.join(bd, "attest_connect.json"))
        disc = _read_json(os.path.join(bd, "connect.disconnected1"))
        rec = {
            "bootstrap_dir": os.path.basename(bd),
            "hostname": _short_host(att.get("hostname")
                                    or (remote_hosts[idx] if idx < len(remote_hosts) else None)),
            "locality_id": att.get("locality_id"),
            "connector_cpuset_effective": att.get("connector_cpuset_effective"),
            "tcp_nodelay_verified": bool(att.get("tcp_nodelay_attested") and att.get("tcp_nodelay")),
            "joined": os.path.isfile(os.path.join(bd, "connect.joined1")),
            "served": os.path.isfile(os.path.join(bd, "connect.disconnected1"))
            and bool(disc.get("served")),
            "graceful_disconnect": os.path.isfile(os.path.join(bd, "connect.disconnected1"))
            and bool(disc.get("served")),
        }
        rec.update(classify_connector_shutdown(disc))
        connectors.append(rec)

    # Late-parcel inference (same signature as the A1 diagnostic): a before_dispatch system_error co-
    # occurring with any serve-timeout-expired connector is the serve-window race. Under the hardened
    # contract this should be False. INFERENCE from two observed facts, not a direct connector capture.
    last_anom = calls[-1] if (calls and calls[-1]["wait_for_status"] != "ready") else {}
    late_parcel_after_shutdown_detected = bool(
        (last_anom.get("exception_stage") == "before_dispatch")
        and ("system_error" in (last_anom.get("exception_type") or ""))
        and any(c.get("serve_timeout_expired") for c in connectors))

    gate = native_composition_validation_gate(
        mode=composition_mode, calls=calls, x=x, n=n, root_locality=root_loc,
        declared_remote_localities=remote_locs, connectors=connectors, error=error, expected_k=k,
        late_parcel_after_shutdown_detected=late_parcel_after_shutdown_detected)

    # Aggregate wait_for_status: ready iff every call ready; else the first non-ready status.
    if calls and all(c["wait_for_status"] == "ready" for c in calls):
        agg_wait = "ready"
    elif calls:
        agg_wait = next((c["wait_for_status"] for c in calls if c["wait_for_status"] != "ready"),
                        "unknown")
    else:
        agg_wait = "unknown"

    return {
        "experiment_id": EXPERIMENT_ID,
        "slice": "2a_native_composition_retest",
        "kind": "native_composition_smoke",
        "composition_primitive": composition_mode,
        "hpx_native_composition": gate["hpx_native_composition"],
        "polled_in_success_path": gate["polled_in_success_path"],
        "watchdog": gate["watchdog"],
        "native_mode_experimental": gate["native_mode_experimental"],
        "same_axis_comparison": False,
        "n": n, "k": k, "prewarm": prewarm, "dispatch_timeout_s": dispatch_timeout_s,
        "calls_completed": len(calls),
        "calls_ready": sum(1 for c in calls if c["wait_for_status"] == "ready"),
        "first_anomaly_index": first_anomaly_index,
        "short_circuited": short_circuited,
        "wait_for_status": agg_wait,
        "no_dispatch_timeout": gate["gates"]["no_dispatch_timeout"],
        "timed_out_leaf_count": max((c["timed_out_leaf_count"] for c in calls), default=n),
        "composite_oracle_correct": gate["gates"]["composite_oracle_correct"],
        "dispatch_elapsed_s": dispatch_elapsed_s,
        "calls": calls,
        # connector-lifetime (hardened). connector_lifetime_mode is the DRIVER's policy label; the
        # per-connector records carry the CONNECTOR-reported mode (classify_connector_shutdown).
        "connector_lifetime_mode": "heartbeat_root_completion",
        "connector_lifetime_mode_connector_reported": CONNECTOR_LIFETIME_MODE,
        "heartbeat_enabled": heartbeat,
        "serve_timeout_s": serve_timeout,
        "connectors": connectors,
        "connector_shutdown_reasons": [c.get("connector_shutdown_reason") for c in connectors],
        "connector_shutdown_reason": (connectors[0].get("connector_shutdown_reason")
                                      if connectors else "unknown"),
        "root_completion_signaled": bool(connectors)
        and all(c.get("root_completion_signaled") for c in connectors),
        "connector_stayed_alive_until_root_done": bool(connectors)
        and all(c.get("connector_stayed_alive_until_root_done") for c in connectors),
        "serve_timeout_expired_any": any(c.get("serve_timeout_expired") for c in connectors),
        "late_parcel_after_shutdown_detected": late_parcel_after_shutdown_detected,
        # placement / topology
        "node_set": [_short_host(root_host)] + [c["hostname"] for c in connectors],
        "root_locality": root_loc,
        "remote_locality_ids": remote_locs,
        "root_effective_cpuset": root_cpuset,
        "root_port": root_port,
        "prefer_subnet": prefer_subnet,
        "hpx_config": hpx_config,
        # verdict
        "validation_gate": gate["gates"],
        "mechanics_ok": gate["mechanics_ok"],
        "cross_node_composition_validated": gate["cross_node_composition_validated"],
        "fences": fence_block(),
        "error": error,
        "slurm_job_id": slurm["slurm_job_id"],
    }


def run_native_composition_smoke(*, composition_mode="when_all_then_reduce", env=None, import_fn=None,
                                 write=True, x=7, n=8, k=20, prewarm=5, dispatch_timeout_s=8.0,
                                 prefer_subnet="10.42.5.", root_port=7920, await_timeout=60,
                                 serve_timeout=90, n_remote=2, heartbeat=True):
    """Slice-2a native-composition retest under the hardened connector-lifetime contract. Skips cleanly
    off-cluster / ext-unbuilt / for an unsupported mode. Rostam-only. Returns (status, artifact): status
    is 'pass' when the run's pass criterion holds -- for a NATIVE mode, cross_node_composition_validated;
    for the poll CONTROL, mechanics_ok -- else 'fail'; 'skip' off-cluster/unbuilt."""
    if composition_mode not in _COMPOSITION_DEFAULTS or composition_mode not in _DIAG_CONFIG_ID:
        print(f"exp63 native-composition-smoke: unknown/unsupported composition_mode "
              f"{composition_mode!r} (choices: {list(_DIAG_CONFIG_ID)})")
        return "fail", None
    pre, reason = _progress_preconditions(env, import_fn)
    if reason:
        print(f"exp63 native-composition-smoke[{composition_mode}]: {reason}")
        return "skip", None
    ext, slurm, connector_bin = pre
    art = _drive_native_composition(
        ext, slurm, connector_bin, composition_mode=composition_mode, x=x, n=n, k=k, prewarm=prewarm,
        dispatch_timeout_s=dispatch_timeout_s, prefer_subnet=prefer_subnet, root_port=root_port,
        await_timeout=await_timeout, serve_timeout=serve_timeout, n_remote=n_remote,
        heartbeat=heartbeat, env=env)
    native = art["hpx_native_composition"] and not art["polled_in_success_path"]
    passed = art["cross_node_composition_validated"] if native else art["mechanics_ok"]
    print(f"exp63 native-composition-smoke[{composition_mode}]: "
          f"calls_completed={art['calls_completed']}/{k} wait_for_status={art['wait_for_status']} "
          f"composite_oracle_correct={art['composite_oracle_correct']} "
          f"no_dispatch_timeout={art['no_dispatch_timeout']} "
          f"timed_out_leaf_count={art['timed_out_leaf_count']} error={art['error']}")
    print(f"exp63 native-composition-smoke[{composition_mode}]: "
          f"connector_shutdown_reasons={art['connector_shutdown_reasons']} "
          f"root_completion_signaled={art['root_completion_signaled']} "
          f"connectors_stayed_alive={art['connector_stayed_alive_until_root_done']} "
          f"late_parcel={art['late_parcel_after_shutdown_detected']} "
          f"cross_node_composition_validated={art['cross_node_composition_validated']}")
    if write:
        path = _native_smoke_artifact_path(art["slurm_job_id"], composition_mode, serve_timeout,
                                           root_port)
        with open(path, "w") as fh:
            json.dump(art, fh, indent=2, sort_keys=True)
        print(f"exp63 native-composition-smoke[{composition_mode}]: wrote {os.path.basename(path)} "
              f"(gitignored)")
    return ("pass" if passed else "fail"), art


# ---------------------------------------------------------------------------
# Off-cluster clean skip (collective / tree phases -- still Slice-0 stubs).
# ---------------------------------------------------------------------------

def cluster_available(env=None):
    env = os.environ if env is None else env
    return bool(env.get("SLURM_JOB_ID")) and bool(env.get("SLURM_JOB_NODELIST"))


def skip_reason(env=None):
    if cluster_available(env):
        return None
    return ("no SLURM_JOB_ID/SLURM_JOB_NODELIST: off-cluster; exp63 distributed phases skip cleanly")


def _default_ext_import():
    # Slice 0 ships no C++/pybind. The collective ext lands in a later slice.
    raise ImportError("exp63 collective ext not built (Slice 0 is pure-Python scaffold)")


def _ext_available(import_fn):
    try:
        (import_fn or _default_ext_import)()
        return True, None
    except Exception as e:  # noqa: BLE001 -- any import failure means "unavailable", skip cleanly.
        return False, str(e)


def run_hpx_collective_local_smoke(*, env=None, import_fn=None, write=True):
    """Slice 1 (future) in-process single-locality collective smoke. Correctness-only; NOT a stall
    test (no parcelport). Slice 0 always skips: needs a native ext that does not exist yet."""
    ok, err = _ext_available(import_fn)
    if not ok:
        print(f"exp63 hpx-collective-local-smoke: SKIP -- collective ext unavailable: {err}")
        return "skip", None
    print("exp63 hpx-collective-local-smoke: SKIP -- Slice 0 has no native collective yet.")
    return "skip", None


def run_hpx_collective_remote_smoke(*, env=None, import_fn=None, write=True):
    """Slice 2 (future) cross-locality collective dry run. Slice 0 always skips: needs cluster + a
    native ext that does not exist yet."""
    reason = skip_reason(env)
    if reason:
        print(f"exp63 hpx-collective-remote-smoke: SKIP -- {reason}")
        return "skip", None
    ok, err = _ext_available(import_fn)
    if not ok:
        print(f"exp63 hpx-collective-remote-smoke: SKIP -- collective ext unavailable: {err}")
        return "skip", None
    print("exp63 hpx-collective-remote-smoke: SKIP -- Slice 0 has no native collective yet.")
    return "skip", None


# ---------------------------------------------------------------------------
# Slice 2b -- depth-2 STAR-of-partials fan-in (NOT a k-ary tree) under the hardened lifetime contract.
#
# TOPOLOGY (honest): a depth-2 STAR / root-of-partials. Each of the r remote localities folds its OWN
# contiguous leaf block and returns ONE partial; the root composes the r remote partial futures with a
# VALIDATED native wait (dataflow_reduce default / when_all_then_reduce). Flat native composes N leaf
# futures; root-of-partials composes r remote partial futures -- this structurally bounds the root
# fan-in by remote-locality count. At N=8, r=2 that is MECHANISM / TOPOLOGY evidence, NOT a performance
# result (no latency, no ratio, no differencing).
#
# WHY HAND-ROLLED, NOT hpx::collectives::reduce: no fixed communicator, no generation / membership
# state, one action future per remote locality, and a departed locality surfaces as an EXCEPTION
# through its future -- lower risk than a collective under connect-mode dynamic membership. Collectives
# stay later. This is NOT a new progress proof: it rides the SAME native wait + TCP-parcelport substrate
# already validated in Slice 2a; the wait_for(timeout) is a BOUNDED HARNESS WATCHDOG (not the
# success-path poll), and on timeout the outstanding partial actions are abandoned in flight (caveat).
# HPX-only, closed-int64, no Ray, no payload, no collectives, no same-axis, no performance claim.
# ---------------------------------------------------------------------------

def _partition_blocks(n, r):
    """Contiguous non-empty block partition of [0, n) into r blocks: the first (n % r) blocks get one
    extra leaf. Returns [(i_begin, i_count), ...]. Mirrors the C++ partition EXACTLY. Requires r >= 1
    and n >= r so every block is non-empty."""
    if r < 1:
        raise ValueError("r must be >= 1")
    if n < r:
        raise ValueError("n must be >= r so every block is non-empty")
    base, rem = divmod(n, r)
    blocks, i_begin = [], 0
    for j in range(r):
        i_count = base + (1 if j < rem else 0)
        blocks.append((i_begin, i_count))
        i_begin += i_count
    return blocks


def _local_partial_oracle(x, i_begin, i_count):
    """Closed int64 fold of leaf_value over the contiguous block [i_begin, i_begin + i_count),
    accumulated mod 2^64 (order-independent) -- matches the C++ exp63_partial."""
    acc = 0
    for k in range(i_count):
        acc = u64(acc + leaf_value(x, i_begin + k))
    return i64(acc)


def _tree_smoke_artifact_path(job_id, partial_collect_wait, serve_timeout, root_port, exp_dir=None):
    """Non-clobbering Slice-2b artifact path: partial_collect_wait + serve-timeout + root-port + job id
    are ALL in the filename. Matches gitignore exp63_*_hpx.json."""
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    short = _MODE_SHORT.get(partial_collect_wait, partial_collect_wait.replace("_", ""))
    return os.path.join(
        exp_dir,
        f"exp63_treesmoke_{job_id}_{short}_st{int(serve_timeout)}_p{int(root_port)}_hpx.json")


def tree_of_partials_validation_gate(*, partial_collect_wait, calls, x, n, root_locality,
                                     declared_remote_localities, connectors, error, expected_k,
                                     late_parcel_after_shutdown_detected):
    """PURE fail-closed Slice-2b validation gate for the depth-2 star-of-partials fan-in.

    `calls` is the per-call list the driver captured, each a dict with:
      wait_for_status, exception_type, timed_out_partial_count, composite (int or None),
      partials (list of {partial_sum, i_begin, i_count, locality}).

    cross_node_composition_validated is True ONLY if every gate below holds AND the mode is HPX-native
    with no success-path poll. On top of the Slice-2a mechanics (all K ready, no timeout/exception,
    correct composite, graceful root-completion lifecycle, no late parcel, fences), it adds the
    STAR-topology structural gates: partials all remote, one partial per remote locality, blocks tile
    [0, n) exactly once, and each local partial oracle correct. Any missing locality, duplicate /
    uncovered index, wrong local partial, wrong composite, root-local partial, poll path, serve-timeout
    connector, stall, or late parcel fails closed."""
    prov = composition_provenance_for("tree_of_partials", watchdog=_watchdog_for("tree_of_partials"))
    polled = prov["polled_in_success_path"]        # False for tree_of_partials
    hpx_native = prov["hpx_native_composition"]     # True for tree_of_partials
    declared = set(declared_remote_localities)
    r = len(declared)
    conns = connectors or []
    have_calls = len(calls) == expected_k and expected_k > 0

    def _partials(c):
        return c.get("partials", [])

    def _all_remote(c):
        ps = _partials(c)
        return bool(ps) and all(int(p["locality"]) != root_locality for p in ps)

    def _each_remote_once(c):
        locs = sorted(int(p["locality"]) for p in _partials(c))
        return locs == sorted(declared) and all(int(p["i_count"]) >= 1 for p in _partials(c))

    def _covered_once(c):
        covered = []
        for p in _partials(c):
            b, cnt = int(p["i_begin"]), int(p["i_count"])
            covered.extend(range(b, b + cnt))
        return sorted(covered) == list(range(n))

    def _local_oracles(c):
        return all(int(p["partial_sum"]) == _local_partial_oracle(x, int(p["i_begin"]),
                                                                   int(p["i_count"]))
                   for p in _partials(c))

    gates = {
        "all_calls_completed": bool(error is None and have_calls),
        "wait_for_status_ready": bool(have_calls
                                      and all(c["wait_for_status"] == "ready" for c in calls)),
        "no_exception": bool(error is None and have_calls
                             and all(not c.get("exception_type") for c in calls)),
        "no_dispatch_timeout": bool(have_calls
                                    and all(int(c["timed_out_partial_count"]) == 0 for c in calls)),
        "partials_all_remote": bool(have_calls and all(_all_remote(c) for c in calls)),
        "partials_count_matches_remotes": bool(
            have_calls and r >= 2 and all(len(_partials(c)) == r for c in calls)),
        "all_remote_localities_contributed": bool(have_calls and r >= 2
                                                  and all(_each_remote_once(c) for c in calls)),
        "leaf_indices_covered_once": bool(have_calls and all(_covered_once(c) for c in calls)),
        "local_partial_oracles_correct": bool(have_calls and all(_local_oracles(c) for c in calls)),
        "composite_oracle_correct": bool(have_calls and all(
            c["composite"] is not None and int(c["composite"]) == composite_oracle(x, n)
            for c in calls)),
        "connector_lifecycle_graceful": bool(conns) and all(
            c.get("joined") and c.get("served") and c.get("graceful_disconnect") for c in conns),
        "connector_shutdown_root_completion": bool(conns) and all(
            c.get("connector_shutdown_reason") == "root_completion_signal" for c in conns),
        "no_late_parcel_after_shutdown": late_parcel_after_shutdown_detected is False,
        "fences_locked_false": _fences_locked(fence_block()),
        "same_axis_comparison_false": True,
    }
    mechanics_ok = all(gates.values())
    cross_node_composition_validated = bool(hpx_native and not polled and mechanics_ok)
    return {
        "composition_primitive": "tree_of_partials",
        "partial_topology": "depth2_star_of_partials_contiguous_blocks",
        "partial_collect_wait": partial_collect_wait,
        "hpx_native_composition": hpx_native,
        "polled_in_success_path": polled,
        "watchdog": prov["watchdog"],
        "native_mode_experimental": prov["native_mode_experimental"],
        "root_reduces_partial_count": r,
        "gates": gates,
        "mechanics_ok": mechanics_ok,
        "cross_node_composition_validated": cross_node_composition_validated,
    }


def _drive_tree_of_partials(ext, slurm, connector_bin, *, partial_collect_wait, x=7, n=8, k=20,
                            prewarm=5, dispatch_timeout_s=8.0, prefer_subnet="10.42.5.",
                            root_port=7930, await_timeout=60, serve_timeout=90, n_remote=2,
                            heartbeat=True, env=None):
    """Rostam-only Slice-2b driver (UNVALIDATED off-cluster). Bring up the embedded AGAS root + n_remote
    HARDENED connectors ONCE, run `prewarm` warmups then K TIMED calls of the SAFE
    fanout_fanin_tree_remote_diag path (r partial actions collected by the selected native wait),
    capturing per-call {wait_for_status, composite, partials, timed_out, exception}. Short-circuits on
    the FIRST non-ready call. Hardened lifecycle on by default: heartbeat before EVERY dispatch,
    completion sentinel only AFTER all calls + capture."""
    import subprocess
    import tempfile

    env = os.environ if env is None else env
    root_hpx_threads, connector_threads = 4, 8
    hostnames = slurm["hostnames"]
    root_host = hostnames[0]
    remote_hosts = [h for h in hostnames if h != root_host][:n_remote]
    root_ip = _first_ip_for_subnet(prefer_subnet)
    os.makedirs(EXP63_RUNS, exist_ok=True)

    procs, bootdirs = [], []
    started = False
    error = None
    root_loc = None
    root_cpuset = None
    hpx_config = None
    remote_locs = []
    calls = []
    first_anomaly_index = None
    short_circuited = False
    dispatch_elapsed_s = None
    try:
        if len(remote_hosts) < 2:
            raise RuntimeError(f"need >=2 remote hosts distinct from root; got {remote_hosts}")
        if not root_ip:
            raise RuntimeError(f"no root IP for --prefer-subnet {prefer_subnet}")
        root_extra = build_a1_root_hpx_args(root_ip=root_ip, root_port=root_port, variant="a1_normal")
        ext.start(root_hpx_threads, root_extra)
        started = True
        root_loc = int(ext.local_locality_id())
        root_cpuset = _effective_cpuset()
        try:
            hpx_config = dict(ext.hpx_config_provenance())
        except Exception:  # noqa: BLE001
            hpx_config = None
        for idx, rhost in enumerate(remote_hosts):
            bd = tempfile.mkdtemp(
                prefix=f"tree_{slurm['slurm_job_id']}_{_MODE_SHORT.get(partial_collect_wait, 'wait')}_"
                       f"c{idx + 1}_", dir=EXP63_RUNS)
            bootdirs.append(bd)
            cmd = build_connector_srun_cmd(
                rhost, bd, connector_bin=connector_bin, connector_threads=connector_threads,
                serve_timeout=serve_timeout, prefer_subnet=prefer_subnet, root_ip=root_ip,
                root_port=root_port)
            procs.append(subprocess.Popen(cmd))
        joined = int(ext.await_remotes(len(remote_hosts), await_timeout))
        if joined < len(remote_hosts):
            raise RuntimeError(f"only {joined} of {len(remote_hosts)} remote localities joined")
        remote_locs = [int(v) for v in ext.remote_locality_ids()]
        for _ in range(prewarm):
            if heartbeat:
                _touch_heartbeat(bootdirs)
            ext.fanout_fanin_tree_remote_diag(x, n, dispatch_timeout_s, partial_collect_wait, True)
        t_all0 = time.perf_counter_ns()
        for call_idx in range(k):
            if heartbeat:
                _touch_heartbeat(bootdirs)  # reset the connector deadman BEFORE each dispatch
            d = dict(ext.fanout_fanin_tree_remote_diag(x, n, dispatch_timeout_s, partial_collect_wait,
                                                       True))
            composite = d.get("value")
            partials = [{"partial_sum": int(p[0]), "i_begin": int(p[1]), "i_count": int(p[2]),
                         "locality": int(p[3])} for p in d.get("partials", [])]
            rec = {
                "call_index": call_idx,
                "wait_for_status": d.get("wait_for_status", "unknown"),
                "exception_type": d.get("exception_type") or "",
                "exception_stage": d.get("exception_stage") or "none",
                "timed_out_partial_count": int(d.get("timed_out_partial_count", n_remote)),
                "composite": (int(composite) if composite is not None else None),
                "composite_oracle_correct": (composite is not None
                                             and int(composite) == composite_oracle(x, n)),
                "partials": partials,
                "partial_localities": [p["locality"] for p in partials],
                "n_localities": int(d.get("n_localities", 0)),
            }
            calls.append(rec)
            if rec["wait_for_status"] != "ready" or rec["exception_type"] \
                    or rec["timed_out_partial_count"] != 0:
                first_anomaly_index = call_idx
                short_circuited = call_idx < k - 1
                break
        dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    finally:
        if heartbeat:
            _write_completion(bootdirs)
        for p in procs:
            try:
                p.wait(timeout=max(10, serve_timeout))
            except Exception:  # noqa: BLE001
                p.kill()
        if started:
            try:
                ext.shutdown()
            except Exception as exc:  # noqa: BLE001
                error = error or f"shutdown: {type(exc).__name__}: {exc}"

    connectors = []
    for idx, bd in enumerate(bootdirs):
        att = _read_json(os.path.join(bd, "attest_connect.json"))
        disc = _read_json(os.path.join(bd, "connect.disconnected1"))
        rec = {
            "bootstrap_dir": os.path.basename(bd),
            "hostname": _short_host(att.get("hostname")
                                    or (remote_hosts[idx] if idx < len(remote_hosts) else None)),
            "locality_id": att.get("locality_id"),
            "connector_cpuset_effective": att.get("connector_cpuset_effective"),
            "tcp_nodelay_verified": bool(att.get("tcp_nodelay_attested") and att.get("tcp_nodelay")),
            "joined": os.path.isfile(os.path.join(bd, "connect.joined1")),
            "served": os.path.isfile(os.path.join(bd, "connect.disconnected1"))
            and bool(disc.get("served")),
            "graceful_disconnect": os.path.isfile(os.path.join(bd, "connect.disconnected1"))
            and bool(disc.get("served")),
        }
        rec.update(classify_connector_shutdown(disc))
        connectors.append(rec)

    last_anom = calls[-1] if (calls and calls[-1]["wait_for_status"] != "ready") else {}
    late_parcel_after_shutdown_detected = bool(
        (last_anom.get("exception_stage") == "before_dispatch")
        and ("system_error" in (last_anom.get("exception_type") or ""))
        and any(c.get("serve_timeout_expired") for c in connectors))

    gate = tree_of_partials_validation_gate(
        partial_collect_wait=partial_collect_wait, calls=calls, x=x, n=n, root_locality=root_loc,
        declared_remote_localities=remote_locs, connectors=connectors, error=error, expected_k=k,
        late_parcel_after_shutdown_detected=late_parcel_after_shutdown_detected)

    if calls and all(c["wait_for_status"] == "ready" for c in calls):
        agg_wait = "ready"
    elif calls:
        agg_wait = next((c["wait_for_status"] for c in calls if c["wait_for_status"] != "ready"),
                        "unknown")
    else:
        agg_wait = "unknown"

    # Topology witness from the last call that produced partials (structural provenance only).
    witness_call = next((c for c in reversed(calls) if c["partials"]), None)
    partial_leaf_counts = [p["i_count"] for p in witness_call["partials"]] if witness_call else []
    partial_locality_ids = [p["locality"] for p in witness_call["partials"]] if witness_call else []

    return {
        "experiment_id": EXPERIMENT_ID,
        "slice": "2b_tree_of_partials",
        "kind": "tree_of_partials_smoke",
        "composition_primitive": "tree_of_partials",
        "partial_topology": gate["partial_topology"],
        "partial_collect_wait": partial_collect_wait,
        "hpx_native_composition": gate["hpx_native_composition"],
        "polled_in_success_path": gate["polled_in_success_path"],
        "watchdog": gate["watchdog"],
        "native_mode_experimental": gate["native_mode_experimental"],
        "same_axis_comparison": False,
        "n": n, "k": k, "prewarm": prewarm, "dispatch_timeout_s": dispatch_timeout_s,
        "calls_completed": len(calls),
        "calls_ready": sum(1 for c in calls if c["wait_for_status"] == "ready"),
        "first_anomaly_index": first_anomaly_index,
        "short_circuited": short_circuited,
        "wait_for_status": agg_wait,
        "no_dispatch_timeout": gate["gates"]["no_dispatch_timeout"],
        "timed_out_partial_count": max((c["timed_out_partial_count"] for c in calls), default=n_remote),
        "composite_oracle_correct": gate["gates"]["composite_oracle_correct"],
        "dispatch_elapsed_s": dispatch_elapsed_s,
        "calls": calls,
        # topology witness (structural provenance; NOT a performance measurement)
        "partials_count": len(partial_locality_ids),
        "partial_leaf_counts": partial_leaf_counts,
        "partial_locality_ids": partial_locality_ids,
        "root_reduces_partial_count": gate["root_reduces_partial_count"],
        "all_remote_localities_contributed": gate["gates"]["all_remote_localities_contributed"],
        "leaf_indices_covered_once": gate["gates"]["leaf_indices_covered_once"],
        "local_partial_oracles_correct": gate["gates"]["local_partial_oracles_correct"],
        # connector-lifetime (hardened)
        "connector_lifetime_mode": "heartbeat_root_completion",
        "connector_lifetime_mode_connector_reported": CONNECTOR_LIFETIME_MODE,
        "heartbeat_enabled": heartbeat,
        "serve_timeout_s": serve_timeout,
        "connectors": connectors,
        "connector_shutdown_reasons": [c.get("connector_shutdown_reason") for c in connectors],
        "connector_shutdown_reason": (connectors[0].get("connector_shutdown_reason")
                                      if connectors else "unknown"),
        "root_completion_signaled": bool(connectors)
        and all(c.get("root_completion_signaled") for c in connectors),
        "connector_stayed_alive_until_root_done": bool(connectors)
        and all(c.get("connector_stayed_alive_until_root_done") for c in connectors),
        "serve_timeout_expired_any": any(c.get("serve_timeout_expired") for c in connectors),
        "late_parcel_after_shutdown_detected": late_parcel_after_shutdown_detected,
        # placement / topology
        "node_set": [_short_host(root_host)] + [c["hostname"] for c in connectors],
        "root_locality": root_loc,
        "remote_locality_ids": remote_locs,
        "root_effective_cpuset": root_cpuset,
        "root_port": root_port,
        "prefer_subnet": prefer_subnet,
        "hpx_config": hpx_config,
        # verdict
        "validation_gate": gate["gates"],
        "mechanics_ok": gate["mechanics_ok"],
        "cross_node_composition_validated": gate["cross_node_composition_validated"],
        "fences": fence_block(),
        "error": error,
        "slurm_job_id": slurm["slurm_job_id"],
    }


def run_tree_of_partials_remote_smoke(*, partial_collect_wait="dataflow_reduce", env=None,
                                      import_fn=None, write=True, x=7, n=8, k=20, prewarm=5,
                                      dispatch_timeout_s=8.0, prefer_subnet="10.42.5.", root_port=7930,
                                      await_timeout=60, serve_timeout=90, n_remote=2, heartbeat=True):
    """Slice-2b depth-2 star-of-partials fan-in under the hardened connector-lifetime contract. Skips
    cleanly off-cluster / ext-unbuilt / for an unsupported collect wait. Rostam-only. Returns
    (status, artifact): 'pass' iff cross_node_composition_validated, else 'fail'; 'skip' otherwise."""
    if partial_collect_wait not in ("dataflow_reduce", "when_all_then_reduce"):
        print(f"exp63 tree-of-partials-remote-smoke: unknown/unsupported partial_collect_wait "
              f"{partial_collect_wait!r} (choices: ['dataflow_reduce', 'when_all_then_reduce'])")
        return "fail", None
    pre, reason = _progress_preconditions(env, import_fn)
    if reason:
        print(f"exp63 tree-of-partials-remote-smoke[{partial_collect_wait}]: {reason}")
        return "skip", None
    ext, slurm, connector_bin = pre
    art = _drive_tree_of_partials(
        ext, slurm, connector_bin, partial_collect_wait=partial_collect_wait, x=x, n=n, k=k,
        prewarm=prewarm, dispatch_timeout_s=dispatch_timeout_s, prefer_subnet=prefer_subnet,
        root_port=root_port, await_timeout=await_timeout, serve_timeout=serve_timeout,
        n_remote=n_remote, heartbeat=heartbeat, env=env)
    passed = art["cross_node_composition_validated"]
    print(f"exp63 tree-of-partials-remote-smoke[{partial_collect_wait}]: "
          f"calls_completed={art['calls_completed']}/{k} wait_for_status={art['wait_for_status']} "
          f"partials_count={art['partials_count']} root_reduces_partial_count="
          f"{art['root_reduces_partial_count']} covered_once={art['leaf_indices_covered_once']} "
          f"local_partials_ok={art['local_partial_oracles_correct']} "
          f"composite_oracle_correct={art['composite_oracle_correct']} error={art['error']}")
    print(f"exp63 tree-of-partials-remote-smoke[{partial_collect_wait}]: "
          f"partial_locality_ids={art['partial_locality_ids']} "
          f"connector_shutdown_reasons={art['connector_shutdown_reasons']} "
          f"root_completion_signaled={art['root_completion_signaled']} "
          f"late_parcel={art['late_parcel_after_shutdown_detected']} "
          f"cross_node_composition_validated={art['cross_node_composition_validated']}")
    if write:
        path = _tree_smoke_artifact_path(art["slurm_job_id"], partial_collect_wait, serve_timeout,
                                         root_port)
        with open(path, "w") as fh:
            json.dump(art, fh, indent=2, sort_keys=True)
        print(f"exp63 tree-of-partials-remote-smoke[{partial_collect_wait}]: wrote "
              f"{os.path.basename(path)} (gitignored)")
    return ("pass" if passed else "fail"), art


# ---------------------------------------------------------------------------
# CLI. Slice 0 wires only --phase selftest to real work; every other phase skips cleanly.
# ---------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(
        description="exp63 HPX-native collective/tree reduction (Slice 1b: HPX progress root-cause)")
    ap.add_argument("--phase",
                    choices=("selftest", "smoke", "progress-diagnosis", "progress-sweep",
                             "a1-diagnostic", "native-composition-smoke",
                             "hpx-collective-local-smoke", "hpx-collective-remote-smoke",
                             "tree-of-partials-remote-smoke"),
                    default="selftest",
                    help="selftest runs the pure layer; progress-diagnosis/progress-sweep drive the "
                         "HPX 3-node progress micro-slice; a1-diagnostic runs the INSTRUMENTED A1 "
                         "rerun; native-composition-smoke runs the Slice-2a native-composition retest "
                         "(when_all_then_reduce / dataflow_reduce, poll control) under the hardened "
                         "connector-lifetime contract (all skip cleanly off-cluster/ext-unbuilt); the "
                         "collective/tree phases remain Slice-0 skip stubs.")
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--n", type=int, default=8, help="inner fanout N")
    ap.add_argument("--k", type=int, default=20, help="timed calls (dry-run small K)")
    ap.add_argument("--prewarm", type=int, default=5, help="warmup calls before timing")
    ap.add_argument("--config-id", default="A1",
                    help="progress-diagnosis: which sweep config (A0..A5, or B0/B1)")
    ap.add_argument("--include-phase-b", action="store_true",
                    help="progress-sweep: also run the deferred Phase-B (TCP tuning / MPI-IB) configs")
    ap.add_argument("--dispatch-timeout-s", type=float, default=8.0,
                    help="bounded per-call watchdog (fail closed; poll control finishes in ~us)")
    ap.add_argument("--prefer-subnet", default="10.42.5.")
    ap.add_argument("--root-port", type=int, default=7912, help="embedded-root AGAS/parcelport port")
    ap.add_argument("--await-timeout", type=int, default=90)
    ap.add_argument("--serve-timeout", type=int, default=180)
    ap.add_argument("--n-remote", type=int, default=2, help="remote connector count (>=2 for 4/4)")
    ap.add_argument("--diagnostic-variant", default="a1_normal", choices=DIAGNOSTIC_VARIANTS,
                    help="a1-diagnostic: normal | root_bind_none | max_bg_threads | roomier_cpuset")
    ap.add_argument("--max-bg-threads", type=int, default=8,
                    help="a1-diagnostic a1_max_bg_threads: hpx.max_background_threads override")
    ap.add_argument("--composition-mode", default="when_all_then_reduce",
                    choices=("root_flat_gather_poll", "when_all_then_reduce", "dataflow_reduce"),
                    help="a1-diagnostic composition: root_flat_gather_poll (A0 CONTROL) | "
                         "when_all_then_reduce (A1) | dataflow_reduce (A5), same instrumented path")
    ap.add_argument("--no-completion-signal", action="store_true",
                    help="a1-diagnostic CONTROL: disable the root heartbeat + completion sentinel so a "
                         "connector must leave via its serve-timeout DEADMAN (proves the "
                         "serve_timeout_expired classification). Default: hardened (heartbeat on).")
    ap.add_argument("--partial-collect-wait", default="dataflow_reduce",
                    choices=("dataflow_reduce", "when_all_then_reduce"),
                    help="tree-of-partials-remote-smoke: the validated native wait the root uses to "
                         "collect the r remote partials. dataflow_reduce (default, fused HPX form) or "
                         "when_all_then_reduce.")
    args = ap.parse_args(argv)

    if args.phase == "selftest":
        import selftest_slice0
        return selftest_slice0.main()

    if args.phase == "smoke":
        reason = skip_reason()
        if reason:
            print(f"exp63 smoke: SKIP -- {reason}")
        else:
            print("exp63 smoke: cluster context present; use --phase progress-diagnosis / "
                  "progress-sweep for the HPX 3-node progress micro-slice (needs the built ext).")
        return 0

    if args.phase == "progress-diagnosis":
        status, _ = run_progress_diagnosis(
            config_id=args.config_id, x=args.x, n=args.n, k=args.k, prewarm=args.prewarm,
            dispatch_timeout_s=args.dispatch_timeout_s, prefer_subnet=args.prefer_subnet,
            root_port=args.root_port, await_timeout=args.await_timeout,
            serve_timeout=args.serve_timeout, n_remote=args.n_remote)
        return 1 if status == "fail" else 0

    if args.phase == "progress-sweep":
        status, _ = run_progress_sweep(
            include_phase_b=args.include_phase_b, x=args.x, n=args.n, k=args.k, prewarm=args.prewarm,
            dispatch_timeout_s=args.dispatch_timeout_s, prefer_subnet=args.prefer_subnet,
            root_port=args.root_port, await_timeout=args.await_timeout,
            serve_timeout=args.serve_timeout, n_remote=args.n_remote)
        return 1 if status == "fail" else 0

    if args.phase == "a1-diagnostic":
        status, _ = run_a1_diagnostic(
            variant=args.diagnostic_variant, x=args.x, n=args.n, k=args.k, prewarm=args.prewarm,
            dispatch_timeout_s=args.dispatch_timeout_s, prefer_subnet=args.prefer_subnet,
            root_port=args.root_port, await_timeout=args.await_timeout,
            serve_timeout=args.serve_timeout, n_remote=args.n_remote,
            max_bg_threads=args.max_bg_threads, composition_mode=args.composition_mode,
            heartbeat=not args.no_completion_signal)
        return 1 if status == "fail" else 0

    if args.phase == "native-composition-smoke":
        status, _ = run_native_composition_smoke(
            composition_mode=args.composition_mode, x=args.x, n=args.n, k=args.k,
            prewarm=args.prewarm, dispatch_timeout_s=args.dispatch_timeout_s,
            prefer_subnet=args.prefer_subnet, root_port=args.root_port,
            await_timeout=args.await_timeout, serve_timeout=args.serve_timeout,
            n_remote=args.n_remote, heartbeat=not args.no_completion_signal)
        return 1 if status == "fail" else 0

    if args.phase == "hpx-collective-local-smoke":
        status, _ = run_hpx_collective_local_smoke()
        return 1 if status == "fail" else 0

    if args.phase == "hpx-collective-remote-smoke":
        status, _ = run_hpx_collective_remote_smoke()
        return 1 if status == "fail" else 0

    if args.phase == "tree-of-partials-remote-smoke":
        status, _ = run_tree_of_partials_remote_smoke(
            partial_collect_wait=args.partial_collect_wait, x=args.x, n=args.n, k=args.k,
            prewarm=args.prewarm, dispatch_timeout_s=args.dispatch_timeout_s,
            prefer_subnet=args.prefer_subnet, root_port=args.root_port,
            await_timeout=args.await_timeout, serve_timeout=args.serve_timeout,
            n_remote=args.n_remote, heartbeat=not args.no_completion_signal)
        return 1 if status == "fail" else 0

    print(f"exp63: unhandled phase {args.phase!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
