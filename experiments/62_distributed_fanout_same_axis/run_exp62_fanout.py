"""exp62 -- distributed fanout/fanin at the same Python caller boundary. SLICE 1.

SCOPE / FENCES:
  * Slice 0 helpers are PURE PYTHON (no runtime imports at module load).
  * Slice 1 adds SINGLE-LOCALITY local mechanism smokes only: `hpx-local-smoke` (via the optional
    fanout_ext pybind module) and `ray-local-smoke` (via an optional local Ray). Both import their
    runtime lazily inside the phase and skip cleanly when it is unavailable. This is NOT distributed
    evidence and NOT a performance experiment; there is no connector and no plain action yet.
  * This module holds the closed-int64 oracle, the concurrency-safe reduce-time witness tally,
    placement assignment, fail-closed gates, and the arm/pair/band artifact builders that the later
    runtime slices reuse. The Slice 0 builders make NO performance claim by themselves.
  * It computes nothing about Ray-vs-HPX: no speedup, no ratio, no arm differencing, no placement-band
    differencing. Those four fences are hard-locked False here and stay False downstream.

This is experiment-only measurement scaffolding. It is NOT shipped rayx.runtime API, NOT an object
store, NOT arbitrary remote Python execution, NOT real inference, and NOT a Ray replacement.

Conceptual workload the later slices wire to real runtimes:
    fanout_fanin(x, N) -> int64
  One outer blocking Python call returns one int64. Inner fanout N is the parallelism: N leaf actions
  distributed across localities, then reduced to one scalar. Outer regime is QD1 (one composite call
  in flight at a time).
"""

import argparse
import json
import math
import os
import sys
import time
import uuid

EXPERIMENT_ID = "exp62"
LEAF_XOR = 0x52415958  # "RAYX" -- shared closed-int64 constant with exp61's dist-probe oracle.
REDUCE_OP = "sum_int64"

_MASK64 = (1 << 64) - 1


# ---------------------------------------------------------------------------
# int64 / u64 wrapping.
# ---------------------------------------------------------------------------

def u64(v):
    """Wrap to unsigned 64-bit."""
    return int(v) & _MASK64


def i64(v):
    """Wrap to signed 64-bit two's complement. The closed value model is int64."""
    v = int(v) & _MASK64
    return v - (1 << 64) if v >= (1 << 63) else v


# ---------------------------------------------------------------------------
# Oracle (closed int64). Placement-INDEPENDENT by construction.
# ---------------------------------------------------------------------------

def leaf_value(x, i):
    """Per-leaf oracle: (x ^ 0x52415958) + (i << 1), wrapped to int64.

    Depends ONLY on payload x and leaf index i -- never on which locality runs the leaf. That is what
    makes the composite placement-independent and lets a separate witness prove distribution."""
    return i64((int(x) ^ LEAF_XOR) + (int(i) << 1))


def composite_oracle(x, n):
    """Composite oracle: int64 sum of leaf_value(x, i) for i in 0..n-1. Order-independent mod 2**64."""
    if n < 0:
        raise ValueError("n must be >= 0")
    total = 0
    for i in range(n):
        total += (int(x) ^ LEAF_XOR) + (int(i) << 1)
    return i64(total)


# ---------------------------------------------------------------------------
# Placement assignment.
# ---------------------------------------------------------------------------

PLACEMENT_MODES = ("all-remote", "round-robin", "single-locality")
DISTRIBUTED_MODES = ("all-remote", "round-robin")


def placement_class(placement_mode):
    """single-locality is local mechanism smoke; the distributed modes are 'distributed'."""
    return "local_mechanism_smoke" if placement_mode == "single-locality" else "distributed"


def placement_is_distributed(placement_mode):
    """True ONLY for real distributed placement. single-locality is NEVER distributed evidence."""
    return placement_mode in DISTRIBUTED_MODES


def _connector_cpuset_ok(cpuset, min_cpus):
    """Connector cpuset not-collapsed gate. None (not asserted) -> True. Otherwise the effective
    cpuset must be a non-empty list, not exactly [0], with at least min_cpus distinct CPUs."""
    if cpuset is None or min_cpus is None:
        return True
    if not isinstance(cpuset, (list, tuple)) or not cpuset:
        return False
    if list(cpuset) == [0]:
        return False
    return len(set(cpuset)) >= min_cpus


def build_leaf_assignments(n, localities, placement_mode, root_locality):
    """Assign each leaf index 0..n-1 to a locality. Returns list of length n.

      * all-remote (DEFAULT): leaves spread round-robin across the NON-root localities only. The
        root/coordinator runs no leaf; it only fans out and reduces.
      * round-robin (labeled alternative, never default): leaves spread across ALL localities.
      * single-locality (Slice 1 local mechanism smoke): every leaf runs on the single root locality.

    An unknown placement_mode raises ValueError (fail closed). all-remote with no remote locality and
    round-robin with < 2 localities also raise."""
    if placement_mode not in PLACEMENT_MODES:
        raise ValueError(f"unknown placement_mode: {placement_mode!r}")
    if n < 0:
        raise ValueError("n must be >= 0")
    localities = list(localities)
    if placement_mode == "single-locality":
        targets = [root_locality]
    elif placement_mode == "all-remote":
        targets = [loc for loc in localities if loc != root_locality]
        if not targets:
            raise ValueError("all-remote requires >= 1 non-root (remote) locality")
    else:  # round-robin
        targets = localities
        if len(targets) < 2:
            raise ValueError("round-robin requires >= 2 localities")
    return [targets[i % len(targets)] for i in range(n)]


# ---------------------------------------------------------------------------
# Concurrency-safe witness (reduce-time tally over returned leaf records).
#
# Each leaf returns a record { "i": int, "value": int, "locality": int|str }. The single reduce
# continuation (later slices) tallies these AFTER the fact. There is NO shared mutable counter
# incremented concurrently -- distribution is reconstructed from immutable per-leaf records.
# ---------------------------------------------------------------------------

def tally_witness(leaf_records, root_locality):
    """Reduce-time distribution tally. Localities may be ints or strings; both are kept verbatim."""
    per = {}
    for rec in leaf_records:
        loc = rec["locality"]
        per[loc] = per.get(loc, 0) + 1
    total = sum(per.values())
    local = per.get(root_locality, 0)
    return {
        "witness_leaf_count": total,
        "leaves_per_locality": per,
        "leaves_local": local,
        "leaves_remote": total - local,
        "n_localities_covered": len(per),
    }


# ---------------------------------------------------------------------------
# Distribution + thread-cover gates.
# ---------------------------------------------------------------------------

def evaluate_distribution_gate(placement_mode, witness, n):
    """Fail-closed distribution gate.

      * all-remote: leaves_local == 0 and leaves_remote == n.
      * round-robin: n_localities_covered >= 2.
      * single-locality: n_localities_covered == 1 and leaves_local == n. This is an HONEST
        single-locality assertion, NOT a distribution claim and NOT distributed evidence.
      * unknown mode: False (fail closed)."""
    if placement_mode == "single-locality":
        return witness.get("n_localities_covered") == 1 and witness.get("leaves_local") == n
    if placement_mode == "all-remote":
        return witness.get("leaves_local") == 0 and witness.get("leaves_remote") == n
    if placement_mode == "round-robin":
        return witness.get("n_localities_covered", 0) >= 2
    return False


def evaluate_threads_cover_fanout(leaves_per_locality, threads_per_locality,
                                  min_threads_per_locality=1):
    """Anti-starvation: every locality that received >= 1 leaf must have >= min HPX threads. A
    receiving locality missing from the thread map fails closed. Empty fanout fails closed."""
    receiving = list(leaves_per_locality.keys())
    if not receiving:
        return False
    for loc in receiving:
        t = threads_per_locality.get(loc)
        if t is None or t < min_threads_per_locality:
            return False
    return True


def multi_remote_gates(witness, remote_locs, connectors):
    """Slice 4a fail-closed multi-remote gates. PURE function over the reduce-time witness, the
    expected remote locality ids, and the per-connector attest/lifecycle dicts. >=2 remote localities
    are required everywhere, every expected remote must receive >=1 leaf, every connector cpuset must
    be uncollapsed (>= its min, not [0]) and TCP_NODELAY-verified, and every connector must show the
    full joined -> served -> graceful-disconnect lifecycle. HPX-only; never a same-axis gate."""
    lpl = witness.get("leaves_per_locality", {})
    covers_all = bool(remote_locs) and all(lpl.get(loc, 0) >= 1 for loc in remote_locs)
    return {
        "n_remote_localities_ge_2": len(remote_locs) >= 2,
        "leaves_per_remote_locality_covers_all": covers_all and len(remote_locs) >= 2,
        "all_connector_cpuset_not_collapsed": (len(connectors) >= 2
                                               and all(c.get("cpuset_ok") for c in connectors)),
        "all_connector_nodelay_true": (len(connectors) >= 2
                                       and all(c.get("tcp_nodelay_verified") for c in connectors)),
        "all_connector_lifecycle_ok": (len(connectors) >= 2 and all(
            c.get("joined") and c.get("served") and c.get("graceful_disconnect")
            for c in connectors)),
    }


# ---------------------------------------------------------------------------
# Stats helpers (exp61 style).
# ---------------------------------------------------------------------------

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
    s = sorted(v for v in vals if v is not None)
    if not s:
        return None
    m = len(s) // 2
    return s[m] if len(s) % 2 else (s[m - 1] + s[m]) / 2.0


def _spread(vals):
    s = [v for v in vals if v is not None]
    return {"min": min(s), "max": max(s)} if s else {"min": None, "max": None}


# ---------------------------------------------------------------------------
# Hard fences. Locked False for the whole exp62 arc (same as exp61).
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


# HPX composition modes reported by fanout_ext. Native modes compose with future continuations
# (when_all(...).then / dataflow) and do NOT poll futures in the success path; the interim
# root_flat_gather_poll path polls is_ready with a 50us sleep. hpx_native == not polled_in_success.
NATIVE_COMPOSITION_MODES = ("when_all_get", "when_all_then_reduce", "dataflow_reduce")
POLL_COMPOSITION_MODES = ("root_flat_gather_poll",)

# The PROVEN cross-node composition (validated on Rostam Slice 4a job 158813). Any >=2-locality /
# Slice 4b / matched-band work MUST use this.
PROVEN_CROSS_NODE_COMPOSITION_MODE = "root_flat_gather_poll"
# Native continuation modes that are an UNVALIDATED cross-node spike. On Rostam job 158814 the
# when_all(...).then + future::wait_for path was mathematically correct but the composed future
# stalled to dispatch_timeout_s (~30s) cross-node and the run failed. Kept in-tree for debugging /
# a future HPX-native reduction spike (hpx::collectives), never as a validated default.
EXPERIMENTAL_CROSS_NODE_COMPOSITION_MODES = ("when_all_then_reduce", "dataflow_reduce")


def composition_provenance(*, composition_primitive, watchdog, ran_on_hpx_thread,
                           polled_in_success_path, hpx_native_composition):
    """Pure: normalize the HPX composition-provenance fields returned by fanout_ext and a fail-closed
    consistency gate. The composition MUST have run on an HPX thread; an HPX-native composition MUST
    NOT have polled futures in the success path (hpx_native == not polled). Any non-bool / empty
    label / native-but-polled mismatch fails closed. Also records the experimental/validated STATUS of
    the composition so downstream (Slice 4b / matched-band) code cannot silently treat the native
    spike as validated. Returns the provenance fields plus composition_provenance_consistent. This is
    a provenance/honesty gate only -- it carries no performance, ratio, speedup, or Ray-comparison
    meaning."""
    native = hpx_native_composition is True
    polled = polled_in_success_path is True
    consistent = (
        ran_on_hpx_thread is True
        and isinstance(polled_in_success_path, bool)
        and isinstance(hpx_native_composition, bool)
        and native == (not polled)
        and isinstance(composition_primitive, str) and bool(composition_primitive)
        and isinstance(watchdog, str) and bool(watchdog))
    return {
        "composition_primitive": composition_primitive,
        "watchdog": watchdog,
        "composition_ran_on_hpx_thread": ran_on_hpx_thread is True,
        "polled_in_success_path": polled,
        "hpx_native_composition": native,
        "composition_provenance_consistent": bool(consistent),
        # Status fields (not gates): the native continuation modes are an EXPERIMENTAL cross-node
        # spike (exp62 job 158814 informative negative); the poll path is the proven cross-node one.
        "native_mode_experimental": native,
        "cross_node_composition_validated": not native,
        "proven_cross_node_composition_mode": PROVEN_CROSS_NODE_COMPOSITION_MODE,
    }


def _warn_experimental_cross_node_composition(mode, phase):
    """Loud stderr guard so future Slice 4b / matched-band work cannot accidentally adopt the native
    continuation modes as a validated cross-node default. The native modes are kept in-tree for
    debugging only; on Rostam job 158814 they were mathematically correct but stalled to the composed
    future timeout (~30s) cross-node. No-op for the proven root_flat_gather_poll mode."""
    if mode in EXPERIMENTAL_CROSS_NODE_COMPOSITION_MODES:
        print(f"WARNING [{phase}]: composition-mode '{mode}' is an EXPERIMENTAL cross-node "
              f"UNVALIDATED spike (exp62 job 158814: correct math but the composed future stalled to "
              f"the dispatch timeout cross-node). Slice 4b / matched-band work MUST use "
              f"'{PROVEN_CROSS_NODE_COMPOSITION_MODE}' (default, proven). Proceeding with the spike "
              f"mode for debugging only.", file=sys.stderr)


def _apply_composition_provenance(art, *, composition_primitive, watchdog, ran_on_hpx_thread,
                                  polled_in_success_path, hpx_native_composition):
    """Merge composition-provenance fields into an HPX arm artifact and add the fail-closed
    composition_provenance_consistent gate, then recompute overall. Single source of truth so the
    local smoke and the remote island builders record composition provenance identically."""
    prov = composition_provenance(
        composition_primitive=composition_primitive, watchdog=watchdog,
        ran_on_hpx_thread=ran_on_hpx_thread, polled_in_success_path=polled_in_success_path,
        hpx_native_composition=hpx_native_composition)
    consistent = prov.pop("composition_provenance_consistent")
    art["provenance"].update(prov)
    art["gates"]["composition_provenance_consistent"] = consistent
    art["overall"] = "pass" if all(art["gates"].values()) else "fail"
    return art


# ---------------------------------------------------------------------------
# Provenance / metadata block shared by artifacts.
# ---------------------------------------------------------------------------

def _provenance(*, placement_mode, n, witness, root_threads, connector_threads,
                dispatch_timeout_s, composition_primitive, reduce_primitive,
                parcelport_transport, timed_out_leaf_count=0,
                selected_subnet=None, root_hostname=None, connector_hostname=None,
                root_endpoint=None, connector_endpoint=None, tcp_parcelport=None,
                hpx_tcp_nodelay_verified=None, remote_localities_joined=None,
                localities_distinct=None, root_cpuset=None, connector_cpuset_effective=None,
                parcel_coalescing="unknown", parcelport_array_optimization="unknown"):
    return {
        "experiment_id": EXPERIMENT_ID,
        "placement_mode": placement_mode,
        "fanout_placement": placement_mode,
        "placement_class": placement_class(placement_mode),
        "placement_is_distributed": placement_is_distributed(placement_mode),
        "inner_fanout_n": int(n),
        "outer_qd1": True,
        "reduce_op": REDUCE_OP,
        "composition_primitive": composition_primitive,
        "reduce_primitive": reduce_primitive,
        "root_threads": root_threads,
        "connector_threads": connector_threads,
        "leaves_per_locality": dict(witness.get("leaves_per_locality", {})),
        "leaves_local": witness.get("leaves_local"),
        "leaves_remote": witness.get("leaves_remote"),
        "n_localities": witness.get("n_localities_covered"),
        "dispatch_timeout_s": dispatch_timeout_s,
        "timed_out_leaf_count": timed_out_leaf_count,
        "parcel_coalescing": parcel_coalescing,
        "parcelport_array_optimization": parcelport_array_optimization,
        "parcelport_transport": parcelport_transport,
        # Slice 2 distributed placement/provenance (None / "unknown" when not applicable).
        "selected_subnet": selected_subnet,
        "root_hostname": root_hostname,
        "connector_hostname": connector_hostname,
        "root_endpoint": root_endpoint,
        "connector_endpoint": connector_endpoint,
        "tcp_parcelport": tcp_parcelport,
        "hpx_tcp_nodelay_verified": hpx_tcp_nodelay_verified,
        "remote_localities_joined": remote_localities_joined,
        "localities_distinct": localities_distinct,
        "root_cpuset": root_cpuset,
        "connector_cpuset_effective": connector_cpuset_effective,
    }


# ---------------------------------------------------------------------------
# Per-arm island artifact.
#
# ONE arm (ray xor hpx), ONE island. Carries its own gates, stats, witness, provenance, and fences.
# overall == "pass" only if every per-arm gate is True. Any unknown/missing input fails closed.
# ---------------------------------------------------------------------------

def build_arm_artifact(*, arm, island_id, x, n, placement_mode, root_locality, localities,
                       leaf_records, threads_per_locality, latency_samples_ns, dispatch_elapsed_s,
                       dispatch_timeout_s, prewarm_count, expected_prewarm, k, w, clock,
                       measured_value=None, error=None,
                       measurement_boundary="python_caller_boundary",
                       min_threads_per_locality=1, root_threads=None, connector_threads=None,
                       composition_primitive="unknown", reduce_primitive="unknown",
                       parcelport_transport="tcp", timed_out_leaf_count=0,
                       remote_localities_joined=None, localities_distinct=None,
                       selected_subnet=None, root_hostname=None, connector_hostname=None,
                       root_endpoint=None, connector_endpoint=None, tcp_parcelport=None,
                       hpx_tcp_nodelay_verified=None, root_cpuset=None,
                       connector_cpuset_effective=None, connector_cpuset_min=None,
                       parcel_coalescing="unknown", parcelport_array_optimization="unknown"):
    if arm not in ("ray", "hpx"):
        raise ValueError(f"arm must be 'ray' or 'hpx', got {arm!r}")

    witness = tally_witness(leaf_records, root_locality)
    if measured_value is None:
        measured_value = i64(sum(int(rec["value"]) for rec in leaf_records))

    gates = {
        "leaves_dispatched_eq_n": len(leaf_records) == n,
        "witness_leaf_count_eq_n": witness["witness_leaf_count"] == n,
        "distribution_ok": evaluate_distribution_gate(placement_mode, witness, n),
        "threads_cover_fanout": evaluate_threads_cover_fanout(
            witness["leaves_per_locality"], threads_per_locality, min_threads_per_locality),
        "composite_oracle_correct": measured_value == composite_oracle(x, n),
        # watchdog: no wall-clock overrun AND no leaf left unready by the bounded dispatch.
        "no_dispatch_timeout": (dispatch_elapsed_s is not None and dispatch_timeout_s is not None
                                and dispatch_elapsed_s <= dispatch_timeout_s
                                and timed_out_leaf_count == 0),
        "prewarm_correct": prewarm_count == expected_prewarm,
        "no_error": error is None,
        "same_boundary": measurement_boundary == "python_caller_boundary",
        # distributed placement gates: enforced when the runner supplies them; not-asserted (None)
        # is treated as not-applicable so synthetic / single-locality arms are unaffected.
        "remote_join_ok": remote_localities_joined is None or remote_localities_joined >= 1,
        "localities_distinct": localities_distinct is None or localities_distinct is True,
        # connector cpuset must not collapse: enforced when an effective cpuset is supplied. Fails
        # closed on [0] or fewer than connector_cpuset_min CPUs (the Slice 2 [0] caveat).
        "connector_cpuset_not_collapsed": _connector_cpuset_ok(
            connector_cpuset_effective, connector_cpuset_min),
    }
    overall = "pass" if all(gates.values()) else "fail"

    return {
        "experiment_id": EXPERIMENT_ID,
        "arm": arm,
        "island_id": island_id,
        "placement_mode": placement_mode,
        "placement_class": placement_class(placement_mode),
        "x": int(x),
        "measured_value": measured_value,
        "config": {"k": k, "w": w, "prewarm": prewarm_count, "clock": clock,
                   "measurement_boundary": measurement_boundary},
        "witness": witness,
        "stats": _stats(latency_samples_ns),
        "dispatch_elapsed_s": dispatch_elapsed_s,
        "timed_out_leaf_count": timed_out_leaf_count,
        "provenance": _provenance(
            placement_mode=placement_mode, n=n, witness=witness, root_threads=root_threads,
            connector_threads=connector_threads, dispatch_timeout_s=dispatch_timeout_s,
            composition_primitive=composition_primitive, reduce_primitive=reduce_primitive,
            parcelport_transport=parcelport_transport, timed_out_leaf_count=timed_out_leaf_count,
            selected_subnet=selected_subnet, root_hostname=root_hostname,
            connector_hostname=connector_hostname, root_endpoint=root_endpoint,
            connector_endpoint=connector_endpoint, tcp_parcelport=tcp_parcelport,
            hpx_tcp_nodelay_verified=hpx_tcp_nodelay_verified,
            remote_localities_joined=remote_localities_joined,
            localities_distinct=localities_distinct, root_cpuset=root_cpuset,
            connector_cpuset_effective=connector_cpuset_effective,
            parcel_coalescing=parcel_coalescing,
            parcelport_array_optimization=parcelport_array_optimization),
        "gates": gates,
        "overall": overall,
        "error": error,
        "same_axis_comparison": False,  # never set True at arm level.
        "fences": fence_block(),
    }


# ---------------------------------------------------------------------------
# Slice 3 pair manifest + cross-island agreement + combined band aggregate.
#
# node_placement is the NODE-level placement (cross_node here); it is distinct from the arm-level
# placement_mode/fanout_placement (all-remote). same_axis_comparison is hard-locked False at the
# manifest level; it can only flip True in the combined band when EVERY gate passes.
# ---------------------------------------------------------------------------

NODE_PLACEMENT_CROSS = "cross_node"
NODE_PLACEMENT_MULTI = "cross_node_multi_remote"  # Slice 4a: root + >=2 remote localities
# Invariants every island (both arms) must AGREE on across the band.
BAND_AGREEMENT_KEYS = ("node_pair", "selected_subnet", "k", "w", "prewarm", "clock",
                       "inner_fanout_n", "fanout_placement", "node_placement")


def _g(art, name):
    """A gate value from an arm artifact, defaulting fail-closed (False) when absent."""
    return art.get("gates", {}).get(name) is True


def _p(art, key, default=None):
    return art.get("provenance", {}).get(key, default)


def _agreement_canon(art):
    """Extract the BAND_AGREEMENT_KEYS from an arm artifact into a hashable-friendly dict."""
    cfg = art.get("config", {})
    pr = art.get("provenance", {})
    node_pair = art.get("node_pair")
    return {
        "node_pair": tuple(node_pair) if isinstance(node_pair, list) else node_pair,
        "selected_subnet": pr.get("selected_subnet"),
        "k": cfg.get("k"),
        "w": cfg.get("w"),
        "prewarm": cfg.get("prewarm"),
        "clock": cfg.get("clock"),
        "inner_fanout_n": pr.get("inner_fanout_n"),
        "fanout_placement": pr.get("fanout_placement"),
        "node_placement": art.get("node_placement"),
    }


def _cross_island_agreement(arts):
    """All islands (both arms) must carry a non-None value for each agreement key AND agree on it.
    Returns (agree: bool, disagreements: dict)."""
    canons = [_agreement_canon(a) for a in arts]
    agree, disagreements = True, {}
    for key in BAND_AGREEMENT_KEYS:
        vals = [c.get(key) for c in canons]
        if any(v is None for v in vals) or len(set(vals)) != 1:
            agree = False
            disagreements[key] = [list(v) if isinstance(v, tuple) else v for v in vals]
    return agree, disagreements


def build_pair_manifest(*, manifest_id, x, n, ray_artifact, hpx_artifact,
                        node_placement=NODE_PLACEMENT_CROSS):
    """Correlate one matched HPX + Ray island. Juxtaposes the arms WITHOUT differencing or ratioing
    them. TRACKABLE artifact; same_axis_comparison hard-locked False at the manifest level."""
    rc, hc = ray_artifact["config"], hpx_artifact["config"]
    rw, hw = ray_artifact["witness"], hpx_artifact["witness"]
    rp = ray_artifact.get("ray_placement", {})
    gates = {
        "both_arms_pass": ray_artifact["overall"] == "pass" and hpx_artifact["overall"] == "pass",
        "same_measurement_boundary": (rc["measurement_boundary"] == hc["measurement_boundary"]
                                      == "python_caller_boundary"),
        "same_k_w_prewarm_clock": ((rc["k"], rc["w"], rc["prewarm"], rc["clock"])
                                   == (hc["k"], hc["w"], hc["prewarm"], hc["clock"])),
        "same_inner_fanout_n": _p(ray_artifact, "inner_fanout_n") == _p(hpx_artifact, "inner_fanout_n")
                               == int(n),
        "same_fanout_placement": _p(ray_artifact, "fanout_placement")
                                 == _p(hpx_artifact, "fanout_placement") == "all-remote",
        "same_node_placement": (ray_artifact.get("node_placement")
                                == hpx_artifact.get("node_placement") == node_placement),
        "same_node_pair": (ray_artifact.get("node_pair") is not None
                           and ray_artifact.get("node_pair") == hpx_artifact.get("node_pair")),
        "same_selected_subnet": (_p(ray_artifact, "selected_subnet") is not None
                                 and _p(ray_artifact, "selected_subnet")
                                 == _p(hpx_artifact, "selected_subnet")),
        "both_oracles_correct": _g(ray_artifact, "composite_oracle_correct")
                                and _g(hpx_artifact, "composite_oracle_correct"),
        "both_distribution_ok": _g(ray_artifact, "distribution_ok")
                                and _g(hpx_artifact, "distribution_ok"),
        "both_leaves_local_zero": rw.get("leaves_local") == 0 and hw.get("leaves_local") == 0,
        "both_leaves_remote_eq_n": rw.get("leaves_remote") == n and hw.get("leaves_remote") == n,
        "hpx_nodelay_true": _p(hpx_artifact, "hpx_tcp_nodelay_verified") is True,
        "hpx_connector_cpuset_not_collapsed": _g(hpx_artifact, "connector_cpuset_not_collapsed"),
        "hpx_threads_cover_fanout": _g(hpx_artifact, "threads_cover_fanout"),
        "hpx_no_dispatch_timeout": _g(hpx_artifact, "no_dispatch_timeout"),
        "ray_hard_placement_true": rp.get("hard_placement") is True,
        "ray_leaves_on_target_node": rp.get("leaves_on_target_node") is True,
        "ray_coordinator_single_submission_true": rp.get("single_submission") is True,
    }
    overall = "pass" if all(gates.values()) else "fail"
    return {
        "experiment_id": EXPERIMENT_ID,
        "manifest_id": manifest_id,
        "x": int(x),
        "inner_fanout_n": int(n),
        "fanout_placement": "all-remote",
        "node_placement": node_placement,
        "node_pair": hpx_artifact.get("node_pair"),
        "selected_subnet": _p(hpx_artifact, "selected_subnet"),
        "ray": {"island_id": ray_artifact["island_id"], "overall": ray_artifact["overall"],
                "stats": ray_artifact["stats"], "witness": rw, "ray_placement": rp},
        "hpx": {"island_id": hpx_artifact["island_id"], "overall": hpx_artifact["overall"],
                "stats": hpx_artifact["stats"], "witness": hw},
        "gates": gates,
        "overall": overall,
        "same_axis_comparison": False,
        "note": ("Pair manifest correlates one matched HPX + Ray island with measurement-plane "
                 "labels. It NEVER differences or ratios the arms."),
        "fences": fence_block(),
    }


# ---------------------------------------------------------------------------
# Per-arm band aggregate.
#
# ONE arm across R islands. Each island contributes its own p50/p90/p99/mean; across islands we
# summarize with median + spread. We NEVER difference arms, NEVER ratio, NEVER compute speedup, and
# NEVER difference placement bands. same_axis_comparison flips True ONLY if every gate passes.
# ---------------------------------------------------------------------------

def build_band_aggregate(*, arm, island_artifacts, placement_mode, min_r=5):
    if arm not in ("ray", "hpx"):
        raise ValueError(f"arm must be 'ray' or 'hpx', got {arm!r}")

    r = len(island_artifacts)
    cols = {"p50_ns": [], "p90_ns": [], "p99_ns": [], "mean_ns": []}
    for isl in island_artifacts:
        st = isl.get("stats", {})
        for c in cols:
            cols[c].append(st.get(c))

    def _all_gate(name):
        return bool(island_artifacts) and all(
            isl.get("gates", {}).get(name) is True for isl in island_artifacts)

    gates = {
        "r_at_least_5": r >= min_r,
        "all_arms_pass": bool(island_artifacts) and all(
            isl.get("overall") == "pass" for isl in island_artifacts),
        "all_distribution_ok": _all_gate("distribution_ok"),
        "all_witness_leaf_count_eq_n": _all_gate("witness_leaf_count_eq_n"),
        "all_threads_cover_fanout": _all_gate("threads_cover_fanout"),
        "no_missing_island_stats": all(all(v is not None for v in col) for col in cols.values()),
        # GUARD: a single-locality (local mechanism smoke) band can NEVER become distributed
        # same-axis evidence, no matter how clean its other gates are.
        "placement_is_distributed": placement_is_distributed(placement_mode),
    }
    same_axis = all(gates.values())
    across = {c: {"median": _median(cols[c]), "spread": _spread(cols[c])} for c in cols}

    payload = {
        "experiment_id": EXPERIMENT_ID,
        "arm": arm,
        "placement_mode": placement_mode,
        "r": r,
        "min_r": min_r,
        "per_island": [
            {"island_id": isl.get("island_id"), "overall": isl.get("overall"),
             "stats": isl.get("stats", {})} for isl in island_artifacts],
        "across_islands": across,
        "gates": gates,
        "same_axis_comparison": same_axis,
        "note": ("Per-arm band for ONE arm. p50/p90/p99/mean summarized across islands SEPARATELY. "
                 "Arms are never differenced; placement bands are never differenced."),
        "fences": fence_block(),
    }
    overall = "pass" if same_axis else "fail"
    return payload, overall


# ---------------------------------------------------------------------------
# Slice 3 combined band aggregate (ONE trackable file).
#
# Holds the HPX and Ray per-arm bands as SEPARATE blocks, the cross-island agreement, the combined
# gates, fences, and metadata. same_axis_comparison flips True only if every gate passes across all
# islands of BOTH arms. NEVER differences arms, NEVER ratios, NEVER computes speedup.
# ---------------------------------------------------------------------------

def build_fanout_band_aggregate(*, hpx_islands, ray_islands, job_id, band_id, min_r=5,
                                node_placement=NODE_PLACEMENT_CROSS):
    hpx_band, _ = build_band_aggregate(arm="hpx", island_artifacts=hpx_islands,
                                       placement_mode="all-remote", min_r=min_r)
    ray_band, _ = build_band_aggregate(arm="ray", island_artifacts=ray_islands,
                                       placement_mode="all-remote", min_r=min_r)
    all_islands = list(hpx_islands) + list(ray_islands)
    agree, disagreements = _cross_island_agreement(all_islands)

    def _all(arts, name):
        return bool(arts) and all(_g(a, name) for a in arts)

    gates = {
        "r_at_least_5": len(hpx_islands) >= min_r and len(ray_islands) >= min_r,
        "all_arms_pass": bool(all_islands) and all(a.get("overall") == "pass" for a in all_islands),
        "all_distribution_ok": _all(all_islands, "distribution_ok"),
        "all_witness_leaf_count_eq_n": _all(all_islands, "witness_leaf_count_eq_n"),
        "all_hpx_threads_cover_fanout": _all(hpx_islands, "threads_cover_fanout"),
        "all_hpx_nodelay_true": bool(hpx_islands)
                                and all(_p(a, "hpx_tcp_nodelay_verified") is True for a in hpx_islands),
        "all_hpx_connector_cpuset_not_collapsed": _all(hpx_islands,
                                                       "connector_cpuset_not_collapsed"),
        "all_hpx_no_dispatch_timeout": _all(hpx_islands, "no_dispatch_timeout"),
        "all_ray_hard_placement": bool(ray_islands) and all(
            a.get("ray_placement", {}).get("hard_placement") is True
            and a.get("ray_placement", {}).get("leaves_on_target_node") is True
            and a.get("ray_placement", {}).get("single_submission") is True for a in ray_islands),
        "node_placement_is_cross_node": all(a.get("node_placement") == node_placement
                                            for a in all_islands) and bool(all_islands),
        "cross_island_agreement": agree,
        "hpx_band_pass": hpx_band["same_axis_comparison"] is True,
        "ray_band_pass": ray_band["same_axis_comparison"] is True,
    }
    same_axis = all(gates.values())
    overall = "pass" if same_axis else "fail"

    shared = _agreement_canon(hpx_islands[0]) if (agree and hpx_islands) else None
    if shared:
        shared = {k: (list(v) if isinstance(v, tuple) else v) for k, v in shared.items()}

    payload = {
        "experiment_id": EXPERIMENT_ID,
        "band_id": band_id,
        "job_id": job_id,
        "node_placement": node_placement,
        "hpx_band": hpx_band,
        "ray_band": ray_band,
        "cross_island_agreement": {"agree": agree, "shared_invariants": shared,
                                   "disagreements": disagreements},
        "gates": gates,
        "same_axis_comparison": same_axis,
        "metadata": {
            "r_hpx": len(hpx_islands), "r_ray": len(ray_islands), "min_r": min_r,
            "fanout_placement": "all-remote", "measurement_boundary": "python_caller_boundary",
            "note": ("Combined band. HPX and Ray per-arm bands are kept SEPARATE; arms are never "
                     "differenced and placement bands are never differenced. same_axis_comparison "
                     "means structurally valid same-axis evidence, NOT a ratio/winner comparison."),
        },
        "fences": fence_block(),
    }
    return payload, overall


# ---------------------------------------------------------------------------
# Slice 4b multi-remote same-axis correlation (>=2 remote nodes/localities, cross_node_multi_remote).
#
# HPX arm uses the PROVEN root_flat_gather_poll composition over >=2 remote localities; Ray arm uses a
# coordinator (num_cpus=0 on the head/driver node, zero leaves) that hard-pins N leaves round-robin
# across >=2 remote node ids. Correlated on node_set (order-independent). Never differences/ratios
# arms; same_axis_comparison flips True only if EVERY gate passes across both arms.
# ---------------------------------------------------------------------------

def ray_multi_remote_gates(*, witness, remote_node_ids, leaves_per_node, coordinator_node_id,
                           driver_node_id, head_num_cpus, coordinator_num_cpus, hard_placement,
                           cluster_connected, n):
    """PURE Slice 4b Ray multi-remote gates. remote_node_ids = intended remote target node ids;
    leaves_per_node = observed {node_id: count}. Fails closed: cluster connected, >=2 DISTINCT remote
    nodes each covered by >=1 leaf, coordinator on the driver/head node running ZERO leaves, head +
    coordinator num_cpus == 0, hard placement, and the closed-int64 distribution (leaves_local==0,
    leaves_remote==n). HPX-comparison-free; carries no ratio/speedup meaning."""
    remote_ids = list(remote_node_ids)
    distinct = len(set(remote_ids)) == len(remote_ids) and len(remote_ids) >= 2
    covers_all = (len(remote_ids) >= 2
                  and all(leaves_per_node.get(r, 0) >= 1 for r in remote_ids))
    coord_on_driver = coordinator_node_id is not None and coordinator_node_id == driver_node_id
    coord_zero_leaves = leaves_per_node.get(coordinator_node_id, 0) == 0
    return {
        "ray_cluster_connect": cluster_connected is True,
        "ray_n_remote_nodes_ge_2": len(remote_ids) >= 2,
        "distinct_remote_node_ids": distinct,
        "coordinator_on_driver_node": coord_on_driver,
        "coordinator_runs_zero_leaves": coord_zero_leaves,
        "ray_head_num_cpus_zero": head_num_cpus == 0,
        "ray_coordinator_num_cpus_zero": coordinator_num_cpus == 0,
        "ray_hard_placement": hard_placement is True,
        "leaves_local_eq_0": witness.get("leaves_local") == 0,
        "leaves_remote_eq_n": witness.get("leaves_remote") == n,
        "ray_leaves_cover_all_remote_nodes": covers_all,
    }


def _node_set_canon(ns):
    """Order-independent canonical node_set for cross-arm agreement (sorted tuple), or None."""
    return tuple(sorted(ns)) if isinstance(ns, list) else None


# Multi-remote band agreement is on node_set (order-independent), not the 2-node node_pair.
BAND_AGREEMENT_KEYS_MULTI = ("node_set", "selected_subnet", "k", "w", "prewarm", "clock",
                             "inner_fanout_n", "fanout_placement", "node_placement")


def _agreement_canon_multi(art):
    cfg = art.get("config", {})
    pr = art.get("provenance", {})
    return {
        "node_set": _node_set_canon(art.get("node_set")),
        "selected_subnet": pr.get("selected_subnet"),
        "k": cfg.get("k"), "w": cfg.get("w"), "prewarm": cfg.get("prewarm"),
        "clock": cfg.get("clock"), "inner_fanout_n": pr.get("inner_fanout_n"),
        "fanout_placement": pr.get("fanout_placement"),
        "node_placement": art.get("node_placement"),
    }


def _cross_island_agreement_multi(arts):
    canons = [_agreement_canon_multi(a) for a in arts]
    agree, disagreements = True, {}
    for key in BAND_AGREEMENT_KEYS_MULTI:
        vals = [c.get(key) for c in canons]
        if any(v is None for v in vals) or len(set(vals)) != 1:
            agree = False
            disagreements[key] = [list(v) if isinstance(v, tuple) else v for v in vals]
    return agree, disagreements


def build_pair_manifest_multi_remote(*, manifest_id, x, n, ray_artifact, hpx_artifact):
    """Correlate one matched HPX + Ray multi-remote island (>=2 remote nodes/localities). TRACKABLE;
    juxtaposes the arms WITHOUT differencing/ratioing. same_axis_comparison hard-locked False at the
    manifest level. Requires matched slurm_job_id and node_set (order-independent)."""
    rc, hc = ray_artifact["config"], hpx_artifact["config"]
    rw, hw = ray_artifact["witness"], hpx_artifact["witness"]
    rp = ray_artifact.get("ray_placement", {})
    hpx_remote_ids = hpx_artifact.get("remote_locality_ids", []) or []
    ray_ns, hpx_ns = _node_set_canon(ray_artifact.get("node_set")), _node_set_canon(
        hpx_artifact.get("node_set"))
    gates = {
        "both_arms_pass": ray_artifact["overall"] == "pass" and hpx_artifact["overall"] == "pass",
        "same_measurement_boundary": (rc["measurement_boundary"] == hc["measurement_boundary"]
                                      == "python_caller_boundary"),
        "same_k_w_prewarm_clock": ((rc["k"], rc["w"], rc["prewarm"], rc["clock"])
                                   == (hc["k"], hc["w"], hc["prewarm"], hc["clock"])),
        "same_inner_fanout_n": _p(ray_artifact, "inner_fanout_n") == _p(hpx_artifact,
                                                                        "inner_fanout_n") == int(n),
        "same_fanout_placement": _p(ray_artifact, "fanout_placement")
                                 == _p(hpx_artifact, "fanout_placement") == "all-remote",
        "same_node_placement": (ray_artifact.get("node_placement")
                                == hpx_artifact.get("node_placement") == NODE_PLACEMENT_MULTI),
        "same_slurm_job_id": (ray_artifact.get("slurm_job_id") is not None
                              and ray_artifact.get("slurm_job_id") == hpx_artifact.get("slurm_job_id")),
        "same_node_set": ray_ns is not None and ray_ns == hpx_ns,
        "same_selected_subnet": (_p(ray_artifact, "selected_subnet") is not None
                                 and _p(ray_artifact, "selected_subnet")
                                 == _p(hpx_artifact, "selected_subnet")),
        "both_oracles_correct": _g(ray_artifact, "composite_oracle_correct")
                                and _g(hpx_artifact, "composite_oracle_correct"),
        "both_distribution_ok": _g(ray_artifact, "distribution_ok")
                                and _g(hpx_artifact, "distribution_ok"),
        "both_leaves_local_zero": rw.get("leaves_local") == 0 and hw.get("leaves_local") == 0,
        "both_leaves_remote_eq_n": rw.get("leaves_remote") == n and hw.get("leaves_remote") == n,
        # HPX arm >=2-remote coverage + connector health (proven poll composition).
        "hpx_n_remote_localities_ge_2": len(hpx_remote_ids) >= 2,
        "hpx_leaves_cover_all_remote_localities": _g(hpx_artifact,
                                                     "leaves_per_remote_locality_covers_all"),
        "hpx_all_connector_lifecycle_ok": _g(hpx_artifact, "all_connector_lifecycle_ok"),
        "hpx_all_connector_nodelay_true": _g(hpx_artifact, "all_connector_nodelay_true"),
        "hpx_all_connector_cpuset_not_collapsed": _g(hpx_artifact,
                                                     "all_connector_cpuset_not_collapsed"),
        "hpx_no_dispatch_timeout": _g(hpx_artifact, "no_dispatch_timeout"),
        # Ray arm >=2-remote coverage + placement.
        "ray_n_remote_nodes_ge_2": _g(ray_artifact, "ray_n_remote_nodes_ge_2"),
        "ray_leaves_cover_all_remote_nodes": _g(ray_artifact, "ray_leaves_cover_all_remote_nodes"),
        "ray_distinct_remote_node_ids": _g(ray_artifact, "distinct_remote_node_ids"),
        "ray_coordinator_on_driver_node": _g(ray_artifact, "coordinator_on_driver_node"),
        "ray_coordinator_runs_zero_leaves": _g(ray_artifact, "coordinator_runs_zero_leaves"),
        "ray_hard_placement": _g(ray_artifact, "ray_hard_placement"),
    }
    overall = "pass" if all(gates.values()) else "fail"
    return {
        "experiment_id": EXPERIMENT_ID,
        "manifest_id": manifest_id,
        "x": int(x),
        "inner_fanout_n": int(n),
        "fanout_placement": "all-remote",
        "node_placement": NODE_PLACEMENT_MULTI,
        "node_set": hpx_artifact.get("node_set"),
        "selected_subnet": _p(hpx_artifact, "selected_subnet"),
        "ray": {"island_id": ray_artifact["island_id"], "overall": ray_artifact["overall"],
                "stats": ray_artifact["stats"], "witness": rw, "ray_placement": rp},
        "hpx": {"island_id": hpx_artifact["island_id"], "overall": hpx_artifact["overall"],
                "stats": hpx_artifact["stats"], "witness": hw,
                "remote_locality_ids": hpx_remote_ids},
        "gates": gates,
        "overall": overall,
        "same_axis_comparison": False,
        "note": ("Multi-remote pair manifest correlates one matched HPX + Ray >=2-remote island with "
                 "measurement-plane labels. It NEVER differences or ratios the arms."),
        "fences": fence_block(),
    }


def build_fanout_band_aggregate_multi_remote(*, hpx_islands, ray_islands, job_id, band_id, min_r=5):
    """ONE trackable combined multi-remote band aggregate. Per-arm bands kept SEPARATE; arms never
    differenced/ratioed. same_axis_comparison flips True only if every gate passes across both arms
    and all islands. Correlates on node_set (order-independent)."""
    hpx_band, _ = build_band_aggregate(arm="hpx", island_artifacts=hpx_islands,
                                       placement_mode="all-remote", min_r=min_r)
    ray_band, _ = build_band_aggregate(arm="ray", island_artifacts=ray_islands,
                                       placement_mode="all-remote", min_r=min_r)
    all_islands = list(hpx_islands) + list(ray_islands)
    agree, disagreements = _cross_island_agreement_multi(all_islands)

    def _all(arts, name):
        return bool(arts) and all(_g(a, name) for a in arts)

    gates = {
        "r_at_least_5": len(hpx_islands) >= min_r and len(ray_islands) >= min_r,
        "all_arms_pass": bool(all_islands) and all(a.get("overall") == "pass" for a in all_islands),
        "all_distribution_ok": _all(all_islands, "distribution_ok"),
        "all_witness_leaf_count_eq_n": _all(all_islands, "witness_leaf_count_eq_n"),
        "all_hpx_leaves_cover_all_remote_localities": _all(hpx_islands,
                                                           "leaves_per_remote_locality_covers_all"),
        "all_hpx_connector_lifecycle_ok": _all(hpx_islands, "all_connector_lifecycle_ok"),
        "all_hpx_connector_nodelay_true": _all(hpx_islands, "all_connector_nodelay_true"),
        "all_hpx_connector_cpuset_not_collapsed": _all(hpx_islands,
                                                       "all_connector_cpuset_not_collapsed"),
        "all_hpx_no_dispatch_timeout": _all(hpx_islands, "no_dispatch_timeout"),
        "all_ray_cover_all_remote_nodes": _all(ray_islands, "ray_leaves_cover_all_remote_nodes"),
        "all_ray_hard_placement": _all(ray_islands, "ray_hard_placement"),
        "all_ray_coordinator_zero_leaves": _all(ray_islands, "coordinator_runs_zero_leaves"),
        "node_placement_is_multi_remote": bool(all_islands) and all(
            a.get("node_placement") == NODE_PLACEMENT_MULTI for a in all_islands),
        "cross_island_agreement": agree,
        "hpx_band_pass": hpx_band["same_axis_comparison"] is True,
        "ray_band_pass": ray_band["same_axis_comparison"] is True,
    }
    same_axis = all(gates.values())
    overall = "pass" if same_axis else "fail"

    shared = _agreement_canon_multi(hpx_islands[0]) if (agree and hpx_islands) else None
    if shared:
        shared = {k: (list(v) if isinstance(v, tuple) else v) for k, v in shared.items()}

    payload = {
        "experiment_id": EXPERIMENT_ID,
        "band_id": band_id,
        "job_id": job_id,
        "node_placement": NODE_PLACEMENT_MULTI,
        "hpx_band": hpx_band,
        "ray_band": ray_band,
        "cross_island_agreement": {"agree": agree, "shared_invariants": shared,
                                   "disagreements": disagreements},
        "gates": gates,
        "same_axis_comparison": same_axis,
        "metadata": {
            "r_hpx": len(hpx_islands), "r_ray": len(ray_islands), "min_r": min_r,
            "fanout_placement": "all-remote", "measurement_boundary": "python_caller_boundary",
            "hpx_composition": "root_flat_gather_poll (proven cross-node)",
            "note": ("Combined multi-remote band. HPX (proven root_flat_gather_poll over >=2 remote "
                     "localities) and Ray (coordinator + round-robin leaves across >=2 remote nodes) "
                     "per-arm bands are kept SEPARATE; arms are never differenced and placement bands "
                     "are never differenced. same_axis_comparison means structurally valid same-axis "
                     "evidence, NOT a ratio/winner comparison."),
        },
        "fences": fence_block(),
    }
    return payload, overall


# ---------------------------------------------------------------------------
# Slice 1 local mechanism smokes (SINGLE-LOCALITY). NOT distributed evidence, NO performance claim.
#
# Both arms reuse the Slice 0 oracle/witness/gates via build_arm_artifact under placement_mode
# "single-locality", so placement_class is "local_mechanism_smoke" and no band built from them can
# flip same_axis_comparison True (the placement_is_distributed band guard fails closed).
# ---------------------------------------------------------------------------

LOCAL_SMOKE_DISPATCH_TIMEOUT_S = 30.0


def _new_id():
    return uuid.uuid4().hex[:12]


def _write_local_artifact(artifact, arm, exp_dir=None):
    """Write a raw, GITIGNORED per-run smoke artifact: exp62_fanout_<island_id>_<arm>.json."""
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    path = os.path.join(exp_dir, f"exp62_fanout_{artifact['island_id']}_{arm}.json")
    with open(path, "w") as fh:
        json.dump(artifact, fh, indent=2, sort_keys=True)
    return path


def _print_gate_summary(tag, artifact):
    print(f"{tag}: overall={artifact['overall']} placement_class={artifact['placement_class']}")
    for name, val in artifact["gates"].items():
        print(f"    [{'ok  ' if val else 'FAIL'}] {name}")


def _build_local_smoke_artifact(*, arm, x, n, records, latency_samples_ns, root_locality,
                                threads_per_locality, composition_primitive, reduce_primitive,
                                dispatch_elapsed_s, prewarm_count, k, root_threads,
                                measured_value=None, parcelport_transport="none"):
    """Build a single-locality arm artifact from real or synthetic leaf records. expected_prewarm is
    pinned to prewarm_count so prewarm_correct passes; w mirrors the prewarm count."""
    localities = sorted(set(rec["locality"] for rec in records)) or [root_locality]
    island_id = f"{arm}-local-{_new_id()}"
    return build_arm_artifact(
        arm=arm, island_id=island_id, x=x, n=n, placement_mode="single-locality",
        root_locality=root_locality, localities=localities, leaf_records=records,
        threads_per_locality=threads_per_locality, latency_samples_ns=latency_samples_ns,
        dispatch_elapsed_s=dispatch_elapsed_s, dispatch_timeout_s=LOCAL_SMOKE_DISPATCH_TIMEOUT_S,
        prewarm_count=prewarm_count, expected_prewarm=prewarm_count, k=k, w=prewarm_count,
        clock="perf_counter_ns", measured_value=measured_value,
        composition_primitive=composition_primitive, reduce_primitive=reduce_primitive,
        root_threads=root_threads, parcelport_transport=parcelport_transport)


def _default_hpx_import():
    # The pybind module is built into build/ (or build/Release). Put those on sys.path before the
    # import, exactly like exp61's runner.
    here = os.path.dirname(os.path.abspath(__file__))
    for d in (os.path.join(here, "build"), os.path.join(here, "build", "Release")):
        if os.path.isdir(d) and d not in sys.path:
            sys.path.insert(0, d)
    import fanout_ext
    return fanout_ext


def run_hpx_local_smoke(*, x=7, n=8, hpx_threads=4, prewarm=5, k=30, composition_mode="when_all_get",
                        import_fn=_default_hpx_import, write=True):
    """Single-locality HPX fanout/fanin mechanism smoke. Skips cleanly if fanout_ext is not built.

    The ext dispatches N ordinary LOCAL hpx::async leaves on one locality (find_here) and composes
    the fan-in per composition_mode: 'when_all_get' (default) or the native 'when_all_then_reduce'
    continuation. Both run on an HPX thread and never poll -- this local path lets the native
    continuation be exercised OFF-CLUSTER. Each leaf carries the runtime hpx::get_locality_id()."""
    try:
        ext = import_fn()
    except ImportError as exc:
        print(f"hpx-local-smoke: SKIP -- fanout_ext not built ({exc})")
        return "skip", None

    ext.start(hpx_threads)
    try:
        for _ in range(prewarm):
            ext.fanout_fanin(x, n, composition_mode)
        lat, last = [], None
        t_all0 = time.perf_counter_ns()
        for _ in range(k):
            t0 = time.perf_counter_ns()
            last = ext.fanout_fanin(x, n, composition_mode)
            lat.append(time.perf_counter_ns() - t0)
        dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9
    finally:
        ext.shutdown()

    (value, leaves, comp_prim, reduce_prim, _n_loc, _inner_n,
     watchdog, ran_hpx, polled, hpx_native) = last
    records = [{"i": int(i), "value": int(v), "locality": int(loc)} for (i, v, loc) in leaves]
    root_loc = records[0]["locality"] if records else 0
    threads = {loc: hpx_threads for loc in set(rec["locality"] for rec in records)} \
        or {root_loc: hpx_threads}
    art = _build_local_smoke_artifact(
        arm="hpx", x=x, n=n, records=records, latency_samples_ns=lat, root_locality=root_loc,
        threads_per_locality=threads, composition_primitive=comp_prim, reduce_primitive=reduce_prim,
        dispatch_elapsed_s=dispatch_elapsed_s, prewarm_count=prewarm, k=k, root_threads=hpx_threads,
        measured_value=int(value))
    _apply_composition_provenance(
        art, composition_primitive=comp_prim, watchdog=watchdog, ran_on_hpx_thread=ran_hpx,
        polled_in_success_path=polled, hpx_native_composition=hpx_native)
    _print_gate_summary("hpx-local-smoke", art)
    if write:
        print(f"hpx-local-smoke: wrote {os.path.basename(_write_local_artifact(art, 'hpx'))} "
              f"(gitignored)")
    return ("pass" if art["overall"] == "pass" else "fail"), art


def _default_ray_import():
    import ray
    return ray


def run_ray_local_smoke(*, x=7, n=8, prewarm=3, k=20, import_fn=_default_ray_import, write=True):
    """Single-node, local Ray coordinator fanout/fanin smoke. Ray-PATTERN mapping, NOT HPX best
    practice. One Python caller boundary: ray.get(coordinator.remote(x, N)); the coordinator fans
    out N leaf tasks and reduces. Skips cleanly if Ray is absent."""
    try:
        ray = import_fn()
    except ImportError as exc:
        print(f"ray-local-smoke: SKIP -- ray not installed ({exc})")
        return "skip", None

    if not ray.is_initialized():
        ray.init(num_cpus=os.cpu_count(), ignore_reinit_error=True, configure_logging=False,
                 include_dashboard=False)
    try:
        leaf_xor = LEAF_XOR

        @ray.remote
        def _leaf(xx, i):
            node = ray.get_runtime_context().get_node_id()
            return (int(i), i64((int(xx) ^ leaf_xor) + (int(i) << 1)), node)

        @ray.remote
        def _coordinator(xx, nn):
            recs = ray.get([_leaf.remote(xx, i) for i in range(nn)])
            return i64(sum(int(rec[1]) for rec in recs)), recs

        for _ in range(prewarm):
            ray.get(_coordinator.remote(x, n))
        lat, last = [], None
        t_all0 = time.perf_counter_ns()
        for _ in range(k):
            t0 = time.perf_counter_ns()
            last = ray.get(_coordinator.remote(x, n))
            lat.append(time.perf_counter_ns() - t0)
        dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9
        node_id = str(ray.get_runtime_context().get_node_id())
    finally:
        ray.shutdown()

    value, recs = last
    records = [{"i": int(i), "value": int(v), "locality": str(loc)} for (i, v, loc) in recs]
    cpus = max(1, os.cpu_count() or 1)
    threads = {loc: cpus for loc in set(rec["locality"] for rec in records)} or {node_id: cpus}
    art = _build_local_smoke_artifact(
        arm="ray", x=x, n=n, records=records, latency_samples_ns=lat, root_locality=node_id,
        threads_per_locality=threads, composition_primitive="ray.coordinator.remote",
        reduce_primitive="driver_fold_sum_int64", dispatch_elapsed_s=dispatch_elapsed_s,
        prewarm_count=prewarm, k=k, root_threads=cpus, measured_value=int(value))
    _print_gate_summary("ray-local-smoke", art)
    if write:
        print(f"ray-local-smoke: wrote {os.path.basename(_write_local_artifact(art, 'ray'))} "
              f"(gitignored)")
    return ("pass" if art["overall"] == "pass" else "fail"), art


# ---------------------------------------------------------------------------
# Slice 2 one-remote-locality HPX fanout dry-run orchestrator.
#
# all-remote, ONE connector locality, ONE dry-run island, small K. NOT a band, NOT R>=5, NOT Ray,
# NO same_axis_comparison. Skips cleanly off-cluster or when the connector binary is missing.
# ---------------------------------------------------------------------------

EXP62_RUNS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_exp62_runs")


def _short_host(h):
    return h.split(".")[0] if h else h


def _scontrol_hostnames(nodelist):
    """Expand a Slurm nodelist to hostnames via `scontrol show hostnames`. [] on any failure."""
    import subprocess
    try:
        out = subprocess.run(["scontrol", "show", "hostnames", nodelist],
                             capture_output=True, text=True, timeout=15)
        if out.returncode == 0:
            return [h for h in out.stdout.split() if h]
    except Exception:  # noqa: BLE001
        pass
    return []


def _slurm_info(env=None):
    """Resolve the Slurm allocation: job id, nodelist, hostnames, two_node. Best-effort, no raise."""
    env = os.environ if env is None else env
    job_id = env.get("SLURM_JOB_ID")
    nodelist = env.get("SLURM_JOB_NODELIST")
    hostnames = _scontrol_hostnames(nodelist) if nodelist else []
    if not hostnames and nodelist:
        # last-resort: SLURM_JOB_NUM_NODES tells count but not names; leave hostnames empty.
        hostnames = []
    return {"slurm_job_id": job_id, "nodelist": nodelist, "hostnames": hostnames,
            "two_node": len(hostnames) >= 2}


def _first_ip_for_subnet(prefer_subnet):
    """Best-effort first local IPv4 whose prefix matches prefer_subnet (e.g. '10.42.5.'). None if
    none match or prefer_subnet is empty. Tries getaddrinfo first, then `hostname -I` (Linux) since
    getaddrinfo(gethostname()) often misses the eno16 selected-subnet address on the cluster."""
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


def _find_connector_bin(here=None):
    """Locate the built fanout_connector executable, or None (a skip-clean precondition)."""
    here = os.path.dirname(os.path.abspath(__file__)) if here is None else here
    for d in (os.path.join(here, "build"), os.path.join(here, "build", "Release")):
        cand = os.path.join(d, "fanout_connector")
        if os.path.isfile(cand):
            return cand
    return None


def _effective_cpuset():
    """This process's effective CPU affinity as a sorted list (Linux), else None."""
    try:
        return sorted(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return None


def _hpx_remote_preconditions(env, import_fn, label):
    """Resolve (ext, slurm, connector_bin, root_host, remote_host, root_ip) or return a skip reason
    string. Common to the dry-run and band HPX phases."""
    slurm = _slurm_info(env)
    if not slurm["two_node"]:
        return None, (f"{label}: SKIP -- need a >=2-node Slurm allocation (slurm_job_id="
                      f"{slurm['slurm_job_id']}, hostnames={slurm['hostnames']}).")
    connector_bin = _find_connector_bin()
    if connector_bin is None:
        return None, f"{label}: SKIP -- fanout_connector not built (build the CMake target first)."
    try:
        ext = import_fn()
    except ImportError as exc:
        return None, f"{label}: SKIP -- fanout_ext not built ({exc})."
    return (ext, slurm, connector_bin), None


def _hpx_remote_island(ext, slurm, connector_bin, root_host, remote_host, root_ip, *, x, n, k,
                       prewarm, hpx_threads, connector_threads, connector_min_threads,
                       connector_cpus_per_task, cpu_bind_mask, prefer_subnet, root_port,
                       await_timeout, serve_timeout, dispatch_timeout_s, island_id, node_placement,
                       bootdir_prefix, w=None, composition_mode="root_flat_gather_poll"):
    w = prewarm if w is None else w
    """Run ONE all-remote HPX fanout island: start the embedded AGAS root, launch the connector on
    the remote node (optionally with cpu-bind for the band), await it, time K calls, tear down, read
    the connector attestation, and build the arm artifact. Returns (art, bootdir)."""
    import subprocess
    import tempfile

    os.makedirs(EXP62_RUNS, exist_ok=True)
    bootdir = tempfile.mkdtemp(prefix=bootdir_prefix, dir=EXP62_RUNS)

    proc = None
    started = False
    error = None
    last = None
    lat = []
    remote_loc = -1
    root_loc = None
    root_cpuset = None
    dispatch_elapsed_s = None
    try:
        if not remote_host:
            raise RuntimeError("no remote host distinct from root in SLURM hostnames")
        if not root_ip:
            raise RuntimeError(f"no root IP for --prefer-subnet {prefer_subnet}")
        root_extra = ["--hpx:ignore-batch-env",
                      f"--hpx:hpx={root_ip}:{root_port}", f"--hpx:agas={root_ip}:{root_port}",
                      "--hpx:expect-connecting-localities"]
        ext.start(hpx_threads, root_extra)
        started = True
        root_loc = int(ext.local_locality_id())
        root_cpuset = _effective_cpuset()
        # connector srun. For the band, request a full CPU set on the remote node and bind with a
        # MASK (not map_cpu), plus --hpx:threads + --hpx:bind=none (exp61 Slice 5 lesson) so the
        # connector's effective cpuset does NOT collapse to [0].
        conn_cmd = ["srun", "--nodes=1", "--ntasks=1", f"--nodelist={remote_host}"]
        if connector_cpus_per_task:
            conn_cmd.append(f"--cpus-per-task={connector_cpus_per_task}")
        if cpu_bind_mask:
            conn_cmd.append(f"--cpu-bind=mask_cpu:{cpu_bind_mask}")
        conn_cmd += [connector_bin, "--role=connect", f"--bootstrap={bootdir}",
                     f"--serve-timeout={serve_timeout}", f"--prefer-subnet={prefer_subnet}",
                     f"--agas-preprobe-host={root_ip}", f"--agas-preprobe-port={root_port}",
                     f"--hpx:threads={connector_threads}", "--hpx:bind=none",
                     "--hpx:ignore-batch-env", f"--hpx:agas={root_ip}:{root_port}"]
        proc = subprocess.Popen(conn_cmd)
        remote_loc = int(ext.await_remote(await_timeout))
        if remote_loc < 0:
            raise RuntimeError("no remote locality joined within await timeout")
        for _ in range(prewarm):
            ext.fanout_fanin_remote(x, n, dispatch_timeout_s, composition_mode)
        t_all0 = time.perf_counter_ns()
        for _ in range(k):
            t0 = time.perf_counter_ns()
            last = ext.fanout_fanin_remote(x, n, dispatch_timeout_s, composition_mode)
            lat.append(time.perf_counter_ns() - t0)
        dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            with open(os.path.join(bootdir, "served1.ok"), "w") as fh:
                fh.write("served\n")
        except OSError:
            pass
        if proc is not None:
            try:
                proc.wait(timeout=max(10, serve_timeout))
            except Exception:  # noqa: BLE001
                proc.kill()
        if started:
            try:
                ext.shutdown()
            except Exception as exc:  # noqa: BLE001
                error = error or f"shutdown: {type(exc).__name__}: {exc}"

    attest = {}
    attest_path = os.path.join(bootdir, "attest_connect.json")
    if os.path.isfile(attest_path):
        try:
            with open(attest_path) as fh:
                attest = json.load(fh)
        except Exception:  # noqa: BLE001
            attest = {}

    if last is not None:
        (value, leaves, comp_prim, reduce_prim, _n_loc, _inner_n, timed_out,
         watchdog, ran_hpx, polled, hpx_native) = last
        records = [{"i": int(i), "value": int(v), "locality": int(loc)} for (i, v, loc) in leaves]
        measured_value = int(value)
    else:
        comp_prim, reduce_prim, timed_out = "unknown", "unknown", n
        watchdog, ran_hpx, polled, hpx_native = "unknown", False, False, False
        records, measured_value = [], None

    remote_joined = 1 if remote_loc >= 0 else 0
    localities_distinct = (root_loc is not None and remote_loc >= 0 and remote_loc != root_loc)
    connector_host = attest.get("hostname") or remote_host
    nodelay_verified = bool(attest.get("tcp_nodelay_attested") and attest.get("tcp_nodelay"))
    transport = attest.get("hpx_parcelport_transport", "unknown")

    art = build_arm_artifact(
        arm="hpx", island_id=island_id, x=x, n=n, placement_mode="all-remote",
        root_locality=(root_loc if root_loc is not None else 0),
        localities=[root_loc, remote_loc] if remote_loc >= 0 else [root_loc],
        leaf_records=records, threads_per_locality={remote_loc: connector_threads},
        latency_samples_ns=lat, dispatch_elapsed_s=dispatch_elapsed_s,
        dispatch_timeout_s=dispatch_timeout_s, prewarm_count=prewarm, expected_prewarm=prewarm,
        k=k, w=w, clock="perf_counter_ns", measured_value=measured_value, error=error,
        min_threads_per_locality=connector_min_threads,
        composition_primitive=comp_prim, reduce_primitive=reduce_prim,
        parcelport_transport=transport, timed_out_leaf_count=int(timed_out),
        remote_localities_joined=remote_joined, localities_distinct=localities_distinct,
        selected_subnet=prefer_subnet, root_hostname=_short_host(root_host),
        connector_hostname=_short_host(connector_host),
        root_endpoint=f"{root_ip}:{root_port}" if root_ip else None,
        connector_endpoint=attest.get("advertised_hpx_endpoint"),
        tcp_parcelport=(transport == "tcp"), hpx_tcp_nodelay_verified=nodelay_verified,
        root_threads=hpx_threads, connector_threads=connector_threads,
        root_cpuset=root_cpuset, connector_cpuset_effective=attest.get("connector_cpuset_effective"),
        connector_cpuset_min=connector_min_threads)
    _apply_composition_provenance(
        art, composition_primitive=comp_prim, watchdog=watchdog, ran_on_hpx_thread=ran_hpx,
        polled_in_success_path=polled, hpx_native_composition=hpx_native)
    art["slurm_job_id"] = slurm["slurm_job_id"]
    art["node_pair"] = [_short_host(root_host), _short_host(connector_host)]
    art["node_placement"] = node_placement
    art["bootstrap_dir"] = bootdir
    art["connector_attestation"] = attest
    art["cpu_bind_mask"] = cpu_bind_mask
    return art, bootdir


def _hpx_multi_remote_island(ext, slurm, connector_bin, root_host, remote_hosts, root_ip, *, x, n, k,
                             prewarm, hpx_threads, connector_threads, connector_min_threads,
                             connector_cpus_per_task, cpu_bind_masks, prefer_subnet, root_port,
                             await_timeout, serve_timeout, dispatch_timeout_s, island_id,
                             bootdir_prefix, w=None, composition_mode="root_flat_gather_poll"):
    """Slice 4a: ONE all-remote, ROUND-ROBIN HPX fanout island across >=2 remote localities (one
    connector per remote node). Start the embedded AGAS root, launch every connector, await >=R
    remotes (FAIL CLOSED if fewer join), time K calls, signal each connector, wait for GRACEFUL
    disconnect, aggregate per-connector attest + lifecycle, and build the artifact with multi-remote
    gates. HPX-only: NEVER sets same_axis_comparison. Returns (art, bootdirs).

    composition_primitive=root_flat_gather_reduce is an INTERIM stepping stone; the watchdog is the
    bounded is_ready poll (design debt). HPX collectives / tree reduction are the future target."""
    import subprocess
    import tempfile

    w = prewarm if w is None else w
    os.makedirs(EXP62_RUNS, exist_ok=True)
    R = len(remote_hosts)
    cpu_bind_masks = cpu_bind_masks or []
    procs, bootdirs = [], []
    started = False
    error = None
    last = None
    lat = []
    remote_locs = []
    root_loc = None
    root_cpuset = None
    dispatch_elapsed_s = None
    try:
        if R < 2:
            raise RuntimeError(f"Slice 4a needs >=2 remote hosts, got {R}")
        if not root_ip:
            raise RuntimeError(f"no root IP for --prefer-subnet {prefer_subnet}")
        root_extra = ["--hpx:ignore-batch-env",
                      f"--hpx:hpx={root_ip}:{root_port}", f"--hpx:agas={root_ip}:{root_port}",
                      "--hpx:expect-connecting-localities"]
        ext.start(hpx_threads, root_extra)
        started = True
        root_loc = int(ext.local_locality_id())
        root_cpuset = _effective_cpuset()
        # One connector per remote node. Each joins the SAME root AGAS and serves independently until
        # signalled (served1.ok) or serve-timeout, then post(disconnect)+stop. Mask per node may
        # differ; correctness is enforced by the attested-cpuset gate, not by the requested mask.
        for idx, rhost in enumerate(remote_hosts):
            bd = tempfile.mkdtemp(prefix=f"{bootdir_prefix}c{idx + 1}_", dir=EXP62_RUNS)
            bootdirs.append(bd)
            mask = cpu_bind_masks[idx] if idx < len(cpu_bind_masks) else None
            cmd = ["srun", "--nodes=1", "--ntasks=1", f"--nodelist={rhost}"]
            if connector_cpus_per_task:
                cmd.append(f"--cpus-per-task={connector_cpus_per_task}")
            if mask:
                cmd.append(f"--cpu-bind=mask_cpu:{mask}")
            cmd += [connector_bin, "--role=connect", f"--bootstrap={bd}",
                    f"--serve-timeout={serve_timeout}", f"--prefer-subnet={prefer_subnet}",
                    f"--agas-preprobe-host={root_ip}", f"--agas-preprobe-port={root_port}",
                    f"--hpx:threads={connector_threads}", "--hpx:bind=none",
                    "--hpx:ignore-batch-env", f"--hpx:agas={root_ip}:{root_port}"]
            procs.append(subprocess.Popen(cmd))
        # Await >=R remotes; FAIL CLOSED if fewer join (never relabel a partial join as multi-remote).
        joined = int(ext.await_remotes(R, await_timeout))
        if joined < R:
            raise RuntimeError(f"only {joined} of {R} remote localities joined within "
                               f"{await_timeout}s")
        remote_locs = [int(v) for v in ext.remote_locality_ids()]
        for _ in range(prewarm):
            ext.fanout_fanin_remote(x, n, dispatch_timeout_s, composition_mode)
        t_all0 = time.perf_counter_ns()
        for _ in range(k):
            t0 = time.perf_counter_ns()
            last = ext.fanout_fanin_remote(x, n, dispatch_timeout_s, composition_mode)
            lat.append(time.perf_counter_ns() - t0)
        dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    finally:
        # Signal every connector to stop serving, then wait for graceful disconnect of each.
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

    # Per-connector attest + lifecycle aggregation.
    connectors = []
    for idx, bd in enumerate(bootdirs):
        att, disc = {}, {}
        ap, dp = os.path.join(bd, "attest_connect.json"), os.path.join(bd, "connect.disconnected1")
        if os.path.isfile(ap):
            try:
                with open(ap) as fh:
                    att = json.load(fh)
            except Exception:  # noqa: BLE001
                att = {}
        if os.path.isfile(dp):
            try:
                with open(dp) as fh:
                    disc = json.load(fh)
            except Exception:  # noqa: BLE001
                disc = {}
        cpuset = att.get("connector_cpuset_effective")
        connectors.append({
            "bootstrap_dir": os.path.basename(bd),
            "hostname": _short_host(att.get("hostname")
                                    or (remote_hosts[idx] if idx < len(remote_hosts) else None)),
            "advertised_ip": att.get("advertised_ip"),
            "advertised_endpoint": att.get("advertised_hpx_endpoint"),
            "locality_id": att.get("locality_id"),
            "connector_cpuset_effective": cpuset,
            "cpuset_ok": _connector_cpuset_ok(cpuset, connector_min_threads),
            "transport": att.get("hpx_parcelport_transport", "unknown"),
            "tcp_nodelay_verified": bool(att.get("tcp_nodelay_attested") and att.get("tcp_nodelay")),
            "joined": os.path.isfile(os.path.join(bd, "connect.joined1")),
            "served": os.path.isfile(os.path.join(bd, "served1.ok")),
            "graceful_disconnect": os.path.isfile(dp) and bool(disc.get("served")),
        })

    # Leaf records from the last timed call (closed-int64 oracle reduction).
    if last is not None:
        (value, leaves, comp_prim, reduce_prim, _n_loc, _inner_n, timed_out,
         watchdog, ran_hpx, polled, hpx_native) = last
        records = [{"i": int(i), "value": int(v), "locality": int(loc)} for (i, v, loc) in leaves]
        measured_value = int(value)
    else:
        comp_prim, reduce_prim, timed_out = "root_flat_gather_reduce", "root_fold_sum_int64", n
        watchdog, ran_hpx, polled, hpx_native = "unknown", False, False, False
        records, measured_value = [], None

    localities_all = ([root_loc] + remote_locs) if root_loc is not None else list(remote_locs)
    threads_per_locality = {loc: connector_threads for loc in remote_locs}
    all_nodelay = bool(connectors) and all(c["tcp_nodelay_verified"] for c in connectors)
    transports = {c["transport"] for c in connectors}
    transport = transports.pop() if len(transports) == 1 else "mixed"

    art = build_arm_artifact(
        arm="hpx", island_id=island_id, x=x, n=n, placement_mode="all-remote",
        root_locality=(root_loc if root_loc is not None else 0), localities=localities_all,
        leaf_records=records, threads_per_locality=threads_per_locality,
        latency_samples_ns=lat, dispatch_elapsed_s=dispatch_elapsed_s,
        dispatch_timeout_s=dispatch_timeout_s, prewarm_count=prewarm, expected_prewarm=prewarm, k=k,
        w=w, clock="perf_counter_ns", measured_value=measured_value,
        error=error, min_threads_per_locality=connector_min_threads,
        composition_primitive=comp_prim, reduce_primitive=reduce_prim,
        parcelport_transport=transport, timed_out_leaf_count=int(timed_out),
        remote_localities_joined=len(remote_locs),
        localities_distinct=(len(set(remote_locs)) == len(remote_locs) and bool(remote_locs)),
        selected_subnet=prefer_subnet, root_hostname=_short_host(root_host),
        connector_hostname=None, tcp_parcelport=(transport == "tcp"),
        hpx_tcp_nodelay_verified=all_nodelay, root_threads=hpx_threads,
        connector_threads=connector_threads, root_cpuset=root_cpuset,
        connector_cpuset_effective=None, connector_cpuset_min=connector_min_threads)

    # Multi-remote fail-closed gates (added on top of the shared per-arm gates), via the pure helper
    # so the gate logic has a single source of truth the hermetic selftests exercise directly.
    witness = art["witness"]
    lpl = witness.get("leaves_per_locality", {})
    art["gates"].update(multi_remote_gates(witness, remote_locs, connectors))
    # Composition provenance + fail-closed consistency gate (records watchdog and whether the
    # success path polled); recomputes overall to include the multi-remote and composition gates.
    _apply_composition_provenance(
        art, composition_primitive=comp_prim, watchdog=watchdog, ran_on_hpx_thread=ran_hpx,
        polled_in_success_path=polled, hpx_native_composition=hpx_native)

    art["node_placement"] = NODE_PLACEMENT_MULTI
    art["node_set"] = [_short_host(root_host)] + [c["hostname"] for c in connectors]
    art["n_remote_localities"] = len(remote_locs)
    art["remote_locality_ids"] = remote_locs
    art["leaves_per_remote_locality"] = {str(loc): lpl.get(loc, 0) for loc in remote_locs}
    art["connectors"] = connectors
    art["slurm_job_id"] = slurm["slurm_job_id"]
    art["dry_run"] = True
    art["not_distributed_evidence_band"] = True  # HPX-only mechanism dry-run, not a same-axis band
    art["provenance"]["composition_note"] = (
        "root_flat_gather_reduce is an INTERIM stepping stone; when_all_then_reduce / dataflow_reduce "
        "are the pre-Slice-4b HPX-native continuation spike; HPX collectives / tree reduction remain "
        "the later target")
    return art, bootdirs


def run_hpx_remote_smoke(*, x=7, n=8, k=20, prewarm=5, hpx_threads=4, connector_threads=4,
                         connector_min_threads=2, prefer_subnet="10.42.5.", root_port=7910,
                         await_timeout=60, serve_timeout=120, dispatch_timeout_s=30.0,
                         composition_mode="root_flat_gather_poll",
                         import_fn=_default_hpx_import, env=None, write=True):
    """Slice 2 one-remote-locality all-remote HPX fanout DRY-RUN (small K). Skips cleanly off-cluster
    or when the connector/ext is unavailable. NO band, NO Ray, NO same_axis_comparison."""
    _warn_experimental_cross_node_composition(composition_mode, "hpx-remote-smoke")
    env = os.environ if env is None else env
    pre, reason = _hpx_remote_preconditions(env, import_fn, "hpx-remote-smoke")
    if reason:
        print(reason + " Real remote validation happens only on Rostam.")
        return "skip", None
    ext, slurm, connector_bin = pre
    root_host = slurm["hostnames"][0]
    remote_host = next((h for h in slurm["hostnames"] if h != root_host), None)
    root_ip = _first_ip_for_subnet(prefer_subnet)
    run_id = uuid.uuid4().hex[:12]
    art, bootdir = _hpx_remote_island(
        ext, slurm, connector_bin, root_host, remote_host, root_ip, x=x, n=n, k=k, prewarm=prewarm,
        hpx_threads=hpx_threads, connector_threads=connector_threads,
        connector_min_threads=connector_min_threads, connector_cpus_per_task=None,
        cpu_bind_mask=None, prefer_subnet=prefer_subnet, root_port=root_port,
        await_timeout=await_timeout, serve_timeout=serve_timeout,
        dispatch_timeout_s=dispatch_timeout_s, composition_mode=composition_mode,
        island_id=f"hpx-remote-{slurm['slurm_job_id']}-{run_id}",
        node_placement=NODE_PLACEMENT_CROSS,
        bootdir_prefix=f"hpx_remote_smoke_{slurm['slurm_job_id']}_{run_id}_")
    art["dry_run"] = True
    art["not_distributed_evidence_band"] = True
    _print_gate_summary("hpx-remote-smoke", art)
    print(f"hpx-remote-smoke: remote_loc={art['witness'].get('n_localities_covered')} "
          f"timed_out_leaf_count={art['timed_out_leaf_count']} node_pair={art['node_pair']} "
          f"error={art['error']}")
    if write:
        print(f"hpx-remote-smoke: wrote {os.path.basename(_write_local_artifact(art, 'hpx'))} "
              f"(gitignored); bootstrap {os.path.basename(bootdir)} under _exp62_runs/")
    return ("pass" if art["overall"] == "pass" else "fail"), art


def run_hpx_multi_remote_smoke(*, x=7, n=8, k=20, prewarm=5, hpx_threads=4, connector_threads=8,
                               connector_min_threads=8, n_remote=2, prefer_subnet="10.42.5.",
                               root_port=7910, cpu_bind_masks=None, await_timeout=90,
                               serve_timeout=180, dispatch_timeout_s=30.0,
                               composition_mode="root_flat_gather_poll",
                               import_fn=_default_hpx_import, env=None, write=True):
    """Slice 4a HPX-only >=2-remote-locality all-remote ROUND-ROBIN DRY-RUN (small K). Needs a
    >=(1+n_remote)-node Slurm allocation: root + n_remote distinct remote nodes. Skips cleanly
    off-cluster / without the connector or ext. NO band, NO Ray, NEVER same_axis_comparison."""
    _warn_experimental_cross_node_composition(composition_mode, "hpx-multi-remote-smoke")
    env = os.environ if env is None else env
    pre, reason = _hpx_remote_preconditions(env, import_fn, "hpx-multi-remote-smoke")
    if reason:
        print(reason + " Real multi-remote validation happens only on Rostam.")
        return "skip", None
    ext, slurm, connector_bin = pre
    hostnames = slurm["hostnames"]
    root_host = hostnames[0]
    remote_hosts = [h for h in hostnames if h != root_host][:n_remote]
    if len(remote_hosts) < max(2, n_remote):
        print(f"hpx-multi-remote-smoke: SKIP -- need >=2 remote nodes distinct from root "
              f"(hostnames={hostnames}, n_remote={n_remote}).")
        return "skip", None
    root_ip = _first_ip_for_subnet(prefer_subnet)
    run_id = uuid.uuid4().hex[:12]
    art, bootdirs = _hpx_multi_remote_island(
        ext, slurm, connector_bin, root_host, remote_hosts, root_ip, x=x, n=n, k=k, prewarm=prewarm,
        hpx_threads=hpx_threads, connector_threads=connector_threads,
        connector_min_threads=connector_min_threads, connector_cpus_per_task=connector_threads,
        cpu_bind_masks=cpu_bind_masks, prefer_subnet=prefer_subnet, root_port=root_port,
        await_timeout=await_timeout, serve_timeout=serve_timeout,
        dispatch_timeout_s=dispatch_timeout_s, composition_mode=composition_mode,
        island_id=f"hpx-multi-remote-{slurm['slurm_job_id']}-{run_id}",
        bootdir_prefix=f"hpx_multi_remote_smoke_{slurm['slurm_job_id']}_{run_id}_")
    _print_gate_summary("hpx-multi-remote-smoke", art)
    print(f"hpx-multi-remote-smoke: n_remote_localities={art['n_remote_localities']} "
          f"remote_locality_ids={art['remote_locality_ids']} "
          f"leaves_per_remote_locality={art['leaves_per_remote_locality']} "
          f"timed_out_leaf_count={art['timed_out_leaf_count']} node_set={art['node_set']} "
          f"error={art['error']}")
    for c in art["connectors"]:
        print(f"  connector loc={c['locality_id']} host={c['hostname']} "
              f"cpuset={c['connector_cpuset_effective']} cpuset_ok={c['cpuset_ok']} "
              f"nodelay={c['tcp_nodelay_verified']} joined={c['joined']} served={c['served']} "
              f"graceful={c['graceful_disconnect']}")
    if write:
        print(f"hpx-multi-remote-smoke: wrote {os.path.basename(_write_local_artifact(art, 'hpx'))} "
              f"(gitignored); {len(bootdirs)} bootstrap dirs under _exp62_runs/")
    return ("pass" if art["overall"] == "pass" else "fail"), art


def run_hpx_multi_remote_fanout(*, band_id, island_index, x=7, n=8, k=1000, w=100, prewarm=1,
                                hpx_threads=4, connector_threads=8, n_remote=2,
                                prefer_subnet="10.42.5.", root_port=7910, cpu_bind_masks=None,
                                await_timeout=90, serve_timeout=240, dispatch_timeout_s=30.0,
                                composition_mode="root_flat_gather_poll",
                                import_fn=_default_hpx_import, env=None, write=True):
    """Slice 4b HPX BAND island: ONE all-remote round-robin fanout across >=2 remote localities on the
    PROVEN root_flat_gather_poll composition, written as a raw gitignored band island for matched-band
    correlation (node_placement=cross_node_multi_remote). ONE island per process. Skips cleanly
    off-cluster / without the connector or ext."""
    _warn_experimental_cross_node_composition(composition_mode, "hpx-multi-remote-fanout")
    env = os.environ if env is None else env
    pre, reason = _hpx_remote_preconditions(env, import_fn, "hpx-multi-remote-fanout")
    if reason:
        print(reason)
        return "skip", None
    ext, slurm, connector_bin = pre
    hostnames = slurm["hostnames"]
    root_host = hostnames[0]
    remote_hosts = [h for h in hostnames if h != root_host][:n_remote]
    if len(remote_hosts) < max(2, n_remote):
        print(f"hpx-multi-remote-fanout: SKIP -- need >=2 remote nodes distinct from root "
              f"(hostnames={hostnames}, n_remote={n_remote}).")
        return "skip", None
    root_ip = _first_ip_for_subnet(prefer_subnet)
    art, _bootdirs = _hpx_multi_remote_island(
        ext, slurm, connector_bin, root_host, remote_hosts, root_ip, x=x, n=n, k=k, w=w,
        prewarm=prewarm, hpx_threads=hpx_threads, connector_threads=connector_threads,
        connector_min_threads=connector_threads, connector_cpus_per_task=connector_threads,
        cpu_bind_masks=cpu_bind_masks, prefer_subnet=prefer_subnet, root_port=root_port,
        await_timeout=await_timeout, serve_timeout=serve_timeout,
        dispatch_timeout_s=dispatch_timeout_s, composition_mode=composition_mode,
        island_id=f"{band_id}-i{island_index}-hpx",
        bootdir_prefix=f"mrband_{slurm['slurm_job_id']}_{band_id}_i{island_index}_hpx_")
    # Band island (not a dry-run mechanism smoke): it IS distributed evidence.
    art["dry_run"] = False
    art["not_distributed_evidence_band"] = False
    art["band_island"] = True
    art["band_id"] = band_id
    art["island_index"] = island_index
    _print_gate_summary(f"hpx-multi-remote-fanout[i{island_index}]", art)
    print(f"hpx-multi-remote-fanout[i{island_index}]: "
          f"n_remote_localities={art.get('n_remote_localities')} "
          f"leaves_per_remote_locality={art.get('leaves_per_remote_locality')} "
          f"node_set={art.get('node_set')} error={art['error']}")
    if write:
        _write_band_island(art, band_id, island_index, "hpx")
    return ("pass" if art["overall"] == "pass" else "fail"), art


def _write_band_island(artifact, band_id, island_index, arm, exp_dir=None):
    """Write a raw, GITIGNORED band island artifact: exp62_fanout_<band_id>_i<idx>_<arm>.json."""
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    path = os.path.join(exp_dir, f"exp62_fanout_{band_id}_i{island_index}_{arm}.json")
    with open(path, "w") as fh:
        json.dump(artifact, fh, indent=2, sort_keys=True)
    return path


def run_hpx_fanout(*, band_id, island_index, x=7, n=8, k=1000, w=100, prewarm=1, hpx_threads=4,
                   connector_threads=8, prefer_subnet="10.42.5.", root_port=7910,
                   cpu_bind_mask="0xFF", await_timeout=60, serve_timeout=240,
                   dispatch_timeout_s=30.0, composition_mode="root_flat_gather_poll",
                   import_fn=_default_hpx_import, env=None, write=True):
    """Slice 3 HPX band island (ONE island per process, to avoid in-process HPX re-init). K=1000/
    W=100/prewarm=1 default; connector pinned to an 8-core mask. Writes a raw gitignored island
    artifact. Skips cleanly off-cluster / without the connector or ext."""
    _warn_experimental_cross_node_composition(composition_mode, "hpx-fanout")
    env = os.environ if env is None else env
    pre, reason = _hpx_remote_preconditions(env, import_fn, "hpx-fanout")
    if reason:
        print(reason)
        return "skip", None
    ext, slurm, connector_bin = pre
    root_host = slurm["hostnames"][0]
    remote_host = next((h for h in slurm["hostnames"] if h != root_host), None)
    root_ip = _first_ip_for_subnet(prefer_subnet)
    art, bootdir = _hpx_remote_island(
        ext, slurm, connector_bin, root_host, remote_host, root_ip, x=x, n=n, k=k, w=w,
        prewarm=prewarm, hpx_threads=hpx_threads, connector_threads=connector_threads,
        connector_min_threads=connector_threads, connector_cpus_per_task=connector_threads,
        cpu_bind_mask=cpu_bind_mask, prefer_subnet=prefer_subnet, root_port=root_port,
        await_timeout=await_timeout, serve_timeout=serve_timeout,
        dispatch_timeout_s=dispatch_timeout_s, composition_mode=composition_mode,
        island_id=f"{band_id}-i{island_index}-hpx", node_placement=NODE_PLACEMENT_CROSS,
        bootdir_prefix=f"band_{slurm['slurm_job_id']}_{band_id}_i{island_index}_hpx_")
    art["dry_run"] = False
    art["band_island"] = True
    art["band_id"] = band_id
    art["island_index"] = island_index
    cpuset = art["provenance"].get("connector_cpuset_effective")
    _print_gate_summary(f"hpx-fanout[i{island_index}]", art)
    print(f"hpx-fanout[i{island_index}]: connector_cpuset={cpuset} cpu_bind_mask={cpu_bind_mask} "
          f"timed_out={art['timed_out_leaf_count']} node_pair={art['node_pair']} error={art['error']}")
    if write:
        print(f"hpx-fanout[i{island_index}]: wrote "
              f"{os.path.basename(_write_band_island(art, band_id, island_index, 'hpx'))} "
              f"(gitignored)")
    return ("pass" if art["overall"] == "pass" else "fail"), art


# ---------------------------------------------------------------------------
# Slice 3 Ray distributed fanout arm (Ray-on-Slurm, modeled on exp59). Ray-PATTERN mapping, NOT HPX
# best practice. Coordinator on node A fans out N leaf TASKS hard-pinned to node B and reduces; ONE
# caller boundary: ray.get(coordinator.remote(x, N)). Skips cleanly off-cluster / without Ray.
# ---------------------------------------------------------------------------

def _sh(cmd, timeout=120, env=None):
    import subprocess
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout,
                           env=env, text=True)
        return p.returncode, p.stdout, p.stderr
    except Exception as exc:  # noqa: BLE001
        return 1, "", f"[exec error] {exc}"


def _node_ip_on_subnet(node, prefer_subnet, env=None):
    """First IPv4 of `node` matching prefer_subnet, via `srun --overlap hostname -I`. None on miss."""
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "hostname", "-I"],
                     timeout=60, env=env)
    if rc != 0:
        return None
    for tok in out.split():
        if tok.count(".") == 3 and tok.startswith(prefer_subnet):
            return tok
    return None


def _ray_popen_block(cmd, env, log_path):
    """Launch a PERSISTENT `ray start --block` srun step via Popen (exp59 launcher-lifetime fix):
    Slurm reaps a non-blocking `ray start` step and kills the daemons, so we keep the step alive."""
    import subprocess
    lf = open(log_path, "ab", buffering=0)
    p = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                         env=env, start_new_session=True)
    p._exp62_logfile = lf  # keep handle to close at cleanup
    return p


def _ray_stop_node(node, env):
    return _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "ray", "stop", "--force"],
               timeout=90, env=env)


def _orphan_check_node(node, env, patterns=("raylet", "gcs_server", "plasma")):
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "pgrep", "-af",
                      "|".join(patterns)], timeout=60, env=env)
    # pgrep -af with a pattern list: fall back to a simple ps grep if needed.
    if rc not in (0, 1):
        rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "bash", "-lc",
                          "ps -e -o comm= | grep -E 'raylet|gcs_server|plasma' || true"],
                         timeout=60, env=env)
    # Drop the detector's OWN command lines: `pgrep -af <pattern>` is run under an srun wrapper whose
    # argv literally contains the pattern string, so pgrep matches that srun/pgrep and self-reports a
    # phantom orphan. Real ray daemons (raylet/gcs_server/plasma_store) never carry srun/pgrep in argv.
    return [ln for ln in (out or "").splitlines()
            if ln.strip() and "pgrep" not in ln and "srun" not in ln]


def run_ray_fanout(*, band_id, islands=5, x=7, n=8, k=1000, w=100, prewarm=1,
                   prefer_subnet="10.42.5.", ray_port=6379, worker_cpus=8,
                   ray_dispatch_timeout_s=30.0,
                   import_fn=_default_ray_import, env=None, write=True):
    """Slice 3 Ray band arm: bootstrap a 2-node Ray cluster (head on node A, worker on node B), run R
    islands of hard-pinned coordinator/leaf fanout, write R raw gitignored ray island artifacts, then
    tear the cluster down with an orphan check. Skips cleanly off-cluster / without Ray."""
    env = dict(os.environ if env is None else env)
    slurm = _slurm_info(env)
    if not slurm["two_node"]:
        print(f"ray-fanout: SKIP -- need a >=2-node Slurm allocation (hostnames={slurm['hostnames']}).")
        return "skip", []
    try:
        ray = import_fn()
    except ImportError as exc:
        print(f"ray-fanout: SKIP -- ray not installed ({exc}).")
        return "skip", []

    os.makedirs(EXP62_RUNS, exist_ok=True)
    node_a, node_b = slurm["hostnames"][0], slurm["hostnames"][1]
    ip_a = _node_ip_on_subnet(node_a, prefer_subnet, env)
    ip_b = _node_ip_on_subnet(node_b, prefer_subnet, env)
    logdir = EXP62_RUNS
    head_log = os.path.join(logdir, f"ray_head_{slurm['slurm_job_id']}_{band_id}.log")
    worker_log = os.path.join(logdir, f"ray_worker_{slurm['slurm_job_id']}_{band_id}.log")

    head_p = worker_p = None
    error = None
    arts = []
    no_orphans = None
    try:
        if not ip_a or not ip_b:
            raise RuntimeError(f"could not resolve subnet IPs (a={ip_a} b={ip_b})")
        # 1) head on node A, worker on node B -- both PERSISTENT (--block) so Slurm does not reap them.
        # The head intentionally advertises 0 CPUs: node A is coordinator/control-plane only, all leaf
        # compute consumes node B CPUs. The coordinator is therefore submitted with num_cpus=0 (below)
        # so it can schedule on the 0-CPU head; otherwise Ray would treat it as a 1-CPU task and, with
        # hard soft=False pinning to node A, it would stay PENDING forever.
        head_num_cpus = 0
        head_cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", node_a, "--export=ALL",
                    "ray", "start", "--head", "--node-ip-address", ip_a, "--port", str(ray_port),
                    "--include-dashboard", "false", "--num-cpus", str(head_num_cpus), "--block"]
        head_p = _ray_popen_block(head_cmd, env, head_log)
        # readiness: poll head GCS reachability before the worker joins.
        _wait_tcp(ip_a, ray_port, timeout_s=90)
        worker_cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", node_b, "--export=ALL",
                      "ray", "start", "--address", f"{ip_a}:{ray_port}", "--node-ip-address", ip_b,
                      "--num-cpus", str(worker_cpus), "--block"]
        worker_p = _ray_popen_block(worker_cmd, env, worker_log)
        # 2) driver connects (runs on node A); wait for both nodes Alive.
        ray.init(address=f"{ip_a}:{ray_port}", log_to_driver=False)
        _wait_ray_nodes(ray, expected=2, timeout_s=120)
        ray_version = getattr(ray, "__version__", None)
        nid_a = _ray_node_id_for_ip(ray, ip_a)
        nid_b = _ray_node_id_for_ip(ray, ip_b)
        if not nid_a or not nid_b or nid_a == nid_b:
            raise RuntimeError(f"could not resolve distinct Ray node ids (a={nid_a} b={nid_b})")

        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        from ray.exceptions import GetTimeoutError
        leaf_xor = LEAF_XOR

        @ray.remote
        def _leaf(xx, i):
            node = str(ray.get_runtime_context().get_node_id())
            return (int(i), i64((int(xx) ^ leaf_xor) + (int(i) << 1)), node)

        @ray.remote
        def _coordinator(xx, nn, target_nid):
            strat = NodeAffinitySchedulingStrategy(node_id=target_nid, soft=False)
            futs = [_leaf.options(scheduling_strategy=strat).remote(xx, i) for i in range(nn)]
            recs = ray.get(futs)
            return i64(sum(int(r[1]) for r in recs)), recs, str(ray.get_runtime_context().get_node_id())

        # Coordinator pinned HARD to node A; num_cpus=0 so it schedules on the 0-CPU head as a pure
        # orchestrator (it only fans out + reduces, runs NO leaves). Leaves are pinned HARD to node B.
        coord_strat = NodeAffinitySchedulingStrategy(node_id=nid_a, soft=False)
        coord_num_cpus = 0

        def _submit_coordinator():
            # ONE caller-boundary submission: ray.get(coordinator.remote(x, N)), bounded so a
            # placement failure FAILS CLOSED (records an artifact) instead of hanging to SLURM timeout.
            return ray.get(_coordinator.options(num_cpus=coord_num_cpus,
                                                scheduling_strategy=coord_strat).remote(x, n, nid_b),
                           timeout=ray_dispatch_timeout_s)

        for idx in range(1, islands + 1):
            lat, last, dispatch_timed_out = [], None, False
            t_all0 = time.perf_counter_ns()
            try:
                for _ in range(prewarm):
                    _submit_coordinator()
                for _ in range(k):
                    t0 = time.perf_counter_ns()
                    last = _submit_coordinator()
                    lat.append(time.perf_counter_ns() - t0)
            except GetTimeoutError:
                dispatch_timed_out = True
                print(f"ray-fanout[i{idx}]: GetTimeoutError after {ray_dispatch_timeout_s}s -- "
                      f"coordinator could not be scheduled / completed; failing closed.")
            dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9

            if last is None:
                # No coordinator result within the bounded timeout: fail closed with an artifact.
                value, records, coord_node, per_node, leaves_on_target = None, [], None, {}, False
            else:
                value, recs, coord_node = last
                records = [{"i": int(i), "value": int(v), "locality": str(loc)} for (i, v, loc) in recs]
                per_node = {}
                for rec in records:
                    per_node[rec["locality"]] = per_node.get(rec["locality"], 0) + 1
                leaves_on_target = bool(records) and all(r["locality"] == nid_b for r in records)

            island_error = (f"ray dispatch timeout after {ray_dispatch_timeout_s}s"
                            if dispatch_timed_out else None)
            art = build_arm_artifact(
                arm="ray", island_id=f"{band_id}-i{idx}-ray", x=x, n=n, placement_mode="all-remote",
                root_locality=nid_a, localities=[nid_a, nid_b], leaf_records=records,
                threads_per_locality={nid_b: worker_cpus}, latency_samples_ns=lat,
                dispatch_elapsed_s=dispatch_elapsed_s, dispatch_timeout_s=None,
                prewarm_count=prewarm, expected_prewarm=prewarm, k=k, w=w,
                clock="perf_counter_ns",
                measured_value=(int(value) if value is not None else None), error=island_error,
                min_threads_per_locality=1, composition_primitive="ray.coordinator.remote",
                reduce_primitive="coordinator_fold_sum_int64", parcelport_transport="ray_object_transport",
                timed_out_leaf_count=0, remote_localities_joined=1,
                localities_distinct=(nid_a != nid_b), selected_subnet=prefer_subnet,
                root_hostname=_short_host(node_a), connector_hostname=_short_host(node_b),
                root_threads=None, connector_threads=worker_cpus)
            # Ray arm: no HPX dispatch-timeout concept. no_dispatch_timeout reflects the bounded
            # coordinator ray.get guard: True only if the coordinator completed within the timeout.
            ray_no_dispatch_timeout = not dispatch_timed_out
            art["gates"]["no_dispatch_timeout"] = ray_no_dispatch_timeout
            art["overall"] = "pass" if all(art["gates"].values()) else "fail"
            art["node_pair"] = [_short_host(node_a), _short_host(node_b)]
            art["node_placement"] = NODE_PLACEMENT_CROSS
            art["slurm_job_id"] = slurm["slurm_job_id"]
            art["band_island"] = True
            art["band_id"] = band_id
            art["island_index"] = idx
            art["ray_placement"] = {
                "hard_placement": True, "soft": False, "single_submission": True,
                "coordinator_single_submission": True,
                "leaves_on_target_node": leaves_on_target, "driver_node_id": nid_a,
                "coordinator_node_id": coord_node,
                "coordinator_on_driver_node": (coord_node is not None and coord_node == nid_a),
                "target_node_id": nid_b,
                "ray_head_num_cpus": head_num_cpus, "ray_coordinator_num_cpus": coord_num_cpus,
                "ray_dispatch_timeout_s": ray_dispatch_timeout_s,
                "ray_no_dispatch_timeout": ray_no_dispatch_timeout,
                "leaves_per_node": per_node, "ray_version": ray_version,
                "ip_a": ip_a, "ip_b": ip_b}
            arts.append(art)
            _print_gate_summary(f"ray-fanout[i{idx}]", art)
            print(f"ray-fanout[i{idx}]: leaves_on_target={leaves_on_target} per_node={per_node} "
                  f"coord_node==A={coord_node == nid_a}")
            if write:
                _write_band_island(art, band_id, idx, "ray")
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        print(f"ray-fanout: ERROR {error}")
    finally:
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass
        _ray_stop_node(node_b, env)
        _ray_stop_node(node_a, env)
        for p in (worker_p, head_p):
            if p is not None:
                try:
                    p.terminate(); p.wait(timeout=20)
                except Exception:  # noqa: BLE001
                    p.kill()
                try:
                    p._exp62_logfile.close()
                except Exception:  # noqa: BLE001
                    pass
        orph = (_orphan_check_node(node_a, env) + _orphan_check_node(node_b, env))
        no_orphans = (len(orph) == 0)
        print(f"ray-fanout: teardown no_orphans={no_orphans}"
              + (f" orphans={orph}" if orph else ""))

    status = "pass" if (error is None and arts and all(a["overall"] == "pass" for a in arts)
                        and no_orphans) else "fail"
    return status, arts


def run_ray_multi_remote_fanout(*, band_id, islands=5, x=7, n=8, k=1000, w=100, prewarm=1,
                                n_remote=2, prefer_subnet="10.42.5.", ray_port=6379, worker_cpus=8,
                                ray_dispatch_timeout_s=30.0,
                                import_fn=_default_ray_import, env=None, write=True):
    """Slice 4b Ray multi-remote band arm: head on node A (num_cpus=0, control-plane only) + workers
    on >=2 remote nodes; a coordinator hard-pinned to node A (num_cpus=0, runs ZERO leaves) hard-pins
    N leaves ROUND-ROBIN across the remote node ids. One caller-boundary submission per timed call.
    Writes R raw gitignored ray band islands, tears the cluster down with an orphan check. Skips
    cleanly off-cluster / without Ray. node_placement=cross_node_multi_remote."""
    env = dict(os.environ if env is None else env)
    slurm = _slurm_info(env)
    hostnames = slurm["hostnames"]
    if len(hostnames) < (1 + n_remote):
        print(f"ray-multi-remote-fanout: SKIP -- need >=1+{n_remote} nodes "
              f"(hostnames={hostnames}).")
        return "skip", []
    try:
        ray = import_fn()
    except ImportError as exc:
        print(f"ray-multi-remote-fanout: SKIP -- ray not installed ({exc}).")
        return "skip", []

    os.makedirs(EXP62_RUNS, exist_ok=True)
    node_a = hostnames[0]
    remote_nodes = hostnames[1:1 + n_remote]
    node_set = [_short_host(node_a)] + sorted(_short_host(h) for h in remote_nodes)
    ip_a = _node_ip_on_subnet(node_a, prefer_subnet, env)
    remote_ips = [_node_ip_on_subnet(h, prefer_subnet, env) for h in remote_nodes]
    head_log = os.path.join(EXP62_RUNS, f"ray_head_{slurm['slurm_job_id']}_{band_id}.log")

    head_p, worker_ps = None, []
    error, arts, no_orphans = None, [], None
    try:
        if not ip_a or any(ip is None for ip in remote_ips):
            raise RuntimeError(f"could not resolve subnet IPs (a={ip_a} remotes={remote_ips})")
        head_num_cpus = 0  # node A is coordinator/control-plane only; all leaf compute is remote.
        head_cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", node_a, "--export=ALL",
                    "ray", "start", "--head", "--node-ip-address", ip_a, "--port", str(ray_port),
                    "--include-dashboard", "false", "--num-cpus", str(head_num_cpus), "--block"]
        head_p = _ray_popen_block(head_cmd, env, head_log)
        _wait_tcp(ip_a, ray_port, timeout_s=90)
        for h, ip_h in zip(remote_nodes, remote_ips):
            wlog = os.path.join(EXP62_RUNS, f"ray_worker_{_short_host(h)}_{slurm['slurm_job_id']}_"
                                            f"{band_id}.log")
            wcmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", h, "--export=ALL",
                    "ray", "start", "--address", f"{ip_a}:{ray_port}", "--node-ip-address", ip_h,
                    "--num-cpus", str(worker_cpus), "--block"]
            worker_ps.append(_ray_popen_block(wcmd, env, wlog))
        ray.init(address=f"{ip_a}:{ray_port}", log_to_driver=False)
        _wait_ray_nodes(ray, expected=1 + n_remote, timeout_s=120)
        ray_version = getattr(ray, "__version__", None)
        nid_a = _ray_node_id_for_ip(ray, ip_a)
        remote_nids = [_ray_node_id_for_ip(ray, ip) for ip in remote_ips]
        if not nid_a or any(nid is None for nid in remote_nids) \
                or len(set([nid_a] + remote_nids)) != (1 + n_remote):
            raise RuntimeError(f"could not resolve distinct Ray node ids "
                               f"(a={nid_a} remotes={remote_nids})")

        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        from ray.exceptions import GetTimeoutError
        leaf_xor = LEAF_XOR

        @ray.remote
        def _leaf(xx, i):
            return (int(i), i64((int(xx) ^ leaf_xor) + (int(i) << 1)),
                    str(ray.get_runtime_context().get_node_id()))

        @ray.remote
        def _coordinator(xx, nn, target_nids):
            # round-robin leaves across the remote node ids, each HARD-pinned (soft=False).
            r = len(target_nids)
            futs = []
            for i in range(nn):
                strat = NodeAffinitySchedulingStrategy(node_id=target_nids[i % r], soft=False)
                futs.append(_leaf.options(scheduling_strategy=strat).remote(xx, i))
            recs = ray.get(futs)
            return (i64(sum(int(rr[1]) for rr in recs)), recs,
                    str(ray.get_runtime_context().get_node_id()))

        coord_strat = NodeAffinitySchedulingStrategy(node_id=nid_a, soft=False)
        coord_num_cpus = 0

        def _submit():
            return ray.get(_coordinator.options(num_cpus=coord_num_cpus,
                                                scheduling_strategy=coord_strat)
                           .remote(x, n, remote_nids), timeout=ray_dispatch_timeout_s)

        for idx in range(1, islands + 1):
            lat, last, dispatch_timed_out = [], None, False
            t_all0 = time.perf_counter_ns()
            try:
                for _ in range(prewarm):
                    _submit()
                for _ in range(k):
                    t0 = time.perf_counter_ns()
                    last = _submit()
                    lat.append(time.perf_counter_ns() - t0)
            except GetTimeoutError:
                dispatch_timed_out = True
                print(f"ray-multi-remote-fanout[i{idx}]: GetTimeoutError after "
                      f"{ray_dispatch_timeout_s}s -- failing closed.")
            dispatch_elapsed_s = (time.perf_counter_ns() - t_all0) / 1e9

            if last is None:
                value, records, coord_node, per_node = None, [], None, {}
            else:
                value, recs, coord_node = last
                records = [{"i": int(i), "value": int(v), "locality": str(loc)}
                           for (i, v, loc) in recs]
                per_node = {}
                for rec in records:
                    per_node[rec["locality"]] = per_node.get(rec["locality"], 0) + 1

            island_error = (f"ray dispatch timeout after {ray_dispatch_timeout_s}s"
                            if dispatch_timed_out else None)
            art = build_arm_artifact(
                arm="ray", island_id=f"{band_id}-i{idx}-ray", x=x, n=n, placement_mode="all-remote",
                root_locality=nid_a, localities=[nid_a] + remote_nids, leaf_records=records,
                threads_per_locality={nid: worker_cpus for nid in remote_nids},
                latency_samples_ns=lat, dispatch_elapsed_s=dispatch_elapsed_s, dispatch_timeout_s=None,
                prewarm_count=prewarm, expected_prewarm=prewarm, k=k, w=w, clock="perf_counter_ns",
                measured_value=(int(value) if value is not None else None), error=island_error,
                min_threads_per_locality=1, composition_primitive="ray.coordinator.remote",
                reduce_primitive="coordinator_fold_sum_int64",
                parcelport_transport="ray_object_transport", timed_out_leaf_count=0,
                remote_localities_joined=n_remote, localities_distinct=True,
                selected_subnet=prefer_subnet, root_hostname=_short_host(node_a),
                connector_hostname=None, root_threads=None, connector_threads=worker_cpus)
            ray_no_dispatch_timeout = not dispatch_timed_out
            # Ray multi-remote gates (round-robin coverage, coordinator zero-leaf, hard placement).
            art["gates"].update(ray_multi_remote_gates(
                witness=art["witness"], remote_node_ids=remote_nids, leaves_per_node=per_node,
                coordinator_node_id=coord_node, driver_node_id=nid_a, head_num_cpus=head_num_cpus,
                coordinator_num_cpus=coord_num_cpus, hard_placement=True,
                cluster_connected=True, n=n))
            art["gates"]["no_dispatch_timeout"] = ray_no_dispatch_timeout
            art["overall"] = "pass" if all(art["gates"].values()) else "fail"
            art["node_set"] = node_set
            art["node_placement"] = NODE_PLACEMENT_MULTI
            art["slurm_job_id"] = slurm["slurm_job_id"]
            art["band_island"] = True
            art["band_id"] = band_id
            art["island_index"] = idx
            art["n_remote_localities"] = n_remote
            art["ray_placement"] = {
                "hard_placement": True, "soft": False, "single_submission": True,
                "coordinator_single_submission": True, "driver_node_id": nid_a,
                "coordinator_node_id": coord_node,
                "coordinator_on_driver_node": (coord_node is not None and coord_node == nid_a),
                "target_node_ids": remote_nids, "remote_hostnames": [_short_host(h)
                                                                     for h in remote_nodes],
                "ray_head_num_cpus": head_num_cpus, "ray_coordinator_num_cpus": coord_num_cpus,
                "ray_dispatch_timeout_s": ray_dispatch_timeout_s,
                "ray_no_dispatch_timeout": ray_no_dispatch_timeout,
                "leaves_per_node": per_node, "ray_version": ray_version,
                "ip_a": ip_a, "remote_ips": remote_ips}
            arts.append(art)
            _print_gate_summary(f"ray-multi-remote-fanout[i{idx}]", art)
            print(f"ray-multi-remote-fanout[i{idx}]: per_node={per_node} "
                  f"coord_on_driver={coord_node == nid_a} node_set={node_set}")
            if write:
                _write_band_island(art, band_id, idx, "ray")
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        print(f"ray-multi-remote-fanout: ERROR {error}")
    finally:
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass
        for h in remote_nodes:
            _ray_stop_node(h, env)
        _ray_stop_node(node_a, env)
        for p in ([*worker_ps, head_p]):
            if p is not None:
                try:
                    p.terminate(); p.wait(timeout=20)
                except Exception:  # noqa: BLE001
                    p.kill()
                try:
                    p._exp62_logfile.close()
                except Exception:  # noqa: BLE001
                    pass
        orph = _orphan_check_node(node_a, env)
        for h in remote_nodes:
            orph += _orphan_check_node(h, env)
        no_orphans = (len(orph) == 0)
        print(f"ray-multi-remote-fanout: teardown no_orphans={no_orphans}"
              + (f" orphans={orph}" if orph else ""))

    status = "pass" if (error is None and arts and all(a["overall"] == "pass" for a in arts)
                        and no_orphans) else "fail"
    return status, arts


def _wait_tcp(ip, port, timeout_s=90):
    import socket
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        s = socket.socket()
        s.settimeout(2)
        try:
            s.connect((ip, int(port)))
            s.close()
            return True
        except Exception:  # noqa: BLE001
            try:
                s.close()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1.5)
    raise RuntimeError(f"GCS {ip}:{port} not reachable within {timeout_s}s")


def _wait_ray_nodes(ray, expected, timeout_s=120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        alive = len([nd for nd in ray.nodes() if nd.get("Alive")])
        if alive >= expected:
            return True
        time.sleep(1.5)
    raise RuntimeError(f"only {alive} Ray nodes Alive (<{expected}) within {timeout_s}s")


def _ray_node_id_for_ip(ray, ip):
    for nd in ray.nodes():
        if nd.get("Alive") and nd.get("NodeManagerAddress") == ip:
            return nd.get("NodeID")
    return None


# ---------------------------------------------------------------------------
# Slice 3 island loaders + manifest / band-aggregate phases (load raw island files, no cluster).
# ---------------------------------------------------------------------------

def _load_band_island(band_id, island_index, arm, exp_dir=None):
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    path = os.path.join(exp_dir, f"exp62_fanout_{band_id}_i{island_index}_{arm}.json")
    if not os.path.isfile(path):
        return None
    with open(path) as fh:
        return json.load(fh)


def _write_trackable(payload, name, exp_dir=None):
    exp_dir = os.path.dirname(os.path.abspath(__file__)) if exp_dir is None else exp_dir
    path = os.path.join(exp_dir, name)
    with open(path, "w") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    return path


def phase_pair_manifest_fanout(*, band_id, islands=5, x=7, n=8, write=True, exp_dir=None):
    """Build one TRACKABLE manifest per matched island from the raw island files. Returns
    (overall, [statuses])."""
    statuses = []
    for idx in range(1, islands + 1):
        hpx = _load_band_island(band_id, idx, "hpx", exp_dir)
        ray = _load_band_island(band_id, idx, "ray", exp_dir)
        if hpx is None or ray is None:
            print(f"pair-manifest-fanout[i{idx}]: FAIL -- missing island file "
                  f"(hpx={hpx is not None} ray={ray is not None})")
            statuses.append("fail")
            continue
        man = build_pair_manifest(manifest_id=f"{band_id}-i{idx}", x=x, n=n,
                                  ray_artifact=ray, hpx_artifact=hpx)
        statuses.append(man["overall"])
        failed = [g for g, v in man["gates"].items() if not v]
        print(f"pair-manifest-fanout[i{idx}]: overall={man['overall']}"
              + ("" if man["overall"] == "pass" else f" failed_gates={failed}"))
        if write:
            _write_trackable(man, f"exp62_fanout_{band_id}_i{idx}_manifest.json", exp_dir)
    overall = "pass" if statuses and all(s == "pass" for s in statuses) else "fail"
    return overall, statuses


def phase_band_aggregate_fanout(*, band_id, job_id, islands=5, write=True, exp_dir=None):
    """Build the ONE TRACKABLE combined band aggregate from the raw island files."""
    hpx_islands, ray_islands = [], []
    for idx in range(1, islands + 1):
        h = _load_band_island(band_id, idx, "hpx", exp_dir)
        r = _load_band_island(band_id, idx, "ray", exp_dir)
        if h is not None:
            hpx_islands.append(h)
        if r is not None:
            ray_islands.append(r)
    payload, overall = build_fanout_band_aggregate(
        hpx_islands=hpx_islands, ray_islands=ray_islands, job_id=job_id, band_id=band_id)
    failed = [g for g, v in payload["gates"].items() if not v]
    print(f"band-aggregate-fanout: overall={overall} same_axis_comparison="
          f"{payload['same_axis_comparison']}" + ("" if overall == "pass" else f" failed={failed}"))
    if write:
        name = f"exp62_fanout_band_{job_id}_aggregate.json"
        print(f"band-aggregate-fanout: wrote {os.path.basename(_write_trackable(payload, name, exp_dir))} "
              "(trackable)")
    return overall, payload


def phase_pair_manifest_multi_remote(*, band_id, islands=5, x=7, n=8, write=True, exp_dir=None):
    """Slice 4b: build one TRACKABLE multi-remote manifest per matched island from the raw mrband
    island files. Returns (overall, [statuses])."""
    statuses = []
    for idx in range(1, islands + 1):
        hpx = _load_band_island(band_id, idx, "hpx", exp_dir)
        ray = _load_band_island(band_id, idx, "ray", exp_dir)
        if hpx is None or ray is None:
            print(f"pair-manifest-multi-remote[i{idx}]: FAIL -- missing island file "
                  f"(hpx={hpx is not None} ray={ray is not None})")
            statuses.append("fail")
            continue
        man = build_pair_manifest_multi_remote(manifest_id=f"{band_id}-i{idx}", x=x, n=n,
                                               ray_artifact=ray, hpx_artifact=hpx)
        statuses.append(man["overall"])
        failed = [g for g, v in man["gates"].items() if not v]
        print(f"pair-manifest-multi-remote[i{idx}]: overall={man['overall']}"
              + ("" if man["overall"] == "pass" else f" failed_gates={failed}"))
        if write:
            _write_trackable(man, f"exp62_fanout_{band_id}_i{idx}_manifest.json", exp_dir)
    overall = "pass" if statuses and all(s == "pass" for s in statuses) else "fail"
    return overall, statuses


def phase_band_aggregate_multi_remote(*, band_id, job_id, islands=5, write=True, exp_dir=None):
    """Slice 4b: build the ONE TRACKABLE combined multi-remote band aggregate from the raw mrband
    island files."""
    hpx_islands, ray_islands = [], []
    for idx in range(1, islands + 1):
        h = _load_band_island(band_id, idx, "hpx", exp_dir)
        r = _load_band_island(band_id, idx, "ray", exp_dir)
        if h is not None:
            hpx_islands.append(h)
        if r is not None:
            ray_islands.append(r)
    payload, overall = build_fanout_band_aggregate_multi_remote(
        hpx_islands=hpx_islands, ray_islands=ray_islands, job_id=job_id, band_id=band_id)
    failed = [g for g, v in payload["gates"].items() if not v]
    print(f"band-aggregate-multi-remote: overall={overall} same_axis_comparison="
          f"{payload['same_axis_comparison']}" + ("" if overall == "pass" else f" failed={failed}"))
    if write:
        name = f"exp62_fanout_mrband_{job_id}_aggregate.json"
        print(f"band-aggregate-multi-remote: wrote "
              f"{os.path.basename(_write_trackable(payload, name, exp_dir))} (trackable)")
    return overall, payload


# ---------------------------------------------------------------------------
# Off-cluster clean skip.
# ---------------------------------------------------------------------------

def cluster_available(env=None):
    env = os.environ if env is None else env
    return bool(env.get("SLURM_JOB_ID")) and bool(env.get("SLURM_JOB_NODELIST"))


def skip_reason(env=None):
    if cluster_available(env):
        return None
    return "no SLURM_JOB_ID/SLURM_JOB_NODELIST: off-cluster; exp62 distributed phases skip cleanly"


# ---------------------------------------------------------------------------
# CLI: Slice 0 exposes only --phase {selftest, smoke}. No runtime is wired yet.
# ---------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(
        description="exp62 distributed fanout/fanin (Slice 3: matched cross-node R>=5 band)")
    ap.add_argument("--phase",
                    choices=("selftest", "smoke", "hpx-local-smoke", "ray-local-smoke",
                             "local-smoke-all", "hpx-remote-smoke", "hpx-multi-remote-smoke",
                             "hpx-fanout", "ray-fanout",
                             "pair-manifest-fanout", "band-aggregate-fanout",
                             "hpx-multi-remote-fanout", "ray-multi-remote-fanout",
                             "pair-manifest-multi-remote", "band-aggregate-multi-remote"),
                    default="selftest",
                    help="Slice 0-2 phases; Slice 3: hpx-fanout (one HPX band island), ray-fanout "
                         "(all R Ray band islands), pair-manifest-fanout (trackable manifests), "
                         "band-aggregate-fanout (trackable combined aggregate); Slice 4a: "
                         "hpx-multi-remote-smoke (HPX-only >=2-remote-locality dry-run); Slice 4b: "
                         "hpx-multi-remote-fanout / ray-multi-remote-fanout (>=2-remote band islands, "
                         "proven poll composition), pair-manifest-multi-remote, "
                         "band-aggregate-multi-remote.")
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--n", type=int, default=8, help="inner fanout N")
    ap.add_argument("--k", type=int, default=20, help="timed calls (dry-run small K)")
    ap.add_argument("--w", type=int, default=5, help="prewarm count")
    ap.add_argument("--hpx-threads", type=int, default=4)
    ap.add_argument("--connector-threads", type=int, default=4)
    ap.add_argument("--prefer-subnet", default="10.42.5.")
    ap.add_argument("--root-port", type=int, default=7910, help="AGAS root port")
    ap.add_argument("--dispatch-timeout-s", type=float, default=30.0)
    # Slice 3 band knobs
    ap.add_argument("--band-id", default=None, help="band id shared by matched hpx/ray islands")
    ap.add_argument("--island-index", type=int, default=1, help="island index for hpx-fanout")
    ap.add_argument("--islands", type=int, default=5, help="island count R for ray/manifest/aggregate")
    ap.add_argument("--band-k", type=int, default=1000, help="band timed calls K")
    ap.add_argument("--band-w", type=int, default=100, help="band W (config label)")
    ap.add_argument("--prewarm", type=int, default=1, help="band warmup calls before timing")
    ap.add_argument("--cpu-bind-mask", default="0xFF", help="connector cpu-bind mask_cpu value")
    # Slice 4a multi-remote knobs
    ap.add_argument("--n-remote", type=int, default=2, help="number of remote localities/nodes (>=2)")
    ap.add_argument("--cpu-bind-masks", default=None,
                    help="comma-separated per-remote-node cpu-bind mask_cpu values (aligned to the "
                         "remote nodes); omit to let Slurm bind the allocated CPUs (gated on the "
                         "attested effective cpuset either way)")
    ap.add_argument("--worker-cpus", type=int, default=8, help="ray worker --num-cpus on node B")
    ap.add_argument("--ray-dispatch-timeout-s", type=float, default=30.0,
                    help="bounded ray.get timeout for the coordinator (fail closed, no hang)")
    ap.add_argument("--job-id", default=None, help="job id for the aggregate filename")
    # Pre-Slice-4b HPX composition spike. None => each HPX phase uses its own default (local:
    # when_all_get, remote: root_flat_gather_poll). Native modes (when_all_then_reduce /
    # dataflow_reduce) use future continuations with no success-path is_ready poll.
    ap.add_argument("--composition-mode", default=None,
                    choices=("when_all_get", "when_all_then_reduce", "dataflow_reduce",
                             "root_flat_gather_poll"),
                    help="HPX fan-in composition mode for the HPX phases (spike).")
    args = ap.parse_args(argv)

    if args.phase == "selftest":
        import selftest_slice0
        return selftest_slice0.main()

    if args.phase == "smoke":
        reason = skip_reason()
        if reason:
            print(f"exp62 smoke: SKIP -- {reason}")
        else:
            print("exp62 smoke: cluster context present; use --phase hpx-remote-smoke for the "
                  "Slice 2 one-remote dry-run, or the Slice 3 *-fanout phases for the band.")
        return 0

    # Spike: None => each phase's native default (local: when_all_get, remote: root_flat_gather_poll).
    local_mode = args.composition_mode or "when_all_get"
    remote_mode = args.composition_mode or "root_flat_gather_poll"

    if args.phase == "hpx-local-smoke":
        status, _ = run_hpx_local_smoke(x=args.x, n=args.n, composition_mode=local_mode)
        return 1 if status == "fail" else 0

    if args.phase == "ray-local-smoke":
        status, _ = run_ray_local_smoke(x=args.x, n=args.n)
        return 1 if status == "fail" else 0

    if args.phase == "local-smoke-all":
        h_status, _ = run_hpx_local_smoke(x=args.x, n=args.n, composition_mode=local_mode)
        r_status, _ = run_ray_local_smoke(x=args.x, n=args.n)
        print(f"local-smoke-all: hpx={h_status} ray={r_status}")
        return 1 if "fail" in (h_status, r_status) else 0

    if args.phase == "hpx-remote-smoke":
        status, _ = run_hpx_remote_smoke(
            x=args.x, n=args.n, k=args.k, prewarm=args.w, hpx_threads=args.hpx_threads,
            connector_threads=args.connector_threads, prefer_subnet=args.prefer_subnet,
            root_port=args.root_port, dispatch_timeout_s=args.dispatch_timeout_s,
            composition_mode=remote_mode)
        return 1 if status == "fail" else 0

    if args.phase == "hpx-multi-remote-smoke":
        masks = ([m.strip() for m in args.cpu_bind_masks.split(",") if m.strip()]
                 if args.cpu_bind_masks else None)
        # connector_threads defaults to 8 for the multi-remote dry-run (resource-shape-clean cpuset),
        # unless the caller overrode --connector-threads away from its 4 default.
        cthreads = args.connector_threads if args.connector_threads != 4 else 8
        status, _ = run_hpx_multi_remote_smoke(
            x=args.x, n=args.n, k=args.k, prewarm=args.w, hpx_threads=args.hpx_threads,
            connector_threads=cthreads, connector_min_threads=cthreads, n_remote=args.n_remote,
            prefer_subnet=args.prefer_subnet, root_port=args.root_port, cpu_bind_masks=masks,
            dispatch_timeout_s=args.dispatch_timeout_s, composition_mode=remote_mode)
        return 1 if status == "fail" else 0

    band_id = args.band_id or f"band-{_slurm_info().get('slurm_job_id') or 'local'}"
    job_id = args.job_id or _slurm_info().get("slurm_job_id") or "local"

    if args.phase == "hpx-fanout":
        status, _ = run_hpx_fanout(
            band_id=band_id, island_index=args.island_index, x=args.x, n=args.n, k=args.band_k,
            w=args.band_w, prewarm=args.prewarm, hpx_threads=args.hpx_threads,
            connector_threads=args.connector_threads if args.connector_threads != 4 else 8,
            prefer_subnet=args.prefer_subnet, root_port=args.root_port,
            cpu_bind_mask=args.cpu_bind_mask, dispatch_timeout_s=args.dispatch_timeout_s,
            composition_mode=remote_mode)
        return 1 if status == "fail" else 0

    if args.phase == "ray-fanout":
        status, _ = run_ray_fanout(
            band_id=band_id, islands=args.islands, x=args.x, n=args.n, k=args.band_k,
            w=args.band_w, prewarm=args.prewarm, prefer_subnet=args.prefer_subnet,
            worker_cpus=args.worker_cpus, ray_dispatch_timeout_s=args.ray_dispatch_timeout_s)
        return 1 if status == "fail" else 0

    if args.phase == "pair-manifest-fanout":
        overall, _ = phase_pair_manifest_fanout(band_id=band_id, islands=args.islands, x=args.x,
                                                n=args.n)
        print(f"pair-manifest-fanout: overall={overall}")
        return 1 if overall == "fail" else 0

    if args.phase == "band-aggregate-fanout":
        overall, _ = phase_band_aggregate_fanout(band_id=band_id, job_id=job_id, islands=args.islands)
        print(f"band-aggregate-fanout: overall={overall}")
        return 1 if overall == "fail" else 0

    # Slice 4b multi-remote band (default band id prefix mrband-<job>).
    mr_band_id = args.band_id or f"mrband-{_slurm_info().get('slurm_job_id') or 'local'}"

    if args.phase == "hpx-multi-remote-fanout":
        status, _ = run_hpx_multi_remote_fanout(
            band_id=mr_band_id, island_index=args.island_index, x=args.x, n=args.n, k=args.band_k,
            w=args.band_w, prewarm=args.prewarm, hpx_threads=args.hpx_threads,
            connector_threads=args.connector_threads if args.connector_threads != 4 else 8,
            n_remote=args.n_remote, prefer_subnet=args.prefer_subnet, root_port=args.root_port,
            dispatch_timeout_s=args.dispatch_timeout_s, composition_mode=remote_mode)
        return 1 if status == "fail" else 0

    if args.phase == "ray-multi-remote-fanout":
        status, _ = run_ray_multi_remote_fanout(
            band_id=mr_band_id, islands=args.islands, x=args.x, n=args.n, k=args.band_k,
            w=args.band_w, prewarm=args.prewarm, n_remote=args.n_remote,
            prefer_subnet=args.prefer_subnet, worker_cpus=args.worker_cpus,
            ray_dispatch_timeout_s=args.ray_dispatch_timeout_s)
        return 1 if status == "fail" else 0

    if args.phase == "pair-manifest-multi-remote":
        overall, _ = phase_pair_manifest_multi_remote(band_id=mr_band_id, islands=args.islands,
                                                      x=args.x, n=args.n)
        print(f"pair-manifest-multi-remote: overall={overall}")
        return 1 if overall == "fail" else 0

    if args.phase == "band-aggregate-multi-remote":
        overall, _ = phase_band_aggregate_multi_remote(band_id=mr_band_id, job_id=job_id,
                                                       islands=args.islands)
        print(f"band-aggregate-multi-remote: overall={overall}")
        return 1 if overall == "fail" else 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
