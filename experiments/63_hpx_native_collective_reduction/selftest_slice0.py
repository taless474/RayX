"""exp63 Slice 0 selftests -- hermetic, pure Python. No Ray, no HPX, no Rostam, no external deps.

Run:  python3 selftest_slice0.py
Exits non-zero on any failed check.
"""

import os
import sys

import run_exp63_collective as r


_FAILS = []


def chk(name, cond):
    print(f"  [{'ok  ' if cond else 'FAIL'}] {name}")
    if not cond:
        _FAILS.append(name)


def _raises(fn):
    try:
        fn()
        return False
    except Exception:
        return True


# Forbidden claim fields: no artifact may imply object store, arbitrary remote Python, Ray-replacement,
# real inference, production/fabric semantics, or a Ray-vs-HPX ratio/speedup.
_FORBIDDEN_KEYS = {
    "object_store", "objectref", "object_ref", "remote_python", "arbitrary_python",
    "ray_replacement", "inference", "model_inference", "production", "fault_tolerance",
    "general_fabric", "speedup", "ratio", "ray_vs_hpx",
}


def _keys_deep(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k)
            yield from _keys_deep(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            yield from _keys_deep(v)


# --------------------------------------------------------------------------- scalar oracle

def test_scalar_oracle():
    print("scalar oracle correctness:")
    chk("leaf depends only on x,i", r.leaf_value(7, 3) == r.i64((7 ^ r.LEAF_XOR) + (3 << 1)))
    chk("deterministic", r.composite_oracle(12345, 6) == r.composite_oracle(12345, 6))
    fwd = r.composite_oracle(12345, 6)
    rev = r.i64(sum(((12345 ^ r.LEAF_XOR) + (i << 1)) for i in reversed(range(6))))
    chk("order-independent", fwd == rev)
    chk("empty fanout = 0", r.composite_oracle(7, 0) == 0)
    chk("int64 wrap closed", -(1 << 63) <= r.composite_oracle(2 ** 62, 1000) < (1 << 63))
    chk("negative n raises", _raises(lambda: r.composite_oracle(7, -1)))


def test_composite_oracle_n8():
    print("composite oracle correctness for N=8:")
    expected = r.i64(sum((7 ^ r.LEAF_XOR) + (i << 1) for i in range(8)))
    chk("N=8 matches hand sum", r.composite_oracle(7, 8) == expected)
    chk("N=8 == sum of leaf_value", r.composite_oracle(7, 8)
        == r.i64(sum(r.leaf_value(7, i) for i in range(8))))
    chk("oracle_gate_scalar true on match", r.oracle_gate_scalar(r.composite_oracle(7, 8), 7, 8))
    chk("oracle_gate_scalar false on mismatch",
        r.oracle_gate_scalar(r.composite_oracle(7, 8) + 1, 7, 8) is False)


# --------------------------------------------------------------------------- vector oracle stub

def test_vector_oracle_stub():
    print("vector-sum oracle stub deterministic + shape-checked:")
    v = r.vector_sum_oracle(7, 8, 4)
    chk("shape == dim", len(v) == 4)
    chk("deterministic", r.vector_sum_oracle(7, 8, 4) == r.vector_sum_oracle(7, 8, 4))
    hand = [r.i64(sum(r.leaf_vector(7, i, 4)[d] for i in range(8))) for d in range(4)]
    chk("elementwise sum matches", v == hand)
    chk("dim 0 -> empty", r.vector_sum_oracle(7, 8, 0) == [])
    chk("n 0 -> zeros", r.vector_sum_oracle(7, 0, 3) == [0, 0, 0])
    chk("all int64", all(-(1 << 63) <= e < (1 << 63) for e in r.vector_sum_oracle(2 ** 62, 500, 4)))
    chk("oracle_gate_vector true", r.oracle_gate_vector(v, 7, 8, 4))
    chk("oracle_gate_vector false on wrong shape", r.oracle_gate_vector(v[:3], 7, 8, 4) is False)
    chk("negative dim raises", _raises(lambda: r.vector_sum_oracle(7, 8, -1)))


# --------------------------------------------------------------------------- top-k oracle stub

def test_topk_oracle_stub():
    print("top-k oracle stub deterministic + shape-checked:")
    tk = r.topk_oracle(7, 8, 3)
    chk("shape == min(k,n)", len(tk) == 3)
    chk("deterministic", r.topk_oracle(7, 8, 3) == r.topk_oracle(7, 8, 3))
    chk("k > n clamps to n", len(r.topk_oracle(7, 5, 100)) == 5)
    chk("k 0 -> empty", r.topk_oracle(7, 8, 0) == [])
    scores = [s for (_, s) in tk]
    chk("sorted by score desc", scores == sorted(scores, reverse=True))
    chk("entries are (index, score)", all(isinstance(i, int) and isinstance(s, int)
                                          for (i, s) in tk))
    chk("indices are real leaf indices", all(0 <= i < 8 for (i, _) in tk))
    chk("oracle_gate_topk true", r.oracle_gate_topk(tk, 7, 8, 3))
    chk("oracle_gate_topk false on truncation", r.oracle_gate_topk(tk[:2], 7, 8, 3) is False)
    chk("negative k raises", _raises(lambda: r.topk_oracle(7, 8, -1)))


# --------------------------------------------------------------------------- composition provenance

def test_composition_provenance_per_mode():
    print("composition provenance consistency per candidate primitive:")
    poll = r.composition_provenance_for("root_flat_gather_poll",
                                        watchdog="bounded_is_ready_poll_50us")
    chk("poll consistent", poll["composition_provenance_consistent"] is True)
    chk("poll polled True", poll["polled_in_success_path"] is True)
    chk("poll native False", poll["hpx_native_composition"] is False)
    chk("poll cross-node validated True (proven control)",
        poll["cross_node_composition_validated"] is True)
    chk("poll not experimental", poll["native_mode_experimental"] is False)

    for mode in ("when_all_then_reduce", "dataflow_reduce", "tree_of_partials",
                 "hpx_collective_reduce"):
        p = r.composition_provenance_for(mode, watchdog="composed_future_wait_for")
        chk(f"{mode} consistent", p["composition_provenance_consistent"] is True)
        chk(f"{mode} native True", p["hpx_native_composition"] is True)
        chk(f"{mode} polled False", p["polled_in_success_path"] is False)
        chk(f"{mode} cross-node validated False (until proven)",
            p["cross_node_composition_validated"] is False)
        chk(f"{mode} flagged experimental", p["native_mode_experimental"] is True)
        chk(f"{mode} points at proven fallback",
            p["proven_cross_node_composition_mode"] == "root_flat_gather_poll")

    # dataflow_reduce registry classification (exp63 Slice-1b fix): it must resolve to the native,
    # non-polled shape with the composed_future_wait_for watchdog -- NOT the poll control.
    dfr = r.composition_provenance_for("dataflow_reduce", watchdog=r._watchdog_for("dataflow_reduce"))
    chk("dataflow_reduce in COMPOSITION_PRIMITIVES", "dataflow_reduce" in r.COMPOSITION_PRIMITIVES)
    chk("dataflow_reduce watchdog composed_future_wait_for",
        r._watchdog_for("dataflow_reduce") == "composed_future_wait_for"
        and dfr["watchdog"] == "composed_future_wait_for")
    chk("dataflow_reduce is native mode", r._is_native_mode("dataflow_reduce") is True)
    chk("dataflow_reduce not the proven control",
        dfr["cross_node_composition_validated"] is False
        and dfr["proven_cross_node_composition_mode"] == "root_flat_gather_poll")


def test_composition_provenance_fail_closed():
    print("composition provenance fails closed on malformed / dishonest input:")
    # native must NOT poll in the success path.
    bad1 = r.composition_provenance(
        composition_primitive="when_all_then_reduce", watchdog="composed_future_wait_for",
        ran_on_hpx_thread=True, polled_in_success_path=True, hpx_native_composition=True)
    chk("native+polled inconsistent", bad1["composition_provenance_consistent"] is False)
    # composition must run on an HPX thread.
    bad2 = r.composition_provenance(
        composition_primitive="tree_of_partials", watchdog="child_future_join",
        ran_on_hpx_thread=False, polled_in_success_path=False, hpx_native_composition=True)
    chk("not-hpx-thread inconsistent", bad2["composition_provenance_consistent"] is False)
    # empty watchdog label.
    bad3 = r.composition_provenance(
        composition_primitive="tree_of_partials", watchdog="",
        ran_on_hpx_thread=True, polled_in_success_path=False, hpx_native_composition=True)
    chk("empty watchdog inconsistent", bad3["composition_provenance_consistent"] is False)
    # unknown primitive.
    bad4 = r.composition_provenance(
        composition_primitive="magic_reduce", watchdog="w",
        ran_on_hpx_thread=True, polled_in_success_path=False, hpx_native_composition=True)
    chk("unknown primitive inconsistent", bad4["composition_provenance_consistent"] is False)
    # only the proven control may claim cross-node validated.
    bad5 = r.composition_provenance(
        composition_primitive="hpx_collective_reduce", watchdog="w",
        ran_on_hpx_thread=True, polled_in_success_path=False, hpx_native_composition=True,
        cross_node_composition_validated=True)
    chk("native claiming validated inconsistent", bad5["composition_provenance_consistent"] is False)


def test_collective_provenance():
    print("collective provenance (communicator/generation/all-contributed):")
    good = r.collective_provenance(communicator="exp63_reduce", generation=0,
                                   all_participants_contributed=True, watchdog="collective_wait")
    chk("good collective consistent", good["composition_provenance_consistent"] is True)
    chk("communicator recorded", good["collective_communicator"] == "exp63_reduce")
    chk("generation recorded", good["collective_generation"] == 0)
    chk("all_participants_contributed True", good["all_participants_contributed"] is True)
    chk("native True", good["hpx_native_composition"] is True)
    chk("cross-node validated False (until proven)",
        good["cross_node_composition_validated"] is False)
    miss = r.collective_provenance(communicator="exp63_reduce", generation=0,
                                   all_participants_contributed=False, watchdog="collective_wait")
    chk("missing participant fails closed", miss["composition_provenance_consistent"] is False)
    bad = r.collective_provenance(communicator="", generation=0,
                                  all_participants_contributed=True, watchdog="collective_wait")
    chk("empty communicator fails closed", bad["composition_provenance_consistent"] is False)


# --------------------------------------------------------------------------- progress diagnosis

def test_progress_diagnosis():
    print("progress-diagnosis provenance (Slice 1 root-cause):")
    # baseline reproduces the passive stall: dispatch elapsed == timeout, not woken.
    stall = r.progress_diagnosis(mode="passive_wait_baseline", no_dispatch_timeout=False,
                                 woke_without_success_poll=False, dispatch_elapsed_s=30.0,
                                 dispatch_timeout_s=30.0)
    chk("baseline stall not ok", stall["passive_progress_ok"] is False)
    # a fixed config: passive wait woke fast without a success-path poll.
    fixed = r.progress_diagnosis(mode="increase_hpx_threads", no_dispatch_timeout=True,
                                 woke_without_success_poll=True, dispatch_elapsed_s=0.002,
                                 dispatch_timeout_s=30.0)
    chk("fixed passive progress ok", fixed["passive_progress_ok"] is True)
    # the poll control proves correctness but is NOT passive progress (woke via poll).
    control = r.progress_diagnosis(mode="yield_poll_control", no_dispatch_timeout=True,
                                   woke_without_success_poll=False, dispatch_elapsed_s=0.003,
                                   dispatch_timeout_s=30.0)
    chk("poll control not counted as passive progress", control["passive_progress_ok"] is False)
    # unknown mode fails closed.
    unk = r.progress_diagnosis(mode="bogus", no_dispatch_timeout=True,
                               woke_without_success_poll=True, dispatch_elapsed_s=0.001,
                               dispatch_timeout_s=30.0)
    chk("unknown mode not ok", unk["passive_progress_ok"] is False)
    # elapsed at/over budget fails closed even if flags claim ok.
    over = r.progress_diagnosis(mode="increase_hpx_threads", no_dispatch_timeout=True,
                                woke_without_success_poll=True, dispatch_elapsed_s=30.0,
                                dispatch_timeout_s=30.0)
    chk("elapsed >= timeout not ok", over["passive_progress_ok"] is False)


# --------------------------------------------------------------------------- payload provenance

def test_payload_provenance_flags():
    print("payload provenance flags always synthetic / not-model / no-inference:")
    scal = r.payload_provenance(payload_mode="scalar_int64")
    chk("scalar shape", scal["payload_shape"] == "scalar" and scal["payload_len"] == 1)
    chk("scalar bytes 8", scal["payload_bytes"] == 8)
    vec = r.payload_provenance(payload_mode="fixed_vector_stub", vector_dim=8)
    chk("vector shape/len", vec["payload_shape"] == "vector" and vec["payload_len"] == 8)
    chk("vector bytes 8*dim", vec["payload_bytes"] == 64)
    tk = r.payload_provenance(payload_mode="topk_stub", topk=5)
    chk("topk shape/len", tk["payload_shape"] == "topk" and tk["payload_len"] == 5)
    chk("topk bytes 16*k", tk["payload_bytes"] == 80)
    for label, p in (("scalar", scal), ("vector", vec), ("topk", tk)):
        chk(f"{label} synthetic", p["payload_is_synthetic"] is True)
        chk(f"{label} not model output", p["payload_not_model_output"] is True)
        chk(f"{label} no inference", p["no_inference"] is True)
    chk("unknown payload raises", _raises(lambda: r.payload_provenance(payload_mode="bogus")))
    chk("vector needs dim>=1", _raises(
        lambda: r.payload_provenance(payload_mode="fixed_vector_stub", vector_dim=0)))
    chk("topk needs k>=1", _raises(
        lambda: r.payload_provenance(payload_mode="topk_stub", topk=0)))


# --------------------------------------------------------------------------- distribution witness

def _partials(counts, root=0):
    """counts: dict locality -> n_leaves. Build one partial record per locality."""
    return [{"locality": loc, "n_leaves": nl} for loc, nl in counts.items()]


def test_distribution_witness():
    print("distribution witness + fail-closed gate:")
    w = r.contribution_witness(_partials({1: 4, 2: 4}), root_locality=0)
    chk("n_remote_localities 2", w["n_remote_localities"] == 2)
    chk("leaves_remote 8", w["leaves_remote"] == 8)
    chk("no leaves on root", 0 not in w["leaves_per_locality"])
    g = r.distribution_gate(witness=w, declared_remote_localities=[1, 2], expected_remote_leaves=8)
    chk("all distribution gates true", all(g.values()))
    # one remote fails ge_2 + coverage closed.
    w1 = r.contribution_witness(_partials({1: 8}), root_locality=0)
    g1 = r.distribution_gate(witness=w1, declared_remote_localities=[1, 2], expected_remote_leaves=8)
    chk("one remote ge_2 False", g1["n_remote_localities_ge_2"] is False)
    chk("declared not covered False", g1["all_declared_remotes_covered"] is False)
    chk("participant missing False", g1["all_participants_contributed"] is False)
    # empty declared set fails closed.
    g2 = r.distribution_gate(witness=w, declared_remote_localities=[], expected_remote_leaves=8)
    chk("empty declared covers False", g2["all_declared_remotes_covered"] is False)
    chk("empty declared contributed False", g2["all_participants_contributed"] is False)
    # wrong expected count fails.
    g3 = r.distribution_gate(witness=w, declared_remote_localities=[1, 2], expected_remote_leaves=9)
    chk("wrong expected leaves False", g3["leaves_cover_expected"] is False)


# --------------------------------------------------------------------------- fences + scaffold record

def _scaffold():
    comp = r.composition_provenance_for("tree_of_partials", watchdog="child_future_join")
    pay = r.payload_provenance(payload_mode="scalar_int64")
    prog = r.progress_diagnosis(mode="increase_hpx_threads", no_dispatch_timeout=True,
                                woke_without_success_poll=True, dispatch_elapsed_s=0.002,
                                dispatch_timeout_s=30.0)
    w = r.contribution_witness(_partials({1: 4, 2: 4}), root_locality=0)
    dist = r.distribution_gate(witness=w, declared_remote_localities=[1, 2], expected_remote_leaves=8)
    return r.build_scaffold_record(composition=comp, payload=pay, progress=prog, distribution=dist)


def test_fences_locked():
    print("hard fences locked False everywhere:")
    rec = _scaffold()
    for k in ("speedup_computed", "ratio_reported", "arms_differenced",
              "placement_bands_differenced"):
        chk(f"{k} False", rec["fences"][k] is False)
    chk("_fences_locked recognizes the block", r._fences_locked(rec["fences"]) is True)
    chk("fence_block is all-False", all(v is False for v in r.fence_block().values()))


def test_no_same_axis_in_slice0():
    print("no same_axis_comparison True in Slice 0:")
    rec = _scaffold()
    chk("scaffold same_axis False", rec["same_axis_comparison"] is False)
    chk("slice tag 0", rec["slice"] == 0)


def test_no_forbidden_claim_fields():
    print("no forbidden claim fields anywhere in the scaffold record:")
    rec = _scaffold()
    leaked = {k.lower() for k in _keys_deep(rec)} & _FORBIDDEN_KEYS
    chk("no forbidden keys", not leaked)


# --------------------------------------------------------------------------- off-cluster skip

def test_off_cluster_skip():
    print("off-cluster skip:")
    chk("no slurm -> not available", r.cluster_available(env={}) is False)
    chk("no slurm -> skip reason", r.skip_reason(env={}) is not None)
    on = {"SLURM_JOB_ID": "158900", "SLURM_JOB_NODELIST": "medusa[00-02]"}
    chk("slurm present -> available", r.cluster_available(env=on) is True)
    chk("slurm present -> no skip", r.skip_reason(env=on) is None)


def test_future_phases_skip_cleanly():
    print("future runtime phases skip cleanly (off-cluster or ext-unbuilt):")
    ps, _ = r.run_progress_diagnosis(env={}, write=False)
    chk("progress-diagnosis skip off-cluster", ps == "skip")
    # local collective smoke needs no cluster, but the ext is absent in Slice 0 -> skip.
    ls, la = r.run_hpx_collective_local_smoke(write=False)
    chk("collective-local-smoke skip (ext unbuilt)", ls == "skip" and la is None)
    rs, _ = r.run_hpx_collective_remote_smoke(env={}, write=False)
    chk("collective-remote-smoke skip off-cluster", rs == "skip")
    ts, _ = r.run_tree_of_partials_remote_smoke(env={}, write=False)
    chk("tree-of-partials-remote-smoke skip off-cluster", ts == "skip")
    # even with a cluster present, a forced ext ImportError skips (no C++ in Slice 0).
    on = {"SLURM_JOB_ID": "158900", "SLURM_JOB_NODELIST": "medusa[00-02]"}

    def _boom():
        raise ImportError("forced: exp63 ext absent")
    cs, ca = r.run_hpx_collective_remote_smoke(env=on, import_fn=_boom, write=False)
    chk("collective-remote-smoke skip on-cluster ext-unbuilt", cs == "skip" and ca is None)


# --------------------------------------------------------------------------- Slice 1a: progress sweep

def _good_partials():
    return [{"locality": 1, "n_leaves": 4}, {"locality": 2, "n_leaves": 4}]


def _healthy_connectors():
    return [{"joined": True, "served": True, "graceful_disconnect": True},
            {"joined": True, "served": True, "graceful_disconnect": True}]


def _progress_art(config, *, woke=None, no_timeout=True, timed_out=0, measured="oracle",
                  elapsed=0.002, dispatch_timeout_s=8.0):
    """Synthesize one per-config artifact. Defaults describe a HEALTHY run: native modes wake without
    poll; the poll control does not (woke=False). Override to build a stalled config."""
    x, n = 7, 8
    native = r._is_native_mode(config["composition_primitive"])
    if woke is None:
        woke = native
    mv = r.composite_oracle(x, n) if measured == "oracle" else measured
    return r.build_progress_config_artifact(
        config=config, x=x, n=n, k=20, w=5, prewarm=5, dispatch_timeout_s=dispatch_timeout_s,
        measured_value=mv, dispatch_elapsed_s=elapsed, no_dispatch_timeout=no_timeout,
        passive_wait_woke_without_poll=woke, timed_out_leaf_count=timed_out,
        partials=_good_partials(), root_locality=0, declared_remote_localities=[1, 2],
        connectors=_healthy_connectors(), root_cpuset=[0, 1, 2, 3],
        connector_cpusets=[[0, 2, 4, 6], [1, 3, 5, 7]], root_hostname="medusa00",
        connector_hostnames=["medusa01", "medusa02"], remote_locality_ids=[1, 2],
        selected_subnet="10.42.5.", slurm_job_id="J1", tcp_nodelay_verified=True)


def _stalled_art(config):
    """A native stall: passive wait never woke, hit the dispatch timeout, leaves timed out, no value."""
    return _progress_art(config, woke=False, no_timeout=False, timed_out=8, measured=None,
                         elapsed=8.0, dispatch_timeout_s=8.0)


def test_progress_matrix_phase_a():
    print("progress config matrix Phase A (A0..A5, no B):")
    m = r.progress_config_matrix()
    chk("A0..A5 exactly", [c["id"] for c in m] == ["A0", "A1", "A2", "A3", "A4", "A5"])
    chk("no phase B by default", all(c["phase"] == "A" for c in m))
    chk("A0 poll control", m[0]["composition_primitive"] == "root_flat_gather_poll"
        and m[0]["role"] == "known_good_poll_control")
    chk("A1 repro 158814", m[1]["role"] == "reproduce_158814_stall"
        and m[1]["composition_primitive"] == "when_all_then_reduce")
    chk("A2 more root threads", m[2]["root_threads"] == 8 and m[2]["background_progress"] == "baseline")
    chk("A3 background yielder", m[3]["background_progress"] == "background_yielder")
    chk("A5 dataflow probe", m[5]["composition_primitive"] == "dataflow_reduce")
    chk("all tcp, connector_threads 8", all(c["transport"] == "tcp" and c["connector_threads"] == 8
                                            for c in m))


def test_progress_matrix_phase_b():
    print("progress config matrix Phase B only with include_phase_b=True:")
    m = r.progress_config_matrix(include_phase_b=True)
    chk("appends B0 B1", [c["id"] for c in m][-2:] == ["B0", "B1"])
    chk("B configs deferred", all(c["deferred"] for c in m if c["phase"] == "B"))
    chk("phase A not deferred", all(not c["deferred"] for c in m if c["phase"] == "A"))
    b1 = [c for c in m if c["id"] == "B1"][0]
    chk("B1 mpi/ib transport", b1["transport"] == "mpi")


def test_poll_control_not_passive_progress():
    print("A0 poll control passes correctness/distribution but NOT passive-progress:")
    a0 = _progress_art(r.progress_config_matrix()[0])
    chk("correctness/distribution ok", a0["correctness_distribution_ok"] is True)
    chk("outcome control_pass", a0["outcome"] == "control_pass")
    chk("overall pass", a0["overall"] == "pass")
    chk("NOT passive-progress capable", a0["passive_progress_capable"] is False)
    chk("polled True", a0["provenance"]["polled_in_success_path"] is True)
    chk("native False", a0["provenance"]["hpx_native_composition"] is False)


def test_native_woke_passes_passive_progress():
    print("synthetic native woke-without-poll config passes passive-progress:")
    a3 = _progress_art(r.progress_config_matrix()[3])
    chk("passive-progress capable", a3["passive_progress_capable"] is True)
    chk("outcome native_progress_pass", a3["outcome"] == "native_progress_pass")
    chk("overall pass", a3["overall"] == "pass")
    chk("native True", a3["provenance"]["hpx_native_composition"] is True)
    chk("woke recorded True", a3["progress"]["passive_wait_woke_without_poll"] is True)
    chk("cross-node validated stays False (needs curated promotion)",
        a3["provenance"]["cross_node_composition_validated"] is False)


def test_stalled_native_fails_closed():
    print("stalled native config fails closed:")
    a1 = _stalled_art(r.progress_config_matrix()[1])
    chk("NOT passive-progress capable", a1["passive_progress_capable"] is False)
    chk("outcome native_stalled", a1["outcome"] == "native_stalled")
    chk("overall stalled (not pass)", a1["overall"] == "stalled")
    chk("no_dispatch_timeout False", a1["progress"]["no_dispatch_timeout"] is False)
    chk("timed_out_leaf_count 8", a1["progress"]["timed_out_leaf_count"] == 8)
    chk("oracle correct False (no value returned)",
        a1["correctness"]["composite_oracle_correct"] is False)


def test_stall_short_circuit():
    print("stall short-circuit advances native stalls, spares the control:")
    sc = r.stall_short_circuit(composition_primitive="when_all_then_reduce", first_call_timed_out=True,
                               no_dispatch_timeout=False, passive_wait_woke_without_poll=False, k=20)
    chk("native stall detected", sc["stalled"] is True)
    chk("advance to next config", sc["advance_to_next_config"] is True)
    chk("calls_spent 1", sc["calls_spent"] == 1)
    chk("budget saved 19", sc["budget_saved_calls"] == 19)
    ok = r.stall_short_circuit(composition_primitive="when_all_then_reduce", first_call_timed_out=False,
                               no_dispatch_timeout=True, passive_wait_woke_without_poll=True, k=20)
    chk("healthy native runs full K", ok["stalled"] is False and ok["calls_spent"] == 20)
    ctrl = r.stall_short_circuit(composition_primitive="root_flat_gather_poll",
                                 first_call_timed_out=True, no_dispatch_timeout=False,
                                 passive_wait_woke_without_poll=False, k=20)
    chk("poll control never short-circuits", ctrl["stalled"] is False and ctrl["calls_spent"] == 20)


def test_aggregate_picks_winner():
    print("aggregate picks the winning native passive-progress config:")
    m = r.progress_config_matrix()
    arts = [_progress_art(m[0]),          # A0 control pass
            _stalled_art(m[1]), _stalled_art(m[2]),
            _progress_art(m[3]),          # A3 wins
            _stalled_art(m[4]), _stalled_art(m[5])]
    agg = r.build_progress_sweep_aggregate(arts)
    chk("winner A3", agg["winning_passive_progress_config"] == "A3")
    chk("passive capable [A3]", agg["passive_progress_capable_configs"] == ["A3"])
    chk("poll control pass", agg["poll_control_status"] == "pass")
    chk("phase_b_needed False (winner exists)", agg["phase_b_needed"] is False)
    chk("tree_of_partials_recommended False", agg["tree_of_partials_recommended"] is False)
    chk("collectives deferred", agg["collectives_deferred"] is True)
    chk("next: retest native", agg["next_direction"] == "retest_when_all_then_reduce_at_band_scale")
    chk("aggregate overall pass", agg["overall"] == "pass")


def test_aggregate_all_native_stall():
    print("aggregate flags phase_b_needed + tree_of_partials when all native TCP stall:")
    m = r.progress_config_matrix()
    arts = [_progress_art(m[0])] + [_stalled_art(c) for c in m[1:]]
    agg = r.build_progress_sweep_aggregate(arts)
    chk("no winner", agg["winning_passive_progress_config"] is None)
    chk("phase_b_needed True", agg["phase_b_needed"] is True)
    chk("tree_of_partials_recommended True", agg["tree_of_partials_recommended"] is True)
    chk("collectives deferred True", agg["collectives_deferred"] is True)
    chk("poll control pass", agg["poll_control_status"] == "pass")
    chk("next: phase_b then tree", agg["next_direction"]
        == "phase_b_mpi_ib_then_tree_of_partials_with_poll")
    chk("aggregate overall pass (control healthy)", agg["overall"] == "pass")


def test_progress_fences_and_same_axis():
    print("progress artifact + aggregate: fences locked, same_axis False:")
    m = r.progress_config_matrix()
    a = _progress_art(m[3])
    agg = r.build_progress_sweep_aggregate([_progress_art(m[0]), a])
    for label, obj in (("artifact", a), ("aggregate", agg)):
        chk(f"{label} fences locked", r._fences_locked(obj["fences"]))
        chk(f"{label} same_axis False", obj["same_axis_comparison"] is False)


def test_progress_no_forbidden_keys():
    print("progress artifact + aggregate: no forbidden claim / ratio / speedup keys:")
    m = r.progress_config_matrix(include_phase_b=True)
    arts = [_progress_art(m[0])] + [_stalled_art(c) for c in m[1:6]]
    agg = r.build_progress_sweep_aggregate(arts)
    for label, obj in (("artifact", arts[3]), ("aggregate", agg)):
        leaked = {k.lower() for k in _keys_deep(obj)} & _FORBIDDEN_KEYS
        chk(f"{label} no forbidden keys", not leaked)


def test_progress_diagnosis_still_skips():
    print("run_progress_diagnosis stays skip-only off-cluster (no faked evidence):")
    ps, pa = r.run_progress_diagnosis(env={}, write=False)
    chk("skip + no artifact", ps == "skip" and pa is None)


# --------------------------------------------------------------------------- Slice 1b: driver seams

def test_config_to_launch_args():
    print("config_to_launch_args maps each config to concrete launch knobs:")
    m = r.progress_config_matrix()
    la0 = r.config_to_launch_args(m[0])
    chk("A0 mode poll", la0["composition_mode"] == "root_flat_gather_poll")
    chk("A0 root threads 4", la0["root_hpx_threads"] == 4)
    chk("A0 connector threads 8", la0["connector_threads"] == 8)
    chk("A0 no yielder", la0["background_yielder"] is False)
    chk("A0 bind balanced", la0["root_bind"] == "balanced")
    chk("A0 transport tcp", la0["transport"] == "tcp")
    la2 = r.config_to_launch_args(m[2])
    chk("A2 root threads 8", la2["root_hpx_threads"] == 8 and la2["background_yielder"] is False)
    la3 = r.config_to_launch_args(m[3])
    chk("A3 yielder True", la3["background_yielder"] is True)
    la5 = r.config_to_launch_args(m[5])
    chk("A5 dataflow + yielder", la5["composition_mode"] == "dataflow_reduce"
        and la5["background_yielder"] is True)
    mb = {c["id"]: c for c in r.progress_config_matrix(include_phase_b=True)}
    chk("B1 transport mpi", r.config_to_launch_args(mb["B1"])["transport"] == "mpi")


def test_partials_from_records():
    print("_partials_from_records groups leaf records into per-locality partials:")
    recs = [{"i": i, "value": r.leaf_value(7, i), "locality": (1 if i % 2 == 0 else 2)}
            for i in range(8)]
    partials = r._partials_from_records(recs)
    chk("two localities", {p["locality"] for p in partials} == {1, 2})
    chk("4/4 split", sorted(p["n_leaves"] for p in partials) == [4, 4])
    w = r.contribution_witness(partials, 0)
    chk("leaves_remote 8", w["leaves_remote"] == 8)
    chk("empty records -> no partials", r._partials_from_records([]) == [])


def _native_last(x=7, n=8, timed_out=0):
    """Mimic collective_ext.fanout_fanin_remote's RemoteResult tuple for a native success."""
    leaves = [(i, r.leaf_value(x, i), (1 if i % 2 == 0 else 2)) for i in range(n)]
    value = r.composite_oracle(x, n) if timed_out == 0 else 0
    return (value, leaves, "when_all_then_reduce", "root_fold_sum_int64", 2, n, timed_out,
            "composed_future_wait_for", True, False, True)


def test_ext_result_shape_to_artifact():
    print("C++ RemoteResult tuple parses into the expected per-config artifact fields:")
    config = r.progress_config_matrix()[3]  # A3 native background_yielder
    last = _native_last()
    value, leaves = last[0], last[1]
    records = [{"i": int(i), "value": int(v), "locality": int(loc)} for (i, v, loc) in leaves]
    timed_out = int(last[6])
    native = r._is_native_mode("when_all_then_reduce")
    woke = bool(native and timed_out == 0 and r.oracle_gate_scalar(int(value), 7, 8))
    art = r.build_progress_config_artifact(
        config=config, x=7, n=8, k=20, w=5, prewarm=5, dispatch_timeout_s=8.0,
        measured_value=int(value), dispatch_elapsed_s=0.003, no_dispatch_timeout=(timed_out == 0),
        passive_wait_woke_without_poll=woke, timed_out_leaf_count=timed_out,
        partials=r._partials_from_records(records), root_locality=0,
        declared_remote_localities=[1, 2],
        connectors=[{"joined": True, "served": True, "graceful_disconnect": True} for _ in range(2)],
        hpx_config_provenance={"hpx.parcel.tcp.array_optimization": "unknown",
                               "hpx.threads": "8"})
    chk("native success passive-progress capable", art["passive_progress_capable"] is True)
    chk("outcome native_progress_pass", art["outcome"] == "native_progress_pass")
    chk("expected artifact blocks present",
        {"progress", "correctness", "distribution", "provenance", "gates"}.issubset(art))
    chk("hpx_config recorded verbatim",
        art["provenance"]["hpx_config"]["hpx.parcel.tcp.array_optimization"] == "unknown")
    chk("distribution 4/4", art["distribution"]["leaves_per_remote_locality"] == {1: 4, 2: 4})
    chk("same_axis False", art["same_axis_comparison"] is False)
    chk("fences locked", r._fences_locked(art["fences"]))


def test_provenance_unknown_and_stall_fail_closed():
    print("unknown HPX-config values never flip gates; a stalled/errored config fails closed:")
    config = r.progress_config_matrix()[1]  # A1 native
    art = r.build_progress_config_artifact(
        config=config, x=7, n=8, k=20, w=5, prewarm=5, dispatch_timeout_s=8.0, measured_value=None,
        dispatch_elapsed_s=8.0, no_dispatch_timeout=False, passive_wait_woke_without_poll=False,
        timed_out_leaf_count=8, partials=[], root_locality=0, declared_remote_localities=[1, 2],
        connectors=None, hpx_config_provenance={"hpx.threads": "unknown",
                                                "hpx.parcel.tcp.enable": "unknown"})
    chk("errored/stalled not passive-progress", art["passive_progress_capable"] is False)
    chk("outcome native_stalled", art["outcome"] == "native_stalled")
    chk("hpx_config is provenance, not a gate", "hpx_config" not in art["gates"])
    chk("unknown values do not appear as gate keys",
        not any("unknown" in str(k).lower() for k in art["gates"]))
    chk("fences still locked", r._fences_locked(art["fences"]))


def test_progress_phases_skip_when_unbuilt():
    print("progress phases skip cleanly off-cluster / when the ext or connector is unbuilt:")
    ds, da = r.run_progress_diagnosis(env={}, write=False)
    chk("progress-diagnosis skip off-cluster", ds == "skip" and da is None)
    ss, sa = r.run_progress_sweep(env={}, write=False)
    chk("progress-sweep skip off-cluster", ss == "skip" and sa is None)
    # unknown config id fails before any cluster work.
    us, _ = r.run_progress_diagnosis(config_id="ZZ", env={}, write=False)
    chk("unknown config_id fails", us == "fail")
    # a >=3-node allocation shape but no built ext/connector still skips (never faked).
    on3 = {"SLURM_JOB_ID": "200", "SLURM_JOB_NODELIST": "medusa[00-02]"}
    orig = r._scontrol_hostnames
    try:
        r._scontrol_hostnames = lambda nodelist: ["medusa00", "medusa01", "medusa02"]

        def _boom():
            raise ImportError("collective_ext absent")
        cs, ca = r.run_progress_diagnosis(config_id="A1", env=on3, import_fn=_boom, write=False)
        chk("3-node but unbuilt -> skip", cs == "skip" and ca is None)
    finally:
        r._scontrol_hostnames = orig


# --------------------------------------------------------------------------- Slice 1b-follow-up:
# instrumented A1 diagnostic pure-mapping tests (dataflow_reduce/A5 classification + instrumentation).

def test_a5_dataflow_native_stalled():
    print("A5 dataflow_reduce, when it stalls, classifies as native_stalled (not control/fail/unknown):")
    a5 = _stalled_art(r.progress_config_matrix()[5])
    chk("A5 mode dataflow_reduce", a5["composition_primitive"] == "dataflow_reduce")
    chk("outcome native_stalled", a5["outcome"] == "native_stalled")
    chk("overall stalled", a5["overall"] == "stalled")
    chk("native True", a5["provenance"]["hpx_native_composition"] is True)
    chk("polled False", a5["provenance"]["polled_in_success_path"] is False)
    chk("watchdog composed_future_wait_for",
        a5["provenance"]["watchdog"] == "composed_future_wait_for")
    chk("cross-node validated False", a5["provenance"]["cross_node_composition_validated"] is False)
    chk("NOT passive-progress capable", a5["passive_progress_capable"] is False)
    # a healthy (woke) A5 is a native pass, proving the stall verdict is earned, not hard-coded.
    a5_ok = _progress_art(r.progress_config_matrix()[5])
    chk("healthy A5 native_progress_pass", a5_ok["outcome"] == "native_progress_pass")


def test_wait_for_status_mapping():
    print("diagnostic_instrumentation maps future::wait_for status honestly:")
    ready = r.diagnostic_instrumentation(n=8, wait_for_status="ready",
                                         leaf_futures_ready_count_at_timeout=8)
    chk("ready -> returned_ready True", ready["wait_for_returned_ready"] is True
        and ready["wait_for_returned_timeout"] is False and ready["wait_for_status"] == "ready")
    to = r.diagnostic_instrumentation(n=8, wait_for_status="timeout",
                                      leaf_futures_ready_count_at_timeout=8,
                                      composed_future_ready_at_timeout=True)
    chk("timeout -> returned_timeout True", to["wait_for_returned_timeout"] is True
        and to["wait_for_returned_ready"] is False and to["wait_for_status"] == "timeout")
    dfr = r.diagnostic_instrumentation(n=8, wait_for_status="deferred")
    chk("deferred preserved, neither ready nor timeout", dfr["wait_for_status"] == "deferred"
        and dfr["wait_for_returned_ready"] is False and dfr["wait_for_returned_timeout"] is False)
    unk = r.diagnostic_instrumentation(n=8, wait_for_status="banana")
    chk("unknown status normalizes to 'unknown'", unk["wait_for_status"] == "unknown")


def test_exception_stage_mapping():
    print("diagnostic_instrumentation maps exception stage/type/message; unknown stage -> unknown:")
    for stage in ("after_wait_for", "before_get", "after_disconnect", "finalize", "none"):
        d = r.diagnostic_instrumentation(n=8, wait_for_status="ready", exception_stage=stage)
        chk(f"stage {stage} preserved", d["exception_stage"] == stage)
    seen = r.diagnostic_instrumentation(n=8, wait_for_status="timeout", exception_stage="after_wait_for",
                                        exception_type="std::system_error",
                                        exception_message="Operation not permitted")
    chk("exception_observed True with type", seen["exception_observed"] is True
        and seen["exception_type"] == "std::system_error"
        and "Operation not permitted" in seen["exception_message"])
    bad = r.diagnostic_instrumentation(n=8, wait_for_status="ready", exception_stage="wherever")
    chk("unknown stage -> 'unknown'", bad["exception_stage"] == "unknown")
    clean = r.diagnostic_instrumentation(n=8, wait_for_status="ready")
    chk("no exception -> observed False, stage none", clean["exception_observed"] is False
        and clean["exception_stage"] == "none")


def test_unsafe_timeout_abandonment_fails_closed():
    print("unsafe_timeout_abandonment is DERIVED fail-closed on timeout:")
    # timeout with all leaves ready -> safe (no unready leaf abandoned).
    safe = r.diagnostic_instrumentation(n=8, wait_for_status="timeout",
                                        leaf_futures_ready_count_at_timeout=8)
    chk("all leaves ready at timeout -> safe", safe["unsafe_timeout_abandonment"] is False)
    # timeout with fewer than n ready -> unsafe.
    unsafe = r.diagnostic_instrumentation(n=8, wait_for_status="timeout",
                                          leaf_futures_ready_count_at_timeout=3)
    chk("partial leaves ready at timeout -> unsafe", unsafe["unsafe_timeout_abandonment"] is True)
    # timeout with readiness unknown -> cannot prove safe -> unsafe (fail closed).
    unknown = r.diagnostic_instrumentation(n=8, wait_for_status="timeout",
                                           leaf_futures_ready_count_at_timeout=None)
    chk("unknown readiness at timeout -> fail closed unsafe",
        unknown["unsafe_timeout_abandonment"] is True)
    # ready (no timeout) is never an abandonment.
    ready = r.diagnostic_instrumentation(n=8, wait_for_status="ready")
    chk("ready -> not abandonment", ready["unsafe_timeout_abandonment"] is False)
    # an explicit ext-asserted value is honored (not overridden by the derivation).
    asserted = r.diagnostic_instrumentation(n=8, wait_for_status="timeout",
                                            leaf_futures_ready_count_at_timeout=8,
                                            unsafe_timeout_abandonment=True)
    chk("explicit ext assertion honored", asserted["unsafe_timeout_abandonment"] is True)


def test_diagnostic_variants_and_fences():
    print("diagnostic variants add only supported HPX flags and never touch claim fences:")
    # diagnostic_variant_root_extra now NEVER emits a bind flag (bind is owned by build_a1_root_hpx_args
    # so it appears exactly once). It only carries the non-bind max_background_threads ini.
    chk("normal adds nothing", r.diagnostic_variant_root_extra("a1_normal") == [])
    chk("bind_none adds no bind flag here (single-owner bind)",
        r.diagnostic_variant_root_extra("a1_root_bind_none") == [])
    chk("max_bg_threads supported ini flag",
        r.diagnostic_variant_root_extra("a1_max_bg_threads", max_bg_threads=8)
        == ["--hpx:ini=hpx.max_background_threads=8"])
    chk("roomier cpuset is allocation-level, no code arg",
        r.diagnostic_variant_root_extra("a1_roomier_cpuset") == [])
    chk("unknown variant adds nothing (never guess a flag)",
        r.diagnostic_variant_root_extra("a1_bogus") == [])
    chk("no variant extra ever contains a bind flag",
        all(not any("--hpx:bind" in a for a in r.diagnostic_variant_root_extra(v))
            for v in r.DIAGNOSTIC_VARIANTS))
    # every variant normalizes into the instrumentation block; unknown -> 'unknown'; no fence leaks.
    for v in r.DIAGNOSTIC_VARIANTS:
        d = r.diagnostic_instrumentation(n=8, wait_for_status="ready", diagnostic_variant=v)
        chk(f"variant {v} preserved", d["diagnostic_variant"] == v)
    bogus = r.diagnostic_instrumentation(n=8, wait_for_status="ready", diagnostic_variant="a1_bogus")
    chk("unknown variant -> 'unknown'", bogus["diagnostic_variant"] == "unknown")
    leaked = {k.lower() for k in _keys_deep(bogus)} & _FORBIDDEN_KEYS
    chk("instrumentation block has no forbidden claim keys", not leaked)


def test_a1_root_hpx_args_single_bind():
    print("build_a1_root_hpx_args emits EXACTLY ONE --hpx:bind (no 'none;none' double bind):")
    for v in r.DIAGNOSTIC_VARIANTS:
        args = r.build_a1_root_hpx_args(root_ip="10.42.5.30", root_port=7913, variant=v)
        binds = [a for a in args if a.startswith("--hpx:bind=")]
        chk(f"{v}: exactly one bind flag", len(binds) == 1)
    # a1_root_bind_none: the single bind is exactly '--hpx:bind=none' (not 'none;none').
    none_args = r.build_a1_root_hpx_args(root_ip="10.42.5.30", root_port=7913,
                                         variant="a1_root_bind_none")
    chk("bind_none -> single --hpx:bind=none", [a for a in none_args if a.startswith("--hpx:bind=")]
        == ["--hpx:bind=none"])
    chk("bind_none never builds none;none", all("none;none" not in a for a in none_args))
    chk("bind mode helper: bind_none is none", r._a1_root_bind_for("a1_root_bind_none") == "none")
    # every other variant keeps balanced.
    for v in ("a1_normal", "a1_max_bg_threads", "a1_roomier_cpuset"):
        bargs = r.build_a1_root_hpx_args(root_ip="10.42.5.30", root_port=7913, variant=v)
        chk(f"{v} bind balanced", [a for a in bargs if a.startswith("--hpx:bind=")]
            == ["--hpx:bind=balanced"] and r._a1_root_bind_for(v) == "balanced")
    # max_bg_threads variant carries the ini flag exactly once, still one bind.
    mba = r.build_a1_root_hpx_args(root_ip="10.42.5.30", root_port=7913, variant="a1_max_bg_threads",
                                   max_bg_threads=8)
    chk("max_bg_threads carries ini + one bind",
        mba.count("--hpx:ini=hpx.max_background_threads=8") == 1
        and len([a for a in mba if a.startswith("--hpx:bind=")]) == 1)


def test_connector_srun_decoupled_from_root_cpuset():
    print("build_connector_srun_cmd binds the connector inside its own node, independent of root -c:")
    cmd = r.build_connector_srun_cmd(
        "medusa01", "/tmp/bd", connector_bin="/path/collective_connector", connector_threads=8,
        serve_timeout=90, prefer_subnet="10.42.5.", root_ip="10.42.5.30", root_port=7913)
    chk("is an srun command", cmd[0] == "srun")
    chk("uses --overlap", "--overlap" in cmd)
    chk("uses --cpu-bind=none (does not inherit root step cpu mask)", "--cpu-bind=none" in cmd)
    chk("targets the connector node", "--nodelist=medusa01" in cmd)
    chk("keeps its own -c 8", "--cpus-per-task=8" in cmd)
    chk("connector HPX bind=none", "--hpx:bind=none" in cmd)
    chk("connector threads 8", "--hpx:threads=8" in cmd)
    chk("preprobes the root agas", f"--agas-preprobe-host=10.42.5.30" in cmd)
    # the command references only the connector's own node -- never the root's -c / cpuset.
    chk("no root cpus-per-task leaked into connector cmd",
        not any("--cpus-per-task=40" in a for a in cmd))
    chk("connector bind mode constant is none", r.CONNECTOR_BIND_MODE == "none")


def test_diagnostic_cpuset_provenance_fields():
    print("diagnostic_instrumentation records root/connector step cpuset+bind provenance:")
    d = r.diagnostic_instrumentation(
        n=8, wait_for_status="ready", root_effective_cpuset=[0, 2, 4, 6, 8, 10, 12, 14],
        root_bind_mode="balanced", root_threads=4, root_step_cpus_per_task="40",
        connector_effective_cpusets=[[0, 2, 4, 6, 8, 10, 12, 14], [0, 2, 4, 6, 8, 10, 12, 14]],
        connector_threads=8, connector_step_cpus_per_task=8, connector_bind_mode="none",
        slurm_cpus_per_task="40",
        slurm_step_context={"slurm_job_id": "158911", "slurm_step_id": "3"},
        diagnostic_variant="a1_roomier_cpuset")
    for key in ("root_step_cpus_per_task", "connector_step_cpus_per_task", "connector_bind_mode",
                "slurm_step_context", "root_effective_cpuset", "root_bind_mode",
                "connector_effective_cpusets", "diagnostic_variant"):
        chk(f"has {key}", key in d)
    chk("root roomier -c recorded", d["root_step_cpus_per_task"] == "40")
    chk("connector keeps its own -c 8", d["connector_step_cpus_per_task"] == 8)
    chk("connector bind mode none", d["connector_bind_mode"] == "none")
    chk("root/connector cpusets distinct-able", d["root_effective_cpuset"] == [0, 2, 4, 6, 8, 10, 12, 14]
        and d["connector_effective_cpusets"][0] == [0, 2, 4, 6, 8, 10, 12, 14])
    chk("slurm step context carried", d["slurm_step_context"]["slurm_job_id"] == "158911")
    # prior instrumentation fields are preserved alongside the new provenance.
    for key in ("wait_for_status", "wait_for_returned_ready", "wait_for_returned_timeout",
                "exception_stage", "exception_type", "exception_message",
                "unsafe_timeout_abandonment", "leaf_futures_ready_count_at_timeout",
                "composed_future_ready_at_timeout"):
        chk(f"prior field preserved: {key}", key in d)
    leaked = {k.lower() for k in _keys_deep(d)} & _FORBIDDEN_KEYS
    chk("no forbidden claim keys", not leaked)


# --------------------------------------------------------------------------- Slice 1b-follow-up2:
# root-process resource-trend instrumentation (pure parsers + fail-closed trend summary).

_STATUS_SAMPLE = (
    "Name:\tpython3\n"
    "State:\tR (running)\n"
    "Threads:\t37\n"
    "VmSize:\t 123456 kB\n"
    "VmRSS:\t  45678 kB\n"
)


def test_proc_status_parsers():
    print("parse_proc_status_threads / _mem read /proc/self/status text, fail closed on absence:")
    chk("threads parsed", r.parse_proc_status_threads(_STATUS_SAMPLE) == 37)
    chk("VmRSS parsed", r.parse_proc_status_mem(_STATUS_SAMPLE, "VmRSS") == 45678)
    chk("VmSize parsed", r.parse_proc_status_mem(_STATUS_SAMPLE, "VmSize") == 123456)
    chk("threads unknown when absent", r.parse_proc_status_threads("Name:\tx\n") == "unknown")
    chk("threads unknown on empty", r.parse_proc_status_threads("") == "unknown")
    chk("mem unknown when absent", r.parse_proc_status_mem("Threads:\t3\n", "VmRSS") == "unknown")
    chk("threads unknown on garbage value",
        r.parse_proc_status_threads("Threads:\tNaN\n") == "unknown")


def _raise_oserror(_):
    raise OSError("no such directory")


def test_count_open_fds_fail_closed():
    print("count_open_fds counts a listing and fails closed to 'unknown' on a missing directory:")
    chk("counts injected listing",
        r.count_open_fds("/proc/self/fd", lister=lambda d: ["0", "1", "2", "5"]) == 4)
    chk("empty listing -> 0", r.count_open_fds("/x", lister=lambda d: []) == 0)
    chk("missing dir -> unknown", r.count_open_fds("/nope", lister=_raise_oserror) == "unknown")


def test_cgroup_pids_parser():
    print("parse_cgroup_pids handles known current, numeric/max max, and missing values:")
    a = r.parse_cgroup_pids("12", "100")
    chk("numeric current/max", a["pids_current"] == 12 and a["pids_max"] == 100)
    b = r.parse_cgroup_pids("7", "max")
    chk("max unlimited preserved", b["pids_current"] == 7 and b["pids_max"] == "max")
    c = r.parse_cgroup_pids(None, None)
    chk("missing -> unknown", c["pids_current"] == "unknown" and c["pids_max"] == "unknown")
    d = r.parse_cgroup_pids("  99\n", "  512\n")
    chk("whitespace tolerated", d["pids_current"] == 99 and d["pids_max"] == 512)
    e = r.parse_cgroup_pids("", "garbage")
    chk("empty/garbage -> unknown", e["pids_current"] == "unknown" and e["pids_max"] == "unknown")


def test_resource_snapshot_fail_closed():
    print("resource_snapshot normalizes every field to 'unknown' when its source is unavailable:")
    empty = r.resource_snapshot(3, "before_dispatch", status_text=None, fd_lister=_raise_oserror,
                                pids_current_text=None, pids_max_text=None)
    chk("call_index/stage kept", empty["call_index"] == 3 and empty["stage"] == "before_dispatch")
    for key in ("threads", "open_fds", "pids_current", "pids_max", "vm_rss_kb", "vm_size_kb"):
        chk(f"{key} unknown", empty[key] == "unknown")
    full = r.resource_snapshot(4, "after_call", status_text=_STATUS_SAMPLE,
                               fd_lister=lambda d: list(range(20)), pids_current_text="50",
                               pids_max_text="512")
    chk("threads/fds/pids populated", full["threads"] == 37 and full["open_fds"] == 20
        and full["pids_current"] == 50 and full["pids_max"] == 512)


def _snap(ci, stage, threads, fds, cur, mx=512):
    return {"call_index": ci, "stage": stage, "threads": threads, "open_fds": fds,
            "pids_current": cur, "pids_max": mx, "vm_rss_kb": "unknown", "vm_size_kb": "unknown"}


def test_resource_trend_growth_and_near_limit():
    print("resource_trend_summary reports deltas, monotonic growth, and a near-limit suspicion:")
    snaps = [_snap(-1, "before_first_call", 30, 40, 100),
             _snap(0, "before_dispatch", 33, 42, 200),
             _snap(1, "before_dispatch", 36, 44, 480),
             _snap(1, "at_exception", 39, 46, 500)]
    t = r.resource_trend_summary(snaps)
    chk("threads_delta 9", t["threads_delta"] == 9)
    chk("fds_delta 6", t["fds_delta"] == 6)
    chk("pids_current_delta 400", t["pids_current_delta"] == 400)
    chk("threads strictly increasing", t["threads_strictly_increasing"] is True)
    chk("fds strictly increasing", t["fds_strictly_increasing"] is True)
    chk("pids_max_effective 512", t["pids_max_effective"] == 512)
    chk("near-limit suspected (500/512 >= 90%)", t["resource_limit_hit_suspected"] is True)
    chk("reason names pids near limit", "pids.current" in t["resource_limit_hit_reason"])
    # a comfortably-below-limit, flat series raises no suspicion.
    flat = [_snap(-1, "before_first_call", 30, 40, 50), _snap(0, "after_call", 30, 40, 50)]
    tf = r.resource_trend_summary(flat)
    chk("flat below-limit not suspected", tf["resource_limit_hit_suspected"] is False)
    chk("flat threads_delta 0", tf["threads_delta"] == 0)


def test_resource_trend_no_overclaim_on_unknown():
    print("resource_trend_summary never overclaims when values are unknown:")
    snaps = [_snap(-1, "before_first_call", "unknown", "unknown", "unknown", mx="unknown"),
             _snap(0, "at_exception", "unknown", "unknown", "unknown", mx="unknown")]
    t = r.resource_trend_summary(snaps)
    chk("threads_delta unknown", t["threads_delta"] == "unknown")
    chk("fds_delta unknown", t["fds_delta"] == "unknown")
    chk("pids_current_delta unknown", t["pids_current_delta"] == "unknown")
    chk("growth unknown, not True", t["threads_strictly_increasing"] == "unknown")
    chk("no suspicion on unknowns", t["resource_limit_hit_suspected"] is False)
    chk("pids_max_effective unknown", t["pids_max_effective"] == "unknown")
    # a single snapshot cannot form a delta.
    one = r.resource_trend_summary([_snap(-1, "before_first_call", 30, 40, 50)])
    chk("single snapshot -> delta unknown", one["threads_delta"] == "unknown"
        and one["resource_limit_hit_suspected"] is False)


def test_diagnostic_errno_and_resource_fields():
    print("diagnostic_instrumentation carries errno code/category + resource trend, no overclaim:")
    snaps = [_snap(-1, "before_first_call", 30, 40, 100), _snap(7, "at_exception", 42, 52, 500)]
    trend = r.resource_trend_summary(snaps)
    d = r.diagnostic_instrumentation(
        n=8, wait_for_status="unknown", exception_stage="before_dispatch",
        exception_type="St12system_error", exception_message="Operation not permitted",
        exception_code_value=1, exception_code_category="generic",
        resource_snapshots=snaps, resource_trend_summary=trend, diagnostic_variant="a1_normal")
    chk("errno value mapped", d["exception_code_value"] == 1)
    chk("errno category mapped", d["exception_code_category"] == "generic")
    chk("snapshots carried", isinstance(d["resource_snapshots"], list) and len(d["resource_snapshots"]) == 2)
    chk("trend summary carried", d["resource_trend_summary"]["threads_delta"] == 12)
    chk("threads_delta lifted", d["threads_delta"] == 12)
    chk("fds_delta lifted", d["fds_delta"] == 12)
    chk("pids_current_delta lifted", d["pids_current_delta"] == 400)
    chk("limit suspicion lifted", d["resource_limit_hit_suspected"] is True)
    chk("limit reason lifted", "pids.current" in d["resource_limit_hit_reason"])
    # non-system_error / absent code -> None (never fabricated), category None.
    d2 = r.diagnostic_instrumentation(n=8, wait_for_status="ready", exception_code_value=None,
                                      exception_code_category=None)
    chk("absent errno -> None", d2["exception_code_value"] is None
        and d2["exception_code_category"] is None)
    chk("no snapshots -> empty list + no data reason", d2["resource_snapshots"] == []
        and d2["resource_limit_hit_suspected"] is False)
    chk("prior instrumentation preserved", "wait_for_status" in d and "unsafe_timeout_abandonment" in d)
    leaked = {k.lower() for k in _keys_deep(d)} & _FORBIDDEN_KEYS
    chk("no forbidden claim keys with resource fields", not leaked)


def test_a0_a1_diag_mode_routing_and_diagnostic_fields():
    print("A0/A1 diag share the instrumented path: mode routing, labels, diagnostic_information:")
    chk("A0 config id", r._DIAG_CONFIG_ID["root_flat_gather_poll"] == "A0-diag")
    chk("A1 config id", r._DIAG_CONFIG_ID["when_all_then_reduce"] == "A1-diag")
    p0 = r._a1_diag_artifact_path("158999", "a1_normal", exp_dir="/tmp",
                                  composition_mode="root_flat_gather_poll")
    p1 = r._a1_diag_artifact_path("158999", "a1_normal", exp_dir="/tmp",
                                  composition_mode="when_all_then_reduce")
    chk("A0 filename tagged a0diag", "a0diag_a1_normal" in p0 and p0 != p1)
    chk("A1 filename tagged a1diag", "a1diag_a1_normal" in p1)
    # unknown composition mode fails fast (before cluster work); a valid mode skips off-cluster.
    st_bad, _ = r.run_a1_diagnostic(composition_mode="bogus", env={}, write=False)
    chk("unknown composition mode -> fail", st_bad == "fail")
    st_skip, _ = r.run_a1_diagnostic(composition_mode="root_flat_gather_poll", env={}, write=False)
    chk("A0 skips cleanly off-cluster", st_skip == "skip")
    # diagnostic_information + errno fields flow through the instrumentation block.
    d = r.diagnostic_instrumentation(
        n=8, wait_for_status="unknown", exception_stage="before_dispatch",
        exception_type="St12system_error", exception_message="Operation not permitted",
        exception_code_value=1, exception_code_category="generic",
        exception_diagnostic_information="{hpx}: file x, function y: EPERM",
        exception_diagnostic_available=True)
    chk("diagnostic info carried", "EPERM" in d["exception_diagnostic_information"])
    chk("diagnostic available True", d["exception_diagnostic_available"] is True)
    d2 = r.diagnostic_instrumentation(n=8, wait_for_status="ready")
    chk("absent diagnostic -> None/False", d2["exception_diagnostic_information"] is None
        and d2["exception_diagnostic_available"] is False)
    leaked = {k.lower() for k in _keys_deep(d)} & _FORBIDDEN_KEYS
    chk("no forbidden keys with diagnostic fields", not leaked)


# ---------------------------------------------------------------------------

def test_connector_shutdown_classification():
    print("connector-lifetime hardening: shutdown-reason classification is honest and fail-closed:")
    # root completion -> graceful lifecycle fields
    done = {"connector_lifetime_mode": "root_completion_or_heartbeat_deadman",
            "connector_shutdown_reason": "root_completion_signal", "root_completion_signaled": True,
            "serve_timeout_expired": False, "serve_timeout_s": 90,
            "connector_stayed_alive_until_root_done": True, "served": True,
            "root_completion_unix": 111.0, "connector_observed_completion_unix": 112.0}
    c = r.classify_connector_shutdown(done)
    chk("root-completion reason preserved", c["connector_shutdown_reason"] == "root_completion_signal")
    chk("root-completion signaled True", c["root_completion_signaled"] is True)
    chk("root-completion stayed-alive True", c["connector_stayed_alive_until_root_done"] is True)
    chk("root-completion not timeout", c["serve_timeout_expired"] is False)
    chk("completion times carried", c["root_completion_signal_time"] == 111.0
        and c["connector_observed_completion_time"] == 112.0)
    # serve-timeout deadman -> timeout-classified, DISTINCT from root-completion
    to = {"connector_shutdown_reason": "serve_timeout_expired", "root_completion_signaled": False,
          "serve_timeout_expired": True, "serve_timeout_s": 90,
          "connector_stayed_alive_until_root_done": False}
    t = r.classify_connector_shutdown(to)
    chk("timeout reason preserved", t["connector_shutdown_reason"] == "serve_timeout_expired")
    chk("timeout distinct from root-completion",
        t["connector_shutdown_reason"] != c["connector_shutdown_reason"])
    chk("timeout not root-signaled", t["root_completion_signaled"] is False)
    chk("timeout not stayed-alive", t["connector_stayed_alive_until_root_done"] is False)
    # old fixed-serve-window build (no new fields) -> unknown, NEVER labeled root-completion
    legacy = r.classify_connector_shutdown({"served": True})
    chk("legacy build -> unknown reason", legacy["connector_shutdown_reason"] == "unknown")
    chk("legacy not root-signaled", legacy["root_completion_signaled"] is False)
    chk("legacy lifetime mode unknown", legacy["connector_lifetime_mode"] == "unknown")
    # a bogus reason string is fail-closed to unknown (never trusted)
    bogus = r.classify_connector_shutdown({"connector_shutdown_reason": "made_up"})
    chk("bogus reason -> unknown", bogus["connector_shutdown_reason"] == "unknown")
    # a teardown error is preserved as its own reason
    err = r.classify_connector_shutdown({"connector_shutdown_reason": "error"})
    chk("error reason preserved", err["connector_shutdown_reason"] == "error")
    # no forbidden claim keys leak through the classifier
    leaked = {k.lower() for k in _keys_deep(c)} & _FORBIDDEN_KEYS
    chk("no forbidden claim keys in classification", not leaked)


# --------------------------------------------------------------------------- Slice 2a: native-
# composition retest under the hardened connector-lifetime contract (pure gate + filename + skip).

def _native_calls(mode, x=7, n=8, k=20, remote_locs=(1, 2), ready=True, timed_out=0):
    """Per-call records shaped like _drive_native_composition captures (ready or stalled)."""
    calls = []
    for c in range(k):
        locs = [remote_locs[i % len(remote_locs)] for i in range(n)]
        composite = r.composite_oracle(x, n) if ready else None
        calls.append({
            "call_index": c,
            "wait_for_status": "ready" if ready else "timeout",
            "exception_type": "",
            "exception_stage": "none",
            "timed_out_leaf_count": 0 if ready else timed_out,
            "composite": composite,
            "composite_oracle_correct": ready,
            "leaf_localities": locs,
            "n_localities": len(remote_locs),
        })
    return calls


def _hardened_connectors(reason="root_completion_signal", count=2):
    """`count` connectors that joined/served/left gracefully via the given shutdown reason."""
    return [{"joined": True, "served": True, "graceful_disconnect": True,
             "connector_shutdown_reason": reason,
             "root_completion_signaled": reason == "root_completion_signal",
             "serve_timeout_expired": reason == "serve_timeout_expired",
             "connector_stayed_alive_until_root_done": reason == "root_completion_signal"}
            for _ in range(count)]


def _gate(mode, calls, connectors, *, error=None, expected_k=20, late=False):
    return r.native_composition_validation_gate(
        mode=mode, calls=calls, x=7, n=8, root_locality=0, declared_remote_localities=[1, 2],
        connectors=connectors, error=error, expected_k=expected_k,
        late_parcel_after_shutdown_detected=late)


def test_native_composition_validation_gate():
    print("Slice 2a native-composition validation gate is honest and fail-closed:")
    # clean native when_all_then_reduce under hardened lifecycle -> validated
    g = _gate("when_all_then_reduce", _native_calls("when_all_then_reduce"), _hardened_connectors())
    chk("clean native validated", g["cross_node_composition_validated"] is True)
    chk("clean native mechanics_ok", g["mechanics_ok"] is True)
    chk("native hpx_native True / polled False",
        g["hpx_native_composition"] is True and g["polled_in_success_path"] is False)
    chk("all gate booleans True", all(g["gates"].values()))

    # dataflow_reduce classification stays native and can validate when clean
    gd = _gate("dataflow_reduce", _native_calls("dataflow_reduce"), _hardened_connectors())
    chk("dataflow native classification",
        gd["hpx_native_composition"] is True and gd["polled_in_success_path"] is False)
    chk("dataflow validated when clean", gd["cross_node_composition_validated"] is True)

    # poll CONTROL: mechanics can pass but polled_in_success_path=True -> NOT native validation
    gp = _gate("root_flat_gather_poll", _native_calls("root_flat_gather_poll"), _hardened_connectors())
    chk("poll control polled_in_success_path True", gp["polled_in_success_path"] is True)
    chk("poll control mechanics_ok", gp["mechanics_ok"] is True)
    chk("poll control NOT native-validated", gp["cross_node_composition_validated"] is False)

    # native validation REQUIRES root_completion_signal: serve_timeout_expired connectors fail it
    gt = _gate("when_all_then_reduce", _native_calls("when_all_then_reduce"),
               _hardened_connectors("serve_timeout_expired"))
    chk("serve_timeout_expired fails root-completion gate",
        gt["gates"]["connector_shutdown_root_completion"] is False)
    chk("serve_timeout_expired not native-validated", gt["cross_node_composition_validated"] is False)

    # a native STALL (timeout, oracle None, timed_out>0, <K calls) fails closed
    gs = _gate("when_all_then_reduce",
               _native_calls("when_all_then_reduce", ready=False, timed_out=8, k=1),
               _hardened_connectors())
    chk("stalled native not validated", gs["cross_node_composition_validated"] is False)
    chk("stalled native wait/oracle/timeout gates fail",
        gs["gates"]["wait_for_status_ready"] is False
        and gs["gates"]["composite_oracle_correct"] is False
        and gs["gates"]["no_dispatch_timeout"] is False)
    chk("short call list fails all_calls_completed", gs["gates"]["all_calls_completed"] is False)

    # a root-local leaf trips placement gates
    local_calls = _native_calls("when_all_then_reduce")
    local_calls[0]["leaf_localities"][0] = 0  # a leaf on the root locality
    gl = _gate("when_all_then_reduce", local_calls, _hardened_connectors())
    chk("root-local leaf fails placement",
        gl["gates"]["leaves_local_zero"] is False and gl["gates"]["leaves_remote_all"] is False)

    # only one remote covered trips coverage
    uncovered = _native_calls("when_all_then_reduce", remote_locs=(1,))  # all leaves on loc 1
    gu = _gate("when_all_then_reduce", uncovered, _hardened_connectors())
    chk("uncovered remote fails coverage",
        gu["gates"]["both_remote_localities_covered"] is False)

    # a late parcel after shutdown fails closed
    gj = _gate("when_all_then_reduce", _native_calls("when_all_then_reduce"), _hardened_connectors(),
               late=True)
    chk("late parcel fails gate", gj["gates"]["no_late_parcel_after_shutdown"] is False
        and gj["cross_node_composition_validated"] is False)

    # a driver error fails closed even with clean-looking calls
    ge = _gate("when_all_then_reduce", _native_calls("when_all_then_reduce"), _hardened_connectors(),
               error="RuntimeError: only 1 of 2 remote localities joined")
    chk("driver error fails all_calls_completed/no_exception",
        ge["gates"]["all_calls_completed"] is False and ge["gates"]["no_exception"] is False)
    chk("driver error not validated", ge["cross_node_composition_validated"] is False)

    # fences + same_axis locked in the gate
    chk("fences locked in gate", g["gates"]["fences_locked_false"] is True)
    chk("same_axis False in gate", g["gates"]["same_axis_comparison_false"] is True)
    # no forbidden claim keys anywhere in the gate result
    leaked = {k.lower() for k in _keys_deep(g)} & _FORBIDDEN_KEYS
    chk("no forbidden claim keys in gate", not leaked)


def test_native_smoke_artifact_path_no_clobber():
    print("Slice 2a artifact filenames encode mode+serve-timeout+root-port+job (no clobber):")
    p_wa = r._native_smoke_artifact_path("300", "when_all_then_reduce", 90, 7920)
    p_df = r._native_smoke_artifact_path("300", "dataflow_reduce", 90, 7920)
    p_st = r._native_smoke_artifact_path("300", "when_all_then_reduce", 180, 7920)
    p_pt = r._native_smoke_artifact_path("300", "when_all_then_reduce", 90, 7921)
    names = {p_wa, p_df, p_st, p_pt}
    chk("four distinct filenames across mode/timeout/port", len(names) == 4)
    chk("mode in filename", "whenallthenreduce" in p_wa and "dataflowreduce" in p_df)
    chk("serve-timeout in filename", "st90" in p_wa and "st180" in p_st)
    chk("root-port in filename", "p7920" in p_wa and "p7921" in p_pt)
    chk("matches gitignore exp63_*_hpx.json",
        os.path.basename(p_wa).startswith("exp63_") and p_wa.endswith("_hpx.json"))


def test_native_composition_smoke_skips():
    print("native-composition-smoke skips cleanly off-cluster / ext-unbuilt / unsupported mode:")
    s0, a0 = r.run_native_composition_smoke(env={}, write=False)
    chk("skip off-cluster", s0 == "skip" and a0 is None)
    # unsupported mode fails before any cluster work, regardless of environment.
    su, au = r.run_native_composition_smoke(composition_mode="bogus_mode", env={}, write=False)
    chk("unsupported mode -> fail", su == "fail" and au is None)
    # a >=3-node allocation shape but no built ext/connector still skips (never faked).
    on3 = {"SLURM_JOB_ID": "300", "SLURM_JOB_NODELIST": "medusa[00-02]"}
    orig = r._scontrol_hostnames
    try:
        r._scontrol_hostnames = lambda nodelist: ["medusa00", "medusa01", "medusa02"]

        def _boom():
            raise ImportError("collective_ext absent")
        sb, ab = r.run_native_composition_smoke(env=on3, import_fn=_boom, write=False)
        chk("3-node but unbuilt -> skip", sb == "skip" and ab is None)
    finally:
        r._scontrol_hostnames = orig


# --------------------------------------------------------------------------- Slice 2b: depth-2
# star-of-partials fan-in (pure partition + gate + filename + skip).

def _clean_partials(x, n, remote_locs):
    """Clean partials list: contiguous blocks over remote_locs, each partial_sum correct (matches the
    C++ exp63_partial + the driver's partial dicts)."""
    blocks = r._partition_blocks(n, len(remote_locs))
    return [{"partial_sum": r._local_partial_oracle(x, i_begin, i_count),
             "i_begin": i_begin, "i_count": i_count, "locality": remote_locs[j]}
            for j, (i_begin, i_count) in enumerate(blocks)]


def _tree_calls(x, n, remote_locs, k=20, ready=True, partials_override=None,
                composite_override="oracle"):
    calls = []
    for c in range(k):
        partials = partials_override if partials_override is not None \
            else _clean_partials(x, n, remote_locs)
        if composite_override == "oracle":
            composite = r.composite_oracle(x, n) if ready else None
        else:
            composite = composite_override
        calls.append({
            "call_index": c,
            "wait_for_status": "ready" if ready else "timeout",
            "exception_type": "",
            "exception_stage": "none",
            "timed_out_partial_count": 0 if ready else len(remote_locs),
            "composite": composite,
            "composite_oracle_correct": ready and composite_override == "oracle",
            "partials": partials,
            "partial_localities": [p["locality"] for p in partials],
            "n_localities": len(remote_locs),
        })
    return calls


def _tree_gate(partial_collect_wait, calls, connectors, *, x=7, n=8, remote_locs=(1, 2), error=None,
               expected_k=20, late=False, root_loc=0):
    return r.tree_of_partials_validation_gate(
        partial_collect_wait=partial_collect_wait, calls=calls, x=x, n=n, root_locality=root_loc,
        declared_remote_localities=list(remote_locs), connectors=connectors, error=error,
        expected_k=expected_k, late_parcel_after_shutdown_detected=late)


def test_partition_blocks_tiles():
    print("Slice 2b contiguous partition tiles [0,n) exactly once and partials fold to composite:")
    for n, rr in [(8, 2), (8, 3), (7, 2), (10, 4), (5, 5)]:
        blocks = r._partition_blocks(n, rr)
        chk(f"n={n} r={rr}: r blocks", len(blocks) == rr)
        covered = []
        nonempty = True
        for (b, cnt) in blocks:
            nonempty = nonempty and cnt >= 1
            covered.extend(range(b, b + cnt))
        chk(f"n={n} r={rr}: all blocks non-empty", nonempty)
        chk(f"n={n} r={rr}: tiles [0,n) once", sorted(covered) == list(range(n)))
    chk("n<r raises", _raises(lambda: r._partition_blocks(2, 3)))
    chk("r<1 raises", _raises(lambda: r._partition_blocks(8, 0)))
    acc = 0
    for (b, c) in r._partition_blocks(8, 2):
        acc = r.u64(acc + r._local_partial_oracle(7, b, c))
    chk("local partials fold to composite_oracle", r.i64(acc) == r.composite_oracle(7, 8))


def test_tree_of_partials_validation_gate():
    print("Slice 2b tree-of-partials validation gate is honest and fail-closed:")
    # clean, under both validated native collect waits -> validated
    for wait in ("dataflow_reduce", "when_all_then_reduce"):
        g = _tree_gate(wait, _tree_calls(7, 8, [1, 2]), _hardened_connectors())
        chk(f"clean {wait} validated", g["cross_node_composition_validated"] is True)
        chk(f"clean {wait} mechanics_ok", g["mechanics_ok"] is True)
        chk(f"{wait} native / not polled",
            g["hpx_native_composition"] is True and g["polled_in_success_path"] is False)
        chk(f"{wait} all gate booleans True", all(g["gates"].values()))
        chk(f"{wait} root_reduces_partial_count == r", g["root_reduces_partial_count"] == 2)
        chk(f"{wait} topology label",
            g["partial_topology"] == "depth2_star_of_partials_contiguous_blocks")

    # generalize to r=3
    g3 = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2, 3]), _hardened_connectors(count=3),
                    remote_locs=[1, 2, 3])
    chk("r=3 validated", g3["cross_node_composition_validated"] is True
        and g3["root_reduces_partial_count"] == 3)

    # native-vs-poll distinction (tree is native-by-construction; the poll path is the FLAT control)
    chk("tree primitive is native (not polled)",
        r.composition_provenance_for("tree_of_partials", watchdog="w")["polled_in_success_path"]
        is False)
    chk("flat poll control is polled (never native-validated)",
        r.composition_provenance_for("root_flat_gather_poll", watchdog="w")["polled_in_success_path"]
        is True)

    # missing locality: only r-1 partials
    miss = _clean_partials(7, 8, [1, 2])[:1]
    gm = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2], partials_override=miss),
                    _hardened_connectors())
    chk("missing locality fails count + contribution",
        gm["gates"]["partials_count_matches_remotes"] is False
        and gm["gates"]["all_remote_localities_contributed"] is False)
    chk("missing locality not validated", gm["cross_node_composition_validated"] is False)

    # duplicate / overlapping leaf index (loc2 also folds [0,4)); local partials kept correct to
    # ISOLATE the coverage gate
    dup = _clean_partials(7, 8, [1, 2])
    dup[1]["i_begin"], dup[1]["i_count"] = 0, 4
    dup[1]["partial_sum"] = r._local_partial_oracle(7, 0, 4)
    gd = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2], partials_override=dup),
                    _hardened_connectors())
    chk("overlapping index fails coverage-once", gd["gates"]["leaf_indices_covered_once"] is False)
    chk("overlap not validated", gd["cross_node_composition_validated"] is False)

    # uncovered / gap leaf index (index 3 uncovered), local partials kept correct
    gap = _clean_partials(7, 8, [1, 2])
    gap[0]["i_count"] = 3
    gap[0]["partial_sum"] = r._local_partial_oracle(7, 0, 3)
    gg = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2], partials_override=gap),
                    _hardened_connectors())
    chk("gap index fails coverage-once", gg["gates"]["leaf_indices_covered_once"] is False)

    # wrong local partial fails
    badp = _clean_partials(7, 8, [1, 2])
    badp[0]["partial_sum"] = badp[0]["partial_sum"] + 1
    gp = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2], partials_override=badp),
                    _hardened_connectors())
    chk("wrong local partial fails", gp["gates"]["local_partial_oracles_correct"] is False
        and gp["cross_node_composition_validated"] is False)

    # wrong root composite fails
    gc = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2], composite_override=12345),
                    _hardened_connectors())
    chk("wrong composite fails", gc["gates"]["composite_oracle_correct"] is False
        and gc["cross_node_composition_validated"] is False)

    # a partial on the ROOT locality fails placement
    rl = _clean_partials(7, 8, [1, 2])
    rl[0]["locality"] = 0
    grl = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2], partials_override=rl),
                     _hardened_connectors())
    chk("root-local partial fails partials_all_remote", grl["gates"]["partials_all_remote"] is False
        and grl["cross_node_composition_validated"] is False)

    # serve_timeout_expired connector cannot validate
    gt = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2]),
                    _hardened_connectors("serve_timeout_expired"))
    chk("serve_timeout_expired fails root-completion gate",
        gt["gates"]["connector_shutdown_root_completion"] is False
        and gt["cross_node_composition_validated"] is False)

    # stall (timeout, composite None, <K calls) fails closed
    gs = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2], k=1, ready=False),
                    _hardened_connectors())
    chk("stall not validated", gs["cross_node_composition_validated"] is False)
    chk("stall wait/timeout/all-calls gates fail",
        gs["gates"]["wait_for_status_ready"] is False and gs["gates"]["no_dispatch_timeout"] is False
        and gs["gates"]["all_calls_completed"] is False)

    # late parcel fails closed
    gl = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2]), _hardened_connectors(), late=True)
    chk("late parcel fails gate", gl["gates"]["no_late_parcel_after_shutdown"] is False
        and gl["cross_node_composition_validated"] is False)

    # driver error fails closed
    ge = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2]), _hardened_connectors(),
                    error="RuntimeError: only 1 of 2 remote localities joined")
    chk("driver error fails all_calls_completed", ge["gates"]["all_calls_completed"] is False
        and ge["cross_node_composition_validated"] is False)

    # fences + same_axis + no forbidden keys
    g = _tree_gate("dataflow_reduce", _tree_calls(7, 8, [1, 2]), _hardened_connectors())
    chk("fences locked in gate", g["gates"]["fences_locked_false"] is True)
    chk("same_axis False in gate", g["gates"]["same_axis_comparison_false"] is True)
    leaked = {k.lower() for k in _keys_deep(g)} & _FORBIDDEN_KEYS
    chk("no forbidden claim keys in tree gate", not leaked)


def test_tree_smoke_artifact_path_no_clobber():
    print("Slice 2b artifact filenames encode wait+serve-timeout+port+job (no clobber):")
    p_df = r._tree_smoke_artifact_path("300", "dataflow_reduce", 90, 7930)
    p_wa = r._tree_smoke_artifact_path("300", "when_all_then_reduce", 90, 7930)
    p_st = r._tree_smoke_artifact_path("300", "dataflow_reduce", 180, 7930)
    p_pt = r._tree_smoke_artifact_path("300", "dataflow_reduce", 90, 7932)
    names = {p_df, p_wa, p_st, p_pt}
    chk("four distinct filenames across wait/timeout/port", len(names) == 4)
    chk("wait in filename", "dataflowreduce" in p_df and "whenallthenreduce" in p_wa)
    chk("serve-timeout in filename", "st90" in p_df and "st180" in p_st)
    chk("port in filename", "p7930" in p_df and "p7932" in p_pt)
    chk("treesmoke tag + gitignore match",
        os.path.basename(p_df).startswith("exp63_treesmoke_") and p_df.endswith("_hpx.json"))


def test_tree_of_partials_smoke_skips():
    print("tree-of-partials-remote-smoke skips cleanly off-cluster / ext-unbuilt / unsupported wait:")
    for w in ("dataflow_reduce", "when_all_then_reduce"):
        s, a = r.run_tree_of_partials_remote_smoke(partial_collect_wait=w, env={}, write=False)
        chk(f"skip off-cluster ({w})", s == "skip" and a is None)
    su, au = r.run_tree_of_partials_remote_smoke(partial_collect_wait="bogus", env={}, write=False)
    chk("unsupported collect wait -> fail", su == "fail" and au is None)
    on3 = {"SLURM_JOB_ID": "300", "SLURM_JOB_NODELIST": "medusa[00-02]"}
    orig = r._scontrol_hostnames
    try:
        r._scontrol_hostnames = lambda nodelist: ["medusa00", "medusa01", "medusa02"]

        def _boom():
            raise ImportError("collective_ext absent")
        sb, ab = r.run_tree_of_partials_remote_smoke(env=on3, import_fn=_boom, write=False)
        chk("3-node but unbuilt -> skip", sb == "skip" and ab is None)
    finally:
        r._scontrol_hostnames = orig


def main():
    tests = [
        test_scalar_oracle, test_composite_oracle_n8, test_vector_oracle_stub, test_topk_oracle_stub,
        test_composition_provenance_per_mode, test_composition_provenance_fail_closed,
        test_collective_provenance, test_progress_diagnosis, test_payload_provenance_flags,
        test_distribution_witness, test_fences_locked, test_no_same_axis_in_slice0,
        test_no_forbidden_claim_fields, test_off_cluster_skip, test_future_phases_skip_cleanly,
        test_progress_matrix_phase_a, test_progress_matrix_phase_b,
        test_poll_control_not_passive_progress, test_native_woke_passes_passive_progress,
        test_stalled_native_fails_closed, test_stall_short_circuit, test_aggregate_picks_winner,
        test_aggregate_all_native_stall, test_progress_fences_and_same_axis,
        test_progress_no_forbidden_keys, test_progress_diagnosis_still_skips,
        test_config_to_launch_args, test_partials_from_records, test_ext_result_shape_to_artifact,
        test_provenance_unknown_and_stall_fail_closed, test_progress_phases_skip_when_unbuilt,
        test_a5_dataflow_native_stalled, test_wait_for_status_mapping, test_exception_stage_mapping,
        test_unsafe_timeout_abandonment_fails_closed, test_diagnostic_variants_and_fences,
        test_a1_root_hpx_args_single_bind, test_connector_srun_decoupled_from_root_cpuset,
        test_diagnostic_cpuset_provenance_fields, test_proc_status_parsers,
        test_count_open_fds_fail_closed, test_cgroup_pids_parser, test_resource_snapshot_fail_closed,
        test_resource_trend_growth_and_near_limit, test_resource_trend_no_overclaim_on_unknown,
        test_diagnostic_errno_and_resource_fields, test_a0_a1_diag_mode_routing_and_diagnostic_fields,
        test_connector_shutdown_classification,
        test_native_composition_validation_gate, test_native_smoke_artifact_path_no_clobber,
        test_native_composition_smoke_skips,
        test_partition_blocks_tiles, test_tree_of_partials_validation_gate,
        test_tree_smoke_artifact_path_no_clobber, test_tree_of_partials_smoke_skips,
    ]
    for t in tests:
        t()
    print()
    if _FAILS:
        print(f"FAILED ({len(_FAILS)}): {', '.join(_FAILS)}")
        return 1
    print(f"all exp63 Slice 0+1a+1b selftests passed ({len(tests)} groups)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
