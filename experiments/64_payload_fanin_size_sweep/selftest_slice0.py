#!/usr/bin/env python3
# exp64 Slice 0 selftest -- pure oracle + corrected-design checks. No HPX, no Ray, no cluster.
# Runs anywhere. Exit 0 iff every group passes.

import sys

import run_exp64_payload as R


class _Checker:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    def ok(self, cond, label):
        mark = "ok  " if cond else "FAIL"
        print(f"  [{mark}] {label}")
        if cond:
            self.passed += 1
        else:
            self.failed += 1


def _check_scalar_oracle(c):
    # leaf_value matches the closed formula and composite is the order-independent sum.
    x, n = 7, 8
    c.ok(R.leaf_value(x, 0) == R._to_int64((x ^ R.LEAF_XOR) + 0), "leaf_value(x,0) closed form")
    c.ok(R.leaf_value(x, 3) == R._to_int64((x ^ R.LEAF_XOR) + 6), "leaf_value(x,3) closed form")
    ref = sum(((x ^ R.LEAF_XOR) + (i << 1)) for i in range(n)) & R.MASK64
    c.ok(R.composite_oracle(x, n) == R._to_int64(ref), "composite_oracle == int64 sum of leaves")
    # order independence: permuting leaves cannot change the sum
    fwd = R.composite_oracle(x, n)
    perm = 0
    for i in reversed(range(n)):
        perm = (perm + ((x ^ R.LEAF_XOR) + (i << 1))) & R.MASK64
    c.ok(R._to_int64(perm) == fwd, "composite_oracle order-independent")


def _check_payload_byte_oracle(c):
    x = 7
    for i in (0, 1, 5):
        base = R.leaf_value(x, i) & R.MASK64
        c.ok(R.payload_byte(x, i, 0) == (base & 0xFF), f"payload_byte(x,{i},0) == low byte of leaf")
        c.ok(0 <= R.payload_byte(x, i, 200) <= 255, f"payload_byte(x,{i},200) in [0,255]")
    # sawtooth period 256: byte(k) and byte(k+256) match
    c.ok(R.payload_byte(x, 2, 5) == R.payload_byte(x, 2, 5 + 256), "payload_byte period 256")
    # payload_bytes returns exactly S bytes matching payload_byte
    s = 300
    buf = R.payload_bytes(x, 4, s)
    c.ok(len(buf) == s, "payload_bytes length == S")
    c.ok(all(buf[k] == R.payload_byte(x, 4, k) for k in range(s)), "payload_bytes match payload_byte")


def _check_payload_digest_oracle(c):
    x, n = 7, 8
    # fast digest equals naive reference across the full default ladder (validates the fast path)
    for s in R.DEFAULT_SIZE_LADDER:
        fast = R.payload_digest(x, n, s)
        naive = R._payload_digest_naive(x, n, s)
        c.ok(fast == naive, f"payload_digest fast==naive at S={s}")
    # digest equals the sum of folding the actually-returned bytes (the boundary-fold Python will do)
    s = 1024
    folded = 0
    for i in range(n):
        buf = R.payload_bytes(x, i, s)
        folded = (folded + sum(buf)) & R.MASK64
    c.ok(R._to_int64(folded) == R.payload_digest(x, n, s), "digest == fold of returned bytes")


def _check_s0_degeneracy(c):
    x, n = 7, 8
    c.ok(R.payload_digest(x, n, 0) == 0, "S=0 payload_digest is 0 (no payload)")
    c.ok(R.payload_bytes(x, 0, 0) == b"", "S=0 payload_bytes is empty")
    # S=0 leaves the scalar oracle intact -- the floor still carries scalar + witnesses
    c.ok(R.composite_oracle(x, n) != 0 or n == 0, "S=0 keeps scalar oracle meaningful")


def _check_ladder_defaults(c):
    c.ok(R.DEFAULT_SIZE_LADDER == [0, 64, 1024, 16384, 262144], "default ladder is the agreed set")
    c.ok(R.DEFAULT_SIZE_LADDER[0] == 0, "ladder starts at the S=0 floor")
    c.ok(set(R.SIZE_LADDER_INTERPRETATION) == set(R.DEFAULT_SIZE_LADDER),
         "every ladder size has an interpretation")


def _check_provenance_labels(c):
    rec = R.build_provenance(x=7, n=8, sizes=R.DEFAULT_SIZE_LADDER, phase="selftest")
    ok, problems = R.validate_provenance(rec)
    c.ok(ok, f"provenance validates (problems={problems})")
    c.ok(rec["poll_mode_baseline"] is True, "provenance records poll-mode baseline")
    c.ok(rec["hpx_composition_mode"] == "root_flat_gather_poll", "provenance records gather baseline")
    c.ok(rec["hpx_composition_kind"] == "naive_all_to_root_gather_baseline",
         "provenance records naive all-to-root gather, not the answer")
    c.ok(rec["hpx_idiomatic_native_composition"] is False
         and rec["hpx_collective_or_tree_reduction"] is False,
         "provenance records NOT native/idiomatic, NOT collective/tree")
    c.ok(rec["timed_call_returns_payload_bytes_to_python"] is True,
         "provenance records payload crosses the Python boundary")
    c.ok(rec["digest_folded_inside_runtime"] is False
         and rec["digest_check_after_timing_outside_rtt"] is True
         and rec["fold_location_identical_across_arms"] is True,
         "provenance records post-timing, out-of-RTT, identical-fold digest check")
    c.ok(rec["payload_repr_cpp"] == "hpx::serialization::serialize_buffer<char>",
         "provenance records serialize_buffer payload vehicle")
    c.ok(rec["payload_synthetic"] is True and rec["payload_is_model_output"] is False
         and rec["real_inference"] is False, "provenance records synthetic payload, no inference")
    c.ok(rec["ray_transport_regime_may_change_with_size"] is True
         and rec["infer_hpx_cause_from_kink_requires_ray_transport_metadata"] is True,
         "provenance records the Ray transport caveat")


def _check_fences_and_forbidden_keys(c):
    rec = R.build_provenance(x=7, n=8, sizes=R.DEFAULT_SIZE_LADDER, phase="selftest")
    for k in R.FENCE_KEYS_FALSE:
        c.ok(rec.get(k) is False, f"fence {k} locked False")
    c.ok(rec["same_axis_comparison"] is False, "same_axis_comparison locked False (Slice 0)")
    # no forbidden speedup/superiority key present anywhere in the record
    bad = [k for k in rec for sub in R.FORBIDDEN_KEY_SUBSTRINGS if sub in k.lower()]
    c.ok(not bad, f"no forbidden claim keys present (found={bad})")
    # a deliberately poisoned record must fail validation (the gate actually bites)
    poisoned = dict(rec)
    poisoned["speedup_computed"] = True
    ok, _ = R.validate_provenance(poisoned)
    c.ok(not ok, "validate_provenance rejects a poisoned fence")


def _synthetic_payload_result(x, n, s, remote_locs):
    """Build a result dict shaped like payload_ext.fanout_fanin_payload_remote would return, with
    Python-generated payload bytes and all leaves on remote localities (round-robin)."""
    leaves = []
    acc = 0
    for i in range(n):
        loc = remote_locs[i % len(remote_locs)]
        pay = R.payload_bytes(x, i, s)
        leaves.append({"i": i, "value": R.leaf_value(x, i), "locality": loc,
                       "payload": pay, "payload_len": s})
        acc = (acc + R.leaf_value(x, i)) & R.MASK64
    return {"composite": R._to_int64(acc), "n": n, "payload_bytes": s,
            "n_localities": len(remote_locs), "timed_out_leaf_count": 0, "leaves": leaves}


def _check_payload_gates_pure(c):
    x, n, root_loc, remote_locs = 7, 8, 0, [1, 2]
    for s in (0, 1024, 262144):
        res = _synthetic_payload_result(x, n, s, remote_locs)
        folded = R.fold_payload_digest(res["leaves"])
        c.ok(folded == R.payload_digest(x, n, s), f"fold == payload_digest at S={s}")
        gates = R.compute_payload_gates(x=x, n=n, payload_bytes=s, root_loc=root_loc,
                                        remote_locs=remote_locs, result=res, folded_digest=folded)
        c.ok(all(gates.values()), f"all gates pass on a clean S={s} result ({gates})")
    # a leaf on the ROOT locality must trip leaves_local_zero / leaves_remote_all
    res = _synthetic_payload_result(x, n, 64, remote_locs)
    res["leaves"][0]["locality"] = root_loc
    g = R.compute_payload_gates(x=x, n=n, payload_bytes=64, root_loc=root_loc,
                                remote_locs=remote_locs, result=res,
                                folded_digest=R.fold_payload_digest(res["leaves"]))
    c.ok(not g["leaves_local_zero"] and not g["leaves_remote_all"], "root-local leaf fails placement gate")
    # a corrupted payload byte must trip payload_digest_correct
    res2 = _synthetic_payload_result(x, n, 64, remote_locs)
    bad = bytearray(res2["leaves"][0]["payload"])
    bad[0] = (bad[0] + 1) & 0xFF
    res2["leaves"][0]["payload"] = bytes(bad)
    g2 = R.compute_payload_gates(x=x, n=n, payload_bytes=64, root_loc=root_loc,
                                 remote_locs=remote_locs, result=res2,
                                 folded_digest=R.fold_payload_digest(res2["leaves"]))
    c.ok(not g2["payload_digest_correct"], "corrupted payload byte fails digest gate")
    # only one remote locality covered must trip every_remote_locality_covered
    res3 = _synthetic_payload_result(x, n, 0, [1])  # all leaves land on loc 1
    g3 = R.compute_payload_gates(x=x, n=n, payload_bytes=0, root_loc=root_loc,
                                 remote_locs=[1, 2], result=res3,
                                 folded_digest=R.fold_payload_digest(res3["leaves"]))
    c.ok(not g3["every_remote_locality_covered"], "uncovered remote locality fails coverage gate")


def _synthetic_ray_records(x, n, s, remote_nids):
    """Records shaped like the Ray coordinator returns: {i, value, payload bytes, node_id}, all leaves
    round-robin across the remote worker node ids."""
    return [{"i": i, "value": R.leaf_value(x, i), "payload": R.payload_bytes(x, i, s),
             "node_id": remote_nids[i % len(remote_nids)]} for i in range(n)]


def _check_ray_payload_gates_pure(c):
    x, n, head, remote = 7, 8, "HEAD", ["W1", "W2"]
    composite = R._to_int64(sum(R.leaf_value(x, i) for i in range(n)) & R.MASK64)
    for s in (0, 262144):
        recs = _synthetic_ray_records(x, n, s, remote)
        folded = R.fold_payload_digest(recs)
        c.ok(folded == R.payload_digest(x, n, s), f"ray fold == payload_digest at S={s}")
        g = R.compute_ray_payload_gates(
            x=x, n=n, payload_bytes=s, remote_node_ids=remote, coordinator_node_id=head,
            driver_node_id=head, head_num_cpus=0, coordinator_num_cpus=0, hard_placement=True,
            records=recs, measured_composite=composite, folded_digest=folded, no_dispatch_timeout=True)
        c.ok(all(g.values()), f"all ray gates pass on a clean S={s} result ({g})")
    # a leaf landing on the HEAD/coordinator node trips leaves_local_zero / remote_all / zero-leaves
    recs = _synthetic_ray_records(x, n, 64, remote)
    recs[0]["node_id"] = head
    g = R.compute_ray_payload_gates(
        x=x, n=n, payload_bytes=64, remote_node_ids=remote, coordinator_node_id=head,
        driver_node_id=head, head_num_cpus=0, coordinator_num_cpus=0, hard_placement=True,
        records=recs, measured_composite=composite, folded_digest=R.fold_payload_digest(recs),
        no_dispatch_timeout=True)
    c.ok(not g["leaves_local_zero"] and not g["leaves_remote_all"]
         and not g["coordinator_runs_zero_leaves"], "leaf on head fails placement + zero-leaves gates")
    # corrupted payload byte trips payload_digest_correct
    recs = _synthetic_ray_records(x, n, 64, remote)
    bad = bytearray(recs[0]["payload"])
    bad[0] = (bad[0] + 1) & 0xFF
    recs[0]["payload"] = bytes(bad)
    g = R.compute_ray_payload_gates(
        x=x, n=n, payload_bytes=64, remote_node_ids=remote, coordinator_node_id=head,
        driver_node_id=head, head_num_cpus=0, coordinator_num_cpus=0, hard_placement=True,
        records=recs, measured_composite=composite, folded_digest=R.fold_payload_digest(recs),
        no_dispatch_timeout=True)
    c.ok(not g["payload_digest_correct"], "corrupted payload byte fails ray digest gate")
    # only one remote node covered trips coverage + balance
    recs = _synthetic_ray_records(x, n, 0, ["W1"])  # all leaves on W1
    g = R.compute_ray_payload_gates(
        x=x, n=n, payload_bytes=0, remote_node_ids=["W1", "W2"], coordinator_node_id=head,
        driver_node_id=head, head_num_cpus=0, coordinator_num_cpus=0, hard_placement=True,
        records=recs, measured_composite=composite, folded_digest=R.fold_payload_digest(recs),
        no_dispatch_timeout=True)
    c.ok(not g["every_remote_node_covered"] and not g["leaves_per_remote_balanced"],
         "uncovered remote node fails coverage + balance gates")
    # non-zero head/coordinator cpus, a coordinator off the head node, a dispatch timeout each fail closed
    recs = _synthetic_ray_records(x, n, 64, remote)
    folded = R.fold_payload_digest(recs)
    gh = R.compute_ray_payload_gates(
        x=x, n=n, payload_bytes=64, remote_node_ids=remote, coordinator_node_id=head,
        driver_node_id=head, head_num_cpus=8, coordinator_num_cpus=8, hard_placement=True,
        records=recs, measured_composite=composite, folded_digest=folded, no_dispatch_timeout=True)
    c.ok(not gh["ray_head_num_cpus_zero"] and not gh["coordinator_num_cpus_zero"],
         "non-zero head/coordinator cpus fail closed")
    gd = R.compute_ray_payload_gates(
        x=x, n=n, payload_bytes=64, remote_node_ids=remote, coordinator_node_id="W1",
        driver_node_id=head, head_num_cpus=0, coordinator_num_cpus=0, hard_placement=True,
        records=recs, measured_composite=composite, folded_digest=folded, no_dispatch_timeout=True)
    c.ok(not gd["coordinator_on_head_node"], "coordinator off the head node fails closed")
    gt = R.compute_ray_payload_gates(
        x=x, n=n, payload_bytes=64, remote_node_ids=remote, coordinator_node_id=head,
        driver_node_id=head, head_num_cpus=0, coordinator_num_cpus=0, hard_placement=True,
        records=recs, measured_composite=composite, folded_digest=folded, no_dispatch_timeout=False)
    c.ok(not gt["no_dispatch_timeout"], "dispatch timeout fails closed")


def _check_offcluster_phases_skip(c):
    for name in ("hpx-payload-remote-smoke", "hpx-payload-native-smoke", "ray-payload-remote-smoke",
                 "payload-ladder-manifest", "payload-band-aggregate", "smoke", "remote-smoke",
                 "size-sweep"):
        rc = R.main(["--phase", name])
        c.ok(rc == 0, f"phase {name} skips cleanly off-cluster (rc={rc})")
    # manifest phase with a job that has no artifacts on disk also skips cleanly (rc=0)
    rc = R.main(["--phase", "payload-ladder-manifest", "--job", "nonexistent_job_zzz"])
    c.ok(rc == 0, f"payload-ladder-manifest --job <absent> skips cleanly (rc={rc})")
    # band aggregate with a band-id that has no island manifests also skips cleanly (rc=0)
    rc = R.main(["--phase", "payload-band-aggregate", "--band-id", "nonexistent_band_zzz"])
    c.ok(rc == 0, f"payload-band-aggregate --band-id <absent> skips cleanly (rc={rc})")
    # A3: idle-backoff-off (--native-idle-backoff-ms 0) + the sequential_leaf_wait positive control
    # still skips cleanly off-cluster -- the new lever/mode must not crash the pre-cluster path
    rc = R.main(["--phase", "hpx-payload-native-smoke", "--native-idle-backoff-ms", "0",
                 "--native-modes", "sequential_leaf_wait", "--smoke-sizes", "0"])
    c.ok(rc == 0,
         f"native-smoke --native-idle-backoff-ms 0 + sequential_leaf_wait skips cleanly (rc={rc})")


# ---------------------------------------------------------------------------
# Slice 5 Phase A native-readiness checks (pure; synthetic ext results). Native modes must not poll on
# the success path and must reach wait_for_status=ready; the flat gather is ALWAYS retained; every
# corruption fails the readiness gate closed. HPX-only; no Ray, no cross-arm anything.
# ---------------------------------------------------------------------------

def _synthetic_native_result(x, n, s, remote_locs, mode):
    """Shape what payload_ext.fanout_fanin_payload_remote returns in Slice 5 Phase A (poll or native):
    the Slice-1 payload result PLUS the readiness/topology fields the ext now emits."""
    is_native = mode in R.NATIVE_READINESS_MODES
    base = _synthetic_payload_result(x, n, s, remote_locs)
    base.update({
        "readiness_composition": mode,
        "success_path_polling_used": (not is_native),
        "wait_for_status": "ready",
        "root_flat_gather_retained": True,
        "payload_data_movement_topology": "root_flat_gather",
        "payload_bytes_cross_python_boundary": True,
        "python_bytes_direct_copy": True,
    })
    return base


def _check_native_readiness_gates_pure(c):
    x, n, remote = 7, 8, [1, 2]
    # poll control: success-path poll USED; all readiness gates pass
    res = _synthetic_native_result(x, n, 64, remote, "root_flat_gather_poll")
    g = R.compute_native_readiness_gates(mode="root_flat_gather_poll", result=res)
    c.ok(all(g.values()), f"poll control readiness-gates pass ({g})")
    c.ok(res["success_path_polling_used"] is True, "poll control: success_path_polling_used True")
    # native modes: success-path poll NOT used; all readiness gates pass
    for mode in ("when_all_then_reduce", "dataflow_reduce"):
        res = _synthetic_native_result(x, n, 262144, remote, mode)
        g = R.compute_native_readiness_gates(mode=mode, result=res)
        c.ok(all(g.values()), f"{mode} readiness-gates pass ({g})")
        c.ok(res["success_path_polling_used"] is False,
             f"{mode}: success_path_polling_used False")
    # a native mode that still polled on the success path fails the match gate CLOSED
    res = _synthetic_native_result(x, n, 0, remote, "when_all_then_reduce")
    res["success_path_polling_used"] = True  # tamper: native claims it polled
    g = R.compute_native_readiness_gates(mode="when_all_then_reduce", result=res)
    c.ok(not g["success_path_polling_used_matches_mode"], "native+polling fails match gate closed")
    # wait_for_status != ready fails the readiness gate CLOSED
    res = _synthetic_native_result(x, n, 0, remote, "dataflow_reduce")
    res["wait_for_status"] = "timeout"
    g = R.compute_native_readiness_gates(mode="dataflow_reduce", result=res)
    c.ok(not g["wait_for_status_ready"], "wait_for_status timeout fails readiness gate closed")
    # flat gather must be RETAINED; a False trips the invariant gate CLOSED
    res = _synthetic_native_result(x, n, 0, remote, "when_all_then_reduce")
    res["root_flat_gather_retained"] = False  # tamper
    g = R.compute_native_readiness_gates(mode="when_all_then_reduce", result=res)
    c.ok(not g["root_flat_gather_retained"], "flat-gather-not-retained fails closed")
    # a wrong topology label also fails closed
    res = _synthetic_native_result(x, n, 0, remote, "dataflow_reduce")
    res["payload_data_movement_topology"] = "tree_of_partials"  # tamper
    g = R.compute_native_readiness_gates(mode="dataflow_reduce", result=res)
    c.ok(not g["payload_topology_root_flat_gather"], "non-root-flat topology fails closed")
    # yield DIAGNOSTIC: it POLLS the composed future, so success_path_polling_used=True is EXPECTED and
    # the readiness gates still pass (diagnostic is not a native CLAIM mode)
    resd = _synthetic_native_result(x, n, 0, remote, "when_all_then_reduce_yield")
    c.ok(resd["success_path_polling_used"] is True, "yield diagnostic result reports polling used")
    gd = R.compute_native_readiness_gates(mode="when_all_then_reduce_yield", result=resd)
    c.ok(all(gd.values()), f"yield diagnostic readiness-gates pass ({gd})")


def _synth_calls_rtt(rtt_ns_list):
    """Synthetic per-call records carrying only the rtt fields the deadline-margin gate reads."""
    return [{"call_index": i, "rtt_ns": int(r), "rtt_ms": int(r) / 1e6}
            for i, r in enumerate(rtt_ns_list)]


def _check_native_deadline_margin_gate(c):
    dt = 8.0
    # job-159418 shape: native mode, every RTT pinned near the 8 s dispatch timeout -> gate FAILS CLOSED
    stalled = _synth_calls_rtt([8_000_600_000] * 5)  # ~8.0006 s each
    g = R.compute_native_deadline_margin_gate(mode="when_all_then_reduce", calls=stalled,
                                              dispatch_timeout_s=dt)
    c.ok(g["native_wait_deadline_margin_gate"] is False
         and g["native_wait_completed_before_timeout_deadline"] is False,
         "native mode pinned at dispatch_timeout fails the deadline-margin gate (job-159418 shape)")
    c.ok(abs(g["native_wait_deadline_margin_threshold_s"] - 4.0) < 1e-9,
         "deadline-margin threshold is 0.5 * dispatch_timeout_s")
    c.ok(g["native_wait_observed_max_rtt_s"] > 4.0, "observed max RTT recorded above the threshold")
    # a PROMPT native mode (sub-ms) passes the margin gate
    prompt = _synth_calls_rtt([300_000, 350_000, 320_000])  # ~0.3 ms
    g2 = R.compute_native_deadline_margin_gate(mode="dataflow_reduce", calls=prompt,
                                               dispatch_timeout_s=dt)
    c.ok(g2["native_wait_deadline_margin_gate"] is True,
         "prompt native mode passes the deadline-margin gate")
    # the poll CONTROL is EXEMPT: even a large RTT does not trip the native margin gate
    g3 = R.compute_native_deadline_margin_gate(mode="root_flat_gather_poll", calls=stalled,
                                               dispatch_timeout_s=dt)
    c.ok(g3["native_wait_deadline_margin_applicable"] is False
         and g3["native_wait_deadline_margin_gate"] is True,
         "poll control is exempt from the native deadline-margin gate")
    # the yield DIAGNOSTIC is subject to the gate (its whole point is prompt completion)
    g4 = R.compute_native_deadline_margin_gate(mode="when_all_then_reduce_yield", calls=stalled,
                                               dispatch_timeout_s=dt)
    c.ok(g4["native_wait_deadline_margin_applicable"] is True
         and g4["native_wait_deadline_margin_gate"] is False,
         "yield diagnostic stalled to the deadline fails the margin gate")


def _check_job159418_false_positive_now_fails(c):
    x, n, remote = 7, 8, [1, 2]
    # per-call readiness gates PASS for a native ready result (wait_for_status=ready, correct labels)
    res = _synthetic_native_result(x, n, 0, remote, "when_all_then_reduce")
    rg = R.compute_native_readiness_gates(mode="when_all_then_reduce", result=res)
    # ...but the measured calls were pinned at the dispatch timeout -> deadline-margin gate FAILS
    stalled = _synth_calls_rtt([8_000_600_000] * 5)
    mg = R.compute_native_deadline_margin_gate(mode="when_all_then_reduce", calls=stalled,
                                               dispatch_timeout_s=8.0)
    would_pass = all(rg.values()) and mg["native_wait_deadline_margin_gate"]
    c.ok(all(rg.values()) and not mg["native_wait_deadline_margin_gate"] and not would_pass,
         "job-159418 shape: readiness gates pass but the deadline-margin gate fails -> NOT a pass")


def _check_phase_a_labels(c):
    # mode taxonomy: poll is the control (not native); the two native modes are the candidates
    c.ok("root_flat_gather_poll" not in R.NATIVE_READINESS_MODES,
         "root_flat_gather_poll is the control, NOT a native mode")
    for m in R.NATIVE_READINESS_MODES:
        c.ok(m in R.DEFAULT_NATIVE_PHASE_MODES, f"native mode {m} is in the default Phase A modes")
    c.ok(R.DEFAULT_NATIVE_PHASE_MODES[0] == "root_flat_gather_poll",
         "poll control runs first in the Phase A mode list")
    # native provenance addresses the POLL half only; gather half stays retained
    pn = R._readiness_provenance("when_all_then_reduce")
    c.ok(pn["poll_half_addressed"] is True and pn["gather_half_addressed"] is False,
         "native readiness provenance: poll half addressed, gather half NOT")
    c.ok(pn["root_flat_gather_retained"] is True
         and pn["payload_data_movement_topology"] == "root_flat_gather"
         and pn["hpx_native_readiness_composition"] is True,
         "native readiness provenance retains the root-flat gather topology")
    pp = R._readiness_provenance("root_flat_gather_poll")
    c.ok(pp["poll_half_addressed"] is False and pp["success_path_polling_used"] is True
         and pp["hpx_native_readiness_composition"] is False,
         "poll control provenance: poll used, poll half NOT addressed")
    # blocker/claim-scope labels are honest until Phase C exists
    c.ok(R.NATIVE_CLAIM_SCOPE_PHASE_A == "multithread_unchecked",
         "native_claim_scope stays multithread_unchecked until Phase C (threads=1) runs")
    c.ok(R.CONNECTOR_LIFETIME_MODE == "root_completion_or_heartbeat_deadman",
         "Phase A uses the hardened connector lifetime mode")
    # diagnostic taxonomy: the yield variant is diagnostic-only, never a native claim mode
    c.ok("when_all_then_reduce_yield" in R.DIAGNOSTIC_READINESS_MODES
         and "when_all_then_reduce_yield" not in R.NATIVE_READINESS_MODES,
         "yield variant is diagnostic-only, not a native claim mode")
    pd = R._readiness_provenance("when_all_then_reduce_yield")
    c.ok(pd["diagnostic_only"] is True and pd["poll_half_addressed"] is False
         and pd["success_path_polling_used"] is True and pd["native_wait_variant"] == "yield_poll",
         "yield diagnostic provenance: diagnostic-only, polls, never retires the poll half")
    c.ok(R._native_wait_variant("when_all_then_reduce") == "wait_for"
         and R._native_wait_variant("root_flat_gather_poll") == "n/a"
         and R._native_wait_variant("when_all_then_reduce_yield") == "yield_poll",
         "native_wait_variant labels correct per mode")
    # the readiness provenance carries no forbidden claim keys
    c.ok(not R._scan_forbidden_keys(pn, []) and not R._scan_forbidden_keys(pp, [])
         and not R._scan_forbidden_keys(pd, []),
         "readiness provenance carries no forbidden claim keys")


# ---------------------------------------------------------------------------
# Slice 5 Phase A3: scheduler idle-backoff disclosure, the tight promptness sanity gate, and the pure
# poll-half retirement invariant. idle-backoff-off is a CPU-for-latency runtime-spin workaround on the
# TCP parcelport, NOT an event-driven wakeup; these checks keep that honest and keep the poll half from
# being retired by a stall, a slow-but-not-timeout wake, an undisclosed driver, or a non-claim mode.
# ---------------------------------------------------------------------------

def _check_idle_backoff_disclosure(c):
    # observed "0" -> disabled: runtime spins, progress driver recorded, QD1 not affected, confound cleared
    d0 = R.compute_idle_backoff_disclosure({"hpx.max_idle_backoff_time": "0",
                                            "hpx.max_idle_loop_count": "1000"})
    c.ok(d0["idle_backoff_mode"] == "disabled", "idle-backoff observed 0 -> disabled")
    c.ok(d0["runtime_spins_when_idle"] is True
         and d0["runtime_progress_driver"] == "idle_backoff_disabled"
         and d0["scheduler_idle_backoff_may_affect_qd1"] is False
         and d0["bg_thread_result_confounded_by_idle_backoff"] is False,
         "disabled: spins, progress driver recorded, QD1 unaffected, bg-thread confound cleared")
    # nonzero recorded value (exp58-style) -> recorded_only, NOT disabled
    d50 = R.compute_idle_backoff_disclosure({"hpx.max_idle_backoff_time": "50"})
    c.ok(d50["idle_backoff_mode"] == "recorded_only", "idle-backoff observed 50 -> recorded_only")
    c.ok(d50["runtime_spins_when_idle"] is False
         and d50["runtime_progress_driver"] == "none"
         and d50["scheduler_idle_backoff_may_affect_qd1"] is True
         and d50["bg_thread_result_confounded_by_idle_backoff"] is True,
         "recorded_only: no spin, no progress driver, may affect QD1, confound present")
    # absent key -> unknown; None cfg -> unknown (defensive)
    du = R.compute_idle_backoff_disclosure({})
    dn = R.compute_idle_backoff_disclosure(None)
    c.ok(du["idle_backoff_mode"] == "unknown" and dn["idle_backoff_mode"] == "unknown",
         "absent key / None cfg -> unknown")
    c.ok(du["runtime_progress_driver"] == "none"
         and du["bg_thread_result_confounded_by_idle_backoff"] is True,
         "unknown: no progress driver recorded, confound not cleared")
    # the confound note discloses the runtime-spin honestly (not event-driven) vs confounded attribution
    c.ok("not event-driven" in d0["idle_backoff_confound_note"]
         and "confounded" in d50["idle_backoff_confound_note"],
         "confound note discloses runtime-spin honestly, not as an event-driven wakeup")


def _check_native_promptness_gate(c):
    thr = R.NATIVE_PROMPTNESS_THRESHOLD_S
    c.ok(abs(thr - 0.01) < 1e-12, "promptness threshold is 0.01 s")
    # a native mode stalled near the 8 s deadline: max RTT >> threshold -> promptness FAILS closed
    stalled = _synth_calls_rtt([8_000_600_000] * 5)
    g = R.compute_native_promptness_gate(mode="when_all_then_reduce", calls=stalled)
    c.ok(g["native_wait_promptness_applicable"] is True
         and g["native_wait_promptness_sanity_gate"] is False,
         "native mode stalled to the deadline fails the promptness gate")
    # ~0.5 ms wake (A2 yield/poll floor) passes -- well under 0.01 s
    prompt = _synth_calls_rtt([400_000, 520_000, 480_000])
    g2 = R.compute_native_promptness_gate(mode="dataflow_reduce", calls=prompt)
    c.ok(g2["native_wait_promptness_sanity_gate"] is True
         and g2["native_wait_promptness_observed_max_rtt_s"] < thr,
         "prompt native mode (~0.5 ms) passes the promptness gate")
    # the exact A3 gap: a 100 ms wake PASSES the coarse deadline-margin gate but FAILS the tight promptness
    slow = _synth_calls_rtt([100_000_000] * 3)
    gm = R.compute_native_deadline_margin_gate(mode="when_all_then_reduce", calls=slow,
                                               dispatch_timeout_s=8.0)
    gp = R.compute_native_promptness_gate(mode="when_all_then_reduce", calls=slow)
    c.ok(gm["native_wait_deadline_margin_gate"] is True
         and gp["native_wait_promptness_sanity_gate"] is False,
         "100 ms wake passes the coarse margin gate but fails the tight promptness gate")
    # poll control is EXEMPT even for a stalled RTT
    gpoll = R.compute_native_promptness_gate(mode="root_flat_gather_poll", calls=stalled)
    c.ok(gpoll["native_wait_promptness_applicable"] is False
         and gpoll["native_wait_promptness_sanity_gate"] is True,
         "poll control is exempt from the promptness gate")
    # an applicable native mode with zero observed calls fails closed
    ge = R.compute_native_promptness_gate(mode="dataflow_reduce", calls=[])
    c.ok(ge["native_wait_promptness_sanity_gate"] is False,
         "native mode with zero calls fails the promptness gate closed")


def _check_poll_half_retirement_invariant(c):
    prompt = _synth_calls_rtt([400_000, 520_000])
    disabled = R.compute_idle_backoff_disclosure({"hpx.max_idle_backoff_time": "0"})
    recorded = R.compute_idle_backoff_disclosure({"hpx.max_idle_backoff_time": "50"})
    unknown = R.compute_idle_backoff_disclosure({})
    gp = R.compute_native_promptness_gate(mode="when_all_then_reduce", calls=prompt)

    def _retire(mode, *, overall_pass, polled, promptness, disclosure):
        return R.compute_poll_half_retirement(
            mode=mode, overall_pass=overall_pass, success_path_polling_used=polled,
            promptness_gate=promptness, disclosure=disclosure)

    # HAPPY PATH: claim + pass + no user-poll + prompt + disclosed(disabled) -> RETIRED via idle_backoff
    r = _retire("when_all_then_reduce", overall_pass=True, polled=False, promptness=gp,
                disclosure=disabled)
    c.ok(r["poll_half_blocker_retired"] is True
         and r["poll_retired_via"] == "runtime_idle_backoff_disabled"
         and r["poll_retired_via"] == R.POLL_RETIRED_VIA_IDLE_BACKOFF,
         "claim + pass + prompt + disclosed(disabled) + no-user-poll -> RETIRED via runtime_idle_backoff_disabled")

    # TAMPER: success_path_polling_used=True -> NOT retired
    rt = _retire("when_all_then_reduce", overall_pass=True, polled=True, promptness=gp,
                 disclosure=disabled)
    c.ok(rt["poll_half_blocker_retired"] is False and rt["poll_retired_via"] == "none",
         "tamper success_path_polling_used=True -> NOT retired")

    # overall_pass False -> NOT retired
    rf = _retire("dataflow_reduce", overall_pass=False, polled=False, promptness=gp,
                 disclosure=disabled)
    c.ok(rf["poll_half_blocker_retired"] is False, "overall_pass False -> NOT retired")

    # slow-but-not-timeout wake (promptness gate False) -> NOT retired even when disabled + pass
    gslow = R.compute_native_promptness_gate(mode="when_all_then_reduce",
                                             calls=_synth_calls_rtt([100_000_000] * 3))
    rs = _retire("when_all_then_reduce", overall_pass=True, polled=False, promptness=gslow,
                 disclosure=disabled)
    c.ok(rs["poll_half_blocker_retired"] is False, "slow-but-not-timeout wake -> NOT retired")

    # recorded_only / unknown idle-backoff: no runtime progress driver disclosed -> NOT retired
    rr = _retire("when_all_then_reduce", overall_pass=True, polled=False, promptness=gp,
                 disclosure=recorded)
    ru = _retire("when_all_then_reduce", overall_pass=True, polled=False, promptness=gp,
                 disclosure=unknown)
    c.ok(rr["poll_half_blocker_retired"] is False and ru["poll_half_blocker_retired"] is False,
         "recorded_only / unknown idle-backoff (no disclosed driver) -> NOT retired")

    # YIELD DIAGNOSTIC can NEVER retire (not a claim mode), even if we wrongly claim it did not poll
    gpy = R.compute_native_promptness_gate(mode="when_all_then_reduce_yield", calls=prompt)
    ry = _retire("when_all_then_reduce_yield", overall_pass=True, polled=True, promptness=gpy,
                 disclosure=disabled)
    ry2 = _retire("when_all_then_reduce_yield", overall_pass=True, polled=False, promptness=gpy,
                  disclosure=disabled)
    c.ok(ry["poll_half_blocker_retired"] is False and ry2["poll_half_blocker_retired"] is False,
         "yield diagnostic can NEVER retire the poll half (not a claim mode)")

    # SEQUENTIAL_LEAF_WAIT positive control can NEVER retire (not a claim mode)
    gps = R.compute_native_promptness_gate(mode="sequential_leaf_wait", calls=prompt)
    rseq = _retire("sequential_leaf_wait", overall_pass=True, polled=False, promptness=gps,
                   disclosure=disabled)
    c.ok(rseq["poll_half_blocker_retired"] is False,
         "sequential_leaf_wait positive control can NEVER retire the poll half")

    # POLL CONTROL can NEVER retire (not a claim mode)
    gpc = R.compute_native_promptness_gate(mode="root_flat_gather_poll", calls=prompt)
    rpc = _retire("root_flat_gather_poll", overall_pass=True, polled=True, promptness=gpc,
                  disclosure=disabled)
    c.ok(rpc["poll_half_blocker_retired"] is False, "poll control can NEVER retire the poll half")


# ---------------------------------------------------------------------------
# A4-progress-probe: pure continuation-vs-waiter discriminator over synthetic ROOT-clock timestamps. No
# HPX/cluster: the C++ timestamp capture is exercised only on Rostam; here we verify the classifier, the
# derived deltas, sentinel/monotonicity fail-closed behavior, threshold reuse, and the fences (the probe
# never creates poll retirement; the yield mode stays diagnostic/reference).
# ---------------------------------------------------------------------------

_T0 = 1_000_000_000_000  # arbitrary steady_clock base (ns)


def _disc(mode, *, ds, ce, cc, wr, rtt_ns, timeout=8.0):
    return R.compute_progress_discriminator(
        mode=mode, t_dispatch_start_ns=ds, t_continuation_entered_ns=ce,
        t_continuation_completed_ns=cc, t_wait_returned_ns=wr, rtt_ns=rtt_ns,
        dispatch_timeout_s=timeout)


def _check_a4_progress_discriminator(c):
    T = _T0
    NATIVE = "when_all_then_reduce"

    # (1) classifier covers all five required signatures ---------------------------------------------
    # none: prompt outer RTT (< 0.01s) and prompt continuation
    g = _disc(NATIVE, ds=T, ce=T + 300_000, cc=T + 350_000, wr=T + 400_000, rtt_ns=400_000)
    c.ok(g["progress_deferred_to_timeout_signature"] == "none", "signature none (prompt)")
    # continuation_at_timeout: the reduce itself only entered near the deadline
    g = _disc(NATIVE, ds=T, ce=T + 8_000_000_000, cc=T + 8_000_100_000, wr=T + 8_000_200_000,
              rtt_ns=8_000_400_000)
    c.ok(g["progress_deferred_to_timeout_signature"] == "continuation_at_timeout",
         "signature continuation_at_timeout (reduce ran at ~timeout)")
    # waiter_resume_at_timeout: reduce ran early, caller resumed at timeout
    g = _disc(NATIVE, ds=T, ce=T + 300_000, cc=T + 400_000, wr=T + 8_000_000_000,
              rtt_ns=8_000_400_000)
    c.ok(g["progress_deferred_to_timeout_signature"] == "waiter_resume_at_timeout",
         "signature waiter_resume_at_timeout (continuation early, waiter late)")
    # ambiguous: moderate continuation delay, no clean timeout signature
    g = _disc(NATIVE, ds=T, ce=T + 500_000_000, cc=T + 500_100_000, wr=T + 500_200_000,
              rtt_ns=500_300_000)
    c.ok(g["progress_deferred_to_timeout_signature"] == "ambiguous",
         "signature ambiguous (fallback)")
    # continuation_uncaptured: continuation never ran (sentinel 0) though body timestamps present
    g = _disc(NATIVE, ds=T, ce=0, cc=0, wr=T + 8_000_000_000, rtt_ns=8_000_000_000)
    c.ok(g["progress_deferred_to_timeout_signature"] == "continuation_uncaptured",
         "signature continuation_uncaptured (continuation sentinel)")

    # (2) derived deltas correct and sentinel-safe ---------------------------------------------------
    g = _disc(NATIVE, ds=T, ce=T + 1_000_000_000, cc=T + 1_500_000_000, wr=T + 2_000_000_000,
              rtt_ns=2_000_000_000)
    c.ok(abs(g["continuation_delay_s"] - 1.0) < 1e-9, "continuation_delay_s = entered - dispatch")
    c.ok(abs(g["continuation_duration_s"] - 0.5) < 1e-9, "continuation_duration_s = completed - entered")
    c.ok(abs(g["wait_return_delay_after_continuation_s"] - 0.5) < 1e-9,
         "wait_return_delay_after_continuation_s = wait_returned - completed")
    gs = _disc(NATIVE, ds=T, ce=0, cc=0, wr=T + 1_000_000_000, rtt_ns=1_000_000_000)
    c.ok(gs["continuation_delay_s"] is None and gs["continuation_duration_s"] is None
         and gs["wait_return_delay_after_continuation_s"] is None,
         "sentinel continuation timestamps -> None deltas (no crash)")

    # (3) monotonicity gate fails on scrambled timestamps --------------------------------------------
    scrambled = _disc(NATIVE, ds=T + 5_000_000_000, ce=T + 1_000_000_000, cc=T + 2_000_000_000,
                      wr=T + 3_000_000_000, rtt_ns=3_000_000_000)
    c.ok(scrambled["progress_instrumentation_ok"] is False
         and scrambled["progress_deferred_to_timeout_signature"] == "instrumentation_invalid",
         "scrambled (out-of-order) timestamps fail the monotonicity gate closed")
    ordered = _disc(NATIVE, ds=T, ce=T + 1, cc=T + 2, wr=T + 3, rtt_ns=3)
    c.ok(ordered["progress_instrumentation_ok"] is True, "ordered timestamps pass the monotonicity gate")

    # (4) continuation_fired_before_wait_return behavior ---------------------------------------------
    c.ok(ordered["continuation_fired_before_wait_return"] is True,
         "continuation_fired_before_wait_return True when 0 < completed <= wait_returned")
    c.ok(gs["continuation_fired_before_wait_return"] is False,
         "continuation_fired_before_wait_return False when completed is sentinel")

    # non-applicable modes (poll control / positive control) -> n/a, not applicable -----------------
    for m in ("root_flat_gather_poll", "sequential_leaf_wait"):
        gna = _disc(m, ds=T, ce=0, cc=0, wr=T + 400_000, rtt_ns=400_000)
        c.ok(gna["progress_discriminator_applicable"] is False
             and gna["progress_deferred_to_timeout_signature"] == "n/a",
             f"{m}: discriminator not applicable (n/a)")

    # (5) probe fields never create poll retirement --------------------------------------------------
    disabled = R.compute_idle_backoff_disclosure({"hpx.max_idle_backoff_time": "0"})
    prompt_calls = [{"call_index": 0, "rtt_ns": 400_000}]
    gp = R.compute_native_promptness_gate(mode="when_all_then_reduce_yield", calls=prompt_calls)
    # a yield mode with a prompt 'none' progress signature STILL cannot retire (not a claim mode)
    ry = R.compute_poll_half_retirement(
        mode="when_all_then_reduce_yield", overall_pass=True, success_path_polling_used=True,
        promptness_gate=gp, disclosure=disabled)
    c.ok(ry["poll_half_blocker_retired"] is False,
         "A4 progress signal does not let the yield diagnostic retire the poll half")
    agg_keys = set(R.aggregate_progress_probe(
        [{"progress": ordered}], mode=NATIVE).keys())
    c.ok("poll_half_blocker_retired" not in agg_keys and "poll_retired_via" not in agg_keys,
         "progress-probe aggregate carries NO poll-retirement keys")

    # (6) yield mode remains diagnostic/reference ----------------------------------------------------
    c.ok("when_all_then_reduce_yield" in R.NATIVE_COMPOSITION_MODES
         and "when_all_then_reduce_yield" in R.DIAGNOSTIC_READINESS_MODES
         and "when_all_then_reduce_yield" not in R.NATIVE_READINESS_MODES,
         "yield mode: discriminator applies but it stays diagnostic-only, never a claim mode")

    # (7) threshold reuse ----------------------------------------------------------------------------
    gthr = _disc(NATIVE, ds=T, ce=T + 300_000, cc=T + 350_000, wr=T + 400_000, rtt_ns=400_000,
                 timeout=8.0)
    c.ok(abs(gthr["progress_prompt_floor_s"] - R.NATIVE_PROMPTNESS_THRESHOLD_S) < 1e-12,
         "prompt floor reuses NATIVE_PROMPTNESS_THRESHOLD_S")
    c.ok(abs(gthr["progress_deferred_floor_s"] - R.NATIVE_DEADLINE_MARGIN_FRACTION * 8.0) < 1e-9,
         "deferred floor = NATIVE_DEADLINE_MARGIN_FRACTION * dispatch_timeout_s")

    # aggregate: agreeing calls collapse; disagreeing -> 'mixed'; empty -> 'n/a' ---------------------
    p_at = _disc(NATIVE, ds=T, ce=T + 8_000_000_000, cc=T + 8_000_100_000, wr=T + 8_000_200_000,
                 rtt_ns=8_000_400_000)
    agg_same = R.aggregate_progress_probe([{"progress": p_at}, {"progress": p_at}], mode=NATIVE)
    c.ok(agg_same["progress_deferred_to_timeout_signature"] == "continuation_at_timeout"
         and agg_same["all_progress_instrumentation_ok"] is True,
         "aggregate collapses agreeing per-call signatures")
    p_none = _disc(NATIVE, ds=T, ce=T + 300_000, cc=T + 350_000, wr=T + 400_000, rtt_ns=400_000)
    agg_mixed = R.aggregate_progress_probe([{"progress": p_at}, {"progress": p_none}], mode=NATIVE)
    c.ok(agg_mixed["progress_deferred_to_timeout_signature"] == "mixed",
         "aggregate reports 'mixed' when per-call signatures disagree")
    agg_empty = R.aggregate_progress_probe([], mode=NATIVE)
    c.ok(agg_empty["progress_deferred_to_timeout_signature"] == "n/a",
         "aggregate over zero calls is 'n/a'")


# ---------------------------------------------------------------------------
# Run 1 blocked-waiter probe: LOCAL no-parcelport control mode, the waiter-suspended fast-path guard, the
# leaves_all_local scoring, and the pure interpreter helpers. All pure/off-cluster.
# ---------------------------------------------------------------------------

_LOCAL = "local_when_all_then_reduce_wait_for"


def _synth_local_result(x, n, root_loc, s=0):
    res = _synthetic_payload_result(x, n, s, [root_loc])  # all leaves on the root locality
    res["local_control"] = True
    res["placement_class"] = "local_control"
    res["local_leaf_delay_ms"] = 2
    return res


def _check_run1_blocked_waiter(c):
    T = 1_000_000_000_000

    # (a) taxonomy: local control has a continuation (discriminator applies) but is NEVER a claim mode
    c.ok(_LOCAL in R.LOCAL_CONTROL_MODES and _LOCAL in R.NATIVE_COMPOSITION_MODES,
         "local control is a composition mode (discriminator applies)")
    c.ok(_LOCAL not in R.NATIVE_READINESS_MODES and _LOCAL not in R.DIAGNOSTIC_READINESS_MODES
         and _LOCAL not in R.POSITIVE_CONTROL_MODES and _LOCAL not in R.POLLING_MODES,
         "local control is neither a claim, diagnostic, positive-control, nor polling mode")
    c.ok(R._native_wait_variant(_LOCAL) == "local_wait_for", "local control wait variant = local_wait_for")
    pv = R._readiness_provenance(_LOCAL)
    c.ok(pv["hpx_native_readiness_composition"] is False and pv["diagnostic_only"] is False
         and pv["poll_half_addressed"] is False and pv["success_path_polling_used"] is False,
         "local control provenance: not a claim, blocks, never addresses the poll half")

    # (b) local control can NEVER retire the poll half (not a claim mode)
    disabled = R.compute_idle_backoff_disclosure({"hpx.max_idle_backoff_time": "0"})
    gp = R.compute_native_promptness_gate(mode=_LOCAL, calls=[{"call_index": 0, "rtt_ns": 400_000}])
    rr = R.compute_poll_half_retirement(mode=_LOCAL, overall_pass=True, success_path_polling_used=False,
                                        promptness_gate=gp, disclosure=disabled)
    c.ok(rr["poll_half_blocker_retired"] is False, "local control can NEVER retire the poll half")

    # (c) waiter_suspended_before_ready fast-path guard
    g_susp = R.compute_progress_discriminator(
        mode=_LOCAL, t_dispatch_start_ns=T, t_continuation_entered_ns=T + 100,
        t_continuation_completed_ns=T + 200, t_wait_returned_ns=T + 300,
        t_waiter_entered_ns=T + 150, rtt_ns=300, dispatch_timeout_s=8.0)
    c.ok(g_susp["waiter_suspended_before_ready"] is True,
         "waiter_suspended_before_ready True when t_waiter_entered < t_continuation_completed")
    g_fast = R.compute_progress_discriminator(
        mode=_LOCAL, t_dispatch_start_ns=T, t_continuation_entered_ns=T + 100,
        t_continuation_completed_ns=T + 200, t_wait_returned_ns=T + 300,
        t_waiter_entered_ns=T + 250, rtt_ns=300, dispatch_timeout_s=8.0)
    c.ok(g_fast["waiter_suspended_before_ready"] is False,
         "waiter_suspended_before_ready False (fast-path artifact) when waiter entered after readiness")
    g_unk = R.compute_progress_discriminator(
        mode=_LOCAL, t_dispatch_start_ns=T, t_continuation_entered_ns=T + 100,
        t_continuation_completed_ns=T + 200, t_wait_returned_ns=T + 300,
        t_waiter_entered_ns=0, rtt_ns=300, dispatch_timeout_s=8.0)
    c.ok(g_unk["waiter_suspended_before_ready"] is None,
         "waiter_suspended_before_ready None when t_waiter_entered not captured")

    # aggregate surfaces all_waiter_suspended_before_ready as a fail-closed AND over known calls
    agg = R.aggregate_progress_probe([{"progress": g_susp}, {"progress": g_fast}], mode=_LOCAL)
    c.ok(agg["all_waiter_suspended_before_ready"] is False,
         "aggregate all_waiter_suspended_before_ready False if any call was a fast-path artifact")
    agg2 = R.aggregate_progress_probe([{"progress": g_susp}, {"progress": g_susp}], mode=_LOCAL)
    c.ok(agg2["all_waiter_suspended_before_ready"] is True,
         "aggregate all_waiter_suspended_before_ready True when every call genuinely suspended")

    # (d) leaves_all_local scoring: local result passes on leaves_all_local, no all-remote keys
    lr = _synth_local_result(7, 8, 0)
    gl = R.compute_payload_gates(x=7, n=8, payload_bytes=0, root_loc=0, remote_locs=[1, 2],
                                 result=lr, folded_digest=R.fold_payload_digest(lr["leaves"]))
    c.ok(gl.get("leaves_all_local") is True and "leaves_remote_all" not in gl
         and "leaves_local_zero" not in gl and all(gl.values()),
         "local control scored by leaves_all_local, not the all-remote/balanced gates")
    rr2 = _synthetic_payload_result(7, 8, 0, [1, 2])
    gr = R.compute_payload_gates(x=7, n=8, payload_bytes=0, root_loc=0, remote_locs=[1, 2],
                                 result=rr2, folded_digest=R.fold_payload_digest(rr2["leaves"]))
    c.ok(gr.get("leaves_remote_all") is True and "leaves_all_local" not in gr,
         "remote result keeps the all-remote gates (local branch does not leak)")

    # (e) interpret_local_vs_remote_resume
    c.ok(R.interpret_local_vs_remote_resume("waiter_resume_at_timeout", "waiter_resume_at_timeout")
         == "pure_hpx_scheduler_resume", "local+remote both stall -> pure HPX scheduler resume")
    c.ok(R.interpret_local_vs_remote_resume("none", "waiter_resume_at_timeout")
         == "remote_parcel_or_crosspool_resume", "local prompt, remote stall -> parcel/cross-pool resume")
    c.ok(R.interpret_local_vs_remote_resume("none", "none") == "no_resume_defect_observed",
         "both prompt -> no resume defect observed")
    c.ok(R.interpret_local_vs_remote_resume("ambiguous", "none") == "ambiguous",
         "otherwise ambiguous")

    # (f) interpret_threadcount_resume
    c.ok(R.interpret_threadcount_resume({1: "none", 2: "waiter_resume_at_timeout",
                                         4: "waiter_resume_at_timeout"}) == "wake_sleeping_worker",
         "threads=1 prompt, >1 stall -> wake-a-sleeping-worker")
    c.ok(R.interpret_threadcount_resume({1: "waiter_resume_at_timeout", 2: "waiter_resume_at_timeout",
                                         4: "waiter_resume_at_timeout"}) == "resume_broken_all_threadcounts",
         "all thread counts stall -> resume broken regardless of worker count")
    c.ok(R.interpret_threadcount_resume({1: "none", 2: "none", 4: "none"})
         == "no_resume_defect_observed", "all thread counts prompt -> no defect")
    c.ok(R.interpret_threadcount_resume({}) == "ambiguous", "empty thread map -> ambiguous")

    # (g) classify_untimed_wait (pure; C++ untimed mode DEFERRED)
    c.ok(R.classify_untimed_wait(resumed=False, hung=True, delay_s=None)
         == "untimed_hung_ready_resume_lost", "untimed hung -> ready-resume lost")
    c.ok(R.classify_untimed_wait(resumed=True, hung=False, delay_s=0.0004)
         == "untimed_resumed_prompt", "untimed resumed promptly -> timer was interfering")
    c.ok(R.classify_untimed_wait(resumed=True, hung=False, delay_s=2.0)
         == "untimed_resumed_slow", "untimed resumed slowly -> slow, not lost")
    c.ok(R.classify_untimed_wait(resumed=False, hung=False, delay_s=None) == "ambiguous",
         "untimed neither hung nor resumed -> ambiguous")


def _check_native_heartbeat_completion(c):
    import os
    import tempfile
    bd = tempfile.mkdtemp(prefix="exp64_selftest_hb_")
    try:
        # heartbeat writes root.alive; completion writes root.done + legacy served1.ok
        R._touch_heartbeat([bd])
        c.ok(os.path.isfile(os.path.join(bd, "root.alive")), "heartbeat writes root.alive")
        c.ok(not os.path.isfile(os.path.join(bd, "root.done")),
             "heartbeat alone does NOT write the completion sentinel")
        R._write_completion([bd])
        c.ok(os.path.isfile(os.path.join(bd, "root.done"))
             and os.path.isfile(os.path.join(bd, "served1.ok")),
             "completion writes root.done + legacy served1.ok")
    finally:
        import shutil
        shutil.rmtree(bd, ignore_errors=True)


# ---------------------------------------------------------------------------
# Slice 3 payload-ladder MANIFEST checks (pure; synthetic matched per-arm artifacts). A clean matched
# ladder must pass structurally with same_axis_comparison=True and every fence False; each single
# corruption must fail CLOSED (overall_manifest_pass=False AND same_axis_comparison=False).
# ---------------------------------------------------------------------------

def _synth_calls(measured):
    return [{"call_index": i, "rtt_ms": 1.0 + 0.01 * i, "rtt_ns": int((1.0 + 0.01 * i) * 1e6),
             "gates_pass": True,
             "gates": {"no_dispatch_timeout": True, "leaves_local_zero": True,
                       "leaves_remote_all": True, "leaves_per_remote_balanced": True}}
            for i in range(measured)]


def _synthetic_ladder_artifact(arm, *, job="900001", size=0, x=7, n=8, prewarm=3, measured=5,
                               node_set=("medusa00", "medusa01", "medusa02"), subnet="10.42.5."):
    """A synthetic per-arm ladder artifact carrying exactly the fields the manifest reads (seeded through
    build_provenance so the locked fences are present and False)."""
    rec = R.build_provenance(x=x, n=n, sizes=[size], phase=f"{arm}-payload-remote-smoke")
    rec.update({
        "arm": arm, "payload_bytes": int(size), "n": n,
        "slurm_job_id": job, "node_set": list(node_set),
        "prewarm": prewarm, "measured": measured,
        "boundary": R.TIMING_BOUNDARY, "clock": R.TIMING_CLOCK,
        "prefer_subnet": subnet, "selected_subnet": subnet,
        "evidence_grade": R.EVIDENCE_GRADE_R1,
        "distributional_evidence": False, "percentiles_evidence_ready": False,
        "phase_affinity_recorded": True, "effective_cpu_binding": [0, 1, 2, 3],
        "prewarm_excluded_from_timed": True,
        "expected_digest": R.payload_digest(x, n, size),
        "calls": _synth_calls(measured),
        "structural_gates": {"timed_call_returns_payload_bytes_to_python": True,
                             "digest_check_after_timing_outside_rtt": True,
                             "digest_folded_inside_runtime": False},
        "overall_pass": True,
        "all_remote_all_calls": True,
        "distribution_balanced_all_calls": True,
        "dispatch_no_timeout_all_calls": True,
    })
    if arm == "hpx":
        rec.update({"hpx_parcelport": "tcp", "hpx_composition": "root_flat_gather_poll",
                    "transport_family": "tcp", "hpx_teardown_clean": True,
                    "connector_lifecycle_ok": True,
                    # Slice 4 review-folded provenance blocks (serialization runtime path not observed)
                    "hpx_serialization": {
                        "payload_representation": "hpx::serialization::serialize_buffer<char>",
                        "zero_copy_runtime_path_taken": "not_observed"},
                    "hpx_poll": {"hpx_composition": "root_flat_gather_poll",
                                 "hpx_poll_interval_us": 50,
                                 "hpx_not_exp63_native_composition": True},
                    "hpx_runtime": {"hpx_threads": "4", "hpx_bind": "balanced"},
                    "numa_nic": {"selected_iface": "not_observed"},
                    "connector_anomaly_witness": {"connector_lifecycle_ok": True,
                                                  "connector_shutdown_reason": "served_signal"}})
    else:
        rec.update({"transport_family": "ray_object_transport", "no_orphan_proof": True,
                    "object_return_path": "not_observed", "resource_map": "not_observed"})
    return rec


def _clean_ladder_pair(job="900001", ladder=None):
    ladder = list(ladder or R.DEFAULT_SIZE_LADDER)
    hpx = [_synthetic_ladder_artifact("hpx", job=job, size=s) for s in ladder]
    ray = [_synthetic_ladder_artifact("ray", job=job, size=s) for s in ladder]
    return hpx, ray, ladder


def _collect_keys(obj, out):
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.append(str(k))
            _collect_keys(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _collect_keys(v, out)
    return out


def _check_manifest_clean(c):
    hpx, ray, ladder = _clean_ladder_pair()
    m = R.build_payload_ladder_manifest(hpx, ray, job="900001", ladder_sizes=ladder)
    ok, probs = R.validate_payload_ladder_manifest(m)
    failed = [g for g, v in m["correlation_gates"].items() if not v]
    c.ok(ok and m["overall_manifest_pass"] and m["same_axis_comparison"],
         f"clean matched ladder passes structurally (failed={failed} probs={probs})")
    c.ok(m["evidence_grade"] == "structural_r1", "manifest evidence_grade is structural_r1")
    c.ok(m["distributional_evidence"] is False and m["percentiles_evidence_ready"] is False,
         "manifest disclaims distributional/percentile evidence")
    c.ok(m["no_cross_arm_timing_computed"] is True, "manifest declares no cross-arm timing computed")
    for k in R.MANIFEST_FENCE_KEYS_FALSE:
        c.ok(m.get(k) is False, f"manifest fence {k} locked False")
    c.ok(set(m["arms"].keys()) == {"hpx", "ray"}, "arms stored as keyed hpx/ray blocks")
    c.ok(all("mean_rtt_ms" in m["arms"][a]["by_size"][str(s)]["rtt_within_arm"]
             for a in ("hpx", "ray") for s in ladder),
         "within-arm RTT summaries present per arm/size")
    # no cross-arm arithmetic key snuck in (targeted tokens for arm-vs-arm numbers, not honesty labels)
    keys = [k.lower() for k in _collect_keys(m, [])]
    banned = [k for k in keys
              for sub in ("rtt_diff", "rtt_ratio", "_delta_", "delta_ms", "cross_arm_ratio",
                          "arm_ratio", "vs_ray", "vs_hpx", "ratio_value", "speedup_value")
              if sub in k]
    c.ok(not banned, f"manifest has no cross-arm arithmetic keys (found={banned})")
    c.ok(not R._scan_forbidden_keys(m, []), "manifest has no forbidden claim keys")


def _fails_closed(c, hpx, ray, gate, label, job="900001", ladder=None):
    ladder = list(ladder or R.DEFAULT_SIZE_LADDER)
    m = R.build_payload_ladder_manifest(hpx, ray, job=job, ladder_sizes=ladder)
    closed = (m["overall_manifest_pass"] is False) and (m["same_axis_comparison"] is False)
    gate_false = gate is None or (m["correlation_gates"].get(gate) is False)
    ok, _ = R.validate_payload_ladder_manifest(m)
    c.ok(closed and gate_false and ok,
         f"{label} fails closed (pass={m['overall_manifest_pass']} "
         f"gate {gate}={m['correlation_gates'].get(gate)} validator_ok={ok})")


def _check_manifest_fail_closed(c):
    # node_set mismatch
    hpx, ray, ladder = _clean_ladder_pair()
    for a in ray:
        a["node_set"] = ["medusa10", "medusa11", "medusa12"]
    _fails_closed(c, hpx, ray, "node_set_matched", "node_set mismatch")

    # SLURM_JOB_ID / allocation identity mismatch
    hpx, ray, ladder = _clean_ladder_pair()
    for a in hpx:
        a["slurm_job_id"] = "111111"
    _fails_closed(c, hpx, ray, "single_slurm_job_identity", "allocation id mismatch")

    # missing size in one arm
    hpx, ray, ladder = _clean_ladder_pair()
    ray = ray[:-1]
    _fails_closed(c, hpx, ray, "ladder_fully_covered_both_arms", "missing size in ray arm")

    # prewarm mismatch
    hpx, ray, ladder = _clean_ladder_pair()
    for a in ray:
        a["prewarm"] = 99
    _fails_closed(c, hpx, ray, "prewarm_matched", "prewarm mismatch")

    # measured mismatch
    hpx, ray, ladder = _clean_ladder_pair()
    for a in ray:
        a["measured"] = 6
    _fails_closed(c, hpx, ray, "measured_matched", "measured mismatch")

    # boundary mismatch
    hpx, ray, ladder = _clean_ladder_pair()
    for a in hpx:
        a["boundary"] = "wall_clock_time"
    _fails_closed(c, hpx, ray, "boundary_matched", "boundary mismatch")

    # clock mismatch
    hpx, ray, ladder = _clean_ladder_pair()
    for a in ray:
        a["clock"] = "time_time"
    _fails_closed(c, hpx, ray, "clock_matched", "clock mismatch")

    # subnet mismatch
    hpx, ray, ladder = _clean_ladder_pair()
    for a in ray:
        a["prefer_subnet"] = "10.42.9."
        a["selected_subnet"] = "10.42.9."
    _fails_closed(c, hpx, ray, "subnet_matched", "subnet mismatch")

    # expected_digest mismatch at one size
    hpx, ray, ladder = _clean_ladder_pair()
    hpx[2]["expected_digest"] = (hpx[2]["expected_digest"] or 0) + 1
    _fails_closed(c, hpx, ray, "expected_digest_matched_every_size", "expected_digest mismatch")

    # either arm overall_pass False
    hpx, ray, ladder = _clean_ladder_pair()
    ray[1]["overall_pass"] = False
    _fails_closed(c, hpx, ray, "both_arms_overall_pass_all_sizes", "ray overall_pass False")

    # leaf on head/root (not all-remote)
    hpx, ray, ladder = _clean_ladder_pair()
    hpx[0]["all_remote_all_calls"] = False
    _fails_closed(c, hpx, ray, "both_arms_all_remote_all_sizes", "leaf on root not all-remote")

    # non-4/4 distribution
    hpx, ray, ladder = _clean_ladder_pair()
    ray[0]["distribution_balanced_all_calls"] = False
    _fails_closed(c, hpx, ray, "both_arms_balanced_distribution_all_sizes", "non-4/4 distribution")

    # prewarm-included / timed-count mismatch (calls length != measured)
    hpx, ray, ladder = _clean_ladder_pair()
    hpx[0]["calls"] = hpx[0]["calls"][:-1]
    _fails_closed(c, hpx, ray, "prewarm_excluded_from_timed_both_arms", "timed-count mismatch")

    # missing affinity provenance
    hpx, ray, ladder = _clean_ladder_pair()
    ray[0]["phase_affinity_recorded"] = False
    _fails_closed(c, hpx, ray, "ray_phase_affinity_recorded", "missing ray affinity provenance")

    # HPX residue not clear before Ray phase
    hpx, ray, ladder = _clean_ladder_pair()
    hpx[0]["hpx_teardown_clean"] = False
    _fails_closed(c, hpx, ray, "hpx_residue_clear_before_ray", "HPX residue not clear")

    # Ray orphan proof fails
    hpx, ray, ladder = _clean_ladder_pair()
    ray[0]["no_orphan_proof"] = False
    _fails_closed(c, hpx, ray, "ray_no_orphan_proof", "ray orphan proof fail")

    # a forbidden ratio/speedup/winner key in a source artifact is caught by the gate and the manifest
    # is fenced closed. (The manifest never embeds raw artifacts, so it itself stays clean/consistent
    # and the validator legitimately passes on the fenced-closed manifest.)
    hpx, ray, ladder = _clean_ladder_pair()
    ray[0]["speedup_value_ms"] = 1.0
    m = R.build_payload_ladder_manifest(hpx, ray, job="900001", ladder_sizes=ladder)
    okv, _ = R.validate_payload_ladder_manifest(m)
    c.ok(m["correlation_gates"]["no_forbidden_keys"] is False
         and m["overall_manifest_pass"] is False and m["same_axis_comparison"] is False
         and not R._scan_forbidden_keys(m, []) and okv,
         "forbidden speedup key caught by gate; manifest fenced closed and stays clean")

    # dispatch timeout in one arm
    hpx, ray, ladder = _clean_ladder_pair()
    ray[3]["dispatch_no_timeout_all_calls"] = False
    _fails_closed(c, hpx, ray, "no_dispatch_timeout_both_arms", "ray dispatch timeout")


def _check_manifest_validator_bites(c):
    # a manifest whose same_axis_comparison is forced True over a failing gate must be rejected
    hpx, ray, ladder = _clean_ladder_pair()
    ray[0]["no_orphan_proof"] = False
    m = R.build_payload_ladder_manifest(hpx, ray, job="900001", ladder_sizes=ladder)
    m["same_axis_comparison"] = True  # tamper
    ok, probs = R.validate_payload_ladder_manifest(m)
    c.ok(not ok, f"validator rejects same_axis_comparison=True over a failing gate (probs={probs})")
    # a manifest whose speedup fence is flipped True must be rejected
    hpx, ray, ladder = _clean_ladder_pair()
    m = R.build_payload_ladder_manifest(hpx, ray, job="900001", ladder_sizes=ladder)
    m["speedup_computed"] = True  # tamper
    ok, _ = R.validate_payload_ladder_manifest(m)
    c.ok(not ok, "validator rejects a flipped speedup_computed fence")


# ---------------------------------------------------------------------------
# Slice 4 payload-ladder BAND checks (pure; synthetic R matched islands). A clean R=5 band earns
# matched_band_r5 with WITHIN-ARM distributions; distributional_payload_ladder stays blocked; any
# structural defect fails the band closed. No cross-arm arithmetic anywhere.
# ---------------------------------------------------------------------------

def _synth_island(*, island_index, job, band_id="bandZ", ladder=None, measured=30, prewarm=5,
                  node_set=None, subnet="10.42.5."):
    """A synthetic clean island: R.build_payload_ladder_manifest over measured=30 arm arts + the raw
    hpx_by/ray_by the band reads for within-arm distributions."""
    ladder = list(ladder or R.DEFAULT_SIZE_LADDER)
    node_set = node_set or [f"medusa{10 + island_index * 3 + k:02d}" for k in range(3)]
    hpx_by, ray_by = {}, {}
    for s in ladder:
        hpx_by[s] = _synthetic_ladder_artifact("hpx", job=job, size=s, measured=measured,
                                               prewarm=prewarm, node_set=node_set, subnet=subnet)
        ray_by[s] = _synthetic_ladder_artifact("ray", job=job, size=s, measured=measured,
                                               prewarm=prewarm, node_set=node_set, subnet=subnet)
    manifest = R.build_payload_ladder_manifest(
        list(hpx_by.values()), list(ray_by.values()), job=job, ladder_sizes=ladder,
        band_id=band_id, island_index=island_index, required_measured=measured)
    ok, _ = R.validate_payload_ladder_manifest(manifest)
    manifest["validator_ok"] = ok
    return {"island_index": island_index, "job": job, "manifest": manifest,
            "hpx_by": hpx_by, "ray_by": ray_by}


def _clean_band(band_id="bandZ", n_islands=5, measured=30, ladder=None):
    ladder = list(ladder or R.DEFAULT_SIZE_LADDER)
    islands = [_synth_island(island_index=i, job=f"90{i:04d}", band_id=band_id, ladder=ladder,
                             measured=measured) for i in range(1, n_islands + 1)]
    return islands, ladder


def _check_within_arm_stats(c):
    s = R._within_arm_stats([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    c.ok(s["n"] == 10 and s["min_ms"] == 1 and s["max_ms"] == 10 and s["mean_ms"] == 5.5,
         f"within-arm min/mean/max correct ({s})")
    c.ok(abs(s["p50_ms"] - 5.5) < 1e-9 and abs(s["p90_ms"] - 9.1) < 1e-9,
         f"within-arm p50/p90 correct ({s['p50_ms']},{s['p90_ms']})")
    # a TIGHT vector (small CV, no heavy tail) trips neither coarse flag
    tight = R._within_arm_stats([100, 101, 102, 103, 104])
    c.ok(tight["high_variability_flag"] is False and tight["multimodal_suspected"] is False,
         f"tight vector is neither high-variability nor multimodal (cv={tight['cv']:.4f})")
    # high-CV vector trips high_variability_flag
    hv = R._within_arm_stats([1, 1, 1, 1, 50])
    c.ok(hv["high_variability_flag"] is True, f"high-CV vector flags high variability (cv={hv['cv']})")
    # heavy upper tail trips multimodal_suspected (p90-p50 dwarfs p50-min)
    mm = R._within_arm_stats([1, 1, 1, 1, 1, 1, 1, 1, 1, 40])
    c.ok(mm["multimodal_suspected"] is True, f"heavy-tail vector flags multimodal ({mm})")
    # across-island median + range on known per-island p50s
    a = R._across_island([2.0, 4.0, 6.0])
    c.ok(a["median"] == 4.0 and a["range"] == [2.0, 6.0] and a["n_islands"] == 3,
         f"across-island median/range correct ({a})")
    c.ok(R._within_arm_stats([])["n"] == 0, "empty within-arm stats degrade cleanly")


def _check_band_clean(c):
    islands, ladder = _clean_band()
    band = R.build_payload_band_aggregate(islands, band_id="bandZ", required_islands=5,
                                          required_measured=30, ladder_sizes=ladder)
    ok, probs = R.validate_payload_band_aggregate(band)
    failed = [g for g, v in band["band_gates"].items() if not v]
    c.ok(ok and band["overall_band_pass"] and band["same_axis_comparison"],
         f"clean R=5 band passes (failed={failed} probs={probs})")
    c.ok(band["evidence_grade"] == "matched_band_r5", "band evidence_grade is matched_band_r5")
    c.ok(band["distributional_evidence"] is True and band["percentiles_evidence_ready"] is True,
         "band earns within-arm distributional + p50/p90 evidence")
    c.ok(band["p99_evidence_ready"] is False, "p99 is not evidence-ready at measured=30")
    c.ok(band["distributional_payload_ladder_ready"] is False
         and band["distributional_payload_ladder_blocked_reason"],
         "distributional_payload_ladder stays blocked (serialization runtime path not observed)")
    for k in R.BAND_FENCE_KEYS_FALSE:
        c.ok(band.get(k) is False, f"band fence {k} locked False")
    c.ok(band["no_cross_arm_timing_computed"] is True, "band declares no cross-arm timing computed")
    c.ok(set(band["arms"].keys()) == {"hpx", "ray"}
         and all("by_size" in band["arms"][a] and "provenance" in band["arms"][a]
                 for a in ("hpx", "ray")), "band arms are keyed hpx/ray blocks")
    hz = band["arms"]["hpx"]["by_size"]["0"]
    c.ok("across_island_p50" in hz and "across_island_p90" in hz
         and all("p50_ms" in pi for pi in hz["per_island"]),
         "band carries within-arm p50/p90 per island + across-island summaries")
    c.ok(not R._scan_forbidden_keys(band, []) and not R._scan_cross_arm_tokens(band["arms"], []),
         "band has no forbidden or cross-arm arithmetic keys")


def _band_fails(c, islands, gate, label, band_id="bandZ", required_islands=5, required_measured=30,
                ladder=None, island_independence="fresh_allocation_per_island"):
    ladder = list(ladder or R.DEFAULT_SIZE_LADDER)
    band = R.build_payload_band_aggregate(islands, band_id=band_id, required_islands=required_islands,
                                          required_measured=required_measured, ladder_sizes=ladder,
                                          island_independence=island_independence)
    ok, _ = R.validate_payload_band_aggregate(band)
    closed = (band["overall_band_pass"] is False and band["same_axis_comparison"] is False
              and band["distributional_evidence"] is False
              and band["evidence_grade"] != "matched_band_r5")
    gate_false = gate is None or (band["band_gates"].get(gate) is False)
    c.ok(closed and gate_false and ok,
         f"{label} fails closed (pass={band['overall_band_pass']} "
         f"gate {gate}={band['band_gates'].get(gate)} validator_ok={ok})")


def _check_band_fail_closed(c):
    # fewer than 5 islands
    islands, ladder = _clean_band(n_islands=4)
    _band_fails(c, islands, "islands_present_ge_required", "fewer than 5 islands")

    # measured < required in one island
    islands, ladder = _clean_band()
    low = _synth_island(island_index=3, job="903333", band_id="bandZ", measured=10)
    islands[2] = low
    _band_fails(c, islands, "all_islands_measured_ge_required", "measured<30 in an island")

    # an island manifest failing fails the band
    islands, ladder = _clean_band()
    islands[1]["manifest"]["overall_manifest_pass"] = False
    _band_fails(c, islands, "all_islands_manifest_pass", "an island manifest not pass")

    # structural drift across islands (island 5 uses measured=35; internally valid, drifts the band)
    islands, ladder = _clean_band()
    islands[4] = _synth_island(island_index=5, job="905555", band_id="bandZ", measured=35)
    _band_fails(c, islands, "structural_params_consistent_across_islands", "structural drift across islands")

    # a flagged island (digest mismatch) fails the band closed, not silently dropped
    islands, ladder = _clean_band()
    islands[0]["hpx_by"][1024]["expected_digest"] = (islands[0]["hpx_by"][1024]["expected_digest"] or 0) + 1
    _band_fails(c, islands, "all_islands_clean_quality", "flagged island (digest) not cherry-picked")

    # forbidden ratio/speedup/winner key in an island manifest
    islands, ladder = _clean_band()
    islands[0]["manifest"]["speedup_value_here"] = 1.0
    _band_fails(c, islands, "no_forbidden_keys", "forbidden speedup key in a manifest")


def _check_band_no_cross_arm(c):
    islands, ladder = _clean_band()
    band = R.build_payload_band_aggregate(islands, band_id="bandZ", required_islands=5,
                                          required_measured=30, ladder_sizes=ladder)
    c.ok(not R._scan_cross_arm_tokens(band, []), "clean band has zero cross-arm arithmetic keys")
    # inject a cross-arm key into the arms block -> validator must bite
    band["arms"]["hpx"]["by_size"]["0"]["rtt_ratio_vs_ray"] = 1.23
    ok, probs = R.validate_payload_band_aggregate(band)
    c.ok(not ok, f"validator rejects an injected cross-arm arithmetic key (probs={probs})")


def _check_band_validator_bites(c):
    islands, ladder = _clean_band(n_islands=4)  # a failing band
    band = R.build_payload_band_aggregate(islands, band_id="bandZ", required_islands=5,
                                          required_measured=30, ladder_sizes=ladder)
    band["same_axis_comparison"] = True  # tamper over a failing gate
    ok, _ = R.validate_payload_band_aggregate(band)
    c.ok(not ok, "validator rejects same_axis_comparison=True over a failing band gate")
    # distributional_payload_ladder_ready must never be forced True
    islands, ladder = _clean_band()
    band = R.build_payload_band_aggregate(islands, band_id="bandZ", required_islands=5,
                                          required_measured=30, ladder_sizes=ladder)
    band["distributional_payload_ladder_ready"] = True  # tamper
    ok, _ = R.validate_payload_band_aggregate(band)
    c.ok(not ok, "validator rejects distributional_payload_ladder_ready=True (serialization not observed)")
    # same_allocation_rounds passes structurally but does NOT earn distributional_evidence
    islands, ladder = _clean_band()
    band = R.build_payload_band_aggregate(islands, band_id="bandZ", required_islands=5,
                                          required_measured=30, ladder_sizes=ladder,
                                          island_independence="same_allocation_rounds")
    okv, _ = R.validate_payload_band_aggregate(band)
    c.ok(okv and band["overall_band_pass"] and band["distributional_evidence"] is False,
         "same_allocation_rounds passes structurally but not as distributional evidence")


def run_all_selftests():
    c = _Checker()
    print("exp64 Slice 0 selftest -- pure oracle + corrected-design layer:")
    _check_scalar_oracle(c)
    _check_payload_byte_oracle(c)
    _check_payload_digest_oracle(c)
    _check_s0_degeneracy(c)
    _check_ladder_defaults(c)
    _check_provenance_labels(c)
    _check_fences_and_forbidden_keys(c)
    _check_payload_gates_pure(c)
    _check_ray_payload_gates_pure(c)
    _check_native_readiness_gates_pure(c)
    _check_native_deadline_margin_gate(c)
    _check_job159418_false_positive_now_fails(c)
    _check_phase_a_labels(c)
    _check_idle_backoff_disclosure(c)
    _check_native_promptness_gate(c)
    _check_poll_half_retirement_invariant(c)
    _check_a4_progress_discriminator(c)
    _check_run1_blocked_waiter(c)
    _check_native_heartbeat_completion(c)
    _check_manifest_clean(c)
    _check_manifest_fail_closed(c)
    _check_manifest_validator_bites(c)
    _check_within_arm_stats(c)
    _check_band_clean(c)
    _check_band_fail_closed(c)
    _check_band_no_cross_arm(c)
    _check_band_validator_bites(c)
    _check_offcluster_phases_skip(c)
    total = c.passed + c.failed
    if c.failed:
        print(f"\nexp64 Slice 0 selftest FAILED: {c.failed}/{total} checks failed")
        return 1
    print(f"\nall exp64 Slice 0 selftests passed ({total} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(run_all_selftests())
