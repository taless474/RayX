"""exp62 Slice 0 selftests -- hermetic, pure Python. No Ray, no HPX, no Rostam, no external deps.

Run:  python3 selftest_slice0.py
Exits non-zero on any failed check.
"""

import sys

import run_exp62_fanout as r


_FAILS = []


def chk(name, cond):
    print(f"  [{'ok  ' if cond else 'FAIL'}] {name}")
    if not cond:
        _FAILS.append(name)


# Forbidden claim fields: artifacts/manifests/bands must never imply object store, arbitrary remote
# Python, Ray-replacement, real inference, or production/fabric semantics.
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


def _records(plan, x):
    return [{"i": i, "value": r.leaf_value(x, i), "locality": plan[i]} for i in range(len(plan))]


def _good_arm(n=4, mode="all-remote", arm="hpx"):
    root, localities = 0, [0, 1, 2]
    plan = r.build_leaf_assignments(n, localities, mode, root)
    records = _records(plan, 7)
    threads = {loc: 4 for loc in set(plan)}
    return dict(arm=arm, island_id="i1", x=7, n=n, placement_mode=mode, root_locality=root,
                localities=localities, leaf_records=records, threads_per_locality=threads,
                latency_samples_ns=[100_000 + j for j in range(1000)], dispatch_elapsed_s=0.01,
                dispatch_timeout_s=30, prewarm_count=100, expected_prewarm=100, k=1000, w=100,
                clock="perf_counter_ns", min_threads_per_locality=2)


# --------------------------------------------------------------------------- oracle

def test_oracle():
    print("oracle deterministic + order-independent:")
    x, n = 12345, 6
    chk("deterministic", r.composite_oracle(x, n) == r.composite_oracle(x, n))
    fwd = r.composite_oracle(x, n)
    rev = r.i64(sum(((x ^ r.LEAF_XOR) + (i << 1)) for i in reversed(range(n))))
    chk("order-independent", fwd == rev)
    chk("leaf depends only on x,i", r.leaf_value(x, 3) == r.i64((x ^ r.LEAF_XOR) + (3 << 1)))
    chk("int64 wrap closed", -(1 << 63) <= r.composite_oracle(2**62, 1000) < (1 << 63))
    chk("empty fanout = 0", r.composite_oracle(x, 0) == 0)


def test_witness_tally():
    print("witness tally:")
    plan = r.build_leaf_assignments(5, [0, 1, 2], "all-remote", 0)
    w = r.tally_witness(_records(plan, 3), 0)
    chk("count", w["witness_leaf_count"] == 5)
    chk("per-locality sums to N", sum(w["leaves_per_locality"].values()) == 5)
    chk("no leaves on root", w["leaves_local"] == 0)
    chk("string localities supported", r.tally_witness(
        [{"i": 0, "value": 1, "locality": "medusa01"}], "medusa00")["n_localities_covered"] == 1)


# --------------------------------------------------------------------------- placement

def test_all_remote_assignment():
    print("all-remote assignment:")
    plan = r.build_leaf_assignments(4, [0, 1, 2], "all-remote", 0)
    chk("root excluded", 0 not in plan)
    w = r.tally_witness(_records(plan, 1), 0)
    chk("leaves_local == 0", w["leaves_local"] == 0)
    chk("distribution_ok", r.evaluate_distribution_gate("all-remote", w, 4) is True)


def test_all_remote_violation():
    print("all-remote violation:")
    # Force a leaf onto the root locality.
    bad = _records(r.build_leaf_assignments(3, [0, 1, 2], "all-remote", 0), 1)
    bad.append({"i": 3, "value": r.leaf_value(1, 3), "locality": 0})
    w = r.tally_witness(bad, 0)
    chk("distribution_ok False", r.evaluate_distribution_gate("all-remote", w, 4) is False)
    chk("no remote raises", _raises(lambda: r.build_leaf_assignments(4, [0], "all-remote", 0)))
    chk("unknown mode gate False", r.evaluate_distribution_gate("bogus", w, 4) is False)


def test_round_robin():
    print("round-robin coverage:")
    plan = r.build_leaf_assignments(4, [0, 1], "round-robin", 0)
    w = r.tally_witness(_records(plan, 1), 0)
    chk("covers >= 2 localities", w["n_localities_covered"] >= 2)
    chk("distribution_ok", r.evaluate_distribution_gate("round-robin", w, 4) is True)
    chk("<2 localities raises", _raises(lambda: r.build_leaf_assignments(4, [0], "round-robin", 0)))
    chk("unknown mode raises", _raises(
        lambda: r.build_leaf_assignments(4, [0, 1], "bogus", 0)))


# --------------------------------------------------------------------------- arm gates

def test_valid_arm_passes():
    print("valid arm artifact:")
    art = r.build_arm_artifact(**_good_arm())
    chk("overall pass", art["overall"] == "pass")
    chk("all gates true", all(art["gates"].values()))
    chk("measured == oracle", art["measured_value"] == r.composite_oracle(7, 4))
    chk("provenance present", art["provenance"]["reduce_op"] == "sum_int64"
        and art["provenance"]["outer_qd1"] is True)
    chk("parcelport tcp", art["provenance"]["parcelport_transport"] == "tcp")
    chk("unknown provenance labeled", art["provenance"]["parcel_coalescing"] == "unknown")


def test_under_witnessed_fails():
    print("under-witnessed records:")
    a = _good_arm(n=4)
    a["leaf_records"] = a["leaf_records"][:3]  # only 3 of 4 leaves returned
    art = r.build_arm_artifact(**a)
    chk("leaves_dispatched_eq_n False", art["gates"]["leaves_dispatched_eq_n"] is False)
    chk("witness_leaf_count_eq_n False", art["gates"]["witness_leaf_count_eq_n"] is False)
    chk("overall fail", art["overall"] == "fail")


def test_oracle_false_fails():
    print("oracle mismatch:")
    a = _good_arm(n=4)
    a["measured_value"] = r.composite_oracle(7, 4) + 1
    art = r.build_arm_artifact(**a)
    chk("composite_oracle_correct False", art["gates"]["composite_oracle_correct"] is False)
    chk("overall fail", art["overall"] == "fail")


def test_dispatch_timeout_fails():
    print("dispatch timeout:")
    a = _good_arm()
    a["dispatch_elapsed_s"] = 45.0  # exceeds 30s budget
    art = r.build_arm_artifact(**a)
    chk("no_dispatch_timeout False", art["gates"]["no_dispatch_timeout"] is False)
    chk("overall fail", art["overall"] == "fail")


def test_thread_starved_fails():
    print("thread-starved fanout:")
    a = _good_arm()
    # set one receiving locality to 1 thread while min required is 2
    a["threads_per_locality"] = {**a["threads_per_locality"]}
    some = next(iter(a["threads_per_locality"]))
    a["threads_per_locality"][some] = 1
    art = r.build_arm_artifact(**a)
    chk("threads_cover_fanout False", art["gates"]["threads_cover_fanout"] is False)
    chk("overall fail", art["overall"] == "fail")
    # locality missing from thread map also fails closed
    a2 = _good_arm()
    a2["threads_per_locality"] = {k: v for k, v in a2["threads_per_locality"].items()
                                  if k != some}
    art2 = r.build_arm_artifact(**a2)
    chk("missing locality fails closed", art2["gates"]["threads_cover_fanout"] is False)


def test_prewarm_and_error():
    print("prewarm + error gates:")
    a = _good_arm()
    a["prewarm_count"] = 50  # expected 100
    art = r.build_arm_artifact(**a)
    chk("prewarm_correct False", art["gates"]["prewarm_correct"] is False)
    a2 = _good_arm()
    a2["error"] = "leaf crashed"
    art2 = r.build_arm_artifact(**a2)
    chk("no_error False", art2["gates"]["no_error"] is False)
    a3 = _good_arm()
    a3["measurement_boundary"] = "not_the_python_caller_boundary"
    art3 = r.build_arm_artifact(**a3)
    chk("same_boundary False", art3["gates"]["same_boundary"] is False)


# --------------------------------------------------------------------------- pair manifest

def test_valid_pair_manifest():
    print("pair manifest (Slice 3 matched island):")
    hpx_art = _synth_hpx_island(idx=1)
    ray_art = _synth_ray_island(idx=1)
    pm = r.build_pair_manifest(manifest_id="m1", x=7, n=8, ray_artifact=ray_art, hpx_artifact=hpx_art)
    if pm["overall"] != "pass":
        print("    failed gates:", [g for g, v in pm["gates"].items() if not v])
    chk("overall pass", pm["overall"] == "pass")
    chk("all gates true", all(pm["gates"].values()))
    chk("same_node_placement gate", pm["gates"]["same_node_placement"] is True)
    chk("hpx_connector_cpuset_not_collapsed", pm["gates"]["hpx_connector_cpuset_not_collapsed"])
    chk("ray_hard_placement_true", pm["gates"]["ray_hard_placement_true"] is True)
    chk("manifest same_axis_comparison False", pm["same_axis_comparison"] is False)
    # mismatched W must fail the pair manifest
    hpx_bad = _synth_hpx_island(idx=2, w=50)
    pm2 = r.build_pair_manifest(manifest_id="m2", x=7, n=8, ray_artifact=ray_art, hpx_artifact=hpx_bad)
    chk("mismatched W fails manifest", pm2["gates"]["same_k_w_prewarm_clock"] is False)
    # mismatched node_pair fails
    hpx_np = _synth_hpx_island(idx=3, node_pair=["medusa00", "medusa02"])
    pm3 = r.build_pair_manifest(manifest_id="m3", x=7, n=8, ray_artifact=ray_art, hpx_artifact=hpx_np)
    chk("mismatched node_pair fails", pm3["gates"]["same_node_pair"] is False)


# --------------------------------------------------------------------------- band

def test_band():
    print("band aggregate:")
    islands = [r.build_arm_artifact(**{**_good_arm(arm="hpx"), "island_id": f"i{j}"})
               for j in range(1, 6)]
    payload, overall = r.build_band_aggregate(arm="hpx", island_artifacts=islands,
                                              placement_mode="all-remote")
    chk("R=5 band pass", overall == "pass")
    chk("same_axis_comparison True", payload["same_axis_comparison"] is True)
    chk("across-island median present", payload["across_islands"]["p50_ns"]["median"] is not None)
    chk("all_distribution_ok gate", payload["gates"]["all_distribution_ok"] is True)
    chk("all_witness_leaf_count_eq_n gate", payload["gates"]["all_witness_leaf_count_eq_n"] is True)
    chk("all_threads_cover_fanout gate", payload["gates"]["all_threads_cover_fanout"] is True)
    # R<5 keeps same_axis_comparison False
    p4, o4 = r.build_band_aggregate(arm="hpx", island_artifacts=islands[:4],
                                    placement_mode="all-remote")
    chk("R=4 same_axis_comparison False", p4["same_axis_comparison"] is False and o4 == "fail")
    # one failed island fails the band
    failed = [*islands[:4], r.build_arm_artifact(**{**_good_arm(arm="hpx"), "island_id": "bad",
                                                    "dispatch_elapsed_s": 45.0})]
    pf, of = r.build_band_aggregate(arm="hpx", island_artifacts=failed,
                                    placement_mode="all-remote")
    chk("failed island fails band", of == "fail" and pf["same_axis_comparison"] is False)


# --------------------------------------------------------------------------- fences + claim hygiene

def test_fences_locked():
    print("hard fences locked False:")
    art = r.build_arm_artifact(**_good_arm())
    pm = r.build_pair_manifest(
        manifest_id="m", x=7, n=4,
        ray_artifact=r.build_arm_artifact(**{**_good_arm(arm="ray"), "island_id": "r"}),
        hpx_artifact=art)
    band, _ = r.build_band_aggregate(
        arm="hpx", placement_mode="all-remote",
        island_artifacts=[r.build_arm_artifact(**{**_good_arm(arm="hpx"), "island_id": f"i{j}"})
                          for j in range(5)])
    for label, obj in (("arm", art), ("pair", pm), ("band", band)):
        f = obj["fences"]
        chk(f"{label} speedup_computed False", f["speedup_computed"] is False)
        chk(f"{label} ratio_reported False", f["ratio_reported"] is False)
        chk(f"{label} arms_differenced False", f["arms_differenced"] is False)
        chk(f"{label} placement_bands_differenced False", f["placement_bands_differenced"] is False)


def test_no_claim_fields():
    print("no production/RayX-runtime claim fields:")
    art = r.build_arm_artifact(**_good_arm())
    pm = r.build_pair_manifest(
        manifest_id="m", x=7, n=4,
        ray_artifact=r.build_arm_artifact(**{**_good_arm(arm="ray"), "island_id": "r"}),
        hpx_artifact=art)
    band, _ = r.build_band_aggregate(
        arm="hpx", placement_mode="all-remote",
        island_artifacts=[r.build_arm_artifact(**{**_good_arm(arm="hpx"), "island_id": f"i{j}"})
                          for j in range(5)])
    for label, obj in (("arm", art), ("pair", pm), ("band", band)):
        keys = {k.lower() for k in _keys_deep(obj)}
        leaked = keys & _FORBIDDEN_KEYS
        chk(f"{label} no forbidden keys", not leaked)


def test_off_cluster_skip():
    print("off-cluster skip:")
    chk("no slurm -> not available", r.cluster_available(env={}) is False)
    chk("no slurm -> skip reason", r.skip_reason(env={}) is not None)
    on = {"SLURM_JOB_ID": "158734", "SLURM_JOB_NODELIST": "medusa[00-01]"}
    chk("slurm present -> available", r.cluster_available(env=on) is True)
    chk("slurm present -> no skip", r.skip_reason(env=on) is None)


# --------------------------------------------------------------------------- Slice 1 additions

def _single_locality_records(n=8, x=7, loc=0):
    return [{"i": i, "value": r.leaf_value(x, i), "locality": loc} for i in range(n)]


def test_single_locality_mode():
    print("single-locality placement mode:")
    plan = r.build_leaf_assignments(8, [0], "single-locality", 0)
    chk("all leaves on root", set(plan) == {0})
    w = r.tally_witness(_single_locality_records(8), 0)
    chk("distribution gate passes", r.evaluate_distribution_gate("single-locality", w, 8) is True)
    chk("placement_class local", r.placement_class("single-locality") == "local_mechanism_smoke")
    chk("placement_is_distributed False", r.placement_is_distributed("single-locality") is False)
    chk("all-remote is distributed", r.placement_is_distributed("all-remote") is True)


def test_local_smoke_artifact_passes():
    print("local smoke artifact (synthetic, available path):")
    recs = _single_locality_records(8)
    art = r._build_local_smoke_artifact(
        arm="hpx", x=7, n=8, records=recs, latency_samples_ns=[100 + j for j in range(30)],
        root_locality=0, threads_per_locality={0: 4}, composition_primitive="hpx::when_all",
        reduce_primitive="local_fold_sum_int64", dispatch_elapsed_s=0.01, prewarm_count=5, k=30,
        root_threads=4)
    chk("overall pass", art["overall"] == "pass")
    chk("all gates true", all(art["gates"].values()))
    chk("placement_class local_mechanism_smoke", art["placement_class"] == "local_mechanism_smoke")
    chk("measured == oracle", art["measured_value"] == r.composite_oracle(7, 8))
    chk("provenance placement_is_distributed False",
        art["provenance"]["placement_is_distributed"] is False)


def test_local_smoke_band_guard():
    print("local smoke band guard (cannot be distributed evidence):")
    islands = [r._build_local_smoke_artifact(
        arm="hpx", x=7, n=8, records=_single_locality_records(8),
        latency_samples_ns=[100 + j for j in range(30)], root_locality=0,
        threads_per_locality={0: 4}, composition_primitive="hpx::when_all",
        reduce_primitive="local_fold_sum_int64", dispatch_elapsed_s=0.01, prewarm_count=5, k=30,
        root_threads=4) for _ in range(5)]
    chk("all islands pass", all(isl["overall"] == "pass" for isl in islands))
    payload, overall = r.build_band_aggregate(arm="hpx", island_artifacts=islands,
                                              placement_mode="single-locality")
    chk("placement_is_distributed gate False", payload["gates"]["placement_is_distributed"] is False)
    chk("same_axis_comparison False", payload["same_axis_comparison"] is False)
    chk("band overall fail (guard)", overall == "fail")
    chk("band fences still locked", payload["fences"]["speedup_computed"] is False)


def test_hpx_smoke_skips_when_unbuilt():
    print("hpx-local-smoke skip when ext unbuilt:")
    def _boom():
        raise ImportError("forced: fanout_ext absent")
    status, art = r.run_hpx_local_smoke(import_fn=_boom, write=False)
    chk("status skip", status == "skip")
    chk("no artifact", art is None)


def test_ray_smoke_skips_when_absent():
    print("ray-local-smoke skip when Ray absent:")
    def _boom():
        raise ImportError("forced: ray absent")
    status, art = r.run_ray_local_smoke(import_fn=_boom, write=False)
    chk("status skip", status == "skip")
    chk("no artifact", art is None)


def test_local_smoke_no_claim_fields():
    print("local smoke artifact no forbidden claim fields:")
    art = r._build_local_smoke_artifact(
        arm="hpx", x=7, n=8, records=_single_locality_records(8),
        latency_samples_ns=[100 + j for j in range(30)], root_locality=0,
        threads_per_locality={0: 4}, composition_primitive="hpx::when_all",
        reduce_primitive="local_fold_sum_int64", dispatch_elapsed_s=0.01, prewarm_count=5, k=30,
        root_threads=4)
    leaked = {k.lower() for k in _keys_deep(art)} & _FORBIDDEN_KEYS
    chk("no forbidden keys", not leaked)
    chk("fences locked", art["fences"]["ratio_reported"] is False)


# --------------------------------------------------------------------------- Slice 2 additions

def _remote_records(n=8, x=7, remote_loc=1):
    """All-remote one-remote leaf records: every leaf executed on the single remote locality."""
    return [{"i": i, "value": r.leaf_value(x, i), "locality": remote_loc} for i in range(n)]


def _good_remote_arm(n=8, x=7, root_loc=0, remote_loc=1, connector_threads=8, timed_out=0,
                     remote_joined=1, distinct=True):
    return dict(
        arm="hpx", island_id="remote-i1", x=x, n=n, placement_mode="all-remote",
        root_locality=root_loc, localities=[root_loc, remote_loc],
        leaf_records=_remote_records(n, x, remote_loc),
        threads_per_locality={remote_loc: connector_threads},
        latency_samples_ns=[200_000 + j for j in range(50)], dispatch_elapsed_s=0.5,
        dispatch_timeout_s=30.0, prewarm_count=5, expected_prewarm=5, k=20, w=5,
        clock="perf_counter_ns", min_threads_per_locality=2,
        composition_primitive="hpx::async_per_leaf+is_ready_watchdog",
        reduce_primitive="root_fold_sum_int64", parcelport_transport="tcp",
        timed_out_leaf_count=timed_out, remote_localities_joined=remote_joined,
        localities_distinct=distinct, selected_subnet="10.42.5.", root_hostname="medusa00",
        connector_hostname="medusa01", tcp_parcelport=True, hpx_tcp_nodelay_verified=True,
        root_threads=4, connector_threads=connector_threads)


def test_one_remote_all_remote_arm():
    print("one-remote all-remote arm:")
    art = r.build_arm_artifact(**_good_remote_arm())
    chk("overall pass", art["overall"] == "pass")
    chk("distribution_ok", art["gates"]["distribution_ok"] is True)
    chk("leaves_local == 0", art["witness"]["leaves_local"] == 0)
    chk("leaves_remote == n", art["witness"]["leaves_remote"] == 8)
    chk("n_localities_covered == 1", art["witness"]["n_localities_covered"] == 1)
    chk("remote_join_ok", art["gates"]["remote_join_ok"] is True)
    chk("localities_distinct gate", art["gates"]["localities_distinct"] is True)
    chk("no_dispatch_timeout", art["gates"]["no_dispatch_timeout"] is True)
    chk("placement_class distributed", art["placement_class"] == "distributed")
    chk("nodelay recorded", art["provenance"]["hpx_tcp_nodelay_verified"] is True)
    chk("connector hostname recorded", art["provenance"]["connector_hostname"] == "medusa01")


def test_watchdog_timeout_fails():
    print("watchdog timed_out_leaf_count fails:")
    # one leaf never came back: only n-1 records, timed_out=1
    a = _good_remote_arm()
    a["leaf_records"] = a["leaf_records"][:7]
    a["timed_out_leaf_count"] = 1
    art = r.build_arm_artifact(**a)
    chk("no_dispatch_timeout False", art["gates"]["no_dispatch_timeout"] is False)
    chk("witness_leaf_count_eq_n False", art["gates"]["witness_leaf_count_eq_n"] is False)
    chk("overall fail", art["overall"] == "fail")
    chk("timed_out recorded", art["timed_out_leaf_count"] == 1)


def test_remote_gates_fail_closed():
    print("distributed placement gates fail closed:")
    a = r.build_arm_artifact(**_good_remote_arm(remote_joined=0))
    chk("remote_join_ok False when none joined", a["gates"]["remote_join_ok"] is False)
    chk("overall fail", a["overall"] == "fail")
    b = r.build_arm_artifact(**_good_remote_arm(distinct=False))
    chk("localities_distinct False when not distinct", b["gates"]["localities_distinct"] is False)
    chk("overall fail (not distinct)", b["overall"] == "fail")
    # a thread-starved connector (1 thread, N=8) fails the cover gate.
    c = r.build_arm_artifact(**_good_remote_arm(connector_threads=1))
    chk("threads_cover_fanout False (starved connector)",
        c["gates"]["threads_cover_fanout"] is False)


def test_remote_arm_single_locality_guard():
    print("remote arm cannot be a single-locality / same-axis band:")
    islands = [r.build_arm_artifact(**{**_good_remote_arm(), "island_id": f"ri{j}"})
               for j in range(5)]
    payload, overall = r.build_band_aggregate(arm="hpx", island_artifacts=islands,
                                              placement_mode="all-remote")
    # all-remote IS distributed, so the guard does not block here -- but Slice 2 never BUILDS a band.
    # We only assert the band machinery still locks fences and never emits ratios.
    chk("band fences locked", payload["fences"]["ratio_reported"] is False)
    chk("band placement_is_distributed True", payload["gates"]["placement_is_distributed"] is True)


def test_remote_smoke_skips_off_cluster():
    print("hpx-remote-smoke skips off-cluster:")
    status, art = r.run_hpx_remote_smoke(env={}, write=False)
    chk("status skip", status == "skip")
    chk("no artifact", art is None)


def test_remote_arm_no_claim_fields():
    print("remote arm no forbidden claim fields:")
    art = r.build_arm_artifact(**_good_remote_arm())
    leaked = {k.lower() for k in _keys_deep(art)} & _FORBIDDEN_KEYS
    chk("no forbidden keys", not leaked)


# --------------------------------------------------------------------------- Slice 3 additions

def _synth_hpx_island(idx=1, n=8, x=7, k=1000, w=100, prewarm=1, node_pair=None, nodelay=True,
                      cpuset=None):
    """A fully-passing synthetic HPX band island (cross-node all-remote)."""
    recs = [{"i": i, "value": r.leaf_value(x, i), "locality": 1} for i in range(n)]
    art = r.build_arm_artifact(
        arm="hpx", island_id=f"band-i{idx}-hpx", x=x, n=n, placement_mode="all-remote",
        root_locality=0, localities=[0, 1], leaf_records=recs, threads_per_locality={1: 8},
        latency_samples_ns=[200_000 + j for j in range(50)], dispatch_elapsed_s=2.0,
        dispatch_timeout_s=30.0, prewarm_count=prewarm, expected_prewarm=prewarm, k=k, w=w,
        clock="perf_counter_ns", min_threads_per_locality=8,
        composition_primitive="hpx::when_all+wait_for", reduce_primitive="root_fold_sum_int64",
        parcelport_transport="tcp", timed_out_leaf_count=0, remote_localities_joined=1,
        localities_distinct=True, selected_subnet="10.42.5.", root_hostname="medusa00",
        connector_hostname="medusa01", tcp_parcelport=True, hpx_tcp_nodelay_verified=nodelay,
        root_threads=4, connector_threads=8,
        connector_cpuset_effective=(cpuset if cpuset is not None else [0, 1, 2, 3, 4, 5, 6, 7]),
        connector_cpuset_min=8)
    art["node_pair"] = node_pair or ["medusa00", "medusa01"]
    art["node_placement"] = r.NODE_PLACEMENT_CROSS
    art["band_island"] = True
    return art


def _synth_ray_island(idx=1, n=8, x=7, k=1000, w=100, prewarm=1, node_pair=None,
                      leaves_on_target=True):
    """A fully-passing synthetic Ray band island (cross-node all-remote, hard-pinned)."""
    nid_b = "nodeB"
    recs = [{"i": i, "value": r.leaf_value(x, i), "locality": nid_b} for i in range(n)]
    art = r.build_arm_artifact(
        arm="ray", island_id=f"band-i{idx}-ray", x=x, n=n, placement_mode="all-remote",
        root_locality="nodeA", localities=["nodeA", nid_b], leaf_records=recs,
        threads_per_locality={nid_b: 8}, latency_samples_ns=[500_000 + j for j in range(50)],
        dispatch_elapsed_s=5.0, dispatch_timeout_s=None, prewarm_count=prewarm,
        expected_prewarm=prewarm, k=k, w=w, clock="perf_counter_ns",
        min_threads_per_locality=1, composition_primitive="ray.coordinator.remote",
        reduce_primitive="coordinator_fold_sum_int64", parcelport_transport="ray_object_transport",
        timed_out_leaf_count=0, remote_localities_joined=1, localities_distinct=True,
        selected_subnet="10.42.5.", root_hostname="medusa00", connector_hostname="medusa01",
        root_threads=None, connector_threads=8)
    art["gates"]["no_dispatch_timeout"] = True  # Ray has no HPX dispatch-timeout concept
    art["overall"] = "pass" if all(art["gates"].values()) else "fail"
    art["node_pair"] = node_pair or ["medusa00", "medusa01"]
    art["node_placement"] = r.NODE_PLACEMENT_CROSS
    art["band_island"] = True
    art["ray_placement"] = {"hard_placement": True, "soft": False, "single_submission": True,
                            "leaves_on_target_node": leaves_on_target, "driver_node_id": "nodeA",
                            "target_node_id": nid_b, "leaves_per_node": {nid_b: n}}
    return art


def test_synth_islands_pass():
    print("synthetic band islands pass:")
    h, ry = _synth_hpx_island(), _synth_ray_island()
    if h["overall"] != "pass":
        print("    hpx failed:", [g for g, v in h["gates"].items() if not v])
    chk("hpx island pass", h["overall"] == "pass")
    chk("hpx connector_cpuset_not_collapsed", h["gates"]["connector_cpuset_not_collapsed"] is True)
    if ry["overall"] != "pass":
        print("    ray failed:", [g for g, v in ry["gates"].items() if not v])
    chk("ray island pass", ry["overall"] == "pass")


def test_connector_cpuset_collapse_fails():
    print("connector cpuset collapse fails closed:")
    h0 = _synth_hpx_island(cpuset=[0])
    chk("[0] collapses", h0["gates"]["connector_cpuset_not_collapsed"] is False)
    h4 = _synth_hpx_island(cpuset=[0, 1, 2, 3])
    chk("4 cpus < min 8 fails", h4["gates"]["connector_cpuset_not_collapsed"] is False)


def test_combined_band_aggregate():
    print("combined band aggregate:")
    hpx = [_synth_hpx_island(idx=i) for i in range(1, 6)]
    ray = [_synth_ray_island(idx=i) for i in range(1, 6)]
    payload, overall = r.build_fanout_band_aggregate(hpx_islands=hpx, ray_islands=ray,
                                                     job_id="J", band_id="B")
    if overall != "pass":
        print("    failed:", [g for g, v in payload["gates"].items() if not v])
    chk("R=5 combined band pass", overall == "pass")
    chk("same_axis_comparison True", payload["same_axis_comparison"] is True)
    chk("has hpx_band block", "hpx_band" in payload and payload["hpx_band"]["arm"] == "hpx")
    chk("has ray_band block", "ray_band" in payload and payload["ray_band"]["arm"] == "ray")
    chk("agreement true", payload["cross_island_agreement"]["agree"] is True)
    chk("fences locked", payload["fences"] == {"speedup_computed": False, "ratio_reported": False,
                                               "arms_differenced": False,
                                               "placement_bands_differenced": False})
    chk("no separate per-arm aggregate keys", "hpx_aggregate" not in payload)


def test_band_r_below_5_fails():
    print("combined band R<5 fails closed:")
    hpx = [_synth_hpx_island(idx=i) for i in range(1, 5)]
    ray = [_synth_ray_island(idx=i) for i in range(1, 5)]
    payload, overall = r.build_fanout_band_aggregate(hpx_islands=hpx, ray_islands=ray,
                                                     job_id="J", band_id="B")
    chk("R=4 fails", overall == "fail")
    chk("same_axis_comparison False", payload["same_axis_comparison"] is False)


def test_band_disagreement_fails():
    print("combined band cross-island disagreement fails closed:")
    hpx = [_synth_hpx_island(idx=i) for i in range(1, 6)]
    ray = [_synth_ray_island(idx=i) for i in range(1, 6)]
    ray[2]["node_pair"] = ["medusa00", "medusa02"]  # one island disagrees
    payload, overall = r.build_fanout_band_aggregate(hpx_islands=hpx, ray_islands=ray,
                                                     job_id="J", band_id="B")
    chk("disagreement fails", overall == "fail")
    chk("cross_island_agreement False", payload["gates"]["cross_island_agreement"] is False)


def test_band_ray_off_target_fails():
    print("combined band ray-off-target fails closed:")
    hpx = [_synth_hpx_island(idx=i) for i in range(1, 6)]
    ray = [_synth_ray_island(idx=i) for i in range(1, 6)]
    ray[1]["ray_placement"]["leaves_on_target_node"] = False
    payload, overall = r.build_fanout_band_aggregate(hpx_islands=hpx, ray_islands=ray,
                                                     job_id="J", band_id="B")
    chk("ray off-target fails band", overall == "fail")
    chk("all_ray_hard_placement False", payload["gates"]["all_ray_hard_placement"] is False)


def test_fanout_phases_skip_off_cluster():
    print("Slice 3 fanout phases skip off-cluster:")
    hs, _ = r.run_hpx_fanout(band_id="B", island_index=1, env={}, write=False)
    chk("hpx-fanout skip", hs == "skip")
    rs, _ = r.run_ray_fanout(band_id="B", islands=5, env={}, write=False)
    chk("ray-fanout skip", rs == "skip")


def test_band_no_claim_fields():
    print("combined band no forbidden claim fields:")
    hpx = [_synth_hpx_island(idx=i) for i in range(1, 6)]
    ray = [_synth_ray_island(idx=i) for i in range(1, 6)]
    payload, _ = r.build_fanout_band_aggregate(hpx_islands=hpx, ray_islands=ray, job_id="J",
                                               band_id="B")
    leaked = {k.lower() for k in _keys_deep(payload)} & _FORBIDDEN_KEYS
    chk("no forbidden keys", not leaked)


def test_ray_arm_zero_cpu_coordinator_config():
    # Hermetic (no Ray) guard locking the intended Ray-arm placement contract: the head advertises
    # 0 CPUs (node A is control-plane only) and the coordinator is submitted with num_cpus=0 so it can
    # schedule on the 0-CPU head while staying HARD-pinned (soft=False) to node A; leaves stay hard-
    # pinned to node B; and the coordinator ray.get is BOUNDED so a placement failure fails closed
    # instead of hanging to the SLURM timeout. Regressing any of these breaks this test.
    import inspect
    src = inspect.getsource(r.run_ray_fanout)
    print("ray arm: 0-CPU head + num_cpus=0 coordinator config:")
    chk("head_num_cpus = 0", "head_num_cpus = 0" in src)
    chk("head srun uses head_num_cpus", 'str(head_num_cpus)' in src)
    chk("coord_num_cpus = 0", "coord_num_cpus = 0" in src)
    chk("coordinator submitted with num_cpus=coord_num_cpus", "num_cpus=coord_num_cpus" in src)
    chk("coordinator hard-pinned to node A (soft=False)",
        "NodeAffinitySchedulingStrategy(node_id=nid_a, soft=False)" in src)
    chk("leaves hard-pinned to node B (soft=False)",
        "NodeAffinitySchedulingStrategy(node_id=target_nid, soft=False)" in src)
    chk("coordinator ray.get is bounded (no hang)", "timeout=ray_dispatch_timeout_s" in src)
    chk("GetTimeoutError handled (fail closed)", "except GetTimeoutError" in src)
    # The CLI must expose the bounded-dispatch knob with a finite default.
    chk("ray-dispatch-timeout default finite",
        r.run_ray_fanout.__kwdefaults__.get("ray_dispatch_timeout_s", 0) > 0)


# --------------------------------------------------------------------------- Slice 4a multi-remote

def _conn(ok=True):
    """A synthetic healthy connector lifecycle/attest record for multi_remote_gates."""
    return {"cpuset_ok": ok, "tcp_nodelay_verified": ok,
            "joined": ok, "served": ok, "graceful_disconnect": ok}


def test_multi_remote_distribution_witness():
    print("Slice 4a multi-remote distribution witness (all-remote round-robin, >=2 remotes):")
    plan = r.build_leaf_assignments(8, [0, 1, 2], "all-remote", 0)  # root 0 runs none; remotes 1,2
    w = r.tally_witness(_records(plan, 7), 0)
    chk("leaves_local == 0", w["leaves_local"] == 0)
    chk("leaves_remote == N", w["leaves_remote"] == 8)
    chk("n_localities_covered == 2", w["n_localities_covered"] == 2)
    chk("all-remote distribution_ok", r.evaluate_distribution_gate("all-remote", w, 8) is True)
    chk("threads cover both remotes",
        r.evaluate_threads_cover_fanout(w["leaves_per_locality"], {1: 8, 2: 8}, 8) is True)


def test_multi_remote_gates_pass():
    print("Slice 4a gates pass: >=2 covered remotes + two healthy connectors:")
    w = r.tally_witness(_records(r.build_leaf_assignments(8, [0, 1, 2], "all-remote", 0), 7), 0)
    g = r.multi_remote_gates(w, [1, 2], [_conn(), _conn()])
    chk("all multi-remote gates true", all(g.values()))
    chk("n_remote_localities_ge_2", g["n_remote_localities_ge_2"] is True)
    chk("leaves_per_remote_locality_covers_all", g["leaves_per_remote_locality_covers_all"] is True)


def test_multi_remote_one_remote_fails_closed():
    print("Slice 4a fails closed when only one remote joins:")
    w = r.tally_witness(_records(r.build_leaf_assignments(8, [0, 1], "all-remote", 0), 7), 0)
    g = r.multi_remote_gates(w, [1], [_conn()])
    chk("n_remote_localities_ge_2 False", g["n_remote_localities_ge_2"] is False)
    chk("covers_all False (needs >=2)", g["leaves_per_remote_locality_covers_all"] is False)
    chk("cpuset gate needs >=2 connectors", g["all_connector_cpuset_not_collapsed"] is False)


def test_multi_remote_uncovered_remote_fails_closed():
    print("Slice 4a fails closed when a declared remote receives no leaves:")
    recs = [{"i": i, "value": r.leaf_value(7, i), "locality": 1} for i in range(8)]  # all on loc 1
    w = r.tally_witness(recs, 0)
    g = r.multi_remote_gates(w, [1, 2], [_conn(), _conn()])
    chk("covers_all False", g["leaves_per_remote_locality_covers_all"] is False)
    chk("n_remote_localities_ge_2 True", g["n_remote_localities_ge_2"] is True)


def test_multi_remote_connector_gates_fail_closed():
    print("Slice 4a per-connector cpuset / nodelay / lifecycle gates fail closed:")
    w = r.tally_witness(_records(r.build_leaf_assignments(8, [0, 1, 2], "all-remote", 0), 7), 0)
    chk("collapsed cpuset fails",
        r.multi_remote_gates(w, [1, 2], [_conn(), {**_conn(), "cpuset_ok": False}])
        ["all_connector_cpuset_not_collapsed"] is False)
    chk("missing graceful disconnect fails",
        r.multi_remote_gates(w, [1, 2], [_conn(), {**_conn(), "graceful_disconnect": False}])
        ["all_connector_lifecycle_ok"] is False)
    chk("missing nodelay fails",
        r.multi_remote_gates(w, [1, 2], [_conn(), {**_conn(), "tcp_nodelay_verified": False}])
        ["all_connector_nodelay_true"] is False)
    chk("not-joined fails lifecycle",
        r.multi_remote_gates(w, [1, 2], [_conn(), {**_conn(), "joined": False}])
        ["all_connector_lifecycle_ok"] is False)


def test_multi_remote_cpuset_helper():
    print("Slice 4a connector cpuset helper (>=8 distinct, not [0]):")
    chk("8 distinct ok", r._connector_cpuset_ok([0, 2, 4, 6, 8, 10, 12, 14], 8) is True)
    chk("[0] collapses", r._connector_cpuset_ok([0], 8) is False)
    chk("4 < min 8 fails", r._connector_cpuset_ok([0, 1, 2, 3], 8) is False)


def test_multi_remote_not_same_axis():
    print("Slice 4a is HPX-only: arm artifact never sets same_axis, fences stay locked:")
    recs = _records(r.build_leaf_assignments(8, [0, 1, 2], "all-remote", 0), 7)
    art = r.build_arm_artifact(
        arm="hpx", island_id="m", x=7, n=8, placement_mode="all-remote", root_locality=0,
        localities=[0, 1, 2], leaf_records=recs, threads_per_locality={1: 8, 2: 8},
        latency_samples_ns=[100 + j for j in range(1000)], dispatch_elapsed_s=0.01,
        dispatch_timeout_s=30, prewarm_count=5, expected_prewarm=5, k=20, w=5,
        clock="perf_counter_ns", composition_primitive="root_flat_gather_reduce")
    chk("arm same_axis_comparison False", art["same_axis_comparison"] is False)
    chk("composition_primitive labelled", art["provenance"]["composition_primitive"]
        == "root_flat_gather_reduce")
    chk("fences locked", art["fences"] == {"speedup_computed": False, "ratio_reported": False,
                                           "arms_differenced": False,
                                           "placement_bands_differenced": False})


def test_multi_remote_phase_skips_off_cluster():
    print("Slice 4a hpx-multi-remote-smoke skips off-cluster:")
    status, art = r.run_hpx_multi_remote_smoke(env={}, write=False)
    chk("status skip", status == "skip")
    chk("no artifact", art is None)


# --------------------------------------------------------------------------- pre-Slice-4b spike

def test_composition_provenance_native_and_poll():
    print("composition provenance: native and interim-poll fields both consistent:")
    nat = r.composition_provenance(
        composition_primitive="when_all_then_reduce", watchdog="composed_future_wait_for",
        ran_on_hpx_thread=True, polled_in_success_path=False, hpx_native_composition=True)
    chk("native consistent", nat["composition_provenance_consistent"] is True)
    chk("native flag True", nat["hpx_native_composition"] is True)
    chk("native not polled", nat["polled_in_success_path"] is False)
    chk("native watchdog recorded", nat["watchdog"] == "composed_future_wait_for")
    # status fields: native is experimental / cross-node UNvalidated; poll is the proven fallback.
    chk("native experimental", nat["native_mode_experimental"] is True)
    chk("native cross-node NOT validated", nat["cross_node_composition_validated"] is False)
    chk("native points at proven fallback",
        nat["proven_cross_node_composition_mode"] == "root_flat_gather_poll")
    poll = r.composition_provenance(
        composition_primitive="root_flat_gather_reduce", watchdog="bounded_is_ready_poll_50us",
        ran_on_hpx_thread=True, polled_in_success_path=True, hpx_native_composition=False)
    chk("poll consistent", poll["composition_provenance_consistent"] is True)
    chk("poll not native", poll["hpx_native_composition"] is False)
    chk("poll polled True", poll["polled_in_success_path"] is True)
    chk("poll not experimental", poll["native_mode_experimental"] is False)
    chk("poll cross-node validated", poll["cross_node_composition_validated"] is True)


def test_composition_provenance_fail_closed():
    print("composition provenance fails closed on inconsistency:")
    # HPX-native must NOT poll in the success path.
    bad1 = r.composition_provenance(
        composition_primitive="when_all_then_reduce", watchdog="composed_future_wait_for",
        ran_on_hpx_thread=True, polled_in_success_path=True, hpx_native_composition=True)
    chk("native+polled inconsistent", bad1["composition_provenance_consistent"] is False)
    # composition must run on an HPX thread.
    bad2 = r.composition_provenance(
        composition_primitive="when_all_then_reduce", watchdog="composed_future_wait_for",
        ran_on_hpx_thread=False, polled_in_success_path=False, hpx_native_composition=True)
    chk("not-hpx-thread inconsistent", bad2["composition_provenance_consistent"] is False)
    # labels must be non-empty strings.
    bad3 = r.composition_provenance(
        composition_primitive="when_all_then_reduce", watchdog="",
        ran_on_hpx_thread=True, polled_in_success_path=False, hpx_native_composition=True)
    chk("empty watchdog inconsistent", bad3["composition_provenance_consistent"] is False)


def test_composition_gate_applied_to_arm():
    print("composition gate merges into HPX arm; fences stay locked; native-but-polled fails closed:")
    art = r.build_arm_artifact(**_good_remote_arm())
    r._apply_composition_provenance(
        art, composition_primitive="when_all_then_reduce", watchdog="composed_future_wait_for",
        ran_on_hpx_thread=True, polled_in_success_path=False, hpx_native_composition=True)
    chk("gate present True", art["gates"]["composition_provenance_consistent"] is True)
    chk("prov hpx_native True", art["provenance"]["hpx_native_composition"] is True)
    chk("prov polled False", art["provenance"]["polled_in_success_path"] is False)
    chk("prov watchdog recorded", art["provenance"]["watchdog"] == "composed_future_wait_for")
    chk("still not same_axis", art["same_axis_comparison"] is False)
    chk("fences locked", r._fences_locked(art["fences"]))
    chk("overall still pass", art["overall"] == "pass")
    # a native mode that (bug) reports success-path polling must fail the arm closed.
    bad = r.build_arm_artifact(**_good_remote_arm())
    r._apply_composition_provenance(
        bad, composition_primitive="when_all_then_reduce", watchdog="composed_future_wait_for",
        ran_on_hpx_thread=True, polled_in_success_path=True, hpx_native_composition=True)
    chk("bad gate False", bad["gates"]["composition_provenance_consistent"] is False)
    chk("bad overall fail", bad["overall"] == "fail")


def test_local_smoke_native_mode_skips_when_unbuilt():
    print("hpx-local-smoke native composition mode skips cleanly when ext is unbuilt:")
    def _raise():
        raise ImportError("fanout_ext not built")
    status, art = r.run_hpx_local_smoke(composition_mode="when_all_then_reduce",
                                        import_fn=_raise, write=False)
    chk("status skip", status == "skip")
    chk("no artifact", art is None)


def test_experimental_composition_guard_warns():
    print("experimental cross-node native modes warn; proven poll mode is silent:")
    import io
    import contextlib
    def _emit(mode):
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            r._warn_experimental_cross_node_composition(mode, "hpx-multi-remote-smoke")
        return buf.getvalue()
    chk("when_all_then_reduce warns", "EXPERIMENTAL" in _emit("when_all_then_reduce"))
    chk("dataflow_reduce warns", "EXPERIMENTAL" in _emit("dataflow_reduce"))
    chk("root_flat_gather_poll silent", _emit("root_flat_gather_poll") == "")
    chk("proven fallback named in constant",
        r.PROVEN_CROSS_NODE_COMPOSITION_MODE == "root_flat_gather_poll")


# --------------------------------------------------------------------------- Slice 4b multi-remote

def _mr_connectors(hosts=("medusa01", "medusa02"), locs=(1, 2)):
    return [{"hostname": h, "locality_id": loc,
             "connector_cpuset_effective": [0, 2, 4, 6, 8, 10, 12, 14], "cpuset_ok": True,
             "transport": "tcp", "tcp_nodelay_verified": True, "joined": True, "served": True,
             "graceful_disconnect": True} for h, loc in zip(hosts, locs)]


def _synth_hpx_mr_island(idx=1, n=8, x=7, k=1000, w=100, prewarm=1,
                         node_set=("medusa00", "medusa01", "medusa02"), remote_locs=(1, 2), job="J1"):
    """Fully-passing synthetic HPX multi-remote band island (proven poll composition)."""
    recs = _records(r.build_leaf_assignments(n, [0, *remote_locs], "all-remote", 0), x)
    art = r.build_arm_artifact(
        arm="hpx", island_id=f"mrband-i{idx}-hpx", x=x, n=n, placement_mode="all-remote",
        root_locality=0, localities=[0, *remote_locs], leaf_records=recs,
        threads_per_locality={loc: 8 for loc in remote_locs},
        latency_samples_ns=[200_000 + j for j in range(50)], dispatch_elapsed_s=2.0,
        dispatch_timeout_s=30.0, prewarm_count=prewarm, expected_prewarm=prewarm, k=k, w=w,
        clock="perf_counter_ns", min_threads_per_locality=8,
        composition_primitive="root_flat_gather_reduce", reduce_primitive="root_fold_sum_int64",
        parcelport_transport="tcp", timed_out_leaf_count=0,
        remote_localities_joined=len(remote_locs), localities_distinct=True,
        selected_subnet="10.42.5.", root_hostname=node_set[0], connector_hostname=None,
        tcp_parcelport=True, hpx_tcp_nodelay_verified=True, root_threads=4, connector_threads=8,
        connector_cpuset_min=8)
    conns = _mr_connectors(hosts=node_set[1:], locs=remote_locs)
    art["gates"].update(r.multi_remote_gates(art["witness"], list(remote_locs), conns))
    r._apply_composition_provenance(
        art, composition_primitive="root_flat_gather_reduce", watchdog="bounded_is_ready_poll_50us",
        ran_on_hpx_thread=True, polled_in_success_path=True, hpx_native_composition=False)
    art["node_set"] = list(node_set)
    art["node_placement"] = r.NODE_PLACEMENT_MULTI
    art["remote_locality_ids"] = list(remote_locs)
    art["n_remote_localities"] = len(remote_locs)
    art["connectors"] = conns
    art["slurm_job_id"] = job
    art["band_island"] = True
    return art


def _synth_ray_mr_island(idx=1, n=8, x=7, k=1000, w=100, prewarm=1,
                         node_set=("medusa00", "medusa01", "medusa02"), remote_nids=("B", "C"),
                         coord_nid="A", job="J1", hard=True, connected=True, leaves_per_node=None):
    """Fully-passing synthetic Ray multi-remote band island (coordinator + round-robin leaves)."""
    nid_a = coord_nid
    plan = [remote_nids[i % len(remote_nids)] for i in range(n)]
    recs = [{"i": i, "value": r.leaf_value(x, i), "locality": plan[i]} for i in range(n)]
    art = r.build_arm_artifact(
        arm="ray", island_id=f"mrband-i{idx}-ray", x=x, n=n, placement_mode="all-remote",
        root_locality=nid_a, localities=[nid_a, *remote_nids], leaf_records=recs,
        threads_per_locality={nid: 8 for nid in remote_nids},
        latency_samples_ns=[300_000 + j for j in range(50)], dispatch_elapsed_s=2.0,
        dispatch_timeout_s=None, prewarm_count=prewarm, expected_prewarm=prewarm, k=k, w=w,
        clock="perf_counter_ns", min_threads_per_locality=1,
        composition_primitive="ray.coordinator.remote", reduce_primitive="coordinator_fold_sum_int64",
        parcelport_transport="ray_object_transport", timed_out_leaf_count=0,
        remote_localities_joined=len(remote_nids), localities_distinct=True,
        selected_subnet="10.42.5.", root_hostname=node_set[0], connector_hostname=None,
        root_threads=None, connector_threads=8)
    per_node = dict(leaves_per_node) if leaves_per_node is not None else {}
    if leaves_per_node is None:
        for rec in recs:
            per_node[rec["locality"]] = per_node.get(rec["locality"], 0) + 1
    art["gates"].update(r.ray_multi_remote_gates(
        witness=art["witness"], remote_node_ids=list(remote_nids), leaves_per_node=per_node,
        coordinator_node_id=coord_nid, driver_node_id=nid_a, head_num_cpus=0, coordinator_num_cpus=0,
        hard_placement=hard, cluster_connected=connected, n=n))
    art["gates"]["no_dispatch_timeout"] = True
    art["overall"] = "pass" if all(art["gates"].values()) else "fail"
    art["node_set"] = list(node_set)
    art["node_placement"] = r.NODE_PLACEMENT_MULTI
    art["slurm_job_id"] = job
    art["n_remote_localities"] = len(remote_nids)
    art["ray_placement"] = {"hard_placement": hard, "coordinator_node_id": coord_nid,
                            "coordinator_on_driver_node": coord_nid == nid_a,
                            "leaves_per_node": per_node, "target_node_ids": list(remote_nids)}
    art["band_island"] = True
    return art


def _ray_mr_gate_kwargs(**over):
    base = dict(witness={"leaves_local": 0, "leaves_remote": 8}, remote_node_ids=["B", "C"],
                leaves_per_node={"B": 4, "C": 4}, coordinator_node_id="A", driver_node_id="A",
                head_num_cpus=0, coordinator_num_cpus=0, hard_placement=True, cluster_connected=True,
                n=8)
    base.update(over)
    return base


def test_ray_multi_remote_gates_pass():
    print("Slice 4b ray multi-remote gates pass on a good round-robin:")
    g = r.ray_multi_remote_gates(**_ray_mr_gate_kwargs())
    chk("all ray mr gates True", all(g.values()))
    for name in ("ray_n_remote_nodes_ge_2", "distinct_remote_node_ids", "coordinator_on_driver_node",
                 "coordinator_runs_zero_leaves", "ray_hard_placement",
                 "ray_leaves_cover_all_remote_nodes"):
        chk(f"{name} True", g[name] is True)


def test_ray_multi_remote_gates_fail_closed():
    print("Slice 4b ray multi-remote gates fail closed:")
    one = r.ray_multi_remote_gates(**_ray_mr_gate_kwargs(remote_node_ids=["B"],
                                                         leaves_per_node={"B": 8}))
    chk("one remote -> ge_2 False", one["ray_n_remote_nodes_ge_2"] is False)
    chk("one remote -> covers_all False", one["ray_leaves_cover_all_remote_nodes"] is False)
    unc = r.ray_multi_remote_gates(**_ray_mr_gate_kwargs(leaves_per_node={"B": 8}))
    chk("uncovered remote -> covers_all False", unc["ray_leaves_cover_all_remote_nodes"] is False)
    cl = r.ray_multi_remote_gates(**_ray_mr_gate_kwargs(leaves_per_node={"A": 1, "B": 4, "C": 3}))
    chk("coordinator runs leaves -> zero_leaves False", cl["coordinator_runs_zero_leaves"] is False)
    nd = r.ray_multi_remote_gates(**_ray_mr_gate_kwargs(coordinator_node_id="X"))
    chk("coordinator off driver -> on_driver False", nd["coordinator_on_driver_node"] is False)
    hp = r.ray_multi_remote_gates(**_ray_mr_gate_kwargs(hard_placement=False))
    chk("soft placement -> hard False", hp["ray_hard_placement"] is False)
    hc = r.ray_multi_remote_gates(**_ray_mr_gate_kwargs(head_num_cpus=1))
    chk("head cpus!=0 -> head_zero False", hc["ray_head_num_cpus_zero"] is False)
    cc = r.ray_multi_remote_gates(**_ray_mr_gate_kwargs(coordinator_num_cpus=1))
    chk("coord cpus!=0 -> coord_zero False", cc["ray_coordinator_num_cpus_zero"] is False)


def test_multi_remote_pair_manifest_pass():
    print("Slice 4b multi-remote pair manifest passes; same_axis locked False; fences locked:")
    hpx = _synth_hpx_mr_island()
    ray = _synth_ray_mr_island()
    man = r.build_pair_manifest_multi_remote(manifest_id="mrband-i1", x=7, n=8,
                                             ray_artifact=ray, hpx_artifact=hpx)
    chk("both arms pass", hpx["overall"] == "pass" and ray["overall"] == "pass")
    chk("manifest overall pass", man["overall"] == "pass")
    chk("same_node_set gate", man["gates"]["same_node_set"] is True)
    chk("same_node_placement multi", man["gates"]["same_node_placement"] is True)
    chk("hpx covers all remote localities", man["gates"]["hpx_leaves_cover_all_remote_localities"])
    chk("ray covers all remote nodes", man["gates"]["ray_leaves_cover_all_remote_nodes"])
    chk("manifest same_axis False", man["same_axis_comparison"] is False)
    chk("manifest fences locked", r._fences_locked(man["fences"]))


def test_multi_remote_node_set_disagreement_fails():
    print("Slice 4b multi-remote manifest fails on node_set disagreement:")
    hpx = _synth_hpx_mr_island()
    ray = _synth_ray_mr_island(node_set=("medusa00", "medusa01", "medusa03"))
    man = r.build_pair_manifest_multi_remote(manifest_id="mrband-i1", x=7, n=8,
                                             ray_artifact=ray, hpx_artifact=hpx)
    chk("same_node_set False", man["gates"]["same_node_set"] is False)
    chk("manifest overall fail", man["overall"] == "fail")


def test_multi_remote_aggregate_pass_r5():
    print("Slice 4b multi-remote aggregate passes at R=5; same_axis True; fences locked:")
    hpx = [_synth_hpx_mr_island(idx=i) for i in range(1, 6)]
    ray = [_synth_ray_mr_island(idx=i) for i in range(1, 6)]
    payload, overall = r.build_fanout_band_aggregate_multi_remote(
        hpx_islands=hpx, ray_islands=ray, job_id="J1", band_id="mrband-J1")
    chk("aggregate overall pass", overall == "pass")
    chk("same_axis True", payload["same_axis_comparison"] is True)
    chk("cross_island_agreement", payload["gates"]["cross_island_agreement"] is True)
    chk("node_placement_is_multi_remote", payload["gates"]["node_placement_is_multi_remote"] is True)
    chk("fences locked", r._fences_locked(payload["fences"]))
    chk("no ratio/speedup keys leaked", not ({k.lower() for k in _keys_deep(payload)}
                                             & (_FORBIDDEN_KEYS - {"ratio_reported",
                                                                   "speedup_computed"})))


def test_multi_remote_aggregate_r_below_5_fails():
    print("Slice 4b multi-remote aggregate fails below R=5:")
    hpx = [_synth_hpx_mr_island(idx=i) for i in range(1, 5)]
    ray = [_synth_ray_mr_island(idx=i) for i in range(1, 5)]
    payload, overall = r.build_fanout_band_aggregate_multi_remote(
        hpx_islands=hpx, ray_islands=ray, job_id="J1", band_id="mrband-J1")
    chk("r_at_least_5 False", payload["gates"]["r_at_least_5"] is False)
    chk("same_axis False", payload["same_axis_comparison"] is False)


def test_multi_remote_same_axis_false_until_all_pass():
    print("Slice 4b multi-remote same_axis stays False if any island/gate fails:")
    hpx = [_synth_hpx_mr_island(idx=i) for i in range(1, 6)]
    ray = [_synth_ray_mr_island(idx=i) for i in range(1, 6)]
    ray[2] = _synth_ray_mr_island(idx=3, hard=False)  # one soft-placement island
    payload, overall = r.build_fanout_band_aggregate_multi_remote(
        hpx_islands=hpx, ray_islands=ray, job_id="J1", band_id="mrband-J1")
    chk("one soft island -> overall fail", overall == "fail")
    chk("same_axis False", payload["same_axis_comparison"] is False)


def test_multi_remote_phases_skip_off_cluster():
    print("Slice 4b band phases skip / fail cleanly off-cluster:")
    hs, _ = r.run_hpx_multi_remote_fanout(band_id="mrband-x", island_index=1, env={},
                                          import_fn=lambda: (_ for _ in ()).throw(ImportError("x")),
                                          write=False)
    chk("hpx-multi-remote-fanout skip", hs == "skip")
    rs, _ = r.run_ray_multi_remote_fanout(band_id="mrband-x", env={}, write=False)
    chk("ray-multi-remote-fanout skip", rs == "skip")
    ov, _ = r.phase_pair_manifest_multi_remote(band_id="mrband-none", islands=5, write=False,
                                               exp_dir="/tmp/exp62_no_such_dir")
    chk("pair-manifest-multi-remote fail on missing files", ov == "fail")


def _raises(fn):
    try:
        fn()
        return False
    except Exception:
        return True


def main():
    tests = [test_oracle, test_witness_tally, test_all_remote_assignment, test_all_remote_violation,
             test_round_robin, test_valid_arm_passes, test_under_witnessed_fails,
             test_oracle_false_fails, test_dispatch_timeout_fails, test_thread_starved_fails,
             test_prewarm_and_error, test_valid_pair_manifest, test_band, test_fences_locked,
             test_no_claim_fields, test_off_cluster_skip,
             test_single_locality_mode, test_local_smoke_artifact_passes,
             test_local_smoke_band_guard, test_hpx_smoke_skips_when_unbuilt,
             test_ray_smoke_skips_when_absent, test_local_smoke_no_claim_fields,
             test_one_remote_all_remote_arm, test_watchdog_timeout_fails,
             test_remote_gates_fail_closed, test_remote_arm_single_locality_guard,
             test_remote_smoke_skips_off_cluster, test_remote_arm_no_claim_fields,
             test_synth_islands_pass, test_connector_cpuset_collapse_fails,
             test_combined_band_aggregate, test_band_r_below_5_fails, test_band_disagreement_fails,
             test_band_ray_off_target_fails, test_fanout_phases_skip_off_cluster,
             test_band_no_claim_fields, test_ray_arm_zero_cpu_coordinator_config,
             test_multi_remote_distribution_witness, test_multi_remote_gates_pass,
             test_multi_remote_one_remote_fails_closed, test_multi_remote_uncovered_remote_fails_closed,
             test_multi_remote_connector_gates_fail_closed, test_multi_remote_cpuset_helper,
             test_multi_remote_not_same_axis, test_multi_remote_phase_skips_off_cluster,
             test_composition_provenance_native_and_poll, test_composition_provenance_fail_closed,
             test_composition_gate_applied_to_arm, test_local_smoke_native_mode_skips_when_unbuilt,
             test_experimental_composition_guard_warns,
             test_ray_multi_remote_gates_pass, test_ray_multi_remote_gates_fail_closed,
             test_multi_remote_pair_manifest_pass, test_multi_remote_node_set_disagreement_fails,
             test_multi_remote_aggregate_pass_r5, test_multi_remote_aggregate_r_below_5_fails,
             test_multi_remote_same_axis_false_until_all_pass,
             test_multi_remote_phases_skip_off_cluster]
    for t in tests:
        t()
    print()
    if _FAILS:
        print(f"FAILED ({len(_FAILS)}): {', '.join(_FAILS)}")
        return 1
    print(f"all Slice 0 selftests passed ({len(tests)} groups)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
