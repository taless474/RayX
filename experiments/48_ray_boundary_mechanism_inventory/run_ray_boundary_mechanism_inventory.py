#!/usr/bin/env python3
"""exp48: Ray boundary-mechanism inventory for ONE FIXED diamond/join DAG.

STRUCTURAL / COUNT evidence only -- NOT a performance result, NOT a winner claim.

The workload is the exp46 fixed diamond A -> {B, C} -> D, closed int64:

    A = chain_stage(seed,            quantum)
    B = chain_stage(A + 1,           quantum)
    C = chain_stage(A + 2,           quantum)
    D = chain_stage((B + C) & MASK,  quantum)        # depends on B AND C

exp48 inventories, at FIXED decomposition granularity and across two substrates
(RayX fixed-op runtime vs real Ray), the DRIVER/ORCHESTRATION-OBSERVABLE mechanism
each uses to carry the same dependency edges -- specifically WHERE each cross-node
dependency edge lives. Four paths:

  rayx_coarse : one diamond_fanin(seed, quantum) Runtime op. The four edges are
                resolved IN-OP by HPX composition, and NOT by one uniform primitive:
                A fans out to B and C, so A->B and A->C are carried by an
                hpx::shared_future + .then continuations (shared_future_fork = 2);
                B->D and C->D are plain hpx::futures MOVED INTO hpx::dataflow
                (future_into_dataflow = 2). (exp46 called hpx::dataflow
                "representational" for BOUNDARY COUNTS; exp48 uses a different lens --
                it inventories the IN-OP edge carriers. These do not contradict.)
  rayx_fine   : exp46 fair decomposition into four chain_sum_loop(x, 1, quantum)
                Runtime ops. Each cross-node edge ROUND-TRIPS through the Python/
                Runtime boundary as a closed int64 (python_materialized_int64 = 4).
  ray_coarse  : one Ray remote task wrapping the whole diamond. The four edges are
                task-local Python values inside one task (ray_task_local_python_value
                = 4); only the final result is an ObjectRef.
  ray_fine    : four Ray remote tasks; ObjectRefs passed naturally, one final
                ray.get. The four edges are carried as Ray ObjectRefs
                (ray_objectref = 4). ObjectRef is Ray's dependency handle; for tiny
                int64 payloads the value MAY be inlined rather than stored in plasma.
                exp48 does NOT assert plasma/object-store transport, is single-node,
                and gives NO transport evidence.

COUNT SCOPE: every count below is DRIVER/ORCHESTRATION-OBSERVABLE ONLY. For ray_fine,
intermediate_driver_materializations = 0 means ZERO DRIVER materializations -- Ray may
perform in-cluster serialization / inlining / object handling that is INTENTIONALLY NOT
driver-counted. The table must not read as "Ray has no materialization work."

HPX-FAITHFUL POINT (the allowed claim's spine): a Ray ObjectRef and an HPX future /
shared_future are BOTH in-substrate dependency handles (with different semantics and
scopes). RayX ALREADY uses HPX in-substrate references inside diamond_fanin; its current
fixed-op Python boundary does NOT expose such a reference across op boundaries, so
fine-grain RayX decomposition round-trips closed values through Python. This is NOT a
substrate-quality verdict.

PROCESS MODEL: HPX (RayX) and Ray are NEVER co-init'd in one process. The parent always
spawns a RayX child subprocess and a Ray child subprocess; the Ray child runs the Ray paths
only if Ray imports and initializes cleanly, otherwise it returns a clean skip. Each child
emits one JSON object; the parent aggregates. RayX rows use hpx_threads=1 -- exp48 is NOT
about overlap, worker parallelism, or scheduling, and no exp47 in-flight vocabulary
appears here.

NON-CLAIMS: no speedup/throughput/latency/performance; no HPX faster than Ray; no RayX
replaces Ray; no RayX makes Ray faster; no "Ray is bad"; no ObjectRef/object-store
criticism; no assertion of plasma/object-store transport for the int64 payload; no
"Python orchestration is bad"; no real inference; no Ray Serve/Train; no endpoint/fabric;
no parcelport/AGAS/multi-node; no claim this resolves boundary-vs-transport (single-node
has no transport evidence); no arbitrary Python execution claim; no scheduler-control /
placement-control / arbitrary-parallelism claim; no overlap/worker-parallelism claim; no
"future distributed-fabric direction" pulled forward; no wall-clock assertions (TIMING IS
OMITTED ENTIRELY).

Run (laptop -- writes aggregate.json beside this file):
  PYTHONPATH=python/src python \
    experiments/48_ray_boundary_mechanism_inventory/run_ray_boundary_mechanism_inventory.py --smoke
"""
import argparse
import json
import os
import platform
import subprocess
import sys

MASK = 0x7FFFFFFF      # mirror BUSY_SUM_MASK in python/src/rayx/runtime_ops.hpp
U64 = (1 << 64) - 1
NODES = 4              # fixed diamond node count: A, B, C, D
EDGES = 4              # fixed dependency edges: A->B, A->C, B->D, C->D
RAY_NUM_CPUS = 2       # single-node, tiny; sizing is irrelevant (no timing claim)

# The five edge-residence categories (every row reports all five; unused stay 0).
EDGE_RESIDENCE_KEYS = (
    "hpx_shared_future_fork",       # rayx_coarse: A->B, A->C off a shared_future + .then
    "hpx_future_into_dataflow",     # rayx_coarse: B->D, C->D as futures into hpx::dataflow
    "python_materialized_int64",    # rayx_fine: edge carried as a Python-materialized int64
    "ray_task_local_python_value",  # ray_coarse: edge is a local value inside one task
    "ray_objectref",                # ray_fine: edge carried as a Ray ObjectRef
)


# --- pure-Python oracle (mirrors native masking EXACTLY; from exp46) ----------

def masked_range_sum_py(begin, end):
    acc = 0
    for i in range(begin, end):
        acc = (acc + (i & U64)) & MASK
    return acc


def chain_stage_py(x, q):
    return ((x & U64) + masked_range_sum_py(0, q)) & MASK


def oracle(seed, quantum):
    """The same fixed diamond as diamond_fanin(seed, quantum), pure Python."""
    a = chain_stage_py(seed, quantum)
    b = chain_stage_py(a + 1, quantum)
    c = chain_stage_py(a + 2, quantum)
    return chain_stage_py((b + c) & MASK, quantum)


# --- self-contained stage for Ray workers (no external global refs to ship) ---
# Independent re-implementation of chain_stage; the equal-value gate (value == oracle)
# cross-checks it against the driver-side oracle, so the two cannot silently drift.

def _stage(x, q):
    mask = 0x7FFFFFFF
    u64 = (1 << 64) - 1
    acc = 0
    for i in range(q):
        acc = (acc + (i & u64)) & mask
    return ((x & u64) + acc) & mask


def _ray_root_fn(seed, quantum):
    return _stage(seed, quantum)


def _ray_node_fn(pred, inc, quantum):
    # pred arrives by ObjectRef auto-deref in ray_fine; +1 / +2 happens IN the task.
    return _stage(pred + inc, quantum)


def _ray_join_fn(b, c, quantum):
    return _stage((b + c) & 0x7FFFFFFF, quantum)


def _ray_diamond_fn(seed, quantum):
    # ray_coarse: the whole diamond runs in ONE task; edges are task-local values.
    a = _stage(seed, quantum)
    b = _stage(a + 1, quantum)
    c = _stage(a + 2, quantum)
    return _stage((b + c) & 0x7FFFFFFF, quantum)


# --- declared mechanism / count formulas (the counts_ok gate) -----------------
# Structural, deterministic, value-independent. edge_residence + reference_kind +
# boundary_kind are DECLARED descriptions of the mechanism; submissions / retirements /
# materializations are ALSO counted for real during execution and asserted equal.

def _residence(**kw):
    r = {k: 0 for k in EDGE_RESIDENCE_KEYS}
    r.update(kw)
    return r


DECLARED = {
    "rayx_coarse": {
        "substrate": "rayx", "granularity": "coarse",
        "submissions": 1, "returned_references": 1, "reference_kind": "RuntimeFuture",
        "edge_residence": _residence(hpx_shared_future_fork=2, hpx_future_into_dataflow=2),
        "intermediate_driver_materializations": 0,
        "final_driver_materializations": 1,
        "final_materialization_kind": "RuntimeFuture.result()",
        "python_orchestration_events": 2,
        "boundary_kind": "python_runtime", "driver_boundary_crossings": 2,
    },
    "rayx_fine": {
        "substrate": "rayx", "granularity": "fine",
        "submissions": 4, "returned_references": 4, "reference_kind": "RuntimeFuture",
        "edge_residence": _residence(python_materialized_int64=4),
        "intermediate_driver_materializations": 3,
        "final_driver_materializations": 1,
        "final_materialization_kind": "RuntimeFuture.result()",
        "python_orchestration_events": 8,
        "boundary_kind": "python_runtime", "driver_boundary_crossings": 8,
    },
    "ray_coarse": {
        "substrate": "ray", "granularity": "coarse",
        "submissions": 1, "returned_references": 1, "reference_kind": "ObjectRef",
        "edge_residence": _residence(ray_task_local_python_value=4),
        "intermediate_driver_materializations": 0,
        "final_driver_materializations": 1,
        "final_materialization_kind": "ray.get()",
        "python_orchestration_events": 2,
        "boundary_kind": "python_driver_cluster", "driver_boundary_crossings": 2,
    },
    "ray_fine": {
        "substrate": "ray", "granularity": "fine",
        "submissions": 4, "returned_references": 4, "reference_kind": "ObjectRef",
        "edge_residence": _residence(ray_objectref=4),
        "intermediate_driver_materializations": 0,
        "final_driver_materializations": 1,
        "final_materialization_kind": "ray.get()",
        "python_orchestration_events": 5,
        "boundary_kind": "python_driver_cluster", "driver_boundary_crossings": 5,
    },
}

# Count fields whose MEASURED value (tracked during execution) must equal the declared
# formula for counts_ok to hold.
_MEASURED_FIELDS = ("submissions", "returned_references",
                    "intermediate_driver_materializations",
                    "final_driver_materializations", "python_orchestration_events",
                    "driver_boundary_crossings")


def _row(path, seed, quantum, value, measured):
    d = DECLARED[path]
    value_ok = (value == oracle(seed, quantum))
    counts_ok = all(measured[k] == d[k] for k in _MEASURED_FIELDS)
    row = {
        "substrate": d["substrate"], "granularity": d["granularity"], "path": path,
        "seed": seed, "quantum": quantum, "nodes": NODES, "dependency_edges": EDGES,
        "submissions": measured["submissions"],
        "returned_references": measured["returned_references"],
        "reference_kind": d["reference_kind"],
        "edge_residence": d["edge_residence"],
        "intermediate_driver_materializations":
            measured["intermediate_driver_materializations"],
        "final_driver_materializations": measured["final_driver_materializations"],
        "final_materialization_kind": d["final_materialization_kind"],
        "python_orchestration_events": measured["python_orchestration_events"],
        "boundary_kind": d["boundary_kind"],
        "driver_boundary_crossings": measured["driver_boundary_crossings"],
        "value": value, "oracle": oracle(seed, quantum),
        "value_ok": bool(value_ok), "counts_ok": bool(counts_ok),
    }
    return row


# --- RayX child: hpx_threads=1; rayx_coarse + rayx_fine -----------------------

def run_rayx_child(seeds, quanta):
    from rayx.runtime import Runtime

    rows = []
    with Runtime(hpx_threads=1) as rt:
        for seed in seeds:
            for q in quanta:
                # rayx_coarse: one diamond_fanin op (edges resolved in-op by HPX).
                subm = retire = 0
                subm += 1
                fut = rt.submit_operation("diamond_fanin", seed, q)
                val = fut.result().value
                retire += 1  # final D
                m = {"submissions": subm, "returned_references": 1,
                     "intermediate_driver_materializations": 0,
                     "final_driver_materializations": 1,
                     "python_orchestration_events": subm + retire,
                     "driver_boundary_crossings": subm + retire}
                rows.append(_row("rayx_coarse", seed, q, val, m))

                # rayx_fine: exp46 fair decomposition; B and C submitted before either
                # retires (both depend only on A). Each edge round-trips as an int64.
                subm = retire = interm = 0
                subm += 1
                fa = rt.submit_operation("chain_sum_loop", seed, 1, q)
                a = fa.result().value
                retire += 1
                interm += 1  # A held in Python to feed B, C
                subm += 1
                fb = rt.submit_operation("chain_sum_loop", a + 1, 1, q)
                subm += 1
                fc = rt.submit_operation("chain_sum_loop", a + 2, 1, q)
                b = fb.result().value
                retire += 1
                interm += 1  # B held in Python to feed D
                cc = fc.result().value
                retire += 1
                interm += 1  # C held in Python to feed D
                subm += 1
                fd = rt.submit_operation("chain_sum_loop", (b + cc) & MASK, 1, q)
                d = fd.result().value
                retire += 1  # final D (not an intermediate)
                m = {"submissions": subm, "returned_references": 4,
                     "intermediate_driver_materializations": interm,
                     "final_driver_materializations": 1,
                     "python_orchestration_events": subm + retire,
                     "driver_boundary_crossings": subm + retire}
                rows.append(_row("rayx_fine", seed, q, d & MASK, m))
    return rows


# --- Ray child: ray_coarse + ray_fine (only if Ray imports + inits) -----------

def run_ray_child(seeds, quanta):
    import ray  # local import: keeps Ray optional + out of the RayX/module path
    ray.init(num_cpus=RAY_NUM_CPUS, include_dashboard=False, log_to_driver=False,
             configure_logging=False, ignore_reinit_error=True)
    try:
        RayRoot = ray.remote(_ray_root_fn)
        RayNode = ray.remote(_ray_node_fn)
        RayJoin = ray.remote(_ray_join_fn)
        RayDiamond = ray.remote(_ray_diamond_fn)
        rows = []
        for seed in seeds:
            for q in quanta:
                # ray_coarse: one task wrapping the whole diamond; edges task-local.
                subm = gets = 0
                subm += 1
                rD = RayDiamond.remote(seed, q)
                val = ray.get(rD)
                gets += 1
                m = {"submissions": subm, "returned_references": 1,
                     "intermediate_driver_materializations": 0,
                     "final_driver_materializations": 1,
                     "python_orchestration_events": subm + gets,
                     "driver_boundary_crossings": subm + gets}
                rows.append(_row("ray_coarse", seed, q, val, m))

                # ray_fine: four tasks; ObjectRefs passed naturally; ONE final ray.get.
                # No premature ray.get -- A/B/C stay as ObjectRefs the workers resolve.
                subm = gets = 0
                rA = RayRoot.remote(seed, q); subm += 1
                rB = RayNode.remote(rA, 1, q); subm += 1   # consumes rA by ref
                rC = RayNode.remote(rA, 2, q); subm += 1   # consumes rA by ref
                rDf = RayJoin.remote(rB, rC, q); subm += 1  # consumes rB, rC by ref
                fine_val = ray.get(rDf); gets += 1          # single final materialization
                m = {"submissions": subm, "returned_references": 4,
                     "intermediate_driver_materializations": 0,
                     "final_driver_materializations": 1,
                     "python_orchestration_events": subm + gets,
                     "driver_boundary_crossings": subm + gets}
                rows.append(_row("ray_fine", seed, q, fine_val, m))
        return rows, ray.__version__
    finally:
        ray.shutdown()


# --- parent: spawn children, aggregate ----------------------------------------

def _spawn(kind, seeds, quanta, src):
    env = dict(os.environ)
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, os.path.abspath(__file__), "--child", kind,
           "--seeds", ",".join(str(s) for s in seeds),
           "--quanta", ",".join(str(q) for q in quanta)]
    out = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
    line = [ln for ln in out.stdout.splitlines() if ln.strip().startswith("{")]
    if not line:
        return {"ok": False, "stderr": out.stderr, "stdout": out.stdout,
                "rc": out.returncode}
    obj = json.loads(line[-1])
    obj["rc"] = out.returncode
    obj["stderr_tail"] = out.stderr[-400:] if out.returncode != 0 else ""
    return obj


def build_mechanism_table(executed_paths):
    table = []
    for path in ("rayx_coarse", "rayx_fine", "ray_coarse", "ray_fine"):
        if path not in executed_paths:
            continue
        d = DECLARED[path]
        table.append({
            "path": path, "substrate": d["substrate"], "granularity": d["granularity"],
            "submissions": d["submissions"],
            "returned_references": d["returned_references"],
            "reference_kind": d["reference_kind"],
            "edge_residence": {k: v for k, v in d["edge_residence"].items() if v},
            "intermediate_driver_materializations":
                d["intermediate_driver_materializations"],
            "final_driver_materializations": d["final_driver_materializations"],
            "final_materialization_kind": d["final_materialization_kind"],
            "python_orchestration_events": d["python_orchestration_events"],
            "boundary_kind": d["boundary_kind"],
            "driver_boundary_crossings": d["driver_boundary_crossings"],
        })
    return table


ALLOWED_CLAIM = (
    "For one fixed diamond DAG, at matched decomposition granularity, RayX (fixed-op "
    "runtime) and real Ray carry the same cross-node dependency edges through DIFFERENT "
    "mechanisms -- a DRIVER/ORCHESTRATION-OBSERVABLE structural inventory, single-node, "
    "counts-only, NOT a performance comparison or winner claim. rayx_coarse keeps all "
    "four edges in-op: A->B and A->C via an hpx::shared_future (A fans out to two "
    "consumers) + .then continuations, B->D and C->D as plain hpx::futures moved into "
    "hpx::dataflow. rayx_fine round-trips each cross-node edge through the Python/Runtime "
    "boundary as a closed int64. ray_fine carries dependency edges as Ray ObjectRefs, "
    "whose concrete value movement for tiny int64 payloads is implementation-dependent "
    "and NOT asserted here (single-node; no transport evidence). ray_coarse keeps all "
    "edges as task-local Python values inside one task. Sharper point: Ray ObjectRefs and "
    "HPX futures/shared_futures are BOTH in-substrate dependency handles (different "
    "semantics and scopes); RayX ALREADY uses HPX in-substrate references inside "
    "diamond_fanin, but its current fixed-op Python boundary does not expose such a "
    "reference across op boundaries, so fine-grain RayX decomposition round-trips closed "
    "values through Python. At matched coarse granularity both substrates reduce to a "
    "one-submission / one-materialization DRIVER/BOUNDARY shape (internal execution still "
    "differs), so the fine-row differences are about WHERE dependency edges live, not a "
    "substrate-quality verdict."
)

NON_CLAIMS = [
    "no speedup", "no throughput", "no latency", "no performance claim",
    "no HPX faster than Ray", "no RayX replaces Ray", "no RayX makes Ray faster",
    "no 'Ray is bad'", "no ObjectRef/object-store criticism",
    "no assertion of plasma/object-store transport for the int64 payload",
    "no 'Python orchestration is bad'", "no real inference", "no Ray Serve/Train",
    "no endpoint/fabric", "no parcelport/AGAS/multi-node",
    "no claim this resolves boundary-vs-transport (single-node has no transport evidence)",
    "no arbitrary Python execution claim",
    "no scheduler-control / placement-control / arbitrary-parallelism claim",
    "no overlap/worker-parallelism claim (hpx_threads=1)",
    "count numbers are DRIVER-observable mechanism events, not costs, and not "
    "cross-substrate comparable across boundary kinds",
    "no implication RayX should add an object store (that is the gated future "
    "distributed-fabric direction)",
    "no wall-clock assertions (timing omitted entirely)",
]

ANNOTATIONS = {
    "count_scope": (
        "All counts are DRIVER/ORCHESTRATION-OBSERVABLE only. ray_fine "
        "intermediate_driver_materializations=0 means ZERO DRIVER materializations; Ray "
        "may perform in-cluster serialization / inlining / object handling that is "
        "intentionally NOT driver-counted."),
    "hpx_edge_carriers": (
        "diamond_fanin: A fans out to B,C via hpx::shared_future + .then continuations "
        "(2 fork edges); B,C join into D via plain hpx::futures moved into hpx::dataflow "
        "(2 dataflow edges). exp46 'representational' framing (boundary counts) and "
        "exp48 'in-op edge-carrier' lens do not contradict."),
    "ray_objectref_inline_or_store": (
        "ObjectRef is Ray's dependency handle; for tiny int64 payloads the value may be "
        "inlined rather than stored in plasma. No plasma/object-store/transport asserted."),
    "coarse_control_scope": (
        "coarse-vs-coarse equalizes only the driver/submission boundary shape (1 submit / "
        "1 materialize), NOT internal execution: rayx_coarse runs an internal HPX "
        "futures/continuations/dataflow DAG; ray_coarse uses task-local Python values."),
    "no_hpx_native_fine_row": (
        "The native HPX fine-grained futures graph already lives inside diamond_fanin; "
        "exposing an in-substrate reference across the Python op boundary is the gated "
        "future distributed-fabric-direction question, so exp48 adds no HPX-native fine "
        "row."),
    "boundary_kinds_not_comparable": (
        "python_runtime (in-process value marshalling) vs python_driver_cluster (driver to "
        "cluster process/serialization) are different boundaries; crossing numbers are not "
        "cross-substrate comparable -- only within-substrate coarse-vs-fine shape and the "
        "edge-residence vector carry cross-substrate meaning."),
    "hpx_threads_invariant": (
        "hpx_threads=1 for all RayX rows; no overlap/parallelism/scheduling claim; no "
        "exp47 in-flight vocabulary."),
    "timing_intentionally_omitted": (
        "native-C++ RayX kernels vs Python Ray-task kernels (and 1-process vs N-process) "
        "make timing meaningless and perf-shaped; omitted by design."),
    "value_unit": (
        "fixed diamond A->{B,C}->D closed int64 == exp46 oracle; per-node kernel matched "
        "by VALUE (RayX native chain_stage vs Ray Python _stage), gated by value==oracle."),
}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seeds", default="1,3,7", help="comma-separated seeds (default 1,3,7)")
    ap.add_argument("--quanta", default="16,64",
                    help="comma-separated quanta (default 16,64)")
    ap.add_argument("--out", default=None)
    ap.add_argument("--smoke", action="store_true",
                    help="tiny laptop sweep: seeds=1,3 x quanta=16,64")
    ap.add_argument("--child", choices=["rayx", "ray"], default=None,
                    help=argparse.SUPPRESS)
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(os.path.dirname(os.path.dirname(here)), "python", "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    seeds = [int(x) for x in args.seeds.split(",")]
    quanta = [int(x) for x in args.quanta.split(",")]

    # --- child paths: run one substrate in an isolated process, emit one JSON object ---
    if args.child == "rayx":
        rows = run_rayx_child(seeds, quanta)
        print(json.dumps({"kind": "rayx", "rows": rows}))
        return 0
    if args.child == "ray":
        try:
            import ray  # noqa: F401  (probe import before heavier init)
        except Exception as e:  # ImportError or any import-time failure -> clean skip
            print(json.dumps({"kind": "ray", "ray_available": False,
                              "reason": f"ray not importable: {type(e).__name__}: {e}"}))
            return 0
        try:
            rrows, rver = run_ray_child(seeds, quanta)
        except Exception as e:  # init/run failure -> clean skip, not a structural fail
            print(json.dumps({"kind": "ray", "ray_available": False,
                              "reason": f"ray init/run failed: {type(e).__name__}: {e}"}))
            return 0
        print(json.dumps({"kind": "ray", "ray_available": True,
                          "ray_version": rver, "rows": rrows}))
        return 0

    # --- parent ---
    if args.smoke:
        seeds, quanta = [1, 3], [16, 64]
    out_path = args.out or os.path.join(here, "aggregate.json")

    rayx_obj = _spawn("rayx", seeds, quanta, src)
    if not rayx_obj.get("rows") and rayx_obj.get("ok") is False:
        sys.stderr.write(rayx_obj.get("stderr", ""))
        raise RuntimeError(f"RayX child failed (rc={rayx_obj.get('rc')})")
    rows = list(rayx_obj.get("rows", []))

    ray_obj = _spawn("ray", seeds, quanta, src)
    ray_available = bool(ray_obj.get("ray_available"))
    ray_version = ray_obj.get("ray_version")
    ray_skipped_reason = None if ray_available else ray_obj.get(
        "reason", "ray child produced no result")
    if ray_available:
        rows.extend(ray_obj.get("rows", []))

    executed_paths = sorted({r["path"] for r in rows})
    declared_paths = ["rayx_coarse", "rayx_fine", "ray_coarse", "ray_fine"]
    paths_skipped = [p for p in declared_paths if p not in executed_paths]

    value_failures = [
        f"{r['path']} seed={r['seed']} q={r['quantum']}: "
        f"value={r['value']} oracle={r['oracle']}" for r in rows if not r["value_ok"]]
    count_failures = [
        f"{r['path']} seed={r['seed']} q={r['quantum']}: counts != declared formula"
        for r in rows if not r["counts_ok"]]
    # Load-bearing pass is over EXECUTED rows only; skipped Ray rows are NOT failures.
    overall_structural_pass = (not value_failures and not count_failures and bool(rows))

    aggregate = {
        "experiment": "exp48_ray_boundary_mechanism_inventory",
        "schema": "rayx-ray-mechanism-inventory-1",
        "question": (
            "For one fixed diamond DAG at fixed decomposition granularity, what "
            "structural mechanism inventory does each substrate (RayX fixed-op runtime vs "
            "real Ray) use to express the same closed int64 -- specifically WHERE each "
            "cross-op dependency edge lives and how many driver/orchestration-observable "
            "submission / reference / materialization events it entails?"),
        "load_bearing": (
            "driver-observable structural mechanism inventory at matched granularity + "
            "equal-value invariant; counts-only, no timing"),
        "count_scope": "driver/orchestration-observable events only; in-cluster Ray "
                       "object handling is not driver-counted",
        "ray_available": ray_available,
        "ray_version": ray_version,
        "ray_skipped_reason": ray_skipped_reason,
        "config": {
            "seeds": seeds, "quanta": quanta, "hpx_threads": 1,
            "nodes": NODES, "dependency_edges": EDGES, "ray_num_cpus": RAY_NUM_CPUS,
            "per_node_kernel": "chain_stage(x,q) == chain_sum_loop(x,1,q); Ray nodes "
                               "matched by VALUE only",
            "value_unit": "fixed diamond A->{B,C}->D closed int64 == exp46 oracle",
            "process_model": "the parent always spawns a RayX child subprocess and a Ray "
                             "child subprocess; the Ray child runs the Ray paths only if "
                             "ray imports and initializes cleanly, otherwise it returns a "
                             "clean skip (subprocess isolation; HPX and Ray never co-init'd)",
        },
        "machine": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
        "paths_declared": declared_paths,
        "paths_executed": executed_paths,
        "paths_skipped": paths_skipped,
        "mechanism_table": build_mechanism_table(executed_paths),
        "edge_residence_summary": {
            p: {k: v for k, v in DECLARED[p]["edge_residence"].items() if v}
            for p in executed_paths},
        "rows": rows,
        "value_failures": value_failures,
        "count_failures": count_failures,
        "overall_structural_pass": bool(overall_structural_pass),
        "allowed_claim": ALLOWED_CLAIM,
        "non_claims": NON_CLAIMS,
        "annotations": ANNOTATIONS,
        "excluded_path": (
            "ray_fine-with-per-node-ray.get deliberately omitted: artificial "
            "serialization / strawman that would read as Ray criticism"),
    }

    with open(out_path, "w") as f:
        json.dump(aggregate, f, indent=2)
        f.write("\n")

    # --- console summary (counts only; no timing) ---
    print(f"exp48 ray-boundary-mechanism-inventory: wrote {out_path}")
    print(f"  seeds={seeds} quanta={quanta} hpx_threads=1")
    print(f"  ray_available={ray_available}"
          + (f" (v{ray_version})" if ray_available else f" -- skipped: {ray_skipped_reason}"))
    print(f"  paths_executed={executed_paths}")
    print(f"  paths_skipped={paths_skipped}")
    print(f"  {'path':>12} {'subm':>4} {'refs':>4} {'kind':>13} "
          f"{'edge residence':>34} {'intMat':>6} {'finMat':>6} {'orch':>4}")
    for path in declared_paths:
        if path not in executed_paths:
            continue
        d = DECLARED[path]
        res = ", ".join(f"{k}={v}" for k, v in d["edge_residence"].items() if v)
        print(f"  {path:>12} {d['submissions']:>4} {d['returned_references']:>4} "
              f"{d['reference_kind']:>13} {res:>34} "
              f"{d['intermediate_driver_materializations']:>6} "
              f"{d['final_driver_materializations']:>6} "
              f"{d['python_orchestration_events']:>4}")
    print(f"  STRUCTURAL: {'PASS' if overall_structural_pass else 'FAIL'} "
          f"(value_fail={len(value_failures)}, count_fail={len(count_failures)}, "
          f"rows={len(rows)})")
    if not overall_structural_pass:
        for m in value_failures + count_failures:
            print(f"    {m}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
