#!/usr/bin/env python3
"""exp32 -- intra-actor native CPU scaling inside one Ray actor (quick mode).

CORE QUESTION (not a benchmark, not a Ray comparison verdict):
inside ONE long-lived Ray actor (one process, one Ray boundary held constant),
can RayX/HPX scale native CPU-bound Async work (`busy_sum`) across HPX workers,
while an equivalent pure-Python in-process CPU loop stays GIL-bound (~1x)?

METHOD -- strong scaling of a fixed batch, normalized per leg:
  * A batch is K `busy_sum(n)` ops. We vary W = num_lanes = hpx_threads in {1,2,4}
    and measure the wall to complete all K. With W workers the K ops run in
    ceil(K/W) waves, so ideal T(W) ~ (K/W) * op.
  * PER-LEG NORMALIZED SPEEDUP = T_leg(W=1) / T_leg(W). Dividing each leg by its
    OWN single-worker time CANCELS the absolute C++-vs-CPython per-op cost (M1),
    leaving only how the engine SCALES (M2). We compare the two speedup curves,
    NOT raw Python-vs-RayX wall time.
  * The Python leg runs the ITERATIVE masked loop (genuine CPU work), not the
    closed form -- otherwise it would compute the answer instantly.

ALLOWED CLAIM (only if the data supports it):
  "Inside one long-lived Ray actor, RayX/HPX shows intra-process native CPU
   scaling for Async native work, while pure-Python CPU work in one process
   remains GIL-bound."
FORBIDDEN: "RayX makes Ray faster", "HPX beats Ray", "RayX replaces Ray", any
raw Python-vs-RayX wall-time speedup claim, any sizing/capacity guidance.

The Python leg is the STRUCTURAL GIL-bound baseline: it is serial in-process
(we deliberately do not thread it; threaded pure-Python CPU would also be ~1x
under the GIL). Ray's idiomatic CPU-scaling answer is MORE actors/processes --
acknowledged here, deliberately NOT measured as a head-to-head leg.

End-to-end numbers include the Ray actor boundary and should not be read as a
pure engine metric; the in-actor numbers are the engine-clean comparison.

Observation-only and machine-specific. No JSONL/corpus artifact is written; the
runner only prints a compact summary. Skips cleanly (exit 0) if Ray or the built
`_rayx` / `rayx.runtime` is unavailable.

An opt-in `--decouple` panel separately probes the runtime scaling bound
(effective parallelism ~ min(num_lanes, hpx_threads, cores)) using busy_sum only,
with all cells held to <= 4 effective workers (P-core region; no W>4 knee). It is
a runtime-bound observation, not a benchmark, and does not modify the quick
result.

An opt-in `--full` mode runs the same workload over a wider W in {1,2,4,8,16,32}
sweep (K=32 so a batch can occupy every worker; K must be >= max(W) or the batch
cannot keep all workers busy). It is a PREPARED TOOL for FUTURE homogeneous
many-core Linux validation -- on this Apple-silicon laptop the W>4 cells are
confounded by P/E heterogeneity, SMT, and thermal behavior, so full-mode output
here is SMOKE-ONLY, not evidence. Full mode keeps the SAME structural gates as
quick and never turns timing into a pass/fail gate; a noisy/non-monotone run
prints NOISY/INCONCLUSIVE, not FAIL.

Usage:
    python experiments/32_ray_hosting_rayx_cpu_scaling/run_ray_hosting_rayx_cpu_scaling.py --quick
    python experiments/32_ray_hosting_rayx_cpu_scaling/run_ray_hosting_rayx_cpu_scaling.py --decouple
    python experiments/32_ray_hosting_rayx_cpu_scaling/run_ray_hosting_rayx_cpu_scaling.py --full

Exit code: 0 == gates passed or cleanly skipped; 1 == a structural gate failed.
"""
import argparse
import os
import statistics
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

# busy_sum checkpoint stride (mirrors BUSY_SUM_STRIDE in runtime_ops.hpp); used
# only to report the derived checkpoint_count of the load op.
BUSY_SUM_STRIDE = 8192

# Quick-mode parameters (the spec's defaults).
W_VALUES = (1, 2, 4)
K_OPS = 4               # ops per batch; divisible by every W for clean waves
N_WORK = 5_000_000     # ms-scale C++ op; Python equivalent stays < ~1s
WARMUP = 1
REPS = 3

# Decoupling-panel parameters (opt-in --decouple). K must be >= the largest
# num_lanes/hpx_threads used, or a batch cannot occupy every worker (K=4 would
# cap occupancy at 4). Every panel cell keeps EFFECTIVE parallelism <= 4 (only
# four busy bodies ever run at once), so it stays in the P-core region on this
# laptop -- hardware-clean, no W>4 knee.
K_DECOUPLE = 8
REPS_DECOUPLE = 5

# Full-mode parameters (opt-in --full). Prepared tool for FUTURE homogeneous
# many-core Linux validation -- NOT validated evidence on this Apple-silicon
# laptop (see --full's printed warning and the exp32 note). Default W sweep walks
# into the W>4 saturation regime; K must be >= max(W) so a batch can occupy every
# worker (the earlier K=4 mistake capped occupancy at 4 -- do not repeat it).
W_VALUES_FULL = (1, 2, 4, 8, 16, 32)
K_FULL = 32            # >= max(W_VALUES_FULL)=32; K must be >= max(W) or the
                       # batch cannot occupy all workers (avoids the K=4 cap)
REPS_FULL = 5


def busy_sum_value(n):
    """Closed form of busy_sum(n): (n*(n-1)/2) mod 2^31. Used for GATES only."""
    return (n * (n - 1) // 2) % (2 ** 31)


# --------------------------------------------------------------------------- #
# Ray actors (each runs in its own Ray worker process)                        #
# --------------------------------------------------------------------------- #
def _build_actors(ray):
    """Define the two Ray actors inside a function so this module imports without
    Ray present (clean-skip path)."""

    @ray.remote
    class PyCpuActor:
        """Pure-Python in-process CPU baseline. SERIAL: `w` is nominal -- we do
        not thread Python (threaded pure-Python CPU would still be ~1x under the
        GIL). The flat speedup curve is the STRUCTURAL baseline."""

        def run_batch(self, w, n, k):
            t0 = time.perf_counter()
            agg = 0
            for _ in range(k):
                acc = 0
                for i in range(n):                  # genuine CPU work
                    acc = (acc + i) & 0x7FFFFFFF
                agg += acc
            in_actor_ms = (time.perf_counter() - t0) * 1e3
            return {"agg": agg, "in_actor_ms": in_actor_ms, "k": k, "w": w}

    @ray.remote
    class RayxCpuActor:
        """Hosts ONE rayx.runtime.Runtime(num_lanes=w, hpx_threads=w) and runs
        Async-policy busy_sum across its lanes. ActorHandle/RuntimeFuture/
        OperationResult are created and retired INSIDE this actor; only plain
        scalars/containers cross the Ray boundary."""

        def __init__(self, num_lanes, hpx_threads, rayx_src):
            # Ray workers are fresh processes; put python/src on the path here
            # (the driver's sys.path does not propagate to the worker).
            import sys as _sys
            if rayx_src not in _sys.path:
                _sys.path.insert(0, rayx_src)
            from rayx.runtime import Runtime         # raises if _rayx missing
            # num_lanes and hpx_threads are separate so the decoupling panel can
            # set them independently; quick mode passes them equal (coupled W).
            self._rt = Runtime(num_lanes=num_lanes, hpx_threads=hpx_threads)
            self._num_lanes = num_lanes
            self._hpx_threads = hpx_threads

        def lane_ids(self):
            return [d["actor_id"] for d in self._rt.lane_stats()]

        def run_batch(self, n, k):
            t0 = time.perf_counter()
            futs = [self._rt.submit_operation("busy_sum", n) for _ in range(k)]
            results = self._rt.get(futs)             # retired inside the actor
            in_actor_ms = (time.perf_counter() - t0) * 1e3
            agg = sum(r.value for r in results)
            statuses = [r.status for r in results]
            return {"agg": agg, "in_actor_ms": in_actor_ms, "k": k,
                    "num_lanes": self._num_lanes, "hpx_threads": self._hpx_threads,
                    "statuses": statuses}

        def shutdown(self):
            self._rt.shutdown()

    return PyCpuActor, RayxCpuActor


# --------------------------------------------------------------------------- #
# Measurement helpers                                                          #
# --------------------------------------------------------------------------- #
def _spread(xs):
    """(max - min) / median as a fraction; 0 for a single sample."""
    med = statistics.median(xs)
    if med <= 0 or len(xs) < 2:
        return 0.0
    return (max(xs) - min(xs)) / med


def _measure(ray, call, warmup, reps):
    """Run `call()` (returns the actor's result dict) warmup+reps times.
    Returns (result_dicts, in_actor_ms_list, end_to_end_ms_list)."""
    for _ in range(warmup):
        ray.get(call())
    results, in_actor, end_to_end = [], [], []
    for _ in range(reps):
        t0 = time.perf_counter()
        res = ray.get(call())
        end_to_end.append((time.perf_counter() - t0) * 1e3)
        results.append(res)
        in_actor.append(res["in_actor_ms"])
    return results, in_actor, end_to_end


def _run_scaling(w_values, k_ops, n_work, warmup, reps):
    """Shared strong-scaling core for --quick and --full. Both modes run the
    SAME workload shape (PY serial CPU loop leg + RayX Async busy_sum leg) and the
    SAME per-leg normalization; they differ only in the W sweep / K / reps passed
    in. Returns (rows, gates, skipped, skip_reason). rows: list of per-(leg,W)
    dicts with timing + speedup; gates: dict of bool."""
    gates = {}
    # ---- clean-skip probe (before spawning anything) ----
    try:
        import ray  # noqa: F401
    except Exception as e:
        return [], gates, True, f"ray unavailable: {type(e).__name__}: {e}"
    try:
        from rayx.runtime import Runtime  # noqa: F401
    except Exception as e:
        return [], gates, True, f"rayx.runtime unavailable: {type(e).__name__}: {e}"

    import ray
    expected_agg = k_ops * busy_sum_value(n_work)
    agg_ok = True
    futures_completed = True
    lane_ids_ok = True
    plain_types_ok = True
    clean_shutdown = True

    max_w = max(w_values)
    ray.init(num_cpus=max_w + 1, ignore_reinit_error=True,
             log_to_driver=False, configure_logging=False)
    PyCpuActor, RayxCpuActor = _build_actors(ray)
    rows = []
    try:
        # ===== P leg: one Python actor, looped over W (W is nominal/serial) ====
        py_actor = PyCpuActor.options(num_cpus=1).remote()
        for w in w_values:
            res, ia, e2e = _measure(
                ray, lambda w=w: py_actor.run_batch.remote(w, n_work, k_ops),
                warmup, reps)
            for r in res:
                if r["agg"] != expected_agg:
                    agg_ok = False
                if not isinstance(r, dict) or not all(
                        isinstance(r[k], (int, float)) for k in
                        ("agg", "in_actor_ms", "k", "w")):
                    plain_types_ok = False
            rows.append({"leg": "PY", "w": w,
                         "in_actor": ia, "end_to_end": e2e})
        ray.kill(py_actor)

        # ===== C leg: one RayX actor per W (one Runtime per process) ==========
        for w in w_values:
            actor = RayxCpuActor.options(num_cpus=w).remote(w, w, RAYX_SRC)
            ids = ray.get(actor.lane_ids.remote())
            if len(ids) != w or not all(
                    isinstance(i, str) and i.startswith("rt-hpx-") for i in ids):
                lane_ids_ok = False
            res, ia, e2e = _measure(
                ray, lambda a=actor: a.run_batch.remote(n_work, k_ops),
                warmup, reps)
            for r in res:
                if r["agg"] != expected_agg:
                    agg_ok = False
                if any(s != "completed" for s in r["statuses"]):
                    futures_completed = False
                # plain-types: dict of int/float/str/list, no RayX/Ray objects
                if not (isinstance(r, dict)
                        and isinstance(r["agg"], int)
                        and isinstance(r["in_actor_ms"], float)
                        and isinstance(r["statuses"], list)
                        and all(isinstance(s, str) for s in r["statuses"])):
                    plain_types_ok = False
            try:
                ray.get(actor.shutdown.remote())
            except Exception:
                clean_shutdown = False
            ray.kill(actor)
            rows.append({"leg": "RAYX", "w": w,
                         "in_actor": ia, "end_to_end": e2e})
    finally:
        ray.shutdown()

    # ---- per-leg normalized speedup + efficiency ----
    by_leg = {}
    for row in rows:
        by_leg.setdefault(row["leg"], {})[row["w"]] = row
    for leg, perw in by_leg.items():
        base = statistics.median(perw[1]["in_actor"])  # W=1 in-actor median
        for w, row in perw.items():
            med = statistics.median(row["in_actor"])
            row["in_actor_med"] = med
            row["in_actor_spread"] = _spread(row["in_actor"])
            row["end_to_end_med"] = statistics.median(row["end_to_end"])
            row["speedup"] = (base / med) if med > 0 else float("nan")
            row["efficiency"] = row["speedup"] / w

    gates = {
        "agg_ok": agg_ok,
        "futures_completed": futures_completed,
        "lane_ids_ok": lane_ids_ok,
        "plain_types_ok": plain_types_ok,
        "clean_shutdown": clean_shutdown,
    }
    return rows, gates, False, None


def run_quick():
    """Quick mode: the original W in {1,2,4}, K=4 sweep. Unchanged behavior."""
    return _run_scaling(W_VALUES, K_OPS, N_WORK, WARMUP, REPS)


def _full_w_values(cpu):
    """Choose the --full W sweep for this machine. Default is (1,2,4,8,16,32),
    intended for a homogeneous many-core Linux node. If the detected CPU count is
    below a default W, those high-W cells are dropped (they are not appropriate on
    this machine) and a warning string is returned. Returns (w_values, warning)."""
    w_values = list(W_VALUES_FULL)
    if cpu is None:
        return w_values, None
    capped = [w for w in w_values if w <= cpu]
    if not capped:
        capped = [1]
    if capped != w_values:
        dropped = [w for w in w_values if w > cpu]
        warn = (f"detected cpu_count={cpu} < default max W=32; dropped high-W "
                f"cells {dropped} (not appropriate on this machine). Use a "
                f"homogeneous many-core node for the full W sweep.")
        return capped, warn
    return capped, None


def run_full():
    """Full mode: walk the (capped) W in {1,2,4,8,16,32} sweep at K=32 (K must be
    >= max(W) or the batch cannot occupy all workers). Same workload and per-leg
    normalization as quick; only the sweep/K/reps differ. Returns
    (rows, gates, skipped, reason, w_values, warning)."""
    w_values, warn = _full_w_values(os.cpu_count())
    rows, gates, skipped, reason = _run_scaling(
        tuple(w_values), K_FULL, N_WORK, WARMUP, REPS_FULL)
    return rows, gates, skipped, reason, w_values, warn


# --------------------------------------------------------------------------- #
# Decoupling panel (opt-in --decouple): observe the runtime scaling bound      #
# effective parallelism ~ min(num_lanes, hpx_threads, cores). RAYX-only.       #
# --------------------------------------------------------------------------- #
def run_decouple():
    """Returns (rows, gates, skipped, reason). Four RAYX cells, all K=K_DECOUPLE,
    all with effective parallelism <= 4 (P-core region). No Python leg -- the
    bound is a runtime property, not a language comparison."""
    gates = {}
    try:
        import ray  # noqa: F401
    except Exception as e:
        return [], gates, True, f"ray unavailable: {type(e).__name__}: {e}"
    try:
        from rayx.runtime import Runtime  # noqa: F401
    except Exception as e:
        return [], gates, True, f"rayx.runtime unavailable: {type(e).__name__}: {e}"

    import ray
    K = K_DECOUPLE
    n = N_WORK
    expected_agg = K * busy_sum_value(n)
    agg_ok = futures_completed = lane_ids_ok = plain_types_ok = clean_shutdown = True

    # (label, num_lanes, hpx_threads). worker_bound and lane_bound both have
    # effective parallelism min(lanes, threads) = 4, so both should track the
    # coupled 4x reference, not the 1x baseline.
    cells_spec = [
        ("baseline_1x", 1, 1),
        ("coupled_4x", 4, 4),
        ("worker_bound", 8, 4),
        ("lane_bound", 4, 8),
    ]
    max_ht = max(ht for _, _, ht in cells_spec)
    ray.init(num_cpus=max_ht + 1, ignore_reinit_error=True,
             log_to_driver=False, configure_logging=False)
    _, RayxCpuActor = _build_actors(ray)
    rows = []
    try:
        for label, nl, ht in cells_spec:
            actor = RayxCpuActor.options(num_cpus=ht).remote(nl, ht, RAYX_SRC)
            ids = ray.get(actor.lane_ids.remote())
            if len(ids) != nl or not all(
                    isinstance(i, str) and i.startswith("rt-hpx-") for i in ids):
                lane_ids_ok = False
            res, ia, e2e = _measure(
                ray, lambda a=actor: a.run_batch.remote(n, K),
                WARMUP, REPS_DECOUPLE)
            for r in res:
                if r["agg"] != expected_agg:
                    agg_ok = False
                if any(s != "completed" for s in r["statuses"]):
                    futures_completed = False
                if not (isinstance(r, dict)
                        and isinstance(r["agg"], int)
                        and isinstance(r["in_actor_ms"], float)
                        and isinstance(r["num_lanes"], int)
                        and isinstance(r["hpx_threads"], int)
                        and isinstance(r["statuses"], list)
                        and all(isinstance(s, str) for s in r["statuses"])):
                    plain_types_ok = False
            try:
                ray.get(actor.shutdown.remote())
            except Exception:
                clean_shutdown = False
            ray.kill(actor)
            rows.append({"label": label, "num_lanes": nl, "hpx_threads": ht,
                         "in_actor": ia, "end_to_end": e2e})
    finally:
        ray.shutdown()

    by = {row["label"]: row for row in rows}
    base = statistics.median(by["baseline_1x"]["in_actor"])
    ref = statistics.median(by["coupled_4x"]["in_actor"])
    for row in rows:
        med = statistics.median(row["in_actor"])
        row["in_actor_med"] = med
        row["in_actor_spread"] = _spread(row["in_actor"])
        row["end_to_end_med"] = statistics.median(row["end_to_end"])
        row["speedup_vs_1x"] = (base / med) if med > 0 else float("nan")
        row["ratio_to_4x"] = (med / ref) if ref > 0 else float("nan")

    gates = {
        "agg_ok": agg_ok,
        "futures_completed": futures_completed,
        "lane_ids_ok": lane_ids_ok,
        "plain_types_ok": plain_types_ok,
        "clean_shutdown": clean_shutdown,
    }
    return rows, gates, False, None


# --------------------------------------------------------------------------- #
# Machine-info capture (portable, cheap; the run never depends on it)          #
# --------------------------------------------------------------------------- #
def _lscpu_summary():
    """A few lscpu fields on Linux only, if lscpu is present. Best-effort: any
    failure returns None and the run does NOT depend on it."""
    if not sys.platform.startswith("linux"):
        return None
    import shutil
    import subprocess
    if shutil.which("lscpu") is None:
        return None
    try:
        out = subprocess.run(["lscpu"], capture_output=True, text=True,
                             timeout=2)
    except Exception:
        return None
    if out.returncode != 0:
        return None
    wanted = ("Model name", "CPU(s)", "Socket(s)", "Core(s) per socket",
              "Thread(s) per core")
    parts = []
    for line in out.stdout.splitlines():
        key, sep, val = line.partition(":")
        if sep and key.strip() in wanted:
            parts.append(f"{key.strip()}={val.strip()}")
    return "; ".join(parts) if parts else None


def _print_machine_info():
    """Compact, portable machine-info block printed at the top of every mode."""
    import platform
    try:
        import ray
        ray_ver = getattr(ray, "__version__", "unknown")
    except Exception:
        ray_ver = "unavailable"
    try:
        uname = platform.uname()
        system = f"{uname.system} {uname.release}"
    except Exception:
        system = "n/a"
    print("machine-info (portable, cheap; run does not depend on it):")
    print(f"  platform={platform.platform()}")
    print(f"  system={system} machine={platform.machine()} "
          f"processor={platform.processor() or 'n/a'}")
    print(f"  python={platform.python_version()} cpu_count={os.cpu_count()} "
          f"ray={ray_ver}")
    lscpu = _lscpu_summary()
    if lscpu:
        print(f"  lscpu: {lscpu}")
    print()


# --------------------------------------------------------------------------- #
# Reporting                                                                    #
# --------------------------------------------------------------------------- #
def _interpret(rows):
    """SUPPORT only if RAYX speedup rises monotonically with W and is materially
    > 1 at max W, PY stays ~1x within noise, and spreads do not dominate."""
    by = {(r["leg"], r["w"]): r for r in rows}
    ws = sorted({r["w"] for r in rows})
    rayx = [by[("RAYX", w)]["speedup"] for w in ws]
    py = [by[("PY", w)]["speedup"] for w in ws]
    rayx_spreads = [by[("RAYX", w)]["in_actor_spread"] for w in ws]

    rayx_monotone = all(rayx[i + 1] >= rayx[i] - 0.05 for i in range(len(rayx) - 1))
    rayx_material = rayx[-1] >= 1.5
    py_flat = all(abs(s - 1.0) <= 0.25 for s in py)
    # noise guard: the W=1->maxW RAYX gain must exceed the worst RAYX spread
    gain = rayx[-1] - rayx[0]
    noise_ok = gain > max(rayx_spreads + [0.0])

    if rayx_monotone and rayx_material and py_flat and noise_ok:
        verdict = "SUPPORT"
        msg = ("RAYX in-actor speedup rises with W ("
               + " -> ".join(f"{v:.2f}" for v in rayx)
               + ") while PY stays ~1x ("
               + " -> ".join(f"{v:.2f}" for v in py)
               + "): intra-process native CPU scaling is present for RayX and "
               "structurally absent for an in-process Python CPU loop (GIL).")
    elif not noise_ok:
        verdict = "INCONCLUSIVE"
        msg = ("RAYX W=1->max gain (%.2f) does not exceed measurement spread; "
               "cannot separate scaling from noise on this run." % gain)
    else:
        verdict = "INCONCLUSIVE"
        msg = ("RAYX speedup did not rise cleanly with W ("
               + " -> ".join(f"{v:.2f}" for v in rayx)
               + ") and/or PY was not flat; no clean scaling claim on this run.")
    return verdict, msg


def _interpret_full(rows):
    """Full-mode reading. SAME scaling logic as quick, but full mode walks into
    the W>4 saturation regime, so a failed noise guard is reported as an
    observational NOISY (not FAIL) and a non-monotone / non-flat run as
    INCONCLUSIVE. Full mode never turns timing into a pass/fail gate."""
    by = {(r["leg"], r["w"]): r for r in rows}
    ws = sorted({r["w"] for r in rows})
    rayx = [by[("RAYX", w)]["speedup"] for w in ws]
    py = [by[("PY", w)]["speedup"] for w in ws]
    rayx_spreads = [by[("RAYX", w)]["in_actor_spread"] for w in ws]

    rayx_monotone = all(rayx[i + 1] >= rayx[i] - 0.05 for i in range(len(rayx) - 1))
    rayx_material = rayx[-1] >= 1.5
    py_flat = all(abs(s - 1.0) <= 0.25 for s in py)
    gain = rayx[-1] - rayx[0]
    noise_ok = gain > max(rayx_spreads + [0.0])

    rayx_str = " -> ".join(f"{v:.2f}" for v in rayx)
    py_str = " -> ".join(f"{v:.2f}" for v in py)
    if rayx_monotone and rayx_material and py_flat and noise_ok:
        return ("SUPPORT",
                f"RAYX in-actor speedup rises across the full W sweep ({rayx_str}) "
                f"while PY stays ~1x ({py_str}).")
    if not noise_ok:
        return ("NOISY",
                f"RAYX W=1->max gain ({gain:.2f}) does not exceed measurement "
                f"spread; cannot separate scaling from noise on this run "
                f"(RAYX {rayx_str}).")
    return ("INCONCLUSIVE",
            f"RAYX speedup did not rise cleanly across the full sweep ({rayx_str}) "
            f"and/or PY was not flat ({py_str}); no clean scaling reading this run "
            f"(expected past the W>4 saturation knee on heterogeneous hardware).")


def _interpret_decouple(rows):
    """SUPPORT only if the coupled 4x reference is genuinely ~4-effective AND both
    decoupled cells track it (ratio_to_4x near 1) -- i.e. extra lanes beyond
    workers, or workers beyond lanes, do not help."""
    by = {r["label"]: r for r in rows}
    ref_speedup = by["coupled_4x"]["speedup_vs_1x"]
    wb = by["worker_bound"]["ratio_to_4x"]
    lb = by["lane_bound"]["ratio_to_4x"]
    LO, HI = 0.7, 1.4
    ref_ok = ref_speedup >= 2.5
    wb_ok = LO <= wb <= HI
    lb_ok = LO <= lb <= HI
    curves = (f"coupled_4x speedup_vs_1x={ref_speedup:.2f}; "
              f"worker_bound ratio_to_4x={wb:.2f}; lane_bound ratio_to_4x={lb:.2f}")
    if ref_ok and wb_ok and lb_ok:
        return ("SUPPORT",
                "decoupled cells track min(num_lanes, hpx_threads, cores): "
                "worker_bound (lanes=8, hpx_threads=4) performs like the coupled "
                "4-effective reference -- extra lanes beyond workers do not help "
                "(worker-bound); lane_bound (lanes=4, hpx_threads=8) likewise -- "
                "extra workers beyond lanes do not help (lane-bound). " + curves)
    if not ref_ok:
        return ("INCONCLUSIVE",
                "could not establish the 4-effective reference (coupled_4x did not "
                "reach ~4x vs 1x); the bound cannot be read cleanly this run. "
                + curves)
    return ("INCONCLUSIVE",
            f"a decoupled cell fell outside [{LO}, {HI}]x of the 4-effective "
            "reference (noise or unexpected scaling); the existing exp32 quick "
            "SUPPORT is unchanged. " + curves)


def _print_decouple():
    cpc = (N_WORK + BUSY_SUM_STRIDE - 1) // BUSY_SUM_STRIDE
    print("exp32 decoupling panel -- runtime min(num_lanes, hpx_threads, cores) "
          "bound (observation-only, machine-specific)")
    _print_machine_info()
    print(f"cpu_count={os.cpu_count()} (effective parallelism <= 4 per cell, "
          f"P-core region)  n={N_WORK} K={K_DECOUPLE} checkpoint_count={cpc} "
          f"warmup={WARMUP} reps={REPS_DECOUPLE}\n")

    rows, gates, skipped, reason = run_decouple()
    if skipped:
        print(f"SKIP: {reason}")
        return 0

    hdr = (f"  {'cell':<14}{'lanes':>6}{'hpx_thr':>8}{'in_actor_ms(med)':>18}"
           f"{'spread':>9}{'speedup/1x':>12}{'ratio/4x':>10}")
    print(hdr)
    notes = {"worker_bound": "lanes>workers", "lane_bound": "workers>lanes"}
    by = {r["label"]: r for r in rows}
    for label in ("baseline_1x", "coupled_4x", "worker_bound", "lane_bound"):
        r = by[label]
        note = ("  <- " + notes[label]) if label in notes else ""
        print(f"  {label:<14}{r['num_lanes']:>6}{r['hpx_threads']:>8}"
              f"{r['in_actor_med']:>18.1f}{r['in_actor_spread'] * 100:>8.1f}%"
              f"{r['speedup_vs_1x']:>12.2f}{r['ratio_to_4x']:>10.2f}{note}")
    print()

    gates_ok = all(gates.values())
    print("  gates: " + ", ".join(f"{k}={v}" for k, v in sorted(gates.items())))
    print()

    verdict, msg = _interpret_decouple(rows)
    print(f"DECOUPLING PANEL (observation-only, this run/machine) [{verdict}]: {msg}")
    print("  Observation-only runtime-bound probe; not a benchmark or sizing "
          "claim, not 'RayX makes Ray faster' / 'HPX beats Ray'. Does NOT modify "
          "the exp32 quick SUPPORT.")
    print()
    if gates_ok:
        print("STRUCTURAL GATES: PASS")
        return 0
    print("STRUCTURAL GATES: FAIL")
    return 1


def _print_scaling_table(rows):
    """Shared PY/RAYX scaling table for --quick and --full."""
    hdr = (f"  {'leg':<6}{'W':>3}{'in_actor_ms(med)':>18}{'spread':>9}"
           f"{'speedup':>9}{'eff':>7}{'end_to_end_ms(med)':>20}")
    print(hdr)
    for leg in ("PY", "RAYX"):
        for row in sorted((r for r in rows if r["leg"] == leg),
                          key=lambda r: r["w"]):
            print(f"  {leg:<6}{row['w']:>3}{row['in_actor_med']:>18.1f}"
                  f"{row['in_actor_spread'] * 100:>8.1f}%{row['speedup']:>9.2f}"
                  f"{row['efficiency']:>7.2f}{row['end_to_end_med']:>20.1f}")
    print()


def _print_quick():
    cpc = (N_WORK + BUSY_SUM_STRIDE - 1) // BUSY_SUM_STRIDE
    print("exp32 intra-actor CPU scaling (quick) -- observation-only, "
          "machine-specific")
    _print_machine_info()
    print(f"cpu_count={os.cpu_count()} (Apple silicon may be P/E heterogeneous)  "
          f"K={K_OPS} n={N_WORK} checkpoint_count={cpc} "
          f"warmup={WARMUP} reps={REPS}\n")

    rows, gates, skipped, reason = run_quick()
    if skipped:
        print(f"SKIP: {reason}")
        return 0

    _print_scaling_table(rows)

    gates_ok = all(gates.values())
    print("  gates: " + ", ".join(f"{k}={v}" for k, v in sorted(gates.items())))

    # M1 context only (explicitly NOT a speedup claim).
    by = {(r["leg"], r["w"]): r for r in rows}
    py1 = by[("PY", 1)]["in_actor_med"]
    rx1 = by[("RAYX", 1)]["in_actor_med"]
    if rx1 > 0:
        print(f"  M1 context only (NOT a speedup claim): native/CPython per-op "
              f"factor ~= {py1 / rx1:.1f}x at W=1")
    print()

    verdict, msg = _interpret(rows)
    print(f"READING (observation-only, this run/machine) [{verdict}]: {msg}")
    print("  End-to-end numbers include the Ray actor boundary and should not be "
          "read as a pure engine metric.")
    print("  Ray's idiomatic CPU scaling answer is MORE actors/processes "
          "(not measured here). This is not 'RayX makes Ray faster' / "
          "'HPX beats Ray' / 'RayX replaces Ray'.")
    print()

    if gates_ok:
        print("STRUCTURAL GATES: PASS")
        return 0
    print("STRUCTURAL GATES: FAIL")
    return 1


def _print_full():
    cpc = (N_WORK + BUSY_SUM_STRIDE - 1) // BUSY_SUM_STRIDE
    print("exp32 intra-actor CPU scaling (FULL) -- prepared tool for FUTURE "
          "homogeneous many-core Linux validation; observation-only")
    _print_machine_info()
    print("WARNING -- --full is intended for a HOMOGENEOUS MANY-CORE LINUX node.")
    print("  Apple-silicon P-core/E-core asymmetry, SMT, and laptop thermal")
    print("  throttling can confound the W>4 cells. Output on this Mac/laptop is")
    print("  SMOKE-ONLY, NOT evidence; do not tune thresholds from it.")

    w_values, warn = _full_w_values(os.cpu_count())
    if warn:
        print(f"  NOTE: {warn}")
    print()
    print(f"  W_values={list(w_values)} K={K_FULL} n={N_WORK} "
          f"checkpoint_count={cpc} warmup={WARMUP} reps={REPS_FULL}\n")

    rows, gates, skipped, reason, _w, _warn = run_full()
    if skipped:
        print(f"SKIP: {reason}")
        return 0

    _print_scaling_table(rows)

    gates_ok = all(gates.values())
    print("  gates: " + ", ".join(f"{k}={v}" for k, v in sorted(gates.items())))
    print()

    verdict, msg = _interpret_full(rows)
    print(f"READING (observation-only, this run/machine) [{verdict}]: {msg}")
    print("  Full-mode timing is OBSERVATIONAL only -- never a pass/fail gate. "
          "On heterogeneous hardware a NOISY / INCONCLUSIVE reading past W=4 is "
          "expected, not a failure.")
    print("  End-to-end numbers include the Ray actor boundary and should not be "
          "read as a pure engine metric. This is not 'RayX makes Ray faster' / "
          "'HPX beats Ray' / 'RayX replaces Ray', and not a sizing/capacity claim.")
    print()

    if gates_ok:
        print("STRUCTURAL GATES: PASS")
        return 0
    print("STRUCTURAL GATES: FAIL")
    return 1


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quick", action="store_true",
                    help="run the quick-mode CPU-scaling probe (the default)")
    ap.add_argument("--decouple", action="store_true",
                    help="run the decoupling panel "
                         "(min(num_lanes, hpx_threads, cores) bound)")
    ap.add_argument("--full", action="store_true",
                    help="run the full W in {1,2,4,8,16,32} sweep at K=32 "
                         "(prepared tool for future homogeneous many-core Linux "
                         "validation; smoke-only on this laptop)")
    args = ap.parse_args()
    if args.decouple:
        return _print_decouple()
    if args.full:
        return _print_full()
    return _print_quick()


if __name__ == "__main__":
    sys.exit(main())
