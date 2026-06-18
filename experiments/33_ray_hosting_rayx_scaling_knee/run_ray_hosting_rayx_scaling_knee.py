#!/usr/bin/env python3
"""exp33 -- intra-actor RayX/HPX native CPU scaling KNEE sweep (prepared tool).

CORE QUESTION (observation-only; NOT a benchmark, NOT a Ray comparison verdict):
inside ONE long-lived Ray actor (one process, one Ray boundary held constant),
*where* does RayX/HPX native Async CPU scaling (`busy_sum`) stop being efficient
as the worker count W grows, and how does that knee move with operation
GRANULARITY (the per-op work size `n`, which also sets the derived
`checkpoint_count = ceil(n / BUSY_SUM_STRIDE)`)?

This is the deferred saturation-knee follow-up to exp32. exp32 established (on
homogeneous many-core Linux) that intra-actor RayX/HPX `busy_sum` scales while an
in-process Python CPU loop stays GIL-bound; it deliberately capped W at 4 on the
Apple-silicon laptop and left the W>4 knee to "a homogeneous many-core box". exp33
is that box's tool. It does NOT reinterpret exp32 and makes no new exp32 claim.

PREPARED TOOL, NOT YET TRUSTED EVIDENCE. The knee is hardware-gated: on Apple
silicon / P-core+E-core / SMT / laptop-thermal hardware the W>4 rolloff is
confounded and any output here is SMOKE-ONLY, not evidence. A trusted knee reading
requires a homogeneous many-core Linux node (see `--full`).

METHOD -- strong scaling of a fixed batch, per (granularity, leg) normalized:
  * For each granularity `n`, a batch is K `busy_sum(n)` ops. K >= max(W) (we use
    K = 2*max(W)) so every W cell can keep all workers occupied -- never repeat
    exp32's earlier K=4-vs-W>4 occupancy cap. K is held FIXED across the W sweep
    within a granularity, so it is strong scaling of one batch.
  * We vary W = num_lanes = hpx_threads and measure the wall to complete all K.
  * SPEEDUP vs W=1 = T(W=1) / T(W); EFFICIENCY = speedup / W. The knee is the
    first W whose efficiency falls below a conservative threshold (default 0.70),
    or "no clear knee in tested range".
  * busy_sum's CHECKPOINT STRIDE is a compile-time constant (BUSY_SUM_STRIDE in
    runtime_ops.hpp), NOT a runtime parameter -- we do not modify C++ to make it
    one. Sweeping `n` is the available lever: it moves the derived
    `checkpoint_count`, so the granularity sweep doubles as a coarse
    checkpoint-count sweep, reported per cell.

The RayX/HPX scaling knee is the point. A pure-Python leg is OPTIONAL and small
(`--with-py`): it is only a GIL flatness reference at the smallest granularity, not
the focus (exp32 already made the GIL point).

ALLOWED CLAIM (only on appropriate hardware, only if the data supports it):
  "Inside one long-lived Ray actor, intra-process RayX/HPX native Async CPU
   scaling stays efficient up to W=<knee> for this op granularity on this machine,
   and rolls off beyond it."
FORBIDDEN: "RayX makes Ray faster", "HPX beats Ray", "RayX replaces Ray", Ray
cluster scaling, any benchmark / sizing / capacity guidance, any raw
Python-vs-RayX wall-time speedup claim.

End-to-end numbers include the Ray actor boundary and are not a pure engine
metric; the in-actor numbers are the engine-clean comparison.

Observation-only and machine-specific. No JSONL/corpus artifact is written; the
runner only prints a compact summary. Skips cleanly (exit 0) if Ray or the built
`_rayx` / `rayx.runtime` is unavailable.

Usage:
    python experiments/33_ray_hosting_rayx_scaling_knee/run_ray_hosting_rayx_scaling_knee.py --smoke
    python experiments/33_ray_hosting_rayx_scaling_knee/run_ray_hosting_rayx_scaling_knee.py --full
    (add --with-py to either for the small optional GIL reference leg)

Exit code: 0 == structural gates passed or cleanly skipped; 1 == a structural gate
failed. Timing/efficiency is NEVER a pass/fail gate.
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

# busy_sum checkpoint stride (mirrors the compile-time BUSY_SUM_STRIDE in
# runtime_ops.hpp). Used ONLY to report the derived checkpoint_count of each
# granularity; it is not a knob we can set at runtime.
BUSY_SUM_STRIDE = 8192

# Conservative efficiency threshold for the observational knee. Efficiency =
# speedup / W; the knee is the first W (> 1) below this. This is a reporting
# threshold, NOT a pass/fail gate.
KNEE_EFFICIENCY = 0.70

WARMUP = 1

# ---- smoke mode (cheap, Mac/laptop-safe structural validation) ----
# Small W, small K, small granularities, few reps: exercises every code path
# (granularity sweep, knee detection, gates) in well under a minute. SMOKE-ONLY.
SMOKE_W = (1, 2, 4)
SMOKE_N = (500_000, 2_000_000)     # two granularities -> two checkpoint counts
SMOKE_REPS = 2

# ---- full mode (homogeneous many-core Linux knee sweep) ----
# W walks up to (and, on smaller boxes, past) the core count so the knee is
# visible; 32 is appended only when there are enough logical CPUs. K = 2*max(W).
FULL_W_BASE = (1, 2, 4, 8, 16)
FULL_W_HIGH = 32                   # appended only if cpu_count >= FULL_W_HIGH
FULL_N = (2_000_000, 20_000_000)   # small + large granularity
FULL_REPS = 5

# Optional small Python GIL-reference leg (`--with-py`): smallest granularity
# only, K capped, so it stays bounded even in full mode. Not the focus.
PY_K_CAP = 8
PY_REPS = 2


def busy_sum_value(n):
    """Closed form of busy_sum(n): (n*(n-1)/2) mod 2^31. Used for GATES only."""
    return (n * (n - 1) // 2) % (2 ** 31)


def busy_sum_checkpoints(n):
    """Derived checkpoint_count for busy_sum(n): ceil(n/STRIDE), >= 1. Mirrors
    busy_sum_checkpoints() in runtime_ops.hpp; reported, never set."""
    if n <= BUSY_SUM_STRIDE:
        return 1
    return (n - 1) // BUSY_SUM_STRIDE + 1


# --------------------------------------------------------------------------- #
# Ray actors (each runs in its own Ray worker process)                        #
# --------------------------------------------------------------------------- #
def _build_actors(ray):
    """Define the Ray actors inside a function so this module imports without Ray
    present (clean-skip path). Same Ray-hosted shape as exp32."""

    @ray.remote
    class RayxCpuActor:
        """Hosts ONE rayx.runtime.Runtime(num_lanes=w, hpx_threads=w) and runs
        Async-policy busy_sum across its lanes. ActorHandle/RuntimeFuture/
        OperationResult are created and retired INSIDE this actor; only plain
        scalars/containers cross the Ray boundary."""

        def __init__(self, num_lanes, hpx_threads, rayx_src):
            import sys as _sys
            if rayx_src not in _sys.path:
                _sys.path.insert(0, rayx_src)
            from rayx.runtime import Runtime         # raises if _rayx missing
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
            return {"agg": agg, "in_actor_ms": in_actor_ms, "k": k, "n": n,
                    "num_lanes": self._num_lanes, "hpx_threads": self._hpx_threads,
                    "statuses": statuses}

        def shutdown(self):
            self._rt.shutdown()

    @ray.remote
    class PyCpuActor:
        """Optional small pure-Python GIL reference (SERIAL; `w` is nominal). Only
        run with --with-py, at the smallest granularity, with a capped K."""

        def run_batch(self, w, n, k):
            t0 = time.perf_counter()
            agg = 0
            for _ in range(k):
                acc = 0
                for i in range(n):                  # genuine CPU work
                    acc = (acc + i) & 0x7FFFFFFF
                agg += acc
            in_actor_ms = (time.perf_counter() - t0) * 1e3
            return {"agg": agg, "in_actor_ms": in_actor_ms, "k": k, "n": n, "w": w}

    return RayxCpuActor, PyCpuActor


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


# --------------------------------------------------------------------------- #
# Sweep                                                                        #
# --------------------------------------------------------------------------- #
def run_sweep(w_values, k, n_values, reps, with_py):
    """Returns (rows, gates, skipped, reason). rows: per-(n, leg, W) dicts with
    timing + speedup + efficiency. The RAYX leg covers the full (n, W) grid; the
    optional PY leg covers only the smallest n with a capped K."""
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
    agg_ok = True
    futures_completed = True
    lane_ids_ok = True
    plain_types_ok = True
    clean_shutdown = True

    max_w = max(w_values)
    ray.init(num_cpus=max_w + 1, ignore_reinit_error=True,
             log_to_driver=False, configure_logging=False)
    RayxCpuActor, PyCpuActor = _build_actors(ray)
    rows = []
    try:
        # ===== RAYX leg: full (n, W) grid, one Runtime per W per granularity ====
        for n in n_values:
            expected_agg = k * busy_sum_value(n)
            for w in w_values:
                actor = RayxCpuActor.options(num_cpus=w).remote(w, w, RAYX_SRC)
                ids = ray.get(actor.lane_ids.remote())
                if len(ids) != w or not all(
                        isinstance(i, str) and i.startswith("rt-hpx-") for i in ids):
                    lane_ids_ok = False
                res, ia, e2e = _measure(
                    ray, lambda a=actor, n=n: a.run_batch.remote(n, k),
                    WARMUP, reps)
                for r in res:
                    if r["agg"] != expected_agg:
                        agg_ok = False
                    if any(s != "completed" for s in r["statuses"]):
                        futures_completed = False
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
                rows.append({"leg": "RAYX", "n": n, "k": k, "w": w,
                             "in_actor": ia, "end_to_end": e2e})

        # ===== optional small PY GIL reference: smallest n, capped K ============
        if with_py:
            n_py = min(n_values)
            k_py = min(k, PY_K_CAP)
            expected_agg = k_py * busy_sum_value(n_py)
            py_actor = PyCpuActor.options(num_cpus=1).remote()
            for w in w_values:
                res, ia, e2e = _measure(
                    ray,
                    lambda w=w, n=n_py: py_actor.run_batch.remote(w, n, k_py),
                    WARMUP, PY_REPS)
                for r in res:
                    if r["agg"] != expected_agg:
                        agg_ok = False
                    if not (isinstance(r, dict) and all(
                            isinstance(r[key], (int, float))
                            for key in ("agg", "in_actor_ms", "k", "n", "w"))):
                        plain_types_ok = False
                rows.append({"leg": "PY", "n": n_py, "k": k_py, "w": w,
                             "in_actor": ia, "end_to_end": e2e})
            ray.kill(py_actor)
    finally:
        ray.shutdown()

    # ---- per (granularity, leg) normalized speedup + efficiency ----
    by = {}
    for row in rows:
        by.setdefault((row["n"], row["leg"]), {})[row["w"]] = row
    for (_n, _leg), perw in by.items():
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
# Knee detection + reporting                                                   #
# --------------------------------------------------------------------------- #
def _full_w_values(cpu):
    """Full-mode W sweep: (1,2,4,8,16), plus 32 when there are enough logical
    CPUs. Returns (w_values, warning|None). We do NOT drop high-W cells (reaching
    and exceeding the core count is how the knee becomes visible), but we warn
    when a W exceeds the logical CPU count so oversubscription rolloff is not
    misread as a true scaling limit."""
    w_values = list(FULL_W_BASE)
    if cpu is not None and cpu >= FULL_W_HIGH:
        w_values.append(FULL_W_HIGH)
    warn = None
    if cpu is not None:
        over = [w for w in w_values if w > cpu]
        if over:
            warn = (f"W cells {over} exceed cpu_count={cpu} (logical, incl. SMT): "
                    f"these are OVERSUBSCRIBED -- any efficiency rolloff there may "
                    f"be oversubscription, not a true scaling knee.")
    return w_values, warn


def _knee(perw):
    """perw: {w: row} for one (granularity, RAYX) curve. Returns (knee_w, eff) of
    the first W>1 with efficiency < KNEE_EFFICIENCY, else (None, min_eff_info)."""
    ws = sorted(perw)
    for w in ws:
        if w == 1:
            continue
        if perw[w]["efficiency"] < KNEE_EFFICIENCY:
            return w, perw[w]["efficiency"]
    # no knee: report the worst (lowest) efficiency seen above W=1
    above = [(w, perw[w]["efficiency"]) for w in ws if w != 1]
    if not above:
        return None, None
    w_min, eff_min = min(above, key=lambda t: t[1])
    return None, (w_min, eff_min)


def _print_table_and_knees(rows):
    """Print one table per granularity (RAYX, then optional PY) plus the knee
    reading for each RAYX granularity curve."""
    ns = sorted({r["n"] for r in rows})
    by = {}
    for r in rows:
        by.setdefault((r["n"], r["leg"]), {})[r["w"]] = r

    knee_lines = []
    for n in ns:
        cpc = busy_sum_checkpoints(n)
        print(f"  granularity n={n} (derived checkpoint_count={cpc}):")
        hdr = (f"    {'leg':<6}{'W':>3}{'K':>4}{'in_actor_ms(med)':>18}"
               f"{'spread':>9}{'speedup':>9}{'eff':>7}{'end_to_end_ms(med)':>20}")
        print(hdr)
        for leg in ("RAYX", "PY"):
            perw = by.get((n, leg))
            if not perw:
                continue
            for w in sorted(perw):
                row = perw[w]
                print(f"    {leg:<6}{w:>3}{row['k']:>4}"
                      f"{row['in_actor_med']:>18.1f}"
                      f"{row['in_actor_spread'] * 100:>8.1f}%"
                      f"{row['speedup']:>9.2f}{row['efficiency']:>7.2f}"
                      f"{row['end_to_end_med']:>20.1f}")
        rayx_perw = by.get((n, "RAYX"))
        if rayx_perw:
            knee_w, info = _knee(rayx_perw)
            if knee_w is not None:
                knee_lines.append(
                    f"  n={n}: knee at W={knee_w} "
                    f"(efficiency {info:.2f} < {KNEE_EFFICIENCY:.2f}).")
            elif info is not None:
                w_min, eff_min = info
                knee_lines.append(
                    f"  n={n}: no clear knee in tested range "
                    f"(min efficiency {eff_min:.2f} at W={w_min}, "
                    f">= {KNEE_EFFICIENCY:.2f}).")
            else:
                knee_lines.append(f"  n={n}: only W=1 tested; no knee reading.")
        print()
    print("KNEE READING (observation-only, this run/machine):")
    for line in knee_lines:
        print(line)
    print()


def _run_mode(mode, w_values, k, n_values, reps, with_py, w_warn):
    """Shared print path for --smoke and --full."""
    title = ("exp33 RayX/HPX intra-actor CPU scaling KNEE -- "
             + ("SMOKE (cheap local structural validation; SMOKE-ONLY, not "
                "evidence)" if mode == "smoke"
                else "FULL (prepared tool for homogeneous many-core Linux)"))
    print(title)
    _print_machine_info()
    print("WARNING -- exp33 is intended for a HOMOGENEOUS MANY-CORE LINUX node.")
    print("  Apple-silicon P-core/E-core asymmetry, SMT, and laptop thermal")
    print("  throttling confound the W>4 knee. Output on this Mac/laptop is")
    print("  SMOKE-ONLY, NOT evidence; do not tune thresholds or read a knee")
    print("  from it.")
    if w_warn:
        print(f"  NOTE: {w_warn}")
    print()
    cpcs = ", ".join(f"n={n}->cp={busy_sum_checkpoints(n)}" for n in n_values)
    print(f"  W_values={list(w_values)} K={k} (>= max(W)) "
          f"granularities=[{cpcs}] warmup={WARMUP} reps={reps} "
          f"with_py={with_py} knee_efficiency={KNEE_EFFICIENCY}\n")

    rows, gates, skipped, reason = run_sweep(w_values, k, n_values, reps, with_py)
    if skipped:
        print(f"SKIP: {reason}")
        return 0

    _print_table_and_knees(rows)

    gates_ok = all(gates.values())
    print("  gates: " + ", ".join(f"{key}={v}" for key, v in sorted(gates.items())))
    print()
    print("  Observation-only and machine-specific. Timing/efficiency is NEVER a "
          "pass/fail gate; the knee is a reading, not a verdict.")
    print("  This is NOT 'RayX makes Ray faster' / 'HPX beats Ray' / 'RayX "
          "replaces Ray', NOT Ray cluster scaling, and NOT a benchmark / sizing / "
          "capacity claim. It does not reinterpret exp32.")
    print("  End-to-end numbers include the Ray actor boundary and are not a pure "
          "engine metric.")
    print()

    if gates_ok:
        print("STRUCTURAL GATES: PASS")
        return 0
    print("STRUCTURAL GATES: FAIL")
    return 1


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--smoke", action="store_true",
                    help="cheap local structural validation (Mac/laptop-safe, "
                         "SMOKE-ONLY; the default)")
    ap.add_argument("--full", action="store_true",
                    help="homogeneous many-core Linux knee sweep "
                         "(W up to 16/32, K=2*max(W), small+large granularity)")
    ap.add_argument("--with-py", action="store_true",
                    help="add the small optional pure-Python GIL reference leg "
                         "(smallest granularity, capped K)")
    args = ap.parse_args()

    if args.full:
        w_values, w_warn = _full_w_values(os.cpu_count())
        k = 2 * max(w_values)
        return _run_mode("full", w_values, k, FULL_N, FULL_REPS,
                         args.with_py, w_warn)

    # default: smoke
    cpu = os.cpu_count() or max(SMOKE_W)
    w_values = [w for w in SMOKE_W if w <= cpu] or [1]
    k = 2 * max(w_values)
    return _run_mode("smoke", w_values, k, SMOKE_N, SMOKE_REPS,
                     args.with_py, None)


if __name__ == "__main__":
    sys.exit(main())
