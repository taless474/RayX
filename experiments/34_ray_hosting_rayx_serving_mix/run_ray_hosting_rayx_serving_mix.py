#!/usr/bin/env python3
"""exp34 -- intra-actor RayX/HPX serving-shaped latency-mix probe (slices 1+2).

OBSERVATION-ONLY, MACHINE-SPECIFIC. This probes the CURRENT Ray-hosted
Runtime/lane adapter under a synthetic serving-shaped mix. It does NOT evaluate
HPX-native priority scheduling. The current ServiceLane / RuntimeLane adapter
serves each lane as a FIFO queue, so any head-of-line behavior observed here is a
property of THIS ADAPTER, not a general HPX scheduling property. Priorities and
cooperative-yield policy are OUT OF SCOPE; W=32 / NUMA / binding are OUT OF SCOPE.

CORE QUESTION (not a benchmark, not a Ray comparison verdict):
inside ONE long-lived Ray actor (one process, one Runtime, one Ray boundary held
constant), at a FIXED clean worker count W, does a synthetic serving-shaped mix of
small-but-real CPU work (class S) and heavier CPU work (class C) show a fixed-W
scheduling/interference effect that is NOT merely exp32 homogeneous CPU
strong-scaling?

WHY FIXED-W IS PRIMARY: latency under a heterogeneous mix is, in an HPX-native
view, a SCHEDULING/yield/priority question at fixed worker count -- not simply a
matter of adding OS workers. So fixed-W behavior is the headline; the optional W
sweep (--full) is SECONDARY and must NOT be read as an exp32-style scaling result.

PRIMARY FACTOR -- offered load relative to the per-W knee:
  * under-knee : outstanding window < W (lanes underutilized; little queueing)
  * near-knee  : outstanding window ~ W (roughly matched)
  * over-knee  : outstanding window > W (more in-flight than workers; queueing)
The runner drives a CLOSED-LOOP window: it keeps ~`outstanding` requests in
flight, submitting a replacement as each retires, so the load level is a real,
controlled factor rather than a single all-at-once batch.

REQUEST CLASSES:
  * S = short / small-but-real CPU request: Async `busy_sum(n_s)`.
  * C = heavier CPU request:                Async `busy_sum(n_c)`.
  * Wp = SYNTHETIC parked wait (opt-in --with-parked): Async `park_ms(ms)`.
We deliberately do NOT use the instantaneous Inline `square`/`add` as the S
latency signal -- a sub-microsecond op would measure lane + Python-boundary
overhead, not HPX. S/C are Async `busy_sum` so they actually dispatch to the HPX
worker pool.

SLICE 1 (default, S/C only): a baseline/harness. It exercises NO HPX-native
overlap or priority/yield mechanism, so SUPPORT is unreachable by construction and
a clean full run reads BASELINE/STOP (narrow), never plain STOP.

SLICE 2 (opt-in --with-parked, S/C/Wp): SUPPORT-capable. park_ms COOPERATIVELY
suspends the HPX task (frees the worker), so parked waits can overlap compute --
a synthetic cooperative-overlap property with no CPython analog and not reducible
to exp32 homogeneous CPU strong-scaling. Wp is SYNTHETIC: NOT real I/O, NOT
inference, NOT a latency-SLO result, and parked readings never assert wall-clock
(overlap_ratio is approximate/structural and never a pass/fail gate).

METRICS (all observation-only; timing is NEVER a pass/fail gate):
  * overall + per-class throughput (requests / in-actor second)
  * per-class total-latency p50 / p90 from OperationResult.total_ms
  * per-class p99 ONLY when the pooled per-class sample count is high enough
    (>= P99_MIN_SAMPLES); otherwise printed as `NA`
  * per-class APPROXIMATE queue_wait_ms / service_ms_observed as CONTEXT ONLY.
    queue_wait_ms is APPROXIMATE: it crosses the Python submit clock and the
    native service clock (different domains) and is clamped at 0, so it is NOT
    used for per-class p99 queue/service attribution. Precise tail attribution
    would require native enqueue/dequeue timing in C++, which is OUT OF SCOPE.
  * in-actor wall and end-to-end wall reported SEPARATELY; end-to-end includes the
    Ray actor boundary and is NOT a pure engine metric.
  * per-class sample counts are always printed (so a missing p99 is self-explaining).

STRUCTURAL GATES (the ONLY pass/fail):
  agg_ok, futures_completed, plain_types_ok, lane_ids_ok, clean_shutdown.
Exit 0 == gates passed or cleanly skipped (Ray or rayx.runtime unavailable);
exit 1 == a structural gate failed.

ALLOWED CLAIMS:
  "Under this synthetic serving-shaped mix, the current Ray-hosted Runtime/lane
   adapter [does/does not] show a fixed-W scheduling/interference effect."
  "Observation-only and machine-specific."
  "This does not evaluate HPX priority scheduling because priorities are out of
   scope."
FORBIDDEN: "RayX makes Ray faster", "HPX beats Ray", "RayX replaces Ray", Ray
cluster scaling, Ray Serve behavior, real inference, latency-SLO / capacity /
sizing guidance, p99 queue/service attribution from cross-clock queue_wait_ms,
NUMA/socket attribution without binding evidence, and treating FIFO-lane HOL
blocking as an HPX-native scheduling property.

SCOPE (source-complete exp34): lane_impl="std" only; no W=32; no lane_impl="hpx";
no HPX priorities; no pools/partitioning/APEX/numactl/binding/counters/affinity; no
JSONL/corpus artifact. The only optional mechanism is the synthetic --with-parked
Wp class above.

Usage:
    python experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py                  # --smoke (default), S/C baseline
    python experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py --smoke --with-parked
    python experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py --full
    python experiments/34_ray_hosting_rayx_serving_mix/run_ray_hosting_rayx_serving_mix.py --full --with-parked
"""
import argparse
import os
import random
import statistics
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)

# busy_sum checkpoint stride (mirrors BUSY_SUM_STRIDE in runtime_ops.hpp); used
# only to report the derived checkpoint_count of each class's op.
BUSY_SUM_STRIDE = 8192

# Baseline S/C mix (slice 1, default): fraction of the mix that is the heavy class
# C (by count). Deterministic per-class counts: n_c = round(N * F_C), n_s = N - n_c.
F_C = 0.25
SEED = 1234  # deterministic mix order (seeded shuffle); identical per cell/rep.

# Parked S/C/Wp mix (slice 2, opt-in --with-parked): fractions of class C and the
# synthetic parked-wait class Wp; S = remainder. Wp is SYNTHETIC COOPERATIVE PARKED
# WAIT via park_ms -- NOT real I/O, NOT inference, NOT a latency-SLO result.
F_C_PARKED = 0.20
F_WP_PARKED = 0.30
# park_ms argument for the Wp class (synthetic parked-wait duration). Kept small so
# runs stay cheap; park_ms cooperatively suspends the HPX task (frees the worker),
# which is what lets parked waits overlap compute. Echoed back as the op's value.
PARK_MS = 5

# Stable print/iteration order for request classes (filtered to those present).
CLASS_ORDER = ("S", "C", "Wp")

# Interpretation-only (NEVER a gate): an approximate/structural overlap signal of
# at least this median ratio across W<=16 cells is read as a cooperative-overlap
# SUPPORT signal under --with-parked. park-related readings never assert wall-clock.
OVERLAP_SUPPORT = 2.0

# p99 is only emitted when the POOLED per-class sample count reaches this floor;
# otherwise the cell prints `NA`. p50/p90 are always printed (observational).
P99_MIN_SAMPLES = 100

# Load levels (primary factor). Mapped to a closed-loop outstanding window below.
LOAD_LEVELS = ("under", "near", "over")

# ---- smoke-mode parameters (laptop-safe DEFAULT; SMOKE-ONLY, not evidence) ----
W_SMOKE = 4            # one fixed clean W point (capped to cpu_count below)
N_SMOKE = 60           # total requests per call (low -> p99 will be NA: expected)
REPS_SMOKE = 2
WARMUP_SMOKE = 1
# fine granularity only in smoke: (n_s, n_c)
GRAN_SMOKE = (("fine", 200_000, 2_000_000),)

# ---- full-mode parameters (homogeneous many-core Linux; observation-only) ----
W_FULL = (4, 8, 16)    # NO W=32 in slice 1 (capped to cpu_count below)
N_FULL = 200           # per-class pooled counts reach >= P99_MIN_SAMPLES at reps=3
REPS_FULL = 3
WARMUP_FULL = 1
# fine + coarse granularity (reuse the exp33 lesson that the knee moves with n)
GRAN_FULL = (("fine", 200_000, 2_000_000),
             ("coarse", 2_000_000, 20_000_000))


def busy_sum_value(n):
    """Closed form of busy_sum(n): (n*(n-1)/2) mod 2^31. Used for GATES only."""
    return (n * (n - 1) // 2) % (2 ** 31)


def checkpoint_count(n):
    """Derived checkpoint_count = ceil(n / BUSY_SUM_STRIDE). Reported for context."""
    return (n + BUSY_SUM_STRIDE - 1) // BUSY_SUM_STRIDE


def mix_counts(n_total, with_parked):
    """EXACT per-class counts for the mix. Baseline (S/C) or parked (S/C/Wp).

    Baseline: n_c = round(N*F_C), n_s = N - n_c.
    Parked:   n_c = round(N*F_C_PARKED), n_wp = round(N*F_WP_PARKED),
              n_s = N - n_c - n_wp. Deterministic, so per-class counts gate cleanly."""
    if with_parked:
        n_c = round(n_total * F_C_PARKED)
        n_wp = round(n_total * F_WP_PARKED)
        n_s = n_total - n_c - n_wp
        return {"S": n_s, "C": n_c, "Wp": n_wp}
    n_c = round(n_total * F_C)
    n_s = n_total - n_c
    return {"S": n_s, "C": n_c}


def mix_sequence(counts, seed):
    """Deterministic shuffled label list with EXACT per-class counts (seeded)."""
    seq = []
    for cls in CLASS_ORDER:
        seq += [cls] * counts.get(cls, 0)
    random.Random(seed).shuffle(seq)
    return seq


def class_ops_for(n_s, n_c, ms, with_parked):
    """Map each present class to its (op_id, arg). S/C are Async busy_sum; Wp is the
    synthetic cooperative parked wait park_ms(ms) (echoes ms; Async)."""
    ops = {"S": ("busy_sum", n_s), "C": ("busy_sum", n_c)}
    if with_parked:
        ops["Wp"] = ("park_ms", ms)
    return ops


def expected_value(op_id, arg):
    """Closed-form completion value used for the agg gate (driver-side only):
    busy_sum(n) -> busy_sum_value(n); park_ms(ms) -> ms (the op echoes ms back)."""
    return busy_sum_value(arg) if op_id == "busy_sum" else arg


def outstanding_for(level, w, n_total):
    """Closed-loop window size for a load level relative to the per-W knee."""
    if level == "under":
        o = max(1, w // 2)
    elif level == "near":
        o = w
    else:  # over
        o = w * 4
    return max(1, min(o, n_total))


# --------------------------------------------------------------------------- #
# Ray actor (runs in its own Ray worker process; hosts ONE Runtime)           #
# --------------------------------------------------------------------------- #
def _build_actor(ray):
    """Define the Ray actor inside a function so this module imports without Ray
    present (clean-skip path)."""

    @ray.remote
    class RayxServingActor:
        """Hosts ONE rayx.runtime.Runtime(num_lanes=W, hpx_threads=W) and runs a
        synthetic serving mix (S/C, plus optional synthetic parked Wp) over a
        CLOSED-LOOP window. RuntimeFuture / OperationResult are created and retired
        INSIDE this actor; only plain scalars/lists/dicts cross the Ray boundary."""

        def __init__(self, w, rayx_src):
            import sys as _sys
            if rayx_src not in _sys.path:
                _sys.path.insert(0, rayx_src)
            from rayx.runtime import Runtime  # raises if _rayx missing
            self._rt = Runtime(num_lanes=w, hpx_threads=w)
            self._w = w

        def lane_ids(self):
            return [d["actor_id"] for d in self._rt.lane_stats()]

        def run_mix(self, seq, class_ops, outstanding):
            """Drive the closed-loop mix; return ONLY plain rows/aggregates.

            `class_ops` maps each class label -> [op_id, arg] (e.g. S/C ->
            ["busy_sum", n]; Wp -> ["park_ms", ms]). Keeps ~`outstanding` requests
            in flight, submitting a replacement as each retires. Per-class
            value_sum is returned (a plain int) so the DRIVER can check correctness
            against the closed form -- the actor does not import the driver module."""
            classes = sorted(class_ops)
            cls_of = {}

            def submit(i):
                cls = seq[i]
                op_id, arg = class_ops[cls]
                f = self._rt.submit_operation(op_id, arg)
                cls_of[f] = cls
                return f

            per_class = {c: {"count": 0, "value_sum": 0, "total_ms": [],
                             "queue_wait_ms": [], "service_ms": [],
                             "all_completed": True} for c in classes}
            n_completed = 0
            n_total = len(seq)
            idx = 0
            inflight = []

            t0 = time.perf_counter()
            while idx < n_total and len(inflight) < outstanding:
                inflight.append(submit(idx))
                idx += 1
            while inflight:
                ready, inflight = self._rt.wait(inflight, num_returns=1)
                for f in ready:
                    cls = cls_of.pop(f)
                    res = f.result()         # retired INSIDE the actor
                    row = res.row
                    pc = per_class[cls]
                    pc["count"] += 1
                    pc["total_ms"].append(float(row["total_ms"]))
                    pc["queue_wait_ms"].append(float(row["queue_wait_ms"]))
                    pc["service_ms"].append(float(row["service_ms_observed"]))
                    if row["status"] != "completed":
                        pc["all_completed"] = False
                    else:
                        n_completed += 1
                        pc["value_sum"] += int(res.value)
                    if idx < n_total:
                        inflight.append(submit(idx))
                        idx += 1
            in_actor_ms = (time.perf_counter() - t0) * 1e3

            return {"in_actor_ms": float(in_actor_ms), "n_total": int(n_total),
                    "n_completed": int(n_completed), "per_class": per_class}

        def shutdown(self):
            self._rt.shutdown()

    return RayxServingActor


# --------------------------------------------------------------------------- #
# Measurement helpers                                                          #
# --------------------------------------------------------------------------- #
def _pct(xs, q):
    """Linear-interpolation percentile (q in [0,100]; p100 == max). Observational."""
    s = sorted(xs)
    if not s:
        return float("nan")
    if len(s) == 1:
        return float(s[0])
    pos = (len(s) - 1) * (q / 100.0)
    lo = int(pos)
    frac = pos - lo
    if lo + 1 < len(s):
        return s[lo] + (s[lo + 1] - s[lo]) * frac
    return float(s[lo])


def _mean(xs):
    return statistics.fmean(xs) if xs else float("nan")


def _check_plain_result(r):
    """True iff the actor's result dict is composed only of plain Python-safe
    types (no RayX/Ray objects leaked across the boundary)."""
    if not (isinstance(r, dict)
            and isinstance(r["in_actor_ms"], float)
            and isinstance(r["n_total"], int)
            and isinstance(r["n_completed"], int)
            and isinstance(r["per_class"], dict)):
        return False
    for pc in r["per_class"].values():
        if not (isinstance(pc, dict)
                and isinstance(pc["count"], int)
                and isinstance(pc["value_sum"], int)
                and isinstance(pc["all_completed"], bool)
                and all(isinstance(v, float) for v in pc["total_ms"])
                and all(isinstance(v, float) for v in pc["queue_wait_ms"])
                and all(isinstance(v, float) for v in pc["service_ms"])):
            return False
    return True


def _run_cells(w_values, gran_specs, n_total, reps, warmup, with_parked):
    """Run every (W, granularity, load-level) cell. Returns
    (cells, gates, skipped, reason). `cells` is a list of per-cell metric dicts;
    `gates` is the aggregated structural-gate dict. With `with_parked`, the mix adds
    the synthetic parked class Wp (park_ms) and each cell carries a parked-overlap
    context block."""
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
    counts0 = mix_counts(n_total, with_parked)
    seq = mix_sequence(counts0, SEED)
    classes = sorted(counts0)

    agg_ok = True
    futures_completed = True
    lane_ids_ok = True
    plain_types_ok = True
    clean_shutdown = True

    max_w = max(w_values)
    ray.init(num_cpus=max_w + 1, ignore_reinit_error=True,
             log_to_driver=False, configure_logging=False)
    RayxServingActor = _build_actor(ray)
    cells = []
    try:
        for w in w_values:
            for gran_name, n_s, n_c in gran_specs:
                # one actor (one Runtime) per (W, granularity); all load levels
                # share it so the FIFO-lane adapter is held constant across levels.
                actor = RayxServingActor.options(num_cpus=w).remote(w, RAYX_SRC)
                ids = ray.get(actor.lane_ids.remote())
                if len(ids) != w or not all(
                        isinstance(i, str) and i.startswith("rt-hpx-")
                        for i in ids):
                    lane_ids_ok = False
                class_ops = class_ops_for(n_s, n_c, PARK_MS, with_parked)
                exp_val = {c: expected_value(*class_ops[c]) for c in classes}
                for level in LOAD_LEVELS:
                    o = outstanding_for(level, w, n_total)
                    call = (lambda a=actor, co=class_ops, ow=o:
                            a.run_mix.remote(seq, co, ow))
                    for _ in range(warmup):
                        ray.get(call())
                    pooled = {c: {"total_ms": [], "qw": [], "svc": []}
                              for c in classes}
                    counts = {c: 0 for c in classes}
                    value_sums = {c: 0 for c in classes}
                    in_actor, end_to_end = [], []
                    for _ in range(reps):
                        t0 = time.perf_counter()
                        r = ray.get(call())
                        end_to_end.append((time.perf_counter() - t0) * 1e3)
                        in_actor.append(r["in_actor_ms"])
                        if not _check_plain_result(r):
                            plain_types_ok = False
                        if r["n_completed"] != r["n_total"]:
                            futures_completed = False
                        for c in classes:
                            pc = r["per_class"][c]
                            pooled[c]["total_ms"] += pc["total_ms"]
                            pooled[c]["qw"] += pc["queue_wait_ms"]
                            pooled[c]["svc"] += pc["service_ms"]
                            counts[c] += pc["count"]
                            value_sums[c] += pc["value_sum"]
                            if not pc["all_completed"]:
                                futures_completed = False
                    # agg gate: exact per-class counts AND value correctness
                    for c in classes:
                        if counts[c] != counts0[c] * reps:
                            agg_ok = False
                        if value_sums[c] != exp_val[c] * counts0[c] * reps:
                            agg_ok = False
                    cells.append(_cell_metrics(
                        w, gran_name, level, n_s, n_c, o, n_total, reps,
                        pooled, counts, in_actor, end_to_end,
                        with_parked=with_parked))
                try:
                    ray.get(actor.shutdown.remote())
                except Exception:
                    clean_shutdown = False
                ray.kill(actor)
    finally:
        ray.shutdown()

    gates = {
        "agg_ok": agg_ok,
        "futures_completed": futures_completed,
        "lane_ids_ok": lane_ids_ok,
        "plain_types_ok": plain_types_ok,
        "clean_shutdown": clean_shutdown,
    }
    return cells, gates, False, None


def _cell_metrics(w, gran, level, n_s, n_c, outstanding, n_total, reps,
                  pooled, counts, in_actor, end_to_end, with_parked=False):
    """Reduce one cell's pooled per-request rows to a plain metric dict."""
    in_actor_med = statistics.median(in_actor)
    total_in_actor_s = sum(in_actor) / 1e3
    per_class = {}
    for c in pooled:
        tm = pooled[c]["total_ms"]
        samples = len(tm)
        thrpt = (samples / total_in_actor_s) if total_in_actor_s > 0 else float("nan")
        per_class[c] = {
            "samples": samples,
            "p50": _pct(tm, 50) if tm else float("nan"),
            "p90": _pct(tm, 90) if tm else float("nan"),
            # p99 ONLY when pooled samples are sufficient; else None -> "NA"
            "p99": _pct(tm, 99) if samples >= P99_MIN_SAMPLES else None,
            "mean_qw": _mean(pooled[c]["qw"]),
            "mean_svc": _mean(pooled[c]["svc"]),
            "thrpt": thrpt,
        }
    overall_samples = sum(counts.values())
    overall_thrpt = ((overall_samples / total_in_actor_s)
                     if total_in_actor_s > 0 else float("nan"))
    cell = {
        "w": w, "gran": gran, "level": level,
        "n_s": n_s, "n_c": n_c, "outstanding": outstanding,
        "n_total": n_total, "reps": reps,
        "in_actor_med": in_actor_med,
        "end_to_end_med": statistics.median(end_to_end),
        "overall_thrpt": overall_thrpt,
        "per_class": per_class,
    }
    if with_parked and "Wp" in per_class:
        # Parked-overlap CONTEXT (approximate/structural; NEVER a gate, never a
        # wall-clock claim). total_parked_demand_ms is the serial parked-time
        # demand of all Wp ops across reps; in_actor_wall_ms is the actual wall.
        # overlap_ratio = demand / wall > 1 means parked waits were overlapped
        # (cooperative park frees the worker), not paid serially. It does NOT by
        # itself prove cooperative-vs-compute overlap -- read it with the S/C table.
        wp_samples = per_class["Wp"]["samples"]
        wall_ms = sum(in_actor)
        demand_ms = float(wp_samples * PARK_MS)
        cell["parked"] = {
            "wp_count": wp_samples,
            "park_ms": PARK_MS,
            "total_parked_demand_ms": demand_ms,
            "in_actor_wall_ms": float(wall_ms),
            "overlap_ratio": (demand_ms / wall_ms) if wall_ms > 0 else float("nan"),
        }
    return cell


# --------------------------------------------------------------------------- #
# Machine-info capture (portable, cheap; the run never depends on it)          #
# --------------------------------------------------------------------------- #
def _lscpu_summary():
    if not sys.platform.startswith("linux"):
        return None
    import shutil
    import subprocess
    if shutil.which("lscpu") is None:
        return None
    try:
        out = subprocess.run(["lscpu"], capture_output=True, text=True, timeout=2)
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
def _cap_w_values(w_values, cpu):
    """Drop W cells that exceed the logical CPU count (with a warning), so an
    oversubscribed W is not misread. Slice 1 has no W=32, so this is only a
    small-machine guard."""
    if cpu is None:
        return list(w_values), None
    capped = [w for w in w_values if w <= cpu]
    if not capped:
        capped = [1]
    if capped != list(w_values):
        dropped = [w for w in w_values if w > cpu]
        return capped, (f"cpu_count={cpu} < some W cells; dropped {dropped} "
                        f"(not appropriate on this machine).")
    return capped, None


def _print_cell_table(cells):
    """Per-class metric table, grouped by (W, granularity), one block per cell."""
    last_key = None
    for c in sorted(cells, key=lambda x: (x["w"], x["gran"],
                                          LOAD_LEVELS.index(x["level"]))):
        key = (c["w"], c["gran"])
        if key != last_key:
            cc_s, cc_c = checkpoint_count(c["n_s"]), checkpoint_count(c["n_c"])
            print(f"  W={c['w']}  granularity={c['gran']}  "
                  f"n_s={c['n_s']} (cpc={cc_s})  n_c={c['n_c']} (cpc={cc_c})")
            last_key = key
        print(f"    load={c['level']:<5} outstanding={c['outstanding']:<3} "
              f"N={c['n_total']} reps={c['reps']}  "
              f"in_actor_ms(med)={c['in_actor_med']:.1f}  "
              f"end_to_end_ms(med)={c['end_to_end_med']:.1f}  "
              f"overall_thrpt={c['overall_thrpt']:.0f}/s")
        hdr = (f"      {'class':<6}{'samples':>8}{'p50_ms':>9}{'p90_ms':>9}"
               f"{'p99_ms':>9}{'mean_qw':>9}{'mean_svc':>10}{'thrpt/s':>10}")
        print(hdr)
        for cl in CLASS_ORDER:
            if cl not in c["per_class"]:
                continue
            m = c["per_class"][cl]
            p99 = "NA" if m["p99"] is None else f"{m['p99']:.2f}"
            print(f"      {cl:<6}{m['samples']:>8}{m['p50']:>9.2f}{m['p90']:>9.2f}"
                  f"{p99:>9}{m['mean_qw']:>9.2f}{m['mean_svc']:>10.2f}"
                  f"{m['thrpt']:>10.0f}")
        p = c.get("parked")
        if p:
            print(f"      parked-overlap (synthetic; approx/structural, NOT a "
                  f"gate): Wp={p['wp_count']} park_ms={p['park_ms']} "
                  f"parked_demand_ms={p['total_parked_demand_ms']:.0f} "
                  f"wall_ms={p['in_actor_wall_ms']:.1f} "
                  f"overlap_ratio={p['overlap_ratio']:.2f}")
        print()


def _interpret(cells, mode, with_parked):
    """Observation-only reading.

    BASELINE path (S/C, no --with-parked): exercises NO overlap or priority/yield
    mechanism, so SUPPORT is unreachable by construction; a clean full run reads
    BASELINE/STOP (narrow), never plain STOP.

    PARKED path (--with-parked): SUPPORT-capable -- a fixed-W (W<=16) synthetic
    cooperative-overlap signal (parked waits overlapped, parked demand >> wall)
    that is not reducible to exp32 homogeneous CPU strong-scaling reads SUPPORT
    (narrow). Otherwise STOP (no useful overlap signal). INCONCLUSIVE if dominated
    by the boundary/measurement or if samples are too sparse."""
    # Boundary dominance: end-to-end materially above in-actor anywhere.
    boundary_hot = [c for c in cells
                    if c["in_actor_med"] > 0
                    and (c["end_to_end_med"] - c["in_actor_med"])
                    / c["in_actor_med"] > 0.5]
    # Tail readability: did any per-class cell reach p99 eligibility?
    any_p99 = any(m["p99"] is not None
                  for c in cells for m in c["per_class"].values())

    obs = []
    if with_parked:
        # Parked-overlap observation (approximate/structural, NEVER a gate, never a
        # wall-clock claim): overlap_ratio per (W, granularity) at over-knee.
        for c in sorted(cells, key=lambda x: (x["w"], x["gran"],
                                              LOAD_LEVELS.index(x["level"]))):
            p = c.get("parked")
            if p and c["level"] == "over":
                obs.append(f"W={c['w']}/{c['gran']}/over: parked_demand="
                           f"{p['total_parked_demand_ms']:.0f}ms wall="
                           f"{p['in_actor_wall_ms']:.1f}ms overlap_ratio="
                           f"{p['overlap_ratio']:.2f}")
    else:
        # Fixed-W interference observation: per (W, granularity), S-class p90 under
        # over-knee vs under-knee. A large ratio is FIFO-adapter head-of-line
        # blocking, NOT an HPX-native scheduling property.
        groups = {}
        for c in cells:
            groups.setdefault((c["w"], c["gran"]), {})[c["level"]] = c
        for (w, g), lv in sorted(groups.items()):
            if "under" in lv and "over" in lv:
                su = lv["under"]["per_class"]["S"]["p90"]
                so = lv["over"]["per_class"]["S"]["p90"]
                if su and su > 0:
                    obs.append(f"W={w}/{g}: S p90 under->over = "
                               f"{su:.2f}->{so:.2f} ms (x{so / su:.2f})")

    if mode == "smoke":
        kind = "parked-overlap" if with_parked else "S/C baseline"
        return ("INCONCLUSIVE (smoke-only)",
                f"smoke validates {kind} structure only -- sample counts are "
                "deliberately low (per-class p99 = NA expected) and this is NOT "
                "evidence. Run --full"
                f"{' --with-parked' if with_parked else ''} on a homogeneous "
                "many-core Linux node for an observation.", obs)
    if boundary_hot:
        return ("INCONCLUSIVE",
                "the Ray actor boundary / measurement is not negligible vs in-actor "
                "work in some cells (end-to-end > 1.5x in-actor); the effect cannot "
                "be cleanly separated -- size the per-class work larger or treat "
                "this run as boundary-dominated.", obs)
    if not any_p99:
        return ("INCONCLUSIVE",
                "no per-class cell reached the p99 sample floor; tail behavior is "
                "unreadable on this run (raise N or reps).", obs)

    if with_parked:
        # SUPPORT-capable cooperative-overlap reading at fixed W (<=16 by
        # construction; no W=32 in exp34). overlap_ratio is approximate/structural
        # and NEVER a gate; SUPPORT is phrased narrowly.
        ratios = [c["parked"]["overlap_ratio"] for c in cells
                  if c.get("parked")
                  and c["parked"]["overlap_ratio"] == c["parked"]["overlap_ratio"]]
        med_overlap = statistics.median(ratios) if ratios else float("nan")
        if ratios and med_overlap >= OVERLAP_SUPPORT:
            return ("SUPPORT",
                    "under this SYNTHETIC parked-wait mix, the current Ray-hosted "
                    "Runtime/lane adapter shows a fixed-W cooperative-overlap "
                    f"signal (median overlap_ratio={med_overlap:.2f} >= "
                    f"{OVERLAP_SUPPORT:g}: parked demand is absorbed rather than "
                    "paid serially while S/C metrics remain in the table). This is "
                    "a synthetic cooperative-overlap property (park_ms suspends the "
                    "HPX task and frees the worker), NOT reducible to exp32 "
                    "homogeneous CPU strong-scaling. Wp is synthetic, NOT real I/O; "
                    "this is NOT HPX priority scheduling, NOT Ray Serve, NOT Ray "
                    "cluster scaling, NOT a latency-SLO/capacity claim, NOT 'HPX "
                    "beats Ray', NOT 'RayX makes Ray faster'.", obs)
        return ("STOP",
                "the synthetic parked-wait mix shows no useful fixed-W "
                f"cooperative-overlap signal (median overlap_ratio={med_overlap:.2f}"
                f" < {OVERLAP_SUPPORT:g}) or it stays adapter/boundary dominated; no "
                "W=32/NUMA/binding is motivated. overlap_ratio is "
                "approximate/structural only.", obs)

    # Slice 1 baseline: SUPPORT unreachable by construction.
    return ("BASELINE/STOP",
            "slice 1 is an S/C FIFO-adapter BASELINE only: it exercises no "
            "HPX-native overlap or priority/yield mechanism, so it CANNOT produce "
            "SUPPORT and this is NOT final exp34 evidence (and NOT a claim that the "
            "adapter has no serving behavior). Any S-vs-C head-of-line interference "
            "above is a FIFO ADAPTER property, not HPX-native scheduling. If this "
            "baseline is comfortable in W<=16, that does NOT motivate "
            "W=32/NUMA/binding; the next meaningful lever, if needed, is the "
            "--with-parked cooperative-overlap path or a priority/yield slice.",
            obs)


def _print_reading(cells, mode, with_parked):
    verdict, msg, obs = _interpret(cells, mode, with_parked)
    print(f"READING (observation-only, this run/machine) [{verdict}]: {msg}")
    if obs:
        if with_parked:
            print("  parked-overlap observations (synthetic; approx/structural, "
                  "NEVER a gate, never a wall-clock claim):")
        else:
            print("  fixed-W interference observations (FIFO-adapter property, NOT "
                  "HPX-native scheduling):")
        for line in obs:
            print(f"    {line}")
    if with_parked:
        print("  Wp is SYNTHETIC cooperative parked wait (park_ms): NOT real I/O, "
              "NOT inference, NOT a latency-SLO result; overlap_ratio is "
              "approximate/structural and never a pass/fail gate.")
    print("  queue_wait_ms is APPROXIMATE (Python submit clock vs native service "
          "clock, clamped at 0): context only, never per-class p99 attribution.")
    print("  End-to-end includes the Ray actor boundary and is NOT a pure engine "
          "metric. This is not 'RayX makes Ray faster' / 'HPX beats Ray' / 'RayX "
          "replaces Ray', not Ray Serve, not real inference, and not a "
          "sizing/capacity claim. It does NOT evaluate HPX priority scheduling "
          "(priorities are out of scope).")
    print()


def _run_and_print(mode, with_parked):
    slice_tag = "slice 2 parked-overlap" if with_parked else "slice 1 S/C baseline"
    if mode == "smoke":
        w_values, warn = _cap_w_values((W_SMOKE,), os.cpu_count())
        gran_specs, n_total = GRAN_SMOKE, N_SMOKE
        reps, warmup = REPS_SMOKE, WARMUP_SMOKE
        title = (f"exp34 serving-shaped latency-mix (SMOKE, {slice_tag}) -- "
                 "structural validation only; SMOKE-ONLY, not evidence")
    else:
        w_values, warn = _cap_w_values(W_FULL, os.cpu_count())
        gran_specs, n_total = GRAN_FULL, N_FULL
        reps, warmup = REPS_FULL, WARMUP_FULL
        title = (f"exp34 serving-shaped latency-mix (FULL, {slice_tag}) -- "
                 "homogeneous many-core Linux; observation-only, machine-specific")

    print(title)
    _print_machine_info()
    print("NOTE -- observation-only probe of the CURRENT Ray-hosted Runtime/lane")
    print("  adapter (FIFO lanes). Does NOT evaluate HPX priority scheduling.")
    print("  FIXED-W is primary; the W set is secondary and is NOT an exp32-style")
    print("  scaling result. NO W=32, NO lane_impl=hpx, NO priorities.")
    if with_parked:
        print("  --with-parked: SUPPORT-capable. Wp = SYNTHETIC cooperative parked")
        print("  wait (park_ms), NOT real I/O / inference / latency-SLO.")
    else:
        print("  S/C baseline (slice 1): BASELINE/STOP only; SUPPORT unreachable.")
    if mode != "smoke" and not sys.platform.startswith("linux"):
        print("  On Mac/laptop/heterogeneous hardware this --full output is "
              "SMOKE-ONLY, not evidence.")
    if warn:
        print(f"  {warn}")
    counts0 = mix_counts(n_total, with_parked)
    counts_str = " ".join(f"{k}={counts0[k]}" for k in CLASS_ORDER if k in counts0)
    parked_str = f" park_ms={PARK_MS}" if with_parked else ""
    print(f"  W_values={list(w_values)} load_levels={list(LOAD_LEVELS)} "
          f"N={n_total} ({counts_str}){parked_str} reps={reps} "
          f"warmup={warmup} p99_min_samples={P99_MIN_SAMPLES}")
    print()

    cells, gates, skipped, reason = _run_cells(
        w_values, gran_specs, n_total, reps, warmup, with_parked)
    if skipped:
        print(f"SKIP: {reason}")
        return 0

    _print_cell_table(cells)
    gates_ok = all(gates.values())
    print("  gates: " + ", ".join(f"{k}={v}" for k, v in sorted(gates.items())))
    print()
    _print_reading(cells, mode, with_parked)

    if gates_ok:
        print("STRUCTURAL GATES: PASS")
        return 0
    print("STRUCTURAL GATES: FAIL")
    return 1


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--smoke", action="store_true",
                    help="structural smoke (the default): W=4, fine granularity, "
                         "few requests; SMOKE-ONLY, not evidence")
    ap.add_argument("--full", action="store_true",
                    help="W in {4,8,16} x fine/coarse granularity x under/near/over "
                         "load levels (homogeneous many-core Linux; no W=32)")
    ap.add_argument("--with-parked", action="store_true",
                    help="slice 2: add the SYNTHETIC cooperative parked class Wp "
                         "(park_ms) -> SUPPORT-capable cooperative-overlap path. "
                         "Still no W=32, std lane only, no priorities, not real I/O")
    args = ap.parse_args()
    return _run_and_print("full" if args.full else "smoke", args.with_parked)


if __name__ == "__main__":
    sys.exit(main())
