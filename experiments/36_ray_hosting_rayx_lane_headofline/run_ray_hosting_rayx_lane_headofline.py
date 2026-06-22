#!/usr/bin/env python3
"""exp36 -- Ray-hosted RayX lane-count head-of-line (HOL) diagnostic.

NO-NEW-API MECHANISM PROBE (observation-only, machine-specific). exp35 read a
valid STOP: the Ray-hosted RayX adapter did not broadly preserve fine-grain
compute-class retention under added synthetic parked load. Step-0 code inspection
settled the mechanism:

  * `park_ms` is DispatchPolicy::Async; the RuntimeLane consumer is an hpx::thread.
  * Async dispatch is `hpx::async(exec_, body).get()` -- so the worker is FREED
    during `hpx::this_thread::sleep_for` (cooperative suspension is true by
    construction), BUT the lane consumer waits on `.get()` and does NOT pop the
    next op until the park completes. A park therefore occupies its lane for ~park_ms.
  * submit_operation round-robins ops across `num_lanes` lanes.

So exp35's erosion is best explained by PER-LANE FIFO HEAD-OF-LINE occupancy: a
compute op round-robined behind a park on the same lane waits ~park_ms.

QUESTION:
  At fixed `hpx_threads` (fixed worker-pool capacity), does raising `num_lanes`
  (more serial FIFO admission slots) DILUTE per-lane HOL and RECOVER fine-grain
  compute retention? If yes, exp35's STOP is per-lane HOL, not worker contention,
  scheduler/timer churn, or the CPython driver.

ISOLATION (why this is clean):
  * Compute parallelism is hard-capped at `hpx_threads` (the parallel_executor
    pool), independent of `num_lanes`. So more lanes CANNOT buy compute capacity --
    retention cannot exceed ~1.0 for capacity reasons, which defends against a
    FALSE SUPPORT (a recovery means HOL was removed, not workers added).
  * Park COUNT and `park_ms` are held CONSTANT across the lane sweep, so the same
    timers fire in every cell -- only their distribution across lanes changes. The
    sweep separates "lanes" from "timer/scheduler churn" by construction. (Spreading
    parks over more lanes can even RAISE the instantaneous wake-up rate, which biases
    AGAINST the HOL hypothesis -- so a recovery is conservative evidence for HOL.)

THREE ARMS (per cell, exp35 structure):
  * Arm C  -- compute-only:  K_C  busy_sum(n_c) ops.
  * Arm Wp -- parked-only:   K_Wp park_ms(ms) ops.
  * Arm T  -- matched:       same K_C compute PLUS K_Wp parked, class-aware closed
               loop holding compute concurrency at O and adding parked on top.

PRIMARY READING -- compute-class RETENTION as a function of `num_lanes` at fixed
`hpx_threads`. Read the TREND/slope, not just the top endpoint (graded SUPPORT).

GATES (the ONLY pass/fail): agg_ok, futures_completed, plain_types_ok, lane_ids_ok,
clean_shutdown. Timing / retention / qd / lane_stats are READING criteria only.
Exit 0 = gates passed or cleanly skipped; exit 1 = a structural gate failed.

NOT real I/O, NOT inference, NOT Ray Serve, NOT Ray cluster scaling, NOT HPX
priority scheduling, NOT a latency-SLO/capacity/sizing claim, NOT "RayX makes Ray
faster" / "HPX beats Ray" / "RayX replaces Ray", and NO socket/NUMA attribution.
More lanes is a DIAGNOSTIC lever, not automatically the best production design;
exp36 does NOT evaluate the alternative non-blocking-consumer (continuation) lane
contract. `lane_impl` does not apply to rayx.runtime; no W=32; no NUMA/binding;
no priorities/pools/counters.

Usage:
    python experiments/36_ray_hosting_rayx_lane_headofline/run_ray_hosting_rayx_lane_headofline.py            # --smoke (default)
    python experiments/36_ray_hosting_rayx_lane_headofline/run_ray_hosting_rayx_lane_headofline.py --full
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

# busy_sum checkpoint stride (mirrors BUSY_SUM_STRIDE in runtime_ops.hpp); used only
# to report the derived checkpoint_count of the compute op.
BUSY_SUM_STRIDE = 8192

# Synthetic parked-wait duration for the Wp class (echoed back as the op's value).
# Single-chunk (< PARK_MS_STRIDE = 10 ms). Synthetic cooperative wait, NOT real I/O.
PARK_MS = 5

# p99 retention is only emitted when the pooled per-arm C sample count reaches this
# floor; otherwise NA. p50/p90 retention always reported (observational).
P99_MIN_SAMPLES = 100

# Cap on K_Wp from calibration, so timer/rescheduling overhead of an absurd parked
# count cannot masquerade as "no overlap".
K_WP_CAP = 400

# Reading thresholds (READING criteria only; NEVER structural gates). Identical to
# exp35 -- exp36 changes how they are COMBINED (graded slope reading), not the bands.
RET_THR_SUPPORT = 0.90    # C throughput retention >= this = compute intact
RET_P90_SUPPORT = 1.20    # C p90 retention <= this = latency intact
RET_THR_STOP = 0.70       # C throughput retention <= this = degraded/eroded

# Compute-baseline-flatness guard: compute-only wall_C should stay within this
# fractional band across the num_lanes sweep at fixed hpx_threads (else the ladder
# is INCONCLUSIVE -- adding lanes may be changing compute capacity / driver behavior).
WALL_C_FLAT_TOL = 0.25

# A "clear rise" in retention across the lane sweep needs at least this absolute gain
# from baseline (nl=ht) to the top nl to be read as a trend (else flat -> STOP/INCONC).
RET_RISE_MIN = 0.10

# How often (in retire iterations) to sample lane_stats during an arm.
LANE_SAMPLE_STRIDE = 3

# ---- smoke-mode parameters (laptop-safe DEFAULT; SMOKE-ONLY, not evidence) ----
LADDERS_SMOKE = ((4, (4, 8)),)        # hpx_threads -> num_lanes sweep
KC_SMOKE = 16
REPS_SMOKE = 2
WARMUP_SMOKE = 1
LEVELS_SMOKE = ("over",)
GRAN_SMOKE = ("fine", 2_000_000)

# ---- full-mode parameters (homogeneous many-core Linux; observation-only) ------
# Fine granularity only; no W=32; num_lanes capped at 16.
LADDERS_FULL = ((4, (4, 8, 16)), (8, (8, 16)))
KC_FULL = 60              # reps=3 -> pooled C >= 180 so p99 retention is eligible
REPS_FULL = 3
WARMUP_FULL = 1
LEVELS_FULL = ("near", "over")
GRAN_FULL = ("fine", 2_000_000)


def busy_sum_value(n):
    """Closed form of busy_sum(n): (n*(n-1)/2) mod 2^31. Used for GATES only."""
    return (n * (n - 1) // 2) % (2 ** 31)


def checkpoint_count(n):
    """Derived checkpoint_count = ceil(n / BUSY_SUM_STRIDE). Reported for context."""
    return (n + BUSY_SUM_STRIDE - 1) // BUSY_SUM_STRIDE


def expected_value(op_id, arg):
    """Closed-form completion value for the agg gate: busy_sum(n) -> busy_sum_value,
    park_ms(ms) -> ms (the op echoes ms back)."""
    return busy_sum_value(arg) if op_id == "busy_sum" else arg


def outstanding_for(level, ht):
    """Closed-loop window relative to hpx_threads (NOT num_lanes): the offered
    concurrency is held constant across the num_lanes sweep so that adding lanes
    spreads the SAME load over more admission slots."""
    if level == "under":
        return max(1, ht // 2)
    if level == "near":
        return ht
    return 4 * ht  # over


# --------------------------------------------------------------------------- #
# Ray actor (one per (hpx_threads, num_lanes) cell; hosts ONE Runtime)         #
# --------------------------------------------------------------------------- #
def _build_actor(ray):
    """Define the Ray actor inside a function so this module imports without Ray
    present (clean-skip path)."""

    @ray.remote
    class RayxLaneActor:
        """Hosts ONE rayx.runtime.Runtime(num_lanes=NL, hpx_threads=HT). NL and HT
        are DECOUPLED here (exp35 pinned NL==HT==W). One actor per (HT, NL) cell --
        num_lanes is a ctor parameter and the HPX runtime is process-global, so a
        fresh process per NL is the only safe way to vary it. RuntimeFuture /
        OperationResult are created and retired INSIDE this actor; only plain
        scalars/lists/dicts cross the Ray boundary."""

        def __init__(self, num_lanes, hpx_threads, rayx_src):
            import sys as _sys
            if rayx_src not in _sys.path:
                _sys.path.insert(0, rayx_src)
            from rayx.runtime import Runtime  # raises if _rayx missing
            self._rt = Runtime(num_lanes=num_lanes, hpx_threads=hpx_threads)
            self._nl = num_lanes
            self._ht = hpx_threads

        def lane_ids(self):
            return [d["actor_id"] for d in self._rt.lane_stats()]

        def run_arm(self, classes, sample_stride):
            """Run one arm. `classes` maps class label -> {op_id, arg, count,
            window}. Holds each class's concurrency at its window, refilling as ops
            retire. Returns ONLY plain rows/aggregates."""
            labels = sorted(classes)
            remaining = {c: int(classes[c]["count"]) for c in labels}
            window = {c: max(1, int(classes[c]["window"])) for c in labels}
            cls_of = {}
            per_class = {c: {"count": 0, "value_sum": 0, "total_ms": [],
                             "all_completed": True} for c in labels}
            n_completed = 0
            has_compute = "C" in labels

            def submit(c):
                op_id = classes[c]["op_id"]
                arg = classes[c]["arg"]
                f = self._rt.submit_operation(op_id, arg)
                cls_of[f] = c
                remaining[c] -= 1
                return f

            # lane_stats backlog/active trace (racy, context-only).
            samples = backlog_count = active_count = qd_max = 0
            qd_sum = 0

            def sample_lane_stats():
                nonlocal samples, backlog_count, active_count, qd_sum, qd_max
                try:
                    st = self._rt.lane_stats()
                except Exception:
                    return
                total_qd = sum(int(d["queue_depth"]) for d in st)
                any_active = any(bool(d["active"]) for d in st)
                samples += 1
                if total_qd > 0:
                    backlog_count += 1
                if any_active:
                    active_count += 1
                qd_sum += total_qd
                if total_qd > qd_max:
                    qd_max = total_qd

            inflight = []
            t0 = time.perf_counter()
            for c in labels:
                while remaining[c] > 0 and sum(
                        1 for f in inflight if cls_of[f] == c) < window[c]:
                    inflight.append(submit(c))
            wall_compute_ms = None
            it = 0
            while inflight:
                ready, inflight = self._rt.wait(inflight, num_returns=1)
                for f in ready:
                    c = cls_of.pop(f)
                    res = f.result()              # retired INSIDE the actor
                    row = res.row
                    pc = per_class[c]
                    pc["count"] += 1
                    pc["total_ms"].append(float(row["total_ms"]))
                    if row["status"] != "completed":
                        pc["all_completed"] = False
                    else:
                        n_completed += 1
                        pc["value_sum"] += int(res.value)
                    if remaining[c] > 0:
                        inflight.append(submit(c))
                if has_compute and wall_compute_ms is None \
                        and per_class["C"]["count"] == classes["C"]["count"]:
                    wall_compute_ms = (time.perf_counter() - t0) * 1e3
                it += 1
                if sample_stride > 0 and it % sample_stride == 0:
                    sample_lane_stats()
            wall_total_ms = (time.perf_counter() - t0) * 1e3
            if has_compute and wall_compute_ms is None:
                wall_compute_ms = wall_total_ms

            lane_trace = {
                "samples": samples,
                "backlog_seen": bool(backlog_count > 0),
                "active_fraction": (active_count / samples) if samples else 0.0,
                "qd_mean": (qd_sum / samples) if samples else 0.0,
                "qd_max": int(qd_max),
            }
            return {
                "wall_total_ms": float(wall_total_ms),
                "wall_compute_ms": (None if wall_compute_ms is None
                                    else float(wall_compute_ms)),
                "n_completed": int(n_completed),
                "per_class": per_class,
                "lane_trace": lane_trace,
            }

        def shutdown(self):
            self._rt.shutdown()

    return RayxLaneActor


# --------------------------------------------------------------------------- #
# Measurement helpers                                                          #
# --------------------------------------------------------------------------- #
def _pct(xs, q):
    """Linear-interpolation percentile (q in [0,100]). Observational."""
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


def _check_plain_arm(r):
    """True iff the actor's arm result is composed only of plain Python-safe types."""
    if not (isinstance(r, dict)
            and isinstance(r["wall_total_ms"], float)
            and (r["wall_compute_ms"] is None or isinstance(r["wall_compute_ms"], float))
            and isinstance(r["n_completed"], int)
            and isinstance(r["per_class"], dict)
            and isinstance(r["lane_trace"], dict)):
        return False
    for pc in r["per_class"].values():
        if not (isinstance(pc["count"], int)
                and isinstance(pc["value_sum"], int)
                and isinstance(pc["all_completed"], bool)
                and all(isinstance(v, float) for v in pc["total_ms"])):
            return False
    lt = r["lane_trace"]
    return (isinstance(lt["samples"], int)
            and isinstance(lt["backlog_seen"], bool)
            and isinstance(lt["active_fraction"], float)
            and isinstance(lt["qd_mean"], float)
            and isinstance(lt["qd_max"], int))


class _Gates:
    """Mutable structural-gate accumulator (the only pass/fail)."""

    def __init__(self):
        self.agg_ok = True
        self.futures_completed = True
        self.lane_ids_ok = True
        self.plain_types_ok = True
        self.clean_shutdown = True

    def as_dict(self):
        return {"agg_ok": self.agg_ok, "futures_completed": self.futures_completed,
                "lane_ids_ok": self.lane_ids_ok, "plain_types_ok": self.plain_types_ok,
                "clean_shutdown": self.clean_shutdown}


def _measure_arm(ray, actor, classes, reps, warmup, gates):
    """Run one arm `reps` times (after `warmup`). Aggregates per-call walls, pools
    C-class total_ms across reps, checks structural gates, and OR/means the lane
    trace. Returns a plain summary dict."""
    call = (lambda: actor.run_arm.remote(classes, LANE_SAMPLE_STRIDE))
    for _ in range(warmup):
        ray.get(call())
    wall_total, wall_compute = [], []
    pooled_C = []
    counts = {c: 0 for c in classes}
    value_sums = {c: 0 for c in classes}
    expected_total = sum(int(classes[c]["count"]) for c in classes)
    lt_backlog = False
    lt_active, lt_qdmean = [], []
    lt_qdmax = 0
    for _ in range(reps):
        r = ray.get(call())
        if not _check_plain_arm(r):
            gates.plain_types_ok = False
        if r["n_completed"] != expected_total:
            gates.futures_completed = False
        wall_total.append(r["wall_total_ms"])
        if r["wall_compute_ms"] is not None:
            wall_compute.append(r["wall_compute_ms"])
        for c in classes:
            pc = r["per_class"][c]
            counts[c] += pc["count"]
            value_sums[c] += pc["value_sum"]
            if not pc["all_completed"]:
                gates.futures_completed = False
            if c == "C":
                pooled_C += pc["total_ms"]
        lt = r["lane_trace"]
        lt_backlog = lt_backlog or lt["backlog_seen"]
        lt_active.append(lt["active_fraction"])
        lt_qdmean.append(lt["qd_mean"])
        lt_qdmax = max(lt_qdmax, lt["qd_max"])
    # agg gate: exact per-class counts and value correctness.
    for c in classes:
        exp_v = expected_value(classes[c]["op_id"], classes[c]["arg"])
        if counts[c] != int(classes[c]["count"]) * reps:
            gates.agg_ok = False
        if value_sums[c] != exp_v * int(classes[c]["count"]) * reps:
            gates.agg_ok = False
    return {
        "wall_total_med": statistics.median(wall_total),
        "wall_compute_med": (statistics.median(wall_compute)
                             if wall_compute else statistics.median(wall_total)),
        "pooled_C": pooled_C,
        "lane_trace": {"backlog_seen": lt_backlog,
                       "active_fraction": (statistics.fmean(lt_active)
                                           if lt_active else 0.0),
                       "qd_mean": (statistics.fmean(lt_qdmean) if lt_qdmean else 0.0),
                       "qd_max": lt_qdmax},
    }


def _run_ladders(ladders, gran_spec, levels, k_c, reps, warmup):
    """Run every (hpx_threads, num_lanes, level) cell. Returns (cells, gates_dict,
    skipped, reason). `cells` is a flat list of plain reading dicts."""
    gates = _Gates()
    try:
        import ray  # noqa: F401
    except Exception as e:
        return [], gates.as_dict(), True, f"ray unavailable: {type(e).__name__}: {e}"
    try:
        from rayx.runtime import Runtime  # noqa: F401
    except Exception as e:
        return ([], gates.as_dict(), True,
                f"rayx.runtime unavailable: {type(e).__name__}: {e}")

    import ray
    gran_name, n_c = gran_spec
    max_ht = max(ht for ht, _ in ladders)
    ray.init(num_cpus=max_ht + 1, ignore_reinit_error=True,
             log_to_driver=False, configure_logging=False)
    RayxLaneActor = _build_actor(ray)
    cells = []
    try:
        for ht, nl_sweep in ladders:
            for level in levels:
                o = outstanding_for(level, ht)
                # Calibrate the 1x K_Wp ONCE per (ht, level) from the BASELINE
                # (num_lanes == hpx_threads) cell, then HOLD it across the whole
                # num_lanes sweep so the lane sweep varies ONLY num_lanes (parked
                # demand + timer load held constant).
                k_wp = None
                for nl in nl_sweep:
                    # Ray num_cpus = hpx_threads (compute capacity), NOT num_lanes:
                    # more admission slots must not buy more CPUs (fixed resources).
                    actor = RayxLaneActor.options(num_cpus=ht).remote(
                        nl, ht, RAYX_SRC)
                    ids = ray.get(actor.lane_ids.remote())
                    if len(ids) != nl or not all(
                            isinstance(i, str) and i.startswith("rt-hpx-")
                            for i in ids):
                        gates.lane_ids_ok = False
                    # ---- Arm C (compute-only) -> wall_C, C baseline -------------
                    arm_c = _measure_arm(
                        ray, actor,
                        {"C": {"op_id": "busy_sum", "arg": n_c, "count": k_c,
                               "window": o}},
                        reps, warmup, gates)
                    wall_C = arm_c["wall_compute_med"]
                    if k_wp is None:
                        k_wp_1x = (int(round(wall_C * o / PARK_MS))
                                   if PARK_MS > 0 else o)
                        k_wp = max(ht, min(K_WP_CAP, k_wp_1x))
                    # ---- Arm Wp (parked-only) -> wall_Wp -----------------------
                    arm_wp = _measure_arm(
                        ray, actor,
                        {"Wp": {"op_id": "park_ms", "arg": PARK_MS,
                                "count": k_wp, "window": o}},
                        reps, warmup, gates)
                    # ---- Arm T (matched) ---------------------------------------
                    arm_t = _measure_arm(
                        ray, actor,
                        {"C": {"op_id": "busy_sum", "arg": n_c, "count": k_c,
                               "window": o},
                         "Wp": {"op_id": "park_ms", "arg": PARK_MS,
                                "count": k_wp, "window": o}},
                        reps, warmup, gates)
                    cells.append(_cell_metrics(
                        ht, nl, gran_name, n_c, level, o, k_c, k_wp, reps,
                        arm_c, arm_wp, arm_t))
                    try:
                        ray.get(actor.shutdown.remote())
                    except Exception:
                        gates.clean_shutdown = False
                    ray.kill(actor)
    finally:
        ray.shutdown()
    return cells, gates.as_dict(), False, None


def _cell_metrics(ht, nl, gran, n_c, level, o, k_c, k_wp, reps,
                  arm_c, arm_wp, arm_t):
    """Reduce one (hpx_threads, num_lanes, level) cell to a plain reading dict."""
    wall_C = arm_c["wall_compute_med"]
    wall_Wp = arm_wp["wall_total_med"]
    wall_CWp = arm_t["wall_compute_med"]
    wall_CWp_total = arm_t["wall_total_med"]

    # throughput from the compute-completion wall (per-call basis).
    thr_C_ref = (k_c / (wall_C / 1e3)) if wall_C > 0 else float("nan")
    thr_C_test = (k_c / (wall_CWp / 1e3)) if wall_CWp > 0 else float("nan")
    thr_retention = (thr_C_test / thr_C_ref) if thr_C_ref > 0 else float("nan")

    cC, cT = arm_c["pooled_C"], arm_t["pooled_C"]
    n_samples = min(len(cC), len(cT))

    def ret(q):
        a, b = _pct(cC, q), _pct(cT, q)
        return (b / a) if a > 0 else float("nan")

    p99_ret = ret(99) if n_samples >= P99_MIN_SAMPLES else None

    return {
        "ht": ht, "nl": nl, "gran": gran, "n_c": n_c, "level": level,
        "outstanding": o, "k_c": k_c, "k_wp": k_wp, "reps": reps,
        "wall_C": wall_C, "wall_Wp": wall_Wp,
        "wall_CWp": wall_CWp, "wall_CWp_total": wall_CWp_total,
        "thr_retention": thr_retention,
        "p50_ret": ret(50), "p90_ret": ret(90), "p99_ret": p99_ret,
        "n_samples_C": n_samples,
        "qd_mean_C": arm_c["lane_trace"]["qd_mean"],
        "qd_mean_T": arm_t["lane_trace"]["qd_mean"],
        "qd_max_T": arm_t["lane_trace"]["qd_max"],
        "lane_trace_T": arm_t["lane_trace"],
    }


# --------------------------------------------------------------------------- #
# Machine-info (portable, cheap; the run never depends on it)                  #
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
def _fmt(x, spec=".2f"):
    if x is None:
        return "NA"
    try:
        if x != x:  # NaN
            return "NA"
    except TypeError:
        return "NA"
    return format(x, spec)


def _cap_ladders(ladders, cpu):
    """Drop any cell whose hpx_threads exceeds the local cpu_count (the worker pool
    can't exceed cores meaningfully); never drops num_lanes (lanes are admission
    slots, intentionally allowed > cores for the diagnostic)."""
    if cpu is None:
        return ladders, None
    kept, dropped = [], []
    for ht, sweep in ladders:
        if ht <= cpu:
            kept.append((ht, sweep))
        else:
            dropped.append(ht)
    if not kept:
        kept = [(1, (1,))]
    if dropped:
        return kept, (f"cpu_count={cpu} < some hpx_threads; dropped ladders "
                      f"hpx_threads={dropped} (not appropriate on this machine).")
    return kept, None


def _print_cell_table(cells):
    last_key = None
    for c in sorted(cells, key=lambda x: (x["ht"], x["level"], x["nl"])):
        key = (c["ht"], c["level"])
        if key != last_key:
            print(f"  hpx_threads={c['ht']}  load={c['level']} "
                  f"outstanding={c['outstanding']}  granularity={c['gran']} "
                  f"(n_c={c['n_c']}, cpc={checkpoint_count(c['n_c'])})  "
                  f"K_C={c['k_c']} K_Wp={c['k_wp']} reps={c['reps']}")
            last_key = key
        base = " (baseline nl==ht)" if c["nl"] == c["ht"] else ""
        print(f"    num_lanes={c['nl']:<3}{base:<18} "
              f"wall_C={_fmt(c['wall_C'],'.1f')} wall_Wp={_fmt(c['wall_Wp'],'.1f')} "
              f"wall_CWp={_fmt(c['wall_CWp'],'.1f')} (total="
              f"{_fmt(c['wall_CWp_total'],'.1f')}) ms")
        print(f"        thr_retention={_fmt(c['thr_retention'])}  "
              f"p50/p90/p99 ret={_fmt(c['p50_ret'])}/{_fmt(c['p90_ret'])}/"
              f"{_fmt(c['p99_ret'])} (Cn={c['n_samples_C']})")
        print(f"        qd_mean[C={_fmt(c['qd_mean_C'],'.1f')} "
              f"T={_fmt(c['qd_mean_T'],'.1f')}] qd_max_T={c['qd_max_T']} "
              f"lane_stats_T[backlog={c['lane_trace_T']['backlog_seen']} "
              f"active_frac={_fmt(c['lane_trace_T']['active_fraction'])}]")
    print()


def _ladder_key(c):
    return (c["ht"], c["level"])


def _interpret_ladder(ladder_cells):
    """Graded reading for ONE (hpx_threads, level) ladder, sorted by num_lanes.
    Returns (verdict, message). Reads the TREND across num_lanes, not just the top
    endpoint."""
    cells = sorted(ladder_cells, key=lambda x: x["nl"])
    base = cells[0]      # nl == ht (baseline)
    top = cells[-1]      # largest num_lanes
    ht, level = base["ht"], base["level"]
    tag = f"hpx_threads={ht} load={level}"

    base_ret = base["thr_retention"]
    top_ret = top["thr_retention"]
    rets = [c["thr_retention"] for c in cells]

    # --- Control 1: positive control (baseline must erode) ---
    if not (base_ret == base_ret and base_ret <= RET_THR_STOP):
        return ("INCONCLUSIVE",
                f"[{tag}] baseline (nl={base['nl']}) did NOT reproduce erosion "
                f"(thr_retention={_fmt(base_ret)} > {RET_THR_STOP:g}): no disease to "
                "cure, so the lane-count lever is untestable on this ladder.")

    # --- Control 2: driver / lane_stats (matched arms must be backlogged/active) ---
    backlog_any = any(c["lane_trace_T"]["backlog_seen"] for c in cells)
    active_mean = statistics.fmean(
        [c["lane_trace_T"]["active_fraction"] for c in cells])
    if not backlog_any and active_mean < 0.1:
        return ("INCONCLUSIVE",
                f"[{tag}] lane_stats shows HPX not backlogged/active in the matched "
                "arms (driver-governed): wall indicators measure the CPython driver, "
                "not the scheduler. Raise K / windows.")

    # --- Control 3: compute-baseline flatness across num_lanes ---
    wall_cs = [c["wall_C"] for c in cells if c["wall_C"] > 0]
    flat_ok = True
    if wall_cs:
        wmin, wmax = min(wall_cs), max(wall_cs)
        if wmin > 0 and (wmax - wmin) / wmin > WALL_C_FLAT_TOL:
            flat_ok = False
    if not flat_ok:
        curve = " ".join(f"nl={c['nl']}:{_fmt(c['wall_C'],'.1f')}ms" for c in cells)
        return ("INCONCLUSIVE",
                f"[{tag}] compute-only wall_C is NOT flat across num_lanes "
                f"(>{WALL_C_FLAT_TOL:.0%}: {curve}): adding lanes may be changing "
                "compute capacity or driver behavior, so retention recovery cannot be "
                "cleanly attributed to head-of-line dilution.")

    # --- HOL corroboration: does queue depth fall as num_lanes rises? ---
    qd_base = base["qd_mean_T"]
    qd_top = top["qd_mean_T"]
    qd_falls = (qd_base > 0 and qd_top < qd_base)

    # --- Trend / graded reading ---
    rise = (top_ret - base_ret) if (top_ret == top_ret and base_ret == base_ret) \
        else float("nan")
    monotone = all(rets[i + 1] >= rets[i] - 0.03 for i in range(len(rets) - 1))
    top_recovers = (top_ret == top_ret and top_ret >= RET_THR_SUPPORT
                    and (top["p90_ret"] != top["p90_ret"]
                         or top["p90_ret"] <= RET_P90_SUPPORT))

    qd_note = ("queue depth fell with more lanes (qd_mean_T "
               f"{_fmt(qd_base,'.1f')}->{_fmt(qd_top,'.1f')})" if qd_falls else
               "queue depth did NOT fall with more lanes (qd_mean_T "
               f"{_fmt(qd_base,'.1f')}->{_fmt(qd_top,'.1f')}) -- the HOL story is "
               "INCOMPLETE for this ladder")

    if top_recovers and rise >= RET_RISE_MIN and qd_falls:
        return ("FULL SUPPORT",
                f"[{tag}] baseline eroded (thr_retention={_fmt(base_ret)}) and "
                f"compute RECOVERED as num_lanes rose (-> {_fmt(top_ret)} at "
                f"nl={top['nl']}, p90 {_fmt(top['p90_ret'])}); {qd_note}. At fixed "
                "hpx_threads (no added worker capacity), this confirms per-lane FIFO "
                "head-of-line as the exp35 eroder.")
    if rise >= RET_RISE_MIN and monotone:
        return ("PARTIAL SUPPORT",
                f"[{tag}] retention rises clearly with num_lanes "
                f"({' -> '.join(_fmt(r) for r in rets)}) but the top cell did not "
                f"fully reach the SUPPORT band (top={_fmt(top_ret)}, "
                f"p90={_fmt(top['p90_ret'])}); {qd_note}. Lane count is a MAJOR "
                "contributor, but adding lanes did not fully restore compute "
                "retention under this cell -- a residual (worker-pool/scheduler/"
                "driver) remains.")
    if top_ret == top_ret and top_ret <= RET_THR_STOP and rise < RET_RISE_MIN:
        return ("STOP",
                f"[{tag}] retention stayed flat/bad across the lane sweep "
                f"({' -> '.join(_fmt(r) for r in rets)}; top={_fmt(top_ret)} <= "
                f"{RET_THR_STOP:g}); {qd_note}. Increasing admission slots did NOT "
                "recover compute: per-lane head-of-line is not sufficient/dominant on "
                "this evidence -- the cause lies elsewhere (worker-pool/scheduler/"
                "timer churn or the CPython driver).")
    return ("INCONCLUSIVE",
            f"[{tag}] retention trend is noisy/non-monotone/mid-band without a clear "
            f"slope ({' -> '.join(_fmt(r) for r in rets)}); {qd_note}. Cannot cleanly "
            "separate head-of-line dilution from noise on this ladder.")


def _print_reading(cells, mode):
    if mode == "smoke":
        print("READING (observation-only) [INCONCLUSIVE (smoke-only)]: smoke "
              "validates the decoupled num_lanes/hpx_threads ladder, three-arm / "
              "calibration / lane_stats structure only -- sample counts are low and "
              "this is NOT evidence. Run --full on a homogeneous many-core Linux "
              "node for an observation.")
    else:
        # Group cells into (hpx_threads, level) ladders and read each.
        keys = sorted({_ladder_key(c) for c in cells})
        print("READING (observation-only, this run/machine) -- per-ladder graded "
              "lane-count head-of-line verdict:")
        for k in keys:
            ladder = [c for c in cells if _ladder_key(c) == k]
            verdict, msg = _interpret_ladder(ladder)
            print(f"  [{verdict}] {msg}")
    print("  Step-0 mechanism: park_ms is Async; the RuntimeLane consumer is an "
          "hpx::thread doing hpx::async(exec_, body).get(), so the park FREES the "
          "HPX worker (cooperative suspension is TRUE BY CONSTRUCTION) but the lane "
          "consumer is HELD until the park completes -> per-lane FIFO head-of-line.")
    print("  Compute parallelism is capped at hpx_threads regardless of num_lanes, "
          "so more lanes cannot buy compute capacity (defends against a false "
          "SUPPORT); park count/duration are held constant across the lane sweep, so "
          "timer/scheduler-churn load is fixed and any recovery is attributable to "
          "lanes alone.")
    print("  exp36 confirms or refutes the lane-HOL mechanism WITHIN the current "
          "serial-lane contract; it does NOT evaluate a non-blocking RuntimeLane "
          "consumer (continuations instead of .get()). More lanes is a DIAGNOSTIC "
          "lever, not automatically the best production design.")
    print("  park_ms is SYNTHETIC cooperative parked wait (NOT real I/O, NOT "
          "inference). Not Ray Serve, not cluster scaling, not HPX priority "
          "scheduling, not NUMA, not a latency-SLO/capacity/performance claim.")
    print()


def _run_and_print(mode):
    if mode == "smoke":
        ladders, warn = _cap_ladders(LADDERS_SMOKE, os.cpu_count())
        gran_spec, levels = GRAN_SMOKE, LEVELS_SMOKE
        k_c = KC_SMOKE
        reps, warmup = REPS_SMOKE, WARMUP_SMOKE
        title = ("exp36 lane-count head-of-line diagnostic (SMOKE) -- structural "
                 "validation only; SMOKE-ONLY, not evidence")
    else:
        ladders, warn = _cap_ladders(LADDERS_FULL, os.cpu_count())
        gran_spec, levels = GRAN_FULL, LEVELS_FULL
        k_c = KC_FULL
        reps, warmup = REPS_FULL, WARMUP_FULL
        title = ("exp36 lane-count head-of-line diagnostic (FULL) -- homogeneous "
                 "many-core Linux; observation-only, machine-specific")

    print(title)
    _print_machine_info()
    print("NOTE -- NO-NEW-API lane-count HOL probe. Fix hpx_threads (worker pool),")
    print("  sweep num_lanes (serial FIFO admission slots). park_ms FREES the HPX")
    print("  worker, but the RuntimeLane consumer waits on hpx::async(...).get() and")
    print("  holds the lane for ~park_ms -> per-lane head-of-line. Tests whether more")
    print("  lanes dilute HOL and recover fine compute retention. Fine only, no W=32,")
    print("  no NUMA/binding, no priorities/pools/counters. park_ms synthetic (not I/O).")
    if mode != "smoke" and not sys.platform.startswith("linux"):
        print("  On Mac/laptop/heterogeneous hardware this --full output is "
              "SMOKE-ONLY, not evidence.")
    if warn:
        print(f"  {warn}")
    print(f"  ladders(hpx_threads->num_lanes)="
          f"{[(ht, list(s)) for ht, s in ladders]} "
          f"granularity={gran_spec[0]} levels={list(levels)} K_C={k_c} "
          f"park_ms={PARK_MS} reps={reps} warmup={warmup} "
          f"p99_min_samples={P99_MIN_SAMPLES}")
    print()

    cells, gates, skipped, reason = _run_ladders(
        ladders, gran_spec, levels, k_c, reps, warmup)
    if skipped:
        print(f"SKIP: {reason}")
        return 0

    _print_cell_table(cells)
    gates_ok = all(gates.values())
    print("  gates: " + ", ".join(f"{k}={v}" for k, v in sorted(gates.items())))
    print()
    _print_reading(cells, mode)

    if gates_ok:
        print("STRUCTURAL GATES: PASS")
        return 0
    print("STRUCTURAL GATES: FAIL")
    return 1


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--smoke", action="store_true",
                    help="structural smoke (the default): hpx_threads=4, "
                         "num_lanes={4,8}, fine, over, small K_C; SMOKE-ONLY")
    ap.add_argument("--full", action="store_true",
                    help="hpx_threads=4 (nl 4/8/16) + hpx_threads=8 (nl 8/16), "
                         "fine, near/over (homogeneous many-core Linux; no W=32)")
    args = ap.parse_args()
    return _run_and_print("full" if args.full else "smoke")


if __name__ == "__main__":
    sys.exit(main())
