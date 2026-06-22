#!/usr/bin/env python3
"""exp37 -- Ray-hosted RayX serial-blocking vs non-blocking op-lane comparison.

MECHANISM PROBE (observation-only, machine-specific). exp35 measured fine-grain
compute-class retention erosion under a synthetic parked+compute mix; Step-0 code
inspection localized the cause to per-lane head-of-line: the serial RuntimeLane
consumer dispatches an Async op via hpx::async(exec_, body).get() and does NOT pop
the next op until that op (including a full park_ms) completes. exp36 confirmed it by
DILUTING head-of-line across more independent FIFO lanes (raising num_lanes at fixed
hpx_threads recovered retention).

exp37 tests the experimental non-blocking op-lane prototype, which tries to REMOVE
per-lane head-of-line (rather than dilute it): the consumer dispatches Async ops via
hpx::async(...).then(continuation) WITHOUT the inline .get(), so it can pop and
dispatch more work while a parked op is suspended, bounded by max_inflight_per_lane.

QUESTION:
  At FIXED num_lanes == hpx_threads (the exact config that eroded in exp35/exp36),
  does switching op-lanes from serial-blocking to non-blocking recover fine-grain
  compute retention under the parked+compute mix, and how much of the result is the
  non-blocking RETIREMENT-PATH overhead rather than head-of-line removal?

RETIREMENT-PATH OVERHEAD IS A FIRST-CLASS CONFOUND (not free): the non-blocking path
adds a .then(...) continuation, per-lane mutex contention in that continuation, a
cv_.notify_all() wakeup, a completion-worker -> consumer-worker handoff, and in-flight
accounting -- none of which the serial .get() path has. exp37 measures the OVERHEAD
FLOOR explicitly with nb(max_inflight=1) (one Async op per lane at a time, but retired
through the continuation/cv path instead of .get()) and reads recovery ABOVE that
floor.

THREE ARMS per cell (exp35/exp36 structure):
  * Arm C  -- compute-only:  K_C  busy_sum(n_c).
  * Arm Wp -- parked-only:   K_Wp park_ms(ms).
  * Arm T  -- matched:       same K_C compute PLUS K_Wp parked, class-aware closed
               loop holding compute concurrency at O and adding parked on top.

PER (hpx_threads, level): one SERIAL cell (the stable anchor) plus a NON-BLOCKING
max_inflight sweep, all at the SAME num_lanes == hpx_threads -- the only intended
lever is op-lane dispatch behaviour (+ max_inflight within non-blocking). The driver
retires by READINESS (wait/as_completed), never by submission order: non-blocking
lanes complete ORDER-AGNOSTICALLY (the RuntimeFuture / OperationResult result contract
and the op-lane admission contract are unchanged; only per-lane completion order is).

GATES (the ONLY pass/fail): agg_ok, futures_completed, plain_types_ok, lane_ids_ok,
clean_shutdown. Timing / retention / qd / lane_stats are READING criteria only.
Exit 0 = gates passed or cleanly skipped; exit 1 = a structural gate failed.

NON-BLOCKING IS EXPERIMENTAL and OPERATION-LANE-ONLY (actor lanes are always serial;
their safety is covered by tests/integration/test_runtime_nonblocking_lane.py, not by
this experiment). exp37 is NOT a Ray Serve / real-I/O / inference / cluster /
capacity / sizing claim, NOT "HPX beats Ray" / "RayX makes Ray faster", and does NOT
recommend making non-blocking the default. lane_impl does not apply to rayx.runtime;
no W=32; no NUMA/binding; no priorities/pools/counters.

Usage:
    python experiments/37_ray_hosting_rayx_nonblocking_lane/run_ray_hosting_rayx_nonblocking_lane.py            # --smoke (default)
    python experiments/37_ray_hosting_rayx_nonblocking_lane/run_ray_hosting_rayx_nonblocking_lane.py --full
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
# exp35/exp36 -- exp37 changes how they are COMBINED (serial vs non-blocking sweep,
# overhead floor), not the bands.
RET_THR_SUPPORT = 0.90    # C throughput retention >= this = compute intact
RET_P90_SUPPORT = 1.20    # C p90 retention <= this = latency intact
RET_THR_STOP = 0.70       # C throughput retention <= this = degraded/eroded

# Compute-baseline-flatness guard: compute-only wall_C should stay within this
# fractional band across the modes/max_inflight at fixed hpx_threads (else the ladder
# is INCONCLUSIVE -- the mode/cap may be changing compute capacity / driver behaviour).
WALL_C_FLAT_TOL = 0.25

# "Clear recovery" needs at least this absolute gain from the nb(1) OVERHEAD FLOOR to
# the top max_inflight (recovery is read ABOVE the floor, not above serial).
RET_RISE_MIN = 0.10

# Overhead-floor bias guard: if nb(max_inflight=1) retention is more than this BELOW
# the serial anchor, the continuation/cv retirement path is biasing the floor and the
# serial-vs-nonblocking comparison is read as INCONCLUSIVE (biased). A nb(1) much
# ABOVE serial (while serial eroded) is also a confound (the mode helping without
# added in-flight concurrency) and is flagged the same way.
OVERHEAD_BIAS_TOL = 0.10

# How often (in retire iterations) to sample lane_stats during an arm.
LANE_SAMPLE_STRIDE = 3

# ---- smoke-mode parameters (laptop-safe DEFAULT; SMOKE-ONLY, not evidence) ----
HT_SMOKE = (4,)            # num_lanes == hpx_threads
MI_SWEEP_SMOKE = (1, 4)    # non-blocking max_inflight sweep
KC_SMOKE = 16
REPS_SMOKE = 2
WARMUP_SMOKE = 1
LEVELS_SMOKE = ("over",)
GRAN_SMOKE = ("fine", 2_000_000)

# ---- full-mode parameters (homogeneous many-core Linux; observation-only) ------
# Fine granularity only; num_lanes == hpx_threads; no W=32. Non-blocking max_inflight
# sweep {1,2,4}: nb(1) is the retirement-path OVERHEAD FLOOR, nb(2) the expected main
# head-of-line-removal step (one in-flight slot past the park), nb(4) the plateau/knee.
HT_FULL = (4, 8)
MI_SWEEP_FULL = (1, 2, 4)
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
    """Closed-loop window relative to hpx_threads (== num_lanes here): the offered
    concurrency is held constant across the serial-vs-nonblocking comparison so the
    only intended lever is op-lane dispatch behaviour."""
    if level == "under":
        return max(1, ht // 2)
    if level == "near":
        return ht
    return 4 * ht  # over


def _mode_label(nonblocking, max_inflight):
    return f"nb-mi{max_inflight}" if nonblocking else "serial"


def _mode_sort_key(cell):
    # serial first, then non-blocking by ascending max_inflight.
    return (-1, 0) if not cell["nonblocking"] else (1, cell["max_inflight"])


# --------------------------------------------------------------------------- #
# Ray actor (one per (hpx_threads, mode, max_inflight) cell; hosts ONE Runtime) #
# --------------------------------------------------------------------------- #
def _build_actor(ray):
    """Define the Ray actor inside a function so this module imports without Ray
    present (clean-skip path)."""

    @ray.remote
    class RayxModeActor:
        """Hosts ONE rayx.runtime.Runtime(num_lanes=NL, hpx_threads=HT) in either the
        default SERIAL op-lane mode or the experimental NON-BLOCKING op-lane mode
        (experimental_nonblocking_op_lanes=True, max_inflight_per_lane=MI). One actor
        per (HT, mode, MI) cell -- the mode / max_inflight are constructor-fixed and the
        HPX runtime is process-global, so a fresh process per cell is required.
        RuntimeFuture / OperationResult are created and retired INSIDE this actor; only
        plain scalars/lists/dicts cross the Ray boundary."""

        def __init__(self, num_lanes, hpx_threads, nonblocking, max_inflight,
                     rayx_src):
            import sys as _sys
            if rayx_src not in _sys.path:
                _sys.path.insert(0, rayx_src)
            from rayx.runtime import Runtime  # raises if _rayx missing
            if nonblocking:
                self._rt = Runtime(
                    num_lanes=num_lanes, hpx_threads=hpx_threads,
                    experimental_nonblocking_op_lanes=True,
                    max_inflight_per_lane=max_inflight)
            else:
                self._rt = Runtime(num_lanes=num_lanes, hpx_threads=hpx_threads)

        def lane_ids(self):
            return [d["actor_id"] for d in self._rt.lane_stats()]

        def run_arm(self, classes, sample_stride):
            """Run one arm. `classes` maps class label -> {op_id, arg, count,
            window}. Holds each class's concurrency at its window, refilling as ops
            retire. Retires by READINESS (order-agnostic). Returns ONLY plain rows."""
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
                # num_returns=1: retire whatever is READY (order-agnostic) -- required
                # because non-blocking lanes complete out of submission order.
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

    return RayxModeActor


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


def _run_ladders(ht_values, gran_spec, levels, mi_sweep, k_c, reps, warmup):
    """Run every (hpx_threads, level) ladder: one SERIAL cell + a NON-BLOCKING
    max_inflight sweep, all at num_lanes == hpx_threads. Returns (cells, gates_dict,
    skipped, reason)."""
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
    max_ht = max(ht_values)
    ray.init(num_cpus=max_ht + 1, ignore_reinit_error=True,
             log_to_driver=False, configure_logging=False)
    RayxModeActor = _build_actor(ray)
    cells = []
    try:
        for ht in ht_values:
            nl = ht  # num_lanes == hpx_threads (the exp35/exp36 eroded config)
            for level in levels:
                o = outstanding_for(level, ht)
                # Mode order: SERIAL first (the anchor; calibrates K_Wp), then the
                # non-blocking max_inflight sweep. K_Wp is held across all modes so the
                # only lever is op-lane dispatch behaviour.
                k_wp = None
                modes = [(False, None)] + [(True, mi) for mi in mi_sweep]
                for nonblocking, mi in modes:
                    actor = RayxModeActor.options(num_cpus=ht).remote(
                        nl, ht, nonblocking, (mi if mi is not None else 1), RAYX_SRC)
                    ids = ray.get(actor.lane_ids.remote())
                    if len(ids) != nl or not all(
                            isinstance(i, str) and i.startswith("rt-hpx-")
                            for i in ids):
                        gates.lane_ids_ok = False
                    arm_c = _measure_arm(
                        ray, actor,
                        {"C": {"op_id": "busy_sum", "arg": n_c, "count": k_c,
                               "window": o}},
                        reps, warmup, gates)
                    wall_C = arm_c["wall_compute_med"]
                    if k_wp is None:  # calibrate from the SERIAL baseline wall_C
                        k_wp_1x = (int(round(wall_C * o / PARK_MS))
                                   if PARK_MS > 0 else o)
                        k_wp = max(ht, min(K_WP_CAP, k_wp_1x))
                    arm_wp = _measure_arm(
                        ray, actor,
                        {"Wp": {"op_id": "park_ms", "arg": PARK_MS,
                                "count": k_wp, "window": o}},
                        reps, warmup, gates)
                    arm_t = _measure_arm(
                        ray, actor,
                        {"C": {"op_id": "busy_sum", "arg": n_c, "count": k_c,
                               "window": o},
                         "Wp": {"op_id": "park_ms", "arg": PARK_MS,
                                "count": k_wp, "window": o}},
                        reps, warmup, gates)
                    cells.append(_cell_metrics(
                        ht, nl, gran_name, n_c, level, o, k_c, k_wp, reps,
                        nonblocking, mi, arm_c, arm_wp, arm_t))
                    try:
                        ray.get(actor.shutdown.remote())
                    except Exception:
                        gates.clean_shutdown = False
                    ray.kill(actor)
    finally:
        ray.shutdown()
    return cells, gates.as_dict(), False, None


def _cell_metrics(ht, nl, gran, n_c, level, o, k_c, k_wp, reps,
                  nonblocking, max_inflight, arm_c, arm_wp, arm_t):
    """Reduce one (hpx_threads, level, mode) cell to a plain reading dict."""
    wall_C = arm_c["wall_compute_med"]
    wall_Wp = arm_wp["wall_total_med"]
    wall_CWp = arm_t["wall_compute_med"]
    wall_CWp_total = arm_t["wall_total_med"]

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
        "nonblocking": bool(nonblocking),
        "max_inflight": (None if max_inflight is None else int(max_inflight)),
        "mode_label": _mode_label(nonblocking, max_inflight),
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


def _cap_threads(ht_values, cpu):
    """Drop any hpx_threads cell that exceeds the local cpu_count (the worker pool
    can't exceed cores meaningfully); num_lanes == hpx_threads here."""
    if cpu is None:
        return list(ht_values), None
    kept = [ht for ht in ht_values if ht <= cpu]
    if not kept:
        kept = [1]
    if kept != list(ht_values):
        dropped = [ht for ht in ht_values if ht > cpu]
        return kept, (f"cpu_count={cpu} < some hpx_threads; dropped {dropped} "
                      f"(not appropriate on this machine).")
    return kept, None


def _print_cell_table(cells):
    last_key = None
    for c in sorted(cells, key=lambda x: (x["ht"], x["level"], _mode_sort_key(x))):
        key = (c["ht"], c["level"])
        if key != last_key:
            print(f"  hpx_threads={c['ht']}  num_lanes={c['nl']}  load={c['level']} "
                  f"outstanding={c['outstanding']}  granularity={c['gran']} "
                  f"(n_c={c['n_c']}, cpc={checkpoint_count(c['n_c'])})  "
                  f"K_C={c['k_c']} K_Wp={c['k_wp']} reps={c['reps']}")
            last_key = key
        anchor = " (serial anchor)" if not c["nonblocking"] else ""
        print(f"    mode={c['mode_label']:<8}{anchor:<16} "
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
    """Graded reading for ONE (hpx_threads, level) ladder: a SERIAL anchor + a
    non-blocking max_inflight sweep. Reads recovery ABOVE the nb(1) retirement-path
    OVERHEAD FLOOR, across the max_inflight sweep. Returns (verdict, message)."""
    serial = next((c for c in ladder_cells if not c["nonblocking"]), None)
    nbs = sorted([c for c in ladder_cells if c["nonblocking"]],
                 key=lambda c: c["max_inflight"])
    if serial is None or not nbs:
        return ("INCONCLUSIVE", "ladder missing the serial anchor or non-blocking "
                                "sweep (internal).")
    ht, level = serial["ht"], serial["level"]
    tag = f"hpx_threads={ht} num_lanes={ht} load={level}"
    nb1 = next((c for c in nbs if c["max_inflight"] == 1), None)
    top = nbs[-1]
    rs = serial["thr_retention"]
    seq = (f"serial={_fmt(rs)} -> " +
           " -> ".join(f"{c['mode_label']}={_fmt(c['thr_retention'])}" for c in nbs))

    # --- Control 1: positive control (serial anchor must erode) ---
    if not (rs == rs and rs <= RET_THR_STOP):
        return ("INCONCLUSIVE",
                f"[{tag}] serial anchor did NOT reproduce erosion "
                f"(thr_retention={_fmt(rs)} > {RET_THR_STOP:g}): no head-of-line to "
                "remove, so the serial-vs-nonblocking comparison is untestable here.")

    # --- Control 2: driver / lane_stats (matched arms must be backlogged/active) ---
    backlog_any = any(c["lane_trace_T"]["backlog_seen"] for c in ladder_cells)
    active_mean = statistics.fmean(
        [c["lane_trace_T"]["active_fraction"] for c in ladder_cells])
    if not backlog_any and active_mean < 0.1:
        return ("INCONCLUSIVE",
                f"[{tag}] lane_stats shows HPX not backlogged/active in the matched "
                "arms (driver-governed): wall indicators measure the CPython driver, "
                "not the scheduler. Raise K / windows.")

    # --- Control 3: compute-baseline flatness across modes/max_inflight ---
    wall_cs = [c["wall_C"] for c in ladder_cells if c["wall_C"] > 0]
    if wall_cs:
        wmin, wmax = min(wall_cs), max(wall_cs)
        if wmin > 0 and (wmax - wmin) / wmin > WALL_C_FLAT_TOL:
            curve = " ".join(f"{c['mode_label']}:{_fmt(c['wall_C'],'.1f')}ms"
                             for c in [serial] + nbs)
            return ("INCONCLUSIVE",
                    f"[{tag}] compute-only wall_C is NOT flat across modes "
                    f"(>{WALL_C_FLAT_TOL:.0%}: {curve}): the mode/cap may be changing "
                    "compute capacity or driver behaviour, so retention recovery "
                    "cannot be cleanly attributed to head-of-line removal.")

    # --- Control 4: retirement-path OVERHEAD FLOOR (nb(1) vs serial) ---
    floor_note = "nb(1) overhead floor unavailable"
    if nb1 is not None and rs == rs and nb1["thr_retention"] == nb1["thr_retention"]:
        nb1_minus_serial = nb1["thr_retention"] - rs
        floor_note = (f"nb1_minus_serial={_fmt(nb1_minus_serial)} "
                      f"(serial={_fmt(rs)}, nb-mi1={_fmt(nb1['thr_retention'])})")
        if rs - nb1["thr_retention"] > OVERHEAD_BIAS_TOL:
            return ("INCONCLUSIVE",
                    f"[{tag}] nb(max_inflight=1) is materially BELOW the serial anchor "
                    f"({floor_note}): the continuation/cv retirement path is biasing "
                    "the overhead floor, so the serial-vs-nonblocking comparison is "
                    "biased by retirement overhead, not readable as head-of-line "
                    "removal.")
        if nb1["thr_retention"] - rs > 0.20:
            return ("INCONCLUSIVE",
                    f"[{tag}] nb(max_inflight=1) is well ABOVE the serial anchor "
                    f"({floor_note}) while serial eroded: the mode is recovering "
                    "WITHOUT added in-flight concurrency -- a confound (one Async op "
                    "per lane should behave ~serially); not cleanly readable.")

    # --- HOL corroboration: does queue depth fall as max_inflight rises? ---
    qd_base = nb1["qd_mean_T"] if nb1 is not None else nbs[0]["qd_mean_T"]
    qd_top = top["qd_mean_T"]
    qd_falls = (qd_base > 0 and qd_top < qd_base)
    qd_note = ("queue depth fell with more in-flight (qd_mean_T "
               f"{_fmt(qd_base,'.1f')}->{_fmt(qd_top,'.1f')})" if qd_falls else
               "queue depth did NOT fall with more in-flight (qd_mean_T "
               f"{_fmt(qd_base,'.1f')}->{_fmt(qd_top,'.1f')}) -- the head-of-line "
               "story is INCOMPLETE for this ladder")

    # --- Trend / graded reading (recovery measured ABOVE the nb(1) floor) ---
    floor_ret = nb1["thr_retention"] if nb1 is not None else nbs[0]["thr_retention"]
    nb_rets = [c["thr_retention"] for c in nbs]
    rise = ((top["thr_retention"] - floor_ret)
            if top["thr_retention"] == top["thr_retention"]
            and floor_ret == floor_ret else float("nan"))
    monotone = all(nb_rets[i + 1] >= nb_rets[i] - 0.03
                   for i in range(len(nb_rets) - 1))
    top_recovers = (top["thr_retention"] == top["thr_retention"]
                    and top["thr_retention"] >= RET_THR_SUPPORT
                    and (top["p90_ret"] != top["p90_ret"]
                         or top["p90_ret"] <= RET_P90_SUPPORT))
    shape = ("step at nb-mi2"
             if (len(nbs) >= 3 and nb_rets[1] - nb_rets[0] >= RET_RISE_MIN
                 and nb_rets[-1] - nb_rets[1] < RET_RISE_MIN)
             else "ramp across the sweep")

    if top_recovers and rise >= RET_RISE_MIN and qd_falls:
        return ("FULL SUPPORT",
                f"[{tag}] serial eroded ({seq}); non-blocking RECOVERED compute above "
                f"the retirement-path floor (top {top['mode_label']}="
                f"{_fmt(top['thr_retention'])}, p90 {_fmt(top['p90_ret'])}; "
                f"{floor_note}; {shape}); {qd_note}. At fixed num_lanes==hpx_threads "
                "(no added lanes/workers), the non-blocking op-lane removes per-lane "
                "head-of-line within the op-lane admission contract.")
    if rise >= RET_RISE_MIN and monotone:
        return ("PARTIAL SUPPORT",
                f"[{tag}] non-blocking retention rises clearly with max_inflight "
                f"({seq}) but the top cell did not fully reach the SUPPORT band "
                f"(top={_fmt(top['thr_retention'])}, p90={_fmt(top['p90_ret'])}; "
                f"{floor_note}; {shape}); {qd_note}. Non-blocking dispatch is a MAJOR "
                "lever, but a residual remains -- retirement-path overhead "
                "(continuation / per-lane mutex / notify / handoff) or another "
                "limiter beyond single-slot head-of-line.")
    if (top["thr_retention"] == top["thr_retention"]
            and top["thr_retention"] <= RET_THR_STOP and rise < RET_RISE_MIN):
        return ("STOP",
                f"[{tag}] the current non-blocking PROTOTYPE did not recover retention "
                f"across the sweep ({seq}; top={_fmt(top['thr_retention'])} <= "
                f"{RET_THR_STOP:g}; {floor_note}); {qd_note}. This does NOT refute the "
                "head-of-line mechanism (Step-0 + exp36 already support it) -- it "
                "indicates retirement-path overhead / continuation churn / handoff "
                "cost dominated, or another residual limiter became dominant.")
    return ("INCONCLUSIVE",
            f"[{tag}] non-blocking retention trend is noisy/non-monotone/mid-band "
            f"without a clear slope ({seq}; {floor_note}); {qd_note}. Cannot cleanly "
            "separate head-of-line removal from retirement-path overhead / noise.")


def _print_reading(cells, mode):
    if mode == "smoke":
        print("READING (observation-only) [INCONCLUSIVE (smoke-only)]: smoke "
              "validates the serial-vs-nonblocking ladder, the max_inflight sweep, the "
              "three-arm / calibration / lane_stats structure, and order-agnostic "
              "retirement only -- sample counts are low and this is NOT evidence. Run "
              "--full on a homogeneous many-core Linux node for an observation.")
    else:
        keys = sorted({_ladder_key(c) for c in cells})
        print("READING (observation-only, this run/machine) -- per-ladder graded "
              "serial-vs-nonblocking verdict (recovery read ABOVE the nb(1) "
              "retirement-path overhead floor):")
        for k in keys:
            ladder = [c for c in cells if _ladder_key(c) == k]
            verdict, msg = _interpret_ladder(ladder)
            print(f"  [{verdict}] {msg}")
    print("  Step-0 mechanism: the SERIAL RuntimeLane consumer does "
          "hpx::async(exec_, body).get(), so a park holds its lane until it completes "
          "(per-lane head-of-line); exp36 confirmed it by DILUTING head-of-line across "
          "more independent FIFO lanes. The NON-BLOCKING op-lane uses .then(...) "
          "without the inline .get(), trying to REMOVE per-lane head-of-line up to "
          "max_inflight.")
    print("  exp36 is an in-flight-capacity ANALOGY, not an equivalence: exp36 "
          "diluted head-of-line across N independent FIFOs; exp37 removes it per lane "
          "up to the in-flight cap. Use exp36 only as a cross-experiment sanity check.")
    print("  The non-blocking RETIREMENT PATH is a first-class confound (continuation "
          "/ per-lane mutex contention / cv_.notify_all() wakeup / completion-worker "
          "-> consumer-worker handoff / in-flight accounting); nb(max_inflight=1) is "
          "its OVERHEAD FLOOR and recovery is read above it. Expected shape: serial "
          "eroded, nb(1) ~= serial (possibly slightly worse), nb(2) the main step, "
          "nb(4) plateau/knee; a smooth ramp is itself evidence of a residual limiter "
          "beyond single-slot head-of-line.")
    print("  Non-blocking is EXPERIMENTAL and operation-lane-only; actor lanes are "
          "always serial (their safety is covered by the prototype integration tests, "
          "not this experiment). The RuntimeFuture / OperationResult result contract "
          "and the op-lane admission contract are unchanged; only per-lane completion "
          "order is relaxed (order-agnostic completion).")
    print("  park_ms is SYNTHETIC cooperative parked wait (NOT real I/O, NOT "
          "inference). Not Ray Serve, not cluster scaling, not HPX priority "
          "scheduling, not NUMA, not a latency-SLO/capacity/performance claim, and "
          "NOT a recommendation to make non-blocking the default.")
    print()


def _run_and_print(mode):
    if mode == "smoke":
        ht_values, warn = _cap_threads(HT_SMOKE, os.cpu_count())
        gran_spec, levels = GRAN_SMOKE, LEVELS_SMOKE
        mi_sweep = MI_SWEEP_SMOKE
        k_c, reps, warmup = KC_SMOKE, REPS_SMOKE, WARMUP_SMOKE
        title = ("exp37 serial-vs-nonblocking op-lane (SMOKE) -- structural "
                 "validation only; SMOKE-ONLY, not evidence")
    else:
        ht_values, warn = _cap_threads(HT_FULL, os.cpu_count())
        gran_spec, levels = GRAN_FULL, LEVELS_FULL
        mi_sweep = MI_SWEEP_FULL
        k_c, reps, warmup = KC_FULL, REPS_FULL, WARMUP_FULL
        title = ("exp37 serial-vs-nonblocking op-lane (FULL) -- homogeneous "
                 "many-core Linux; observation-only, machine-specific")

    print(title)
    _print_machine_info()
    print("NOTE -- serial-blocking vs EXPERIMENTAL non-blocking op-lanes at FIXED")
    print("  num_lanes == hpx_threads (the exp35/exp36 eroded config). The serial")
    print("  consumer holds its lane on hpx::async(...).get(); the non-blocking lane")
    print("  uses .then(...) to remove per-lane head-of-line up to max_inflight. The")
    print("  non-blocking RETIREMENT PATH is a confound; nb(max_inflight=1) is its")
    print("  overhead floor. Fine only, no W=32, no NUMA/binding, no priorities/pools/")
    print("  counters. park_ms is synthetic (not I/O). Experimental, op-lanes only.")
    if mode != "smoke" and not sys.platform.startswith("linux"):
        print("  On Mac/laptop/heterogeneous hardware this --full output is "
              "SMOKE-ONLY, not evidence.")
    if warn:
        print(f"  {warn}")
    print(f"  hpx_threads(=num_lanes)={list(ht_values)} max_inflight_sweep="
          f"{list(mi_sweep)} granularity={gran_spec[0]} levels={list(levels)} "
          f"K_C={k_c} park_ms={PARK_MS} reps={reps} warmup={warmup} "
          f"p99_min_samples={P99_MIN_SAMPLES}")
    print()

    cells, gates, skipped, reason = _run_ladders(
        ht_values, gran_spec, levels, mi_sweep, k_c, reps, warmup)
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
                         "num_lanes=4, fine, over, serial + nb(mi in {1,4}), small "
                         "K_C; SMOKE-ONLY")
    ap.add_argument("--full", action="store_true",
                    help="hpx_threads(=num_lanes) in {4,8}, fine, near/over, serial + "
                         "nb(max_inflight in {1,2,4}) (homogeneous many-core Linux)")
    args = ap.parse_args()
    return _run_and_print("full" if args.full else "smoke")


if __name__ == "__main__":
    sys.exit(main())
